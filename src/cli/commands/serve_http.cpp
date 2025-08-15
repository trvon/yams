#include <yams/mcp/mcp_server.h>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <signal.h>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "Ws2_32.lib")
using socket_t = SOCKET;
static constexpr socket_t INVALID_SOCKET_FD = INVALID_SOCKET;
static inline void close_socket(socket_t s) {
    closesocket(s);
}
static inline int get_last_sock_err() {
    return WSAGetLastError();
}
#else
#include <fcntl.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
using socket_t = int;
static constexpr socket_t INVALID_SOCKET_FD = -1;
static inline void close_socket(socket_t s) {
    ::close(s);
}
static inline int get_last_sock_err() {
    return errno;
}
#endif

namespace yams::cli {
using json = nlohmann::json;

// ========== Utility ==========

static inline std::string ltrim(std::string s) {
    s.erase(s.begin(),
            std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isspace(c); }));
    return s;
}
static inline std::string rtrim(std::string s) {
    s.erase(
        std::find_if(s.rbegin(), s.rend(), [](unsigned char c) { return !std::isspace(c); }).base(),
        s.end());
    return s;
}
static inline std::string trim(std::string s) {
    return rtrim(ltrim(std::move(s)));
}
static inline std::string to_lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

static inline std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::string cur;
    std::istringstream iss(s);
    while (std::getline(iss, cur, delim))
        out.push_back(cur);
    return out;
}

static inline std::unordered_set<std::string> parse_allowlist_env() {
    std::unordered_set<std::string> out;
    const char* env = std::getenv("YAMS_MCP_ALLOW_ORIGINS");
    if (!env || !*env) {
        // default allowlist for local development
        out.insert("http://localhost");
        out.insert("http://127.0.0.1");
        out.insert("null");
        return out;
    }
    std::string s(env);
    for (auto& token : split(s, ',')) {
        token = trim(token);
        if (!token.empty())
            out.insert(token);
    }
    return out;
}

static inline bool origin_allowed(const std::unordered_set<std::string>& allow,
                                  const std::string& origin) {
    if (origin.empty())
        return true; // treat empty as local invocation
    if (allow.find(origin) != allow.end())
        return true;
    // Normalize localhost origins with trailing slash variations removed
    std::string norm = origin;
    if (!norm.empty() && norm.back() == '/')
        norm.pop_back();
    if (allow.find(norm) != allow.end())
        return true;
    return false;
}

static inline bool send_all(socket_t s, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
#ifdef _WIN32
        int n = ::send(s, data + sent, static_cast<int>(len - sent), 0);
#else
        ssize_t n = ::send(s, data + sent, len - sent, 0);
#endif
        if (n <= 0)
            return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static inline bool send_str(socket_t s, const std::string& str) {
    return send_all(s, str.data(), str.size());
}

static inline bool read_line(socket_t s, std::string& out) {
    out.clear();
    char c;
    while (true) {
#ifdef _WIN32
        int n = ::recv(s, &c, 1, 0);
#else
        ssize_t n = ::recv(s, &c, 1, 0);
#endif
        if (n <= 0) {
            return false;
        }
        if (c == '\r') {
            // peek next
#ifdef _WIN32
            int m = ::recv(s, &c, 1, MSG_PEEK);
#else
            ssize_t m = ::recv(s, &c, 1, MSG_PEEK);
#endif
            if (m > 0 && c == '\n') {
#ifdef _WIN32
                ::recv(s, &c, 1, 0);
#else
                ::recv(s, &c, 1, 0);
#endif
            }
            break;
        }
        if (c == '\n')
            break;
        out.push_back(c);
        // protect from extremely long lines
        if (out.size() > 64 * 1024)
            return false;
    }
    return true;
}

static inline bool read_exact(socket_t s, size_t len, std::string& out) {
    out.clear();
    out.resize(len);
    size_t got = 0;
    while (got < len) {
#ifdef _WIN32
        int n = ::recv(s, &out[got], static_cast<int>(len - got), 0);
#else
        ssize_t n = ::recv(s, &out[got], len - got, 0);
#endif
        if (n <= 0)
            return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

struct HttpRequest {
    std::string method;
    std::string path;
    std::string version;
    std::unordered_map<std::string, std::string> headers; // lower-cased keys
    std::string body;
};

static inline bool parse_request(socket_t s, HttpRequest& req) {
    std::string line;
    if (!read_line(s, line))
        return false;
    line = trim(line);
    // Request line: METHOD SP PATH SP HTTP/1.1
    {
        std::istringstream iss(line);
        if (!(iss >> req.method))
            return false;
        if (!(iss >> req.path))
            return false;
        if (!(iss >> req.version))
            return false;
    }

    // headers
    while (true) {
        if (!read_line(s, line))
            return false;
        if (line.empty())
            break; // end headers
        auto pos = line.find(':');
        if (pos == std::string::npos)
            return false;
        std::string key = to_lower(trim(line.substr(0, pos)));
        std::string val = trim(line.substr(pos + 1));
        req.headers[key] = val;
    }

    // body
    size_t content_length = 0;
    if (auto it = req.headers.find("content-length"); it != req.headers.end()) {
        content_length = static_cast<size_t>(std::stoull(it->second));
    }
    if (content_length > 0) {
        if (!read_exact(s, content_length, req.body))
            return false;
    } else if (req.method == "POST") {
        // No Content-Length for POST; reject
        return false;
    }
    return true;
}

static inline void write_response(socket_t s, int status, const std::string& reason,
                                  const std::string& content_type, const std::string& body,
                                  const std::string& origin_header = "", bool cors = true,
                                  bool allow_post_only = false) {
    std::ostringstream oss;
    oss << "HTTP/1.1 " << status << " " << reason << "\r\n";
    if (!content_type.empty()) {
        oss << "Content-Type: " << content_type << "\r\n";
    }
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "Connection: close\r\n";

    if (cors) {
        if (!origin_header.empty()) {
            oss << "Access-Control-Allow-Origin: " << origin_header << "\r\n";
            oss << "Vary: Origin\r\n";
        }
        oss << "Access-Control-Allow-Methods: POST, GET, OPTIONS\r\n";
        oss << "Access-Control-Allow-Headers: Content-Type, Accept, Mcp-Session-Id\r\n";
        if (allow_post_only) {
            oss << "Allow: POST\r\n";
        }
    }

    oss << "\r\n";
    auto head = oss.str();
    send_str(s, head);
    if (!body.empty())
        send_str(s, body);
}

static inline void write_405(socket_t s, const std::string& origin_header) {
    write_response(s, 405, "Method Not Allowed", "text/plain", "Method Not Allowed", origin_header,
                   true, true);
}
static inline void write_404(socket_t s, const std::string& origin_header) {
    write_response(s, 404, "Not Found", "text/plain", "Not Found", origin_header);
}
static inline void write_400(socket_t s, const std::string& origin_header,
                             const std::string& msg = "Bad Request") {
    write_response(s, 400, "Bad Request", "text/plain", msg, origin_header);
}
static inline void write_403(socket_t s, const std::string& origin_header) {
    write_response(s, 403, "Forbidden", "text/plain", "Forbidden", origin_header);
}
static inline void write_202(socket_t s, const std::string& origin_header) {
    write_response(s, 202, "Accepted", "text/plain", "", origin_header);
}
static inline void write_200_json(socket_t s, const std::string& origin_header,
                                  const std::string& body) {
    write_response(s, 200, "OK", "application/json", body, origin_header);
}
static inline void write_204_options(socket_t s, const std::string& origin_header) {
    write_response(s, 204, "No Content", "text/plain", "", origin_header);
}

// ========== HTTP server core ==========

class MinimalHttpServer {
public:
    MinimalHttpServer(mcp::MCPServer& server, std::string bindHost, uint16_t port, std::string path,
                      std::atomic<bool>* shutdownFlag = nullptr)
        : server_(server), host_(std::move(bindHost)), port_(port), path_(std::move(path)),
          shutdown_(shutdownFlag) {}

    // Blocking run-loop
    void run() {
#ifdef _WIN32
        WSADATA wsaData;
        int wsaerr = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (wsaerr != 0) {
            spdlog::error("WSAStartup failed: {}", wsaerr);
            return;
        }
#endif
        auto allowlist = parse_allowlist_env();

        socket_t srv = INVALID_SOCKET_FD;
        addrinfo hints{};
        addrinfo* res = nullptr;

        hints.ai_family = AF_INET; // IPv4 for simplicity
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_flags = AI_PASSIVE;

        std::string portStr = std::to_string(port_);

        int gai =
            ::getaddrinfo(host_.empty() ? nullptr : host_.c_str(), portStr.c_str(), &hints, &res);
        if (gai != 0 || !res) {
            spdlog::error("getaddrinfo failed: {}", gai);
            cleanup_winsock();
            return;
        }

        srv = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (srv == INVALID_SOCKET_FD) {
            spdlog::error("socket() failed: {}", get_last_sock_err());
            ::freeaddrinfo(res);
            cleanup_winsock();
            return;
        }

#ifndef _WIN32
        int opt = 1;
        setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#else
        BOOL opt = TRUE;
        setsockopt(srv, SOL_SOCKET, SO_EXCLUSIVEADDRUSE, (const char*)&opt, sizeof(opt));
#endif

        if (::bind(srv, res->ai_addr, static_cast<int>(res->ai_addrlen)) != 0) {
            spdlog::error(
                "bind() failed on {}:{} (errno={}): another process may already be listening. If "
                "you recently closed a client, wait a moment and retry or change --port.",
                host_, port_, get_last_sock_err());
            close_socket(srv);
            ::freeaddrinfo(res);
            cleanup_winsock();
            return;
        }
        ::freeaddrinfo(res);

        if (::listen(srv, 16) != 0) {
            spdlog::error("listen() failed: {}", get_last_sock_err());
            close_socket(srv);
            cleanup_winsock();
            return;
        }

        spdlog::info("YAMS MCP HTTP server listening on {}:{}{}", host_, port_, path_);
        // Initialize idle timer and optionally start parent-process monitor
        lastRequest_ = std::chrono::steady_clock::now();
        start_parent_watch();
        // Read idle-exit from env (seconds); CLI flag can override via env until wired
        if (const char* idle = std::getenv("YAMS_MCP_IDLE_EXIT")) {
            int secs = std::atoi(idle);
            if (secs > 0)
                idleExitSeconds_ = secs;
        }

        while (!should_stop()) {
            // Idle-exit: shut down after N seconds without requests
            if (idleExitSeconds_ > 0) {
                auto now = std::chrono::steady_clock::now();
                auto idleFor =
                    std::chrono::duration_cast<std::chrono::seconds>(now - lastRequest_).count();
                if (idleFor >= idleExitSeconds_) {
                    spdlog::info("Idle timeout ({}s) reached, shutting down HTTP server",
                                 idleExitSeconds_);
                    break;
                }
            }
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(srv, &rfds);
            timeval tv{};
            tv.tv_sec = 0;
            tv.tv_usec = 250 * 1000; // 250ms accept poll
            int sel = ::select(static_cast<int>(srv + 1), &rfds, nullptr, nullptr, &tv);
            if (sel <= 0) {
                continue; // timeout or error; check shutdown and loop
            }

            sockaddr_storage cliaddr{};
            socklen_t clilen = sizeof(cliaddr);
            socket_t cli = ::accept(srv, (sockaddr*)&cliaddr, &clilen);
            if (cli == INVALID_SOCKET_FD) {
                continue;
            }

            std::thread(&MinimalHttpServer::handle_client, this, cli, allowlist).detach();
        }

        close_socket(srv);
        cleanup_winsock();
        spdlog::info("YAMS MCP HTTP server stopped");
    }

private:
    bool should_stop() const { return shutdown_ && shutdown_->load(); }

    static std::string get_header(const std::unordered_map<std::string, std::string>& h,
                                  const std::string& key) {
        auto it = h.find(key);
        if (it == h.end())
            return "";
        return it->second;
    }

    // Parent-process monitoring: if launcher exits, stop the server
    void start_parent_watch() {
        // Optional explicit parent PID via env (works cross-platform when provided)
        int envParentPid = 0;
        if (const char* p = std::getenv("YAMS_PARENT_PID")) {
            envParentPid = std::atoi(p);
        }
#ifndef _WIN32
        pid_t initialPpid = getppid();
#endif
        if (envParentPid > 0
#ifndef _WIN32
            || initialPpid > 1
#endif
        ) {
            monitorThread_ = std::thread([this, envParentPid
#ifndef _WIN32
                                          ,
                                          initialPpid
#endif
            ]() {
                using namespace std::chrono_literals;
                while (!should_stop()) {
                    std::this_thread::sleep_for(500ms);
                    bool parentGone = false;
                    if (envParentPid > 0) {
#ifndef _WIN32
                        if (::kill(envParentPid, 0) == -1 && errno == ESRCH)
                            parentGone = true;
#else
                        // Best-effort on Windows without extra deps: rely on envParentPid not
                        // changing Optionally, add a lightweight named event in a future pass
#endif
                    } else {
#ifndef _WIN32
                        // If parent becomes '1' (init/launchd), original parent exited
                        if (getppid() == 1 && initialPpid != 1)
                            parentGone = true;
#endif
                    }
                    if (parentGone) {
                        spdlog::info("Detected launcher process exit; shutting down HTTP server");
                        if (shutdown_)
                            shutdown_->store(true);
                        break;
                    }
                }
            });
            monitorThread_.detach();
        }
    }

    void handle_client(socket_t cli, const std::unordered_set<std::string>& allowlist) {
        HttpRequest req;
        if (!parse_request(cli, req)) {
            write_400(cli, "");
            close_socket(cli);
            return;
        }

        const std::string origin = get_header(req.headers, "origin");
        if (!origin_allowed(allowlist, origin)) {
            write_403(cli, origin);
            close_socket(cli);
            return;
        }

        // CORS preflight
        if (req.method == "OPTIONS") {
            write_204_options(cli, origin.empty() ? "*" : origin);
            close_socket(cli);
            return;
        }

        // Only /mcp endpoint
        if (req.path != path_) {
            if (req.method == "GET")
                write_405(cli, origin);
            else
                write_404(cli, origin);
            close_socket(cli);
            return;
        }

        if (req.method == "GET") {
            // SSE not implemented in Phase 1
            write_405(cli, origin);
            close_socket(cli);
            return;
        }

        if (req.method != "POST") {
            write_405(cli, origin);
            close_socket(cli);
            return;
        }

        // Parse JSON body
        json body;
        try {
            body = json::parse(req.body);
        } catch (const std::exception& e) {
            write_400(cli, origin, std::string("Invalid JSON: ") + e.what());
            close_socket(cli);
            return;
        }

        // Process per Streamable HTTP rules:
        // - If body contains requests (with id), we must return a JSON (single/batch) with
        // corresponding responses.
        // - If body contains only notifications/responses: return 202 Accepted with no body.
        std::vector<json> responses;
        auto process_one = [&](const json& msg) {
            // If it's a pure response from client, ignore (server shouldn't receive, but spec
            // permits)
            if (msg.is_object() && (msg.contains("result") || msg.contains("error"))) {
                return;
            }
            // delegate to MCPServer
            json resp = server_.processMessage(msg);
            if (!resp.is_null()) {
                // serialize access to MCPServer if needed
                std::lock_guard<std::mutex> lk(m_);
                responses.push_back(std::move(resp));
            }
        };

        if (body.is_array()) {
            for (const auto& item : body) {
                if (!item.is_object())
                    continue;
                process_one(item);
            }
        } else if (body.is_object()) {
            process_one(body);
        } else {
            write_400(cli, origin, "Body must be a JSON object or array");
            // Update idle timer on completed request
            lastRequest_ = std::chrono::steady_clock::now();
            close_socket(cli);
            return;
        }

        if (responses.empty()) {
            write_202(cli, origin.empty() ? "*" : origin);
        } else if (responses.size() == 1) {
            write_200_json(cli, origin.empty() ? "*" : origin, responses.front().dump());
        } else {
            write_200_json(cli, origin.empty() ? "*" : origin, json(responses).dump());
        }

        close_socket(cli);
    }

    static void cleanup_winsock() {
#ifdef _WIN32
        WSACleanup();
#endif
    }

private:
    mcp::MCPServer& server_;
    std::string host_;
    uint16_t port_;
    std::string path_;
    std::atomic<bool>* shutdown_;
    std::mutex m_; // serialize server access for safety
    // Idle-exit and parent-monitoring state
    std::chrono::steady_clock::time_point lastRequest_{};
    int idleExitSeconds_{0};
    std::thread monitorThread_;
};

// Public entry point for HTTP serving.
// This blocks until shutdownFlag is set (if provided) or on fatal error.
void run_mcp_http_server(mcp::MCPServer& server, const std::string& host, uint16_t port,
                         const std::string& path, std::atomic<bool>* shutdownFlag) {
    MinimalHttpServer http(server, host, port, path, shutdownFlag);
    http.run();
}

} // namespace yams::cli
