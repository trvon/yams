#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <string>
#include <string_view>
#include <thread>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>
#endif

namespace yams::daemon {

namespace {
inline std::string sanitize_for_terminal(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (unsigned char c : in) {
        if (c >= 0x20 && c <= 0x7E) {
            out.push_back(static_cast<char>(c));
        } else if (c == '\n' || c == '\r' || c == '\t') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('?');
        }
    }
    return out;
}

#ifndef _WIN32
[[maybe_unused]] bool canWriteToDirectory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;
    if (!fs::exists(dir))
        return false;
    auto testFile = dir / (".yams-test-" + std::to_string(getpid()));
    std::ofstream test(testFile);
    if (test.good()) {
        test.close();
        std::error_code ec;
        fs::remove(testFile, ec);
        return true;
    }
    return false;
}

[[maybe_unused]] std::filesystem::path getXDGRuntimeDir() {
    const char* xdgRuntime = std::getenv("XDG_RUNTIME_DIR");
    return xdgRuntime ? std::filesystem::path(xdgRuntime) : std::filesystem::path();
}
#endif

} // namespace

// Implementation class
class DaemonClient::Impl {
public:
    explicit Impl(const ClientConfig& config)
        : config_(config), headerTimeout_(config.headerTimeout), bodyTimeout_(config.bodyTimeout) {}

    ~Impl() = default;

    ClientConfig config_;
    CircuitBreaker breaker_;
    std::chrono::milliseconds headerTimeout_{30000}; // 30s default
    std::chrono::milliseconds bodyTimeout_{60000};   // 60s default
    TransportOptions transportOptions_;
    std::shared_ptr<AsioConnectionPool> pool_;

    void refresh_transport() {
        transportOptions_.socketPath = config_.socketPath;
        transportOptions_.headerTimeout = headerTimeout_;
        transportOptions_.bodyTimeout = bodyTimeout_;
        transportOptions_.requestTimeout = config_.requestTimeout;
        transportOptions_.maxInflight = config_.maxInflight;
        transportOptions_.poolEnabled = !config_.singleUseConnections;
        pool_ = AsioConnectionPool::get_or_create(transportOptions_);
    }
};

// DaemonClient implementation
DaemonClient::DaemonClient(const ClientConfig& config) : pImpl(std::make_unique<Impl>(config)) {
    if (pImpl->config_.socketPath.empty()) {
        // Prefer config-first resolution without depending on IPC FSM
        // Try env -> config.toml (daemon.socket_path) -> defaults
        namespace fs = std::filesystem;
        [[maybe_unused]] auto resolveSocketPathConfigFirstLocal = []() -> fs::path {
            if (const char* env = std::getenv("YAMS_DAEMON_SOCKET"); env && *env) {
                return fs::path(env);
            }
            // Minimal config reader for daemon.socket_path
            fs::path cfgPath;
            if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
                cfgPath = fs::path(cfgEnv);
            } else if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
                cfgPath = fs::path(xdg) / "yams" / "config.toml";
            } else if (const char* home = std::getenv("HOME")) {
                cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
            }
            if (!cfgPath.empty() && fs::exists(cfgPath)) {
                try {
                    std::ifstream f(cfgPath);
                    std::string line;
                    bool in_daemon = false;
                    auto ltrim = [](std::string& s) {
                        s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                                    return !std::isspace(ch);
                                }));
                    };
                    auto rtrim = [](std::string& s) {
                        s.erase(std::find_if(s.rbegin(), s.rend(),
                                             [](unsigned char ch) { return !std::isspace(ch); })
                                    .base(),
                                s.end());
                    };
                    while (std::getline(f, line)) {
                        ltrim(line);
                        rtrim(line);
                        if (line.empty() || line[0] == '#')
                            continue;
                        if (line.front() == '[') {
                            in_daemon = (line == "[daemon]" || line == "[ daemon ]");
                            continue;
                        }
                        auto pos = line.find('=');
                        if (pos == std::string::npos)
                            continue;
                        std::string key = line.substr(0, pos);
                        std::string val = line.substr(pos + 1);
                        ltrim(key);
                        rtrim(key);
                        ltrim(val);
                        rtrim(val);
                        if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                                                (val.front() == '\'' && val.back() == '\''))) {
                            val = val.substr(1, val.size() - 2);
                        }
                        if (key == "daemon.socket_path" || (in_daemon && key == "socket_path")) {
                            if (!val.empty())
                                return fs::path(val);
                        }
                    }
                } catch (...) {
                }
            }
            // Fallbacks
            if (const char* xdgRun = std::getenv("XDG_RUNTIME_DIR")) {
                return fs::path(xdgRun) / "yams-daemon.sock";
            }
            // Non-root: temp dir; root: /var/run
            if (::geteuid() == 0) {
                return fs::path("/var/run/yams-daemon.sock");
            }
            // Use a user-specific socket path by default to match the PID file behavior
            return fs::temp_directory_path() / "yams-daemon.sock";
        };
        pImpl->config_.socketPath = yams::daemon::ConnectionFsm::resolve_socket_path_config_first();
    }
    // Normalize dataDir for all CLI/MCP callers: env > config.toml(core.data_dir) > XDG/HOME > cwd
    if (pImpl->config_.dataDir.empty()) {
        namespace fs = std::filesystem;
        auto expand_tilde = [](std::string p) -> std::string {
            if (!p.empty() && p.front() == '~') {
                if (const char* home = std::getenv("HOME"))
                    return std::string(home) + p.substr(1);
            }
            return p;
        };
        try {
            // 1) Explicit environment override
            if (const char* envStorage = std::getenv("YAMS_STORAGE")) {
                if (*envStorage)
                    pImpl->config_.dataDir = fs::path(envStorage);
            }
            if (pImpl->config_.dataDir.empty()) {
                if (const char* envData = std::getenv("YAMS_DATA_DIR")) {
                    if (*envData)
                        pImpl->config_.dataDir = fs::path(envData);
                }
            }

            // 2) core.data_dir from config.toml
            if (pImpl->config_.dataDir.empty()) {
                fs::path cfgPath;
                if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
                    cfgPath = fs::path(cfgEnv);
                } else if (const char* xdg = std::getenv("XDG_CONFIG_HOME")) {
                    cfgPath = fs::path(xdg) / "yams" / "config.toml";
                } else if (const char* home = std::getenv("HOME")) {
                    cfgPath = fs::path(home) / ".config" / "yams" / "config.toml";
                }

                if (!cfgPath.empty() && fs::exists(cfgPath)) {
                    std::ifstream f(cfgPath);
                    std::string line;
                    bool in_core = false;
                    while (std::getline(f, line)) {
                        // strip leading/trailing spaces
                        auto ltrim = [](std::string& s) {
                            s.erase(s.begin(),
                                    std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                                        return !std::isspace(ch);
                                    }));
                        };
                        auto rtrim = [](std::string& s) {
                            s.erase(std::find_if(s.rbegin(), s.rend(),
                                                 [](unsigned char ch) { return !std::isspace(ch); })
                                        .base(),
                                    s.end());
                        };
                        ltrim(line);
                        rtrim(line);
                        if (line.empty() || line[0] == '#')
                            continue;
                        if (line.front() == '[') {
                            // section header like [core]
                            in_core = (line == "[core]" || line == "[ core ]");
                            continue;
                        }
                        // support both flattened and sectioned forms
                        auto pos = line.find('=');
                        if (pos == std::string::npos)
                            continue;
                        std::string key = line.substr(0, pos);
                        std::string val = line.substr(pos + 1);
                        ltrim(key);
                        rtrim(key);
                        ltrim(val);
                        rtrim(val);
                        if (val.size() >= 2 && ((val.front() == '"' && val.back() == '"') ||
                                                (val.front() == '\'' && val.back() == '\''))) {
                            val = val.substr(1, val.size() - 2);
                        }
                        if (key == "core.data_dir" || (in_core && key == "data_dir")) {
                            val = expand_tilde(val);
                            if (!val.empty()) {
                                pImpl->config_.dataDir = fs::path(val);
                                break;
                            }
                        }
                    }
                }
            }

            // 3) XDG/HOME defaults
            if (pImpl->config_.dataDir.empty()) {
                if (const char* xdgDataHome = std::getenv("XDG_DATA_HOME")) {
                    pImpl->config_.dataDir = fs::path(xdgDataHome) / "yams";
                } else if (const char* homeEnv = std::getenv("HOME")) {
                    pImpl->config_.dataDir = fs::path(homeEnv) / ".local" / "share" / "yams";
                } else {
                    pImpl->config_.dataDir = fs::current_path() / "yams_data";
                }
            }
        } catch (...) {
            // Best-effort only; leave empty if something unexpected happened
        }
    }
    pImpl->refresh_transport();

    // In test builds, default to no auto-start to avoid forking child daemons
#ifdef YAMS_TESTING
    pImpl->config_.autoStart = false;
#endif
    // Honor env toggle to disable auto-start (useful in tests/CI to avoid forking)
    if (const char* noauto = std::getenv("YAMS_DISABLE_AUTOSTART")) {
        std::string v(noauto);
        std::transform(v.begin(), v.end(), v.begin(), ::tolower);
        if (v == "1" || v == "true" || v == "on" || v == "yes") {
            pImpl->config_.autoStart = false;
        }
    }
    // Optional env override for max inflight (load testing): YAMS_MAX_INFLIGHT
    if (const char* mi = std::getenv("YAMS_MAX_INFLIGHT")) {
        try {
            auto v = std::stoul(mi);
            if (v > 0)
                pImpl->config_.maxInflight = v;
        } catch (...) {
        }
    }
    // Timeout overrides: prefer explicit env, else bump conservative defaults to 120s
    auto parse_ms = [](const char* v) -> std::optional<std::chrono::milliseconds> {
        if (!v)
            return std::nullopt;
        try {
            long ms = std::stol(std::string(v));
            if (ms > 0)
                return std::chrono::milliseconds(ms);
        } catch (...) {
        }
        return std::nullopt;
    };
    // Unified request timeout sets both header/body
    if (auto rt = parse_ms(std::getenv("YAMS_REQUEST_TIMEOUT_MS"))) {
        pImpl->headerTimeout_ = *rt;
        pImpl->bodyTimeout_ = *rt;
    }
    if (auto ht = parse_ms(std::getenv("YAMS_HEADER_TIMEOUT_MS"))) {
        pImpl->headerTimeout_ = *ht;
    }
    if (auto bt = parse_ms(std::getenv("YAMS_BODY_TIMEOUT_MS"))) {
        pImpl->bodyTimeout_ = *bt;
    }
    // If still at legacy defaults (30s/60s), raise to 120s to tolerate large inference
    if (pImpl->headerTimeout_ == std::chrono::milliseconds(30000) &&
        pImpl->bodyTimeout_ == std::chrono::milliseconds(60000)) {
        pImpl->headerTimeout_ = std::chrono::milliseconds(120000);
        pImpl->bodyTimeout_ = std::chrono::milliseconds(120000);
    }
    spdlog::debug("DaemonClient init: resolved socket='{}'", pImpl->config_.socketPath.string());
    if (!pImpl->config_.dataDir.empty()) {
        spdlog::debug("DaemonClient init: resolved dataDir='{}'", pImpl->config_.dataDir.string());
    }
}

DaemonClient::~DaemonClient() = default;

DaemonClient::DaemonClient(DaemonClient&&) noexcept = default;
DaemonClient& DaemonClient::operator=(DaemonClient&&) noexcept = default;

void DaemonClient::setHeaderTimeout(std::chrono::milliseconds timeout) {
    if (pImpl) {
        pImpl->headerTimeout_ = timeout;
        pImpl->refresh_transport();
    }
}

void DaemonClient::setBodyTimeout(std::chrono::milliseconds timeout) {
    if (pImpl) {
        pImpl->bodyTimeout_ = timeout;
        pImpl->refresh_transport();
    }
}

// New: lightweight readiness probe that sends a real Ping and waits briefly
static bool
pingDaemonSync(const std::filesystem::path& socketPath,
               std::chrono::milliseconds /*requestTimeout*/ = std::chrono::milliseconds(500),
               std::chrono::milliseconds /*headerTimeout*/ = std::chrono::milliseconds(250),
               std::chrono::milliseconds /*bodyTimeout*/ = std::chrono::milliseconds(250)) {
    // Best-effort synchronous connectivity probe: try to synchronously connect to the UNIX socket.
    try {
        auto path = socketPath.empty() ? DaemonClient::resolveSocketPathConfigFirst() : socketPath;
        if (path.empty())
            return false;
        boost::asio::io_context io;
        boost::asio::local::stream_protocol::socket sock(io);
        boost::system::error_code ec;
        sock.connect(boost::asio::local::stream_protocol::endpoint(path.string()), ec);
        if (!ec) {
            sock.close();
            return true;
        }
        return false;
    } catch (...) {
        return false;
    }
}

boost::asio::awaitable<Result<void>> DaemonClient::connect() {
    // Async variant using adapter's connect helper and timers; avoids blocking sleeps
    // If daemon is not reachable and autoStart is disabled, surface a failure.
    const auto socketPath = pImpl->config_.socketPath.empty()
                                ? DaemonClient::resolveSocketPathConfigFirst()
                                : pImpl->config_.socketPath;
    if (socketPath.empty()) {
        co_return Error{ErrorCode::NetworkError, "Socket path not resolved"};
    }

    using boost::asio::awaitable;
    using boost::asio::steady_timer;
    using boost::asio::use_awaitable;
    using boost::asio::this_coro::executor;
    using boost::asio::experimental::awaitable_operators::operator||;

    auto try_connect =
        [&](std::chrono::milliseconds timeout) -> boost::asio::awaitable<Result<void>> {
        auto ex = co_await boost::asio::this_coro::executor;
        boost::asio::local::stream_protocol::socket sock(ex);
        boost::asio::local::stream_protocol::endpoint ep(socketPath.string());
        steady_timer t(ex);
        t.expires_after(timeout);
        auto which =
            co_await (sock.async_connect(ep, use_awaitable) || t.async_wait(use_awaitable));
        if (which.index() == 1) {
            co_return Error{ErrorCode::Timeout, "Connection timeout"};
        }
        // Success
        boost::system::error_code ec;
        sock.close(ec);
        co_return Result<void>();
    };

    {
        auto r = co_await try_connect(pImpl->config_.connectTimeout);
        if (r) {
            if (!pImpl->config_.enableCircuitBreaker)
                pImpl->breaker_.recordSuccess();
            co_return Result<void>();
        }
        if (!pImpl->config_.autoStart) {
            co_return r.error();
        }
    }

    // Auto-start path
    if (auto result = startDaemon(pImpl->config_); !result) {
        spdlog::warn("Failed to auto-start daemon: {}",
                     sanitize_for_terminal(result.error().message));
        spdlog::info("Please manually start the daemon with: yams daemon start");
        co_return result.error();
    }

    // Retry with exponential backoff using steady_timer
    const int maxRetries = 10;
    const auto baseDelay = std::chrono::milliseconds(100);
    for (int i = 0; i < maxRetries; ++i) {
        auto r = co_await try_connect(pImpl->config_.connectTimeout);
        if (r) {
            spdlog::debug("Daemon started successfully after {} retries", i + 1);
            co_return Result<void>();
        }
        steady_timer t(co_await boost::asio::this_coro::executor);
        auto delay = baseDelay * (1 << std::min(i, 5));
        t.expires_after(delay);
        co_await t.async_wait(boost::asio::use_awaitable);
    }
    co_return Error{ErrorCode::Timeout, "Daemon failed to start after retries"};
}

void DaemonClient::disconnect() {
    // No-op for Asio transport; connections are per-request and scoped in adapter
}

bool DaemonClient::isConnected() const {
    // Treat connectivity as liveness of the daemon (socket + ping), not a persistent socket
    return pingDaemonSync(pImpl->config_.socketPath, std::chrono::milliseconds(250),
                          std::chrono::milliseconds(150), std::chrono::milliseconds(300));
}

boost::asio::awaitable<Result<SearchResponse>> DaemonClient::search(const SearchRequest& req) {
    // Always use streaming search
    co_return co_await streamingSearch(req);
}

boost::asio::awaitable<Result<SearchResponse>>
DaemonClient::streamingSearch(const SearchRequest& req) {
    spdlog::debug("DaemonClient::streamingSearch called");
    auto handler = std::make_shared<StreamingSearchHandler>(req.pathsOnly, req.limit);

    spdlog::debug("DaemonClient::streamingSearch calling sendRequestStreaming");
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

boost::asio::awaitable<Result<GetResponse>> DaemonClient::get(const GetRequest& req) {
    struct GetHandler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* res = std::get_if<GetResponse>(&r)) {
                response_ = *res;
            } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error_ = Error{err->code, err->message};
            }
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* res = std::get_if<GetResponse>(&r)) {
                response_.content.append(res->content);
            } else if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error_ = Error{err->code, err->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error_ = e; }
        void onComplete() override {}

        Result<GetResponse> getResults() {
            if (error_) {
                return *error_;
            }
            return response_;
        }

        GetResponse response_;
        std::optional<Error> error_;
    };

    auto handler = std::make_shared<GetHandler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }
    co_return handler->getResults();
}

boost::asio::awaitable<Result<ListResponse>> DaemonClient::list(const ListRequest& req) {
    spdlog::debug("DaemonClient::list entry: [{}] streaming=true", getRequestName(Request{req}));
    co_return co_await streamingList(req);
}

boost::asio::awaitable<Result<GrepResponse>> DaemonClient::grep(const GrepRequest& req) {
    spdlog::debug("DaemonClient::grep entry: [{}] streaming=true", getRequestName(Request{req}));
    co_return co_await streamingGrep(req);
}

boost::asio::awaitable<Result<StatusResponse>> DaemonClient::status() {
    StatusRequest req;
    req.detailed = false; // avoid heavy daemon-side scans by default

    // Transient-aware retry loop for early startup/socket closure races
    Error lastErr{ErrorCode::NetworkError, "Uninitialized"};
    for (int attempt = 0; attempt < 5; ++attempt) {
        auto response = co_await sendRequest(req);
        if (response) {
            if (auto* res = std::get_if<StatusResponse>(&response.value())) {
                co_return *res;
            }
            if (auto* er = std::get_if<ErrorResponse>(&response.value())) {
                co_return Error{er->code, er->message};
            }
            co_return Error{ErrorCode::InvalidData, "Unexpected response type"};
        }

        lastErr = response.error();
        const std::string& msg = lastErr.message;
        bool transient = (lastErr.code == ErrorCode::NetworkError) &&
                         (msg.find("Connection closed") != std::string::npos ||
                          msg.find("Connection reset") != std::string::npos ||
                          msg.find("ECONNRESET") != std::string::npos ||
                          msg.find("EPIPE") != std::string::npos ||
                          msg.find("Broken pipe") != std::string::npos ||
                          msg.find("read header failed") != std::string::npos ||
                          msg.find("read payload failed") != std::string::npos ||
                          msg.find("Read failed") != std::string::npos);
        if (!transient) {
            co_return lastErr;
        }
        using namespace std::chrono_literals;
        boost::asio::steady_timer t(co_await boost::asio::this_coro::executor);
        t.expires_after(std::chrono::milliseconds(75 * (attempt + 1)));
        co_await t.async_wait(boost::asio::use_awaitable);
    }
    co_return lastErr;
}

boost::asio::awaitable<Result<Response>> DaemonClient::executeRequest(const Request& req) {
    co_return co_await sendRequest(req);
}

boost::asio::awaitable<Result<void>> DaemonClient::shutdown(bool graceful) {
    ShutdownRequest req;
    req.graceful = graceful;

    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
            else
                ok = true;
        }
        bool onChunkReceived(const Response& r, bool /*last*/) override {
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            ok = true;
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        bool ok{false};
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    co_return Result<void>();
}

boost::asio::awaitable<Result<void>> DaemonClient::ping() {
    PingRequest req;

    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (std::holds_alternative<PongResponse>(r))
                pong = true;
            else if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (std::holds_alternative<PongResponse>(r))
                pong = true;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        bool pong{false};
        std::optional<Error> error;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->pong)
        co_return Result<void>();
    co_return Error{ErrorCode::InvalidData, "Unexpected response type for ping"};
}

boost::asio::awaitable<Result<Response>> DaemonClient::sendRequest(const Request& req) {
    spdlog::debug("DaemonClient::sendRequest: [{}] streaming={} sock='{}'", getRequestName(req),
                  false, pImpl->config_.socketPath.string());
    // Skip the legacy connect() call when using AsioTransportAdapter
    // The adapter creates its own connection, and calling connect() here
    // causes a double connection issue where the POSIX socket immediately EOFs

    AsioTransportAdapter adapter(pImpl->transportOptions_);
    auto r = co_await adapter.send_request(req);
    if (!r)
        co_return r.error();
    co_return r.value();
}

// StreamingListHandler implementation
void DaemonClient::StreamingListHandler::onHeaderReceived(const Response& headerResponse) {
    // Parse header information from response
    if (auto* listRes = std::get_if<ListResponse>(&headerResponse)) {
        // Store total count from header
        totalCount_ = listRes->totalCount;

        // Pre-allocate results vector if we know the size
        if (totalCount_ > 0 && limit_ > 0) {
            items_.reserve(static_cast<size_t>(
                std::min<uint64_t>(totalCount_, static_cast<uint64_t>(limit_))));
        } else if (totalCount_ > 0) {
            items_.reserve(totalCount_);
        } else {
            items_.reserve(100); // Default reservation
        }

        // Add any items that might be in the header
        for (const auto& item : listRes->items) {
            if (limit_ > 0 && count_ >= limit_) {
                break;
            }

            items_.push_back(item);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << item.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
    }
}

// StreamingSearchHandler implementation
void DaemonClient::StreamingSearchHandler::onHeaderReceived(const Response& headerResponse) {
    // Parse header information from response
    if (auto* searchRes = std::get_if<SearchResponse>(&headerResponse)) {
        spdlog::debug("StreamingSearchHandler: header received (totalCount={}, elapsed={}ms)",
                      searchRes->totalCount, searchRes->elapsed.count());
        // Store total count and elapsed time from header
        totalCount_ = searchRes->totalCount;
        elapsed_ = searchRes->elapsed;

        // Pre-allocate results vector if we know the size
        if (totalCount_ > 0 && limit_ > 0) {
            results_.reserve(static_cast<size_t>(
                std::min<uint64_t>(totalCount_, static_cast<uint64_t>(limit_))));
        } else if (totalCount_ > 0) {
            results_.reserve(totalCount_);
        } else {
            results_.reserve(100); // Default reservation
        }

        // Add any results that might be in the header
        for (const auto& result : searchRes->results) {
            if (limit_ > 0 && count_ >= limit_) {
                break;
            }

            results_.push_back(result);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << result.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
    }
}

bool DaemonClient::StreamingListHandler::onChunkReceived(const Response& chunkResponse,
                                                         bool isLastChunk) {
    (void)isLastChunk;
    // Process chunk data
    if (auto* listRes = std::get_if<ListResponse>(&chunkResponse)) {
        // Update totals if they changed
        if (listRes->totalCount > 0) {
            totalCount_ = listRes->totalCount;
        }

        // Process items in this chunk
        for (const auto& item : listRes->items) {
            if (limit_ > 0 && count_ >= limit_) {
                return false; // Stop processing if we reached the limit
            }

            items_.push_back(item);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                std::cout << item.path << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
        return false; // Stop processing on error
    }

    return true; // Continue processing next chunks
}

bool DaemonClient::StreamingSearchHandler::onChunkReceived(const Response& chunkResponse,
                                                           bool isLastChunk) {
    (void)isLastChunk;
    // Process chunk data
    if (auto* searchRes = std::get_if<SearchResponse>(&chunkResponse)) {
        // Update totals if they changed
        if (searchRes->totalCount > 0) {
            totalCount_ = searchRes->totalCount;
        }

        // Process results in this chunk
        for (const auto& result : searchRes->results) {
            if (limit_ > 0 && count_ >= limit_) {
                return false; // Stop processing if we reached the limit
            }

            results_.push_back(result);
            count_++;

            // Print result immediately if progressive output is enabled
            if (pathsOnly_) {
                const std::string* p = &result.path;
                auto it = result.metadata.find("path");
                if ((p->empty()) && it != result.metadata.end()) {
                    std::cout << it->second << std::endl;
                } else if (!p->empty()) {
                    std::cout << *p << std::endl;
                } else if (!result.id.empty()) {
                    std::cout << result.id << std::endl;
                }
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        // Store error
        error_ = Error{errRes->code, errRes->message};
        return false; // Stop processing on error
    }

    return true; // Continue processing next chunks
}

void DaemonClient::StreamingListHandler::onError(const Error& error) {
    error_ = error;

    // Log error for immediate feedback
    spdlog::error("List error: {}", error.message);
}

void DaemonClient::StreamingListHandler::onComplete() {
    // Final processing when all chunks have been received
    if (!error_ && !pathsOnly_) {
        spdlog::debug("List complete: found {} items (of {} total)", count_, totalCount_);
    }
    if (!error_ && pathsOnly_ && count_ == 0) {
        std::cout << "(no results)" << std::endl;
    }
}

Result<ListResponse> DaemonClient::StreamingListHandler::getResults() const {
    if (error_) {
        return *error_;
    }

    // Construct complete response
    ListResponse response;
    response.items = items_;
    response.totalCount = totalCount_;

    return response;
}

void DaemonClient::StreamingSearchHandler::onError(const Error& error) {
    error_ = error;

    // Log error for immediate feedback
    spdlog::error("Search error: {}", error.message);
}

void DaemonClient::StreamingSearchHandler::onComplete() {
    // Final processing when all chunks have been received
    if (!error_ && !pathsOnly_) {
        spdlog::debug("Search complete: found {} results (of {} total) in {}ms", count_,
                      totalCount_, elapsed_.count());
    }
    if (!error_ && pathsOnly_ && count_ == 0) {
        // Explicitly indicate no results for paths-only output
        std::cout << "(no results)" << std::endl;
    }
}

Result<SearchResponse> DaemonClient::StreamingSearchHandler::getResults() const {
    if (error_) {
        return *error_;
    }

    // Construct complete response
    SearchResponse response;
    response.results = results_;
    response.totalCount = totalCount_;
    response.elapsed = elapsed_;

    return response;
}

// Static helper to set timeout environment variables
void DaemonClient::setTimeoutEnvVars(std::chrono::milliseconds headerTimeout,
                                     std::chrono::milliseconds bodyTimeout) {
    // Set environment variables
    setenv("YAMS_HEADER_TIMEOUT", std::to_string(headerTimeout.count()).c_str(), 1);
    setenv("YAMS_BODY_TIMEOUT", std::to_string(bodyTimeout.count()).c_str(), 1);
}

// Streaming list helper method
boost::asio::awaitable<Result<ListResponse>> DaemonClient::streamingList(const ListRequest& req) {
    auto handler = std::make_shared<StreamingListHandler>(req.pathsOnly, req.limit);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

// Streaming Grep handler methods
void DaemonClient::StreamingGrepHandler::onHeaderReceived(const Response& headerResponse) {
    if (auto* grepRes = std::get_if<GrepResponse>(&headerResponse)) {
        totalMatches_ = grepRes->totalMatches;
        filesSearched_ = grepRes->filesSearched;

        // Process any matches included in the header
        for (const auto& m : grepRes->matches) {
            // Enforce per-file cap if set
            if (perFileMax_ > 0) {
                auto& cnt = perFileCount_[m.file];
                if (cnt >= perFileMax_) {
                    continue;
                }
                cnt++;
            }
            matches_.push_back(m);
            if (pathsOnly_) {
                std::cout << m.file << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&headerResponse)) {
        error_ = Error{errRes->code, errRes->message};
    }
}

bool DaemonClient::StreamingGrepHandler::onChunkReceived(const Response& chunkResponse,
                                                         bool /*isLastChunk*/) {
    if (auto* grepRes = std::get_if<GrepResponse>(&chunkResponse)) {
        // Update totals if present
        if (grepRes->totalMatches > 0) {
            totalMatches_ = grepRes->totalMatches;
        }
        if (grepRes->filesSearched > 0) {
            filesSearched_ = grepRes->filesSearched;
        }

        for (const auto& m : grepRes->matches) {
            if (perFileMax_ > 0) {
                auto& cnt = perFileCount_[m.file];
                if (cnt >= perFileMax_) {
                    continue;
                }
                cnt++;
            }
            matches_.push_back(m);
            if (pathsOnly_) {
                std::cout << m.file << std::endl;
            }
        }
    } else if (auto* errRes = std::get_if<ErrorResponse>(&chunkResponse)) {
        error_ = Error{errRes->code, errRes->message};
        return false;
    }
    return true;
}

void DaemonClient::StreamingGrepHandler::onError(const Error& error) {
    error_ = error;
    spdlog::error("Grep error: {}", error.message);
}

void DaemonClient::StreamingGrepHandler::onComplete() {
    if (!error_ && !pathsOnly_) {
        spdlog::debug("Grep complete: {} matches across {} files", totalMatches_, filesSearched_);
    }
    if (!error_ && pathsOnly_ && matches_.empty()) {
        std::cout << "(no results)" << std::endl;
    }
}

Result<GrepResponse> DaemonClient::StreamingGrepHandler::getResults() const {
    if (error_) {
        return *error_;
    }
    GrepResponse r;
    r.matches = matches_;
    r.totalMatches = totalMatches_;
    r.filesSearched = filesSearched_;
    return r;
}

// Streaming grep helper method
boost::asio::awaitable<Result<GrepResponse>> DaemonClient::streamingGrep(const GrepRequest& req) {
    // If pathsOnly or filesOnly/countOnly, we can progressively print in handler
    size_t perFileCap = 0;
    if (req.maxMatches > 0) {
        perFileCap = req.maxMatches;
    }
    auto handler =
        std::make_shared<StreamingGrepHandler>(req.pathsOnly || req.filesOnly, perFileCap);

    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }

    co_return handler->getResults();
}

boost::asio::awaitable<Result<void>>
DaemonClient::sendRequestStreaming(const Request& req,
                                   std::shared_ptr<ChunkedResponseHandler> handler) {
    spdlog::debug("DaemonClient::sendRequestStreaming: [{}] streaming={} sock='{}'",
                  getRequestName(req), true, pImpl->config_.socketPath.string());
    // Skip the legacy connect() call when using AsioTransportAdapter
    // The adapter creates its own connection, and calling connect() here
    // causes a double connection issue where the POSIX socket immediately EOFs

    auto onHeader = [handler](const Response& r) { handler->onHeaderReceived(r); };
    auto onChunk = [handler](const Response& r, bool last) {
        // Pretty-print progress events if present, then forward to handler
        if (auto* ev = std::get_if<EmbeddingEvent>(&r)) {
            spdlog::info(
                "[embed] model={} processed={}/{} success={} failure={} inserted={} phase={} {}",
                ev->modelName, ev->processed, ev->total, ev->success, ev->failure, ev->inserted,
                ev->phase, ev->message);
            // Do not terminate stream on events
            (void)last;
            return true;
        }
        if (auto* ml = std::get_if<ModelLoadEvent>(&r)) {
            if (ml->bytesTotal > 0) {
                spdlog::info("[model] {} {} {}/{} bytes {}", ml->modelName, ml->phase,
                             ml->bytesLoaded, ml->bytesTotal, ml->message);
            } else {
                spdlog::info("[model] {} {} {}", ml->modelName, ml->phase, ml->message);
            }
            (void)last;
            return true;
        }
        return handler->onChunkReceived(r, last);
    };
    auto onError = [handler](const Error& e) { handler->onError(e); };
    auto onComplete = [handler]() { handler->onComplete(); };

    AsioTransportAdapter adapter(pImpl->transportOptions_);
    spdlog::debug("DaemonClient::sendRequestStreaming calling adapter.send_request_streaming");
    auto res = co_await adapter.send_request_streaming(req, onHeader, onChunk, onError, onComplete);
    if (!res)
        co_return res.error();
    co_return Result<void>();
}

// Streaming AddDocument helper: header-first then final chunk contains full response
boost::asio::awaitable<Result<AddDocumentResponse>>
DaemonClient::streamingAddDocument(const AddDocumentRequest& req) {
    struct AddDocHandler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& headerResponse) override {
            // In unary (non-streaming) server paths, the full AddDocumentResponse may arrive here.
            if (auto* add = std::get_if<AddDocumentResponse>(&headerResponse)) {
                value = *add;
                return;
            }
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
                return;
            }
            if (auto* st = std::get_if<StatusResponse>(&headerResponse)) {
                std::string status = st->overallStatus.empty()
                                         ? (st->ready ? "ready" : "initializing")
                                         : st->overallStatus;
                error = Error{ErrorCode::InvalidState,
                              std::string("Daemon not ready yet (status=") + status + ")"};
                return;
            }
        }
        bool onChunkReceived(const Response& chunkResponse, bool isLastChunk) override {
            if (auto* err = std::get_if<ErrorResponse>(&chunkResponse)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* st = std::get_if<StatusResponse>(&chunkResponse)) {
                // When daemon is not ready, dispatcher returns StatusResponse instead of
                // processing the request. Surface a friendly message and stop.
                std::string status = st->overallStatus.empty()
                                         ? (st->ready ? "ready" : "initializing")
                                         : st->overallStatus;
                error = Error{ErrorCode::InvalidState,
                              std::string("Daemon not ready yet (status=") + status + ")"};
                return false;
            }
            if (auto* add = std::get_if<AddDocumentResponse>(&chunkResponse)) {
                if (!isLastChunk) {
                    // We expect the full response as the last chunk; ignore interim
                    return true;
                }
                value = *add;
                return true;
            }
            // Ignore other chunk types
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<AddDocumentResponse> value;
    };

    auto handler = std::make_shared<AddDocHandler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result) {
        co_return result.error();
    }
    if (handler->error.has_value()) {
        co_return handler->error.value();
    }
    if (handler->value.has_value()) {
        co_return handler->value.value();
    }
    co_return Error{ErrorCode::InvalidData, "Missing AddDocumentResponse in stream"};
}

boost::asio::awaitable<Result<EmbeddingResponse>>
DaemonClient::generateEmbedding(const GenerateEmbeddingRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* fin = std::get_if<EmbeddingResponse>(&r))
                value = *fin;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* ev = std::get_if<EmbeddingEvent>(&r)) {
                spdlog::info("[embed] model={} processed={} total={} success={} failure={} {}",
                             ev->modelName, ev->processed, ev->total, ev->success, ev->failure,
                             ev->message);
                (void)isLast;
                return true;
            }
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            if (auto* fin = std::get_if<EmbeddingResponse>(&r)) {
                if (isLast)
                    value = *fin;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<EmbeddingResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing EmbeddingResponse in stream"};
}

boost::asio::awaitable<Result<BatchEmbeddingResponse>>
DaemonClient::generateBatchEmbeddings(const BatchEmbeddingRequest& req) {
    // Route through streaming path even for batch for progress events
    co_return co_await streamingBatchEmbeddings(req);
}

boost::asio::awaitable<Result<BatchEmbeddingResponse>>
DaemonClient::streamingBatchEmbeddings(const BatchEmbeddingRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        Handler() {}
        void onHeaderReceived(const Response& headerResponse) override {
            // Support unary (non-chunked) server responses: final may arrive as header
            if (auto* fin = std::get_if<BatchEmbeddingResponse>(&headerResponse)) {
                // Treat as final; no partials were sent
                value = *fin;
                return;
            }
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
                return;
            }
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* e = std::get_if<EmbeddingEvent>(&r)) {
                // Progress only; continue
                (void)e;
                return true;
            }
            if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* fin = std::get_if<BatchEmbeddingResponse>(&r)) {
                if (!isLast) {
                    // Partial chunk – accumulate embeddings client-side
                    if (fin->dimensions > 0 && dim == 0)
                        dim = fin->dimensions;
                    if (!value)
                        value = BatchEmbeddingResponse{};
                    if (value->dimensions == 0 && fin->dimensions > 0)
                        value->dimensions = fin->dimensions;
                    value->embeddings.reserve(value->embeddings.size() + fin->embeddings.size());
                    for (auto& v : fin->embeddings)
                        value->embeddings.push_back(std::move(v));
                    value->successCount += static_cast<uint32_t>(fin->embeddings.size());
                    return true;
                }
                // Last chunk – may be summary only; ensure value is set
                if (!value)
                    value = *fin; // fallback if server sent full final
                else {
                    value->modelUsed = fin->modelUsed;
                    if (value->dimensions == 0)
                        value->dimensions = fin->dimensions;
                }
                return true;
            }
            if (auto* init = std::get_if<GetInitResponse>(&r)) {
                // Store handle to download after stream completes
                initResp = *init;
                return true;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<BatchEmbeddingResponse> value;
        size_t dim{0};
        std::optional<GetInitResponse> initResp;
    };

    auto handler = std::make_shared<Handler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result)
        co_return result.error();
    if (handler->error)
        co_return handler->error.value();
    if (handler->value)
        co_return handler->value.value();
    if (handler->initResp.has_value()) {
        // Download to memory, reconstruct BatchEmbeddingResponse
        auto ir = handler->initResp.value();
        // Fetch bytes
        std::string data;
        data.reserve(static_cast<size_t>(ir.totalSize));
        uint64_t remaining = ir.totalSize;
        uint64_t offset = 0;
        const uint32_t step = ir.chunkSize > 0 ? ir.chunkSize : 256 * 1024;
        while (remaining > 0) {
            GetChunkRequest creq{ir.transferId, offset,
                                 static_cast<uint32_t>(std::min<uint64_t>(remaining, step))};
            auto cres = co_await sendRequest(Request{creq});
            if (!cres)
                co_return cres.error();
            auto* chunk = std::get_if<GetChunkResponse>(&cres.value());
            if (!chunk)
                co_return Error{ErrorCode::InvalidData, "Invalid chunk response"};
            if (!chunk->data.empty())
                data.append(chunk->data);
            const uint64_t wrote = chunk->data.size();
            if (wrote == 0 && chunk->bytesRemaining == remaining)
                break;
            offset += wrote;
            remaining = chunk->bytesRemaining;
        }
        if (ir.transferId != 0) {
            GetEndRequest ereq{ir.transferId};
            (void)co_await sendRequest(Request{ereq});
        }
        // Parse metadata
        size_t dim = 0, count = 0;
        try {
            auto it = ir.metadata.find("dim");
            if (it != ir.metadata.end())
                dim = static_cast<size_t>(std::stoul(it->second));
            it = ir.metadata.find("count");
            if (it != ir.metadata.end())
                count = static_cast<size_t>(std::stoul(it->second));
        } catch (...) {
            dim = 0;
            count = 0;
        }
        if (dim == 0 || count == 0)
            co_return Error{ErrorCode::InvalidData, "Missing embedding metadata"};
        // Reconstruct embeddings from little-endian f32
        const size_t bytes_needed = dim * count * sizeof(float);
        if (data.size() < bytes_needed)
            co_return Error{ErrorCode::InvalidData, "Downloaded size mismatch"};
        BatchEmbeddingResponse out;
        out.dimensions = dim;
        out.modelUsed = ir.metadata.count("model") ? ir.metadata.at("model") : std::string{};
        out.processingTimeMs = 0;
        out.successCount = count;
        out.failureCount = 0;
        out.embeddings.resize(count);
        const char* ptr = data.data();
        for (size_t i = 0; i < count; ++i) {
            out.embeddings[i].resize(dim);
            std::memcpy(out.embeddings[i].data(), ptr + i * dim * sizeof(float),
                        dim * sizeof(float));
        }
        co_return out;
    }
    co_return Error{ErrorCode::InvalidData, "Missing BatchEmbeddingResponse or handle in stream"};
}

boost::asio::awaitable<Result<EmbedDocumentsResponse>>
DaemonClient::streamingEmbedDocuments(const EmbedDocumentsRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& headerResponse) override {
            // Support unary (non-chunked) server responses: final may arrive as header
            if (auto* fin = std::get_if<EmbedDocumentsResponse>(&headerResponse)) {
                value = *fin;
                return;
            }
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
                return;
            }
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* e = std::get_if<EmbeddingEvent>(&r)) {
                // Emit a simple, user-visible progress line for repair UI and CLI flows
                // Format: [embed] model processed/total success failure inserted phase message
                try {
                    std::cout << "[embed] model=" << e->modelName << " " << e->processed << "/"
                              << e->total << " success=" << e->success << " failure=" << e->failure
                              << " inserted=" << e->inserted << " phase=" << e->phase
                              << (e->message.empty() ? "" : (" " + e->message)) << "\n";
                    std::cout.flush();
                } catch (...) {
                }
                return true;
            }
            if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* fin = std::get_if<EmbedDocumentsResponse>(&r)) {
                if (!isLast)
                    return true;
                value = *fin;
                return true;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::optional<EmbedDocumentsResponse> value;
    };

    auto handler = std::make_shared<Handler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result)
        co_return result.error();
    if (handler->error)
        co_return handler->error.value();
    if (handler->value)
        co_return handler->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing EmbedDocumentsResponse in stream"};
}

boost::asio::awaitable<Result<std::vector<EmbeddingEvent>>>
DaemonClient::callEvents(const EmbedDocumentsRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& headerResponse) override {
            if (auto* err = std::get_if<ErrorResponse>(&headerResponse)) {
                error = Error{err->code, err->message};
            }
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* e = std::get_if<EmbeddingEvent>(&r)) {
                try {
                    events.push_back(*e);
                } catch (...) {
                }
                return true;
            }
            if (auto* err = std::get_if<ErrorResponse>(&r)) {
                error = Error{err->code, err->message};
                return false;
            }
            if (auto* fin = std::get_if<EmbedDocumentsResponse>(&r)) {
                (void)fin; // final stats are implied by events; keep API lean
                return true;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        void onComplete() override {}
        std::optional<Error> error;
        std::vector<EmbeddingEvent> events;
    };

    auto handler = std::make_shared<Handler>();
    auto result = co_await sendRequestStreaming(req, handler);
    if (!result)
        co_return result.error();
    if (handler->error)
        co_return handler->error.value();
    co_return handler->events;
}

boost::asio::awaitable<Result<ModelLoadResponse>>
DaemonClient::loadModel(const LoadModelRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* fin = std::get_if<ModelLoadResponse>(&r))
                value = *fin;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool isLast) override {
            if (auto* ev = std::get_if<ModelLoadEvent>(&r)) {
                if (ev->bytesTotal > 0) {
                    spdlog::info("[model] {} {} {}/{} bytes {}", ev->modelName, ev->phase,
                                 ev->bytesLoaded, ev->bytesTotal, ev->message);
                } else {
                    spdlog::info("[model] {} {} {}", ev->modelName, ev->phase, ev->message);
                }
                (void)isLast;
                return true;
            }
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            if (auto* fin = std::get_if<ModelLoadResponse>(&r)) {
                if (isLast)
                    value = *fin;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<ModelLoadResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing ModelLoadResponse in stream"};
}

boost::asio::awaitable<Result<SuccessResponse>>
DaemonClient::unloadModel(const UnloadModelRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* ok = std::get_if<SuccessResponse>(&r))
                value = *ok;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* ok = std::get_if<SuccessResponse>(&r))
                value = *ok;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<SuccessResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing SuccessResponse in stream"};
}

boost::asio::awaitable<Result<ModelStatusResponse>>
DaemonClient::getModelStatus(const ModelStatusRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* s = std::get_if<ModelStatusResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* s = std::get_if<ModelStatusResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<ModelStatusResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing ModelStatusResponse in stream"};
}

boost::asio::awaitable<Result<GetStatsResponse>>
DaemonClient::getStats(const GetStatsRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* s = std::get_if<GetStatsResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* s = std::get_if<GetStatsResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<GetStatsResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing GetStatsResponse in stream"};
}

boost::asio::awaitable<Result<UpdateDocumentResponse>>
DaemonClient::updateDocument(const UpdateDocumentRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* s = std::get_if<UpdateDocumentResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* s = std::get_if<UpdateDocumentResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<UpdateDocumentResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing UpdateDocumentResponse in stream"};
}

std::filesystem::path DaemonClient::resolveSocketPath() {
#ifdef _WIN32
    // Use temp directory on Windows; AF_UNIX is supported via afunix.h on recent Windows.
    return std::filesystem::temp_directory_path() / "yams-daemon.sock";
#else
    return yams::daemon::ConnectionFsm::resolve_socket_path();
#endif
}

std::filesystem::path DaemonClient::resolveSocketPathConfigFirst() {
#ifndef _WIN32
    return yams::daemon::ConnectionFsm::resolve_socket_path_config_first();
#else
    return resolveSocketPath();
#endif
}

bool DaemonClient::isDaemonRunning(const std::filesystem::path& socketPath) {
    auto path = socketPath.empty() ? resolveSocketPathConfigFirst() : socketPath;
    // Lightweight readiness probe using real Ping via Asio transport
    return pingDaemonSync(path, std::chrono::milliseconds(400), std::chrono::milliseconds(200),
                          std::chrono::milliseconds(300));
}

Result<void> DaemonClient::startDaemon(const ClientConfig& config) {
#ifdef _WIN32
    return Error{ErrorCode::InternalError, "Auto-start not supported on Windows"};
#else
    spdlog::info("Starting YAMS daemon...");

    // Resolve socket path with unified precedence: explicit config > config.toml > env > defaults
    auto socketPath = config.socketPath.empty() ? DaemonClient::resolveSocketPathConfigFirst()
                                                : config.socketPath;

    // Determine data dir from config or environment (YAMS_STORAGE)
    std::filesystem::path dataDir = config.dataDir;
    if (dataDir.empty()) {
        if (const char* env = std::getenv("YAMS_STORAGE")) {
            dataDir = std::filesystem::path(env);
        }
    }

    // Fork and exec yams-daemon
    pid_t pid = fork();
    if (pid < 0) {
        return Error{ErrorCode::InternalError, "Failed to fork: " + std::string(strerror(errno))};
    }

    if (pid == 0) {
        // Child process - exec yams-daemon
        // Detach stdio from parent (MCP/CLI) to avoid corrupting parent's stdout framing
        int devnull = ::open("/dev/null", O_RDWR);
        if (devnull >= 0) {
            (void)::dup2(devnull, STDIN_FILENO);
            (void)::dup2(devnull, STDOUT_FILENO);
            (void)::dup2(devnull, STDERR_FILENO);
            if (devnull > 2)
                ::close(devnull);
        }
        // If a dataDir is provided, export it and pass as explicit CLI arg
        if (!dataDir.empty()) {
            setenv("YAMS_STORAGE", dataDir.c_str(), 1);
            // Also export legacy alias used by some components
            setenv("YAMS_DATA_DIR", dataDir.c_str(), 1);
        }

        // Determine config file path (env override > XDG/HOME)
        std::string configPath;
        if (const char* cfgEnv = std::getenv("YAMS_CONFIG"); cfgEnv && *cfgEnv) {
            configPath = cfgEnv;
        } else if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            configPath = (std::filesystem::path(xdgConfigHome) / "yams" / "config.toml").string();
        } else if (const char* homeEnv = std::getenv("HOME")) {
            configPath =
                (std::filesystem::path(homeEnv) / ".config" / "yams" / "config.toml").string();
        }

        // Allow overriding daemon path for development via YAMS_DAEMON_BIN
        std::string exePath;
        if (const char* daemonBin = std::getenv("YAMS_DAEMON_BIN"); daemonBin && *daemonBin) {
            exePath = daemonBin;
        } else {
            // Try to auto-detect relative to this process path
            // On Linux, read /proc/self/exe
            std::error_code ec;
            std::filesystem::path selfExe;
#ifndef _WIN32
            char buf[4096];
            ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                selfExe = std::filesystem::path(buf);
            }
#endif
            if (!selfExe.empty()) {
                auto cliDir = selfExe.parent_path();
                // Common build-tree locations
                std::vector<std::filesystem::path> candidates = {
                    cliDir / "yams-daemon", cliDir.parent_path() / "yams-daemon",
                    cliDir.parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "daemon" / "yams-daemon",
                    cliDir.parent_path().parent_path() / "yams-daemon"};
                for (const auto& p : candidates) {
                    if (std::filesystem::exists(p)) {
                        exePath = p.string();
                        break;
                    }
                }
            }
            if (exePath.empty()) {
                // Fall back to PATH lookup
                exePath = "yams-daemon";
            }
        }

        // Ensure child daemon inherits our resolved socket path for consistency
        if (!socketPath.empty()) {
            setenv("YAMS_DAEMON_SOCKET", socketPath.c_str(), 1);
        }

        // Use execlp to search PATH (or direct path if overridden) for yams-daemon
        // Pass socket and optional config/log-level arguments
        const char* ll = std::getenv("YAMS_LOG_LEVEL");
        bool haveCfg = !configPath.empty() && std::filesystem::exists(configPath);
        const char* dataArg = (!dataDir.empty() ? dataDir.c_str() : nullptr);
        if (haveCfg && ll && *ll) {
            if (dataArg) {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--config",
                       configPath.c_str(), "--log-level", ll, "--data-dir", dataArg, nullptr);
            } else {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--config",
                       configPath.c_str(), "--log-level", ll, nullptr);
            }
        } else if (haveCfg) {
            if (dataArg) {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--config",
                       configPath.c_str(), "--data-dir", dataArg, nullptr);
            } else {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), "--config",
                       configPath.c_str(), nullptr);
            }
        } else if (ll && *ll) {
            if (dataArg) {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(),
                       "--log-level", ll, "--data-dir", dataArg, nullptr);
            } else {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(),
                       "--log-level", ll, nullptr);
            }
        } else {
            // Basic args
            if (dataArg) {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(),
                       "--data-dir", dataArg, nullptr);
            } else {
                execlp(exePath.c_str(), exePath.c_str(), "--socket", socketPath.c_str(), nullptr);
            }
        }

        // If we get here, exec failed
        spdlog::error("Failed to exec yams-daemon: {}", strerror(errno));
        spdlog::error("Make sure yams-daemon is installed and in your PATH");
        spdlog::error("You can manually start the daemon with: yams daemon start");
        exit(1);
    }

    // Parent process: we don't wait for the daemon to exit.

    spdlog::info("Daemon process spawned, client will now poll for readiness");
    return Result<void>();
#endif
}

// Circuit Breaker implementation
CircuitBreaker::CircuitBreaker(const Config& config)
    : config_(config), lastStateChange_(std::chrono::steady_clock::now()) {}

bool CircuitBreaker::shouldAllow() {
    if (state_ == State::Closed) {
        return true;
    }

    if (state_ == State::Open) {
        if (shouldTransitionToHalfOpen()) {
            transitionTo(State::HalfOpen);
            return true;
        }
        return false;
    }

    // HalfOpen - allow limited requests
    return true;
}

void CircuitBreaker::recordSuccess() {
    if (state_ == State::HalfOpen) {
        consecutiveSuccesses_++;
        if (consecutiveSuccesses_ >= config_.successThreshold) {
            transitionTo(State::Closed);
        }
    }
    consecutiveFailures_ = 0;
}

void CircuitBreaker::recordFailure() {
    consecutiveFailures_++;
    consecutiveSuccesses_ = 0;

    if (state_ == State::Closed && consecutiveFailures_ >= config_.failureThreshold) {
        transitionTo(State::Open);
    } else if (state_ == State::HalfOpen) {
        transitionTo(State::Open);
    }
}

void CircuitBreaker::transitionTo(State newState) {
    state_ = newState;
    lastStateChange_ = std::chrono::steady_clock::now();

    if (newState == State::Open) {
        openedAt_ = lastStateChange_;
    }

    consecutiveFailures_ = 0;
    consecutiveSuccesses_ = 0;
}

bool CircuitBreaker::shouldTransitionToHalfOpen() const {
    auto now = std::chrono::steady_clock::now();
    auto timeSinceOpen = std::chrono::duration_cast<std::chrono::seconds>(now - openedAt_);
    return timeSinceOpen >= config_.openTimeout;
}

void DaemonClient::setStreamingEnabled(bool enabled) {
    if (pImpl) {
        pImpl->config_.enableChunkedResponses = enabled;
    }
}

boost::asio::awaitable<Result<AddResponse>> DaemonClient::add(const AddRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* s = std::get_if<AddResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* s = std::get_if<AddResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<AddResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing AddResponse in stream"};
}

boost::asio::awaitable<Result<SuccessResponse>> DaemonClient::remove(const DeleteRequest& req) {
    struct Handler : public ChunkedResponseHandler {
        void onHeaderReceived(const Response& r) override {
            if (auto* s = std::get_if<SuccessResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r))
                error = Error{er->code, er->message};
        }
        bool onChunkReceived(const Response& r, bool /*isLast*/) override {
            if (auto* s = std::get_if<SuccessResponse>(&r))
                value = *s;
            if (auto* er = std::get_if<ErrorResponse>(&r)) {
                error = Error{er->code, er->message};
                return false;
            }
            return true;
        }
        void onError(const Error& e) override { error = e; }
        std::optional<Error> error;
        std::optional<SuccessResponse> value;
    };
    auto h = std::make_shared<Handler>();
    auto r = co_await sendRequestStreaming(req, h);
    if (!r)
        co_return r.error();
    if (h->error)
        co_return h->error.value();
    if (h->value)
        co_return h->value.value();
    co_return Error{ErrorCode::InvalidData, "Missing SuccessResponse in stream"};
}

} // namespace yams::daemon
