#include <yams/mcp/http_server.h>

#include <boost/algorithm/string.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include <spdlog/spdlog.h>

using nlohmann::json;

namespace yams::mcp {

// --- SSEChannel ---

SSEChannel::SSEChannel(tcp::socket socket) : socket_(std::move(socket)) {}

void SSEChannel::start() {
    // Write initial SSE/HTTP headers synchronously (manual)
    beast::error_code ec;
    std::string hdr = "HTTP/1.1 200 OK\r\n"
                      "Content-Type: text/event-stream\r\n"
                      "Cache-Control: no-cache\r\n"
                      "Connection: keep-alive\r\n\r\n";
    boost::asio::write(socket_, boost::asio::buffer(hdr), ec);
    if (ec) {
        spdlog::warn("SSE header write failed: {}", ec.message());
        return;
    }
    open_.store(true);
    // Kick writer loop (keepalive + queued events). For simplicity, run inline.
    writerLoop();
}

void SSEChannel::send(const std::string& data) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!open_.load())
        return;
    // Prepare SSE event chunk
    std::string ev;
    ev.reserve(data.size() + 32);
    ev += "event: message\n";
    ev += "data: ";
    ev += data;
    ev += "\n\n";
    queue_.push_back(std::move(ev));
}

void SSEChannel::close() {
    open_.store(false);
    beast::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
}

void SSEChannel::writerLoop() {
    beast::error_code ec;
    // basic keepalive: send a comment line every 15s when idle
    auto last = std::chrono::steady_clock::now();
    while (open_.load()) {
        std::string chunk;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (!queue_.empty()) {
                chunk = std::move(queue_.front());
                queue_.pop_front();
            }
        }
        if (!chunk.empty()) {
            boost::asio::write(socket_, boost::asio::buffer(chunk), ec);
            if (ec)
                break;
            last = std::chrono::steady_clock::now();
            continue;
        }
        auto now = std::chrono::steady_clock::now();
        if (now - last > std::chrono::seconds(15)) {
            std::string ping = ": ping\n\n";
            boost::asio::write(socket_, boost::asio::buffer(ping), ec);
            if (ec)
                break;
            last = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    // try graceful shutdown
    socket_.shutdown(tcp::socket::shutdown_send, ec);
}

// --- SessionRegistry ---

void SessionRegistry::bind(const std::string& sessionId, std::shared_ptr<SSEChannel> ch) {
    std::lock_guard<std::mutex> lk(mu_);
    map_[sessionId] = ch;
}

void SessionRegistry::unbind(const std::string& sessionId) {
    std::lock_guard<std::mutex> lk(mu_);
    map_.erase(sessionId);
}

void SessionRegistry::publish(const std::string& sessionId, const json& note) {
    std::shared_ptr<SSEChannel> ch;
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = map_.find(sessionId);
        if (it != map_.end())
            ch = it->second.lock();
    }
    if (!ch)
        return;
    ch->send(note.dump());
}

// --- HttpMcpServer ---

HttpMcpServer::HttpMcpServer(boost::asio::io_context& ioc, std::shared_ptr<MCPServer> mcp,
                             const Config& cfg)
    : ioc_(ioc), acceptor_(ioc), mcp_(std::move(mcp)), cfg_(cfg) {}

void HttpMcpServer::run() {
    beast::error_code ec;
    const tcp::endpoint ep{boost::asio::ip::make_address(cfg_.bindAddress, ec), cfg_.bindPort};
    if (ec)
        throw std::runtime_error("Invalid bind address: " + ec.message());
    acceptor_.open(ep.protocol(), ec);
    if (ec)
        throw std::runtime_error("acceptor open failed: " + ec.message());
    acceptor_.set_option(boost::asio::socket_base::reuse_address(true));
    acceptor_.bind(ep, ec);
    if (ec)
        throw std::runtime_error("bind failed: " + ec.message());
    acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec)
        throw std::runtime_error("listen failed: " + ec.message());
    spdlog::info("MCP HTTP/SSE listening on {}:{}", cfg_.bindAddress, cfg_.bindPort);
    for (;;) {
        tcp::socket socket{ioc_};
        acceptor_.accept(socket, ec);
        if (ec) {
            spdlog::warn("accept error: {}", ec.message());
            continue;
        }
        std::thread(&HttpMcpServer::handleSession, this, std::move(socket)).detach();
    }
}

static std::optional<std::string> getQueryParam(const std::string& target, const std::string& key) {
    auto pos = target.find('?');
    if (pos == std::string::npos)
        return std::nullopt;
    auto q = target.substr(pos + 1);
    std::vector<std::string> parts;
    boost::split(parts, q, boost::is_any_of("&"));
    for (auto& p : parts) {
        auto eq = p.find('=');
        if (eq == std::string::npos)
            continue;
        auto k = p.substr(0, eq);
        if (k == key)
            return p.substr(eq + 1);
    }
    return std::nullopt;
}

void HttpMcpServer::handleSession(tcp::socket socket) {
    beast::flat_buffer buffer;
    beast::error_code ec;
    http::request<http::string_body> req;
    http::read(socket, buffer, req, ec);
    if (ec) {
        spdlog::debug("http read error: {}", ec.message());
        return;
    }
    std::string target = std::string(req.target());

    if (req.method() == http::verb::post &&
        (target == "/" || target == "/jsonrpc" || target.rfind("/mcp/jsonrpc", 0) == 0)) {
        std::string sessionId;
        if (auto h = req.find("X-Session-Id"); h != req.end())
            sessionId = std::string(h->value());
        if (sessionId.empty()) {
            if (auto qs = getQueryParam(target, "session"))
                sessionId = *qs;
        }
        auto res = handleJsonRpc(req, sessionId);
        res.set(http::field::server, "yams-mcp");
        res.set(http::field::access_control_allow_origin, "*");
        res.set(http::field::access_control_allow_methods, "POST, OPTIONS");
        res.set(http::field::access_control_allow_headers, "Content-Type, X-Session-Id");
        res.keep_alive(false);
        http::write(socket, res, ec);
        socket.shutdown(tcp::socket::shutdown_send, ec);
        return;
    }

    if (req.method() == http::verb::options) {
        http::response<http::string_body> res{http::status::no_content, req.version()};
        res.set(http::field::access_control_allow_origin, "*");
        res.set(http::field::access_control_allow_methods, "POST, GET, OPTIONS");
        res.set(http::field::access_control_allow_headers, "Content-Type, X-Session-Id");
        res.set(http::field::access_control_max_age, "86400");
        res.keep_alive(false);
        http::write(socket, res, ec);
        return;
    }

    if (req.method() == http::verb::get &&
        (target.rfind("/events", 0) == 0 || target.rfind("/mcp/events", 0) == 0)) {
        auto qs = getQueryParam(target, "session");
        if (!qs) {
            http::response<http::string_body> res{http::status::bad_request, req.version()};
            res.set(http::field::content_type, "text/plain");
            res.set(http::field::access_control_allow_origin, "*");
            res.body() = "missing session";
            res.prepare_payload();
            http::write(socket, res, ec);
            return;
        }
        handleSse(std::move(socket), *qs);
        return;
    }

    http::response<http::string_body> res{http::status::not_found, req.version()};
    res.set(http::field::content_type, "text/plain");
    res.set(http::field::access_control_allow_origin, "*");
    res.body() = "not found";
    res.prepare_payload();
    http::write(socket, res, ec);
}

http::response<http::string_body>
HttpMcpServer::handleJsonRpc(http::request<http::string_body>& req, const std::string& sessionId) {
    http::response<http::string_body> res{http::status::ok, req.version()};
    res.set(http::field::content_type, "application/json; charset=utf-8");
    try {
        auto j = json::parse(req.body());
        // Establish session context for notifications generated during this request
        MCPServer::SessionContext ctx(
            *mcp_, sessionId,
            [this](const std::string& sid, const json& note) { sessions_.publish(sid, note); });
        // Special-case tools/call for synchronous HTTP execution
        if (j.is_object() and j.value("method", "") == std::string("tools/call")) {
            auto id = j.value("id", nullptr);
            auto params = j.value("params", json::object());
            auto name = params.value("name", std::string(""));
            auto args = params.value("arguments", json::object());
            auto out = mcp_->callToolPublic(name, args);
            const json env = {{"jsonrpc", "2.0"}, {"id", id}, {"result", out}};
            res.body() = env.dump();
            res.prepare_payload();
            return res;
        }
        auto r = mcp_->handleRequestPublic(j);
        if (!j.contains("id")) {
            // Notification: return 204
            res.result(http::status::no_content);
            res.body() = "";
            return res;
        }
        if (r) {
            res.body() = r.value().dump();
        } else {
            // On error from handler, return JSON-RPC error envelope
            const json err = {{"jsonrpc", "2.0"},
                              {"id", j.value("id", nullptr)},
                              {"error", {{"code", -32603}, {"message", r.error().message}}}};
            res.body() = err.dump();
        }
        res.prepare_payload();
        return res;
    } catch (const std::exception& e) {
        res.result(http::status::bad_request);
        const json err = {
            {"jsonrpc", "2.0"},
            {"id", nullptr},
            {"error", {{"code", -32700}, {"message", std::string("parse error: ") + e.what()}}}};
        res.body() = err.dump();
        res.prepare_payload();
        return res;
    }
}

void HttpMcpServer::handleSse(tcp::socket socket, const std::string& sessionId) {
    auto ch = std::make_shared<SSEChannel>(std::move(socket));
    sessions_.bind(sessionId, ch);
    ch->start();
    sessions_.unbind(sessionId);
}

} // namespace yams::mcp
