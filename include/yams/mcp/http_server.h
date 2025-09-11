#pragma once

#include <yams/mcp/mcp_server.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <atomic>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace yams::mcp {

namespace http = boost::beast::http;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

// Simple SSE channel for one session
class SSEChannel : public std::enable_shared_from_this<SSEChannel> {
public:
    explicit SSEChannel(tcp::socket socket);
    void start(); // sends headers and begins keepalive schedule
    void send(const std::string& data);
    void close();
    bool isOpen() const { return open_.load(); }

private:
    void writerLoop();

    tcp::socket socket_;
    std::mutex mu_;
    std::deque<std::string> queue_;
    std::atomic<bool> open_{false};
};

// Registry mapping session id -> SSE channel
class SessionRegistry {
public:
    void bind(const std::string& sessionId, std::shared_ptr<SSEChannel> ch);
    void unbind(const std::string& sessionId);
    void publish(const std::string& sessionId, const nlohmann::json& note);

private:
    std::mutex mu_;
    std::unordered_map<std::string, std::weak_ptr<SSEChannel>> map_;
};

// Minimal HTTP server hosting /mcp/jsonrpc and /mcp/events
class HttpMcpServer {
public:
    struct Config {
        std::string bindAddress = "127.0.0.1";
        uint16_t bindPort = 8757;
    };

    HttpMcpServer(boost::asio::io_context& ioc, std::shared_ptr<MCPServer> mcp, const Config& cfg);
    void run(); // blocking accept loop

private:
    void handleSession(tcp::socket socket);
    http::response<http::string_body> handleJsonRpc(http::request<http::string_body>& req,
                                                    const std::string& sessionId);
    void handleSse(tcp::socket socket, const std::string& sessionId);

    boost::asio::io_context& ioc_;
    tcp::acceptor acceptor_;
    std::shared_ptr<MCPServer> mcp_;
    Config cfg_{};
    SessionRegistry sessions_;
};

} // namespace yams::mcp
