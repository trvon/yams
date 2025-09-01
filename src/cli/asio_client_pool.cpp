#include <yams/cli/asio_client_pool.hpp>

#include <spdlog/spdlog.h>
#include <array>
#include <span>
#include <yams/core/task.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/asio_transport.h>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

namespace yams::cli {

using yams::daemon::Message;
using yams::daemon::MessageFramer;
using yams::daemon::PingRequest;
using yams::daemon::PongResponse;
using yams::daemon::Request;
using yams::daemon::Response;
using yams::daemon::StatusRequest;
using yams::daemon::StatusResponse;

namespace {
struct Connection {
    explicit Connection(std::shared_ptr<boost::asio::io_context> io)
        : strand(boost::asio::make_strand(*io)), socket(strand) {}

    boost::asio::strand<boost::asio::io_context::executor_type> strand;
    boost::asio::local::stream_protocol::socket socket;
    bool connected = false;

    yams::Result<void> connect(const std::filesystem::path& socketPath) {
        if (socket.is_open()) {
            connected = true;
            return yams::Result<void>();
        }
        try {
            boost::asio::local::stream_protocol::endpoint ep(socketPath.string());
            socket.connect(ep);
            connected = true;
            return yams::Result<void>();
        } catch (const boost::system::system_error& e) {
            connected = false;
            return yams::Error{yams::ErrorCode::NetworkError,
                               std::string("connect failed: ") + e.code().message()};
        }
    }

    void close() {
        try {
            if (socket.is_open()) {
                try {
                    socket.shutdown(boost::asio::socket_base::shutdown_both);
                } catch (...) {
                }
                socket.close();
            }
        } catch (...) {
        }
        connected = false;
    }
};
} // namespace

yams::Result<Message> AsioClientPool::roundtrip(const Request& req) {
    auto io = std::make_shared<boost::asio::io_context>();
    auto conn = std::make_shared<Connection>(io);
    // Resolve daemon socket path using existing helper
    auto sock = cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath()
                                        : cfg_.socketPath;
    if (sock.empty()) {
        return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    if (auto rc = conn->connect(sock); !rc)
        return rc.error();

    // Build message
    Message msg;
    msg.version = yams::daemon::PROTOCOL_VERSION;
    msg.requestId =
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = req;
    msg.clientVersion = "yams-cli";

    MessageFramer framer;
    auto framed = framer.frame_message(msg);
    if (!framed)
        return framed.error();
    auto& bytes = framed.value();

    // Write all
    boost::system::error_code ec;
    std::size_t n = boost::asio::write(conn->socket, boost::asio::buffer(bytes), ec);
    if (ec) {
        conn->close();
        return Error{ErrorCode::NetworkError, std::string("write failed: ") + ec.message()};
    }
    if (n != bytes.size()) {
        return Error{ErrorCode::NetworkError, "short write to daemon"};
    }

    // Read header (fixed size)
    std::array<uint8_t, MessageFramer::HEADER_SIZE> hdrBuf{};
    std::size_t hread = boost::asio::read(conn->socket, boost::asio::buffer(hdrBuf), ec);
    if (ec) {
        conn->close();
        return Error{ErrorCode::NetworkError, std::string("read header failed: ") + ec.message()};
    }
    if (hread != hdrBuf.size()) {
        return Error{ErrorCode::NetworkError, "short read on header"};
    }

    auto hdrRes = framer.parse_header(std::span<const uint8_t>(hdrBuf.data(), hdrBuf.size()));
    if (!hdrRes)
        return hdrRes.error();
    auto hdr = hdrRes.value();

    // Read payload
    std::vector<uint8_t> payload(hdr.payload_size);
    if (hdr.payload_size > 0) {
    std::size_t pread = boost::asio::read(conn->socket, boost::asio::buffer(payload), ec);
        if (ec) {
            conn->close();
            return Error{ErrorCode::NetworkError,
                         std::string("read payload failed: ") + ec.message()};
        }
        if (pread != payload.size()) {
            return Error{ErrorCode::NetworkError, "short read on payload"};
        }
    }

    // Recompose frame and parse
    std::vector<uint8_t> frame;
    frame.reserve(hdrBuf.size() + payload.size());
    frame.insert(frame.end(), hdrBuf.begin(), hdrBuf.end());
    frame.insert(frame.end(), payload.begin(), payload.end());
    auto parsed = framer.parse_frame(frame);
    // Proactively close after each request: server defaults to close_after_response=true,
    // so sockets are not reusable. Avoid stale-descriptor reuse that triggers ECONNRESET.
    conn->close();
    if (!parsed)
        return parsed.error();
    return parsed.value();
}

yams::Task<yams::Result<yams::daemon::Response>> AsioClientPool::async_call_variant(
    const yams::daemon::Request& req) {
    using namespace yams::daemon;
    auto sock = cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath()
                                        : cfg_.socketPath;
    if (sock.empty()) {
        co_return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    AsioTransportAdapter::Options opts{};
    opts.socketPath = sock;
    opts.headerTimeout = cfg_.header_timeout;
    opts.bodyTimeout = cfg_.body_timeout;
    opts.requestTimeout = cfg_.request_timeout;
    AsioTransportAdapter transport(opts);
    auto r = co_await transport.send_request(req);
    co_return r;
}

yams::Task<yams::Result<void>> AsioClientPool::async_ping() {
    using namespace yams::daemon;
    Request req = PingRequest{};
    auto sock = cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath()
                                        : cfg_.socketPath;
    if (sock.empty()) {
        co_return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    AsioTransportAdapter::Options opts{};
    opts.socketPath = sock;
    opts.headerTimeout = cfg_.header_timeout;
    opts.bodyTimeout = cfg_.body_timeout;
    opts.requestTimeout = cfg_.request_timeout;
    AsioTransportAdapter transport(opts);
    auto r = co_await transport.send_request(req);
    if (!r) {
        co_return r.error();
    }
    auto& resp = r.value();
    if (std::holds_alternative<PongResponse>(resp)) {
        co_return Result<void>();
    }
    if (auto* er = std::get_if<ErrorResponse>(&resp)) {
        co_return Error{er->code, er->message};
    }
    co_return Error{ErrorCode::InvalidData, "Unexpected response variant for Ping"};
}

yams::Task<yams::Result<StatusResponse>> AsioClientPool::async_status() {
    using namespace yams::daemon;
    Request req = StatusRequest{};
    auto sock = cfg_.socketPath.empty() ? yams::daemon::DaemonClient::resolveSocketPath()
                                        : cfg_.socketPath;
    if (sock.empty()) {
        co_return Error{ErrorCode::InvalidState, "Could not resolve daemon socket path"};
    }
    AsioTransportAdapter::Options opts{};
    opts.socketPath = sock;
    opts.headerTimeout = cfg_.header_timeout;
    opts.bodyTimeout = cfg_.body_timeout;
    opts.requestTimeout = cfg_.request_timeout;
    AsioTransportAdapter transport(opts);
    auto r = co_await transport.send_request(req);
    if (!r) {
        co_return r.error();
    }
    auto& resp = r.value();
    if (auto* ok = std::get_if<StatusResponse>(&resp)) {
        co_return *ok;
    }
    if (auto* er = std::get_if<ErrorResponse>(&resp)) {
        co_return Error{er->code, er->message};
    }
    co_return Error{ErrorCode::InvalidData, "Unexpected response variant for Status"};
}

yams::Result<void> AsioClientPool::ping() {
    Request req = PingRequest{};
    auto res = roundtrip(req);
    if (!res)
        return res.error();
    auto& msg = res.value();
    // Expect a Response payload wrapping PongResponse
    if (auto* resp = std::get_if<yams::daemon::Response>(&msg.payload)) {
        if (std::holds_alternative<PongResponse>(*resp)) {
            return yams::Result<void>();
        }
        if (auto* er = std::get_if<yams::daemon::ErrorResponse>(resp)) {
            return Error{er->code, er->message};
        }
        return Error{ErrorCode::InvalidData, "Unexpected response variant for Ping"};
    }
    return Error{ErrorCode::InvalidData, "Unexpected message kind for Ping"};
}

yams::Result<StatusResponse> AsioClientPool::status() {
    Request req = StatusRequest{};
    auto res = roundtrip(req);
    if (!res)
        return res.error();
    auto& msg = res.value();
    if (auto* resp = std::get_if<yams::daemon::Response>(&msg.payload)) {
        if (auto* ok = std::get_if<StatusResponse>(resp))
            return *ok;
        if (auto* er = std::get_if<yams::daemon::ErrorResponse>(resp))
            return Error{er->code, er->message};
        return Error{ErrorCode::InvalidData, "Unexpected response variant for Status"};
    }
    return Error{ErrorCode::InvalidData, "Unexpected message kind for Status"};
}

} // namespace yams::cli
