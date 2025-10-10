#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <spdlog/spdlog.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <unordered_map>
#include <vector>
#include <yams/core/format.h>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#endif

namespace yams::daemon {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;
using namespace boost::asio::experimental::awaitable_operators;

AsioTransportAdapter::AsioTransportAdapter(const Options& opts) : opts_(opts) {
    bool metrics_on = FsmMetricsRegistry::instance().enabled();
    const char* s = std::getenv("YAMS_FSM_SNAPSHOTS");
    bool snaps_on = (s && (std::strcmp(s, "1") == 0 || std::strcmp(s, "true") == 0 ||
                           std::strcmp(s, "TRUE") == 0 || std::strcmp(s, "on") == 0 ||
                           std::strcmp(s, "ON") == 0));
    fsm_.enable_metrics(metrics_on);
    fsm_.enable_snapshots(snaps_on);
}

boost::asio::awaitable<std::shared_ptr<AsioConnection>>
AsioTransportAdapter::get_or_create_connection(const Options& opts) {
    auto pool = AsioConnectionPool::get_or_create(opts);
    co_return co_await pool->acquire();
}

awaitable<Result<std::unique_ptr<boost::asio::local::stream_protocol::socket>>>
AsioTransportAdapter::async_connect_with_timeout(const std::filesystem::path& path,
                                                 std::chrono::milliseconds timeout) {
    auto& io_ctx = GlobalIOContext::instance().get_io_context();
    auto socket = std::make_unique<boost::asio::local::stream_protocol::socket>(io_ctx);
    boost::asio::local::stream_protocol::endpoint endpoint(path.string());
    try {
        if (!std::filesystem::exists(path)) {
            std::string msg = "Daemon not started (socket not found at '" + path.string() +
                              "'). Set YAMS_DAEMON_SOCKET or update config (daemon.socket_path).";
            spdlog::debug("AsioTransportAdapter preflight: {}", msg);
            co_return Error{ErrorCode::NetworkError, std::move(msg)};
        }
#ifndef _WIN32
        {
            struct stat st;
            if (::stat(path.c_str(), &st) == 0) {
                if (!S_ISSOCK(st.st_mode)) {
                    std::string msg = "Path exists but is not a socket: '" + path.string() + "'";
                    spdlog::debug("AsioTransportAdapter preflight: {}", msg);
                    co_return Error{ErrorCode::NetworkError, std::move(msg)};
                }
            }
        }
#endif
        // Bind timer to the same io_context as the socket to avoid cross-executor races
        boost::asio::steady_timer timer(io_ctx);
        timer.expires_after(timeout);
        auto connect_result = co_await (socket->async_connect(endpoint, use_awaitable) ||
                                        timer.async_wait(use_awaitable));
        if (connect_result.index() == 1) {
            socket->close();
            co_return Error{ErrorCode::Timeout,
                            yams::format("Connection timeout (socket='{}')", path.string())};
        }
        co_return std::move(socket);
    } catch (const boost::system::system_error& e) {
        const auto ec = e.code();
        if (ec == boost::asio::error::connection_refused ||
            ec == make_error_code(boost::system::errc::connection_refused)) {
            co_return Error{ErrorCode::NetworkError,
                            yams::format("Connection refused (socket='{}'). Is the daemon running? "
                                         "Try 'yams daemon start' or verify daemon.socket_path.",
                                         path.string())};
        }
        co_return Error{ErrorCode::NetworkError,
                        yams::format("Connection failed ({}): {}", ec.message(), e.what())};
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, yams::format("Connection failed: {}", e.what())};
    }
}

awaitable<Result<std::vector<uint8_t>>>
AsioTransportAdapter::async_read_exact(boost::asio::local::stream_protocol::socket& socket,
                                       size_t size, std::chrono::milliseconds timeout) {
    try {
        std::vector<uint8_t> buffer(size);
        boost::asio::steady_timer timer(co_await this_coro::executor);
        timer.expires_after(timeout);
        auto read_result =
            co_await (boost::asio::async_read(socket, boost::asio::buffer(buffer), use_awaitable) ||
                      timer.async_wait(use_awaitable));
        if (read_result.index() == 1) {
            co_return Error{ErrorCode::Timeout, "Read timeout"};
        }
        co_return buffer;
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, yams::format("Read failed: {}", e.what())};
    }
}

awaitable<Result<void>>
AsioTransportAdapter::async_write_all(boost::asio::local::stream_protocol::socket& socket,
                                      const std::vector<uint8_t>& data,
                                      std::chrono::milliseconds timeout) {
    try {
        boost::asio::steady_timer timer(co_await this_coro::executor);
        timer.expires_after(timeout);
        auto write_result =
            co_await (boost::asio::async_write(socket, boost::asio::buffer(data), use_awaitable) ||
                      timer.async_wait(use_awaitable));
        if (write_result.index() == 1) {
            co_return Error{ErrorCode::Timeout, "Write timeout"};
        }
        co_return Result<void>{};
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, yams::format("Write failed: {}", e.what())};
    }
}

boost::asio::awaitable<Result<Response>> AsioTransportAdapter::send_request(const Request& req) {
    Request copy = req;
    co_return co_await send_request(std::move(copy));
}

boost::asio::awaitable<Result<Response>> AsioTransportAdapter::send_request(Request&& req) {
    auto conn = co_await get_or_create_connection(opts_);
    if (!conn || !conn->alive) {
        co_return Error{ErrorCode::NetworkError, "Failed to establish connection"};
    }

    static std::atomic<uint64_t> g_req_id{
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};

    const auto req_type = getMessageType(req);

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = g_req_id.fetch_add(1, std::memory_order_relaxed);
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = std::move(req);
    msg.clientVersion = "yams-client-0.3.4";
    msg.expectsStreamingResponse = false;

    MessageFramer framer;
    std::vector<uint8_t> frame;
    frame.reserve(MessageFramer::HEADER_SIZE + 4096);
    auto frame_res = framer.frame_message_into(msg, frame);
    if (!frame_res) {
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    auto& io = GlobalIOContext::instance().get_io_context();
    auto response_ch = std::make_shared<AsioConnection::response_channel_t>(io, 1);

    co_await boost::asio::dispatch(conn->strand, use_awaitable);
    if (conn->handlers.size() >= conn->opts.maxInflight) {
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
    }
    {
        AsioConnection::Handler h;
        h.unary.emplace(AsioConnection::UnaryHandler{response_ch});
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto wres = co_await conn->async_write_frame(std::move(frame));
    if (!wres) {
        co_await boost::asio::dispatch(conn->strand, use_awaitable);
        conn->handlers.erase(msg.requestId);
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        co_return wres.error();
    }
    spdlog::info("AsioTransportAdapter::send_request wrote frame req_id={} type={}", msg.requestId,
                 static_cast<int>(req_type));

    // Release connection AFTER writing frame - connection can now be reused while we wait for
    // response
    conn->in_use.store(false, std::memory_order_release);

    auto [ec, response_result] =
        co_await response_ch->async_receive(boost::asio::as_tuple(use_awaitable));
    if (ec) {
        co_return Error{ErrorCode::NetworkError, "Failed to receive response: " + ec.message()};
    }
    co_return *response_result;
}

boost::asio::awaitable<Result<void>>
AsioTransportAdapter::send_request_streaming(const Request& req, HeaderCallback onHeader,
                                             ChunkCallback onChunk, ErrorCallback onError,
                                             CompleteCallback onComplete) {
    auto conn = co_await get_or_create_connection(opts_);
    if (!conn || !conn->alive) {
        co_return Error{ErrorCode::NetworkError, "Failed to establish connection"};
    }

    static std::atomic<uint64_t> g_req_id{
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count())};

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = g_req_id.fetch_add(1, std::memory_order_relaxed);
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = req;
    msg.clientVersion = "yams-client-0.3.4";
    msg.expectsStreamingResponse = true;

    MessageFramer framer;
    std::vector<uint8_t> frame;
    frame.reserve(MessageFramer::HEADER_SIZE + 4096);
    auto frame_res = framer.frame_message_into(msg, frame);
    if (!frame_res) {
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    auto& io = GlobalIOContext::instance().get_io_context();
    auto done_ch = std::make_shared<AsioConnection::void_channel_t>(io, 1);

    co_await boost::asio::dispatch(conn->strand, use_awaitable);
    if (conn->handlers.size() >= conn->opts.maxInflight) {
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
    }
    {
        AsioConnection::Handler h;
        h.streaming.emplace(onHeader, onChunk, onError, onComplete);
        h.streaming->done_channel = done_ch;
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto wres = co_await conn->async_write_frame(std::move(frame));
    if (!wres) {
        co_await boost::asio::dispatch(conn->strand, use_awaitable);
        conn->handlers.erase(msg.requestId);
        // Release connection before returning error
        conn->in_use.store(false, std::memory_order_release);
        co_return wres.error();
    }
    spdlog::info("AsioTransportAdapter::send_request_streaming wrote frame req_id={} type={}",
                 msg.requestId, static_cast<int>(getMessageType(req)));

    // Release connection AFTER writing frame - connection can now be reused while we wait for
    // streaming response
    conn->in_use.store(false, std::memory_order_release);

    auto [ec, void_result] = co_await done_ch->async_receive(boost::asio::as_tuple(use_awaitable));
    if (ec) {
        co_return Error{ErrorCode::NetworkError,
                        "Failed to receive stream completion: " + ec.message()};
    }
    co_return void_result;
}

} // namespace yams::daemon
