#include <yams/daemon/client/asio_connection_pool.h>

#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <spdlog/spdlog.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <shared_mutex>
#include <system_error>
#include <vector>

#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#endif

namespace yams::daemon {

ConnectionRegistry& ConnectionRegistry::instance() {
    static auto* reg = new ConnectionRegistry();
    return *reg;
}

void ConnectionRegistry::add(std::weak_ptr<AsioConnection> conn) {
    std::lock_guard<std::mutex> lk(mutex_);
    connections_.push_back(std::move(conn));
}

void ConnectionRegistry::closeAll() {
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto& weak : connections_) {
        if (auto conn = weak.lock()) {
            conn->alive.store(false, std::memory_order_release);
            conn->socket.release();
        }
    }
    connections_.clear();
}

using boost::asio::awaitable;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;
using boost::asio::as_tuple;
using boost::asio::use_future;
using namespace boost::asio::experimental::awaitable_operators;

namespace {

awaitable<Result<std::unique_ptr<AsioConnection::socket_t>>>
async_connect_with_timeout(const TransportOptions& opts) {
    static bool trace = [] {
        if (const char* raw = std::getenv("YAMS_STREAM_TRACE")) {
            std::string v(raw);
            for (auto& ch : v)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            return (v == "1" || v == "true" || v == "on");
        }
        return false;
    }();
    auto ex = co_await this_coro::executor;
    auto socket = std::make_unique<AsioConnection::socket_t>(ex);
    boost::asio::local::stream_protocol::endpoint endpoint(opts.socketPath.string());

    if (trace) {
        spdlog::info("stream-trace: async_connect socket='{}'", opts.socketPath.string());
    }

    boost::asio::steady_timer timer(ex);
    timer.expires_after(opts.requestTimeout);
    auto connect_result = co_await (socket->async_connect(endpoint, as_tuple(use_awaitable)) ||
                                    timer.async_wait(as_tuple(use_awaitable)));

    if (connect_result.index() == 1) {
        socket->close();
        co_return Error{ErrorCode::Timeout, "Connection timeout (pool connect)"};
    }

    auto& [ec] = std::get<0>(connect_result);
    if (ec) {
        if (ec == boost::asio::error::connection_refused ||
            ec == make_error_code(boost::system::errc::connection_refused)) {
            co_return Error{ErrorCode::NetworkError,
                            std::string("Connection refused. Is the daemon running? Try 'yams "
                                        "daemon start' or verify daemon.socket_path (pool). ") +
                                ec.message()};
        }
        co_return Error{ErrorCode::NetworkError,
                        std::string("Connection failed (pool): ") + ec.message()};
    }

    if (trace) {
        spdlog::info("stream-trace: async_connect succeeded socket='{}'", opts.socketPath.string());
    }
    co_return std::move(socket);
}

awaitable<Result<std::vector<uint8_t>>>
async_read_exact(AsioConnection::socket_t& socket, size_t size, std::chrono::milliseconds timeout) {
    std::vector<uint8_t> buffer(size);
    boost::asio::steady_timer timer(co_await this_coro::executor);
    timer.expires_after(timeout);
    auto read_result = co_await (
        boost::asio::async_read(socket, boost::asio::buffer(buffer), as_tuple(use_awaitable)) ||
        timer.async_wait(as_tuple(use_awaitable)));

    if (read_result.index() == 1) {
        co_return Error{ErrorCode::Timeout, "Read timeout"};
    }

    auto& [ec, bytes_read] = std::get<0>(read_result);
    if (ec) {
        co_return Error{ErrorCode::NetworkError, ec.message()};
    }

    co_return buffer;
}

} // namespace

namespace {
std::shared_mutex& registry_mutex() {
    static std::shared_mutex m;
    return m;
}

std::unordered_map<std::string, std::shared_ptr<AsioConnectionPool>>& registry_map() {
    static std::unordered_map<std::string, std::shared_ptr<AsioConnectionPool>> map;
    return map;
}
} // namespace

AsioConnectionPool::AsioConnectionPool(const TransportOptions& opts, bool shared)
    : opts_(opts), shared_(shared) {}

std::shared_ptr<AsioConnectionPool>
AsioConnectionPool::get_or_create(const TransportOptions& opts) {
    if (!opts.poolEnabled) {
        return std::make_shared<AsioConnectionPool>(opts, false);
    }

    auto key = opts.socketPath.string();

    // Fast path: check if pool exists with shared lock (concurrent reads)
    {
        std::shared_lock<std::shared_mutex> lk(registry_mutex());
        auto& map = registry_map();
        if (auto it = map.find(key); it != map.end()) {
            return it->second;
        }
    }

    // Slow path: create new pool with exclusive lock (single writer)
    {
        std::lock_guard<std::shared_mutex> lk(registry_mutex());
        auto& map = registry_map();
        // Double-check: another thread may have created it while we waited
        if (auto it = map.find(key); it != map.end()) {
            return it->second;
        }
        auto pool = std::make_shared<AsioConnectionPool>(opts, true);
        map[key] = pool;
        return pool;
    }
}

void AsioConnectionPool::shutdown_all(std::chrono::milliseconds timeout) {
    std::vector<std::shared_ptr<AsioConnectionPool>> pools;
    {
        std::lock_guard<std::shared_mutex> lk(registry_mutex());
        auto& map = registry_map();
        pools.reserve(map.size());
        for (auto& [_, pool] : map) {
            if (pool) {
                pools.push_back(pool);
            }
        }
        map.clear();
    }

    for (auto& pool : pools) {
        pool->shutdown(timeout);
    }
}

void AsioConnectionPool::cleanup_stale_connections() {
    connection_pool_.erase(std::ranges::remove_if(connection_pool_,
                                                  [](const std::weak_ptr<AsioConnection>& weak) {
                                                      return weak.expired();
                                                  })
                               .begin(),
                           connection_pool_.end());
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::acquire() {
    // Fast path: try to reuse an existing idle connection
    {
        std::lock_guard<std::mutex> lk(mutex_);
        cleanup_stale_connections();

        for (auto& weak : connection_pool_) {
            if (auto conn = weak.lock()) {
                // Check if connection is alive and not currently in use
                if (conn->alive.load(std::memory_order_acquire) &&
                    !conn->in_use.exchange(true, std::memory_order_acq_rel)) {
                    // Verify socket is still open
                    if (conn->socket && conn->socket->is_open()) {
                        spdlog::debug("Connection pool: reusing existing connection");
                        co_return conn;
                    }
                    // Socket closed, mark as dead and continue searching
                    conn->alive.store(false, std::memory_order_release);
                    conn->in_use.store(false, std::memory_order_release);
                }
            }
        }
    }

    // Slow path: create new connection
    spdlog::debug("Connection pool: creating new connection");
    co_return co_await create_connection();
}

void AsioConnectionPool::release(const std::shared_ptr<AsioConnection>& conn) {
    if (conn) {
        conn->in_use.store(false, std::memory_order_release);
    }
}

void AsioConnectionPool::shutdown(std::chrono::milliseconds timeout) {
    std::lock_guard<std::mutex> lk(mutex_);

    for (auto& weak : connection_pool_) {
        if (auto conn = weak.lock()) {
            conn->close();

            if (conn->read_loop_future.valid()) {
                auto status = conn->read_loop_future.wait_for(timeout);
                if (status != std::future_status::ready) {
                    spdlog::warn("Connection read loop did not complete within {}ms",
                                 timeout.count());
                }
            }
        }
    }

    connection_pool_.clear();
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::create_connection() {
    auto conn = std::make_shared<AsioConnection>(opts_);
    auto socket_res = co_await async_connect_with_timeout(opts_);
    if (!socket_res) {
        co_return nullptr;
    }
    conn->socket = std::move(socket_res.value());
    conn->alive.store(true, std::memory_order_release);
    conn->in_use.store(true, std::memory_order_release); // Mark as in-use for caller
    try {
        if (conn->socket && conn->socket->is_open()) {
            boost::asio::socket_base::send_buffer_size send_sz(64 * 1024);
            boost::asio::socket_base::receive_buffer_size recv_sz(64 * 1024);
            conn->socket->set_option(send_sz);
            conn->socket->set_option(recv_sz);
        }
    } catch (...) {
    }

    {
        std::lock_guard<std::mutex> lk(mutex_);
        cleanup_stale_connections();
        connection_pool_.push_back(conn);
    }

    ConnectionRegistry::instance().add(conn);

    co_return conn;
}

awaitable<void> AsioConnectionPool::ensure_read_loop_started(std::shared_ptr<AsioConnection> conn) {
    if (!conn->read_started.exchange(true)) {
        auto executor = conn->opts.executor.has_value()
                            ? *conn->opts.executor
                            : GlobalIOContext::instance().get_io_context().get_executor();
        conn->read_loop_future = co_spawn(
            executor,
            [weak_conn = std::weak_ptr(conn)]() -> awaitable<void> {
                auto conn = weak_conn.lock();
                if (!conn) {
                    co_return;
                }
                co_await boost::asio::post(conn->strand, use_awaitable);
                MessageFramer framer;
                for (;;) {
                    auto conn = weak_conn.lock();
                    if (!conn) {
                        co_return;
                    }

                    if (!conn->socket || !conn->socket->is_open()) {
                        co_return;
                    }

                    auto hres = co_await async_read_exact(
                        *conn->socket, sizeof(MessageFramer::FrameHeader),
                        conn->streaming_started.load(std::memory_order_relaxed)
                            ? conn->opts.bodyTimeout
                            : conn->opts.headerTimeout);
                    if (!hres) {
                        Error e = hres.error();
                        if (e.message == "Read timeout" ||
                            e.message.find("End of file") != std::string::npos) {
                            e.message = "Connection closed by server (possibly stale connection)";
                        }
                        if (auto c = weak_conn.lock()) {
                            // Acquire strand before accessing handlers map
                            co_await boost::asio::dispatch(c->strand, use_awaitable);
                            for (auto& [rid, h] : c->handlers) {
                                if (h.unary) {
                                    try {
                                        h.unary->promise->set_value(Result<Response>(e));
                                    } catch (const std::future_error&) {
                                        // Promise already satisfied, ignore
                                    }
                                    // Cancel timer to wake up waiter
                                    if (h.unary->notify_timer) {
                                        boost::system::error_code ec;
                                        h.unary->notify_timer->cancel(ec);
                                    }
                                }
                                if (h.streaming) {
                                    h.streaming->onError(e);
                                    try {
                                        h.streaming->done_promise->set_value(Result<void>(e));
                                    } catch (const std::future_error&) {
                                        // Promise already satisfied, ignore
                                    }
                                    // Cancel timer to wake up waiter
                                    if (h.streaming->notify_timer) {
                                        boost::system::error_code ec;
                                        h.streaming->notify_timer->cancel(ec);
                                    }
                                }
                            }
                            c->handlers.clear();
                            c->alive = false;
                            c->streaming_started.store(false, std::memory_order_relaxed);
                        }
                        co_return;
                    }
                    MessageFramer::FrameHeader netHeader;
                    std::memcpy(&netHeader, hres.value().data(), sizeof(netHeader));
                    auto header = netHeader;
                    header.from_network();

                    std::vector<uint8_t> payload;
                    if (header.payload_size > 0) {
                        auto pres = co_await async_read_exact(*conn->socket, header.payload_size,
                                                              conn->opts.bodyTimeout);
                        if (!pres) {
                            Error e = pres.error();
                            if (e.message == "Read timeout" ||
                                e.message.find("End of file") != std::string::npos) {
                                e.message =
                                    "Connection closed by server (possibly stale connection)";
                            }
                            if (auto c = weak_conn.lock()) {
                                // Acquire strand before accessing handlers map
                                co_await boost::asio::dispatch(c->strand, use_awaitable);
                                for (auto& [rid, h] : c->handlers) {
                                    if (h.unary) {
                                        try {
                                            h.unary->promise->set_value(Result<Response>(e));
                                        } catch (const std::future_error&) {
                                            // Promise already satisfied, ignore
                                        }
                                        // Cancel timer to wake up waiter
                                        if (h.unary->notify_timer) {
                                            boost::system::error_code ec;
                                            h.unary->notify_timer->cancel(ec);
                                        }
                                    }
                                    if (h.streaming) {
                                        h.streaming->onError(e);
                                        try {
                                            h.streaming->done_promise->set_value(Result<void>(e));
                                        } catch (const std::future_error&) {
                                            // Promise already satisfied, ignore
                                        }
                                        // Cancel timer to wake up waiter
                                        if (h.streaming->notify_timer) {
                                            boost::system::error_code ec;
                                            h.streaming->notify_timer->cancel(ec);
                                        }
                                    }
                                }
                                c->handlers.clear();
                                c->alive = false;
                                c->streaming_started.store(false, std::memory_order_relaxed);
                            }
                            co_return;
                        }
                        payload = std::move(pres.value());
                    }

                    std::vector<uint8_t> frame;
                    frame.reserve(sizeof(MessageFramer::FrameHeader) + payload.size());
                    frame.insert(frame.end(), hres.value().begin(), hres.value().end());
                    frame.insert(frame.end(), payload.begin(), payload.end());

                    auto msgRes = framer.parse_frame(frame);
                    if (!msgRes) {
                        Error e{ErrorCode::InvalidData, "Failed to parse daemon response frame"};
                        if (auto c = weak_conn.lock()) {
                            // Acquire strand before accessing handlers map
                            co_await boost::asio::dispatch(c->strand, use_awaitable);
                            for (auto& [rid, h] : c->handlers) {
                                if (h.unary) {
                                    try {
                                        h.unary->promise->set_value(Result<Response>(e));
                                    } catch (const std::future_error&) {
                                        // Promise already satisfied, ignore
                                    }
                                    // Cancel timer to wake up waiter
                                    if (h.unary->notify_timer) {
                                        boost::system::error_code ec;
                                        h.unary->notify_timer->cancel(ec);
                                    }
                                }
                                if (h.streaming) {
                                    h.streaming->onError(e);
                                    try {
                                        h.streaming->done_promise->set_value(Result<void>(e));
                                    } catch (const std::future_error&) {
                                        // Promise already satisfied, ignore
                                    }
                                    // Cancel timer to wake up waiter
                                    if (h.streaming->notify_timer) {
                                        boost::system::error_code ec;
                                        h.streaming->notify_timer->cancel(ec);
                                    }
                                }
                            }
                            c->handlers.clear();
                            c->alive = false;
                            c->streaming_started.store(false, std::memory_order_relaxed);
                        }
                        co_return;
                    }
                    auto& msg = msgRes.value();
                    static std::atomic<bool> warned{false};
                    if (!warned.load()) {
                        if (msg.version < PROTOCOL_VERSION) {
                            spdlog::warn(
                                "Daemon protocol v{} < client v{}; consider upgrading daemon",
                                msg.version, PROTOCOL_VERSION);
                            warned.store(true);
                        } else if (msg.version > PROTOCOL_VERSION) {
                            spdlog::warn(
                                "Daemon protocol v{} > client v{}; consider upgrading client",
                                msg.version, PROTOCOL_VERSION);
                            warned.store(true);
                        }
                    }
                    uint64_t reqId = msg.requestId;

                    // Re-acquire strand protection before accessing handlers map
                    // to prevent race with concurrent emplace() from send_request
                    co_await boost::asio::dispatch(conn->strand, use_awaitable);

                    AsioConnection::Handler* handlerPtr = nullptr;
                    if (auto it = conn->handlers.find(reqId); it != conn->handlers.end()) {
                        handlerPtr = &it->second;
                    }
                    if (!handlerPtr) {
                        spdlog::warn("ASIO read loop: no handler for daemon response with request "
                                     "id {}. This may indicate a response was already received or "
                                     "the request timed out.",
                                     reqId);
                        continue;
                    }

                    bool isChunked = header.is_chunked();
                    bool isHeaderOnly = header.is_header_only();
                    bool isLast = header.is_last_chunk();

                    // Move the response to avoid copying large vectors (e.g., DeleteResponse with
                    // many results)
                    Response r = std::move(std::get<Response>(msg.payload));
                    if (!isChunked) {
                        if (handlerPtr->unary) {
                            try {
                                handlerPtr->unary->promise->set_value(
                                    Result<Response>(std::move(r)));
                            } catch (const std::future_error&) {
                                // Promise already satisfied, ignore
                            }
                            // Cancel the notify timer to wake up the waiter immediately
                            if (handlerPtr->unary->notify_timer) {
                                boost::system::error_code ec;
                                handlerPtr->unary->notify_timer->cancel(ec);
                            }
                            conn->handlers.erase(reqId);
                        } else if (handlerPtr->streaming) {
                            handlerPtr->streaming->onHeader(r);
                            handlerPtr->streaming->onComplete();
                            try {
                                handlerPtr->streaming->done_promise->set_value(Result<void>());
                            } catch (const std::future_error&) {
                                // Promise already satisfied, ignore
                            }
                            // Cancel timer to wake up waiter
                            if (handlerPtr->streaming->notify_timer) {
                                boost::system::error_code ec;
                                handlerPtr->streaming->notify_timer->cancel(ec);
                            }
                            conn->handlers.erase(reqId);
                            conn->streaming_started.store(false, std::memory_order_relaxed);
                        }
                        continue;
                    }

                    if (handlerPtr->streaming) {
                        if (isHeaderOnly)
                            conn->streaming_started.store(true, std::memory_order_relaxed);
                        if (isHeaderOnly) {
                            handlerPtr->streaming->onHeader(r);
                        } else {
                            bool cont = handlerPtr->streaming->onChunk(r, isLast);
                            if (!cont || isLast) {
                                handlerPtr->streaming->onComplete();
                                try {
                                    handlerPtr->streaming->done_promise->set_value(Result<void>());
                                } catch (const std::future_error&) {
                                    // Promise already satisfied, ignore
                                }
                                // Cancel timer to wake up waiter
                                if (handlerPtr->streaming->notify_timer) {
                                    boost::system::error_code ec;
                                    handlerPtr->streaming->notify_timer->cancel(ec);
                                }
                                conn->handlers.erase(reqId);
                                conn->streaming_started.store(false, std::memory_order_relaxed);
                            }
                        }
                    }
                }
            },
            boost::asio::use_future);
    }
    co_return;
}

} // namespace yams::daemon
