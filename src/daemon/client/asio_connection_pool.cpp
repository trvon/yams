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

using boost::asio::awaitable;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;
using boost::asio::as_tuple;
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
    try {
        if (trace) {
            spdlog::info("stream-trace: async_connect socket='{}'", opts.socketPath.string());
        }
        // Skip filesystem validation for performance - let connect() fail naturally if socket
        // missing Connection errors are handled below and provide clear diagnostics This
        // optimization is critical for high-concurrency scenarios (multi-agent systems)
        boost::asio::steady_timer timer(ex);
        timer.expires_after(opts.requestTimeout);
        auto connect_result = co_await (socket->async_connect(endpoint, use_awaitable) ||
                                        timer.async_wait(use_awaitable));
        if (connect_result.index() == 1) {
            socket->close();
            co_return Error{ErrorCode::Timeout, "Connection timeout (pool connect)"};
        }
        if (trace) {
            spdlog::info("stream-trace: async_connect succeeded socket='{}'",
                         opts.socketPath.string());
        }
        co_return std::move(socket);
    } catch (const boost::system::system_error& e) {
        const auto ec = e.code();
        if (ec == boost::asio::error::connection_refused ||
            ec == make_error_code(boost::system::errc::connection_refused)) {
            co_return Error{ErrorCode::NetworkError,
                            std::string("Connection refused. Is the daemon running? Try 'yams "
                                        "daemon start' or verify daemon.socket_path (pool). ") +
                                ec.message()};
        }
        co_return Error{ErrorCode::NetworkError,
                        std::string("Connection failed (pool): ") + ec.message()};
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, e.what()};
    }
}

awaitable<Result<std::vector<uint8_t>>>
async_read_exact(AsioConnection::socket_t& socket, size_t size, std::chrono::milliseconds timeout) {
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
        co_return Error{ErrorCode::NetworkError, e.what()};
    }
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

void AsioConnectionPool::cleanup_stale_connections() {
    // Remove dead/closed connections from pool (lock must be held by caller)
    connection_pool_.erase(
        std::ranges::remove_if(connection_pool_,
                               [](const std::weak_ptr<AsioConnection>& weak) {
                                   auto conn = weak.lock();
                                   return !conn || !conn->alive.load(std::memory_order_relaxed) ||
                                          !conn->socket || !conn->socket->is_open();
                               })
            .begin(),
        connection_pool_.end());
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::acquire() {
    if (!shared_) {
        co_return co_await create_connection();
    }

    // Try to find an available connection from the pool
    std::shared_ptr<AsioConnection> existing;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        cleanup_stale_connections();

        // Look for an alive, open, AND NOT IN USE connection
        for (auto& weak : connection_pool_) {
            if (auto conn = weak.lock()) {
                // Atomically check and set in_use flag
                bool expected = false;
                if (conn->alive.load(std::memory_order_relaxed) && conn->socket &&
                    conn->socket->is_open() &&
                    conn->in_use.compare_exchange_strong(expected, true,
                                                         std::memory_order_acquire)) {
                    // Successfully checked out this connection
                    existing = conn;
                    break;
                }
            }
        }
    }

    if (existing) {
        co_return existing;
    }

    // No available connection - create a new one (outside the lock)
    auto fresh = co_await create_connection();

    if (shared_ && fresh && fresh->alive.load(std::memory_order_relaxed)) {
        std::lock_guard<std::mutex> lk(mutex_);
        // Re-check for available connections that might have been released
        // while we were creating a new one.
        for (auto& weak : connection_pool_) {
            if (auto conn = weak.lock()) {
                bool expected = false;
                if (conn->alive.load(std::memory_order_relaxed) && conn->socket &&
                    conn->socket->is_open() &&
                    conn->in_use.compare_exchange_strong(expected, true,
                                                         std::memory_order_acquire)) {
                    // A connection became available. Use it and discard the one we created.
                    boost::system::error_code ec;
                    fresh->socket->close(ec);
                    fresh->alive = false;
                    co_return conn;
                }
            }
        }

        // No connection became available. Use the one we created.
        fresh->in_use.store(true, std::memory_order_relaxed);
        if (connection_pool_.size() < kMaxPoolSize) {
            connection_pool_.push_back(fresh);
        }
    }
    co_return fresh;
}

void AsioConnectionPool::release(std::shared_ptr<AsioConnection> conn) {
    if (conn) {
        conn->in_use.store(false, std::memory_order_release);
    }
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::create_connection() {
    auto conn = std::make_shared<AsioConnection>(opts_);
    auto socket_res = co_await async_connect_with_timeout(opts_);
    if (!socket_res) {
        co_return nullptr;
    }
    conn->socket = std::move(socket_res.value());
    conn->alive = true;

    // Start background read loop to receive responses
    if (!conn->read_started.exchange(true)) {
        co_spawn(
            GlobalIOContext::instance().get_io_context(),
            [weak_conn = std::weak_ptr(conn)]() -> awaitable<void> {
                if (auto conn = weak_conn.lock()) {
                    co_await boost::asio::dispatch(conn->strand, use_awaitable);
                } else {
                    co_return;
                }

                MessageFramer framer;
                for (;;) {
                    auto conn = weak_conn.lock();
                    if (!conn) {
                        co_return;
                    }

                    auto hres = co_await async_read_exact(
                        *conn->socket, sizeof(MessageFramer::FrameHeader),
                        conn->streaming_started.load(std::memory_order_relaxed)
                            ? conn->opts.bodyTimeout
                            : conn->opts.headerTimeout);
                    if (!hres) {
                        Error e = hres.error();
                        if (auto c = weak_conn.lock()) {
                            for (auto& [rid, h] : c->handlers) {
                                if (h.unary)
                                    h.unary->channel->try_send(
                                        make_error_code(boost::system::errc::io_error),
                                        std::make_shared<Result<Response>>(e));
                                if (h.streaming) {
                                    h.streaming->onError(e);
                                    h.streaming->done_channel->try_send(boost::system::error_code{},
                                                                        e);
                                }
                            }
                            c->handlers.clear();
                            c->alive = false;
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
                            if (auto c = weak_conn.lock()) {
                                for (auto& [rid, h] : c->handlers) {
                                    if (h.unary)
                                        h.unary->channel->try_send(
                                            make_error_code(boost::system::errc::io_error),
                                            std::make_shared<Result<Response>>(e));
                                    if (h.streaming) {
                                        h.streaming->onError(e);
                                        h.streaming->done_channel->try_send(
                                            boost::system::error_code{}, e);
                                    }
                                }
                                c->handlers.clear();
                                c->alive = false;
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
                    if (!msgRes)
                        continue;
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

                    AsioConnection::Handler* handlerPtr = nullptr;
                    if (auto it = conn->handlers.find(reqId); it != conn->handlers.end()) {
                        handlerPtr = &it->second;
                    }
                    if (!handlerPtr)
                        continue;

                    bool isChunked = header.is_chunked();
                    bool isHeaderOnly = header.is_header_only();
                    bool isLast = header.is_last_chunk();

                    const Response& r = std::get<Response>(msg.payload);
                    if (!isChunked) {
                        if (handlerPtr->unary) {
                            handlerPtr->unary->channel->try_send(
                                boost::system::error_code{}, std::make_shared<Result<Response>>(r));
                            conn->handlers.erase(reqId);
                        } else if (handlerPtr->streaming) {
                            handlerPtr->streaming->onHeader(r);
                            handlerPtr->streaming->onComplete();
                            handlerPtr->streaming->done_channel->try_send(
                                boost::system::error_code{}, Result<void>());
                            conn->handlers.erase(reqId);
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
                                handlerPtr->streaming->done_channel->try_send(
                                    boost::system::error_code{}, Result<void>());
                                conn->handlers.erase(reqId);
                            }
                        }
                    }
                }
            },
            boost::asio::detached);
    }

    co_return conn;
}

} // namespace yams::daemon
