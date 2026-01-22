#include <yams/daemon/client/asio_connection_pool.h>

#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/profiling.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/cancellation_state.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <spdlog/spdlog.h>

#include <atomic>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <optional>
#include <shared_mutex>
#include <system_error>
#include <vector>

#ifndef _WIN32
#include <poll.h>
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
            // Use the connection's cancel() method to emit cancellation signals
            // This notifies all pending coroutines before closing the socket
            conn->cancel();
        }
    }
    connections_.clear();
}

using boost::asio::awaitable;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;
using boost::asio::as_tuple;
using boost::asio::use_future;

namespace {

awaitable<Result<std::unique_ptr<AsioConnection::socket_t>>>
async_connect_with_timeout(const TransportOptions& opts) {
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    static bool trace = [] {
        if (const char* raw = std::getenv("YAMS_STREAM_TRACE")) {
            std::string v(raw);
            for (auto& ch : v)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            return (v == "1" || v == "true" || v == "on");
        }
        return false;
    }();
    auto executor = co_await this_coro::executor;
    auto socket = std::make_unique<AsioConnection::socket_t>(executor);
    boost::asio::local::stream_protocol::endpoint endpoint(opts.socketPath.string());

    if (trace) {
        spdlog::info("stream-trace: async_connect socket='{}'", opts.socketPath.string());
    }

    // Race connect against timeout using async_initiate (no experimental APIs)
    using ConnectResult = std::tuple<boost::system::error_code>;
    using RaceResult = std::variant<ConnectResult, bool>;

    auto connect_result = co_await boost::asio::async_initiate<
        decltype(use_awaitable), void(std::exception_ptr, RaceResult)>(
        [&socket, &endpoint, executor, timeout = opts.requestTimeout](auto handler) mutable {
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto timer = std::make_shared<boost::asio::steady_timer>(executor);
            timer->expires_after(timeout);

            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

            timer->async_wait([completed, handlerPtr,
                               completion_exec](const boost::system::error_code& ec) mutable {
                if (ec == boost::asio::error::operation_aborted)
                    return;
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<1>, true));
                    });
                }
            });

            socket->async_connect(endpoint, [timer, completed, handlerPtr, completion_exec](
                                                const boost::system::error_code& ec) mutable {
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    timer->cancel();
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr), ec]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<0>, ConnectResult{ec}));
                    });
                }
            });
        },
        use_awaitable);

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
    // Check cancellation before proceeding
    auto cs = co_await this_coro::cancellation_state;
    if (cs.cancelled() != boost::asio::cancellation_type::none) {
        co_return Error{ErrorCode::OperationCancelled, "Operation cancelled"};
    }

    std::vector<uint8_t> buffer(size);
    auto executor = co_await this_coro::executor;

    // Race read against timeout using async_initiate (no experimental APIs)
    using ReadResult = std::tuple<boost::system::error_code, std::size_t>;
    using RaceResult = std::variant<ReadResult, bool>;

    auto read_result = co_await boost::asio::async_initiate<decltype(use_awaitable),
                                                            void(std::exception_ptr, RaceResult)>(
        [&socket, &buffer, executor, timeout](auto handler) mutable {
            auto completed = std::make_shared<std::atomic<bool>>(false);
            auto timer = std::make_shared<boost::asio::steady_timer>(executor);
            timer->expires_after(timeout);

            using HandlerT = std::decay_t<decltype(handler)>;
            auto handlerPtr = std::make_shared<HandlerT>(std::move(handler));
            auto completion_exec = boost::asio::get_associated_executor(*handlerPtr, executor);

            timer->async_wait([completed, handlerPtr,
                               completion_exec](const boost::system::error_code& ec) mutable {
                if (ec == boost::asio::error::operation_aborted)
                    return;
                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                    boost::asio::post(completion_exec, [h = std::move(*handlerPtr)]() mutable {
                        std::move(h)(std::exception_ptr{},
                                     RaceResult(std::in_place_index<1>, true));
                    });
                }
            });

            boost::asio::async_read(
                socket, boost::asio::buffer(buffer),
                [timer, completed, handlerPtr, completion_exec](const boost::system::error_code& ec,
                                                                std::size_t bytes) mutable {
                    if (!completed->exchange(true, std::memory_order_acq_rel)) {
                        timer->cancel();
                        boost::asio::post(completion_exec, [h = std::move(*handlerPtr), ec,
                                                            bytes]() mutable {
                            std::move(h)(std::exception_ptr{},
                                         RaceResult(std::in_place_index<0>, ReadResult{ec, bytes}));
                        });
                    }
                });
        },
        use_awaitable);

    if (read_result.index() == 1) {
        co_return Error{ErrorCode::Timeout, "Read timeout"};
    }

    auto& [ec, bytes_read] = std::get<0>(read_result);
    if (ec) {
        co_return Error{ErrorCode::NetworkError, ec.message()};
    }

    co_return buffer;
}

bool socket_looks_healthy(AsioConnection::socket_t& socket) {
#ifndef _WIN32
    int fd = socket.native_handle();
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLIN | POLLERR | POLLHUP | POLLNVAL;
    pfd.revents = 0;
    int res = ::poll(&pfd, 1, 0);
    if (res < 0) {
        if (errno == EINTR) {
            return true;
        }
        return false;
    }
    if (res == 0) {
        return true;
    }
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        return false;
    }
    if (pfd.revents & POLLIN) {
        char buf;
        ssize_t n = ::recv(fd, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) {
            return false;
        }
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
    }
#endif
    return true;
}

} // namespace

namespace {

std::shared_mutex& registry_mutex() {
    static auto* m = new std::shared_mutex();
    return *m;
}

std::unordered_map<std::string, std::shared_ptr<AsioConnectionPool>>& registry_map() {
    static auto* map = new std::unordered_map<std::string, std::shared_ptr<AsioConnectionPool>>();
    return *map;
}

} // namespace

AsioConnectionPool::AsioConnectionPool(const TransportOptions& opts, bool shared)
    : opts_(opts), shared_(shared) {}

AsioConnectionPool::~AsioConnectionPool() {
    if (!shutdown_.load(std::memory_order_acquire)) {
        shutdown(std::chrono::milliseconds(500));
    }
}

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
    }

    for (auto& pool : pools) {
        pool->shutdown(timeout);
    }

    {
        std::lock_guard<std::shared_mutex> lk(registry_mutex());
        registry_map().clear();
    }
}

void AsioConnectionPool::cleanup_stale_connections() {
    std::erase_if(connection_pool_,
                  [](const std::weak_ptr<AsioConnection>& weak) { return weak.expired(); });
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::acquire() {
    YAMS_ZONE_SCOPED_N("ConnectionPool::acquire");
    // Check if pool is being shut down
    if (shutdown_.load(std::memory_order_acquire)) {
        co_return nullptr;
    }

    // Fast path: try to reuse an existing idle connection
    {
        YAMS_ZONE_SCOPED_N("ConnectionPool::acquire::tryReuse");
        std::lock_guard<std::mutex> lk(mutex_);
        cleanup_stale_connections();

        for (auto& weak : connection_pool_) {
            if (auto conn = weak.lock()) {
                // Check if connection is alive and not currently in use
                if (conn->alive.load(std::memory_order_acquire) &&
                    !conn->in_use.exchange(true, std::memory_order_acq_rel)) {
                    if (conn->read_loop_future.valid()) {
                        if (conn->read_loop_future.wait_for(std::chrono::milliseconds(0)) ==
                            std::future_status::ready) {
                            conn->alive.store(false, std::memory_order_release);
                            conn->in_use.store(false, std::memory_order_release);
                            continue;
                        }
                    }
                    // Verify socket is still open
                    if (conn->socket && conn->socket->is_open() &&
                        socket_looks_healthy(*conn->socket)) {
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
    co_return co_await create_connection();
}

void AsioConnectionPool::release(const std::shared_ptr<AsioConnection>& conn) {
    YAMS_ZONE_SCOPED_N("ConnectionPool::release");
    if (conn) {
        conn->in_use.store(false, std::memory_order_release);
    }
}

void AsioConnectionPool::shutdown(std::chrono::milliseconds timeout) {
    // Mark as shutdown first, preventing new operations
    if (shutdown_.exchange(true, std::memory_order_acq_rel)) {
        // Already shut down, skip
        return;
    }

    // Emit cancellation signal to abort any pending acquire/create_connection operations
    // This wakes up coroutines waiting on async_connect_with_timeout
    shutdown_signal_.emit(boost::asio::cancellation_type::terminal);

    std::lock_guard<std::mutex> lk(mutex_);

    for (auto& weak : connection_pool_) {
        if (auto conn = weak.lock()) {
            // Use cancel() to emit cancellation signals to all pending coroutines
            // This properly notifies waiters before closing the socket
            conn->cancel();

            if (conn->read_loop_future.valid()) {
                try {
                    auto status = conn->read_loop_future.wait_for(timeout);
                    (void)status;
                } catch (...) {
                }
            }
        }
    }

    connection_pool_.clear();
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::create_connection() {
    YAMS_ZONE_SCOPED_N("ConnectionPool::create_connection");
    // Check shutdown before starting
    if (shutdown_.load(std::memory_order_acquire)) {
        co_return nullptr;
    }

    auto conn = std::make_shared<AsioConnection>(opts_);
    auto socketMissing = [&]() {
        std::error_code ec;
        return !std::filesystem::exists(opts_.socketPath, ec);
    };

    constexpr int kMaxRetries = 3;
    auto backoff = std::chrono::milliseconds(50);
    Result<std::unique_ptr<AsioConnection::socket_t>> socket_res =
        Error{ErrorCode::NetworkError, "Connection failed"};
    for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
        if (socketMissing()) {
            if (attempt + 1 < kMaxRetries) {
                std::this_thread::sleep_for(backoff);
                backoff = std::min(backoff * 2, std::chrono::milliseconds(250));
                continue;
            }
        }
        socket_res = co_await async_connect_with_timeout(opts_);
        if (socket_res) {
            break;
        }
        if (socketMissing() && attempt + 1 < kMaxRetries) {
            std::this_thread::sleep_for(backoff);
            backoff = std::min(backoff * 2, std::chrono::milliseconds(250));
            continue;
        }
        break;
    }

    // Check shutdown after connection attempt - pool may have been shut down while connecting
    if (shutdown_.load(std::memory_order_acquire)) {
        // Close the socket if we got one, then bail
        if (socket_res && socket_res.value()) {
            boost::system::error_code ec;
            socket_res.value()->close(ec);
        }
        co_return nullptr;
    }

    if (!socket_res) {
        if (socketMissing()) {
            spdlog::debug("[ConnectionPool::create_connection] socket not available: {}",
                          opts_.socketPath.string());
        } else {
            spdlog::warn("[ConnectionPool::create_connection] socket_res is error: {}",
                         socket_res.error().message);
        }
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

    // For non-shared pools, keep the pool alive as long as the connection is in use
    if (!shared_) {
        conn->pool_keepalive = shared_from_this();
    }

    ConnectionRegistry::instance().add(conn);

    co_return conn;
}

awaitable<void> AsioConnectionPool::ensure_read_loop_started(std::shared_ptr<AsioConnection> conn) {
    // Serialize read loop checks/starts on the connection's strand to prevent races
    co_await boost::asio::dispatch(conn->strand, use_awaitable);

    // Check if loop needs to be started/restarted
    bool need_start = false;
    if (!conn->read_started.load(std::memory_order_acquire)) {
        need_start = true;
        conn->read_started.store(true, std::memory_order_release);
    } else if (!conn->read_loop_future.valid()) {
        need_start = true;
    } else if (conn->read_loop_future.wait_for(std::chrono::milliseconds(0)) ==
               std::future_status::ready) {
        try {
            conn->read_loop_future.get();
        } catch (...) {
        }
        need_start = true;
    }

    if (!need_start) {
        co_return;
    }

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
                // Check cancellation state at each iteration
                auto cs = co_await this_coro::cancellation_state;
                if (cs.cancelled() != boost::asio::cancellation_type::none) {
                    co_return;
                }

                // Re-check connection validity (may have been cancelled during co_await)
                if (auto c = weak_conn.lock(); !c) {
                    co_return;
                }

                // Check connection's alive flag (set to false by cancel())
                if (!conn->alive.load(std::memory_order_acquire)) {
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
                    // If connection was cancelled, exit immediately without cleanup
                    // (cleanup is handled by the shutdown code)
                    if (auto c = weak_conn.lock()) {
                        if (!c->alive.load(std::memory_order_acquire)) {
                            co_return;
                        }
                    }

                    Error e = hres.error();
                    if (e.code == ErrorCode::OperationCancelled) {
                        co_return;
                    }
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
                                    h.unary->notify_timer->cancel();
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
                                    h.streaming->notify_timer->cancel();
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
                        // If connection was cancelled, exit immediately without cleanup
                        if (auto c = weak_conn.lock()) {
                            if (!c->alive.load(std::memory_order_acquire)) {
                                co_return;
                            }
                        }

                        Error e = pres.error();
                        if (e.code == ErrorCode::OperationCancelled) {
                            co_return;
                        }
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
                                        h.unary->notify_timer->cancel();
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
                                        h.streaming->notify_timer->cancel();
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
                    // If connection was cancelled, exit immediately without cleanup
                    if (auto c = weak_conn.lock()) {
                        if (!c->alive.load(std::memory_order_acquire)) {
                            co_return;
                        }
                    }

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
                                    h.unary->notify_timer->cancel();
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
                                    h.streaming->notify_timer->cancel();
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
                    try {
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
                    } catch (...) {
                    }
                }
                uint64_t reqId = msg.requestId;

                // Check if connection was cancelled before trying strand dispatch
                if (!conn->alive.load(std::memory_order_acquire)) {
                    co_return;
                }

                // Re-acquire strand protection before accessing handlers map
                // to prevent race with concurrent emplace() from send_request
                co_await boost::asio::dispatch(conn->strand, use_awaitable);

                AsioConnection::Handler* handlerPtr = nullptr;
                if (auto it = conn->handlers.find(reqId); it != conn->handlers.end()) {
                    handlerPtr = &it->second;
                }
                if (!handlerPtr) {
                    bool wasTimedOut = false;
                    if (auto it = conn->timed_out_requests.find(reqId);
                        it != conn->timed_out_requests.end()) {
                        wasTimedOut = true;
                        conn->timed_out_requests.erase(it);
                    }
                    try {
                        if (wasTimedOut) {
                            spdlog::debug("ASIO read loop: late daemon response after timeout "
                                          "(request id {})",
                                          reqId);
                        } else {
                            spdlog::warn(
                                "ASIO read loop: no handler for daemon response with request "
                                "id {}. This may indicate a response was already received or "
                                "the request timed out.",
                                reqId);
                        }
                    } catch (...) {
                    }
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
                            handlerPtr->unary->promise->set_value(Result<Response>(std::move(r)));
                        } catch (const std::future_error&) {
                            // Promise already satisfied, ignore
                        }
                        // Cancel the notify timer to wake up the waiter immediately
                        if (handlerPtr->unary->notify_timer) {
                            handlerPtr->unary->notify_timer->cancel();
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
                            handlerPtr->streaming->notify_timer->cancel();
                        }
                        conn->handlers.erase(reqId);
                        conn->streaming_started.store(false, std::memory_order_relaxed);
                    }
                    continue;
                }

                if (handlerPtr->streaming) {
                    if (isHeaderOnly) {
                        conn->streaming_started.store(true, std::memory_order_relaxed);
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
                                handlerPtr->streaming->notify_timer->cancel();
                            }
                            conn->handlers.erase(reqId);
                            conn->streaming_started.store(false, std::memory_order_relaxed);
                        }
                    }
                }
            }
        },
        boost::asio::use_future);
    co_return;
}

} // namespace yams::daemon
