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
        if (!std::filesystem::exists(opts.socketPath)) {
            std::string msg =
                "Daemon not started (socket not found at '" + opts.socketPath.string() + "').";
            spdlog::debug("AsioConnectionPool: {}", msg);
            co_return Error{ErrorCode::NetworkError, std::move(msg)};
        }
#ifndef _WIN32
        {
            struct stat st;
            if (::stat(opts.socketPath.c_str(), &st) == 0) {
                if (!S_ISSOCK(st.st_mode)) {
                    std::string msg =
                        "Path exists but is not a socket: '" + opts.socketPath.string() + "'";
                    spdlog::debug("AsioConnectionPool: {}", msg);
                    co_return Error{ErrorCode::NetworkError, std::move(msg)};
                }
            }
        }
#endif
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
std::mutex& registry_mutex() {
    static std::mutex m;
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
    std::lock_guard<std::mutex> lk(registry_mutex());
    auto key = opts.socketPath.string();
    auto& map = registry_map();
    if (auto it = map.find(key); it != map.end()) {
        return it->second;
    }
    auto pool = std::make_shared<AsioConnectionPool>(opts, true);
    map[key] = pool;
    return pool;
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::acquire() {
    if (!shared_) {
        co_return co_await create_connection();
    }
    std::shared_ptr<AsioConnection> existing;
    {
        std::lock_guard<std::mutex> lk(mutex_);
        existing = cachedStrong_;
    }
    if (existing && existing->alive.load(std::memory_order_relaxed) && existing->socket &&
        existing->socket->is_open()) {
        co_return existing;
    }
    if (existing) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (cachedStrong_.get() == existing.get()) {
            cachedStrong_.reset();
            cached_.reset();
        }
    }
    auto fresh = co_await create_connection();
    if (shared_ && fresh && fresh->alive.load(std::memory_order_relaxed)) {
        std::lock_guard<std::mutex> lk(mutex_);
        cachedStrong_ = fresh;
        cached_ = fresh;
    }
    co_return fresh;
}

awaitable<std::shared_ptr<AsioConnection>> AsioConnectionPool::create_connection() {
    auto conn = std::make_shared<AsioConnection>(opts_);
    auto socket_res = co_await async_connect_with_timeout(opts_);
    if (!socket_res) {
        co_return nullptr;
    }
    conn->socket = std::move(socket_res.value());
    conn->alive = true;

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
