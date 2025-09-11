#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/as_tuple.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>

#include <yams/daemon/ipc/fsm_metrics_registry.h>

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

struct AsioTransportAdapter::Connection {
    using socket_t = boost::asio::local::stream_protocol::socket;
    using response_channel_t = boost::asio::experimental::channel<void(
        boost::system::error_code, std::shared_ptr<Result<Response>>)>;
    using void_channel_t =
        boost::asio::experimental::channel<void(boost::system::error_code, Result<void>)>;

    explicit Connection(const Options& o)
        : opts(o), strand(GlobalIOContext::instance().get_io_context()) {}

    Options opts;
    boost::asio::io_context::strand strand;
    std::unique_ptr<socket_t> socket;
    std::atomic<bool> read_started{false};
    std::atomic<bool> alive{false};
    // When any streaming request has delivered its first header, switch header reads to bodyTimeout
    std::atomic<bool> streaming_started{false};

    struct UnaryHandler {
        std::shared_ptr<response_channel_t> channel;
    };
    struct StreamingHandler {
        HeaderCallback onHeader;
        ChunkCallback onChunk;
        ErrorCallback onError;
        CompleteCallback onComplete;
        std::shared_ptr<void_channel_t> done_channel;

        StreamingHandler() = default;
        StreamingHandler(HeaderCallback h, ChunkCallback c, ErrorCallback e, CompleteCallback comp)
            : onHeader(std::move(h)), onChunk(std::move(c)), onError(std::move(e)),
              onComplete(std::move(comp)) {}
    };
    struct Handler {
        std::optional<UnaryHandler> unary;
        std::optional<StreamingHandler> streaming;
    };

    std::unordered_map<uint64_t, Handler> handlers;
    std::deque<std::vector<uint8_t>> write_queue;
    bool writing{false};
    std::atomic<uint64_t> total_bytes_written{0};
    std::atomic<uint64_t> total_batches{0};
    std::atomic<uint64_t> total_frames{0};
    std::atomic<size_t> peak_handlers{0};
    std::atomic<size_t> batch_cap{256 * 1024};
    std::chrono::steady_clock::time_point last_adjust{std::chrono::steady_clock::now()};

    boost::asio::awaitable<Result<void>> async_write_frame(std::vector<uint8_t> frame) {
        co_await boost::asio::dispatch(strand, use_awaitable);
        write_queue.emplace_back(std::move(frame));
        if (writing)
            co_return Result<void>();
        writing = true;
        while (!write_queue.empty()) {
            std::vector<uint8_t> batch;
            batch.reserve(write_queue.front().size());
            std::size_t batched = 0;
            auto cap = batch_cap.load(std::memory_order_relaxed);
            std::size_t frames = 0;
            while (!write_queue.empty() && batched < cap) {
                auto& f = write_queue.front();
                batched += f.size();
                frames++;
                batch.insert(batch.end(), f.begin(), f.end());
                write_queue.pop_front();
            }
            auto res = co_await AsioTransportAdapter(opts).async_write_all(*socket, batch,
                                                                           opts.bodyTimeout);
            if (!res) {
                writing = false;
                co_return res;
            }
            total_batches.fetch_add(1, std::memory_order_relaxed);
            total_frames.fetch_add(frames, std::memory_order_relaxed);
            total_bytes_written.fetch_add(batched, std::memory_order_relaxed);
            auto now = std::chrono::steady_clock::now();
            if (now - last_adjust > std::chrono::seconds(1)) {
                size_t handlers_sz = 0;
                handlers_sz = handlers.size();
                peak_handlers.store(std::max(peak_handlers.load(), handlers_sz));
                auto f = total_frames.exchange(0);
                auto b = total_batches.exchange(0);
                last_adjust = now;
                if (b > 0) {
                    double avg_frames = static_cast<double>(f) / static_cast<double>(b);
                    if (avg_frames > 2.0 && cap < 1024 * 1024) {
                        batch_cap.store(cap * 2, std::memory_order_relaxed);
                    } else if (avg_frames < 1.2 && cap > 64 * 1024) {
                        batch_cap.store(cap / 2, std::memory_order_relaxed);
                    }
                }
                if (handlers_sz > opts.maxInflight * 3 / 4 && opts.maxInflight < 1024) {
                    opts.maxInflight *= 2;
                } else if (handlers_sz < opts.maxInflight / 4 && opts.maxInflight > 64) {
                    opts.maxInflight /= 2;
                }
            }
        }
        writing = false;
        co_return Result<void>();
    }
};

namespace {
struct ConnRegistry {
    std::mutex m;
    std::unordered_map<std::string, std::weak_ptr<AsioTransportAdapter::Connection>> map;
} g_conn_registry;
} // namespace

Task<std::shared_ptr<AsioTransportAdapter::Connection>>
AsioTransportAdapter::get_or_create_connection(const Options& opts) {
    std::unique_lock lock(g_conn_registry.m);
    auto key = opts.socketPath.string();
    if (auto it = g_conn_registry.map.find(key); it != g_conn_registry.map.end()) {
        if (auto sp = it->second.lock()) {
            co_return sp;
        }
    }
    auto conn = std::make_shared<Connection>(opts);
    g_conn_registry.map[key] = conn;
    lock.unlock();

    auto sres = co_await AsioTransportAdapter(opts).async_connect_with_timeout(opts.socketPath,
                                                                               opts.requestTimeout);
    if (!sres) {
        std::lock_guard<std::mutex> lock(g_conn_registry.m);
        g_conn_registry.map.erase(key);
        co_return nullptr;
    }
    conn->socket = std::move(sres.value());
    conn->alive = true;

    if (!conn->read_started.exchange(true)) {
        auto& io = GlobalIOContext::instance().get_io_context();
        co_spawn(
            io,
            [conn]() -> awaitable<void> {
                // Run the entire read loop on the per-connection strand to serialize
                // access to connection state (handlers map, lifecycle flags, etc.).
                co_await boost::asio::dispatch(conn->strand, use_awaitable);
                MessageFramer framer;
                for (;;) {
                    auto hres = co_await AsioTransportAdapter(conn->opts)
                                    .async_read_exact(
                                        *conn->socket, sizeof(MessageFramer::FrameHeader),
                                        // After initial streaming header, tolerate longer gaps
                                        // between chunks
                                        (conn->streaming_started.load(std::memory_order_relaxed)
                                             ? conn->opts.bodyTimeout
                                             : conn->opts.headerTimeout));
                    if (!hres) {
                        Error e = hres.error();
                        for (auto& [rid, h] : conn->handlers) {
                            if (h.unary)
                                h.unary->channel->try_send(
                                    make_error_code(boost::system::errc::io_error),
                                    std::make_shared<Result<Response>>(e));
                            if (h.streaming) {
                                h.streaming->onError(e);
                                h.streaming->done_channel->try_send(boost::system::error_code{}, e);
                            }
                        }
                        conn->handlers.clear();
                        conn->alive = false;
                        co_return;
                    }
                    MessageFramer::FrameHeader netHeader;
                    std::memcpy(&netHeader, hres.value().data(), sizeof(netHeader));
                    auto header = netHeader;
                    header.from_network();

                    std::vector<uint8_t> payload;
                    if (header.payload_size > 0) {
                        auto pres = co_await AsioTransportAdapter(conn->opts)
                                        .async_read_exact(*conn->socket, header.payload_size,
                                                          conn->opts.bodyTimeout);
                        if (!pres) {
                            Error e = pres.error();
                            for (auto& [rid, h] : conn->handlers) {
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
                            conn->handlers.clear();
                            conn->alive = false;
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

                    AsioTransportAdapter::Connection::Handler* handlerPtr = nullptr;
                    if (auto it = conn->handlers.find(reqId); it != conn->handlers.end()) {
                        handlerPtr = &it->second;
                    }
                    if (!handlerPtr) {
                        continue;
                    }

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
                        // Mark that a streaming session has begun after receiving its first
                        // header-only frame
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
                    // remain on strand until next iteration
                }
            },
            detached);
    }

    co_return conn;
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
        boost::asio::steady_timer timer(co_await this_coro::executor);
        timer.expires_after(timeout);
        auto connect_result = co_await (socket->async_connect(endpoint, use_awaitable) ||
                                        timer.async_wait(use_awaitable));
        if (connect_result.index() == 1) {
            socket->close();
            co_return Error{ErrorCode::Timeout, "Connection timeout"};
        }
        co_return std::move(socket);
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

Task<Result<Response>> AsioTransportAdapter::send_request(const Request& req) {
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
    msg.expectsStreamingResponse = false;

    MessageFramer framer;
    auto framed = framer.frame_message(msg);
    if (!framed) {
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    auto& io = GlobalIOContext::instance().get_io_context();
    auto response_ch = std::make_shared<Connection::response_channel_t>(io, 1);

    co_await boost::asio::dispatch(conn->strand, use_awaitable);
    if (conn->handlers.size() >= conn->opts.maxInflight) {
        co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
    }
    {
        Connection::Handler h;
        h.unary.emplace(Connection::UnaryHandler{response_ch});
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto wres = co_await conn->async_write_frame(framed.value());
    if (!wres) {
        co_await boost::asio::dispatch(conn->strand, use_awaitable);
        conn->handlers.erase(msg.requestId);
        co_return wres.error();
    }

    auto [ec, response_result] =
        co_await response_ch->async_receive(boost::asio::experimental::as_tuple(use_awaitable));
    if (ec) {
        co_return Error{ErrorCode::NetworkError, "Failed to receive response: " + ec.message()};
    }
    co_return *response_result;
}

Task<Result<void>> AsioTransportAdapter::send_request_streaming(const Request& req,
                                                                HeaderCallback onHeader,
                                                                ChunkCallback onChunk,
                                                                ErrorCallback onError,
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
    auto framed = framer.frame_message(msg);
    if (!framed) {
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    auto& io = GlobalIOContext::instance().get_io_context();
    auto done_ch = std::make_shared<Connection::void_channel_t>(io, 1);

    co_await boost::asio::dispatch(conn->strand, use_awaitable);
    if (conn->handlers.size() >= conn->opts.maxInflight) {
        co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
    }
    {
        Connection::Handler h;
        h.streaming.emplace(onHeader, onChunk, onError, onComplete);
        h.streaming->done_channel = done_ch;
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto wres = co_await conn->async_write_frame(framed.value());
    if (!wres) {
        co_await boost::asio::dispatch(conn->strand, use_awaitable);
        conn->handlers.erase(msg.requestId);
        co_return wres.error();
    }

    auto [ec, void_result] =
        co_await done_ch->async_receive(boost::asio::experimental::as_tuple(use_awaitable));
    if (ec) {
        co_return Error{ErrorCode::NetworkError,
                        "Failed to receive stream completion: " + ec.message()};
    }
    co_return void_result;
}

} // namespace yams::daemon
