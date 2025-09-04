#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/ipc/ipc_protocol.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/strand.hpp>

#include <yams/daemon/ipc/fsm_metrics_registry.h>

#include <spdlog/spdlog.h>
#include <yams/core/format.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>
#include <span>
#include <unordered_map>
#include <deque>

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/stat.h>
#endif

namespace yams::daemon {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
namespace this_coro = boost::asio::this_coro;
using namespace boost::asio::experimental::awaitable_operators;

AsioTransportAdapter::AsioTransportAdapter(const Options& opts) : opts_(opts) {
    // Enable FSM observability based on daemon logging configuration
    // Metrics are enabled when FSM metrics registry is enabled (controlled by log level)
    bool metrics_on = FsmMetricsRegistry::instance().enabled();
    
    // Snapshots can still be controlled via environment for debugging
    const char* s = std::getenv("YAMS_FSM_SNAPSHOTS");
    bool snaps_on = (s && (std::strcmp(s, "1") == 0 || std::strcmp(s, "true") == 0 ||
                           std::strcmp(s, "TRUE") == 0 || std::strcmp(s, "on") == 0 ||
                           std::strcmp(s, "ON") == 0));
    
    fsm_.enable_metrics(metrics_on);
    fsm_.enable_snapshots(snaps_on);
}

// -----------------------------------------------------------------------------
// Multiplexed connection shared per socket path
// -----------------------------------------------------------------------------

struct AsioTransportAdapter::Connection {
    using socket_t = boost::asio::local::stream_protocol::socket;

    explicit Connection(const Options& o)
        : opts(o), strand(GlobalIOContext::instance().get_io_context()) {}

    Options opts;
    boost::asio::io_context::strand strand;
    std::unique_ptr<socket_t> socket;
    std::atomic<bool> read_started{false};
    std::atomic<bool> alive{false};

    struct UnaryHandler {
        std::promise<Result<Response>> promise;
    };
    struct StreamingHandler {
        HeaderCallback onHeader;
        ChunkCallback onChunk;
        ErrorCallback onError;
        CompleteCallback onComplete;
        std::promise<Result<void>> done;

        StreamingHandler() = default;
        StreamingHandler(HeaderCallback h, ChunkCallback c, ErrorCallback e, CompleteCallback comp)
            : onHeader(std::move(h)), onChunk(std::move(c)), onError(std::move(e)),
              onComplete(std::move(comp)), done() {}
    };
    struct Handler {
        std::optional<UnaryHandler> unary;
        std::optional<StreamingHandler> streaming;
    };

    std::mutex mtx;
    std::unordered_map<uint64_t, Handler> handlers; // requestId -> handler
    std::deque<std::vector<uint8_t>> write_queue;
    bool writing{false};
    // Telemetry (lightweight)
    std::atomic<uint64_t> total_bytes_written{0};
    std::atomic<uint64_t> total_batches{0};
    std::atomic<uint64_t> total_frames{0};
    std::atomic<size_t>   peak_handlers{0};
    std::atomic<size_t>   batch_cap{256 * 1024};
    std::chrono::steady_clock::time_point last_adjust{std::chrono::steady_clock::now()};

    // Enqueue write on strand to serialize writes and coalesce small frames
    boost::asio::awaitable<Result<void>> async_write_frame(std::vector<uint8_t> frame) {
        co_await boost::asio::dispatch(strand, use_awaitable);
        write_queue.emplace_back(std::move(frame));
        if (writing) co_return Result<void>();
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
            auto res = co_await AsioTransportAdapter(opts).async_write_all(*socket, batch, opts.bodyTimeout);
            if (!res) {
                writing = false;
                co_return res;
            }
            total_batches.fetch_add(1, std::memory_order_relaxed);
            total_frames.fetch_add(frames, std::memory_order_relaxed);
            total_bytes_written.fetch_add(batched, std::memory_order_relaxed);
            // Adaptive tuning (coarse): adjust batch cap up if many frames, down if singletons
            auto now = std::chrono::steady_clock::now();
            if (now - last_adjust > std::chrono::seconds(1)) {
                size_t handlers_sz = 0;
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    handlers_sz = handlers.size();
                }
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
                // Adjust maxInflight gently if persistent pressure
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

std::shared_ptr<AsioTransportAdapter::Connection>
AsioTransportAdapter::get_or_create_connection(const Options& opts) {
    std::lock_guard<std::mutex> lock(g_conn_registry.m);
    auto key = opts.socketPath.string();
    if (auto it = g_conn_registry.map.find(key); it != g_conn_registry.map.end()) {
        if (auto sp = it->second.lock()) {
            return sp;
        }
    }
    auto conn = std::make_shared<Connection>(opts);
    g_conn_registry.map[key] = conn;

    // Establish socket synchronously via async bridge
    std::promise<Result<std::unique_ptr<Connection::socket_t>>> p;
    auto fut = p.get_future();
    auto& io = GlobalIOContext::instance().get_io_context();
    co_spawn(io,
             [opts, conn, &p]() -> awaitable<void> {
                 auto sres = co_await AsioTransportAdapter(opts).async_connect_with_timeout(
                     opts.socketPath, opts.requestTimeout);
                 if (!sres) {
                     p.set_value(Error{sres.error().code, sres.error().message});
                     co_return;
                 }
                 p.set_value(std::move(sres).value());
             },
             detached);
    auto s = fut.get();
    if (!s) {
        return nullptr;
    }
    // Move unique_ptr from result into connection safely
    conn->socket = std::move(s).value();
    conn->alive = true;

    // Start demux read loop once per connection
    if (!conn->read_started.exchange(true)) {
        co_spawn(io,
                 [conn]() -> awaitable<void> {
                     MessageFramer framer;
                     for (;;) {
                         auto hres = co_await AsioTransportAdapter(conn->opts)
                                          .async_read_exact(*conn->socket,
                                                            sizeof(MessageFramer::FrameHeader),
                                                            conn->opts.headerTimeout);
                         if (!hres) {
                             Error e = hres.error();
                             std::lock_guard<std::mutex> lk(conn->mtx);
                             for (auto& [rid, h] : conn->handlers) {
                                 if (h.unary) h.unary->promise.set_value(e);
                                 if (h.streaming) {
                                     h.streaming->onError(e);
                                     h.streaming->done.set_value(e);
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
                                 std::lock_guard<std::mutex> lk(conn->mtx);
                                 for (auto& [rid, h] : conn->handlers) {
                                     if (h.unary) h.unary->promise.set_value(e);
                                     if (h.streaming) {
                                         h.streaming->onError(e);
                                         h.streaming->done.set_value(e);
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
                         if (!msgRes) continue;
                         auto& msg = msgRes.value();
                         uint64_t reqId = msg.requestId;

                         AsioTransportAdapter::Connection::Handler* handlerPtr = nullptr;
                         std::unique_lock<std::mutex> lk(conn->mtx);
                         if (auto it = conn->handlers.find(reqId); it != conn->handlers.end()) {
                             handlerPtr = &it->second;
                         }
                         if (!handlerPtr) {
                             lk.unlock();
                             continue;
                         }

                         bool isChunked = header.is_chunked();
                         bool isHeaderOnly = header.is_header_only();
                         bool isLast = header.is_last_chunk();

                         const Response& r = std::get<Response>(msg.payload);
                         if (!isChunked) {
                             if (handlerPtr->unary) {
                                 handlerPtr->unary->promise.set_value(r);
                                 conn->handlers.erase(reqId);
                             } else if (handlerPtr->streaming) {
                                 handlerPtr->streaming->onHeader(r);
                                 handlerPtr->streaming->onComplete();
                                 handlerPtr->streaming->done.set_value(Result<void>());
                                 conn->handlers.erase(reqId);
                             }
                             lk.unlock();
                             continue;
                         }

                         if (handlerPtr->streaming) {
                             if (isHeaderOnly) {
                                 handlerPtr->streaming->onHeader(r);
                             } else {
                                 handlerPtr->streaming->onChunk(r, isLast);
                                 if (isLast) {
                                     handlerPtr->streaming->onComplete();
                                     handlerPtr->streaming->done.set_value(Result<void>());
                                     conn->handlers.erase(reqId);
                                 }
                             }
                         }
                         lk.unlock();
                     }
                 },
                 detached);
    }

    return conn;
}

awaitable<Result<std::unique_ptr<boost::asio::local::stream_protocol::socket>>>
AsioTransportAdapter::async_connect_with_timeout(const std::filesystem::path& path, 
                                                std::chrono::milliseconds timeout) {
    auto& io_ctx = GlobalIOContext::instance().get_io_context();
    auto socket = std::make_unique<boost::asio::local::stream_protocol::socket>(io_ctx);
    
    boost::asio::local::stream_protocol::endpoint endpoint(path.string());
    
    try {
        // Preflight: ensure the socket path exists to catch mismatches early
        if (!std::filesystem::exists(path)) {
            std::string msg = "Daemon not started (socket not found at '" + path.string() +
                              "'). Set YAMS_DAEMON_SOCKET or update config (daemon.socket_path).";
            spdlog::debug("AsioTransportAdapter preflight: {}", msg);
            co_return Error{ErrorCode::NetworkError, std::move(msg)};
        }
        // Verify it is actually a socket (not a stale regular file)
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
        // Set up a timer for timeout
        boost::asio::steady_timer timer(co_await this_coro::executor);
        timer.expires_after(timeout);
        
        // Start both the connect and the timer
        auto connect_result = co_await (
            socket->async_connect(endpoint, use_awaitable) ||
            timer.async_wait(use_awaitable)
        );
        
        if (connect_result.index() == 1) {
            // Timer fired first - timeout
            socket->close();
            co_return Error{ErrorCode::Timeout, "Connection timeout"};
        }
        
        // Connection succeeded
        co_return std::move(socket);
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, 
                   yams::format("Connection failed: {}", e.what())};
    }
}

awaitable<Result<std::vector<uint8_t>>>
AsioTransportAdapter::async_read_exact(boost::asio::local::stream_protocol::socket& socket,
                                      size_t size,
                                      std::chrono::milliseconds timeout) {
    try {
        std::vector<uint8_t> buffer(size);
        
        boost::asio::steady_timer timer(co_await this_coro::executor);
        timer.expires_after(timeout);
        
        auto read_result = co_await (
            boost::asio::async_read(socket, boost::asio::buffer(buffer), use_awaitable) ||
            timer.async_wait(use_awaitable)
        );
        
        if (read_result.index() == 1) {
            // Timer fired first - timeout
            co_return Error{ErrorCode::Timeout, "Read timeout"};
        }
        
        co_return buffer;
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, 
                       yams::format("Read failed: {}", e.what())};
    }
}

awaitable<Result<void>>
AsioTransportAdapter::async_write_all(boost::asio::local::stream_protocol::socket& socket,
                                     const std::vector<uint8_t>& data,
                                     std::chrono::milliseconds timeout) {
    try {
        boost::asio::steady_timer timer(co_await this_coro::executor);
        timer.expires_after(timeout);
        
        auto write_result = co_await (
            boost::asio::async_write(socket, boost::asio::buffer(data), use_awaitable) ||
            timer.async_wait(use_awaitable)
        );
        
        if (write_result.index() == 1) {
            // Timer fired first - timeout
            co_return Error{ErrorCode::Timeout, "Write timeout"};
        }
        
        co_return Result<void>{};
    } catch (const std::exception& e) {
        co_return Error{ErrorCode::NetworkError, 
                       yams::format("Write failed: {}", e.what())};
    }
}

awaitable<Result<void>> AsioTransportAdapter::receive_frames(
    boost::asio::local::stream_protocol::socket& socket, MessageFramer& framer,
    HeaderCallback onHeader, ChunkCallback onChunk, ErrorCallback onError,
    CompleteCallback onComplete) {
    fsm_.on_readable(0); // Transition to reading

    // Read response frames (non-chunked completes in first iteration)
    bool expectingMore = true;
    bool headerReceived = false;   // Initial header-only frame received
    bool headerNotified = false;   // onHeader() delivered to client

    spdlog::debug("AsioTransportAdapter::receive_frames: waiting for response frames");

    while (expectingMore) {
        // Read frame header
        auto header_result = co_await async_read_exact(socket, sizeof(MessageFramer::FrameHeader),
                                                       opts_.headerTimeout);
        if (!header_result) {
            fsm_.on_error(static_cast<int>(header_result.error().code));
            onError(header_result.error());
            co_return header_result.error();
        }
        FsmMetricsRegistry::instance().incrementHeaderReads(1);
        FsmMetricsRegistry::instance().addBytesReceived(header_result.value().size());

        MessageFramer::FrameHeader netHeader;
        std::memcpy(&netHeader, header_result.value().data(), sizeof(MessageFramer::FrameHeader));
        MessageFramer::FrameHeader header = netHeader;
        header.from_network();

        {
            ConnectionFsm::FrameInfo info{0u, header.flags, header.payload_size};
            fsm_.on_header_parsed(info);
        }

        if (header.payload_size > 100 * 1024 * 1024) {
            auto err = Error{ErrorCode::InvalidData, "Payload too large"};
            fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
            onError(err);
            co_return err;
        }

        bool isHeaderOnly = header.is_header_only();
        bool isLastChunk = header.is_last_chunk();
        bool isChunked = header.is_chunked();

        spdlog::debug(
            "AsioTransportAdapter: received frame - header_only={}, last_chunk={}, chunked={}, payload_size={}",
            isHeaderOnly, isLastChunk, isChunked, header.payload_size);

        // Non-chunked complete response
        if (!isChunked && !headerReceived) {
            if (header.payload_size > 0) {
                auto payload_result = co_await async_read_exact(socket, header.payload_size,
                                                               opts_.bodyTimeout);
                if (!payload_result) {
                    fsm_.on_error(static_cast<int>(payload_result.error().code));
                    onError(payload_result.error());
                    co_return payload_result.error();
                }

                // Reconstruct complete frame
                std::vector<uint8_t> complete_frame;
                complete_frame.reserve(sizeof(MessageFramer::FrameHeader) + payload_result.value().size());
                complete_frame.insert(complete_frame.end(), header_result.value().begin(),
                                      header_result.value().end());
                complete_frame.insert(complete_frame.end(), payload_result.value().begin(),
                                      payload_result.value().end());

                auto respMsg = framer.parse_frame(complete_frame);
                if (!respMsg) {
                    fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                    onError(respMsg.error());
                    co_return respMsg.error();
                }

                if (!std::holds_alternative<Response>(respMsg.value().payload)) {
                    auto err = Error{ErrorCode::InvalidData, "Expected Response but got Request"};
                    onError(err);
                    co_return err;
                }

                const auto& response = std::get<Response>(respMsg.value().payload);
                spdlog::debug("AsioTransportAdapter: delivering complete non-chunked response to handler");
                onHeader(response);
                onChunk(response, true);
                onComplete();
                break;
            }
        } else if (isHeaderOnly && !headerReceived) {
            // Initial header-only frame: no payload; wait for first chunk
            headerReceived = true;
            spdlog::debug("AsioTransportAdapter: header-only frame received (stream start)");
        } else if (header.payload_size > 0) {
            // Chunk frame
            auto payload_result =
                co_await async_read_exact(socket, header.payload_size, opts_.bodyTimeout);
            if (!payload_result) {
                fsm_.on_error(static_cast<int>(payload_result.error().code));
                onError(payload_result.error());
                co_return payload_result.error();
            }
            FsmMetricsRegistry::instance().incrementPayloadReads(1);
            FsmMetricsRegistry::instance().addBytesReceived(payload_result.value().size());

            // Reconstruct complete frame
            std::vector<uint8_t> complete_frame;
            complete_frame.reserve(sizeof(MessageFramer::FrameHeader) + payload_result.value().size());
            complete_frame.insert(complete_frame.end(), header_result.value().begin(),
                                  header_result.value().end());
            complete_frame.insert(complete_frame.end(), payload_result.value().begin(),
                                  payload_result.value().end());

            auto respMsg = framer.parse_frame(complete_frame);
            if (!respMsg || !std::holds_alternative<Response>(respMsg.value().payload)) {
                auto err = Error{ErrorCode::InvalidData, "Invalid chunk response"};
                fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                onError(err);
                co_return err;
            }

            const auto& parsed = std::get<Response>(respMsg.value().payload);
            if (!headerNotified) {
                spdlog::debug("AsioTransportAdapter: delivering onHeader to handler");
                onHeader(parsed);
                headerNotified = true;
            }
            spdlog::debug("AsioTransportAdapter: delivering onChunk to handler (last={})", isLastChunk);
            bool continueReading = onChunk(parsed, isLastChunk);
            if (!continueReading) {
                expectingMore = false;
            }
        }

        if (isLastChunk) {
            expectingMore = false;
        }
    }

    fsm_.on_close_request();
    onComplete();
    co_return Result<void>{};
}

Task<Result<Response>> AsioTransportAdapter::send_request(const Request& req) {
    spdlog::debug("AsioTransportAdapter::send_request called (NON-STREAMING) with request type: {}", req.index());
    auto conn = get_or_create_connection(opts_);
    if (!conn || !conn->alive) {
        co_return Error{ErrorCode::NetworkError, "Failed to establish connection"};
    }

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = req;
    msg.clientVersion = "yams-client-0.3.4";
    msg.expectsStreamingResponse = false;

    MessageFramer framer;
    auto framed = framer.frame_message(msg);
    if (!framed) {
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    AsioTransportAdapter::Connection::UnaryHandler uh;
    auto fut = uh.promise.get_future();
    {
        std::lock_guard<std::mutex> lk(conn->mtx);
        if (conn->handlers.size() >= conn->opts.maxInflight) {
            co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
        }
        AsioTransportAdapter::Connection::Handler h;
        h.unary = std::move(uh);
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto frame = framed.value();
    std::promise<Result<void>> _pw;
    auto _pf = _pw.get_future();
    auto& _io = GlobalIOContext::instance().get_io_context();
    co_spawn(_io,
             [conn, frame = std::move(frame), &_pw]() -> awaitable<void> {
                 auto wres = co_await conn->async_write_frame(std::move(frame));
                 _pw.set_value(std::move(wres));
             },
             detached);
    auto wres = _pf.get();
    if (!wres) {
        std::lock_guard<std::mutex> lk(conn->mtx);
        conn->handlers.erase(msg.requestId);
        co_return wres.error();
    }

    co_return fut.get();
}

Task<Result<void>> AsioTransportAdapter::send_request_streaming(const Request& req, 
                                                               HeaderCallback onHeader,
                                                               ChunkCallback onChunk, 
                                                               ErrorCallback onError,
                                                               CompleteCallback onComplete) {
    spdlog::debug("AsioTransportAdapter::send_request_streaming called (STREAMING) with request type: {}", req.index());
    auto conn = get_or_create_connection(opts_);
    if (!conn || !conn->alive) {
        co_return Error{ErrorCode::NetworkError, "Failed to establish connection"};
    }

    Message msg;
    msg.version = PROTOCOL_VERSION;
    msg.requestId = static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    msg.timestamp = std::chrono::steady_clock::now();
    msg.payload = req;
    msg.clientVersion = "yams-client-0.3.4";
    msg.expectsStreamingResponse = true;  // Streaming request

    MessageFramer framer;
    auto framed = framer.frame_message(msg);
    if (!framed) {
        co_return Error{ErrorCode::InvalidData, "Frame build failed"};
    }

    AsioTransportAdapter::Connection::StreamingHandler sh(onHeader, onChunk, onError, onComplete);
    auto fut = sh.done.get_future();
    {
        std::lock_guard<std::mutex> lk(conn->mtx);
        if (conn->handlers.size() >= conn->opts.maxInflight) {
            co_return Error{ErrorCode::ResourceExhausted, "Too many in-flight requests"};
        }
        AsioTransportAdapter::Connection::Handler h;
        h.streaming = std::move(sh);
        conn->handlers.emplace(msg.requestId, std::move(h));
    }

    auto frame = framed.value();
    std::promise<Result<void>> _pw;
    auto _pf = _pw.get_future();
    auto& _io = GlobalIOContext::instance().get_io_context();
    co_spawn(_io,
             [conn, frame = std::move(frame), &_pw]() -> awaitable<void> {
                 auto wres = co_await conn->async_write_frame(std::move(frame));
                 _pw.set_value(std::move(wres));
             },
             detached);
    auto wres = _pf.get();
    if (!wres) {
        std::lock_guard<std::mutex> lk(conn->mtx);
        conn->handlers.erase(msg.requestId);
        co_return wres.error();
    }

    co_return fut.get();
}

} // namespace yams::daemon
