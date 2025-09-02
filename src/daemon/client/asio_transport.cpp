#include <yams/daemon/client/asio_transport.h>
#include <yams/daemon/client/global_io_context.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

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

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
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

awaitable<Result<std::unique_ptr<boost::asio::local::stream_protocol::socket>>>
AsioTransportAdapter::async_connect_with_timeout(const std::filesystem::path& path, 
                                                std::chrono::milliseconds timeout) {
    auto& io_ctx = GlobalIOContext::instance().get_io_context();
    auto socket = std::make_unique<boost::asio::local::stream_protocol::socket>(io_ctx);
    
    boost::asio::local::stream_protocol::endpoint endpoint(path.string());
    
    try {
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

Task<Result<Response>> AsioTransportAdapter::send_request(const Request& req) {
    // Convert boost::asio::awaitable to Task using co_spawn
    auto& io_ctx = GlobalIOContext::instance().get_io_context();
    
    std::promise<Result<Response>> promise;
    auto future = promise.get_future();
    
    co_spawn(io_ctx,
        [this, req, promise = std::move(promise)]() mutable -> awaitable<void> {
            fsm_.on_connect(-1); // Placeholder fd

            // Connect with timeout
            auto socket_result = co_await async_connect_with_timeout(opts_.socketPath, 
                                                                   opts_.requestTimeout);
            if (!socket_result) {
                fsm_.on_error(static_cast<int>(socket_result.error().code));
                promise.set_value(socket_result.error());
                co_return;
            }
            
            auto& socket = *socket_result.value();
            int fd = socket.native_handle();
            fsm_.on_connect(fd);

            Message msg;
            msg.version = PROTOCOL_VERSION;
            msg.requestId =
                static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            msg.timestamp = std::chrono::steady_clock::now();
            msg.payload = req;
            msg.clientVersion = "yams-client-0.3.4";

            MessageFramer framer;
            auto framed = framer.frame_message(msg);
            if (!framed) {
                fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                promise.set_value(framed.error());
                co_return;
            }

            fsm_.on_writable(0); // Transition to writing
            auto wres = co_await async_write_all(socket, framed.value(), opts_.bodyTimeout);
            if (!wres) {
                fsm_.on_error(static_cast<int>(wres.error().code));
                promise.set_value(wres.error());
                co_return;
            }
            FsmMetricsRegistry::instance().incrementPayloadWrites(1);
            FsmMetricsRegistry::instance().addBytesSent(framed.value().size());

            fsm_.on_readable(0); // Transition to reading
            auto header_result = co_await async_read_exact(socket, 
                                                          sizeof(MessageFramer::FrameHeader),
                                                          opts_.headerTimeout);
            if (!header_result) {
                fsm_.on_error(static_cast<int>(header_result.error().code));
                promise.set_value(header_result.error());
                co_return;
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
                fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                promise.set_value(Error{ErrorCode::InvalidData, "Payload too large"});
                co_return;
            }

            auto payload_result = co_await async_read_exact(socket, header.payload_size, 
                                                           opts_.bodyTimeout);
            if (!payload_result) {
                fsm_.on_error(static_cast<int>(payload_result.error().code));
                promise.set_value(payload_result.error());
                co_return;
            }
            FsmMetricsRegistry::instance().incrementPayloadReads(1);
            FsmMetricsRegistry::instance().addBytesReceived(payload_result.value().size());

            // Reconstruct complete frame with header + payload
            std::vector<uint8_t> complete_frame;
            complete_frame.reserve(sizeof(MessageFramer::FrameHeader) + payload_result.value().size());
            complete_frame.insert(complete_frame.end(), header_result.value().begin(), header_result.value().end());
            complete_frame.insert(complete_frame.end(), payload_result.value().begin(), payload_result.value().end());

            auto respMsg = framer.parse_frame(complete_frame);
            if (!respMsg) {
                fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                promise.set_value(respMsg.error());
                co_return;
            }

            if (!std::holds_alternative<Response>(respMsg.value().payload)) {
                fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                promise.set_value(Error{ErrorCode::InvalidData, "Expected Response but got something else"});
                co_return;
            }

            fsm_.on_close_request();
            promise.set_value(std::get<Response>(respMsg.value().payload));
        },
        detached
    );
    
    co_return future.get();
}

Task<Result<void>> AsioTransportAdapter::send_request_streaming(const Request& req, 
                                                               HeaderCallback onHeader,
                                                               ChunkCallback onChunk, 
                                                               ErrorCallback onError,
                                                               CompleteCallback onComplete) {
    auto& io_ctx = GlobalIOContext::instance().get_io_context();
    
    std::promise<Result<void>> promise;
    auto future = promise.get_future();
    
    co_spawn(io_ctx,
        [this, req, onHeader, onChunk, onError, onComplete, 
         promise = std::move(promise)]() mutable -> awaitable<void> {
            fsm_.on_connect(-1); // Placeholder fd

            // Connect with timeout
            auto socket_result = co_await async_connect_with_timeout(opts_.socketPath, 
                                                                   opts_.requestTimeout);
            if (!socket_result) {
                fsm_.on_error(static_cast<int>(socket_result.error().code));
                onError(socket_result.error());
                promise.set_value(socket_result.error());
                co_return;
            }
            
            auto& socket = *socket_result.value();
            int fd = socket.native_handle();
            fsm_.on_connect(fd);

            Message msg;
            msg.version = PROTOCOL_VERSION;
            msg.requestId =
                static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
            msg.timestamp = std::chrono::steady_clock::now();
            msg.payload = req;
            msg.clientVersion = "yams-client-0.3.4";

            MessageFramer framer;
            auto framed = framer.frame_message(msg);
            if (!framed) {
                fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                onError(framed.error());
                promise.set_value(framed.error());
                co_return;
            }

            fsm_.on_writable(0); // Transition to writing
            auto wres = co_await async_write_all(socket, framed.value(), opts_.bodyTimeout);
            if (!wres) {
                fsm_.on_error(static_cast<int>(wres.error().code));
                onError(wres.error());
                promise.set_value(wres.error());
                co_return;
            }
            FsmMetricsRegistry::instance().incrementPayloadWrites(1);
            FsmMetricsRegistry::instance().addBytesSent(framed.value().size());

            fsm_.on_readable(0); // Transition to reading
            
            // Read streaming response frames
            bool expectingMore = true;
            bool headerReceived = false;
            
            while (expectingMore) {
                // Read frame header
                auto header_result = co_await async_read_exact(socket, 
                                                              sizeof(MessageFramer::FrameHeader),
                                                              opts_.headerTimeout);
                if (!header_result) {
                    fsm_.on_error(static_cast<int>(header_result.error().code));
                    onError(header_result.error());
                    promise.set_value(header_result.error());
                    co_return;
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
                    promise.set_value(err);
                    co_return;
                }

                // Check frame type
                bool isHeaderOnly = header.is_header_only();
                bool isLastChunk = header.is_last_chunk();
                
                if (isHeaderOnly && !headerReceived) {
                    // This is the initial header frame
                    headerReceived = true;
                    
                    if (header.payload_size > 0) {
                        auto payload_result = co_await async_read_exact(socket, header.payload_size,
                                                                       opts_.bodyTimeout);
                        if (!payload_result) {
                            fsm_.on_error(static_cast<int>(payload_result.error().code));
                            onError(payload_result.error());
                            promise.set_value(payload_result.error());
                            co_return;
                        }
                        FsmMetricsRegistry::instance().incrementPayloadReads(1);
                        FsmMetricsRegistry::instance().addBytesReceived(payload_result.value().size());

                        // Reconstruct complete frame with header + payload
                        std::vector<uint8_t> complete_frame;
                        complete_frame.reserve(sizeof(MessageFramer::FrameHeader) + payload_result.value().size());
                        complete_frame.insert(complete_frame.end(), header_result.value().begin(), header_result.value().end());
                        complete_frame.insert(complete_frame.end(), payload_result.value().begin(), payload_result.value().end());

                        auto respMsg = framer.parse_frame(complete_frame);
                        if (!respMsg || !std::holds_alternative<Response>(respMsg.value().payload)) {
                            auto err = Error{ErrorCode::InvalidData, "Invalid header response"};
                            fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                            onError(err);
                            promise.set_value(err);
                            co_return;
                        }
                        
                        onHeader(std::get<Response>(respMsg.value().payload));
                    }
                } else if (header.payload_size > 0) {
                    // This is a chunk frame
                    auto payload_result = co_await async_read_exact(socket, header.payload_size,
                                                                   opts_.bodyTimeout);
                    if (!payload_result) {
                        fsm_.on_error(static_cast<int>(payload_result.error().code));
                        onError(payload_result.error());
                        promise.set_value(payload_result.error());
                        co_return;
                    }
                    FsmMetricsRegistry::instance().incrementPayloadReads(1);
                    FsmMetricsRegistry::instance().addBytesReceived(payload_result.value().size());

                    // Reconstruct complete frame with header + payload
                    std::vector<uint8_t> complete_frame;
                    complete_frame.reserve(sizeof(MessageFramer::FrameHeader) + payload_result.value().size());
                    complete_frame.insert(complete_frame.end(), header_result.value().begin(), header_result.value().end());
                    complete_frame.insert(complete_frame.end(), payload_result.value().begin(), payload_result.value().end());

                    auto respMsg = framer.parse_frame(complete_frame);
                    if (!respMsg || !std::holds_alternative<Response>(respMsg.value().payload)) {
                        auto err = Error{ErrorCode::InvalidData, "Invalid chunk response"};
                        fsm_.on_error(static_cast<int>(ErrorCode::InvalidData));
                        onError(err);
                        promise.set_value(err);
                        co_return;
                    }
                    
                    bool continueReading = onChunk(std::get<Response>(respMsg.value().payload), 
                                                  isLastChunk);
                    if (!continueReading) {
                        // Client wants to stop reading
                        expectingMore = false;
                    }
                }
                
                if (isLastChunk) {
                    expectingMore = false;
                }
            }
            
            fsm_.on_close_request();
            onComplete();
            promise.set_value(Result<void>{});
        },
        detached
    );
    
    co_return future.get();
}

} // namespace yams::daemon