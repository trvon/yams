#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/proto_serializer.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <memory>

namespace yams::daemon {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using local = boost::asio::local::stream_protocol;

SocketServer::SocketServer(const Config& config, 
                         RequestDispatcher* dispatcher,
                         StateComponent* state)
    : config_(config)
    , dispatcher_(dispatcher)
    , state_(state) {}

SocketServer::~SocketServer() {
    stop();
}

Result<void> SocketServer::start() {
    if (running_.exchange(true)) {
        return Error{ErrorCode::InvalidState, "Socket server already running"};
    }

    try {
        spdlog::info("Starting socket server on {}", config_.socketPath.string());
        
        // Remove existing socket file
        std::error_code ec;
        std::filesystem::remove(config_.socketPath, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            spdlog::warn("Failed to remove existing socket: {}", ec.message());
        }
        
        // Ensure parent directory exists
        auto parent = config_.socketPath.parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }
        
        // Create acceptor and bind
        acceptor_ = std::make_unique<local::acceptor>(io_context_);
        local::endpoint endpoint(config_.socketPath.string());
        acceptor_->open(endpoint.protocol());
        acceptor_->bind(endpoint);
        acceptor_->listen(boost::asio::socket_base::max_listen_connections);
        
        // Set socket permissions
        std::filesystem::permissions(config_.socketPath,
            std::filesystem::perms::owner_all | 
            std::filesystem::perms::group_read | 
            std::filesystem::perms::group_write);
        
        actualSocketPath_ = config_.socketPath;
        
        // Start worker threads
        for (size_t i = 0; i < config_.workerThreads; ++i) {
            workers_.emplace_back([this] { 
                try {
                    io_context_.run();
                } catch (const std::exception& e) {
                    spdlog::error("Worker thread exception: {}", e.what());
                }
            });
        }
        
        // Start accept loop
        co_spawn(io_context_, accept_loop(), detached);
        
        // Update state if available
        if (state_) {
            state_->readiness.ipcServerReady.store(true);
        }
        
        spdlog::info("Socket server listening on {}", config_.socketPath.string());
        return {};
        
    } catch (const std::exception& e) {
        running_ = false;
        return Error{ErrorCode::IOError, 
                    fmt::format("Failed to start socket server: {}", e.what())};
    }
}

Result<void> SocketServer::stop() {
    if (!running_.exchange(false)) {
        return Error{ErrorCode::InvalidState, "Socket server not running"};
    }

    spdlog::info("Stopping socket server");
    stopping_ = true;

    // Close acceptor first to stop new connections
    if (acceptor_ && acceptor_->is_open()) {
        boost::system::error_code ec;
        acceptor_->close(ec);
        if (ec) {
            spdlog::warn("Error closing acceptor: {}", ec.message());
        }
    }
    
    // Stop io_context
    io_context_.stop();
    
    // Join all worker threads
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    
    // Update state
    if (state_) {
        state_->readiness.ipcServerReady.store(false);
    }
    
    // Remove socket file
    if (!actualSocketPath_.empty()) {
        std::error_code ec;
        std::filesystem::remove(actualSocketPath_, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            spdlog::warn("Failed to remove socket file: {}", ec.message());
        }
        actualSocketPath_.clear();
    }
    
    spdlog::info("Socket server stopped");
    return {};
}

awaitable<void> SocketServer::accept_loop() {
    spdlog::debug("Accept loop started");
    
    while (running_ && !stopping_) {
        bool need_delay = false;
        
        try {
            // Check connection limit
            if (activeConnections_.load() >= config_.maxConnections) {
                // Brief delay when at capacity
                boost::asio::steady_timer timer(io_context_);
                timer.expires_after(std::chrono::milliseconds(100));
                co_await timer.async_wait(use_awaitable);
                continue;
            }
            
            auto socket = co_await acceptor_->async_accept(use_awaitable);
            
            auto current = activeConnections_.fetch_add(1) + 1;
            totalConnections_.fetch_add(1);
            
            spdlog::debug("New connection accepted, active: {}", current);
            
            // Update metrics
            if (state_) {
                state_->stats.activeConnections.store(current);
            }
            
            // Handle connection in parallel
            co_spawn(acceptor_->get_executor(),
                    handle_connection(std::move(socket)),
                    detached);
                    
        } catch (const boost::system::system_error& e) {
            if (!running_ || stopping_) break;
            
            if (e.code() == boost::asio::error::operation_aborted) {
                break;
            }
            
            spdlog::warn("Accept error: {} ({})", e.what(), e.code().message());
            need_delay = true;
        } catch (const std::exception& e) {
            if (!running_ || stopping_) break;
            spdlog::error("Unexpected accept error: {}", e.what());
            break;
        }
        
        // Delay outside of catch block if needed
        if (need_delay) {
            boost::asio::steady_timer timer(io_context_);
            timer.expires_after(config_.acceptBackoffMs);
            try {
                co_await timer.async_wait(use_awaitable);
            } catch (const boost::system::system_error&) {
                // Timer cancelled, continue
            }
            
            if (!running_ || stopping_) break;
        }
    }
    
    spdlog::debug("Accept loop ended");
}



awaitable<void> SocketServer::handle_connection(local::socket socket) {
    // Ensure cleanup on exit
    struct CleanupGuard {
        SocketServer* server;
        ~CleanupGuard() { 
            auto current = server->activeConnections_.fetch_sub(1) - 1;
            if (server->state_) {
                server->state_->stats.activeConnections.store(current);
            }
            spdlog::debug("Connection closed, active: {}", current);
        }
    } guard{this};
    
    try {
        // Create request handler for this connection
        RequestHandler handler(dispatcher_);
        
        // Set socket options
        socket.set_option(local::socket::keep_alive(true));
        
        // Connection timeout handling
        auto timeout_duration = config_.connectionTimeout;
        auto last_activity = std::chrono::steady_clock::now();
        
        // Create message framer and frame reader for this connection
        MessageFramer framer;
        FrameReader reader(16 * 1024 * 1024); // 16MB max frame size
        
        while (running_ && !stopping_) {
            // Read data in chunks
            std::array<uint8_t, 4096> buffer;
            boost::system::error_code read_ec;
            size_t bytes_read = 0;
            
            // Set up timeout
            boost::asio::steady_timer timer(socket.get_executor());
            timer.expires_after(timeout_duration);
            
            // Race between read and timeout
            bool timeout_fired = false;
            bool read_completed = false;
            
            co_spawn(socket.get_executor(),
                [&]() -> awaitable<void> {
                    try {
                        bytes_read = co_await boost::asio::async_read(
                            socket, 
                            boost::asio::buffer(buffer),
                            boost::asio::transfer_at_least(1),
                            use_awaitable);
                        read_completed = true;
                        timer.cancel();
                    } catch (const boost::system::system_error& e) {
                        read_ec = e.code();
                        read_completed = true;
                    }
                },
                detached);
            
            co_spawn(socket.get_executor(),
                [&]() -> awaitable<void> {
                    try {
                        co_await timer.async_wait(use_awaitable);
                        timeout_fired = true;
                        socket.cancel();
                    } catch (...) {
                        // Timer was cancelled
                    }
                },
                detached);
            
            // Wait for either to complete
            while (!read_completed && !timeout_fired) {
                boost::asio::steady_timer poll_timer(socket.get_executor());
                poll_timer.expires_after(std::chrono::milliseconds(10));
                co_await poll_timer.async_wait(use_awaitable);
            }
            
            if (timeout_fired || read_ec == boost::asio::error::operation_aborted) {
                spdlog::debug("Connection timeout");
                break;
            }
            
            if (read_ec) {
                if (read_ec == boost::asio::error::eof) {
                    spdlog::debug("Client disconnected (EOF)");
                } else {
                    spdlog::debug("Read error: {}", read_ec.message());
                }
                break;
            }
            
            if (bytes_read == 0) {
                spdlog::debug("Client disconnected (0 bytes)");
                break;
            }
            
            // Feed data to frame reader
            spdlog::debug("Received {} bytes from client", bytes_read);
            // Log first few bytes for debugging
            if (bytes_read >= 20) {
                spdlog::debug("Frame header bytes: {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x} {:02x}",
                    buffer[0], buffer[1], buffer[2], buffer[3], 
                    buffer[4], buffer[5], buffer[6], buffer[7]);
            }
            
            auto feed_result = reader.feed(buffer.data(), bytes_read);
            
            if (feed_result.status == FrameReader::FrameStatus::InvalidFrame) {
                spdlog::warn("Invalid frame received");
                // Log more details about the invalid frame
                if (bytes_read >= 8) {
                    uint32_t magic = 0;
                    std::memcpy(&magic, buffer.data(), sizeof(magic));
                    magic = ntohl(magic);
                    spdlog::warn("Expected magic: 0x{:08x}, got: 0x{:08x}", MessageFramer::MAGIC, magic);
                }
                break;
            }
            
            if (feed_result.status == FrameReader::FrameStatus::FrameTooLarge) {
                spdlog::warn("Frame too large");
                break;
            }
            
            // Process any complete frames
            while (reader.has_frame()) {
                auto frame_result = reader.get_frame();
                if (!frame_result) {
                    spdlog::error("Failed to get frame: {}", frame_result.error().message);
                    break;
                }
                
                // Parse the frame
                spdlog::debug("Got complete frame of {} bytes", frame_result.value().size());
                auto message_result = framer.parse_frame(frame_result.value());
                if (!message_result) {
                    spdlog::error("Failed to parse frame: {}", message_result.error().message);
                    
                    // Send error response
                    ErrorResponse error;
                    error.code = message_result.error().code;
                    error.message = message_result.error().message;
                    Message errorMsg;
                    errorMsg.requestId = 0; // No request ID available
                    errorMsg.payload = Response(error);
                    
                    auto error_frame = framer.frame_message(errorMsg);
                    if (error_frame) {
                        try {
                            co_await boost::asio::async_write(
                                socket,
                                boost::asio::buffer(error_frame.value()),
                                use_awaitable);
                        } catch (...) {
                            // Ignore write errors
                        }
                    }
                    break;
                }
                
                // Extract request from message
                const auto& message = message_result.value();
                auto* request_ptr = std::get_if<Request>(&message.payload);
                if (!request_ptr) {
                    spdlog::error("Received non-request message");
                    continue;
                }
            
                // Update activity timestamp
                last_activity = std::chrono::steady_clock::now();
                
                // Process request
                spdlog::debug("Processing request type: {}, id: {}", 
                    getRequestName(*request_ptr), message.requestId);
                
                Response response = dispatcher_->dispatch(*request_ptr);
                
                // Create response message
                Message responseMsg;
                responseMsg.requestId = message.requestId;
                responseMsg.payload = response;
                
                // Frame and send response
                auto response_frame = framer.frame_message(responseMsg);
                if (!response_frame) {
                    spdlog::error("Failed to frame response: {}", response_frame.error().message);
                    break;
                }
                
                try {
                    co_await boost::asio::async_write(
                        socket,
                        boost::asio::buffer(response_frame.value()),
                        use_awaitable);
                } catch (const boost::system::system_error& e) {
                    spdlog::debug("Failed to write response: {}", e.what());
                    break;
                }
                

            }
            
            // Update metrics
            if (state_) {
                state_->stats.requestsProcessed.fetch_add(1);
            }
        }
    } catch (const std::exception& e) {
        spdlog::debug("Connection handler error: {}", e.what());
    }
    
    // Socket will be closed when it goes out of scope
}

} // namespace yams::daemon