#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/request_handler.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
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
#ifndef _WIN32
#include <sys/un.h>
#endif
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
#ifndef _WIN32
        // Guard against AF_UNIX sun_path length limit (~108 bytes on Linux)
        {
            std::string sp = config_.socketPath.string();
            if (sp.size() >= sizeof(sockaddr_un::sun_path)) {
                running_ = false;
                return Error{ErrorCode::InvalidArgument,
                             std::string("Socket path too long for AF_UNIX (") +
                                 std::to_string(sp.size()) + "/" +
                                 std::to_string(sizeof(sockaddr_un::sun_path)) + ") : '" + sp + "'"};
            }
        }
#endif
        // Ensure io_context is fresh and kept alive while we spin up threads
        io_context_.restart();
        work_guard_.emplace(io_context_.get_executor());

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
        
        // Start accept loop before spawning worker threads to ensure pending work exists
        co_spawn(io_context_, accept_loop(), detached);

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
    
    // Stop io_context and release work guard so threads can exit
    io_context_.stop();
    
    // Join all worker threads
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    workers_.clear();
    work_guard_.reset();
    
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
        // Create request handler for this connection with streaming support
        RequestHandler::Config handlerConfig;
        handlerConfig.enable_streaming = true;
        handlerConfig.enable_multiplexing = true; // Default: multiplex per connection
        handlerConfig.chunk_size = 64 * 1024; // 64KB chunks
        // Persistent connections; client may issue multiple requests sequentially
        handlerConfig.close_after_response = false;
        handlerConfig.graceful_half_close = true;
        handlerConfig.max_inflight_per_connection = 128;
        MuxMetricsRegistry::instance().setWriterBudget(handlerConfig.writer_budget_bytes_per_turn);
        RequestHandler handler(dispatcher_, handlerConfig);
        
        // Create a stop_source for this connection
        std::stop_source stop_source;
        
        // Delegate connection handling to RequestHandler which supports streaming
        co_await handler.handle_connection(std::move(socket), stop_source.get_token());
    } catch (const std::exception& e) {
        spdlog::debug("Connection handler error: {}", e.what());
    }
    
    // Socket will be closed when it goes out of scope
}

} // namespace yams::daemon
