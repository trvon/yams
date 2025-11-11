#include <yams/daemon/components/PoolManager.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningSnapshot.h>
#include <yams/daemon/ipc/message_framing.h>
#include <yams/daemon/ipc/mux_metrics_registry.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_handler.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

#ifdef __linux__
#include <sys/prctl.h>
#endif

namespace {
void set_current_thread_name(const std::string& name) {
#ifdef __linux__
    prctl(PR_SET_NAME, name.c_str(), 0, 0, 0);
#elif __APPLE__
    pthread_setname_np(name.c_str());
#endif
}

bool stream_trace_enabled() {
    static int enabled = [] {
        if (const char* raw = std::getenv("YAMS_STREAM_TRACE")) {
            std::string v(raw);
            for (auto& ch : v)
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            if (v == "1" || v == "true" || v == "on")
                return 1;
        }
        return 0;
    }();
    return enabled != 0;
}

// Diagnostic thread removed - simplified architecture with fixed worker pool
} // namespace

#include <spdlog/spdlog.h>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>

#include <atomic>
#include <cctype>
#include <cstdlib>
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

SocketServer::SocketServer(const Config& config, RequestDispatcher* dispatcher,
                           StateComponent* state)
    : config_(config), dispatcher_(dispatcher), state_(state) {}

SocketServer::~SocketServer() {
    stop();
}

Result<void> SocketServer::start() {
    if (running_.exchange(true)) {
        return Error{ErrorCode::InvalidState, "Socket server already running"};
    }

    stopping_.store(false, std::memory_order_relaxed);

    try {
        spdlog::info("Starting socket server on {}", config_.socketPath.string());

        if (config_.workerThreads == 0) {
            config_.workerThreads = 1;
            spdlog::warn("SocketServer: workerThreads was 0; coercing to 1");
        }

        // Normalize to an absolute path to avoid surprises after daemon chdir("/")
        std::filesystem::path sockPath = config_.socketPath;
        if (!sockPath.is_absolute()) {
            try {
                sockPath = std::filesystem::absolute(sockPath);
            } catch (...) {
                // fallback: keep original
            }
        }

        std::error_code ec;
        std::filesystem::remove(sockPath, ec);
        if (ec && ec != std::errc::no_such_file_or_directory) {
            spdlog::warn("Failed to remove existing socket: {}", ec.message());
        }

        auto parent = sockPath.parent_path();
        if (!parent.empty() && !std::filesystem::exists(parent)) {
            std::filesystem::create_directories(parent);
        }

#ifndef _WIN32
        {
            std::string sp = sockPath.string();
            if (sp.size() >= sizeof(sockaddr_un::sun_path)) {
                running_ = false;
                return Error{
                    ErrorCode::InvalidArgument,
                    std::string("Socket path too long for AF_UNIX (") + std::to_string(sp.size()) +
                        "/" + std::to_string(sizeof(sockaddr_un::sun_path)) + ") : '" + sp + "'"};
            }
        }
#endif
        io_context_.restart();
        work_guard_.emplace(io_context_.get_executor());

        acceptor_ = std::make_unique<local::acceptor>(io_context_);
        local::endpoint endpoint(sockPath.string());
        acceptor_->open(endpoint.protocol());
        acceptor_->bind(endpoint);
        acceptor_->listen(boost::asio::socket_base::max_listen_connections);

        std::filesystem::permissions(sockPath, std::filesystem::perms::owner_all |
                                                   std::filesystem::perms::group_read |
                                                   std::filesystem::perms::group_write);

        actualSocketPath_ = sockPath;

        // Initialize bounded concurrency semaphore (PBI-066-42)
        // Use maxConnections as the limit for natural backpressure
        connectionSlots_ = std::make_unique<std::counting_semaphore<>>(config_.maxConnections);
        spdlog::info("SocketServer: bounded concurrency enabled (max {} slots)",
                     config_.maxConnections);

        // Seed writer budget prior to first connection
        std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = 256ULL * 1024;
        setWriterBudget(initialBudget);

        co_spawn(
            io_context_,
            [this]() -> awaitable<void> {
                co_await accept_loop();
                co_return;
            },
            detached);
        spdlog::info("SocketServer: accept_loop scheduled");

        // Get tuneable worker count from TuneAdvisor
        // Use poolMaxSizeIpc() for fixed pool sized for peak load (deterministic, no runtime
        // scaling) Override via YAMS_POOL_IPC_MAX env var
        try {
            auto rec = TuneAdvisor::poolMaxSizeIpc();
            if (rec > 0) {
                config_.workerThreads = rec;
            }
        } catch (...) {
        }

        // Worker pool using poll() pattern for responsive shutdown
        workers_.reserve(config_.workerThreads);
        try {
            for (size_t i = 0; i < config_.workerThreads; ++i) {
                workers_.emplace_back([this, i]() {
                    set_current_thread_name("yams-ipc-" + std::to_string(i));
                    spdlog::info("SocketServer: worker {} starting (poll loop)", i);
                    try {
                        // Poll loop - responsive shutdown unlike blocking run()
                        while (!io_context_.stopped() && running_.load(std::memory_order_relaxed)) {
                            std::size_t count = io_context_.poll();
                            if (count == 0) {
                                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                            }
                        }
                    } catch (const std::exception& e) {
                        spdlog::error("SocketServer: worker {} exception: {}", i, e.what());
                    }
                    spdlog::info("SocketServer: worker {} exiting", i);
                });
            }
        } catch (...) {
            // If thread creation fails, clean up already-created threads before rethrowing
            spdlog::error("Failed to create worker thread, cleaning up {} existing workers",
                          workers_.size());
            running_ = false;
            io_context_.stop();
            for (auto& worker : workers_) {
                if (worker.joinable()) {
                    try {
                        worker.join();
                    } catch (...) {
                    }
                }
            }
            workers_.clear();
            throw; // Rethrow to propagate error
        }

        if (state_) {
            state_->readiness.ipcServerReady.store(true);
            try {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - state_->stats.startTime)
                              .count();
                state_->initDurationsMs.emplace("ipc_server", static_cast<uint64_t>(ms));
            } catch (...) {
            }
        }

        spdlog::info("Socket server listening on {}", sockPath.string());
        return {};

    } catch (const std::exception& e) {
        running_ = false;
        spdlog::error("SocketServer::start exception: {}", e.what());
        return Error{ErrorCode::IOError,
                     fmt::format("Failed to start socket server: {}", e.what())};
    }
}

Result<void> SocketServer::stop() {
    try {
        if (!running_.exchange(false)) {
            return Error{ErrorCode::InvalidState, "Socket server not running"};
        }

        spdlog::info("Stopping socket server");
        stopping_.store(true, std::memory_order_relaxed);

        // Request stop on all active connections via stop_source
        try {
            stop_source_.request_stop();
        } catch (...) {
        }

        // Close all active sockets IMMEDIATELY for deterministic shutdown
        try {
            std::vector<std::shared_ptr<boost::asio::local::stream_protocol::socket>> sockets;
            {
                std::lock_guard<std::mutex> lk(activeSocketsMutex_);
                for (auto& weak_sock : activeSockets_) {
                    if (auto sock = weak_sock.lock()) {
                        sockets.push_back(sock);
                    }
                }
                activeSockets_.clear();
            }
            for (auto& sock : sockets) {
                if (sock && sock->is_open()) {
                    boost::system::error_code ec;
                    sock->close(ec);
                }
            }
            spdlog::info("Closed {} active connections", sockets.size());
        } catch (const std::exception& e) {
            spdlog::warn("Exception while closing active sockets: {}", e.what());
        } catch (...) {
            spdlog::warn("Unknown exception while closing active sockets");
        }

        // Wait for connection handlers to complete with timeout (PBI-066-41)
        try {
            std::vector<std::future<void>> futures;
            {
                std::lock_guard<std::mutex> lk(connectionFuturesMutex_);
                futures = std::move(connectionFutures_);
                connectionFutures_.clear();
            }

            if (!futures.empty()) {
                spdlog::info("SocketServer: waiting for {} connection handlers to complete",
                             futures.size());

                const auto timeout = std::chrono::seconds(2);
                const auto deadline = std::chrono::steady_clock::now() + timeout;
                size_t completed = 0;

                for (auto& fut : futures) {
                    auto remaining = deadline - std::chrono::steady_clock::now();
                    if (remaining <= std::chrono::seconds(0)) {
                        spdlog::warn(
                            "SocketServer: timeout waiting for connections ({}/{} completed)",
                            completed, futures.size());
                        break;
                    }

                    if (fut.wait_for(remaining) == std::future_status::ready) {
                        try {
                            fut.get(); // Get result, may throw
                            ++completed;
                        } catch (const std::exception& e) {
                            spdlog::warn("Connection handler exception: {}", e.what());
                            ++completed;
                        }
                    }
                }

                spdlog::info("SocketServer: {}/{} connection handlers completed gracefully",
                             completed, futures.size());
            }
        } catch (const std::exception& e) {
            spdlog::warn("Exception waiting for connection futures: {}", e.what());
        }

        // Close acceptor
        try {
            if (acceptor_ && acceptor_->is_open()) {
                boost::system::error_code ec;
                acceptor_->close(ec);
            }
        } catch (...) {
        }

        // Stop io_context to signal workers to exit
        spdlog::info("SocketServer: stopping io_context");
        try {
            work_guard_.reset();
            io_context_.stop();
        } catch (...) {
        }

        // Join worker threads (they check io_context_.stopped() in poll loop)
        spdlog::info("SocketServer: joining {} worker threads", workers_.size());
        for (size_t i = 0; i < workers_.size(); ++i) {
            if (workers_[i].joinable()) {
                try {
                    workers_[i].join();
                } catch (const std::system_error& e) {
                    spdlog::warn("SocketServer: worker {} join failed: {}", i, e.what());
                }
            }
        }
        workers_.clear();
        spdlog::info("SocketServer: all workers stopped");

        if (state_) {
            state_->readiness.ipcServerReady.store(false);
        }

        // Cleanup socket file
        if (!actualSocketPath_.empty()) {
            std::error_code ec;
            std::filesystem::remove(actualSocketPath_, ec);
            actualSocketPath_.clear();
        }

        spdlog::info("Socket server stopped (total_conn={} active_conn={})",
                     totalConnections_.load(std::memory_order_relaxed),
                     activeConnections_.load(std::memory_order_relaxed));
    } catch (const std::exception& e) {
        spdlog::error("SocketServer::stop unhandled exception: {}", e.what());
    } catch (...) {
        spdlog::error("SocketServer::stop unknown exception");
    }
    stopping_.store(false, std::memory_order_relaxed);
    return {};
}

void SocketServer::setWriterBudget(std::size_t bytes) {
    if (bytes == 0)
        bytes = TuneAdvisor::writerBudgetBytesPerTurn();
    if (bytes == 0)
        bytes = 256ULL * 1024;
    if (!writerBudget_)
        writerBudget_ = std::make_shared<std::atomic<std::size_t>>(bytes);
    else
        writerBudget_->store(bytes, std::memory_order_relaxed);
    MuxMetricsRegistry::instance().setWriterBudget(bytes);
}

awaitable<void> SocketServer::accept_loop() {
    static const bool trace = stream_trace_enabled();
    static bool trace_env_logged = false;
    static bool logged_entry = false;
    spdlog::debug("Accept loop started");
    if (!logged_entry) {
        logged_entry = true;
        spdlog::info("SocketServer: accept_loop coroutine entered");
    }
    if (!trace && !trace_env_logged) {
        trace_env_logged = true;
        if (const char* raw = std::getenv("YAMS_STREAM_TRACE")) {
            spdlog::info("stream-trace: accept_loop env present but disabled ('{}')", raw);
        } else {
            spdlog::info("stream-trace: accept_loop env not set");
        }
    }
    if (trace) {
        spdlog::info("stream-trace: accept_loop starting (max_conn={} worker_threads={} socket={})",
                     config_.maxConnections, config_.workerThreads,
                     actualSocketPath_.empty() ? config_.socketPath.string()
                                               : actualSocketPath_.string());
    }

    while (running_ && !stopping_) {
#if defined(TRACY_ENABLE)
        ZoneScopedN("SocketServer::accept_loop");
#endif

        try {
            // Acquire slot - blocks naturally when at capacity (PBI-066-42)
            // This replaces ~70 lines of manual backpressure logic
            if (connectionSlots_) {
                connectionSlots_->acquire();
                if (trace) {
                    spdlog::debug("stream-trace: acquired connection slot");
                }
            }

            // Check if we're shutting down after acquiring slot
            if (!running_ || stopping_) {
                if (connectionSlots_) {
                    connectionSlots_->release(); // Release slot before exit
                }
                break;
            }

            if (trace) {
                spdlog::info("stream-trace: waiting for accept (active={} total={})",
                             activeConnections_.load(std::memory_order_relaxed),
                             totalConnections_.load(std::memory_order_relaxed));
            }

            // Use as_tuple to avoid exception overhead during shutdown
            auto [ec, socket] =
                co_await acceptor_->async_accept(boost::asio::as_tuple(use_awaitable));

            // Handle errors without exception
            if (ec) {
                // Release semaphore on error paths
                if (connectionSlots_) {
                    connectionSlots_->release();
                }

                if (!running_ || stopping_) {
                    break; // Clean shutdown
                }

                if (ec == boost::asio::error::operation_aborted) {
                    break; // Acceptor closed
                }

                // Handle platform-specific errors
                static int einval_streak = 0;
                bool need_delay = false;
                auto backoff_ms = config_.acceptBackoffMs;

#if defined(__APPLE__)
                if (ec.value() == EINVAL) {
                    ++einval_streak;
                    spdlog::debug("Accept error (EINVAL): {} (streak={})", ec.message(),
                                  einval_streak);
                    if (einval_streak >= 3) {
                        try {
                            boost::system::error_code rebuild_ec;
                            if (acceptor_)
                                acceptor_->close(rebuild_ec);
                            auto rebuildPath = actualSocketPath_;
                            if (rebuildPath.empty()) {
                                rebuildPath = config_.socketPath;
                            }
                            std::filesystem::remove(rebuildPath, rebuild_ec);
                            acceptor_ = std::make_unique<local::acceptor>(io_context_);
                            local::endpoint endpoint(rebuildPath.string());
                            acceptor_->open(endpoint.protocol());
                            acceptor_->bind(endpoint);
                            acceptor_->listen(boost::asio::socket_base::max_listen_connections);
                            actualSocketPath_ = rebuildPath;
                            static std::atomic<bool> s_warned_once{false};
                            static const bool s_quiet = []() {
                                if (const char* v = std::getenv("YAMS_QUIET_EINVAL_REBUILD")) {
                                    return *v != '\0' && std::string(v) != "0" &&
                                           strcasecmp(v, "false") != 0;
                                }
                                return true;
                            }();
                            if (!s_quiet && !s_warned_once.exchange(true)) {
                                spdlog::warn("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                             config_.socketPath.string());
                            } else {
                                spdlog::debug("Rebuilt IPC acceptor after repeated EINVAL on {}",
                                              config_.socketPath.string());
                            }
                            if (state_) {
                                state_->stats.ipcEinvalRebuilds.fetch_add(
                                    1, std::memory_order_relaxed);
                            }
                            einval_streak = 0;
                        } catch (const std::exception& re) {
                            spdlog::error("Failed to rebuild IPC acceptor: {}", re.what());
                        }
                    }
                    need_delay = true;
                    backoff_ms = std::chrono::milliseconds(100);
                } else
#endif
                {
                    einval_streak = 0;
                    spdlog::warn("Accept error: {} ({})", ec.message(), ec.value());
                    need_delay = true;
                }

                if (need_delay) {
                    boost::asio::steady_timer timer(io_context_);
                    timer.expires_after(backoff_ms);
                    try {
                        co_await timer.async_wait(use_awaitable);
                    } catch (const boost::system::system_error&) {
                    }
                    if (!running_ || stopping_)
                        break;
                }
                continue; // Retry accept
            }

            if (trace) {
                spdlog::info("stream-trace: accept completed (active={} total={})",
                             activeConnections_.load(std::memory_order_relaxed),
                             totalConnections_.load(std::memory_order_relaxed));
            }

            auto current = activeConnections_.fetch_add(1) + 1;
            totalConnections_.fetch_add(1);

            spdlog::info("SocketServer: accepted connection, active={} total={}", current,
                         totalConnections_.load());

            if (state_) {
                state_->stats.activeConnections.store(current);
            }

            // Spawn connection with use_future for graceful shutdown tracking (PBI-066-41)
            // Pass semaphore for automatic release when connection completes (PBI-066-42)
            {
                std::lock_guard<std::mutex> lk(connectionFuturesMutex_);

                // Prune completed futures before adding new one
                prune_completed_futures();

                // Create a capturing lambda that releases the semaphore on completion
                auto wrapped_handler = [this,
                                        socket = std::move(socket)]() mutable -> awaitable<void> {
                    // RAII guard for semaphore - releases on any exit path
                    struct SemaphoreGuard {
                        std::counting_semaphore<>* sem;
                        ~SemaphoreGuard() {
                            if (sem) {
                                sem->release();
                            }
                        }
                    } guard{connectionSlots_.get()};

                    // Handle the connection
                    co_await handle_connection(std::move(socket));
                };

                // Spawn and track the connection
                connectionFutures_.push_back(co_spawn(acceptor_->get_executor(), wrapped_handler(),
                                                      boost::asio::use_future));
            }

        } catch (const std::exception& e) {
            if (!running_ || stopping_)
                break;

            // Release semaphore on unexpected exception
            if (connectionSlots_) {
                connectionSlots_->release();
            }

            spdlog::error("Unexpected error in accept loop: {}", e.what());
            break;
        }
    }

    spdlog::debug("Accept loop ended");
}

awaitable<void> SocketServer::handle_connection(local::socket socket) {
#if defined(TRACY_ENABLE)
    ZoneScopedN("SocketServer::handle_connection");
#endif
    static const bool trace = stream_trace_enabled();
    struct CleanupGuard {
        SocketServer* server;
        bool trace;
        ~CleanupGuard() {
            auto current = server->activeConnections_.fetch_sub(1) - 1;
            if (server->state_) {
                server->state_->stats.activeConnections.store(current);
            }
            if (trace) {
                spdlog::info("stream-trace: handle_connection cleanup active={} total={}", current,
                             server->totalConnections_.load());
            } else {
                spdlog::debug("Connection closed, active: {}", current);
            }
        }
    } guard{this, trace};

    if (trace) {
        spdlog::info("stream-trace: handle_connection begin active={} total={} socket_valid={}",
                     activeConnections_.load(std::memory_order_relaxed),
                     totalConnections_.load(std::memory_order_relaxed), socket.is_open());
    }

    try {
        if (!writerBudget_) {
            std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
            if (initialBudget == 0)
                initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
            if (initialBudget == 0)
                initialBudget = 256ULL * 1024;
            writerBudget_ = std::make_shared<std::atomic<std::size_t>>(initialBudget);
        }

        RequestHandler::Config handlerConfig;
        handlerConfig.worker_executor = io_context_.get_executor();
        handlerConfig.writer_budget_ref = writerBudget_;
        handlerConfig.writer_budget_bytes_per_turn = writerBudget_->load(std::memory_order_relaxed);
        handlerConfig.enable_streaming = true;
        handlerConfig.enable_multiplexing = true;
        if (dispatcher_) {
            try {
                handlerConfig.worker_executor = dispatcher_->getWorkerExecutor();
                handlerConfig.worker_job_signal = dispatcher_->getWorkerJobSignal();
            } catch (...) {
            }
        }
        handlerConfig.chunk_size = TuneAdvisor::chunkSize();
        handlerConfig.close_after_response = false;
        handlerConfig.graceful_half_close = true;
        auto connectionTimeout = config_.connectionTimeout;
        if (connectionTimeout.count() == 0) {
            // C++ IPC should be fast: 2s read timeout is generous for local sockets
            // This prevents hanging connections during shutdown and keeps everything sub-second
            connectionTimeout = std::chrono::milliseconds(2000);
        }
        auto timeoutSeconds = std::chrono::duration_cast<std::chrono::seconds>(connectionTimeout);
        if (timeoutSeconds.count() == 0 && connectionTimeout.count() > 0) {
            timeoutSeconds = std::chrono::seconds(1);
        }
        handlerConfig.read_timeout = timeoutSeconds;
        handlerConfig.write_timeout = timeoutSeconds;
        handlerConfig.max_inflight_per_connection = TuneAdvisor::serverMaxInflightPerConn();
        if (handlerConfig.writer_budget_bytes_per_turn == 0) {
            handlerConfig.writer_budget_bytes_per_turn =
                TuneAdvisor::serverWriterBudgetBytesPerTurn();
            if (handlerConfig.writer_budget_bytes_per_turn == 0)
                handlerConfig.writer_budget_bytes_per_turn =
                    TuneAdvisor::writerBudgetBytesPerTurn();
            if (handlerConfig.writer_budget_bytes_per_turn == 0)
                handlerConfig.writer_budget_bytes_per_turn = 256ULL * 1024;
            if (writerBudget_)
                writerBudget_->store(handlerConfig.writer_budget_bytes_per_turn,
                                     std::memory_order_relaxed);
        }
        MuxMetricsRegistry::instance().setWriterBudget(handlerConfig.writer_budget_bytes_per_turn);
        RequestDispatcher* disp = nullptr;
        {
            std::lock_guard<std::mutex> lk(dispatcherMutex_);
            disp = dispatcher_;
        }
        try {
            if (disp) {
                handlerConfig.worker_executor = disp->getWorkerExecutor();
                handlerConfig.worker_job_signal = disp->getWorkerJobSignal();
            }
        } catch (...) {
        }
        RequestHandler handler(disp, handlerConfig);

        // Wrap socket in shared_ptr for tracking and use modern C++20 move semantics
        auto sock =
            std::make_shared<boost::asio::local::stream_protocol::socket>(std::move(socket));

        // Register for deterministic shutdown
        register_socket(sock);

        // Use the server's stop_source token so we can cancel connections during shutdown
        auto token = stop_source_.get_token();

        co_await handler.handle_connection(std::move(*sock), token);
    } catch (const std::exception& e) {
        spdlog::error("SocketServer::handle_connection error: {}", e.what());
    }
}

void SocketServer::register_socket(
    std::weak_ptr<boost::asio::local::stream_protocol::socket> socket) {
    std::lock_guard<std::mutex> lk(activeSocketsMutex_);
    // Clean up expired weak_ptrs while we're here
    activeSockets_.erase(std::remove_if(activeSockets_.begin(), activeSockets_.end(),
                                        [](const auto& weak) { return weak.expired(); }),
                         activeSockets_.end());
    activeSockets_.push_back(std::move(socket));
}

void SocketServer::prune_completed_futures() {
    // Caller must hold connectionFuturesMutex_
    // Remove futures that are ready (completed)
    connectionFutures_.erase(std::remove_if(connectionFutures_.begin(), connectionFutures_.end(),
                                            [](std::future<void>& f) {
                                                return f.wait_for(std::chrono::seconds(0)) ==
                                                       std::future_status::ready;
                                            }),
                             connectionFutures_.end());
}

} // namespace yams::daemon
