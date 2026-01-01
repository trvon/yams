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
#include <boost/asio/associated_executor.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/asio/write.hpp>

#include <atomic>
#include <cctype>
#include <cstddef>
#include <cstdlib>

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

SocketServer::SocketServer(const Config& config, WorkCoordinator* coordinator,
                           RequestDispatcher* dispatcher, StateComponent* state)
    : config_(config), coordinator_(coordinator), dispatcher_(dispatcher), state_(state) {}

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
        if (!coordinator_ || !coordinator_->isRunning()) {
            running_ = false;
            return Error{ErrorCode::InvalidState,
                         "WorkCoordinator must be started before SocketServer"};
        }

        auto io_context = coordinator_->getIOContext();
        acceptor_ = std::make_unique<local::acceptor>(*io_context);
        local::endpoint endpoint(sockPath.string());
        acceptor_->open(endpoint.protocol());
        acceptor_->bind(endpoint);
        acceptor_->listen(boost::asio::socket_base::max_listen_connections);

#ifndef _WIN32
        // Set socket permissions on Unix (Windows Unix sockets don't support filesystem
        // permissions)
        std::filesystem::permissions(sockPath, std::filesystem::perms::owner_all |
                                                   std::filesystem::perms::group_read |
                                                   std::filesystem::perms::group_write);
#endif

        actualSocketPath_ = sockPath;

        connectionSlots_ = std::make_unique<std::counting_semaphore<>>(config_.maxConnections);
        spdlog::info("SocketServer: bounded concurrency enabled (max {} slots)",
                     config_.maxConnections);

        if (state_) {
            const size_t maxConn = (config_.maxConnections > 0) ? config_.maxConnections : 1024;
            state_->stats.maxConnections.store(maxConn, std::memory_order_relaxed);
            state_->stats.connectionSlotsFree.store(maxConn, std::memory_order_relaxed);
            state_->stats.oldestConnectionAge.store(0, std::memory_order_relaxed);
            state_->stats.forcedCloseCount.store(0, std::memory_order_relaxed);
            if (maxConn == 0 || config_.maxConnections == 0) {
                spdlog::warn("SocketServer: connection limit is 0 (config={}, using={})",
                             config_.maxConnections, maxConn);
            }
        }

        // Seed writer budget prior to first connection
        std::size_t initialBudget = TuneAdvisor::serverWriterBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = TuneAdvisor::writerBudgetBytesPerTurn();
        if (initialBudget == 0)
            initialBudget = 256ULL * 1024;
        setWriterBudget(initialBudget);

        acceptLoopFuture_ = co_spawn(
            coordinator_->getExecutor(),
            [this]() -> awaitable<void> {
                co_await accept_loop();
                co_return;
            },
            boost::asio::use_future);
        spdlog::info("SocketServer: accept_loop scheduled on WorkCoordinator");

        // WorkCoordinator handles all threading - no need for separate worker pool
        spdlog::info("SocketServer: using WorkCoordinator ({} threads) for async execution",
                     coordinator_->getWorkerCount());

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
            std::vector<std::shared_ptr<TrackedSocket>> sockets;
            {
                std::lock_guard<std::mutex> lk(activeSocketsMutex_);
                for (auto& weak_sock : activeSockets_) {
                    if (auto tracked = weak_sock.lock()) {
                        sockets.push_back(std::move(tracked));
                    }
                }
                activeSockets_.clear();
            }

            const auto closed = close_sockets_on_executor(std::move(sockets));
            spdlog::info("Closed {} active connections", closed);
        } catch (const std::exception& e) {
            spdlog::warn("Exception while closing active sockets: {}", e.what());
        } catch (...) {
            spdlog::warn("Unknown exception while closing active sockets");
        }

        // Drain connection handler futures so no IPC coroutine continues
        // running after we tear down executors (prevents TSAN races during
        // restart cycles).
        std::vector<std::future<void>> pending;
        try {
            std::lock_guard<std::mutex> lk(connectionFuturesMutex_);
            pending.swap(connectionFutures_);
        } catch (const std::exception& e) {
            spdlog::warn("Exception moving connection futures: {}", e.what());
        }

        for (auto& future : pending) {
            if (!future.valid()) {
                continue;
            }
            try {
                // Use short timeout to avoid blocking shutdown
                auto status = future.wait_for(std::chrono::milliseconds(100));
                if (status == std::future_status::timeout) {
                    spdlog::debug("Connection future timed out during shutdown");
                }
            } catch (const std::exception& e) {
                spdlog::warn("Connection future wait failed: {}", e.what());
            } catch (...) {
                spdlog::warn("Connection future wait failed with unknown exception");
            }
        }

        // Close acceptor on executor thread and wait for accept loop completion
        close_acceptor_on_executor();

        if (acceptLoopFuture_.valid()) {
            try {
                // Use short timeout to avoid blocking shutdown
                auto status = acceptLoopFuture_.wait_for(std::chrono::milliseconds(500));
                if (status == std::future_status::ready) {
                    acceptLoopFuture_.get();
                    spdlog::info("SocketServer: accept_loop completed");
                } else {
                    spdlog::debug("SocketServer: accept_loop timed out during shutdown");
                }
            } catch (const std::exception& e) {
                spdlog::warn("SocketServer: accept_loop terminated with exception: {}", e.what());
            } catch (...) {
                spdlog::warn("SocketServer: accept_loop terminated with unknown exception");
            }
            acceptLoopFuture_ = {};
        }

        // Destroy acceptor while WorkCoordinator is still alive to avoid dangling
        // references during later shutdown (e.g., when destructor runs after
        // WorkCoordinator teardown).
        acceptor_.reset();
        connectionSlots_.reset();

        // WorkCoordinator manages thread lifecycle - just signal completion
        spdlog::info("SocketServer: accept loop stopped, WorkCoordinator continues running");

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
        spdlog::info(
            "stream-trace: accept_loop starting (max_conn={} socket={})", config_.maxConnections,
            actualSocketPath_.empty() ? config_.socketPath.string() : actualSocketPath_.string());
    }

    while (running_ && !stopping_) {
#if defined(TRACY_ENABLE)
        ZoneScopedN("SocketServer::accept_loop");
#endif

        try {
            // Use non-blocking try_acquire with async retry to avoid blocking the accept loop
            if (connectionSlots_) {
                while (!connectionSlots_->try_acquire()) {
                    if (!running_ || stopping_) {
                        break;
                    }
                    // Async wait before retrying - doesn't block other coroutines
                    boost::asio::steady_timer slot_timer(*coordinator_->getIOContext());
                    slot_timer.expires_after(std::chrono::milliseconds(1));
                    co_await slot_timer.async_wait(use_awaitable);
                }
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

            // Generate monotonic connection token for end-to-end tracing
            const uint64_t conn_token = connectionToken_.fetch_add(1, std::memory_order_relaxed);

            if (trace) {
                spdlog::info("stream-trace: [conn={}] accept_start (active={} total={})",
                             conn_token, activeConnections_.load(std::memory_order_relaxed),
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
                            acceptor_ =
                                std::make_unique<local::acceptor>(*coordinator_->getIOContext());
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
                    boost::asio::steady_timer timer(*coordinator_->getIOContext());
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
                spdlog::info("stream-trace: [conn={}] slot_acquired", conn_token);
            }

            if (trace) {
                spdlog::info("stream-trace: [conn={}] accept completed (active={} total={})",
                             conn_token, activeConnections_.load(std::memory_order_relaxed),
                             totalConnections_.load(std::memory_order_relaxed));
            }

            auto current = activeConnections_.fetch_add(1) + 1;
            totalConnections_.fetch_add(1);

            spdlog::info("SocketServer: [conn={}] accepted connection, active={} total={}",
                         conn_token, current, totalConnections_.load());

            if (state_) {
                state_->stats.activeConnections.store(current);
                auto maxConn = state_->stats.maxConnections.load(std::memory_order_relaxed);
                if (maxConn == 0)
                    maxConn = 1024;
                auto slotsFree = (current < maxConn) ? (maxConn - current) : 0;
                state_->stats.connectionSlotsFree.store(slotsFree, std::memory_order_relaxed);
            }

            auto connectionExecutor = boost::asio::make_strand(coordinator_->getExecutor());
            auto sock = std::make_shared<local::socket>(std::move(socket));
            auto tracked = std::make_shared<TrackedSocket>();
            tracked->socket = sock;
            tracked->executor = connectionExecutor;
            tracked->created_at =
                std::chrono::steady_clock::now(); // Track connection creation time
            register_socket(tracked);

            // Spawn connection with use_future for graceful shutdown tracking (PBI-066-41)
            // Pass semaphore for automatic release when connection completes (PBI-066-42)
            {
                std::lock_guard<std::mutex> lk(connectionFuturesMutex_);

                // Prune completed futures before adding new one
                prune_completed_futures();

                if (trace) {
                    spdlog::info("stream-trace: [conn={}] handler_spawned", conn_token);
                }

                // Create a capturing lambda that releases the semaphore on completion
                auto wrapped_handler = [this, conn_token, tracked]() mutable -> awaitable<void> {
                    // RAII guard for semaphore - releases on any exit path
                    struct SemaphoreGuard {
                        std::counting_semaphore<>* sem;
                        ~SemaphoreGuard() {
                            if (sem) {
                                sem->release();
                            }
                        }
                    } guard{connectionSlots_.get()};

                    // Handle the connection with tracing token
                    co_await handle_connection(tracked, conn_token);
                };

                // Spawn and track the connection
                connectionFutures_.push_back(
                    co_spawn(connectionExecutor, wrapped_handler(), boost::asio::use_future));
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

awaitable<void> SocketServer::handle_connection(std::shared_ptr<TrackedSocket> tracked_socket,
                                                uint64_t conn_token) {
#if defined(TRACY_ENABLE)
    ZoneScopedN("SocketServer::handle_connection");
#endif
    static const bool trace = stream_trace_enabled();
    const auto handler_start_time = std::chrono::steady_clock::now();

    if (!tracked_socket || !tracked_socket->socket) {
        co_return;
    }

    auto sock = tracked_socket->socket;

    // Track IPC task lifecycle: pending (spawned) -> active (executing) -> done
    struct IpcTaskTracker {
        StateComponent* state;
        bool active{false};
        IpcTaskTracker(StateComponent* s) : state(s) {
            if (state) {
                state->stats.ipcTasksPending.fetch_add(1, std::memory_order_relaxed);
            }
        }
        void markActive() {
            if (state && !active) {
                state->stats.ipcTasksPending.fetch_sub(1, std::memory_order_relaxed);
                state->stats.ipcTasksActive.fetch_add(1, std::memory_order_relaxed);
                active = true;
            }
        }
        ~IpcTaskTracker() {
            if (state) {
                if (active) {
                    state->stats.ipcTasksActive.fetch_sub(1, std::memory_order_relaxed);
                } else {
                    state->stats.ipcTasksPending.fetch_sub(1, std::memory_order_relaxed);
                }
            }
        }
    } ipcTracker{state_};

    struct CleanupGuard {
        SocketServer* server;
        bool trace;
        uint64_t conn_token;
        std::chrono::steady_clock::time_point start_time;
        ~CleanupGuard() {
            auto current = server->activeConnections_.fetch_sub(1) - 1;
            if (server->state_) {
                server->state_->stats.activeConnections.store(current);
                auto maxConn = server->state_->stats.maxConnections.load(std::memory_order_relaxed);
                if (maxConn == 0)
                    maxConn = 1024;
                auto slotsFree = (current < maxConn) ? (maxConn - current) : 0;
                server->state_->stats.connectionSlotsFree.store(slotsFree,
                                                                std::memory_order_relaxed);
            }
            auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - start_time)
                                   .count();
            if (trace) {
                spdlog::info("stream-trace: [conn={}] handle_connection cleanup active={} total={} "
                             "duration_ms={}",
                             conn_token, current, server->totalConnections_.load(), duration_ms);
            } else {
                spdlog::debug("[conn={}] Connection closed, active: {}, duration_ms: {}",
                              conn_token, current, duration_ms);
            }
        }
    } guard{this, trace, conn_token, handler_start_time};

    if (trace) {
        spdlog::info("stream-trace: [conn={}] handler_ready active={} total={} socket_valid={}",
                     conn_token, activeConnections_.load(std::memory_order_relaxed),
                     totalConnections_.load(std::memory_order_relaxed), sock && sock->is_open());
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
        handlerConfig.writer_budget_ref = writerBudget_;
        handlerConfig.writer_budget_bytes_per_turn = writerBudget_->load(std::memory_order_relaxed);
        handlerConfig.enable_streaming = true;
        handlerConfig.enable_multiplexing = true;
        // Keep worker_job_signal for cross-executor coordination, but don't override
        // worker_executor This keeps IPC handlers on dedicated yams-ipc-N threads (prevents
        // starvation by background work)
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
            connectionTimeout = std::chrono::milliseconds(TuneAdvisor::ipcTimeoutMs());
        }
        auto timeoutSeconds = std::chrono::duration_cast<std::chrono::seconds>(connectionTimeout);
        if (timeoutSeconds.count() == 0 && connectionTimeout.count() > 0) {
            timeoutSeconds = std::chrono::seconds(1);
        }
        handlerConfig.read_timeout = timeoutSeconds;
        handlerConfig.write_timeout = timeoutSeconds;
        auto streamChunkTimeoutMs = TuneAdvisor::streamChunkTimeoutMs();
        if (streamChunkTimeoutMs == 0 && connectionTimeout.count() > 0) {
            streamChunkTimeoutMs = static_cast<uint32_t>(connectionTimeout.count());
        }
        handlerConfig.stream_chunk_timeout = std::chrono::milliseconds(streamChunkTimeoutMs);
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
        // Keep worker_job_signal for cross-executor coordination, but don't override
        // worker_executor This keeps IPC handlers on dedicated yams-ipc-N threads (prevents
        // starvation by background work)
        try {
            if (disp) {
                handlerConfig.worker_job_signal = disp->getWorkerJobSignal();
            }
        } catch (...) {
        }
        auto handler = std::make_shared<RequestHandler>(disp, handlerConfig);

        // Wrap socket in shared_ptr for tracking and use modern C++20 move semantics
        // Use the server's stop_source token so we can cancel connections during shutdown
        auto token = stop_source_.get_token();

        // Mark IPC task as active (transitioned from pending to executing)
        ipcTracker.markActive();

        // Enforce maximum connection lifetime to prevent zombie connections
        if (config_.maxConnectionLifetime.count() > 0) {
            // Use async_initiate to race connection handler against lifetime timer
            // (replaces experimental::awaitable_operators)
            auto executor = sock->get_executor();
            auto lifetime = config_.maxConnectionLifetime;
            auto created_at = tracked_socket->created_at;

            bool timedOut =
                co_await boost::asio::async_initiate<decltype(boost::asio::use_awaitable),
                                                     void(std::exception_ptr, bool)>(
                    [this, handler, sock, token, conn_token, executor, lifetime,
                     created_at](auto completion_handler) mutable {
                        // Shared state for race coordination
                        auto completed = std::make_shared<std::atomic<bool>>(false);
                        auto timer = std::make_shared<boost::asio::steady_timer>(executor);
                        timer->expires_after(lifetime);

                        using HandlerT = std::decay_t<decltype(completion_handler)>;
                        auto handlerPtr = std::make_shared<HandlerT>(std::move(completion_handler));
                        auto completion_exec =
                            boost::asio::get_associated_executor(*handlerPtr, executor);

                        // Set up lifetime timer
                        timer->async_wait([this, completed, handlerPtr, completion_exec, conn_token,
                                           created_at,
                                           lifetime](const boost::system::error_code& ec) mutable {
                            if (ec == boost::asio::error::operation_aborted)
                                return; // Cancelled by handler completion
                            if (!completed->exchange(true, std::memory_order_acq_rel)) {
                                // Timer won - lifetime exceeded
                                auto newCount =
                                    forcedCloseCount_.fetch_add(1, std::memory_order_relaxed) + 1;
                                if (state_) {
                                    state_->stats.forcedCloseCount.store(newCount,
                                                                         std::memory_order_relaxed);
                                }
                                auto age_s = std::chrono::duration_cast<std::chrono::seconds>(
                                                 std::chrono::steady_clock::now() - created_at)
                                                 .count();
                                spdlog::warn("[conn={}] Connection lifetime exceeded (age={}s, "
                                             "limit={}s) - forcing close",
                                             conn_token, age_s, lifetime.count());
                                boost::asio::post(
                                    completion_exec, [h = std::move(*handlerPtr)]() mutable {
                                        std::move(h)(nullptr, true); // timedOut = true
                                    });
                            }
                        });

                        // Spawn connection handler
                        boost::asio::co_spawn(
                            executor,
                            [handler, sock, token, conn_token, timer, completed, handlerPtr,
                             completion_exec]() mutable -> boost::asio::awaitable<void> {
                                co_await handler->handle_connection(sock, token, conn_token);

                                if (!completed->exchange(true, std::memory_order_acq_rel)) {
                                    // Handler completed first
                                    timer->cancel();
                                    boost::asio::post(
                                        completion_exec, [h = std::move(*handlerPtr)]() mutable {
                                            std::move(h)(nullptr, false); // timedOut = false
                                        });
                                }
                            },
                            boost::asio::detached);
                    },
                    boost::asio::use_awaitable);

            if (timedOut) {
                boost::system::error_code ec;
                sock->close(ec);
            }
        } else {
            co_await handler->handle_connection(sock, token, conn_token);
        }
    } catch (const std::exception& e) {
        spdlog::error("SocketServer::handle_connection error: {}", e.what());
    }
}

void SocketServer::register_socket(std::shared_ptr<TrackedSocket> tracked_socket) {
    if (!tracked_socket) {
        return;
    }

    std::lock_guard<std::mutex> lk(activeSocketsMutex_);
    // Clean up expired weak_ptrs while we're here
    activeSockets_.erase(std::remove_if(activeSockets_.begin(), activeSockets_.end(),
                                        [](const auto& weak) { return weak.expired(); }),
                         activeSockets_.end());
    activeSockets_.push_back(tracked_socket);
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

void SocketServer::execute_on_io_context(std::function<void()> fn) {
    if (!fn) {
        return;
    }

    auto io_context = coordinator_->getIOContext();
    auto executor = coordinator_->getExecutor();
    if (io_context->stopped() || executor.running_in_this_thread()) {
        fn();
        return;
    }

    std::promise<void> promise;
    auto future = promise.get_future();
    boost::asio::post(executor, [fn = std::move(fn), promise = std::move(promise)]() mutable {
        try {
            fn();
            promise.set_value();
        } catch (...) {
            try {
                promise.set_exception(std::current_exception());
            } catch (...) {
            }
        }
    });

    future.get();
}

void SocketServer::close_acceptor_on_executor() {
    if (!acceptor_) {
        return;
    }

    try {
        execute_on_io_context([this]() {
            if (!acceptor_) {
                return;
            }
            boost::system::error_code ec;
            acceptor_->cancel(ec);
            if (acceptor_->is_open()) {
                acceptor_->close(ec);
            }
        });
    } catch (const std::exception& e) {
        spdlog::warn("SocketServer: failed to close acceptor on executor: {}", e.what());
    }
}

std::size_t SocketServer::close_sockets_on_executor(
    std::vector<std::shared_ptr<TrackedSocket>> tracked_sockets) {
    if (tracked_sockets.empty()) {
        return 0;
    }

    std::vector<std::future<void>> completions;
    completions.reserve(tracked_sockets.size());
    std::size_t scheduled = 0;

    for (auto& tracked : tracked_sockets) {
        if (!tracked || !tracked->socket) {
            continue;
        }

        auto sock = tracked->socket;

        auto promise = std::make_shared<std::promise<void>>();
        auto future = promise->get_future();
        completions.emplace_back(std::move(future));
        ++scheduled;

        auto exec = tracked->executor;
        if (!exec) {
            exec = sock->get_executor();
        }
        try {
            boost::asio::dispatch(exec, [sock, promise]() mutable {
                boost::system::error_code ec;
                sock->cancel(ec);
                sock->close(ec);
                promise->set_value();
            });
        } catch (const std::exception& e) {
            spdlog::warn("SocketServer: failed to dispatch socket close: {}", e.what());
            promise->set_value();
        } catch (...) {
            spdlog::warn("SocketServer: failed to dispatch socket close (unknown error)");
            promise->set_value();
        }
    }

    for (auto& future : completions) {
        try {
            future.wait();
        } catch (...) {
        }
    }

    return scheduled;
}

uint64_t SocketServer::oldestConnectionAgeSeconds() const {
    std::lock_guard<std::mutex> lk(activeSocketsMutex_);
    if (activeSockets_.empty()) {
        return 0;
    }

    auto now = std::chrono::steady_clock::now();
    uint64_t oldestAge = 0;

    for (const auto& weakPtr : activeSockets_) {
        if (auto tracked = weakPtr.lock()) {
            auto age =
                std::chrono::duration_cast<std::chrono::seconds>(now - tracked->created_at).count();
            if (static_cast<uint64_t>(age) > oldestAge) {
                oldestAge = static_cast<uint64_t>(age);
            }
        }
    }

    return oldestAge;
}

} // namespace yams::daemon
