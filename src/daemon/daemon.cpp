#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <future>
#include <mutex>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif // For getuid(), geteuid(), getpid()
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/compat/thread_stop_compat.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/daemon/components/TuningManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/connection_fsm.h>
#if defined(TRACY_ENABLE)
#include <tracy/Tracy.hpp>
#endif

#include <yams/config/config_migration.h>

#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <yams/profiling.h>

namespace {
void set_current_thread_name(const std::string& name) {
#ifdef __linux__
    prctl(PR_SET_NAME, name.c_str(), 0, 0, 0);
#elif __APPLE__
    pthread_setname_np(name.c_str());
#endif
}
} // namespace
// IPC server implementation removed in PBI-007; client uses Boost.Asio path.

namespace yams::daemon {

YamsDaemon::YamsDaemon(const DaemonConfig& config)
    : config_(config), asyncInitStartedFuture_(asyncInitStartedPromise_.get_future().share()) {
    spdlog::info("[YamsDaemon] Constructor entry");
    // Resolve paths if not explicitly set
    if (config_.socketPath.empty()) {
        // Keep centralized FSM-based resolution for consistency with client/CLI
        config_.socketPath = yams::daemon::ConnectionFsm::resolve_socket_path_config_first();
    }
    if (config_.pidFile.empty()) {
        config_.pidFile = resolveSystemPath(PathType::PidFile);
    }
    if (config_.logFile.empty()) {
        config_.logFile = resolveSystemPath(PathType::LogFile);
    }

    lifecycleManager_ = std::make_unique<LifecycleComponent>(this, config_.pidFile);
    serviceManager_ = std::make_shared<ServiceManager>(config_, state_, lifecycleFsm_);

    // Create metrics aggregator before the dispatcher so status can pull from a single snapshot
    metrics_ = std::make_unique<DaemonMetrics>(&lifecycleFsm_, &state_, serviceManager_.get(),
                                               serviceManager_->getWorkCoordinator());
    requestDispatcher_ =
        std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_, metrics_.get());
    // Prepare centralized tuning manager early; start it in start() before sockets/services
    try {
        tuningManager_ = std::make_unique<TuningManager>(serviceManager_.get(), &state_,
                                                         serviceManager_->getWorkCoordinator());
        spdlog::debug("TuningManager constructed early in YamsDaemon");
        tuningManager_->setRepairControlHook([this](uint32_t tokens, uint32_t batch) {
            try {
                std::lock_guard<std::mutex> lock(repairCoordinatorMutex_);
                if (repairCoordinator_) {
                    repairCoordinator_->setMaintenanceTokens(tokens);
                    repairCoordinator_->setMaxBatch(batch);
                }
            } catch (...) {
            }
        });
    } catch (const std::exception& e) {
        spdlog::warn("Failed to construct TuningManager early: {}", e.what());
    }
    state_.stats.startTime = std::chrono::steady_clock::now();

    spdlog::info("Daemon configuration resolved:");
    spdlog::info("  Data dir: {}", config_.dataDir.string());
    spdlog::info("  Socket: {}", config_.socketPath.string());
    spdlog::info("  PID file: {}", config_.pidFile.string());
    spdlog::info("  Log file: {}", config_.logFile.string());

    // Let in-process components (e.g., EmbeddingGenerator/HybridBackend) know they are running
    // inside the daemon so they can avoid creating a DaemonBackend and self-calling the IPC API.
#ifndef _WIN32
    ::setenv("YAMS_IN_DAEMON", "1", 1);
#else
    _putenv_s("YAMS_IN_DAEMON", "1");
#endif

    if (!config_.socketPath.empty()) {
#ifndef _WIN32
        ::setenv("YAMS_DAEMON_SOCKET", config_.socketPath.c_str(), 1);
#else
        _putenv_s("YAMS_DAEMON_SOCKET", config_.socketPath.string().c_str());
#endif
        spdlog::debug("Seeded YAMS_DAEMON_SOCKET='{}'", config_.socketPath.string());
    }
}

YamsDaemon::~YamsDaemon() {
    if (running_) {
        stop();
    }
    if (tuningManager_) {
        tuningManager_->stop();
    }
    {
        std::lock_guard<std::mutex> lock(shutdownThreadMutex_);
        if (shutdownThread_.joinable()) {
            try {
                shutdownThread_.join();
            } catch (const std::exception& e) {
                try {
                    spdlog::warn("[YamsDaemon] shutdown thread join exception: {}", e.what());
                } catch (...) {
                }
            } catch (...) {
                try {
                    spdlog::warn("[YamsDaemon] shutdown thread join unknown exception");
                } catch (...) {
                }
            }
        }
    }
}

Result<size_t> YamsDaemon::autoloadPluginsNow() {
    if (!serviceManager_) {
        return Error{ErrorCode::NotInitialized, "ServiceManager not initialized"};
    }
    // Bridge awaitable<Result<size_t>> to sync Result<size_t> without requiring
    // default-constructible T
    try {
        boost::asio::thread_pool tp(1);
        std::promise<Result<size_t>> prom;
        auto fut = prom.get_future();
        auto sm = serviceManager_.get();
        boost::asio::co_spawn(
            tp,
            [sm, p = std::move(prom)]() mutable -> boost::asio::awaitable<void> {
                auto r = co_await sm->autoloadPluginsNow();
                p.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);
        tp.join();
        return fut.get();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<void> YamsDaemon::start() {
    if (running_.exchange(true)) {
        return Error{ErrorCode::InvalidState, "Daemon already running"};
    }

    spdlog::info("Starting YAMS daemon...");

    // Starting -> Initializing (ensure ordering so HealthyEvent can promote to Ready)
    spdlog::info("[Startup] Phase: FSM Reset");
    lifecycleFsm_.reset();

    spdlog::info("[Startup] Phase: LifecycleManager Init");
    if (auto result = lifecycleManager_->initialize(); !result) {
        running_ = false;
        return result;
    }
    spdlog::info("[Startup] Phase: LifecycleManager Init OK");

    // Start centralized tuning BEFORE accepting connections or initializing services
    spdlog::info("[Startup] Phase: TuningManager Start");
    try {
        if (tuningManager_) {
            tuningManager_->start();
            spdlog::info("TuningManager started (early)");
        } else {
            spdlog::warn("TuningManager not available; skipping start");
        }
    } catch (const std::exception& e) {
        spdlog::warn("Failed to start TuningManager early: {}", e.what());
    }
    spdlog::info("[Startup] Phase: TuningManager Start OK");

    // Transition to Initializing as soon as core lifecycle is bootstrapped,
    // before kicking off async service initialization to avoid race with HealthyEvent.
    spdlog::info("[Startup] Phase: FSM BootstrappedEvent");
    lifecycleFsm_.dispatch(BootstrappedEvent{});

    // Start integrated socket server ASAP so status requests work during initialization
    spdlog::info("[Startup] Phase: SocketServer Start");
    SocketServer::Config socketConfig;
    socketConfig.socketPath = config_.socketPath;
    // Derive maxConnections from TuneAdvisor (env/config) with a sane computed fallback.
    // Priority:
    //  1) YAMS_MAX_ACTIVE_CONN via TuneAdvisor::maxActiveConn()
    //  2) Computed: recommendedThreads * ioConnPerThread * 4 (burst factor), min 256
    {
        uint64_t cap = 0;
        try {
            cap = TuneAdvisor::maxActiveConn();
        } catch (...) {
            cap = 0;
        }
        if (cap == 0) {
            uint32_t rec = 0;
            uint32_t per = 8;
            try {
                rec = TuneAdvisor::recommendedThreads();
            } catch (...) {
            }
            try {
                per = TuneAdvisor::ioConnPerThread();
            } catch (...) {
            }
            uint64_t computed = static_cast<uint64_t>(rec) * static_cast<uint64_t>(per) * 4ull;
            if (computed < 256ull)
                computed = 256ull;
            cap = computed;
        }
        if (cap == 0)
            cap = 1024;
        socketConfig.maxConnections = static_cast<std::size_t>(cap);
    }

    socketServer_ = std::make_unique<SocketServer>(
        socketConfig, serviceManager_->getWorkCoordinator(), requestDispatcher_.get(), &state_);

    // Provide SocketServer to DaemonMetrics for connection age tracking
    if (metrics_) {
        metrics_->setSocketServer(socketServer_.get());
    }

    if (auto result = socketServer_->start(); !result) {
        running_ = false;
        serviceManager_->shutdown();
        lifecycleManager_->shutdown();
        return Error{ErrorCode::IOError,
                     "Failed to start socket server: " + result.error().message};
    }
    spdlog::info("[Startup] Phase: SocketServer Start OK");

    // Mark IPC as ready now that socket server is running
    state_.readiness.ipcServerReady = true;
    spdlog::info("Socket server started on {}", config_.socketPath.string());

    if (tuningManager_) {
        tuningManager_->setWriterBudgetHook([this](std::size_t bytes) {
            try {
                if (socketServer_)
                    socketServer_->setWriterBudget(bytes);
            } catch (...) {
            }
        });
    }

#ifdef YAMS_TESTING
    // Fast-start mode for tests: skip heavy service initialization and allow
    // streaming stubs/status responses to operate over the live socket server.
    // Enable by setting YAMS_TEST_FAST_START=1 in the environment.
    if (const char* fast = std::getenv("YAMS_TEST_FAST_START");
        fast && *fast && std::string(fast) != "0" && std::string(fast) != "false") {
        spdlog::warn("YAMS_TEST_FAST_START enabled: skipping ServiceManager initialization");
        // Leave lifecycle FSM in its current (Initializing) state; readiness flags
        // for services remain false. RequestDispatcher will serve Status responses
        // and RequestHandler will use streaming stubs for Search/List/Grep.
        return Result<void>();
    }
#endif

    spdlog::info("[Startup] Phase: ServiceManager Init");
    if (auto result = serviceManager_->initialize(); !result) {
        running_ = false;
        lifecycleManager_->shutdown();
        // Stop socket if we fail to initialize services
        if (socketServer_) {
            (void)socketServer_->stop();
            socketServer_.reset();
            state_.readiness.ipcServerReady = false;
        }
        return result;
    }
    spdlog::info("[Startup] Phase: ServiceManager Init OK (async)");

    // Launch background task coroutines after io_context is live (post-initialize)
    try {
        serviceManager_->startBackgroundTasks();
    } catch (const std::exception& e) {
        spdlog::warn("Failed to start background tasks: {}", e.what());
    }

    // Defer RepairCoordinator start until lifecycle is Healthy/Ready to avoid competing with init

    // Bootstrapped event already dispatched before service initialization to prevent race.

    // Note: Main loop now runs on the calling thread via runLoop() instead of a separate thread.
    // This avoids thread resource exhaustion on Windows (EAGAIN with 48+ threads).

    spdlog::info("YAMS daemon started successfully.");
    spdlog::info("Hint: If this is a new or migrated data directory, run 'yams repair "
                 "--embeddings' to generate vector embeddings.");
    spdlog::info("Hint: Use 'yams stats --verbose' to monitor worker pool (threads/active/queued) "
                 "and streaming metrics.");
    return Result<void>();
}

void YamsDaemon::runLoop() {
    set_current_thread_name("yams-daemon-main");

    if (serviceManager_) {
        spdlog::info("Triggering deferred service initialization...");
        serviceManager_->startAsyncInit(&asyncInitStartedPromise_, &asyncInitBarrierSet_);

        if (asyncInitBarrierSet_.load(std::memory_order_acquire)) {
            spdlog::info("runLoop: waiting for async init barrier...");
            try {
                asyncInitStartedFuture_.wait();
                spdlog::info("runLoop: async init barrier passed");
            } catch (const std::exception& e) {
                spdlog::warn("runLoop: async init barrier wait failed: {}", e.what());
            }
        }

        initWaiterThread_ = std::thread([this]() {
            set_current_thread_name("yams-init-waiter");
            spdlog::info(
                "[InitWaiter] Thread started, waiting for ServiceManager terminal state...");

            auto snapshot = serviceManager_->waitForServiceManagerTerminalState(300);

            if (stopRequested_.load()) {
                spdlog::info("[InitWaiter] Stop requested, exiting without dispatching events");
                return;
            }

            initHandled_.store(true, std::memory_order_release);

            if (snapshot.state == ServiceManagerState::Ready) {
                spdlog::info(
                    "[InitWaiter] ServiceManager reached Ready state, dispatching HealthyEvent");
                lifecycleFsm_.dispatch(HealthyEvent{});
                try {
                    if (metrics_) {
                        metrics_->startPolling();
                        spdlog::info("[InitWaiter] DaemonMetrics background polling started");
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("[InitWaiter] Failed to start metrics polling: {}", e.what());
                }
            } else if (snapshot.state == ServiceManagerState::Failed) {
                spdlog::error("[InitWaiter] ServiceManager failed: {}", snapshot.lastError);
                lifecycleFsm_.dispatch(FailureEvent{snapshot.lastError});
            } else {
                spdlog::warn("[InitWaiter] ServiceManager in unexpected terminal state: {}",
                             static_cast<int>(snapshot.state));
            }
            spdlog::info("[InitWaiter] Thread exiting");
        });
        spdlog::info("runLoop: init waiter thread spawned");
    } else {
        initHandled_.store(true, std::memory_order_release);
    }

    lifecycleFsm_.tick();

    spdlog::info("runLoop: entering main while loop, stopRequested={}", stopRequested_.load());

    while (!stopRequested_.load()) {
        spdlog::debug("runLoop: loop iteration, initHandled={}", initHandled_.load());
#if defined(TRACY_ENABLE)
        YAMS_FRAME_MARK_START("daemon_tick");
#endif
        if (signalCheckHook_) {
            try {
                if (signalCheckHook_()) {
                    lifecycleFsm_.dispatch(ShutdownRequestedEvent{});
#if defined(TRACY_ENABLE)
                    YAMS_FRAME_MARK_END("daemon_tick");
#endif
                    break;
                }
            } catch (...) {
            }
        }

        uint32_t tick_ms = TuneAdvisor::statusTickMs();
        spdlog::debug("runLoop: waiting on cv for {}ms", tick_ms);
        std::unique_lock<std::mutex> lock(stop_mutex_);
        if (stop_cv_.wait_for(lock, std::chrono::milliseconds(tick_ms),
                              [&] { return stopRequested_.load(); })) {
            spdlog::info(
                "runLoop: cv woke with stop requested, dispatching ShutdownRequestedEvent");
            lifecycleFsm_.dispatch(ShutdownRequestedEvent{});

#if defined(TRACY_ENABLE)
            YAMS_FRAME_MARK_END("daemon_tick");
#endif
            break;
        }
        try {
            if (metrics_) {
                metrics_->refresh();
            }
        } catch (...) {
        }
        // Apply pending reload requests (e.g., SIGHUP) for tuning-only adjustments
        if (reloadRequested_.load(std::memory_order_relaxed)) {
            try {
                reloadTuningConfig();
            } catch (...) {
            }
            reloadRequested_.store(false, std::memory_order_relaxed);
        }
        // Deferred background tasks: require FSM Ready
        // Note: RepairCoordinator triggers lazy-loading of embeddings/vector DB,
        // so we start it when FSM is Ready rather than waiting for searchEngineReady.
        // This breaks the circular dependency where search engine waits for embeddings
        // which wait for RepairCoordinator which waits for search engine.
        auto snap = lifecycleFsm_.snapshot();
        {
            std::lock_guard<std::mutex> lk(repairCoordinatorMutex_);
            spdlog::debug(
                "[DaemonLoop] FSM state={}, enableAutoRepair={}, repairCoordinator_exists={}",
                static_cast<int>(snap.state), config_.enableAutoRepair,
                (repairCoordinator_ != nullptr));
            if (snap.state == LifecycleState::Ready) {
                if (config_.enableAutoRepair && !repairCoordinator_) {
                    spdlog::info("[DaemonLoop] Starting RepairCoordinator...");
                    try {
                        RepairCoordinator::Config rcfg;
                        rcfg.enable = true;
                        rcfg.dataDir = config_.dataDir;
                        rcfg.maxBatch = static_cast<std::uint32_t>(config_.autoRepairBatchSize);
                        auto activeFn = [this]() -> size_t {
                            return static_cast<size_t>(state_.stats.activeConnections.load());
                        };
                        repairCoordinator_ = std::make_unique<RepairCoordinator>(
                            serviceManager_.get(), &state_, activeFn, rcfg);
                        repairCoordinator_->start();
                    } catch (const std::exception& e) {
                        spdlog::warn("Failed to start RepairCoordinator: {}", e.what());
                    }
                }

                static bool modelPreloadSkipped = false;
                if (!modelPreloadSkipped) {
                    modelPreloadSkipped = true;
                    spdlog::info("Model preload disabled - models will load on first use");
                }
            }
        }
        {
            std::lock_guard<std::mutex> lk(repairCoordinatorMutex_);
            if (repairCoordinator_) {
                size_t active = static_cast<size_t>(state_.stats.activeConnections.load());
                uint32_t busyThresh = TuneAdvisor::repairBusyConnThreshold();
                auto now = std::chrono::steady_clock::now();
                static std::chrono::steady_clock::time_point rcBusySince{};
                static std::chrono::steady_clock::time_point rcReadySince{};
                bool isBusy = (active >= busyThresh);
                if (isBusy) {
                    if (rcBusySince.time_since_epoch().count() == 0)
                        rcBusySince = now;
                    rcReadySince = {};
                } else {
                    if (rcReadySince.time_since_epoch().count() == 0)
                        rcReadySince = now;
                    rcBusySince = {};
                }
                uint32_t degradeHold = TuneAdvisor::repairDegradeHoldMs();
                uint32_t readyHold = TuneAdvisor::repairReadyHoldMs();
                bool busyHeld =
                    isBusy && rcBusySince.time_since_epoch().count() != 0 &&
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - rcBusySince)
                            .count() >= degradeHold;
                bool idleHeld =
                    !isBusy && rcReadySince.time_since_epoch().count() != 0 &&
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - rcReadySince)
                            .count() >= readyHold;
                bool pending = state_.stats.repairQueueDepth.load() > 0;
                auto fsmSnap = lifecycleFsm_.snapshot();
                if (fsmSnap.state == LifecycleState::Ready) {
                    if (pending && busyHeld)
                        lifecycleFsm_.dispatch(DegradedEvent{});
                } else if (fsmSnap.state == LifecycleState::Degraded) {
                    if (!pending || idleHeld)
                        lifecycleFsm_.dispatch(HealthyEvent{});
                }
            }
        }
        // Periodic tasks can be added here

        // Readiness ticker: nudge searchProgress while long-running init is in progress
        try {
            if (!state_.readiness.searchEngineReady.load()) {
                auto now = std::chrono::steady_clock::now();
                auto elapsed = now - state_.stats.startTime;
                auto sec = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();

                // Target ramps toward 90% over time; clamp to avoid claiming ready
                int target = 5 + static_cast<int>(std::min<int64_t>(sec * 3, 90));
                int cur = state_.readiness.searchProgress.load();

                if (cur < target) {
                    state_.readiness.searchProgress.store(std::min(95, target),
                                                          std::memory_order_relaxed);
                    spdlog::debug("Readiness ticker: nudged searchProgress to {}%",
                                  state_.readiness.searchProgress.load());
                }
            }
        } catch (...) {
            // best-effort ticker; ignore errors
        }

#if defined(TRACY_ENABLE)
        YAMS_FRAME_MARK_END("daemon_tick");
#endif
    }
    spdlog::debug("Daemon main loop exiting.");
}

Result<void> YamsDaemon::stop() {
    if (!running_.exchange(false)) {
        return Error{ErrorCode::InvalidState, "Daemon not running"};
    }

    spdlog::info("Stopping YAMS daemon...");

    stopRequested_.store(true, std::memory_order_release);
    stop_cv_.notify_all();

    if (serviceManager_) {
        serviceManager_->cancelServiceManagerWait();
    }
    if (initWaiterThread_.joinable()) {
        spdlog::debug("Joining init waiter thread...");
        initWaiterThread_.join();
        spdlog::debug("Init waiter thread joined");
    }

    // Stop components that use WorkCoordinator's io_context BEFORE stopping ServiceManager
    // (ServiceManager stops WorkCoordinator, which would prevent async operations from completing)

    // Stop tuning manager (uses WorkCoordinator strand + timers)
    if (tuningManager_) {
        try {
            spdlog::debug("Stopping tuning manager...");
            tuningManager_->stop();
            spdlog::debug("Tuning manager stopped");
        } catch (const std::exception& e) {
            spdlog::warn("TuningManager stop exception: {}", e.what());
        } catch (...) {
        }
        tuningManager_.reset();
    }

    // Stop metrics (uses WorkCoordinator strand + timers)
    if (metrics_) {
        spdlog::debug("Stopping metrics polling...");
        metrics_->stopPolling();
        spdlog::debug("Metrics polling stopped");
        // Destroy metrics (and its strand) while WorkCoordinator is still alive
        spdlog::debug("Resetting metrics before WorkCoordinator shutdown...");
        metrics_.reset();
    }

    // Stop socket server before ServiceManager tears down the WorkCoordinator
    if (socketServer_) {
        spdlog::debug("Stopping socket server...");
        auto stopResult = socketServer_->stop();
        if (!stopResult) {
            spdlog::warn("Socket server stop returned error: {}", stopResult.error().message);
        }
        state_.readiness.ipcServerReady = false;
    }

    // Stop ServiceManager (this will stop WorkCoordinator's io_context in Phase 4)
    if (serviceManager_) {
        spdlog::debug("Shutting down service manager...");
        serviceManager_->shutdown();
        spdlog::debug("Service manager shutdown complete");
    }

    // Note: Main loop runs on caller's thread via runLoop(), no daemon thread to join

    {
        std::lock_guard<std::mutex> lock(repairCoordinatorMutex_);
        if (repairCoordinator_) {
            repairCoordinator_->stop();
            repairCoordinator_.reset();
        }
    }

    // Metrics already stopped earlier (before ServiceManager)

    if (socketServer_) {
        spdlog::debug("Resetting socket server...");
        socketServer_.reset();
        spdlog::debug("Socket server reset");
    }

    if (lifecycleManager_) {
        lifecycleManager_->shutdown();
    }

    if (lifecycleManager_) {
        lifecycleManager_->shutdown();
    }

    // NOTE: We do NOT reset GlobalIOContext here.
    // GlobalIOContext is a singleton that should persist for the process lifetime.
    // Resetting it after multiple daemon lifecycles causes resource corruption.
    // For production use, daemons typically run for extended periods without restart.
    // For testing, the accumulated resources are acceptable and freed at process exit.

    // Mark lifecycle stopped
    lifecycleFsm_.dispatch(StoppedEvent{});

    spdlog::info("YAMS daemon stopped.");
    return Result<void>();
}

// run() method removed; loop is inlined in start()

void YamsDaemon::onDocumentAdded(const std::string& hash, const std::string& path) {
    std::lock_guard<std::mutex> lock(repairCoordinatorMutex_);
    if (repairCoordinator_) {
        RepairCoordinator::DocumentAddedEvent event{hash, path};
        repairCoordinator_->onDocumentAdded(event);
    }
}

void YamsDaemon::onDocumentRemoved(const std::string& hash) {
    std::lock_guard<std::mutex> lock(repairCoordinatorMutex_);
    if (repairCoordinator_) {
        RepairCoordinator::DocumentRemovedEvent event{hash};
        repairCoordinator_->onDocumentRemoved(event);
    }
}

// Path resolution helpers remain static methods of YamsDaemon
namespace {
std::filesystem::path getXDGStateHome() {
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA for state/log files
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        return std::filesystem::path(localAppData);
    }
    return std::filesystem::path();
#else
    const char* xdgState = std::getenv("XDG_STATE_HOME");
    if (xdgState)
        return std::filesystem::path(xdgState);
    const char* home = std::getenv("HOME");
    return home ? std::filesystem::path(home) / ".local" / "state" : std::filesystem::path();
#endif
}
} // namespace

std::filesystem::path YamsDaemon::resolveSystemPath(PathType type) {
    namespace fs = std::filesystem;
#ifdef _WIN32
    bool isRoot = false;
    int uid = 0; // Not used for path generation on Windows usually, or use GetCurrentProcessId() if
                 // needed
#else
    bool isRoot = (geteuid() == 0);
    uid_t uid = getuid();
#endif

    switch (type) {
        case PathType::Socket:
            return yams::daemon::ConnectionFsm::resolve_socket_path();
        case PathType::PidFile:
#ifdef _WIN32
            if (auto xdg = getXDGRuntimeDir(); !xdg.empty() && canWriteToDirectory(xdg))
                return xdg / "yams-daemon.pid";
            return fs::temp_directory_path() / "yams-daemon.pid";
#else
            if (isRoot)
                return fs::path("/var/run/yams-daemon.pid");
            if (auto xdg = getXDGRuntimeDir(); !xdg.empty() && canWriteToDirectory(xdg))
                return xdg / "yams-daemon.pid";
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".pid");
#endif
        case PathType::LogFile:
#ifdef _WIN32
            if (auto xdg = getXDGStateHome(); !xdg.empty()) {
                auto logDir = xdg / "yams";
                std::error_code ec;
                fs::create_directories(logDir, ec);
                if (canWriteToDirectory(logDir))
                    return logDir / "daemon.log";
            }
            return fs::temp_directory_path() / "yams-daemon.log";
#else
            if (isRoot && canWriteToDirectory("/var/log"))
                return fs::path("/var/log/yams-daemon.log");
            if (auto xdg = getXDGStateHome(); !xdg.empty()) {
                auto logDir = xdg / "yams";
                std::error_code ec;
                fs::create_directories(logDir, ec);
                if (canWriteToDirectory(logDir))
                    return logDir / "daemon.log";
            }
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".log");
#endif
    }
    return fs::path();
}

bool YamsDaemon::canWriteToDirectory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;
    if (!fs::exists(dir))
        return false;
#ifdef _WIN32
    auto pid = _getpid();
#else
    auto pid = getpid();
#endif
    auto testFile = dir / (".yams-test-" + std::to_string(pid));
    std::ofstream test(testFile);
    if (test.good()) {
        test.close();
        std::error_code ec;
        fs::remove(testFile, ec);
        return true;
    }
    return false;
}

std::filesystem::path YamsDaemon::getXDGRuntimeDir() {
#ifdef _WIN32
    // Windows: Use LOCALAPPDATA for runtime files (no true XDG equivalent)
    if (const char* localAppData = std::getenv("LOCALAPPDATA")) {
        auto yamDir = std::filesystem::path(localAppData) / "yams";
        std::error_code ec;
        std::filesystem::create_directories(yamDir, ec);
        return yamDir;
    }
    return std::filesystem::temp_directory_path();
#else
    const char* xdgRuntime = std::getenv("XDG_RUNTIME_DIR");
    return xdgRuntime ? std::filesystem::path(xdgRuntime) : std::filesystem::path();
#endif
}

// getXDGStateHome() moved to anonymous namespace helper above

#ifdef YAMS_TESTING
// Test accessor implementations delegate to ServiceManager
std::shared_ptr<yams::metadata::MetadataRepository> YamsDaemon::_test_getMetadataRepo() const {
    return serviceManager_ ? serviceManager_->getMetadataRepo() : nullptr;
}

std::shared_ptr<yams::vector::VectorIndexManager> YamsDaemon::_test_getVectorIndexManager() const {
    return serviceManager_ ? serviceManager_->getVectorIndexManager() : nullptr;
}

std::shared_ptr<yams::vector::EmbeddingGenerator> YamsDaemon::_test_getEmbeddingGenerator() const {
    return serviceManager_ ? serviceManager_->getEmbeddingGenerator() : nullptr;
}
#endif

} // namespace yams::daemon

namespace yams::daemon {

void YamsDaemon::reloadTuningConfig() {
    try {
        if (config_.configFilePath.empty()) {
            spdlog::info("[Reload] No config file path recorded; skipping reload");
            return;
        }
        if (!std::filesystem::exists(config_.configFilePath)) {
            spdlog::warn("[Reload] Config file missing: {}", config_.configFilePath.string());
            return;
        }
    } catch (...) {
    }
    try {
        yams::config::ConfigMigrator migrator;
        auto parsed = migrator.parseTomlConfig(config_.configFilePath.string());
        if (!parsed) {
            spdlog::warn("[Reload] Failed to parse config: {}", parsed.error().message);
            return;
        }
        const auto& toml = parsed.value();
        if (toml.find("tuning") == toml.end()) {
            spdlog::info("[Reload] No [tuning] section in config; nothing to apply");
            return;
        }
        const auto& tune = toml.at("tuning");
        auto as_int = [&](const char* k) -> std::optional<int> {
            auto it = tune.find(k);
            if (it == tune.end())
                return std::nullopt;
            try {
                return std::stoi(it->second);
            } catch (...) {
                return std::nullopt;
            }
        };
        TuningConfig tc = config_.tuning; // start from current
        if (auto v = as_int("target_cpu_percent"))
            tc.targetCpuPercent = static_cast<uint32_t>(*v);
        if (auto v = as_int("post_ingest_capacity"))
            tc.postIngestCapacity = static_cast<std::size_t>(*v);
        if (auto v = as_int("post_ingest_threads_min"))
            tc.postIngestThreadsMin = static_cast<std::size_t>(*v);
        if (auto v = as_int("post_ingest_threads_max"))
            tc.postIngestThreadsMax = static_cast<std::size_t>(*v);
        if (auto v = as_int("admit_warn_threshold"))
            tc.admitWarnThreshold = static_cast<std::size_t>(*v);
        if (auto v = as_int("admit_stop_threshold"))
            tc.admitStopThreshold = static_cast<std::size_t>(*v);
        if (auto v = as_int("control_interval_ms"))
            tc.controlIntervalMs = static_cast<uint32_t>(*v);
        if (auto v = as_int("hold_ms"))
            tc.holdMs = static_cast<uint32_t>(*v);
        config_.tuning = tc;
        if (serviceManager_) {
            serviceManager_->setTuningConfig(tc);
        }
        spdlog::info("[Reload] Applied tuning config: cap={}, threads={}..{}",
                     tc.postIngestCapacity, tc.postIngestThreadsMin, tc.postIngestThreadsMax);
    } catch (const std::exception& e) {
        spdlog::warn("[Reload] Exception during tuning reload: {}", e.what());
    } catch (...) {
        spdlog::warn("[Reload] Unknown error during tuning reload");
    }
}

void YamsDaemon::spawnShutdownThread(std::function<void()> shutdownFn) {
    std::lock_guard<std::mutex> lock(shutdownThreadMutex_);
    if (shutdownThread_.joinable()) {
        return;
    }
    shutdownThread_ = std::thread(std::move(shutdownFn));
}

} // namespace yams::daemon

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
using uid_t = int;
inline uid_t getuid() {
    return 0;
}
inline uid_t geteuid() {
    return 0;
}
#endif
