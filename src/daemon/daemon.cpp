#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <mutex>
#include <thread>
#include <unistd.h> // For getuid(), geteuid(), getpid()
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

YamsDaemon::YamsDaemon(const DaemonConfig& config) : config_(config) {
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
    serviceManager_ = std::make_unique<ServiceManager>(config_, state_);
    // Create metrics aggregator before the dispatcher so status can pull from a single snapshot
    metrics_ = std::make_unique<DaemonMetrics>(&lifecycleFsm_, &state_, serviceManager_.get());
    requestDispatcher_ =
        std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_, metrics_.get());
    // Prepare centralized tuning manager early; start it in start() before sockets/services
    try {
        tuningManager_ = std::make_unique<TuningManager>(serviceManager_.get(), &state_);
        spdlog::debug("TuningManager constructed early in YamsDaemon");
        // Bridge repair control into TuningManager (centralized tokens/batch)
        tuningManager_->setRepairControlHook([this](uint32_t tokens, uint32_t batch) {
            try {
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
}

Result<size_t> YamsDaemon::autoloadPluginsNow() {
    if (!serviceManager_) {
        return Error{ErrorCode::NotInitialized, "ServiceManager not initialized"};
    }
    return serviceManager_->autoloadPluginsNow();
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
    socketConfig.workerThreads = config_.workerThreads;
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
        socketConfig.maxConnections = static_cast<std::size_t>(cap);
    }

    socketServer_ = std::make_unique<SocketServer>(socketConfig, requestDispatcher_.get(), &state_);

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

    // Plugin auto-loading is managed by ServiceManager after core readiness; avoid duplicate loads
    // here

    // Set callback to transition to Ready state when initialization completes
    spdlog::info("[Startup] Phase: ServiceManager Set Callback");
    serviceManager_->setInitCompleteCallback([this](bool success, const std::string& error) {
        spdlog::info("Init complete callback invoked: success={}, error={}", success, error);
        if (success) {
            spdlog::info("Dispatching HealthyEvent to lifecycleFsm_");
            lifecycleFsm_.dispatch(HealthyEvent{});
            spdlog::info("HealthyEvent dispatched, lifecycle should now be Ready");
            // Start background metrics polling to keep cache hot for fast status responses
            try {
                if (metrics_) {
                    metrics_->startPolling();
                    spdlog::info("DaemonMetrics background polling started");
                }
            } catch (const std::exception& e) {
                spdlog::warn("Failed to start metrics polling: {}", e.what());
            }
            // Note: Actual RepairCoordinator start is deferred to daemon loop once
            // searchEngineReady is true to avoid competing with search/vectors initialization.
        } else {
            spdlog::error("Init failed, dispatching FailureEvent: {}", error);
            lifecycleFsm_.dispatch(FailureEvent{error});
        }
    });

    // Kick off service initialization (async)
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

    // Defer RepairCoordinator start until lifecycle is Healthy/Ready to avoid competing with init

    // Bootstrapped event already dispatched before service initialization to prevent race.

    // Start lightweight main loop thread; no in-process IPC acceptor
    daemonThread_ = yams::compat::jthread([this](yams::compat::stop_token token) {
        set_current_thread_name("yams-daemon-main");
        spdlog::debug("Daemon main loop started.");
        // Drive lifecycle FSM periodically
        lifecycleFsm_.tick();
        while (!token.stop_requested() && !stopRequested_.load()) {
#if defined(TRACY_ENABLE)
            YAMS_FRAME_MARK_START("daemon_tick");
#endif
            // Allow tuning of the main loop tick for metrics refresh and readiness nudges.
            // Read each iteration so env changes take effect without restart.
            uint32_t tick_ms = TuneAdvisor::statusTickMs();
            std::unique_lock<std::mutex> lock(stop_mutex_);
            if (stop_cv_.wait_for(lock, std::chrono::milliseconds(tick_ms),
                                  [&] { return stopRequested_.load(); })) {
                // Shutdown requested: inform lifecycle FSM and exit loop
                lifecycleFsm_.dispatch(ShutdownRequestedEvent{});

#if defined(TRACY_ENABLE)
                YAMS_FRAME_MARK_END("daemon_tick");
#endif
                break;
            }
            // Periodically refresh metrics snapshot cache to ensure fast status replies
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
            // Deferred background tasks: require FSM Ready and searchEngineReady
            auto snap = lifecycleFsm_.snapshot();
            if (snap.state == LifecycleState::Ready && state_.readiness.searchEngineReady.load()) {
                // Start RepairCoordinator if enabled
                if (config_.enableAutoRepair && !repairCoordinator_) {
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

                if (serviceManager_ && serviceManager_->shouldPreloadEmbeddings()) {
                    serviceManager_->scheduleEmbeddingWarmup();
                }

                // Model preload disabled - models will load on-demand when first embedding is
                // requested This avoids complexity with thread pool executors and ensures fast
                // daemon startup
                static bool modelPreloadSkipped = false;
                if (!modelPreloadSkipped) {
                    modelPreloadSkipped = true;
                    spdlog::info("Model preload disabled - models will load on first use");
                }
            }
            // RepairCoordinator tokens are tuned by TuningManager. Keep FSM signaling only.
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
                auto snap = lifecycleFsm_.snapshot();
                if (snap.state == LifecycleState::Ready) {
                    if (pending && busyHeld)
                        lifecycleFsm_.dispatch(DegradedEvent{});
                } else if (snap.state == LifecycleState::Degraded) {
                    if (!pending || idleHeld)
                        lifecycleFsm_.dispatch(HealthyEvent{});
                }
            }
            // Periodic tasks can be added here
            // TODO(PBI-007-06): Prefer DaemonLifecycleFsm as lifecycle authority; call fsm_.tick()
            // here once FSM is wired

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
    });

    spdlog::info("YAMS daemon started successfully.");
    spdlog::info("Hint: If this is a new or migrated data directory, run 'yams repair "
                 "--embeddings' to generate vector embeddings.");
    spdlog::info("Hint: Use 'yams stats --verbose' to monitor worker pool (threads/active/queued) "
                 "and streaming metrics.");
    return Result<void>();
}

Result<void> YamsDaemon::stop() {
    if (!running_.exchange(false)) {
        return Error{ErrorCode::InvalidState, "Daemon not running"};
    }

    spdlog::info("Stopping YAMS daemon...");

    // Stop socket server first to prevent new connections
    if (socketServer_) {
        spdlog::debug("Stopping socket server...");
        socketServer_->stop();
        socketServer_.reset();
        state_.readiness.ipcServerReady = false;
    }

    // Stop tuning manager to cease pool adjustments before tearing down services
    if (tuningManager_) {
        try {
            tuningManager_->stop();
        } catch (...) {
        }
        tuningManager_.reset();
    }

    stopRequested_ = true;
    stop_cv_.notify_all();

    if (daemonThread_.joinable()) {
        daemonThread_.join();
    }

    // Stop background coordinator before tearing down IPC/services it references
    if (repairCoordinator_) {
        repairCoordinator_->stop();
        repairCoordinator_.reset();
    }

    // No in-process IPC server to stop

    if (serviceManager_) {
        serviceManager_->shutdown();
    }

    if (lifecycleManager_) {
        lifecycleManager_->shutdown();
    }

    // No socket file created by in-process server

    // Mark lifecycle stopped
    lifecycleFsm_.dispatch(StoppedEvent{});

    spdlog::info("YAMS daemon stopped.");
    return Result<void>();
}

// run() method removed; loop is inlined in start()

void YamsDaemon::onDocumentAdded(const std::string& hash, const std::string& path) {
    if (repairCoordinator_) {
        RepairCoordinator::DocumentAddedEvent event{hash, path};
        repairCoordinator_->onDocumentAdded(event);
    }
}

void YamsDaemon::onDocumentRemoved(const std::string& hash) {
    if (repairCoordinator_) {
        RepairCoordinator::DocumentRemovedEvent event{hash};
        repairCoordinator_->onDocumentRemoved(event);
    }
}

// Path resolution helpers remain static methods of YamsDaemon
namespace {
std::filesystem::path getXDGStateHome() {
    const char* xdgState = std::getenv("XDG_STATE_HOME");
    if (xdgState)
        return std::filesystem::path(xdgState);
    const char* home = std::getenv("HOME");
    return home ? std::filesystem::path(home) / ".local" / "state" : std::filesystem::path();
}
} // namespace

std::filesystem::path YamsDaemon::resolveSystemPath(PathType type) {
    namespace fs = std::filesystem;
    bool isRoot = (geteuid() == 0);
    uid_t uid = getuid();

    switch (type) {
        case PathType::Socket:
            return yams::daemon::ConnectionFsm::resolve_socket_path();
        case PathType::PidFile:
            if (isRoot)
                return fs::path("/var/run/yams-daemon.pid");
            if (auto xdg = getXDGRuntimeDir(); !xdg.empty() && canWriteToDirectory(xdg))
                return xdg / "yams-daemon.pid";
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".pid");
        case PathType::LogFile:
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
    }
    return fs::path();
}

bool YamsDaemon::canWriteToDirectory(const std::filesystem::path& dir) {
    namespace fs = std::filesystem;
    if (!fs::exists(dir))
        return false;
    auto testFile = dir / (".yams-test-" + std::to_string(getpid()));
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
    const char* xdgRuntime = std::getenv("XDG_RUNTIME_DIR");
    return xdgRuntime ? std::filesystem::path(xdgRuntime) : std::filesystem::path();
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

} // namespace yams::daemon
