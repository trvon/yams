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
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/connection_fsm.h>
#include <yams/daemon/ipc/resource_tuner.h>
#include <yams/daemon/resource/plugin_loader.h>

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
} // namespace
// IPC server implementation removed in PBI-007; client uses Boost.Asio path.

namespace yams::daemon {

YamsDaemon::YamsDaemon(const DaemonConfig& config) : config_(config) {
    // Resolve paths if not explicitly set
    if (config_.socketPath.empty()) {
        // Prefer config-first resolution to stay consistent with client/CLI
        // Honors [daemon].socket_path in config.toml and env overrides.
        config_.socketPath = yams::daemon::ConnectionFsm::resolve_socket_path_config_first();
    }
    if (config_.pidFile.empty()) {
        config_.pidFile = resolveSystemPath(PathType::PidFile);
    }
    if (config_.logFile.empty()) {
        config_.logFile = resolveSystemPath(PathType::LogFile);
    }

    // Initialize logging sink early so all subsequent logs go to file
    try {
        auto existing = spdlog::get("daemon");
        if (!existing) {
            // Use rotating file sink to preserve logs across crashes
            const size_t max_size = 10 * 1024 * 1024; // 10MB per file
            const size_t max_files = 5;               // Keep 5 rotated files
            auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                config_.logFile.string(), max_size, max_files);

            // Initialize a small async logging thread pool for cross-thread logging

            auto logger = std::make_shared<spdlog::logger>("daemon", rotating_sink);
            spdlog::set_default_logger(logger);
            spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%t] [%^%l%$] %v");
            // Honor YAMS_LOG_LEVEL if set; default to 'info' to reduce idle load
            auto lvl = spdlog::level::info;
            try {
                if (const char* env = std::getenv("YAMS_LOG_LEVEL")) {
                    std::string v(env);
                    for (auto& c : v)
                        c = static_cast<char>(std::tolower(c));
                    if (v == "trace")
                        lvl = spdlog::level::trace;
                    else if (v == "debug")
                        lvl = spdlog::level::debug;
                    else if (v == "info")
                        lvl = spdlog::level::info;
                    else if (v == "warn" || v == "warning")
                        lvl = spdlog::level::warn;
                    else if (v == "error")
                        lvl = spdlog::level::err;
                    else if (v == "critical" || v == "fatal")
                        lvl = spdlog::level::critical;
                    else if (v == "off")
                        lvl = spdlog::level::off;
                }
            } catch (...) {
            }
            spdlog::set_level(lvl);
        } else {
            spdlog::set_default_logger(existing);
            // Existing logger present: still honor YAMS_LOG_LEVEL override
            auto lvl = spdlog::level::info;
            try {
                if (const char* env = std::getenv("YAMS_LOG_LEVEL")) {
                    std::string v(env);
                    for (auto& c : v)
                        c = static_cast<char>(std::tolower(c));
                    if (v == "trace")
                        lvl = spdlog::level::trace;
                    else if (v == "debug")
                        lvl = spdlog::level::debug;
                    else if (v == "info")
                        lvl = spdlog::level::info;
                    else if (v == "warn" || v == "warning")
                        lvl = spdlog::level::warn;
                    else if (v == "error")
                        lvl = spdlog::level::err;
                    else if (v == "critical" || v == "fatal")
                        lvl = spdlog::level::critical;
                    else if (v == "off")
                        lvl = spdlog::level::off;
                }
            } catch (...) {
            }
            spdlog::set_level(lvl);
        }
    } catch (const std::exception& e) {
        // Fall back to default stderr logger if file cannot be created
        spdlog::warn("Failed to initialize file logger ({}), continuing with default logger",
                     e.what());
        auto lvl = spdlog::level::info;
        try {
            if (const char* env = std::getenv("YAMS_LOG_LEVEL")) {
                std::string v(env);
                for (auto& c : v)
                    c = static_cast<char>(std::tolower(c));
                if (v == "trace")
                    lvl = spdlog::level::trace;
                else if (v == "debug")
                    lvl = spdlog::level::debug;
                else if (v == "info")
                    lvl = spdlog::level::info;
                else if (v == "warn" || v == "warning")
                    lvl = spdlog::level::warn;
                else if (v == "error")
                    lvl = spdlog::level::err;
                else if (v == "critical" || v == "fatal")
                    lvl = spdlog::level::critical;
                else if (v == "off")
                    lvl = spdlog::level::off;
            }
        } catch (...) {
        }
        spdlog::set_level(lvl);
    }

    lifecycleManager_ = std::make_unique<LifecycleComponent>(this, config_.pidFile);
    serviceManager_ = std::make_unique<ServiceManager>(config_, state_);
    // Create metrics aggregator before the dispatcher so status can pull from a single snapshot
    metrics_ = std::make_unique<DaemonMetrics>(&lifecycleFsm_, &state_, serviceManager_.get());
    requestDispatcher_ =
        std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_, metrics_.get());
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

    // Ensure configured pluginDir is honored even when constructing daemon directly (tests) and
    // not via daemon_main path which already sets configured plugin directories.
    try {
        if (!config_.pluginDir.empty()) {
            PluginLoader::setConfiguredPluginDirectories({config_.pluginDir});
            spdlog::info("Configured plugin directory set to: {}", config_.pluginDir.string());
        }
    } catch (const std::exception& e) {
        spdlog::warn("Failed to set configured plugin directories: {}", e.what());
    } catch (...) {
        spdlog::warn("Unknown error while setting configured plugin directories");
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
    lifecycleFsm_.reset();

    if (auto result = lifecycleManager_->initialize(); !result) {
        running_ = false;
        return result;
    }

    // Transition to Initializing as soon as core lifecycle is bootstrapped,
    // before kicking off async service initialization to avoid race with HealthyEvent.
    lifecycleFsm_.dispatch(BootstrappedEvent{});

    // Start integrated socket server ASAP so status requests work during initialization
    SocketServer::Config socketConfig;
    socketConfig.socketPath = config_.socketPath;
    socketConfig.workerThreads = config_.workerThreads;
    socketConfig.maxConnections = 100; // TODO: make configurable

    socketServer_ = std::make_unique<SocketServer>(socketConfig, requestDispatcher_.get(), &state_);

    if (auto result = socketServer_->start(); !result) {
        running_ = false;
        serviceManager_->shutdown();
        lifecycleManager_->shutdown();
        return Error{ErrorCode::IOError,
                     "Failed to start socket server: " + result.error().message};
    }

    // Mark IPC as ready now that socket server is running
    state_.readiness.ipcServerReady = true;
    spdlog::info("Socket server started on {}", config_.socketPath.string());

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
    serviceManager_->setInitCompleteCallback([this](bool success, const std::string& error) {
        if (success) {
            lifecycleFsm_.dispatch(HealthyEvent{});
            // Note: Actual RepairCoordinator start is deferred to daemon loop once
            // searchEngineReady is true to avoid competing with search/vectors initialization.
        } else {
            lifecycleFsm_.dispatch(FailureEvent{error});
        }
    });

    // Kick off service initialization (async)
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

    // Defer RepairCoordinator start until lifecycle is Healthy/Ready to avoid competing with init

    // Bootstrapped event already dispatched before service initialization to prevent race.

    // Start lightweight main loop thread; no in-process IPC acceptor
    daemonThread_ = yams::compat::jthread([this](yams::compat::stop_token token) {
        set_current_thread_name("yams-daemon-main");
        spdlog::debug("Daemon main loop started.");
        // Drive lifecycle FSM periodically
        lifecycleFsm_.tick();
        // Allow tuning of the main loop tick for metrics refresh and readiness nudges
        uint32_t tick_ms = TuneAdvisor::statusTickMs();
        while (!token.stop_requested() && !stopRequested_.load()) {
            std::unique_lock<std::mutex> lock(stop_mutex_);
            if (stop_cv_.wait_for(lock, std::chrono::milliseconds(tick_ms),
                                  [&] { return stopRequested_.load(); })) {
                // Shutdown requested: inform lifecycle FSM and exit loop
                lifecycleFsm_.dispatch(ShutdownRequestedEvent{});
                break;
            }
            // Periodically refresh metrics snapshot cache to ensure fast status replies
            try {
                if (metrics_) {
                    metrics_->refresh();
                    // Feed ResourceTuner with centralized metrics to adjust pools
                    auto snap = metrics_->getSnapshot();
                    std::uint64_t workerThreads = 0;
                    try {
                        workerThreads = static_cast<std::uint64_t>(
                            serviceManager_ ? serviceManager_->getWorkerThreads() : 0);
                    } catch (...) {
                    }
                    ResourceTuner::instance().updateLoadHints(
                        snap.cpuUsagePercent,
                        static_cast<std::uint64_t>(snap.muxQueuedBytes >= 0 ? snap.muxQueuedBytes
                                                                            : 0),
                        static_cast<std::uint64_t>(snap.workerQueued), workerThreads,
                        static_cast<std::uint64_t>(snap.activeConnections));
                }
            } catch (...) {
            }
            // Deferred RepairCoordinator start: require FSM Ready and searchEngineReady
            if (config_.enableAutoRepair && !repairCoordinator_) {
                auto snap = lifecycleFsm_.snapshot();
                if (snap.state == LifecycleState::Ready &&
                    state_.readiness.searchEngineReady.load()) {
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
            }
            // Live-tune RepairCoordinator scheduling using TuneAdvisor and load
            if (repairCoordinator_) {
                size_t active = static_cast<size_t>(state_.stats.activeConnections.load());
                uint32_t busyThresh = TuneAdvisor::repairBusyConnThreshold();
                // Hysteresis on busy/idle
                auto now = std::chrono::steady_clock::now();
                bool isBusy = (active >= busyThresh);
                if (isBusy) {
                    if (repairBusySince_.time_since_epoch().count() == 0)
                        repairBusySince_ = now;
                    repairReadySince_ = {};
                } else {
                    if (repairReadySince_.time_since_epoch().count() == 0)
                        repairReadySince_ = now;
                    repairBusySince_ = {};
                }
                uint32_t degradeHold = TuneAdvisor::repairDegradeHoldMs();
                uint32_t readyHold = TuneAdvisor::repairReadyHoldMs();
                bool busyHeld =
                    isBusy && repairBusySince_.time_since_epoch().count() != 0 &&
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - repairBusySince_)
                            .count() >= degradeHold;
                bool idleHeld =
                    !isBusy && repairReadySince_.time_since_epoch().count() != 0 &&
                    std::chrono::duration_cast<std::chrono::milliseconds>(now - repairReadySince_)
                            .count() >= readyHold;

                uint32_t tokens =
                    busyHeld ? TuneAdvisor::repairTokensBusy() : TuneAdvisor::repairTokensIdle();
                uint32_t batch = TuneAdvisor::repairMaxBatch();
                repairCoordinator_->setMaintenanceTokens(tokens);
                repairCoordinator_->setMaxBatch(batch);

                // Rate limiting: cap batches per second by temporarily disabling tokens
                uint32_t maxPerSec = TuneAdvisor::repairMaxBatchesPerSec();
                if (maxPerSec > 0) {
                    auto curBatches = state_.stats.repairBatchesAttempted.load();
                    if (repairRateWindowStart_.time_since_epoch().count() == 0) {
                        repairRateWindowStart_ = now;
                        repairBatchesAtWindowStart_ = curBatches;
                    } else {
                        auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                             now - repairRateWindowStart_)
                                             .count();
                        auto produced = (curBatches >= repairBatchesAtWindowStart_)
                                            ? (curBatches - repairBatchesAtWindowStart_)
                                            : 0;
                        if (elapsedMs < 1000) {
                            if (produced >= maxPerSec) {
                                // Pause new batches this window
                                repairCoordinator_->setMaintenanceTokens(0);
                            }
                        } else {
                            // reset window
                            repairRateWindowStart_ = now;
                            repairBatchesAtWindowStart_ = curBatches;
                        }
                    }
                }

                // FSM signaling: mark Degraded when repairs pending and system sustained busy with
                // no tokens
                bool pending = state_.stats.repairQueueDepth.load() > 0;
                auto snap = lifecycleFsm_.snapshot();
                if (snap.state == LifecycleState::Ready) {
                    if (pending && busyHeld && tokens == 0) {
                        lifecycleFsm_.dispatch(DegradedEvent{});
                    }
                } else if (snap.state == LifecycleState::Degraded) {
                    if (!pending || idleHeld) {
                        lifecycleFsm_.dispatch(HealthyEvent{});
                    }
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
            // Delegate to centralized FSM resolver for consistency
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
