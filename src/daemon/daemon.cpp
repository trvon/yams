#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>
#include <fstream>
#include <thread>
#include <unistd.h> // For getuid(), geteuid(), getpid()
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/SocketServer.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/connection_fsm.h>
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
            auto logger = std::make_shared<spdlog::logger>("daemon", rotating_sink);
            spdlog::set_default_logger(logger);
            spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
            spdlog::set_level(spdlog::level::debug);
        } else {
            spdlog::set_default_logger(existing);
            spdlog::set_level(spdlog::level::debug);
        }
    } catch (const std::exception& e) {
        // Fall back to default stderr logger if file cannot be created
        spdlog::warn("Failed to initialize file logger ({}), continuing with default logger",
                     e.what());
        spdlog::set_level(spdlog::level::debug);
    }

    lifecycleManager_ = std::make_unique<LifecycleComponent>(this, config_.pidFile);
    serviceManager_ = std::make_unique<ServiceManager>(config_, state_);
    requestDispatcher_ = std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_);
    state_.stats.startTime = std::chrono::steady_clock::now();

    spdlog::info("Daemon configuration resolved:");
    spdlog::info("  Data dir: {}", config_.dataDir.string());
    spdlog::info("  Socket: {}", config_.socketPath.string());
    spdlog::info("  PID file: {}", config_.pidFile.string());
    spdlog::info("  Log file: {}", config_.logFile.string());

    // Seed process-wide socket override so any code paths that rely on centralized
    // resolution (FSM helpers) observe the same socket path chosen here.
    // This prevents divergence between server-held state and late resolvers.
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

    // Initialize request dispatcher before IPC so Ping/Status can be handled immediately
    requestDispatcher_ = std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_);

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

    // Set callback to transition to Ready state when initialization completes
    serviceManager_->setInitCompleteCallback([this](bool success, const std::string& error) {
        if (success) {
            lifecycleFsm_.dispatch(HealthyEvent{});
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

    // Start feature-flagged repair coordinator (idle-only)
    if (config_.enableAutoRepair) {
        RepairCoordinator::Config rcfg;
        rcfg.enable = true;
        rcfg.dataDir = config_.dataDir;
        rcfg.maxBatch = static_cast<std::uint32_t>(config_.autoRepairBatchSize);
        // Safe lambda: only dereference ipcServer_ while daemon is running; we stop
        // the coordinator before tearing down ipcServer_ in stop().
        auto activeFn = []() -> size_t { return 0; };
        repairCoordinator_ =
            std::make_unique<RepairCoordinator>(serviceManager_.get(), &state_, activeFn, rcfg);
        repairCoordinator_->start();
    }

    // Bootstrapped event already dispatched before service initialization to prevent race.

    // Start lightweight main loop thread; no in-process IPC acceptor
    daemonThread_ = std::jthread([this](std::stop_token token) {
        spdlog::debug("Daemon main loop started.");
        // Drive lifecycle FSM periodically
        lifecycleFsm_.tick();
        while (!token.stop_requested() && !stopRequested_.load()) {
            std::unique_lock<std::mutex> lock(stop_mutex_);
            if (stop_cv_.wait_for(lock, std::chrono::seconds(1),
                                  [&] { return stopRequested_.load(); })) {
                // Shutdown requested: inform lifecycle FSM and exit loop
                lifecycleFsm_.dispatch(ShutdownRequestedEvent{});
                break;
            }
            // Periodic tasks can be added here
            // TODO(PBI-007-06): Prefer DaemonLifecycleFsm as lifecycle authority; call fsm_.tick()
            // here once FSM is wired
        }
        spdlog::debug("Daemon main loop exiting.");
    });

    spdlog::info("YAMS daemon started successfully.");
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
