#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <fstream>
#include <thread>
#include <unistd.h> // For getuid(), geteuid(), getpid()
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/components/RepairCoordinator.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
// IPC server implementation removed in PBI-007; client uses Boost.Asio path.

namespace yams::daemon {

YamsDaemon::YamsDaemon(const DaemonConfig& config) : config_(config) {
    // Resolve paths if not explicitly set
    if (config_.socketPath.empty()) {
        config_.socketPath = resolveSystemPath(PathType::Socket);
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
            auto logger = spdlog::basic_logger_mt("daemon", config_.logFile.string(), true);
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
    daemonFsm_ = std::make_unique<DaemonFSM>();
    serviceManager_ = std::make_unique<ServiceManager>(config_, state_);
    serviceManager_->setFsm(daemonFsm_.get());
    requestDispatcher_ = std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_);
    requestDispatcher_->setFsm(daemonFsm_.get());
    state_.stats.startTime = std::chrono::steady_clock::now();

    spdlog::info("Daemon configuration resolved:");
    spdlog::info("  Data dir: {}", config_.dataDir.string());
    spdlog::info("  Socket: {}", config_.socketPath.string());
    spdlog::info("  PID file: {}", config_.pidFile.string());
    spdlog::info("  Log file: {}", config_.logFile.string());
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

    if (auto result = lifecycleManager_->initialize(); !result) {
        running_ = false;
        return result;
    }

    if (auto result = serviceManager_->initialize(); !result) {
        running_ = false;
        lifecycleManager_->shutdown();
        return result;
    }

    // Start in-process UNIX socket IPC server (Boost.Asio-backed I/O)
    state_.readiness.ipcServerReady = false;
    {
        UnixIpcServer::Config scfg;
        scfg.socketPath = config_.socketPath;
        scfg.backlog = 128;
        scfg.permission_octal = 0660;
        // Align handler config with reasonable defaults
        scfg.handlerCfg.enable_streaming = true;
        scfg.handlerCfg.chunk_size = 64 * 1024;
        scfg.handlerCfg.close_after_response = true;
        scfg.handlerCfg.max_frame_size = 10 * 1024 * 1024; // 10MB
        ipcServer_ = std::make_unique<UnixIpcServer>(scfg, requestDispatcher_.get(), &state_);
        if (auto r = ipcServer_->start(); !r) {
            spdlog::error("Failed to start IPC server: {}", r.error().message);
            serviceManager_->shutdown();
            lifecycleManager_->shutdown();
            running_ = false;
            return r;
        }
        if (daemonFsm_)
            daemonFsm_->on(DaemonFSM::Event::IpcListening);
    }

    // Start feature-flagged repair coordinator (idle-only)
    if (config_.enableAutoRepair) {
        RepairCoordinator::Config rcfg;
        rcfg.enable = true;
        rcfg.dataDir = config_.dataDir;
        rcfg.maxBatch = static_cast<std::uint32_t>(config_.autoRepairBatchSize);
        rcfg.tickMs = 2000;
        auto activeFn = [this]() -> size_t {
            // Use IPC server active count if available
            return ipcServer_ ? ipcServer_->active_connections() : 0;
        };
        repairCoordinator_ =
            std::make_unique<RepairCoordinator>(serviceManager_.get(), &state_, activeFn, rcfg);
        repairCoordinator_->start();
    }

    // Start lightweight main loop thread; no in-process IPC acceptor
    daemonThread_ = std::jthread([this](std::stop_token token) {
        spdlog::debug("Daemon main loop started.");
        while (!token.stop_requested() && !stopRequested_.load()) {
            std::unique_lock<std::mutex> lock(stop_mutex_);
            if (stop_cv_.wait_for(lock, std::chrono::seconds(1),
                                  [&] { return stopRequested_.load(); })) {
                break; // Shutdown requested
            }
            // Periodic tasks can be added here
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

    // Stop IPC server
    if (ipcServer_) {
        ipcServer_->stop();
        ipcServer_.reset();
    }

    if (serviceManager_) {
        serviceManager_->shutdown();
    }
    if (daemonFsm_) {
        daemonFsm_->on(DaemonFSM::Event::Stopped);
    }

    if (lifecycleManager_) {
        lifecycleManager_->shutdown();
    }

    // Socket file cleaned up by UnixIpcServer

    spdlog::info("YAMS daemon stopped.");
    return Result<void>();
}

// run() method removed; loop is inlined in start()

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
            if (isRoot)
                return fs::path("/var/run/yams-daemon.sock");
            if (auto xdg = getXDGRuntimeDir(); !xdg.empty() && canWriteToDirectory(xdg))
                return xdg / "yams-daemon.sock";
            return fs::path("/tmp") / ("yams-daemon-" + std::to_string(uid) + ".sock");
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