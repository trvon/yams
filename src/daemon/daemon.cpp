#include <spdlog/spdlog.h>
#include <fstream>
#include <thread>
#include <unistd.h> // For getuid(), geteuid(), getpid()
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/async_ipc_server.h>

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

    lifecycleManager_ = std::make_unique<LifecycleComponent>(this, config_.pidFile);
    serviceManager_ = std::make_unique<ServiceManager>(config_, state_);
    requestDispatcher_ = std::make_unique<RequestDispatcher>(this, serviceManager_.get(), &state_);
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

    SimpleAsyncIpcServer::Config serverConfig;
    serverConfig.socket_path = config_.socketPath;
    serverConfig.worker_threads = config_.workerThreads;
    ipcServer_ = std::make_unique<SimpleAsyncIpcServer>(serverConfig);
    ipcServer_->set_handler([this](const Request& req) -> Response {
        state_.stats.requestsProcessed++;
        return requestDispatcher_->dispatch(req);
    });

    if (auto result = ipcServer_->start(); !result) {
        running_ = false;
        serviceManager_->shutdown();
        lifecycleManager_->shutdown();
        return result;
    }
    state_.readiness.ipcServerReady = true;

    daemonThread_ = std::jthread([this](std::stop_token token) { run(token); });

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

    if (ipcServer_) {
        ipcServer_->stop();
    }

    if (serviceManager_) {
        serviceManager_->shutdown();
    }

    if (lifecycleManager_) {
        lifecycleManager_->shutdown();
    }

    // Clean up socket file
    if (!config_.socketPath.empty() && std::filesystem::exists(config_.socketPath)) {
        try {
            std::filesystem::remove(config_.socketPath);
            spdlog::debug("Removed socket file: {}", config_.socketPath.string());
        } catch (const std::exception& e) {
            spdlog::warn("Failed to remove socket file: {}", e.what());
        }
    }

    spdlog::info("YAMS daemon stopped.");
    return Result<void>();
}

void YamsDaemon::run(std::stop_token stopToken) {
    spdlog::debug("Daemon main loop started.");
    while (!stopToken.stop_requested() && !stopRequested_.load()) {
        std::unique_lock<std::mutex> lock(stop_mutex_);
        if (stop_cv_.wait_for(lock, std::chrono::seconds(1),
                              [&] { return stopRequested_.load(); })) {
            break; // Shutdown requested
        }
        // Periodic tasks can be added here
    }
    spdlog::debug("Daemon main loop exiting.");
}

// Path resolution helpers remain static methods of YamsDaemon
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

std::filesystem::path YamsDaemon::getXDGStateHome() {
    const char* xdgState = std::getenv("XDG_STATE_HOME");
    if (xdgState)
        return std::filesystem::path(xdgState);
    const char* home = std::getenv("HOME");
    return home ? std::filesystem::path(home) / ".local" / "state" : std::filesystem::path();
}

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