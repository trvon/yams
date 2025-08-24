#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/resource/onnx_model_pool.h> // For DaemonConfig

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>

namespace yams::daemon {

// Forward declarations for components
class AsyncIpcServer;
class LifecycleComponent;
class ServiceManager;
class RequestDispatcher;

struct DaemonConfig {
    std::filesystem::path dataDir;
    std::filesystem::path socketPath;
    std::filesystem::path pidFile;
    std::filesystem::path logFile;
    size_t workerThreads = 4;
    size_t maxMemoryGb = 4;
    std::chrono::milliseconds requestTimeout{5000};
    bool healthMonitoring = true;
    std::string logLevel = "info";
    size_t maxLogFiles = 5;
    size_t maxLogSizeMb = 10;
    bool enableModelProvider = false;
    ModelPoolConfig modelPoolConfig;
    std::filesystem::path pluginDir;
    bool autoLoadPlugins = true;
    std::vector<std::string> enabledPlugins;
    bool enableAutoRepair = true;
    size_t autoRepairBatchSize = 32;
    size_t maxPendingRepairs = 1000;
};

class YamsDaemon {
public:
    explicit YamsDaemon(const DaemonConfig& config = {});
    ~YamsDaemon();

    // Lifecycle management
    Result<void> start();
    Result<void> stop();
    void requestStop() {
        stopRequested_ = true;
        stop_cv_.notify_all();
    }
    bool isRunning() const { return running_.load(); }
    bool isStopRequested() const { return stopRequested_.load(); }

    const StateComponent& getState() const { return state_; }

    // Path resolution helpers
    enum class PathType { Socket, PidFile, LogFile };
    static std::filesystem::path resolveSystemPath(PathType type);
    static bool canWriteToDirectory(const std::filesystem::path& dir);
    static std::filesystem::path getXDGRuntimeDir();
    static std::filesystem::path getXDGStateHome();

private:
    void run(std::stop_token stopToken);

    // Core Components
    DaemonConfig config_;
    StateComponent state_;
    std::unique_ptr<LifecycleComponent> lifecycleManager_;
    std::unique_ptr<ServiceManager> serviceManager_;
    std::unique_ptr<RequestDispatcher> requestDispatcher_;
    std::unique_ptr<AsyncIpcServer> ipcServer_;

    // Threading and state
    std::atomic<bool> running_{false};
    std::atomic<bool> stopRequested_{false};
    std::jthread daemonThread_;
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
};

} // namespace yams::daemon