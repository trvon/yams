#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/ipc/async_ipc_server.h>
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
class LifecycleComponent;
class ServiceManager;
class RequestDispatcher;
class RepairCoordinator;

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

    // Streaming keepalive configuration
    std::chrono::milliseconds heartbeatInterval{500}; // default 500ms
    std::chrono::milliseconds heartbeatJitter{50};    // default +/-50ms applied per tick

    // Daemon-side download policy scaffolding (disabled by default until sandboxed)
    struct DownloadPolicy {
        bool enable{false};                               // feature gate
        std::vector<std::string> allowedHosts{};          // exact or wildcard patterns
        std::vector<std::string> allowedSchemes{"https"}; // schemes to allow (default https)
        bool requireChecksum{true};                       // require algo:hex on requests
        bool storeOnly{true};                     // only write into CAS (no arbitrary paths)
        std::chrono::milliseconds timeout{60000}; // per-request timeout
        std::uint64_t maxFileBytes{0};            // 0 = unlimited
        size_t rateLimitRps{0};                   // 0 = unlimited
        std::string sandbox{"subprocess"};        // future: platform-specific isolation
    } downloadPolicy;
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
    std::unique_ptr<SimpleAsyncIpcServer> ipcServer_;
    std::unique_ptr<RepairCoordinator> repairCoordinator_;

    // Threading and state
    std::atomic<bool> running_{false};
    std::atomic<bool> stopRequested_{false};
    std::jthread daemonThread_;
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
};

} // namespace yams::daemon