#pragma once

#include <yams/core/types.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/resource/onnx_model_pool.h> // For DaemonConfig

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <yams/compat/thread_stop_compat.h>
//

namespace yams::daemon {

// Forward declarations for components
class LifecycleComponent;
class ServiceManager;
class RequestDispatcher;
class DaemonMetrics;
class RepairCoordinator;
class SocketServer;
// Forward decls for GTEST-only accessors are below guarded by YAMS_TESTING

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
    std::string pluginNamePolicy{"relaxed"};
    // Default to disabled to avoid unintended plugin loading in plain daemon
    // invocations and tests that don't explicitly opt in. Tests that rely on
    // plugins (e.g., ONNX embeddings) set this to true in their config.
    bool autoLoadPlugins = false;
    bool enableAutoRepair = true;
    bool useMockModelProvider = false;
    size_t autoRepairBatchSize = 32;
    size_t maxPendingRepairs = 1000;

    // Streaming keepalive configuration
    std::chrono::milliseconds heartbeatInterval{500}; // default 500ms
    std::chrono::milliseconds heartbeatJitter{50};    // default +/-50ms applied per tick

    // Forward decls for GTEST-only accessors are below guarded by YAMS_TESTING
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
    // Lifecycle FSM accessor
    const DaemonLifecycleFsm& getLifecycle() const { return lifecycleFsm_; }
    // Mark or clear subsystem degradation (sticky until cleared by operator action)
    void setSubsystemDegraded(const std::string& name, bool degraded,
                              const std::string& reason = std::string()) {
        lifecycleFsm_.setSubsystemDegraded(name, degraded, reason);
    }

    // Socket server accessor (for testing)
    SocketServer* getSocketServer() const { return socketServer_.get(); }

    // Document event notifications - forwarded to RepairCoordinator
    void onDocumentAdded(const std::string& hash, const std::string& path);
    void onDocumentRemoved(const std::string& hash);

    // Path resolution helpers
    enum class PathType { Socket, PidFile, LogFile };
    static std::filesystem::path resolveSystemPath(PathType type);
    static bool canWriteToDirectory(const std::filesystem::path& dir);
    static std::filesystem::path getXDGRuntimeDir();
    DaemonConfig config_;
    StateComponent state_;
    std::unique_ptr<LifecycleComponent> lifecycleManager_;
    std::unique_ptr<ServiceManager> serviceManager_;
    std::unique_ptr<RequestDispatcher> requestDispatcher_;
    std::unique_ptr<DaemonMetrics> metrics_;
    // Integrated socket server (replaces external yams-socket-server)
    std::unique_ptr<SocketServer> socketServer_;
    std::unique_ptr<RepairCoordinator> repairCoordinator_;

    // Lifecycle FSM (authoritative lifecycle state)
    DaemonLifecycleFsm lifecycleFsm_{};

    // Threading and state
    std::atomic<bool> running_{false};
    std::atomic<bool> stopRequested_{false};
    yams::compat::jthread daemonThread_;
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
    // Deferred repair startup control
    std::atomic<bool> repairStarted_{false};
    std::chrono::steady_clock::time_point repairIdleSince_{};
    // Hysteresis + rate limit tracking for repair
    std::chrono::steady_clock::time_point repairBusySince_{};
    std::chrono::steady_clock::time_point repairReadySince_{};
    std::chrono::steady_clock::time_point repairRateWindowStart_{};
    uint64_t repairBatchesAtWindowStart_{0};

public:
    // On-demand plugin autoload bridge
    Result<size_t> autoloadPluginsNow();
    ServiceManager* getServiceManager() const { return serviceManager_.get(); }
};

} // namespace yams::daemon
