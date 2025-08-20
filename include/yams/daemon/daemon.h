#pragma once

#include <yams/core/types.h>
#include <yams/daemon/resource/onnx_model_pool.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// Forward declarations for cross-component references
namespace yams {
namespace metadata {
class Database;
class ConnectionPool;
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace metadata
namespace search {
class SearchExecutor;
class SearchEngineBuilder;
} // namespace search
namespace vector {
class VectorIndexManager;
class EmbeddingGenerator;
} // namespace vector
} // namespace yams

// Forward declare API store
namespace yams {
namespace api {
class IContentStore;
}
} // namespace yams

namespace yams::daemon {

// Forward declarations
class AsyncIpcServer;
class IModelProvider;
class RetrievalSessionManager;

struct DaemonConfig {
    // Paths will be resolved based on runtime environment (root vs user)
    // Default empty means auto-detect based on permissions
    // Data directory for storage (if empty, daemon will resolve defaults or use YAMS_STORAGE env)
    std::filesystem::path dataDir;
    std::filesystem::path socketPath; // Auto: /var/run or XDG_RUNTIME_DIR or /tmp
    std::filesystem::path pidFile;    // Auto: /var/run or XDG_RUNTIME_DIR or /tmp
    std::filesystem::path logFile;    // Auto: /var/log or XDG_STATE_HOME or ~/.local/state

    size_t workerThreads = 4;
    size_t maxMemoryGb = 4;
    std::chrono::milliseconds requestTimeout{5000};
    bool healthMonitoring = true;
    std::string logLevel = "info";
    size_t maxLogFiles = 5;   // For log rotation
    size_t maxLogSizeMb = 10; // Max size before rotation

    // Model provider configuration
    bool enableModelProvider = false; // Enable model loading and embedding generation
    ModelPoolConfig modelPoolConfig;  // Configuration for ONNX model pool
};

class YamsDaemon {
public:
    explicit YamsDaemon(const DaemonConfig& config = {});
    ~YamsDaemon();

    // Lifecycle management
    Result<void> start();
    Result<void> stop();
    bool isRunning() const { return running_.load(); }

    // Signal handlers
    void handleSignal(int signal);

    // Statistics
    struct Stats {
        std::chrono::steady_clock::time_point startTime;
        std::atomic<size_t> requestsProcessed{0};
        std::atomic<size_t> activeConnections{0};
        std::atomic<size_t> totalConnections{0};
    };

    const Stats& getStats() const { return stats_; }

    // Path resolution helpers (public for use by daemon_main and client)
    enum class PathType { Socket, PidFile, LogFile };

    static std::filesystem::path resolveSystemPath(PathType type);
    static bool canWriteToDirectory(const std::filesystem::path& dir);
    static std::filesystem::path getXDGRuntimeDir();
    static std::filesystem::path getXDGStateHome();

    // Ensure a singleton embedding generator exists and is initialized (thread-safe)
    [[nodiscard]] std::shared_ptr<yams::vector::EmbeddingGenerator>
    ensureEmbeddingGenerator() noexcept;

#ifdef GTEST_API_
    // GTEST-only accessors to seed and inspect daemon internals in tests
    [[nodiscard]] std::shared_ptr<yams::metadata::MetadataRepository>
    _test_getMetadataRepo() const {
        return metadataRepo_;
    }
    [[nodiscard]] std::shared_ptr<yams::vector::VectorIndexManager>
    _test_getVectorIndexManager() const {
        return vectorIndexManager_;
    }
    [[nodiscard]] std::shared_ptr<yams::vector::EmbeddingGenerator>
    _test_getEmbeddingGenerator() const {
        return embeddingGenerator_;
    }
#endif

private:
    // Main daemon loop
    void run(std::stop_token stopToken);

    // PID file management
    Result<void> createPidFile();
    Result<void> removePidFile();
    bool isDaemonRunning() const;

    // Signal handling setup
    void setupSignalHandlers();
    void cleanupSignalHandlers();

    // Resource initialization
    Result<void> initializeResources();
    void shutdownResources();

    // Model preloading with progress tracking
    void preloadModels();

private:
    std::atomic<bool> running_{false};
    std::atomic<bool> stopRequested_{false};
    std::jthread daemonThread_;
    // Preload thread removed - model provider handles initialization
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;

    std::filesystem::path pidFile_;
    std::unique_ptr<AsyncIpcServer> ipcServer_;
    std::unique_ptr<IModelProvider> modelProvider_;
    // Search and metadata resources
    std::shared_ptr<yams::metadata::Database> database_;
    std::shared_ptr<yams::metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<yams::search::SearchExecutor> searchExecutor_;
    std::shared_ptr<yams::api::IContentStore> contentStore_;
    std::unique_ptr<RetrievalSessionManager> retrievalSessions_;
    // Hybrid search resources (daemon-owned, persistent)
    std::shared_ptr<yams::vector::VectorIndexManager> vectorIndexManager_;
    std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGenerator_;
    std::unique_ptr<yams::metadata::KnowledgeGraphStore> knowledgeGraphStore_;
    std::shared_ptr<yams::search::SearchEngineBuilder> searchBuilder_;
    // Synchronization for lazy singleton creation of embedding generator
    mutable std::mutex embeddingMutex_;

    DaemonConfig config_;
    Stats stats_;

    // Static instance for signal handling
    static YamsDaemon* instance_;
    static void signalHandler(int signal);
};

} // namespace yams::daemon
