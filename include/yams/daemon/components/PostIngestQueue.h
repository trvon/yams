#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <cstddef>
#include <unordered_map>
#include <variant>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <yams/daemon/components/GradientLimiter.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/query_concept_extractor.h>

namespace yams {
namespace api {
class IContentStore;
}
namespace metadata {
class MetadataRepository;
}
namespace extraction {
class IContentExtractor;
}
namespace vector {
class VectorDatabase;
} // namespace vector
} // namespace yams

namespace yams::daemon {
class ExternalEntityProviderAdapter;
class IModelProvider;
class WorkCoordinator;
class GraphComponent;
class KGWriteQueue;

// Simple LRU cache for metadata lookups
// Template parameters: Key type, Value type
template <typename K, typename V> class LruCache {
public:
    explicit LruCache(size_t maxSize = 1000, std::chrono::seconds ttl = std::chrono::seconds(5))
        : maxSize_(maxSize), ttl_(ttl) {}

    std::optional<V> get(const K& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it == map_.end()) {
            return std::nullopt;
        }

        // Check TTL
        auto now = std::chrono::steady_clock::now();
        if (now - it->second.timestamp > ttl_) {
            map_.erase(it);
            return std::nullopt;
        }

        // Move to front (most recently used)
        touch(it);
        return it->second.value;
    }

    void put(const K& key, V value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            it->second.value = std::move(value);
            it->second.timestamp = std::chrono::steady_clock::now();
            touch(it);
        } else {
            if (map_.size() >= maxSize_) {
                evict_lru();
            }
            auto now = std::chrono::steady_clock::now();
            order_.push_front(key);
            map_.emplace(key, CacheEntry{std::move(value), now, order_.begin()});
        }
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        map_.clear();
        order_.clear();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return map_.size();
    }

private:
    using Timestamp = std::chrono::steady_clock::time_point;
    using OrderIterator = typename std::list<K>::iterator;

    struct CacheEntry {
        V value;
        Timestamp timestamp;
        OrderIterator orderIt;
    };

    mutable std::mutex mutex_;
    std::unordered_map<K, CacheEntry> map_;
    std::list<K> order_;
    size_t maxSize_;
    std::chrono::seconds ttl_;

    void touch(typename std::unordered_map<K, CacheEntry>::iterator it) {
        order_.erase(it->second.orderIt);
        order_.push_front(it->first);
        it->second.orderIt = order_.begin();
    }

    void evict_lru() {
        if (!order_.empty()) {
            map_.erase(order_.back());
            order_.pop_back();
        }
    }
};

// Metadata cache structures
struct MetadataCache {
    static constexpr size_t kMaxEntries = 1000;
    static constexpr std::chrono::seconds kTtl{5};

    LruCache<std::string, metadata::DocumentInfo> infoCache{kMaxEntries, kTtl};
    LruCache<int64_t, std::vector<std::string>> tagsCache{kMaxEntries, kTtl};
};

class PostIngestQueue {
public:
    static constexpr double kKgBackpressureThreshold = 0.95;

    struct Task {
        std::string hash;
        std::string mime;
        std::string session;
        std::chrono::steady_clock::time_point enqueuedAt;
        enum class Stage : uint8_t { Metadata = 0, KnowledgeGraph = 1 } stage;
    };

    /// Result of successful text extraction, ready for batch DB insertion.
    struct PreparedMetadataEntry {
        int64_t documentId = 0;
        std::string hash;
        std::string fileName;
        std::string filePath; // Full path for KG node creation
        std::string title;
        std::string extractedText;
        std::string mimeType;
        std::string extension;
        std::vector<std::string> tags;
        std::string language;
        std::shared_ptr<std::vector<std::byte>> contentBytes;

        // Dispatch flags (determined during preparation)
        bool shouldDispatchKg = true;
        bool shouldDispatchSymbol = false;
        std::string symbolLanguage;
        bool shouldDispatchEntity = false;
        bool shouldDispatchTitle = false;
        bool shouldDispatchEmbed = true;
        std::string titleTextSnippet; // First N chars for async GLiNER title extraction
    };

    /// Result of failed text extraction, requires status update.
    struct ExtractionFailure {
        int64_t documentId = 0;
        std::string hash;
        std::string errorMessage;
    };

    PostIngestQueue(std::shared_ptr<api::IContentStore> store,
                    std::shared_ptr<metadata::MetadataRepository> meta,
                    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors,
                    std::shared_ptr<metadata::KnowledgeGraphStore> kg,
                    std::shared_ptr<GraphComponent> graphComponent, WorkCoordinator* coordinator,
                    WorkCoordinator* entityCoordinator, std::size_t capacity = 1000);
    ~PostIngestQueue();

    void start();
    void stop();

    void enqueue(Task t);
    void enqueueBatch(std::vector<Task> tasks);
    bool tryEnqueue(const Task& t);
    bool tryEnqueue(Task&& t);

    std::size_t size() const;
    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }

    // File/directory add tracking
    std::uint64_t filesAdded() const { return filesAdded_.load(); }
    std::uint64_t directoriesAdded() const { return directoriesAdded_.load(); }
    std::uint64_t filesProcessed() const { return filesProcessed_.load(); }
    std::uint64_t directoriesProcessed() const { return directoriesProcessed_.load(); }
    void incFilesAdded(std::uint64_t count = 1) {
        filesAdded_.fetch_add(count, std::memory_order_relaxed);
    }
    void incDirectoriesAdded(std::uint64_t count = 1) {
        directoriesAdded_.fetch_add(count, std::memory_order_relaxed);
    }
    void incFilesProcessed(std::uint64_t count = 1) {
        filesProcessed_.fetch_add(count, std::memory_order_relaxed);
    }
    void incDirectoriesProcessed(std::uint64_t count = 1) {
        directoriesProcessed_.fetch_add(count, std::memory_order_relaxed);
    }
    double latencyMsEma() const { return latencyMsEma_.load(); }
    double ratePerSecEma() const { return ratePerSecEma_.load(); }
    std::size_t capacity() const { return capacity_.load(std::memory_order_relaxed); }

    // Backpressure observability
    std::uint64_t backpressureRejects() const {
        return backpressureRejects_.load(std::memory_order_relaxed);
    }
    double kgFillRatio(std::size_t* depthOut = nullptr, std::size_t* capacityOut = nullptr) const {
        return kgChannelFillRatio(depthOut, capacityOut);
    }

    bool started() const { return stageStarted_[0].load(); }

    // Per-stage inflight counts
    std::size_t extractionInFlight() const { return stageInFlight_[0].load(); }
    std::size_t kgInFlight() const { return stageInFlight_[1].load(); }
    std::size_t symbolInFlight() const { return stageInFlight_[2].load(); }
    std::size_t entityInFlight() const { return stageInFlight_[3].load(); }
    std::size_t titleInFlight() const { return stageInFlight_[4].load(); }
    std::size_t totalInFlight() const {
        std::size_t total = 0;
        for (std::size_t i = 0; i < kStageCount; ++i) {
            total += stageInFlight_[i].load();
        }
        return total;
    }

    // Per-stage queue depths (approximate, from channel sizes)
    std::size_t kgQueueDepth() const;
    std::size_t symbolQueueDepth() const;
    std::size_t entityQueueDepth() const;
    std::size_t titleQueueDepth() const;

    // Stage concurrency limits (dynamic via TuneAdvisor)
    static std::size_t maxExtractionConcurrent();
    static std::size_t maxKgConcurrent();
    static std::size_t maxSymbolConcurrent();
    static std::size_t maxEntityConcurrent();
    static std::size_t maxTitleConcurrent();

    // Gradient-based adaptive limiter accessors (for TuningManager)
    GradientLimiter* extractionLimiter() const { return limiters_[0].get(); }
    GradientLimiter* kgLimiter() const { return limiters_[1].get(); }
    GradientLimiter* symbolLimiter() const { return limiters_[2].get(); }
    GradientLimiter* entityLimiter() const { return limiters_[3].get(); }
    GradientLimiter* titleLimiter() const { return limiters_[4].get(); }
    GradientLimiter* embedLimiter() const { return limiters_[5].get(); }

    void setCapacity(std::size_t cap) {
        if (cap > 0) {
            capacity_.store(cap, std::memory_order_relaxed);
        }
    }

    // ========================================================================
    // Pause/Resume Support (for ResourceGovernor pressure response)
    // ========================================================================

    /// Post-ingest processing stages (for pause/resume control)
    enum class Stage : std::uint8_t {
        Extraction = 0, // Text/content extraction
        KnowledgeGraph, // KG triple generation
        Symbol,         // Symbol extraction
        Entity,         // Entity extraction (binary analysis)
        Title           // Title extraction (GLiNER)
    };
    static constexpr std::size_t kStageCount = 5;
    static constexpr std::size_t kLimiterCount = 6; // 5 stages + embed
    static constexpr std::array<const char*, kStageCount> kStageNames = {
        "Extraction", "KnowledgeGraph", "Symbol", "Entity", "Title"};

    /// Pause a specific processing stage
    /// @param stage The stage to pause
    void pauseStage(Stage stage);

    /// Resume a specific processing stage
    /// @param stage The stage to resume
    void resumeStage(Stage stage);

    /// Check if a stage is paused
    /// @param stage The stage to check
    /// @return true if the stage is paused
    [[nodiscard]] bool isStagePaused(Stage stage) const;

    /// Pause all processing stages (emergency shutdown)
    void pauseAll();

    /// Resume all processing stages (return to normal operation)
    void resumeAll();

    // Update extractors after plugins are loaded (called by ServiceManager)
    void setExtractors(std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors) {
        extractors_ = std::move(extractors);
    }

    // Set extension-to-language map for symbol extraction (from plugin capabilities)
    void setSymbolExtensionMap(std::unordered_map<std::string, std::string> extMap);

    // Set entity providers for binary entity extraction
    void setEntityProviders(std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> providers);

    // Set title extractor (GLiNER-backed) for deriving better FTS titles.
    void setTitleExtractor(search::EntityExtractionFunc extractor);

    // Set KGWriteQueue for async NL entity KG population (merged with title extraction)
    void setKgWriteQueue(KGWriteQueue* queue) { kgWriteQueue_ = queue; }

    /// Set callback to be invoked when the queue drains (all stages become idle).
    /// Used by ServiceManager to trigger search engine rebuild.
    using DrainCallback = std::function<void()>;
    void setDrainCallback(DrainCallback callback) {
        std::lock_guard<std::mutex> lock(drainCallbackMutex_);
        drainCallback_ = std::move(callback);
    }

private:
    boost::asio::awaitable<void> channelPoller();
    boost::asio::awaitable<void> kgPoller();
    boost::asio::awaitable<void> symbolPoller();
    boost::asio::awaitable<void> entityPoller();
    boost::asio::awaitable<void> titlePoller();
    void processTask(const std::string& hash, const std::string& mime);
    void processMetadataStage(
        const std::string& hash, const std::string& mime,
        const std::optional<metadata::DocumentInfo>& info = std::nullopt,
        const std::vector<std::string>* tagsOverride = nullptr,
        const std::unordered_map<std::string, std::string>& symbolExtensionMap = {},
        const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders = {});

    /// Prepare metadata for batch insertion (extraction only, no DB writes).
    /// Returns PreparedMetadataEntry on success, ExtractionFailure on failure.
    std::variant<PreparedMetadataEntry, ExtractionFailure> prepareMetadataEntry(
        const std::string& hash, const std::string& mime, const metadata::DocumentInfo& info,
        const std::vector<std::string>& tags,
        const std::unordered_map<std::string, std::string>& symbolExtensionMap,
        const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders);

    void processKnowledgeGraphBatch(std::vector<InternalEventBus::KgJob>&& jobs);
    void processSymbolExtractionStage(const std::string& hash, int64_t docId,
                                      const std::string& filePath, const std::string& language,
                                      std::shared_ptr<std::vector<std::byte>> contentBytes);
    void processSymbolExtractionBatch(std::vector<InternalEventBus::SymbolExtractionJob>&& jobs);
    void processEntityExtractionBatch(std::vector<InternalEventBus::EntityExtractionJob>&& jobs);
    void processTitleExtractionBatch(std::vector<InternalEventBus::TitleExtractionJob>&& jobs);
    void dispatchToKgChannel(const std::string& hash, int64_t docId, const std::string& filePath,
                             std::vector<std::string> tags,
                             std::shared_ptr<std::vector<std::byte>> contentBytes);
    void dispatchToSymbolChannel(const std::string& hash, int64_t docId,
                                 const std::string& filePath, const std::string& language,
                                 std::shared_ptr<std::vector<std::byte>> contentBytes);
    void dispatchToEntityChannel(const std::string& hash, int64_t docId,
                                 const std::string& filePath, const std::string& extension,
                                 std::shared_ptr<std::vector<std::byte>> contentBytes);
    void processEntityExtractionStage(const std::string& hash, int64_t docId,
                                      const std::string& filePath, const std::string& extension,
                                      std::shared_ptr<std::vector<std::byte>> contentBytes);
    void dispatchToTitleChannel(const std::string& hash, int64_t docId,
                                const std::string& textSnippet, const std::string& fallbackTitle,
                                const std::string& filePath, const std::string& language,
                                const std::string& mimeType);
    void processTitleExtractionStage(const std::string& hash, int64_t docId,
                                     const std::string& textSnippet,
                                     const std::string& fallbackTitle, const std::string& filePath,
                                     const std::string& language, const std::string& mimeType);
    std::size_t resolveChannelCapacity() const;
    std::size_t boundedStageChannelCapacity(std::size_t defaultCap) const;

    struct BackpressureStatus {
        std::size_t kgDepth{0};
        std::size_t kgCapacity{0};
        double kgFillRatio{0.0};
        double threshold{0.0};
    };
    std::size_t adaptiveExtractionBatchSize(std::size_t baseBatchSize) const;
    std::size_t adaptiveStageBatchSize(std::size_t queueDepth, std::size_t baseBatchSize,
                                       std::size_t batchCap) const;
    double kgChannelFillRatio(std::size_t* depthOut = nullptr,
                              std::size_t* capacityOut = nullptr) const;
    [[nodiscard]] bool isKgChannelBackpressured() const;
    BackpressureStatus getBackpressureStatus() const;

    void checkDrainAndSignal(); // Check if drained and signal corpus stats stale
    std::string deriveTitle(const std::string& text, const std::string& fileName,
                            const std::string& mimeType, const std::string& extension) const;
    void refreshStageAvailability();
    void logStageAvailabilitySnapshot() const;

    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    std::shared_ptr<GraphComponent> graphComponent_;
    WorkCoordinator* coordinator_;
    WorkCoordinator* entityCoordinator_;

    std::atomic<bool> stop_{false};

    // Per-stage arrays indexed by Stage enum (0..4)
    std::array<std::atomic<bool>, kStageCount> stageStarted_{};
    std::array<std::atomic<bool>, kStageCount> stagePaused_{};
    std::array<std::atomic<std::size_t>, kStageCount> stageInFlight_{};

    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    // File/directory add tracking
    std::atomic<std::uint64_t> filesAdded_{0};
    std::atomic<std::uint64_t> directoriesAdded_{0};
    std::atomic<std::uint64_t> filesProcessed_{0};
    std::atomic<std::uint64_t> directoriesProcessed_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::atomic<std::size_t> capacity_{1000};
    // Concurrency limits now dynamic via TuneAdvisor (PBI-05a)

    // Drain detection: signals corpus stats stale once per drain cycle
    std::atomic<bool> wasActive_{false}; // True if we had work since last drain

    std::chrono::steady_clock::time_point lastCompleteTs_{};
    static constexpr double kAlpha_ = 0.2;

    mutable std::mutex extMapMutex_;
    std::unordered_map<std::string, std::string> symbolExtensionMap_;

    mutable std::mutex entityMutex_;
    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders_;

    mutable std::mutex drainCallbackMutex_;
    DrainCallback drainCallback_;

    search::EntityExtractionFunc titleExtractor_;
    KGWriteQueue* kgWriteQueue_{nullptr};

    // Cached EventBus channel pointers — resolved once in start(), avoids
    // per-call mutex + unordered_map<string> lookups on every enqueue/dispatch/depth query.
    std::shared_ptr<SpscQueue<InternalEventBus::PostIngestTask>> postIngestChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::PostIngestTask>> postIngestRpcChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::KgJob>> kgChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::SymbolExtractionJob>> symbolChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::EntityExtractionJob>> entityChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::TitleExtractionJob>> titleChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedChannel_;

    /// Initialize cached channel pointers from InternalEventBus
    void initializeChannels();

    // Gradient-based adaptive concurrency limiters (Netflix Gradient2 algorithm)
    // Index 0-4 = stages (Extraction..Title), index 5 = Embed
    std::array<std::unique_ptr<GradientLimiter>, kLimiterCount> limiters_;

    // Job tracking for latency measurement
    struct ActiveJob {
        std::chrono::steady_clock::time_point startTime;
        std::chrono::steady_clock::time_point enqueueTime;
        GradientLimiter* limiter;
        std::string stage;
    };
    std::unordered_map<std::string, ActiveJob> activeJobs_;
    mutable std::mutex activeJobsMutex_;
    std::atomic<uint64_t> nextJobId_{0};

    /// Initialize gradient limiters
    void initializeGradientLimiters();

    /// Complete a tracked job and report latency to its limiter
    void completeJob(const std::string& jobId, bool success);

    /// Try to acquire a slot from a limiter
    bool tryAcquireLimiterSlot(GradientLimiter* limiter, const std::string& jobId,
                               const std::string& stage);

    // =========================================================================
    // Parallel Processing & Caching (PBI-05a optimizations)
    // =========================================================================

    /// Process batch with work-stealing parallelization (default)
    void processBatch(std::vector<InternalEventBus::PostIngestTask>&& tasks);

    /// Snapshot stage config under locks (symbol extension map + entity providers)
    struct StageConfigSnapshot {
        std::unordered_map<std::string, std::string> symbolExtensionMap;
        std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders;
    };
    StageConfigSnapshot snapshotStageConfig();

    /// Commit batch results: DB write, failure status updates
    void commitBatchResults(std::vector<PreparedMetadataEntry>& successes,
                            std::vector<ExtractionFailure>& failures);

    /// Dispatch successes to downstream channels (KG, symbol, entity, title)
    void dispatchSuccesses(const std::vector<PreparedMetadataEntry>& successes);

    /// Process a chunk of tasks in parallel (used by processBatch)
    struct ChunkResult {
        std::vector<PreparedMetadataEntry> successes;
        std::vector<ExtractionFailure> failures;
    };
    ChunkResult processChunkParallel(
        const std::vector<InternalEventBus::PostIngestTask>& tasks,
        const std::unordered_map<std::string, std::string>& symbolExtensionMap,
        const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders);

    /// Get document info from cache or DB
    std::optional<metadata::DocumentInfo> getCachedDocumentInfo(const std::string& hash);

    /// Get document tags from cache or DB
    std::optional<std::vector<std::string>> getCachedDocumentTags(int64_t docId);

    /// Cache for metadata lookups
    MetadataCache metadataCache_;

    /// Semaphore to limit concurrent text extractions (memory safety)
    std::unique_ptr<std::counting_semaphore<>> extractionSemaphore_;

    /// Initialize the extraction semaphore based on TuneAdvisor limits
    void initializeExtractionSemaphore();

    // Backpressure metrics
    std::atomic<std::uint64_t> backpressureRejects_{0};
};

} // namespace yams::daemon
