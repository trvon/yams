#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/steady_timer.hpp>
#include <yams/daemon/components/GradientLimiter.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/query_concept_extractor.h>

namespace yams::api {
class IContentStore;
}

namespace yams::metadata {
class MetadataRepository;
class ContentIndexWriter;
} // namespace yams::metadata

namespace yams::extraction {
class IContentExtractor;
}

namespace yams::vector {
class VectorDatabase;
}

namespace yams::daemon {
template <typename Task> struct PressureLimitedPollerConfig;
class ExternalEntityProviderAdapter;
class IModelProvider;
class WorkCoordinator;
class GraphComponent;
class WriteCoordinator;

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
        auto now = Clock::now();
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
            auto now = Clock::now();
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
    using Clock = std::chrono::steady_clock;
    using Timestamp = Clock::time_point;
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
    static constexpr double kKgBackpressureThreshold = 0.85;

    struct Timing {
        std::uint64_t calls{0};
        std::uint64_t totalMs{0};
        std::uint64_t maxMs{0};
        std::uint64_t totalUs{0};
        std::uint64_t maxUs{0};
        std::int64_t firstStartMs{0};
        std::int64_t lastEndMs{0};
    };

    struct BatchMetrics {
        std::uint64_t extractionBatches{0};
        std::uint64_t extractionTasks{0};
        std::uint64_t extractionSuccesses{0};
        std::uint64_t extractionFailures{0};
        std::uint64_t embedJobsEmitted{0};
        std::uint64_t embedDocsEmitted{0};
        std::uint64_t embedPreparedDocsEmitted{0};
        std::uint64_t embedHashOnlyDocsEmitted{0};
        std::uint64_t contentIndexCalls{0};
        std::uint64_t contentIndexEntries{0};
        std::uint64_t contentIndexChunks{0};
        std::uint64_t contentIndexMaxEntries{0};
        std::uint64_t contentIndexMaxChunkEntries{0};
    };

    struct MetricsSnapshot {
        std::unordered_map<std::string, Timing> timings;
        BatchMetrics batches;
    };

    struct Task {
        std::string hash;
        std::string mime;
        int64_t documentId{-1};
        std::string filePath;
        bool noEmbeddings{false};
    };

    /// Result of successful text extraction, ready for batch DB insertion.
    struct PreparedMetadataEntry {
        int64_t documentId = 0;
        std::string hash;
        std::string fileName;
        std::string filePath; // Full path for KG node creation
        std::string title;
        std::string extractedText;
        std::string abstract; // IMRAD-detected abstract section
        std::string mimeType;
        std::string extension;
        std::vector<std::string> tags;
        std::string language;
        std::shared_ptr<std::vector<std::byte>> contentBytes;
        bool priorContentExtracted = false;
        metadata::ExtractionStatus priorExtractionStatus = metadata::ExtractionStatus::Pending;

        // Dispatch flags (determined during preparation)
        bool shouldDispatchKg = true;
        bool shouldDispatchSymbol = false;
        std::string symbolLanguage;
        bool shouldDispatchEntity = false;
        bool shouldDispatchTitle = false;
        bool shouldDispatchEmbed = true;
        std::string titleTextSnippet; // First N chars for async GLiNER title extraction
        std::vector<std::string> fallbackEntities; // ScientificAdapter entities for bulk write
        std::vector<std::string> fallbackAliases;  // ScientificAdapter aliases for bulk write
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
    std::uint64_t titleNlDocsProcessed() const {
        return titleNlDocsProcessed_.load(std::memory_order_relaxed);
    }
    std::uint64_t titleNlDocsWithEntities() const {
        return titleNlDocsWithEntities_.load(std::memory_order_relaxed);
    }
    std::uint64_t titleNlEntitiesExtracted() const {
        return titleNlEntitiesExtracted_.load(std::memory_order_relaxed);
    }
    std::uint64_t deferredDocEntitiesQueued() const {
        return deferredDocEntitiesQueued_.load(std::memory_order_relaxed);
    }
    std::uint64_t deferredDocEntityQueueFailures() const {
        return deferredDocEntityQueueFailures_.load(std::memory_order_relaxed);
    }
    std::uint64_t segmentNodesCreated() const {
        return segmentNodesCreated_.load(std::memory_order_relaxed);
    }
    std::uint64_t segmentEdgesCreated() const {
        return segmentEdgesCreated_.load(std::memory_order_relaxed);
    }
    std::uint64_t entitySegmentEdgesCreated() const {
        return entitySegmentEdgesCreated_.load(std::memory_order_relaxed);
    }
    std::uint64_t bodySegmentNodesCreated() const {
        return bodySegmentNodesCreated_.load(std::memory_order_relaxed);
    }
    std::uint64_t bodyEntitySegmentEdgesCreated() const {
        return bodyEntitySegmentEdgesCreated_.load(std::memory_order_relaxed);
    }

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
    MetricsSnapshot metricsSnapshot() const;
    void resetMetrics();

    // Backpressure observability
    std::uint64_t backpressureRejects() const {
        return backpressureRejects_.load(std::memory_order_relaxed);
    }
    double kgFillRatio(std::size_t* depthOut = nullptr, std::size_t* capacityOut = nullptr) const {
        return kgChannelFillRatio(depthOut, capacityOut);
    }

    [[nodiscard]] bool started() const { return anyStageStarted(); }

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
    GradientLimiter* extractionLimiter() const;
    GradientLimiter* kgLimiter() const;
    GradientLimiter* symbolLimiter() const;
    GradientLimiter* entityLimiter() const;
    GradientLimiter* titleLimiter() const;
    GradientLimiter* embedLimiter() const;

    void setCapacity(std::size_t cap) {
        if (cap > 0) {
            capacity_.store(cap, std::memory_order_relaxed);
        }
    }
    void setBatchCoalesceWindow(std::chrono::milliseconds window) {
        constexpr auto kMaxWindow = std::chrono::milliseconds{20};
        const auto bounded = std::clamp(window, std::chrono::milliseconds{0}, kMaxWindow);
        batchCoalesceMs_.store(static_cast<std::uint32_t>(bounded.count()),
                               std::memory_order_relaxed);
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

    void setWriteCoordinator(WriteCoordinator* coord) { writeCoordinator_ = coord; }

    /// Set callback to be invoked when the queue drains (all stages become idle).
    /// Used by ServiceManager to trigger search engine rebuild.
    using DrainCallback = std::function<void()>;
    void setDrainCallback(DrainCallback callback) {
        std::lock_guard<std::mutex> lock(drainCallbackMutex_);
        drainCallback_ = std::move(callback);
    }

    [[nodiscard]] search::EntityExtractionFunc getTitleExtractor() const;
    [[nodiscard]] bool hasTitleExtractor() const;

private:
    template <typename Task>
    PressureLimitedPollerConfig<Task> makePollerConfig(
        Stage stage, std::string stageName, std::function<GradientLimiter*()> getLimiter,
        std::function<std::size_t()> maxConcurrent, boost::asio::any_io_executor executor,
        std::function<std::string(const Task&)> getHash,
        std::shared_ptr<boost::asio::steady_timer> wakeTimer);

    boost::asio::awaitable<void> channelPoller();
    boost::asio::awaitable<void> kgPoller();
    boost::asio::awaitable<void> symbolPoller();
    boost::asio::awaitable<void> entityPoller();
    boost::asio::awaitable<void> titlePoller();

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
                                      std::vector<std::byte>* contentBytes);
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
                                      std::vector<std::byte>* contentBytes);
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
    void requeueMissedEntityExtractions();

    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<metadata::MetadataRepository> meta_;
    std::unique_ptr<metadata::ContentIndexWriter> contentIndexWriter_;
    std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors_;
    std::shared_ptr<metadata::KnowledgeGraphStore> kg_;
    std::shared_ptr<GraphComponent> graphComponent_;
    WorkCoordinator* coordinator_;
    WorkCoordinator* entityCoordinator_;

    std::atomic<bool> stop_{false};
    std::atomic<bool> startGuard_{false};

    // Per-stage arrays indexed by Stage enum (0..4)
    std::array<std::atomic<bool>, kStageCount> stageStarted_{};
    std::array<std::atomic<bool>, kStageCount> stagePaused_{};
    std::array<std::atomic<std::size_t>, kStageCount> stageInFlight_{};
    std::atomic<std::size_t> callbacksInFlight_{0};

    [[nodiscard]] bool anyStageStarted() const {
        for (std::size_t i = 0; i < kStageCount; ++i) {
            if (stageStarted_[i].load(std::memory_order_acquire)) {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] bool allStagesStarted() const {
        for (std::size_t i = 0; i < kStageCount; ++i) {
            if (!stageStarted_[i].load(std::memory_order_acquire)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool allStagesStopped() const {
        for (std::size_t i = 0; i < kStageCount; ++i) {
            if (stageStarted_[i].load(std::memory_order_acquire)) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool shutdownQuiesced() const {
        return allStagesStopped() && totalInFlight() == 0 &&
               callbacksInFlight_.load(std::memory_order_acquire) == 0;
    }

    mutable std::mutex lifecycleMutex_;
    std::condition_variable lifecycleCv_;
    void notifyLifecycle() {
        std::lock_guard<std::mutex> lock(lifecycleMutex_);
        lifecycleCv_.notify_all();
    }

    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    std::atomic<std::uint64_t> titleNlDocsProcessed_{0};
    std::atomic<std::uint64_t> titleNlDocsWithEntities_{0};
    std::atomic<std::uint64_t> titleNlEntitiesExtracted_{0};
    std::atomic<std::uint64_t> deferredDocEntitiesQueued_{0};
    std::atomic<std::uint64_t> deferredDocEntityQueueFailures_{0};
    std::atomic<std::uint64_t> segmentNodesCreated_{0};
    std::atomic<std::uint64_t> segmentEdgesCreated_{0};
    std::atomic<std::uint64_t> entitySegmentEdgesCreated_{0};
    std::atomic<std::uint64_t> bodySegmentNodesCreated_{0};
    std::atomic<std::uint64_t> bodyEntitySegmentEdgesCreated_{0};
    // File/directory add tracking
    std::atomic<std::uint64_t> filesAdded_{0};
    std::atomic<std::uint64_t> directoriesAdded_{0};
    std::atomic<std::uint64_t> filesProcessed_{0};
    std::atomic<std::uint64_t> directoriesProcessed_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::atomic<std::size_t> capacity_{1000};
    std::atomic<std::uint32_t> batchCoalesceMs_{0};
    // Concurrency limits now dynamic via TuneAdvisor (PBI-05a)

    // Drain detection: signals corpus stats stale once per drain cycle
    std::atomic<bool> wasActive_{false}; // True if we had work since last drain

    static constexpr double kAlpha_ = 0.2;

    mutable std::mutex extMapMutex_;
    std::unordered_map<std::string, std::string> symbolExtensionMap_;

    mutable std::mutex entityMutex_;
    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders_;

    mutable std::mutex drainCallbackMutex_;
    DrainCallback drainCallback_;

    mutable std::mutex titleExtractorMutex_;
    search::EntityExtractionFunc titleExtractor_;
    WriteCoordinator* writeCoordinator_{nullptr};

    // Cached EventBus channel pointers — resolved once in start(), avoids
    // per-call mutex + unordered_map<string> lookups on every enqueue/dispatch/depth query.
    std::shared_ptr<SpscQueue<InternalEventBus::PostIngestTask>> postIngestChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::PostIngestTask>> postIngestRpcChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::KgJob>> kgChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::SymbolExtractionJob>> symbolChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::EntityExtractionJob>> entityChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::TitleExtractionJob>> titleChannel_;
    std::shared_ptr<SpscQueue<InternalEventBus::EmbedJob>> embedChannel_;

    // Event-driven wake timers: the enqueuer signals the timer's strand to
    // cancel the wait and immediately wake the corresponding poller coroutine.
    // Access to the shared_ptr holders is synchronized here because enqueue and
    // shutdown happen from arbitrary threads while the pollers own the timers.
    mutable std::mutex wakeTimerMutex_;
    std::shared_ptr<boost::asio::steady_timer> extractionWakeTimer_;
    std::shared_ptr<boost::asio::steady_timer> kgWakeTimer_;
    std::shared_ptr<boost::asio::steady_timer> symbolWakeTimer_;
    std::shared_ptr<boost::asio::steady_timer> entityWakeTimer_;
    std::shared_ptr<boost::asio::steady_timer> titleWakeTimer_;
    void setWakeTimer(Stage stage, std::shared_ptr<boost::asio::steady_timer> timer);
    void signalWakeTimer(Stage stage);
    void signalAllWakeTimers();
    void clearWakeTimers();

    /// Initialize cached channel pointers from InternalEventBus
    void initializeChannels();

    // Gradient-based adaptive concurrency limiters (Netflix Gradient2 algorithm)
    // Index 0-4 = stages (Extraction..Title), index 5 = Embed
    // limiters_ owns the instances. limiterPtrs_ mirrors the raw pointers for
    // lock-free hot-path reads. Writes to both happen only in
    // initializeGradientLimiters() (guarded by limiterMutex_).
    std::array<std::unique_ptr<GradientLimiter>, kLimiterCount> limiters_;
    std::array<std::atomic<GradientLimiter*>, kLimiterCount> limiterPtrs_{};
    mutable std::mutex limiterMutex_;

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
    void recordTiming(const std::string& name, std::chrono::steady_clock::time_point start);
    void recordTimingAggregate(const std::string& name, std::uint64_t calls, std::uint64_t totalUs,
                               std::uint64_t maxUs);

    struct DispatchTimingAccumulator {
        std::uint64_t calls{0};
        std::uint64_t totalUs{0};
        std::uint64_t maxUs{0};

        void add(std::chrono::steady_clock::duration elapsed);
    };

    struct DispatchTimingSet {
        DispatchTimingAccumulator setupConfig;
        DispatchTimingAccumulator setupChunker;
        DispatchTimingAccumulator postConsumed;
        DispatchTimingAccumulator embedPrepare;
        DispatchTimingAccumulator embedEnqueue;
        DispatchTimingAccumulator contentLoad;
        DispatchTimingAccumulator kgDispatch;
        DispatchTimingAccumulator symbolDispatch;
        DispatchTimingAccumulator entityDispatch;
        DispatchTimingAccumulator titleDispatch;
    };

    struct PreparedDispatchPlan {
        bool dispatchKg{false};
        bool dispatchSymbol{false};
        bool dispatchEntity{false};
        bool dispatchTitle{false};
        bool dispatchEmbed{false};
        bool loadContentForNonEmbedding{false};
        std::string symbolLanguage;
    };

    PreparedDispatchPlan buildDispatchPlan(const PreparedMetadataEntry& prepared,
                                           bool embedStageActive, bool hasEmbedQueue) const;
    void recordDispatchTimingSet(const DispatchTimingSet& timings);
    std::shared_ptr<std::vector<std::byte>> getOrLoadDispatchContent(
        const std::string& hash,
        std::unordered_map<std::string, std::shared_ptr<std::vector<std::byte>>>& contentByHash);
    void dispatchNonEmbeddingStages(const PreparedMetadataEntry& prepared,
                                    const PreparedDispatchPlan& plan,
                                    std::shared_ptr<std::vector<std::byte>> contentBytes,
                                    DispatchTimingSet& timings);

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

    /// Get document info from cache or DB
    std::optional<metadata::DocumentInfo> getCachedDocumentInfo(const std::string& hash);

    /// Get document tags from cache or DB
    std::optional<std::vector<std::string>> getCachedDocumentTags(int64_t docId);

    /// Cache for metadata lookups
    MetadataCache metadataCache_;
    mutable std::mutex metricsMutex_;
    std::unordered_map<std::string, Timing> timings_;
    std::atomic<std::uint64_t> extractionBatches_{0};
    std::atomic<std::uint64_t> extractionTasks_{0};
    std::atomic<std::uint64_t> extractionSuccesses_{0};
    std::atomic<std::uint64_t> extractionFailures_{0};
    std::atomic<std::uint64_t> embedJobsEmitted_{0};
    std::atomic<std::uint64_t> embedDocsEmitted_{0};
    std::atomic<std::uint64_t> embedPreparedDocsEmitted_{0};
    std::atomic<std::uint64_t> embedHashOnlyDocsEmitted_{0};
    std::atomic<std::uint64_t> contentIndexCalls_{0};
    std::atomic<std::uint64_t> contentIndexEntries_{0};
    std::atomic<std::uint64_t> contentIndexChunks_{0};
    std::atomic<std::uint64_t> contentIndexMaxEntries_{0};
    std::atomic<std::uint64_t> contentIndexMaxChunkEntries_{0};

    // Backpressure metrics
    std::atomic<std::uint64_t> backpressureRejects_{0};

    // Graph-construction diagnostics (for tracing entity extraction → edge creation)
    std::atomic<std::uint64_t> gs_processed_{0};
    std::atomic<std::uint64_t> gs_totalEntities_{0};
    std::atomic<std::uint64_t> gs_highValueEntities_{0};
    std::atomic<std::uint64_t> gs_totalEdges_{0};
    std::atomic<std::uint64_t> gs_totalPrimaryTopicEdges_{0};

    // Entity dispatch diagnostics
    std::atomic<std::uint64_t> entityDispatched_{0};
};

} // namespace yams::daemon
