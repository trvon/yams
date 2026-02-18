#pragma once

#include <atomic>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
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

class PostIngestQueue {
public:
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

        // Dispatch flags (determined during preparation)
        bool shouldDispatchKg = true;
        bool shouldDispatchSymbol = false;
        std::string symbolLanguage;
        bool shouldDispatchEntity = false;
        bool shouldDispatchTitle = false;
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
    bool tryEnqueue(const Task& t);
    bool tryEnqueue(Task&& t);

    /// Dispatch an embedding job for already-extracted documents (e.g. fast-track).
    bool dispatchEmbedJobWithRetry(const std::vector<std::string>& hashes, bool recordOnFailure,
                                   bool notifyOnFailure);

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
    std::size_t capacity() const { return capacity_; }

    /// Returns true once the channel poller coroutine has started.
    /// Tests should wait for this before enqueueing to avoid race conditions.
    bool started() const { return started_.load(); }

    // Per-stage inflight counts
    std::size_t extractionInFlight() const { return inFlight_.load(); }
    std::size_t kgInFlight() const { return kgInFlight_.load(); }
    std::size_t enrichInFlight() const { return enrichInFlight_.load(); }
    std::size_t symbolInFlight() const { return enrichInFlight_.load(); }
    std::size_t entityInFlight() const { return enrichInFlight_.load(); }
    std::size_t titleInFlight() const { return enrichInFlight_.load(); }
    std::size_t totalInFlight() const {
        return inFlight_.load() + kgInFlight_.load() + enrichInFlight();
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

    void setCapacity(std::size_t cap) { capacity_ = cap > 0 ? cap : capacity_; }

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

    // Callback for embedding dispatch failures (e.g., to trigger repair coordinator)
    using EmbedFailureCallback = std::function<void(const std::vector<std::string>&)>;
    void setEmbedFailureCallback(EmbedFailureCallback callback) {
        std::lock_guard<std::mutex> lock(embedFailureCallbackMutex_);
        embedFailureCallback_ = std::move(callback);
    }

private:
    boost::asio::awaitable<void> channelPoller();
    boost::asio::awaitable<void> kgPoller();
    boost::asio::awaitable<void> enrichPoller();
    void processBatch(std::vector<InternalEventBus::PostIngestTask>&& tasks);
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

    void processKnowledgeGraphStage(const std::string& hash, int64_t docId,
                                    const std::string& filePath,
                                    const std::vector<std::string>& tags,
                                    std::shared_ptr<std::vector<std::byte>> contentBytes);
    void processSymbolExtractionStage(const std::string& hash, int64_t docId,
                                      const std::string& filePath, const std::string& language,
                                      std::shared_ptr<std::vector<std::byte>> contentBytes);
    void processEmbeddingBatch(const std::vector<std::string>& hashes);
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
    void checkDrainAndSignal(); // Check if drained and signal corpus stats stale
    std::string deriveTitle(const std::string& text, const std::string& fileName,
                            const std::string& mimeType, const std::string& extension) const;
    void recordEmbedRetry(const std::vector<std::string>& hashes);
    void flushEmbedRetriesOnDrain();
    void notifyEmbedFailure(const std::vector<std::string>& hashes);
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
    std::atomic<bool> started_{false};
    std::atomic<bool> kgStarted_{false};
    std::atomic<bool> symbolStarted_{false};

    // Pause flags for ResourceGovernor pressure response
    std::atomic<bool> extractionPaused_{false};
    std::atomic<bool> kgPaused_{false};
    std::atomic<bool> symbolPaused_{false};
    std::atomic<bool> entityPaused_{false};
    std::atomic<bool> titlePaused_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    // File/directory add tracking
    std::atomic<std::uint64_t> filesAdded_{0};
    std::atomic<std::uint64_t> directoriesAdded_{0};
    std::atomic<std::uint64_t> filesProcessed_{0};
    std::atomic<std::uint64_t> directoriesProcessed_{0};
    std::atomic<std::size_t> inFlight_{0};
    std::atomic<std::size_t> kgInFlight_{0};
    std::atomic<std::size_t> enrichInFlight_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::size_t capacity_{1000};
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

    mutable std::mutex embedFailureCallbackMutex_;
    EmbedFailureCallback embedFailureCallback_;
    mutable std::mutex embedRetryMutex_;
    std::deque<std::string> embedRetryHashes_;

    search::EntityExtractionFunc titleExtractor_;
    KGWriteQueue* kgWriteQueue_{nullptr};
};

} // namespace yams::daemon
