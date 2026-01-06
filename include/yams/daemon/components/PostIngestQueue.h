#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>

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

class PostIngestQueue {
public:
    struct Task {
        std::string hash;
        std::string mime;
        std::string session;
        std::chrono::steady_clock::time_point enqueuedAt;
        enum class Stage : uint8_t { Metadata = 0, KnowledgeGraph = 1 } stage;
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

    std::size_t size() const;
    std::size_t processed() const { return processed_.load(); }
    std::size_t failed() const { return failed_.load(); }
    double latencyMsEma() const { return latencyMsEma_.load(); }
    double ratePerSecEma() const { return ratePerSecEma_.load(); }
    std::size_t capacity() const { return capacity_; }

    // Per-stage inflight counts
    std::size_t extractionInFlight() const { return inFlight_.load(); }
    std::size_t kgInFlight() const { return kgInFlight_.load(); }
    std::size_t symbolInFlight() const { return symbolInFlight_.load(); }
    std::size_t entityInFlight() const { return entityInFlight_.load(); }
    std::size_t totalInFlight() const {
        return inFlight_.load() + kgInFlight_.load() + symbolInFlight_.load() +
               entityInFlight_.load();
    }

    // Stage concurrency limits (dynamic via TuneAdvisor)
    static std::size_t maxExtractionConcurrent();
    static std::size_t maxKgConcurrent();
    static std::size_t maxSymbolConcurrent();
    static std::size_t maxEntityConcurrent();

    void setCapacity(std::size_t cap) { capacity_ = cap > 0 ? cap : capacity_; }

    // Update extractors after plugins are loaded (called by ServiceManager)
    void setExtractors(std::vector<std::shared_ptr<extraction::IContentExtractor>> extractors) {
        extractors_ = std::move(extractors);
    }

    // Set extension-to-language map for symbol extraction (from plugin capabilities)
    void setSymbolExtensionMap(std::unordered_map<std::string, std::string> extMap) {
        std::lock_guard<std::mutex> lock(extMapMutex_);
        symbolExtensionMap_ = std::move(extMap);
    }

    // Set entity providers for binary entity extraction
    void setEntityProviders(std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> providers) {
        std::lock_guard<std::mutex> lock(entityMutex_);
        entityProviders_ = std::move(providers);
    }

private:
    boost::asio::awaitable<void> channelPoller();
    boost::asio::awaitable<void> kgPoller();
    boost::asio::awaitable<void> symbolPoller();
    boost::asio::awaitable<void> entityPoller();
    void processBatch(std::vector<InternalEventBus::PostIngestTask>&& tasks);
    void processTask(const std::string& hash, const std::string& mime);
    void processMetadataStage(
        const std::string& hash, const std::string& mime,
        const std::optional<metadata::DocumentInfo>& info = std::nullopt,
        const std::vector<std::string>* tagsOverride = nullptr,
        const std::unordered_map<std::string, std::string>& symbolExtensionMap = {},
        const std::vector<std::shared_ptr<ExternalEntityProviderAdapter>>& entityProviders = {});

    void processKnowledgeGraphStage(const std::string& hash, int64_t docId,
                                    const std::string& filePath,
                                    const std::vector<std::string>& tags);
    void processSymbolExtractionStage(const std::string& hash, int64_t docId,
                                      const std::string& filePath, const std::string& language);
    void processEmbeddingStage(const std::string& hash, const std::string& mime);
    void processEmbeddingBatch(const std::vector<std::string>& hashes);
    void dispatchToKgChannel(const std::string& hash, int64_t docId, const std::string& filePath,
                             std::vector<std::string> tags);
    void dispatchToSymbolChannel(const std::string& hash, int64_t docId,
                                 const std::string& filePath, const std::string& language);
    void dispatchToEntityChannel(const std::string& hash, int64_t docId,
                                 const std::string& filePath, const std::string& extension);
    void processEntityExtractionStage(const std::string& hash, int64_t docId,
                                      const std::string& filePath, const std::string& extension);
    std::size_t resolveChannelCapacity() const;

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
    std::atomic<bool> entityStarted_{false};
    std::atomic<std::size_t> processed_{0};
    std::atomic<std::size_t> failed_{0};
    std::atomic<std::size_t> inFlight_{0};
    std::atomic<std::size_t> kgInFlight_{0};
    std::atomic<std::size_t> symbolInFlight_{0};
    std::atomic<std::size_t> entityInFlight_{0};
    std::atomic<double> latencyMsEma_{0.0};
    std::atomic<double> ratePerSecEma_{0.0};
    std::size_t capacity_{1000};
    // Concurrency limits now dynamic via TuneAdvisor (PBI-05a)

    std::chrono::steady_clock::time_point lastCompleteTs_{};
    static constexpr double kAlpha_ = 0.2;

    mutable std::mutex extMapMutex_;
    std::unordered_map<std::string, std::string> symbolExtensionMap_;

    mutable std::mutex entityMutex_;
    std::vector<std::shared_ptr<ExternalEntityProviderAdapter>> entityProviders_;
};

} // namespace yams::daemon
