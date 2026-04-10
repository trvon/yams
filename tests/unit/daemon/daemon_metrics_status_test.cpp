// Daemon metrics and status test suite (Catch2)
// Consolidates status/metrics tests: embedding status, plugin degradation, WAL metrics, FSM states
// Covers: DaemonMetrics, StatusResponse serialization, plugin degradation, WAL metrics

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <random>
#include <span>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/session_service.hpp>
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/daemon_lifecycle.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/ipc/request_context.h>
#include <yams/daemon/ipc/request_context_registry.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>
#include <yams/version.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

using namespace yams;
using namespace yams::daemon;

// =============================================================================
// Test Helpers
// =============================================================================

namespace {
class StubLifecycle : public IDaemonLifecycle {
public:
    explicit StubLifecycle(LifecycleState state = LifecycleState::Ready) {
        snapshot_.state = state;
    }

    LifecycleSnapshot getLifecycleSnapshot() const override {
        ++getLifecycleSnapshotCalls_;
        if (throwOnGetLifecycleSnapshotCall_ > 0 &&
            getLifecycleSnapshotCalls_ == throwOnGetLifecycleSnapshotCall_) {
            throw std::runtime_error("getLifecycleSnapshot boom");
        }
        if (throwOnGetLifecycleSnapshotCount_ > 0) {
            --throwOnGetLifecycleSnapshotCount_;
            throw std::runtime_error("getLifecycleSnapshot boom");
        }
        return snapshot_;
    }
    void setSubsystemDegraded(const std::string& subsystem, bool degraded,
                              const std::string& reason) override {
        if (throwOnSetSubsystemDegraded_) {
            throw std::runtime_error("setSubsystemDegraded boom");
        }
        lastSubsystem_ = subsystem;
        lastDegraded_ = degraded;
        lastReason_ = reason;
        setSubsystemDegradedCalls_++;
    }
    void onDocumentRemoved(const std::string& hash) override { removedHashes_.push_back(hash); }
    void requestShutdown(bool, bool) override {}

    const std::string& lastSubsystem() const { return lastSubsystem_; }
    bool lastDegraded() const { return lastDegraded_; }
    const std::string& lastReason() const { return lastReason_; }
    int setSubsystemDegradedCalls() const { return setSubsystemDegradedCalls_; }
    void resetDegradedState() {
        lastSubsystem_.clear();
        lastDegraded_ = false;
        lastReason_.clear();
        setSubsystemDegradedCalls_ = 0;
    }
    void setThrowOnSetSubsystemDegraded(bool enabled) { throwOnSetSubsystemDegraded_ = enabled; }
    void setThrowOnGetLifecycleSnapshot(bool enabled) {
        throwOnGetLifecycleSnapshotCount_ = enabled ? 1 : 0;
    }
    void setThrowOnGetLifecycleSnapshotCount(int count) {
        throwOnGetLifecycleSnapshotCount_ = count;
    }
    void setThrowOnGetLifecycleSnapshotCall(int call) { throwOnGetLifecycleSnapshotCall_ = call; }
    const std::vector<std::string>& removedHashes() const { return removedHashes_; }

private:
    LifecycleSnapshot snapshot_{};
    std::string lastSubsystem_;
    bool lastDegraded_{false};
    std::string lastReason_;
    int setSubsystemDegradedCalls_{0};
    bool throwOnSetSubsystemDegraded_{false};
    mutable int getLifecycleSnapshotCalls_{0};
    mutable int throwOnGetLifecycleSnapshotCount_{0};
    int throwOnGetLifecycleSnapshotCall_{0};
    std::vector<std::string> removedHashes_;
};

// Minimal stub provider for embedding tests
class StubModelProvider : public IModelProvider {
public:
    explicit StubModelProvider(size_t dim, std::string path)
        : dim_(dim), path_(std::move(path)), available_(true) {}

    Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        lastTextArg_ = text;
        if (auto it = singleTextErrors_.find(text); it != singleTextErrors_.end()) {
            return it->second;
        }
        if (generateEmbeddingError_) {
            return *generateEmbeddingError_;
        }
        if (!singleEmbeddingResult_.empty()) {
            return singleEmbeddingResult_;
        }
        return std::vector<float>(dim_, 0.25f);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        lastBatchSizeArg_ = texts.size();
        if (!texts.empty()) {
            lastTextArg_ = texts.front();
        }
        if (generateBatchError_ && !isWarmupBatch(texts)) {
            return *generateBatchError_;
        }
        if (!batchEmbeddingResult_.empty()) {
            return batchEmbeddingResult_;
        }
        return std::vector<std::vector<float>>(lastBatchSizeArg_, std::vector<float>(dim_, 0.5f));
    }
    Result<std::vector<float>> generateEmbeddingFor(const std::string&,
                                                    const std::string& text) override {
        lastTextArg_ = text;
        return generateEmbedding(text);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string&, const std::vector<std::string>& texts) override {
        lastBatchSizeArg_ = texts.size();
        if (!texts.empty()) {
            lastTextArg_ = texts.front();
        }
        return generateBatchEmbeddings(texts);
    }
    Result<void> loadModel(const std::string& modelName) override {
        if (loadError_) {
            return *loadError_;
        }
        lastOptionsModel_.clear();
        lastOptionsJson_.clear();
        loaded_.push_back(modelName);
        return Result<void>();
    }
    Result<void> loadModelWithOptions(const std::string& modelName,
                                      const std::string& optionsJson) override {
        if (loadError_) {
            return *loadError_;
        }
        lastOptionsModel_ = modelName;
        lastOptionsJson_ = optionsJson;
        loaded_.push_back(modelName);
        return Result<void>();
    }
    Result<void> unloadModel(const std::string& modelName) override {
        auto it = std::find(loaded_.begin(), loaded_.end(), modelName);
        if (it == loaded_.end()) {
            return ErrorCode::NotFound;
        }
        loaded_.erase(it);
        return Result<void>();
    }
    bool isModelLoaded(const std::string& modelName) const override {
        if (throwOnIsModelLoadedCount_ > 0) {
            --throwOnIsModelLoadedCount_;
            throw std::runtime_error("isModelLoaded boom");
        }
        return std::find(loaded_.begin(), loaded_.end(), modelName) != loaded_.end();
    }
    std::vector<std::string> getLoadedModels() const override { return loaded_; }
    size_t getLoadedModelCount() const override { return loaded_.size(); }
    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        if (failModelInfo_) {
            return ErrorCode::NotFound;
        }
        if (!isModelLoaded(modelName))
            return ErrorCode::NotFound;
        ModelInfo mi;
        mi.name = modelName;
        mi.path = path_;
        mi.embeddingDim = dim_;
        return mi;
    }
    size_t getEmbeddingDim(const std::string&) const override { return dim_; }
    std::shared_ptr<vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& = "") override {
        return nullptr;
    }
    std::string getProviderName() const override { return "StubProvider"; }
    std::string getProviderVersion() const override { return "vtest"; }
    bool isAvailable() const override { return available_; }
    size_t getMemoryUsage() const override {
        if (throwOnMemoryUsage_) {
            throw std::runtime_error("memory boom");
        }
        return 0;
    }
    void releaseUnusedResources() override {}
    void shutdown() override {}
    void setAvailable(bool available) { available_ = available; }
    const std::string& lastOptionsModel() const { return lastOptionsModel_; }
    const std::string& lastOptionsJson() const { return lastOptionsJson_; }
    void setLoadError(Error error) { loadError_ = std::move(error); }
    void clearLoadError() { loadError_.reset(); }
    void setThrowOnMemoryUsage(bool enabled) { throwOnMemoryUsage_ = enabled; }
    void setFailModelInfo(bool enabled) { failModelInfo_ = enabled; }
    void setThrowOnIsModelLoaded(bool enabled) { throwOnIsModelLoadedCount_ = enabled ? 1 : 0; }
    void setThrowOnIsModelLoadedCount(int count) { throwOnIsModelLoadedCount_ = count; }
    void setSingleEmbeddingResult(std::vector<float> embedding) {
        singleEmbeddingResult_ = std::move(embedding);
    }
    void setBatchEmbeddingResult(std::vector<std::vector<float>> embeddings) {
        batchEmbeddingResult_ = std::move(embeddings);
    }
    void setGenerateEmbeddingError(Error error) { generateEmbeddingError_ = std::move(error); }
    void setGenerateBatchError(Error error) { generateBatchError_ = std::move(error); }
    void setSingleTextError(const std::string& text, Error error) {
        singleTextErrors_[text] = std::move(error);
    }
    void clearSingleTextErrors() { singleTextErrors_.clear(); }

private:
    static bool isWarmupBatch(const std::vector<std::string>& texts) {
        static const std::array<std::string, 4> kWarmupTexts = {
            "warmup text one", "warmup text two", "warmup text three", "warmup text four"};
        return texts.size() == kWarmupTexts.size() &&
               std::equal(texts.begin(), texts.end(), kWarmupTexts.begin());
    }

    size_t dim_;
    std::string path_;
    bool available_;
    std::vector<std::string> loaded_;
    std::string lastOptionsModel_;
    std::string lastOptionsJson_;
    std::optional<Error> loadError_;
    std::optional<Error> generateEmbeddingError_;
    std::optional<Error> generateBatchError_;
    bool throwOnMemoryUsage_{false};
    bool failModelInfo_{false};
    mutable int throwOnIsModelLoadedCount_{0};
    std::vector<float> singleEmbeddingResult_;
    std::vector<std::vector<float>> batchEmbeddingResult_;
    std::unordered_map<std::string, Error> singleTextErrors_;
    mutable std::string lastTextArg_;
    mutable std::size_t lastBatchSizeArg_{0};
};

class EnvGuard {
public:
    EnvGuard(const char* name, const char* value) : name_(name) {
        if (const char* current = std::getenv(name_)) {
            original_ = current;
        }
        setValue(value);
    }

    ~EnvGuard() {
        if (original_) {
            setValue(original_->c_str());
        } else {
            setValue(nullptr);
        }
    }

private:
    void setValue(const char* value) {
#ifdef _WIN32
        if (value) {
            _putenv_s(name_, value);
        } else {
            _putenv_s(name_, "");
        }
#else
        if (value) {
            ::setenv(name_, value, 1);
        } else {
            ::unsetenv(name_);
        }
#endif
    }

    const char* name_;
    std::optional<std::string> original_;
};

class TuneAdvisorListGuard {
public:
    TuneAdvisorListGuard(uint32_t inflightLimit, uint32_t admissionWaitMs)
        : originalInflightLimit_(TuneAdvisor::listInflightLimit()),
          originalAdmissionWaitMs_(TuneAdvisor::listAdmissionWaitMs()) {
        TuneAdvisor::setListInflightLimit(inflightLimit);
        TuneAdvisor::setListAdmissionWaitMs(admissionWaitMs);
    }

    ~TuneAdvisorListGuard() {
        TuneAdvisor::setListInflightLimit(originalInflightLimit_);
        TuneAdvisor::setListAdmissionWaitMs(originalAdmissionWaitMs_);
    }

private:
    uint32_t originalInflightLimit_;
    uint32_t originalAdmissionWaitMs_;
};

template <typename Fn> class ScopeExit {
public:
    explicit ScopeExit(Fn fn) : fn_(std::move(fn)) {}
    ~ScopeExit() { fn_(); }

private:
    Fn fn_;
};

std::shared_ptr<metadata::ConnectionPool> getPruneTestPool() {
    static std::shared_ptr<metadata::ConnectionPool> pool = [] {
        metadata::ConnectionPoolConfig cfg{};
        cfg.enableWAL = false;
        cfg.enableForeignKeys = false;
        auto p = std::make_shared<metadata::ConnectionPool>(":memory:", cfg);
        (void)p->initialize();
        return p;
    }();
    return pool;
}

class StubPruneMetadataRepository : public metadata::MetadataRepository {
public:
    StubPruneMetadataRepository() : metadata::MetadataRepository(*getPruneTestPool()) {}

    void addDocument(const metadata::DocumentInfo& doc) {
        std::lock_guard<std::mutex> lk(mu_);
        docs_.push_back(doc);
        docsByHash_[doc.sha256Hash] = doc;
        docsById_[doc.id] = doc;
    }

    void setQueryError(Error error) { queryError_ = std::move(error); }
    void setThrowOnQuery(std::string message) { throwOnQuery_ = std::move(message); }
    void setMissingHash(const std::string& hash) { missingHashes_.insert(hash); }
    void setThrowOnLookupHash(const std::string& hash, std::string message) {
        throwOnLookup_[hash] = std::move(message);
    }
    void setDeleteError(int64_t id, Error error) { deleteErrors_[id] = std::move(error); }
    void setSnapshots(std::vector<std::string> snapshots) { snapshots_ = std::move(snapshots); }
    void setSnapshotError(Error error) { snapshotError_ = std::move(error); }
    void setSnapshotDocuments(std::vector<metadata::DocumentInfo> docs) {
        snapshotDocs_ = std::move(docs);
    }
    void setSnapshotDocumentsError(Error error) { snapshotDocsError_ = std::move(error); }
    void setExactPathDocument(std::optional<metadata::DocumentInfo> doc) {
        std::lock_guard<std::mutex> lk(mu_);
        exactPathDocument_ = std::move(doc);
    }
    void setExactPathError(Error error) {
        std::lock_guard<std::mutex> lk(mu_);
        exactPathError_ = std::move(error);
    }
    void setAllMetadata(int64_t id,
                        std::unordered_map<std::string, metadata::MetadataValue> values) {
        std::lock_guard<std::mutex> lk(mu_);
        metadataById_[id] = std::move(values);
    }
    void setAllMetadataError(int64_t id, Error error) {
        std::lock_guard<std::mutex> lk(mu_);
        metadataByIdErrors_[id] = std::move(error);
    }
    void setPathTreeDocuments(std::vector<metadata::DocumentInfo> docs) {
        std::lock_guard<std::mutex> lk(mu_);
        pathTreeDocs_ = std::move(docs);
    }
    void setMetadataValueCounts(
        std::unordered_map<std::string, std::vector<metadata::MetadataValueCount>> counts) {
        metadataValueCounts_ = std::move(counts);
    }
    void setMetadataValueCountsError(Error error) { metadataValueCountsError_ = std::move(error); }

    std::size_t deleteCallCount() const {
        std::lock_guard<std::mutex> lk(mu_);
        return deleteCallCount_;
    }

    std::vector<int64_t> deletedIds() const {
        std::lock_guard<std::mutex> lk(mu_);
        return deletedIds_;
    }

    Result<std::vector<metadata::DocumentInfo>>
    queryDocuments(const metadata::DocumentQueryOptions&) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (throwOnQuery_) {
            throw std::runtime_error(*throwOnQuery_);
        }
        if (queryError_) {
            return *queryError_;
        }
        return docs_;
    }

    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsBySnapshot(const std::string&) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (snapshotDocsError_) {
            return *snapshotDocsError_;
        }
        return snapshotDocs_;
    }

    Result<std::vector<std::string>> getSnapshots() override {
        std::lock_guard<std::mutex> lk(mu_);
        if (snapshotError_) {
            return *snapshotError_;
        }
        return snapshots_;
    }

    Result<std::unordered_map<std::string, std::vector<metadata::MetadataValueCount>>>
    getMetadataValueCounts(const std::vector<std::string>&,
                           const metadata::DocumentQueryOptions&) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (metadataValueCountsError_) {
            return *metadataValueCountsError_;
        }
        return metadataValueCounts_;
    }

    Result<std::optional<metadata::DocumentInfo>>
    getDocumentByHash(const std::string& hash) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (auto it = throwOnLookup_.find(hash); it != throwOnLookup_.end()) {
            throw std::runtime_error(it->second);
        }
        if (missingHashes_.count(hash) > 0) {
            return std::optional<metadata::DocumentInfo>(std::nullopt);
        }
        if (auto it = docsByHash_.find(hash); it != docsByHash_.end()) {
            return std::optional<metadata::DocumentInfo>(it->second);
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

    Result<std::unordered_map<std::string, metadata::MetadataValue>>
    getAllMetadata(int64_t documentId) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (auto it = metadataByIdErrors_.find(documentId); it != metadataByIdErrors_.end()) {
            return it->second;
        }
        if (auto it = metadataById_.find(documentId); it != metadataById_.end()) {
            return it->second;
        }
        return std::unordered_map<std::string, metadata::MetadataValue>{};
    }

    Result<std::optional<metadata::DocumentInfo>>
    findDocumentByExactPath(const std::string&) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (exactPathError_) {
            return *exactPathError_;
        }
        if (exactPathDocument_.has_value()) {
            return exactPathDocument_;
        }
        return std::optional<metadata::DocumentInfo>(std::nullopt);
    }

    Result<std::vector<metadata::DocumentInfo>>
    findDocumentsByPathTreePrefix(std::string_view, bool = true, int limit = 0) override {
        std::lock_guard<std::mutex> lk(mu_);
        if (limit > 0 && static_cast<std::size_t>(limit) < pathTreeDocs_.size()) {
            return std::vector<metadata::DocumentInfo>(pathTreeDocs_.begin(),
                                                       pathTreeDocs_.begin() + limit);
        }
        return pathTreeDocs_;
    }

    Result<void> deleteDocument(int64_t id) override {
        std::lock_guard<std::mutex> lk(mu_);
        deleteCallCount_++;
        if (auto it = deleteErrors_.find(id); it != deleteErrors_.end()) {
            return it->second;
        }
        deletedIds_.push_back(id);
        if (auto it = docsById_.find(id); it != docsById_.end()) {
            docsByHash_.erase(it->second.sha256Hash);
            docsById_.erase(it);
        }
        docs_.erase(
            std::remove_if(docs_.begin(), docs_.end(),
                           [id](const metadata::DocumentInfo& doc) { return doc.id == id; }),
            docs_.end());
        return Result<void>();
    }

private:
    mutable std::mutex mu_;
    std::vector<metadata::DocumentInfo> docs_;
    std::unordered_map<std::string, metadata::DocumentInfo> docsByHash_;
    std::unordered_map<int64_t, metadata::DocumentInfo> docsById_;
    std::optional<Error> queryError_;
    std::optional<Error> snapshotError_;
    std::optional<Error> snapshotDocsError_;
    std::optional<Error> metadataValueCountsError_;
    std::optional<std::string> throwOnQuery_;
    std::unordered_set<std::string> missingHashes_;
    std::unordered_map<std::string, std::string> throwOnLookup_;
    std::unordered_map<int64_t, Error> deleteErrors_;
    std::optional<metadata::DocumentInfo> exactPathDocument_;
    std::optional<Error> exactPathError_;
    std::unordered_map<int64_t, std::unordered_map<std::string, metadata::MetadataValue>>
        metadataById_;
    std::unordered_map<int64_t, Error> metadataByIdErrors_;
    std::vector<metadata::DocumentInfo> pathTreeDocs_;
    std::vector<std::string> snapshots_;
    std::vector<metadata::DocumentInfo> snapshotDocs_;
    std::unordered_map<std::string, std::vector<metadata::MetadataValueCount>> metadataValueCounts_;
    std::size_t deleteCallCount_{0};
    std::vector<int64_t> deletedIds_;
};

class StubContentStore : public api::IContentStore {
public:
    void setBlob(const std::string& hash, const std::string& content) {
        blobs_[hash] = std::vector<std::byte>(reinterpret_cast<const std::byte*>(content.data()),
                                              reinterpret_cast<const std::byte*>(content.data()) +
                                                  content.size());
    }

    void setRemoveResult(const std::string& hash, Result<bool> result) {
        removeResults_[hash] = std::move(result);
    }

    Result<api::StoreResult> storeBytes(std::span<const std::byte> data,
                                        const api::ContentMetadata&) override {
        std::string hash = "stub-hash-" + std::to_string(blobs_.size() + 1);
        blobs_[hash] = std::vector<std::byte>(data.begin(), data.end());
        api::StoreResult result;
        result.contentHash = hash;
        result.bytesStored = blobs_[hash].size();
        return result;
    }

    Result<std::vector<std::byte>> retrieveBytes(const std::string& hash) override {
        auto it = blobs_.find(hash);
        if (it == blobs_.end()) {
            return Error{ErrorCode::NotFound, "content not found"};
        }
        return it->second;
    }

    Result<api::IContentStore::RawContent> retrieveRaw(const std::string& hash) override {
        auto bytes = retrieveBytes(hash);
        if (!bytes) {
            return bytes.error();
        }
        api::IContentStore::RawContent raw;
        raw.data = std::move(bytes.value());
        return raw;
    }

    std::future<Result<api::IContentStore::RawContent>>
    retrieveRawAsync(const std::string& hash) override {
        return std::async(std::launch::deferred, [this, hash]() { return retrieveRaw(hash); });
    }

    Result<api::StoreResult> store(const std::filesystem::path& path, const api::ContentMetadata&,
                                   api::ProgressCallback) override {
        std::ifstream ifs(path, std::ios::binary);
        if (!ifs.good()) {
            return Error{ErrorCode::FileNotFound, "stub file not found"};
        }
        std::vector<std::byte> bytes;
        char ch = 0;
        while (ifs.get(ch)) {
            bytes.push_back(static_cast<std::byte>(ch));
        }
        if (!ifs.eof()) {
            return Error{ErrorCode::IOError, "stub read failed"};
        }
        return storeBytes(bytes, {});
    }
    Result<api::RetrieveResult> retrieve(const std::string&, const std::filesystem::path&,
                                         api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::StoreResult> storeStream(std::istream&, const api::ContentMetadata&,
                                         api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<api::RetrieveResult> retrieveStream(const std::string&, std::ostream&,
                                               api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }
    Result<bool> exists(const std::string&) const override { return ErrorCode::NotImplemented; }
    Result<bool> remove(const std::string& hash) override {
        if (auto it = removeResults_.find(hash); it != removeResults_.end()) {
            return it->second;
        }
        blobs_.erase(hash);
        return true;
    }
    Result<api::ContentMetadata> getMetadata(const std::string&) const override {
        return ErrorCode::NotImplemented;
    }
    Result<void> updateMetadata(const std::string&, const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    std::vector<Result<api::StoreResult>>
    storeBatch(const std::vector<std::filesystem::path>&,
               const std::vector<api::ContentMetadata>&) override {
        return {};
    }
    std::vector<Result<bool>> removeBatch(const std::vector<std::string>&) override { return {}; }
    api::ContentStoreStats getStats() const override { return {}; }
    api::HealthStatus checkHealth() const override { return {}; }
    Result<void> verify(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> compact(api::ProgressCallback) override { return ErrorCode::NotImplemented; }
    Result<void> garbageCollect(api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
    }

private:
    std::unordered_map<std::string, std::vector<std::byte>> blobs_;
    std::unordered_map<std::string, Result<bool>> removeResults_;
};

class StubGraphQueryService : public app::services::IGraphQueryService {
public:
    void addConnectedDocument(std::string hash, std::string relation, int distance = 1,
                              double relevance = 0.0) {
        app::services::GraphConnectedNode node{};
        node.nodeMetadata.documentHash = std::move(hash);
        node.distance = distance;
        node.relevanceScore = relevance;
        app::services::GraphEdgeDescriptor edge{};
        edge.relation = std::move(relation);
        node.connectingEdges.push_back(std::move(edge));
        response_.allConnectedNodes.push_back(std::move(node));
    }

    Result<app::services::GraphQueryResponse>
    query(const app::services::GraphQueryRequest&) override {
        return response_;
    }

    Result<app::services::ListSnapshotsResponse>
    listSnapshots(const app::services::ListSnapshotsRequest&) override {
        return app::services::ListSnapshotsResponse{};
    }

    Result<app::services::PathHistoryResponse>
    getPathHistory(const app::services::PathHistoryRequest&) override {
        return app::services::PathHistoryResponse{};
    }

    Result<std::optional<std::int64_t>> resolveToNodeId(const std::string&) override {
        return std::optional<std::int64_t>(std::nullopt);
    }

private:
    app::services::GraphQueryResponse response_{};
};

std::filesystem::path makeTempDir(const std::string& prefix);

struct GraphDispatcherFixture {
    GraphDispatcherFixture() {
        testDir = makeTempDir("yams_graph_dispatcher_");
        cfg.dataDir = testDir;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        svc = std::make_unique<ServiceManager>(cfg, state, lifecycleFsm);
        dispatcher = std::make_unique<RequestDispatcher>(&lifecycle, svc.get(), &state);
    }

    ~GraphDispatcherFixture() {
        kgStore.reset();
        repo.reset();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove_all(testDir, ec);
    }

    void initMetadata(bool withKg = true) {
        metadata::ConnectionPoolConfig poolConfig{};
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<metadata::ConnectionPool>(
            (testDir / "graph_dispatcher.db").string(), poolConfig);
        REQUIRE(pool->initialize().has_value());

        repo = std::make_shared<metadata::MetadataRepository>(*pool);
        svc->__test_setMetadataRepo(repo);

        if (withKg) {
            metadata::KnowledgeGraphStoreConfig kgConfig{};
            kgConfig.enable_alias_fts = true;
            kgConfig.enable_wal = false;
            auto kgResult = metadata::makeSqliteKnowledgeGraphStore(*pool, kgConfig);
            REQUIRE(kgResult.has_value());
            kgStore = std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(kgResult).value());
            repo->setKnowledgeGraphStore(kgStore);
        }
    }

    int64_t upsertNode(const std::string& key, const std::string& label, const std::string& type,
                       const std::string& properties = {}) {
        REQUIRE(kgStore);
        metadata::KGNode node;
        node.nodeKey = key;
        node.label = label;
        node.type = type;
        if (!properties.empty()) {
            node.properties = properties;
        }
        auto result = kgStore->upsertNode(node);
        REQUIRE(result.has_value());
        return result.value();
    }

    void addEdge(int64_t srcNodeId, int64_t dstNodeId, const std::string& relation,
                 float weight = 1.0f, const std::string& properties = {}) {
        REQUIRE(kgStore);
        metadata::KGEdge edge;
        edge.srcNodeId = srcNodeId;
        edge.dstNodeId = dstNodeId;
        edge.relation = relation;
        edge.weight = weight;
        if (!properties.empty()) {
            edge.properties = properties;
        }
        REQUIRE(kgStore->addEdge(edge).has_value());
    }

    void execSql(const std::string& sql) {
        REQUIRE(pool);
        auto result = pool->withConnection(
            [&](metadata::Database& db) -> Result<void> { return db.execute(sql); });
        REQUIRE(result.has_value());
    }

    std::filesystem::path testDir;
    DaemonConfig cfg;
    StubLifecycle lifecycle;
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    std::unique_ptr<ServiceManager> svc;
    std::unique_ptr<RequestDispatcher> dispatcher;
    std::shared_ptr<metadata::ConnectionPool> pool;
    std::shared_ptr<metadata::MetadataRepository> repo;
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore;
};

std::filesystem::path makeTempDir(const std::string& prefix) {
    auto base = std::filesystem::temp_directory_path();
    auto dir = base / (prefix + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
}

std::filesystem::path writeTempFile(const std::string& prefix, const std::string& name,
                                    const std::string& content) {
    auto path = makeTempDir(prefix) / name;
    std::ofstream out(path, std::ios::binary);
    REQUIRE(out.good());
    out << content;
    out.close();
    REQUIRE(out.good());
    return path;
}

std::filesystem::path createMockExternalPlugin(const std::filesystem::path& baseDir,
                                               const std::string& name) {
    auto pluginDir = baseDir / name;
    std::filesystem::create_directories(pluginDir);

    {
        std::ofstream pluginFile(pluginDir / "plugin.py");
        pluginFile << R"PY(#!/usr/bin/env python3
import json
import sys


def handle_request(req):
    method = req.get("method", "")
    if method == "handshake.manifest":
        return {
            "name": "dispatcher_external_plugin",
            "version": "1.0.0",
            "interfaces": ["content_extractor_v1"]
        }
    if method == "plugin.init":
        return {"status": "initialized"}
    if method == "plugin.health":
        return {"status": "ok"}
    if method == "plugin.shutdown":
        return {"status": "ok"}
    raise ValueError(f"Method not found: {method}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req_id = None
    try:
        req = json.loads(line)
        req_id = req.get("id")
        response = {"jsonrpc": "2.0", "id": req_id, "result": handle_request(req)}
    except Exception as exc:
        response = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32603, "message": str(exc)}
        }
    print(json.dumps(response), flush=True)
)PY";
    }

    {
        std::ofstream manifestFile(pluginDir / "yams-plugin.json");
        manifestFile << R"JSON({
  "name": "dispatcher_external_plugin",
  "version": "1.0.0",
  "interfaces": ["content_extractor_v1"],
  "entry": {
    "fallback_cmd": ["/usr/bin/env", "python3", "-u", "${plugin_dir}/plugin.py"],
    "env": {
      "PYTHONUNBUFFERED": "1"
    }
  }
})JSON";
    }

    return pluginDir;
}

std::string makePythonShimPath() {
    auto shimDir = makeTempDir("yams_python_shim_");
    auto shim = shimDir / "python";

    {
        std::ofstream shimFile(shim);
        shimFile << "#!/bin/sh\n"
                    "exec /usr/bin/env python3 \"$@\"\n";
    }

    std::filesystem::permissions(shim,
                                 std::filesystem::perms::owner_read |
                                     std::filesystem::perms::owner_write |
                                     std::filesystem::perms::owner_exec,
                                 std::filesystem::perm_options::replace);

    if (const char* currentPath = std::getenv("PATH"); currentPath && *currentPath) {
        return shimDir.string() + ":" + currentPath;
    }
    return shimDir.string() + ":/usr/bin:/bin";
}

Response dispatchRequest(RequestDispatcher& dispatcher, const Request& req) {
    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(req), boost::asio::use_future);
    ioc.run();
    return fut.get();
}

std::shared_ptr<SpscQueue<InternalEventBus::StoreDocumentTask>>
getStoreDocumentTaskChannel(std::size_t capacity = 0) {
    auto existing = InternalEventBus::instance().get_channel<InternalEventBus::StoreDocumentTask>(
        "store_document_tasks");
    if (existing) {
        return existing;
    }
    if (capacity == 0) {
        capacity = static_cast<std::size_t>(TuneAdvisor::storeDocumentChannelCapacity());
    }
    return InternalEventBus::instance().get_or_create_channel<InternalEventBus::StoreDocumentTask>(
        "store_document_tasks", capacity);
}

void drainStoreDocumentTasks() {
    auto channel = InternalEventBus::instance().get_channel<InternalEventBus::StoreDocumentTask>(
        "store_document_tasks");
    if (!channel) {
        return;
    }
    InternalEventBus::StoreDocumentTask task;
    while (channel->try_pop(task)) {
    }
}

template <typename Predicate> bool waitForCondition(Predicate&& predicate, int attempts = 100) {
    for (int i = 0; i < attempts; ++i) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return predicate();
}
} // namespace

// =============================================================================
// Embedding Status Tests
// =============================================================================

TEST_CASE("DaemonMetrics: Embedding provider status", "[daemon][metrics][embedding]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_metrics_embed_");
    ServiceManager svc(cfg, state, lifecycleFsm);

    SECTION("Snapshot includes embedding backend information") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/test-model.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setAdoptedProviderPluginName("stub");

        // Model provider is now directly used - no need to ensure generator

        DaemonMetrics metrics(nullptr, &state, &svc, svc.getWorkCoordinator());
        auto snap = metrics.getSnapshot();

        REQUIRE(snap != nullptr);

        // Backend should be "plugin:<name>" format or "unknown"
        bool validBackend =
            (snap->embeddingBackend == "unknown" || snap->embeddingBackend.starts_with("plugin:"));
        REQUIRE(validBackend);
    }

    SECTION("Metrics snapshot doesn't crash without provider") {
        // No provider set
        DaemonMetrics metrics(nullptr, &state, &svc, svc.getWorkCoordinator());
        auto snap = metrics.getSnapshot();

        REQUIRE(snap != nullptr);
        REQUIRE(!snap->embeddingBackend.empty());
    }
}

// =============================================================================
// WAL Metrics Tests
// =============================================================================

TEST_CASE("DaemonMetrics: WAL metrics in GetStats", "[daemon][metrics][wal]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_metrics_wal_");

    YamsDaemon daemon(cfg);
    DaemonLifecycleAdapter lifecycleAdapter(&daemon);
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycleAdapter, &svc, &state);

    SECTION("GetStats includes WAL metric keys") {
        GetStatsRequest req;
        Request r = req;

        boost::asio::io_context ioc;
        auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(r), boost::asio::use_future);
        ioc.run();
        auto resp = fut.get();

        REQUIRE(std::holds_alternative<GetStatsResponse>(resp));

        auto stats = std::get<GetStatsResponse>(resp);

        // WAL metrics should be present
        REQUIRE(stats.additionalStats.count("wal_active_transactions") > 0);
        REQUIRE(stats.additionalStats.count("wal_pending_entries") > 0);
    }
}

TEST_CASE("RequestDispatcher: status includes canonical readiness flags",
          "[daemon][status][readiness]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_status_readiness_flags_");

    YamsDaemon daemon(cfg);
    DaemonLifecycleAdapter lifecycleAdapter(&daemon);
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycleAdapter, &svc, &state);

    StatusRequest req;
    req.detailed = false;
    Request r = req;

    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(r), boost::asio::use_future);
    ioc.run();
    auto resp = fut.get();

    REQUIRE(std::holds_alternative<StatusResponse>(resp));
    const auto& status = std::get<StatusResponse>(resp);

    const std::array<std::string_view, 20> requiredCoreReadinessKeys = {
        readiness::kIpcServer,
        readiness::kContentStore,
        readiness::kDatabase,
        readiness::kMetadataRepo,
        readiness::kSearchEngine,
        readiness::kModelProvider,
        readiness::kVectorIndex,
        readiness::kVectorDb,
        readiness::kPlugins,
        readiness::kVectorDbInitAttempted,
        readiness::kVectorDbReady,
        readiness::kVectorDbDim,
        readiness::kEmbeddingReady,
        readiness::kEmbeddingDegraded,
        readiness::kPluginsReady,
        readiness::kPluginsDegraded,
        readiness::kRepairService,
        readiness::kSearchEngineBuildReasonInitial,
        readiness::kSearchEngineBuildReasonRebuild,
        readiness::kSearchEngineBuildReasonDegraded};

    for (const auto key : requiredCoreReadinessKeys) {
        INFO("missing readiness key: " << key);
        REQUIRE(status.readinessStates.count(std::string(key)) > 0);
    }
}

TEST_CASE("RequestDispatcher: status includes repair metrics and flags",
          "[daemon][status][repair]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_status_repair_metrics_");

    YamsDaemon daemon(cfg);
    DaemonLifecycleAdapter lifecycleAdapter(&daemon);
    StateComponent state;
    state.stats.repairQueueDepth.store(7);
    state.stats.repairBatchesAttempted.store(11);
    state.stats.repairEmbeddingsGenerated.store(5);
    state.stats.repairEmbeddingsSkipped.store(2);
    state.stats.repairFailedOperations.store(1);
    state.stats.repairIdleTicks.store(99);
    state.stats.repairBusyTicks.store(33);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycleAdapter, &svc, &state);

    StatusRequest req;
    req.detailed = true;
    Request r = req;

    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(r), boost::asio::use_future);
    ioc.run();
    auto resp = fut.get();

    REQUIRE(std::holds_alternative<StatusResponse>(resp));
    const auto& status = std::get<StatusResponse>(resp);

    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairQueueDepth)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairBatchesAttempted)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairEmbeddingsGenerated)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairEmbeddingsSkipped)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairFailedOperations)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairIdleTicks)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairBusyTicks)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairRunning)) > 0);
    REQUIRE(status.requestCounts.count(std::string(metrics::kRepairInProgress)) > 0);
}

TEST_CASE("RequestDispatcher: status responds even when lifecycle not ready",
          "[daemon][status][readiness]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_status_not_ready_");

    YamsDaemon daemon(cfg);
    DaemonLifecycleAdapter lifecycleAdapter(&daemon);
    // Daemon is not started; lifecycle state is expected to be non-ready.
    {
        const auto snap = daemon.getLifecycle().snapshot();
        REQUIRE(snap.state != LifecycleState::Ready);
        REQUIRE(snap.state != LifecycleState::Degraded);
    }

    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycleAdapter, &svc, &state);

    StatusRequest req;
    req.detailed = true;
    Request r = req;

    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(r), boost::asio::use_future);
    ioc.run();
    auto resp = fut.get();

    REQUIRE(std::holds_alternative<StatusResponse>(resp));
}

TEST_CASE("RequestDispatcher: repair handler surfaces missing service states",
          "[daemon][repair][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_repair_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    RepairRequest repairReq;
    repairReq.repairOrphans = true;
    repairReq.dryRun = true;

    SECTION("returns not initialized when ServiceManager is absent") {
        RequestDispatcher noServiceDispatcher(&lifecycle, nullptr, &state);
        Request req = repairReq;

        auto resp = dispatchRequest(noServiceDispatcher, req);

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("ServiceManager") != std::string::npos);
    }

    SECTION("returns not initialized when RepairService is not running") {
        Request req = repairReq;

        auto resp = dispatchRequest(dispatcher, req);

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("RepairService not running") != std::string::npos);
        CHECK(state.stats.repairInProgress.load(std::memory_order_relaxed) == false);
    }

    SECTION("delegates to RepairService and toggles worker metrics") {
        svc.startRepairService([] { return size_t{0}; });
        auto repairService = svc.getRepairServiceShared();
        REQUIRE(repairService != nullptr);

        Request req = repairReq;
        auto resp = dispatchRequest(dispatcher, req);

        REQUIRE(std::holds_alternative<RepairResponse>(resp));
        const auto& repairResp = std::get<RepairResponse>(resp);
        CHECK(repairResp.success);
        CHECK(repairResp.totalOperations == 1);
        REQUIRE(repairResp.operationResults.size() == 1);
        CHECK(repairResp.operationResults.front().operation == "orphans");
        CHECK_FALSE(repairResp.operationResults.front().message.empty());
        CHECK(svc.getWorkerPosted() == 1);
        CHECK(svc.getWorkerCompleted() == 1);
        CHECK(svc.getWorkerActive() == 0);
        CHECK(state.stats.repairInProgress.load(std::memory_order_relaxed) == false);

        svc.stopRepairService();
    }

    SECTION("converts RepairService exceptions into error responses") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        repo->setThrowOnQuery("repair query exception");
        svc.__test_setMetadataRepo(repo);
        svc.startRepairService([] { return size_t{0}; });
        auto repairService = svc.getRepairServiceShared();
        REQUIRE(repairService != nullptr);

        RepairRequest throwingReq;
        throwingReq.repairDownloads = true;
        Request req = throwingReq;

        auto resp = dispatchRequest(dispatcher, req);

        REQUIRE(std::holds_alternative<RepairResponse>(resp));
        const auto& repairResp = std::get<RepairResponse>(resp);
        CHECK_FALSE(repairResp.success);
        CHECK(repairResp.totalOperations == 0);
        REQUIRE(repairResp.errors.size() == 1);
        CHECK(repairResp.errors.front() == "Internal error: repair query exception");
        CHECK(repairResp.operationResults.empty());
        CHECK(svc.getWorkerPosted() == 1);
        CHECK(svc.getWorkerCompleted() == 1);
        CHECK(svc.getWorkerActive() == 0);
        CHECK(state.stats.repairInProgress.load(std::memory_order_relaxed) == false);

        svc.stopRepairService();
    }
}

TEST_CASE("RequestDispatcher: tree diff handler validates inputs and missing metadata repo",
          "[daemon][tree-diff][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_tree_diff_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    SECTION("returns invalid argument when snapshot ids are missing") {
        ListTreeDiffRequest req;
        req.targetSnapshotId = "target-only";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message.find("baseSnapshotId") != std::string::npos);
    }

    SECTION("returns internal error when metadata repository is unavailable") {
        ListTreeDiffRequest req;
        req.baseSnapshotId = "base";
        req.targetSnapshotId = "target";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository not available") != std::string::npos);
    }
}

TEST_CASE("RequestDispatcher: prune handler reports missing metadata repo",
          "[daemon][prune][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_prune_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    PruneRequest req;
    req.dryRun = true;
    req.extensions = {"log"};

    auto resp = dispatchRequest(dispatcher, Request{req});

    REQUIRE(std::holds_alternative<ErrorResponse>(resp));
    const auto& err = std::get<ErrorResponse>(resp);
    CHECK(err.code == ErrorCode::InternalError);
    CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
}

TEST_CASE("RequestDispatcher: prune handler covers parsing and execution branches",
          "[daemon][prune][dispatcher]") {
    auto makeDoc = [](int64_t id, std::string filePath, std::string ext, int64_t size,
                      std::string hash, int64_t ageDays) {
        metadata::DocumentInfo doc{};
        doc.id = id;
        doc.filePath = std::move(filePath);
        doc.fileName = std::filesystem::path(doc.filePath).filename().string();
        doc.fileExtension = std::move(ext);
        doc.fileSize = size;
        doc.sha256Hash = std::move(hash);
        doc.mimeType = "text/plain";
        const auto now =
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.modifiedTime = now - std::chrono::hours(ageDays * 24);
        doc.indexedTime = now;
        return doc;
    };

    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_prune_dispatcher_live_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    auto repo = std::make_shared<StubPruneMetadataRepository>();
    svc.__test_setMetadataRepo(repo);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    repo->addDocument(makeDoc(1, "build/output.log", "log", 2048, std::string(64, 'a'), 10));
    repo->addDocument(
        makeDoc(2, "logs/server.log", ".log", 3 * 1024 * 1024, std::string(64, 'b'), 14));
    repo->addDocument(makeDoc(3, "notes/readme.txt", "txt", 64, std::string(64, 'c'), 2));
    repo->addDocument(makeDoc(4, "cache/old.tmp", "tmp", 4096, std::string(64, 'd'), 500));

    SECTION("dry run parses age and size filters across units") {
        PruneRequest req;
        req.dryRun = true;
        req.olderThan = "1w";
        req.largerThan = "1KB";
        req.smallerThan = "5MB";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.filesDeleted == 0);
        CHECK(pruneResp.filesFailed == 0);
        CHECK(pruneResp.totalBytesFreed == (2048 + 3 * 1024 * 1024 + 4096));
        REQUIRE(pruneResp.categoryCounts.count("build-object") == 1);
        REQUIRE(pruneResp.categoryCounts.count("none") == 1);
        REQUIRE(pruneResp.categoryCounts.count("temp") == 1);
        CHECK(pruneResp.categoryCounts.at("build-object") == 1);
        CHECK(pruneResp.categoryCounts.at("none") == 1);
        CHECK(pruneResp.categoryCounts.at("temp") == 1);
    }

    SECTION("dry run accepts month and year age units") {
        PruneRequest monthReq;
        monthReq.dryRun = true;
        monthReq.olderThan = "1m";

        auto monthResp = dispatchRequest(dispatcher, Request{monthReq});

        REQUIRE(std::holds_alternative<PruneResponse>(monthResp));
        const auto& monthPruneResp = std::get<PruneResponse>(monthResp);
        REQUIRE(monthPruneResp.categoryCounts.count("temp") == 1);
        CHECK(monthPruneResp.categoryCounts.at("temp") == 1);
        CHECK(monthPruneResp.categoryCounts.count("logs") == 0);

        PruneRequest yearReq;
        yearReq.dryRun = true;
        yearReq.olderThan = "1y";

        auto yearResp = dispatchRequest(dispatcher, Request{yearReq});

        REQUIRE(std::holds_alternative<PruneResponse>(yearResp));
        const auto& yearPruneResp = std::get<PruneResponse>(yearResp);
        CHECK(yearPruneResp.filesDeleted == 0);
        CHECK(yearPruneResp.totalBytesFreed == 4096);
        REQUIRE(yearPruneResp.categoryCounts.count("temp") == 1);
        CHECK(yearPruneResp.categoryCounts.at("temp") == 1);
    }

    SECTION("dry run accepts gigabyte size units") {
        PruneRequest req;
        req.dryRun = true;
        req.largerThan = "1GB";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.totalBytesFreed == 0);
        CHECK(pruneResp.categoryCounts.empty());
    }

    SECTION("dry run falls back on unknown age unit and raw byte sizes") {
        PruneRequest req;
        req.dryRun = true;
        req.olderThan = "3q";
        req.largerThan = "2000";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.totalBytesFreed == (2048 + 3 * 1024 * 1024 + 4096));
        REQUIRE(pruneResp.categoryCounts.count("build-object") == 1);
        REQUIRE(pruneResp.categoryCounts.count("none") == 1);
        REQUIRE(pruneResp.categoryCounts.count("temp") == 1);
        CHECK(pruneResp.categoryCounts.at("build-object") == 1);
        CHECK(pruneResp.categoryCounts.at("none") == 1);
        CHECK(pruneResp.categoryCounts.at("temp") == 1);
    }

    SECTION("dry run filters by extension and categories") {
        PruneRequest req;
        req.dryRun = true;
        req.categories = {"build"};
        req.extensions = {"log"};

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.totalBytesFreed == 2048);
        CHECK(pruneResp.categoryCounts.size() == 1);
        REQUIRE(pruneResp.categoryCounts.count("build-object") == 1);
        CHECK(pruneResp.categoryCounts.at("build-object") == 1);
    }

    SECTION("dry run matches dotted extensions when category is none") {
        PruneRequest req;
        req.dryRun = true;
        req.categories = {"none"};
        req.extensions = {"log"};

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.totalBytesFreed == 3 * 1024 * 1024);
        CHECK(pruneResp.categoryCounts.size() == 1);
        REQUIRE(pruneResp.categoryCounts.count("none") == 1);
        CHECK(pruneResp.categoryCounts.at("none") == 1);
    }

    SECTION("dry run handles query failures from repository") {
        repo->setQueryError(Error{ErrorCode::InternalError, "query boom"});

        auto resp = dispatchRequest(dispatcher, Request{PruneRequest{}});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.errorMessage.empty());
        CHECK(pruneResp.totalBytesFreed == 0);
        CHECK(pruneResp.categoryCounts.empty());
    }

    SECTION("dry run handles repository exceptions") {
        repo->setThrowOnQuery("query exception");

        auto resp = dispatchRequest(dispatcher, Request{PruneRequest{}});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.errorMessage == "Failed to query candidates: query exception");
    }

    SECTION("apply mode counts deleted missing failed and exception cases") {
        repo->setMissingHash(std::string(64, 'b'));
        repo->setDeleteError(4, Error{ErrorCode::InternalError, "delete failed"});
        repo->setThrowOnLookupHash(std::string(64, 'c'), "lookup boom");

        PruneRequest req;
        req.dryRun = false;
        req.olderThan = "1d";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.filesDeleted == 2);
        CHECK(pruneResp.filesFailed == 2);
        CHECK(pruneResp.totalBytesFreed == 2048);
        REQUIRE(pruneResp.categoryCounts.count("build-object") == 1);
        REQUIRE(pruneResp.categoryCounts.count("none") == 1);
        REQUIRE(pruneResp.categoryCounts.count("temp") == 1);
        CHECK(pruneResp.categoryCounts.at("build-object") == 1);
        CHECK(pruneResp.categoryCounts.at("none") == 2);
        CHECK(pruneResp.categoryCounts.at("temp") == 1);
        CHECK(repo->deleteCallCount() == 2);
        CHECK(repo->deletedIds() == std::vector<int64_t>{1});
    }

    SECTION("apply mode yields during large prune batches") {
        for (int i = 0; i < 105; ++i) {
            repo->addDocument(makeDoc(1000 + i, "cache/bulk_" + std::to_string(i) + ".tmp", "tmp",
                                      2048, "bulk-hash-" + std::to_string(i), 30));
        }

        PruneRequest req;
        req.dryRun = false;
        req.categories = {"temp"};
        req.extensions = {"tmp"};
        req.olderThan = "1d";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.filesDeleted == 106);
        CHECK(pruneResp.filesFailed == 0);
        CHECK(pruneResp.totalBytesFreed == (4096 + 105 * 2048));
        REQUIRE(pruneResp.categoryCounts.count("temp") == 1);
        CHECK(pruneResp.categoryCounts.at("temp") == 106);
        CHECK(repo->deleteCallCount() == 106);
        CHECK(repo->deletedIds().size() == 106);
    }

    SECTION("dry run leaves empty size filters as zero") {
        PruneRequest req;
        req.dryRun = true;
        req.largerThan = "";
        req.smallerThan = "";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PruneResponse>(resp));
        const auto& pruneResp = std::get<PruneResponse>(resp);
        CHECK(pruneResp.totalBytesFreed == (2048 + 3 * 1024 * 1024 + 64 + 4096));
        REQUIRE(pruneResp.categoryCounts.count("build-object") == 1);
        REQUIRE(pruneResp.categoryCounts.count("none") == 1);
        REQUIRE(pruneResp.categoryCounts.count("temp") == 1);
        CHECK(pruneResp.categoryCounts.at("build-object") == 1);
        CHECK(pruneResp.categoryCounts.at("none") == 2);
        CHECK(pruneResp.categoryCounts.at("temp") == 1);
    }
}

TEST_CASE("RequestDispatcher: model handlers cover validation and status branches",
          "[daemon][model][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_model_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    SECTION("load model rejects missing name when provider exists") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/model.onnx");
        svc.__test_setModelProvider(provider);

        LoadModelRequest req;
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidData);
        CHECK(err.message == "modelName is required");
    }

    SECTION("load model with options succeeds and records provider state") {
        auto provider = std::make_shared<StubModelProvider>(768, "/tmp/provider-model.onnx");
        svc.__test_setModelProvider(provider);

        LoadModelRequest req;
        req.modelName = "dispatcher-model";
        req.optionsJson = R"({"mode":"offline"})";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        const auto& loadResp = std::get<ModelLoadResponse>(resp);
        CHECK(loadResp.success);
        CHECK(loadResp.modelName == "dispatcher-model");
        CHECK(provider->isModelLoaded("dispatcher-model"));
        CHECK(provider->lastOptionsModel() == "dispatcher-model");
        CHECK(provider->lastOptionsJson() == req.optionsJson);
    }

    SECTION("load model failure marks embedding degraded") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/failing-model.onnx");
        provider->setLoadError(Error{ErrorCode::InvalidState, "provider refused load"});
        svc.__test_setModelProvider(provider);
        lifecycle.resetDegradedState();

        LoadModelRequest req;
        req.modelName = "failing-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == "provider refused load");
        CHECK(lifecycle.setSubsystemDegradedCalls() == 1);
        CHECK(lifecycle.lastSubsystem() == "embedding");
        CHECK(lifecycle.lastDegraded());
        CHECK(lifecycle.lastReason() == "provider_load_failed");
    }

    SECTION("load model sanitizes invalid utf8 in provider errors") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/utf8-model.onnx");
        std::string invalidMessage = "prefix ";
        invalidMessage.push_back(static_cast<char>(0xC2));
        invalidMessage.push_back(static_cast<char>(0xA9));
        invalidMessage.push_back(static_cast<char>(0xE2));
        invalidMessage.push_back(static_cast<char>(0x82));
        invalidMessage.push_back(static_cast<char>(0xAC));
        invalidMessage.push_back(static_cast<char>(0xF0));
        invalidMessage.push_back(static_cast<char>(0x9F));
        invalidMessage.push_back(static_cast<char>(0x92));
        invalidMessage.push_back(static_cast<char>(0xA9));
        invalidMessage.push_back(static_cast<char>(0xC0));
        invalidMessage.push_back(static_cast<char>(0xFF));
        provider->setLoadError(Error{ErrorCode::InvalidState, invalidMessage});
        svc.__test_setModelProvider(provider);

        LoadModelRequest req;
        req.modelName = "utf8-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        std::string expected = "prefix ";
        expected.push_back(static_cast<char>(0xC2));
        expected.push_back(static_cast<char>(0xA9));
        expected.push_back(static_cast<char>(0xE2));
        expected.push_back(static_cast<char>(0x82));
        expected.push_back(static_cast<char>(0xAC));
        expected.push_back(static_cast<char>(0xF0));
        expected.push_back(static_cast<char>(0x9F));
        expected.push_back(static_cast<char>(0x92));
        expected.push_back(static_cast<char>(0xA9));
        expected += "\xEF\xBF\xBD\xEF\xBF\xBD";
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == expected);
    }

    SECTION("load model returns internal error when provider throws after load") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/throwing-model.onnx");
        provider->setThrowOnMemoryUsage(true);
        svc.__test_setModelProvider(provider);

        LoadModelRequest req;
        req.modelName = "throwing-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Load model failed: memory boom") != std::string::npos);
    }

    SECTION("load model skips rebuild when model is already loaded") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/already-loaded-model.onnx");
        REQUIRE(provider->loadModel("already-loaded-model").has_value());
        svc.__test_setModelProvider(provider);

        LoadModelRequest req;
        req.modelName = "already-loaded-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        const auto& loadResp = std::get<ModelLoadResponse>(resp);
        CHECK(loadResp.success);
        CHECK(loadResp.modelName == "already-loaded-model");
        CHECK(provider->getLoadedModelCount() == 1);
    }

    SECTION("load model honors timeout env floor") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/timeout-model.onnx");
        svc.__test_setModelProvider(provider);
        EnvGuard timeoutGuard("YAMS_MODEL_LOAD_TIMEOUT_MS", "5");

        LoadModelRequest req;
        req.modelName = "timeout-clamped-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        CHECK(std::get<ModelLoadResponse>(resp).success);
        CHECK(provider->isModelLoaded("timeout-clamped-model"));
    }

    SECTION("load model survives invalid timeout env value") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/timeout-invalid.onnx");
        svc.__test_setModelProvider(provider);
        EnvGuard timeoutGuard("YAMS_MODEL_LOAD_TIMEOUT_MS", "not-a-number");

        LoadModelRequest req;
        req.modelName = "timeout-invalid-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        CHECK(std::get<ModelLoadResponse>(resp).success);
        CHECK(provider->isModelLoaded("timeout-invalid-model"));
    }

    SECTION("load model rebuilds when cached search engine reports missing embedding generator") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/rebuild-health.onnx");
        REQUIRE(provider->loadModel("already-loaded-health-model").has_value());
        svc.__test_setModelProvider(provider);

        metadata::ConnectionPoolConfig poolCfg{};
        poolCfg.enableWAL = false;
        poolCfg.enableForeignKeys = false;
        auto pool = std::make_shared<metadata::ConnectionPool>(":memory:", poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        search::SearchEngineConfig config;
        config.vectorWeight = 1.0f;
        auto unhealthyEngine =
            std::make_shared<search::SearchEngine>(repo, nullptr, nullptr, nullptr, config);
        svc.__test_setCachedSearchEngine(unhealthyEngine, true);

        LoadModelRequest req;
        req.modelName = "already-loaded-health-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        CHECK(std::get<ModelLoadResponse>(resp).success);
        CHECK(provider->getLoadedModelCount() == 1);
    }

    SECTION("load model ignores isModelLoaded exceptions before loading") {
        auto provider = std::make_shared<StubModelProvider>(4, "/tmp/isloaded-throw.onnx");
        svc.__test_setModelProvider(provider);

        metadata::ConnectionPoolConfig poolCfg{};
        poolCfg.enableWAL = false;
        poolCfg.enableForeignKeys = false;
        auto pool = std::make_shared<metadata::ConnectionPool>(":memory:", poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        yams::vector::VectorDatabaseConfig vectorCfg{};
        vectorCfg.database_path = ":memory:";
        vectorCfg.embedding_dim = 4;
        vectorCfg.create_if_missing = true;
        vectorCfg.use_in_memory = true;
        auto vectorDb = std::make_shared<yams::vector::VectorDatabase>(vectorCfg);
        REQUIRE(vectorDb->initialize());

        search::SearchEngineConfig config;
        config.vectorWeight = 1.0f;
        auto healthyEngine = std::make_shared<search::SearchEngine>(
            repo, vectorDb, provider->getEmbeddingGenerator(), nullptr, config);
        svc.__test_setCachedSearchEngine(healthyEngine, true);
        provider->setThrowOnIsModelLoadedCount(1);

        LoadModelRequest req;
        req.modelName = "isloaded-throw-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        const auto& loadResp = std::get<ModelLoadResponse>(resp);
        CHECK(loadResp.success);
        CHECK(loadResp.modelName == "isloaded-throw-model");
        provider->setThrowOnIsModelLoaded(false);
        CHECK(provider->isModelLoaded("isloaded-throw-model"));
    }

    SECTION("load model survives lifecycle degradation callback exceptions") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/lifecycle-throw.onnx");
        svc.__test_setModelProvider(provider);
        lifecycle.setThrowOnSetSubsystemDegraded(true);

        LoadModelRequest req;
        req.modelName = "lifecycle-throw-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelLoadResponse>(resp));
        const auto& loadResp = std::get<ModelLoadResponse>(resp);
        CHECK(loadResp.success);
        CHECK(loadResp.modelName == "lifecycle-throw-model");
        CHECK(provider->isModelLoaded("lifecycle-throw-model"));
        lifecycle.setThrowOnSetSubsystemDegraded(false);
    }

    SECTION("unload model rejects missing name") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/model.onnx");
        svc.__test_setModelProvider(provider);

        UnloadModelRequest req;
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidData);
        CHECK(err.message == "modelName is required");
    }

    SECTION("unload model propagates provider missing-model error") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/model.onnx");
        svc.__test_setModelProvider(provider);

        UnloadModelRequest req;
        req.modelName = "missing-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
    }

    SECTION("unload model reports provider not ready when no provider is available") {
        auto resp = dispatchRequest(dispatcher, Request{UnloadModelRequest{"missing-provider"}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message.find("Plugin system") != std::string::npos);
    }

    SECTION("model status returns loaded model details and filters by name") {
        auto provider = std::make_shared<StubModelProvider>(512, "/tmp/status-model.onnx");
        svc.__test_setModelProvider(provider);
        REQUIRE(provider->loadModel("status-model").has_value());

        ModelStatusRequest req;
        req.modelName = "status-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ModelStatusResponse>(resp));
        const auto& statusResp = std::get<ModelStatusResponse>(resp);
        REQUIRE(statusResp.models.size() == 1);
        CHECK(statusResp.models.front().name == "status-model");
        CHECK(statusResp.models.front().loaded);
        CHECK(statusResp.models.front().embeddingDim == 512);
    }

    SECTION("model status handles model info lookup failure") {
        auto provider = std::make_shared<StubModelProvider>(256, "/tmp/status-fallback.onnx");
        REQUIRE(provider->loadModel("status-fallback").has_value());
        provider->setFailModelInfo(true);
        svc.__test_setModelProvider(provider);

        auto resp = dispatchRequest(dispatcher, Request{ModelStatusRequest{}});

        REQUIRE(std::holds_alternative<ModelStatusResponse>(resp));
        const auto& statusResp = std::get<ModelStatusResponse>(resp);
        REQUIRE(statusResp.models.size() == 1);
        CHECK(statusResp.models.front().name == "status-fallback");
        CHECK(statusResp.models.front().memoryMb == 0);
        CHECK(statusResp.models.front().maxSequenceLength == 0);
        CHECK(statusResp.models.front().embeddingDim == 256);
    }

    SECTION("model status returns empty response when provider unavailable") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/model.onnx");
        provider->setAvailable(false);
        svc.__test_setModelProvider(provider);

        auto resp = dispatchRequest(dispatcher, Request{ModelStatusRequest{}});

        REQUIRE(std::holds_alternative<ModelStatusResponse>(resp));
        const auto& statusResp = std::get<ModelStatusResponse>(resp);
        CHECK(statusResp.models.empty());
        CHECK(statusResp.totalMemoryMb == 0);
    }

    SECTION("model status converts provider exceptions into internal errors") {
        auto provider = std::make_shared<StubModelProvider>(384, "/tmp/model.onnx");
        REQUIRE(provider->loadModel("status-throw").has_value());
        provider->setThrowOnMemoryUsage(true);
        svc.__test_setModelProvider(provider);

        auto resp = dispatchRequest(dispatcher, Request{ModelStatusRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("memory boom") != std::string::npos);
    }
}

TEST_CASE("RequestDispatcher: document handlers cover direct helper and error branches",
          "[daemon][documents][dispatcher]") {
    auto makeDoc = [](int64_t id, std::string filePath, std::string hash) {
        metadata::DocumentInfo doc{};
        doc.id = id;
        doc.filePath = std::move(filePath);
        doc.fileName = std::filesystem::path(doc.filePath).filename().string();
        doc.fileExtension = std::filesystem::path(doc.filePath).extension().string();
        doc.fileSize = 12;
        doc.sha256Hash = std::move(hash);
        doc.mimeType = "text/plain";
        doc.setIndexedTime(1710000000 + id);
        return doc;
    };

    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_documents_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    SECTION("prepareSession returns missing-session error for unknown session") {
        EnvGuard stateHome("XDG_STATE_HOME",
                           makeTempDir("yams_prepare_missing_session_").string().c_str());

        RequestDispatcher::PrepareSessionOptions opts;
        opts.sessionName = "missing";

        CHECK(dispatcher.prepareSession(opts) == -2);
    }

    SECTION("prepareSession warms current session documents") {
        auto stateRoot = makeTempDir("yams_prepare_session_");
        EnvGuard stateHome("XDG_STATE_HOME", stateRoot.string().c_str());

        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(7, "src/demo.cpp", "warm-hash");
        repo->setPathTreeDocuments({doc});
        store->setBlob("warm-hash", "hello snippet world");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        auto sessionSvc = app::services::makeSessionService(nullptr);
        REQUIRE(sessionSvc != nullptr);
        sessionSvc->init("warm-session", "coverage");
        sessionSvc->addPathSelector("src/**/*.cpp", {}, {});

        RequestDispatcher::PrepareSessionOptions opts;
        opts.sessionName = "warm-session";
        opts.limit = 5;
        opts.snippetLen = 5;

        CHECK(dispatcher.prepareSession(opts) == 1);

        auto materialized = sessionSvc->listMaterialized("warm-session");
        REQUIRE(materialized.size() == 1);
        CHECK(materialized.front().hash == "warm-hash");
        CHECK(materialized.front().snippet == "hello");
    }

    SECTION("cancel request succeeds when context is registered") {
        auto ctx = std::make_shared<RequestContext>();
        RequestContextRegistry::instance().register_context(4242, ctx);

        auto resp = dispatchRequest(dispatcher, Request{CancelRequest{4242}});

        RequestContextRegistry::instance().deregister_context(4242);
        REQUIRE(std::holds_alternative<SuccessResponse>(resp));
        CHECK(std::get<SuccessResponse>(resp).message == "Cancel accepted");
        CHECK(ctx->canceled.load(std::memory_order_relaxed));
    }

    SECTION("get init rejects missing hash and name") {
        auto resp = dispatchRequest(dispatcher, Request{GetInitRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message.find("hash or name required") != std::string::npos);
    }

    SECTION("get init reports missing content store") {
        GetInitRequest req;
        req.hash = "missing-store-hash";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("content store unavailable") != std::string::npos);
    }

    SECTION("get init reports name resolution failure") {
        GetInitRequest req;
        req.name = "missing-name";
        req.byName = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("Metadata repository not available") != std::string::npos);
    }

    SECTION("get init reports retrieve bytes failure") {
        auto store = std::make_shared<StubContentStore>();
        svc.__test_setContentStore(store);

        GetInitRequest req;
        req.hash = "missing-bytes";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("retrieveBytes failed") != std::string::npos);
    }

    SECTION("get init reports missing retrieval session manager") {
        auto store = std::make_shared<StubContentStore>();
        store->setBlob("needs-session-hash", "hello retrieval session");
        svc.__test_setContentStore(store);

        GetInitRequest req;
        req.hash = "needs-session-hash";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("retrieval session manager unavailable") != std::string::npos);
    }

    SECTION("get init chunk and end succeed across retrieval session lifecycle") {
        auto store = std::make_shared<StubContentStore>();
        store->setBlob("session-flow-hash", "hello retrieval flow");
        svc.__test_setContentStore(store);
        svc.__test_setRetrievalSessionManager(std::make_unique<RetrievalSessionManager>());

        GetInitRequest initReq;
        initReq.hash = "session-flow-hash";
        initReq.chunkSize = 5;

        auto initResp = dispatchRequest(dispatcher, Request{initReq});

        REQUIRE(std::holds_alternative<GetInitResponse>(initResp));
        const auto& init = std::get<GetInitResponse>(initResp);
        CHECK(init.transferId > 0);
        CHECK(init.totalSize == 20);
        CHECK(init.chunkSize == 5);
        CHECK(init.metadata.at("hash") == "session-flow-hash");

        auto chunkResp =
            dispatchRequest(dispatcher, Request{GetChunkRequest{init.transferId, 0, 5}});

        REQUIRE(std::holds_alternative<GetChunkResponse>(chunkResp));
        const auto& chunk = std::get<GetChunkResponse>(chunkResp);
        CHECK(chunk.data == "hello");
        CHECK(chunk.bytesRemaining == 15);

        auto endResp = dispatchRequest(dispatcher, Request{GetEndRequest{init.transferId}});

        REQUIRE(std::holds_alternative<SuccessResponse>(endResp));
        CHECK(std::get<SuccessResponse>(endResp).message == "OK");

        auto postEndChunk =
            dispatchRequest(dispatcher, Request{GetChunkRequest{init.transferId, 0, 5}});

        REQUIRE(std::holds_alternative<ErrorResponse>(postEndChunk));
        const auto& postEndErr = std::get<ErrorResponse>(postEndChunk);
        CHECK(postEndErr.code == ErrorCode::NotFound);
        CHECK(postEndErr.message.find("Invalid transferId") != std::string::npos);
    }

    SECTION("get chunk rejects offsets beyond total size") {
        auto store = std::make_shared<StubContentStore>();
        store->setBlob("offset-error-hash", "hello retrieval flow");
        svc.__test_setContentStore(store);
        svc.__test_setRetrievalSessionManager(std::make_unique<RetrievalSessionManager>());

        GetInitRequest initReq;
        initReq.hash = "offset-error-hash";

        auto initResp = dispatchRequest(dispatcher, Request{initReq});

        REQUIRE(std::holds_alternative<GetInitResponse>(initResp));
        const auto& init = std::get<GetInitResponse>(initResp);

        auto chunkResp =
            dispatchRequest(dispatcher, Request{GetChunkRequest{init.transferId, 99, 5}});

        REQUIRE(std::holds_alternative<ErrorResponse>(chunkResp));
        const auto& err = std::get<ErrorResponse>(chunkResp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message.find("Offset beyond total size") != std::string::npos);
    }

    SECTION("get chunk respects max bytes cap and can return empty chunk") {
        auto store = std::make_shared<StubContentStore>();
        store->setBlob("cap-flow-hash", "hello retrieval flow");
        svc.__test_setContentStore(store);
        svc.__test_setRetrievalSessionManager(std::make_unique<RetrievalSessionManager>());

        GetInitRequest initReq;
        initReq.hash = "cap-flow-hash";
        initReq.chunkSize = 10;
        initReq.maxBytes = 7;

        auto initResp = dispatchRequest(dispatcher, Request{initReq});

        REQUIRE(std::holds_alternative<GetInitResponse>(initResp));
        const auto& init = std::get<GetInitResponse>(initResp);

        auto chunkResp =
            dispatchRequest(dispatcher, Request{GetChunkRequest{init.transferId, 7, 5}});

        REQUIRE(std::holds_alternative<GetChunkResponse>(chunkResp));
        const auto& chunk = std::get<GetChunkResponse>(chunkResp);
        CHECK(chunk.data.empty());
        CHECK(chunk.bytesRemaining == 0);
    }

    SECTION("get chunk reports missing retrieval session manager") {
        auto resp = dispatchRequest(dispatcher, Request{GetChunkRequest{9999, 0, 4}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("retrieval session manager unavailable") != std::string::npos);
    }

    SECTION("get chunk reports invalid transfer id") {
        svc.__test_setRetrievalSessionManager(std::make_unique<RetrievalSessionManager>());

        auto resp = dispatchRequest(dispatcher, Request{GetChunkRequest{9999, 0, 4}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message.find("Invalid transferId") != std::string::npos);
    }

    SECTION("get end succeeds even without retrieval session manager") {
        auto resp = dispatchRequest(dispatcher, Request{GetEndRequest{777}});

        REQUIRE(std::holds_alternative<SuccessResponse>(resp));
        CHECK(std::get<SuccessResponse>(resp).message == "OK");
    }

    SECTION("delete rejects directory removal without recursive flag") {
        DeleteRequest req;
        req.directory = "cache";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message.find("recursive flag") != std::string::npos);
    }

    SECTION("delete recursive directory returns not found when no documents match") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto store = std::make_shared<StubContentStore>();
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        DeleteRequest req;
        req.directory = "cache";
        req.recursive = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message.find("No documents found matching criteria") != std::string::npos);
    }

    SECTION("delete reports mixed success and failure results") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto store = std::make_shared<StubContentStore>();
        auto successDoc = makeDoc(21, "/tmp/delete/ok.txt", "delete-ok-hash");
        auto failedDoc = makeDoc(22, "/tmp/delete/fail.txt", "delete-fail-hash");
        repo->addDocument(successDoc);
        repo->addDocument(failedDoc);
        store->setBlob(successDoc.sha256Hash, "ok");
        store->setBlob(failedDoc.sha256Hash, "fail");
        store->setRemoveResult(failedDoc.sha256Hash,
                               Error{ErrorCode::IOError, "store remove failed"});
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        DeleteRequest req;
        req.pattern = "*.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<DeleteResponse>(resp));
        const auto& deleteResp = std::get<DeleteResponse>(resp);
        CHECK_FALSE(deleteResp.dryRun);
        CHECK(deleteResp.successCount == 1);
        CHECK(deleteResp.failureCount == 1);
        REQUIRE(deleteResp.results.size() == 2);
        CHECK(lifecycle.removedHashes().size() == 1);
        CHECK(lifecycle.removedHashes().front() == successDoc.sha256Hash);
    }

    SECTION("delete reports missing content store") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        svc.__test_setMetadataRepo(repo);

        DeleteRequest req;
        req.hash = "no-store-hash";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("Content store not available") != std::string::npos);
    }

    SECTION("get request maps related documents when graph view is enabled") {
        auto repoPath = makeTempDir("yams_get_graph_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto graphQuery = std::make_shared<StubGraphQueryService>();
        auto mainDoc = makeDoc(69, "/tmp/get-graph/main.txt",
                               "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        auto relatedDoc =
            makeDoc(70, "/tmp/get-graph/related.txt",
                    "1111111111111111111111111111111111111111111111111111111111111111");
        auto mainId = repo->insertDocument(mainDoc);
        auto relatedId = repo->insertDocument(relatedDoc);
        REQUIRE(mainId.has_value());
        REQUIRE(relatedId.has_value());
        REQUIRE(repo->upsertPathTreeForDocument(mainDoc, mainId.value(), true, {}).has_value());
        REQUIRE(
            repo->upsertPathTreeForDocument(relatedDoc, relatedId.value(), true, {}).has_value());
        graphQuery->addConnectedDocument(relatedDoc.sha256Hash, "same_content", 1, 0.8);
        store->setBlob(mainDoc.sha256Hash, "main content");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);
        svc.__test_setGraphQueryService(graphQuery);

        GetRequest req;
        req.hash = mainDoc.sha256Hash;
        req.showGraph = true;
        req.graphDepth = 1;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GetResponse>(resp));
        const auto& getResp = std::get<GetResponse>(resp);
        CHECK(getResp.graphEnabled);
        CHECK(getResp.hash == mainDoc.sha256Hash);
        REQUIRE(getResp.related.size() == 1);
        CHECK(getResp.related.front().hash == relatedDoc.sha256Hash);
        CHECK(getResp.related.front().relationship == "same_content");
    }

    SECTION("get request still succeeds without query trace env") {
        auto repoPath = makeTempDir("yams_get_query_trace_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(71, "/tmp/get/query-trace.txt",
                           "9999999999999999999999999999999999999999999999999999999999999999");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "query trace content");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        GetRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GetResponse>(resp));
        const auto& getResp = std::get<GetResponse>(resp);
        CHECK(getResp.hash == doc.sha256Hash);
        CHECK(getResp.name == doc.fileName);
    }

    SECTION("get request maps fallback documents vector response") {
        auto repoPath = makeTempDir("yams_get_documents_vector_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(72, "/tmp/get/documents-vector.txt",
                           "abababababababababababababababababababababababababababababababab");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "documents vector content");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        RequestDispatcher::__test_forceGetResponseDocumentsVectorOnce();

        GetRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GetResponse>(resp));
        const auto& getResp = std::get<GetResponse>(resp);
        CHECK(getResp.hash == doc.sha256Hash);
        CHECK(getResp.name == doc.fileName);
        CHECK(getResp.path == doc.filePath);
    }

    SECTION("get request returns not found when service result is forced empty") {
        auto repoPath = makeTempDir("yams_get_empty_result_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(73, "/tmp/get/empty-result.txt",
                           "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "empty result content");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        RequestDispatcher::__test_forceGetEmptyResultOnce();

        GetRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "No documents found matching criteria");
    }

    SECTION("file history reports missing metadata repository") {
        FileHistoryRequest req;
        req.filepath = "missing.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("Metadata repository not available") != std::string::npos);
    }

    SECTION("file history falls back to filename query and reports no snapshot versions") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto doc = makeDoc(9, "/tmp/archive/report.txt", "history-hash");
        repo->addDocument(doc);
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = "report.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        CHECK_FALSE(history.found);
        CHECK(history.totalVersions == 0);
        CHECK(history.versions.empty());
        CHECK(history.message == "File found in index but not in any snapshot");
        CHECK(history.filepath.find("report.txt") != std::string::npos);
    }

    SECTION("file history reports file not found in index") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = "ghost.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        CHECK_FALSE(history.found);
        CHECK(history.totalVersions == 0);
        CHECK(history.message == "File not found in index");
    }

    SECTION("file history returns sorted snapshot versions from exact path") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto doc = makeDoc(11, "/tmp/archive/sorted-report.txt", "sorted-history-hash");
        repo->setExactPathDocument(doc);
        repo->setAllMetadata(
            doc.id, {{"snapshot_id", metadata::MetadataValue("snap-alpha")},
                     {"snapshot_id:snap-beta", metadata::MetadataValue("")},
                     {"snapshot_time:snap-alpha", metadata::MetadataValue("1710000000000000")}});
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        REQUIRE(history.found);
        REQUIRE(history.totalVersions == 2);
        REQUIRE(history.versions.size() == 2);
        CHECK(history.message == "Found 2 version(s) across snapshots");
        CHECK(history.versions[0].snapshotId == "snap-beta");
        CHECK(history.versions[1].snapshotId == "snap-alpha");
        CHECK(history.versions[0].indexedTimestamp > history.versions[1].indexedTimestamp);
    }

    SECTION(
        "file history falls back to document indexed time when snapshot-specific time is missing") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto doc = makeDoc(42, "/tmp/archive/fallback-time.txt", "fallback-time-hash");
        repo->setExactPathDocument(doc);
        repo->setAllMetadata(doc.id, {{"snapshot_id", metadata::MetadataValue("snap-fallback")}});
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        REQUIRE(history.found);
        REQUIRE(history.totalVersions == 1);
        REQUIRE(history.versions.size() == 1);
        CHECK(history.versions.front().snapshotId == "snap-fallback");
        CHECK(history.versions.front().indexedTimestamp == 1710000042);
    }

    SECTION("file history ignores malformed snapshot timestamps and falls back to indexed time") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto doc = makeDoc(64, "/tmp/archive/bad-snapshot-time.txt", "bad-snapshot-time-hash");
        repo->setExactPathDocument(doc);
        repo->setAllMetadata(doc.id, {{"snapshot_id", metadata::MetadataValue("snap-bad-time")},
                                      {"snapshot_time", metadata::MetadataValue("not-a-number")},
                                      {"snapshot_time:snap-bad-time",
                                       metadata::MetadataValue("still-not-a-number")}});
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        REQUIRE(history.found);
        REQUIRE(history.totalVersions == 1);
        REQUIRE(history.versions.size() == 1);
        CHECK(history.versions.front().snapshotId == "snap-bad-time");
        CHECK(history.versions.front().indexedTimestamp == 1710000064);
    }

    SECTION("list request paths-only mode trims snippets metadata and tags") {
        auto repoPath = makeTempDir("yams_list_dispatcher_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto doc = makeDoc(55, "/tmp/list/paths-only.md", "list-paths-hash");
        REQUIRE(repo->insertDocument(doc).has_value());
        svc.__test_setMetadataRepo(repo);

        ListRequest req;
        req.limit = 5;
        req.pathsOnly = true;
        req.showMetadata = true;
        req.showTags = true;
        req.showSnippets = true;
        req.recent = false;
        req.recentCount = 0;
        req.namePattern = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ListResponse>(resp));
        const auto& listResp = std::get<ListResponse>(resp);
        REQUIRE(listResp.items.size() == 1);
        CHECK(listResp.totalCount == 1);
        CHECK(listResp.items.front().path == doc.filePath);
        CHECK(listResp.items.front().snippet.empty());
        CHECK(listResp.items.front().metadata.empty());
        CHECK(listResp.items.front().tags.empty());
    }

    SECTION("list request emits dispatch timing stats by default") {
        auto repoPath = makeTempDir("yams_list_query_trace_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto doc = makeDoc(56, "/tmp/list/query-trace.md", "list-query-trace-hash");
        REQUIRE(repo->insertDocument(doc).has_value());
        svc.__test_setMetadataRepo(repo);

        ListRequest req;
        req.limit = 3;
        req.recent = false;
        req.recentCount = 0;
        req.namePattern = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ListResponse>(resp));
        const auto& listResp = std::get<ListResponse>(resp);
        REQUIRE(listResp.items.size() == 1);
        CHECK(listResp.listStats.contains("phase_dispatch_total_ms"));
        CHECK(listResp.listStats.contains("phase_dispatch_map_us"));
    }

    SECTION("list request parses filter tags and recent count") {
        auto repoPath = makeTempDir("yams_list_filter_tags_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto doc = makeDoc(67, "/tmp/list/filter-tags.md", "list-filter-tags-hash");
        REQUIRE(repo->insertDocument(doc).has_value());
        svc.__test_setMetadataRepo(repo);

        ListRequest req;
        req.limit = 5;
        req.offset = 0;
        req.recent = false;
        req.recentCount = 3;
        req.filterTags = " alpha, beta ";
        req.namePattern = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ListResponse>(resp));
        const auto& listResp = std::get<ListResponse>(resp);
        CHECK(listResp.totalCount >= 0);
    }

    SECTION("list request works when inflight limit is disabled") {
        TuneAdvisorListGuard tuneGuard(0, 1);

        auto repoPath = makeTempDir("yams_list_no_inflight_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto doc = makeDoc(68, "/tmp/list/no-inflight.md", "list-no-inflight-hash");
        REQUIRE(repo->insertDocument(doc).has_value());
        svc.__test_setMetadataRepo(repo);

        ListRequest req;
        req.limit = 5;
        req.offset = 0;
        req.recent = true;
        req.showSnippets = true;
        req.noSnippets = true;
        req.namePattern = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ListResponse>(resp));
        const auto& listResp = std::get<ListResponse>(resp);
        REQUIRE(listResp.items.size() == 1);
        CHECK(listResp.items.front().snippet.empty());
    }

    SECTION("list request reports missing metadata repository") {
        ListRequest req;
        req.limit = 2;
        req.recent = false;
        req.recentCount = 0;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message.find("Metadata repository not available") != std::string::npos);
    }

    SECTION("list request reports resource exhaustion when inflight limit is saturated") {
        TuneAdvisorListGuard tuneGuard(1, 1);

        auto repoPath = makeTempDir("yams_list_exhaustion_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);
        auto doc = makeDoc(60, "/tmp/list/exhausted.md", "list-exhausted-hash");
        REQUIRE(repo->insertDocument(doc).has_value());
        svc.__test_setMetadataRepo(repo);

        RequestDispatcher::__test_setListInflightRequests(1);
        auto resetGuard = ScopeExit([] { RequestDispatcher::__test_setListInflightRequests(0); });

        ListRequest req;
        req.limit = 1;
        req.recent = false;
        req.recentCount = 0;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::ResourceExhausted);
        CHECK(err.message.find("List concurrency limit reached") != std::string::npos);
    }

    SECTION("list request catches std exceptions") {
        RequestDispatcher::__test_forceListExceptionOnce("forced list exception");

        ListRequest req;
        req.limit = 2;
        req.recent = false;
        req.recentCount = 0;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "List failed: forced list exception");
    }

    SECTION("list request catches unknown exceptions") {
        RequestDispatcher::__test_forceListUnknownExceptionOnce();

        ListRequest req;
        req.limit = 2;
        req.recent = false;
        req.recentCount = 0;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "List failed: unknown error");
    }

    SECTION("add document queues deferred work when system is under pressure") {
        const bool originalGovernor = TuneAdvisor::enableResourceGovernor();
        const bool originalAdmission = TuneAdvisor::enableAdmissionControl();
        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setEnableResourceGovernor(originalGovernor);
            TuneAdvisor::setEnableAdmissionControl(originalAdmission);
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Normal);
            drainStoreDocumentTasks();
        });

        TuneAdvisor::setEnableResourceGovernor(true);
        TuneAdvisor::setEnableAdmissionControl(true);
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Emergency);
        drainStoreDocumentTasks();

        EnvGuard retriesGuard("YAMS_INGEST_ENQUEUE_RETRIES", "not-a-number");
        EnvGuard baseDelayGuard("YAMS_INGEST_ENQUEUE_BASE_DELAY_MS", "-5");
        EnvGuard maxDelayGuard("YAMS_INGEST_ENQUEUE_MAX_DELAY_MS", "999999");

        AddDocumentRequest req;
        req.name = "pressure.txt";
        req.content = "queued under pressure";

        auto resp = dispatchRequest(dispatcher, Request{req});

        INFO([&] {
            if (std::holds_alternative<ErrorResponse>(resp)) {
                return std::get<ErrorResponse>(resp).message;
            }
            return std::string("not an error");
        }());
        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        auto hasher = yams::crypto::createSHA256Hasher();
        REQUIRE(hasher != nullptr);
        CHECK(addResp.path == "pressure.txt");
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash == hasher->hash(req.content));
        CHECK(addResp.message == "Queued for deferred processing (system under pressure).");

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask task;
        REQUIRE(channel->try_pop(task));
        CHECK(task.request.name == req.name);
        CHECK(task.request.content == req.content);
    }

    SECTION("add document clears deferred hash when pressure-path hashing fails") {
        const bool originalGovernor = TuneAdvisor::enableResourceGovernor();
        const bool originalAdmission = TuneAdvisor::enableAdmissionControl();
        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setEnableResourceGovernor(originalGovernor);
            TuneAdvisor::setEnableAdmissionControl(originalAdmission);
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Normal);
            drainStoreDocumentTasks();
        });

        TuneAdvisor::setEnableResourceGovernor(true);
        TuneAdvisor::setEnableAdmissionControl(true);
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Emergency);
        drainStoreDocumentTasks();

        auto filePath = writeTempFile("yams_documents_pressure_file_", "pressure-file.txt",
                                      "pressure file content");
        RequestDispatcher::__test_forceDocumentsHashFailureOnce();

        AddDocumentRequest req;
        req.path = filePath.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash.empty());
        CHECK(addResp.message == "Queued for deferred processing (system under pressure).");
    }

    SECTION("add document clears deferred hash for recursive pressure path") {
        const bool originalGovernor = TuneAdvisor::enableResourceGovernor();
        const bool originalAdmission = TuneAdvisor::enableAdmissionControl();
        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setEnableResourceGovernor(originalGovernor);
            TuneAdvisor::setEnableAdmissionControl(originalAdmission);
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Normal);
            drainStoreDocumentTasks();
        });

        TuneAdvisor::setEnableResourceGovernor(true);
        TuneAdvisor::setEnableAdmissionControl(true);
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Emergency);
        drainStoreDocumentTasks();

        AddDocumentRequest req;
        req.path = makeTempDir("yams_documents_pressure_recursive_").string();
        req.recursive = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash.empty());
        CHECK(addResp.message == "Queued for deferred processing (system under pressure).");
    }

    SECTION("add document reports resource exhaustion when pressure queue is full") {
        const bool originalGovernor = TuneAdvisor::enableResourceGovernor();
        const bool originalAdmission = TuneAdvisor::enableAdmissionControl();
        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setEnableResourceGovernor(originalGovernor);
            TuneAdvisor::setEnableAdmissionControl(originalAdmission);
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Normal);
            drainStoreDocumentTasks();
        });

        TuneAdvisor::setEnableResourceGovernor(true);
        TuneAdvisor::setEnableAdmissionControl(true);
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Emergency);
        drainStoreDocumentTasks();

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask filler;
        while (channel->try_push(filler)) {
        }

        AddDocumentRequest req;
        req.name = "queue-full.txt";
        req.content = "full";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::ResourceExhausted);
        CHECK(err.message == "Ingestion queue is full. Please try again later.");
    }

    SECTION("add document queues async work while daemon is initializing") {
        DaemonConfig notReadyCfg;
        notReadyCfg.dataDir = makeTempDir("yams_documents_dispatcher_not_ready_");
        StubLifecycle notReadyLifecycle(LifecycleState::Starting);
        StateComponent notReadyState;
        notReadyState.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        notReadyState.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm notReadyLifecycleFsm;
        ServiceManager notReadySvc(notReadyCfg, notReadyState, notReadyLifecycleFsm);
        RequestDispatcher notReadyDispatcher(&notReadyLifecycle, &notReadySvc, &notReadyState);

        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        AddDocumentRequest req;
        req.name = "initializing.txt";
        req.content = "booting daemon";

        auto resp = dispatchRequest(notReadyDispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        auto hasher = yams::crypto::createSHA256Hasher();
        REQUIRE(hasher != nullptr);
        CHECK(addResp.path == "initializing.txt");
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash == hasher->hash(req.content));
        CHECK(addResp.message ==
              "Ingestion accepted for asynchronous processing (daemon initializing).");

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask task;
        REQUIRE(channel->try_pop(task));
        CHECK(task.request.name == req.name);
        CHECK(task.request.content == req.content);
    }

    SECTION("add document reports queue full while daemon is initializing") {
        DaemonConfig notReadyCfg;
        notReadyCfg.dataDir = makeTempDir("yams_documents_dispatcher_not_ready_full_");
        StubLifecycle notReadyLifecycle(LifecycleState::Starting);
        StateComponent notReadyState;
        notReadyState.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        notReadyState.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm notReadyLifecycleFsm;
        ServiceManager notReadySvc(notReadyCfg, notReadyState, notReadyLifecycleFsm);
        RequestDispatcher notReadyDispatcher(&notReadyLifecycle, &notReadySvc, &notReadyState);

        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            drainStoreDocumentTasks();
        });

        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        drainStoreDocumentTasks();

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask filler;
        while (channel->try_push(filler)) {
        }

        AddDocumentRequest req;
        req.name = "initializing-full.txt";
        req.content = "booting daemon full";

        auto resp = dispatchRequest(notReadyDispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::ResourceExhausted);
        CHECK(err.message == "Ingestion queue is full. Please try again later.");
    }

    SECTION("add document clears async hash when daemon-initializing hashing fails") {
        DaemonConfig notReadyCfg;
        notReadyCfg.dataDir = makeTempDir("yams_documents_dispatcher_not_ready_hash_fail_");
        StubLifecycle notReadyLifecycle(LifecycleState::Starting);
        StateComponent notReadyState;
        notReadyState.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        notReadyState.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm notReadyLifecycleFsm;
        ServiceManager notReadySvc(notReadyCfg, notReadyState, notReadyLifecycleFsm);
        RequestDispatcher notReadyDispatcher(&notReadyLifecycle, &notReadySvc, &notReadyState);

        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        auto filePath =
            writeTempFile("yams_documents_init_hash_fail_", "init-file.txt", "init file content");
        RequestDispatcher::__test_forceDocumentsHashFailureOnce();

        AddDocumentRequest req;
        req.path = filePath.string();

        auto resp = dispatchRequest(notReadyDispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash.empty());
        CHECK(addResp.message ==
              "Ingestion accepted for asynchronous processing (daemon initializing).");
    }

    SECTION("add document hashes file path while daemon is initializing") {
        DaemonConfig notReadyCfg;
        notReadyCfg.dataDir = makeTempDir("yams_documents_dispatcher_not_ready_file_hash_");
        StubLifecycle notReadyLifecycle(LifecycleState::Starting);
        StateComponent notReadyState;
        notReadyState.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        notReadyState.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm notReadyLifecycleFsm;
        ServiceManager notReadySvc(notReadyCfg, notReadyState, notReadyLifecycleFsm);
        RequestDispatcher notReadyDispatcher(&notReadyLifecycle, &notReadySvc, &notReadyState);

        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        auto filePath = writeTempFile("yams_documents_init_file_hash_", "init-file-hash.txt",
                                      "init file hash content");
        AddDocumentRequest req;
        req.path = filePath.string();

        auto resp = dispatchRequest(notReadyDispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        auto hasher = yams::crypto::createSHA256Hasher();
        REQUIRE(hasher != nullptr);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash == hasher->hashFile(req.path));
        CHECK(addResp.message ==
              "Ingestion accepted for asynchronous processing (daemon initializing).");
    }

    SECTION("add document treats directory path as async directory ingestion") {
        auto dirPath = makeTempDir("yams_documents_directory_ingest_");
        drainStoreDocumentTasks();

        AddDocumentRequest req;
        req.path = dirPath.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.path == dirPath.string());
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash.empty());
        CHECK(addResp.message == "Directory ingestion accepted for asynchronous processing.");

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask task;
        REQUIRE(channel->try_pop(task));
        CHECK(task.request.path == req.path);
    }

    SECTION("add document rejects missing path and content") {
        AddDocumentRequest req;
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message == "Provide either 'path' or 'content' + 'name'");
    }

    SECTION("add document uses async single-file path by default when ready") {
        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        AddDocumentRequest req;
        req.name = "async-default.txt";
        req.content = "ready but async";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        auto hasher = yams::crypto::createSHA256Hasher();
        REQUIRE(hasher != nullptr);
        CHECK(addResp.path == "async-default.txt");
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash == hasher->hash(req.content));
        CHECK(addResp.extractionStatus == "pending");
        CHECK(addResp.message == "Ingestion accepted for asynchronous processing.");

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask task;
        REQUIRE(channel->try_pop(task));
        CHECK(task.request.name == req.name);
        CHECK(task.request.content == req.content);
    }

    SECTION("add document hashes file paths on async path when ready") {
        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        auto filePath =
            writeTempFile("yams_documents_async_file_", "async-file.txt", "async file content");
        AddDocumentRequest req;
        req.path = filePath.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        auto hasher = yams::crypto::createSHA256Hasher();
        REQUIRE(hasher != nullptr);
        CHECK(addResp.path == req.path);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash == hasher->hashFile(req.path));
        CHECK(addResp.message == "Ingestion accepted for asynchronous processing.");
    }

    SECTION("add document hashes file path on pressure path after one retry") {
        const bool originalGovernor = TuneAdvisor::enableResourceGovernor();
        const bool originalAdmission = TuneAdvisor::enableAdmissionControl();
        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setEnableResourceGovernor(originalGovernor);
            TuneAdvisor::setEnableAdmissionControl(originalAdmission);
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Normal);
            RequestDispatcher::__test_setDocumentsEnqueueFailuresBeforeSuccess(0);
            drainStoreDocumentTasks();
        });

        TuneAdvisor::setEnableResourceGovernor(true);
        TuneAdvisor::setEnableAdmissionControl(true);
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        ResourceGovernor::instance().testing_updateScalingCaps(ResourcePressureLevel::Emergency);
        drainStoreDocumentTasks();

        EnvGuard retriesGuard("YAMS_INGEST_ENQUEUE_RETRIES", "3");
        EnvGuard baseDelayGuard("YAMS_INGEST_ENQUEUE_BASE_DELAY_MS", "1");
        EnvGuard maxDelayGuard("YAMS_INGEST_ENQUEUE_MAX_DELAY_MS", "1");
        RequestDispatcher::__test_setDocumentsEnqueueFailuresBeforeSuccess(1);

        auto filePath = writeTempFile("yams_documents_pressure_retry_", "pressure-retry.txt",
                                      "pressure retry content");
        AddDocumentRequest req;
        req.path = filePath.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        auto hasher = yams::crypto::createSHA256Hasher();
        REQUIRE(hasher != nullptr);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash == hasher->hashFile(req.path));
        CHECK(addResp.message == "Queued for deferred processing (system under pressure).");
    }

    SECTION("add document falls back to initializing path when later lifecycle snapshot throws") {
        DaemonConfig notReadyCfg;
        notReadyCfg.dataDir = makeTempDir("yams_documents_dispatcher_lifecycle_throw_late_");
        StubLifecycle notReadyLifecycle(LifecycleState::Ready);
        notReadyLifecycle.setThrowOnGetLifecycleSnapshotCall(2);
        StateComponent notReadyState;
        notReadyState.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        notReadyState.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm notReadyLifecycleFsm;
        ServiceManager notReadySvc(notReadyCfg, notReadyState, notReadyLifecycleFsm);
        RequestDispatcher notReadyDispatcher(&notReadyLifecycle, &notReadySvc, &notReadyState);

        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        AddDocumentRequest req;
        req.name = "late-throw.txt";
        req.content = "late throw";

        auto resp = dispatchRequest(notReadyDispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.message ==
              "Ingestion accepted for asynchronous processing (daemon initializing).");
    }

    SECTION("add document reports queue full on async single-file path") {
        const uint32_t originalCapacity = TuneAdvisor::storeDocumentChannelCapacity();
        auto restore = ScopeExit([&] {
            TuneAdvisor::setStoreDocumentChannelCapacity(originalCapacity);
            drainStoreDocumentTasks();
        });

        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        TuneAdvisor::setStoreDocumentChannelCapacity(64);
        drainStoreDocumentTasks();

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask filler;
        while (channel->try_push(filler)) {
        }

        AddDocumentRequest req;
        req.name = "async-full.txt";
        req.content = "full async queue";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::ResourceExhausted);
        CHECK(err.message == "Ingestion queue is full. Please try again later.");
    }

    SECTION("add document clears async hash when hashing fails on ready path") {
        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "0");
        drainStoreDocumentTasks();

        auto filePath = writeTempFile("yams_documents_async_hash_fail_", "async-hash-fail.txt",
                                      "async hash fail content");
        RequestDispatcher::__test_forceDocumentsHashFailureOnce();

        AddDocumentRequest req;
        req.path = filePath.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.documentsAdded == 0);
        CHECK(addResp.hash.empty());
        CHECK(addResp.extractionStatus == "pending");
        CHECK(addResp.message == "Ingestion accepted for asynchronous processing.");
    }

    SECTION("add document uses sync fallback when explicitly enabled") {
        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "1");
        drainStoreDocumentTasks();
        svc.__test_setContentStore(std::make_shared<StubContentStore>());

        AddDocumentRequest req;
        req.name = "sync-add.txt";
        req.content = "sync path content";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.path == "sync-add.txt");
        CHECK(addResp.documentsAdded == 1);
        CHECK(addResp.size == req.content.size());
        CHECK(addResp.extractionStatus == "pending");
        CHECK(addResp.message == "Document stored successfully.");
        CHECK_FALSE(addResp.hash.empty());

        auto channel = getStoreDocumentTaskChannel();
        InternalEventBus::StoreDocumentTask task;
        CHECK_FALSE(channel->try_pop(task));
    }

    SECTION("add document sync fallback forwards metadata and path inputs") {
        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "true");
        drainStoreDocumentTasks();
        svc.__test_setContentStore(std::make_shared<StubContentStore>());

        auto filePath =
            writeTempFile("yams_documents_sync_file_", "sync-file.txt", "sync file content");
        AddDocumentRequest req;
        req.path = filePath.string();
        req.mimeType = "text/custom";
        req.disableAutoMime = true;
        req.tags = {"alpha", "beta"};
        req.metadata["task"] = "documents";
        req.collection = "focus";
        req.snapshotId = "snap-1";
        req.snapshotLabel = "label-1";
        req.sessionId = "session-1";
        req.noEmbeddings = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<AddDocumentResponse>(resp));
        const auto& addResp = std::get<AddDocumentResponse>(resp);
        CHECK(addResp.path == req.path);
        CHECK(addResp.documentsAdded == 1);
        CHECK(addResp.size == std::filesystem::file_size(filePath));
        CHECK_FALSE(addResp.hash.empty());
    }

    SECTION("add document sync fallback returns store errors") {
        EnvGuard syncGuard("YAMS_SYNC_SINGLE_FILE_ADD", "1");
        drainStoreDocumentTasks();
        svc.__test_setContentStore(std::shared_ptr<api::IContentStore>{});

        AddDocumentRequest req;
        req.name = "sync-error.txt";
        req.content = "sync failure";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message == "Content store not available");
    }

    SECTION("file history skips documents when metadata lookup fails") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto failingDoc = makeDoc(57, "/tmp/archive/history-mixed.txt", "metadata-fail-hash");
        auto okDoc = makeDoc(58, "/var/tmp/archive/history-mixed.txt", "metadata-ok-hash");
        repo->addDocument(failingDoc);
        repo->addDocument(okDoc);
        repo->setAllMetadataError(failingDoc.id,
                                  Error{ErrorCode::DatabaseError, "metadata lookup failed"});
        repo->setAllMetadata(okDoc.id,
                             {{"snapshot_id", metadata::MetadataValue("snap-ok")},
                              {"snapshot_time", metadata::MetadataValue("1710000000000000")}});
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = "history-mixed.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        REQUIRE(history.found);
        CHECK(history.totalVersions == 1);
        REQUIRE(history.versions.size() == 1);
        CHECK(history.versions.front().snapshotId == "snap-ok");
    }

    SECTION("file history falls back when exact path lookup errors") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto doc =
            makeDoc(63, "/tmp/archive/fallback-exact-error.txt", "fallback-exact-error-hash");
        repo->addDocument(doc);
        repo->setExactPathError(Error{ErrorCode::DatabaseError, "exact lookup failed"});
        repo->setAllMetadata(doc.id,
                             {{"snapshot_id", metadata::MetadataValue("snap-fallback-error")}});
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = doc.filePath;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        REQUIRE(history.found);
        CHECK(history.totalVersions == 1);
        REQUIRE(history.versions.size() == 1);
        CHECK(history.versions.front().snapshotId == "snap-fallback-error");
    }

    SECTION("file history caps oversized filename matches to first hundred documents") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        for (int i = 0; i < 101; ++i) {
            auto doc = makeDoc(1000 + i,
                               "/tmp/archive/subdir_" + std::to_string(i) + "/history-overflow.txt",
                               "history-overflow-hash-" + std::to_string(i));
            doc.fileName = "history-overflow.txt";
            repo->addDocument(doc);
            repo->setAllMetadata(
                doc.id, {{"snapshot_id", metadata::MetadataValue("snap-" + std::to_string(i))}});
        }
        svc.__test_setMetadataRepo(repo);

        FileHistoryRequest req;
        req.filepath = "history-overflow.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<FileHistoryResponse>(resp));
        const auto& history = std::get<FileHistoryResponse>(resp);
        REQUIRE(history.found);
        CHECK(history.totalVersions == 100);
        CHECK(history.versions.size() == 100);
    }

    SECTION("file history reports invalid filepath when current directory is missing") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        svc.__test_setMetadataRepo(repo);

        auto originalCwd = std::filesystem::current_path();
        auto doomedDir = makeTempDir("yams_missing_cwd_");
        std::filesystem::current_path(doomedDir);
        std::filesystem::remove_all(doomedDir);

        FileHistoryRequest req;
        req.filepath = "history-from-missing-cwd.txt";

        auto resp = dispatchRequest(dispatcher, Request{req});

        std::filesystem::current_path(originalCwd);

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message.find("Invalid filepath") != std::string::npos);
    }

    SECTION("cat request reports missing content") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(58, "/tmp/cat/missing.txt", "cat-missing-hash");
        repo->addDocument(doc);
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        CatRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message.find("Document not found") != std::string::npos);
    }

    SECTION("cat request reports missing content payload for indexed document") {
        auto repoPath = makeTempDir("yams_cat_missing_content_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(59, "/tmp/cat/metadata-only.txt",
                           "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        REQUIRE(repo->insertDocument(doc).has_value());
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        CatRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message.find("Document content not found") != std::string::npos);
    }

    SECTION("cat request hits missing document branch after successful retrieval") {
        auto repoPath = makeTempDir("yams_cat_force_missing_doc_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(61, "/tmp/cat/force-missing-doc.txt",
                           "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "payload");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        RequestDispatcher::__test_forceCatMissingDocumentOnce();

        CatRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "Document not found");
    }

    SECTION("cat request hits content unavailable branch after successful retrieval") {
        auto repoPath = makeTempDir("yams_cat_force_missing_content_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(62, "/tmp/cat/force-missing-content.txt",
                           "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "payload");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        RequestDispatcher::__test_forceCatMissingContentOnce();

        CatRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Content unavailable");
    }

    SECTION("cat request hits native missing document branch") {
        auto repoPath = makeTempDir("yams_cat_native_missing_doc_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(65, "/tmp/cat/native-missing-doc.txt",
                           "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "payload");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        RequestDispatcher::__test_forceCatNativeMissingDocumentOnce();

        CatRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "Document not found");
    }

    SECTION("cat request hits native missing content branch") {
        auto repoPath = makeTempDir("yams_cat_native_missing_content_repo_") / "metadata.db";
        metadata::ConnectionPoolConfig poolCfg{};
        auto pool = std::make_shared<metadata::ConnectionPool>(repoPath.string(), poolCfg);
        REQUIRE(pool->initialize().has_value());
        auto repo = std::make_shared<metadata::MetadataRepository>(*pool);

        auto store = std::make_shared<StubContentStore>();
        auto doc = makeDoc(66, "/tmp/cat/native-missing-content.txt",
                           "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        REQUIRE(repo->insertDocument(doc).has_value());
        store->setBlob(doc.sha256Hash, "payload");
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(store);

        RequestDispatcher::__test_forceCatNativeMissingContentOnce();

        CatRequest req;
        req.hash = doc.sha256Hash;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Content unavailable");
    }
}

TEST_CASE("RequestDispatcher: download handler enforces daemon policy",
          "[daemon][documents][dispatcher][download]") {
    EnvGuard daemonDownloadEnv("YAMS_ENABLE_DAEMON_DOWNLOAD", "0");

    auto dispatchDownload = [](RequestDispatcher& dispatcher, const std::string& url,
                               std::string outputPath = {}, std::string checksum = {}) {
        DownloadRequest req;
        req.url = url;
        req.outputPath = std::move(outputPath);
        req.checksum = std::move(checksum);
        return dispatchRequest(dispatcher, Request{req});
    };
    auto waitForDownloadJob = [](RequestDispatcher& dispatcher, const std::string& jobId,
                                 std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        DownloadResponse latest;
        while (std::chrono::steady_clock::now() < deadline) {
            auto resp = dispatchRequest(dispatcher, Request{DownloadStatusRequest{jobId}});
            if (std::holds_alternative<DownloadResponse>(resp)) {
                latest = std::get<DownloadResponse>(resp);
                if (latest.state != "queued" && latest.state != "running") {
                    return latest;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return latest;
    };

    SECTION("disabled policy returns safe reminder") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_disabled_");

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("Daemon download is disabled") != std::string::npos);
    }

    SECTION("disallowed scheme is rejected before download starts") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_scheme_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "http://example.com/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("scheme 'http'") != std::string::npos);
    }

    SECTION("invalid URL shape is rejected before policy host checks") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_invalid_url_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "not-a-url");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error == "Invalid download URL");
    }

    SECTION("download URL rejects missing authority after scheme") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_missing_authority_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error == "Invalid download URL");
    }

    SECTION("download URL rejects empty host after credentials stripping") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_empty_host_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://user@/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error == "Invalid download URL");
    }

    SECTION("wildcard host policy accepts matching subdomains before checksum gate") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_wildcard_host_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = true;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"*.example.com"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://api.example.com/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("checksum is required") != std::string::npos);
    }

    SECTION("IPv6 authority parses and matches exact allowed host") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_ipv6_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = true;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"::1"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://[::1]/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("checksum is required") != std::string::npos);
    }

    SECTION("invalid IPv6 authority is rejected") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_ipv6_invalid_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://[]/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error == "Invalid download URL");
    }

    SECTION("disallowed host is rejected before download starts") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_host_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"downloads.example.com"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("host 'example.com'") != std::string::npos);
    }

    SECTION("store-only policy rejects output path") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_store_only_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.storeOnly = true;

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin", "/tmp/export.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("store_only=true") != std::string::npos);
    }

    SECTION("invalid checksum format is rejected before download starts") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_bad_checksum_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp =
            dispatchDownload(dispatcher, "https://example.com/file.bin", {}, "sha1:not-allowed");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("invalid checksum format") != std::string::npos);
    }

    SECTION("unsupported checksum algorithm with valid hex is rejected") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_bad_checksum_algo_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp =
            dispatchDownload(dispatcher, "https://example.com/file.bin", {}, "sha1:abcdef12");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("invalid checksum format") != std::string::npos);
    }

    SECTION("malformed checksum missing separator is rejected") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_bad_checksum_shape_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin", {}, "sha256");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("invalid checksum format") != std::string::npos);
    }

    SECTION("checksum-required policy rejects missing checksum") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_checksum_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = true;
        cfg.downloadPolicy.allowedSchemes = {"https"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.error.find("checksum is required") != std::string::npos);
    }

    SECTION("valid checksum enters job tracking and handles unavailable service") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_valid_checksum_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"*"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        RequestDispatcher::__test_forceDownloadServiceUnavailableOnce();

        auto resp =
            dispatchDownload(dispatcher, "https://example.com/file.bin", {}, "sha256:abcd1234");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK_FALSE(dl.success);
        CHECK(dl.state == "queued");
        CHECK_FALSE(dl.jobId.empty());

        const auto finalStatus = waitForDownloadJob(dispatcher, dl.jobId);
        CHECK(finalStatus.jobId == dl.jobId);
        CHECK(finalStatus.state == "failed");
        CHECK(finalStatus.error == "Download service not available in daemon");
    }

    SECTION("sha512 checksum enters job tracking and handles unavailable service") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_sha512_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"*"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        RequestDispatcher::__test_forceDownloadServiceUnavailableOnce();

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin", {},
                                     "sha512:abcdef1234567890");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK(dl.state == "queued");
        CHECK_FALSE(dl.jobId.empty());

        const auto finalStatus = waitForDownloadJob(dispatcher, dl.jobId);
        CHECK(finalStatus.jobId == dl.jobId);
        CHECK(finalStatus.state == "failed");
        CHECK(finalStatus.error == "Download service not available in daemon");
    }

    SECTION("md5 checksum enters job tracking and handles unavailable service") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_md5_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"*"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        RequestDispatcher::__test_forceDownloadServiceUnavailableOnce();

        auto resp = dispatchDownload(dispatcher, "https://example.com/file.bin", {},
                                     "md5:abcdef1234567890");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK(dl.state == "queued");
        CHECK_FALSE(dl.jobId.empty());

        const auto finalStatus = waitForDownloadJob(dispatcher, dl.jobId);
        CHECK(finalStatus.jobId == dl.jobId);
        CHECK(finalStatus.state == "failed");
        CHECK(finalStatus.error == "Download service not available in daemon");
    }

    SECTION("forced successful download completes the tracked job") {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_download_policy_success_");
        cfg.downloadPolicy.enable = true;
        cfg.downloadPolicy.requireChecksum = false;
        cfg.downloadPolicy.allowedSchemes = {"https"};
        cfg.downloadPolicy.allowedHosts = {"*"};

        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        RequestDispatcher::__test_forceDownloadServiceSuccessOnce();

        auto resp =
            dispatchDownload(dispatcher, "https://example.com/file.bin", {}, "sha256:abcd1234");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK(dl.state == "queued");
        CHECK_FALSE(dl.jobId.empty());

        const auto finalStatus = waitForDownloadJob(dispatcher, dl.jobId);
        CHECK(finalStatus.jobId == dl.jobId);
        CHECK(finalStatus.state == "completed");
        CHECK(finalStatus.success);
        CHECK(finalStatus.error.empty());
        CHECK(finalStatus.hash == "sha256:forced-download-success");
        CHECK(finalStatus.localPath.find("forced-download-success.bin") != std::string::npos);
        CHECK(finalStatus.size == 64);
    }
}

TEST_CASE("RequestDispatcher: download job status and list use in-memory registry",
          "[daemon][documents][dispatcher][download-jobs]") {
    RequestDispatcher::__test_resetDownloadJobs();

    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_download_job_registry_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    DownloadResponse first;
    first.url = "https://example.com/a.bin";
    first.jobId = "download-1";
    first.state = "completed";
    first.createdAtMs = 100;
    first.updatedAtMs = 200;
    first.success = true;

    DownloadResponse second;
    second.url = "https://example.com/b.bin";
    second.jobId = "download-2";
    second.state = "failed";
    second.createdAtMs = 300;
    second.updatedAtMs = 400;
    second.success = false;
    second.error = "network boom";

    RequestDispatcher::__test_seedDownloadJob(first);
    RequestDispatcher::__test_seedDownloadJob(second);

    SECTION("download status returns the seeded job") {
        auto resp = dispatchRequest(dispatcher, Request{DownloadStatusRequest{"download-2"}});

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK(dl.jobId == "download-2");
        CHECK(dl.state == "failed");
        CHECK(dl.error == "network boom");
    }

    SECTION("download status returns not found for unknown job") {
        auto resp = dispatchRequest(dispatcher, Request{DownloadStatusRequest{"missing-job"}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "Download job not found");
    }

    SECTION("list download jobs returns most recent first and honors limit") {
        auto resp = dispatchRequest(dispatcher, Request{ListDownloadJobsRequest{1}});

        REQUIRE(std::holds_alternative<ListDownloadJobsResponse>(resp));
        const auto& jobs = std::get<ListDownloadJobsResponse>(resp);
        REQUIRE(jobs.jobs.size() == 1);
        CHECK(jobs.jobs.front().jobId == "download-2");
        CHECK(jobs.jobs.front().state == "failed");
    }

    SECTION("list download jobs clamps zero limit to one") {
        auto resp = dispatchRequest(dispatcher, Request{ListDownloadJobsRequest{0}});

        REQUIRE(std::holds_alternative<ListDownloadJobsResponse>(resp));
        const auto& jobs = std::get<ListDownloadJobsResponse>(resp);
        REQUIRE(jobs.jobs.size() == 1);
        CHECK(jobs.jobs.front().jobId == "download-2");
    }

    SECTION("cancel download job marks running job as canceled") {
        DownloadResponse running;
        running.url = "https://example.com/c.bin";
        running.jobId = "download-3";
        running.state = "running";
        running.createdAtMs = 500;
        running.updatedAtMs = 500;
        running.success = false;
        RequestDispatcher::__test_seedDownloadJob(running);

        auto resp = dispatchRequest(dispatcher, Request{CancelDownloadJobRequest{"download-3"}});

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK(dl.jobId == "download-3");
        CHECK(dl.state == "canceled");
        CHECK(dl.error == "Canceled by client");
        CHECK_FALSE(dl.success);
    }

    SECTION("cancel download job keeps terminal jobs terminal") {
        auto resp = dispatchRequest(dispatcher, Request{CancelDownloadJobRequest{"download-2"}});

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        CHECK(dl.jobId == "download-2");
        CHECK(dl.state == "failed");
        CHECK(dl.error == "network boom");
    }

    SECTION("cancel download job returns not found for unknown job") {
        auto resp = dispatchRequest(dispatcher, Request{CancelDownloadJobRequest{"missing-job"}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "Download job not found");
    }
}

TEST_CASE("RequestDispatcher: download jobs reload from persisted registry",
          "[daemon][documents][dispatcher][download-jobs]") {
    RequestDispatcher::__test_resetDownloadJobs();
    auto dispatchDownload = [](RequestDispatcher& dispatcher, const std::string& url,
                               std::string outputPath = {}, std::string checksum = {}) {
        DownloadRequest req;
        req.url = url;
        req.outputPath = std::move(outputPath);
        req.checksum = std::move(checksum);
        return dispatchRequest(dispatcher, Request{req});
    };
    auto waitForDownloadJob = [](RequestDispatcher& dispatcher, const std::string& jobId,
                                 std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        DownloadResponse latest;
        while (std::chrono::steady_clock::now() < deadline) {
            auto resp = dispatchRequest(dispatcher, Request{DownloadStatusRequest{jobId}});
            if (std::holds_alternative<DownloadResponse>(resp)) {
                latest = std::get<DownloadResponse>(resp);
                if (latest.state != "queued" && latest.state != "running") {
                    return latest;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return latest;
    };

    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_download_job_registry_persist_");
    cfg.downloadPolicy.enable = true;
    cfg.downloadPolicy.requireChecksum = false;
    cfg.downloadPolicy.allowedSchemes = {"https"};
    cfg.downloadPolicy.allowedHosts = {"*"};

    std::string jobId;

    {
        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        RequestDispatcher::__test_forceDownloadServiceUnavailableOnce();

        auto resp =
            dispatchDownload(dispatcher, "https://example.com/file.bin", {}, "sha256:abcd1234");

        REQUIRE(std::holds_alternative<DownloadResponse>(resp));
        const auto& dl = std::get<DownloadResponse>(resp);
        REQUIRE_FALSE(dl.jobId.empty());
        CHECK_FALSE(dl.success);
        CHECK(dl.state == "queued");
        jobId = dl.jobId;

        const auto finalStatus = waitForDownloadJob(dispatcher, jobId);
        CHECK(finalStatus.state == "failed");
        CHECK(finalStatus.error == "Download service not available in daemon");
    }

    RequestDispatcher::__test_forgetDownloadJobsCache();

    {
        StubLifecycle lifecycle;
        StateComponent state;
        state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
        DaemonLifecycleFsm lifecycleFsm;
        ServiceManager svc(cfg, state, lifecycleFsm);
        RequestDispatcher dispatcher(&lifecycle, &svc, &state);

        auto statusResp = dispatchRequest(dispatcher, Request{DownloadStatusRequest{jobId}});
        REQUIRE(std::holds_alternative<DownloadResponse>(statusResp));
        const auto& status = std::get<DownloadResponse>(statusResp);
        CHECK(status.jobId == jobId);
        CHECK(status.state == "failed");
        CHECK(status.error == "Download service not available in daemon");

        auto listResp = dispatchRequest(dispatcher, Request{ListDownloadJobsRequest{1}});
        REQUIRE(std::holds_alternative<ListDownloadJobsResponse>(listResp));
        const auto& list = std::get<ListDownloadJobsResponse>(listResp);
        REQUIRE(list.jobs.size() == 1);
        CHECK(list.jobs.front().jobId == jobId);
        CHECK(list.jobs.front().state == "failed");
    }
}

TEST_CASE("RequestDispatcher: embedding handlers cover generation and repair branches",
          "[daemon][embedding][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_embedding_dispatcher_");
    cfg.configFilePath = cfg.dataDir / "missing-config.toml";
    EnvGuard preferredModelGuard("YAMS_PREFERRED_MODEL", nullptr);

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    SECTION("generate embedding marks provider unavailable as degraded") {
        auto resp = dispatchRequest(
            dispatcher, Request{GenerateEmbeddingRequest{"hello world", "embed-model", true}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message.find("Plugin system") != std::string::npos);
        CHECK(lifecycle.setSubsystemDegradedCalls() == 1);
        CHECK(lifecycle.lastSubsystem() == "embedding");
        CHECK(lifecycle.lastDegraded());
        CHECK(lifecycle.lastReason() == "provider_unavailable");
    }

    SECTION("generate embedding auto-loads unloaded model and returns vector") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/embed-model.onnx");
        provider->setSingleEmbeddingResult({0.1f, 0.2f, 0.3f});
        svc.__test_setModelProvider(provider);

        GenerateEmbeddingRequest req{"hello world", "embed-model", true};
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<EmbeddingResponse>(resp));
        const auto& embedResp = std::get<EmbeddingResponse>(resp);
        CHECK(embedResp.embedding == std::vector<float>{0.1f, 0.2f, 0.3f});
        CHECK(embedResp.dimensions == 3);
        CHECK(embedResp.modelUsed == "embed-model");
        CHECK(provider->isModelLoaded("embed-model"));
    }

    SECTION("generate embedding forwards generator errors") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/embed-error.onnx");
        provider->setGenerateEmbeddingError(
            Error{ErrorCode::InvalidState, "generator unavailable"});
        svc.__test_setModelProvider(provider);

        GenerateEmbeddingRequest req{"hello world", "embed-error", true};
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == "generator unavailable");
    }

    SECTION("generate embedding sanitizes utf8 in load failures") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/embed-utf8.onnx");
        std::string invalidMessage = "prefix ";
        invalidMessage.push_back(static_cast<char>(0xC2));
        invalidMessage.push_back(static_cast<char>(0xA9));
        invalidMessage.push_back(static_cast<char>(0xE2));
        invalidMessage.push_back(static_cast<char>(0x82));
        invalidMessage.push_back(static_cast<char>(0xAC));
        invalidMessage.push_back(static_cast<char>(0xF0));
        invalidMessage.push_back(static_cast<char>(0x9F));
        invalidMessage.push_back(static_cast<char>(0x92));
        invalidMessage.push_back(static_cast<char>(0xA9));
        invalidMessage.push_back(static_cast<char>(0xC0));
        invalidMessage.push_back(static_cast<char>(0xFF));
        provider->setLoadError(Error{ErrorCode::InvalidState, invalidMessage});
        svc.__test_setModelProvider(provider);

        GenerateEmbeddingRequest req{"hello world", "embed-utf8", true};
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        std::string expected = "prefix ";
        expected.push_back(static_cast<char>(0xC2));
        expected.push_back(static_cast<char>(0xA9));
        expected.push_back(static_cast<char>(0xE2));
        expected.push_back(static_cast<char>(0x82));
        expected.push_back(static_cast<char>(0xAC));
        expected.push_back(static_cast<char>(0xF0));
        expected.push_back(static_cast<char>(0x9F));
        expected.push_back(static_cast<char>(0x92));
        expected.push_back(static_cast<char>(0xA9));
        expected += "\xEF\xBF\xBD\xEF\xBF\xBD";
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == expected);
    }

    SECTION("generate embedding honors timeout env floor while auto-loading") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/embed-timeout.onnx");
        provider->setSingleEmbeddingResult({0.7f, 0.8f, 0.9f});
        svc.__test_setModelProvider(provider);
        EnvGuard timeoutGuard("YAMS_MODEL_LOAD_TIMEOUT_MS", "5");

        GenerateEmbeddingRequest req{"hello world", "embed-timeout", true};
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<EmbeddingResponse>(resp));
        const auto& embedResp = std::get<EmbeddingResponse>(resp);
        CHECK(embedResp.embedding == std::vector<float>{0.7f, 0.8f, 0.9f});
        CHECK(provider->isModelLoaded("embed-timeout"));
    }

    SECTION("generate embedding converts isModelLoaded exceptions into internal errors") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/embed-throw.onnx");
        provider->setThrowOnIsModelLoadedCount(2);
        svc.__test_setModelProvider(provider);

        GenerateEmbeddingRequest req{"hello world", "embed-throw", true};
        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Embedding generation failed: isModelLoaded boom") !=
              std::string::npos);
    }

    SECTION("batch embedding falls back to per-item generation") {
        auto provider = std::make_shared<StubModelProvider>(2, "/tmp/batch-fallback.onnx");
        provider->setGenerateBatchError(Error{ErrorCode::NotImplemented, "batch unsupported"});
        provider->setSingleEmbeddingResult({0.4f, 0.6f});
        svc.__test_setModelProvider(provider);

        BatchEmbeddingRequest req;
        req.texts = {"one", "two"};
        req.modelName = "batch-fallback";
        req.normalize = false;
        req.batchSize = 2;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<BatchEmbeddingResponse>(resp));
        const auto& batchResp = std::get<BatchEmbeddingResponse>(resp);
        REQUIRE(batchResp.embeddings.size() == 2);
        CHECK(batchResp.embeddings[0] == std::vector<float>{0.4f, 0.6f});
        CHECK(batchResp.embeddings[1] == std::vector<float>{0.4f, 0.6f});
        CHECK(batchResp.successCount == 2);
        CHECK(batchResp.failureCount == 0);
        CHECK(batchResp.dimensions == 2);
        CHECK(batchResp.modelUsed == "batch-fallback");
    }

    SECTION("batch embedding counts per-item failures during fallback") {
        auto provider = std::make_shared<StubModelProvider>(2, "/tmp/batch-partial.onnx");
        provider->setGenerateBatchError(Error{ErrorCode::NotImplemented, "batch unsupported"});
        provider->setSingleEmbeddingResult({0.9f, 0.1f});
        provider->setSingleTextError("bad", Error{ErrorCode::InternalError, "single failed"});
        svc.__test_setModelProvider(provider);

        BatchEmbeddingRequest req;
        req.texts = {"good", "bad", "good-again"};
        req.modelName = "batch-partial";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<BatchEmbeddingResponse>(resp));
        const auto& batchResp = std::get<BatchEmbeddingResponse>(resp);
        REQUIRE(batchResp.embeddings.size() == 2);
        CHECK(batchResp.successCount == 2);
        CHECK(batchResp.failureCount == 1);
        CHECK(batchResp.dimensions == 2);
        CHECK(batchResp.modelUsed == "batch-partial");
    }

    SECTION("batch embedding forwards non-fallback errors") {
        auto provider = std::make_shared<StubModelProvider>(2, "/tmp/batch-error.onnx");
        provider->setGenerateBatchError(Error{ErrorCode::InvalidState, "batch failed"});
        svc.__test_setModelProvider(provider);

        BatchEmbeddingRequest req;
        req.texts = {"one", "two"};
        req.modelName = "batch-error";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == "batch failed");
    }

    SECTION("batch embedding auto-loads model and returns direct batch results") {
        auto provider = std::make_shared<StubModelProvider>(2, "/tmp/batch-direct.onnx");
        provider->setBatchEmbeddingResult({{0.2f, 0.8f}});
        svc.__test_setModelProvider(provider);
        EnvGuard timeoutGuard("YAMS_MODEL_LOAD_TIMEOUT_MS", "not-a-number");

        BatchEmbeddingRequest req;
        req.texts = {"one", "two"};
        req.modelName = "batch-direct";
        req.normalize = true;
        req.batchSize = 2;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<BatchEmbeddingResponse>(resp));
        const auto& batchResp = std::get<BatchEmbeddingResponse>(resp);
        REQUIRE(batchResp.embeddings.size() == 1);
        CHECK(batchResp.embeddings[0] == std::vector<float>{0.2f, 0.8f});
        CHECK(batchResp.successCount == 1);
        CHECK(batchResp.failureCount == 1);
        CHECK(batchResp.dimensions == 2);
        CHECK(batchResp.modelUsed == "batch-direct");
        CHECK(provider->isModelLoaded("batch-direct"));
    }

    SECTION("batch embedding returns load failures for unloaded models") {
        auto provider = std::make_shared<StubModelProvider>(2, "/tmp/batch-load-fail.onnx");
        provider->setLoadError(Error{ErrorCode::InvalidState, "batch load refused"});
        svc.__test_setModelProvider(provider);
        EnvGuard timeoutGuard("YAMS_MODEL_LOAD_TIMEOUT_MS", "5");

        BatchEmbeddingRequest req;
        req.texts = {"one", "two"};
        req.modelName = "batch-load-fail";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == "batch load refused");
    }

    SECTION("batch embedding converts isModelLoaded exceptions into internal errors") {
        auto provider = std::make_shared<StubModelProvider>(2, "/tmp/batch-throw.onnx");
        provider->setThrowOnIsModelLoadedCount(2);
        svc.__test_setModelProvider(provider);

        BatchEmbeddingRequest req;
        req.texts = {"one"};
        req.modelName = "batch-throw";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Batch embedding failed: isModelLoaded boom") != std::string::npos);
    }

    SECTION("embed documents rejects missing service manager") {
        RequestDispatcher noServiceDispatcher(&lifecycle, nullptr, &state);

        auto resp = dispatchRequest(noServiceDispatcher, Request{EmbedDocumentsRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message == "ServiceManager not available");
    }

    SECTION("embed documents reports unavailable model provider") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-provider.onnx");
        provider->setAvailable(false);
        svc.__test_setModelProvider(provider);

        EmbedDocumentsRequest req;
        req.modelName = "repair-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message == "Model provider not available");
    }

    SECTION("embed documents returns success when repair finds no work") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-empty.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());
        svc.__test_setMetadataRepo(std::make_shared<StubPruneMetadataRepository>());

        EmbedDocumentsRequest req;
        req.modelName = "repair-model";
        req.batchSize = 4;
        req.skipExisting = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<EmbedDocumentsResponse>(resp));
        const auto& embedResp = std::get<EmbedDocumentsResponse>(resp);
        CHECK(embedResp.requested == 0);
        CHECK(embedResp.embedded == 0);
        CHECK(embedResp.skipped == 0);
        CHECK(embedResp.failed == 0);
    }

    SECTION("embed documents rejects non-empty hash lists that resolve to no daemon documents") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-missing-hashes.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());
        svc.__test_setMetadataRepo(std::make_shared<StubPruneMetadataRepository>());

        EmbedDocumentsRequest req;
        req.modelName = "repair-model";
        req.documentHashes = {std::string(64, 'a')};
        req.batchSize = 4;
        req.skipExisting = false;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message.find("requested documents were found") != std::string::npos);
    }

    SECTION("embed documents rejects degraded providers") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-degraded.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setModelProviderDegraded(true, "simulated degraded state");

        auto resp = dispatchRequest(dispatcher, Request{EmbedDocumentsRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == "Embedding generation disabled: provider degraded");

        svc.__test_setModelProviderDegraded(false);
    }

    SECTION("embed documents reports missing content store") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-content.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setMetadataRepo(std::make_shared<StubPruneMetadataRepository>());

        EmbedDocumentsRequest req;
        req.modelName = "repair-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message == "Content store not available");
    }

    SECTION("embed documents reports missing metadata repo") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-meta.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());

        EmbedDocumentsRequest req;
        req.modelName = "repair-model";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message == "Metadata repository not available");
    }

    SECTION("embed documents reports missing configured model name") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-empty-model.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());
        svc.__test_setMetadataRepo(std::make_shared<StubPruneMetadataRepository>());

        EmbedDocumentsRequest req;
        req.modelName = "";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotInitialized);
        CHECK(err.message == "No embedding model configured");
    }

    SECTION("embed documents uses daemon config resolver when request model is empty") {
        auto provider = std::make_shared<StubModelProvider>(3, "/tmp/repair-config-model.onnx");
        svc.__test_setModelProvider(provider);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());
        svc.__test_setMetadataRepo(std::make_shared<StubPruneMetadataRepository>());

        const auto configPath = cfg.dataDir / "config.toml";
        {
            std::ofstream out(configPath);
            REQUIRE(out.is_open());
            out << "[embeddings]\n";
            out << "preferred_model = \"config-repair-model\"\n";
        }
        cfg.configFilePath = configPath;

        DaemonLifecycleFsm configLifecycleFsm;
        ServiceManager configSvc(cfg, state, configLifecycleFsm);
        configSvc.__test_setModelProvider(provider);
        configSvc.__test_setContentStore(std::make_shared<StubContentStore>());
        configSvc.__test_setMetadataRepo(std::make_shared<StubPruneMetadataRepository>());
        RequestDispatcher configDispatcher(&lifecycle, &configSvc, &state);

        EmbedDocumentsRequest req;
        req.modelName.clear();
        req.batchSize = 4;
        req.skipExisting = true;

        auto resp = dispatchRequest(configDispatcher, Request{req});

        REQUIRE(std::holds_alternative<EmbedDocumentsResponse>(resp));
        CHECK(provider->isModelLoaded("config-repair-model"));
    }
}

TEST_CASE("RequestDispatcher: graph maintenance handlers report missing graph component",
          "[daemon][graph][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_graph_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    SECTION("graph repair returns internal error when graph component is unavailable") {
        GraphRepairRequest req;
        req.dryRun = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("GraphComponent not available") != std::string::npos);
    }

    SECTION("graph validate returns internal error when graph component is unavailable") {
        auto resp = dispatchRequest(dispatcher, Request{GraphValidateRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("GraphComponent not available") != std::string::npos);
    }
}

TEST_CASE("RequestDispatcher: graph query and ingest handlers cover dispatcher branches",
          "[daemon][graph][dispatcher][query]") {
    GraphDispatcherFixture fixture;

    SECTION("graph query reports unavailable metadata repository") {
        auto resp = dispatchRequest(*fixture.dispatcher, Request{GraphQueryRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Metadata repository unavailable");
    }

    SECTION("graph query reports unavailable knowledge graph store") {
        fixture.initMetadata(false);

        auto resp = dispatchRequest(*fixture.dispatcher, Request{GraphQueryRequest{}});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK_FALSE(graphResp.kgAvailable);
        CHECK(graphResp.warning == "Knowledge graph not available");
    }

    SECTION("graph path history reports unavailable metadata repository") {
        GraphPathHistoryRequest req;
        req.path = "/repo/file.cpp";

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Metadata repository unavailable");
    }

    SECTION("graph query traversal requires an origin") {
        fixture.initMetadata();

        auto resp = dispatchRequest(*fixture.dispatcher, Request{GraphQueryRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message.find("No valid origin specified") != std::string::npos);
    }

    SECTION("graph query lists node types") {
        fixture.initMetadata();
        fixture.upsertNode("fn:list:1", "alpha", "function");
        fixture.upsertNode("fn:list:2", "beta", "function");
        fixture.upsertNode("path:list:1", "file.cpp", "file");

        GraphQueryRequest req;
        req.listTypes = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.kgAvailable);
        CHECK(graphResp.originNode.label == "listTypes");
        REQUIRE(graphResp.nodeTypeCounts.size() == 2);
        CHECK(graphResp.nodeTypeCounts[0] == std::pair<std::string, uint64_t>{"function", 2});
        CHECK(graphResp.nodeTypeCounts[1] == std::pair<std::string, uint64_t>{"file", 1});
    }

    SECTION("graph query list types reports store errors") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_nodes RENAME TO kg_nodes_broken");

        GraphQueryRequest req;
        req.listTypes = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code != ErrorCode::Success);
        CHECK_FALSE(err.message.empty());
    }

    SECTION("graph query lists relation counts") {
        fixture.initMetadata();
        auto callerId = fixture.upsertNode("fn:rel:caller", "caller", "function");
        auto calleeId = fixture.upsertNode("fn:rel:callee", "callee", "function");
        auto fileId = fixture.upsertNode("path:rel:file", "file.cpp", "file");
        fixture.addEdge(callerId, calleeId, "calls");
        fixture.addEdge(callerId, fileId, "includes");

        GraphQueryRequest req;
        req.listRelations = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.originNode.label == "listRelations");
        REQUIRE(graphResp.relationTypeCounts.size() == 2);
        CHECK(graphResp.relationTypeCounts[0] == std::pair<std::string, uint64_t>{"calls", 1});
        CHECK(graphResp.relationTypeCounts[1] == std::pair<std::string, uint64_t>{"includes", 1});
    }

    SECTION("graph query list relations reports store errors") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_edges RENAME TO kg_edges_broken");

        GraphQueryRequest req;
        req.listRelations = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code != ErrorCode::Success);
        CHECK_FALSE(err.message.empty());
    }

    SECTION("graph query search mode requires a pattern") {
        fixture.initMetadata();

        GraphQueryRequest req;
        req.searchMode = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message == "searchPattern is required for search mode");
    }

    SECTION("graph query search mode returns matching nodes with properties") {
        fixture.initMetadata();
        fixture.upsertNode("fn:search:alpha", "AlphaNode", "function", R"({"rank":1})");
        fixture.upsertNode("fn:search:beta", "BetaNode", "function");

        GraphQueryRequest req;
        req.searchMode = true;
        req.searchPattern = "%Alpha%";
        req.includeNodeProperties = true;
        req.limit = 10;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.originNode.label == "search:%Alpha%");
        REQUIRE(graphResp.connectedNodes.size() == 1);
        CHECK(graphResp.connectedNodes[0].nodeKey == "fn:search:alpha");
        CHECK(graphResp.connectedNodes[0].properties == R"({"rank":1})");
    }

    SECTION("graph query search mode reports store errors") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_nodes RENAME TO kg_nodes_broken");

        GraphQueryRequest req;
        req.searchMode = true;
        req.searchPattern = "%Alpha%";

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code != ErrorCode::Success);
        CHECK_FALSE(err.message.empty());
    }

    SECTION("graph query list by type requires node type") {
        fixture.initMetadata();

        GraphQueryRequest req;
        req.listByType = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message == "nodeType is required for listByType mode");
    }

    SECTION("graph query list by type returns paginated nodes") {
        fixture.initMetadata();
        fixture.upsertNode("fn:type:1", "TypeOne", "function", R"({"kind":"a"})");
        fixture.upsertNode("fn:type:2", "TypeTwo", "function", R"({"kind":"b"})");
        fixture.upsertNode("path:type:1", "file", "file");

        GraphQueryRequest req;
        req.listByType = true;
        req.nodeType = "function";
        req.limit = 1;
        req.offset = 0;
        req.includeNodeProperties = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.originNode.label == "listByType:function");
        CHECK(graphResp.totalNodesFound == 2);
        CHECK(graphResp.truncated);
        REQUIRE(graphResp.connectedNodes.size() == 1);
        CHECK(graphResp.connectedNodes[0].type == "function");
        CHECK_FALSE(graphResp.connectedNodes[0].properties.empty());
    }

    SECTION("graph query list by type reports store errors") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_nodes RENAME TO kg_nodes_broken");

        GraphQueryRequest req;
        req.listByType = true;
        req.nodeType = "function";

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code != ErrorCode::Success);
        CHECK_FALSE(err.message.empty());
    }

    SECTION("graph query isolated mode canonicalizes relation name") {
        fixture.initMetadata();
        auto calledId = fixture.upsertNode("fn:isolated:called", "called", "function");
        fixture.upsertNode("fn:isolated:one", "isolated1", "function");
        fixture.upsertNode("fn:isolated:two", "isolated2", "function");
        auto callerId = fixture.upsertNode("fn:isolated:caller", "caller", "function");
        fixture.addEdge(callerId, calledId, "calls");

        GraphQueryRequest req;
        req.isolatedMode = true;
        req.nodeType = "function";
        req.isolatedRelation = " CALL ";
        req.limit = 10;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.originNode.label == "isolated:function:calls");
        CHECK(graphResp.totalNodesFound == 3);
        for (const auto& node : graphResp.connectedNodes) {
            CHECK(node.nodeKey != "fn:isolated:called");
        }
    }

    SECTION("graph query isolated mode uses default node type and relation") {
        fixture.initMetadata();
        auto calledId = fixture.upsertNode("fn:isolated:default:called", "called", "function");
        fixture.upsertNode("fn:isolated:default:free", "free", "function");
        auto callerId = fixture.upsertNode("fn:isolated:default:caller", "caller", "function");
        fixture.addEdge(callerId, calledId, "calls");

        GraphQueryRequest req;
        req.isolatedMode = true;
        req.limit = 10;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.originNode.label == "isolated:function:calls");
        REQUIRE(graphResp.connectedNodes.size() == 2);
    }

    SECTION("graph query isolated mode reports store errors") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_nodes RENAME TO kg_nodes_broken");

        GraphQueryRequest req;
        req.isolatedMode = true;
        req.limit = 10;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code != ErrorCode::Success);
        CHECK_FALSE(err.message.empty());
    }

    SECTION("graph query node key lookup returns not found") {
        fixture.initMetadata();

        GraphQueryRequest req;
        req.nodeKey = "missing-node";

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "Node not found: missing-node");
    }

    SECTION("graph query traverses from node key with canonical relation filters") {
        fixture.initMetadata();
        auto callerId = fixture.upsertNode("fn:key:caller", "caller", "function");
        auto calleeId = fixture.upsertNode("fn:key:callee", "callee", "function");
        auto fileId = fixture.upsertNode("path:key:file", "header", "file");
        fixture.addEdge(callerId, calleeId, "calls", 0.8f, R"({"source":"unit"})");
        fixture.addEdge(callerId, fileId, "includes");

        GraphQueryRequest req;
        req.nodeKey = "fn:key:caller";
        req.maxDepth = 1;
        req.maxResults = 10;
        req.relationFilters = {" Call "};
        req.includeEdgeProperties = true;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        CHECK(graphResp.kgAvailable);
        CHECK(graphResp.originNode.nodeKey == "fn:key:caller");
        REQUIRE(graphResp.connectedNodes.size() == 1);
        CHECK(graphResp.connectedNodes[0].nodeKey == "fn:key:callee");
        REQUIRE(graphResp.edges.size() == 1);
        CHECK(graphResp.edges[0].relation == "calls");
        CHECK(graphResp.edges[0].properties == R"({"source":"unit"})");
    }

    SECTION("graph query canonicalizes relation aliases and skips blank filters") {
        fixture.initMetadata();
        auto originId = fixture.upsertNode("fn:key:origin", "origin", "function");
        auto includeId = fixture.upsertNode("path:key:include", "header", "file");
        auto inheritId = fixture.upsertNode("cls:key:inherit", "Base", "class");
        auto implementId = fixture.upsertNode("iface:key:impl", "Iface", "interface");
        auto referenceId = fixture.upsertNode("sym:key:ref", "Ref", "symbol");
        auto renameToId = fixture.upsertNode("path:key:rename_to", "new.cpp", "file");
        auto renameFromId = fixture.upsertNode("path:key:rename_from", "old.cpp", "file");
        auto customId = fixture.upsertNode("sym:key:custom", "Custom", "symbol");
        fixture.addEdge(originId, includeId, "includes");
        fixture.addEdge(originId, inheritId, "inherits");
        fixture.addEdge(originId, implementId, "implements");
        fixture.addEdge(originId, referenceId, "references");
        fixture.addEdge(originId, renameToId, "renamed_to");
        fixture.addEdge(originId, renameFromId, "renamed_from");
        fixture.addEdge(originId, customId, "custom_rel");

        GraphQueryRequest req;
        req.nodeKey = "fn:key:origin";
        req.maxDepth = 1;
        req.maxResults = 20;
        req.relationFilters = {"   ",       " Include ", "inherit",     "implement",
                               "reference", "rename_to", "rename_from", "custom-rel"};

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphQueryResponse>(resp));
        const auto& graphResp = std::get<GraphQueryResponse>(resp);
        REQUIRE(graphResp.edges.size() == 7);
        std::unordered_set<std::string> relations;
        for (const auto& edge : graphResp.edges) {
            relations.insert(edge.relation);
        }
        CHECK(relations.count("includes") == 1);
        CHECK(relations.count("inherits") == 1);
        CHECK(relations.count("implements") == 1);
        CHECK(relations.count("references") == 1);
        CHECK(relations.count("renamed_to") == 1);
        CHECK(relations.count("renamed_from") == 1);
        CHECK(relations.count("custom_rel") == 1);
    }

    SECTION("graph path history requires path") {
        fixture.initMetadata();

        auto resp = dispatchRequest(*fixture.dispatcher, Request{GraphPathHistoryRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidArgument);
        CHECK(err.message == "Path is required for path history query");
    }

    SECTION("graph path history returns empty response when kg store is unavailable") {
        fixture.initMetadata(false);

        GraphPathHistoryRequest req;
        req.path = "/repo/file.cpp";

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphPathHistoryResponse>(resp));
        const auto& historyResp = std::get<GraphPathHistoryResponse>(resp);
        CHECK(historyResp.queryPath == "/repo/file.cpp");
        CHECK(historyResp.history.empty());
        CHECK_FALSE(historyResp.hasMore);
    }

    SECTION("graph path history returns snapshot entries") {
        fixture.initMetadata();
        fixture.upsertNode("path:snap1:/repo/file.cpp", "/repo/file.cpp", "path",
                           R"({"snapshot_id":"snap1","path":"/repo/file.cpp"})");
        fixture.upsertNode("path:snap2:/repo/file.cpp", "/repo/file.cpp", "path",
                           R"({"snapshot_id":"snap2","path":"/repo/file.cpp"})");

        GraphPathHistoryRequest req;
        req.path = "/repo/file.cpp";
        req.limit = 10;

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<GraphPathHistoryResponse>(resp));
        const auto& historyResp = std::get<GraphPathHistoryResponse>(resp);
        CHECK(historyResp.queryPath == "/repo/file.cpp");
        REQUIRE(historyResp.history.size() == 2);
        CHECK(historyResp.history[0].snapshotId == "snap2");
        CHECK(historyResp.history[0].blobHash.empty());
        CHECK(historyResp.history[0].changeType == "unknown");
        CHECK(historyResp.history[1].snapshotId == "snap1");
        CHECK_FALSE(historyResp.hasMore);
    }

    SECTION("graph path history reports store errors") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_nodes RENAME TO kg_nodes_broken");

        GraphPathHistoryRequest req;
        req.path = "/repo/file.cpp";

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code != ErrorCode::Success);
        CHECK_FALSE(err.message.empty());
    }

    SECTION("kg ingest reports unavailable metadata repository") {
        auto resp = dispatchRequest(*fixture.dispatcher, Request{KgIngestRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Metadata repository unavailable");
    }

    SECTION("kg ingest reports unavailable knowledge graph store") {
        fixture.initMetadata(false);

        auto resp = dispatchRequest(*fixture.dispatcher, Request{KgIngestRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Knowledge graph store unavailable");
    }

    SECTION("kg ingest records node edge and alias resolution errors") {
        fixture.initMetadata();
        auto existingId = fixture.upsertNode("fn:existing", "existing", "function");
        (void)existingId;

        KgIngestRequest req;
        req.nodes.push_back(KgIngestNode{"fn:existing", "existing updated", "function", ""});
        req.edges.push_back(KgIngestEdge{"fn:existing", "fn:missing", "calls", 1.0f, ""});
        req.aliases.push_back(KgIngestAlias{"fn:missing", "missing-alias", "unit", 0.5f});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK(ingestResp.success);
        CHECK(ingestResp.nodesInserted == 1);
        CHECK(ingestResp.edgesSkipped == 1);
        CHECK(ingestResp.aliasesSkipped == 1);
        REQUIRE(ingestResp.errors.size() == 2);
        CHECK(ingestResp.errors[0] == "Edge destination node not found: fn:missing");
        CHECK(ingestResp.errors[1] == "Alias node not found: fn:missing");
    }

    SECTION("kg ingest inserts nodes edges and aliases") {
        fixture.initMetadata();

        KgIngestRequest req;
        req.nodes.push_back(
            KgIngestNode{"fn:ingest:src", "Source", "function", R"({"role":"src"})"});
        req.nodes.push_back(KgIngestNode{"fn:ingest:dst", "Dest", "function", ""});
        req.edges.push_back(KgIngestEdge{"fn:ingest:src", "fn:ingest:dst", "calls", 0.7f,
                                         R"({"origin":"dispatcher"})"});
        req.aliases.push_back(KgIngestAlias{"fn:ingest:src", "sourceAlias", "unit", 0.9f});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK(ingestResp.success);
        CHECK(ingestResp.nodesInserted == 2);
        CHECK(ingestResp.edgesInserted == 1);
        CHECK(ingestResp.aliasesInserted == 1);
        CHECK(ingestResp.errors.empty());

        auto sourceNode = fixture.kgStore->getNodeByKey("fn:ingest:src");
        REQUIRE(sourceNode.has_value());
        REQUIRE(sourceNode.value().has_value());
        auto edges =
            fixture.kgStore->getEdgesFrom(sourceNode.value()->id, std::string_view("calls"), 10, 0);
        REQUIRE(edges.has_value());
        REQUIRE(edges.value().size() == 1);
        auto aliases = fixture.kgStore->resolveAliasExact("sourceAlias", 10);
        REQUIRE(aliases.has_value());
        REQUIRE(aliases.value().size() == 1);
    }

    SECTION("kg ingest reports node upsert failures") {
        fixture.initMetadata();
        fixture.execSql("ALTER TABLE kg_nodes RENAME TO kg_nodes_broken");

        KgIngestRequest req;
        req.nodes.push_back(KgIngestNode{"fn:broken:node", "Broken", "function", ""});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK_FALSE(ingestResp.success);
        CHECK(ingestResp.nodesInserted == 0);
        REQUIRE(ingestResp.errors.size() == 1);
        CHECK(ingestResp.errors[0].find("Node upsert failed:") == 0);
    }

    SECTION("kg ingest inserts edges without de-duplication when requested") {
        fixture.initMetadata();
        fixture.upsertNode("fn:addedges:src", "Source", "function");
        fixture.upsertNode("fn:addedges:dst", "Dest", "function");

        KgIngestRequest req;
        req.skipExistingEdges = false;
        req.edges.push_back(KgIngestEdge{"fn:addedges:src", "fn:addedges:dst", "calls", 1.0f, ""});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK(ingestResp.success);
        CHECK(ingestResp.edgesInserted == 1);
        CHECK(ingestResp.errors.empty());
    }

    SECTION("kg ingest reports edge insert failures") {
        fixture.initMetadata();
        fixture.upsertNode("fn:edgefail:src", "Source", "function");
        fixture.upsertNode("fn:edgefail:dst", "Dest", "function");
        fixture.execSql("ALTER TABLE kg_edges RENAME TO kg_edges_broken");

        KgIngestRequest req;
        req.skipExistingEdges = false;
        req.edges.push_back(KgIngestEdge{"fn:edgefail:src", "fn:edgefail:dst", "calls", 1.0f, ""});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK_FALSE(ingestResp.success);
        CHECK(ingestResp.edgesInserted == 0);
        REQUIRE(ingestResp.errors.size() == 1);
        CHECK(ingestResp.errors[0].find("Edge insert failed:") == 0);
    }

    SECTION("kg ingest reports alias insert failures") {
        fixture.initMetadata();
        fixture.upsertNode("fn:aliasfail:node", "Node", "function");
        fixture.execSql("ALTER TABLE kg_aliases RENAME TO kg_aliases_broken");

        KgIngestRequest req;
        req.aliases.push_back(KgIngestAlias{"fn:aliasfail:node", "brokenAlias", "unit", 0.4f});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK(ingestResp.success);
        CHECK(ingestResp.aliasesInserted == 0);
        REQUIRE(ingestResp.errors.size() == 1);
        CHECK(ingestResp.errors[0].find("Alias insert failed:") == 0);
    }

    SECTION("kg ingest resolves existing nodes for edges and aliases") {
        fixture.initMetadata();
        auto srcId = fixture.upsertNode("fn:existing:src", "Source", "function");
        auto dstId = fixture.upsertNode("fn:existing:dst", "Dest", "function");
        auto aliasNodeId = fixture.upsertNode("fn:existing:alias", "AliasTarget", "function");
        (void)srcId;
        (void)dstId;
        (void)aliasNodeId;

        KgIngestRequest req;
        req.edges.push_back(KgIngestEdge{"fn:existing:src", "fn:existing:dst", "calls", 1.0f, ""});
        req.aliases.push_back(KgIngestAlias{"fn:existing:alias", "existingAlias", "", 0.6f});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK(ingestResp.success);
        CHECK(ingestResp.nodesInserted == 0);
        CHECK(ingestResp.edgesInserted == 1);
        CHECK(ingestResp.aliasesInserted == 1);
        CHECK(ingestResp.errors.empty());

        auto aliases = fixture.kgStore->resolveAliasExact("existingAlias", 10);
        REQUIRE(aliases.has_value());
        REQUIRE(aliases.value().size() == 1);
    }

    SECTION("kg ingest reports missing edge source nodes") {
        fixture.initMetadata();
        fixture.upsertNode("fn:existing:dst_only", "Dest", "function");

        KgIngestRequest req;
        req.edges.push_back(
            KgIngestEdge{"fn:missing:src", "fn:existing:dst_only", "calls", 1.0f, ""});

        auto resp = dispatchRequest(*fixture.dispatcher, Request{req});

        REQUIRE(std::holds_alternative<KgIngestResponse>(resp));
        const auto& ingestResp = std::get<KgIngestResponse>(resp);
        CHECK(ingestResp.success);
        CHECK(ingestResp.edgesInserted == 0);
        CHECK(ingestResp.edgesSkipped == 1);
        REQUIRE(ingestResp.errors.size() == 1);
        CHECK(ingestResp.errors[0] == "Edge source node not found: fn:missing:src");
    }
}

TEST_CASE("RequestDispatcher: plugin handlers cover readiness and error branches",
          "[daemon][plugin][dispatcher]") {
    auto makeReadyService = []() {
        DaemonConfig cfg;
        cfg.dataDir = makeTempDir("yams_plugin_dispatcher_");

        auto state = std::make_unique<StateComponent>();
        state->readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state->readiness.metadataRepoReady.store(true, std::memory_order_relaxed);

        auto lifecycleFsm = std::make_unique<DaemonLifecycleFsm>();
        auto svc = std::make_unique<ServiceManager>(cfg, *state, *lifecycleFsm);
        svc->__test_pluginScanComplete(0);
        REQUIRE(svc->getPluginHostFsmSnapshot().state == PluginHostState::Ready);

        return std::tuple{std::move(state), std::move(lifecycleFsm), std::move(svc)};
    };

    SECTION("plugin handlers reject non-ready host state") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        svc->__test_pluginLoadFailed("simulated plugin init failure");

        auto resp = dispatchRequest(dispatcher, Request{PluginTrustListRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InvalidState);
        CHECK(err.message == "Plugin subsystem not ready (state=failed)");
    }

    SECTION("plugin handlers reject missing service manager and non-ready FSM states") {
        auto state = std::make_unique<StateComponent>();
        state->readiness.contentStoreReady.store(true, std::memory_order_relaxed);
        state->readiness.metadataRepoReady.store(true, std::memory_order_relaxed);

        StubLifecycle lifecycle;

        {
            RequestDispatcher dispatcher(&lifecycle, nullptr, state.get());
            auto resp = dispatchRequest(dispatcher, Request{PluginTrustListRequest{}});

            REQUIRE(std::holds_alternative<ErrorResponse>(resp));
            const auto& err = std::get<ErrorResponse>(resp);
            CHECK(err.code == ErrorCode::InvalidState);
            CHECK(err.message == "Plugin host unavailable");
        }

        {
            auto [readyState, lifecycleFsm, svc] = makeReadyService();
            RequestDispatcher dispatcher(&lifecycle, svc.get(), readyState.get());

            svc->__test_pluginScanStarted(1);

            auto resp = dispatchRequest(dispatcher, Request{PluginTrustListRequest{}});

            REQUIRE(std::holds_alternative<ErrorResponse>(resp));
            const auto& err = std::get<ErrorResponse>(resp);
            CHECK(err.code == ErrorCode::InvalidState);
            CHECK(err.message == "Plugin subsystem not ready (state=scanning)");
        }

        {
            auto [readyState, lifecycleFsm, svc] = makeReadyService();
            RequestDispatcher dispatcher(&lifecycle, svc.get(), readyState.get());

            svc->__test_pluginLoaded("mock-loaded-plugin");
            svc->__test_pluginScanComplete(1);
            svc->__test_pluginLoaded("mock-loaded-plugin");

            auto resp = dispatchRequest(dispatcher, Request{PluginTrustListRequest{}});

            REQUIRE(std::holds_alternative<ErrorResponse>(resp));
            const auto& err = std::get<ErrorResponse>(resp);
            CHECK(err.code == ErrorCode::InvalidState);
            CHECK(err.message == "Plugin subsystem not ready (state=loading)");
        }
    }

    SECTION("plugin handlers report missing hosts and trust failures") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        svc->__test_setAbiHost({});
        svc->__test_setExternalPluginHost({});

        auto scanResp = dispatchRequest(dispatcher, Request{PluginScanRequest{}});
        REQUIRE(std::holds_alternative<ErrorResponse>(scanResp));
        const auto& scanErr = std::get<ErrorResponse>(scanResp);
        CHECK(scanErr.code == ErrorCode::NotImplemented);
        CHECK(scanErr.message == "No plugin host available");

        PluginLoadRequest loadReq;
        loadReq.pathOrName = "missing-plugin";
        auto loadResp = dispatchRequest(dispatcher, Request{loadReq});

        REQUIRE(std::holds_alternative<ErrorResponse>(loadResp));
        const auto& loadErr = std::get<ErrorResponse>(loadResp);
        CHECK(loadErr.code == ErrorCode::NotImplemented);
        CHECK(loadErr.message == "No plugin host available");

        auto unloadResp =
            dispatchRequest(dispatcher, Request{PluginUnloadRequest{"missing-plugin"}});
        REQUIRE(std::holds_alternative<ErrorResponse>(unloadResp));
        const auto& unloadErr = std::get<ErrorResponse>(unloadResp);
        CHECK(unloadErr.code == ErrorCode::NotFound);
        CHECK(unloadErr.message == "Plugin not found or unload failed");

        auto trustAddResp =
            dispatchRequest(dispatcher, Request{PluginTrustAddRequest{"/tmp/missing-host"}});
        REQUIRE(std::holds_alternative<ErrorResponse>(trustAddResp));
        const auto& trustAddErr = std::get<ErrorResponse>(trustAddResp);
        CHECK(trustAddErr.code == ErrorCode::Unknown);
        CHECK(trustAddErr.message == "Trust add failed");

        auto trustRemoveResp =
            dispatchRequest(dispatcher, Request{PluginTrustRemoveRequest{"/tmp/missing-host"}});
        REQUIRE(std::holds_alternative<ErrorResponse>(trustRemoveResp));
        const auto& trustRemoveErr = std::get<ErrorResponse>(trustRemoveResp);
        CHECK(trustRemoveErr.code == ErrorCode::Unknown);
        CHECK(trustRemoveErr.message == "Trust remove failed");
    }

    SECTION("plugin load dry run reports missing plugin") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        PluginLoadRequest req;
        req.pathOrName = (svc->getResolvedDataDir() / "missing_plugin.py").string();
        req.dryRun = true;

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::NotFound);
        CHECK(err.message == "Plugin not found");
    }

    SECTION("plugin scan and load search default directories") {
        auto homeDir = makeTempDir("yams_plugin_home_");
        auto homeText = homeDir.string();
        EnvGuard homeGuard("HOME", homeText.c_str());

        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        auto abiDir = homeDir / ".local" / "lib" / "yams" / "plugins";
        std::filesystem::create_directories(abiDir);

        auto fakePlugin = abiDir / "yams_default_scan.so";
        {
            std::ofstream out(fakePlugin, std::ios::binary);
            out << "not a valid plugin";
        }

        auto externalDir = homeDir / ".local" / "lib" / "yams" / "external-plugins";
        std::filesystem::create_directories(externalDir);
        auto externalPluginDir =
            createMockExternalPlugin(externalDir, "dispatcher_external_default");

        auto missingResp = dispatchRequest(
            dispatcher, Request{PluginLoadRequest{"missing_default_plugin.so", "", false}});
        REQUIRE(std::holds_alternative<ErrorResponse>(missingResp));
        const auto& missingErr = std::get<ErrorResponse>(missingResp);
        CHECK(missingErr.code == ErrorCode::NotFound);
        CHECK(missingErr.message == "Plugin not found");

        auto scanResp = dispatchRequest(dispatcher, Request{PluginScanRequest{}});
        REQUIRE(std::holds_alternative<PluginScanResponse>(scanResp));
        const auto& scan = std::get<PluginScanResponse>(scanResp);

        bool foundDefaultPlugin = false;
        bool foundDefaultExternalPlugin = false;
        for (const auto& record : scan.plugins) {
            if (record.path == fakePlugin.string()) {
                foundDefaultPlugin = true;
                CHECK(record.name == "yams_default_scan");
            }
            if (record.path == externalPluginDir.string()) {
                foundDefaultExternalPlugin = true;
                CHECK(record.name == "dispatcher_external_plugin");
            }
        }
        CHECK(foundDefaultPlugin);
        CHECK(foundDefaultExternalPlugin);

        auto trustResp =
            dispatchRequest(dispatcher, Request{PluginTrustAddRequest{abiDir.string()}});
        REQUIRE(std::holds_alternative<SuccessResponse>(trustResp));

        auto* external = svc->getExternalPluginHost();
        REQUIRE(external != nullptr);
        REQUIRE(external->trustAdd(externalDir).has_value());

        PluginLoadRequest req;
        req.pathOrName = fakePlugin.filename().string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Plugin load failed for: " + fakePlugin.string());

        PluginLoadRequest externalReq;
        externalReq.pathOrName = externalPluginDir.filename().string();

        auto externalResp = dispatchRequest(dispatcher, Request{externalReq});

        REQUIRE(std::holds_alternative<PluginLoadResponse>(externalResp));
        const auto& externalLoad = std::get<PluginLoadResponse>(externalResp);
        CHECK(externalLoad.loaded);
        CHECK(externalLoad.record.name == "dispatcher_external_plugin");
        CHECK(externalLoad.record.path == externalPluginDir.string());

        auto externalUnloadResp =
            dispatchRequest(dispatcher, Request{PluginUnloadRequest{"dispatcher_external_plugin"}});
        REQUIRE(std::holds_alternative<SuccessResponse>(externalUnloadResp));
    }

    SECTION("plugin handlers route external plugins when ABI host is absent") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        svc->__test_setAbiHost({});

        auto pluginDir = createMockExternalPlugin(svc->getResolvedDataDir(), "dispatcher_external");

        PluginScanRequest scanReq;
        scanReq.target = pluginDir.string();
        auto scanResp = dispatchRequest(dispatcher, Request{scanReq});

        REQUIRE(std::holds_alternative<PluginScanResponse>(scanResp));
        const auto& scan = std::get<PluginScanResponse>(scanResp);
        REQUIRE(scan.plugins.size() == 1);
        CHECK(scan.plugins.front().name == "dispatcher_external_plugin");

        PluginLoadRequest dryRunReq;
        dryRunReq.pathOrName = pluginDir.string();
        dryRunReq.dryRun = true;
        auto dryRunResp = dispatchRequest(dispatcher, Request{dryRunReq});

        REQUIRE(std::holds_alternative<PluginLoadResponse>(dryRunResp));
        const auto& dryRun = std::get<PluginLoadResponse>(dryRunResp);
        CHECK_FALSE(dryRun.loaded);
        CHECK(dryRun.message == "dry-run");
        CHECK(dryRun.record.name == "dispatcher_external_plugin");

        auto* external = svc->getExternalPluginHost();
        REQUIRE(external != nullptr);
        REQUIRE(external->trustAdd(pluginDir).has_value());

        PluginLoadRequest loadReq;
        loadReq.pathOrName = pluginDir.string();
        auto loadResp = dispatchRequest(dispatcher, Request{loadReq});

        REQUIRE(std::holds_alternative<PluginLoadResponse>(loadResp));
        const auto& load = std::get<PluginLoadResponse>(loadResp);
        CHECK(load.loaded);
        CHECK(load.message == "loaded");
        CHECK(load.record.name == "dispatcher_external_plugin");
        CHECK(external->listLoaded().size() == 1);

        auto unloadResp =
            dispatchRequest(dispatcher, Request{PluginUnloadRequest{"dispatcher_external_plugin"}});

        REQUIRE(std::holds_alternative<SuccessResponse>(unloadResp));
        CHECK(std::get<SuccessResponse>(unloadResp).message == "unloaded");
        CHECK(external->listLoaded().empty());
    }

    SECTION("plugin load routes direct external files before ABI fallback") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());
        auto shimPath = makePythonShimPath();
        EnvGuard pythonGuard("PATH", shimPath.c_str());

        auto pluginDir =
            createMockExternalPlugin(svc->getResolvedDataDir(), "dispatcher_external_py");
        auto pluginFile = pluginDir / "plugin.py";

        auto* external = svc->getExternalPluginHost();
        REQUIRE(external != nullptr);
        REQUIRE(external->trustAdd(pluginFile).has_value());

        PluginLoadRequest req;
        req.pathOrName = pluginFile.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PluginLoadResponse>(resp));
        const auto& load = std::get<PluginLoadResponse>(resp);
        CHECK(load.loaded);
        CHECK(load.record.name == "dispatcher_external_plugin");
        CHECK(load.record.path == pluginFile.string());

        auto unloadResp =
            dispatchRequest(dispatcher, Request{PluginUnloadRequest{"dispatcher_external_plugin"}});
        REQUIRE(std::holds_alternative<SuccessResponse>(unloadResp));
    }

    SECTION("plugin load recognizes manifest-adjacent executable files") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        auto pluginDir =
            createMockExternalPlugin(svc->getResolvedDataDir(), "dispatcher_external_exec");
        auto executable = pluginDir / "plugin_runner";
        std::filesystem::copy_file(pluginDir / "plugin.py", executable,
                                   std::filesystem::copy_options::overwrite_existing);
        std::filesystem::permissions(executable,
                                     std::filesystem::perms::owner_read |
                                         std::filesystem::perms::owner_write |
                                         std::filesystem::perms::owner_exec,
                                     std::filesystem::perm_options::replace);

        auto* external = svc->getExternalPluginHost();
        REQUIRE(external != nullptr);
        REQUIRE(external->trustAdd(executable).has_value());

        PluginLoadRequest req;
        req.pathOrName = executable.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<PluginLoadResponse>(resp));
        const auto& load = std::get<PluginLoadResponse>(resp);
        CHECK(load.loaded);
        CHECK(load.record.name == "dispatcher_external_plugin");
        CHECK(load.record.path == executable.string());

        auto unloadResp =
            dispatchRequest(dispatcher, Request{PluginUnloadRequest{"dispatcher_external_plugin"}});
        REQUIRE(std::holds_alternative<SuccessResponse>(unloadResp));
    }

    SECTION("plugin load falls back after untrusted external file fails") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());

        auto pluginDir =
            createMockExternalPlugin(svc->getResolvedDataDir(), "dispatcher_external_untrusted");
        auto pluginFile = pluginDir / "plugin.py";

        PluginLoadRequest req;
        req.pathOrName = pluginFile.string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Plugin load failed for: " + pluginFile.string());
    }

    SECTION("plugin trust add loads regular files and reuses loaded path metadata") {
        auto [state, lifecycleFsm, svc] = makeReadyService();
        StubLifecycle lifecycle;
        RequestDispatcher dispatcher(&lifecycle, svc.get(), state.get());
        auto shimPath = makePythonShimPath();
        EnvGuard pythonGuard("PATH", shimPath.c_str());

        auto pluginDir =
            createMockExternalPlugin(svc->getResolvedDataDir(), "dispatcher_external_trust_file");
        auto pluginFile = pluginDir / "plugin.py";

        auto trustAddResp =
            dispatchRequest(dispatcher, Request{PluginTrustAddRequest{pluginFile.string()}});
        REQUIRE(std::holds_alternative<SuccessResponse>(trustAddResp));

        auto* external = svc->getExternalPluginHost();
        REQUIRE(external != nullptr);
        REQUIRE(waitForCondition([&] { return external->listLoaded().size() == 1; }));
        CHECK(external->listLoaded().front().path == pluginFile);

        auto secondTrustResp =
            dispatchRequest(dispatcher, Request{PluginTrustAddRequest{pluginDir.string()}});
        REQUIRE(std::holds_alternative<SuccessResponse>(secondTrustResp));
        CHECK(waitForCondition([&] { return external->listLoaded().size() == 1; }));

        auto unloadResp =
            dispatchRequest(dispatcher, Request{PluginUnloadRequest{"dispatcher_external_plugin"}});
        REQUIRE(std::holds_alternative<SuccessResponse>(unloadResp));
    }
}

TEST_CASE("RequestDispatcher: collection handlers report missing dependencies and batch defaults",
          "[daemon][collections][dispatcher]") {
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_collections_dispatcher_");

    StubLifecycle lifecycle;
    StateComponent state;
    state.readiness.contentStoreReady.store(true, std::memory_order_relaxed);
    state.readiness.metadataRepoReady.store(true, std::memory_order_relaxed);
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&lifecycle, &svc, &state);

    struct SeededDocument {
        int64_t id{0};
        std::string hash;
        std::string relativePath;
    };

    auto makeReadyService = [&](StateComponent& readyState, DaemonLifecycleFsm& readyLifecycleFsm,
                                const std::string& prefix) {
        EnvGuard disableVectors("YAMS_DISABLE_VECTORS", "1");
        EnvGuard disableVectorDb("YAMS_DISABLE_VECTOR_DB", "1");
        EnvGuard skipModelLoading("YAMS_SKIP_MODEL_LOADING", "1");
        EnvGuard safeSingleInstance("YAMS_TEST_SAFE_SINGLE_INSTANCE", "1");

        DaemonConfig readyCfg = cfg;
        readyCfg.dataDir = makeTempDir(prefix);

        auto readySvc = std::make_shared<ServiceManager>(readyCfg, readyState, readyLifecycleFsm);
        auto initResult = readySvc->initialize();
        REQUIRE(initResult.has_value());

        readySvc->startAsyncInit();
        auto serviceSnap = readySvc->waitForServiceManagerTerminalState(30);
        REQUIRE(serviceSnap.state == ServiceManagerState::Ready);
        REQUIRE(readySvc->getMetadataRepo() != nullptr);
        REQUIRE(readySvc->getContentStore() != nullptr);

        return readySvc;
    };

    auto seedDocument = [&](const std::shared_ptr<ServiceManager>& readySvc,
                            const std::string& relativePath, const std::string& text,
                            const std::string& collectionName, const std::string& snapshotId) {
        auto meta = readySvc->getMetadataRepo();
        auto store = readySvc->getContentStore();
        REQUIRE(meta != nullptr);
        REQUIRE(store != nullptr);

        const auto textBytes = std::as_bytes(std::span<const char>(text.data(), text.size()));
        auto storeRes = store->storeBytes(textBytes);
        REQUIRE(storeRes.has_value());

        std::filesystem::path relativeDocPath(relativePath);
        std::string extension = relativeDocPath.extension().string();
        if (!extension.empty() && extension.front() == '.') {
            extension.erase(0, 1);
        }

        metadata::DocumentInfo doc{};
        doc.fileName = relativeDocPath.filename().string();
        doc.filePath = relativePath;
        doc.fileExtension = extension;
        doc.fileSize = static_cast<int64_t>(text.size());
        doc.sha256Hash = storeRes.value().contentHash;
        doc.mimeType = "text/plain";
        const auto now =
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.createdTime = now;
        doc.modifiedTime = now;
        doc.indexedTime = now;

        auto idRes = meta->insertDocument(doc);
        REQUIRE(idRes.has_value());
        REQUIRE(
            meta->setMetadata(idRes.value(), "collection", metadata::MetadataValue(collectionName))
                .has_value());
        REQUIRE(meta->setMetadata(idRes.value(), "snapshot_id", metadata::MetadataValue(snapshotId))
                    .has_value());

        return SeededDocument{idRes.value(), storeRes.value().contentHash, relativePath};
    };

    SECTION("list snapshots returns internal error when metadata repo is unavailable") {
        auto resp = dispatchRequest(dispatcher, Request{ListSnapshotsRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
    }

    SECTION("list snapshots forwards repository errors") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        repo->setSnapshotError(Error{ErrorCode::DatabaseError, "snapshot lookup failed"});
        svc.__test_setMetadataRepo(repo);

        auto resp = dispatchRequest(dispatcher, Request{ListSnapshotsRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::DatabaseError);
        CHECK(err.message == "snapshot lookup failed");
    }

    SECTION("restore collection returns internal error before dependencies are initialized") {
        RestoreCollectionRequest req;
        req.collection = "dispatcher-collection";
        req.outputDirectory = "out";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
    }

    SECTION("restore collection returns internal error when content store is unavailable") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        svc.__test_setMetadataRepo(repo);

        RestoreCollectionRequest req;
        req.collection = "dispatcher-collection";
        req.outputDirectory = "out";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Content store unavailable") != std::string::npos);
    }

    SECTION("restore collection forwards repository query errors") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        repo->setQueryError(Error{ErrorCode::DatabaseError, "collection query failed"});
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());

        RestoreCollectionRequest req;
        req.collection = "dispatcher-collection";
        req.outputDirectory = "out";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::DatabaseError);
        CHECK(err.message == "Failed to find collection documents: collection query failed");
    }

    SECTION("restore snapshot returns internal error when metadata repo is unavailable") {
        RestoreSnapshotRequest req;
        req.snapshotId = "dispatcher-snapshot";
        req.outputDirectory = "out";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
    }

    SECTION("restore snapshot returns internal error when content store is unavailable") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        svc.__test_setMetadataRepo(repo);

        RestoreSnapshotRequest req;
        req.snapshotId = "dispatcher-snapshot";
        req.outputDirectory = "out";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Content store unavailable") != std::string::npos);
    }

    SECTION("restore snapshot forwards repository query errors") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        repo->setSnapshotDocumentsError(Error{ErrorCode::DatabaseError, "snapshot query failed"});
        svc.__test_setMetadataRepo(repo);
        svc.__test_setContentStore(std::make_shared<StubContentStore>());

        RestoreSnapshotRequest req;
        req.snapshotId = "dispatcher-snapshot";
        req.outputDirectory = "out";

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::DatabaseError);
        CHECK(err.message == "Failed to find snapshot documents: snapshot query failed");
    }

    SECTION("metadata value counts returns internal error when metadata repo is unavailable") {
        MetadataValueCountsRequest req;
        req.keys = {"collection"};

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
    }

    SECTION("metadata value counts forwards repository errors") {
        auto repo = std::make_shared<StubPruneMetadataRepository>();
        repo->setMetadataValueCountsError(Error{ErrorCode::DatabaseError, "counts failed"});
        svc.__test_setMetadataRepo(repo);

        MetadataValueCountsRequest req;
        req.keys = {"collection"};

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::DatabaseError);
        CHECK(err.message == "counts failed");
    }

    SECTION("batch request returns not implemented items for each sequence") {
        BatchRequest req;
        req.items.push_back(BatchItem{.sequenceId = 7, .request = SearchRequest{.query = "one"}});
        req.items.push_back(BatchItem{.sequenceId = 8, .request = ListRequest{}});

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<BatchResponse>(resp));
        const auto& batchResp = std::get<BatchResponse>(resp);
        CHECK(batchResp.totalCount == 2);
        CHECK(batchResp.successCount == 0);
        CHECK(batchResp.errorCount == 2);
        REQUIRE(batchResp.items.size() == 2);
        CHECK(batchResp.items[0].sequenceId == 7);
        CHECK_FALSE(batchResp.items[0].success);
        REQUIRE(std::holds_alternative<ErrorResponse>(batchResp.items[0].response));
        CHECK(std::get<ErrorResponse>(batchResp.items[0].response).code ==
              ErrorCode::NotImplemented);
        CHECK(batchResp.items[1].sequenceId == 8);
        CHECK_FALSE(batchResp.items[1].success);
    }

    SECTION("restore collection exercises regex, layout, and io branches") {
        StubLifecycle readyLifecycle;
        StateComponent readyState;
        DaemonLifecycleFsm readyLifecycleFsm;
        auto readySvc =
            makeReadyService(readyState, readyLifecycleFsm, "yams_collections_live_dispatcher_");
        RequestDispatcher readyDispatcher(&readyLifecycle, readySvc.get(), &readyState);

        const std::string collectionName = "dispatcher-live-collection";
        const std::string snapshotId = "dispatcher-live-snapshot";
        const auto seeded = seedDocument(readySvc, "nested/alpha.txt", "alpha collection text\n",
                                         collectionName, snapshotId);

        auto includeOut = makeTempDir("yams_collection_include_");
        RestoreCollectionRequest includeReq;
        includeReq.collection = collectionName;
        includeReq.outputDirectory = includeOut.string();
        includeReq.includePatterns = {"*alpha.txt"};
        includeReq.dryRun = true;

        auto includeResp = dispatchRequest(readyDispatcher, Request{includeReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(includeResp));
        const auto& included = std::get<RestoreCollectionResponse>(includeResp);
        REQUIRE(included.files.size() == 1);
        CHECK(included.filesRestored == 1);
        CHECK(included.files.front().path == (includeOut / seeded.relativePath).string());

        RestoreCollectionRequest invalidPatternReq;
        invalidPatternReq.collection = collectionName;
        invalidPatternReq.outputDirectory = includeOut.string();
        invalidPatternReq.includePatterns = {"["};
        invalidPatternReq.dryRun = true;

        auto invalidPatternResp = dispatchRequest(readyDispatcher, Request{invalidPatternReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(invalidPatternResp));
        const auto& invalidPattern = std::get<RestoreCollectionResponse>(invalidPatternResp);
        CHECK(invalidPattern.filesRestored == 0);
        CHECK(invalidPattern.files.empty());

        RestoreCollectionRequest excludeReq;
        excludeReq.collection = collectionName;
        excludeReq.outputDirectory = includeOut.string();
        excludeReq.excludePatterns = {"*alpha.txt"};
        excludeReq.dryRun = true;

        auto excludeResp = dispatchRequest(readyDispatcher, Request{excludeReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(excludeResp));
        const auto& excluded = std::get<RestoreCollectionResponse>(excludeResp);
        CHECK(excluded.filesRestored == 0);
        CHECK(excluded.files.empty());

        auto writeOut = makeTempDir("yams_collection_write_");
        RestoreCollectionRequest writeReq;
        writeReq.collection = collectionName;
        writeReq.outputDirectory = writeOut.string();
        writeReq.layoutTemplate = "deep/{hash}{ext}";
        writeReq.overwrite = true;
        writeReq.dryRun = false;

        auto writeResp = dispatchRequest(readyDispatcher, Request{writeReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(writeResp));
        const auto& written = std::get<RestoreCollectionResponse>(writeResp);
        REQUIRE(written.files.size() == 1);
        CHECK(written.filesRestored == 1);
        const auto writtenPath = writeOut / "deep" / (seeded.hash + ".txt");
        CHECK(written.files.front().path == writtenPath.string());
        REQUIRE(std::filesystem::exists(writtenPath));
        std::ifstream writtenFile(writtenPath, std::ios::binary);
        REQUIRE(writtenFile.good());
        std::string writtenText((std::istreambuf_iterator<char>(writtenFile)),
                                std::istreambuf_iterator<char>());
        CHECK(writtenText == "alpha collection text\n");

        auto createDirBlocker = makeTempDir("yams_collection_create_dir_blocker_") / "blocked";
        {
            std::ofstream blocker(createDirBlocker);
            REQUIRE(blocker.good());
            blocker << "blocker";
        }
        RestoreCollectionRequest createDirFailReq;
        createDirFailReq.collection = collectionName;
        createDirFailReq.outputDirectory = createDirBlocker.string();
        createDirFailReq.createDirs = true;
        createDirFailReq.overwrite = true;
        createDirFailReq.dryRun = false;

        auto createDirFailResp = dispatchRequest(readyDispatcher, Request{createDirFailReq});

        REQUIRE(std::holds_alternative<ErrorResponse>(createDirFailResp));
        CHECK(std::get<ErrorResponse>(createDirFailResp).code == ErrorCode::IOError);
        CHECK(std::get<ErrorResponse>(createDirFailResp)
                  .message.find("Failed to create output directory") != std::string::npos);

        auto parentFailOut = makeTempDir("yams_collection_parent_fail_");
        auto occupiedParent = parentFailOut / "occupied";
        {
            std::ofstream blocker(occupiedParent);
            REQUIRE(blocker.good());
            blocker << "occupied";
        }
        RestoreCollectionRequest parentFailReq;
        parentFailReq.collection = collectionName;
        parentFailReq.outputDirectory = parentFailOut.string();
        parentFailReq.layoutTemplate = "occupied/restored.txt";
        parentFailReq.overwrite = true;
        parentFailReq.dryRun = false;

        auto parentFailResp = dispatchRequest(readyDispatcher, Request{parentFailReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(parentFailResp));
        const auto& parentFailed = std::get<RestoreCollectionResponse>(parentFailResp);
        REQUIRE(parentFailed.files.size() == 1);
        CHECK(parentFailed.filesRestored == 0);
        CHECK(parentFailed.files.front().skipped);
        CHECK(parentFailed.files.front().skipReason.find("Failed to create parent directory") !=
              std::string::npos);

        auto openFailOut = makeTempDir("yams_collection_open_fail_");
        RestoreCollectionRequest openFailReq;
        openFailReq.collection = collectionName;
        openFailReq.outputDirectory = openFailOut.string();
        openFailReq.layoutTemplate = ".";
        openFailReq.overwrite = true;
        openFailReq.dryRun = false;

        auto openFailResp = dispatchRequest(readyDispatcher, Request{openFailReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(openFailResp));
        const auto& openFailed = std::get<RestoreCollectionResponse>(openFailResp);
        REQUIRE(openFailed.files.size() == 1);
        CHECK(openFailed.filesRestored == 0);
        CHECK(openFailed.files.front().skipped);
        CHECK(openFailed.files.front().skipReason == "Failed to open output file");

        auto writeFailOut = makeTempDir("yams_collection_write_fail_");
        RestoreCollectionRequest writeFailReq;
        writeFailReq.collection = collectionName;
        writeFailReq.outputDirectory = writeFailOut.string();
        writeFailReq.layoutTemplate = "write-fail/{name}{ext}";
        writeFailReq.overwrite = true;
        writeFailReq.dryRun = false;

        RequestDispatcher::__test_forceRestoreWriteFailureOnce();
        auto writeFailResp = dispatchRequest(readyDispatcher, Request{writeFailReq});

        REQUIRE(std::holds_alternative<RestoreCollectionResponse>(writeFailResp));
        const auto& writeFailed = std::get<RestoreCollectionResponse>(writeFailResp);
        REQUIRE(writeFailed.files.size() == 1);
        CHECK(writeFailed.filesRestored == 0);
        CHECK(writeFailed.files.front().skipped);
        CHECK(writeFailed.files.front().skipReason == "Failed to write file content");

        readySvc->shutdown();
    }

    SECTION("restore snapshot exercises regex, layout, and io branches") {
        StubLifecycle readyLifecycle;
        StateComponent readyState;
        DaemonLifecycleFsm readyLifecycleFsm;
        auto readySvc =
            makeReadyService(readyState, readyLifecycleFsm, "yams_snapshots_live_dispatcher_");
        RequestDispatcher readyDispatcher(&readyLifecycle, readySvc.get(), &readyState);

        const std::string collectionName = "dispatcher-live-collection";
        const std::string snapshotId = "dispatcher-live-snapshot";
        const auto seeded = seedDocument(readySvc, "nested/alpha.txt", "alpha snapshot text\n",
                                         collectionName, snapshotId);

        auto defaultOut = makeTempDir("yams_snapshot_default_");
        RestoreSnapshotRequest defaultReq;
        defaultReq.snapshotId = snapshotId;
        defaultReq.outputDirectory = defaultOut.string();
        defaultReq.includePatterns = {"*alpha.txt"};
        defaultReq.dryRun = true;

        auto defaultResp = dispatchRequest(readyDispatcher, Request{defaultReq});

        REQUIRE(std::holds_alternative<RestoreSnapshotResponse>(defaultResp));
        const auto& defaultRestore = std::get<RestoreSnapshotResponse>(defaultResp);
        REQUIRE(defaultRestore.files.size() == 1);
        CHECK(defaultRestore.filesRestored == 1);
        CHECK(defaultRestore.files.front().path == (defaultOut / seeded.relativePath).string());

        RestoreSnapshotRequest invalidPatternReq;
        invalidPatternReq.snapshotId = snapshotId;
        invalidPatternReq.outputDirectory = defaultOut.string();
        invalidPatternReq.includePatterns = {"["};
        invalidPatternReq.dryRun = true;

        auto invalidPatternResp = dispatchRequest(readyDispatcher, Request{invalidPatternReq});

        REQUIRE(std::holds_alternative<RestoreSnapshotResponse>(invalidPatternResp));
        const auto& invalidPattern = std::get<RestoreSnapshotResponse>(invalidPatternResp);
        CHECK(invalidPattern.filesRestored == 0);
        CHECK(invalidPattern.files.empty());

        auto writeOut = makeTempDir("yams_snapshot_write_");
        RestoreSnapshotRequest writeReq;
        writeReq.snapshotId = snapshotId;
        writeReq.outputDirectory = writeOut.string();
        writeReq.layoutTemplate = "deep/{name}{ext}";
        writeReq.overwrite = true;
        writeReq.dryRun = false;

        auto writeResp = dispatchRequest(readyDispatcher, Request{writeReq});

        REQUIRE(std::holds_alternative<RestoreSnapshotResponse>(writeResp));
        const auto& written = std::get<RestoreSnapshotResponse>(writeResp);
        REQUIRE(written.files.size() == 1);
        CHECK(written.filesRestored == 1);
        const auto writtenPath = writeOut / "deep" / "alpha.txt";
        CHECK(written.files.front().path == writtenPath.string());
        REQUIRE(std::filesystem::exists(writtenPath));
        std::ifstream writtenFile(writtenPath, std::ios::binary);
        REQUIRE(writtenFile.good());
        std::string writtenText((std::istreambuf_iterator<char>(writtenFile)),
                                std::istreambuf_iterator<char>());
        CHECK(writtenText == "alpha snapshot text\n");

        auto createDirBlocker = makeTempDir("yams_snapshot_create_dir_blocker_") / "blocked";
        {
            std::ofstream blocker(createDirBlocker);
            REQUIRE(blocker.good());
            blocker << "blocker";
        }
        RestoreSnapshotRequest createDirFailReq;
        createDirFailReq.snapshotId = snapshotId;
        createDirFailReq.outputDirectory = createDirBlocker.string();
        createDirFailReq.createDirs = true;
        createDirFailReq.overwrite = true;
        createDirFailReq.dryRun = false;

        auto createDirFailResp = dispatchRequest(readyDispatcher, Request{createDirFailReq});

        REQUIRE(std::holds_alternative<ErrorResponse>(createDirFailResp));
        CHECK(std::get<ErrorResponse>(createDirFailResp).code == ErrorCode::IOError);
        CHECK(std::get<ErrorResponse>(createDirFailResp)
                  .message.find("Failed to create output directory") != std::string::npos);

        auto parentFailOut = makeTempDir("yams_snapshot_parent_fail_");
        auto occupiedParent = parentFailOut / "occupied";
        {
            std::ofstream blocker(occupiedParent);
            REQUIRE(blocker.good());
            blocker << "occupied";
        }
        RestoreSnapshotRequest parentFailReq;
        parentFailReq.snapshotId = snapshotId;
        parentFailReq.outputDirectory = parentFailOut.string();
        parentFailReq.layoutTemplate = "occupied/restored.txt";
        parentFailReq.overwrite = true;
        parentFailReq.dryRun = false;

        auto parentFailResp = dispatchRequest(readyDispatcher, Request{parentFailReq});

        REQUIRE(std::holds_alternative<RestoreSnapshotResponse>(parentFailResp));
        const auto& parentFailed = std::get<RestoreSnapshotResponse>(parentFailResp);
        REQUIRE(parentFailed.files.size() == 1);
        CHECK(parentFailed.filesRestored == 0);
        CHECK(parentFailed.files.front().skipped);
        CHECK(parentFailed.files.front().skipReason.find("Failed to create parent directory") !=
              std::string::npos);

        auto openFailOut = makeTempDir("yams_snapshot_open_fail_");
        RestoreSnapshotRequest openFailReq;
        openFailReq.snapshotId = snapshotId;
        openFailReq.outputDirectory = openFailOut.string();
        openFailReq.layoutTemplate = ".";
        openFailReq.overwrite = true;
        openFailReq.dryRun = false;

        auto openFailResp = dispatchRequest(readyDispatcher, Request{openFailReq});

        REQUIRE(std::holds_alternative<RestoreSnapshotResponse>(openFailResp));
        const auto& openFailed = std::get<RestoreSnapshotResponse>(openFailResp);
        REQUIRE(openFailed.files.size() == 1);
        CHECK(openFailed.filesRestored == 0);
        CHECK(openFailed.files.front().skipped);
        CHECK(openFailed.files.front().skipReason == "Failed to open output file");

        auto writeFailOut = makeTempDir("yams_snapshot_write_fail_");
        RestoreSnapshotRequest writeFailReq;
        writeFailReq.snapshotId = snapshotId;
        writeFailReq.outputDirectory = writeFailOut.string();
        writeFailReq.layoutTemplate = "write-fail/{hash}{ext}";
        writeFailReq.overwrite = true;
        writeFailReq.dryRun = false;

        RequestDispatcher::__test_forceRestoreWriteFailureOnce();
        auto writeFailResp = dispatchRequest(readyDispatcher, Request{writeFailReq});

        REQUIRE(std::holds_alternative<RestoreSnapshotResponse>(writeFailResp));
        const auto& writeFailed = std::get<RestoreSnapshotResponse>(writeFailResp);
        REQUIRE(writeFailed.files.size() == 1);
        CHECK(writeFailed.filesRestored == 0);
        CHECK(writeFailed.files.front().skipped);
        CHECK(writeFailed.files.front().skipReason == "Failed to write file content");

        readySvc->shutdown();
    }
}

TEST_CASE("DaemonMetrics: snapshot includes canonical readiness flags",
          "[daemon][metrics][readiness]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_metrics_readiness_flags_");
    ServiceManager svc(cfg, state, lifecycleFsm);
    DaemonMetrics metrics(nullptr, &state, &svc, svc.getWorkCoordinator());

    auto snap = metrics.getSnapshot();
    REQUIRE(snap != nullptr);

    const std::array<std::string_view, 12> requiredCoreReadinessKeys = {
        readiness::kIpcServer,     readiness::kContentStore,
        readiness::kDatabase,      readiness::kMetadataRepo,
        readiness::kSearchEngine,  readiness::kModelProvider,
        readiness::kVectorIndex,   readiness::kVectorDb,
        readiness::kPlugins,       readiness::kVectorDbInitAttempted,
        readiness::kVectorDbReady, readiness::kVectorDbDim};

    for (const auto key : requiredCoreReadinessKeys) {
        INFO("missing readiness key: " << key);
        REQUIRE(snap->readinessStates.count(std::string(key)) > 0);
    }
}

TEST_CASE("DaemonMetrics: snapshot reports generated build version", "[daemon][metrics][version]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_metrics_version_");
    ServiceManager svc(cfg, state, lifecycleFsm);
    DaemonMetrics metrics(nullptr, &state, &svc, svc.getWorkCoordinator());

    auto snap = metrics.getSnapshot();
    REQUIRE(snap != nullptr);
    CHECK(snap->version == yams::version::string_v);
}

TEST_CASE("StatusResponse: post_ingest_rpc requestCounts keys round-trip",
          "[daemon][status][protocol][post_ingest]") {
    StatusResponse s{};
    s.requestCounts["post_ingest_rpc_queued"] = 3;
    s.requestCounts["post_ingest_rpc_capacity"] = 64;
    s.requestCounts["post_ingest_rpc_max_per_batch"] = 8;

    Message m{};
    m.payload = Response{std::in_place_type<StatusResponse>, s};

    auto enc = ProtoSerializer::encode_payload(m);
    REQUIRE(enc.has_value());

    auto dec = ProtoSerializer::decode_payload(enc.value());
    REQUIRE(dec.has_value());

    const auto& resp = std::get<Response>(dec.value().payload);
    REQUIRE(std::holds_alternative<StatusResponse>(resp));

    const auto& decoded = std::get<StatusResponse>(resp);
    REQUIRE(decoded.requestCounts.at("post_ingest_rpc_queued") == 3);
    REQUIRE(decoded.requestCounts.at("post_ingest_rpc_capacity") == 64);
    REQUIRE(decoded.requestCounts.at("post_ingest_rpc_max_per_batch") == 8);
}

TEST_CASE("StatusResponse: backpressure requestCounts keys round-trip",
          "[daemon][status][protocol][post_ingest]") {
    StatusResponse s{};
    s.requestCounts["post_ingest_backpressure_rejects"] = 123;
    s.requestCounts["kg_jobs_depth"] = 3880;
    s.requestCounts["kg_jobs_capacity"] = 4096;
    s.requestCounts["kg_jobs_fill_pct"] = 95;

    Message m{};
    m.payload = Response{std::in_place_type<StatusResponse>, s};

    auto enc = ProtoSerializer::encode_payload(m);
    REQUIRE(enc.has_value());

    auto dec = ProtoSerializer::decode_payload(enc.value());
    REQUIRE(dec.has_value());

    const auto& resp = std::get<Response>(dec.value().payload);
    REQUIRE(std::holds_alternative<StatusResponse>(resp));

    const auto& decoded = std::get<StatusResponse>(resp);
    REQUIRE(decoded.requestCounts.at("post_ingest_backpressure_rejects") == 123);
    REQUIRE(decoded.requestCounts.at("kg_jobs_depth") == 3880);
    REQUIRE(decoded.requestCounts.at("kg_jobs_capacity") == 4096);
    REQUIRE(decoded.requestCounts.at("kg_jobs_fill_pct") == 95);
}

// =============================================================================
// StatusResponse Protocol Serialization Tests
// =============================================================================

TEST_CASE("StatusResponse: Protocol serialization", "[daemon][status][protocol]") {
    SECTION("Round-trip with lifecycle state and error") {
        StatusResponse s{};
        s.running = true;
        s.ready = false;
        s.uptimeSeconds = 42;
        s.requestsProcessed = 7;
        s.activeConnections = 1;
        s.memoryUsageMb = 12.5;
        s.cpuUsagePercent = 3.2;
        s.version = "test";
        s.overallStatus = "failed";
        s.lifecycleState = "starting";
        s.lastError = "boom";
        s.dataDir = "/tmp/daemon-data-dir";
        s.metadataDbPath = "/tmp/daemon-data-dir/yams.db";
        s.vectorDbPath = "/tmp/daemon-data-dir/vectors.db";
        s.requestCounts["worker_threads"] = 4;

        // FSM-exported fields
        s.requestCounts["service_fsm_state"] = 3;
        s.requestCounts["embedding_state"] = 2;
        s.requestCounts["plugin_host_state"] = 1;

        // Readiness states
        s.readinessStates["embedding_ready"] = false;
        s.readinessStates["plugins_ready"] = true;
        s.readinessStates["plugins_degraded"] = true;

        Message m{};
        m.payload = Response{std::in_place_type<StatusResponse>, s};

        // Encode
        auto enc = ProtoSerializer::encode_payload(m);
        REQUIRE(enc.has_value());

        // Decode
        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec.has_value());

        const Message& out = dec.value();
        REQUIRE(std::holds_alternative<Response>(out.payload));

        const auto& resp = std::get<Response>(out.payload);
        REQUIRE(std::holds_alternative<StatusResponse>(resp));

        const auto& decoded = std::get<StatusResponse>(resp);

        // Verify key fields survived round-trip
        REQUIRE(decoded.running == true);
        REQUIRE(decoded.ready == false);
        REQUIRE(decoded.uptimeSeconds == 42);
        REQUIRE(decoded.version == "test");
        REQUIRE(decoded.overallStatus == "failed");
        REQUIRE(decoded.lastError == "boom");
        REQUIRE(decoded.dataDir == "/tmp/daemon-data-dir");
        REQUIRE(decoded.metadataDbPath == "/tmp/daemon-data-dir/yams.db");
        REQUIRE(decoded.vectorDbPath == "/tmp/daemon-data-dir/vectors.db");

        // Verify FSM state counts
        REQUIRE(decoded.requestCounts.at("worker_threads") == 4);
        REQUIRE(decoded.requestCounts.at("service_fsm_state") == 3);

        // Verify readiness flags
        REQUIRE(decoded.readinessStates.at("embedding_ready") == false);
        REQUIRE(decoded.readinessStates.at("plugins_ready") == true);
        REQUIRE(decoded.readinessStates.at("plugins_degraded") == true);
    }

    SECTION("Empty status response serializes correctly") {
        StatusResponse s{};
        Message m{};
        m.payload = Response{std::in_place_type<StatusResponse>, s};

        auto enc = ProtoSerializer::encode_payload(m);
        REQUIRE(enc.has_value());

        auto dec = ProtoSerializer::decode_payload(enc.value());
        REQUIRE(dec.has_value());

        const auto& resp = std::get<Response>(dec.value().payload);
        REQUIRE(std::holds_alternative<StatusResponse>(resp));
    }
}

// =============================================================================
// Plugin Degradation Tests
// =============================================================================
// Note: Full integration tests (DISABLED in original) are skipped here
// These would test daemon start with trust failures and recovery hooks
// They belong in integration tests due to full daemon lifecycle requirements

TEST_CASE("ServiceManager: Plugin degradation tracking", "[daemon][status][plugins]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_plugin_degrade_");

    SECTION("Plugin degradation flag is tracked in FSM") {
        // This is a unit test of the FSM tracking mechanism
        // Full daemon integration tests are separate

        ServiceManager svc(cfg, state, lifecycleFsm);

        // ServiceManager should track plugin states via FSM
        // Actual degradation testing requires daemon start, which is integration level

        // Basic verification: ServiceManager exists and has FSM snapshot capability
        auto snapshot = svc.getServiceManagerFsmSnapshot();
        // ServiceManager starts uninitialized until explicitly initialized
        REQUIRE(snapshot.state == ServiceManagerState::Uninitialized);
    }
}

// =============================================================================
// FSM State Metrics
// =============================================================================

TEST_CASE("DaemonMetrics: FSM state export", "[daemon][metrics][fsm]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_fsm_metrics_");
    ServiceManager svc(cfg, state, lifecycleFsm);

    SECTION("Lifecycle FSM state is tracked") {
        lifecycleFsm.dispatch(BootstrappedEvent{});

        auto snapshot = lifecycleFsm.snapshot();
        REQUIRE(snapshot.state != LifecycleState::Unknown);
    }

    SECTION("Service manager FSM state is accessible") {
        auto snapshot = svc.getServiceManagerFsmSnapshot();
        // ServiceManager starts in Uninitialized state until initialized
        REQUIRE(snapshot.state == ServiceManagerState::Uninitialized);
    }

    SECTION("Embedding provider FSM state is accessible") {
        auto snapshot = svc.getEmbeddingProviderFsmSnapshot();
        // Embedding provider starts in Unavailable state until plugin loaded
        REQUIRE(snapshot.state == EmbeddingProviderState::Unavailable);
    }

    SECTION("Plugin host FSM state is accessible") {
        auto snapshot = svc.getPluginHostFsmSnapshot();
        // When ABI plugins are disabled, PluginManager initializes successfully with
        // an empty plugin list, transitioning to Ready state. When plugins are enabled
        // but PluginManager hasn't been created yet, the fallback is NotInitialized.
        REQUIRE((snapshot.state == PluginHostState::NotInitialized ||
                 snapshot.state == PluginHostState::Ready));
    }
}

// =============================================================================
// DatabaseManager Metrics Tests
// =============================================================================

TEST_CASE("DatabaseManager: Metrics tracking", "[daemon][metrics][database]") {
    StateComponent state;
    DatabaseManager::Dependencies deps{.state = &state};
    DatabaseManager dbm(deps);

    SECTION("Stats start at zero") {
        const auto& stats = dbm.getStats();
        REQUIRE(stats.openDurationMs.load() == 0);
        REQUIRE(stats.migrationDurationMs.load() == 0);
        REQUIRE(stats.openErrors.load() == 0);
        REQUIRE(stats.migrationErrors.load() == 0);
        REQUIRE(stats.repositoryInitErrors.load() == 0);
    }

    SECTION("Stats are returned by reference") {
        // Verify we can access stats multiple times
        const auto& stats1 = dbm.getStats();
        const auto& stats2 = dbm.getStats();
        REQUIRE(stats1.openErrors.load() == stats2.openErrors.load());
    }
}

TEST_CASE("DaemonMetrics: DatabaseManager metrics export", "[daemon][metrics][database]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_db_metrics_");
    ServiceManager svc(cfg, state, lifecycleFsm);

    SECTION("DatabaseManager metrics are included in snapshot") {
        DaemonMetrics metrics(nullptr, &state, &svc, svc.getWorkCoordinator());
        auto snap = metrics.getSnapshot();

        REQUIRE(snap != nullptr);
        // DatabaseManager metrics should be present (may be 0 if not initialized)
        REQUIRE(snap->dbOpenErrors == 0); // Default value when no errors
    }
}

// =============================================================================
// WorkCoordinator Metrics Tests
// =============================================================================

TEST_CASE("WorkCoordinator: Metrics tracking", "[daemon][metrics][work]") {
    WorkCoordinator wc;

    SECTION("Stats before start") {
        auto stats = wc.getStats();
        REQUIRE(stats.workerCount == 0);
        REQUIRE(stats.activeWorkers == 0);
        REQUIRE(stats.isRunning == false);
    }

    SECTION("Stats accessors don't crash") {
        REQUIRE(wc.getWorkerCount() == 0);
        REQUIRE(wc.getActiveWorkerCount() == 0);
        REQUIRE(wc.isRunning() == false);
    }
}

TEST_CASE("DaemonMetrics: WorkCoordinator metrics export", "[daemon][metrics][work]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_wc_metrics_");
    ServiceManager svc(cfg, state, lifecycleFsm);

    SECTION("WorkCoordinator metrics are included in snapshot") {
        DaemonMetrics metrics(nullptr, &state, &svc, svc.getWorkCoordinator());
        auto snap = metrics.getSnapshot();

        REQUIRE(snap != nullptr);
        // WorkCoordinator metrics should be present
        // ServiceManager creates WorkCoordinator with hardware_concurrency() workers
        REQUIRE(snap->workerThreads > 0);              // Should have workers (worker_threads)
        REQUIRE(snap->workCoordinatorRunning == true); // Should be running
    }
}

// =============================================================================
// Search Load Metrics Tests
// =============================================================================

TEST_CASE("ServiceManager: Search load metrics", "[daemon][metrics][search]") {
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    DaemonConfig cfg;
    cfg.dataDir = makeTempDir("yams_search_metrics_");
    ServiceManager svc(cfg, state, lifecycleFsm);

    SECTION("Search load metrics return valid structure") {
        auto metrics = svc.getSearchLoadMetrics();
        REQUIRE(metrics.active == 0);
        REQUIRE(metrics.queued == 0);
        REQUIRE(metrics.concurrencyLimit > 0);
    }

    SECTION("Search lifecycle counters update load metrics") {
        svc.onSearchRequestQueued();
        auto queued = svc.getSearchLoadMetrics();
        REQUIRE(queued.queued == 1);
        REQUIRE(queued.active == 0);

        const auto cap = queued.concurrencyLimit;
        REQUIRE(svc.tryStartSearchRequest(cap));

        auto started = svc.getSearchLoadMetrics();
        REQUIRE(started.queued == 0);
        REQUIRE(started.active == 1);

        svc.onSearchRequestFinished();
        auto finished = svc.getSearchLoadMetrics();
        REQUIRE(finished.active == 0);
        REQUIRE(finished.queued == 0);
    }

    SECTION("Failed search start keeps queued count until rejection") {
        svc.onSearchRequestQueued();
        const auto cap = svc.getSearchLoadMetrics().concurrencyLimit;
        REQUIRE(svc.tryStartSearchRequest(cap));

        svc.onSearchRequestQueued();
        REQUIRE_FALSE(svc.tryStartSearchRequest(cap));

        auto waiting = svc.getSearchLoadMetrics();
        REQUIRE(waiting.active == 1);
        REQUIRE(waiting.queued == 1);

        svc.onSearchRequestRejected();
        auto rejected = svc.getSearchLoadMetrics();
        REQUIRE(rejected.active == 1);
        REQUIRE(rejected.queued == 0);

        svc.onSearchRequestFinished();
        auto finished = svc.getSearchLoadMetrics();
        REQUIRE(finished.active == 0);
        REQUIRE(finished.queued == 0);
    }
}

// =============================================================================
// FsmMetricsRegistry Integration Tests
// =============================================================================

TEST_CASE("FsmMetricsRegistry: Metrics collection", "[daemon][metrics][fsm]") {
    // Reset registry to known state
    FsmMetricsRegistry::instance().reset();

    SECTION("Registry starts with zeros") {
        auto snap = FsmMetricsRegistry::instance().snapshot();
        REQUIRE(snap.transitions == 0);
        REQUIRE(snap.headerReads == 0);
        REQUIRE(snap.payloadReads == 0);
        REQUIRE(snap.payloadWrites == 0);
        REQUIRE(snap.bytesSent == 0);
        REQUIRE(snap.bytesReceived == 0);
    }

    SECTION("Registry increments correctly") {
        FsmMetricsRegistry::instance().incrementTransitions(5);
        FsmMetricsRegistry::instance().incrementHeaderReads(3);
        FsmMetricsRegistry::instance().incrementPayloadReads(2);
        FsmMetricsRegistry::instance().addBytesSent(100);
        FsmMetricsRegistry::instance().addBytesReceived(200);

        auto snap = FsmMetricsRegistry::instance().snapshot();
        REQUIRE(snap.transitions == 5);
        REQUIRE(snap.headerReads == 3);
        REQUIRE(snap.payloadReads == 2);
        REQUIRE(snap.bytesSent == 100);
        REQUIRE(snap.bytesReceived == 200);
    }

    SECTION("Registry reset works") {
        FsmMetricsRegistry::instance().incrementTransitions(10);
        FsmMetricsRegistry::instance().reset();

        auto snap = FsmMetricsRegistry::instance().snapshot();
        REQUIRE(snap.transitions == 0);
    }
}
