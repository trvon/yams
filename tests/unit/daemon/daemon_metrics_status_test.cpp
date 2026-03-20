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

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/DatabaseManager.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/daemon_lifecycle.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/resource/external_plugin_host.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/plugin_host.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

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

    LifecycleSnapshot getLifecycleSnapshot() const override { return snapshot_; }
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
    void onDocumentRemoved(const std::string&) override {}
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

private:
    LifecycleSnapshot snapshot_{};
    std::string lastSubsystem_;
    bool lastDegraded_{false};
    std::string lastReason_;
    int setSubsystemDegradedCalls_{0};
    bool throwOnSetSubsystemDegraded_{false};
};

// Minimal stub provider for embedding tests
class StubModelProvider : public IModelProvider {
public:
    explicit StubModelProvider(size_t dim, std::string path)
        : dim_(dim), path_(std::move(path)), available_(true) {}

    Result<std::vector<float>> generateEmbedding(const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<float>> generateEmbeddingFor(const std::string&,
                                                    const std::string&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string&, const std::vector<std::string>&) override {
        return ErrorCode::NotImplemented;
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

private:
    size_t dim_;
    std::string path_;
    bool available_;
    std::vector<std::string> loaded_;
    std::string lastOptionsModel_;
    std::string lastOptionsJson_;
    std::optional<Error> loadError_;
    bool throwOnMemoryUsage_{false};
    bool failModelInfo_{false};
    mutable int throwOnIsModelLoadedCount_{0};
};

class EnvGuard {
public:
    EnvGuard(const char* name, const char* value) : name_(name) {
        if (const char* current = std::getenv(name_)) {
            original_ = current;
        }
        if (value) {
            ::setenv(name_, value, 1);
        } else {
            ::unsetenv(name_);
        }
    }

    ~EnvGuard() {
        if (original_) {
            ::setenv(name_, original_->c_str(), 1);
        } else {
            ::unsetenv(name_);
        }
    }

private:
    const char* name_;
    std::optional<std::string> original_;
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

    void addDocument(metadata::DocumentInfo doc) {
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
    std::vector<std::string> snapshots_;
    std::vector<metadata::DocumentInfo> snapshotDocs_;
    std::unordered_map<std::string, std::vector<metadata::MetadataValueCount>> metadataValueCounts_;
    std::size_t deleteCallCount_{0};
    std::vector<int64_t> deletedIds_;
};

class StubContentStore : public api::IContentStore {
public:
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

    Result<api::StoreResult> store(const std::filesystem::path&, const api::ContentMetadata&,
                                   api::ProgressCallback) override {
        return ErrorCode::NotImplemented;
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
    Result<bool> remove(const std::string&) override { return ErrorCode::NotImplemented; }
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
};

std::filesystem::path makeTempDir(const std::string& prefix) {
    auto base = std::filesystem::temp_directory_path();
    auto dir = base / (prefix + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
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
