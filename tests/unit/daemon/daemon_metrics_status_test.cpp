// Daemon metrics and status test suite (Catch2)
// Consolidates status/metrics tests: embedding status, plugin degradation, WAL metrics, FSM states
// Covers: DaemonMetrics, StatusResponse serialization, plugin degradation, WAL metrics

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>

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
#include <yams/daemon/resource/model_provider.h>

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

private:
    LifecycleSnapshot snapshot_{};
    std::string lastSubsystem_;
    bool lastDegraded_{false};
    std::string lastReason_;
    int setSubsystemDegradedCalls_{0};
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

std::filesystem::path makeTempDir(const std::string& prefix) {
    auto base = std::filesystem::temp_directory_path();
    auto dir = base / (prefix + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
}

Response dispatchRequest(RequestDispatcher& dispatcher, const Request& req) {
    boost::asio::io_context ioc;
    auto fut = boost::asio::co_spawn(ioc, dispatcher.dispatch(req), boost::asio::use_future);
    ioc.run();
    return fut.get();
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
        for (const auto& record : scan.plugins) {
            if (record.path == fakePlugin.string()) {
                foundDefaultPlugin = true;
                CHECK(record.name == "yams_default_scan");
            }
        }
        CHECK(foundDefaultPlugin);

        auto trustResp =
            dispatchRequest(dispatcher, Request{PluginTrustAddRequest{abiDir.string()}});
        REQUIRE(std::holds_alternative<SuccessResponse>(trustResp));

        PluginLoadRequest req;
        req.pathOrName = fakePlugin.filename().string();

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message == "Plugin load failed for: " + fakePlugin.string());
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

    SECTION("list snapshots returns internal error when metadata repo is unavailable") {
        auto resp = dispatchRequest(dispatcher, Request{ListSnapshotsRequest{}});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
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

    SECTION("metadata value counts returns internal error when metadata repo is unavailable") {
        MetadataValueCountsRequest req;
        req.keys = {"collection"};

        auto resp = dispatchRequest(dispatcher, Request{req});

        REQUIRE(std::holds_alternative<ErrorResponse>(resp));
        const auto& err = std::get<ErrorResponse>(resp);
        CHECK(err.code == ErrorCode::InternalError);
        CHECK(err.message.find("Metadata repository unavailable") != std::string::npos);
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
