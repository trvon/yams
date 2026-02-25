// Daemon metrics and status test suite (Catch2)
// Consolidates status/metrics tests: embedding status, plugin degradation, WAL metrics, FSM states
// Covers: DaemonMetrics, StatusResponse serialization, plugin degradation, WAL metrics

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <filesystem>
#include <random>

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
        loaded_.push_back(modelName);
        return Result<void>();
    }
    Result<void> unloadModel(const std::string&) override { return Result<void>(); }
    bool isModelLoaded(const std::string& modelName) const override {
        return std::find(loaded_.begin(), loaded_.end(), modelName) != loaded_.end();
    }
    std::vector<std::string> getLoadedModels() const override { return loaded_; }
    size_t getLoadedModelCount() const override { return loaded_.size(); }
    Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
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
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}

private:
    size_t dim_;
    std::string path_;
    bool available_;
    std::vector<std::string> loaded_;
};

std::filesystem::path makeTempDir(const std::string& prefix) {
    auto base = std::filesystem::temp_directory_path();
    auto dir = base / (prefix + std::to_string(std::random_device{}()));
    std::filesystem::create_directories(dir);
    return dir;
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
