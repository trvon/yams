// Daemon metrics and status test suite (Catch2)
// Consolidates status/metrics tests: embedding status, plugin degradation, WAL metrics, FSM states
// Covers: DaemonMetrics, StatusResponse serialization, plugin degradation tracking, WAL metrics

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <filesystem>
#include <random>

#include <yams/daemon/components/DaemonLifecycleFsm.h>
#include <yams/daemon/components/DaemonMetrics.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/StateComponent.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/ipc/proto_serializer.h>
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

        DaemonMetrics metrics(nullptr, &state, &svc);
        auto snap = metrics.getSnapshot();

        REQUIRE(snap != nullptr);

        // Backend should be one of known values
        bool validBackend =
            (snap->embeddingBackend == "provider" || snap->embeddingBackend == "local" ||
             snap->embeddingBackend == "unknown");
        REQUIRE(validBackend);
    }

    SECTION("Metrics snapshot doesn't crash without provider") {
        // No provider set
        DaemonMetrics metrics(nullptr, &state, &svc);
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
    StateComponent state;
    DaemonLifecycleFsm lifecycleFsm;
    ServiceManager svc(cfg, state, lifecycleFsm);
    RequestDispatcher dispatcher(&daemon, &svc, &state);

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
        REQUIRE(snapshot.state != PluginHostState::NotInitialized);
    }
}
