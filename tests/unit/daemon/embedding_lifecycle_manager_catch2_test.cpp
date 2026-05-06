#include <catch2/catch_test_macros.hpp>

#include <thread>
#include <yams/daemon/components/EmbeddingLifecycleManager.h>

using namespace yams::daemon;

static EmbeddingLifecycleManager::Dependencies nullDeps() {
    return {
        [] { return std::shared_ptr<IModelProvider>{}; },
        [] { return std::shared_ptr<EmbeddingService>{}; },
        nullptr,
        nullptr,
    };
}

TEST_CASE("EmbeddingLifecycleManager initial state", "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    CHECK(mgr.modelName().empty());
    CHECK(mgr.adoptedPluginName().empty());
    CHECK_FALSE(mgr.isAutoOnAdd());
    CHECK_FALSE(mgr.preloadOnStartup());
    CHECK(mgr.getEmbeddingDimension() == 0);
}

TEST_CASE("EmbeddingLifecycleManager FSM starts Unavailable",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    auto snap = mgr.fsmSnapshot();
    CHECK(snap.state == EmbeddingProviderState::Unavailable);
    CHECK_FALSE(mgr.isDegraded());
    CHECK_FALSE(mgr.isLoadingOrReady());
}

TEST_CASE("EmbeddingLifecycleManager FSM transitions", "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    mgr.fsm().dispatch(ProviderAdoptedEvent{"test-plugin"});
    CHECK(mgr.fsmSnapshot().state == EmbeddingProviderState::ProviderAdopted);

    mgr.fsm().dispatch(ModelLoadStartedEvent{"model-a"});
    CHECK(mgr.isLoadingOrReady());

    mgr.fsm().dispatch(ModelLoadedEvent{"model-a", 384});
    CHECK(mgr.isLoadingOrReady());
    auto snap = mgr.fsmSnapshot();
    CHECK(snap.state == EmbeddingProviderState::ModelReady);
    CHECK(snap.embeddingDimension == 384);
}

TEST_CASE("EmbeddingLifecycleManager isDegraded", "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    mgr.fsm().dispatch(ProviderDegradedEvent{"plugin crashed"});
    CHECK(mgr.isDegraded());

    mgr.fsm().dispatch(ProviderRecoveryEvent{});
    CHECK_FALSE(mgr.isDegraded());
}

TEST_CASE("EmbeddingLifecycleManager model name and plugin name setters",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    mgr.setModelName("bge-small");
    CHECK(mgr.modelName() == "bge-small");

    mgr.setAdoptedPluginName("onnx-provider");
    CHECK(mgr.adoptedPluginName() == "onnx-provider");
}

TEST_CASE("EmbeddingLifecycleManager auto-on-add and preload flags",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    mgr.setAutoOnAdd(true);
    CHECK(mgr.isAutoOnAdd());

    mgr.setPreloadOnStartup(true);
    CHECK(mgr.preloadOnStartup());
    mgr.setPreloadOnStartup(false);
    CHECK_FALSE(mgr.preloadOnStartup());
}

TEST_CASE("EmbeddingLifecycleManager metrics return 0 with null service",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    CHECK(mgr.inFlightJobs() == 0);
    CHECK(mgr.queuedJobs() == 0);
    CHECK(mgr.activeInferSubBatches() == 0);
    CHECK(mgr.inferOldestMs() == 0);
    CHECK(mgr.inferStartedCount() == 0);
    CHECK(mgr.inferCompletedCount() == 0);
    CHECK(mgr.inferLastMs() == 0);
    CHECK(mgr.inferMaxMs() == 0);
    CHECK(mgr.inferWarnCount() == 0);
    CHECK(mgr.semanticEdgesCreated() == 0);
    CHECK(mgr.semanticDocsProcessed() == 0);
    CHECK(mgr.semanticUpdateErrors() == 0);
}

TEST_CASE("EmbeddingLifecycleManager ensureModelReadySync fails without provider",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    auto result = mgr.ensureModelReadySync("test-model");
    CHECK_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::InvalidState);
}

TEST_CASE("EmbeddingLifecycleManager resetWarmupState clears state",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());

    mgr.setModelName("model-a");
    mgr.setAdoptedPluginName("plugin-a");
    mgr.resetWarmupState();
    CHECK(mgr.modelName() == "model-a");
    CHECK(mgr.adoptedPluginName() == "plugin-a");
}

TEST_CASE("EmbeddingLifecycleManager resolvePreferredModel returns empty with null config",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());
    CHECK(mgr.resolvePreferredModel().empty());
}

TEST_CASE("EmbeddingLifecycleManager detectPreloadFlag returns false with null config",
          "[daemon][embedding-lifecycle][catch2]") {
    EmbeddingLifecycleManager mgr(nullDeps());
    CHECK_FALSE(mgr.detectPreloadFlag());
}
