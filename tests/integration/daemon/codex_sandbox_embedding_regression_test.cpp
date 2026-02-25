#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "../../common/test_helpers_catch2.h"
#include "test_async_helpers.h"
#include "test_daemon_harness.h"
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/RequestDispatcher.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/embedded_service_host.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/vector/embedding_generator.h>

using namespace yams::daemon;
using namespace yams::test;
using namespace std::chrono_literals;

namespace {

std::filesystem::path makeUniqueTempDir(const std::string& prefix) {
    namespace fs = std::filesystem;
    const auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    auto dir = fs::temp_directory_path() / (prefix + stamp);
    std::error_code ec;
    fs::create_directories(dir, ec);
    return dir;
}

class StubEmbeddingBackend : public yams::vector::IEmbeddingBackend {
public:
    explicit StubEmbeddingBackend(size_t dim) : dim_(dim) {}

    bool initialize() override {
        initialized_ = true;
        return true;
    }
    void shutdown() override { initialized_ = false; }
    bool isInitialized() const override { return initialized_; }

    yams::Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        std::vector<float> embedding(dim_, 0.0f);
        const float seed = static_cast<float>((text.size() % 31) + 1) / 31.0f;
        for (size_t i = 0; i < embedding.size(); ++i) {
            embedding[i] = seed + static_cast<float>(i % 7) * 0.001f;
        }
        return embedding;
    }

    yams::Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        std::vector<std::vector<float>> out;
        out.reserve(texts.size());
        for (const auto& text : texts) {
            auto one = generateEmbedding(text);
            if (!one) {
                return one.error();
            }
            out.push_back(std::move(one.value()));
        }
        return out;
    }

    size_t getEmbeddingDimension() const override { return dim_; }
    size_t getMaxSequenceLength() const override { return 4096; }
    std::string getBackendName() const override { return "StubEmbeddingBackend"; }
    bool isAvailable() const override { return true; }
    yams::vector::GenerationStats getStats() const override { return {}; }
    void resetStats() override {}

private:
    size_t dim_;
    bool initialized_ = true;
};

class StubEmbeddingModelProvider : public IModelProvider {
public:
    explicit StubEmbeddingModelProvider(size_t dim) : dim_(dim) {}

    yams::Result<std::vector<float>> generateEmbedding(const std::string& text) override {
        std::vector<float> embedding(dim_, 0.0f);
        const float seed = static_cast<float>((text.size() % 31) + 1) / 31.0f;
        for (size_t i = 0; i < embedding.size(); ++i) {
            embedding[i] = seed + static_cast<float>(i % 7) * 0.001f;
        }
        return embedding;
    }

    yams::Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        std::vector<std::vector<float>> out;
        out.reserve(texts.size());
        for (const auto& text : texts) {
            auto one = generateEmbedding(text);
            if (!one) {
                return one.error();
            }
            out.push_back(std::move(one.value()));
        }
        return out;
    }

    yams::Result<std::vector<float>> generateEmbeddingFor(const std::string& modelName,
                                                          const std::string& text) override {
        if (!isModelLoaded(modelName)) {
            return yams::Error{yams::ErrorCode::NotFound,
                               "Requested model not loaded: " + modelName};
        }
        return generateEmbedding(text);
    }

    yams::Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string& modelName,
                               const std::vector<std::string>& texts) override {
        if (!isModelLoaded(modelName)) {
            return yams::Error{yams::ErrorCode::NotFound,
                               "Requested model not loaded: " + modelName};
        }
        return generateBatchEmbeddings(texts);
    }

    yams::Result<void> loadModel(const std::string& modelName) override {
        loaded_.push_back(modelName);
        return yams::Result<void>();
    }

    yams::Result<void> unloadModel(const std::string& modelName) override {
        loaded_.erase(std::remove(loaded_.begin(), loaded_.end(), modelName), loaded_.end());
        return yams::Result<void>();
    }

    bool isModelLoaded(const std::string& modelName) const override {
        return std::find(loaded_.begin(), loaded_.end(), modelName) != loaded_.end();
    }

    std::vector<std::string> getLoadedModels() const override { return loaded_; }
    size_t getLoadedModelCount() const override { return loaded_.size(); }

    yams::Result<ModelInfo> getModelInfo(const std::string& modelName) const override {
        if (!isModelLoaded(modelName)) {
            return yams::Error{yams::ErrorCode::NotFound, "Model not loaded"};
        }
        ModelInfo info;
        info.name = modelName;
        info.embeddingDim = dim_;
        return info;
    }

    size_t getEmbeddingDim(const std::string&) const override { return dim_; }

    std::shared_ptr<yams::vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& = "") override {
        if (!generator_) {
            auto backend = std::make_unique<StubEmbeddingBackend>(dim_);
            generator_ = std::make_shared<yams::vector::EmbeddingGenerator>(std::move(backend));
            (void)generator_->initialize();
        }
        return generator_;
    }

    std::string getProviderName() const override { return "StubEmbeddingProvider"; }
    std::string getProviderVersion() const override { return "vtest"; }
    bool isAvailable() const override { return true; }
    size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}

private:
    size_t dim_;
    std::vector<std::string> loaded_;
    std::shared_ptr<yams::vector::EmbeddingGenerator> generator_;
};

void startHarnessWithRetry(DaemonHarness& harness, int maxRetries = 3,
                           std::chrono::milliseconds retryDelay = 250ms) {
    for (int attempt = 0; attempt < maxRetries; ++attempt) {
        if (harness.start(30s)) {
            return;
        }
        harness.stop();
        std::this_thread::sleep_for(retryDelay);
    }
    FAIL("Failed to start daemon harness after retries");
}

void injectProviderWithoutConfiguredModel(ServiceManager* serviceManager,
                                          const std::string& loadedModelName) {
    REQUIRE(serviceManager != nullptr);

    auto provider = std::make_shared<StubEmbeddingModelProvider>(384);

    auto loadResult = provider->loadModel(loadedModelName);
    REQUIRE(loadResult.has_value());

    serviceManager->__test_setModelProvider(provider);

    auto currentProvider = serviceManager->getModelProvider();
    REQUIRE(currentProvider != nullptr);
    REQUIRE(currentProvider->isAvailable());

    // Critical precondition for this regression: daemon has provider, but no configured
    // embedding model name in ServiceManager.
    REQUIRE(serviceManager->getEmbeddingModelName().empty());
}

void injectMockProviderWithoutConfiguredModel(DaemonHarness& harness,
                                              const std::string& loadedModelName) {
    auto* daemon = harness.daemon();
    REQUIRE(daemon != nullptr);
    injectProviderWithoutConfiguredModel(daemon->getServiceManager(), loadedModelName);
}

Response dispatchEmbedDocumentsDirect(DaemonHarness& harness, const EmbedDocumentsRequest& req) {
    auto* daemon = harness.daemon();
    REQUIRE(daemon != nullptr);

    auto* serviceManager = daemon->getServiceManager();
    REQUIRE(serviceManager != nullptr);

    auto* state = const_cast<StateComponent*>(&daemon->getState());
    RequestDispatcher dispatcher(nullptr, serviceManager, state);

    Request request = req;
    auto promise = std::make_shared<std::promise<yams::Result<Response>>>();
    auto future = promise->get_future();
    boost::asio::co_spawn(
        yams::daemon::GlobalIOContext::global_executor(),
        [aw = dispatcher.dispatch(request), promise]() mutable -> boost::asio::awaitable<void> {
            try {
                auto response = co_await std::move(aw);
                promise->set_value(yams::Result<Response>(std::move(response)));
            } catch (const std::exception& e) {
                promise->set_value(
                    yams::Error{yams::ErrorCode::InternalError,
                                std::string("dispatcher await failed: ") + e.what()});
            }
            co_return;
        },
        boost::asio::detached);

    if (future.wait_for(20s) != std::future_status::ready) {
        FAIL("dispatcher await timeout");
    }
    auto result = future.get();
    REQUIRE(result.has_value());
    return result.value();
}

} // namespace

TEST_CASE("EmbedDocuments request model handling with daemon harness",
          "[daemon][sandbox][embeddings][regression]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    const std::string kModelName = "all-MiniLM-L6-v2";

    DaemonHarness::Options opts;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = false;
    opts.autoLoadPlugins = false;

    SECTION("Repro: fails when neither request nor service config provides a model") {
        DaemonHarness harness(opts);
        startHarnessWithRetry(harness);
        injectMockProviderWithoutConfiguredModel(harness, kModelName);

        EmbedDocumentsRequest embedReq;
        embedReq.documentHashes.clear();
        embedReq.modelName.clear();
        embedReq.normalize = true;
        embedReq.batchSize = 8;
        embedReq.skipExisting = false;

        auto response = dispatchEmbedDocumentsDirect(harness, embedReq);
        REQUIRE(std::holds_alternative<ErrorResponse>(response));
        const auto& error = std::get<ErrorResponse>(response);
        CHECK(error.message.find("No embedding model configured") != std::string::npos);
    }

    SECTION("Guard: succeeds when request explicitly provides model name") {
        DaemonHarness harness(opts);
        startHarnessWithRetry(harness);
        injectMockProviderWithoutConfiguredModel(harness, kModelName);

        EmbedDocumentsRequest embedReq;
        embedReq.documentHashes.clear();
        embedReq.modelName = kModelName;
        embedReq.normalize = true;
        embedReq.batchSize = 8;
        embedReq.skipExisting = false;

        auto response = dispatchEmbedDocumentsDirect(harness, embedReq);
        if (std::holds_alternative<ErrorResponse>(response)) {
            const auto& error = std::get<ErrorResponse>(response);
            CHECK(error.message.find("No embedding model configured") == std::string::npos);
            FAIL("EmbedDocuments returned error: " + error.message);
        }

        REQUIRE(std::holds_alternative<EmbedDocumentsResponse>(response));
        const auto& embedResult = std::get<EmbedDocumentsResponse>(response);
        CHECK(embedResult.failed == 0);
    }
}

TEST_CASE("EmbedDocuments request model handling via in-process client transport",
          "[daemon][sandbox][embeddings][regression][inprocess]") {
    SKIP_DAEMON_TEST_ON_WINDOWS();

    const std::string kModelName = "all-MiniLM-L6-v2";
    const auto dataDir = makeUniqueTempDir("yams_codex_inprocess_embed_regression_");

    EmbeddedServiceHost::Options hostOpts;
    hostOpts.dataDir = dataDir;
    hostOpts.ioThreads = 1;
    hostOpts.enableAutoRepair = false;
    hostOpts.autoLoadPlugins = false;
    hostOpts.enableModelProvider = false;
    hostOpts.initTimeoutSeconds = 30;

    auto hostResult = EmbeddedServiceHost::getOrCreate(hostOpts);
    REQUIRE(hostResult.has_value());
    auto host = hostResult.value();
    REQUIRE(host != nullptr);

    injectProviderWithoutConfiguredModel(host->getServiceManager(), kModelName);

    yams::test::ScopedEnvVar embeddedMode("YAMS_EMBEDDED", std::string("auto"));
    yams::test::ScopedEnvVar inDaemon("YAMS_IN_DAEMON", std::nullopt);

    ClientConfig cfg;
    cfg.dataDir = dataDir;
    cfg.socketPath = dataDir / "missing" / "daemon.sock";
    cfg.transportMode = ClientTransportMode::Auto;
    cfg.autoStart = false;
    cfg.connectTimeout = 3s;
    cfg.headerTimeout = 10s;
    cfg.bodyTimeout = 10s;
    cfg.requestTimeout = 10s;

    DaemonClient client(cfg);
    auto connectRes = yams::cli::run_sync(client.connect(), 5s);
    REQUIRE(connectRes.has_value());

    EmbedDocumentsRequest missingModelReq;
    missingModelReq.documentHashes.clear();
    missingModelReq.modelName.clear();
    missingModelReq.normalize = true;
    missingModelReq.batchSize = 8;
    missingModelReq.skipExisting = false;

    auto missingModelRes = yams::cli::run_sync(client.call(missingModelReq), 10s);
    REQUIRE_FALSE(missingModelRes.has_value());
    CHECK(missingModelRes.error().message.find("No embedding model configured") !=
          std::string::npos);

    EmbedDocumentsRequest requestModelReq;
    requestModelReq.documentHashes.clear();
    requestModelReq.modelName = kModelName;
    requestModelReq.normalize = true;
    requestModelReq.batchSize = 8;
    requestModelReq.skipExisting = false;

    auto requestModelRes = yams::cli::run_sync(client.call(requestModelReq), 10s);
    REQUIRE(requestModelRes.has_value());
    CHECK(requestModelRes.value().failed == 0);

    auto shutdownRes = host->shutdown();
    REQUIRE(shutdownRes.has_value());
    std::error_code ec;
    std::filesystem::remove_all(dataDir, ec);
}
