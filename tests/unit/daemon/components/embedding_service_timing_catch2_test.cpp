#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../../../src/daemon/components/embedding_derivation_policy.h"
#include "../../../../src/daemon/components/embedding_input_selection.h"
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/embed_preparer.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/vector/vector_database.h>

#include "../../../common/metadata_test_db.h"
#include "../../../common/test_helpers_catch2.h"

using namespace std::chrono_literals;

namespace yams::daemon {

TEST_CASE("Prepared embedding payload binds the extracted text snapshot",
          "[daemon][embedding][prepared-freshness]") {
    ConfigResolver::EmbeddingChunkingPolicy policy;
    ConfigResolver::EmbeddingSelectionPolicy selection;
    auto chunker = vector::createChunker(policy.strategy, policy.config, nullptr);
    REQUIRE(chunker);
    embed::EmbedSourceDoc source{"revision", "First extracted text.", "file", "/file",
                                 "text/plain"};
    auto prepared = embed::prepareEmbedPreparedDoc(source, *chunker, selection);
    REQUIRE(prepared);
    prepared->preparationRecipe = embed::embeddingPreparationRecipe(policy, selection);
    CHECK(embed::preparedEmbeddingMatches(*prepared, source.extractedText,
                                          prepared->preparationRecipe));
    CHECK_FALSE(embed::preparedEmbeddingMatches(*prepared, "Replacement extracted text.",
                                                prepared->preparationRecipe));
    CHECK_FALSE(embed::preparedEmbeddingMatches(*prepared, source.extractedText, "other-policy"));
    prepared->sourceTextHash.clear();
    CHECK_FALSE(embed::preparedEmbeddingMatches(*prepared, source.extractedText,
                                                prepared->preparationRecipe));
}

TEST_CASE("Embedding derivation policy binds model identity and preparation parameters",
          "[daemon][embedding][derivation-policy]") {
    ConfigResolver::EmbeddingChunkingPolicy chunking;
    ConfigResolver::EmbeddingSelectionPolicy selection;
    const auto preparation = embed::embeddingPreparationRecipe(chunking, selection);
    const auto recipe = embed::embeddingDerivationRecipe(preparation, "space", "v1", 384);
    CHECK(recipe == embed::embeddingDerivationRecipe(preparation, "space", "v1", 384));
    CHECK(recipe != embed::embeddingDerivationRecipe(preparation, "other-space", "v1", 384));
    CHECK(recipe != embed::embeddingDerivationRecipe(preparation, "space", "v2", 384));
    CHECK(recipe != embed::embeddingDerivationRecipe(preparation, "space", "v1", 768));
    chunking.config.overlap_size++;
    CHECK(preparation != embed::embeddingPreparationRecipe(chunking, selection));
    chunking.config.overlap_size--;
    selection.maxChunksPerDoc++;
    CHECK(preparation != embed::embeddingPreparationRecipe(chunking, selection));
}

TEST_CASE("Embedding derivation ledger completes only minted hashes, once each",
          "[daemon][embedding][derivation][catch2]") {
    embed::EmbeddingDerivationLedger ledger;
    ledger.adopt({{"a", "gen-a", "recipe"}, {"b", "gen-b", "recipe"}});

    auto tokens = ledger.tokensFor({"b", "unknown", "a", "b"});
    REQUIRE(tokens.size() == 2);
    CHECK(tokens[0].hash == "b");
    CHECK(tokens[0].generation == "gen-b");
    CHECK(tokens[1].hash == "a");
    CHECK(tokens[1].generation == "gen-a");
    CHECK(ledger.tokensFor({"unknown"}).empty());
}

TEST_CASE("Embedding input selection falls back for empty prepared payloads",
          "[daemon][embedding][input-selection]") {
    InternalEventBus::EmbedJob job;
    job.hashes = {"empty", "ready", "cold", "cold"};
    InternalEventBus::EmbedPreparedDoc empty;
    empty.hash = "empty";
    InternalEventBus::EmbedPreparedDoc ready;
    ready.hash = "ready";
    ready.chunks.emplace_back();
    job.preparedDocs = {empty, ready, ready};
    const auto selected = embed::selectEmbeddingInputs(job);
    REQUIRE(selected.preparedIndices == std::vector<std::size_t>{1});
    CHECK(selected.gatherHashes == std::vector<std::string>{"empty", "cold"});
}

TEST_CASE("Embedding input selection prefers a valid duplicate over an empty payload",
          "[daemon][embedding][input-selection]") {
    InternalEventBus::EmbedJob job;
    job.hashes = {"revision"};
    InternalEventBus::EmbedPreparedDoc empty;
    empty.hash = "revision";
    auto ready = empty;
    ready.chunks.emplace_back();
    job.preparedDocs = {empty, ready};
    const auto selected = embed::selectEmbeddingInputs(job);
    CHECK(selected.preparedIndices == std::vector<std::size_t>{1});
    CHECK(selected.gatherHashes.empty());
}

class EmbeddingServiceTimingTestAccess {
public:
    static void process(EmbeddingService& service, InternalEventBus::EmbedJob job) {
        service.processEmbedJob(std::move(job));
    }
    static void record(EmbeddingService& service, std::string_view phase) {
        service.recordPhaseTiming(phase, std::chrono::steady_clock::now() - 1ms);
    }
};

namespace {

class RecordingTimingSink final : public EmbeddingPhaseTimingSink {
public:
    void record(std::string_view phase, std::uint64_t elapsedUs) override {
        ++calls;
        lastPhase = phase;
        lastElapsedUs = elapsedUs;
    }

    std::size_t calls{0};
    std::string lastPhase;
    std::uint64_t lastElapsedUs{0};
};

class ThrowingTimingSink final : public EmbeddingPhaseTimingSink {
public:
    void record(std::string_view, std::uint64_t) override {
        throw std::runtime_error("timing sink failure");
    }
};

} // namespace

TEST_CASE("EmbeddingService shutdown before start leaves no deferred service access",
          "[daemon][embedding][shutdown][never-started]") {
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);
    service.shutdown();
    // Poll while service is still alive: the old shutdown queued a closure capturing it.
    // A never-started service must leave no such handler for a later coordinator start.
    CHECK(coordinator.getIOContext()->poll() == 0);
    service.shutdown();
    CHECK(coordinator.getIOContext()->poll() == 0);
}

TEST_CASE("EmbeddingService ignores benchmark environment in production policy",
          "[daemon][components][embedding][config][catch2]") {
    yams::test::ScopedEnvVar benchmarkProfile{"YAMS_BENCH_EMBED_PROFILE", "balanced"};
    yams::test::ScopedEnvVar productConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", std::nullopt};
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);

    const auto effective = service.effectiveConcurrencyPolicy();
    CHECK((effective.coremlUnifiedConcurrency == 1U));
    CHECK((effective.coremlUnifiedConcurrencySource == "default"));
}

TEST_CASE("EmbeddingService accepts typed CoreML concurrency policy",
          "[daemon][components][embedding][config][catch2]") {
    yams::test::ScopedEnvVar benchmarkProfile{"YAMS_BENCH_EMBED_PROFILE", "safe"};
    yams::test::ScopedEnvVar productConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", "7"};
    WorkCoordinator coordinator;
    EmbeddingServiceConfig policy;
    policy.coremlUnifiedConcurrency = 2U;
    policy.coremlUnifiedConcurrencySource = "harness:typed";
    EmbeddingService service(nullptr, nullptr, &coordinator, policy);

    const auto effective = service.effectiveConcurrencyPolicy();
    CHECK((effective.coremlUnifiedConcurrency == 2U));
    CHECK((effective.coremlUnifiedConcurrencySource == "harness:typed"));
}

TEST_CASE("EmbeddingService freezes the compatibility concurrency at construction",
          "[daemon][components][embedding][config][catch2]") {
    yams::test::ScopedEnvVar productConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", "3"};
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);

    {
        yams::test::ScopedEnvVar changedConcurrency{"YAMS_EMBED_COREML_SAFE_CONCURRENCY", "9"};
        const auto effective = service.effectiveConcurrencyPolicy();
        CHECK((effective.coremlUnifiedConcurrency == 3U));
        CHECK((effective.coremlUnifiedConcurrencySource ==
               "environment:YAMS_EMBED_COREML_SAFE_CONCURRENCY"));
    }
}

TEST_CASE("EmbeddingService phase timing is optional and replaceable",
          "[daemon][components][embedding][timing][catch2]") {
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);

    REQUIRE_NOTHROW(EmbeddingServiceTimingTestAccess::record(service, "unset"));

    auto first = std::make_shared<RecordingTimingSink>();
    service.setPhaseTimingSink(first);
    EmbeddingServiceTimingTestAccess::record(service, "infer");
    CHECK((first->calls == 1));
    CHECK((first->lastPhase == "infer"));
    CHECK((first->lastElapsedUs >= 1000));

    auto second = std::make_shared<RecordingTimingSink>();
    service.setPhaseTimingSink(second);
    EmbeddingServiceTimingTestAccess::record(service, "gather");
    CHECK((first->calls == 1));
    CHECK((second->calls == 1));
    CHECK((second->lastPhase == "gather"));

    service.setPhaseTimingSink(std::make_shared<ThrowingTimingSink>());
    REQUIRE_NOTHROW(EmbeddingServiceTimingTestAccess::record(service, "throwing"));
}

namespace {

// Deterministic in-process provider: no daemon, model download, or timing sleeps.
class DerivationTestProvider final : public IModelProvider {
public:
    std::size_t calls{0};
    std::string version{"v1"};

    Result<std::vector<float>> generateEmbedding(const std::string&) override {
        return std::vector<float>(64, 0.125f);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        ++calls;
        return std::vector<std::vector<float>>(texts.size(), std::vector<float>(64, 0.125f));
    }
    Result<std::vector<float>> generateEmbeddingFor(const std::string&,
                                                    const std::string& text) override {
        return generateEmbedding(text);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddingsFor(const std::string&, const std::vector<std::string>& texts) override {
        return generateBatchEmbeddings(texts);
    }
    Result<void> loadModel(const std::string&) override { return {}; }
    Result<void> unloadModel(const std::string&) override { return {}; }
    bool isModelLoaded(const std::string&) const override { return true; }
    std::vector<std::string> getLoadedModels() const override { return {"test-model"}; }
    std::size_t getLoadedModelCount() const override { return 1; }
    Result<ModelInfo> getModelInfo(const std::string& name) const override {
        ModelInfo info;
        info.name = name;
        info.embeddingDim = 64;
        return info;
    }
    std::size_t getEmbeddingDim(const std::string&) const override { return 64; }
    std::shared_ptr<vector::EmbeddingGenerator>
    getEmbeddingGenerator(const std::string& = "") override {
        return {};
    }
    std::string getProviderName() const override { return "derivation-test"; }
    std::string getProviderVersion() const override { return version; }
    bool isAvailable() const override { return true; }
    std::size_t getMemoryUsage() const override { return 0; }
    void releaseUnusedResources() override {}
    void shutdown() override {}
};

struct ServiceDerivationFixture {
    yams::test::SpdlogLevelGuard logLevel;
    yams::test::TempDirGuard temp{"embedding_service_derivation_"};
    std::filesystem::path metadataPath;
    std::unique_ptr<metadata::ConnectionPool> pool;
    std::shared_ptr<metadata::MetadataRepository> repo;
    std::shared_ptr<vector::VectorDatabase> vectors;
    std::shared_ptr<DerivationTestProvider> provider{std::make_shared<DerivationTestProvider>()};
    boost::asio::io_context io;
    WorkCoordinator work;
    std::unique_ptr<WriteCoordinator> writer;
    std::future<void> runner;
    std::unique_ptr<EmbeddingService> service;
    const std::string hash{std::string(64, 'a')};

    ServiceDerivationFixture() {
        spdlog::set_level(spdlog::level::warn);
        metadataPath =
            yams::test::migrated_metadata_db_template().clone("embedding_service_derivation_");
        metadata::ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool = std::make_unique<metadata::ConnectionPool>(metadataPath.string(), config);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<metadata::MetadataRepository>(
            *pool, nullptr, metadata::MetadataRepository::SchemaBootstrapMode::AssumeReady);
        vector::VectorDatabaseConfig vectorConfig;
        vectorConfig.database_path = (temp.path() / "vectors.db").string();
        vectorConfig.embedding_dim = 64;
        vectors = std::make_shared<vector::VectorDatabase>(vectorConfig);
        REQUIRE(vectors->initialize());
        writer = std::make_unique<WriteCoordinator>(io, nullptr, repo);
        service = std::make_unique<EmbeddingService>(nullptr, repo, &work);
        service->setProviders([this] { return provider; }, [] { return "test-model"; },
                              [this] { return vectors; });
        service->setWriteCoordinatorGetter([this] { return writer.get(); });
        metadata::DocumentInfo doc;
        doc.sha256Hash = hash;
        doc.filePath = "/derivation.txt";
        doc.fileName = "derivation.txt";
        doc.mimeType = "text/plain";
        auto inserted = repo->insertDocument(doc);
        REQUIRE(inserted.has_value());
        metadata::BatchContentEntry content;
        content.documentId = inserted.value();
        content.title = doc.fileName;
        content.contentText = "Original extracted text for a deterministic embedding.";
        content.mimeType = doc.mimeType;
        content.extractionMethod = "test";
        content.language = "en";
        REQUIRE(repo->batchInsertContentAndIndex({content}).has_value());
    }

    ~ServiceDerivationFixture() {
        service.reset();
        writer->shutdown();
        io.stop();
        if (runner.valid()) {
            runner.get();
        }
        writer.reset();
        repo.reset();
        pool->shutdown();
        pool.reset();
        yams::test::remove_sqlite_artifacts(metadataPath);
    }

    void process() {
        InternalEventBus::EmbedJob job;
        job.hashes = {hash};
        job.modelName = "test-model";
        job.skipExisting = true;
        job.updateSemanticGraph = false;
        EmbeddingServiceTimingTestAccess::process(*service, std::move(job));
        if (!runner.valid()) {
            writer->start();
            runner = std::async(std::launch::async, [this] { io.run(); });
        }
        REQUIRE(writer->flush(5s).has_value());
    }
};

} // namespace

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "EmbeddingService skipExisting requires the current derivation recipe",
                 "[daemon][embedding][service-derivation]") {
    process();
    REQUIRE(provider->calls > 0);
    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    REQUIRE(ready.value());
    const auto calls = provider->calls;
    process();
    CHECK(provider->calls == calls);
    SECTION("provider recipe changes") {
        provider->version = "v2";
    }
    SECTION("legacy readiness has no derivation provenance") {
        REQUIRE(pool->withConnection([](metadata::Database& db) -> Result<void> {
                        return db.execute("DELETE FROM document_embedding_derivations");
                    })
                    .has_value());
        auto legacyReady = repo->hasDocumentEmbeddingByHash(hash);
        REQUIRE(legacyReady.has_value());
        REQUIRE(legacyReady.value());
    }
    process();
    CHECK(provider->calls > calls);
    auto states = repo->batchGetDocumentEmbeddingDerivations({hash});
    REQUIRE(states.has_value());
    REQUIRE(states.value().contains(hash));
    CHECK(states.value().at(hash).completed);
    const auto recipe = embed::embeddingDerivationRecipe(
        embed::embeddingPreparationRecipe(ConfigResolver::resolveEmbeddingChunkingPolicy(),
                                          ConfigResolver::resolveEmbeddingSelectionPolicy()),
        provider->getEmbeddingSpaceIdentity("test-model"), provider->version, 64);
    CHECK(states.value().at(hash).token.recipe == recipe);
}

} // namespace yams::daemon
