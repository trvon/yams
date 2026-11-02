#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <functional>
#include <future>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../../../../src/daemon/components/embedding_derivation_policy.h"
#include "../../../../src/daemon/components/embedding_input_selection.h"
#include "../../../../src/daemon/components/embedding_job_split.h"
#include "../../../../src/daemon/components/semantic_graph_candidate_policy.h"
#include <yams/crypto/hasher.h>
#include <yams/daemon/components/embed_preparer.h>
#include <yams/daemon/components/EmbeddingService.h>
#include <yams/daemon/components/ResourceGovernor.h>
#include <yams/daemon/components/WorkCoordinator.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/vector_database.h>

#include "../../../common/metadata_test_db.h"
#include "../../../common/test_helpers_catch2.h"

using namespace std::chrono_literals;

namespace yams::metadata {
struct MetadataRepositoryTestAccess {
    template <typename T>
    static Result<T> query(MetadataRepository& repo, bool write,
                           const std::function<Result<T>(Database&)>& operation) {
        return write ? repo.executeWriteQuery<T>(operation) : repo.executeReadQuery<T>(operation);
    }
};
} // namespace yams::metadata

namespace yams::daemon {

TEST_CASE("Grouped embed dispatch bounds prepared and plain jobs without losing remainders",
          "[daemon][embedding][embed-job-cap]") {
    for (const bool prepared : {false, true}) {
        INFO("prepared=" << prepared);
        InternalEventBus::EmbedJob job;
        job.hashes = {"a", "b", "c", "d", "e"};
        job.modelName = "model";
        job.skipExisting = false;
        job.updateSemanticGraph = false;
        if (prepared) {
            // Prepared payloads are sparse and not positionally aligned with hashes.
            for (const auto& hash : {"e", "a", "c"}) {
                InternalEventBus::EmbedPreparedDoc doc;
                doc.hash = hash;
                doc.filePath = std::string("/") + hash;
                doc.chunks.push_back(
                    {std::string(hash) + "-chunk", std::string(hash) + "-text", 1, 7});
                job.preparedDocs.push_back(std::move(doc));
            }
        }
        std::vector<std::string> seen;
        std::vector<std::string> payloads;
        while (!job.hashes.empty()) {
            auto split = embed::takeJobPrefix(job, 2);
            REQUIRE(split.hashes.size() <= 2);
            CHECK(split.batchSize == split.hashes.size());
            CHECK(split.modelName == "model");
            CHECK_FALSE(split.skipExisting);
            CHECK_FALSE(split.updateSemanticGraph);
            for (const auto& doc : split.preparedDocs) {
                CHECK(std::find(split.hashes.begin(), split.hashes.end(), doc.hash) !=
                      split.hashes.end());
                CHECK(doc.filePath == "/" + doc.hash);
                REQUIRE(doc.chunks.size() == 1);
                CHECK(doc.chunks[0].content == doc.hash + "-text");
                payloads.push_back(doc.hash);
            }
            seen.insert(seen.end(), split.hashes.begin(), split.hashes.end());
            if (seen.size() == 2) {
                // One available concurrency slot: this exact remainder is requeued.
                CHECK(job.hashes == std::vector<std::string>{"c", "d", "e"});
                CHECK_FALSE(job.updateSemanticGraph);
                CHECK_FALSE(job.skipExisting);
                CHECK(job.modelName == "model");
                if (prepared) {
                    REQUIRE(job.preparedDocs.size() == 2);
                }
            }
        }
        CHECK(seen == std::vector<std::string>{"a", "b", "c", "d", "e"});
        std::sort(payloads.begin(), payloads.end());
        CHECK(payloads ==
              (prepared ? std::vector<std::string>{"a", "c", "e"} : std::vector<std::string>{}));
    }
}

TEST_CASE("Prepared embed remainder is reclamped after governor pressure rises",
          "[daemon][embedding][embed-job-cap][embed-doc-budget]") {
    auto& governor = ResourceGovernor::instance();
    struct RestorePressure {
        ~RestorePressure() {
            ResourceGovernor::instance().testing_setPressureState(ResourcePressureLevel::Normal,
                                                                  std::chrono::steady_clock::now());
        }
    } restore;
    governor.testing_setPressureState(ResourcePressureLevel::Normal,
                                      std::chrono::steady_clock::now());
    InternalEventBus::EmbedJob job;
    job.hashes = {"a", "b", "c", "d", "e"};
    job.modelName = "model";
    job.skipExisting = false;
    job.updateSemanticGraph = false;
    for (const auto& hash : job.hashes) {
        InternalEventBus::EmbedPreparedDoc doc;
        doc.hash = hash;
        doc.chunks.push_back({hash + "-chunk", hash + "-text", 1, 7});
        job.preparedDocs.push_back(std::move(doc));
    }
    const auto preparedCap = governor.capEmbedJobDocuments(job.hashes.size());
    REQUIRE(preparedCap == 5);
    governor.testing_setPressureState(ResourcePressureLevel::Critical,
                                      std::chrono::steady_clock::now());
    std::vector<std::string> seen;
    while (!job.hashes.empty()) {
        auto split = embed::takeJobPrefix(job, governor.capEmbedJobDocuments(preparedCap));
        REQUIRE(split.hashes.size() == 1);
        REQUIRE(split.preparedDocs.size() == 1);
        CHECK(split.preparedDocs.front().hash == split.hashes.front());
        CHECK(split.preparedDocs.front().chunks.front().content == split.hashes.front() + "-text");
        CHECK_FALSE(split.skipExisting);
        CHECK_FALSE(split.updateSemanticGraph);
        CHECK(split.modelName == "model");
        seen.push_back(split.hashes.front());
    }
    CHECK(seen == std::vector<std::string>{"a", "b", "c", "d", "e"});
    CHECK(job.preparedDocs.empty());
}

TEST_CASE("Monitored embed jobs remain indivisible for completion ownership",
          "[daemon][embedding][embed-job-cap]") {
    InternalEventBus::EmbedJob job;
    job.hashes = {"a", "b", "c"};
    job.monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
    auto monitor = job.monitor;
    auto dispatch = embed::takeJobPrefix(job, 1);
    CHECK(dispatch.hashes.size() == 3);
    CHECK(dispatch.monitor == monitor);
    CHECK_FALSE(monitor->done);
    CHECK(job.hashes.empty());
}

TEST_CASE("Semantic graph output capacity follows retained neighbors rather than scored pairs",
          "[daemon][embedding][semantic-candidate-policy]") {
    const embed::SemanticGraphCandidateCounts dense{1000, 1000, 999000, 8000};
    CHECK(dense.nodeCapacity() == 9000);
    const embed::SemanticGraphCandidateCounts singleNeighbor{1, 1, 1, 1};
    CHECK(singleNeighbor.hasGraphWork());
    CHECK(singleNeighbor.nodeCapacity() == 2);
    const embed::SemanticGraphCandidateCounts noNeighbor{2, 2, 0, 0};
    CHECK_FALSE(noNeighbor.hasGraphWork());
}

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
        boost::asio::io_context completionIo;
        std::exception_ptr error;
        boost::asio::co_spawn(completionIo, service.processEmbedJob(std::move(job)),
                              [&](std::exception_ptr e) { error = e; });
        completionIo.run();
        if (error) {
            std::rethrow_exception(error);
        }
    }
    static bool maintenanceEligible(const EmbeddingService& service, bool idle) {
        return service.semanticBackfillEligible(idle);
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

// Force repair to use the metadata content cache without touching a live content corpus.
class MissingContentStore final : public api::IContentStore {
public:
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
    Result<api::StoreResult> storeBytes(std::span<const std::byte>,
                                        const api::ContentMetadata&) override {
        return ErrorCode::NotImplemented;
    }
    Result<std::vector<std::byte>> retrieveBytes(const std::string&) override {
        return Error{ErrorCode::NotFound, "content intentionally absent in repair test"};
    }
    Result<std::vector<std::byte>> retrieveBytesPrefix(const std::string&, std::size_t) override {
        return Error{ErrorCode::NotFound, "content intentionally absent in repair test"};
    }
    Result<RawContent> retrieveRaw(const std::string&) override {
        return Error{ErrorCode::NotFound, "content intentionally absent in repair test"};
    }
    std::future<Result<RawContent>> retrieveRawAsync(const std::string&) override {
        return std::async(std::launch::deferred, [] {
            return Result<RawContent>(
                Error{ErrorCode::NotFound, "content intentionally absent in repair test"});
        });
    }
    Result<bool> exists(const std::string&) const override { return false; }
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
};

// Deterministic in-process provider: no daemon, model download, or timing sleeps.
class DerivationTestProvider final : public IModelProvider {
public:
    std::size_t calls{0};
    std::vector<std::string> inferredTexts;
    std::string version{"v1"};
    bool fail{false};
    std::function<void()> beforeFailure;

    Result<std::vector<float>> generateEmbedding(const std::string&) override {
        if (fail) {
            return Error{ErrorCode::InternalError, "injected inference failure"};
        }
        return std::vector<float>(64, 0.125f);
    }
    Result<std::vector<std::vector<float>>>
    generateBatchEmbeddings(const std::vector<std::string>& texts) override {
        ++calls;
        inferredTexts.insert(inferredTexts.end(), texts.begin(), texts.end());
        if (beforeFailure) {
            auto callback = std::exchange(beforeFailure, {});
            callback();
        }
        if (fail) {
            return Error{ErrorCode::InternalError, "injected inference failure"};
        }
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

class DerivationRaceRepository final : public metadata::MetadataRepository {
public:
    using metadata::MetadataRepository::MetadataRepository;
    std::function<void()> beforeLookup;
    std::function<void(int64_t)> beforeContent;

    Result<std::optional<metadata::DocumentContent>> getContent(int64_t id) override {
        if (beforeContent) {
            beforeContent(id);
        }
        return metadata::MetadataRepository::getContent(id);
    }

    Result<std::optional<metadata::DocumentInfo>>
    getDocumentByHash(const std::string& hash) override {
        if (beforeLookup) {
            auto callback = std::exchange(beforeLookup, {});
            callback();
        }
        return metadata::MetadataRepository::getDocumentByHash(hash);
    }
};

struct ServiceDerivationFixture {
    // These tests exercise real vector persistence, even in vector-disabled CI lanes.
    // Declare guards before owned resources so restoration happens after teardown.
    yams::test::ScopedEnvVar enableVectors{"YAMS_DISABLE_VECTORS", "0"};
    yams::test::ScopedEnvVar enableVectorAlias{"YAMS_DISABLE_VECTOR", "0"};
    yams::test::ScopedEnvVar enableVectorDb{"YAMS_DISABLE_VECTOR_DB", "0"};
    yams::test::ScopedEnvVar initializeVectors{"YAMS_SQLITE_VEC_SKIP_INIT", "0"};
    yams::test::SpdlogLevelGuard logLevel;
    yams::test::TempDirGuard temp{"embedding_service_derivation_"};
    std::filesystem::path metadataPath;
    std::unique_ptr<metadata::ConnectionPool> pool;
    std::shared_ptr<DerivationRaceRepository> repo;
    std::shared_ptr<vector::VectorDatabase> vectors;
    std::shared_ptr<DerivationTestProvider> provider{std::make_shared<DerivationTestProvider>()};
    std::shared_ptr<api::IContentStore> contentStore{std::make_shared<MissingContentStore>()};
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
        repo = std::make_shared<DerivationRaceRepository>(
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
        if (!runner.valid()) {
            writer->start();
            runner = std::async(std::launch::async, [this] { io.run(); });
        }
        EmbeddingServiceTimingTestAccess::process(*service, std::move(job));
        REQUIRE(writer->flush(5s).has_value());
    }

    Result<yams::repair::EmbeddingRepairStats> repairOnce(bool skipExisting = false) {
        yams::repair::EmbeddingRepairConfig config;
        config.batchSize = 1;
        config.skipExisting = skipExisting;
        config.dataPath = temp.path();
        return yams::repair::repairMissingEmbeddings(contentStore, repo, provider, "test-model",
                                                     config, {hash});
    }
};

} // namespace

// Hash-only repair must bound retained input by completing one document before
// fetching the next. The repository callback observes the real production seam.
TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Hash repair fences A before reading B and preserves full chunk payloads",
                 "[daemon][embedding][embed-retention-red]") {
    yams::test::ScopedEnvVar fullMode{"YAMS_EMBED_SELECTION_MODE", "full"};
    const std::string secondHash(64, 'b');
    const std::vector<std::string> hashes{hash, secondHash};
    std::vector<std::string> expectedTexts;
    std::vector<std::string> expectedIds;
    int64_t secondId = 0;
    const auto policy = ConfigResolver::resolveEmbeddingChunkingPolicy();
    auto chunker = vector::createChunker(policy.strategy, policy.config, nullptr);
    REQUIRE(chunker);
    for (std::size_t index = 0; index < hashes.size(); ++index) {
        std::string text;
        for (int paragraph = 0; paragraph < 40; ++paragraph) {
            text += "Document " + std::to_string(index) + " paragraph " +
                    std::to_string(paragraph) + ": " + std::string(240, 'x' + index) + ".\n\n";
        }
        metadata::DocumentInfo doc;
        if (index == 0) {
            auto found = repo->getDocumentByHash(hash);
            REQUIRE(found);
            REQUIRE(found.value());
            doc = *found.value();
            auto content = repo->getContent(doc.id);
            REQUIRE(content);
            REQUIRE(content.value());
            content.value()->contentText = text;
            REQUIRE(repo->updateContent(*content.value()));
        } else {
            doc.sha256Hash = secondHash;
            doc.filePath = "/second.txt";
            doc.fileName = "second.txt";
            doc.mimeType = "text/plain";
            auto inserted = repo->insertDocument(doc);
            REQUIRE(inserted);
            secondId = inserted.value();
            metadata::BatchContentEntry content;
            content.documentId = secondId;
            content.contentText = text;
            content.title = doc.fileName;
            content.mimeType = doc.mimeType;
            content.extractionMethod = "test";
            content.language = "en";
            REQUIRE(repo->batchInsertContentAndIndex({content}));
        }
        auto chunks = chunker->chunkDocument(text, hashes[index]);
        REQUIRE(chunks.size() > 1);
        for (const auto& chunk : chunks) {
            expectedTexts.push_back(chunk.content);
            expectedIds.push_back(chunk.chunk_id);
        }
    }
    // Start the actual persistence owner before processing; no sleep or polling.
    writer->start();
    runner = std::async(std::launch::async, [this] { io.run(); });
    bool readB = false;
    repo->beforeContent = [&](int64_t id) {
        if (id != secondId) {
            return;
        }
        readB = true;
        CHECK(provider->calls > 0);
        auto states = repo->batchGetDocumentEmbeddingDerivations({hash});
        REQUIRE(states);
        REQUIRE(states.value().contains(hash));
        CHECK(states.value().at(hash).completed);
        CHECK_FALSE(vectors->getVectorsByDocument(hash).empty());
    };
    InternalEventBus::EmbedJob job;
    job.hashes = hashes;
    job.modelName = "test-model";
    job.skipExisting = false;
    job.updateSemanticGraph = false;
    EmbeddingServiceTimingTestAccess::process(*service, std::move(job));
    repo->beforeContent = {};
    REQUIRE(writer->flush(5s));
    CHECK(readB);
    CHECK(provider->inferredTexts == expectedTexts);
    std::vector<std::string> actualIds;
    for (const auto& documentHash : hashes) {
        for (const auto& record : vectors->getVectorsByDocument(documentHash)) {
            if (record.level == vector::EmbeddingLevel::CHUNK) {
                actualIds.push_back(record.chunk_id);
            }
        }
    }
    std::sort(actualIds.begin(), actualIds.end());
    std::sort(expectedIds.begin(), expectedIds.end());
    CHECK(actualIds == expectedIds);
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Hash repair cancellation after A inference leaves B unread and retryable",
                 "[daemon][embedding][embed-retention-red]") {
    const std::string secondHash(64, 'b');
    metadata::DocumentInfo doc;
    doc.sha256Hash = secondHash;
    doc.filePath = "/cancel-second.txt";
    doc.fileName = "cancel-second.txt";
    doc.mimeType = "text/plain";
    auto inserted = repo->insertDocument(doc);
    REQUIRE(inserted);
    metadata::BatchContentEntry content;
    content.documentId = inserted.value();
    content.contentText = "Second document must not be read after cancellation.";
    content.title = doc.fileName;
    content.mimeType = doc.mimeType;
    content.extractionMethod = "test";
    content.language = "en";
    REQUIRE(repo->batchInsertContentAndIndex({content}));
    bool readB = false;
    repo->beforeContent = [&](int64_t id) { readB |= id == inserted.value(); };
    auto monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
    provider->beforeFailure = [monitor] { monitor->cancelRequested.store(true); };
    writer->start();
    runner = std::async(std::launch::async, [this] { io.run(); });
    InternalEventBus::EmbedJob job;
    job.hashes = {hash, secondHash};
    job.modelName = "test-model";
    job.skipExisting = false;
    job.updateSemanticGraph = false;
    job.monitor = monitor;
    EmbeddingServiceTimingTestAccess::process(*service, std::move(job));
    repo->beforeContent = {};
    REQUIRE(writer->flush(5s));
    CHECK_FALSE(readB);
    auto states = repo->batchGetDocumentEmbeddingDerivations({hash, secondHash});
    REQUIRE(states);
    REQUIRE(states.value().contains(hash));
    CHECK(states.value().at(hash).completed);
    REQUIRE(states.value().contains(secondHash));
    CHECK_FALSE(states.value().at(secondHash).completed);
    // A fresh attempt can still finish the unread work, without reminting A.
    const auto firstCalls = provider->calls;
    InternalEventBus::EmbedJob retry;
    retry.hashes = {secondHash};
    retry.modelName = "test-model";
    retry.updateSemanticGraph = false;
    EmbeddingServiceTimingTestAccess::process(*service, std::move(retry));
    REQUIRE(writer->flush(5s));
    CHECK(provider->calls > firstCalls);
    states = repo->batchGetDocumentEmbeddingDerivations({hash, secondHash});
    REQUIRE(states);
    CHECK(states.value().at(hash).completed);
    CHECK(states.value().at(secondHash).completed);
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Embedding exception releases owned unfinished attempts and permits retry",
                 "[daemon][embedding][embed-exception-red]") {
    bool prepared = false;
    bool supersede = false;
    SECTION("hash-only exception after committed A") {}
    SECTION("prepared post-ingest inference exception") {
        prepared = true;
    }
    SECTION("hash-only exception must preserve newer completed B") {
        supersede = true;
    }

    const std::vector<std::string> hashes{hash, std::string(64, 'b'), std::string(64, 'c')};
    for (std::size_t i = 1; i < hashes.size(); ++i) {
        metadata::DocumentInfo doc;
        doc.sha256Hash = hashes[i];
        doc.filePath = "/exception-" + std::to_string(i) + ".txt";
        doc.fileName = "exception-" + std::to_string(i) + ".txt";
        doc.mimeType = "text/plain";
        auto inserted = repo->insertDocument(doc);
        REQUIRE(inserted);
        metadata::BatchContentEntry content;
        content.documentId = inserted.value();
        content.contentText = "Deterministic exception recovery document " + std::to_string(i);
        content.title = doc.fileName;
        content.mimeType = doc.mimeType;
        content.extractionMethod = "test";
        content.language = "en";
        REQUIRE(repo->batchInsertContentAndIndex({content}));
    }
    InternalEventBus::EmbedJob job;
    job.hashes = hashes;
    job.modelName = "test-model";
    job.skipExisting = false;
    job.updateSemanticGraph = false;
    auto monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
    job.monitor = monitor;
    if (prepared) {
        const auto chunking = ConfigResolver::resolveEmbeddingChunkingPolicy();
        const auto selection = ConfigResolver::resolveEmbeddingSelectionPolicy();
        auto chunker = vector::createChunker(chunking.strategy, chunking.config, nullptr);
        REQUIRE(chunker);
        for (const auto& h : hashes) {
            auto doc = repo->getDocumentByHash(h);
            REQUIRE(doc);
            REQUIRE(doc.value());
            auto content = repo->getContent(doc.value()->id);
            REQUIRE(content);
            REQUIRE(content.value());
            embed::EmbedSourceDoc source{h, content.value()->contentText, doc.value()->fileName,
                                         doc.value()->filePath, doc.value()->mimeType};
            auto payload = embed::prepareEmbedPreparedDoc(source, *chunker, selection);
            REQUIRE(payload);
            payload->preparationRecipe = embed::embeddingPreparationRecipe(chunking, selection);
            job.preparedDocs.push_back(std::move(*payload));
        }
    }
    // RepairService marks queued embedding work Processing before dispatch. Content
    // insertion alone marks extraction Completed, which would miss the stranded state.
    REQUIRE(repo->batchUpdateDocumentRepairStatuses(hashes, metadata::RepairStatus::Processing));
    std::vector<metadata::EmbeddingDerivationToken> admitted;
    std::string completedGeneration;
    std::string newerGeneration;
    bool injected = false;
    auto inject = [&] {
        auto states = repo->batchGetDocumentEmbeddingDerivations(hashes);
        REQUIRE(states);
        for (const auto& h : hashes) {
            REQUIRE(states.value().contains(h));
            admitted.push_back(states.value().at(h).token);
        }
        if (!prepared) {
            REQUIRE(states.value().at(hash).completed);
            completedGeneration = states.value().at(hash).token.generation;
            REQUIRE_FALSE(vectors->getVectorsByDocument(hash).empty());
        }
        if (supersede) {
            auto newer = repo->beginDocumentEmbeddingDerivation(hashes[1], "newer-recipe");
            REQUIRE(newer);
            newerGeneration = newer.value().generation;
            auto completed = repo->completeDocumentEmbeddingDerivation(newer.value(), "test-model");
            REQUIRE(completed);
            REQUIRE(completed.value());
        }
        injected = true;
        throw std::bad_alloc{};
    };
    if (prepared) {
        provider->beforeFailure = inject;
    } else {
        // The callback is exchanged before invocation: arm a throw on B, after A's fence.
        provider->beforeFailure = [&] { provider->beforeFailure = inject; };
    }
    writer->start();
    runner = std::async(std::launch::async, [this] { io.run(); });
    try {
        EmbeddingServiceTimingTestAccess::process(*service, std::move(job));
    } catch (const std::bad_alloc&) {
        // Dispatch already catches this exception; swallowing is not the contract here.
        SUCCEED("bad_alloc reached the existing dispatch exception boundary");
    }
    REQUIRE(writer->flush(5s));
    REQUIRE(injected);
    auto states = repo->batchGetDocumentEmbeddingDerivations(hashes);
    REQUIRE(states);
    for (std::size_t i = 0; i < hashes.size(); ++i) {
        INFO("hash index=" << i << " prepared=" << prepared << " supersede=" << supersede);
        auto doc = repo->getDocumentByHash(hashes[i]);
        REQUIRE(doc);
        REQUIRE(doc.value());
        INFO("repairStatus=" << metadata::RepairStatusUtils::toString(doc.value()->repairStatus));
        REQUIRE(states.value().contains(hashes[i]));
        if ((!prepared && i == 0) || (supersede && i == 1)) {
            CHECK(states.value().at(hashes[i]).completed);
            CHECK(states.value().at(hashes[i]).token.generation ==
                  (i == 0 ? completedGeneration : newerGeneration));
            CHECK(doc.value()->repairStatus == metadata::RepairStatus::Completed);
        } else {
            CHECK_FALSE(states.value().at(hashes[i]).completed);
            CHECK(states.value().at(hashes[i]).token.generation == admitted[i].generation);
            CHECK((doc.value()->repairStatus == metadata::RepairStatus::Pending ||
                   doc.value()->repairStatus == metadata::RepairStatus::Failed));
        }
    }
    CHECK(monitor->done);
    CHECK_FALSE(monitor->cancelRequested.load());
    CHECK(monitor->phase == "failed");
    CHECK(monitor->totalDocs == hashes.size());
    CHECK(monitor->processedDocs == hashes.size());
    CHECK(monitor->succeededDocs + monitor->failedDocs + monitor->skippedDocs == hashes.size());
    CHECK(monitor->succeededDocs == (prepared ? 0 : 1));
    CHECK(monitor->failedDocs == (prepared ? 3 : 2));
    CHECK(monitor->skippedDocs == 0);

    // Retry the throwing hash (or an unfinished hash when B was superseded), not a new hash.
    const auto retryHash = prepared ? hash : hashes[supersede ? 2 : 1];
    const auto callsBeforeRetry = provider->calls;
    InternalEventBus::EmbedJob retry;
    retry.hashes = {retryHash};
    retry.modelName = "test-model";
    retry.updateSemanticGraph = false;
    EmbeddingServiceTimingTestAccess::process(*service, std::move(retry));
    REQUIRE(writer->flush(5s));
    CHECK(provider->calls > callsBeforeRetry);
    states = repo->batchGetDocumentEmbeddingDerivations(hashes);
    REQUIRE(states);
    CHECK(states.value().at(retryHash).completed);
    const auto retryIndex = prepared ? 0 : (supersede ? 2 : 1);
    CHECK(states.value().at(retryHash).token.generation != admitted[retryIndex].generation);
    auto retried = repo->getDocumentByHash(retryHash);
    REQUIRE(retried);
    REQUIRE(retried.value());
    CHECK(retried.value()->repairStatus == metadata::RepairStatus::Completed);
    CHECK_FALSE(vectors->getVectorsByDocument(retryHash).empty());
    if (!prepared) {
        CHECK(states.value().at(hash).token.generation == completedGeneration);
    }
    if (supersede) {
        CHECK(states.value().at(hashes[1]).token.generation == newerGeneration);
        CHECK(states.value().at(hashes[1]).completed);
    }
}

namespace {
struct ThrowingCopyPayload {
    int value{42};
    ThrowingCopyPayload() = default;
    ThrowingCopyPayload(const ThrowingCopyPayload&) { throw std::bad_alloc{}; }
    ThrowingCopyPayload& operator=(const ThrowingCopyPayload&) { throw std::bad_alloc{}; }
    ThrowingCopyPayload(ThrowingCopyPayload&&) noexcept = default;
    ThrowingCopyPayload& operator=(ThrowingCopyPayload&&) noexcept = default;
};
} // namespace

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Metadata query hands off committed payload without copying",
                 "[daemon][embedding][embed-exception-review][metadata-query]") {
    bool write = false;
    SECTION("read query") {}
    SECTION("write query") {
        write = true;
    }
    int calls = 0;
    auto operation = [&](metadata::Database& db) -> Result<ThrowingCopyPayload> {
        ++calls;
        REQUIRE(db.execute("CREATE TABLE IF NOT EXISTS exception_commit_probe(value INTEGER)"));
        REQUIRE(db.execute("BEGIN IMMEDIATE"));
        REQUIRE(db.execute("INSERT INTO exception_commit_probe VALUES (42)"));
        REQUIRE(db.execute("COMMIT"));
        return ThrowingCopyPayload{};
    };
    REQUIRE_NOTHROW([&] {
        auto result = metadata::MetadataRepositoryTestAccess::query<ThrowingCopyPayload>(
            *repo, write, operation);
        REQUIRE(result);
        CHECK(result.value().value == 42);
    }());
    CHECK(calls == 1);
    auto count = metadata::MetadataRepositoryTestAccess::query<int>(
        *repo, false, [](metadata::Database& db) -> Result<int> {
            auto statement = db.prepare("SELECT COUNT(*) FROM exception_commit_probe");
            if (!statement)
                return statement.error();
            auto row = statement.value().step();
            if (!row)
                return row.error();
            return statement.value().getInt(0);
        });
    REQUIRE(count);
    CHECK(count.value() == 1);
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Embedding post-ACK exceptions preserve successes and skips",
                 "[daemon][embedding][embed-exception-review]") {
    bool prepared = false;
    bool duplicate = false;
    bool emptyFirst = false;
    bool alreadyComplete = false;
    bool withMonitor = true;
    SECTION("hash post ACK") {}
    SECTION("prepared post ACK") {
        prepared = true;
    }
    SECTION("duplicate post ACK") {
        duplicate = true;
    }
    SECTION("duplicate without monitor") {
        duplicate = true;
        withMonitor = false;
    }
    SECTION("already completed A then post-ACK throwing B") {
        alreadyComplete = true;
    }
    SECTION("empty A then throwing B") {
        emptyFirst = true;
    }
    SECTION("empty A then throwing B without monitor") {
        emptyFirst = true;
        withMonitor = false;
    }
    InternalEventBus::EmbedJob job;
    job.hashes = {hash};
    job.modelName = "test-model";
    job.skipExisting = false;
    job.updateSemanticGraph = false;
    auto monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
    if (withMonitor)
        job.monitor = monitor;
    if (duplicate)
        job.hashes.push_back(hash);
    if (alreadyComplete) {
        process();
        job.skipExisting = true;
    }
    if (emptyFirst || alreadyComplete) {
        const std::string second(64, 'b');
        metadata::DocumentInfo doc;
        doc.sha256Hash = second;
        doc.filePath = "/throw-second.txt";
        doc.fileName = "throw-second.txt";
        doc.mimeType = "text/plain";
        auto id = repo->insertDocument(doc);
        REQUIRE(id);
        metadata::BatchContentEntry entry;
        entry.documentId = id.value();
        entry.contentText = "Throw on inference after an empty document.";
        entry.title = doc.fileName;
        entry.mimeType = doc.mimeType;
        entry.extractionMethod = "test";
        REQUIRE(repo->batchInsertContentAndIndex({entry}));
        if (emptyFirst) {
            auto first = repo->getDocumentByHash(hash);
            REQUIRE(first);
            REQUIRE(first.value());
            auto content = repo->getContent(first.value()->id);
            REQUIRE(content);
            REQUIRE(content.value());
            content.value()->contentText.clear();
            REQUIRE(repo->updateContent(*content.value()));
            provider->beforeFailure = [] { throw std::bad_alloc{}; };
        }
        job.hashes.push_back(second);
    }
    if (!emptyFirst) {
        service->setEmbeddingsCommittedCallback([&](const auto& hashes) {
            auto states = repo->batchGetDocumentEmbeddingDerivations(hashes);
            REQUIRE(states);
            for (const auto& h : hashes)
                CHECK(states.value().at(h).completed);
            throw std::bad_alloc{};
        });
    }
    if (prepared) {
        const auto chunking = ConfigResolver::resolveEmbeddingChunkingPolicy();
        const auto selection = ConfigResolver::resolveEmbeddingSelectionPolicy();
        auto chunker = vector::createChunker(chunking.strategy, chunking.config, nullptr);
        auto doc = repo->getDocumentByHash(hash);
        REQUIRE(doc);
        REQUIRE(doc.value());
        auto content = repo->getContent(doc.value()->id);
        REQUIRE(content);
        REQUIRE(content.value());
        embed::EmbedSourceDoc source{hash, content.value()->contentText, doc.value()->fileName,
                                     doc.value()->filePath, doc.value()->mimeType};
        auto payload = embed::prepareEmbedPreparedDoc(source, *chunker, selection);
        REQUIRE(payload);
        payload->preparationRecipe = embed::embeddingPreparationRecipe(chunking, selection);
        job.preparedDocs.push_back(std::move(*payload));
    }
    if (!runner.valid()) {
        writer->start();
        runner = std::async(std::launch::async, [this] { io.run(); });
    }
    REQUIRE_THROWS_AS(EmbeddingServiceTimingTestAccess::process(*service, std::move(job)),
                      std::bad_alloc);
    REQUIRE(writer->flush(5s));
    CHECK(service->failed() == (emptyFirst ? 1 : 0));
    const auto expectedProcessed = emptyFirst ? 0u : (alreadyComplete ? 2u : 1u);
    CHECK(service->processed() == expectedProcessed);
    if (emptyFirst) {
        auto skippedDoc = repo->getDocumentByHash(hash);
        REQUIRE(skippedDoc);
        REQUIRE(skippedDoc.value());
        CHECK(skippedDoc.value()->repairStatus == metadata::RepairStatus::Skipped);
        auto failedDoc = repo->getDocumentByHash(std::string(64, 'b'));
        REQUIRE(failedDoc);
        REQUIRE(failedDoc.value());
        CHECK(failedDoc.value()->repairStatus == metadata::RepairStatus::Failed);
        process();
        skippedDoc = repo->getDocumentByHash(hash);
        REQUIRE(skippedDoc);
        REQUIRE(skippedDoc.value());
        CHECK(skippedDoc.value()->repairStatus == metadata::RepairStatus::Skipped);
        CHECK(service->processed() == expectedProcessed);
    }
    if (!emptyFirst) {
        auto states = repo->batchGetDocumentEmbeddingDerivations({hash});
        REQUIRE(states);
        CHECK(states.value().at(hash).completed);
        auto doc = repo->getDocumentByHash(hash);
        REQUIRE(doc);
        REQUIRE(doc.value());
        CHECK(doc.value()->repairStatus == metadata::RepairStatus::Completed);
        service->setEmbeddingsCommittedCallback({});
        process();
        CHECK(service->processed() == expectedProcessed);
    }
    if (withMonitor) {
        CHECK(monitor->done);
        CHECK(monitor->phase == "failed");
        CHECK(monitor->succeededDocs == (emptyFirst ? 0 : (alreadyComplete ? 2 : 1)));
        CHECK(monitor->failedDocs == (emptyFirst ? 1 : 0));
        CHECK(monitor->succeededDocs + monitor->failedDocs + monitor->skippedDocs ==
              monitor->totalDocs);
    }
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Prepared bulk partial ACK preserves actual successes and newer tokens",
                 "[daemon][embedding][embed-exception-review]") {
    const std::string second(64, 'b');
    metadata::DocumentInfo doc;
    doc.sha256Hash = second;
    doc.filePath = "/partial-second.txt";
    doc.fileName = "partial-second.txt";
    doc.mimeType = "text/plain";
    auto id = repo->insertDocument(doc);
    REQUIRE(id);
    metadata::BatchContentEntry entry;
    entry.documentId = id.value();
    entry.contentText = "Second prepared bulk document.";
    entry.title = doc.fileName;
    entry.mimeType = doc.mimeType;
    entry.extractionMethod = "test";
    REQUIRE(repo->batchInsertContentAndIndex({entry}));
    InternalEventBus::EmbedJob job;
    job.hashes = {hash, second};
    job.modelName = "test-model";
    job.skipExisting = false;
    job.updateSemanticGraph = false;
    auto monitor = std::make_shared<InternalEventBus::EmbedJobMonitor>();
    job.monitor = monitor;
    const auto chunking = ConfigResolver::resolveEmbeddingChunkingPolicy();
    const auto selection = ConfigResolver::resolveEmbeddingSelectionPolicy();
    auto chunker = vector::createChunker(chunking.strategy, chunking.config, nullptr);
    for (const auto& h : job.hashes) {
        auto found = repo->getDocumentByHash(h);
        REQUIRE(found);
        REQUIRE(found.value());
        auto content = repo->getContent(found.value()->id);
        REQUIRE(content);
        REQUIRE(content.value());
        embed::EmbedSourceDoc source{h, content.value()->contentText, found.value()->fileName,
                                     found.value()->filePath, found.value()->mimeType};
        auto payload = embed::prepareEmbedPreparedDoc(source, *chunker, selection);
        REQUIRE(payload);
        payload->preparationRecipe = embed::embeddingPreparationRecipe(chunking, selection);
        job.preparedDocs.push_back(std::move(*payload));
    }
    std::string newerGeneration;
    provider->beforeFailure = [&] {
        auto newer = repo->beginDocumentEmbeddingDerivation(second, "superseding-recipe");
        REQUIRE(newer);
        newerGeneration = newer.value().generation;
        auto completed = repo->completeDocumentEmbeddingDerivation(newer.value(), "test-model");
        REQUIRE(completed);
        REQUIRE(completed.value());
    };
    bool notified = false;
    service->setEmbeddingsCommittedCallback([&](const auto&) { notified = true; });
    writer->start();
    runner = std::async(std::launch::async, [this] { io.run(); });
    EmbeddingServiceTimingTestAccess::process(*service, std::move(job));
    REQUIRE(writer->flush(5s));
    CHECK(provider->calls == 1);
    CHECK(service->processed() == 1);
    CHECK_FALSE(notified);
    CHECK(service->failed() == 1);
    CHECK(monitor->done);
    CHECK(monitor->phase == "failed");
    CHECK(monitor->succeededDocs == 1);
    CHECK(monitor->failedDocs == 1);
    CHECK(monitor->processedDocs == 2);
    auto states = repo->batchGetDocumentEmbeddingDerivations({hash, second});
    REQUIRE(states);
    CHECK(states.value().at(hash).completed);
    CHECK(states.value().at(second).completed);
    CHECK(states.value().at(second).token.generation == newerGeneration);
    for (const auto& h : {hash, second}) {
        auto found = repo->getDocumentByHash(h);
        REQUIRE(found);
        REQUIRE(found.value());
        CHECK(found.value()->repairStatus == metadata::RepairStatus::Completed);
    }
}

TEST_CASE("Derivation fixture restores vector-disabled CI environment",
          "[daemon][embedding][service-derivation][environment]") {
    yams::test::ScopedEnvVar disabled{"YAMS_DISABLE_VECTORS", "1"};
    yams::test::ScopedEnvVar aliasDisabled{"YAMS_DISABLE_VECTOR", "1"};
    yams::test::ScopedEnvVar dbDisabled{"YAMS_DISABLE_VECTOR_DB", "1"};
    yams::test::ScopedEnvVar skipInit{"YAMS_SQLITE_VEC_SKIP_INIT", "1"};
    {
        ServiceDerivationFixture fixture;
        fixture.process();
        REQUIRE_FALSE(fixture.vectors->getVectorsByDocument(fixture.hash).empty());
    }
    for (const auto* key : {"YAMS_DISABLE_VECTORS", "YAMS_DISABLE_VECTOR", "YAMS_DISABLE_VECTOR_DB",
                            "YAMS_SQLITE_VEC_SKIP_INIT"}) {
        INFO(key);
        REQUIRE(yams::config::getenv_optional(key) == std::optional<std::string>{"1"});
    }
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Standalone embedding repair does not skip stale physical vectors",
                 "[repair][embedding][derivation][standalone]") {
    process();
    REQUIRE_FALSE(vectors->getVectorsByDocument(hash).empty());
    auto document = repo->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    REQUIRE(document.value().has_value());
    auto content = repo->getContent(document.value()->id);
    REQUIRE(content.has_value());
    REQUIRE(content.value().has_value());
    content.value()->contentText = "Changed text with stale physical vectors still present.";
    REQUIRE(repo->updateContent(*content.value()).has_value());
    auto repaired = repairOnce(true);
    REQUIRE(repaired.has_value());
    CHECK(repaired.value().embeddingsGenerated > 0);
    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    CHECK(ready.value());
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Standalone embedding repair completes the current derivation",
                 "[repair][embedding][derivation][standalone]") {
    auto repaired = repairOnce();
    REQUIRE(repaired.has_value());
    CHECK(repaired.value().embeddingsGenerated > 0);

    auto states = repo->batchGetDocumentEmbeddingDerivations({hash});
    REQUIRE(states.has_value());
    REQUIRE(states.value().contains(hash));
    CHECK(states.value().at(hash).completed);

    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    CHECK(ready.value());
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Standalone embedding repair does not restore readiness after content changes",
                 "[repair][embedding][derivation][standalone][content-race]") {
    auto document = repo->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    REQUIRE(document.value().has_value());
    const auto id = document.value()->id;
    bool mutationRan = false;
    provider->beforeFailure = [&] {
        mutationRan = true;
        metadata::BatchContentEntry replacement;
        replacement.documentId = id;
        replacement.title = "derivation.txt";
        replacement.contentText = "Replacement text committed while repair inference is active.";
        replacement.mimeType = "text/plain";
        replacement.extractionMethod = "test";
        replacement.language = "en";
        REQUIRE(repo->batchInsertContentAndIndex({replacement}).has_value());
    };

    auto repaired = repairOnce();
    REQUIRE(repaired.has_value());
    REQUIRE(mutationRan);
    REQUIRE(provider->calls > 0);

    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    CHECK_FALSE(ready.value());
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "EmbeddingService rechecks skip readiness after gathering document identity",
                 "[daemon][embedding][service-derivation][skip-race]") {
    process();
    const auto calls = provider->calls;
    auto document = repo->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    REQUIRE(document.value().has_value());
    const auto id = document.value()->id;
    repo->beforeLookup = [&] {
        metadata::BatchContentEntry content;
        content.documentId = id;
        content.title = "derivation.txt";
        content.contentText = "A replacement extraction after the cached readiness snapshot.";
        content.mimeType = "text/plain";
        content.extractionMethod = "test";
        content.language = "en";
        REQUIRE(repo->batchInsertContentAndIndex({content}).has_value());
    };
    process();
    CHECK(provider->calls > calls);
    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    CHECK(ready.value());
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "EmbeddingService pre-admission provider failure persists terminal repair status",
                 "[daemon][embedding][service-derivation][pre-admission-failure]") {
    SECTION("without an existing derivation") {
        auto states = repo->batchGetDocumentEmbeddingDerivations({hash});
        REQUIRE(states.has_value());
        REQUIRE_FALSE(states.value().contains(hash));
    }
    SECTION("with a current pending derivation") {
        auto token = repo->beginDocumentEmbeddingDerivation(hash, "pending-recipe");
        REQUIRE(token.has_value());
        auto states = repo->batchGetDocumentEmbeddingDerivations({hash});
        REQUIRE(states.has_value());
        REQUIRE(states.value().contains(hash));
        REQUIRE_FALSE(states.value().at(hash).completed);
    }

    REQUIRE(repo->updateDocumentRepairStatus(hash, metadata::RepairStatus::Processing).has_value());
    service->setProviders([]() -> std::shared_ptr<IModelProvider> { return {}; },
                          [] { return "test-model"; }, [this] { return vectors; });

    process();

    auto document = repo->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    REQUIRE(document.value().has_value());
    CHECK(document.value()->repairStatus == metadata::RepairStatus::Failed);
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "EmbeddingService pre-admission failure preserves completed attempts",
                 "[daemon][embedding][service-derivation][pre-admission-failure]") {
    process();
    bool completeInProvider = false;
    SECTION("already completed attempt is retained") {}
    SECTION("newer attempt completes during provider preparation") {
        REQUIRE(repo->beginDocumentEmbeddingDerivation(hash, "pending").has_value());
        completeInProvider = true;
    }
    service->setProviders(
        [&]() -> std::shared_ptr<IModelProvider> {
            if (completeInProvider) {
                auto newer = repo->beginDocumentEmbeddingDerivation(hash, "newer");
                REQUIRE(newer.has_value());
                auto completed =
                    repo->completeDocumentEmbeddingDerivation(newer.value(), "test-model");
                REQUIRE(completed.has_value());
                REQUIRE(completed.value());
            }
            return {};
        },
        [] { return "test-model"; }, [this] { return vectors; });
    process();
    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    CHECK(ready.value());
    auto document = repo->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    REQUIRE(document.value().has_value());
    CHECK(document.value()->repairStatus == metadata::RepairStatus::Completed);
}

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "EmbeddingService late failure preserves a newer completed derivation",
                 "[daemon][embedding][service-derivation][late-failure]") {
    process();
    const auto records = vectors->getVectorsByDocument(hash);
    REQUIRE_FALSE(records.empty());
    provider->version = "v2";
    provider->beforeFailure = [&] {
        // A newer attempt commits while the old attempt is still in inference. This
        // synchronous boundary reproduces the interleaving without threads or sleeps.
        auto newer = repo->beginDocumentEmbeddingDerivation(hash, "newer-recipe");
        REQUIRE(newer.has_value());
        auto complete = repo->completeDocumentEmbeddingDerivation(newer.value(), "test-model");
        REQUIRE(complete.has_value());
        REQUIRE(complete.value());
    };
    provider->fail = true;
    process();
    CHECK(vectors->getVectorsByDocument(hash).size() == records.size());
    auto ready = repo->hasDocumentEmbeddingByHash(hash);
    REQUIRE(ready.has_value());
    CHECK(ready.value());
    auto document = repo->getDocumentByHash(hash);
    REQUIRE(document.has_value());
    REQUIRE(document.value().has_value());
    CHECK(document.value()->repairStatus == metadata::RepairStatus::Completed);
}

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

TEST_CASE_METHOD(ServiceDerivationFixture,
                 "Semantic backfill bounds identical-vector cosine before adaptive thresholds",
                 "[daemon][embedding][semantic-startup]") {
    yams::test::ScopedEnvVar topK{"YAMS_GRAPH_SEMANTIC_TOPK", "8"};
    yams::test::ScopedEnvVar threshold{"YAMS_GRAPH_SEMANTIC_THRESHOLD", std::nullopt};
    auto store = metadata::makeSqliteKnowledgeGraphStore(*pool, {});
    REQUIRE(store.has_value());
    std::shared_ptr<metadata::KnowledgeGraphStore> kg(std::move(store.value()));
    writer = std::make_unique<WriteCoordinator>(io, kg, repo);
    service->setProviders([this] { return provider; }, [] { return "test-model"; },
                          [this] { return vectors; }, [kg] { return kg; });

    // Dyadic components and a fixed integer generator make this portable and replayable.
    // With the old float inverse norm this legal vector's self-cosine rounds above one.
    std::uint32_t seed = 50;
    std::vector<float> embedding;
    double squaredNorm = 0;
    for (std::size_t i = 0; i < 64; ++i) {
        seed = seed * 1664525U + 1013904223U;
        const auto component = static_cast<float>(seed & 65535U) / 32768.0f - 1.0f;
        embedding.push_back(component);
        squaredNorm += static_cast<double>(component) * component;
    }
    const auto oldInverseNorm = static_cast<float>(1.0 / std::sqrt(squaredNorm));
    REQUIRE(static_cast<float>(squaredNorm * oldInverseNorm * oldInverseNorm) > 1.0f);

    std::vector<metadata::KGNode> nodes;
    for (std::size_t i = 0; i < 9; ++i) {
        std::string documentHash(64, 'a');
        documentHash.back() = static_cast<char>('0' + i);
        vector::VectorRecord record;
        record.chunk_id = documentHash;
        record.document_hash = documentHash;
        record.embedding = embedding;
        record.level = vector::EmbeddingLevel::DOCUMENT;
        record.metadata["path"] = "/semantic/" + std::to_string(i);
        REQUIRE(vectors->insertVector(record));
        nodes.push_back(metadata::KGNode{
            .nodeKey = "doc:" + documentHash, .label = documentHash, .type = "document"});
    }
    auto ids = kg->upsertNodes(nodes);
    REQUIRE(ids.has_value());
    writer->start();
    runner = std::async(std::launch::async, [this] { io.run(); });

    SECTION("bounded startup backfill") {
        auto filled = service->backfillSemanticNeighborGraph("test-model", nodes.size());
        REQUIRE(filled.has_value());
        CHECK(filled.value() == nodes.size());
    }
    SECTION("explicit corpus rebuild uses the same scoring boundary") {
        auto rebuilt = service->rebuildSemanticNeighborGraphForCorpus("test-model");
        REQUIRE(rebuilt.has_value());
        CHECK(rebuilt.value() == nodes.size() * 8);
    }
    REQUIRE(writer->flush(5s).has_value());
    for (const auto id : ids.value()) {
        auto edges = kg->getEdgesFrom(id, std::string_view{"semantic_neighbor"});
        REQUIRE(edges.has_value());
        REQUIRE(edges.value().size() == 8);
        for (const auto& edge : edges.value()) {
            CHECK(std::isfinite(edge.weight));
            CHECK(edge.weight >= 0.0f);
            CHECK(edge.weight <= 1.0f);
        }
    }
}

TEST_CASE("Embedding maintenance requires actual startup readiness and stops borrowing on shutdown",
          "[daemon][embedding][startup-eligibility]") {
    WorkCoordinator coordinator;
    EmbeddingService service(nullptr, nullptr, &coordinator);
    CHECK_FALSE(EmbeddingServiceTimingTestAccess::maintenanceEligible(service, true));
    ServiceManagerState phase = ServiceManagerState::OpeningDatabase;
    std::size_t calls = 0;
    service.setStartupStateGetter([&] {
        ++calls;
        return phase;
    });
    for (auto state : {ServiceManagerState::OpeningDatabase, ServiceManagerState::DatabaseReady,
                       ServiceManagerState::MigratingSchema, ServiceManagerState::SchemaReady,
                       ServiceManagerState::VectorsReady, ServiceManagerState::BuildingSearchEngine,
                       ServiceManagerState::Failed, ServiceManagerState::ShuttingDown,
                       ServiceManagerState::Stopped}) {
        phase = state;
        CHECK_FALSE(EmbeddingServiceTimingTestAccess::maintenanceEligible(service, true));
    }
    phase = ServiceManagerState::Ready;
    CHECK(EmbeddingServiceTimingTestAccess::maintenanceEligible(service, true));
    const auto beforeBusy = calls;
    CHECK_FALSE(EmbeddingServiceTimingTestAccess::maintenanceEligible(service, false));
    CHECK(calls == beforeBusy);
    service.shutdown();
    CHECK_FALSE(EmbeddingServiceTimingTestAccess::maintenanceEligible(service, true));
    CHECK(calls == beforeBusy);
}

} // namespace yams::daemon

TEST_CASE("Prepared job dispatch respects a payload byte budget",
          "[daemon][embedding][embed-job-cap][memory]") {
    auto makeJob = [](std::size_t docs, std::size_t chunkBytes) {
        yams::daemon::InternalEventBus::EmbedJob job;
        job.modelName = "model";
        for (std::size_t i = 0; i < docs; ++i) {
            const std::string hash = "doc-" + std::to_string(i);
            job.hashes.push_back(hash);
            yams::daemon::InternalEventBus::EmbedPreparedDoc doc;
            doc.hash = hash;
            doc.sourceTextHash = "source";
            doc.preparationRecipe = "recipe";
            doc.chunks.push_back({hash + "-chunk", std::string(chunkBytes, 'x'), 0, chunkBytes});
            job.preparedDocs.push_back(std::move(doc));
        }
        return job;
    };

    SECTION("splits stay within the budget and lose no documents") {
        auto job = makeJob(5, 1000);
        const std::size_t budget = yams::daemon::embed::preparedPayloadBytes(job.preparedDocs) / 2;
        std::vector<std::string> seen;
        while (!job.hashes.empty()) {
            auto split = yams::daemon::embed::takeJobPrefix(job, 8, budget);
            REQUIRE_FALSE(split.hashes.empty());
            CHECK(yams::daemon::embed::preparedPayloadBytes(split.preparedDocs) <= budget);
            CHECK(split.preparedDocs.size() == split.hashes.size());
            seen.insert(seen.end(), split.hashes.begin(), split.hashes.end());
        }
        CHECK(seen.size() == 5);
        CHECK(seen.front() == "doc-0");
        CHECK(seen.back() == "doc-4");
    }

    SECTION("an oversized single document still makes progress") {
        auto job = makeJob(2, 4096);
        auto split = yams::daemon::embed::takeJobPrefix(job, 8, 16);
        CHECK(split.hashes.size() == 1);
        CHECK(split.preparedDocs.size() == 1);
        CHECK(job.hashes.size() == 1);
    }

    SECTION("a generous budget still honours the document cap") {
        auto job = makeJob(5, 100);
        auto split = yams::daemon::embed::takeJobPrefix(job, 2, 1u << 20);
        CHECK(split.hashes.size() == 2);
    }
}

TEST_CASE("Byte budget cannot be bypassed by prepared ordering or duplicates",
          "[daemon][embedding][embed-job-cap][memory]") {
    auto big = [](std::size_t bytes) { return std::string(bytes, 'x'); };

    SECTION("prepared docs in the wrong order are still counted") {
        yams::daemon::InternalEventBus::EmbedJob job;
        job.modelName = "model";
        job.hashes = {"a", "b"};
        for (const std::string hash : {"b", "a"}) {
            yams::daemon::InternalEventBus::EmbedPreparedDoc doc;
            doc.hash = hash;
            doc.chunks.push_back(
                {hash + "-chunk", big(static_cast<std::size_t>(40) * 1024 * 1024), 0, 1});
            job.preparedDocs.push_back(std::move(doc));
        }
        const std::size_t budget = static_cast<std::size_t>(64) * 1024 * 1024;
        auto split = yams::daemon::embed::takeJobPrefix(job, 8, budget);
        CHECK(split.hashes.size() == 1);
        CHECK(split.preparedDocs.size() == 1);
        CHECK(job.hashes.size() == 1);
    }

    SECTION("a duplicate prepared entry cannot hide a payload") {
        yams::daemon::InternalEventBus::EmbedJob job;
        job.modelName = "model";
        job.hashes = {"a"};
        for (int i = 0; i < 2; ++i) {
            yams::daemon::InternalEventBus::EmbedPreparedDoc doc;
            doc.hash = "a";
            doc.chunks.push_back(
                {"a-chunk", big(static_cast<std::size_t>(40) * 1024 * 1024), 0, 1});
            job.preparedDocs.push_back(std::move(doc));
        }
        auto index = yams::daemon::embed::preparedPayloadIndex(job);
        REQUIRE(index.count("a") == 1);
        CHECK(index.at("a") >= static_cast<std::size_t>(80) * 1024 * 1024);
    }
}
