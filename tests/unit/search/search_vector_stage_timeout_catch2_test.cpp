// Copyright 2025 The YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later

// Lifetime of the parallel vector stage when the component deadline fires. The vector work is
// posted to an executor; when `componentTimeout` elapses the search returns without it. The late
// completion must land in state the posted work owns, never in the locals of the frame that
// already returned. The late write is forced to happen after `searchWithResponse` returns by
// blocking the vector hydration step until the test releases it. A second search issued from the
// same call site then reuses that stack region, so under TSan a stage that still writes into the
// first frame races with the second search's locals; a clean sanitizer run is the pass condition.

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/asio/thread_pool.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_execution_context.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include "tests/common/metadata_test_db.h"
#include "tests/common/test_helpers_catch2.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

// NOLINTBEGIN(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)

namespace {

constexpr auto kWorkerWait = std::chrono::seconds(10);

class FixedEmbeddingBackend final : public vector::IEmbeddingBackend {
public:
    explicit FixedEmbeddingBackend(std::vector<float> embedding)
        : embedding_(std::move(embedding)) {}

    bool initialize() override {
        initialized_ = true;
        return true;
    }
    void shutdown() override { initialized_ = false; }
    bool isInitialized() const override { return initialized_; }
    Result<std::vector<float>> generateEmbedding(const std::string&) override { return embedding_; }
    Result<std::vector<std::vector<float>>>
    generateEmbeddings(std::span<const std::string> texts) override {
        return std::vector<std::vector<float>>(texts.size(), embedding_);
    }
    size_t getEmbeddingDimension() const override { return embedding_.size(); }
    size_t getMaxSequenceLength() const override { return 512; }
    std::string getBackendName() const override { return "fixed-test"; }
    std::string getEmbeddingSpaceIdentity() const override { return "test-space-v1"; }
    bool isAvailable() const override { return true; }
    vector::GenerationStats getStats() const override { return {}; }
    void resetStats() override {}

private:
    std::vector<float> embedding_;
    bool initialized_{false};
};

/// The vector stage hydrates its hits through `batchGetDocumentsByHash`. Once armed, the next
/// hydration blocks until the test releases it and signals when the base lookup has finished,
/// which is the moment the vector work resumes writing its results and counters.
class BlockingHydrationRepository final : public MetadataRepository {
public:
    explicit BlockingHydrationRepository(ConnectionPool& pool) : MetadataRepository(pool) {}

    Result<std::unordered_map<std::string, DocumentInfo>>
    batchGetDocumentsByHash(const std::vector<std::string>& hashes) override {
        if (armed_.exchange(false, std::memory_order_acq_rel)) {
            blockedThread_ = std::this_thread::get_id();
            reached_.set_value();
            release_.get_future().wait();
            auto result = MetadataRepository::batchGetDocumentsByHash(hashes);
            completed_.set_value();
            return result;
        }
        return MetadataRepository::batchGetDocumentsByHash(hashes);
    }

    void arm() { armed_.store(true, std::memory_order_release); }
    bool waitReached() {
        return reached_.get_future().wait_for(kWorkerWait) == std::future_status::ready;
    }
    void release() { release_.set_value(); }
    bool waitCompleted() {
        return completed_.get_future().wait_for(kWorkerWait) == std::future_status::ready;
    }
    std::thread::id blockedThread() const { return blockedThread_; }

private:
    std::atomic<bool> armed_{false};
    std::thread::id blockedThread_;
    std::promise<void> reached_;
    std::promise<void> release_;
    std::promise<void> completed_;
};

struct VectorTimeoutFixture {
    VectorTimeoutFixture()
        : disableVectors("YAMS_DISABLE_VECTORS", std::optional<std::string>("0")),
          skipVecInit("YAMS_SQLITE_VEC_SKIP_INIT", std::optional<std::string>("0")) {
        dbPath = yams::test::make_temp_sqlite_path("search_vector_stage_timeout_");
        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<BlockingHydrationRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());

        vector::VectorDatabaseConfig vectorConfig;
        vectorConfig.database_path = ":memory:";
        vectorConfig.embedding_dim = 2;
        vectorConfig.create_if_missing = true;
        vectorConfig.use_in_memory = true;
        vectorConfig.search_engine = vector::VectorSearchEngine::ExactScan;
        vectorDb = std::make_shared<vector::VectorDatabase>(vectorConfig);
        REQUIRE(vectorDb->initialize());

        addDocument("x1", "alpha one", {1.0F, 0.0F});
        addDocument("x2", "alpha two", {0.9F, 0.1F});
        addDocument("y1", "omega one", {0.0F, 1.0F});
    }

    ~VectorTimeoutFixture() {
        vectorDb.reset();
        kgStore.reset();
        repo.reset();
        if (pool) {
            pool->shutdown();
        }
        pool.reset();
        yams::test::remove_sqlite_artifacts(dbPath);
    }

    void addDocument(const std::string& hash, const std::string& content,
                     const std::vector<float>& embedding) {
        DocumentInfo info;
        info.filePath = "/tmp/" + hash + ".txt";
        info.fileName = hash + ".txt";
        info.fileExtension = ".txt";
        info.fileSize = static_cast<int64_t>(content.size());
        info.sha256Hash = hash;
        info.mimeType = "text/plain";
        info.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        info.modifiedTime = info.createdTime;
        info.indexedTime = info.createdTime;
        auto docId = repo->insertDocument(info);
        REQUIRE(docId.has_value());
        REQUIRE(repo->indexDocumentContent(docId.value(), hash, content, "text/plain").has_value());

        DocumentContent docContent;
        docContent.documentId = docId.value();
        docContent.contentText = content;
        docContent.contentLength = static_cast<int64_t>(content.size());
        docContent.extractionMethod = "test";
        docContent.language = "en";
        REQUIRE(repo->insertContent(docContent).has_value());

        vector::VectorRecord record;
        record.chunk_id = "doc-" + hash;
        record.document_hash = hash;
        record.embedding = embedding;
        record.level = vector::EmbeddingLevel::DOCUMENT;
        REQUIRE(vectorDb->insertVector(record));
    }

    static std::shared_ptr<vector::EmbeddingGenerator> makeGenerator() {
        vector::EmbeddingConfig embeddingConfig;
        embeddingConfig.embedding_dim = 2;
        auto generator = std::make_shared<vector::EmbeddingGenerator>(
            std::make_unique<FixedEmbeddingBackend>(std::vector<float>{1.0F, 0.0F}),
            embeddingConfig);
        REQUIRE(generator->initialize());
        return generator;
    }

    static SearchEngineConfig vectorOnlyConfig(std::chrono::milliseconds componentTimeout) {
        SearchEngineConfig config;
        config.includeDebugInfo = true;
        config.enableParallelExecution = true;
        config.textWeight = 0.0F;
        config.simeonTextWeight = 0.0F;
        config.pathTreeWeight = 0.0F;
        config.kgWeight = 0.0F;
        config.vectorWeight = 1.0F;
        config.entityVectorWeight = 0.0F;
        config.tagWeight = 0.0F;
        config.metadataWeight = 0.0F;
        config.vectorOnlyThreshold = 0.0F;
        config.vectorOnlyPenalty = 1.0F;
        config.vectorMaxResults = 4;
        config.tieredNarrowVectorSearch = false;
        config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::Disabled;
        config.componentTimeout = componentTimeout;
        return config;
    }

    yams::test::ScopedEnvVar disableVectors;
    yams::test::ScopedEnvVar skipVecInit;
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<BlockingHydrationRepository> repo;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
    std::shared_ptr<vector::VectorDatabase> vectorDb;
};

bool contains(const std::vector<std::string>& values, std::string_view needle) {
    return std::ranges::find(values, needle) != values.end();
}

} // namespace

TEST_CASE("Parallel vector stage: a timed-out component completes into its own state",
          "[search][vector][timeout][lifetime]") {
    VectorTimeoutFixture fix;
    auto generator = VectorTimeoutFixture::makeGenerator();

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    boost::asio::thread_pool workers(2);
    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore,
                        VectorTimeoutFixture::vectorOnlyConfig(std::chrono::milliseconds(50)));
    engine.setExecutor(workers.get_executor());

    fix.repo->arm();
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("alpha one", params);

    // The search returned without the vector component.
    REQUIRE(response.has_value());
    const auto& reported = response.value();
    CHECK(contains(reported.timedOutComponents, "vector"));
    CHECK_FALSE(contains(reported.contributingComponents, "vector"));
    CHECK(reported.results.empty());
    CHECK(engine.getStatistics().timedOutQueries.load() == 1);
    CHECK(engine.getStatistics().vectorQueries.load() == 0);

    // The vector work is parked inside hydration on an executor thread and the search frame is
    // gone. Release it and immediately run a second search from this frame: the second search's
    // locals occupy the same stack region, so a stage that still writes into the first frame now
    // writes into the second search. TSan reports that race; the fixed stage writes only into
    // state it owns.
    REQUIRE(fix.repo->waitReached());
    CHECK(fix.repo->blockedThread() != std::this_thread::get_id());
    fix.repo->release();
    engine.setConfig(VectorTimeoutFixture::vectorOnlyConfig(std::chrono::milliseconds(0)));
    auto second = engine.searchWithResponse("alpha one", params);
    REQUIRE(fix.repo->waitCompleted());
    workers.join();

    // The late completion is dropped; nothing retroactively changes what the first search
    // reported, and the second search is complete and correct.
    CHECK(contains(reported.timedOutComponents, "vector"));
    CHECK(reported.results.empty());
    REQUIRE(second.has_value());
    CHECK(second.value().timedOutComponents.empty());
    CHECK(contains(second.value().contributingComponents, "vector"));
    REQUIRE_FALSE(second.value().results.empty());
    CHECK(second.value().results.front().document.sha256Hash == "x1");
    CHECK(engine.getStatistics().vectorQueries.load() == 1);
    CHECK(engine.getStatistics().timedOutQueries.load() == 1);
}

TEST_CASE("Parallel vector stage: a component that meets the deadline is collected",
          "[search][vector][timeout][lifetime]") {
    VectorTimeoutFixture fix;
    auto generator = VectorTimeoutFixture::makeGenerator();

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    boost::asio::thread_pool workers(2);
    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore,
                        VectorTimeoutFixture::vectorOnlyConfig(std::chrono::milliseconds(0)));
    engine.setExecutor(workers.get_executor());

    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("alpha one", params);
    workers.join();

    REQUIRE(response.has_value());
    const auto& reported = response.value();
    CHECK(reported.timedOutComponents.empty());
    CHECK(contains(reported.contributingComponents, "vector"));
    REQUIRE_FALSE(reported.results.empty());
    CHECK(reported.results.front().document.sha256Hash == "x1");
    CHECK(engine.getStatistics().vectorQueries.load() == 1);
    CHECK(engine.getStatistics().timedOutQueries.load() == 0);
    // The stage's diagnostics reached the response, so the collected state was applied.
    CHECK(reported.debugStats.contains("vector_search_rows_visited_status"));
}

// NOLINTEND(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)
