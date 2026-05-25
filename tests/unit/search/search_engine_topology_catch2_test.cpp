#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_execution_context.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

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
    bool isAvailable() const override { return true; }
    vector::GenerationStats getStats() const override { return {}; }
    void resetStats() override {}

private:
    std::vector<float> embedding_;
    bool initialized_{false};
};

struct TopologySearchFixture {
    TopologySearchFixture() {
        dbPath = tempDbPath("search_topology_");
        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<MetadataRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());

        vector::VectorDatabaseConfig vectorConfig;
        vectorConfig.database_path = ":memory:";
        vectorConfig.embedding_dim = 2;
        vectorConfig.create_if_missing = true;
        vectorConfig.use_in_memory = true;
        vectorDb = std::make_shared<vector::VectorDatabase>(vectorConfig);
        REQUIRE(vectorDb->initialize());
    }

    ~TopologySearchFixture() {
        vectorDb.reset();
        kgStore.reset();
        repo.reset();
        if (pool) {
            pool->shutdown();
        }
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
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

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
    std::shared_ptr<vector::VectorDatabase> vectorDb;
};

} // namespace

TEST_CASE("SearchEngine topology routing narrows weak-query vector candidates",
          "[search][topology][catch2]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>("0"));
    yams::test::ScopedEnvVar skipVecInit("YAMS_SQLITE_VEC_SKIP_INIT",
                                         std::optional<std::string>("0"));

    TopologySearchFixture fix;
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});
    fix.addDocument("y1", "omega one", {0.0F, 1.0F});
    fix.addDocument("y2", "omega two", {0.1F, 0.9F});

    yams::topology::ConnectedComponentTopologyEngine topologyEngine;
    yams::topology::TopologyBuildConfig topologyConfig;
    topologyConfig.reciprocalOnly = true;
    topologyConfig.minEdgeScore = 0.5;
    std::vector<yams::topology::TopologyDocumentInput> topologyDocs{
        {.documentHash = "x1",
         .filePath = "/tmp/x1.txt",
         .embedding = {1.0F, 0.0F},
         .neighbors = {{.documentHash = "x2", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "x2",
         .filePath = "/tmp/x2.txt",
         .embedding = {0.9F, 0.1F},
         .neighbors = {{.documentHash = "x1", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "y1",
         .filePath = "/tmp/y1.txt",
         .embedding = {0.0F, 1.0F},
         .neighbors = {{.documentHash = "y2", .score = 0.9F, .reciprocal = true}}},
        {.documentHash = "y2",
         .filePath = "/tmp/y2.txt",
         .embedding = {0.1F, 0.9F},
         .neighbors = {{.documentHash = "y1", .score = 0.9F, .reciprocal = true}}},
    };
    auto topologyBatch = topologyEngine.buildArtifacts(topologyDocs, topologyConfig);
    REQUIRE(topologyBatch.has_value());
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(topologyBatch.value()).has_value());

    vector::EmbeddingConfig embeddingConfig;
    embeddingConfig.embedding_dim = 2;
    auto generator = std::make_shared<vector::EmbeddingGenerator>(
        std::make_unique<FixedEmbeddingBackend>(std::vector<float>{0.0F, 1.0F}), embeddingConfig);
    REQUIRE(generator->initialize());

    SearchEngineConfig config;
    config.includeDebugInfo = true;
    config.enableParallelExecution = true;
    config.enableTieredExecution = true;
    config.textWeight = 1.0F;
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
    config.enableTopologyWeakQueryRouting = true;
    config.topologyMaxClusters = 1;
    config.topologyMaxDocs = 2;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("unmatched query", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    REQUIRE(debug.contains("topology_weak_query_applied"));
    CHECK(debug.at("topology_weak_query_applied") == "1");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "1");
    CHECK(debug.at("topology_weak_query_routed_clusters") == "1");
    CHECK(debug.at("topology_weak_query_added_candidates") == "2");
    CHECK(debug.at("topology_weak_query_total_candidates") == "2");
}
