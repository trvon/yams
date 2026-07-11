#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_execution_context.h>
#include <yams/search/search_topology_stage.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

// Catch2 assertion macros expand through do/while and large generated test bodies.
// NOLINTBEGIN(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)

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
    explicit TopologySearchFixture(
        vector::VectorSearchEngine searchEngine = vector::VectorSearchEngine::SimeonPqAdc)
        : disableVectors("YAMS_DISABLE_VECTORS", std::optional<std::string>("0")),
          skipVecInit("YAMS_SQLITE_VEC_SKIP_INIT", std::optional<std::string>("0")) {
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
        vectorConfig.search_engine = searchEngine;
        vectorConfig.vec0_phss_enabled = searchEngine == vector::VectorSearchEngine::Vec0L2;
        vectorConfig.vec0_phss_candidates = 4;
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

    yams::test::ScopedEnvVar disableVectors;
    yams::test::ScopedEnvVar skipVecInit;
    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
    std::shared_ptr<vector::VectorDatabase> vectorDb;
};

yams::topology::TopologyArtifactBatch buildTwoClusterTopologyBatch() {
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
    return topologyBatch.value();
}

void seedTwoClusterTopology(TopologySearchFixture& fix) {
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(buildTwoClusterTopologyBatch()).has_value());
}

void seedTopologyWithStaleClusterMember(TopologySearchFixture& fix) {
    auto topologyBatch = buildTwoClusterTopologyBatch();
    REQUIRE(!topologyBatch.clusters.empty());
    topologyBatch.clusters.front().memberDocumentHashes.push_back("missing-hash");
    topologyBatch.clusters.front().memberCount =
        topologyBatch.clusters.front().memberDocumentHashes.size();
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(topologyBatch).has_value());
}

void seedTopologyDocuments(TopologySearchFixture& fix) {
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});
    fix.addDocument("y1", "omega one", {0.0F, 1.0F});
    fix.addDocument("y2", "omega two", {0.1F, 0.9F});
}

std::shared_ptr<vector::EmbeddingGenerator> makeFixedGenerator(std::vector<float> embedding) {
    vector::EmbeddingConfig embeddingConfig;
    embeddingConfig.embedding_dim = embedding.size();
    auto generator = std::make_shared<vector::EmbeddingGenerator>(
        std::make_unique<FixedEmbeddingBackend>(std::move(embedding)), embeddingConfig);
    REQUIRE(generator->initialize());
    return generator;
}

SearchEngineConfig topologyRoutingTestConfig(bool enabled) {
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
    config.enableTopologyWeakQueryRouting = enabled;
    config.topologyRoutingMode = enabled ? SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly
                                         : SearchEngineConfig::TopologyRoutingMode::Disabled;
    config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Narrow;
    config.topologyMaxClusters = 1;
    config.topologyMaxDocs = 2;
    return config;
}

} // namespace

TEST_CASE("SearchEngine topology routing narrows weak-query vector candidates",
          "[search][topology][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::Vec0L2};
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
    config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Narrow;
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
    REQUIRE(debug.contains("search_pipeline_interface"));
    CHECK((debug.at("search_pipeline_interface") == "1"));
    CHECK((debug.at("search_pipeline_name") == "classic"));
    REQUIRE(debug.contains("topology_weak_query_applied"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK(debug.at("topology_weak_query_skip_reason").empty());
    CHECK((debug.at("topology_weak_query_applied") == "1"));
    CHECK((debug.at("topology_weak_query_narrow_applied") == "1"));
    CHECK((debug.at("topology_weak_query_allowed_candidates") == "2"));
    CHECK((debug.at("topology_vector_filter_applied") == "1"));
    CHECK((debug.at("topology_vector_filter_fallback") == "0"));
    CHECK((debug.at("topology_vector_partition_ann_applied") == "1"));
    CHECK((debug.at("topology_vector_partition_ann_fallback") == "0"));
    CHECK((debug.at("topology_vector_filter_matched") == "2"));
    CHECK((debug.at("topology_vector_filter_removed") == "0"));
    CHECK((debug.at("topology_vector_scores_reused") == "0"));
    CHECK((debug.at("topology_vector_scores_reused_count") == "0"));
    CHECK((debug.at("topology_member_rerank_rows_visited_actual") == "0"));
    CHECK((debug.at("topology_member_rerank_distance_evaluations_actual") == "0"));
    CHECK((std::stoull(debug.at("vector_search_rows_visited_actual")) +
               std::stoull(debug.at("vector_search_ann_candidate_budget_actual")) >
           0));
    CHECK((debug.at("topology_sidecar_vector_candidates") == "0"));
    CHECK((debug.at("topology_sidecar_post_fusion_count") == "0"));
    CHECK((debug.at("topology_new_post_fusion_count") == "0"));
    CHECK((debug.at("topology_duplicate_post_fusion_count") == "0"));
    CHECK(debug.contains("topology_sidecar_post_fusion_doc_ids"));
    CHECK(debug.contains("topology_new_post_fusion_doc_ids"));
    CHECK(debug.contains("topology_duplicate_post_fusion_doc_ids"));
    CHECK((debug.at("topology_weak_query_routed_clusters") == "1"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "1"));
    CHECK((debug.at("topology_weak_query_total_candidates") == "1"));
    CHECK((debug.at("topology_snapshot_cache_hit") == "0"));

    auto cachedResponse = engine.searchWithResponse("unmatched query", params);
    REQUIRE(cachedResponse.has_value());
    CHECK((cachedResponse.value().debugStats.at("topology_snapshot_cache_hit") == "1"));
}

TEST_CASE("Hybrid topology routing does not copy unrelated lexical hits into vector narrowing",
          "[search][topology][narrowing][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    fix.addDocument("lexical-only", "unique needle", {1.0F, 0.0F});
    seedTwoClusterTopology(fix);

    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologySparseDenseAlpha = 0.0F;
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("unique needle", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    REQUIRE(debug.at("topology_weak_query_narrow_applied") == "1");
    CHECK(debug.at("topology_weak_query_allowed_candidates") == "2");
    CHECK(debug.at("topology_vector_filter_applied") == "1");
    CHECK(debug.at("topology_vector_scores_reused") == "0");
    CHECK(debug.at("topology_member_rerank_distance_evaluations_actual") == "0");

    const auto lexical = std::find_if(
        response.value().results.begin(), response.value().results.end(),
        [](const auto& result) { return result.document.sha256Hash == "lexical-only"; });
    REQUIRE(lexical != response.value().results.end());
}

TEST_CASE("SearchEngine topology routing loads stored artifacts without freshness readiness",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = false;
    context.freshness.topologyArtifactsFresh = false;
    context.freshness.topologyEpoch = 0;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("zz", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK((debug.at("topology_routing_mode") == "weak_query_only"));
    CHECK((debug.at("topology_weak_query_enabled") == "1"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK((debug.at("topology_ready") == "1"));
    CHECK((debug.at("topology_artifacts_fresh") == "1"));
    CHECK(debug.contains("topology_epoch"));
    CHECK(debug.at("topology_weak_query_skip_reason").empty());
    CHECK((debug.at("topology_weak_query_applied") == "1"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "1"));
}

TEST_CASE("SearchEngine rejects inconsistent topology artifacts before routing",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTopologyWithStaleClusterMember(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    context.freshness.topologyArtifactsFresh = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("zz", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK((debug.at("topology_artifact_admitted") == "0"));
    CHECK((debug.at("topology_weak_query_skip_reason").rfind("invalid_artifact:", 0) == 0));
    CHECK((debug.at("topology_weak_query_applied") == "0"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "0"));
}

TEST_CASE("SearchEngine disabled topology routing does not load or add candidates",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(false);

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("zz", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK((debug.at("topology_routing_mode") == "disabled"));
    CHECK((debug.at("topology_weak_query_enabled") == "0"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "0"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "0"));
    CHECK((debug.at("topology_weak_query_skip_reason") == "disabled"));
    CHECK((debug.at("topology_weak_query_applied") == "0"));
    CHECK((debug.at("topology_weak_query_narrow_applied") == "0"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "0"));
}

TEST_CASE("SearchEngine weak-only topology routing skips strong tier-1 queries",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);
    config.weakQueryMinTextHits = 1;
    config.weakQueryMinTopTextScore = 0.0F;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    context.freshness.topologyArtifactsFresh = true;
    context.freshness.topologyEpoch = 7;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("alpha", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK((debug.at("topology_routing_mode") == "weak_query_only"));
    CHECK((debug.at("topology_weak_query_enabled") == "1"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "0"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "0"));
    CHECK((debug.at("topology_weak_query_skip_reason") == "strong_tier1_query"));
    CHECK((debug.at("topology_weak_query_applied") == "0"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "0"));
}

TEST_CASE("SearchEngine hybrid-assist keeps outside-route lexical hits without vector scoring",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    fix.addDocument("z1", "alpha outside route", {0.0F, 1.0F});
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.weakQueryMinTextHits = 1;
    config.weakQueryMinTopTextScore = 0.0F;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("alpha", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK((debug.at("topology_routing_mode") == "hybrid_assist"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK(debug.at("topology_weak_query_skip_reason").empty());
    CHECK((debug.at("topology_weak_query_applied") == "1"));
    CHECK((debug.at("topology_weak_query_narrow_applied") == "1"));
    CHECK((debug.at("topology_weak_query_allowed_candidates") == "2"));
    CHECK((debug.at("topology_sidecar_vector_candidates") == "0"));

    const auto outsideRoute =
        std::find_if(response.value().results.begin(), response.value().results.end(),
                     [](const auto& result) { return result.document.sha256Hash == "z1"; });
    REQUIRE((outsideRoute != response.value().results.end()));
    CHECK_FALSE(outsideRoute->vectorScore.has_value());

    const bool hasTopologyVectorScore = std::any_of(
        response.value().results.begin(), response.value().results.end(), [](const auto& result) {
            return result.graphVectorScore.has_value() && result.graphVectorScore.value() > 0.0;
        });
    CHECK_FALSE(hasTopologyVectorScore);
}

TEST_CASE("SearchEngine topology augmentation preserves global ANN vector candidates",
          "[search][topology][augmentation][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::Vec0L2};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Augment;
    config.topologySparseDenseAlpha = 0.0F;
    config.vectorMaxResults = 1;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("alpha", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_vector_policy") == "augment");
    CHECK(debug.at("topology_weak_query_applied") == "1");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_vector_augmentation_candidates") == "2");
    CHECK(std::stoull(debug.at("topology_member_rerank_distance_evaluations_actual")) > 0U);

    const auto globalAnn =
        std::find_if(response.value().results.begin(), response.value().results.end(),
                     [](const auto& result) { return result.document.sha256Hash == "y1"; });
    REQUIRE(globalAnn != response.value().results.end());
    REQUIRE(globalAnn->vectorScore.has_value());
    CHECK(globalAnn->vectorScore.value() > 0.0);
}

TEST_CASE("SearchEngine rerank-only topology routing loads but does not add candidates",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::RerankOnly;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("zz", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK((debug.at("topology_routing_mode") == "rerank_only"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK((debug.at("topology_weak_query_skip_reason") == "rerank_only_no_expansion"));
    CHECK((debug.at("topology_weak_query_applied") == "0"));
    CHECK((debug.at("topology_weak_query_narrow_applied") == "0"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "0"));
}

TEST_CASE("SearchEngine stage trace reports reranker flags as booleans",
          "[search][trace][rerank][catch2]") {
    yams::test::ScopedEnvVar stageTrace("YAMS_SEARCH_STAGE_TRACE", std::optional<std::string>("1"));
    yams::test::ScopedEnvVar traceTopN("YAMS_SEARCH_STAGE_TRACE_TOP_N",
                                       std::optional<std::string>("4"));

    TopologySearchFixture fix;
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});

    SearchEngineConfig config;
    config.includeDebugInfo = true;
    config.enableParallelExecution = false;
    config.enableTieredExecution = false;
    config.textWeight = 1.0F;
    config.simeonTextWeight = 0.0F;
    config.pathTreeWeight = 0.0F;
    config.kgWeight = 0.0F;
    config.vectorWeight = 0.0F;
    config.entityVectorWeight = 0.0F;
    config.tagWeight = 0.0F;
    config.metadataWeight = 0.0F;
    config.enableGraphRerank = false;
    config.enableReranking = true;
    config.rerankTopK = 5;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, nullptr, fix.kgStore, config);
    SearchParams params;
    params.limit = 2;
    auto response = engine.searchWithResponse("alpha", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    REQUIRE(debug.contains("trace_enabled"));
    CHECK((debug.at("trace_enabled") == "1"));
    REQUIRE(debug.contains("trace_cross_rerank_applied"));
    CHECK((debug.at("trace_cross_rerank_applied") == "0"));
    REQUIRE(debug.contains("trace_turboquant_rerank_applied"));
    CHECK((debug.at("trace_turboquant_rerank_applied") == "0"));
    REQUIRE(debug.contains("trace_rerank_guard_score_gap"));
    CHECK((debug.at("trace_rerank_guard_score_gap") == "0.000000"));
}

TEST_CASE("SearchEngine cross reranker promotes lower fused candidate",
          "[search][rerank][catch2]") {
    TopologySearchFixture fix;
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});

    SearchEngineConfig config;
    config.includeDebugInfo = true;
    config.enableParallelExecution = false;
    config.enableTieredExecution = false;
    config.fusionStrategy = SearchEngineConfig::FusionStrategy::WEIGHTED_SUM;
    config.textWeight = 1.0F;
    config.simeonTextWeight = 0.0F;
    config.pathTreeWeight = 0.0F;
    config.kgWeight = 0.0F;
    config.vectorWeight = 0.0F;
    config.entityVectorWeight = 0.0F;
    config.tagWeight = 0.0F;
    config.metadataWeight = 0.0F;
    config.enableGraphRerank = false;
    config.enableReranking = true;
    config.rerankTopK = 2;
    config.rerankReplaceScores = true;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, nullptr, fix.kgStore, config);
    engine.setCrossReranker(
        [](const std::string&,
           const std::vector<std::string>& documents) -> Result<std::vector<float>> {
            std::vector<float> scores;
            scores.reserve(documents.size());
            for (const auto& doc : documents) {
                scores.push_back(doc.find("x2") != std::string::npos ? 1.0F : 0.1F);
            }
            return scores;
        });

    SearchParams params;
    params.limit = 2;
    auto response = engine.searchWithResponse("alpha", params);
    REQUIRE(response.has_value());
    REQUIRE((response.value().results.size() == 2));

    CHECK((response.value().results.front().document.sha256Hash == "x2"));
    const auto& debug = response.value().debugStats;
    CHECK((debug.at("cross_rerank_available") == "1"));
    CHECK(debug.at("cross_rerank_skip_reason").empty());
    CHECK((debug.at("cross_rerank_replace_scores") == "1"));
}

// NOLINTEND(cppcoreguidelines-avoid-do-while, readability-function-cognitive-complexity)

TEST_CASE("Topology routing session reports route confidence and seed coverage",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1", "not-in-any-cluster"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.weakTier1Query = true;
    request.maxClusters = 2;
    request.maxDocs = 8;
    request.minRouteScore = 0.0F;

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    REQUIRE(session.artifactAdmitted);
    REQUIRE((session.acceptedRoutes >= 1));
    CHECK((session.bestRouteScore > 0.0F));
    CHECK((session.meanAcceptedRouteScore > 0.0F));
    CHECK((session.meanAcceptedRouteScore <= session.bestRouteScore));
    CHECK((session.seedCount == 2));
    CHECK((session.seedsInRoutedClusters == 1));
}

TEST_CASE("Topology routing session excludes min-score-rejected routes from confidence aggregates",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.weakTier1Query = true;
    request.maxClusters = 2;
    request.maxDocs = 8;
    request.minRouteScore = 1000000.0F;

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    CHECK((session.routesRejected >= 1));
    CHECK((session.acceptedRoutes == 0));
    CHECK((session.bestRouteScore == 0.0F));
    CHECK((session.meanAcceptedRouteScore == 0.0F));
    CHECK((session.seedsInRoutedClusters == 0));
    CHECK((session.addedCandidates == 0));
}

TEST_CASE("Topology routing applies the per-cluster member reranker and caps members",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.weakTier1Query = true;
    request.maxClusters = 2;
    request.maxDocs = 16;
    request.perClusterLimit = 1;

    int rerankerCalls = 0;
    TopologyMemberReranker reranker = [&](const std::vector<std::string>& members,
                                          std::size_t limit) {
        ++rerankerCalls;
        return std::vector<std::string>(members.begin(),
                                        members.begin() + std::min(limit, members.size()));
    };

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore, reranker);
    REQUIRE(session.artifactAdmitted);
    REQUIRE((session.acceptedRoutes >= 1));
    CHECK((rerankerCalls >= 1));
    CHECK((session.addedCandidates <= session.acceptedRoutes * request.perClusterLimit));
}

TEST_CASE("Topology routing without a reranker falls back to query-independent medoids",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.weakTier1Query = true;
    request.maxClusters = 2;
    request.maxDocs = 16;
    request.perClusterLimit = 1;

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);
    REQUIRE(session.artifactAdmitted);
    CHECK((session.addedCandidates <= session.acceptedRoutes));
    CHECK((session.routedCandidateHashes.size() <= session.acceptedRoutes));
}

TEST_CASE("Topology routing can expose full selected-cluster membership without reranking",
          "[search][topology][narrowing][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.weakTier1Query = true;
    request.maxClusters = 1;
    request.maxDocs = 1;
    request.collectRouteMembership = true;

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    REQUIRE(session.artifactAdmitted);
    REQUIRE(session.narrowApplied);
    CHECK(session.routedCandidateHashes.size() == 1U);
    CHECK(session.routeAllowedDocumentHashes.size() == 2U);
    CHECK(session.routeAllowedDocumentHashes.contains("y1"));
    CHECK(session.routeAllowedDocumentHashes.contains("y2"));
    CHECK(session.memberRerankCandidates == 0U);
    CHECK(session.memberRerankSelected == 0U);
}

TEST_CASE("Topology snapshot cache validates once and reuses the admitted epoch",
          "[search][topology][cache][catch2]") {
    auto batch = buildTwoClusterTopologyBatch();
    int loadCalls = 0;
    TopologyRoutingSnapshotCache cache([&]() {
        ++loadCalls;
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{batch}};
    });

    auto first = cache.get(batch.topologyEpoch);
    REQUIRE(first.has_value());
    REQUIRE(first.value().snapshot);
    CHECK_FALSE(first.value().cacheHit);
    CHECK((first.value().snapshot->clustersById.size() == batch.clusters.size()));

    auto second = cache.get(batch.topologyEpoch);
    REQUIRE(second.has_value());
    REQUIRE(second.value().snapshot);
    CHECK(second.value().cacheHit);
    CHECK((loadCalls == 1));
}

TEST_CASE("Topology snapshot cache rejects duplicate cluster identifiers",
          "[search][topology][cache][validation][catch2]") {
    auto batch = buildTwoClusterTopologyBatch();
    REQUIRE((batch.clusters.size() >= 2));
    batch.clusters[1].clusterId = batch.clusters[0].clusterId;

    TopologyRoutingSnapshotCache cache([batch]() mutable {
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
    });

    const auto result = cache.get();
    REQUIRE_FALSE(result.has_value());
    CHECK((result.error().message == "invalid_artifact:duplicate_cluster_id"));
}

TEST_CASE("mergeTopologySeedHashes keeps Tier-1 first and caps vector seeds",
          "[unit][search][topology][graph_neighbors]") {
    const auto merged =
        mergeTopologySeedHashes({"t1", "t2", "t1"}, {"v1", "t2", "v2", "v3"}, /*maxVectorSeeds=*/2);
    REQUIRE((merged.size() == 4));
    CHECK((merged[0] == "t1"));
    CHECK((merged[1] == "t2"));
    CHECK((merged[2] == "v1"));
    CHECK((merged[3] == "v2"));

    const auto none = mergeTopologySeedHashes({"t1", "t2"}, {"v1", "v2"}, /*maxVectorSeeds=*/0);
    REQUIRE((none.size() == 2));
    CHECK((none[0] == "t1"));
    CHECK((none[1] == "t2"));

    const auto emptyVec = mergeTopologySeedHashes({"t1"}, {}, /*maxVectorSeeds=*/16);
    REQUIRE((emptyVec.size() == 1));
    CHECK((emptyVec[0] == "t1"));
}

TEST_CASE("rankTopologySeedEvidence keeps ranked lexical evidence and caps fanout",
          "[unit][search][topology][weighted-seeds]") {
    std::vector<ComponentResult> components{
        ComponentResult{.documentHash = "rank-zero",
                        .score = 0.80F,
                        .source = ComponentResult::Source::Text,
                        .rank = 0},
        ComponentResult{.documentHash = "late-high",
                        .score = 0.95F,
                        .source = ComponentResult::Source::Text,
                        .rank = 10},
        ComponentResult{.documentHash = "early",
                        .score = 0.80F,
                        .source = ComponentResult::Source::Text,
                        .rank = 1},
        ComponentResult{.documentHash = "late-high",
                        .score = 0.90F,
                        .source = ComponentResult::Source::SimeonText,
                        .rank = 2},
        ComponentResult{.documentHash = "vector-only",
                        .score = 1.0F,
                        .source = ComponentResult::Source::Vector,
                        .rank = 1},
    };

    const auto evidence = rankTopologySeedEvidence(components, 3);
    REQUIRE((evidence.size() == 3));
    CHECK((evidence[0].documentHash == "rank-zero"));
    CHECK((evidence[1].documentHash == "early"));
    CHECK((evidence[2].documentHash == "late-high"));
    CHECK((evidence[0].weight > evidence[1].weight));
}

TEST_CASE("selectTopologyRoutesForNarrowing adapts probes and abstains at an unsafe boundary",
          "[unit][search][topology][adaptive-probe]") {
    std::vector<yams::topology::ClusterRoute> routes{
        yams::topology::ClusterRoute{.clusterId = "a", .routeScore = 1.00},
        yams::topology::ClusterRoute{.clusterId = "b", .routeScore = 0.97},
        yams::topology::ClusterRoute{.clusterId = "c", .routeScore = 0.80},
    };

    const auto safe = selectTopologyRoutesForNarrowing(routes, /*minClusters=*/1, /*maxClusters=*/3,
                                                       /*adaptiveScoreGap=*/0.05F,
                                                       /*minBoundaryMargin=*/0.10F);
    REQUIRE((safe.routes.size() == 2));
    CHECK((safe.routes[0].clusterId == "a"));
    CHECK((safe.routes[1].clusterId == "b"));
    CHECK((safe.boundaryScoreMargin == 0.17F));
    CHECK_FALSE(safe.abstained);

    const auto unsafe = selectTopologyRoutesForNarrowing(
        routes, /*minClusters=*/1, /*maxClusters=*/2, /*adaptiveScoreGap=*/0.05F,
        /*minBoundaryMargin=*/0.20F);
    REQUIRE((unsafe.routes.size() == 2));
    CHECK(unsafe.abstained);
}

TEST_CASE("fillTopologySkipReason preserves session reason and fills product defaults",
          "[unit][search][topology]") {
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    std::string reason = "graph_medoid_neighbors";
    fillTopologySkipReason(reason, Mode::HybridAssist, false, true, true, false, true, 0);
    CHECK((reason == "graph_medoid_neighbors"));

    reason.clear();
    fillTopologySkipReason(reason, Mode::Disabled, false, false, true, false, false, 0);
    CHECK((reason == "disabled"));

    reason.clear();
    fillTopologySkipReason(reason, Mode::HybridAssist, false, true, true, false, true, 2);
    CHECK((reason == "no_added_candidates"));
}

TEST_CASE("rankGraphNeighborCandidates ranks multi-seed hits and filters score/reciprocal",
          "[unit][search][topology][graph_neighbors]") {
    std::unordered_map<std::string, std::vector<std::tuple<std::string, float, bool>>> adj;
    adj["s1"] = {
        {"n_shared", 0.9F, true},
        {"n_weak", 0.1F, true},
        {"n_oneway", 0.95F, false},
    };
    adj["s2"] = {
        {"n_shared", 0.85F, true},
        // single-seed only — loses to multi-seed n_shared even with higher edge score
        {"n_strong", 0.99F, true},
    };

    auto ranked = rankGraphNeighborCandidates(adj, {"s1", "s2"}, /*maxDocs=*/10,
                                              /*minScore=*/0.25F, /*reciprocalOnly=*/true);
    REQUIRE_FALSE(ranked.empty());
    // Multi-seed hits beat single-seed even when the single edge score is higher.
    CHECK((ranked.front() == "n_shared"));
    // Weak edge below minScore and one-way reciprocal-only drop out.
    CHECK((std::find(ranked.begin(), ranked.end(), "n_weak") == ranked.end()));
    CHECK((std::find(ranked.begin(), ranked.end(), "n_oneway") == ranked.end()));
    CHECK((std::find(ranked.begin(), ranked.end(), "n_strong") != ranked.end()));

    auto limited = rankGraphNeighborCandidates(adj, {"s1", "s2"}, /*maxDocs=*/1, 0.25F, true);
    REQUIRE((limited.size() == 1));
    CHECK((limited.front() == "n_shared"));

    // Reverse-only / one-way edges still rank when reciprocalOnly=false.
    std::unordered_map<std::string, std::vector<std::tuple<std::string, float, bool>>> oneWay;
    oneWay["s1"] = {{"n_oneway", 0.9F, false}};
    auto oneWayRanked =
        rankGraphNeighborCandidates(oneWay, {"s1"}, 5, 0.25F, /*reciprocalOnly=*/false);
    REQUIRE((oneWayRanked.size() == 1));
    CHECK((oneWayRanked.front() == "n_oneway"));
    auto oneWayStrict =
        rankGraphNeighborCandidates(oneWay, {"s1"}, 5, 0.25F, /*reciprocalOnly=*/true);
    CHECK(oneWayStrict.empty());
}
