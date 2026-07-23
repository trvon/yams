#include <catch2/catch_approx.hpp>
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
#include <unordered_set>
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
    std::string getEmbeddingSpaceIdentity() const override { return "test-space-v1"; }
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
        vectorConfig.simeon_pq_centroids = 4;
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
    topologyConfig.embeddingSpaceIdentity = "test-space-v1";
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

yams::topology::TopologyArtifactBatch buildMultiscaleOverlapTopologyBatch() {
    auto batch = buildTwoClusterTopologyBatch();
    auto fineIt = std::ranges::find_if(batch.clusters, [](const auto& cluster) {
        return std::ranges::find(cluster.memberDocumentHashes, "y1") !=
               cluster.memberDocumentHashes.end();
    });
    REQUIRE(fineIt != batch.clusters.end());

    yams::topology::ClusterArtifact coarse = *fineIt;
    coarse.clusterId += ":coarse";
    coarse.level = 1;
    coarse.persistenceScore = 0.70;
    coarse.cohesionScore = 0.80;
    coarse.bridgeMass = 0.30;
    coarse.densityScore = 0.60;
    coarse.parentClusterId.reset();
    coarse.overlapClusterIds = {fineIt->clusterId};
    if (coarse.medoid.has_value()) {
        coarse.medoid->clusterId = coarse.clusterId;
    }

    fineIt->parentClusterId = coarse.clusterId;
    fineIt->overlapClusterIds = {coarse.clusterId};
    for (auto& membership : batch.memberships) {
        if (membership.documentHash != "y1" && membership.documentHash != "y2") {
            continue;
        }
        membership.parentClusterId = coarse.clusterId;
        membership.overlapClusterIds = {coarse.clusterId};
        membership.bridgeScore = membership.documentHash == "y1" ? 0.60 : 0.20;
    }
    batch.clusters.push_back(std::move(coarse));
    return batch;
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
    config.topologyRoutingMode = enabled ? SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly
                                         : SearchEngineConfig::TopologyRoutingMode::Disabled;
    config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Narrow;
    config.topologyMaxClusters = 1;
    config.topologyMaxDocs = 2;
    return config;
}

Result<SearchResponse> runTopologySearch(
    TopologySearchFixture& fix, const std::shared_ptr<vector::EmbeddingGenerator>& generator,
    const SearchEngineConfig& config, std::size_t limit, std::string query = "unmatched query") {
    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = limit;
    return engine.searchWithResponse(query, params);
}

void configureCertifiedTopologyRoute(SearchEngineConfig& config,
                                     std::string constructionFingerprint,
                                     std::size_t maxRowsVisited = 2) {
    config.topologyRouteRiskCalibration.constructionFingerprint =
        std::move(constructionFingerprint);
    config.topologyRouteRiskCalibration.calibrationQueries = 50;
    config.topologyRouteRiskCalibration.protectedCandidates = 100;
    config.topologyRouteRiskCalibration.missedProtectedCandidates = 0;
    config.topologyRouteRiskCalibration.minCalibrationQueries = 30;
    config.topologyRouteRiskCalibration.maxMissesPerThousand = 10;
    config.topologyRouteRiskCalibration.minBoundaryMargin = 0.0F;
    config.topologyRouteRiskCalibration.minSeedHits = 1;

    config.topologyRouteWorkBudget.maxRowsVisited = maxRowsVisited;
    config.topologyRouteWorkBudget.maxExactDistanceEvaluations = 2;
    config.topologyRouteWorkBudget.maxAnnCandidates = 2;
}

} // namespace

TEST_CASE("SearchEngine topology refuses uncertified weak-query narrowing",
          "[search][topology][admission][catch2]") {
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
    CHECK((debug.at("search_pipeline_name") == "topology_evidence"));
    REQUIRE(debug.contains("topology_weak_query_applied"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK(debug.at("topology_weak_query_skip_reason").empty());
    CHECK((debug.at("topology_weak_query_applied") == "1"));
    CHECK((debug.at("topology_route_narrow_proposed") == "1"));
    CHECK((debug.at("topology_route_admission_eligible") == "0"));
    CHECK((debug.at("topology_route_admission_denial_reason") ==
           "protected_fibers_representation_unavailable"));
    CHECK((debug.at("topology_weak_query_narrow_applied") == "0"));
    CHECK((debug.at("topology_weak_query_allowed_candidates") == "2"));
    CHECK((debug.at("topology_vector_filter_applied") == "0"));
    CHECK((debug.at("topology_vector_filter_fallback") == "0"));
    CHECK((debug.at("topology_vector_allowed_set_ann_applied") == "0"));
    CHECK((debug.at("topology_vector_allowed_set_ann_fallback") == "0"));
    CHECK_FALSE(debug.contains("topology_vector_partition_ann_applied"));
    CHECK((debug.at("topology_vector_filter_matched") == "0"));
    CHECK((debug.at("topology_vector_filter_removed") == "0"));
    CHECK_FALSE(debug.contains("topology_vector_scores_reused"));
    CHECK_FALSE(debug.contains("topology_vector_scores_reused_count"));
    CHECK_FALSE(debug.contains("topology_member_rerank_rows_visited_actual"));
    CHECK_FALSE(debug.contains("topology_member_rerank_distance_evaluations_actual"));
    CHECK(debug.at("vector_search_rows_visited_status") == "unavailable");
    CHECK(debug.at("vector_search_exact_distance_evaluations_status") == "unavailable");
    CHECK(debug.at("vector_search_ann_candidate_budget_status") == "unavailable");
    CHECK_FALSE(debug.contains("vector_search_rows_visited_actual"));
    CHECK_FALSE(debug.contains("vector_search_exact_distance_evaluations_actual"));
    CHECK_FALSE(debug.contains("vector_search_ann_candidate_budget_actual"));
    CHECK((debug.at("topology_weak_query_routed_clusters") == "1"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "0"));
    CHECK((debug.at("topology_weak_query_total_candidates") == "0"));
    CHECK((debug.at("topology_snapshot_cache_hit") == "0"));

    auto cachedResponse = engine.searchWithResponse("unmatched query", params);
    REQUIRE(cachedResponse.has_value());
    CHECK((cachedResponse.value().debugStats.at("topology_snapshot_cache_hit") == "1"));
}

TEST_CASE("SearchEngine keeps Simeon PQ global while route admission is unavailable",
          "[search][topology][spq][admission][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    REQUIRE(fix.vectorDb->buildIndex());

    auto generator = makeFixedGenerator({0.1F, 0.9F});
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.topologyMaxDocs = 1;

    auto response = runTopologySearch(fix, generator, config, 4);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_weak_query_allowed_candidates") == "2");
    CHECK(debug.at("topology_route_narrow_proposed") == "1");
    CHECK(debug.at("topology_route_admission_eligible") == "0");
    CHECK(debug.at("topology_route_admission_denial_reason") ==
          "protected_fibers_representation_unavailable");
    CHECK(debug.at("topology_vector_allowed_set_ann_applied") == "0");
    CHECK(debug.at("topology_vector_allowed_set_ann_fallback") == "0");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("vector_search_rows_visited_status") == "observed");
    CHECK(debug.at("vector_search_exact_distance_evaluations_status") == "observed");
    CHECK(debug.at("vector_search_ann_candidate_budget_status") == "observed");
    CHECK(debug.at("vector_search_ann_candidate_budget_actual") == "4");
    CHECK(debug.at("vector_search_rows_visited_actual") == "4");
    CHECK(debug.at("vector_search_exact_distance_evaluations_actual") == "4");
    REQUIRE_FALSE(response.value().results.empty());
}

TEST_CASE("SearchEngine commits hard narrowing when every theorem certificate passes",
          "[search][topology][admission][work][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    auto batch = buildTwoClusterTopologyBatch();
    const auto constructionFingerprint = topologyRoutingConstructionFingerprint(batch);
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    configureCertifiedTopologyRoute(config, constructionFingerprint);

    auto response = runTopologySearch(fix, generator, config, 4, "omega");
    REQUIRE(response.has_value());
    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_route_coordinate_space_identity") == "test-space-v1");
    CHECK(debug.at("topology_route_coordinate_space_alignment_status") == "satisfied");
    CHECK(debug.at("topology_route_protected_relation_coverage_status") == "satisfied");
    CHECK(debug.at("topology_route_protected_fibers_represented_status") == "satisfied");
    CHECK(debug.at("topology_route_certificate_saturates_protected_fibers_status") == "satisfied");
    CHECK(debug.at("topology_route_risk_status") == "satisfied");
    CHECK(debug.at("topology_route_work_status") == "satisfied");
    CHECK(debug.at("topology_route_cover_materialization_status") == "satisfied");
    CHECK(debug.at("topology_route_admission_eligible") == "1");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "1");
    CHECK(debug.at("topology_vector_allowed_set_ann_applied") == "1");
    CHECK(debug.at("topology_vector_global_fill_count") == "0");
    REQUIRE(response.value().results.size() == 2U);
    CHECK(std::ranges::all_of(response.value().results, [](const auto& result) {
        return result.document.sha256Hash == "y1" || result.document.sha256Hash == "y2";
    }));
}

TEST_CASE("SearchEngine discards a trial route whose observed vector work exceeds its budget",
          "[search][topology][admission][work][fallback][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    auto batch = buildTwoClusterTopologyBatch();
    const auto constructionFingerprint = topologyRoutingConstructionFingerprint(batch);
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    configureCertifiedTopologyRoute(config, constructionFingerprint, 1);

    auto response = runTopologySearch(fix, generator, config, 4, "omega");
    REQUIRE(response.has_value());
    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_route_work_status") == "violated");
    CHECK(debug.at("topology_route_admission_eligible") == "0");
    CHECK(debug.at("topology_route_admission_denial_reason") == "work_budget_violated");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_vector_filter_fallback") == "1");
    CHECK(response.value().results.size() == 4U);
}

TEST_CASE("SearchEngine rejects a relation cover that cuts an observed protected pair",
          "[search][topology][admission][pairs][fallback][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    auto batch = buildTwoClusterTopologyBatch();
    auto selectedChart = std::ranges::find_if(batch.clusters, [](const auto& cluster) {
        return std::ranges::find(cluster.memberDocumentHashes, "y1") !=
               cluster.memberDocumentHashes.end();
    });
    REQUIRE(selectedChart != batch.clusters.end());
    REQUIRE(selectedChart->protectedPairCount > 0);
    selectedChart->preservedProtectedPairCount = 0;
    const auto constructionFingerprint = topologyRoutingConstructionFingerprint(batch);
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    configureCertifiedTopologyRoute(config, constructionFingerprint);

    auto response = runTopologySearch(fix, generator, config, 4, "omega");
    REQUIRE(response.has_value());
    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_route_protected_relation_coverage_status") == "violated");
    CHECK(debug.at("topology_route_admission_denial_reason") ==
          "protected_relation_coverage_violated");
    CHECK(debug.at("topology_route_work_status") == "unavailable");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(response.value().results.size() == 4U);
}

TEST_CASE("SearchEngine rejects route calibration from a different topology construction",
          "[search][topology][admission][risk][fallback][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    auto batch = buildTwoClusterTopologyBatch();
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    configureCertifiedTopologyRoute(config, "different-construction");

    auto response = runTopologySearch(fix, generator, config, 4, "omega");
    REQUIRE(response.has_value());
    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_route_protected_relation_coverage_status") == "satisfied");
    CHECK(debug.at("topology_route_risk_status") == "unavailable");
    CHECK(debug.at("topology_route_admission_denial_reason") ==
          "protected_fibers_representation_unavailable");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(response.value().results.size() == 4U);
}

TEST_CASE("SearchEngine does not infer protected fiber representation from bounded miss risk",
          "[search][topology][admission][fiber][fallback][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    auto batch = buildTwoClusterTopologyBatch();
    const auto constructionFingerprint = topologyRoutingConstructionFingerprint(batch);
    yams::topology::MetadataKgTopologyArtifactStore topologyStore(fix.repo, fix.kgStore);
    REQUIRE(topologyStore.storeBatch(batch).has_value());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    configureCertifiedTopologyRoute(config, constructionFingerprint);
    config.topologyRouteRiskCalibration.missedProtectedCandidates = 1;

    auto response = runTopologySearch(fix, generator, config, 4, "omega");
    REQUIRE(response.has_value());
    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_route_risk_status") == "satisfied");
    CHECK(debug.at("topology_route_protected_fibers_represented_status") == "violated");
    CHECK(debug.at("topology_route_admission_denial_reason") == "protected_fibers_unrepresented");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
}

TEST_CASE("SearchEngine keeps multi-route proposals global until admitted",
          "[search][topology][spq][balance][admission][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    REQUIRE(fix.vectorDb->buildIndex());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.topologyMinClusters = 2;
    config.topologyMaxClusters = 2;
    config.topologyMaxDocs = 2;
    config.similarityThreshold = -1.0F;

    auto response = runTopologySearch(fix, generator, config, 2);
    REQUIRE(response.has_value());
    REQUIRE(response.value().results.size() == 2U);

    std::unordered_set<std::string> resultHashes;
    for (const auto& result : response.value().results) {
        resultHashes.insert(result.document.sha256Hash);
    }
    CHECK_FALSE((resultHashes.contains("x1") || resultHashes.contains("x2")));
    CHECK((resultHashes.contains("y1") || resultHashes.contains("y2")));
    CHECK(response.value().debugStats.at("topology_route_narrow_proposed") == "1");
    CHECK(response.value().debugStats.at("topology_route_admission_eligible") == "0");
}

TEST_CASE("SearchEngine does not bound Simeon to an uncertified route",
          "[search][topology][spq][fill][admission][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    REQUIRE(fix.vectorDb->buildIndex());

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.topologyMinClusters = 1;
    config.topologyMaxClusters = 1;
    config.topologyMaxDocs = 4;
    config.similarityThreshold = -1.0F;

    auto response = runTopologySearch(fix, generator, config, 4);
    REQUIRE(response.has_value());
    CHECK(response.value().results.size() == 4U);
    CHECK(response.value().debugStats.at("topology_vector_global_fill_count") == "0");
    CHECK(response.value().debugStats.at("vector_search_ann_candidate_budget_actual") == "4");
    CHECK(response.value().debugStats.at("topology_route_admission_eligible") == "0");
}

TEST_CASE("SearchEngine uses global Vec0 directly for an uncertified route",
          "[search][topology][vec0][fill][admission][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::Vec0L2};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    auto generator = makeFixedGenerator({0.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.topologyMinClusters = 1;
    config.topologyMaxClusters = 1;
    config.topologyMaxDocs = 4;
    config.similarityThreshold = -1.0F;

    auto response = runTopologySearch(fix, generator, config, 4);
    REQUIRE(response.has_value());
    CHECK(response.value().results.size() == 4U);
    CHECK(response.value().debugStats.at("topology_vector_global_fill_count") == "0");
    CHECK(response.value().debugStats.at("topology_route_admission_eligible") == "0");
}

TEST_CASE("SearchEngine topology abstains from narrowing at an ambiguous route boundary",
          "[search][topology][spq][confidence][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    REQUIRE(fix.vectorDb->buildIndex());

    auto generator = makeFixedGenerator({1.0F, 1.0F});
    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologySparseDenseAlpha = 0.0F;
    config.topologyMinClusters = 1;
    config.topologyMaxClusters = 1;
    config.topologyNarrowMinBoundaryMargin = 0.20F;
    config.topologyMaxDocs = 2;
    config.similarityThreshold = -1.0F;

    auto response = runTopologySearch(fix, generator, config, 4);
    REQUIRE(response.has_value());
    CHECK(response.value().results.size() == 4U);

    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_route_confidence_abstained") == "1");
    CHECK(debug.at("topology_weak_query_skip_reason") == "route_low_confidence");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_vector_allowed_set_ann_applied") == "0");
    CHECK(debug.at("vector_search_ann_candidate_budget_actual") == "4");
}

TEST_CASE("SearchEngine topology shadow leaves global ranking and scores unchanged",
          "[search][topology][shadow][catch2]") {
    yams::test::ScopedEnvVar stageTrace("YAMS_SEARCH_STAGE_TRACE", std::optional<std::string>("1"));

    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    REQUIRE(fix.vectorDb->buildIndex());
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    auto globalConfig = topologyRoutingTestConfig(false);
    SearchEngine globalEngine(fix.repo, fix.vectorDb, generator, fix.kgStore, globalConfig);

    auto shadowConfig = topologyRoutingTestConfig(true);
    shadowConfig.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    shadowConfig.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Shadow;
    shadowConfig.topologySparseDenseAlpha = 0.0F;
    shadowConfig.topologyNarrowMinBoundaryMargin = 0.0F;
    shadowConfig.topologyRouteWorkBudget.maxRowsVisited = 4;
    shadowConfig.topologyRouteWorkBudget.maxExactDistanceEvaluations = 4;
    shadowConfig.topologyRouteWorkBudget.maxAnnCandidates = 4;
    SearchEngine shadowEngine(fix.repo, fix.vectorDb, generator, fix.kgStore, shadowConfig);

    SearchParams params;
    params.limit = 4;
    auto global = globalEngine.searchWithResponse("unmatched query", params);
    auto shadow = shadowEngine.searchWithResponse("unmatched query", params);
    REQUIRE(global.has_value());
    REQUIRE(shadow.has_value());
    REQUIRE(shadow.value().results.size() == global.value().results.size());
    bool scoreChanged = false;
    for (std::size_t index = 0; index < global.value().results.size(); ++index) {
        CHECK(shadow.value().results[index].document.sha256Hash ==
              global.value().results[index].document.sha256Hash);
        scoreChanged = scoreChanged ||
                       shadow.value().results[index].score != global.value().results[index].score;
    }
    CHECK_FALSE(scoreChanged);

    const auto& debug = shadow.value().debugStats;
    CHECK(debug.at("topology_vector_policy") == "shadow");
    CHECK(debug.at("topology_shadow_evaluated") == "1");
    CHECK(debug.at("topology_shadow_proposed_action") == "narrow");
    CHECK(debug.at("topology_shadow_retained_candidates") == "2");
    CHECK(debug.at("topology_shadow_removed_candidates") == "2");
    CHECK(debug.at("topology_shadow_retained_candidate_doc_ids") == "y1\ty2");
    CHECK(debug.at("topology_shadow_removed_candidate_doc_ids") == "x1\tx2");
    CHECK(debug.at("topology_route_work_status") == "satisfied");
    CHECK(debug.at("topology_route_work_observed") == "1");
    CHECK(debug.at("topology_route_work_rows_visited_actual") == "2");
    CHECK(debug.at("topology_route_work_exact_distance_evaluations_actual") == "2");
    CHECK(debug.at("topology_route_work_ann_candidate_budget_actual") == "2");
    CHECK(debug.at("topology_weak_query_applied") == "0");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_weak_query_added_candidates") == "0");
    CHECK(debug.at("topology_vector_filter_applied") == "0");
    CHECK(debug.at("topology_vector_allowed_set_ann_applied") == "0");
}

TEST_CASE("SearchEngine topology augmentation materializes query-scored rescue candidates",
          "[search][topology][candidate-rescue][catch2]") {
    yams::test::ScopedEnvVar stageTrace("YAMS_SEARCH_STAGE_TRACE", std::optional<std::string>("1"));

    for (const auto searchEngine : {vector::VectorSearchEngine::SimeonPqAdc,
                                    vector::VectorSearchEngine::ExactScan}) {
        CAPTURE(searchEngine);
        TopologySearchFixture fix{searchEngine};
        seedTopologyDocuments(fix);
        seedTwoClusterTopology(fix);
        REQUIRE(fix.vectorDb->buildIndex());
        auto generator = makeFixedGenerator({0.0F, 1.0F});

        auto config = topologyRoutingTestConfig(true);
        config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
        config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Augment;
        config.topologySparseDenseAlpha = 1.0F;
        config.topologyNarrowMinBoundaryMargin = 0.0F;
        config.textMaxResults = 1;
        config.vectorMaxResults = 1;
        config.topologyMaxDocs = 2;
        config.similarityThreshold = -1.0F;

        auto response = runTopologySearch(fix, generator, config, 4, "alpha one");
        REQUIRE(response.has_value());

        std::vector<std::string> resultHashes;
        for (const auto& result : response.value().results) {
            resultHashes.push_back(result.document.sha256Hash);
        }
        CHECK(std::ranges::find(resultHashes, "x2") != resultHashes.end());

        const auto& debug = response.value().debugStats;
        CHECK(debug.at("topology_vector_policy") == "augment");
        CHECK(debug.at("topology_candidate_rescue_attempted") == "1");
        CHECK(debug.at("topology_candidate_rescue_applied") == "1");
        CHECK(debug.at("topology_candidate_rescue_added_candidates") == "2");
        CHECK(debug.at("topology_candidate_rescue_novel_candidates") == "1");
        CHECK(debug.at("topology_candidate_rescue_evidence_rescues") == "1");
        CHECK(debug.at("topology_candidate_rescue_added_candidate_hashes") == "x2\tx1");
        CHECK(debug.at("topology_candidate_rescue_added_candidate_doc_ids") == "x2\tx1");
        CHECK(debug.at("topology_candidate_rescue_novel_candidate_doc_ids") == "x2");
        CHECK(debug.at("topology_candidate_rescue_evidence_rescue_doc_ids") == "x1");
    }
}

TEST_CASE("SearchEngine shadow policy measures candidate rescue without changing results",
          "[search][topology][candidate-rescue][shadow][catch2]") {
    yams::test::ScopedEnvVar stageTrace("YAMS_SEARCH_STAGE_TRACE", std::optional<std::string>("1"));

    TopologySearchFixture fix{vector::VectorSearchEngine::SimeonPqAdc};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    REQUIRE(fix.vectorDb->buildIndex());
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    auto config = topologyRoutingTestConfig(true);
    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    config.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Shadow;
    config.topologySparseDenseAlpha = 1.0F;
    config.topologyNarrowMinBoundaryMargin = 0.0F;
    config.textMaxResults = 1;
    config.vectorMaxResults = 1;
    config.topologyMaxDocs = 2;
    config.similarityThreshold = -1.0F;

    auto response = runTopologySearch(fix, generator, config, 4, "alpha one");
    REQUIRE(response.has_value());

    std::vector<std::string> resultHashes;
    for (const auto& result : response.value().results) {
        resultHashes.push_back(result.document.sha256Hash);
    }
    CHECK(std::ranges::find(resultHashes, "x2") == resultHashes.end());

    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_vector_policy") == "shadow");
    CHECK(debug.at("topology_candidate_rescue_attempted") == "1");
    CHECK(debug.at("topology_candidate_rescue_applied") == "0");
    CHECK(debug.at("topology_candidate_rescue_added_candidates") == "2");
    CHECK(debug.at("topology_candidate_rescue_novel_candidates") == "1");
    CHECK(debug.at("topology_candidate_rescue_evidence_rescues") == "1");
    CHECK(debug.at("topology_candidate_rescue_added_candidate_hashes") == "x2\tx1");
    CHECK(debug.at("topology_candidate_rescue_added_candidate_doc_ids") == "x2\tx1");
    CHECK(debug.at("topology_candidate_rescue_novel_candidate_doc_ids") == "x2");
    CHECK(debug.at("topology_candidate_rescue_evidence_rescue_doc_ids") == "x1");
    CHECK(debug.at("topology_candidate_rescue_exact_scored_candidate_doc_ids") == "x1\tx2");
}

TEST_CASE("Evidence pipeline receives no topology adjustments under a shadow policy",
          "[search][topology][evidence_pipeline][catch2]") {
    TopologySearchFixture fix{vector::VectorSearchEngine::Vec0L2};
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyReady = true;
    SearchExecutionContextGuard contextGuard(context);

    auto globalConfig = topologyRoutingTestConfig(false);
    SearchEngine globalEngine(fix.repo, fix.vectorDb, generator, fix.kgStore, globalConfig);

    auto evidenceConfig = topologyRoutingTestConfig(true);
    evidenceConfig.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    evidenceConfig.topologyVectorPolicy = SearchEngineConfig::TopologyVectorPolicy::Shadow;
    evidenceConfig.topologySparseDenseAlpha = 0.0F;
    evidenceConfig.topologyNarrowMinBoundaryMargin = 0.0F;
    SearchEngine evidenceEngine(fix.repo, fix.vectorDb, generator, fix.kgStore, evidenceConfig);

    SearchParams params;
    params.limit = 4;
    auto global = globalEngine.searchWithResponse("unmatched query", params);
    auto evidence = evidenceEngine.searchWithResponse("unmatched query", params);
    REQUIRE(global.has_value());
    REQUIRE(evidence.has_value());

    std::vector<std::string> globalHashes;
    std::vector<std::string> evidenceHashes;
    for (const auto& result : global.value().results) {
        globalHashes.push_back(result.document.sha256Hash);
    }
    for (const auto& result : evidence.value().results) {
        evidenceHashes.push_back(result.document.sha256Hash);
    }
    std::ranges::sort(globalHashes);
    std::ranges::sort(evidenceHashes);
    CHECK(evidenceHashes == globalHashes);

    const auto& debug = evidence.value().debugStats;
    CHECK(debug.at("topology_vector_policy") == "shadow");
    CHECK(debug.at("topology_weak_query_applied") == "0");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_vector_filter_applied") == "0");
    CHECK(debug.at("topology_vector_allowed_set_ann_applied") == "0");
    CHECK_FALSE(debug.contains("topology_shadow_retained_candidate_doc_ids"));
    CHECK_FALSE(debug.contains("topology_shadow_removed_candidate_doc_ids"));
    CHECK_FALSE(debug.contains("trace_stage_summary_json"));
    CHECK_FALSE(debug.contains("trace_fusion_source_summary_json"));
    CHECK(debug.at("candidate_pipeline_topology_annotated_candidates") == "0");
}

TEST_CASE("Hybrid topology keeps lexical hits while uncertified narrowing falls back",
          "[search][topology][narrowing][admission][catch2]") {
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
    REQUIRE(debug.at("topology_weak_query_narrow_applied") == "0");
    CHECK(debug.at("topology_route_narrow_proposed") == "1");
    CHECK(debug.at("topology_route_admission_eligible") == "0");
    CHECK(debug.at("topology_weak_query_allowed_candidates") == "2");
    CHECK(debug.at("topology_vector_filter_applied") == "0");
    CHECK_FALSE(debug.contains("topology_vector_scores_reused"));
    CHECK_FALSE(debug.contains("topology_member_rerank_distance_evaluations_actual"));

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
    CHECK(debug.at("topology_routing_freshness_gate") == "1");
    CHECK((debug.at("topology_routing_mode") == "weak_query_only"));
    CHECK((debug.at("topology_weak_query_enabled") == "1"));
    CHECK((debug.at("topology_weak_query_load_attempted") == "1"));
    CHECK((debug.at("topology_weak_query_load_succeeded") == "1"));
    CHECK((debug.at("topology_ready") == "1"));
    CHECK((debug.at("topology_artifacts_fresh") == "1"));
    CHECK(debug.contains("topology_epoch"));
    CHECK(debug.at("topology_weak_query_skip_reason").empty());
    CHECK((debug.at("topology_weak_query_applied") == "1"));
    CHECK((debug.at("topology_weak_query_added_candidates") == "0"));
}

TEST_CASE("SearchEngine abstains from topology routing when daemon artifacts are stale",
          "[search][topology][freshness][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);
    auto generator = makeFixedGenerator({0.0F, 1.0F});

    SearchEngineConfig config = topologyRoutingTestConfig(true);

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    context.freshness.vectorReady = true;
    context.freshness.kgReady = true;
    context.freshness.topologyStatusKnown = true;
    context.freshness.topologyEpoch = 7;
    context.freshness.topologyReady = false;
    context.freshness.topologyArtifactsFresh = false;
    context.freshness.topologyDirtyDocuments = 1;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, generator, fix.kgStore, config);
    SearchParams params;
    params.limit = 4;
    auto response = engine.searchWithResponse("zz", params);
    REQUIRE(response.has_value());

    const auto& debug = response.value().debugStats;
    CHECK(debug.at("topology_routing_freshness_gate") == "0");
    CHECK(debug.at("topology_routing_mode") == "disabled");
    CHECK(debug.at("topology_weak_query_load_attempted") == "0");
    CHECK(debug.at("topology_weak_query_load_succeeded") == "0");
    CHECK(debug.at("topology_weak_query_skip_reason") == "artifacts_not_fresh");
    CHECK(debug.at("topology_weak_query_applied") == "0");
    CHECK(debug.at("topology_weak_query_narrow_applied") == "0");
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

TEST_CASE("SearchEngine global fallback vector-scores outside-route lexical hits",
          "[search][topology][admission][catch2]") {
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
    CHECK((debug.at("topology_weak_query_narrow_applied") == "0"));
    CHECK((debug.at("topology_route_narrow_proposed") == "1"));
    CHECK((debug.at("topology_route_admission_eligible") == "0"));
    CHECK((debug.at("topology_weak_query_allowed_candidates") == "2"));

    const auto outsideRoute =
        std::find_if(response.value().results.begin(), response.value().results.end(),
                     [](const auto& result) { return result.document.sha256Hash == "z1"; });
    REQUIRE((outsideRoute != response.value().results.end()));
    CHECK(outsideRoute->vectorScore.has_value());

    const bool hasTopologyVectorScore = std::any_of(
        response.value().results.begin(), response.value().results.end(), [](const auto& result) {
            return result.graphVectorScore.has_value() && result.graphVectorScore.value() > 0.0;
        });
    CHECK_FALSE(hasTopologyVectorScore);
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
    REQUIRE(debug.contains("trace_rerank_guard_score_gap"));
    REQUIRE(debug.contains("trace_stage_summary_json"));
    REQUIRE(debug.contains("trace_fusion_source_summary_json"));
    CHECK((debug.at("trace_rerank_guard_score_gap") == "0.000000"));
}

TEST_CASE("SearchEngine exposes the topology evidence pipeline as its only frontend",
          "[search][evidence_pipeline][catch2]") {
    TopologySearchFixture fix;
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});

    SearchEngineConfig config;
    config.includeDebugInfo = true;
    config.enableParallelExecution = false;
    config.textWeight = 1.0F;
    config.simeonTextWeight = 0.0F;
    config.pathTreeWeight = 0.0F;
    config.kgWeight = 0.0F;
    config.vectorWeight = 0.0F;
    config.entityVectorWeight = 0.0F;
    config.tagWeight = 0.0F;
    config.metadataWeight = 0.0F;
    config.enableGraphRerank = false;
    config.enableReranking = false;

    SearchExecutionContext context = defaultSearchExecutionContext();
    context.freshness.lexicalReady = true;
    SearchExecutionContextGuard contextGuard(context);

    SearchEngine engine(fix.repo, fix.vectorDb, nullptr, fix.kgStore, config);
    SearchParams params;
    params.limit = 2;
    auto response = engine.searchWithResponse("alpha", params);

    REQUIRE(response);
    REQUIRE(response.value().results.size() == 2);
    const auto& debug = response.value().debugStats;
    CHECK(debug.at("search_pipeline_name") == "topology_evidence");
    CHECK(std::stoull(debug.at("candidate_pipeline_input_components")) > 0);
    CHECK(std::stoull(debug.at("candidate_pipeline_aggregated_candidates")) >=
          response.value().results.size());
    CHECK(debug.at("candidate_pipeline_topology_annotated_candidates") == "0");
}

TEST_CASE("SearchEngine cross reranker promotes lower fused candidate",
          "[search][rerank][catch2]") {
    TopologySearchFixture fix;
    fix.addDocument("x1", "alpha one", {1.0F, 0.0F});
    fix.addDocument("x2", "alpha two", {0.9F, 0.1F});

    SearchEngineConfig config;
    config.includeDebugInfo = true;
    config.enableParallelExecution = false;
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

TEST_CASE("Topology routing options preserve the typed product configuration",
          "[search][topology][options][catch2]") {
    SearchEngineConfig config;
    config.topologyRouteScoringMode = SearchEngineConfig::TopologyRouteScoringMode::SeedCoverage;
    config.topologyExpansionSource = SearchEngineConfig::TopologyExpansionSource::GraphNeighbors;
    config.topologyMinClusters = 2;
    config.topologyMaxClusters = 5;
    config.topologyRoutingRepresentativeLimit = 3;
    config.topologyRoutingAnnCandidateLimit = 19;
    config.topologyAdaptiveProbeScoreGap = 0.07F;
    config.topologyNarrowMinBoundaryMargin = 0.11F;
    config.topologyMaxDocs = 17;
    config.topologySparseDenseAlpha = 0.65F;
    config.topologyMinRouteScore = 0.22F;
    config.topologyRouteRiskCalibration.constructionFingerprint = "atlas-fingerprint";
    config.topologyRouteWorkBudget.maxRowsVisited = 23;
    config.topologyGraphNeighborMinScore = 0.31F;
    config.topologyGraphNeighborReciprocalOnly = false;

    const auto options =
        makeTopologyRoutingOptions(config, SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly,
                                   /*weakTier1Query=*/true, /*collectRouteMembership=*/true);

    CHECK(options.routingMode == SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly);
    CHECK(options.routeScoringMode == SearchEngineConfig::TopologyRouteScoringMode::SeedCoverage);
    CHECK(options.expansionSource == SearchEngineConfig::TopologyExpansionSource::GraphNeighbors);
    CHECK(options.weakTier1Query);
    CHECK(options.minClusters == 2U);
    CHECK(options.maxClusters == 5U);
    CHECK(options.representativeLimit == 3U);
    CHECK(options.denseAnnCandidateLimit == 19U);
    CHECK(options.adaptiveProbeScoreGap == Catch::Approx(0.07F));
    CHECK(options.narrowMinBoundaryMargin == Catch::Approx(0.11F));
    CHECK(options.maxDocs == 17U);
    CHECK(options.sparseDenseAlpha == Catch::Approx(0.65F));
    CHECK(options.minRouteScore == Catch::Approx(0.22F));
    CHECK(options.routeRiskCalibration.constructionFingerprint == "atlas-fingerprint");
    CHECK(options.routeWorkBudget.maxRowsVisited == 23U);
    CHECK(options.collectRouteMembership);
    CHECK(options.graphNeighborMinScore == Catch::Approx(0.31F));
    CHECK_FALSE(options.graphNeighborReciprocalOnly);
}

TEST_CASE("Topology route admission requires the protected-relation proof obligations",
          "[search][topology][admission][catch2]") {
    TopologyRouteAdmission admission;
    CHECK_FALSE(admission.eligibleForNarrowing());
    CHECK(admission.denialReason() == "protected_relation_coverage_unavailable");

    admission.protectedRelationCoverage = TopologyProofObligationStatus::Satisfied;
    admission.coordinateSpaceAlignment = TopologyProofObligationStatus::Satisfied;
    admission.protectedFibersRepresented = TopologyProofObligationStatus::Satisfied;
    admission.certificateSaturatesProtectedFibers = TopologyProofObligationStatus::Satisfied;
    admission.routeRisk = TopologyProofObligationStatus::Satisfied;
    admission.work = TopologyProofObligationStatus::Satisfied;
    admission.coverMaterialization = TopologyProofObligationStatus::Satisfied;
    admission.selectedCoverDocuments = 2;
    admission.materializedCoverDocuments = 2;

    CHECK(admission.eligibleForTrialNarrowing());
    CHECK(admission.eligibleForNarrowing());
    CHECK(admission.denialReason().empty());

    admission.protectedRelationCoverage = TopologyProofObligationStatus::Violated;
    CHECK_FALSE(admission.eligibleForTrialNarrowing());
    CHECK_FALSE(admission.eligibleForNarrowing());
    CHECK(admission.denialReason() == "protected_relation_coverage_violated");

    admission.protectedRelationCoverage = TopologyProofObligationStatus::Satisfied;
    admission.protectedFibersRepresented = TopologyProofObligationStatus::Unavailable;
    CHECK(admission.denialReason() == "protected_fibers_representation_unavailable");

    admission.protectedFibersRepresented = TopologyProofObligationStatus::Satisfied;
    admission.certificateSaturatesProtectedFibers = TopologyProofObligationStatus::Violated;
    CHECK(admission.denialReason() == "protected_fiber_saturation_violated");
}

TEST_CASE("Topology route certificate owns the theorem action boundary",
          "[search][topology][certificate][catch2]") {
    TopologyRouteCertificate certificate;
    CHECK(certificate.action() == TopologyRouteAction::Global);
    CHECK_FALSE(certificate.eligibleForTrialNarrowing());

    certificate.allowedDocumentHashes.insert("document");
    CHECK(certificate.action() == TopologyRouteAction::Augment);

    auto& admission = certificate.admission;
    admission.coordinateSpaceAlignment = TopologyProofObligationStatus::Satisfied;
    admission.protectedRelationCoverage = TopologyProofObligationStatus::Satisfied;
    admission.protectedFibersRepresented = TopologyProofObligationStatus::Satisfied;
    admission.certificateSaturatesProtectedFibers = TopologyProofObligationStatus::Satisfied;
    admission.routeRisk = TopologyProofObligationStatus::Satisfied;
    admission.coverMaterialization = TopologyProofObligationStatus::Satisfied;
    admission.selectedCoverDocuments = 1;
    admission.materializedCoverDocuments = 1;

    CHECK_FALSE(certificate.isConstructionBound());
    CHECK_FALSE(certificate.eligibleForTrialNarrowing());
    certificate.constructionFingerprint = "construction";
    certificate.selectedCoverIds = {"cover"};
    certificate.selectedProtectedRelationFiberIds = {"fiber"};
    CHECK(certificate.isConstructionBound());
    CHECK(certificate.eligibleForTrialNarrowing());
    CHECK(certificate.action() == TopologyRouteAction::Augment);

    admission.work = TopologyProofObligationStatus::Satisfied;
    CHECK(certificate.action() == TopologyRouteAction::Narrow);
}

TEST_CASE("Topology routing session reports route confidence and seed coverage",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1", "not-in-any-cluster"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.maxClusters = 2;
    request.options.maxDocs = 8;
    request.options.minRouteScore = 0.0F;

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
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.maxClusters = 2;
    request.options.maxDocs = 8;
    request.options.minRouteScore = 1000000.0F;

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    CHECK((session.routesRejected >= 1));
    CHECK((session.acceptedRoutes == 0));
    CHECK((session.bestRouteScore == 0.0F));
    CHECK((session.meanAcceptedRouteScore == 0.0F));
    CHECK((session.seedsInRoutedClusters == 0));
    CHECK((session.addedCandidates == 0));
}

TEST_CASE("Topology routing expands through query-independent medoids",
          "[search][topology][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.maxClusters = 2;
    request.options.maxDocs = 16;
    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);
    REQUIRE(session.artifactAdmitted);
    CHECK((session.addedCandidates <= session.acceptedRoutes));
    CHECK((session.routedCandidateHashes.size() <= session.acceptedRoutes));
}

TEST_CASE("Topology routing refuses hard narrowing without complete theorem evidence",
          "[search][topology][narrowing][admission][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    seedTwoClusterTopology(fix);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y2"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.maxClusters = 1;
    request.options.maxDocs = 1;
    request.options.collectRouteMembership = true;

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    REQUIRE(session.artifactAdmitted);
    REQUIRE(session.certificate.hasUsefulRoute());
    CHECK_FALSE(session.certificate.admission.eligibleForNarrowing());
    CHECK(session.certificate.admission.coverMaterialization ==
          TopologyProofObligationStatus::Violated);
    CHECK(session.certificate.admission.certificateSaturatesProtectedFibers ==
          TopologyProofObligationStatus::Violated);
    CHECK(session.certificate.admission.selectedCoverDocuments == 2U);
    CHECK(session.certificate.admission.materializedCoverDocuments == 1U);
    CHECK(session.certificate.admission.denialReason() == "cover_materialization_incomplete");
    CHECK(session.routedCandidateHashes.empty());
    CHECK(session.routedDocs == 1U);
    CHECK(session.certificate.allowedDocumentHashes.size() == 1U);
    CHECK(session.addedCandidates == 0U);
    CHECK(session.addedCandidateHashes.empty());
    CHECK(session.certificate.allowedDocumentHashes.contains("y2"));
    CHECK_FALSE(session.certificate.allowedDocumentHashes.contains("y1"));
    CHECK(session.candidateStructureEvidence.size() == 1U);
    REQUIRE(session.candidateStructureEvidence.contains("y2"));
    CHECK(session.candidateStructureEvidence.at("y2").scaleAgreement == Catch::Approx(0.0F));

    request.options.maxDocs = 0;
    const auto unbounded = runTopologyRoutingSession(request, fix.repo, fix.kgStore);
    REQUIRE(unbounded.certificate.hasUsefulRoute());
    CHECK_FALSE(unbounded.certificate.admission.eligibleForNarrowing());
    CHECK(unbounded.certificate.admission.coverMaterialization ==
          TopologyProofObligationStatus::Satisfied);
    CHECK(unbounded.certificate.admission.certificateSaturatesProtectedFibers ==
          TopologyProofObligationStatus::Satisfied);
    CHECK(unbounded.certificate.admission.selectedCoverDocuments == 2U);
    CHECK(unbounded.certificate.admission.materializedCoverDocuments == 2U);
    CHECK(unbounded.certificate.admission.denialReason() ==
          "protected_fibers_representation_unavailable");
    CHECK(unbounded.certificate.allowedDocumentHashes.size() == 2U);
    CHECK(unbounded.certificate.allowedDocumentHashes.contains("y1"));
    CHECK(unbounded.certificate.allowedDocumentHashes.contains("y2"));
    CHECK_FALSE(unbounded.certificate.constructionFingerprint.empty());
    REQUIRE(unbounded.certificate.selectedCoverIds.size() == 1U);
    REQUIRE(unbounded.certificate.selectedProtectedRelationFiberIds.size() == 1U);
    CHECK(unbounded.routedCandidateHashes.contains("y1"));
}

TEST_CASE("Topology routing exposes selected multiscale structural evidence from the snapshot",
          "[search][topology][evidence_pipeline][multiscale][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    auto batch = buildMultiscaleOverlapTopologyBatch();

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y1"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.maxClusters = 2;
    request.options.maxDocs = 1;
    request.options.collectRouteMembership = true;
    request.snapshotCache = std::make_shared<TopologyRoutingSnapshotCache>([batch]() mutable {
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
    });

    const auto session = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    REQUIRE(session.artifactAdmitted);
    REQUIRE(session.certificate.allowedDocumentHashGroups.size() == 2U);
    CHECK(session.certificate.allowedDocumentHashGroups[0].contains("y1"));
    CHECK(session.certificate.allowedDocumentHashGroups[1].contains("y1"));
    REQUIRE(session.candidateStructureEvidence.contains("y1"));
    const auto& evidence = session.candidateStructureEvidence.at("y1");
    CHECK(evidence.scaleAgreement == Catch::Approx(1.0F));
    CHECK(evidence.overlapSupport > 0.0F);
    CHECK(evidence.persistenceSupport > 0.0F);
    CHECK(evidence.cohesionSupport > 0.0F);
    CHECK(evidence.bridgeSupport > 0.0F);
    CHECK(evidence.densitySupport > 0.0F);
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
    CHECK((first.value().snapshot->membershipsByDocumentHash.size() == batch.memberships.size()));

    auto second = cache.get(batch.topologyEpoch);
    REQUIRE(second.has_value());
    REQUIRE(second.value().snapshot);
    CHECK(second.value().cacheHit);
    CHECK((loadCalls == 1));
}

TEST_CASE("Topology snapshot cache upgrades an exact index when ANN is later required",
          "[search][topology][cache][ann][catch2]") {
    auto batch = buildTwoClusterTopologyBatch();
    int loadCalls = 0;
    TopologyRoutingSnapshotCache cache([&]() {
        ++loadCalls;
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{batch}};
    });

    const auto exact = cache.get(batch.topologyEpoch, false);
    REQUIRE(exact.has_value());
    REQUIRE(exact.value().snapshot);
    CHECK_FALSE(exact.value().snapshot->denseAnnBuildAttempted);
    CHECK_FALSE(exact.value().snapshot->sparseRouteIndex.centroidAnnIndex);

    const auto upgraded = cache.get(batch.topologyEpoch, true);
    REQUIRE(upgraded.has_value());
    REQUIRE(upgraded.value().snapshot);
    CHECK(upgraded.value().cacheHit);
    CHECK(upgraded.value().snapshot->denseAnnBuildAttempted);
    CHECK(upgraded.value().snapshot->sparseRouteIndex.centroidAnnIndex);
    CHECK(loadCalls == 1);
}

TEST_CASE("Topology construction fingerprint excludes publication and routing representatives",
          "[search][topology][cache][identity][catch2]") {
    auto baseline = buildTwoClusterTopologyBatch();
    auto experimental = baseline;
    experimental.snapshotId = "different-publication";
    experimental.generatedAtUnixSeconds += 100;
    experimental.topologyEpoch += 1;
    experimental.clusters.front().routingRepresentatives.push_back(
        yams::topology::ClusterRoutingRepresentative{.documentHash = "x1",
                                                     .embedding = {1.0F, 0.0F}});

    const auto baselineIdentity = topologyRoutingConstructionFingerprint(baseline);
    const auto experimentalIdentity = topologyRoutingConstructionFingerprint(experimental);
    REQUIRE(baselineIdentity.size() == 16);
    CHECK(experimentalIdentity == baselineIdentity);

    experimental.embeddingSpaceIdentity = "test-space-v2";
    CHECK(topologyRoutingConstructionFingerprint(experimental) != baselineIdentity);
    experimental.embeddingSpaceIdentity = baseline.embeddingSpaceIdentity;

    experimental.clusters.front().memberDocumentHashes.push_back("new-member");
    CHECK(topologyRoutingConstructionFingerprint(experimental) != baselineIdentity);
}

TEST_CASE("Topology routing refuses theorem admission across coordinate spaces",
          "[search][topology][identity][admission][catch2]") {
    TopologySearchFixture fix;
    seedTopologyDocuments(fix);
    auto batch = buildTwoClusterTopologyBatch();
    const auto constructionFingerprint = topologyRoutingConstructionFingerprint(batch);

    TopologyRoutingSessionRequest request;
    request.query = "omega";
    request.seedDocumentHashes = {"y2"};
    request.queryEmbedding = std::vector<float>{0.0F, 1.0F};
    request.queryEmbeddingSpaceIdentity = "different-space-v1";
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.weakTier1Query = true;
    request.options.maxClusters = 1;
    request.options.maxDocs = 0;
    request.options.collectRouteMembership = true;
    request.options.routeRiskCalibration.constructionFingerprint = constructionFingerprint;
    request.options.routeRiskCalibration.calibrationQueries = 50;
    request.options.routeRiskCalibration.protectedCandidates = 100;
    request.options.routeRiskCalibration.missedProtectedCandidates = 0;
    request.options.routeRiskCalibration.minCalibrationQueries = 30;
    request.options.routeRiskCalibration.maxMissesPerThousand = 10;
    request.options.routeRiskCalibration.minBoundaryMargin = 0.0F;
    request.options.routeRiskCalibration.minSeedHits = 1;
    request.snapshotCache = std::make_shared<TopologyRoutingSnapshotCache>([batch]() mutable {
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
    });

    const auto mismatched = runTopologyRoutingSession(request, fix.repo, fix.kgStore);
    REQUIRE(mismatched.certificate.hasUsefulRoute());
    CHECK(mismatched.certificate.admission.coordinateSpaceAlignment ==
          TopologyProofObligationStatus::Violated);
    CHECK(mismatched.certificate.admission.denialReason() == "coordinate_space_mismatch");
    CHECK(mismatched.certificate.action() == TopologyRouteAction::Augment);

    request.queryEmbeddingSpaceIdentity = "test-space-v1";
    const auto aligned = runTopologyRoutingSession(request, fix.repo, fix.kgStore);
    CHECK(aligned.certificate.admission.coordinateSpaceAlignment ==
          TopologyProofObligationStatus::Satisfied);
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

TEST_CASE("Topology snapshot cache admits a materialized boundary spill",
          "[search][topology][cache][validation][overlap][catch2]") {
    auto batch = buildTwoClusterTopologyBatch();
    REQUIRE(batch.clusters.size() == 2);
    const auto primaryId = batch.memberships.front().clusterId;
    auto secondary = std::ranges::find_if(
        batch.clusters, [&](const auto& cluster) { return cluster.clusterId != primaryId; });
    REQUIRE(secondary != batch.clusters.end());
    secondary->memberDocumentHashes.push_back(batch.memberships.front().documentHash);
    std::ranges::sort(secondary->memberDocumentHashes);
    secondary->memberCount = secondary->memberDocumentHashes.size();
    batch.memberships.front().overlapClusterIds = {secondary->clusterId};

    TopologyRoutingSnapshotCache cache([batch]() mutable {
        return Result<std::optional<yams::topology::TopologyArtifactBatch>>{
            std::optional<yams::topology::TopologyArtifactBatch>{std::move(batch)}};
    });

    const auto result = cache.get();
    REQUIRE(result.has_value());
    REQUIRE(result.value().snapshot);
    CHECK(result.value()
              .snapshot->sparseRouteIndex.clustersByDocumentHash
              .at(batch.memberships.front().documentHash)
              .size() == 1);
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

TEST_CASE("legacy topology false preserves the hybrid product default",
          "[unit][search][topology][config]") {
    SearchEngineConfig config;
    REQUIRE(config.topologyRoutingMode == SearchEngineConfig::TopologyRoutingMode::HybridAssist);

    CHECK(resolveTopologyRoutingMode(config, false) ==
          SearchEngineConfig::TopologyRoutingMode::HybridAssist);
    CHECK(resolveTopologyRoutingMode(config, true) ==
          SearchEngineConfig::TopologyRoutingMode::WeakQueryOnly);

    config.topologyRoutingMode = SearchEngineConfig::TopologyRoutingMode::Disabled;
    CHECK(resolveTopologyRoutingMode(config) == SearchEngineConfig::TopologyRoutingMode::Disabled);
}

TEST_CASE("fillTopologySkipReason preserves session reason and fills product defaults",
          "[unit][search][topology]") {
    using Mode = SearchEngineConfig::TopologyRoutingMode;
    TopologyRoutingOptions options;
    TopologyRoutingSessionResult session;

    options.routingMode = Mode::HybridAssist;
    std::string reason = "graph_medoid_neighbors";
    fillTopologySkipReason(reason, options, /*hasStores=*/true, session);
    CHECK((reason == "graph_medoid_neighbors"));

    options.routingMode = Mode::Disabled;
    reason.clear();
    fillTopologySkipReason(reason, options, /*hasStores=*/true, session);
    CHECK((reason == "disabled"));

    options.routingMode = Mode::HybridAssist;
    session.loadSucceeded = true;
    session.routedClusters = 2;
    reason.clear();
    fillTopologySkipReason(reason, options, /*hasStores=*/true, session);
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

TEST_CASE("Graph-neighbor trace separates stored relation from the selected cap",
          "[unit][search][topology][graph_neighbors][trace]") {
    TopologySearchFixture fix;
    fix.addDocument("seed", "seed", {1.0F, 0.0F});
    fix.addDocument("near", "near", {0.9F, 0.1F});
    fix.addDocument("far", "far", {0.8F, 0.2F});

    const auto nodeIds = fix.kgStore->upsertNodes({
        KGNode{.nodeKey = "doc:seed", .type = "document"},
        KGNode{.nodeKey = "doc:near", .type = "document"},
        KGNode{.nodeKey = "doc:far", .type = "document"},
    });
    REQUIRE(nodeIds.has_value());
    REQUIRE(nodeIds.value().size() == 3);
    REQUIRE(fix.kgStore
                ->addEdge(KGEdge{.srcNodeId = nodeIds.value()[0],
                                 .dstNodeId = nodeIds.value()[1],
                                 .relation = "semantic_neighbor",
                                 .weight = 0.9F})
                .has_value());
    REQUIRE(fix.kgStore
                ->addEdge(KGEdge{.srcNodeId = nodeIds.value()[0],
                                 .dstNodeId = nodeIds.value()[2],
                                 .relation = "semantic_neighbor",
                                 .weight = 0.8F})
                .has_value());

    TopologyRoutingSessionRequest request;
    request.seedDocumentHashes = {"seed"};
    request.options.routingMode = SearchEngineConfig::TopologyRoutingMode::HybridAssist;
    request.options.expansionSource = SearchEngineConfig::TopologyExpansionSource::GraphNeighbors;
    request.options.maxDocs = 1;
    request.options.collectRouteMembership = true;
    request.options.collectGraphDiagnostics = true;
    request.options.graphNeighborMinScore = 0.0F;
    request.options.graphNeighborReciprocalOnly = false;

    const auto result = runTopologyRoutingSession(request, fix.repo, fix.kgStore);

    REQUIRE(result.graphNeighborTrace.collected);
    CHECK(result.graphNeighborTrace.edgeFetchLimit == 64);
    CHECK(result.graphNeighborTrace.fetchTruncatedSeedCount == 0);
    CHECK(result.graphNeighborTrace.seedDocumentIds == std::vector<std::string>{"seed"});
    CHECK(result.graphNeighborTrace.relationCandidateCount == 2);
    CHECK(result.graphNeighborTrace.relationCandidateDocumentIds ==
          std::vector<std::string>{"near", "far"});
    CHECK(result.graphNeighborTrace.fetchedCandidateCount == 2);
    CHECK(result.graphNeighborTrace.fetchedCandidateDocumentIds ==
          std::vector<std::string>{"near", "far"});
    CHECK(result.graphNeighborTrace.eligibleCandidateCount == 2);
    CHECK(result.graphNeighborTrace.eligibleCandidateDocumentIds ==
          std::vector<std::string>{"near", "far"});
    REQUIRE(result.routedCandidateDocIds.size() == 1);
    CHECK(result.routedCandidateDocIds.front() == "near");
}
