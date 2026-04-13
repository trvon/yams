// SPDX-License-Identifier: GPL-3.0-or-later

#include <chrono>
#include <filesystem>
#include <memory>

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_input_extractor.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/topology/topology_offline_analyzer.h>
#include <yams/vector/vector_database.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::topology;

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

DocumentInfo makeDocumentWithPath(const std::string& path, const std::string& hash) {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 123;
    info.sha256Hash = hash;
    info.mimeType = "text/plain";
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    auto derived = computePathDerivedValues(path);
    info.filePath = derived.normalizedPath;
    info.pathPrefix = derived.pathPrefix;
    info.reversePath = derived.reversePath;
    info.pathHash = derived.pathHash;
    info.parentHash = derived.parentHash;
    info.pathDepth = derived.pathDepth;
    return info;
}

struct TopologyFixture {
    TopologyFixture() {
        dbPath = tempDbPath("topology_baseline_");

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;

        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());

        repository = std::make_shared<MetadataRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());
    }

    ~TopologyFixture() {
        kgStore.reset();
        repository.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repository;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
    std::shared_ptr<vector::VectorDatabase> vectorDb;
};

} // namespace

TEST_CASE("Topology baseline engine builds cluster artifacts", "[unit][topology][baseline]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;

    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{.documentHash = "ccc", .filePath = "/repo/c.md", .neighbors = {}},
    };

    auto result = engine.buildArtifacts(docs, config);
    REQUIRE(result.has_value());

    const auto& batch = result.value();
    REQUIRE(batch.clusters.size() == 2);
    REQUIRE(batch.memberships.size() == 3);

    const auto pairClusterIt =
        std::find_if(batch.clusters.begin(), batch.clusters.end(),
                     [](const ClusterArtifact& cluster) { return cluster.memberCount == 2; });
    REQUIRE(pairClusterIt != batch.clusters.end());
    REQUIRE(pairClusterIt->medoid.has_value());
    CHECK(pairClusterIt->medoid->documentHash == "aaa");
    CHECK(pairClusterIt->persistenceScore == Catch::Approx(0.9));

    const auto outlierMembershipIt =
        std::find_if(batch.memberships.begin(), batch.memberships.end(),
                     [](const DocumentClusterMembership& membership) {
                         return membership.documentHash == "ccc";
                     });
    REQUIRE(outlierMembershipIt != batch.memberships.end());
    CHECK(outlierMembershipIt->role == DocumentTopologyRole::Outlier);

    StableClusterTopologyRouter router;
    auto routes = router.route(TopologyRouteRequest{.queryText = "find b",
                                                    .seedDocumentHashes = {"bbb"},
                                                    .limit = 1,
                                                    .preferStableClusters = true,
                                                    .weakQueryOnly = true},
                               batch);
    REQUIRE(routes.has_value());
    REQUIRE(routes.value().size() == 1);
    CHECK(routes.value().front().clusterId == pairClusterIt->clusterId);
}

TEST_CASE("Metadata KG topology store persists memberships and latest snapshot",
          "[unit][topology][store]") {
    TopologyFixture fix;

    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/a.md", "aaa")).has_value());
    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/b.md", "bbb")).has_value());
    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/c.md", "ccc")).has_value());

    ConnectedComponentTopologyEngine engine;
    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{.documentHash = "ccc", .filePath = "/repo/c.md", .neighbors = {}},
    };
    auto batchResult = engine.buildArtifacts(docs, TopologyBuildConfig{});
    REQUIRE(batchResult.has_value());

    MetadataKgTopologyArtifactStore store(fix.repository, fix.kgStore);
    REQUIRE(store.storeBatch(batchResult.value()).has_value());

    auto loadedBatchResult = store.loadLatest();
    REQUIRE(loadedBatchResult.has_value());
    REQUIRE(loadedBatchResult.value().has_value());
    CHECK(loadedBatchResult.value()->snapshotId == batchResult.value().snapshotId);
    CHECK(loadedBatchResult.value()->clusters.size() == batchResult.value().clusters.size());

    auto membershipsResult =
        store.loadMemberships(std::vector<std::string>{"aaa", "ccc", "missing"});
    REQUIRE(membershipsResult.has_value());
    REQUIRE(membershipsResult.value().size() == 2);

    const auto aaaIt =
        std::find_if(membershipsResult.value().begin(), membershipsResult.value().end(),
                     [](const DocumentClusterMembership& membership) {
                         return membership.documentHash == "aaa";
                     });
    REQUIRE(aaaIt != membershipsResult.value().end());
    CHECK(aaaIt->role == DocumentTopologyRole::Medoid);

    const auto cccIt =
        std::find_if(membershipsResult.value().begin(), membershipsResult.value().end(),
                     [](const DocumentClusterMembership& membership) {
                         return membership.documentHash == "ccc";
                     });
    REQUIRE(cccIt != membershipsResult.value().end());
    CHECK(cccIt->role == DocumentTopologyRole::Outlier);
}

TEST_CASE("Topology extractor and offline analyzer use real stores",
          "[unit][topology][extractor][offline]") {
    TopologyFixture fix;

    vector::VectorDatabaseConfig vectorConfig;
    vectorConfig.database_path = ":memory:";
    vectorConfig.embedding_dim = 4;
    vectorConfig.create_if_missing = true;
    vectorConfig.use_in_memory = true;
    fix.vectorDb = std::make_shared<vector::VectorDatabase>(vectorConfig);
    REQUIRE(fix.vectorDb->initialize());

    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/a.md", "aaa")).has_value());
    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/b.md", "bbb")).has_value());
    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/c.md", "ccc")).has_value());

    vector::VectorRecord aVec;
    aVec.chunk_id = "doc-aaa";
    aVec.document_hash = "aaa";
    aVec.embedding = {1.0F, 0.0F, 0.0F, 0.0F};
    aVec.level = vector::EmbeddingLevel::DOCUMENT;
    REQUIRE(fix.vectorDb->insertVector(aVec));

    vector::VectorRecord bVec;
    bVec.chunk_id = "doc-bbb";
    bVec.document_hash = "bbb";
    bVec.embedding = {0.9F, 0.1F, 0.0F, 0.0F};
    bVec.level = vector::EmbeddingLevel::DOCUMENT;
    REQUIRE(fix.vectorDb->insertVector(bVec));

    vector::VectorRecord cVec;
    cVec.chunk_id = "doc-ccc";
    cVec.document_hash = "ccc";
    cVec.embedding = {0.0F, 1.0F, 0.0F, 0.0F};
    cVec.level = vector::EmbeddingLevel::DOCUMENT;
    REQUIRE(fix.vectorDb->insertVector(cVec));

    metadata::KGNode aNode;
    aNode.nodeKey = "doc:aaa";
    aNode.label = std::string{"aaa"};
    aNode.type = std::string{"document"};
    auto aNodeId = fix.kgStore->upsertNode(aNode);
    REQUIRE(aNodeId.has_value());

    metadata::KGNode bNode;
    bNode.nodeKey = "doc:bbb";
    bNode.label = std::string{"bbb"};
    bNode.type = std::string{"document"};
    auto bNodeId = fix.kgStore->upsertNode(bNode);
    REQUIRE(bNodeId.has_value());

    metadata::KGNode cNode;
    cNode.nodeKey = "doc:ccc";
    cNode.label = std::string{"ccc"};
    cNode.type = std::string{"document"};
    auto cNodeId = fix.kgStore->upsertNode(cNode);
    REQUIRE(cNodeId.has_value());

    std::vector<metadata::KGEdge> edges;
    edges.push_back(metadata::KGEdge{.srcNodeId = aNodeId.value(),
                                     .dstNodeId = bNodeId.value(),
                                     .relation = "semantic_neighbor",
                                     .weight = 0.9F});
    edges.push_back(metadata::KGEdge{.srcNodeId = bNodeId.value(),
                                     .dstNodeId = aNodeId.value(),
                                     .relation = "semantic_neighbor",
                                     .weight = 0.9F});
    REQUIRE(fix.kgStore->addEdgesUnique(edges).has_value());

    auto extractor =
        std::make_shared<TopologyInputExtractor>(fix.repository, fix.kgStore, fix.vectorDb);
    TopologyExtractionStats extractionStats;
    auto extracted = extractor->extract(TopologyExtractionConfig{.limit = 10,
                                                                 .maxNeighborsPerDocument = 8,
                                                                 .includeEmbeddings = true,
                                                                 .includeMetadata = true,
                                                                 .requireEmbeddings = true,
                                                                 .requireGraphNode = true},
                                        &extractionStats);
    REQUIRE(extracted.has_value());
    REQUIRE(extracted.value().size() == 3);
    CHECK(extractionStats.documentsReturned == 3);

    const auto aExtracted = std::find_if(
        extracted.value().begin(), extracted.value().end(),
        [](const TopologyDocumentInput& input) { return input.documentHash == "aaa"; });
    REQUIRE(aExtracted != extracted.value().end());
    REQUIRE(aExtracted->embedding.size() == 4);
    REQUIRE(aExtracted->neighbors.size() == 1);
    CHECK(aExtracted->neighbors.front().documentHash == "bbb");
    CHECK(aExtracted->neighbors.front().reciprocal);
    CHECK(aExtracted->metadata.contains("mime_type"));

    auto engine = std::make_shared<ConnectedComponentTopologyEngine>();
    TopologyOfflineAnalyzer analyzer(extractor, engine);
    auto analysis = analyzer.analyze(
        TopologyExtractionConfig{.limit = 10,
                                 .maxNeighborsPerDocument = 8,
                                 .includeEmbeddings = true,
                                 .includeMetadata = true,
                                 .requireEmbeddings = true,
                                 .requireGraphNode = true},
        TopologyBuildConfig{.inputKind = TopologyInputKind::Hybrid, .reciprocalOnly = true});
    REQUIRE(analysis.has_value());
    CHECK(analysis.value().artifacts.clusters.size() == 2);
    CHECK(analysis.value().extractionStats.documentsReturned == 3);
}
