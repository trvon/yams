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
#include <yams/topology/topology_metadata_store.h>

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
