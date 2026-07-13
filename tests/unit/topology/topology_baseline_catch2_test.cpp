// SPDX-License-Identifier: GPL-3.0-or-later

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <unordered_map>
#include <unordered_set>

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
#include <yams/topology/topology_representatives.h>
#include <yams/vector/vector_database.h>

#include "tests/common/test_helpers_catch2.h"

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

std::unordered_map<std::string, std::string> clusterByDocument(const TopologyArtifactBatch& batch) {
    std::unordered_map<std::string, std::string> clusterOf;
    for (const auto& membership : batch.memberships) {
        clusterOf.emplace(membership.documentHash, membership.clusterId);
    }
    return clusterOf;
}

const ClusterArtifact* findClusterContaining(const TopologyArtifactBatch& batch,
                                             const std::string& documentHash) {
    const auto it = std::find_if(
        batch.clusters.begin(), batch.clusters.end(), [&](const ClusterArtifact& cluster) {
            return std::find(cluster.memberDocumentHashes.begin(),
                             cluster.memberDocumentHashes.end(),
                             documentHash) != cluster.memberDocumentHashes.end();
        });
    return it == batch.clusters.end() ? nullptr : &*it;
}

bool containsHash(const std::vector<std::string>& hashes, const std::string& hash) {
    return std::find(hashes.begin(), hashes.end(), hash) != hashes.end();
}

void requireWellFormedBatch(const TopologyArtifactBatch& batch) {
    std::unordered_set<std::string> clusterIds;
    for (const auto& cluster : batch.clusters) {
        CHECK((cluster.memberCount == cluster.memberDocumentHashes.size()));
        CHECK(clusterIds.insert(cluster.clusterId).second);
    }

    std::unordered_set<std::string> membershipKeys;
    for (const auto& membership : batch.memberships) {
        CAPTURE(membership.documentHash, membership.clusterId);
        CHECK(clusterIds.contains(membership.clusterId));
        CHECK(
            membershipKeys.insert(membership.documentHash + "\x1F" + membership.clusterId).second);
    }

    for (const auto& cluster : batch.clusters) {
        for (const auto& hash : cluster.memberDocumentHashes) {
            CAPTURE(cluster.clusterId, hash);
            const auto it = std::find_if(
                batch.memberships.begin(), batch.memberships.end(),
                [&](const DocumentClusterMembership& membership) {
                    return membership.documentHash == hash &&
                           (membership.clusterId == cluster.clusterId ||
                            containsHash(membership.overlapClusterIds, cluster.clusterId));
                });
            CHECK((it != batch.memberships.end()));
        }
    }
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
    REQUIRE((batch.clusters.size() == 2));
    REQUIRE((batch.memberships.size() == 3));

    const auto pairClusterIt =
        std::find_if(batch.clusters.begin(), batch.clusters.end(),
                     [](const ClusterArtifact& cluster) { return cluster.memberCount == 2; });
    REQUIRE((pairClusterIt != batch.clusters.end()));
    REQUIRE(pairClusterIt->medoid.has_value());
    CHECK((pairClusterIt->medoid->documentHash == "aaa"));
    CHECK((pairClusterIt->persistenceScore == Catch::Approx(0.9)));
    CHECK((pairClusterIt->densityScore == Catch::Approx(1.0)));

    const auto outlierMembershipIt =
        std::find_if(batch.memberships.begin(), batch.memberships.end(),
                     [](const DocumentClusterMembership& membership) {
                         return membership.documentHash == "ccc";
                     });
    REQUIRE((outlierMembershipIt != batch.memberships.end()));
    CHECK((outlierMembershipIt->role == DocumentTopologyRole::Outlier));

    StableClusterTopologyRouter router;
    auto routes = router.route(TopologyRouteRequest{.queryText = "find b",
                                                    .seedDocumentHashes = {"bbb"},
                                                    .limit = 1,
                                                    .preferStableClusters = true,
                                                    .weakQueryOnly = true},
                               batch);
    REQUIRE(routes.has_value());
    REQUIRE((routes.value().size() == 1));
    CHECK((routes.value().front().clusterId == pairClusterIt->clusterId));
}

TEST_CASE("Topology baseline enforces Lean edge-filter contract",
          "[unit][topology][baseline][contract]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;

    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors =
                {
                    {.documentHash = "bbb", .score = 0.9F, .reciprocal = true},
                    {.documentHash = "ccc", .score = 0.49F, .reciprocal = true},
                    {.documentHash = "ddd", .score = 0.9F, .reciprocal = false},
                    {.documentHash = "missing", .score = 0.9F, .reciprocal = true},
                    {.documentHash = "aaa", .score = 1.0F, .reciprocal = true},
                    {.documentHash = "", .score = 1.0F, .reciprocal = true},
                }},
        TopologyDocumentInput{.documentHash = "bbb", .filePath = "/repo/b.md", .neighbors = {}},
        TopologyDocumentInput{.documentHash = "ccc", .filePath = "/repo/c.md", .neighbors = {}},
        TopologyDocumentInput{.documentHash = "ddd", .filePath = "/repo/d.md", .neighbors = {}},
    };

    auto result = engine.buildArtifacts(docs, config);
    REQUIRE(result.has_value());
    const auto& batch = result.value();
    requireWellFormedBatch(batch);

    const auto clusterOf = clusterByDocument(batch);
    REQUIRE((clusterOf.size() == docs.size()));
    CHECK((clusterOf.at("aaa") == clusterOf.at("bbb")));
    CHECK((clusterOf.at("aaa") != clusterOf.at("ccc")));
    CHECK((clusterOf.at("aaa") != clusterOf.at("ddd")));
    CHECK((clusterOf.at("ccc") != clusterOf.at("ddd")));
    CHECK((findClusterContaining(batch, "missing") == nullptr));
}

TEST_CASE("Topology baseline satisfies connected component and router contracts",
          "[unit][topology][baseline][contract]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;

    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.8F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.8F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "ddd", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ddd",
            .filePath = "/repo/d.md",
            .neighbors = {{.documentHash = "ccc", .score = 0.9F, .reciprocal = true}}},
    };

    auto result = engine.buildArtifacts(docs, config);
    REQUIRE(result.has_value());
    const auto& batch = result.value();
    requireWellFormedBatch(batch);

    const auto clusterOf = clusterByDocument(batch);
    CHECK((clusterOf.at("aaa") == clusterOf.at("bbb")));
    CHECK((clusterOf.at("ccc") == clusterOf.at("ddd")));
    CHECK((clusterOf.at("aaa") != clusterOf.at("ccc")));

    StableClusterTopologyRouter router;
    TopologyRouteRequest request;
    request.seedDocumentHashes = {"bbb", "missing"};
    request.limit = 1;
    auto routes = router.route(request, batch);
    REQUIRE(routes.has_value());
    REQUIRE((routes.value().size() <= request.limit));
    for (const auto& route : routes.value()) {
        CAPTURE(route.clusterId);
        CHECK(std::any_of(
            batch.clusters.begin(), batch.clusters.end(),
            [&](const ClusterArtifact& cluster) { return cluster.clusterId == route.clusterId; }));
    }
}

TEST_CASE("Topology baseline dirty regions contain seeds and respect rebuild budget contract",
          "[unit][topology][baseline][contract]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;

    std::vector<TopologyDocumentInput> existingDocs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.8F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.8F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "ddd", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ddd",
            .filePath = "/repo/d.md",
            .neighbors = {{.documentHash = "ccc", .score = 0.9F, .reciprocal = true}}},
    };
    auto existing = engine.buildArtifacts(existingDocs, config);
    REQUIRE(existing.has_value());

    std::vector<TopologyDocumentInput> changedDocs{
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "ccc", .score = 0.95F, .reciprocal = true}}},
    };

    auto region = engine.defineDirtyRegion(existing.value(), changedDocs, config);
    REQUIRE(region.has_value());
    CHECK(containsHash(region.value().seedDocumentHashes, "bbb"));
    for (const auto& seed : region.value().seedDocumentHashes) {
        CAPTURE(seed);
        CHECK(containsHash(region.value().expandedDocumentHashes, seed));
    }
    CHECK(containsHash(region.value().expandedDocumentHashes, "aaa"));
    CHECK(containsHash(region.value().expandedDocumentHashes, "ccc"));
    CHECK(containsHash(region.value().expandedDocumentHashes, "ddd"));
    CHECK_FALSE(region.value().requiresWiderRebuild);
    CHECK((region.value().expandedDocumentHashes.size() <= config.maxDirtyRegionDocs));

    auto tightConfig = config;
    tightConfig.maxDirtyRegionDocs = 2;
    auto tightRegion = engine.defineDirtyRegion(existing.value(), changedDocs, tightConfig);
    REQUIRE(tightRegion.has_value());
    CHECK(tightRegion.value().requiresWiderRebuild);
    CHECK(tightRegion.value().exceededRegionBudget);
}

TEST_CASE("Sparse-guided topology routing uses persisted cluster centroids",
          "[unit][topology][baseline]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;

    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .embedding = {1.0F, 0.0F},
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .embedding = {0.9F, 0.1F},
            .neighbors = {{.documentHash = "aaa", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .embedding = {0.0F, 1.0F},
            .neighbors = {{.documentHash = "ddd", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ddd",
            .filePath = "/repo/d.md",
            .embedding = {0.1F, 0.9F},
            .neighbors = {{.documentHash = "ccc", .score = 0.9F, .reciprocal = true}}},
    };

    auto result = engine.buildArtifacts(docs, config);
    REQUIRE(result.has_value());
    REQUIRE((result.value().clusters.size() == 2U));
    for (const auto& cluster : result.value().clusters) {
        REQUIRE((cluster.centroidEmbedding.size() == 2U));
    }

    SparseGuidedClusterRouter router;
    auto routes = router.route(TopologyRouteRequest{.queryText = "find y-axis docs",
                                                    .seedDocumentHashes = {},
                                                    .limit = 1,
                                                    .preferStableClusters = true,
                                                    .weakQueryOnly = true,
                                                    .scoringMode = RouteScoringMode::Current,
                                                    .queryEmbedding = {0.0F, 1.0F},
                                                    .sparseDenseAlpha = 0.0F},
                               result.value());
    REQUIRE(routes.has_value());
    REQUIRE((routes.value().size() == 1U));
    REQUIRE(routes.value().front().medoidDocumentHash.has_value());
    CHECK((routes.value().front().medoidDocumentHash.value() == "ccc"));
}

TEST_CASE("Sparse-guided topology routing uses weighted lexical evidence",
          "[unit][topology][routing][weighted-seeds]") {
    TopologyArtifactBatch batch;
    batch.clusters = {
        ClusterArtifact{.clusterId = "focused", .memberCount = 1, .memberDocumentHashes = {"high"}},
        ClusterArtifact{
            .clusterId = "broad", .memberCount = 2, .memberDocumentHashes = {"low-a", "low-b"}},
    };

    SparseGuidedClusterRouter router;
    TopologyRouteRequest request;
    request.seedDocumentHashes = {"high", "low-a", "low-b"};
    request.weightedSeedDocuments = {
        WeightedDocumentSeed{.documentHash = "high", .weight = 1.0F},
        WeightedDocumentSeed{.documentHash = "low-a", .weight = 0.1F},
        WeightedDocumentSeed{.documentHash = "low-b", .weight = 0.1F},
    };
    request.limit = 1;
    request.sparseDenseAlpha = 1.0F;

    auto routes = router.route(request, batch);
    REQUIRE(routes.has_value());
    REQUIRE(routes.value().size() == 1);
    CHECK(routes.value().front().clusterId == "focused");
}

TEST_CASE("Sparse-guided topology routing reuses a prepared membership index",
          "[unit][topology][routing][prepared-index]") {
    TopologyArtifactBatch batch;
    batch.clusters = {
        ClusterArtifact{.clusterId = "focused",
                        .memberCount = 2,
                        .memberDocumentHashes = {"high", "peer"},
                        .centroidEmbedding = {1.0F, 0.0F}},
        ClusterArtifact{.clusterId = "broad",
                        .memberCount = 3,
                        .memberDocumentHashes = {"low-a", "low-b", "low-c"},
                        .centroidEmbedding = {0.0F, 1.0F}},
    };

    SparseGuidedClusterRouter router;
    TopologyRouteRequest request;
    request.seedDocumentHashes = {"high", "low-a", "low-b"};
    request.weightedSeedDocuments = {
        WeightedDocumentSeed{.documentHash = "high", .weight = 1.0F},
        WeightedDocumentSeed{.documentHash = "low-a", .weight = 0.2F},
        WeightedDocumentSeed{.documentHash = "low-b", .weight = 0.2F},
    };
    request.queryEmbedding = {1.0F, 0.0F};
    request.limit = 2;
    request.sparseDenseAlpha = 0.7F;

    auto baseline = router.route(request, batch);
    REQUIRE(baseline.has_value());

    const auto index = SparseGuidedClusterRouter::buildRouteIndex(batch);
    SparseRouteWork work;
    auto indexed = router.route(request, batch, index, &work);
    REQUIRE(indexed.has_value());
    REQUIRE(indexed.value().size() == baseline.value().size());
    for (std::size_t i = 0; i < indexed.value().size(); ++i) {
        CHECK(indexed.value()[i].clusterId == baseline.value()[i].clusterId);
        CHECK(indexed.value()[i].routeScore == Catch::Approx(baseline.value()[i].routeScore));
    }
    CHECK(work.seedClusterLookups == 3);
    CHECK(work.clusterMemberHashesScanned == 0);
    CHECK(work.queryNormEvaluations == 1);
}

TEST_CASE("Topology construction emits a deterministic bounded diverse routing cover",
          "[unit][topology][routing][representatives]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = false;
    config.routingRepresentativeCount = 3;

    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{.documentHash = "a",
                              .embedding = {1.0F, 0.0F},
                              .neighbors = {{.documentHash = "b", .score = 1.0F}}},
        TopologyDocumentInput{.documentHash = "b",
                              .embedding = {0.8F, 0.2F},
                              .neighbors = {{.documentHash = "c", .score = 1.0F}}},
        TopologyDocumentInput{.documentHash = "c",
                              .embedding = {0.0F, 1.0F},
                              .neighbors = {{.documentHash = "d", .score = 1.0F}}},
        TopologyDocumentInput{.documentHash = "d",
                              .embedding = {-0.8F, 0.2F},
                              .neighbors = {{.documentHash = "a", .score = 1.0F}}},
    };

    auto first = engine.buildArtifacts(docs, config);
    auto second = engine.buildArtifacts(docs, config);
    REQUIRE(first.has_value());
    REQUIRE(second.has_value());
    REQUIRE(first.value().clusters.size() == 1);
    REQUIRE(second.value().clusters.size() == 1);
    const auto& firstReps = first.value().clusters.front().routingRepresentatives;
    const auto& secondReps = second.value().clusters.front().routingRepresentatives;
    REQUIRE(firstReps.size() == 2);
    REQUIRE(secondReps.size() == firstReps.size());
    for (std::size_t index = 0; index < firstReps.size(); ++index) {
        CHECK(firstReps[index].documentHash == secondReps[index].documentHash);
        CHECK(firstReps[index].embedding == secondReps[index].embedding);
        CHECK(firstReps[index].embedding.size() == 2);
    }
    CHECK(firstReps[0].documentHash != firstReps[1].documentHash);
}

TEST_CASE("SOAR boundary spill chooses a complementary secondary residual",
          "[unit][topology][construction][overlap][soar]") {
    const std::vector<TopologyDocumentInput> documents{
        TopologyDocumentInput{.documentHash = "x", .embedding = {2.0F, 2.0F}},
        TopologyDocumentInput{.documentHash = "near", .embedding = {0.9F, 2.0F}},
        TopologyDocumentInput{.documentHash = "orthogonal", .embedding = {2.0F, 0.8F}},
    };
    TopologyArtifactBatch artifacts;
    artifacts.clusters = {
        ClusterArtifact{.clusterId = "primary",
                        .memberCount = 1,
                        .memberDocumentHashes = {"x"},
                        .centroidEmbedding = {1.0F, 2.0F}},
        ClusterArtifact{.clusterId = "near",
                        .memberCount = 1,
                        .memberDocumentHashes = {"near"},
                        .centroidEmbedding = {0.9F, 2.0F}},
        ClusterArtifact{.clusterId = "orthogonal",
                        .memberCount = 1,
                        .memberDocumentHashes = {"orthogonal"},
                        .centroidEmbedding = {2.0F, 0.8F}},
    };
    artifacts.memberships = {
        DocumentClusterMembership{.documentHash = "x", .clusterId = "primary"},
        DocumentClusterMembership{.documentHash = "near", .clusterId = "near"},
        DocumentClusterMembership{.documentHash = "orthogonal", .clusterId = "orthogonal"},
    };

    TopologyBuildConfig config;
    config.allowOverlap = true;
    config.overlapLimit = 1;
    config.overlapBoundaryDistanceRatio = 1.25;
    config.overlapResidualPenalty = 1.0;

    TopologyRouteRequest routeRequest;
    routeRequest.seedDocumentHashes = {"x"};
    routeRequest.sparseDenseAlpha = 1.0F;
    SparseGuidedClusterRouter router;
    const auto primaryRoutes = router.route(routeRequest, artifacts);
    REQUIRE(primaryRoutes);
    REQUIRE_FALSE(primaryRoutes.value().empty());
    REQUIRE(primaryRoutes.value().front().clusterId == "primary");

    CHECK(applyOrthogonalBoundarySpill(documents, config, artifacts) == 1);
    CHECK(artifacts.memberships.front().overlapClusterIds ==
          std::vector<std::string>{"orthogonal"});
    CHECK(containsHash(artifacts.clusters[2].memberDocumentHashes, "x"));
    CHECK_FALSE(containsHash(artifacts.clusters[1].memberDocumentHashes, "x"));
    const auto routeIndex = SparseGuidedClusterRouter::buildRouteIndex(artifacts);
    REQUIRE(routeIndex.clustersByDocumentHash.contains("x"));
    CHECK(routeIndex.clustersByDocumentHash.at("x").size() == 1);
    const auto spilledRoutes = router.route(routeRequest, artifacts, routeIndex);
    REQUIRE(spilledRoutes);
    REQUIRE_FALSE(spilledRoutes.value().empty());
    CHECK(spilledRoutes.value().front().clusterId == primaryRoutes.value().front().clusterId);

    auto naive = artifacts;
    naive.clusters[2].memberDocumentHashes = {"orthogonal"};
    naive.clusters[2].memberCount = 1;
    naive.memberships.front().overlapClusterIds.clear();
    config.overlapResidualPenalty = 0.0;
    CHECK(applyOrthogonalBoundarySpill(documents, config, naive) == 1);
    CHECK(naive.memberships.front().overlapClusterIds == std::vector<std::string>{"near"});
}

TEST_CASE("Sparse-guided routing scores the closest bounded cluster representative",
          "[unit][topology][routing][representatives]") {
    TopologyArtifactBatch batch;
    batch.clusters = {
        ClusterArtifact{
            .clusterId = "centroid-winner",
            .memberCount = 2,
            .memberDocumentHashes = {"a", "b"},
            .centroidEmbedding = {0.8F, 0.2F},
        },
        ClusterArtifact{
            .clusterId = "cover-winner",
            .memberCount = 2,
            .memberDocumentHashes = {"c", "d"},
            .centroidEmbedding = {0.0F, 1.0F},
            .routingRepresentatives =
                {
                    ClusterRoutingRepresentative{.documentHash = "d", .embedding = {1.0F, 0.0F}},
                },
        },
    };

    TopologyRouteRequest request;
    request.queryEmbedding = {1.0F, 0.0F};
    request.sparseDenseAlpha = 0.0F;
    request.limit = 1;

    const auto index = SparseGuidedClusterRouter::buildRouteIndex(batch);
    SparseRouteWork work;
    SparseGuidedClusterRouter router;
    auto routed = router.route(request, batch, index, &work);

    REQUIRE(routed.has_value());
    REQUIRE(routed.value().size() == 1);
    CHECK(routed.value().front().clusterId == "cover-winner");
    CHECK(work.representativeDistanceEvaluations == 3);
}

TEST_CASE("Sparse-guided routing limits a prebuilt representative cover at query time",
          "[unit][topology][routing][representatives]") {
    TopologyArtifactBatch batch;
    batch.clusters = {
        ClusterArtifact{
            .clusterId = "centroid-winner",
            .memberCount = 2,
            .memberDocumentHashes = {"a", "b"},
            .centroidEmbedding = {0.8F, 0.2F},
        },
        ClusterArtifact{
            .clusterId = "cover-winner",
            .memberCount = 2,
            .memberDocumentHashes = {"c", "d"},
            .centroidEmbedding = {0.0F, 1.0F},
            .routingRepresentatives =
                {
                    ClusterRoutingRepresentative{.documentHash = "d", .embedding = {1.0F, 0.0F}},
                },
        },
    };
    const auto index = SparseGuidedClusterRouter::buildRouteIndex(batch);
    SparseGuidedClusterRouter router;

    TopologyRouteRequest centroidOnly;
    centroidOnly.queryEmbedding = {1.0F, 0.0F};
    centroidOnly.sparseDenseAlpha = 0.0F;
    centroidOnly.limit = 1;
    centroidOnly.maxRoutingRepresentatives = 1;
    SparseRouteWork centroidWork;
    auto centroidRoute = router.route(centroidOnly, batch, index, &centroidWork);
    REQUIRE(centroidRoute.has_value());
    CHECK(centroidRoute.value().front().clusterId == "centroid-winner");
    CHECK(centroidWork.representativeDistanceEvaluations == 2);

    auto covered = centroidOnly;
    covered.maxRoutingRepresentatives = 2;
    SparseRouteWork coveredWork;
    auto coveredRoute = router.route(covered, batch, index, &coveredWork);
    REQUIRE(coveredRoute.has_value());
    CHECK(coveredRoute.value().front().clusterId == "cover-winner");
    CHECK(coveredWork.representativeDistanceEvaluations == 3);
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
            .embedding = {1.0F, 0.0F},
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .embedding = {0.0F, 1.0F},
            .neighbors = {{.documentHash = "aaa", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{.documentHash = "ccc",
                              .filePath = "/repo/c.md",
                              .embedding = {-1.0F, 0.0F},
                              .neighbors = {}},
    };
    TopologyBuildConfig buildConfig;
    buildConfig.routingRepresentativeCount = 2;
    auto batchResult = engine.buildArtifacts(docs, buildConfig);
    REQUIRE(batchResult.has_value());

    MetadataKgTopologyArtifactStore store(fix.repository, fix.kgStore);
    REQUIRE(store.storeBatch(batchResult.value()).has_value());

    auto loadedBatchResult = store.loadLatest();
    REQUIRE(loadedBatchResult.has_value());
    REQUIRE(loadedBatchResult.value().has_value());
    CHECK((loadedBatchResult.value()->snapshotId == batchResult.value().snapshotId));
    CHECK((loadedBatchResult.value()->clusters.size() == batchResult.value().clusters.size()));
    REQUIRE_FALSE(loadedBatchResult.value()->clusters.empty());
    CHECK(loadedBatchResult.value()->clusters.front().densityScore ==
          Catch::Approx(batchResult.value().clusters.front().densityScore));
    REQUIRE(loadedBatchResult.value()->clusters.front().routingRepresentatives.size() == 1);
    CHECK(loadedBatchResult.value()->clusters.front().routingRepresentatives.front().documentHash ==
          batchResult.value().clusters.front().routingRepresentatives.front().documentHash);
    CHECK(loadedBatchResult.value()->clusters.front().routingRepresentatives.front().embedding ==
          batchResult.value().clusters.front().routingRepresentatives.front().embedding);

    auto membershipsResult =
        store.loadMemberships(std::vector<std::string>{"aaa", "ccc", "missing"});
    REQUIRE(membershipsResult.has_value());
    REQUIRE((membershipsResult.value().size() == 2));

    const auto aaaIt =
        std::find_if(membershipsResult.value().begin(), membershipsResult.value().end(),
                     [](const DocumentClusterMembership& membership) {
                         return membership.documentHash == "aaa";
                     });
    REQUIRE((aaaIt != membershipsResult.value().end()));
    CHECK((aaaIt->role == DocumentTopologyRole::Medoid));

    const auto cccIt =
        std::find_if(membershipsResult.value().begin(), membershipsResult.value().end(),
                     [](const DocumentClusterMembership& membership) {
                         return membership.documentHash == "ccc";
                     });
    REQUIRE((cccIt != membershipsResult.value().end()));
    CHECK((cccIt->role == DocumentTopologyRole::Outlier));
}

TEST_CASE("Metadata KG topology store clears stale memberships on partial replacement",
          "[unit][topology][store][partial]") {
    TopologyFixture fix;

    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/a.md", "aaa")).has_value());
    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/b.md", "bbb")).has_value());
    REQUIRE(fix.repository->insertDocument(makeDocumentWithPath("/repo/c.md", "ccc")).has_value());

    ConnectedComponentTopologyEngine engine;
    MetadataKgTopologyArtifactStore store(fix.repository, fix.kgStore);

    std::vector<TopologyDocumentInput> existingDocs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.8F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.8F, .reciprocal = true},
                          {.documentHash = "ccc", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
    };
    auto initialBatch = engine.buildArtifacts(existingDocs, TopologyBuildConfig{});
    REQUIRE(initialBatch.has_value());
    REQUIRE(store.storeBatch(initialBatch.value()).has_value());

    auto initialMemberships = store.loadMemberships(std::vector<std::string>{"aaa", "bbb", "ccc"});
    REQUIRE(initialMemberships.has_value());
    REQUIRE((initialMemberships.value().size() == 3));

    std::vector<TopologyDocumentInput> changedRegion{
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "ccc", .score = 0.95F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.95F, .reciprocal = true}}},
    };
    auto updatedBatch =
        engine.updateArtifacts(initialBatch.value(), changedRegion, TopologyBuildConfig{});
    REQUIRE(updatedBatch.has_value());
    REQUIRE(store.storeBatch(updatedBatch.value()).has_value());

    auto updatedMemberships = store.loadMemberships(std::vector<std::string>{"aaa", "bbb", "ccc"});
    REQUIRE(updatedMemberships.has_value());
    REQUIRE((updatedMemberships.value().size() == 2));
    CHECK(std::none_of(updatedMemberships.value().begin(), updatedMemberships.value().end(),
                       [](const DocumentClusterMembership& membership) {
                           return membership.documentHash == "aaa";
                       }));
}

TEST_CASE("Topology extractor and offline analyzer use real stores",
          "[unit][topology][extractor][offline]") {
    yams::test::ScopedEnvVar disableVectors("YAMS_DISABLE_VECTORS",
                                            std::optional<std::string>("0"));
    yams::test::ScopedEnvVar skipVecInit("YAMS_SQLITE_VEC_SKIP_INIT",
                                         std::optional<std::string>("0"));
    yams::test::ScopedEnvVar disableInMemory("YAMS_VDB_IN_MEMORY", std::nullopt);

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
    REQUIRE((extracted.value().size() == 3));
    CHECK((extractionStats.documentsReturned == 3));

    const auto aExtracted = std::find_if(
        extracted.value().begin(), extracted.value().end(),
        [](const TopologyDocumentInput& input) { return input.documentHash == "aaa"; });
    REQUIRE((aExtracted != extracted.value().end()));
    REQUIRE((aExtracted->embedding.size() == 4));
    REQUIRE((aExtracted->neighbors.size() == 1));
    CHECK((aExtracted->neighbors.front().documentHash == "bbb"));
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
    CHECK((analysis.value().artifacts.clusters.size() == 2));
    CHECK((analysis.value().extractionStats.documentsReturned == 3));
}

TEST_CASE("Topology update replaces removed cluster memberships consistently",
          "[unit][topology][update]") {
    ConnectedComponentTopologyEngine engine;

    std::vector<TopologyDocumentInput> existingDocs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.8F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.8F, .reciprocal = true},
                          {.documentHash = "ccc", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{.documentHash = "ddd", .filePath = "/repo/d.md", .neighbors = {}},
    };
    auto existing = engine.buildArtifacts(existingDocs, TopologyBuildConfig{});
    REQUIRE(existing.has_value());
    REQUIRE((existing.value().memberships.size() == 4));

    std::vector<TopologyDocumentInput> changedRegion{
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "ccc", .score = 0.95F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.95F, .reciprocal = true}}},
    };
    TopologyUpdateStats stats;
    auto updated =
        engine.updateArtifacts(existing.value(), changedRegion, TopologyBuildConfig{}, &stats);
    REQUIRE(updated.has_value());

    const auto aaaMembership =
        std::find_if(updated.value().memberships.begin(), updated.value().memberships.end(),
                     [](const auto& membership) { return membership.documentHash == "aaa"; });
    CHECK((aaaMembership == updated.value().memberships.end()));
    CHECK((updated.value().memberships.size() == 3));
    CHECK((stats.membershipsUpdated >= 3));
}

TEST_CASE("Topology update preserves cluster identity when old medoid survives",
          "[unit][topology][continuity]") {
    ConnectedComponentTopologyEngine engine;

    std::vector<TopologyDocumentInput> existingDocs{
        TopologyDocumentInput{
            .documentHash = "aaa",
            .filePath = "/repo/a.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.7F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "aaa", .score = 0.7F, .reciprocal = true},
                          {.documentHash = "ccc", .score = 0.95F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.95F, .reciprocal = true}}},
    };
    auto existing = engine.buildArtifacts(existingDocs, TopologyBuildConfig{});
    REQUIRE(existing.has_value());
    REQUIRE((existing.value().clusters.size() == 1));
    REQUIRE(existing.value().clusters.front().medoid.has_value());
    CHECK((existing.value().clusters.front().medoid->documentHash == "bbb"));
    const auto oldClusterId = existing.value().clusters.front().clusterId;

    std::vector<TopologyDocumentInput> changedRegion{
        TopologyDocumentInput{
            .documentHash = "bbb",
            .filePath = "/repo/b.md",
            .neighbors = {{.documentHash = "ccc", .score = 0.99F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "ccc",
            .filePath = "/repo/c.md",
            .neighbors = {{.documentHash = "bbb", .score = 0.99F, .reciprocal = true}}},
    };
    auto updated = engine.updateArtifacts(existing.value(), changedRegion, TopologyBuildConfig{});
    REQUIRE(updated.has_value());

    const auto survivingCluster = std::find_if(
        updated.value().clusters.begin(), updated.value().clusters.end(), [](const auto& cluster) {
            return std::find(cluster.memberDocumentHashes.begin(),
                             cluster.memberDocumentHashes.end(),
                             "bbb") != cluster.memberDocumentHashes.end();
        });
    REQUIRE((survivingCluster != updated.value().clusters.end()));
    CHECK((survivingCluster->clusterId == oldClusterId));
}

TEST_CASE("Topology baseline splits oversized CC components (anti-giant)",
          "[unit][topology][baseline][construction]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.5;
    config.maxComponentDocs = 2;

    // Path a-b-c-d-e all reciprocal high-score edges → one giant without the cap.
    auto makeEdge = [](const char* target) {
        return TopologyNeighbor{.documentHash = target, .score = 0.9F, .reciprocal = true};
    };
    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{.documentHash = "a", .filePath = "/a", .neighbors = {makeEdge("b")}},
        TopologyDocumentInput{
            .documentHash = "b", .filePath = "/b", .neighbors = {makeEdge("a"), makeEdge("c")}},
        TopologyDocumentInput{
            .documentHash = "c", .filePath = "/c", .neighbors = {makeEdge("b"), makeEdge("d")}},
        TopologyDocumentInput{
            .documentHash = "d", .filePath = "/d", .neighbors = {makeEdge("c"), makeEdge("e")}},
        TopologyDocumentInput{.documentHash = "e", .filePath = "/e", .neighbors = {makeEdge("d")}},
    };

    auto uncapped = engine.buildArtifacts(docs, [&] {
        TopologyBuildConfig c = config;
        c.maxComponentDocs = 0;
        return c;
    }());
    REQUIRE(uncapped.has_value());
    REQUIRE((uncapped.value().clusters.size() == 1));
    CHECK((uncapped.value().clusters.front().memberCount == 5));

    auto capped = engine.buildArtifacts(docs, config);
    REQUIRE(capped.has_value());
    requireWellFormedBatch(capped.value());
    REQUIRE((capped.value().memberships.size() == 5));
    for (const auto& cluster : capped.value().clusters) {
        CHECK((cluster.memberCount <= config.maxComponentDocs));
    }
    // Must produce more than one published cluster once the giant is split.
    CHECK((capped.value().clusters.size() >= 2));
}

TEST_CASE("Topology baseline minEdgeScore filters weak links",
          "[unit][topology][baseline][construction]") {
    ConnectedComponentTopologyEngine engine;
    TopologyBuildConfig config;
    config.reciprocalOnly = true;
    config.minEdgeScore = 0.8;
    config.maxComponentDocs = 0;

    std::vector<TopologyDocumentInput> docs{
        TopologyDocumentInput{
            .documentHash = "a",
            .filePath = "/a",
            .neighbors = {{.documentHash = "b", .score = 0.9F, .reciprocal = true},
                          {.documentHash = "c", .score = 0.5F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "b",
            .filePath = "/b",
            .neighbors = {{.documentHash = "a", .score = 0.9F, .reciprocal = true}}},
        TopologyDocumentInput{
            .documentHash = "c",
            .filePath = "/c",
            .neighbors = {{.documentHash = "a", .score = 0.5F, .reciprocal = true}}},
    };

    auto result = engine.buildArtifacts(docs, config);
    REQUIRE(result.has_value());
    const auto byDoc = clusterByDocument(result.value());
    CHECK((byDoc.at("a") == byDoc.at("b")));
    CHECK((byDoc.at("a") != byDoc.at("c")));
}
