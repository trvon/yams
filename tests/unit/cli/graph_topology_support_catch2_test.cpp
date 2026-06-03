// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_topology_support.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_artifacts.h>

#include <chrono>
#include <filesystem>
#include <memory>

using namespace yams::cli;
using namespace yams::metadata;
using namespace yams::topology;

namespace {

std::filesystem::path makeTempDbPath() {
    auto dir = std::filesystem::temp_directory_path() / "yams_graph_topology_support_test";
    std::filesystem::create_directories(dir);
    return dir /
           ("graph_topology_support_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
}

DocumentInfo makeDocument(const std::string& path, const std::string& hash) {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = 128;
    info.sha256Hash = hash;
    info.mimeType = "text/plain";
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    populatePathDerivedFields(info);
    return info;
}

TopologyArtifactBatch makeSnapshot() {
    TopologyArtifactBatch snapshot;
    snapshot.snapshotId = "snap-1";

    ClusterArtifact clusterA;
    clusterA.clusterId = "cluster-a";
    clusterA.memberCount = 2;
    snapshot.clusters.push_back(clusterA);

    ClusterArtifact clusterB;
    clusterB.clusterId = "cluster-b";
    clusterB.memberCount = 1;
    snapshot.clusters.push_back(clusterB);

    snapshot.memberships.push_back(DocumentClusterMembership{
        .documentHash = "hash-src",
        .clusterId = "cluster-a",
        .clusterLevel = 0,
        .bridgeScore = 0.1,
        .role = DocumentTopologyRole::Core,
    });
    snapshot.memberships.push_back(DocumentClusterMembership{
        .documentHash = "hash-include",
        .clusterId = "cluster-a",
        .clusterLevel = 0,
        .bridgeScore = 0.2,
        .role = DocumentTopologyRole::Bridge,
    });
    snapshot.memberships.push_back(DocumentClusterMembership{
        .documentHash = "hash-test",
        .clusterId = "cluster-b",
        .clusterLevel = 1,
        .bridgeScore = 0.3,
        .role = DocumentTopologyRole::Outlier,
    });

    return snapshot;
}

struct GraphTopologySupportFixture {
    GraphTopologySupportFixture() {
        dbPath = makeTempDbPath();

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_shared<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());

        metadataRepo = std::make_shared<MetadataRepository>(*pool);
        REQUIRE(metadataRepo->insertDocument(makeDocument("/repo/src/main.cpp", "hash-src"))
                    .has_value());
        REQUIRE(metadataRepo->insertDocument(makeDocument("/repo/include/main.h", "hash-include"))
                    .has_value());
        REQUIRE(metadataRepo->insertDocument(makeDocument("/repo/tests/main_test.cpp", "hash-test"))
                    .has_value());
    }

    ~GraphTopologySupportFixture() {
        metadataRepo.reset();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        std::filesystem::remove(dbPath.string() + "-shm", ec);
        std::filesystem::remove(dbPath.string() + "-wal", ec);
    }

    std::filesystem::path dbPath;
    std::shared_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> metadataRepo;
};

} // namespace

TEST_CASE("GraphTopologySupport labels topology enums", "[cli][graph][topology]") {
    CHECK(topologyInputKindLabel(TopologyInputKind::SemanticNeighborGraph) ==
          "semantic_neighbor_graph");
    CHECK(topologyInputKindLabel(TopologyInputKind::EmbeddingNeighborhood) ==
          "embedding_neighborhood");
    CHECK(topologyInputKindLabel(TopologyInputKind::Hybrid) == "hybrid");

    CHECK(topologyRoleLabel(DocumentTopologyRole::Core) == "core");
    CHECK(topologyRoleLabel(DocumentTopologyRole::Bridge) == "bridge");
    CHECK(topologyRoleLabel(DocumentTopologyRole::Medoid) == "medoid");
    CHECK(topologyRoleLabel(DocumentTopologyRole::Outlier) == "outlier");

    CHECK(formatTopologyRoleCounts({{"bridge", 2}, {"core", 3}}) == "core(3), bridge(2)");
}

TEST_CASE("GraphTopologySupport aggregates cluster stats directly", "[cli][graph][topology]") {
    GraphTopologySupportFixture fixture;
    GraphTopologySupport support(nullptr, "", fixture.metadataRepo);
    const auto snapshot = makeSnapshot();

    const auto statsById = support.buildClusterStatsById(snapshot);
    REQUIRE(statsById.size() == 2);
    REQUIRE(statsById.contains("cluster-a"));
    REQUIRE(statsById.contains("cluster-b"));

    CHECK(statsById.at("cluster-a").scopedMemberCount == 2);
    CHECK(statsById.at("cluster-a").roleCounts.at("core") == 1);
    CHECK(statsById.at("cluster-a").roleCounts.at("bridge") == 1);
    CHECK(statsById.at("cluster-b").scopedMemberCount == 1);
    CHECK(statsById.at("cluster-b").roleCounts.at("outlier") == 1);
}

TEST_CASE("GraphTopologySupport scopes cluster stats and membership views by resolved paths",
          "[cli][graph][topology]") {
    GraphTopologySupportFixture fixture;
    GraphTopologySupport support(nullptr, "", fixture.metadataRepo);
    const auto snapshot = makeSnapshot();
    const std::unordered_set<std::string> scopedPaths = {"/repo/src/main.cpp",
                                                         "/repo/include/main.h"};
    const std::filesystem::path cwd = "/repo";

    const auto statsById = support.buildClusterStatsById(snapshot, scopedPaths, cwd);
    REQUIRE(statsById.contains("cluster-a"));
    REQUIRE(statsById.contains("cluster-b"));
    CHECK(statsById.at("cluster-a").scopedMemberCount == 2);
    CHECK(statsById.at("cluster-b").scopedMemberCount == 0);

    const auto views =
        support.buildClusterMembershipViews(snapshot, snapshot.clusters.front(), scopedPaths, cwd);
    REQUIRE(views.size() == 2);
    CHECK(views[0].membership != nullptr);
    CHECK(views[0].resolvedPath == "/repo/src/main.cpp");
    CHECK(views[0].inScope);
    CHECK(views[1].membership != nullptr);
    CHECK(views[1].resolvedPath == "/repo/include/main.h");
    CHECK(views[1].inScope);

    const auto unscopedViews = support.buildClusterMembershipViews(snapshot, snapshot.clusters[1]);
    REQUIRE(unscopedViews.size() == 1);
    CHECK(unscopedViews.front().resolvedPath == "/repo/tests/main_test.cpp");
    CHECK(unscopedViews.front().inScope);
}

TEST_CASE("GraphTopologySupport returns null snapshot and empty path without CLI context",
          "[cli][graph][topology]") {
    GraphTopologySupport support(nullptr, "snap-missing");

    const auto snapshot = support.loadTopologySnapshot();
    REQUIRE(snapshot.has_value());
    CHECK_FALSE(snapshot.value().has_value());
    CHECK(support.resolveDocumentPathByHash("missing-hash").empty());
}
