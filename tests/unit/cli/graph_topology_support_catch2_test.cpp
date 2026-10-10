// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_scope_support.h>
#include <yams/cli/graph_topology_support.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_artifacts.h>

#include <chrono>
#include <filesystem>
#include <fstream>
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

TEST_CASE("GraphTopologySupport scopes cluster stats and membership views to the cwd",
          "[cli][graph][topology]") {
    // --scope-cwd covers every path under the cwd, as `--list-type --scope-cwd` does (#280).
    // It used to keep only <cwd>/src and <cwd>/include, so tests/ fell out of scope.
    GraphTopologySupportFixture fixture;
    REQUIRE(fixture.metadataRepo->insertDocument(makeDocument("/elsewhere/notes.md", "hash-out"))
                .has_value());
    GraphTopologySupport support(nullptr, "", fixture.metadataRepo);
    auto snapshot = makeSnapshot();
    snapshot.memberships.push_back(DocumentClusterMembership{
        .documentHash = "hash-out",
        .clusterId = "cluster-b",
        .clusterLevel = 1,
        .bridgeScore = 0.4,
        .role = DocumentTopologyRole::Outlier,
    });
    const std::filesystem::path cwd = "/repo";

    auto scopeRes = support.buildCurrentScopePathSet(cwd);
    REQUIRE(scopeRes.has_value());
    const auto& scopedPaths = scopeRes.value();
    CHECK(scopedPaths == std::unordered_set<std::string>{"/repo/src/main.cpp",
                                                         "/repo/include/main.h",
                                                         "/repo/tests/main_test.cpp"});

    const auto statsById = support.buildClusterStatsById(snapshot, scopedPaths, cwd);
    REQUIRE(statsById.contains("cluster-a"));
    REQUIRE(statsById.contains("cluster-b"));
    CHECK(statsById.at("cluster-a").scopedMemberCount == 2);
    CHECK(statsById.at("cluster-b").scopedMemberCount == 1);

    const auto views =
        support.buildClusterMembershipViews(snapshot, snapshot.clusters.front(), scopedPaths, cwd);
    REQUIRE(views.size() == 2);
    CHECK(views[0].membership != nullptr);
    CHECK(views[0].resolvedPath == "/repo/src/main.cpp");
    CHECK(views[0].inScope);
    CHECK(views[1].membership != nullptr);
    CHECK(views[1].resolvedPath == "/repo/include/main.h");
    CHECK(views[1].inScope);

    const auto scopedViews =
        support.buildClusterMembershipViews(snapshot, snapshot.clusters[1], scopedPaths, cwd);
    REQUIRE(scopedViews.size() == 2);
    CHECK(scopedViews[0].resolvedPath == "/repo/tests/main_test.cpp");
    CHECK(scopedViews[0].inScope);
    CHECK(scopedViews[1].resolvedPath == "/elsewhere/notes.md");
    CHECK_FALSE(scopedViews[1].inScope);

    const auto unscopedViews = support.buildClusterMembershipViews(snapshot, snapshot.clusters[1]);
    REQUIRE(unscopedViews.size() == 2);
    CHECK(unscopedViews.front().resolvedPath == "/repo/tests/main_test.cpp");
    CHECK(unscopedViews.front().inScope);
}

TEST_CASE("Graph --scope-cwd path set covers a corpus without src/ or include/",
          "[cli][graph][topology][scope]") {
    // A corpus laid out as <lib>/include and <lib>/src (or not at all) had an empty scope, so
    // `yams graph --topology-clusters --scope-cwd` reported no scoped members.
    GraphTopologySupportFixture fixture;
    const auto base = std::filesystem::path(makeTempDbPath()).replace_extension(".scope");
    std::filesystem::create_directories(base);
    const auto realRoot = std::filesystem::weakly_canonical(base) / "corpus";
    const auto header = realRoot / "simeon" / "include" / "simeon" / "bm25.hpp";
    const auto source = realRoot / "simeon" / "src" / "bm25.cpp";
    const auto sibling = std::filesystem::weakly_canonical(base) / "corpus-other" / "x.md";
    for (const auto& path : {header, source, sibling}) {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream(path) << "x\n";
    }
    REQUIRE(fixture.metadataRepo->insertDocument(makeDocument(header.generic_string(), "h-hdr"))
                .has_value());
    REQUIRE(fixture.metadataRepo->insertDocument(makeDocument(source.generic_string(), "h-src"))
                .has_value());
    REQUIRE(fixture.metadataRepo->insertDocument(makeDocument(sibling.generic_string(), "h-sib"))
                .has_value());

    const std::unordered_set<std::string> expected{header.generic_string(),
                                                   source.generic_string()};
    auto checkScope = [&](const std::filesystem::path& cwd) {
        auto scoped = buildGraphScopedPathSet(cwd, fixture.metadataRepo);
        REQUIRE(scoped.has_value());
        CHECK(scoped.value() == expected);
    };

    checkScope(realRoot);

    // A cwd reached through a symlink still matches the resolved stored paths.
    const auto linkRoot = base / "corpus-link";
    std::error_code linkEc;
    std::filesystem::create_directory_symlink(realRoot, linkRoot, linkEc);
    if (!linkEc) {
        checkScope(linkRoot);
    }

    std::error_code ec;
    std::filesystem::remove_all(base, ec);
}

TEST_CASE("GraphTopologySupport returns null snapshot and empty path without CLI context",
          "[cli][graph][topology]") {
    GraphTopologySupport support(nullptr, "snap-missing");

    const auto snapshot = support.loadTopologySnapshot();
    REQUIRE(snapshot.has_value());
    CHECK_FALSE(snapshot.value().has_value());
    CHECK(support.resolveDocumentPathByHash("missing-hash").empty());
}
