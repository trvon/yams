// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later
//
// Ordered members. Every cluster with at least two members may carry a walk order: a permutation
// over its hash-sorted `memberDocumentHashes`, by event time, by session, or as a greedy
// similarity chain from the representative. The order rides in the road sidecar (v3) and never in
// the main snapshot payload or any routing fingerprint; `topology.road_order = none` (the default)
// stores nothing and leaves every byte unchanged.

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/topology_routing_session.h>
#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_codec.h>
#include <yams/topology/topology_member_order.h>
#include <yams/topology/topology_metadata_store.h>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

using namespace yams;
using namespace yams::metadata;
using namespace yams::topology;

namespace {

// ---------------------------------------------------------------------------------------------
// Input fixtures
// ---------------------------------------------------------------------------------------------

std::vector<float> unit2(float x, float y) {
    const float n = std::sqrt(x * x + y * y);
    return {x / n, y / n};
}

std::vector<float> angle2(double degrees) {
    const double radians = degrees * 3.14159265358979323846 / 180.0;
    return {static_cast<float>(std::cos(radians)), static_cast<float>(std::sin(radians))};
}

TopologyDocumentInput mk(std::string hash, std::vector<float> emb = {},
                         std::optional<std::int64_t> eventTime = {}, std::string sessionId = {}) {
    TopologyDocumentInput doc;
    doc.documentHash = std::move(hash);
    doc.filePath = "/tmp/" + doc.documentHash + ".txt";
    doc.embedding = std::move(emb);
    doc.eventTime = eventTime;
    doc.sessionId = std::move(sessionId);
    return doc;
}

TopologyDocumentInput* find(std::vector<TopologyDocumentInput>& docs, const std::string& hash) {
    for (auto& doc : docs) {
        if (doc.documentHash == hash) {
            return &doc;
        }
    }
    return nullptr;
}

void linkSym(std::vector<TopologyDocumentInput>& docs, const std::string& a, const std::string& b,
             float score, bool reciprocal) {
    find(docs, a)->neighbors.push_back(
        TopologyNeighbor{.documentHash = b, .score = score, .reciprocal = reciprocal});
    find(docs, b)->neighbors.push_back(
        TopologyNeighbor{.documentHash = a, .score = score, .reciprocal = reciprocal});
}

std::vector<TopologyDocumentInput> select(const std::vector<TopologyDocumentInput>& docs,
                                          const std::unordered_set<std::string>& hashes) {
    std::vector<TopologyDocumentInput> out;
    for (const auto& doc : docs) {
        if (hashes.contains(doc.documentHash)) {
            out.push_back(doc);
        }
    }
    return out;
}

// A cluster artifact over hash-sorted members, as every engine stores them.
ClusterArtifact clusterOf(std::vector<std::string> members, std::string medoidHash = {}) {
    ClusterArtifact cluster;
    cluster.clusterId = "topology.cluster." + members.front();
    std::ranges::sort(members);
    cluster.memberCount = members.size();
    cluster.memberDocumentHashes = std::move(members);
    if (!medoidHash.empty()) {
        cluster.medoid = ClusterRepresentative{.clusterId = cluster.clusterId,
                                               .documentHash = std::move(medoidHash),
                                               .filePath = {},
                                               .representativeScore = 1.0};
    }
    return cluster;
}

std::vector<std::string> walk(const ClusterArtifact& cluster) {
    std::vector<std::string> out;
    for (const auto index : cluster.orderedMembers) {
        REQUIRE(index < cluster.memberDocumentHashes.size());
        out.push_back(cluster.memberDocumentHashes[index]);
    }
    return out;
}

// Four graph clusters A = {a0, a1, a2}, B = {b0, b1}, C = {c0, c1, c2}, D = {d0, d1} with
// reciprocal intra edges above the 0.5 floor and symmetric cross edges below it. Event times and
// sessions are chosen so that no cluster's time order is its hash order.
std::vector<TopologyDocumentInput> fourClusters() {
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("a0", unit2(1.0F, 0.0F), 300, "s-a"));
    docs.push_back(mk("a1", unit2(0.99F, 0.05F), 100, "s-a"));
    docs.push_back(mk("a2", unit2(0.98F, 0.10F), 200, "s-a2"));
    docs.push_back(mk("b0", unit2(0.0F, 1.0F), 50, "s-b"));
    docs.push_back(mk("b1", unit2(0.05F, 0.99F), 25, "s-b"));
    docs.push_back(mk("c0", unit2(-1.0F, 0.0F), 900, "s-c"));
    docs.push_back(mk("c1", unit2(-0.99F, 0.05F), std::nullopt, ""));
    docs.push_back(mk("c2", unit2(-0.98F, 0.10F), 800, "s-c"));
    docs.push_back(mk("d0", unit2(0.0F, -1.0F), 70, "s-d"));
    docs.push_back(mk("d1", unit2(0.05F, -0.99F), 60, "s-d"));
    linkSym(docs, "a0", "a1", 0.9F, true);
    linkSym(docs, "a1", "a2", 0.9F, true);
    linkSym(docs, "a0", "a2", 0.9F, true);
    linkSym(docs, "b0", "b1", 0.9F, true);
    linkSym(docs, "c0", "c1", 0.9F, true);
    linkSym(docs, "c1", "c2", 0.9F, true);
    linkSym(docs, "c0", "c2", 0.9F, true);
    linkSym(docs, "d0", "d1", 0.9F, true);
    linkSym(docs, "a1", "b0", 0.25F, true);
    linkSym(docs, "a0", "c0", 0.125F, true);
    linkSym(docs, "b1", "c1", 0.5F, false);
    linkSym(docs, "c0", "d1", 0.25F, true);
    linkSym(docs, "b0", "d0", 0.125F, true);
    return docs;
}

const std::string kA = "topology.cluster.a0";
const std::string kB = "topology.cluster.b0";
const std::string kC = "topology.cluster.c0";
const std::string kD = "topology.cluster.d0";
const std::unordered_set<std::string> kRegionAB{"a0", "a1", "a2", "b0", "b1"};

TopologyBuildConfig graphConfig(RoadOrderKind order, std::size_t roadMaxPerCluster = 8) {
    TopologyBuildConfig cfg;
    cfg.mode = TopologyBuildMode::Exact;
    cfg.reciprocalOnly = true;
    cfg.minEdgeScore = 0.5;
    cfg.roadMaxPerCluster = roadMaxPerCluster;
    cfg.roadOrder = order;
    cfg.embeddingSpaceIdentity = "test-space";
    return cfg;
}

const ClusterArtifact& clusterById(const TopologyArtifactBatch& batch, const std::string& id) {
    const auto it = std::ranges::find(batch.clusters, id, &ClusterArtifact::clusterId);
    REQUIRE(it != batch.clusters.end());
    return *it;
}

void checkOrdersWellFormed(const TopologyArtifactBatch& batch, RoadOrderKind expected) {
    for (const auto& cluster : batch.clusters) {
        if (cluster.memberDocumentHashes.size() < 2) {
            CHECK(cluster.orderKind == RoadOrderKind::None);
            CHECK(cluster.orderedMembers.empty());
            continue;
        }
        CHECK(cluster.orderKind == expected);
        if (expected == RoadOrderKind::None) {
            CHECK(cluster.orderedMembers.empty());
            continue;
        }
        CHECK_FALSE(
            validateMemberPermutation(cluster.orderedMembers, cluster.memberDocumentHashes.size())
                .has_value());
    }
}

void alignStamps(TopologyArtifactBatch& batch, const TopologyArtifactBatch& reference) {
    batch.snapshotId = reference.snapshotId;
    batch.generatedAtUnixSeconds = reference.generatedAtUnixSeconds;
    batch.topologyEpoch = reference.topologyEpoch;
}

// ---------------------------------------------------------------------------------------------
// Store fixture (sqlite-backed knowledge graph)
// ---------------------------------------------------------------------------------------------

std::string roadNodeKey(const std::string& snapshotId) {
    return "topology:roads:" + snapshotId;
}

struct StoreFixture {
    StoreFixture() {
        dbPath =
            std::filesystem::temp_directory_path() /
            ("topo_ordered_members_test_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);

        ConnectionPoolConfig poolConfig;
        poolConfig.minConnections = 1;
        poolConfig.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), poolConfig);
        REQUIRE(pool->initialize().has_value());

        repository = std::make_shared<MetadataRepository>(*pool);
        auto kgResult = makeSqliteKnowledgeGraphStore(*pool, KnowledgeGraphStoreConfig{});
        REQUIRE(kgResult.has_value());
        kgStore = std::shared_ptr<KnowledgeGraphStore>(kgResult.value().release());

        for (const auto* hash : {"hash_a", "hash_b", "hash_c", "hash_d", "hash_e"}) {
            DocumentInfo doc;
            doc.sha256Hash = hash;
            doc.filePath = std::string("/repo/") + hash + ".md";
            doc.fileName = std::string(hash) + ".md";
            doc.fileSize = 100;
            doc.mimeType = "text/plain";
            REQUIRE(repository->insertDocument(doc).has_value());
        }
    }

    ~StoreFixture() {
        kgStore.reset();
        repository.reset();
        pool->shutdown();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    void overwriteNodeProperties(const std::string& nodeKey, const std::string& properties) {
        auto res = pool->withConnection([&](Database& db) -> Result<void> {
            auto stmtRes = db.prepare("UPDATE kg_nodes SET properties = ? WHERE node_key = ?");
            if (!stmtRes) {
                return stmtRes.error();
            }
            auto stmt = std::move(stmtRes).value();
            if (auto bound = stmt.bind(1, properties); !bound) {
                return bound.error();
            }
            if (auto bound = stmt.bind(2, nodeKey); !bound) {
                return bound.error();
            }
            auto stepRes = stmt.step();
            if (!stepRes) {
                return stepRes.error();
            }
            return {};
        });
        REQUIRE(res.has_value());
    }

    bool nodeExists(const std::string& nodeKey) {
        auto node = kgStore->getNodeByKey(nodeKey);
        REQUIRE(node.has_value());
        return node.value().has_value();
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repository;
    std::shared_ptr<KnowledgeGraphStore> kgStore;
};

// Two clusters: X = {hash_a, hash_c, hash_e} ordered, Y = {hash_b, hash_d} ordered, with roads.
TopologyArtifactBatch makeStoredBatch(const std::string& snapshotId, uint64_t epoch) {
    TopologyArtifactBatch batch;
    batch.snapshotId = snapshotId;
    batch.algorithm = "connected_components_v1";
    batch.topologyEpoch = epoch;
    batch.generatedAtUnixSeconds = 1700000000 + epoch;

    ClusterArtifact x = clusterOf({"hash_a", "hash_c", "hash_e"});
    x.clusterId = "cluster-x";
    x.centroidEmbedding = {1.0F, 0.0F};
    x.orderKind = RoadOrderKind::Time;
    x.orderedMembers = {2, 0, 1};
    ClusterArtifact y = clusterOf({"hash_b", "hash_d"});
    y.clusterId = "cluster-y";
    y.centroidEmbedding = {0.0F, 1.0F};
    y.orderKind = RoadOrderKind::Session;
    y.orderedMembers = {1, 0};
    batch.clusters = {x, y};
    for (const auto& cluster : batch.clusters) {
        for (const auto& hash : cluster.memberDocumentHashes) {
            DocumentClusterMembership membership;
            membership.documentHash = hash;
            membership.clusterId = cluster.clusterId;
            batch.memberships.push_back(membership);
        }
    }
    std::ranges::sort(batch.memberships, {}, &DocumentClusterMembership::documentHash);
    batch.roads = {ClusterRoad{"cluster-x", "cluster-y", 0.5},
                   ClusterRoad{"cluster-y", "cluster-x", 0.5}};
    batch.roadMaxPerCluster = 2;
    return batch;
}

TopologyRoadSidecar sidecarFor(const TopologyArtifactBatch& batch) {
    TopologyRoadSidecar sidecar;
    sidecar.snapshotId = batch.snapshotId;
    sidecar.maxPerCluster = batch.roadMaxPerCluster;
    sidecar.roads = batch.roads;
    sidecar.roadsFullBuildEpoch = batch.roadsFullBuildEpoch;
    sidecar.roadsIncrementalPasses = batch.roadsIncrementalPasses;
    sidecar.memberOrders = collectTopologyMemberOrders(batch);
    return sidecar;
}

std::string encodedSidecar(const TopologyRoadSidecar& sidecar) {
    auto encoded = serializeTopologyRoadSidecarCompressed(sidecar);
    REQUIRE(encoded.has_value());
    return encoded.value();
}

uint32_t readU32At(const std::vector<std::byte>& bytes, std::size_t offset) {
    REQUIRE(bytes.size() >= offset + 4);
    uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(std::to_integer<uint8_t>(bytes[offset + i])) << (i * 8);
    }
    return value;
}

// Hand-built v2 sidecar: u32 magic, u32 version = 2, u32 maxPerCluster, u32 reserved, string
// snapshotId, u64 roadsFullBuildEpoch, u32 roadsIncrementalPasses, u32 table count + strings,
// u32 road count, per road (u32 from, u32 to, f64 weight). No order section.
std::vector<std::byte> handBuiltV2(const std::string& snapshotId) {
    std::vector<std::byte> out;
    auto putU32 = [&](uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    };
    auto putU64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<std::byte>((v >> (i * 8)) & 0xFF));
        }
    };
    auto putString = [&](const std::string& s) {
        putU32(static_cast<uint32_t>(s.size()));
        for (const char c : s) {
            out.push_back(static_cast<std::byte>(c));
        }
    };
    auto putDouble = [&](double d) {
        uint64_t raw = 0;
        std::memcpy(&raw, &d, sizeof raw);
        putU64(raw);
    };
    putU32(0x524D4159U); // 'Y','A','M','R'
    putU32(2U);
    putU32(2U);
    putU32(0U);
    putString(snapshotId);
    putU64(7U);
    putU32(1U);
    putU32(2U);
    putString("cluster-x");
    putString("cluster-y");
    putU32(2U);
    putU32(0U);
    putU32(1U);
    putDouble(0.5);
    putU32(1U);
    putU32(0U);
    putDouble(0.5);
    return out;
}

} // namespace

// ---------------------------------------------------------------------------------------------
// Permutation validator
// ---------------------------------------------------------------------------------------------

TEST_CASE("Member permutation validator accepts permutations and names the first violation",
          "[unit][topology][ordered-members][catch2]") {
    const std::vector<uint32_t> ok{2, 0, 1};
    CHECK_FALSE(validateMemberPermutation(ok, 3).has_value());
    CHECK_FALSE(validateMemberPermutation(std::vector<uint32_t>{}, 0).has_value());
    CHECK_FALSE(validateMemberPermutation(std::vector<uint32_t>{0}, 1).has_value());

    CHECK(validateMemberPermutation(ok, 4) ==
          std::optional<std::string_view>{"order_length_mismatch"});
    CHECK(validateMemberPermutation(std::vector<uint32_t>{0, 3, 1}, 3) ==
          std::optional<std::string_view>{"order_index_out_of_range"});
    CHECK(validateMemberPermutation(std::vector<uint32_t>{0, 1, 1}, 3) ==
          std::optional<std::string_view>{"order_index_repeated"});
    CHECK(validateMemberPermutation(std::vector<uint32_t>{}, 2) ==
          std::optional<std::string_view>{"order_length_mismatch"});
}

// ---------------------------------------------------------------------------------------------
// Order kinds on hand-built clusters
// ---------------------------------------------------------------------------------------------

TEST_CASE("Time order sorts by event time, ties by hash, members without a time last",
          "[unit][topology][ordered-members][time][catch2]") {
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("m_a", {}, 300));
    docs.push_back(mk("m_b", {}, std::nullopt));
    docs.push_back(mk("m_c", {}, 100));
    docs.push_back(mk("m_d", {}, 300));
    docs.push_back(mk("m_e", {}, std::nullopt));
    const auto byHash = indexTopologyDocumentsByHash(docs);
    auto cluster = clusterOf({"m_a", "m_b", "m_c", "m_d", "m_e"});

    const auto order = computeClusterMemberOrder(RoadOrderKind::Time, cluster, byHash);
    CHECK(order == std::vector<uint32_t>{2, 0, 3, 1, 4});
    cluster.orderedMembers = order;
    CHECK(walk(cluster) == std::vector<std::string>{"m_c", "m_a", "m_d", "m_b", "m_e"});
    CHECK_FALSE(validateMemberPermutation(order, 5).has_value());

    SECTION("a member missing from the inputs counts as having no time") {
        docs.erase(docs.begin() + 2); // m_c
        const auto partial = indexTopologyDocumentsByHash(docs);
        CHECK(computeClusterMemberOrder(RoadOrderKind::Time, cluster, partial) ==
              std::vector<uint32_t>{0, 3, 1, 2, 4});
    }
    SECTION("no times at all yields the hash order") {
        for (auto& doc : docs) {
            doc.eventTime.reset();
        }
        const auto none = indexTopologyDocumentsByHash(docs);
        CHECK(computeClusterMemberOrder(RoadOrderKind::Time, cluster, none) ==
              std::vector<uint32_t>{0, 1, 2, 3, 4});
    }
    SECTION("fewer than two members yields no order") {
        auto single = clusterOf({"m_a"});
        CHECK(computeClusterMemberOrder(RoadOrderKind::Time, single, byHash).empty());
        ClusterArtifact empty;
        CHECK(computeClusterMemberOrder(RoadOrderKind::Time, empty, byHash).empty());
    }
    SECTION("None yields no order") {
        CHECK(computeClusterMemberOrder(RoadOrderKind::None, cluster, byHash).empty());
    }
}

TEST_CASE("Session order groups by session, orders groups by earliest time then id, unsessioned "
          "members last",
          "[unit][topology][ordered-members][session][catch2]") {
    // Hash-sorted members: h0 (no session, t=5), h1 (s1, 200), h2 (s2, 10), h3 (s1, 50),
    // h4 (no session, no time), h5 (s0, no time), h6 (s2, 10).
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("h0", {}, 5, ""));
    docs.push_back(mk("h1", {}, 200, "s1"));
    docs.push_back(mk("h2", {}, 10, "s2"));
    docs.push_back(mk("h3", {}, 50, "s1"));
    docs.push_back(mk("h4", {}, std::nullopt, ""));
    docs.push_back(mk("h5", {}, std::nullopt, "s0"));
    docs.push_back(mk("h6", {}, 10, "s2"));
    const auto byHash = indexTopologyDocumentsByHash(docs);
    auto cluster = clusterOf({"h0", "h1", "h2", "h3", "h4", "h5", "h6"});

    const auto order = computeClusterMemberOrder(RoadOrderKind::Session, cluster, byHash);
    CHECK_FALSE(validateMemberPermutation(order, 7).has_value());
    cluster.orderedMembers = order;
    // s2 (earliest 10): h2, h6 (tie by hash); s1 (earliest 50): h3, h1; s0 (no time) after the
    // timed groups; then the unsessioned h0 (t=5) and h4 (no time).
    CHECK(walk(cluster) == std::vector<std::string>{"h2", "h6", "h3", "h1", "h5", "h0", "h4"});

    SECTION("groups with the same earliest time order by session id") {
        find(docs, "h3")->eventTime = 10;
        const auto tied = indexTopologyDocumentsByHash(docs);
        cluster.orderedMembers = computeClusterMemberOrder(RoadOrderKind::Session, cluster, tied);
        CHECK(walk(cluster) == std::vector<std::string>{"h3", "h1", "h2", "h6", "h5", "h0", "h4"});
    }
}

TEST_CASE("Similarity chain is a greedy nearest-neighbor tour from the representative",
          "[unit][topology][ordered-members][chain][catch2]") {
    // Five unit vectors on the circle (hash-sorted): d0 = 0deg, d1 = 90deg, d2 = 20deg,
    // d3 = 170deg, d4 = 60deg. From the medoid d2 the greedy tour by cosine is d2 -> d0 (20deg
    // away, vs d4 at 40deg) -> d4 (60deg) -> d1 (30deg away, vs d3 at 170deg) -> d3.
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("d0", angle2(0.0)));
    docs.push_back(mk("d1", angle2(90.0)));
    docs.push_back(mk("d2", angle2(20.0)));
    docs.push_back(mk("d3", angle2(170.0)));
    docs.push_back(mk("d4", angle2(60.0)));
    const auto byHash = indexTopologyDocumentsByHash(docs);
    auto cluster = clusterOf({"d0", "d1", "d2", "d3", "d4"}, "d2");

    const auto order = computeClusterMemberOrder(RoadOrderKind::SimilarityChain, cluster, byHash);
    CHECK(order == std::vector<uint32_t>{2, 0, 4, 1, 3});
    CHECK_FALSE(validateMemberPermutation(order, 5).has_value());

    SECTION("the same inputs in another order give the same tour") {
        std::ranges::reverse(docs);
        const auto reversed = indexTopologyDocumentsByHash(docs);
        CHECK(computeClusterMemberOrder(RoadOrderKind::SimilarityChain, cluster, reversed) ==
              order);
    }
    SECTION("without a representative the tour starts at the smallest usable hash") {
        cluster.medoid.reset();
        // From d0 (0deg): d2 (20) -> d4 (60) -> d1 (90) -> d3 (170).
        CHECK(computeClusterMemberOrder(RoadOrderKind::SimilarityChain, cluster, byHash) ==
              std::vector<uint32_t>{0, 2, 4, 1, 3});
    }
    SECTION("a representative without a usable embedding falls back the same way") {
        find(docs, "d2")->embedding.clear();
        const auto partial = indexTopologyDocumentsByHash(docs);
        // d2 has no embedding: tour from d0 over {d1, d3, d4}: d0 -> d4 -> d1 -> d3, then d2 last.
        CHECK(computeClusterMemberOrder(RoadOrderKind::SimilarityChain, cluster, partial) ==
              std::vector<uint32_t>{0, 4, 1, 3, 2});
    }
    SECTION("equal similarities resolve to the smaller hash") {
        // e0 = (1, 0); e1 and e2 sit at exactly +-30deg, so their cosines to e0 are identical.
        std::vector<TopologyDocumentInput> tie;
        const float c = std::cos(0.5235987755982988F);
        const float s = std::sin(0.5235987755982988F);
        tie.push_back(mk("e0", {1.0F, 0.0F}));
        tie.push_back(mk("e1", {c, -s}));
        tie.push_back(mk("e2", {c, s}));
        const auto tieByHash = indexTopologyDocumentsByHash(tie);
        auto tieCluster = clusterOf({"e0", "e1", "e2"}, "e0");
        CHECK(computeClusterMemberOrder(RoadOrderKind::SimilarityChain, tieCluster, tieByHash) ==
              std::vector<uint32_t>{0, 1, 2});
        // Mirror the two candidates' hashes: the tie still goes to the smaller hash, now the
        // vector that was second before.
        tie[1].documentHash = "e2";
        tie[2].documentHash = "e1";
        const auto swapped = indexTopologyDocumentsByHash(tie);
        CHECK(computeClusterMemberOrder(RoadOrderKind::SimilarityChain, tieCluster, swapped) ==
              std::vector<uint32_t>{0, 1, 2});
    }
    SECTION("no usable embeddings yields the hash order") {
        for (auto& doc : docs) {
            doc.embedding.clear();
        }
        const auto none = indexTopologyDocumentsByHash(docs);
        CHECK(computeClusterMemberOrder(RoadOrderKind::SimilarityChain, cluster, none) ==
              std::vector<uint32_t>{0, 1, 2, 3, 4});
    }
}

// ---------------------------------------------------------------------------------------------
// Engines
// ---------------------------------------------------------------------------------------------

TEST_CASE("Engines store a valid permutation of every kind on clusters with two or more members",
          "[unit][topology][ordered-members][engine][catch2]") {
    const auto docs = fourClusters();
    const auto kind =
        GENERATE(RoadOrderKind::Time, RoadOrderKind::Session, RoadOrderKind::SimilarityChain);

    SECTION("connected components") {
        ConnectedComponentTopologyEngine engine;
        auto batch = engine.buildArtifacts(docs, graphConfig(kind));
        REQUIRE(batch.has_value());
        REQUIRE(batch.value().clusters.size() == 4);
        checkOrdersWellFormed(batch.value(), kind);
        if (kind == RoadOrderKind::Time) {
            CHECK(walk(clusterById(batch.value(), kA)) ==
                  std::vector<std::string>{"a1", "a2", "a0"});
            CHECK(walk(clusterById(batch.value(), kC)) ==
                  std::vector<std::string>{"c2", "c0", "c1"});
        }
        if (kind == RoadOrderKind::Session) {
            // s-a (earliest 100): a1, a0; s-a2 (200): a2.
            CHECK(walk(clusterById(batch.value(), kA)) ==
                  std::vector<std::string>{"a1", "a0", "a2"});
        }
    }
    SECTION("k-means") {
        KMeansTopologyEngine engine;
        auto cfg = graphConfig(kind);
        cfg.kmeansK = 4;
        auto batch = engine.buildArtifacts(docs, cfg);
        REQUIRE(batch.has_value());
        REQUIRE_FALSE(batch.value().clusters.empty());
        checkOrdersWellFormed(batch.value(), kind);
        CHECK(std::ranges::any_of(batch.value().clusters, [](const ClusterArtifact& cluster) {
            return cluster.memberDocumentHashes.size() >= 2;
        }));
    }
    SECTION("louvain") {
        LouvainTopologyEngine engine;
        auto batch = engine.buildArtifacts(docs, graphConfig(kind));
        REQUIRE(batch.has_value());
        checkOrdersWellFormed(batch.value(), kind);
    }
}

TEST_CASE("Orders cover boundary-spill members too",
          "[unit][topology][ordered-members][engine][spill][catch2]") {
    auto docs = fourClusters();
    ConnectedComponentTopologyEngine engine;
    auto cfg = graphConfig(RoadOrderKind::Time);
    cfg.allowOverlap = true;
    cfg.overlapLimit = 1;
    cfg.overlapBoundaryDistanceRatio = 1000.0;
    auto batch = engine.buildArtifacts(docs, cfg);
    REQUIRE(batch.has_value());
    bool spilled = false;
    for (const auto& membership : batch.value().memberships) {
        spilled = spilled || !membership.overlapClusterIds.empty();
    }
    REQUIRE(spilled);
    checkOrdersWellFormed(batch.value(), RoadOrderKind::Time);
}

TEST_CASE(
    "The default order none leaves artifacts, the sidecar and every fingerprint byte-identical",
    "[unit][topology][ordered-members][default][codec][fingerprint][catch2]") {
    const auto docs = fourClusters();
    ConnectedComponentTopologyEngine engine;
    auto plain = engine.buildArtifacts(docs, graphConfig(RoadOrderKind::None));
    auto timed = engine.buildArtifacts(docs, graphConfig(RoadOrderKind::Time));
    REQUIRE(plain.has_value());
    REQUIRE(timed.has_value());
    REQUIRE_FALSE(plain.value().roads.empty());
    checkOrdersWellFormed(plain.value(), RoadOrderKind::None);
    checkOrdersWellFormed(timed.value(), RoadOrderKind::Time);
    alignStamps(timed.value(), plain.value());

    // Main payload: the codec never sees orders.
    auto plainBinary = serializeTopologyBatchBinary(plain.value());
    auto timedBinary = serializeTopologyBatchBinary(timed.value());
    REQUIRE(plainBinary.has_value());
    REQUIRE(timedBinary.has_value());
    CHECK(plainBinary.value() == timedBinary.value());
    auto decoded = deserializeTopologyBatchBinary(timedBinary.value());
    REQUIRE(decoded.has_value());
    checkOrdersWellFormed(decoded.value(), RoadOrderKind::None);

    // Fingerprints: construction, representation and roads ignore orders.
    CHECK(yams::search::topologyRoutingConstructionFingerprint(plain.value()) ==
          yams::search::topologyRoutingConstructionFingerprint(timed.value()));
    CHECK(yams::search::topologyRoutingRepresentationFingerprint(plain.value()) ==
          yams::search::topologyRoutingRepresentationFingerprint(timed.value()));
    CHECK(yams::search::topologyRoadFingerprint(plain.value()) ==
          yams::search::topologyRoadFingerprint(timed.value()));
    CHECK(plain.value().roads == timed.value().roads);

    // Sidecar: with no orders the bytes are the v2 layout, so a roads-only sidecar is unchanged.
    CHECK(collectTopologyMemberOrders(plain.value()).empty());
    auto plainSidecar = serializeTopologyRoadSidecarBinary(sidecarFor(plain.value()));
    REQUIRE(plainSidecar.has_value());
    CHECK(readU32At(plainSidecar.value(), 4) == 2U);
    auto timedSidecar = serializeTopologyRoadSidecarBinary(sidecarFor(timed.value()));
    REQUIRE(timedSidecar.has_value());
    CHECK(readU32At(timedSidecar.value(), 4) == 3U);
    CHECK(plainSidecar.value() != timedSidecar.value());
}

// ---------------------------------------------------------------------------------------------
// Sidecar codec
// ---------------------------------------------------------------------------------------------

TEST_CASE("Road sidecar v3 round trips member orders and v2 bytes decode with no order",
          "[unit][topology][ordered-members][sidecar][codec][catch2]") {
    const auto batch = makeStoredBatch("snap-v3", 3);
    auto sidecar = sidecarFor(batch);
    sidecar.roadsFullBuildEpoch = 7;
    sidecar.roadsIncrementalPasses = 1;
    REQUIRE(sidecar.memberOrders.size() == 2);
    CHECK(sidecar.memberOrders[0] ==
          ClusterMemberOrder{"cluster-x", RoadOrderKind::Time, {2, 0, 1}});
    CHECK(sidecar.memberOrders[1] ==
          ClusterMemberOrder{"cluster-y", RoadOrderKind::Session, {1, 0}});

    SECTION("binary and envelope round trips") {
        auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
        REQUIRE(bytes.has_value());
        CHECK(readU32At(bytes.value(), 4) == 3U);
        auto decoded = deserializeTopologyRoadSidecarBinary(bytes.value());
        REQUIRE(decoded.has_value());
        CHECK(decoded.value().roads == sidecar.roads);
        CHECK(decoded.value().maxPerCluster == 2);
        CHECK(decoded.value().roadsFullBuildEpoch == 7);
        CHECK(decoded.value().roadsIncrementalPasses == 1);
        CHECK(decoded.value().memberOrders == sidecar.memberOrders);

        auto payload = encodedSidecar(sidecar);
        auto fromEnvelope = deserializeTopologyRoadSidecarCompressed(payload);
        REQUIRE(fromEnvelope.has_value());
        CHECK(fromEnvelope.value().memberOrders == sidecar.memberOrders);
        auto envelope = nlohmann::json::parse(payload);
        CHECK(envelope.at("format").get<std::string>() == "topology_roads_zstd_v1");
        CHECK(envelope.at("member_order_count").get<std::size_t>() == 2);
    }
    SECTION("orders without roads still serialize") {
        sidecar.roads.clear();
        sidecar.maxPerCluster = 0;
        auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
        REQUIRE(bytes.has_value());
        auto decoded = deserializeTopologyRoadSidecarBinary(bytes.value());
        REQUIRE(decoded.has_value());
        CHECK(decoded.value().roads.empty());
        CHECK(decoded.value().memberOrders == sidecar.memberOrders);
    }
    SECTION("a hand-built v2 sidecar decodes with no orders") {
        const auto v2 = handBuiltV2("snap-v3");
        auto decoded = deserializeTopologyRoadSidecarBinary(v2);
        REQUIRE(decoded.has_value());
        CHECK(decoded.value().snapshotId == "snap-v3");
        CHECK(decoded.value().roads == sidecar.roads);
        CHECK(decoded.value().roadsFullBuildEpoch == 7);
        CHECK(decoded.value().roadsIncrementalPasses == 1);
        CHECK(decoded.value().memberOrders.empty());

        // And a sidecar without orders serializes to exactly those v2 bytes.
        auto noOrders = sidecar;
        noOrders.memberOrders.clear();
        auto bytes = serializeTopologyRoadSidecarBinary(noOrders);
        REQUIRE(bytes.has_value());
        CHECK(bytes.value() == v2);
    }
    SECTION("an unknown order kind on the wire is rejected by the decoder") {
        auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
        REQUIRE(bytes.has_value());
        // The order section follows the roads; the first entry's kind sits 4 bytes after the
        // entry's cluster index. Locate it by re-encoding with a sentinel-free search: the
        // section starts after the last road weight.
        auto corrupted = bytes.value();
        // Section layout: u32 count, then (u32 clusterIdx, u32 kind, u32 len, len x u32).
        const std::size_t orderSection = corrupted.size() - (4 + (12 + 3 * 4) + (12 + 2 * 4));
        REQUIRE(readU32At(corrupted, orderSection) == 2U);
        const std::size_t firstKind = orderSection + 4 + 4;
        REQUIRE(readU32At(corrupted, firstKind) == static_cast<uint32_t>(RoadOrderKind::Time));
        corrupted[firstKind] = std::byte{42};
        CHECK_FALSE(deserializeTopologyRoadSidecarBinary(corrupted).has_value());
    }
    SECTION("a truncated order section is rejected") {
        auto bytes = serializeTopologyRoadSidecarBinary(sidecar);
        REQUIRE(bytes.has_value());
        auto truncated = bytes.value();
        truncated.resize(truncated.size() - 3);
        CHECK_FALSE(deserializeTopologyRoadSidecarBinary(truncated).has_value());
    }
}

TEST_CASE("Attaching member orders validates each cluster on its own and keeps the roads",
          "[unit][topology][ordered-members][sidecar][validate][catch2]") {
    auto batch = makeStoredBatch("snap-att", 1);
    auto sidecar = sidecarFor(batch);
    // Start from a batch without orders, as the main codec yields.
    for (auto& cluster : batch.clusters) {
        cluster.orderKind = RoadOrderKind::None;
        cluster.orderedMembers.clear();
    }

    SECTION("valid orders attach") {
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 2);
        CHECK(batch.orderUnavailableReason.empty());
        CHECK(clusterById(batch, "cluster-x").orderedMembers == std::vector<uint32_t>{2, 0, 1});
        CHECK(clusterById(batch, "cluster-x").orderKind == RoadOrderKind::Time);
        CHECK(clusterById(batch, "cluster-y").orderedMembers == std::vector<uint32_t>{1, 0});
        CHECK(clusterById(batch, "cluster-y").orderKind == RoadOrderKind::Session);
    }
    SECTION("a length mismatch drops only that cluster's order") {
        sidecar.memberOrders[0].permutation = {0, 1};
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 1);
        CHECK(batch.orderUnavailableReason == "order_length_mismatch:cluster-x");
        CHECK(clusterById(batch, "cluster-x").orderKind == RoadOrderKind::None);
        CHECK(clusterById(batch, "cluster-x").orderedMembers.empty());
        CHECK(clusterById(batch, "cluster-y").orderedMembers == std::vector<uint32_t>{1, 0});
    }
    SECTION("a repeated index") {
        sidecar.memberOrders[1].permutation = {1, 1};
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 1);
        CHECK(batch.orderUnavailableReason == "order_index_repeated:cluster-y");
    }
    SECTION("an out-of-range index") {
        sidecar.memberOrders[1].permutation = {0, 2};
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 1);
        CHECK(batch.orderUnavailableReason == "order_index_out_of_range:cluster-y");
    }
    SECTION("an unknown cluster") {
        sidecar.memberOrders[0].clusterId = "cluster-ghost";
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 1);
        CHECK(batch.orderUnavailableReason == "order_cluster_not_found:cluster-ghost");
    }
    SECTION("kind None on the wire") {
        sidecar.memberOrders[0].kind = RoadOrderKind::None;
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 1);
        CHECK(batch.orderUnavailableReason == "order_kind_invalid:cluster-x");
    }
    SECTION("a duplicate cluster entry keeps the first") {
        sidecar.memberOrders.push_back(
            ClusterMemberOrder{"cluster-x", RoadOrderKind::SimilarityChain, {0, 1, 2}});
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 2);
        CHECK(batch.orderUnavailableReason == "order_duplicate_cluster:cluster-x");
        CHECK(clusterById(batch, "cluster-x").orderKind == RoadOrderKind::Time);
    }
    SECTION("several violations are all reported") {
        sidecar.memberOrders[0].permutation = {0, 1};
        sidecar.memberOrders[1].permutation = {1, 1};
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 0);
        CHECK(batch.orderUnavailableReason ==
              "order_length_mismatch:cluster-x;order_index_repeated:cluster-y");
    }
    SECTION("a snapshot mismatch drops every order") {
        sidecar.snapshotId = "snap-other";
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 0);
        CHECK(batch.orderUnavailableReason == "order_snapshot_mismatch");
    }
    SECTION("attaching resets a stale order the batch already carried") {
        batch.clusters[0].orderKind = RoadOrderKind::SimilarityChain;
        batch.clusters[0].orderedMembers = {1, 2, 0};
        sidecar.memberOrders.erase(sidecar.memberOrders.begin());
        CHECK(attachTopologyMemberOrders(batch, sidecar) == 1);
        CHECK(clusterById(batch, "cluster-x").orderKind == RoadOrderKind::None);
        CHECK(clusterById(batch, "cluster-x").orderedMembers.empty());
    }
    // Roads are never part of the order validation.
    CHECK(batch.roads.size() == 2);
}

// ---------------------------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------------------------

TEST_CASE("MetadataKgTopologyArtifactStore persists member orders in the road sidecar and "
          "reloads them",
          "[unit][topology][ordered-members][store][catch2]") {
    StoreFixture fixture;
    const auto stored = makeStoredBatch("snap-1", 1);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(stored).has_value());
        auto resident = store.loadLatestShared();
        REQUIRE(resident.has_value());
        REQUIRE(resident.value());
        CHECK(clusterById(*resident.value(), "cluster-x").orderedMembers ==
              std::vector<uint32_t>{2, 0, 1});
        CHECK(resident.value()->orderUnavailableReason.empty());
    }

    auto node = fixture.kgStore->getNodeByKey(roadNodeKey("snap-1"));
    REQUIRE(node.has_value());
    REQUIRE(node.value().has_value());
    REQUIRE(node.value()->properties.has_value());
    auto sidecar = deserializeTopologyRoadSidecarCompressed(*node.value()->properties);
    REQUIRE(sidecar.has_value());
    CHECK(sidecar.value().roads == stored.roads);
    CHECK(sidecar.value().memberOrders == collectTopologyMemberOrders(stored));

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->roads == stored.roads);
    CHECK(loaded.value()->roadUnavailableReason.empty());
    CHECK(loaded.value()->orderUnavailableReason.empty());
    CHECK(clusterById(*loaded.value(), "cluster-x").orderKind == RoadOrderKind::Time);
    CHECK(clusterById(*loaded.value(), "cluster-x").orderedMembers ==
          std::vector<uint32_t>{2, 0, 1});
    CHECK(clusterById(*loaded.value(), "cluster-y").orderKind == RoadOrderKind::Session);
    CHECK(clusterById(*loaded.value(), "cluster-y").orderedMembers == std::vector<uint32_t>{1, 0});

    SECTION("orders without roads still get a sidecar; none and no roads get none") {
        auto ordersOnly = makeStoredBatch("snap-2", 2);
        ordersOnly.roads.clear();
        ordersOnly.roadMaxPerCluster = 0;
        REQUIRE(reopened.storeBatch(ordersOnly).has_value());
        REQUIRE(fixture.nodeExists(roadNodeKey("snap-2")));
        auto again = reopened.loadLatestShared();
        REQUIRE(again.has_value());
        REQUIRE(again.value());
        CHECK(again.value()->roads.empty());
        CHECK(clusterById(*again.value(), "cluster-y").orderedMembers ==
              std::vector<uint32_t>{1, 0});

        auto bare = makeStoredBatch("snap-3", 3);
        bare.roads.clear();
        bare.roadMaxPerCluster = 0;
        for (auto& cluster : bare.clusters) {
            cluster.orderKind = RoadOrderKind::None;
            cluster.orderedMembers.clear();
        }
        REQUIRE(reopened.storeBatch(bare).has_value());
        CHECK_FALSE(fixture.nodeExists(roadNodeKey("snap-3")));
    }
}

TEST_CASE("MetadataKgTopologyArtifactStore drops an invalid order with a reason and keeps the "
          "roads and the other orders",
          "[unit][topology][ordered-members][store][catch2]") {
    StoreFixture fixture;
    const auto stored = makeStoredBatch("snap-1", 1);
    {
        MetadataKgTopologyArtifactStore store(fixture.repository, fixture.kgStore);
        REQUIRE(store.storeBatch(stored).has_value());
    }
    auto sidecar = sidecarFor(stored);
    sidecar.memberOrders[0].permutation = {0, 0, 1};
    fixture.overwriteNodeProperties(roadNodeKey("snap-1"), encodedSidecar(sidecar));

    MetadataKgTopologyArtifactStore reopened(fixture.repository, fixture.kgStore);
    auto loaded = reopened.loadLatestShared();
    REQUIRE(loaded.has_value());
    REQUIRE(loaded.value());
    CHECK(loaded.value()->roads == stored.roads);
    CHECK(loaded.value()->roadUnavailableReason.empty());
    CHECK(loaded.value()->orderUnavailableReason == "order_index_repeated:cluster-x");
    CHECK(clusterById(*loaded.value(), "cluster-x").orderKind == RoadOrderKind::None);
    CHECK(clusterById(*loaded.value(), "cluster-x").orderedMembers.empty());
    CHECK(clusterById(*loaded.value(), "cluster-y").orderedMembers == std::vector<uint32_t>{1, 0});

    SECTION("a bad road layer does not take the orders with it") {
        auto badRoads = sidecarFor(stored);
        badRoads.roads[0].toClusterId = badRoads.roads[0].fromClusterId;
        fixture.overwriteNodeProperties(roadNodeKey("snap-1"), encodedSidecar(badRoads));
        MetadataKgTopologyArtifactStore third(fixture.repository, fixture.kgStore);
        auto mixed = third.loadLatestShared();
        REQUIRE(mixed.has_value());
        REQUIRE(mixed.value());
        CHECK(mixed.value()->roads.empty());
        CHECK(mixed.value()->roadUnavailableReason == "road_self_loop");
        CHECK(mixed.value()->orderUnavailableReason.empty());
        CHECK(clusterById(*mixed.value(), "cluster-x").orderedMembers ==
              std::vector<uint32_t>{2, 0, 1});
    }
}

// ---------------------------------------------------------------------------------------------
// Incremental update
// ---------------------------------------------------------------------------------------------

TEST_CASE("Incremental update keeps untouched clusters' orders and recomputes the rebuilt ones",
          "[unit][topology][ordered-members][incremental][catch2]") {
    ConnectedComponentTopologyEngine engine;
    auto docs = fourClusters();
    auto existing = engine.buildArtifacts(docs, graphConfig(RoadOrderKind::Time));
    REQUIRE(existing.has_value());
    REQUIRE(existing.value().clusters.size() == 4);
    checkOrdersWellFormed(existing.value(), RoadOrderKind::Time);
    REQUIRE(walk(clusterById(existing.value(), kA)) == std::vector<std::string>{"a1", "a2", "a0"});
    REQUIRE(walk(clusterById(existing.value(), kB)) == std::vector<std::string>{"b1", "b0"});
    const auto orderC = clusterById(existing.value(), kC).orderedMembers;
    const auto orderD = clusterById(existing.value(), kD).orderedMembers;

    // Region A u B: a0 becomes the earliest and b0 precedes b1. C and D are untouched.
    find(docs, "a0")->eventTime = 1;
    find(docs, "b0")->eventTime = 1;
    TopologyUpdateStats stats;
    auto merged = engine.updateArtifacts(existing.value(), select(docs, kRegionAB),
                                         graphConfig(RoadOrderKind::Time), &stats);
    REQUIRE(merged.has_value());
    REQUIRE(merged.value().clusters.size() == 4);
    checkOrdersWellFormed(merged.value(), RoadOrderKind::Time);
    CHECK(walk(clusterById(merged.value(), kA)) == std::vector<std::string>{"a0", "a1", "a2"});
    CHECK(walk(clusterById(merged.value(), kB)) == std::vector<std::string>{"b0", "b1"});
    CHECK(clusterById(merged.value(), kC).orderedMembers == orderC);
    CHECK(clusterById(merged.value(), kD).orderedMembers == orderD);
    CHECK(merged.value().orderUnavailableReason.empty());

    SECTION("equal to a from-scratch build on the same input") {
        auto full = engine.buildArtifacts(docs, graphConfig(RoadOrderKind::Time));
        REQUIRE(full.has_value());
        for (const auto& cluster : full.value().clusters) {
            CHECK(walk(clusterById(merged.value(), cluster.clusterId)) == walk(cluster));
        }
    }
    SECTION("an update with order none clears every order") {
        auto cleared = engine.updateArtifacts(merged.value(), select(docs, kRegionAB),
                                              graphConfig(RoadOrderKind::None));
        REQUIRE(cleared.has_value());
        checkOrdersWellFormed(cleared.value(), RoadOrderKind::None);
    }
    SECTION("a kind change reaches the rebuilt clusters only, like the representative rule") {
        auto chained = engine.updateArtifacts(merged.value(), select(docs, kRegionAB),
                                              graphConfig(RoadOrderKind::SimilarityChain));
        REQUIRE(chained.has_value());
        CHECK(clusterById(chained.value(), kA).orderKind == RoadOrderKind::SimilarityChain);
        CHECK(clusterById(chained.value(), kB).orderKind == RoadOrderKind::SimilarityChain);
        CHECK(clusterById(chained.value(), kC).orderKind == RoadOrderKind::Time);
        CHECK(clusterById(chained.value(), kC).orderedMembers == orderC);
    }
    SECTION("an empty region returns the existing batch untouched") {
        auto same = engine.updateArtifacts(merged.value(), {}, graphConfig(RoadOrderKind::Time));
        REQUIRE(same.has_value());
        CHECK(clusterById(same.value(), kA).orderedMembers ==
              clusterById(merged.value(), kA).orderedMembers);
    }
}
