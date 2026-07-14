#include <catch2/catch_test_macros.hpp>

#include <yams/topology/topology_alternate_engines.h>
#include <yams/topology/topology_artifacts.h>

#include <cmath>
#include <string>
#include <vector>

using yams::topology::KMeansTopologyEngine;
using yams::topology::TopologyBuildConfig;
using yams::topology::TopologyDocumentInput;

namespace {

std::vector<float> unit2(float x, float y) {
    const float n = std::sqrt(x * x + y * y);
    return {x / n, y / n};
}

TopologyDocumentInput mk(std::string hash, std::vector<float> emb) {
    TopologyDocumentInput doc;
    doc.documentHash = std::move(hash);
    doc.filePath = "/tmp/" + doc.documentHash + ".txt";
    doc.embedding = std::move(emb);
    return doc;
}

// Two well-separated blobs on the unit circle: cluster A near +x, cluster B near +y.
std::vector<TopologyDocumentInput> twoBlobs() {
    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("a0", unit2(1.0f, 0.02f)));
    docs.push_back(mk("a1", unit2(0.98f, 0.05f)));
    docs.push_back(mk("a2", unit2(0.99f, -0.03f)));
    docs.push_back(mk("b0", unit2(0.02f, 1.0f)));
    docs.push_back(mk("b1", unit2(0.05f, 0.98f)));
    docs.push_back(mk("b2", unit2(-0.03f, 0.99f)));
    return docs;
}

} // namespace

TEST_CASE("KMeansTopologyEngine separates two embedding blobs into two clusters",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    auto result = engine.buildArtifacts(twoBlobs(), cfg);
    REQUIRE(result);
    const auto& batch = result.value();
    CHECK(batch.algorithm == "kmeans_v1");
    REQUIRE(batch.clusters.size() == 2);

    // Each cluster is singleton-free and carries a centroid.
    for (const auto& cluster : batch.clusters) {
        CHECK(cluster.memberDocumentHashes.size() == 3);
        CHECK_FALSE(cluster.centroidEmbedding.empty());
    }

    // The a* docs land together and the b* docs land together.
    const auto clusterOf = [&](const std::string& h) -> std::string {
        for (const auto& c : batch.clusters) {
            for (const auto& m : c.memberDocumentHashes) {
                if (m == h) {
                    return c.clusterId;
                }
            }
        }
        return {};
    };
    CHECK(clusterOf("a0") == clusterOf("a1"));
    CHECK(clusterOf("a1") == clusterOf("a2"));
    CHECK(clusterOf("b0") == clusterOf("b1"));
    CHECK(clusterOf("b1") == clusterOf("b2"));
    CHECK(clusterOf("a0") != clusterOf("b0"));
}

TEST_CASE("KMeansTopologyEngine is deterministic across rebuilds",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    auto r1 = engine.buildArtifacts(twoBlobs(), cfg);
    auto r2 = engine.buildArtifacts(twoBlobs(), cfg);
    REQUIRE(r1);
    REQUIRE(r2);
    REQUIRE(r1.value().clusters.size() == r2.value().clusters.size());
    for (std::size_t i = 0; i < r1.value().clusters.size(); ++i) {
        CHECK(r1.value().clusters[i].clusterId == r2.value().clusters[i].clusterId);
        CHECK(r1.value().clusters[i].memberDocumentHashes ==
              r2.value().clusters[i].memberDocumentHashes);
    }
}

TEST_CASE("KMeansTopologyEngine auto-derives k and never exceeds the doc count",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 0; // auto = round(sqrt(n))

    auto result = engine.buildArtifacts(twoBlobs(), cfg);
    REQUIRE(result);
    // sqrt(6) ~ 2.45 -> 2 clusters, no empties/singletons collapse.
    CHECK(result.value().clusters.size() >= 2);
    std::size_t members = 0;
    for (const auto& c : result.value().clusters) {
        members += c.memberDocumentHashes.size();
    }
    CHECK(members == 6);
}

TEST_CASE("KMeansTopologyEngine handles docs without embeddings as singletons",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    auto docs = twoBlobs();
    docs.push_back(mk("noemb", {})); // no embedding

    auto result = engine.buildArtifacts(docs, cfg);
    REQUIRE(result);
    std::size_t members = 0;
    for (const auto& c : result.value().clusters) {
        members += c.memberDocumentHashes.size();
    }
    CHECK(members == docs.size());
}

TEST_CASE("KMeansTopologyEngine survives identical embeddings with k > distinct points",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 3;

    std::vector<TopologyDocumentInput> docs;
    for (int i = 0; i < 4; ++i) {
        docs.push_back(mk("same" + std::to_string(i), unit2(1.0f, 0.0f)));
    }

    auto result = engine.buildArtifacts(docs, cfg);
    REQUIRE(result);
    std::size_t members = 0;
    for (const auto& c : result.value().clusters) {
        CHECK_FALSE(c.memberDocumentHashes.empty()); // no empty clusters emitted
        members += c.memberDocumentHashes.size();
    }
    CHECK(members == docs.size());
}

TEST_CASE("KMeansTopologyEngine keeps every doc when embedding dimensions differ",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    auto docs = twoBlobs();                                // dim 2
    docs.push_back(mk("odd", {0.1f, 0.2f, 0.3f, 0.4f}));   // dim 4 -> excluded from clustering

    auto result = engine.buildArtifacts(docs, cfg);
    REQUIRE(result);
    std::size_t members = 0;
    for (const auto& c : result.value().clusters) {
        members += c.memberDocumentHashes.size();
    }
    CHECK(members == docs.size());
}

TEST_CASE("KMeansTopologyEngine handles a single usable document",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    std::vector<TopologyDocumentInput> docs;
    docs.push_back(mk("only", unit2(1.0f, 0.0f)));

    auto result = engine.buildArtifacts(docs, cfg);
    REQUIRE(result);
    REQUIRE(result.value().clusters.size() == 1);
    CHECK(result.value().clusters.front().memberDocumentHashes.size() == 1);
}

TEST_CASE("KMeansTopologyEngine clamps k above the usable document count",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 50; // >> n

    auto result = engine.buildArtifacts(twoBlobs(), cfg);
    REQUIRE(result);
    CHECK(result.value().clusters.size() <= 6);
    std::size_t members = 0;
    for (const auto& c : result.value().clusters) {
        members += c.memberDocumentHashes.size();
    }
    CHECK(members == 6);
}

TEST_CASE("KMeansTopologyEngine update path falls back to a full rebuild",
          "[topology][kmeans][catch2]") {
    KMeansTopologyEngine engine;
    TopologyBuildConfig cfg;
    cfg.kmeansK = 2;

    auto base = engine.buildArtifacts(twoBlobs(), cfg);
    REQUIRE(base);
    yams::topology::TopologyUpdateStats stats;
    auto updated = engine.updateArtifacts(base.value(), twoBlobs(), cfg, &stats);
    REQUIRE(updated);
    CHECK(stats.fallbackFullRebuilds == 1);
    CHECK(updated.value().clusters.size() == 2);
}
