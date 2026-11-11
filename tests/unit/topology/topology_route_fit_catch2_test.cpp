// SPDX-License-Identifier: GPL-3.0-or-later
// fitRouteVectors and the router's use of fitted vectors.

#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_route_fit.h>

using namespace yams::topology;

namespace {

constexpr std::size_t kDim = 16;

void normalize(float* v) {
    float norm = 0.0F;
    for (std::size_t i = 0; i < kDim; ++i) {
        norm += v[i] * v[i];
    }
    norm = std::sqrt(norm);
    for (std::size_t i = 0; i < kDim; ++i) {
        v[i] /= norm;
    }
}

// A partition where centroids mislead: each cluster holds a tight core around axis c plus one
// outlier document near axis c + clusters. Queries aim at the outliers, which the centroid of
// their own cluster barely points at.
struct Scenario {
    std::size_t clusters{4};
    std::vector<float> keys;
    std::vector<std::uint32_t> clusterOfKey;
    std::vector<float> centroids;
    std::vector<float> queries;
};

Scenario makeScenario(std::size_t queryCount, std::uint64_t seed) {
    Scenario s;
    std::mt19937_64 rng(seed);
    std::normal_distribution<float> noise(0.0F, 0.05F);
    const auto pushVector = [&](std::vector<float>& out, std::size_t axis, float weight,
                                std::size_t otherAxis, float otherWeight) {
        std::vector<float> v(kDim);
        for (auto& x : v) {
            x = noise(rng);
        }
        v[axis] += weight;
        v[otherAxis] += otherWeight;
        normalize(v.data());
        out.insert(out.end(), v.begin(), v.end());
    };
    for (std::uint32_t c = 0; c < s.clusters; ++c) {
        for (int member = 0; member < 12; ++member) {
            pushVector(s.keys, c, 1.0F, c, 0.0F);
            s.clusterOfKey.push_back(c);
        }
        // The outlier lives on its own axis, far from the core.
        pushVector(s.keys, s.clusters + c, 1.0F, c, 0.15F);
        s.clusterOfKey.push_back(c);
    }
    s.centroids.assign(s.clusters * kDim, 0.0F);
    for (std::size_t k = 0; k < s.clusterOfKey.size(); ++k) {
        for (std::size_t i = 0; i < kDim; ++i) {
            s.centroids[s.clusterOfKey[k] * kDim + i] += s.keys[k * kDim + i];
        }
    }
    for (std::size_t c = 0; c < s.clusters; ++c) {
        normalize(s.centroids.data() + c * kDim);
    }
    std::uniform_int_distribution<std::size_t> pick(0, s.clusters - 1);
    for (std::size_t q = 0; q < queryCount; ++q) {
        // Mostly the outlier axis of one cluster, with a pull toward a different cluster's core
        // so centroid routing prefers the wrong cluster.
        const std::size_t target = pick(rng);
        const std::size_t decoy = (target + 1) % s.clusters;
        pushVector(s.queries, s.clusters + target, 1.0F, decoy, 0.6F);
    }
    return s;
}

RouteFitInput inputOf(const Scenario& s) {
    return RouteFitInput{.dim = kDim,
                         .keys = s.keys,
                         .clusterOfKey = s.clusterOfKey,
                         .centroids = s.centroids,
                         .queries = s.queries};
}

} // namespace

TEST_CASE("fitRouteVectors beats misleading centroids on held-out queries",
          "[topology][route-fit][catch2]") {
    const auto scenario = makeScenario(400, 11);
    RouteFitOptions options;
    auto fitted = fitRouteVectors(inputOf(scenario), options);
    REQUIRE(fitted.has_value());
    const auto& report = fitted.value();

    CHECK(report.fitQueries == 320u);
    CHECK(report.holdoutQueries == 80u);
    CHECK(report.recallDepth == 4u);
    REQUIRE(report.routeVectors.size() == scenario.centroids.size());
    // With four clusters and depth four every router has recall 1; the test of routing quality
    // is the top-1 check below. Here the report only has to be self-consistent.
    CHECK(report.fittedHoldoutRecall >= report.centroidHoldoutRecall);
}

TEST_CASE("fitRouteVectors moves the right cluster to the top for fresh queries",
          "[topology][route-fit][catch2]") {
    // Eight clusters so the recall depth of five can discriminate.
    Scenario scenario;
    scenario.clusters = 8;
    {
        std::mt19937_64 rng(5);
        std::normal_distribution<float> noise(0.0F, 0.02F);
        const auto unit = [&](std::size_t axis, std::size_t otherAxis, float otherWeight) {
            std::vector<float> v(kDim);
            for (auto& x : v) {
                x = noise(rng);
            }
            v[axis] += 1.0F;
            v[otherAxis] += otherWeight;
            normalize(v.data());
            return v;
        };
        for (std::uint32_t c = 0; c < 8; ++c) {
            for (int member = 0; member < 10; ++member) {
                const auto v = unit(c, c, 0.0F);
                scenario.keys.insert(scenario.keys.end(), v.begin(), v.end());
                scenario.clusterOfKey.push_back(c);
            }
            const auto outlier = unit(8 + c, c, 0.1F);
            scenario.keys.insert(scenario.keys.end(), outlier.begin(), outlier.end());
            scenario.clusterOfKey.push_back(c);
        }
        scenario.centroids.assign(8 * kDim, 0.0F);
        for (std::size_t k = 0; k < scenario.clusterOfKey.size(); ++k) {
            for (std::size_t i = 0; i < kDim; ++i) {
                scenario.centroids[scenario.clusterOfKey[k] * kDim + i] +=
                    scenario.keys[k * kDim + i];
            }
        }
        for (std::size_t c = 0; c < 8; ++c) {
            normalize(scenario.centroids.data() + c * kDim);
        }
        for (std::size_t q = 0; q < 480; ++q) {
            const std::size_t target = q % 8;
            // Pulled toward five other clusters' cores, so centroid routing ranks the true
            // cluster below all of them.
            std::vector<float> v(kDim);
            for (auto& x : v) {
                x = noise(rng);
            }
            v[8 + target] += 1.0F;
            for (std::size_t offset = 1; offset <= 5; ++offset) {
                v[(target + offset) % 8] += 0.5F;
            }
            normalize(v.data());
            scenario.queries.insert(scenario.queries.end(), v.begin(), v.end());
        }
    }

    auto fitted = fitRouteVectors(inputOf(scenario));
    REQUIRE(fitted.has_value());
    const auto& report = fitted.value();
    CHECK(report.recallDepth == 5u);
    CHECK(report.centroidHoldoutRecall < 0.2);
    CHECK(report.fittedHoldoutRecall > 0.9);
    CHECK(report.adopted);
    CHECK(report.chosenStrength > 0.0);
}

TEST_CASE("fitRouteVectors keeps the centroids when it cannot do better",
          "[topology][route-fit][catch2]") {
    SECTION("too few queries") {
        const auto scenario = makeScenario(20, 2);
        auto fitted = fitRouteVectors(inputOf(scenario));
        REQUIRE(fitted.has_value());
        CHECK_FALSE(fitted.value().adopted);
        CHECK(fitted.value().routeVectors == scenario.centroids);
    }
    SECTION("no strength to try") {
        const auto scenario = makeScenario(400, 2);
        RouteFitOptions options;
        options.ridgeStrengths.clear();
        auto fitted = fitRouteVectors(inputOf(scenario), options);
        REQUIRE(fitted.has_value());
        CHECK_FALSE(fitted.value().adopted);
        CHECK(fitted.value().routeVectors == scenario.centroids);
    }
}

TEST_CASE("fitRouteVectors rejects inconsistent input and honours cancellation",
          "[topology][route-fit][catch2]") {
    const auto scenario = makeScenario(400, 4);

    SECTION("document assigned outside the partition") {
        auto broken = scenario;
        broken.clusterOfKey.front() = 99;
        auto fitted = fitRouteVectors(inputOf(broken));
        REQUIRE_FALSE(fitted.has_value());
        CHECK(fitted.error().code == yams::ErrorCode::InvalidArgument);
    }
    SECTION("matrix not a multiple of dim") {
        auto broken = scenario;
        broken.queries.pop_back();
        auto fitted = fitRouteVectors(inputOf(broken));
        REQUIRE_FALSE(fitted.has_value());
        CHECK(fitted.error().code == yams::ErrorCode::InvalidArgument);
    }
    SECTION("cancelled") {
        RouteFitOptions options;
        options.cancelled = [] { return true; };
        auto fitted = fitRouteVectors(inputOf(scenario), options);
        REQUIRE_FALSE(fitted.has_value());
        CHECK(fitted.error().code == yams::ErrorCode::OperationCancelled);
    }
}

TEST_CASE("SparseGuidedClusterRouter scores by fitted vectors only when every cluster has one",
          "[topology][route-fit][catch2]") {
    TopologyArtifactBatch artifacts;
    const auto addCluster = [&](std::string id, std::vector<float> centroid) {
        ClusterArtifact cluster;
        cluster.clusterId = std::move(id);
        cluster.memberCount = 1;
        cluster.memberDocumentHashes = {"doc-" + cluster.clusterId};
        cluster.centroidEmbedding = std::move(centroid);
        artifacts.clusters.push_back(std::move(cluster));
    };
    addCluster("a", {1.0F, 0.0F});
    addCluster("b", {0.0F, 1.0F});

    TopologyRouteRequest request;
    request.queryEmbedding = {0.9F, 0.1F};
    request.sparseDenseAlpha = 0.0F;
    request.limit = 2;
    const SparseGuidedClusterRouter router;

    const auto top = [&] {
        auto routes = router.route(request, artifacts);
        REQUIRE(routes.has_value());
        REQUIRE_FALSE(routes.value().empty());
        return routes.value().front().clusterId;
    };

    // Centroid routing: the query points along cluster a.
    CHECK(top() == "a");

    // Only one cluster fitted: scales would not be comparable, so centroids still decide.
    artifacts.clusters[1].fittedRouteVector = {5.0F, 0.0F};
    CHECK(top() == "a");

    // Both fitted: the dot product decides, and length matters.
    artifacts.clusters[0].fittedRouteVector = {0.1F, 0.0F};
    CHECK(top() == "b");

    // A fitted vector of the wrong dimension disables fitted scoring again.
    artifacts.clusters[0].fittedRouteVector = {0.1F, 0.0F, 0.0F};
    CHECK(top() == "a");
}
