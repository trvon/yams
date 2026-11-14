// SPDX-License-Identifier: GPL-3.0-or-later
// runRouteFitStage: from a rebuild's documents and sample queries to fitted vectors on a batch.

#include <catch2/catch_test_macros.hpp>

#include <cmath>
#include <cstdint>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_route_fit_stage.h>

using namespace yams::topology;

namespace {

constexpr std::size_t kDim = 16;
constexpr std::size_t kClusters = 8;

std::vector<float> unit(std::vector<float> v) {
    float norm = 0.0F;
    for (const float x : v) {
        norm += x * x;
    }
    norm = std::sqrt(norm);
    for (auto& x : v) {
        x /= norm;
    }
    return v;
}

// Eight clusters, each a tight core on axis c plus one outlier on axis 8 + c. Queries aim at an
// outlier while leaning toward five other cores, so centroid routing ranks the true cluster
// below them. The same shape as the fitRouteVectors unit test.
struct World {
    TopologyArtifactBatch batch;
    std::vector<TopologyDocumentInput> documents;
    std::unordered_map<std::string, std::vector<float>> queryEmbeddings;
    std::vector<std::string> logged;
    std::vector<std::string> generated;
};

World makeWorld(std::size_t loggedCount, std::size_t generatedCount) {
    World world;
    std::mt19937_64 rng(17);
    std::normal_distribution<float> noise(0.0F, 0.02F);
    const auto noisy = [&] {
        std::vector<float> v(kDim);
        for (auto& x : v) {
            x = noise(rng);
        }
        return v;
    };
    world.batch.snapshotId = "snap-1";
    world.batch.embeddingSpaceIdentity = "test-space";
    world.batch.topologyEpoch = 5;
    for (std::size_t c = 0; c < kClusters; ++c) {
        ClusterArtifact cluster;
        cluster.clusterId = "cluster-" + std::to_string(c);
        std::vector<float> sum(kDim, 0.0F);
        for (int member = 0; member < 11; ++member) {
            auto v = noisy();
            if (member < 10) {
                v[c] += 1.0F;
            } else {
                v[kClusters + c] += 1.0F;
                v[c] += 0.1F;
            }
            v = unit(std::move(v));
            TopologyDocumentInput doc;
            doc.documentHash = "doc-" + std::to_string(c) + "-" + std::to_string(member);
            doc.embedding = v;
            for (std::size_t i = 0; i < kDim; ++i) {
                sum[i] += v[i];
            }
            cluster.memberDocumentHashes.push_back(doc.documentHash);
            world.batch.memberships.push_back(DocumentClusterMembership{
                .documentHash = doc.documentHash, .clusterId = cluster.clusterId});
            world.documents.push_back(std::move(doc));
        }
        cluster.memberCount = cluster.memberDocumentHashes.size();
        cluster.centroidEmbedding = sum; // unnormalized on purpose: the stage normalizes
        world.batch.clusters.push_back(std::move(cluster));
    }
    const auto addQuery = [&](std::vector<std::string>& into, const std::string& text,
                              std::size_t index) {
        const std::size_t target = index % kClusters;
        auto v = noisy();
        v[kClusters + target] += 1.0F;
        for (std::size_t offset = 1; offset <= 5; ++offset) {
            v[(target + offset) % kClusters] += 0.5F;
        }
        // Not normalized: the stage must do it.
        for (auto& x : v) {
            x *= 3.0F;
        }
        world.queryEmbeddings.emplace(text, std::move(v));
        into.push_back(text);
    };
    for (std::size_t i = 0; i < loggedCount; ++i) {
        addQuery(world.logged, "logged query " + std::to_string(i), i);
    }
    for (std::size_t i = 0; i < generatedCount; ++i) {
        addQuery(world.generated, "generated title " + std::to_string(i), i + 3);
    }
    return world;
}

RouteFitStageInput inputOf(const World& world) {
    RouteFitStageInput input;
    input.documents = world.documents;
    input.loggedQueries = world.logged;
    input.generatedQueries = world.generated;
    input.embed =
        [&world](
            std::span<const std::string> texts) -> yams::Result<std::vector<std::vector<float>>> {
        std::vector<std::vector<float>> out;
        out.reserve(texts.size());
        for (const auto& text : texts) {
            out.push_back(world.queryEmbeddings.at(text));
        }
        return out;
    };
    return input;
}

} // namespace

TEST_CASE("runRouteFitStage attaches fitted vectors and records where they came from",
          "[topology][route-fit][stage][catch2]") {
    auto world = makeWorld(200, 280);
    // Repeats and blank entries must not count as queries.
    world.logged.push_back("  logged query 0  ");
    world.logged.push_back("");
    world.generated.push_back("logged query 1");
    // A vector left over from an earlier fit must not survive.
    world.batch.clusters[0].fittedRouteVector = {9.0F};

    auto stage = runRouteFitStage(world.batch, inputOf(world));
    REQUIRE(stage.has_value());
    const auto& stats = stage.value();
    REQUIRE(stats.adopted);
    CHECK(stats.skipReason.empty());
    CHECK(stats.fittedClusters == kClusters);
    CHECK(stats.queriesUsed == 480u);
    CHECK(stats.documentsUsed == kClusters * 11);
    for (const auto& cluster : world.batch.clusters) {
        CHECK(cluster.fittedRouteVector.size() == kDim);
    }
    CHECK(world.batch.routeFit.loggedQueries == 200u);
    CHECK(world.batch.routeFit.generatedQueries == 280u);
    CHECK(world.batch.routeFit.fitQueries == 384u);
    CHECK(world.batch.routeFit.holdoutQueries == 96u);
    CHECK(world.batch.routeFit.fittedAtEpoch == 5u);
    CHECK(world.batch.routeFit.fittedHoldoutRecall > world.batch.routeFit.centroidHoldoutRecall);
    CHECK(world.batch.routeFit.chosenStrength > 0.0);
}

TEST_CASE("runRouteFitStage ignores documents it cannot place and small clusters",
          "[topology][route-fit][stage][catch2]") {
    auto world = makeWorld(200, 280);
    // A document the batch does not know, and one with an embedding of another dimension.
    TopologyDocumentInput stray;
    stray.documentHash = "doc-unknown";
    stray.embedding = unit(std::vector<float>(kDim, 1.0F));
    world.documents.push_back(stray);
    world.documents[0].embedding = {1.0F, 0.0F};
    // A ninth cluster with a single document: it must stay on its centroid.
    ClusterArtifact single;
    single.clusterId = "cluster-single";
    single.memberCount = 1;
    single.memberDocumentHashes = {"doc-single"};
    single.centroidEmbedding = unit(std::vector<float>(kDim, 1.0F));
    world.batch.clusters.push_back(single);
    world.batch.memberships.push_back(
        DocumentClusterMembership{.documentHash = "doc-single", .clusterId = "cluster-single"});
    TopologyDocumentInput singleDoc;
    singleDoc.documentHash = "doc-single";
    singleDoc.embedding = single.centroidEmbedding;
    world.documents.push_back(singleDoc);

    auto stage = runRouteFitStage(world.batch, inputOf(world));
    REQUIRE(stage.has_value());
    REQUIRE(stage.value().adopted);
    CHECK(stage.value().documentsUsed == kClusters * 11 - 1 + 1);
    CHECK(stage.value().fittedClusters == kClusters);
    CHECK(world.batch.clusters.back().fittedRouteVector.empty());
}

TEST_CASE("runRouteFitStage skips with a reason and leaves the batch unfitted",
          "[topology][route-fit][stage][catch2]") {
    const auto expectSkipped = [](World& world, const RouteFitStageInput& input,
                                  const std::string& reason) {
        world.batch.clusters[0].fittedRouteVector = {9.0F};
        world.batch.routeFit.fitQueries = 7;
        auto stage = runRouteFitStage(world.batch, input);
        REQUIRE(stage.has_value());
        CHECK_FALSE(stage.value().adopted);
        CHECK(stage.value().skipReason == reason);
        for (const auto& cluster : world.batch.clusters) {
            CHECK(cluster.fittedRouteVector.empty());
        }
        CHECK(world.batch.routeFit == RouteFitProvenance{});
    };

    SECTION("no embedder") {
        auto world = makeWorld(200, 280);
        auto input = inputOf(world);
        input.embed = nullptr;
        expectSkipped(world, input, "no_embedder");
    }
    SECTION("too few distinct queries") {
        auto world = makeWorld(30, 20);
        expectSkipped(world, inputOf(world), "too_few_queries");
    }
    SECTION("query embeddings of another dimension") {
        auto world = makeWorld(200, 280);
        auto input = inputOf(world);
        input.embed = [](std::span<const std::string> texts)
            -> yams::Result<std::vector<std::vector<float>>> {
            return std::vector<std::vector<float>>(texts.size(), std::vector<float>{1.0F, 0.0F});
        };
        expectSkipped(world, input, "query_embedding_dimension_mismatch");
    }
    SECTION("no document has a usable embedding") {
        auto world = makeWorld(200, 280);
        for (auto& document : world.documents) {
            document.embedding.clear();
        }
        expectSkipped(world, inputOf(world), "no_usable_documents");
    }
    SECTION("queries that centroids already route perfectly") {
        // Queries along the cluster cores: the centroid ranks the right cluster first already.
        auto world = makeWorld(0, 0);
        for (std::size_t i = 0; i < 480; ++i) {
            std::vector<float> v(kDim, 0.0F);
            v[i % kClusters] = 1.0F;
            const auto text = "core query " + std::to_string(i);
            world.queryEmbeddings.emplace(text, v);
            world.logged.push_back(text);
        }
        expectSkipped(world, inputOf(world), "no_gain_over_centroids");
        // The report still says how well centroids already did.
        auto again = runRouteFitStage(world.batch, inputOf(world));
        REQUIRE(again.has_value());
        CHECK(again.value().centroidHoldoutRecall == 1.0);
        CHECK(again.value().fittedHoldoutRecall == 1.0);
    }
}

TEST_CASE("isRouteFitTrainingQuery keeps questions and drops lookups",
          "[topology][route-fit][stage][catch2]") {
    CHECK(isRouteFitTrainingQuery("how does the topology router score clusters"));
    CHECK(isRouteFitTrainingQuery("  resource governor  "));
    CHECK(isRouteFitTrainingQuery("governor"));
    CHECK(isRouteFitTrainingQuery("governor?")); // a one-word question, not a pattern
    CHECK(isRouteFitTrainingQuery("decade"));    // hex letters, but too short to be a hash
    CHECK(isRouteFitTrainingQuery("why does src/daemon/main.cpp exit early"));
    CHECK(isRouteFitTrainingQuery("r\xc3\xa9sum\xc3\xa9 parsing"));

    CHECK_FALSE(isRouteFitTrainingQuery(""));
    CHECK_FALSE(isRouteFitTrainingQuery("ab"));
    CHECK_FALSE(isRouteFitTrainingQuery("12345"));
    CHECK_FALSE(isRouteFitTrainingQuery("40e64c1b432276614226886c1912904fc9e693eb"));
    CHECK_FALSE(isRouteFitTrainingQuery("deadbeef"));
    CHECK_FALSE(isRouteFitTrainingQuery("src/daemon/daemon_main.cpp"));
    CHECK_FALSE(isRouteFitTrainingQuery("*.md"));
    CHECK_FALSE(isRouteFitTrainingQuery("README?.md"));
    CHECK_FALSE(isRouteFitTrainingQuery("  note?.txt  "));
    CHECK_FALSE(isRouteFitTrainingQuery("C:\\Users\\notes.txt"));
}

TEST_CASE("runRouteFitStage filters logged lookups and bounds its work",
          "[topology][route-fit][stage][catch2]") {
    auto world = makeWorld(200, 280);
    SECTION("hash and path lookups in the log are not embedded") {
        world.logged.insert(world.logged.begin(), "40e64c1b432276614226886c1912904fc9e693eb");
        world.logged.insert(world.logged.begin(), "src/topology/topology_baseline.cpp");
        auto stage = runRouteFitStage(world.batch, inputOf(world)); // embed throws on unknowns
        REQUIRE(stage.has_value());
        CHECK(stage.value().queriesUsed == 480u);
        CHECK(world.batch.routeFit.loggedQueries == 200u);
    }
    SECTION("queries are embedded in batches") {
        auto input = inputOf(world);
        std::vector<std::size_t> batches;
        input.embed = [&, inner = input.embed](std::span<const std::string> texts) {
            batches.push_back(texts.size());
            return inner(texts);
        };
        RouteFitOptions options;
        options.embedBatchSize = 100;
        auto stage = runRouteFitStage(world.batch, input, options);
        REQUIRE(stage.has_value());
        CHECK(stage.value().adopted);
        CHECK(batches == std::vector<std::size_t>{100, 100, 100, 100, 80});
    }
    SECTION("a pair budget drops generated queries first") {
        RouteFitOptions options;
        options.maxQueryDocumentPairs = 300 * kClusters * 11; // room for 300 of the 480
        auto stage = runRouteFitStage(world.batch, inputOf(world), options);
        REQUIRE(stage.has_value());
        CHECK(stage.value().queriesUsed == 300u);
        REQUIRE(stage.value().adopted);
        CHECK(world.batch.routeFit.loggedQueries == 200u);
        CHECK(world.batch.routeFit.generatedQueries == 100u);
    }
    SECTION("a corpus too large for the budget is skipped, not fitted badly") {
        RouteFitOptions options;
        options.maxQueryDocumentPairs = 50 * kClusters * 11; // 50 queries: under the minimum
        auto stage = runRouteFitStage(world.batch, inputOf(world), options);
        REQUIRE(stage.has_value());
        CHECK_FALSE(stage.value().adopted);
        CHECK(stage.value().skipReason == "corpus_too_large_for_budget");
    }
}

TEST_CASE("runRouteFitStage propagates embedder failures and cancellation",
          "[topology][route-fit][stage][catch2]") {
    auto world = makeWorld(200, 280);
    SECTION("embedder error") {
        auto input = inputOf(world);
        input.embed =
            [](std::span<const std::string>) -> yams::Result<std::vector<std::vector<float>>> {
            return yams::Error{yams::ErrorCode::NotInitialized, "model not loaded"};
        };
        auto stage = runRouteFitStage(world.batch, input);
        REQUIRE_FALSE(stage.has_value());
        CHECK(stage.error().code == yams::ErrorCode::NotInitialized);
    }
    SECTION("cancelled mid-fit") {
        RouteFitOptions options;
        options.cancelled = [] { return true; };
        auto stage = runRouteFitStage(world.batch, inputOf(world), options);
        REQUIRE_FALSE(stage.has_value());
        CHECK(stage.error().code == yams::ErrorCode::OperationCancelled);
        for (const auto& cluster : world.batch.clusters) {
            CHECK(cluster.fittedRouteVector.empty());
        }
    }
}
