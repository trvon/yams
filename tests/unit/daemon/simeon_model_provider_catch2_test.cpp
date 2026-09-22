// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for SimeonModelProvider::scoreDocuments (bi-encoder cosine rerank).

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/simeon_model_provider.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

using yams::daemon::IModelProvider;
using yams::daemon::makeSimeonModelProvider;

namespace {

std::unique_ptr<IModelProvider> makeProvider() {
    auto p = makeSimeonModelProvider(384);
    REQUIRE(p != nullptr);
    REQUIRE(p->isAvailable());
    return p;
}

} // namespace

TEST_CASE("SimeonModelProvider::scoreDocuments empty docs returns empty vector",
          "[daemon][model_provider][simeon][rerank]") {
    auto p = makeProvider();
    auto r = p->scoreDocuments("anything", {});
    REQUIRE(r.has_value());
    CHECK(r.value().empty());
}

TEST_CASE("SimeonModelProvider::scoreDocuments identity sanity: query == doc[0] is max",
          "[daemon][model_provider][simeon][rerank]") {
    auto p = makeProvider();
    const std::string query = "ingest pipeline admission controller";
    std::vector<std::string> docs = {
        query,
        "totally unrelated topic about distributed raft consensus",
        "benchmark report on vector quantization quality",
    };
    auto r = p->scoreDocuments(query, docs);
    REQUIRE(r.has_value());
    REQUIRE(r.value().size() == docs.size());

    const auto& scores = r.value();
    const auto maxIt = std::max_element(scores.begin(), scores.end());
    const std::size_t maxIdx = static_cast<std::size_t>(std::distance(scores.begin(), maxIt));
    CHECK(maxIdx == 0);
    CHECK(scores[0] > 0.99f);
}

TEST_CASE("SimeonModelProvider::scoreDocuments ranks near-duplicate above outlier",
          "[daemon][model_provider][simeon][rerank]") {
    auto p = makeProvider();
    const std::string query = "post ingest queue backpressure tuning";
    std::vector<std::string> docs = {
        "post-ingest queue tuning and backpressure controls",
        "cake recipe with vanilla frosting",
    };
    auto r = p->scoreDocuments(query, docs);
    REQUIRE(r.has_value());
    REQUIRE(r.value().size() == 2u);
    CHECK(r.value()[0] > r.value()[1]);
}

TEST_CASE("SimeonModelProvider::scoreDocuments is deterministic across calls",
          "[daemon][model_provider][simeon][rerank]") {
    auto p = makeProvider();
    const std::string query = "plugin trust list";
    std::vector<std::string> docs = {
        "plugin host trust policy",
        "unrelated benchmark harness",
    };
    auto r1 = p->scoreDocuments(query, docs);
    auto r2 = p->scoreDocuments(query, docs);
    REQUIRE(r1.has_value());
    REQUIRE(r2.has_value());
    REQUIRE(r1.value().size() == r2.value().size());
    for (std::size_t i = 0; i < r1.value().size(); ++i) {
        CHECK(r1.value()[i] == r2.value()[i]);
    }
}

TEST_CASE("SimeonModelProvider::scoreDocuments with FragmentOuterMaxSim mode",
          "[daemon][model_provider][simeon][rerank][maxsim]") {
    using yams::daemon::setSimeonScoringMode;
    using yams::daemon::SimeonScoringMode;

    auto p = makeSimeonModelProvider(384, SimeonScoringMode::FragmentOuterMaxSim);
    REQUIRE(p != nullptr);
    REQUIRE(p->isAvailable());

    SECTION("empty docs returns empty vector") {
        auto r = p->scoreDocuments("anything", {});
        REQUIRE(r.has_value());
        CHECK(r.value().empty());
    }

    SECTION("identity sanity: query == doc[0] is max") {
        const std::string query = "ingest pipeline admission controller";
        std::vector<std::string> docs = {
            query,
            "totally unrelated topic about distributed raft consensus",
            "benchmark report on vector quantization quality",
        };
        auto r = p->scoreDocuments(query, docs);
        REQUIRE(r.has_value());
        REQUIRE(r.value().size() == docs.size());

        const auto& scores = r.value();
        const auto maxIt = std::max_element(scores.begin(), scores.end());
        const std::size_t maxIdx = static_cast<std::size_t>(std::distance(scores.begin(), maxIt));
        CHECK(maxIdx == 0);
        CHECK(scores[0] > 0.99f);
    }

    SECTION("fragment match survives dilution by unrelated sentences") {
        const std::string query = "admission controller backpressure controls";
        // Doc 0 has a sentence directly matching the query followed by unrelated sentences
        std::vector<std::string> docs = {
            "admission controller backpressure controls for queue overflow.\n"
            "This section discusses unrelated database migrations and schema.\n"
            "Another paragraph regarding network socket timeouts and keepalive.\n"
            "Finally some notes on user interface colors and typography.",
            "baking chocolate cookies with vanilla sugar and fresh butter.",
        };
        auto r = p->scoreDocuments(query, docs);
        REQUIRE(r.has_value());
        REQUIRE(r.value().size() == 2u);
        CHECK(r.value()[0] > r.value()[1]);
        CHECK(r.value()[0] > 0.70f);
    }

    SECTION("setSimeonScoringMode switches scoring mode in place") {
        const std::string query = "sparse guided cluster router";
        std::vector<std::string> docs = {
            "sparse guided cluster router for candidate retrieval.\nUnrelated filler "
            "sentence.\nAnother filler sentence.",
        };

        setSimeonScoringMode(*p, SimeonScoringMode::SingleVectorCosine);
        auto rCos = p->scoreDocuments(query, docs);
        REQUIRE(rCos.has_value());

        setSimeonScoringMode(*p, SimeonScoringMode::FragmentOuterMaxSim);
        auto rMaxSim = p->scoreDocuments(query, docs);
        REQUIRE(rMaxSim.has_value());

        // Under MaxSim, the isolated best-matching fragment should score at least as high as
        // the diluted whole-doc cosine score.
        CHECK(rMaxSim.value()[0] >= rCos.value()[0] - 0.05f);
    }
}
