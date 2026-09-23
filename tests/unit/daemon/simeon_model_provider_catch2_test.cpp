// SPDX-License-Identifier: GPL-3.0-or-later
//
// Unit tests for SimeonModelProvider::scoreDocuments (bi-encoder cosine rerank).

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/resource/model_provider.h>
#include <yams/daemon/resource/simeon_model_provider.h>

#include "src/daemon/resource/simeon_fragments.h"

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

namespace {

std::string numberedSentences(std::size_t count, std::size_t insertAt, const std::string& insert) {
    std::string text;
    for (std::size_t i = 0; i < count; ++i) {
        if (i == insertAt) {
            text += insert + " ";
        }
        text += "Filler sentence number " + std::to_string(i) + " about unrelated weather. ";
    }
    return text;
}

} // namespace

TEST_CASE("selectMaxSimFragments samples across the whole document",
          "[daemon][model_provider][simeon][maxsim]") {
    using yams::daemon::detail::selectMaxSimFragments;

    SECTION("Short single sentence yields one fragment") {
        auto fragments = selectMaxSimFragments("Just one sentence here.", 8);
        REQUIRE(fragments.size() == 1U);
        CHECK(fragments.front() == "Just one sentence here.");
    }

    SECTION("Long document keeps first, last, and a head view within budget") {
        const auto text = numberedSentences(40, 40, "");
        auto fragments = selectMaxSimFragments(text, 8);
        REQUIRE(fragments.size() == 8U);
        CHECK(fragments.front().find("number 0 ") != std::string::npos);
        // The last sampled sentence is the document's final sentence, not the eighth.
        CHECK(fragments[6].find("number 39 ") != std::string::npos);
        // Head view (first 512 chars) is the reserved final slot.
        CHECK(fragments.back().size() == 512U);
    }

    SECTION("Budget of one keeps a single fragment") {
        auto fragments = selectMaxSimFragments(numberedSentences(10, 10, ""), 1);
        CHECK(fragments.size() == 1U);
    }

    SECTION("Empty text and zero budget yield nothing") {
        CHECK(selectMaxSimFragments("", 8).empty());
        CHECK(selectMaxSimFragments("Some text here.", 0).empty());
    }
}

TEST_CASE("fragmentsPerDocument bounds the per-call fragment total",
          "[daemon][model_provider][simeon][maxsim]") {
    using yams::daemon::detail::fragmentsPerDocument;
    CHECK(fragmentsPerDocument(10, 8, 256) == 8U);
    CHECK(fragmentsPerDocument(64, 8, 256) == 4U);
    CHECK(fragmentsPerDocument(1000, 8, 256) == 1U);
    CHECK(fragmentsPerDocument(0, 8, 256) == 8U);
}

TEST_CASE("SimeonModelProvider outer MaxSim reaches a late relevant sentence",
          "[daemon][model_provider][simeon][maxsim]") {
    auto provider = yams::daemon::makeSimeonModelProvider(
        384, yams::daemon::SimeonScoringMode::FragmentOuterMaxSim);
    REQUIRE(provider != nullptr);
    REQUIRE(provider->isAvailable());

    const std::string relevant =
        "Quantum annealing hardware schedules qubit couplers for combinatorial optimization.";
    const std::vector<std::string> documents = {
        numberedSentences(30, 25, relevant), // relevant sentence far past the first eight
        numberedSentences(30, 30, ""),       // same filler, no relevant sentence
    };
    auto scores = provider->scoreDocuments(
        "quantum annealing qubit couplers combinatorial optimization", documents);
    REQUIRE(scores.has_value());
    REQUIRE(scores.value().size() == 2U);
    CHECK(scores.value()[0] > scores.value()[1]);
}
