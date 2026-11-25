// Copyright (c) 2026 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>
#include <yams/daemon/components/gliner_query_extractor.h>

#include <algorithm>
#include <string>
#include <vector>

using yams::daemon::createEntityExtractionFunc;
using yams::daemon::createSimeonExtractionFunc;

namespace {

bool hasConcept(const std::vector<yams::search::QueryConcept>& concepts, std::string_view type,
                std::string_view expectedSubstr) {
    return std::any_of(concepts.begin(), concepts.end(), [&](const auto& c) {
        return c.type == type && c.text.find(expectedSubstr) != std::string::npos;
    });
}

} // namespace

TEST_CASE("Simeon extractor extracts structural titles from Markdown, YAML, HTML, and Setext",
          "[daemon][extractor][simeon][title]") {
    auto fn = createSimeonExtractionFunc();
    REQUIRE(fn);

    SECTION("Markdown ATX heading") {
        const std::string doc = "# Resource Governor Architecture\n\nBody text about admission.\n";
        auto res = fn(doc, {"title"});
        REQUIRE(res.has_value());
        CHECK(res.value().usedGliner);
        REQUIRE(hasConcept(res.value().concepts, "title", "Resource Governor Architecture"));
    }

    SECTION("YAML frontmatter title") {
        const std::string doc =
            "---\ntitle: \"Write Coordinator Batching\"\ndate: 2026-11-25\n---\n\nIntro text.\n";
        auto res = fn(doc, {"title"});
        REQUIRE(res.has_value());
        REQUIRE(hasConcept(res.value().concepts, "title", "Write Coordinator Batching"));
    }

    SECTION("HTML title tag") {
        const std::string doc =
            "<html><head><title>YAMS Hybrid Search Guide</title></head><body>Hello</body></html>";
        auto res = fn(doc, {"title"});
        REQUIRE(res.has_value());
        REQUIRE(hasConcept(res.value().concepts, "title", "YAMS Hybrid Search Guide"));
    }

    SECTION("Setext underlined heading") {
        const std::string doc = "Topology Routing Design\n=======================\n\nDetails.\n";
        auto res = fn(doc, {"title"});
        REQUIRE(res.has_value());
        REQUIRE(hasConcept(res.value().concepts, "title", "Topology Routing Design"));
    }
}

TEST_CASE("Simeon extractor falls back to TextRank for plain text without headings",
          "[daemon][extractor][simeon][title]") {
    auto fn = createSimeonExtractionFunc();
    REQUIRE(fn);

    const std::string prose =
        "Content addressable storage deduplicates chunks using SHA256 hashes across documents. "
        "SQLite WAL mode allows concurrent readers while the write coordinator batches metadata "
        "updates. "
        "Deduplicating content addressable chunks reduces disk usage and speeds up incremental "
        "indexing across large corpora.";

    auto res = fn(prose, {"title"});
    REQUIRE(res.has_value());
    CHECK(res.value().usedGliner);
    REQUIRE_FALSE(res.value().concepts.empty());
    CHECK(res.value().concepts.front().type == "title");
    CHECK(res.value().concepts.front().confidence >= 0.55f);
    CHECK(res.value().concepts.front().text.size() <= 120);
}

TEST_CASE("Simeon extractor extracts technical identifiers and multi-word concepts",
          "[daemon][extractor][simeon][entity]") {
    auto fn = createEntityExtractionFunc({});
    REQUIRE(fn);

    const std::string text =
        "The WriteCoordinator batches SQLite WAL mutations for Resource Governor admission. "
        "SearchEngine queries HNSW centroids and sqlite_vec_backend vectors over SIMD.";

    auto res = fn(text, {"title", "technology", "concept"});
    REQUIRE(res.has_value());
    CHECK(res.value().usedGliner);
    CHECK(hasConcept(res.value().concepts, "concept", "WriteCoordinator"));
    CHECK(hasConcept(res.value().concepts, "concept", "HNSW"));
    CHECK(hasConcept(res.value().concepts, "concept", "sqlite_vec_backend"));
    CHECK(hasConcept(res.value().concepts, "concept", "Resource Governor"));
}

TEST_CASE("Simeon extractor handles empty input cleanly", "[daemon][extractor][simeon]") {
    auto fn = createEntityExtractionFunc({});
    REQUIRE(fn);

    auto res = fn("", {"title", "concept"});
    REQUIRE(res.has_value());
    CHECK_FALSE(res.value().usedGliner);
    CHECK(res.value().concepts.empty());
}
