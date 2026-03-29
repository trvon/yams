// Copyright 2025 YAMS Authors
// SPDX-License-Identifier: GPL-3.0-or-later OR MIT

/**
 * @file compressed_ann_dedup_catch2_test.cpp
 * @brief Pre-fusion deduplication regression tests for CompressedANN
 *
 * Tests the pruneDuplicateCompressedAnnResults helper function which:
 * 1. Removes compressed-ANN docs that are already in exact vector results
 * 2. Collapses duplicate compressed-ANN docs to one
 * 3. Preserves exact vector docs
 * 4. Preserves unique compressed-ANN docs
 *
 * Key behaviors tested:
 * - exact+CA duplicate: compressed ANN removed when exact vector exists
 * - CA duplicate+CA duplicate: duplicates collapse to one
 * - hash-only doc: uses documentHash when filePath is empty
 * - path-first dedup: prefers filePath over documentHash
 */

#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_engine.h>

#include <string>
#include <unordered_set>
#include <vector>

using yams::search::ComponentResult;
using yams::search::documentIdFromComponent;
using yams::search::pruneDuplicateCompressedAnnResults;

namespace {

ComponentResult makeComponent(std::string hash, std::string path, ComponentResult::Source source,
                              size_t rank = 0) {
    ComponentResult c;
    c.documentHash = std::move(hash);
    c.filePath = std::move(path);
    c.score = 0.5f;
    c.source = source;
    c.rank = rank;
    return c;
}

} // namespace

TEST_CASE("documentIdFromComponent prefers non-empty filePath", "[search][dedup][compressed_ann]") {
    ComponentResult comp;
    comp.documentHash = "hash-only";
    comp.filePath = "/path/to/file.txt";

    auto id = documentIdFromComponent(comp);

    CHECK(id == "/path/to/file.txt");
}

TEST_CASE("documentIdFromComponent falls back to documentHash when filePath is empty",
          "[search][dedup][compressed_ann]") {
    ComponentResult comp;
    comp.documentHash = "fallback-hash";
    comp.filePath = "";

    auto id = documentIdFromComponent(comp);

    CHECK(id == "fallback-hash");
}

TEST_CASE("documentIdFromComponent returns empty when both are empty",
          "[search][dedup][compressed_ann]") {
    ComponentResult comp;
    comp.documentHash = "";
    comp.filePath = "";

    auto id = documentIdFromComponent(comp);

    CHECK(id.empty());
}

TEST_CASE("pruneDuplicateCompressedAnnResults removes CA when exact vector exists",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc A: exact vector hit
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::Vector, 0));
    // Doc A: compressed ANN hit (should be removed - exact exists)
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 1));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 1);
    CHECK(pruned[0].source == ComponentResult::Source::Vector);
}

TEST_CASE("pruneDuplicateCompressedAnnResults preserves CA when no exact vector exists",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc A: compressed ANN hit only (should be preserved)
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 0));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 1);
    CHECK(pruned[0].source == ComponentResult::Source::CompressedANN);
}

TEST_CASE("pruneDuplicateCompressedAnnResults collapses duplicate CA docs",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc A: multiple compressed ANN hits (should collapse to one)
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 0));
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 1));
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 2));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 1);
    CHECK(pruned[0].source == ComponentResult::Source::CompressedANN);
}

TEST_CASE("pruneDuplicateCompressedAnnResults keeps unique CA docs",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc A: compressed ANN
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 0));
    // Doc B: compressed ANN (different doc, should be preserved)
    components.push_back(
        makeComponent("hash-B", "/path/B.txt", ComponentResult::Source::CompressedANN, 0));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 2);
}

TEST_CASE("pruneDuplicateCompressedAnnResults does not modify exact vector docs",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc A: exact vector only
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::Vector, 0));
    // Doc B: exact vector only
    components.push_back(
        makeComponent("hash-B", "/path/B.txt", ComponentResult::Source::Vector, 0));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 2);
    CHECK(pruned[0].source == ComponentResult::Source::Vector);
    CHECK(pruned[1].source == ComponentResult::Source::Vector);
}

TEST_CASE("pruneDuplicateCompressedAnnResults handles hash-only docs",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc with no filePath, uses hash
    components.push_back(makeComponent("hash-A", "", ComponentResult::Source::Vector, 0));
    components.push_back(makeComponent("hash-A", "", ComponentResult::Source::CompressedANN, 1));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 1);
    CHECK(pruned[0].source == ComponentResult::Source::Vector);
}

TEST_CASE("pruneDuplicateCompressedAnnResults handles mixed sources",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // Doc A: vector
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::Vector, 0));
    // Doc A: CA (removed - exact exists)
    components.push_back(
        makeComponent("hash-A", "/path/A.txt", ComponentResult::Source::CompressedANN, 1));
    // Doc B: CA only (kept)
    components.push_back(
        makeComponent("hash-B", "/path/B.txt", ComponentResult::Source::CompressedANN, 0));
    // Doc B: another CA (deduped)
    components.push_back(
        makeComponent("hash-B", "/path/B.txt", ComponentResult::Source::CompressedANN, 1));
    // Doc C: text (kept, not affected)
    components.push_back(makeComponent("hash-C", "/path/C.txt", ComponentResult::Source::Text, 0));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    REQUIRE(pruned.size() == 3);
    // Check we have exactly one of each source
    std::unordered_set<ComponentResult::Source> sources;
    for (const auto& c : pruned) {
        sources.insert(c.source);
    }
    CHECK(sources.count(ComponentResult::Source::Vector) == 1);
    CHECK(sources.count(ComponentResult::Source::CompressedANN) == 1);
    CHECK(sources.count(ComponentResult::Source::Text) == 1);
}

TEST_CASE("pruneDuplicateCompressedAnnResults returns empty on empty input",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    CHECK(pruned.empty());
}

TEST_CASE("pruneDuplicateCompressedAnnResults handles all sources",
          "[search][dedup][compressed_ann]") {
    std::vector<ComponentResult> components;

    // All non-CA sources should pass through unchanged
    components.push_back(makeComponent("hash-T", "/path/T.txt", ComponentResult::Source::Text, 0));
    components.push_back(
        makeComponent("hash-GT", "/path/GT.txt", ComponentResult::Source::GraphText, 0));
    components.push_back(
        makeComponent("hash-KG", "/path/KG.txt", ComponentResult::Source::KnowledgeGraph, 0));
    components.push_back(
        makeComponent("hash-V", "/path/V.txt", ComponentResult::Source::Vector, 0));
    components.push_back(
        makeComponent("hash-GV", "/path/GV.txt", ComponentResult::Source::GraphVector, 0));
    components.push_back(
        makeComponent("hash-EV", "/path/EV.txt", ComponentResult::Source::EntityVector, 0));
    components.push_back(
        makeComponent("hash-PT", "/path/PT.txt", ComponentResult::Source::PathTree, 0));
    components.push_back(
        makeComponent("hash-Tag", "/path/Tag.txt", ComponentResult::Source::Tag, 0));
    components.push_back(
        makeComponent("hash-M", "/path/M.txt", ComponentResult::Source::Metadata, 0));
    components.push_back(
        makeComponent("hash-S", "/path/S.txt", ComponentResult::Source::Symbol, 0));

    auto pruned = pruneDuplicateCompressedAnnResults(std::move(components));

    CHECK(pruned.size() == 10);
}
