// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

#include <catch2/catch_test_macros.hpp>

#include "../../common/test_helpers_catch2.h"

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams::metadata;

TEST_CASE("FtsSearchQuerySpec: basic FTS when available", "[unit][metadata][fts]") {
    // Skip if FTS5 not available
    Database db;
    auto dbFile = std::filesystem::temp_directory_path() / "fts_qs_catch2_basic.db";
    std::error_code ec;
    std::filesystem::remove(dbFile, ec);
    REQUIRE(db.open(dbFile.string(), ConnectionMode::Create));
    auto f = db.hasFTS5();
    if (!f || !f.value()) {
        SKIP("FTS5 not available");
    }

    ConnectionPool pool(dbFile.string());
    REQUIRE(pool.initialize());
    MetadataRepository repo(pool);

    // Insert a doc and index content
    DocumentInfo d{};
    d.filePath = "/q/hello.txt";
    auto dv = computePathDerivedValues(d.filePath);
    d.fileName = std::filesystem::path(dv.normalizedPath).filename().string();
    d.fileExtension = ".txt";
    d.fileSize = 0;
    d.sha256Hash = "HX";
    d.pathPrefix = dv.pathPrefix;
    d.reversePath = dv.reversePath;
    d.pathHash = dv.pathHash;
    d.parentHash = dv.parentHash;
    d.pathDepth = dv.pathDepth;
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.createdTime = d.modifiedTime = d.indexedTime = now;
    auto id = repo.insertDocument(d);
    REQUIRE(id);
    REQUIRE(
        repo.indexDocumentContent(id.value(), "Hello Title", "hello world content", "text/plain"));

    auto res = repo.search("hello", 10, 0, std::nullopt);
    if (!res.has_value()) {
        SKIP("Search unavailable in this environment: " + res.error().message);
    }
    CHECK(res.value().isSuccess());
    CHECK(res.value().results.size() >= 1);

    // Cleanup
    pool.shutdown();
    std::filesystem::remove(dbFile, ec);
}

namespace {

int64_t insertIndexedDoc(MetadataRepository& repo, const std::string& path, const std::string& hash,
                         const std::string& title, const std::string& content) {
    DocumentInfo d{};
    d.filePath = path;
    auto dv = computePathDerivedValues(d.filePath);
    d.fileName = std::filesystem::path(dv.normalizedPath).filename().string();
    d.fileExtension = ".txt";
    d.fileSize = static_cast<int64_t>(content.size());
    d.sha256Hash = hash;
    d.pathPrefix = dv.pathPrefix;
    d.reversePath = dv.reversePath;
    d.pathHash = dv.pathHash;
    d.parentHash = dv.parentHash;
    d.pathDepth = dv.pathDepth;
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.createdTime = d.modifiedTime = d.indexedTime = now;
    auto id = repo.insertDocument(d);
    REQUIRE(id);
    REQUIRE(repo.indexDocumentContent(id.value(), title, content, "text/plain"));
    return id.value();
}

std::vector<std::string> resultPaths(const SearchResults& results) {
    std::vector<std::string> paths;
    for (const auto& r : results.results) {
        paths.push_back(r.document.filePath);
    }
    return paths;
}

} // namespace

// Prose queries (BEIR claims, questions) almost never have every term in one document. Keyword
// search ran them as a strict AND (or an exact phrase for 3-4 terms) and returned nothing, which
// left keyword-only search near zero on scifact (nDCG@10 0.04 against 0.67 for BM25).
TEST_CASE("FTS keyword search reruns a short natural-language AND page as ranked OR",
          "[unit][metadata][fts][regression]") {
    auto dbFile = std::filesystem::temp_directory_path() / "fts_qs_catch2_nl_or.db";
    std::error_code ec;
    std::filesystem::remove(dbFile, ec);
    {
        Database db;
        REQUIRE(db.open(dbFile.string(), ConnectionMode::Create));
        auto f = db.hasFTS5();
        if (!f || !f.value()) {
            SKIP("FTS5 not available");
        }
    }

    yams::test::ScopedEnvVar fallbackEnv("YAMS_FTS_NL_OR_FALLBACK", std::nullopt);
    yams::test::ScopedEnvVar minResultsEnv("YAMS_FTS_NL_MIN_RESULTS", std::nullopt);
    yams::test::ScopedEnvVar modeEnv("YAMS_FTS_MODE", std::nullopt);
    yams::test::ScopedEnvVar phraseEnv("YAMS_FTS_NL_AUTO_PHRASE", std::nullopt);

    ConnectionPool pool(dbFile.string());
    REQUIRE(pool.initialize());
    MetadataRepository repo(pool);

    insertIndexedDoc(repo, "/nl/goitre.txt", "H1", "Radioiodine therapy for goitre",
                     "Radioiodine reduced thyroid volume in patients with multinodular goitre.");
    insertIndexedDoc(repo, "/nl/flies.txt", "H2", "Rapamycin in Drosophila",
                     "Rapamycin lowered triacylglycerol stores in fruit flies.");
    insertIndexedDoc(repo, "/nl/unrelated.txt", "H3", "Compiler notes",
                     "Register allocation and instruction scheduling.");

    // "treatment" and "non-toxic" appear in no document, so the AND query matches nothing.
    const std::string claim =
        "Radioiodine treatment of non-toxic multinodular goitre reduces thyroid volume.";

    SECTION("sentence-length claim with no all-terms document") {
        auto res = repo.searchKeyword(claim, 10, 0, std::nullopt);
        REQUIRE(res.has_value());
        auto paths = resultPaths(res.value());
        REQUIRE_FALSE(paths.empty());
        CHECK(paths.front() == "/nl/goitre.txt");
        CHECK(std::find(paths.begin(), paths.end(), "/nl/unrelated.txt") == paths.end());
    }

    SECTION("three-term query whose terms are not adjacent") {
        // Three content terms become an exact phrase; the OR rerun must not reuse the phrase.
        auto res = repo.searchKeyword("rapamycin fruit triacylglycerol", 10, 0, std::nullopt);
        REQUIRE(res.has_value());
        auto paths = resultPaths(res.value());
        REQUIRE_FALSE(paths.empty());
        CHECK(paths.front() == "/nl/flies.txt");
    }

    SECTION("candidate search keeps strict AND") {
        auto res = repo.search(claim, 10, 0, std::nullopt);
        REQUIRE(res.has_value());
        CHECK(res.value().results.empty());
    }

    SECTION("strict AND stays available behind the env switch") {
        fallbackEnv.set("0");
        auto res = repo.searchKeyword(claim, 10, 0, std::nullopt);
        REQUIRE(res.has_value());
        CHECK(res.value().results.empty());
    }

    pool.shutdown();
    std::filesystem::remove(dbFile, ec);
}
