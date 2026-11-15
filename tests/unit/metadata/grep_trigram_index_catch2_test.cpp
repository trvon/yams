// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

using namespace yams::metadata;

namespace {

struct TrigramFixture {
    TrigramFixture() {
        const auto base = std::filesystem::temp_directory_path();
        dbPath =
            base /
            ("grep_trigram_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
        ConnectionPoolConfig cfg;
        cfg.enableWAL = false;
        cfg.minConnections = 1;
        cfg.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), cfg);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_unique<MetadataRepository>(*pool);
    }
    ~TrigramFixture() {
        repo.reset();
        pool->shutdown();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    int64_t addDocument(const std::string& name, const std::string& hash) {
        DocumentInfo d;
        d.filePath = "/corpus/" + name;
        const auto derived = computePathDerivedValues(d.filePath);
        d.fileName = name;
        d.fileExtension = ".txt";
        d.sha256Hash = hash;
        d.mimeType = "text/plain";
        d.pathPrefix = derived.pathPrefix;
        d.reversePath = derived.reversePath;
        d.pathHash = derived.pathHash;
        d.parentHash = derived.parentHash;
        d.pathDepth = derived.pathDepth;
        const auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        d.createdTime = d.modifiedTime = d.indexedTime = now;
        auto id = repo->insertDocument(d);
        REQUIRE(id.has_value());
        return id.value();
    }

    std::vector<int64_t> matches(std::vector<std::string> literals) {
        auto result = repo->grepTrigramMatches(literals);
        REQUIRE(result.has_value());
        REQUIRE(result.value().has_value());
        auto ids = *result.value();
        std::ranges::sort(ids);
        return ids;
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::unique_ptr<MetadataRepository> repo;
};

} // namespace

TEST_CASE("Grep trigram index narrows documents to those containing a literal",
          "[metadata][grep][trigram]") {
    TrigramFixture f;
    REQUIRE(f.repo->grepTrigramIndexAvailable());
    const auto alpha = f.addDocument("alpha.txt", "hash-alpha");
    const auto beta = f.addDocument("beta.txt", "hash-beta");
    const auto gamma = f.addDocument("gamma.txt", "hash-gamma");
    REQUIRE(f.repo->indexGrepTrigrams(alpha, "hash-alpha", "if (admitDeferrable(work)) run();")
                .has_value());
    REQUIRE(f.repo->indexGrepTrigrams(beta, "hash-beta", "ResourceGovernor admits deferrable work")
                .has_value());
    REQUIRE(f.repo->indexGrepTrigrams(gamma, "hash-gamma", "nothing relevant here").has_value());

    CHECK(f.matches({"admitDeferrable"}) == std::vector<int64_t>{alpha});
    // Case-insensitive, so a case-sensitive grep still sees every possible match.
    CHECK(f.matches({"ADMITDEFERRABLE"}) == std::vector<int64_t>{alpha});
    CHECK(f.matches({"ResourceGovernor"}) == std::vector<int64_t>{beta});
    // Every literal is required.
    CHECK(f.matches({"admit", "work"}) == std::vector<int64_t>{alpha, beta});
    CHECK(f.matches({"admitDeferrable", "Governor"}).empty());
    // Punctuation and spaces are part of trigrams.
    CHECK(f.matches({"run();"}) == std::vector<int64_t>{alpha});
}

TEST_CASE("Grep trigram index cannot narrow on literals shorter than three characters",
          "[metadata][grep][trigram]") {
    TrigramFixture f;
    const auto doc = f.addDocument("short.txt", "hash-short");
    REQUIRE(f.repo->indexGrepTrigrams(doc, "hash-short", "ab cd").has_value());
    const std::vector<std::string> shortOnly{"ab", "c"};
    auto result = f.repo->grepTrigramMatches(shortOnly);
    REQUIRE(result.has_value());
    CHECK_FALSE(result.value().has_value());
    // A usable literal still narrows when a short one sits beside it.
    CHECK(f.matches({"ab", "b c"}) == std::vector<int64_t>{doc});
    CHECK(f.matches({"ab", "xyz"}).empty());
}

TEST_CASE("Grep trigram index splits literals into characters, not bytes",
          "[metadata][grep][trigram][utf8]") {
    TrigramFixture f;
    const auto doc = f.addDocument("utf8.txt", "hash-utf8");
    REQUIRE(f.repo->indexGrepTrigrams(doc, "hash-utf8", "un café crème brûlée").has_value());
    // "afé" is three characters but four bytes; byte trigrams would not match the index.
    CHECK(f.matches({"afé"}) == std::vector<int64_t>{doc});
    CHECK(f.matches({"crème"}) == std::vector<int64_t>{doc});
    CHECK(f.matches({"crèmes"}).empty());
}

TEST_CASE("Grep trigram coverage tracks indexed content and removal",
          "[metadata][grep][trigram][coverage]") {
    TrigramFixture f;
    const auto doc = f.addDocument("doc.txt", "hash-v1");
    REQUIRE(f.repo->indexGrepTrigrams(doc, "hash-v1", "first version text").has_value());
    {
        auto coverage = f.repo->grepTrigramCoverage();
        REQUIRE(coverage.has_value());
        REQUIRE(coverage.value().contains(doc));
        CHECK(coverage.value().at(doc) == "hash-v1");
    }
    // Re-indexing replaces the trigrams and the covered hash.
    REQUIRE(f.repo->indexGrepTrigrams(doc, "hash-v2", "second revision words").has_value());
    CHECK(f.matches({"first version"}).empty());
    CHECK(f.matches({"second revision"}) == std::vector<int64_t>{doc});
    CHECK(f.repo->grepTrigramCoverage().value().at(doc) == "hash-v2");

    REQUIRE(f.repo->removeGrepTrigrams(doc).has_value());
    CHECK(f.matches({"second revision"}).empty());
    CHECK_FALSE(f.repo->grepTrigramCoverage().value().contains(doc));
}

TEST_CASE("Grep trigram index tolerates bytes that are not valid UTF-8",
          "[metadata][grep][trigram][utf8]") {
    TrigramFixture f;
    const auto doc = f.addDocument("bin.txt", "hash-bin");
    std::string bytes = "needle_one ";
    bytes += static_cast<char>(0xFF);
    bytes += static_cast<char>(0xC3);
    bytes += " needle_two";
    REQUIRE(f.repo->indexGrepTrigrams(doc, "hash-bin", bytes).has_value());
    CHECK(f.matches({"needle_one"}) == std::vector<int64_t>{doc});
    CHECK(f.matches({"needle_two"}) == std::vector<int64_t>{doc});
}
