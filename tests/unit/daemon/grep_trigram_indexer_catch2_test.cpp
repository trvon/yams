// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2026 YAMS Contributors

#include <catch2/catch_test_macros.hpp>

#include <yams/daemon/components/GrepTrigramIndexer.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

using yams::daemon::GrepTrigramIndexer;
using namespace yams::metadata;

namespace {

struct IndexerFixture {
    IndexerFixture() {
        dbPath =
            std::filesystem::temp_directory_path() /
            ("grep_trigram_indexer_" +
             std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".db");
        ConnectionPoolConfig cfg;
        cfg.enableWAL = false;
        cfg.minConnections = 1;
        cfg.maxConnections = 2;
        pool = std::make_unique<ConnectionPool>(dbPath.string(), cfg);
        REQUIRE(pool->initialize().has_value());
        repo = std::make_shared<MetadataRepository>(*pool);
    }
    ~IndexerFixture() {
        repo.reset();
        pool->shutdown();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }

    int64_t add(const std::string& name, const std::string& body, std::string mime = "text/plain",
                std::int64_t size = -1) {
        const auto hash = "hash-" + name;
        blobs[hash] = body;
        DocumentInfo d;
        d.filePath = "/corpus/" + name;
        const auto derived = computePathDerivedValues(d.filePath);
        d.fileName = name;
        d.fileExtension = ".txt";
        d.fileSize = size >= 0 ? size : static_cast<std::int64_t>(body.size());
        d.sha256Hash = hash;
        d.mimeType = std::move(mime);
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

    GrepTrigramIndexer::Options options() {
        GrepTrigramIndexer::Options o;
        o.fetchBytes = [this](const std::string& hash) -> yams::Result<std::vector<std::byte>> {
            ++fetches;
            const auto it = blobs.find(hash);
            if (it == blobs.end())
                return yams::Error{yams::ErrorCode::NotFound, "no blob"};
            std::vector<std::byte> bytes(it->second.size());
            std::memcpy(bytes.data(), it->second.data(), it->second.size());
            return bytes;
        };
        return o;
    }

    std::vector<int64_t> matching(const std::string& literal) {
        const std::vector<std::string> literals{literal};
        auto result = repo->grepTrigramMatches(literals);
        REQUIRE(result.has_value());
        REQUIRE(result.value().has_value());
        auto ids = *result.value();
        std::ranges::sort(ids);
        return ids;
    }

    std::filesystem::path dbPath;
    std::unique_ptr<ConnectionPool> pool;
    std::shared_ptr<MetadataRepository> repo;
    std::map<std::string, std::string> blobs;
    int fetches = 0;
};

} // namespace

TEST_CASE("GrepTrigramIndexer backfills every pending document, then has nothing to do",
          "[daemon][grep][trigram][indexer]") {
    IndexerFixture f;
    const auto a = f.add("a.txt", "if (admitDeferrable(work)) run();");
    const auto b = f.add("b.txt", "ResourceGovernor admits deferrable work");
    GrepTrigramIndexer indexer(f.repo, f.options());
    REQUIRE(indexer.available());

    auto first = indexer.runPass();
    CHECK(first.indexed == 2);
    CHECK_FALSE(first.morePending);
    CHECK(f.matching("admitDeferrable") == std::vector<int64_t>{a});
    CHECK(f.matching("Governor") == std::vector<int64_t>{b});

    auto second = indexer.runPass();
    CHECK(second.indexed == 0);
    CHECK(f.repo->grepTrigramCoverage().value().size() == 2);
}

TEST_CASE("GrepTrigramIndexer works in batches, smallest documents first",
          "[daemon][grep][trigram][indexer]") {
    IndexerFixture f;
    const auto big = f.add("big.txt", std::string(500, 'x') + " needleBig");
    const auto small = f.add("small.txt", "needleSmall");
    auto options = f.options();
    options.batchSize = 1;
    GrepTrigramIndexer indexer(f.repo, options);

    auto first = indexer.runPass();
    CHECK(first.indexed == 1);
    CHECK(first.morePending);
    CHECK(f.matching("needleSmall") == std::vector<int64_t>{small});
    CHECK(f.matching("needleBig").empty());

    auto second = indexer.runPass();
    CHECK(second.indexed == 1);
    CHECK(f.matching("needleBig") == std::vector<int64_t>{big});
}

TEST_CASE("GrepTrigramIndexer stops a pass when admission is refused or a stop is requested",
          "[daemon][grep][trigram][indexer]") {
    IndexerFixture f;
    f.add("a.txt", "first document");
    f.add("b.txt", "second document");
    auto options = f.options();
    int admitted = 0;
    options.admit = [&admitted] { return admitted++ < 1; };
    GrepTrigramIndexer indexer(f.repo, options);
    auto pass = indexer.runPass();
    CHECK(pass.indexed == 1);
    CHECK(pass.interrupted);

    auto stopping = f.options();
    stopping.stopRequested = [] { return true; };
    GrepTrigramIndexer stopped(f.repo, stopping);
    auto none = stopped.runPass();
    CHECK(none.indexed == 0);
    CHECK(none.interrupted);
}

TEST_CASE("GrepTrigramIndexer skips a document it cannot read instead of retrying it forever",
          "[daemon][grep][trigram][indexer]") {
    IndexerFixture f;
    const auto missing = f.add("missing.txt", "never readable");
    f.blobs.erase("hash-missing.txt");
    f.add("ok.txt", "readable text");
    GrepTrigramIndexer indexer(f.repo, f.options());

    auto first = indexer.runPass();
    CHECK(first.indexed == 1);
    CHECK(first.failed == 1);
    const auto fetchesAfterFirst = f.fetches;
    auto second = indexer.runPass();
    CHECK(second.indexed == 0);
    CHECK(second.failed == 0);
    CHECK(f.fetches == fetchesAfterFirst);
    // It stays uncovered, so grep keeps scanning it.
    CHECK_FALSE(f.repo->grepTrigramCoverage().value().contains(missing));
}

TEST_CASE("GrepTrigramIndexer leaves out documents grep does not scan",
          "[daemon][grep][trigram][indexer]") {
    IndexerFixture f;
    f.add("photo.png", "PNG pixels", "image/png");
    f.add("archive.zip", "PK zip", "application/zip");
    f.add("huge.log", "huge contents", "text/plain", 200LL * 1024 * 1024);
    const auto text = f.add("notes.txt", "plain notes");
    GrepTrigramIndexer indexer(f.repo, f.options());
    auto pass = indexer.runPass();
    CHECK(pass.indexed == 1);
    const auto coverage = f.repo->grepTrigramCoverage().value();
    CHECK(coverage.size() == 1);
    CHECK(coverage.contains(text));
}
