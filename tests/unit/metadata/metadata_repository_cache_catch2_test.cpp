// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

// Smoke test for path cache snapshot behavior via findDocumentByExactPath

#include <chrono>
#include <filesystem>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace std::chrono;
using namespace yams::metadata;

namespace {

std::filesystem::path tempDbPath(const char* prefix) {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string(prefix) + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

DocumentInfo mk(const std::string& path, const std::string& hash) {
    DocumentInfo d;
    d.filePath = path;
    auto derived = computePathDerivedValues(path);
    d.fileName = std::filesystem::path(derived.normalizedPath).filename().string();
    d.fileExtension = std::filesystem::path(d.fileName).extension().string();
    d.sha256Hash = hash;
    d.pathPrefix = derived.pathPrefix;
    d.reversePath = derived.reversePath;
    d.pathHash = derived.pathHash;
    d.parentHash = derived.parentHash;
    d.pathDepth = derived.pathDepth;
    auto now = floor<seconds>(system_clock::now());
    d.createdTime = d.modifiedTime = d.indexedTime = now;
    return d;
}

} // namespace

TEST_CASE("MetadataRepositoryCache: repeated exact path hits cache", "[unit][metadata][cache]") {
    auto dbPath = tempDbPath("repo_cache_catch2_");
    ConnectionPool pool(dbPath.string());
    REQUIRE(pool.initialize().has_value());
    MetadataRepository repo(pool);

    auto id = repo.insertDocument(mk("/cache/hit.md", "H1"));
    REQUIRE(id.has_value());

    // First call should populate cache
    auto first = repo.findDocumentByExactPath("/cache/hit.md");
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());
    auto d1 = first.value().value();
    CHECK(d1.filePath == "/cache/hit.md");

    // Second call should return same doc (served from cache path in repo)
    auto second = repo.findDocumentByExactPath("/cache/hit.md");
    REQUIRE(second.has_value());
    REQUIRE(second.value().has_value());
    auto d2 = second.value().value();
    CHECK(d2.filePath == d1.filePath);
    CHECK(d2.sha256Hash == d1.sha256Hash);

    // Cleanup
    pool.shutdown();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}
