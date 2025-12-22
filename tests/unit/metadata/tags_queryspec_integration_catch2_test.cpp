// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 YAMS Contributors

#include <chrono>
#include <cstdlib>
#include <filesystem>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

using namespace yams::metadata;

namespace {

std::filesystem::path tmpdb() {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string("tags_qs_catch2_") + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

DocumentInfo mkdoc(const std::string& path, const std::string& hash) {
    DocumentInfo d;
    d.filePath = path;
    auto dv = computePathDerivedValues(path);
    d.fileName = std::filesystem::path(dv.normalizedPath).filename().string();
    d.fileExtension = std::filesystem::path(d.fileName).extension().string();
    d.sha256Hash = hash;
    d.pathPrefix = dv.pathPrefix;
    d.reversePath = dv.reversePath;
    d.pathHash = dv.pathHash;
    d.parentHash = dv.parentHash;
    d.pathDepth = dv.pathDepth;
    auto now = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    d.createdTime = d.modifiedTime = d.indexedTime = now;
    return d;
}

} // namespace

TEST_CASE("TagsQuerySpec: match any and all", "[unit][metadata][tags]") {
    auto db = tmpdb();
    ConnectionPool pool(db.string());
    REQUIRE(pool.initialize());
    MetadataRepository repo(pool);

    auto id1 = repo.insertDocument(mkdoc("/t/a.md", "H1"));
    REQUIRE(id1);
    auto id2 = repo.insertDocument(mkdoc("/t/b.md", "H2"));
    REQUIRE(id2);

    // Store tags as metadata keys with tag: prefix
    REQUIRE(repo.setMetadata(id1.value(), "tag:alpha", MetadataValue("alpha")).has_value());
    REQUIRE(repo.setMetadata(id1.value(), "tag:beta", MetadataValue("beta")).has_value());
    REQUIRE(repo.setMetadata(id2.value(), "tag:alpha", MetadataValue("alpha")).has_value());

    SECTION("matchAny returns both documents") {
        auto res = repo.findDocumentsByTags({"alpha"}, /*matchAll=*/false);
        REQUIRE(res.has_value());
        CHECK(res.value().size() == 2);
    }

    SECTION("matchAll returns only document with both tags") {
        auto res = repo.findDocumentsByTags({"alpha", "beta"}, /*matchAll=*/true);
        REQUIRE(res.has_value());
        REQUIRE(res.value().size() == 1);
        CHECK(res.value().at(0).sha256Hash == "H1");
    }

    // Cleanup
    pool.shutdown();
    std::error_code ec;
    std::filesystem::remove(db, ec);
}
