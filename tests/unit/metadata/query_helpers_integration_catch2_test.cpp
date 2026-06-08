// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

// Integration exercise for queryDocumentsByPattern using a real MetadataRepository

#include <chrono>
#include <filesystem>
#include <set>

#include <catch2/catch_test_macros.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/metadata/query_helpers.h>

using namespace std::chrono;
using namespace yams::metadata;

namespace {

std::filesystem::path make_temp_db() {
    const char* t = std::getenv("YAMS_TEST_TMPDIR");
    auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
    std::error_code ec;
    std::filesystem::create_directories(base, ec);
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = base / (std::string("qh_int_catch2_") + std::to_string(ts) + ".db");
    std::filesystem::remove(p, ec);
    return p;
}

DocumentInfo make_doc(const std::string& path, const std::string& hash,
                      const std::string& mime = "text/plain", int64_t fileSize = 0) {
    DocumentInfo d;
    d.filePath = path;
    auto derived = computePathDerivedValues(path);
    d.fileName = std::filesystem::path(derived.normalizedPath).filename().string();
    d.fileExtension = std::filesystem::path(d.fileName).extension().string();
    d.fileSize = fileSize;
    d.sha256Hash = hash;
    d.mimeType = mime;
    d.pathPrefix = derived.pathPrefix;
    d.reversePath = derived.reversePath;
    d.pathHash = derived.pathHash;
    d.parentHash = derived.parentHash;
    d.pathDepth = derived.pathDepth;
    auto now = floor<seconds>(system_clock::now());
    d.createdTime = now;
    d.modifiedTime = now;
    d.indexedTime = now;
    d.contentExtracted = false;
    d.extractionStatus = ExtractionStatus::Pending;
    return d;
}

template <typename Projection>
std::set<std::string> collectPaths(const std::vector<Projection>& docs) {
    std::set<std::string> paths;
    for (const auto& doc : docs) {
        paths.insert(doc.filePath);
    }
    return paths;
}

} // namespace

TEST_CASE("QueryHelpersIntegration: query documents by pattern basic",
          "[unit][metadata][query_helpers]") {
    auto dbPath = make_temp_db();
    ConnectionPoolConfig cfg;
    cfg.enableWAL = false;
    cfg.minConnections = 1;
    cfg.maxConnections = 2;
    ConnectionPool pool(dbPath.string(), cfg);
    REQUIRE((pool.initialize().has_value()));
    MetadataRepository repo(pool);

    // Seed representative text and binary docs.
    auto id1 = repo.insertDocument(make_doc("/notes/todo.md", "hashA", "text/plain", 11));
    REQUIRE((id1.has_value()));
    auto id2 = repo.insertDocument(make_doc("/notes/ideas.txt", "hashB", "text/plain", 22));
    REQUIRE((id2.has_value()));
    auto id3 = repo.insertDocument(make_doc("/projects/yams/README.md", "hashC", "text/plain", 33));
    REQUIRE((id3.has_value()));
    auto id4 = repo.insertDocument(make_doc("/notes/todo1.md", "hashD", "text/plain", 44));
    REQUIRE((id4.has_value()));
    auto id5 = repo.insertDocument(make_doc("/images/logo.png", "hashE", "image/png", 55));
    REQUIRE((id5.has_value()));
    SECTION("Pattern: wildcard all documents") {
        auto allDocs = queryDocumentsByPattern(repo, "%");
        REQUIRE((allDocs.has_value()));
        CHECK((allDocs.value().size() == 5));
    }

    SECTION("Pattern: directory prefix") {
        auto r1 = queryDocumentsByPattern(repo, "/notes/%");
        REQUIRE((r1.has_value()));
        CHECK((r1.value().size() == 3));
    }

    SECTION("Pattern: directory prefix limit") {
        auto limited = queryDocumentsByPattern(repo, "/notes/%", 2);
        REQUIRE((limited.has_value()));
        CHECK((limited.value().size() == 2));
    }

    SECTION("Pattern: extension") {
        auto r2 = queryDocumentsByPattern(repo, "%.md");
        REQUIRE((r2.has_value()));
        CHECK((r2.value().size() == 3));
    }

    SECTION("Pattern: contains fragment by name") {
        auto r3 = queryDocumentsByPattern(repo, "%/README.md");
        REQUIRE((r3.has_value()));
        REQUIRE((r3.value().size() == 1));
        CHECK((r3.value().at(0).filePath == "/projects/yams/README.md"));
    }

    SECTION("Glob: empty pattern list matches all documents") {
        auto results = queryDocumentsByGlobPatterns(repo, {});
        REQUIRE((results.has_value()));
        CHECK((results.value().size() == 5));
    }

    SECTION("Glob: recursive markdown") {
        auto results = queryDocumentsByGlobPatterns(repo, {"**/*.md"});
        REQUIRE((results.has_value()));
        CHECK((collectPaths(results.value()) == std::set<std::string>{"/notes/todo.md",
                                                                      "/notes/todo1.md",
                                                                      "/projects/yams/README.md"}));
    }

    SECTION("Glob: question mark wildcard matches a single character") {
        auto results = queryDocumentsByGlobPatterns(repo, {"/notes/todo?.md"});
        REQUIRE((results.has_value()));
        CHECK((collectPaths(results.value()) == std::set<std::string>{"/notes/todo1.md"}));
    }

    SECTION("Glob: multiple patterns dedupe overlaps") {
        auto results = queryDocumentsByGlobPatterns(repo, {"*.md", "/notes/*"});
        REQUIRE((results.has_value()));
        CHECK((collectPaths(results.value()) ==
               std::set<std::string>{"/notes/todo.md", "/notes/todo1.md", "/notes/ideas.txt",
                                     "/projects/yams/README.md"}));
    }

    SECTION("Grep candidates exclude binary mime types") {
        auto results = queryGrepCandidatesByPattern(repo, "%");
        REQUIRE((results.has_value()));
        CHECK((results.value().size() == 4));
        CHECK((collectPaths(results.value()) ==
               std::set<std::string>{"/notes/todo.md", "/notes/todo1.md", "/notes/ideas.txt",
                                     "/projects/yams/README.md"}));
    }

    SECTION("Grep candidates honor question mark globs") {
        auto results = queryGrepCandidatesByGlobPatterns(repo, {"/notes/todo?.md"});
        REQUIRE((results.has_value()));
        CHECK((collectPaths(results.value()) == std::set<std::string>{"/notes/todo1.md"}));
    }

    // Cleanup
    pool.shutdown();
    std::error_code ec;
    std::filesystem::remove(dbPath, ec);
}
