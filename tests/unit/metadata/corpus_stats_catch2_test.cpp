// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

/**
 * @file corpus_stats_catch2_test.cpp
 * @brief Unit tests for CorpusStats struct and getCorpusStats() implementation
 *
 * Tests cover:
 * - CorpusStats struct helper methods (classification, feature detection)
 * - JSON serialization/deserialization roundtrip
 * - getCorpusStats() database queries with various corpus compositions
 * - Edge cases (empty corpus, all code, all prose, mixed)
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/path_utils.h>
#include <yams/search/search_tuner.h>
#include <yams/storage/corpus_stats.h>

#include <chrono>
#include <filesystem>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::metadata;
using namespace yams::storage;
using Catch::Approx;

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

DocumentInfo makeDocument(const std::string& path, const std::string& hash, int64_t size = 1024,
                          const std::string& mime = "text/plain") {
    DocumentInfo info;
    info.filePath = path;
    info.fileName = std::filesystem::path(path).filename().string();
    info.fileExtension = std::filesystem::path(path).extension().string();
    info.fileSize = size;
    info.sha256Hash = hash;
    info.mimeType = mime;
    info.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    info.modifiedTime = info.createdTime;
    info.indexedTime = info.createdTime;
    auto derived = computePathDerivedValues(path);
    info.filePath = std::move(derived.normalizedPath);
    info.pathPrefix = std::move(derived.pathPrefix);
    info.reversePath = std::move(derived.reversePath);
    info.pathHash = std::move(derived.pathHash);
    info.parentHash = std::move(derived.parentHash);
    info.pathDepth = derived.pathDepth;
    info.contentExtracted = true;
    info.extractionStatus = ExtractionStatus::Success;
    return info;
}

struct CorpusStatsFixture {
    CorpusStatsFixture() {
        dbPath_ = tempDbPath("corpus_stats_test_");

        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~CorpusStatsFixture() {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::error_code ec;
        std::filesystem::remove(dbPath_, ec);
    }

    // Helper to insert a document
    int64_t insertDocument(const std::string& path, const std::string& hash, int64_t size = 1024,
                           const std::string& mime = "text/plain") {
        auto doc = makeDocument(path, hash, size, mime);
        auto result = repository_->insertDocument(doc);
        REQUIRE(result.has_value());
        return result.value();
    }

    // Helper to add a tag to a document
    void addTag(int64_t docId, const std::string& tag) {
        auto result = repository_->setMetadata(docId, "tag:" + tag, MetadataValue("true"));
        REQUIRE(result.has_value());
    }

    // Helper to insert a kg_doc_entities row directly (for extractor-split tests).
    void addDocEntity(int64_t docId, const std::string& entityText, const std::string& extractor) {
        auto result = pool_->withConnection([&](auto& db) -> yams::Result<void> {
            auto stmtR = db.prepare("INSERT INTO kg_doc_entities "
                                    "(document_id, entity_text, node_id, start_offset, end_offset, "
                                    "confidence, extractor) VALUES (?, ?, NULL, 0, 0, 1.0, ?)");
            REQUIRE(stmtR.has_value());
            auto& stmt = stmtR.value();
            REQUIRE(stmt.bindAll(docId, entityText, extractor).has_value());
            REQUIRE(stmt.step().has_value());
            return {};
        });
        REQUIRE(result.has_value());
    }

    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

} // namespace

// =============================================================================
// CorpusStats Struct Tests (no database)
// =============================================================================

TEST_CASE("CorpusStats: default values", "[unit][corpus_stats]") {
    CorpusStats stats;

    CHECK(stats.docCount == 0);
    CHECK(stats.totalSizeBytes == 0);
    CHECK(stats.avgDocLengthBytes == 0.0);
    CHECK(stats.codeRatio == 0.0);
    CHECK(stats.proseRatio == 0.0);
    CHECK(stats.binaryRatio == 0.0);
    CHECK(stats.isEmpty());
}

TEST_CASE("CorpusStats: classification helpers", "[unit][corpus_stats]") {
    SECTION("code dominant corpus") {
        CorpusStats stats;
        stats.docCount = 100;
        stats.codeRatio = 0.8;
        stats.proseRatio = 0.15;
        stats.binaryRatio = 0.05;

        CHECK(stats.isCodeDominant());
        CHECK_FALSE(stats.isProseDominant());
        CHECK_FALSE(stats.isMixed());
    }

    SECTION("prose dominant corpus") {
        CorpusStats stats;
        stats.docCount = 100;
        stats.codeRatio = 0.1;
        stats.proseRatio = 0.85;
        stats.binaryRatio = 0.05;

        CHECK_FALSE(stats.isCodeDominant());
        CHECK(stats.isProseDominant());
        CHECK_FALSE(stats.isMixed());
    }

    SECTION("mixed corpus") {
        CorpusStats stats;
        stats.docCount = 100;
        stats.codeRatio = 0.4;
        stats.proseRatio = 0.4;
        stats.binaryRatio = 0.2;

        CHECK_FALSE(stats.isCodeDominant());
        CHECK_FALSE(stats.isProseDominant());
        CHECK(stats.isMixed());
    }

    SECTION("scientific corpus detection - flat relative depth") {
        CorpusStats stats;
        stats.docCount = 150;
        stats.proseRatio = 0.95;
        // Corpus stored at deep absolute path (e.g. /Users/x/papers/), but internally flat.
        stats.pathDepthAvg = 10.0;        // absolute depth is high
        stats.pathRelativeDepthAvg = 0.3; // but relative nesting is near-zero
        stats.tagCoverage = 0.02;
        stats.nativeSymbolDensity = 0.0; // no code symbols (treesitter)
        stats.nerEntityDensity = 1.5;    // high GLiNER NER — expected for biomedical prose
        stats.symbolDensity = 1.5;       // total = NER (hasKnowledgeGraph uses this)

        CHECK(stats.isScientific()); // passes via flatPaths on relative depth
    }

    SECTION("scientific corpus detection - low code structure") {
        CorpusStats stats;
        stats.docCount = 200;
        stats.proseRatio = 0.90;
        stats.pathRelativeDepthAvg = 2.0; // not flat — needs the lowCodeStructure path
        stats.tagCoverage = 0.05;
        stats.nativeSymbolDensity = 0.0; // no code symbols

        CHECK(stats.isScientific()); // passes via lowCodeStructure (no native symbols)
    }

    SECTION("scientific corpus blocked by high nativeSymbolDensity") {
        CorpusStats stats;
        stats.docCount = 100;
        stats.proseRatio = 0.75;
        stats.pathRelativeDepthAvg = 3.0; // not flat
        stats.tagCoverage = 0.03;
        stats.nativeSymbolDensity = 2.5; // real treesitter code symbols present

        CHECK_FALSE(stats.isScientific());
    }

    SECTION("absolute pathDepthAvg alone no longer determines scientific") {
        CorpusStats stats;
        stats.docCount = 150;
        stats.proseRatio = 0.95;
        // Old behaviour: pathDepthAvg = 10 would cause isScientific() = false.
        // New behaviour: pathRelativeDepthAvg = 0 → isScientific() = true.
        stats.pathDepthAvg = 10.0;
        stats.pathRelativeDepthAvg = 0.0;
        stats.tagCoverage = 0.01;
        stats.nativeSymbolDensity = 0.0;

        CHECK(stats.isScientific());
    }
}

TEST_CASE("CorpusStats: size classification", "[unit][corpus_stats]") {
    SECTION("minimal corpus") {
        CorpusStats stats;
        stats.docCount = 50;
        CHECK(stats.isMinimal());
        CHECK(stats.isSmall());
        CHECK_FALSE(stats.isLarge());
    }

    SECTION("small corpus") {
        CorpusStats stats;
        stats.docCount = 500;
        CHECK_FALSE(stats.isMinimal());
        CHECK(stats.isSmall());
        CHECK_FALSE(stats.isLarge());
    }

    SECTION("medium corpus") {
        CorpusStats stats;
        stats.docCount = 5000;
        CHECK_FALSE(stats.isMinimal());
        CHECK_FALSE(stats.isSmall());
        CHECK_FALSE(stats.isLarge());
    }

    SECTION("large corpus") {
        CorpusStats stats;
        stats.docCount = 15000;
        CHECK_FALSE(stats.isMinimal());
        CHECK_FALSE(stats.isSmall());
        CHECK(stats.isLarge());
    }
}

TEST_CASE("CorpusStats: feature availability helpers", "[unit][corpus_stats]") {
    CorpusStats stats;
    stats.docCount = 100;

    SECTION("has knowledge graph") {
        stats.symbolDensity = 0.05;
        CHECK_FALSE(stats.hasKnowledgeGraph());

        stats.symbolDensity = 0.2;
        CHECK(stats.hasKnowledgeGraph());
    }

    SECTION("has paths") {
        stats.pathDepthAvg = 0.5;
        CHECK_FALSE(stats.hasPaths());

        stats.pathDepthAvg = 2.5;
        CHECK(stats.hasPaths());
    }

    SECTION("has tags") {
        stats.tagCoverage = 0.05;
        CHECK_FALSE(stats.hasTags());

        stats.tagCoverage = 0.3;
        CHECK(stats.hasTags());
    }

    SECTION("has embeddings") {
        stats.embeddingCoverage = 0.3;
        CHECK_FALSE(stats.hasEmbeddings());

        stats.embeddingCoverage = 0.7;
        CHECK(stats.hasEmbeddings());
    }
}

TEST_CASE("CorpusStats: expiration check", "[unit][corpus_stats]") {
    CorpusStats stats;
    stats.computedAtMs = 1000;
    stats.ttlMs = 60000; // 60 seconds

    CHECK_FALSE(stats.isExpired(30000)); // 29 seconds later - not expired
    CHECK_FALSE(stats.isExpired(61000)); // exactly at TTL - not expired
    CHECK(stats.isExpired(61001));       // just past TTL - expired
    CHECK(stats.isExpired(200000));      // way past TTL - expired
}

TEST_CASE("CorpusStats: JSON roundtrip", "[unit][corpus_stats]") {
    CorpusStats original;
    original.docCount = 1234;
    original.totalSizeBytes = 5678900;
    original.avgDocLengthBytes = 4601.5;
    original.codeRatio = 0.65;
    original.proseRatio = 0.25;
    original.binaryRatio = 0.10;
    original.embeddingCount = 1000;
    original.embeddingCoverage = 0.81;
    original.tagCount = 500;
    original.docsWithTags = 400;
    original.tagCoverage = 0.32;
    original.symbolCount = 5000;
    original.symbolDensity = 4.05;
    original.nativeSymbolCount = 2000;
    original.nativeSymbolDensity = 1.62;
    original.nerEntityCount = 3000;
    original.nerEntityDensity = 2.43;
    original.pathDepthAvg = 3.5;
    original.pathDepthMax = 8.0;
    original.pathRelativeDepthAvg = 1.2;
    original.computedAtMs = 1704067200000;
    original.usedOnlineOverlay = true;
    original.reconciledComputedAtMs = 1704067100000;
    original.pathDepthMaxApproximate = true;
    original.extensionCounts[".cpp"] = 300;
    original.extensionCounts[".py"] = 200;
    original.extensionCounts[".md"] = 150;

    // Serialize to JSON
    auto json = original.toJson();

    // Verify JSON structure
    CHECK(json["doc_count"] == 1234);
    CHECK(json["total_size_bytes"] == 5678900);
    CHECK(json["code_ratio"].get<double>() == Approx(0.65));
    CHECK(json["classification"]["is_mixed"].get<bool>() == true);
    CHECK(json["top_extensions"].contains(".cpp"));

    // Deserialize back
    auto restored = CorpusStats::fromJson(json);

    // Verify roundtrip
    CHECK(restored.docCount == original.docCount);
    CHECK(restored.totalSizeBytes == original.totalSizeBytes);
    CHECK(restored.avgDocLengthBytes == Approx(original.avgDocLengthBytes));
    CHECK(restored.codeRatio == Approx(original.codeRatio));
    CHECK(restored.proseRatio == Approx(original.proseRatio));
    CHECK(restored.binaryRatio == Approx(original.binaryRatio));
    CHECK(restored.embeddingCount == original.embeddingCount);
    CHECK(restored.embeddingCoverage == Approx(original.embeddingCoverage));
    CHECK(restored.tagCount == original.tagCount);
    CHECK(restored.docsWithTags == original.docsWithTags);
    CHECK(restored.tagCoverage == Approx(original.tagCoverage));
    CHECK(restored.symbolCount == original.symbolCount);
    CHECK(restored.symbolDensity == Approx(original.symbolDensity));
    CHECK(restored.nativeSymbolCount == original.nativeSymbolCount);
    CHECK(restored.nativeSymbolDensity == Approx(original.nativeSymbolDensity));
    CHECK(restored.nerEntityCount == original.nerEntityCount);
    CHECK(restored.nerEntityDensity == Approx(original.nerEntityDensity));
    CHECK(restored.pathDepthAvg == Approx(original.pathDepthAvg));
    CHECK(restored.pathDepthMax == Approx(original.pathDepthMax));
    CHECK(restored.pathRelativeDepthAvg == Approx(original.pathRelativeDepthAvg));
    CHECK(restored.computedAtMs == original.computedAtMs);
    CHECK(restored.usedOnlineOverlay == original.usedOnlineOverlay);
    CHECK(restored.reconciledComputedAtMs == original.reconciledComputedAtMs);
    CHECK(restored.pathDepthMaxApproximate == original.pathDepthMaxApproximate);
    CHECK(restored.extensionCounts[".cpp"] == 300);
    CHECK(restored.extensionCounts[".py"] == 200);
}

TEST_CASE("CorpusStats: fromJson handles missing fields gracefully", "[unit][corpus_stats]") {
    nlohmann::json partial;
    partial["doc_count"] = 100;
    partial["code_ratio"] = 0.5;
    // Missing most fields

    auto stats = CorpusStats::fromJson(partial);

    CHECK(stats.docCount == 100);
    CHECK(stats.codeRatio == Approx(0.5));
    CHECK(stats.proseRatio == 0.0);   // Default
    CHECK(stats.totalSizeBytes == 0); // Default
}

// =============================================================================
// getCorpusStats() Database Integration Tests
// =============================================================================

TEST_CASE("getCorpusStats: empty corpus", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.docCount == 0);
    CHECK(stats.isEmpty());
    CHECK(stats.totalSizeBytes == 0);
    CHECK(stats.codeRatio == 0.0);
    CHECK(stats.proseRatio == 0.0);
}

TEST_CASE("getCorpusStats: code-dominant corpus", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Insert mostly code files
    fix.insertDocument("/src/main.cpp", "hash1", 2000);
    fix.insertDocument("/src/utils.cpp", "hash2", 1500);
    fix.insertDocument("/src/parser.cpp", "hash3", 3000);
    fix.insertDocument("/include/types.h", "hash4", 500);
    fix.insertDocument("/src/main.py", "hash5", 1000);
    fix.insertDocument("/README.md", "hash6", 800); // One prose file

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.docCount == 6);
    CHECK(stats.totalSizeBytes == 8800);
    CHECK(stats.avgDocLengthBytes == Approx(8800.0 / 6.0));

    // 5/6 files are code (.cpp, .h, .py)
    CHECK(stats.codeRatio == Approx(5.0 / 6.0).margin(0.01));
    // 1/6 files are prose (.md)
    CHECK(stats.proseRatio == Approx(1.0 / 6.0).margin(0.01));

    CHECK(stats.isCodeDominant());
    CHECK_FALSE(stats.isProseDominant());
}

TEST_CASE("getCorpusStats: prose-dominant corpus", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Insert mostly prose files
    fix.insertDocument("/docs/guide.md", "hash1", 5000);
    fix.insertDocument("/docs/api.md", "hash2", 3000);
    fix.insertDocument("/docs/faq.txt", "hash3", 2000);
    fix.insertDocument("/notes/ideas.rst", "hash4", 1500);
    fix.insertDocument("/src/main.cpp", "hash5", 1000); // One code file

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.docCount == 5);

    // 4/5 files are prose
    CHECK(stats.proseRatio == Approx(4.0 / 5.0).margin(0.01));
    // 1/5 files are code
    CHECK(stats.codeRatio == Approx(1.0 / 5.0).margin(0.01));

    CHECK_FALSE(stats.isCodeDominant());
    CHECK(stats.isProseDominant());
}

TEST_CASE("getCorpusStats: mixed corpus", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Insert balanced mix
    fix.insertDocument("/src/main.cpp", "hash1", 2000);
    fix.insertDocument("/src/utils.py", "hash2", 1500);
    fix.insertDocument("/docs/guide.md", "hash3", 3000);
    fix.insertDocument("/docs/api.txt", "hash4", 2500);
    fix.insertDocument("/assets/logo.png", "hash5", 5000, "image/png");
    fix.insertDocument("/assets/icon.jpg", "hash6", 3000, "image/jpeg");

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.docCount == 6);

    // 2/6 code, 2/6 prose, 2/6 binary - all < 0.7
    CHECK(stats.codeRatio == Approx(2.0 / 6.0).margin(0.01));
    CHECK(stats.proseRatio == Approx(2.0 / 6.0).margin(0.01));
    CHECK(stats.binaryRatio == Approx(2.0 / 6.0).margin(0.01));

    CHECK(stats.isMixed());
}

TEST_CASE("getCorpusStats: path depth calculation", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Various path depths (note: depth includes root segment)
    // "/file.txt" -> ["", "file.txt"] = depth 2
    // "/src/main.cpp" -> ["", "src", "main.cpp"] = depth 3
    // "/src/utils/helpers.cpp" -> ["", "src", "utils", "helpers.cpp"] = depth 4
    // "/src/utils/internal/impl.cpp" -> ["", "src", "utils", "internal", "impl.cpp"] = depth 5
    fix.insertDocument("/file.txt", "hash1", 100);
    fix.insertDocument("/src/main.cpp", "hash2", 100);
    fix.insertDocument("/src/utils/helpers.cpp", "hash3", 100);
    fix.insertDocument("/src/utils/internal/impl.cpp", "hash4", 100);

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.docCount == 4);

    // Average: (2+3+4+5)/4 = 3.5
    CHECK(stats.pathDepthAvg == Approx(3.5).margin(0.1));
    CHECK(stats.pathDepthMax == Approx(5.0));
    CHECK(stats.hasPaths()); // pathDepthAvg > 1.0
}

TEST_CASE("getCorpusStats: extension distribution", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Multiple files with same extension
    fix.insertDocument("/a.cpp", "hash1", 100);
    fix.insertDocument("/b.cpp", "hash2", 100);
    fix.insertDocument("/c.cpp", "hash3", 100);
    fix.insertDocument("/d.py", "hash4", 100);
    fix.insertDocument("/e.py", "hash5", 100);
    fix.insertDocument("/f.md", "hash6", 100);

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.extensionCounts[".cpp"] == 3);
    CHECK(stats.extensionCounts[".py"] == 2);
    CHECK(stats.extensionCounts[".md"] == 1);
}

TEST_CASE("getCorpusStats: tag coverage", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Insert documents
    auto id1 = fix.insertDocument("/a.cpp", "hash1", 100);
    auto id2 = fix.insertDocument("/b.cpp", "hash2", 100);
    [[maybe_unused]] auto id3 = fix.insertDocument("/c.cpp", "hash3", 100);
    [[maybe_unused]] auto id4 = fix.insertDocument("/d.cpp", "hash4", 100);
    [[maybe_unused]] auto id5 = fix.insertDocument("/e.cpp", "hash5", 100);

    // Add tags to some documents (2/5 = 40% coverage)
    fix.addTag(id1, "important");
    fix.addTag(id1, "reviewed"); // Multiple tags on same doc
    fix.addTag(id2, "wip");

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    CHECK(stats.docCount == 5);
    CHECK(stats.tagCount == 3);     // Total tag assignments
    CHECK(stats.docsWithTags == 2); // Documents with at least one tag
    CHECK(stats.tagCoverage == Approx(2.0 / 5.0).margin(0.01));
    CHECK(stats.hasTags()); // tagCoverage > 0.1
}

TEST_CASE("getCorpusStats: scientific corpus pattern", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // Scientific corpus: mostly prose, shallow paths, no tags
    // All files at root level: "/file.txt" -> depth 2
    fix.insertDocument("/paper1.txt", "hash1", 10000);
    fix.insertDocument("/paper2.txt", "hash2", 12000);
    fix.insertDocument("/abstract.md", "hash3", 500);
    fix.insertDocument("/notes.rst", "hash4", 2000);
    fix.insertDocument("/data.txt", "hash5", 8000);

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // All prose, shallow paths (depth=2 for root-level files), no tags
    CHECK(stats.isProseDominant());
    CHECK(stats.pathDepthAvg == Approx(2.0).margin(0.1)); // Root-level files have depth 2
    // pathRelativeDepthAvg = AVG - MIN = 2 - 2 = 0 (all files at same depth)
    CHECK(stats.pathRelativeDepthAvg == Approx(0.0).margin(0.1));
    CHECK(stats.tagCoverage < 0.1);
    CHECK(stats.nativeSymbolDensity == 0.0); // no code symbols inserted
    // isScientific() uses pathRelativeDepthAvg < 1.5 (flat) and nativeSymbolDensity < 0.1
    // Both hold here, so the corpus is correctly classified as scientific.
    CHECK(stats.isScientific());
}

TEST_CASE("getCorpusStats: timestamp is set", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    fix.insertDocument("/test.cpp", "hash1", 100);

    auto beforeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto afterMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

    auto stats = result.value();
    CHECK(stats.computedAtMs >= beforeMs);
    CHECK(stats.computedAtMs <= afterMs);
}

// =============================================================================
// Extension Classification Tests
// =============================================================================

TEST_CASE("Extension classification sets", "[unit][corpus_stats]") {
    using namespace yams::storage::detail;

    SECTION("code extensions are recognized") {
        CHECK(kCodeExtensions.count(".cpp") == 1);
        CHECK(kCodeExtensions.count(".py") == 1);
        CHECK(kCodeExtensions.count(".rs") == 1);
        CHECK(kCodeExtensions.count(".go") == 1);
        CHECK(kCodeExtensions.count(".ts") == 1);
        CHECK(kCodeExtensions.count(".java") == 1);
    }

    SECTION("prose extensions are recognized") {
        CHECK(kProseExtensions.count(".md") == 1);
        CHECK(kProseExtensions.count(".txt") == 1);
        CHECK(kProseExtensions.count(".rst") == 1);
        CHECK(kProseExtensions.count(".pdf") == 1);
    }

    SECTION("binary extensions are recognized") {
        CHECK(kBinaryExtensions.count(".png") == 1);
        CHECK(kBinaryExtensions.count(".jpg") == 1);
        CHECK(kBinaryExtensions.count(".zip") == 1);
        CHECK(kBinaryExtensions.count(".exe") == 1);
        CHECK(kBinaryExtensions.count(".so") == 1);
    }

    SECTION("extensions are mutually exclusive") {
        // No extension should appear in multiple sets
        for (const auto& ext : kCodeExtensions) {
            CHECK(kProseExtensions.count(ext) == 0);
            CHECK(kBinaryExtensions.count(ext) == 0);
        }
        for (const auto& ext : kProseExtensions) {
            CHECK(kCodeExtensions.count(ext) == 0);
            CHECK(kBinaryExtensions.count(ext) == 0);
        }
    }
}

// =============================================================================
// Extractor-split entity count tests (nativeSymbolCount / nerEntityCount)
// =============================================================================

TEST_CASE("getCorpusStats: extractor-split entity counts", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    auto docId1 = fix.insertDocument("/paper1.txt", "hash1", 5000);
    auto docId2 = fix.insertDocument("/paper2.txt", "hash2", 6000);

    // Insert 3 GLiNER NER entities across the two documents
    fix.addDocEntity(docId1, "monocytes", "gliner_title_nl");
    fix.addDocEntity(docId1, "inflammatory disease", "gliner_title_nl");
    fix.addDocEntity(docId2, "cytokine", "gliner_title_nl");

    // Insert 2 treesitter symbols (simulating a code file also in the corpus)
    fix.addDocEntity(docId1, "parseResult", "symbol_extractor_v1");
    fix.addDocEntity(docId2, "TokenStream", "symbol_extractor_v1");

    fix.repository_->signalCorpusStatsStale();
    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // Total (all extractors)
    CHECK(stats.symbolCount == 5);
    CHECK(stats.symbolDensity == Approx(5.0 / 2.0).margin(0.01));

    // Treesitter symbols only
    CHECK(stats.nativeSymbolCount == 2);
    CHECK(stats.nativeSymbolDensity == Approx(2.0 / 2.0).margin(0.01));

    // GLiNER NER only
    CHECK(stats.nerEntityCount == 3);
    CHECK(stats.nerEntityDensity == Approx(3.0 / 2.0).margin(0.01));
}

TEST_CASE("getCorpusStats: pathRelativeDepthAvg correctness", "[unit][corpus_stats][integration]") {
    CorpusStatsFixture fix;

    // All files at the same absolute depth (depth=2 for "/file.txt" style paths)
    fix.insertDocument("/paper1.txt", "h1", 1000);
    fix.insertDocument("/paper2.txt", "h2", 1000);
    fix.insertDocument("/paper3.txt", "h3", 1000);

    auto result = fix.repository_->getCorpusStats();
    REQUIRE(result.has_value());

    // All files at same depth → relative depth = AVG - MIN = 0
    CHECK(result.value().pathRelativeDepthAvg == Approx(0.0).margin(0.01));

    // Add a nested file
    fix.insertDocument("/subdir/nested.txt", "h4", 1000);
    fix.repository_->signalCorpusStatsStale();

    auto result2 = fix.repository_->getCorpusStats();
    REQUIRE(result2.has_value());

    // Depths: 2, 2, 2, 3 → AVG=2.25, MIN=2 → relative=0.25
    CHECK(result2.value().pathRelativeDepthAvg == Approx(0.25).margin(0.05));
    // pathDepthAvg is still absolute (2.25)
    CHECK(result2.value().pathDepthAvg == Approx(2.25).margin(0.05));
}

// =============================================================================
// SearchTuner FSM tests using corrected CorpusStats
// =============================================================================

TEST_CASE("SearchTuner: biomedical prose corpus selects SCIENTIFIC profile",
          "[unit][corpus_stats][search_tuner]") {
    using namespace yams::search;

    // Simulate SciFact-like corpus: prose docs, deep absolute paths, zero code symbols,
    // high GLiNER NER density (1.5 annotations/doc).
    // docCount must be >= 1000 for SCIENTIFIC (computeState requires !isSmall).
    CorpusStats stats;
    stats.docCount = 1500;
    stats.proseRatio = 0.95;
    stats.codeRatio = 0.05;
    stats.binaryRatio = 0.0;
    stats.pathDepthAvg = 10.0;        // deep absolute path (e.g. /Users/x/.yams/papers/)
    stats.pathRelativeDepthAvg = 0.3; // but corpus is internally flat
    stats.tagCoverage = 0.02;
    stats.nativeSymbolDensity = 0.0; // no treesitter code symbols
    stats.nerEntityDensity = 1.5;    // GLiNER NER annotations (biomedical)
    stats.symbolDensity = 1.5;       // total = NER
    stats.symbolCount = 225;
    stats.embeddingCoverage = 0.95;

    SearchTuner tuner(stats);
    CHECK(tuner.currentState() == TuningState::SCIENTIFIC);

    // SCIENTIFIC profile should apply lower similarity threshold and sub-phrase rescoring
    auto config = tuner.getConfig();
    CHECK(config.similarityThreshold == Approx(0.30f).margin(0.01f));
    CHECK(config.enableSubPhraseRescoring == true);
    CHECK(config.rerankAnchoredMinRelativeScore == Approx(0.70f).margin(0.01f));
}

TEST_CASE("SearchTuner: code corpus still selects code profile after CorpusStats fix",
          "[unit][corpus_stats][search_tuner]") {
    using namespace yams::search;

    CorpusStats stats;
    stats.docCount = 500;
    stats.codeRatio = 0.85;
    stats.proseRatio = 0.15;
    stats.pathDepthAvg = 4.0;
    stats.pathRelativeDepthAvg = 2.0;
    stats.nativeSymbolDensity = 3.5; // many treesitter symbols
    stats.tagCoverage = 0.0;

    SearchTuner tuner(stats);
    CHECK(tuner.currentState() == TuningState::SMALL_CODE);
}

TEST_CASE("SearchTuner: tuningStateToString covers all states",
          "[unit][corpus_stats][search_tuner]") {
    using namespace yams::search;

    // Verify every enum value has a non-UNKNOWN string mapping
    for (auto state : {TuningState::SMALL_CODE, TuningState::LARGE_CODE, TuningState::SMALL_PROSE,
                       TuningState::LARGE_PROSE, TuningState::SCIENTIFIC, TuningState::MIXED,
                       TuningState::MIXED_PRECISION, TuningState::MINIMAL, TuningState::MEDIA}) {
        std::string name = tuningStateToString(state);
        CHECK(name != std::string("UNKNOWN"));
        CHECK(!name.empty());
    }
}
