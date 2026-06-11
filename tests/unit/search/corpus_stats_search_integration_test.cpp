// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright 2025 YAMS Contributors

/**
 * @file corpus_stats_search_integration_test.cpp
 * @brief Integration tests for CorpusStats and SearchEngine interaction
 *
 * Tests cover:
 * - Corpus stats collection from a populated search corpus
 * - Verifying stats reflect actual corpus composition
 * - Future: SearchTuner FSM parameter selection based on corpus stats
 *
 * These tests validate Phase 1 (Corpus Metrics Collection) of the
 * Adaptive Search Engine Tuning epic (yams-7ez4).
 */

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <yams/compat/unistd.h>

#if defined(_WIN32) && __has_include(<onnxruntime_c_api.h>)
#include <onnxruntime_c_api.h>
#define YAMS_ORT_API_VERSION ORT_API_VERSION
#else
#define YAMS_ORT_API_VERSION 0
#endif

#if defined(_WIN32) && YAMS_ORT_API_VERSION < 23

TEST_CASE("CorpusStats Search Integration - Windows skip",
          "[corpus_stats][search][windows][skip]") {
    SUCCEED("Skipping corpus stats search integration tests on Windows: ONNX Runtime API < 23.");
}

#else

#include <yams/api/content_store_builder.h>
#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_engine_builder.h>
#include <yams/search/search_tuner.h>
#include <yams/storage/corpus_stats.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>

#include <yams/app/services/services.hpp>

#include <spdlog/spdlog.h>

#include "common/fixture_manager.h"
#include "common/search_corpus_presets.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::search;
using namespace yams::api;
using Catch::Approx;

// Explicit alias to avoid ambiguity (yams::storage::Database vs yams::metadata::Database)
using CorpusStats = yams::storage::CorpusStats;

namespace fs = std::filesystem;

namespace {

/**
 * @brief Helper to run coroutines synchronously in tests
 */
template <typename T> yams::Result<T> runAwait(boost::asio::awaitable<yams::Result<T>> aw) {
    boost::asio::io_context ioc;

    auto wrapper =
        [aw = std::move(aw)]() mutable -> boost::asio::awaitable<std::optional<yams::Result<T>>> {
        try {
            auto v = co_await std::move(aw);
            co_return std::optional<yams::Result<T>>(std::move(v));
        } catch (...) {
            co_return std::optional<yams::Result<T>>{};
        }
    };

    auto fut = boost::asio::co_spawn(ioc, wrapper(), boost::asio::use_future);
    ioc.run();
    auto opt = fut.get();
    if (opt) {
        return std::move(*opt);
    }
    return yams::Result<T>(yams::Error{yams::ErrorCode::InternalError, "Awaitable failed"});
}

/**
 * @brief Fixture for corpus stats + search integration testing
 */
class CorpusStatsSearchFixture {
public:
    CorpusStatsSearchFixture() {
        setupTestEnvironment();
        setupDatabase();
        setupStorage();
        setupServices();
    }

    ~CorpusStatsSearchFixture() {
        cleanupServices();
        cleanupDatabase();
        cleanupTestEnvironment();
    }

    // Populate a code-dominant corpus
    void populateCodeCorpus() {
        // C++ source files
        createCodeDocument("src/main.cpp", R"(
#include <iostream>
#include "app.hpp"

int main(int argc, char** argv) {
    Application app;
    return app.run(argc, argv);
}
)",
                           {"cpp", "source"});

        createCodeDocument("src/parser.cpp", R"(
#include "parser.hpp"
#include <regex>

class Parser {
    Result<AST> parse(const std::string& input) {
        // Parse input into AST
        return tokenize(input).and_then(buildTree);
    }
};
)",
                           {"cpp", "source"});

        createCodeDocument("src/utils.cpp", R"(
#include "utils.hpp"
#include <algorithm>
#include <string>

std::string trim(const std::string& s) {
    auto start = s.find_first_not_of(" \t\n");
    auto end = s.find_last_not_of(" \t\n");
    return (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
}
)",
                           {"cpp", "source"});

        // Header files
        createCodeDocument("include/app.hpp", R"(
#pragma once

class Application {
public:
    int run(int argc, char** argv);
private:
    void initialize();
    void shutdown();
};
)",
                           {"cpp", "header"});

        createCodeDocument("include/parser.hpp", R"(
#pragma once
#include "result.hpp"

struct AST {};

class Parser {
public:
    Result<AST> parse(const std::string& input);
};
)",
                           {"cpp", "header"});

        // Python files
        createCodeDocument("scripts/build.py", R"(
#!/usr/bin/env python3
import subprocess
import sys

def build():
    subprocess.run(["cmake", "--build", "build"], check=True)

if __name__ == "__main__":
    build()
)",
                           {"python", "script"});

        // One documentation file
        createDocument("README.md", R"(
# Test Project

A sample C++ project for testing corpus stats.

## Building

```bash
mkdir build && cd build
cmake ..
make
```
)",
                       {"docs"});
    }

    // Populate a prose-dominant corpus
    void populateProseCorpus() {
        createDocument("guide.md", R"(
# User Guide

This guide explains how to use the application effectively.

## Getting Started

First, install the dependencies...
)",
                       {"docs", "guide"});

        createDocument("api.md", R"(
# API Reference

## Endpoints

### GET /users
Returns a list of users.

### POST /users
Creates a new user.
)",
                       {"docs", "api"});

        createDocument("faq.txt", R"(
Frequently Asked Questions

Q: How do I install?
A: Run the installer.

Q: What platforms are supported?
A: Windows, macOS, and Linux.
)",
                       {"docs", "faq"});

        createDocument("notes.rst", R"(
Development Notes
=================

Architecture Overview
---------------------

The system consists of three main components...
)",
                       {"docs", "notes"});

        // One code file
        createCodeDocument("config.py", R"(
# Configuration file
DEBUG = True
LOG_LEVEL = "INFO"
)",
                           {"python", "config"});
    }

    // Populate a mixed corpus
    void populateMixedCorpus() {
        // Code files
        createCodeDocument("app.ts", R"(
import express from 'express';

const app = express();
app.get('/', (req, res) => res.send('Hello'));
app.listen(3000);
)",
                           {"typescript", "server"});

        createCodeDocument("utils.go", R"(
package utils

func Max(a, b int) int {
    if a > b {
        return a
    }
    return b
}
)",
                           {"go", "utils"});

        // Prose files
        createDocument("changelog.md", R"(
# Changelog

## v1.0.0
- Initial release
- Added core features
)",
                       {"docs"});

        createDocument("license.txt", R"(
MIT License

Copyright (c) 2025
)",
                       {"legal"});
    }

    void populateStageTimingCorpus(std::size_t docCount) {
        static constexpr const char* kCodeTerms[] = {"parser",   "tokenizer", "scheduler",
                                                     "allocator", "mutex",    "buffer",
                                                     "hashmap",  "iterator",  "callback",
                                                     "socket"};
        static constexpr const char* kProseTerms[] = {
            "installation",    "configuration", "indexing",  "retrieval",   "permissions",
            "troubleshooting", "deployment",    "migration", "performance", "tutorial"};
        for (std::size_t i = 0; i < docCount; ++i) {
            const std::size_t idx = i / 2;
            if (i % 2 == 0) {
                const std::string a = kCodeTerms[idx % 10];
                const std::string b = kCodeTerms[(idx + 3) % 10];
                const std::string suffix = std::to_string(i);
                std::string path = "src/" + a + "/" + b + "_" + suffix + ".cpp";
                std::string content = "#include <memory>\n#include \"" + a +
                                      ".hpp\"\n\nclass " + a + "_" + suffix +
                                      " {\npublic:\n    Result<" + b + "> process_" + b +
                                      "(const std::string& input);\n    void reset_" + a +
                                      "();\nprivate:\n    std::shared_ptr<" + b + "> " + b +
                                      "_;\n    std::mutex " + a + "_mutex_;\n};\n\nResult<" + b +
                                      "> " + a + "_" + suffix + "::process_" + b +
                                      "(const std::string& input) {\n    auto handle = acquire_" +
                                      a + "(input);\n    return dispatch(handle, " + b +
                                      "_);\n}\n";
                createCodeDocument(path, content, {"cpp", "source", a});
            } else {
                const std::string a = kProseTerms[idx % 10];
                const std::string b = kProseTerms[(idx + 7) % 10];
                const std::string suffix = std::to_string(i);
                std::string path = "docs/" + a + "/" + b + "_" + suffix + ".md";
                std::string content =
                    "# " + a + " guide " + suffix + "\n\nThis section covers " + a +
                    " and how it interacts with " + b + ".\n\n## Overview\n\nThe " + a +
                    " workflow requires careful " + b +
                    " before rollout. Operators should review the " + a +
                    " checklist and validate " + b +
                    " settings.\n\n## Steps\n\n1. Prepare the environment for " + a +
                    ".\n2. Verify " + b + " prerequisites.\n3. Run the " + a +
                    " procedure and record results.\n";
                createDocument(path, content, {"docs", a});
            }
        }
    }

    // Access to repository for direct stats queries
    std::shared_ptr<MetadataRepository> metadataRepo() const { return metadataRepo_; }

    // Access to document service
    std::shared_ptr<IDocumentService> docService() const { return docService_; }

private:
    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ =
            fs::temp_directory_path() / ("corpus_stats_search_test_" + pid + "_" + timestamp);
        fs::create_directories(testDir_);

        fixtureManager_ = std::make_shared<yams::test::FixtureManager>(testDir_);
    }

    void setupDatabase() {
        dbPath_ = fs::absolute(testDir_ / "test.db");
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string(), ConnectionMode::Create);
        REQUIRE(openResult);

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        MigrationManager mm(*database_);
        auto initResult = mm.initialize();
        REQUIRE(initResult);

        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = mm.migrate();
        REQUIRE(migrateResult);
    }

    void setupStorage() {
        ContentStoreBuilder builder;
        auto storePath = testDir_ / "storage";
        auto storeResult = builder.withStoragePath(storePath)
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        REQUIRE(storeResult);

        auto& uniqueStore = storeResult.value();
        contentStore_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());
    }

    void setupServices() {
        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.workerExecutor = boost::asio::system_executor();

        docService_ = makeDocumentService(appContext_);
        REQUIRE(docService_);
    }

    std::string createDocument(const std::string& filename, const std::string& content,
                               const std::vector<std::string>& tags = {}) {
        REQUIRE(fixtureManager_);
        REQUIRE(docService_);

        auto fixture = fixtureManager_->createTextFixture(filename, content, tags);

        StoreDocumentRequest storeReq;
        storeReq.path = fixture.path.string();
        storeReq.tags = tags;
        auto storeResult = docService_->store(storeReq);
        REQUIRE(storeResult);

        // Index content for FTS5
        indexDocumentContent(storeResult.value().hash, filename, content);

        return storeResult.value().hash;
    }

    std::string createCodeDocument(const std::string& filename, const std::string& content,
                                   const std::vector<std::string>& tags = {}) {
        return createDocument(filename, content, tags);
    }

    void indexDocumentContent(const std::string& hash, const std::string& title,
                              const std::string& content) {
        auto docResult = metadataRepo_->getDocumentByHash(hash);
        if (!docResult || !docResult.value().has_value()) {
            return;
        }
        const auto& docInfo = docResult.value().value();
        auto indexResult = metadataRepo_->indexDocumentContent(
            docInfo.id, title, content, docInfo.mimeType.empty() ? "text/plain" : docInfo.mimeType);
        if (!indexResult) {
            spdlog::warn("Failed to index document content: {}", indexResult.error().message);
        }
    }

    void cleanupServices() { docService_.reset(); }

    void cleanupDatabase() {
        appContext_.metadataRepo.reset();
        appContext_.store.reset();
        appContext_.searchEngine.reset();
        contentStore_.reset();
        metadataRepo_.reset();
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        if (database_) {
            database_->close();
        }
        database_.reset();
    }

    void cleanupTestEnvironment() {
        fixtureManager_.reset();
        if (fs::exists(testDir_)) {
            fs::remove_all(testDir_);
        }
    }

    fs::path testDir_;
    fs::path dbPath_;
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::shared_ptr<IContentStore> contentStore_;
    AppContext appContext_;
    std::shared_ptr<IDocumentService> docService_;
    std::shared_ptr<yams::test::FixtureManager> fixtureManager_;
};

} // namespace

// =============================================================================
// Integration Tests: Corpus Stats from Real Document Store
// =============================================================================

TEST_CASE("CorpusStats: code-dominant corpus detection", "[corpus_stats][search][integration]") {
    CorpusStatsSearchFixture fix;
    fix.populateCodeCorpus();

    auto result = fix.metadataRepo()->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // Should have 7 documents
    CHECK(stats.docCount == 7);

    // 6/7 are code files (.cpp, .hpp, .py), 1/7 is prose (.md)
    CHECK(stats.codeRatio >= 0.8);
    CHECK(stats.proseRatio <= 0.2);

    // Classification
    CHECK(stats.isCodeDominant());
    CHECK_FALSE(stats.isProseDominant());
    CHECK_FALSE(stats.isMixed());
    CHECK_FALSE(stats.isScientific());

    // Size classification
    CHECK(stats.isMinimal()); // < 100 docs
    CHECK(stats.isSmall());   // < 1000 docs

    // Path structure (files in src/, include/, scripts/)
    CHECK(stats.pathDepthAvg >= 1.5);
    CHECK(stats.hasPaths());

    // Verify extension counts
    CHECK(stats.extensionCounts[".cpp"] == 3);
    CHECK(stats.extensionCounts[".hpp"] == 2);
    CHECK(stats.extensionCounts[".py"] == 1);
    CHECK(stats.extensionCounts[".md"] == 1);
}

TEST_CASE("CorpusStats: prose-dominant corpus detection", "[corpus_stats][search][integration]") {
    CorpusStatsSearchFixture fix;
    fix.populateProseCorpus();

    auto result = fix.metadataRepo()->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // Should have 5 documents
    CHECK(stats.docCount == 5);

    // 4/5 are prose files, 1/5 is code
    CHECK(stats.proseRatio >= 0.7);
    CHECK(stats.codeRatio <= 0.3);

    // Classification
    CHECK_FALSE(stats.isCodeDominant());
    CHECK(stats.isProseDominant());
    CHECK_FALSE(stats.isMixed());
}

TEST_CASE("CorpusStats: mixed corpus detection", "[corpus_stats][search][integration]") {
    CorpusStatsSearchFixture fix;
    fix.populateMixedCorpus();

    auto result = fix.metadataRepo()->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // Should have 4 documents
    CHECK(stats.docCount == 4);

    // 2/4 code, 2/4 prose - neither dominant
    CHECK(stats.codeRatio <= 0.7);
    CHECK(stats.proseRatio <= 0.7);

    // Classification
    CHECK_FALSE(stats.isCodeDominant());
    CHECK_FALSE(stats.isProseDominant());
    CHECK(stats.isMixed());
}

TEST_CASE("CorpusStats: tag coverage from real documents", "[corpus_stats][search][integration]") {
    CorpusStatsSearchFixture fix;
    fix.populateCodeCorpus();

    auto result = fix.metadataRepo()->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // Documents were created with tags
    // Note: Actual tag coverage depends on how StoreDocumentRequest.tags are stored
    // This test verifies the metric is computed, not specific values
    CHECK(stats.tagCount >= 0);
    CHECK(stats.tagCoverage >= 0.0);
    CHECK(stats.tagCoverage <= 1.0);
}

TEST_CASE("CorpusStats: JSON output for CLI/daemon", "[corpus_stats][search][integration]") {
    CorpusStatsSearchFixture fix;
    fix.populateCodeCorpus();

    auto result = fix.metadataRepo()->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();
    auto json = stats.toJson();

    // Verify all expected fields are present
    CHECK(json.contains("doc_count"));
    CHECK(json.contains("total_size_bytes"));
    CHECK(json.contains("avg_doc_length_bytes"));
    CHECK(json.contains("code_ratio"));
    CHECK(json.contains("prose_ratio"));
    CHECK(json.contains("binary_ratio"));
    CHECK(json.contains("embedding_count"));
    CHECK(json.contains("embedding_coverage"));
    CHECK(json.contains("tag_count"));
    CHECK(json.contains("tag_coverage"));
    CHECK(json.contains("symbol_count"));
    CHECK(json.contains("symbol_density"));
    CHECK(json.contains("path_depth_avg"));
    CHECK(json.contains("path_depth_max"));
    CHECK(json.contains("computed_at_ms"));
    CHECK(json.contains("top_extensions"));
    CHECK(json.contains("classification"));

    // Verify classification object
    auto& classif = json["classification"];
    CHECK(classif.contains("is_code_dominant"));
    CHECK(classif.contains("is_prose_dominant"));
    CHECK(classif.contains("is_mixed"));
    CHECK(classif.contains("is_scientific"));
    CHECK(classif.contains("is_minimal"));
    CHECK(classif.contains("is_small"));
    CHECK(classif.contains("is_large"));
    CHECK(classif.contains("has_kg"));
    CHECK(classif.contains("has_paths"));
    CHECK(classif.contains("has_tags"));
    CHECK(classif.contains("has_embeddings"));

    // Verify classification values match computed
    CHECK(classif["is_code_dominant"].get<bool>() == stats.isCodeDominant());
    CHECK(classif["is_small"].get<bool>() == stats.isSmall());
}

// =============================================================================
// Future: Search Tuner FSM Tests (Phase 2)
// =============================================================================

TEST_CASE("CorpusStats: ready for SearchTuner FSM", "[corpus_stats][search][integration]") {
    // This test documents the expected interface for Phase 2 (SearchTuner FSM)
    // The FSM will use corpus stats to select optimal search parameters

    CorpusStatsSearchFixture fix;
    fix.populateCodeCorpus();

    auto result = fix.metadataRepo()->getCorpusStats();
    REQUIRE(result.has_value());

    auto stats = result.value();

    // Phase 2 will use these classification methods:
    // - isCodeDominant() / isProseDominant() / isMixed() -> content type state
    // - isMinimal() / isSmall() / isLarge() -> size state
    // - hasKnowledgeGraph() -> enable/disable KG weight
    // - hasPaths() -> enable/disable path tree weight
    // - hasTags() -> enable/disable tag weight
    // - hasEmbeddings() -> enable/disable vector weight

    // For a code corpus, expected FSM state would be:
    // SMALL_CODE (doc_count < 1000, code_ratio > 0.7)
    // Parameters: k=20, text=0.50, vector=0.15, path=0.20, kg=0.10

    CHECK(stats.isSmall());
    CHECK(stats.isCodeDominant());

    // TODO (Phase 2): Implement SearchTuner and verify parameter selection
    // SearchTuner tuner(stats);
    // CHECK(tuner.currentState() == TuningState::SMALL_CODE);
    // auto config = tuner.getConfig();
    // CHECK(config.rrfK == 20);
    // CHECK(config.textWeight == Approx(0.50));
}

// =============================================================================
// Community Detection: Observability + Latency
// =============================================================================

namespace {

// Shared helper: build a SMALL_PROSE-tuned SearchEngine over a prose corpus.
// proseRatio=0.85, pathRelativeDepthAvg=3.0 (not flat) → isScientific()=false → SMALL_PROSE.
std::unique_ptr<SearchEngine> makeSmallProseEngine(std::shared_ptr<MetadataRepository> repo) {
    SearchEngineConfig config;
    config.textWeight = 1.0f;
    config.vectorWeight = 0.0f;
    config.pathTreeWeight = 0.0f;
    config.kgWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.tagWeight = 0.0f;
    config.metadataWeight = 0.0f;
    config.enableParallelExecution = false;

    auto engine = createSearchEngine(std::move(repo), nullptr, nullptr, nullptr, config);

    CorpusStats tunerStats;
    tunerStats.docCount = 500;
    tunerStats.codeRatio = 0.1f;
    tunerStats.proseRatio = 0.85f;
    tunerStats.pathRelativeDepthAvg = 3.0; // not flat → isScientific()=false
    tunerStats.tagCoverage = 0.3f;         // has tags → isScientific()=false
    engine->setSearchTuner(std::make_shared<SearchTuner>(tunerStats));

    return engine;
}

} // namespace

TEST_CASE("community detection: timing key always present when tuner is installed",
          "[community_detection][search]") {
    CorpusStatsSearchFixture fix;
    fix.populateProseCorpus();

    auto engine = makeSmallProseEngine(fix.metadataRepo());
    REQUIRE(engine != nullptr);

    auto result = engine->searchWithResponse("documents and files", {});
    REQUIRE(result.has_value());

    const auto& timing = result.value().componentTimingMicros;
    REQUIRE(timing.count("community_detection") > 0);
    // Vocabulary scan is O(tokens × vocab_size) — must complete well under 5ms.
    CHECK(timing.at("community_detection") < 5000);
}

TEST_CASE("community detection: scientific query sets community_override in debug stats",
          "[community_detection][search]") {
    CorpusStatsSearchFixture fix;
    fix.populateProseCorpus();

    auto engine = makeSmallProseEngine(fix.metadataRepo());
    REQUIRE(engine != nullptr);

    // 4 scientific vocab hits (protein, gene, disease, treatment) — threshold is 2.
    auto result = engine->searchWithResponse("protein gene disease treatment", {});
    REQUIRE(result.has_value());

    const auto& debug = result.value().debugStats;
    REQUIRE(debug.count("community_override") > 0);
    CHECK(debug.at("community_override") == "SCIENTIFIC");

    // Timing must still be present.
    const auto& timing = result.value().componentTimingMicros;
    REQUIRE(timing.count("community_detection") > 0);
    CHECK(timing.at("community_detection") < 5000);
}

TEST_CASE("community detection: neutral query does not set community_override",
          "[community_detection][search]") {
    CorpusStatsSearchFixture fix;
    fix.populateProseCorpus();

    auto engine = makeSmallProseEngine(fix.metadataRepo());
    REQUIRE(engine != nullptr);

    // No scientific or media vocabulary — override must not fire.
    auto result = engine->searchWithResponse("how to open a file", {});
    REQUIRE(result.has_value());

    const auto& debug = result.value().debugStats;
    CHECK(debug.count("community_override") == 0);

    // Timing key still present (detection always runs when tuner is set).
    const auto& timing = result.value().componentTimingMicros;
    REQUIRE(timing.count("community_detection") > 0);
}

TEST_CASE("search stage latency decomposition", "[.][stage-timing]") {
    CorpusStatsSearchFixture fix;
    constexpr std::size_t kDocCount = 300;
    fix.populateStageTimingCorpus(kDocCount);

    SearchEngineConfig config;
    config.textWeight = 0.50f;
    config.pathTreeWeight = 0.20f;
    config.tagWeight = 0.15f;
    config.metadataWeight = 0.05f;
    config.kgWeight = 0.0f;
    config.vectorWeight = 0.0f;
    config.entityVectorWeight = 0.0f;
    config.includeComponentTiming = true;
    config.enableParallelExecution = false;

    auto engine = createSearchEngine(fix.metadataRepo(), nullptr, nullptr, nullptr, config);
    REQUIRE(engine != nullptr);

    struct StageQuery {
        std::string text;
        std::vector<std::string> tags;
    };
    const std::vector<StageQuery> queries = {
        {"parser tokenizer", {}},
        {"mutex buffer handle", {}},
        {"process_buffer input", {}},
        {"scheduler callback dispatch", {}},
        {"hashmap iterator", {"cpp"}},
        {"socket handle acquire", {}},
        {"Result process input", {}},
        {"reset_parser", {}},
        {"shared_ptr allocator", {}},
        {"dispatch handle callback", {"parser"}},
        {"installation guide", {}},
        {"configuration checklist rollout", {}},
        {"indexing workflow overview", {}},
        {"retrieval performance settings", {}},
        {"troubleshooting deployment", {"docs"}},
        {"migration prerequisites", {}},
        {"permissions validate settings", {}},
        {"tutorial steps prepare environment", {}},
        {"performance review operators", {}},
        {"deployment procedure record results", {"deployment"}},
    };

    for (int warm = 0; warm < 3; ++warm) {
        auto r = engine->searchWithResponse("parser installation overview", {});
        REQUIRE(r.has_value());
    }

    std::map<std::string, std::vector<int64_t>> samples;
    std::vector<int64_t> wallMicros;
    for (const auto& q : queries) {
        SearchParams params;
        params.tags = q.tags;
        const auto t0 = std::chrono::steady_clock::now();
        auto result = engine->searchWithResponse(q.text, params);
        const auto t1 = std::chrono::steady_clock::now();
        REQUIRE(result.has_value());
        wallMicros.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
        for (const auto& [name, micros] : result.value().componentTimingMicros) {
            samples[name].push_back(micros);
        }
    }

    REQUIRE_FALSE(samples.empty());
    REQUIRE(samples.count("text") > 0);
    REQUIRE(samples.count("query_routing") > 0);

    auto mean = [](const std::vector<int64_t>& v) {
        return std::accumulate(v.begin(), v.end(), 0.0) / static_cast<double>(v.size());
    };
    auto p95 = [](std::vector<int64_t> v) {
        std::sort(v.begin(), v.end());
        return static_cast<double>(v[std::min(v.size() - 1, (v.size() * 95) / 100)]);
    };

    double meanSum = 0.0;
    for (const auto& [name, vals] : samples)
        meanSum += mean(vals);

    std::vector<std::pair<std::string, double>> rows;
    rows.reserve(samples.size());
    for (const auto& [name, vals] : samples)
        rows.emplace_back(name, mean(vals));
    std::sort(rows.begin(), rows.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    std::ostringstream out;
    out << "\n| component | n | mean (us) | p95 (us) | share |\n";
    out << "|---|---|---|---|---|\n";
    out << std::fixed << std::setprecision(1);
    for (const auto& [name, m] : rows) {
        const auto& vals = samples.at(name);
        out << "| " << name << " | " << vals.size() << " | " << m << " | " << p95(vals) << " | "
            << (meanSum > 0.0 ? 100.0 * m / meanSum : 0.0) << "% |\n";
    }
    const double wallMean = mean(wallMicros);
    out << "\nwall mean (us): " << wallMean << ", wall p95 (us): " << p95(wallMicros)
        << ", attributed share of wall: "
        << (wallMean > 0.0 ? 100.0 * meanSum / wallMean : 0.0) << "%\n";
    std::cout << out.str() << std::endl;
}

#endif // Windows skip
