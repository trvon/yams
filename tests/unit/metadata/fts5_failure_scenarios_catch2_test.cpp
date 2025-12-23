// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Trevon Wright
//
// FTS5 failure scenario tests - covers common indexing failure modes
// Migrated to Catch2 as part of yams-3s4 / yams-aqc

#include <spdlog/spdlog.h>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <thread>
#include <catch2/catch_test_macros.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/metadata_repository.h>

namespace {
// Helper to generate deterministic test hashes
std::string makeTestHash(int n) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(64) << n;
    return oss.str();
}

struct FTS5FailureFixture {
    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;
    std::unique_ptr<yams::metadata::ConnectionPool> pool_;
    std::unique_ptr<yams::metadata::MetadataRepository> repo_;
    bool fts5Available_ = false;

    FTS5FailureFixture() {
        using namespace yams::metadata;

        // Create temporary directories
        testDir_ = std::filesystem::temp_directory_path() / "yams_test_fts5_failure";
        std::filesystem::remove_all(testDir_);
        std::filesystem::create_directories(testDir_);

        dbPath_ = testDir_ / "test.db";

        // Initialize connection pool and repository
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;

        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        repo_ = std::make_unique<MetadataRepository>(*pool_);

        // Assume FTS5 is available - tests will fail naturally if not
        fts5Available_ = true;
    }

    ~FTS5FailureFixture() {
        repo_.reset();
        pool_->shutdown();
        pool_.reset();
        std::filesystem::remove_all(testDir_);
    }
};
} // namespace

using namespace yams::metadata;

TEST_CASE_METHOD(FTS5FailureFixture, "FTS5 missing document scenario",
                 "[unit][metadata][fts5][failure]") {
    if (!fts5Available_) {
        SKIP("FTS5 not available in this SQLite build");
    }

    // Try to index a document that doesn't exist in metadata
    auto result = repo_->indexDocumentContent(99999, "Ghost Doc", "content", "text/plain");

    // Should fail because document ID doesn't exist
    CHECK_FALSE(result);
    if (!result) {
        // Verify error indicates missing document
        std::string errMsg = result.error().message;
        CHECK((errMsg.find("not found") != std::string::npos ||
               errMsg.find("NOT NULL") != std::string::npos ||
               errMsg.find("FOREIGN KEY") != std::string::npos ||
               errMsg.find("constraint") != std::string::npos));
    }
}

TEST_CASE_METHOD(FTS5FailureFixture, "FTS5 UTF-8 encoding scenario",
                 "[unit][metadata][fts5][failure][utf8]") {
    if (!fts5Available_) {
        SKIP("FTS5 not available in this SQLite build");
    }

    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/utf8_test.txt";
    info.fileName = "utf8_test.txt";
    info.fileExtension = "txt";
    info.fileSize = 100;
    info.sha256Hash = "hash_utf8_content";
    info.mimeType = "text/plain";
    info.indexedTime =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    auto insertResult = repo_->insertDocument(info);
    REQUIRE(insertResult);
    int64_t docId = insertResult.value();

    // Test with various problematic UTF-8 sequences
    std::vector<std::string> problematicContent = {
        // Invalid UTF-8 sequences
        std::string("Invalid \xFF\xFE sequence"),
        // Overlong encodings
        std::string("Overlong \xC0\x80 null"),
        // Lone surrogates
        std::string("Surrogate \xED\xA0\x80 char"),
        // Mix of valid and invalid
        std::string("Valid text \xFF invalid \xFE more valid")};

    for (size_t i = 0; i < problematicContent.size(); ++i) {
        const auto& content = problematicContent[i];

        // FTS5 indexing should either succeed (after sanitization) or fail gracefully
        auto indexResult = repo_->indexDocumentContent(docId, "UTF8 Test", content, "text/plain");

        if (!indexResult) {
            // If it fails, verify it's due to encoding issues
            std::string errMsg = indexResult.error().message;
            CHECK((errMsg.find("UTF") != std::string::npos ||
                   errMsg.find("encoding") != std::string::npos ||
                   errMsg.find("malformed") != std::string::npos));
        }
        // If it succeeds, sanitization worked - verify we can search
        else {
            auto searchResult = repo_->search("Valid");
            CHECK(searchResult);
        }
    }
}

TEST_CASE_METHOD(FTS5FailureFixture, "FTS5 empty content scenario",
                 "[unit][metadata][fts5][failure]") {
    if (!fts5Available_) {
        SKIP("FTS5 not available in this SQLite build");
    }

    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/empty.txt";
    info.fileName = "empty.txt";
    info.fileExtension = "txt";
    info.fileSize = 0;
    info.sha256Hash = "hash_empty";
    info.mimeType = "text/plain";
    info.indexedTime =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    auto insertResult = repo_->insertDocument(info);
    REQUIRE(insertResult);
    int64_t docId = insertResult.value();

    SECTION("Empty content") {
        auto indexResult = repo_->indexDocumentContent(docId, "Empty Doc", "", "text/plain");
        CHECK(indexResult);
    }

    SECTION("Whitespace-only content") {
        auto indexResult =
            repo_->indexDocumentContent(docId, "Whitespace Doc", "   \n\t  ", "text/plain");
        CHECK(indexResult);
    }
}

TEST_CASE_METHOD(FTS5FailureFixture, "FTS5 concurrent indexing scenario",
                 "[unit][metadata][fts5][failure][concurrent]") {
    if (!fts5Available_) {
        SKIP("FTS5 not available in this SQLite build");
    }

    // Insert multiple documents
    std::vector<int64_t> docIds;
    for (int i = 0; i < 10; ++i) {
        DocumentInfo info;
        info.filePath = "/test/concurrent_" + std::to_string(i) + ".txt";
        info.fileName = "concurrent_" + std::to_string(i) + ".txt";
        info.fileExtension = "txt";
        info.fileSize = 100;
        info.sha256Hash = makeTestHash(i);
        info.mimeType = "text/plain";
        info.indexedTime =
            std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

        auto insertResult = repo_->insertDocument(info);
        REQUIRE(insertResult);
        docIds.push_back(insertResult.value());
    }

    // Launch concurrent indexing threads
    std::vector<std::thread> threads;
    std::atomic<int> successes{0};
    std::atomic<int> failures{0};

    for (size_t i = 0; i < docIds.size(); ++i) {
        threads.emplace_back([&, i, docId = docIds[i]]() {
            auto result = repo_->indexDocumentContent(docId, "Concurrent Doc " + std::to_string(i),
                                                      "This is document " + std::to_string(i) +
                                                          " being indexed concurrently",
                                                      "text/plain");
            if (result) {
                ++successes;
            } else {
                ++failures;
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Most should succeed, but some failures are acceptable due to concurrency
    CHECK(successes.load() > 0);
}

TEST_CASE_METHOD(FTS5FailureFixture, "FTS5 large content scenario",
                 "[unit][metadata][fts5][failure][large]") {
    if (!fts5Available_) {
        SKIP("FTS5 not available in this SQLite build");
    }

    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/large.txt";
    info.fileName = "large.txt";
    info.fileExtension = "txt";
    info.fileSize = 10000000;
    info.sha256Hash = "hash_large_content";
    info.mimeType = "text/plain";
    info.indexedTime =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    auto insertResult = repo_->insertDocument(info);
    REQUIRE(insertResult);
    int64_t docId = insertResult.value();

    // Generate large content (10 MB)
    std::string largeContent;
    largeContent.reserve(10 * 1024 * 1024);
    for (int i = 0; i < 10000; ++i) {
        largeContent += "This is line " + std::to_string(i) + " of a very large document. ";
        largeContent += "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ";
        largeContent += "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n";
    }

    // Index large content
    auto indexResult = repo_->indexDocumentContent(docId, "Large Doc", largeContent, "text/plain");
    CHECK(indexResult);

    if (indexResult) {
        // Verify we can search it
        auto searchResult = repo_->search("Lorem");
        CHECK(searchResult);
        if (searchResult) {
            CHECK(searchResult.value().results.size() > 0);
        }
    }
}

TEST_CASE_METHOD(FTS5FailureFixture, "FTS5 duplicate rowid scenario",
                 "[unit][metadata][fts5][failure]") {
    if (!fts5Available_) {
        SKIP("FTS5 not available in this SQLite build");
    }

    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/dup.txt";
    info.fileName = "dup.txt";
    info.fileExtension = "txt";
    info.fileSize = 100;
    info.sha256Hash = "hash_dup_content";
    info.mimeType = "text/plain";
    info.indexedTime =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());

    auto insertResult = repo_->insertDocument(info);
    REQUIRE(insertResult);
    int64_t docId = insertResult.value();

    // Index once
    auto result1 = repo_->indexDocumentContent(docId, "First", "content v1", "text/plain");
    CHECK(result1);

    // Index again with different content (should replace)
    auto result2 = repo_->indexDocumentContent(docId, "Second", "content v2", "text/plain");
    CHECK(result2);

    // Verify only one entry exists (search for v2)
    auto searchResult = repo_->search("v2");
    REQUIRE(searchResult);
    CHECK(searchResult.value().results.size() == 1);
}
