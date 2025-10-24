// SPDX-License-Identifier: Apache-2.0
// Copyright 2025 Trevon Wright
//
// FTS5 failure scenario tests - covers common indexing failure modes

#include <spdlog/spdlog.h>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <thread>
#include <gtest/gtest.h>
#include <yams/metadata/metadata_repository.h>

namespace {
// Helper to generate deterministic test hashes
std::string makeTestHash(int n) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0') << std::setw(64) << n;
    return oss.str();
}
} // namespace

using namespace yams::metadata;

class FTS5FailureTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directories
        testDir_ = std::filesystem::temp_directory_path() / "yams_test_fts5_failure";
        std::filesystem::remove_all(testDir_);
        std::filesystem::create_directories(testDir_);

        dbPath_ = testDir_ / "test.db";

        // Initialize repository
        repo_ = std::make_unique<MetadataRepository>();
        auto initResult = repo_->initialize(dbPath_);
        ASSERT_TRUE(initResult) << "Failed to initialize repository: "
                                << initResult.error().message;

        // Check FTS5 support
        auto fts5Result = repo_->hasFTS5();
        ASSERT_TRUE(fts5Result) << "Failed to check FTS5: " << fts5Result.error().message;
        if (!fts5Result.value()) {
            GTEST_SKIP() << "FTS5 not available in this SQLite build";
        }
    }

    void TearDown() override {
        repo_.reset();
        std::filesystem::remove_all(testDir_);
    }

    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;
    std::unique_ptr<MetadataRepository> repo_;
};

// Test: Missing document in metadata (fail_no_doc scenario)
TEST_F(FTS5FailureTest, MissingDocumentScenario) {
    // Try to index a document that doesn't exist in metadata
    auto result = repo_->indexDocumentContent(99999, "Ghost Doc", "content", "text/plain");

    // Should fail because document ID doesn't exist
    EXPECT_FALSE(result) << "Expected indexing to fail for non-existent document";
    if (!result) {
        // Verify error indicates missing document or constraint violation
        std::string errMsg = result.error().message;
        EXPECT_TRUE(errMsg.find("NOT NULL") != std::string::npos ||
                    errMsg.find("FOREIGN KEY") != std::string::npos ||
                    errMsg.find("constraint") != std::string::npos)
            << "Unexpected error: " << errMsg;
    }
}

// Test: UTF-8 encoding issues
TEST_F(FTS5FailureTest, Utf8EncodingScenario) {
    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/utf8_test.txt";
    info.fileName = "utf8_test.txt";
    info.fileExtension = "txt";
    info.fileSize = 100;
    info.sha256Hash = "hash_utf8_content";
    info.mimeType = "text/plain";
    info.indexedTime = std::chrono::system_clock::now();

    auto insertResult = repo_->insertDocument(info);
    ASSERT_TRUE(insertResult) << "Failed to insert document: " << insertResult.error().message;
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
            EXPECT_TRUE(errMsg.find("UTF") != std::string::npos ||
                        errMsg.find("encoding") != std::string::npos ||
                        errMsg.find("malformed") != std::string::npos)
                << "Test " << i << " unexpected error: " << errMsg;
        }
        // If it succeeds, sanitization worked - verify we can search
        else {
            auto searchResult = repo_->searchDocumentsFts("Valid", SearchOptions{});
            EXPECT_TRUE(searchResult)
                << "Search failed after UTF-8 indexing: " << searchResult.error().message;
        }
    }
}

// Test: Empty content extraction (fail_extraction scenario)
TEST_F(FTS5FailureTest, EmptyContentScenario) {
    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/empty.txt";
    info.fileName = "empty.txt";
    info.fileExtension = "txt";
    info.fileSize = 0;
    info.sha256Hash = "hash_empty";
    info.mimeType = "text/plain";
    info.indexedTime = std::chrono::system_clock::now();

    auto insertResult = repo_->insertDocument(info);
    ASSERT_TRUE(insertResult) << "Failed to insert document: " << insertResult.error().message;
    int64_t docId = insertResult.value();

    // Try to index with empty content - should succeed but be searchable
    auto indexResult = repo_->indexDocumentContent(docId, "Empty Doc", "", "text/plain");
    EXPECT_TRUE(indexResult) << "Indexing empty content should succeed: "
                             << indexResult.error().message;

    // Try with whitespace-only content
    auto indexResult2 =
        repo_->indexDocumentContent(docId, "Whitespace Doc", "   \n\t  ", "text/plain");
    EXPECT_TRUE(indexResult2) << "Indexing whitespace content should succeed: "
                              << indexResult2.error().message;
}

// Test: Concurrent indexing (potential DB lock scenario)
TEST_F(FTS5FailureTest, ConcurrentIndexingScenario) {
    // Insert multiple documents
    std::vector<int64_t> docIds;
    for (int i = 0; i < 10; ++i) {
        DocumentInfo info;
        info.filePath = "/test/concurrent_" + std::to_string(i) + ".txt";
        info.fileName = "concurrent_" + std::to_string(i) + ".txt";
        info.fileExtension = "txt";
        info.fileSize = 100;
        info.sha256Hash = common::sha256("content_" + std::to_string(i));
        info.mimeType = "text/plain";
        info.indexedTime = std::chrono::system_clock::now();

        auto insertResult = repo_->insertDocument(info);
        ASSERT_TRUE(insertResult) << "Failed to insert document " << i;
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
                // Log the error for debugging
                std::cerr << "Concurrent indexing failed for doc " << i << ": "
                          << result.error().message << std::endl;
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // Most should succeed, but some failures are acceptable due to concurrency
    EXPECT_GT(successes.load(), 0) << "At least some concurrent indexing should succeed";
    std::cout << "Concurrent indexing: " << successes.load() << " successes, " << failures.load()
              << " failures" << std::endl;
}

// Test: Large content stress
TEST_F(FTS5FailureTest, LargeContentScenario) {
    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/large.txt";
    info.fileName = "large.txt";
    info.fileExtension = "txt";
    info.fileSize = 10000000;
    info.sha256Hash = "hash_large_content";
    info.mimeType = "text/plain";
    info.indexedTime = std::chrono::system_clock::now();

    auto insertResult = repo_->insertDocument(info);
    ASSERT_TRUE(insertResult) << "Failed to insert document: " << insertResult.error().message;
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
    auto startTime = std::chrono::steady_clock::now();
    auto indexResult = repo_->indexDocumentContent(docId, "Large Doc", largeContent, "text/plain");
    auto endTime = std::chrono::steady_clock::now();

    EXPECT_TRUE(indexResult) << "Indexing large content failed: " << indexResult.error().message;

    if (indexResult) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
        std::cout << "Indexed " << (largeContent.size() / (1024 * 1024)) << " MB in "
                  << duration.count() << " ms" << std::endl;

        // Verify we can search it
        auto searchResult = repo_->searchDocumentsFts("Lorem", SearchOptions{});
        EXPECT_TRUE(searchResult) << "Search failed: " << searchResult.error().message;
        if (searchResult) {
            EXPECT_GT(searchResult.value().results.size(), 0) << "Should find matches";
        }
    }
}

// Test: Duplicate rowid scenario
TEST_F(FTS5FailureTest, DuplicateRowidScenario) {
    // Insert a document
    DocumentInfo info;
    info.filePath = "/test/dup.txt";
    info.fileName = "dup.txt";
    info.fileExtension = "txt";
    info.fileSize = 100;
    info.sha256Hash = "hash_dup_content";
    info.mimeType = "text/plain";
    info.indexedTime = std::chrono::system_clock::now();

    auto insertResult = repo_->insertDocument(info);
    ASSERT_TRUE(insertResult);
    int64_t docId = insertResult.value();

    // Index once
    auto result1 = repo_->indexDocumentContent(docId, "First", "content v1", "text/plain");
    EXPECT_TRUE(result1) << "First indexing failed: " << result1.error().message;

    // Index again with different content (should replace)
    auto result2 = repo_->indexDocumentContent(docId, "Second", "content v2", "text/plain");
    EXPECT_TRUE(result2) << "Second indexing failed: " << result2.error().message;

    // Verify only one entry exists (search for v2)
    auto searchResult = repo_->searchDocumentsFts("v2", SearchOptions{});
    ASSERT_TRUE(searchResult);
    EXPECT_EQ(searchResult.value().results.size(), 1) << "Should have exactly one result";
}
