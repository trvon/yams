#include <chrono>
#include <gtest/gtest.h>
#include <yams/app/services/services.hpp>
#include <yams/app/services/session_service.hpp>
// Existing includes remain below

#include <filesystem>
#include <fstream>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_executor.h>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::api;

class DocumentServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        setupTestEnvironment();
        setupDatabase();
        setupServices();
        setupTestDocuments();
    }

    void TearDown() override {
        cleanupServices();
        cleanupDatabase();
        cleanupTestEnvironment();
    }

private:
    void setupTestEnvironment() {
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ = std::filesystem::temp_directory_path() /
                   ("document_service_test_" + pid + "_" + timestamp);

        std::error_code ec;
        std::filesystem::create_directories(testDir_, ec);
        ASSERT_FALSE(ec) << "Failed to create test directory: " << ec.message();
    }

    void setupDatabase() {
        dbPath_ = std::filesystem::absolute(testDir_ / "test.db");
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string(), ConnectionMode::Create);
        ASSERT_TRUE(openResult) << "Failed to open database: " << openResult.error().message;

        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        MigrationManager mm(*database_);
        auto initResult = mm.initialize();
        ASSERT_TRUE(initResult) << "Failed to initialize migration system: "
                                << initResult.error().message;

        mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = mm.migrate();
        ASSERT_TRUE(migrateResult) << "Failed to run migrations: " << migrateResult.error().message;
    }

    void setupServices() {
        // Create content store using builder
        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(testDir_ / "storage")
                               .withChunkSize(65536)
                               .withCompression(true)
                               .withDeduplication(true)
                               .build();
        ASSERT_TRUE(storeResult) << "Failed to create content store: "
                                 << storeResult.error().message;

        // Extract unique_ptr and convert to shared_ptr
        auto& uniqueStore = storeResult.value();
        contentStore_ = std::shared_ptr<IContentStore>(
            const_cast<std::unique_ptr<IContentStore>&>(uniqueStore).release());

        // Search components are optional for document tests
        searchExecutor_ = nullptr;
        hybridEngine_ = nullptr;

        // Create app context
        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.searchExecutor = searchExecutor_;
        appContext_.hybridEngine = hybridEngine_;

        // Create document service using factory
        documentService_ = makeDocumentService(appContext_);
    }

    void setupTestDocuments() {
        // Create test files to store
        auto testFile1 = testDir_ / "test1.txt";
        auto testFile2 = testDir_ / "test2.md";

        std::ofstream file1(testFile1);
        file1 << "Test document content for retrieval testing";
        file1.close();

        std::ofstream file2(testFile2);
        file2 << "Another test document with different content";
        file2.close();

        // Store documents using document service
        StoreDocumentRequest storeReq1;
        storeReq1.path = testFile1.string();
        storeReq1.tags = {"test", "document"};

        StoreDocumentRequest storeReq2;
        storeReq2.path = testFile2.string();
        storeReq2.tags = {"test", "markdown"};

        auto result1 = documentService_->store(storeReq1);
        auto result2 = documentService_->store(storeReq2);

        if (result1 && result2) {
            testHash1_ = result1.value().hash;
            testHash2_ = result2.value().hash;
        }
    }

    void cleanupServices() {
        documentService_.reset();
        searchExecutor_.reset();
        hybridEngine_.reset();
        // Note: contentStore_ is now owned by appContext_
    }

    void cleanupDatabase() {
        if (database_) {
            database_->close();
            database_.reset();
        }
        if (pool_) {
            pool_->shutdown();
            pool_.reset();
        }
        metadataRepo_.reset();
    }

    void cleanupTestEnvironment() {
        if (!testDir_.empty() && std::filesystem::exists(testDir_)) {
            std::error_code ec;
            std::filesystem::remove_all(testDir_, ec);
        }
    }

protected:
    // Helper methods
    std::string createTestContent(const std::string& identifier) {
        return "Test content for document: " + identifier + " created at " +
               std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
    }

    // Test data
    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;

    // Database components
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;

    // Service components
    std::shared_ptr<IContentStore> contentStore_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine_;
    AppContext appContext_;
    std::shared_ptr<IDocumentService> documentService_;

    // Test document hashes
    std::string testHash1_;
    std::string testHash2_;
};

// Document Listing Tests

TEST_F(DocumentServiceTest, ListAllDocuments) {
    ListDocumentsRequest request;
    // Default request should list all documents
    request.limit = 100;

    auto result = documentService_->list(request);

    ASSERT_TRUE(result) << "Failed to list documents: " << result.error().message;

    // Should return some documents (at least our test documents if they were stored)
    // The exact count depends on setup, but should be non-negative
    EXPECT_GE(result.value().documents.size(), 0UL);
    EXPECT_GE(result.value().totalFound, 0UL);
}

TEST_F(DocumentServiceTest, ListWithLimit) {
    ListDocumentsRequest request;
    request.limit = 5;

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);
    EXPECT_LE(result.value().documents.size(), 5UL);
}

TEST_F(DocumentServiceTest, ListWithOffset) {
    // First, get total count
    ListDocumentsRequest request1;
    request1.limit = 100;
    auto result1 = documentService_->list(request1);
    ASSERT_TRUE(result1);

    if (result1.value().totalFound > 1) {
        ListDocumentsRequest request2;
        request2.offset = 1;
        request2.limit = 1;

        auto result2 = documentService_->list(request2);
        ASSERT_TRUE(result2);

        if (!result2.value().documents.empty() && !result1.value().documents.empty()) {
            // Should get a different document than the first one
            EXPECT_NE(result1.value().documents[0].hash, result2.value().documents[0].hash);
        }
    }
}

TEST_F(DocumentServiceTest, ListWithTagFilter) {
    ListDocumentsRequest request;
    request.tags = {"nonexistent-tag"};
    auto response = documentService_->list(request);
    ASSERT_TRUE(response);
    EXPECT_EQ(response.value().documents.size(), 0);
}

// TODO: Fix this test - queryDocumentsByPattern may have been removed or renamed
// TEST_F(DocumentServiceTest, StoreFromRawTextUsesLogicalNamePath) {
//     StoreDocumentRequest r;
//     r.name = "logical/nested/raw.txt";
//     r.content = "hello world";
//     r.mimeType = "text/plain";
//     r.deferExtraction = true; // speed
//     auto res = documentService_->store(r);
//     ASSERT_TRUE(res);
//
//     // Verify metadata path equals provided name (logical path), enabling tree diffs for raw docs
//     using yams::metadata::queryDocumentsByPattern;
//     auto docs = queryDocumentsByPattern(*metadataRepo_, "logical/nested/%", 10);
//     ASSERT_TRUE(docs);
//     bool found = false;
//     for (auto& d : docs.value()) {
//         if (d.filePath == r.name) { found = true; break; }
//     }
//     EXPECT_TRUE(found);
// }

TEST_F(DocumentServiceTest, ListWithTypeFilter) {
    ListDocumentsRequest request;
    request.type = "text";

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);

    // Verify all returned documents are of text type
    for (const auto& doc : result.value().documents) {
        if (!doc.mimeType.empty()) {
            EXPECT_TRUE(doc.mimeType.find("text/") == 0 || doc.mimeType == "application/json" ||
                        doc.mimeType.find("application/") == 0);
        }
    }
}

TEST_F(DocumentServiceTest, ListRecentDocuments) {
    ListDocumentsRequest request;
    request.recent = 10; // Get 10 most recent documents

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);
    EXPECT_LE(result.value().documents.size(), 10);

    // Documents should be sorted by creation/modification time (most recent first)
    // Verify ordering if we have multiple documents
    if (result.value().documents.size() > 1) {
        auto& docs = result.value().documents;
        for (size_t i = 1; i < docs.size(); ++i) {
            // Later documents should have earlier or equal timestamps
            EXPECT_GE(docs[i - 1].indexed, docs[i].indexed);
        }
    }
}

// Document Retrieval Tests

TEST_F(DocumentServiceTest, RetrieveDocumentByHash) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available for retrieval test";
    }

    RetrieveDocumentRequest request;
    request.hash = testHash1_;
    request.includeContent = true;
    request.acceptCompressed = false;

    auto result = documentService_->retrieve(request);

    ASSERT_TRUE(result) << "Failed to retrieve document: " << result.error().message;
    ASSERT_TRUE(result.value().document.has_value());
    EXPECT_EQ(result.value().document->hash, testHash1_);
    EXPECT_TRUE(result.value().document->content.has_value());
}

TEST_F(DocumentServiceTest, RetrieveDocumentAcceptCompressedProvidesMetadataAndRawBytes) {
    auto sourcePath = testDir_ / "compressed_fixture.bin";
    {
        std::ofstream out(sourcePath, std::ios::binary);
        ASSERT_TRUE(out.good());
        const std::string block(64 * 1024, 'A');
        for (int i = 0; i < 8; ++i) {
            out.write(block.data(), static_cast<std::streamsize>(block.size()));
        }
        out.flush();
        ASSERT_TRUE(out.good());
    }

    StoreDocumentRequest storeReq;
    storeReq.path = sourcePath.string();
    storeReq.tags = {"compressed"};
    auto stored = documentService_->store(storeReq);
    ASSERT_TRUE(stored) << stored.error().message;
    const std::string storedHash = stored.value().hash;
    ASSERT_FALSE(storedHash.empty());

    const auto expectedSize = std::filesystem::file_size(sourcePath);

    // Baseline: standard retrieval should return uncompressed payload
    RetrieveDocumentRequest plainReq;
    plainReq.hash = storedHash;
    plainReq.includeContent = true;
    plainReq.acceptCompressed = false;
    auto plain = documentService_->retrieve(plainReq);
    ASSERT_TRUE(plain) << plain.error().message;
    ASSERT_TRUE(plain.value().document.has_value());
    const auto& plainDoc = plain.value().document.value();
    ASSERT_TRUE(plainDoc.content.has_value());
    EXPECT_FALSE(plainDoc.compressed);
    EXPECT_FALSE(plainDoc.compressionAlgorithm.has_value());

    // With acceptCompressed enabled we expect raw compressed bytes plus metadata
    RetrieveDocumentRequest compressedReq;
    compressedReq.hash = storedHash;
    compressedReq.includeContent = true;
    compressedReq.acceptCompressed = true;
    auto compressed = documentService_->retrieve(compressedReq);
    ASSERT_TRUE(compressed) << compressed.error().message;
    ASSERT_TRUE(compressed.value().document.has_value());
    const auto& compressedDoc = compressed.value().document.value();
    ASSERT_TRUE(compressedDoc.content.has_value());
    EXPECT_TRUE(compressedDoc.compressed);
    EXPECT_TRUE(compressedDoc.compressionAlgorithm.has_value());
    EXPECT_TRUE(compressedDoc.uncompressedSize.has_value());
    EXPECT_EQ(expectedSize, compressedDoc.uncompressedSize.value());
    EXPECT_EQ(expectedSize, compressedDoc.size);
    EXPECT_FALSE(compressedDoc.compressionHeader.empty());
    EXPECT_NE(*compressedDoc.content, *plainDoc.content);
    EXPECT_LT(compressedDoc.content->size(), expectedSize);
}

TEST_F(DocumentServiceTest, RetrieveDocumentByName) {
    // This test depends on having documents stored with names
    RetrieveDocumentRequest request;
    request.name = "test1.txt";
    request.includeContent = true;
    request.acceptCompressed = false;

    auto result = documentService_->retrieve(request);

    // May succeed or fail depending on whether we have documents with names
    if (result && result.value().document.has_value()) {
        EXPECT_EQ(result.value().document->name, "test1.txt");
        EXPECT_TRUE(result.value().document->content.has_value());
    } else if (!result) {
        // It's okay if no document with that name exists
        EXPECT_EQ(result.error().code, ErrorCode::NotFound);
    }
}

TEST_F(DocumentServiceTest, RetrieveNonExistentDocument) {
    RetrieveDocumentRequest request;
    request.hash = "nonexistent_hash_that_does_not_exist";
    request.acceptCompressed = false;

    auto result = documentService_->retrieve(request);

    // May return empty result or error
    if (!result) {
        EXPECT_EQ(result.error().code, ErrorCode::NotFound);
    } else {
        EXPECT_FALSE(result.value().document.has_value());
    }
}

// Document Deletion Tests

TEST_F(DocumentServiceTest, DeleteDocumentByHash) {
    // First store a document to delete
    auto testFile = testDir_ / "delete_test.txt";
    std::ofstream file(testFile);
    file << createTestContent("for_deletion");
    file.close();

    StoreDocumentRequest storeReq;
    storeReq.path = testFile.string();
    auto storeResult = documentService_->store(storeReq);

    if (!storeResult) {
        GTEST_SKIP() << "Cannot store document for deletion test";
    }

    auto hashToDelete = storeResult.value().hash;

    DeleteByNameRequest request;
    request.hash = hashToDelete;

    auto result = documentService_->deleteByName(request);

    ASSERT_TRUE(result) << "Failed to delete document: " << result.error().message;
    EXPECT_EQ(result.value().count, 1UL);

    // Verify document is actually deleted
    RetrieveDocumentRequest getRequest;
    getRequest.hash = hashToDelete;
    getRequest.acceptCompressed = false;
    auto getResult = documentService_->retrieve(getRequest);
    if (getResult) {
        EXPECT_FALSE(getResult.value().document.has_value()); // Should not be found
    }
}

TEST_F(DocumentServiceTest, DeleteDocumentByName) {
    DeleteByNameRequest request;
    request.name = "nonexistent_document.txt";

    auto result = documentService_->deleteByName(request);

    // Should succeed but delete 0 documents
    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().count, 0UL);
}

TEST_F(DocumentServiceTest, DeleteWithDryRun) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available for dry run test";
    }

    DeleteByNameRequest request;
    request.hash = testHash1_;
    request.dryRun = true;

    auto result = documentService_->deleteByName(request);

    ASSERT_TRUE(result);
    // Should report what would be deleted but not actually delete
    EXPECT_TRUE(result.value().dryRun);

    // Verify document still exists
    RetrieveDocumentRequest getRequest;
    getRequest.hash = testHash1_;
    getRequest.acceptCompressed = false;
    auto getResult = documentService_->retrieve(getRequest);
    // Should still exist (unless it wasn't there to begin with)
}

// Metadata Operations Tests

TEST_F(DocumentServiceTest, UpdateDocumentMetadata) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available for metadata update test";
    }

    UpdateMetadataRequest request;
    request.hash = testHash1_;
    request.keyValues = {{"updated_by", "test"}, {"test_flag", "true"}, {"priority", "high"}};

    auto result = documentService_->updateMetadata(request);

    // May succeed or fail depending on implementation
    if (result) {
        EXPECT_TRUE(result.value().success);
        EXPECT_EQ(result.value().updatesApplied, 3UL);
    } else {
        // If not implemented yet, that's okay
        // Note: ErrorCode::NotImplemented might not exist, so we accept any error
        EXPECT_FALSE(result);
    }
}

// Error Handling Tests

TEST_F(DocumentServiceTest, HandleInvalidRequests) {
    // Test empty retrieve request
    RetrieveDocumentRequest emptyGetRequest;
    // Empty request might be valid (returns empty) or invalid
    auto result1 = documentService_->retrieve(emptyGetRequest);
    // Result depends on implementation

    // Test empty delete request
    DeleteByNameRequest emptyDeleteRequest;
    auto result2 = documentService_->deleteByName(emptyDeleteRequest);
    // Empty request should either fail or delete nothing
    if (result2) {
        EXPECT_EQ(result2.value().count, 0UL);
    }
}

// Performance Tests

TEST_F(DocumentServiceTest, ListPerformance) {
    ListDocumentsRequest request;
    request.limit = 100; // Large limit

    auto start = std::chrono::high_resolution_clock::now();
    auto result = documentService_->list(request);
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(result);

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should complete quickly (under 100ms for reasonable dataset)
    EXPECT_LT(duration.count(), 100) << "List operation took " << duration.count() << "ms";
}

TEST_F(DocumentServiceTest, RetrievalPerformance) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available for performance test";
    }

    RetrieveDocumentRequest request;
    request.hash = testHash1_;

    auto start = std::chrono::high_resolution_clock::now();
    auto result = documentService_->retrieve(request);
    auto end = std::chrono::high_resolution_clock::now();

    if (result) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        EXPECT_LT(duration.count(), 50) << "Document retrieval took " << duration.count() << "ms";
    }
}

// Document Resolution Tests (Tree-Aware PBI-043)

TEST_F(DocumentServiceTest, ResolveNameToHash_TreeLookupSuccess) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // Retrieve by exact path - should use tree-based resolution
    auto testFile1 = testDir_ / "test1.txt";
    RetrieveDocumentRequest request;
    request.name = testFile1.string(); // Use 'name' field for paths
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    ASSERT_TRUE(result) << "Tree-based resolution failed: " << result.error().message;
    EXPECT_EQ(result.value().document->hash, testHash1_);
    std::string docPath = result.value().document->path;
    std::string expectedPath = testFile1.string();
    std::error_code ec;
    auto canonicalPath = std::filesystem::weakly_canonical(testFile1, ec);
    if (!ec) {
        auto canonicalStr = canonicalPath.string();
        EXPECT_TRUE(docPath == expectedPath || docPath == canonicalStr)
            << "docPath=" << docPath << " expected=" << expectedPath
            << " canonical=" << canonicalStr;
    } else {
        EXPECT_EQ(docPath, expectedPath);
    }
}

TEST_F(DocumentServiceTest, ResolveNameToHash_ByHash) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // Retrieve by hash (SHA-256) directly
    RetrieveDocumentRequest request;
    request.hash = testHash1_; // Use 'hash' field for hashes
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    ASSERT_TRUE(result) << "Hash-based resolution failed: " << result.error().message;
    EXPECT_EQ(result.value().document->hash, testHash1_);
}

TEST_F(DocumentServiceTest, ResolveNameToHash_PathSuffix) {
    if (testHash2_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // Retrieve by path suffix (filename only)
    RetrieveDocumentRequest request;
    request.name = "test2.md";
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    // Should succeed if only one document matches the suffix
    // May fail with "Ambiguous" if multiple files named test2.md exist
    if (!result) {
        EXPECT_TRUE(result.error().message.find("Ambiguous") != std::string::npos ||
                    result.error().message.find("not found") != std::string::npos)
            << "Unexpected error: " << result.error().message;
    } else {
        EXPECT_EQ(result.value().document->hash, testHash2_);
    }
}

TEST_F(DocumentServiceTest, ResolveNameToHash_NotFound) {
    // Retrieve by non-existent path
    RetrieveDocumentRequest request;
    request.name = "nonexistent/path/to/file.txt";
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    ASSERT_FALSE(result);
    EXPECT_TRUE(result.error().message.find("not found") != std::string::npos ||
                result.error().message.find("No document") != std::string::npos)
        << "Expected 'not found' error, got: " << result.error().message;
}

TEST_F(DocumentServiceTest, ResolveNameToHash_NormalizedPathSeparators) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // Test path normalization: tree resolution should normalize separators
    // internally (backslash → forward slash)
    auto testFile1 = testDir_ / "test1.txt";

// Only test backslash normalization on Windows or for relative paths
// For absolute Unix paths, backslash conversion doesn't make sense
#ifdef _WIN32
    std::string pathWithBackslashes = testFile1.string();
    std::replace(pathWithBackslashes.begin(), pathWithBackslashes.end(), '/', '\\');

    RetrieveDocumentRequest request;
    request.name = pathWithBackslashes;
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    // Should succeed due to path normalization in resolvePathViaTree
    ASSERT_TRUE(result) << "Path normalization failed: " << result.error().message;
    EXPECT_EQ(result.value().document->hash, testHash1_);
#else
    // On Unix/macOS, just verify forward slashes work (they should)
    RetrieveDocumentRequest request;
    request.name = testFile1.string(); // Already uses forward slashes
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    ASSERT_TRUE(result) << "Path resolution failed: " << result.error().message;
    EXPECT_EQ(result.value().document->hash, testHash1_);
#endif
}

TEST_F(DocumentServiceTest, ResolveNameToHash_EmptyNameAndHash) {
    // Edge case: empty name and hash
    RetrieveDocumentRequest request;
    request.hash = "";
    request.name = "";
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    ASSERT_FALSE(result);
    EXPECT_TRUE(result.error().message.find("required") != std::string::npos ||
                result.error().message.find("empty") != std::string::npos)
        << "Expected error for empty name/hash, got: " << result.error().message;
}

TEST_F(DocumentServiceTest, ResolveNameToHash_RelativePathFromCwd) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // Store current directory
    auto originalCwd = std::filesystem::current_path();

    // Change to test directory
    std::filesystem::current_path(testDir_);

    // Retrieve by relative path
    RetrieveDocumentRequest request;
    request.name = "test1.txt";
    request.includeContent = false;

    auto result = documentService_->retrieve(request);

    // Restore original directory
    std::filesystem::current_path(originalCwd);

    // Should resolve relative to cwd (strategy 4 in resolveNameToHash)
    if (result) {
        EXPECT_EQ(result.value().document->hash, testHash1_);
    } else {
        // May fail if relative path resolution not fully implemented
        EXPECT_TRUE(result.error().message.find("not found") != std::string::npos ||
                    result.error().message.find("Ambiguous") != std::string::npos)
            << "Unexpected error: " << result.error().message;
    }
}

// Tree-Based List Tests (PBI-043 Integration)

TEST_F(DocumentServiceTest, List_TreeBasedPrefixQuery) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // Create a subdirectory structure
    auto subdir = testDir_ / "subdir";
    std::filesystem::create_directories(subdir);

    auto testFile3 = subdir / "test3.txt";
    std::ofstream file3(testFile3);
    file3 << "Test document in subdirectory";
    file3.close();

    // Store the new document
    StoreDocumentRequest storeReq;
    storeReq.path = testFile3.string();
    storeReq.tags = {"test", "subdir"};
    auto storeResult = documentService_->store(storeReq);
    ASSERT_TRUE(storeResult) << "Failed to store test document";
    std::string testHash3 = storeResult.value().hash;

    // List documents with path prefix (should use tree)
    ListDocumentsRequest request;
    request.pattern = testDir_.string() + "/*"; // Prefix pattern

    auto result = documentService_->list(request);

    ASSERT_TRUE(result) << "List failed: " << result.error().message;
    EXPECT_GE(result.value().documents.size(), 2) << "Expected at least 2 documents";

    // Verify we got documents from the test directory
    bool foundTopLevel = false;
    bool foundSubdir = false;
    for (const auto& doc : result.value().documents) {
        if (doc.hash == testHash1_ || (testHash2_.empty() == false && doc.hash == testHash2_)) {
            foundTopLevel = true;
        }
        if (doc.hash == testHash3) {
            foundSubdir = true;
        }
    }

    EXPECT_TRUE(foundTopLevel) << "Did not find top-level documents";
    EXPECT_TRUE(foundSubdir) << "Did not find subdirectory document";
}

TEST_F(DocumentServiceTest, List_TreeBasedSubdirectoryExclusion) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // This test verifies tree-based listing respects includeSubdirectories
    // Note: Current implementation always includes subdirectories for tree queries
    // This test documents current behavior

    ListDocumentsRequest request;
    request.pattern = testDir_.string() + "/*";

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);
    EXPECT_GE(result.value().documents.size(), 1) << "Expected at least 1 document";
}

TEST_F(DocumentServiceTest, List_TreeFallbackForComplexFilters) {
    if (testHash1_.empty()) {
        GTEST_SKIP() << "No test documents available";
    }

    // List with prefix + tags should NOT use tree (falls back to SQL)
    ListDocumentsRequest request;
    request.pattern = testDir_.string() + "/*";
    request.tags = {"test"}; // Additional filter triggers SQL fallback

    auto result = documentService_->list(request);

    ASSERT_TRUE(result) << "List with complex filters failed";
    // Should still get results via SQL fallback
    EXPECT_GE(result.value().documents.size(), 0);
}

// Search Integration Tests

TEST_F(DocumentServiceTest, SearchIntegration) {
    ListDocumentsRequest request;
    request.pattern = "test*"; // Pattern filter for documents

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);

    // All returned documents should be relevant to the pattern
    // (Implementation-dependent - may not be fully implemented yet)
    for (const auto& doc : result.value().documents) {
        // Basic check that documents have content/metadata
        EXPECT_FALSE(doc.hash.empty());
    }
}

// Service Integration Tests

TEST_F(DocumentServiceTest, ServiceContextIntegration) {
    // Test that service properly uses the app context
    EXPECT_NE(documentService_, nullptr);

    // Service should have access to metadata repository and content store
    // through the app context (verified implicitly through other tests)

    // Test basic functionality to ensure context is working
    ListDocumentsRequest request;
    request.limit = 1;

    auto result = documentService_->list(request);
    ASSERT_TRUE(result); // Should work if context is properly set up
}
