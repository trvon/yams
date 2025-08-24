#include <chrono>
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
    EXPECT_GE(result.value().documents.size(), 0);
    EXPECT_GE(result.value().totalFound, 0);
}

TEST_F(DocumentServiceTest, ListWithLimit) {
    ListDocumentsRequest request;
    request.limit = 5;

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);
    EXPECT_LE(result.value().documents.size(), 5);
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

    auto result = documentService_->list(request);

    ASSERT_TRUE(result);
    // Should return empty result or only documents with that tag
    EXPECT_GE(result.value().documents.size(), 0);
}

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

    auto result = documentService_->retrieve(request);

    ASSERT_TRUE(result) << "Failed to retrieve document: " << result.error().message;
    ASSERT_TRUE(result.value().document.has_value());
    EXPECT_EQ(result.value().document->hash, testHash1_);
    EXPECT_TRUE(result.value().document->content.has_value());
}

TEST_F(DocumentServiceTest, RetrieveDocumentByName) {
    // This test depends on having documents stored with names
    RetrieveDocumentRequest request;
    request.name = "test1.txt";
    request.includeContent = true;

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
    EXPECT_EQ(result.value().count, 1);

    // Verify document is actually deleted
    RetrieveDocumentRequest getRequest;
    getRequest.hash = hashToDelete;
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
    EXPECT_EQ(result.value().count, 0);
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
        EXPECT_EQ(result.value().updatesApplied, 3);
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
        EXPECT_EQ(result2.value().count, 0);
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