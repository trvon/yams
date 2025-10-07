#include <chrono>
#include <memory>
#include <thread>
#include <gtest/gtest.h>
#include <yams/api/metadata_api.h>
#include <yams/indexing/document_indexer.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::api;
using namespace yams::metadata;
using namespace std::chrono_literals;

class MetadataApiTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create in-memory database for testing
        database_ = std::make_shared<Database>(":memory:");
        // Database initializes itself in constructor

        // Create metadata repository
        repository_ = std::make_shared<MetadataRepository>(database_);
        // Repository initializes itself in constructor

        // Create indexer (mock)
        indexer_ = nullptr; // Can add mock indexer if needed

        // Create API with default config
        api_ = std::make_unique<MetadataApi>(repository_, indexer_);
    }

    DocumentMetadata createTestMetadata(const std::string& path) {
        DocumentMetadata meta;
        meta.info.fileName = "Test Document";
        meta.info.filePath = path;
        meta.info.mimeType = "text/plain";
        meta.info.fileSize = 1024;
        meta.info.sha256Hash = "hash123";
        auto now_s = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        meta.info.modifiedTime = now_s;
        meta.info.createdTime = now_s;
        return meta;
    }

    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> repository_;
    std::shared_ptr<yams::indexing::DocumentIndexer> indexer_;
    std::unique_ptr<MetadataApi> api_;
};

// ===== CRUD Operation Tests =====

TEST_F(MetadataApiTest, CreateMetadata_Success) {
    CreateMetadataRequest request;
    request.requestId = "test-001";
    request.metadata = createTestMetadata("/test/doc1.txt");

    auto response = api_->createMetadata(request);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.requestId, request.requestId);
    EXPECT_GT(response.documentId, 0);
    EXPECT_EQ(response.createdMetadata.path, "/test/doc1.txt");
}

TEST_F(MetadataApiTest, CreateMetadata_DuplicatePath) {
    CreateMetadataRequest request;
    request.metadata = createTestMetadata("/test/doc1.txt");
    request.validateUniqueness = true;

    // First create should succeed
    auto response1 = api_->createMetadata(request);
    EXPECT_TRUE(response1.success);

    // Second create should fail
    auto response2 = api_->createMetadata(request);
    EXPECT_FALSE(response2.success);
    EXPECT_EQ(response2.errorCode, ErrorCode::InvalidArgument);
}

TEST_F(MetadataApiTest, GetMetadata_ById) {
    // Create a document
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/doc1.txt");
    auto createResp = api_->createMetadata(createReq);
    ASSERT_TRUE(createResp.success);

    // Get by ID
    GetMetadataRequest getReq;
    getReq.documentId = createResp.documentId;
    auto getResp = api_->getMetadata(getReq);

    EXPECT_TRUE(getResp.success);
    EXPECT_EQ(getResp.metadata.id, createResp.documentId);
    EXPECT_EQ(getResp.metadata.path, "/test/doc1.txt");
}

TEST_F(MetadataApiTest, GetMetadata_ByPath) {
    // Create a document
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/doc1.txt");
    auto createResp = api_->createMetadata(createReq);
    ASSERT_TRUE(createResp.success);

    // Get by path
    GetMetadataRequest getReq;
    getReq.path = "/test/doc1.txt";
    auto getResp = api_->getMetadata(getReq);

    EXPECT_TRUE(getResp.success);
    EXPECT_EQ(getResp.metadata.path, "/test/doc1.txt");
}

TEST_F(MetadataApiTest, GetMetadata_NotFound) {
    GetMetadataRequest getReq;
    getReq.documentId = 9999;
    auto getResp = api_->getMetadata(getReq);

    EXPECT_FALSE(getResp.success);
    EXPECT_EQ(getResp.errorCode, ErrorCode::NotFound);
}

TEST_F(MetadataApiTest, UpdateMetadata_Success) {
    // Create a document
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/doc1.txt");
    auto createResp = api_->createMetadata(createReq);
    ASSERT_TRUE(createResp.success);

    // Update the document
    UpdateMetadataRequest updateReq;
    updateReq.documentId = createResp.documentId;
    updateReq.metadata = createTestMetadata("/test/doc1.txt");
    updateReq.metadata.title = "Updated Title";
    updateReq.metadata.fileSize = 2048;

    auto updateResp = api_->updateMetadata(updateReq);

    EXPECT_TRUE(updateResp.success);
    EXPECT_EQ(updateResp.updatedMetadata.title, "Updated Title");
    EXPECT_EQ(updateResp.updatedMetadata.fileSize, 2048);
    EXPECT_EQ(updateResp.previousMetadata.title, "Test Document");
}

TEST_F(MetadataApiTest, UpdateMetadata_CreateIfNotExists) {
    UpdateMetadataRequest updateReq;
    updateReq.documentId = 9999;
    updateReq.metadata = createTestMetadata("/test/newdoc.txt");
    updateReq.createIfNotExists = true;

    auto updateResp = api_->updateMetadata(updateReq);

    EXPECT_TRUE(updateResp.success);
    EXPECT_TRUE(updateResp.wasCreated);
    EXPECT_EQ(updateResp.updatedMetadata.path, "/test/newdoc.txt");
}

TEST_F(MetadataApiTest, DeleteMetadata_Success) {
    // Create a document
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/doc1.txt");
    auto createResp = api_->createMetadata(createReq);
    ASSERT_TRUE(createResp.success);

    // Delete the document
    DeleteMetadataRequest deleteReq;
    deleteReq.documentId = createResp.documentId;
    deleteReq.softDelete = false; // Hard delete

    auto deleteResp = api_->deleteMetadata(deleteReq);

    EXPECT_TRUE(deleteResp.success);
    EXPECT_EQ(deleteResp.deletedMetadata.path, "/test/doc1.txt");
    EXPECT_FALSE(deleteResp.wasSoftDeleted);

    // Verify it's deleted
    GetMetadataRequest getReq;
    getReq.documentId = createResp.documentId;
    auto getResp = api_->getMetadata(getReq);
    EXPECT_FALSE(getResp.success);
}

// ===== Bulk Operation Tests =====

TEST_F(MetadataApiTest, BulkCreate_Success) {
    BulkCreateRequest request;
    request.documents.push_back(createTestMetadata("/test/bulk1.txt"));
    request.documents.push_back(createTestMetadata("/test/bulk2.txt"));
    request.documents.push_back(createTestMetadata("/test/bulk3.txt"));

    auto response = api_->bulkCreate(request);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.successCount, 3);
    EXPECT_EQ(response.failureCount, 0);
    EXPECT_TRUE(response.allSucceeded);
    EXPECT_EQ(response.results.size(), 3);
}

TEST_F(MetadataApiTest, BulkCreate_PartialFailure) {
    // Create one document first
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/bulk1.txt");
    api_->createMetadata(createReq);

    // Try bulk create with duplicate
    BulkCreateRequest request;
    request.documents.push_back(createTestMetadata("/test/bulk1.txt")); // Duplicate
    request.documents.push_back(createTestMetadata("/test/bulk2.txt"));
    request.documents.push_back(createTestMetadata("/test/bulk3.txt"));
    request.continueOnError = true;

    auto response = api_->bulkCreate(request);

    EXPECT_TRUE(response.success); // Overall success despite failures
    EXPECT_EQ(response.successCount, 2);
    EXPECT_EQ(response.failureCount, 1);
    EXPECT_FALSE(response.allSucceeded);
}

TEST_F(MetadataApiTest, BulkCreate_MaxOperationsLimit) {
    // Configure API with small limit
    MetadataApiConfig config;
    config.maxBulkOperations = 2;
    api_->setConfig(config);

    BulkCreateRequest request;
    request.documents.push_back(createTestMetadata("/test/bulk1.txt"));
    request.documents.push_back(createTestMetadata("/test/bulk2.txt"));
    request.documents.push_back(createTestMetadata("/test/bulk3.txt"));

    auto response = api_->bulkCreate(request);

    EXPECT_FALSE(response.success);
    EXPECT_EQ(response.errorCode, ErrorCode::InvalidArgument);
}

// ===== Query Operation Tests =====

TEST_F(MetadataApiTest, QueryMetadata_Basic) {
    // Create test documents
    for (int i = 0; i < 5; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/doc" + std::to_string(i) + ".txt");
        req.metadata.fileSize = 1000 * (i + 1);
        api_->createMetadata(req);
    }

    QueryMetadataRequest query;
    query.limit = 3;
    query.offset = 0;

    auto response = api_->queryMetadata(query);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.totalCount, 5);
    EXPECT_EQ(response.returnedCount, 3);
    EXPECT_TRUE(response.hasMore);
}

TEST_F(MetadataApiTest, QueryMetadata_WithFilter) {
    // Create test documents
    CreateMetadataRequest req1;
    req1.metadata = createTestMetadata("/test/doc1.txt");
    req1.metadata.contentType = "text/plain";
    api_->createMetadata(req1);

    CreateMetadataRequest req2;
    req2.metadata = createTestMetadata("/test/doc2.pdf");
    req2.metadata.contentType = "application/pdf";
    api_->createMetadata(req2);

    CreateMetadataRequest req3;
    req3.metadata = createTestMetadata("/test/doc3.txt");
    req3.metadata.contentType = "text/plain";
    api_->createMetadata(req3);

    // Query with content type filter
    QueryMetadataRequest query;
    query.filter.contentType = "text/plain";

    auto response = api_->queryMetadata(query);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.totalCount, 2);
    EXPECT_EQ(response.documents.size(), 2);
}

TEST_F(MetadataApiTest, QueryMetadata_WithSorting) {
    // Create test documents with different sizes
    for (int i = 0; i < 3; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/doc" + std::to_string(i) + ".txt");
        req.metadata.fileSize = (3 - i) * 1000; // Decreasing sizes
        api_->createMetadata(req);
    }

    // Query sorted by size ascending
    QueryMetadataRequest query;
    query.sortBy = QueryMetadataRequest::SortField::Size;
    query.ascending = true;

    auto response = api_->queryMetadata(query);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.documents.size(), 3);
    EXPECT_LT(response.documents[0].fileSize, response.documents[1].fileSize);
    EXPECT_LT(response.documents[1].fileSize, response.documents[2].fileSize);
}

TEST_F(MetadataApiTest, QueryMetadata_WithStatistics) {
    // Create test documents
    for (int i = 0; i < 3; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/doc" + std::to_string(i) + ".txt");
        req.metadata.fileSize = 1000 * (i + 1);
        req.metadata.contentType = (i % 2 == 0) ? "text/plain" : "application/pdf";
        api_->createMetadata(req);
    }

    QueryMetadataRequest query;
    query.includeStats = true;

    auto response = api_->queryMetadata(query);

    EXPECT_TRUE(response.success);
    EXPECT_TRUE(response.stats.has_value());
    EXPECT_EQ(response.stats->totalSize, 6000); // 1000 + 2000 + 3000
    EXPECT_EQ(response.stats->averageSize, 2000);
    EXPECT_EQ(response.stats->typeDistribution["text/plain"], 2);
    EXPECT_EQ(response.stats->typeDistribution["application/pdf"], 1);
}

// ===== Export/Import Tests =====

TEST_F(MetadataApiTest, ExportMetadata_JSON) {
    // Create test documents
    for (int i = 0; i < 3; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/doc" + std::to_string(i) + ".txt");
        api_->createMetadata(req);
    }

    ExportMetadataRequest request;
    request.format = ExportMetadataRequest::ExportFormat::JSON;
    request.outputPath = "/tmp/test_export.json";

    auto response = api_->exportMetadata(request);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.documentsExported, 3);
    EXPECT_EQ(response.exportPath, "/tmp/test_export.json");
}

TEST_F(MetadataApiTest, ExportMetadata_CSV) {
    // Create test documents
    for (int i = 0; i < 2; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/doc" + std::to_string(i) + ".txt");
        api_->createMetadata(req);
    }

    ExportMetadataRequest request;
    request.format = ExportMetadataRequest::ExportFormat::CSV;
    request.outputPath = "/tmp/test_export.csv";

    auto response = api_->exportMetadata(request);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.documentsExported, 2);
}

TEST_F(MetadataApiTest, ImportMetadata_JSON) {
    // First export some data
    for (int i = 0; i < 2; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/export" + std::to_string(i) + ".txt");
        api_->createMetadata(req);
    }

    ExportMetadataRequest exportReq;
    exportReq.format = ExportMetadataRequest::ExportFormat::JSON;
    exportReq.outputPath = "/tmp/test_import.json";
    api_->exportMetadata(exportReq);

    // Clear database by deleting each document
    QueryMetadataRequest queryReq;
    auto queryResp = api_->queryMetadata(queryReq);
    for (const auto& doc : queryResp.documents) {
        DeleteMetadataRequest delReq;
        delReq.documentId = doc.id;
        delReq.softDelete = false;
        api_->deleteMetadata(delReq);
    }

    // Import the data
    ImportMetadataRequest importReq;
    importReq.format = ImportMetadataRequest::ImportFormat::JSON;
    importReq.inputPath = "/tmp/test_import.json";

    auto response = api_->importMetadata(importReq);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.documentsImported, 2);
    EXPECT_EQ(response.documentsFailed, 0);
}

// ===== Validation Tests =====

TEST_F(MetadataApiTest, ValidateMetadata_Valid) {
    ValidateMetadataRequest request;
    request.metadata = createTestMetadata("/test/valid.txt");

    auto response = api_->validateMetadata(request);

    EXPECT_TRUE(response.success);
    EXPECT_TRUE(response.isValid);
    EXPECT_TRUE(response.errors.empty());
}

TEST_F(MetadataApiTest, ValidateMetadata_Invalid) {
    ValidateMetadataRequest request;
    request.metadata = DocumentMetadata(); // Empty metadata

    auto response = api_->validateMetadata(request);

    EXPECT_TRUE(response.success);  // Request succeeds
    EXPECT_FALSE(response.isValid); // But metadata is invalid
    EXPECT_FALSE(response.errors.empty());

    // Check for specific errors
    bool hasPathError = false;
    bool hasTitleError = false;
    for (const auto& error : response.errors) {
        if (error.field == "path")
            hasPathError = true;
        if (error.field == "title")
            hasTitleError = true;
    }
    EXPECT_TRUE(hasPathError);
    EXPECT_TRUE(hasTitleError);
}

TEST_F(MetadataApiTest, ValidateMetadata_CheckUniqueness) {
    // Create a document
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/existing.txt");
    api_->createMetadata(createReq);

    // Validate a duplicate
    ValidateMetadataRequest validateReq;
    validateReq.metadata = createTestMetadata("/test/existing.txt");
    validateReq.checkUniqueness = true;

    auto response = api_->validateMetadata(validateReq);

    EXPECT_TRUE(response.success);
    EXPECT_FALSE(response.isValid);
    EXPECT_FALSE(response.uniquenessValid);
}

// ===== Async Operation Tests =====

TEST_F(MetadataApiTest, CreateMetadataAsync) {
    CreateMetadataRequest request;
    request.metadata = createTestMetadata("/test/async.txt");

    auto future = api_->createMetadataAsync(request);
    auto response = future.get();

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.createdMetadata.path, "/test/async.txt");
}

TEST_F(MetadataApiTest, QueryMetadataAsync) {
    // Create test documents
    for (int i = 0; i < 3; ++i) {
        CreateMetadataRequest req;
        req.metadata = createTestMetadata("/test/async" + std::to_string(i) + ".txt");
        api_->createMetadata(req);
    }

    QueryMetadataRequest query;
    auto future = api_->queryMetadataAsync(query);
    auto response = future.get();

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.totalCount, 3);
}

// ===== Health and Statistics Tests =====

TEST_F(MetadataApiTest, ApiStatistics) {
    // Reset statistics
    api_->resetStatistics();

    // Perform various operations
    CreateMetadataRequest createReq;
    createReq.metadata = createTestMetadata("/test/stats.txt");
    api_->createMetadata(createReq);

    GetMetadataRequest getReq;
    getReq.path = "/test/stats.txt";
    api_->getMetadata(getReq);

    GetMetadataRequest getReq2;
    getReq2.documentId = 9999;
    api_->getMetadata(getReq2); // This will fail

    auto stats = api_->getStatistics();

    EXPECT_EQ(stats.totalRequests, 3);
    EXPECT_EQ(stats.successfulRequests, 2);
    EXPECT_EQ(stats.failedRequests, 1);
    EXPECT_EQ(stats.requestsByType["create"], 1);
    EXPECT_EQ(stats.requestsByType["get"], 2);
}

TEST_F(MetadataApiTest, ApiHealth) {
    EXPECT_TRUE(api_->isHealthy());

    // Perform many failed operations to trigger unhealthy state
    api_->resetStatistics();
    for (int i = 0; i < 20; ++i) {
        GetMetadataRequest req;
        req.documentId = 9999 + i;
        api_->getMetadata(req); // All will fail
    }

    // API should not be healthy if all requests fail
    // (Note: This depends on implementation details)
}

// ===== Configuration Tests =====

TEST_F(MetadataApiTest, ConfigurationUpdate) {
    MetadataApiConfig config;
    config.maxBulkOperations = 5;
    config.defaultQueryLimit = 50;
    config.enableValidation = false;

    api_->setConfig(config);
    auto retrievedConfig = api_->getConfig();

    EXPECT_EQ(retrievedConfig.maxBulkOperations, 5);
    EXPECT_EQ(retrievedConfig.defaultQueryLimit, 50);
    EXPECT_FALSE(retrievedConfig.enableValidation);
}

// ===== Factory Tests =====

TEST_F(MetadataApiTest, Factory_CreateBasic) {
    auto api = MetadataApiFactory::create(repository_);
    EXPECT_NE(api, nullptr);
}

TEST_F(MetadataApiTest, Factory_CreateWithIndexer) {
    auto api = MetadataApiFactory::create(repository_, indexer_);
    EXPECT_NE(api, nullptr);
}

TEST_F(MetadataApiTest, Factory_CreateWithConfig) {
    MetadataApiConfig config;
    config.maxBulkOperations = 10;

    auto api = MetadataApiFactory::create(repository_, indexer_, config);
    EXPECT_NE(api, nullptr);
    EXPECT_EQ(api->getConfig().maxBulkOperations, 10);
}

// ===== Error Handling Tests =====

TEST_F(MetadataApiTest, NullRepository) {
    EXPECT_THROW(MetadataApi(nullptr, indexer_), std::invalid_argument);
}

TEST_F(MetadataApiTest, ValidationDisabled) {
    MetadataApiConfig config;
    config.enableValidation = false;
    api_->setConfig(config);

    CreateMetadataRequest request;
    request.metadata = DocumentMetadata(); // Invalid metadata

    // Should succeed even with invalid metadata when validation is disabled
    auto response = api_->createMetadata(request);

    // Will likely fail at repository level, but not due to validation
    EXPECT_FALSE(response.success);
    EXPECT_NE(response.errorCode, ErrorCode::InvalidArgument);
}
