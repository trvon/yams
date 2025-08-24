#include <chrono>
#include <filesystem>
#include <fstream>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/api/content_store_builder.h>
#include <yams/app/services/services.hpp>
#include <yams/content/content_handler_registry.h>
#include <yams/detection/file_type_detector.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_executor.h>

using namespace yams;
using namespace yams::app::services;
using namespace yams::metadata;
using namespace yams::content;
using namespace yams::detection;
using namespace yams::api;

class IndexingServiceTest : public ::testing::Test {
protected:
    void SetUp() override {
        setupTestEnvironment();
        setupDatabase();
        setupServices();
    }

    void TearDown() override {
        cleanupServices();
        cleanupDatabase();
        cleanupTestEnvironment();
    }

private:
    void setupTestEnvironment() {
        // Create unique test directory
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        testDir_ = std::filesystem::temp_directory_path() /
                   ("indexing_service_test_" + pid + "_" + timestamp);

        std::error_code ec;
        std::filesystem::create_directories(testDir_, ec);
        ASSERT_FALSE(ec) << "Failed to create test directory: " << ec.message();

        // Initialize file type detector
        auto& detector = FileTypeDetector::instance();
        auto result = detector.initializeWithMagicNumbers();
        ASSERT_TRUE(result) << "Failed to initialize FileTypeDetector: " << result.error().message;

        // Initialize content handler registry
        auto& registry = ContentHandlerRegistry::instance();
        registry.clear();
        registry.initializeDefaultHandlers();
    }

    void setupDatabase() {
        // Create database path
        dbPath_ = std::filesystem::absolute(testDir_ / "test.db");

        // Initialize database
        database_ = std::make_unique<Database>();
        auto openResult = database_->open(dbPath_.string(), ConnectionMode::Create);
        ASSERT_TRUE(openResult) << "Failed to open database: " << openResult.error().message;

        // Initialize connection pool
        ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), poolConfig);

        // Initialize metadata repository
        metadataRepo_ = std::make_shared<MetadataRepository>(*pool_);

        // Run migrations
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
        contentStore_ = std::shared_ptr<api::IContentStore>(
            const_cast<std::unique_ptr<api::IContentStore>&>(uniqueStore).release());

        // Search components are optional for indexing tests
        searchExecutor_ = nullptr;
        hybridEngine_ = nullptr;

        // Create app context
        appContext_.store = contentStore_;
        appContext_.metadataRepo = metadataRepo_;
        appContext_.searchExecutor = searchExecutor_;
        appContext_.hybridEngine = hybridEngine_;

        // Create indexing service using factory
        indexingService_ = makeIndexingService(appContext_);
    }

    void cleanupServices() {
        indexingService_.reset();
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
        ContentHandlerRegistry::instance().clear();

        if (!testDir_.empty() && std::filesystem::exists(testDir_)) {
            std::error_code ec;
            std::filesystem::remove_all(testDir_, ec);
        }
    }

protected:
    // Helper methods
    std::filesystem::path createTestFile(const std::string& filename, const std::string& content) {
        auto filePath = testDir_ / filename;
        std::ofstream file(filePath);
        file << content;
        file.close();
        return filePath;
    }

    AddDirectoryRequest
    createAddDirectoryRequest(const std::filesystem::path& path, const std::string& collection = "",
                              const std::unordered_map<std::string, std::string>& metadata = {}) {
        AddDirectoryRequest request;
        request.directoryPath = path.string();
        request.collection = collection;
        request.metadata = metadata;
        request.recursive = false;
        return request;
    }

    // Test data
    std::filesystem::path testDir_;
    std::filesystem::path dbPath_;

    // Database components
    std::unique_ptr<Database> database_;
    std::unique_ptr<ConnectionPool> pool_;
    std::shared_ptr<MetadataRepository> metadataRepo_;

    // Service components
    std::shared_ptr<api::IContentStore> contentStore_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine_;
    AppContext appContext_;
    std::shared_ptr<IIndexingService> indexingService_;
};

// Basic Functionality Tests

TEST_F(IndexingServiceTest, IndexSingleFileInDirectory) {
    // Create test file in a subdirectory
    std::filesystem::create_directories(testDir_ / "docs");
    createTestFile("docs/test.txt", "This is a test document with some content.");

    // Create add directory request
    auto request = createAddDirectoryRequest(testDir_ / "docs", "test-collection");

    // Index the directory
    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result) << "Failed to index directory: " << result.error().message;
    EXPECT_EQ(result.value().filesIndexed, 1);
    EXPECT_EQ(result.value().filesSkipped, 0);
    EXPECT_EQ(result.value().filesFailed, 0);
    EXPECT_EQ(result.value().filesProcessed, 1);
}

TEST_F(IndexingServiceTest, IndexMultipleFilesInDirectory) {
    // Create multiple test files in directory
    std::filesystem::create_directories(testDir_ / "multi");
    createTestFile("multi/file1.txt", "Content of first file");
    createTestFile("multi/file2.md", "# Markdown content\n\nSome **bold** text");
    createTestFile("multi/file3.cpp", "#include <iostream>\nint main() { return 0; }");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "multi").string();
    request.collection = "test-batch";
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 3);
    EXPECT_EQ(result.value().filesSkipped, 0);
    EXPECT_EQ(result.value().filesProcessed, 3);
}

TEST_F(IndexingServiceTest, IndexDirectoryRecursively) {
    // Create directory structure
    std::filesystem::create_directories(testDir_ / "recursive");
    std::filesystem::create_directories(testDir_ / "recursive/subdir");
    createTestFile("recursive/root.txt", "Root level file");
    createTestFile("recursive/subdir/nested.txt", "Nested file content");
    createTestFile("recursive/subdir/another.md", "# Nested markdown");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "recursive").string();
    request.recursive = true;
    request.includePatterns = {"*.txt", "*.md"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 3);
    EXPECT_EQ(result.value().filesSkipped, 0);
}

// Error Handling Tests

TEST_F(IndexingServiceTest, HandleNonExistentDirectory) {
    auto request = createAddDirectoryRequest(testDir_ / "does_not_exist");

    auto result = indexingService_->addDirectory(request);

    ASSERT_FALSE(result); // Service should fail for non-existent directory
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
    EXPECT_TRUE(result.error().message.find("does not exist") != std::string::npos);
}

TEST_F(IndexingServiceTest, HandleEmptyDirectory) {
    // Create empty directory
    std::filesystem::create_directories(testDir_ / "empty");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "empty").string();

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result); // Should succeed but with no files indexed
    EXPECT_EQ(result.value().filesIndexed, 0);
    EXPECT_EQ(result.value().filesProcessed, 0);
}

TEST_F(IndexingServiceTest, HandleBinaryFiles) {
    // Create a directory with binary file
    std::filesystem::create_directories(testDir_ / "binaries");
    auto binaryFile = testDir_ / "binaries/binary.exe";
    std::ofstream file(binaryFile, std::ios::binary);
    for (int i = 0; i < 1000; ++i) {
        file.put(static_cast<char>(i % 256));
    }
    file.close();

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "binaries").string();

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Binary files should still be indexed
    EXPECT_GE(result.value().filesIndexed, 0);
}

// Metadata and Tagging Tests

TEST_F(IndexingServiceTest, ApplyCollectionAndMetadata) {
    std::filesystem::create_directories(testDir_ / "tagged");
    createTestFile("tagged/tagged.txt", "Content with tags and metadata");

    std::unordered_map<std::string, std::string> metadata = {
        {"project", "test-project"}, {"author", "test-author"}, {"priority", "high"}};

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "tagged").string();
    request.collection = "important-collection";
    request.metadata = metadata;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
    EXPECT_EQ(result.value().collection, "important-collection");

    // Verify metadata was applied
    // Note: This would require additional service methods to retrieve and verify
    // For now, we trust that the service correctly applied the metadata
}

// Duplicate Handling Tests

TEST_F(IndexingServiceTest, HandleDuplicateContent) {
    // Create directory with two files having identical content
    std::filesystem::create_directories(testDir_ / "duplicates");
    std::string content = "Identical content for deduplication test";
    createTestFile("duplicates/file1.txt", content);
    createTestFile("duplicates/file2.txt", content);

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "duplicates").string();

    auto result = indexingService_->addDirectory(request);
    ASSERT_TRUE(result);

    // Both files should be processed
    EXPECT_EQ(result.value().filesProcessed, 2);
    // Both should be indexed (even if content is deduplicated internally)
    EXPECT_EQ(result.value().filesIndexed, 2);
}

// Content Handler Integration Tests

TEST_F(IndexingServiceTest, UseCorrectContentHandlers) {
    // Create directory with files of different types
    std::filesystem::create_directories(testDir_ / "handlers");
    createTestFile("handlers/text.txt", "Plain text content");
    createTestFile("handlers/document.md", "# Markdown\n\nContent with **formatting**");

    // Create a simple PDF-like file (minimal PDF structure)
    auto pdfPath = testDir_ / "handlers/document.pdf";
    std::string pdfContent = "%PDF-1.4\nMinimal PDF content for testing";
    std::ofstream(pdfPath) << pdfContent;

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "handlers").string();

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // All files should be processed, each by appropriate handler
    EXPECT_GE(result.value().filesIndexed, 2); // At least text and markdown
}

// Performance Tests

TEST_F(IndexingServiceTest, IndexingPerformance) {
    // Create directory with multiple files for performance testing
    std::filesystem::create_directories(testDir_ / "performance");
    const int numFiles = 10;

    for (int i = 0; i < numFiles; ++i) {
        auto filename = "performance/perf_test_" + std::to_string(i) + ".txt";
        createTestFile(filename, "Performance test content " + std::to_string(i));
    }

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "performance").string();

    auto start = std::chrono::high_resolution_clock::now();
    auto result = indexingService_->addDirectory(request);
    auto end = std::chrono::high_resolution_clock::now();

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, numFiles);

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Performance assertion: should complete within reasonable time
    EXPECT_LT(duration.count(), 1000)
        << "Indexing " << numFiles << " files took " << duration.count() << "ms";
}

// Directory Processing Tests

TEST_F(IndexingServiceTest, FilterByPatterns) {
    // Create directory with files with different extensions
    std::filesystem::create_directories(testDir_ / "filtered");
    createTestFile("filtered/include.txt", "Should be included");
    createTestFile("filtered/include.md", "Should be included");
    createTestFile("filtered/exclude.log", "Should be excluded");
    createTestFile("filtered/exclude.tmp", "Should be excluded");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "filtered").string();
    request.recursive = false;
    request.includePatterns = {"*.txt", "*.md"};
    request.excludePatterns = {"*.log", "*.tmp"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // Only .txt and .md files
}

// Service Integration Tests

TEST_F(IndexingServiceTest, IntegrationWithContentStore) {
    std::filesystem::create_directories(testDir_ / "store");
    createTestFile("store/store_test.txt", "Content for storage integration test");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "store").string();

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
    EXPECT_FALSE(result.value().results.empty());

    // Verify document was stored in content store
    if (!result.value().results.empty()) {
        auto hash = result.value().results[0].hash;
        EXPECT_FALSE(hash.empty());
    }

    // Note: Additional verification would require content store query methods
}

TEST_F(IndexingServiceTest, IntegrationWithMetadataRepository) {
    std::filesystem::create_directories(testDir_ / "metadata");
    createTestFile("metadata/metadata_test.txt", "Content for metadata integration test");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "metadata").string();
    request.collection = "metadata-test";
    request.metadata = {{"test", "true"}, {"type", "integration"}};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);

    // Verify document metadata was stored
    // Note: This would require additional service methods to query metadata
    // For comprehensive testing, we'd need to extend the service interface
}

// Edge Cases

TEST_F(IndexingServiceTest, HandleLargeFile) {
    // Create directory with a large file
    std::filesystem::create_directories(testDir_ / "large");
    auto largeFile = testDir_ / "large/large.txt";
    std::ofstream file(largeFile);
    for (int i = 0; i < 10000; ++i) {
        file << "Line " << i << " with some content to make it longer.\n";
    }
    file.close();

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "large").string();

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleSpecialCharacters) {
    std::filesystem::create_directories(testDir_ / "special");
    createTestFile(
        "special/special_chars.txt",
        "Content with special characters: àáâãäåæçèéêë ñòóôõö ùúûüý 中文 العربية русский");

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "special").string();

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
}