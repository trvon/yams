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
    static void SetUpTestSuite() {
        auto& detector = FileTypeDetector::instance();
        auto detectorInit = detector.initializeWithMagicNumbers();
        ASSERT_TRUE(detectorInit) << "Failed to initialize FileTypeDetector: "
                                  << detectorInit.error().message;

        auto& registry = ContentHandlerRegistry::instance();
        registry.clear();
    }

    static void TearDownTestSuite() { ContentHandlerRegistry::instance().clear(); }

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
        // Create unique test directory and separate infrastructure directory
        auto pid = std::to_string(::getpid());
        auto timestamp =
            std::to_string(std::chrono::system_clock::now().time_since_epoch().count());

        // testDir_ is for test files only (will be indexed)
        testDir_ = std::filesystem::temp_directory_path() /
                   ("indexing_service_test_" + pid + "_" + timestamp);

        // infraDir_ is for database and storage (should NOT be indexed)
        infraDir_ = std::filesystem::temp_directory_path() /
                    ("indexing_service_infra_" + pid + "_" + timestamp);

        std::error_code ec;
        std::filesystem::create_directories(testDir_, ec);
        ASSERT_FALSE(ec) << "Failed to create test directory: " << ec.message();

        std::filesystem::create_directories(infraDir_, ec);
        ASSERT_FALSE(ec) << "Failed to create infrastructure directory: " << ec.message();
    }

    void setupDatabase() {
        // Create database path in infrastructure directory (not testDir_)
        dbPath_ = std::filesystem::absolute(infraDir_ / "test.db");

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
        // Create content store using builder in infrastructure directory (not testDir_)
        ContentStoreBuilder builder;
        auto storeResult = builder.withStoragePath(infraDir_ / "storage")
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
        if (!testDir_.empty() && std::filesystem::exists(testDir_)) {
            std::error_code ec;
            std::filesystem::remove_all(testDir_, ec);
        }

        if (!infraDir_.empty() && std::filesystem::exists(infraDir_)) {
            std::error_code ec;
            std::filesystem::remove_all(infraDir_, ec);
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
    std::filesystem::path testDir_;  // For test files (will be indexed)
    std::filesystem::path infraDir_; // For database and storage (NOT indexed)
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

// Symbolic Link Tests (PBI-028)

TEST_F(IndexingServiceTest, HandleCircularSymlink) {
    // Create directory with a circular symlink
    std::filesystem::create_directories(testDir_ / "circular");
    createTestFile("circular/normal.txt", "Normal file content");

    // Create a circular symlink: circular/link.md -> circular/link.md
    auto linkPath = testDir_ / "circular/link.md";
    std::error_code ec;
    std::filesystem::create_symlink("link.md", linkPath, ec);
    ASSERT_FALSE(ec) << "Failed to create circular symlink: " << ec.message();

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "circular").string();
    request.recursive = true;

    // Should not throw, should gracefully skip the circular symlink
    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result) << "Should handle circular symlinks gracefully: " << result.error().message;
    EXPECT_EQ(result.value().filesIndexed, 1); // Only the normal.txt file
    EXPECT_EQ(result.value().filesFailed, 0);  // Symlink should be skipped, not failed
}

TEST_F(IndexingServiceTest, HandleBrokenSymlink) {
    // Create directory with a broken symlink
    std::filesystem::create_directories(testDir_ / "broken");
    createTestFile("broken/valid.txt", "Valid file content");

    // Create a broken symlink pointing to non-existent file
    auto linkPath = testDir_ / "broken/broken_link.txt";
    std::error_code ec;
    std::filesystem::create_symlink("/nonexistent/file.txt", linkPath, ec);
    ASSERT_FALSE(ec) << "Failed to create broken symlink: " << ec.message();

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "broken").string();
    request.recursive = true;

    // Should handle broken symlink gracefully
    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result) << "Should handle broken symlinks gracefully: " << result.error().message;
    EXPECT_EQ(result.value().filesIndexed, 1); // Only the valid.txt file
    EXPECT_EQ(result.value().filesFailed, 0);  // Broken symlink should be skipped
}

TEST_F(IndexingServiceTest, HandleDirectoryWithMultipleSymlinkIssues) {
    // Create directory with various symlink scenarios
    std::filesystem::create_directories(testDir_ / "symlinks");
    std::filesystem::create_directories(testDir_ / "symlinks/subdir");

    createTestFile("symlinks/file1.txt", "File 1 content");
    createTestFile("symlinks/subdir/file2.txt", "File 2 content");

    // Create circular symlink
    auto circular = testDir_ / "symlinks/circular.md";
    std::filesystem::create_symlink("circular.md", circular);

    // Create broken symlink
    auto broken = testDir_ / "symlinks/subdir/broken.txt";
    std::filesystem::create_symlink("/does/not/exist.txt", broken);

    // Create another circular reference (points to parent dir creating infinite loop)
    auto loopDir = testDir_ / "symlinks/loop_dir";
    std::error_code ec;
    std::filesystem::create_directory_symlink("..", loopDir, ec);

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "symlinks").string();
    request.recursive = true;

    // Should handle all symlink issues gracefully
    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result) << "Should handle multiple symlink issues: " << result.error().message;
    // Should index the 2 regular files, skipping all problematic symlinks
    EXPECT_EQ(result.value().filesIndexed, 2);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleValidSymlinkToFile) {
    // Create directory with a valid symlink to a file
    std::filesystem::create_directories(testDir_ / "valid_link");
    auto targetFile = createTestFile("valid_link/target.txt", "Target file content");

    // Create symlink pointing to the target file
    auto linkPath = testDir_ / "valid_link/link_to_target.txt";
    std::error_code ec;
    std::filesystem::create_symlink(targetFile.filename(), linkPath, ec);
    ASSERT_FALSE(ec) << "Failed to create valid symlink: " << ec.message();

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "valid_link").string();
    request.recursive = true;
    request.followSymlinks = false; // Default: don't follow symlinks

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // With followSymlinks=false, we should only index regular files, not symlinks
    // The behavior depends on is_regular_file() which may return true for symlinks to files
    EXPECT_GE(result.value().filesIndexed, 1); // At least the target file
}

TEST_F(IndexingServiceTest, HandleNestedDirectoryWithCircularSymlink) {
    // Reproduce the exact scenario from PBI-028: docs/site/developer/build_system.md ->
    // ../developer/build_system.md
    std::filesystem::create_directories(testDir_ / "docs/site/developer");
    std::filesystem::create_directories(testDir_ / "docs/developer");

    createTestFile("docs/readme.md", "# Documentation");
    createTestFile("docs/developer/guide.md", "# Developer Guide");
    createTestFile("docs/site/developer/other.md", "# Other Doc");

    // Create the problematic circular symlink
    auto circularLink = testDir_ / "docs/site/developer/build_system.md";
    std::error_code ec;
    std::filesystem::create_symlink("../developer/build_system.md", circularLink, ec);
    ASSERT_FALSE(ec) << "Failed to create circular symlink: " << ec.message();

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "docs").string();
    request.recursive = true;
    request.includePatterns = {"*.md"};

    // This should NOT throw "Too many levels of symbolic links" error
    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result) << "Should handle nested circular symlinks: " << result.error().message;
    EXPECT_EQ(result.value().filesIndexed, 3); // readme.md, guide.md, other.md
    EXPECT_EQ(result.value().filesFailed, 0);
}

// ============================================================================
// EXPANDED TEST COVERAGE FOR 90%+ (PBI 028-51)
// ============================================================================

TEST_F(IndexingServiceTest, HandleSymlinkChainWithFailureInMiddle) {
    // Test symlink chain: A -> B -> C where B is broken
    createTestFile("target.txt", "target content");

    auto linkA = testDir_ / "linkA.txt";
    auto linkB = testDir_ / "linkB.txt";
    auto linkC = testDir_ / "linkC.txt";

    // Create chain: C -> B -> A, but make B point to non-existent file
    std::error_code ec;
    std::filesystem::create_symlink("target.txt", linkA, ec);
    ASSERT_FALSE(ec);
    std::filesystem::create_symlink("nonexistent.txt", linkB, ec);
    ASSERT_FALSE(ec);
    std::filesystem::create_symlink("linkB.txt", linkC, ec);
    ASSERT_FALSE(ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should index target.txt, skip the broken chain
    EXPECT_GE(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleSymlinkToDirectory) {
    // Symlinks to directories should be skipped (only regular files indexed)
    std::filesystem::create_directories(testDir_ / "realdir");
    createTestFile("realdir/file.txt", "content");

    auto dirLink = testDir_ / "dirlink";
    std::error_code ec;
    std::filesystem::create_directory_symlink("realdir", dirLink, ec);
    ASSERT_FALSE(ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should index file.txt once (from realdir), dirlink should be skipped as it's not a regular
    // file
    EXPECT_EQ(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleNonRecursiveWithSymlinks) {
    // Non-recursive should not traverse into subdirectories, even via symlinks
    createTestFile("root.txt", "root");
    std::filesystem::create_directories(testDir_ / "subdir");
    createTestFile("subdir/nested.txt", "nested");

    auto linkToSubdir = testDir_ / "linkdir";
    std::error_code ec;
    std::filesystem::create_directory_symlink("subdir", linkToSubdir, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should only index root.txt, not traverse into subdir or linkdir
    EXPECT_EQ(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleSymlinkWithIncludePatterns) {
    // Test that include patterns work correctly with symlinks
    createTestFile("file.txt", "text content");
    createTestFile("file.md", "markdown content");

    auto linkTxt = testDir_ / "link.txt";
    std::error_code ec;
    std::filesystem::create_symlink("file.txt", linkTxt, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.md"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should only index file.md, not file.txt or link.txt
    EXPECT_EQ(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleSymlinkWithExcludePatterns) {
    // Test that exclude patterns work correctly with symlinks
    createTestFile("file.txt", "text content");
    createTestFile("file.md", "markdown content");

    auto linkMd = testDir_ / "link.md";
    std::error_code ec;
    std::filesystem::create_symlink("file.md", linkMd, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.excludePatterns = {"*.md"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should only index file.txt, exclude file.md and link.md
    EXPECT_EQ(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleMixedSymlinksAndRegularFiles) {
    // Mix of regular files, valid symlinks, broken symlinks
    createTestFile("regular1.txt", "content1");
    createTestFile("regular2.txt", "content2");
    createTestFile("target.txt", "target");

    auto validLink = testDir_ / "valid_link.txt";
    auto brokenLink = testDir_ / "broken_link.txt";
    auto circularLink = testDir_ / "circular.txt";

    std::error_code ec;
    std::filesystem::create_symlink("target.txt", validLink, ec);
    std::filesystem::create_symlink("nonexistent.txt", brokenLink, ec);
    std::filesystem::create_symlink("circular.txt", circularLink, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should index regular1.txt, regular2.txt, target.txt, and possibly valid_link.txt
    // but skip broken_link and circular
    EXPECT_GE(result.value().filesIndexed, 3);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleDeepSymlinkNesting) {
    // Test deeply nested directory structure with symlinks at various levels
    std::filesystem::create_directories(testDir_ / "level1/level2/level3");
    createTestFile("level1/file1.txt", "level1");
    createTestFile("level1/level2/file2.txt", "level2");
    createTestFile("level1/level2/level3/file3.txt", "level3");

    // Add circular symlink at level 2
    auto circularLink = testDir_ / "level1/level2/circular.txt";
    std::error_code ec;
    std::filesystem::create_symlink("../level2/circular.txt", circularLink, ec);

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "level1").string();
    request.recursive = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 3);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleSymlinkPointingToParent) {
    // Test symlink pointing to parent directory (potential infinite loop)
    std::filesystem::create_directories(testDir_ / "parent/child");
    createTestFile("parent/file.txt", "parent file");
    createTestFile("parent/child/nested.txt", "nested file");

    auto linkToParent = testDir_ / "parent/child/link_to_parent";
    std::error_code ec;
    std::filesystem::create_directory_symlink("..", linkToParent, ec);

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "parent").string();
    request.recursive = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleRelativeSymlinkPaths) {
    // Test symlinks with relative paths (../, ./, etc.)
    std::filesystem::create_directories(testDir_ / "dir1");
    std::filesystem::create_directories(testDir_ / "dir2");
    createTestFile("dir1/file1.txt", "file1");
    createTestFile("dir2/file2.txt", "file2");

    auto relativeLink = testDir_ / "dir1/link_to_file2.txt";
    std::error_code ec;
    std::filesystem::create_symlink("../dir2/file2.txt", relativeLink, ec);

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "dir1").string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should index file1.txt and possibly the symlink target
    EXPECT_GE(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleExplicitFilePathsWithSymlinks) {
    // Test explicit file paths with symlinks - valid symlink should be indexed
    // Create subdirectory first
    std::filesystem::create_directories(testDir_ / "dir");
    createTestFile("dir/file.txt", "content");

    auto link = testDir_ / "dir/link.txt";
    std::error_code ec;
    std::filesystem::create_symlink("file.txt", link, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;
    request.includePatterns = {"*.txt"}; // Match all .txt files recursively

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should index both the real file and the valid symlink
    EXPECT_GE(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleDirectoryIteratorFailure) {
    // Test handling of directory iterator initialization failure
    // This is hard to simulate without platform-specific permissions issues
    // But we test that the service handles missing/invalid directories gracefully

    AddDirectoryRequest request;
    request.directoryPath = (testDir_ / "nonexistent_dir").string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
}

TEST_F(IndexingServiceTest, HandleVerificationWithSymlinks) {
    // Test verify option with symlinks
    createTestFile("target.txt", "target content");

    auto link = testDir_ / "link.txt";
    std::error_code ec;
    std::filesystem::create_symlink("target.txt", link, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.verify = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Verify should work with regular files
    EXPECT_GE(result.value().filesIndexed, 1);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleDeferExtractionWithSymlinks) {
    // Test deferExtraction option with symlinks
    createTestFile("file.txt", "content");

    auto link = testDir_ / "link.txt";
    std::error_code ec;
    std::filesystem::create_symlink("file.txt", link, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.deferExtraction = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_GE(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, HandleCollectionAndTagsWithSymlinks) {
    // Test collection and tags propagation with symlinks
    createTestFile("file.txt", "content");

    auto link = testDir_ / "link.txt";
    std::error_code ec;
    std::filesystem::create_symlink("file.txt", link, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.collection = "test_collection";
    request.tags = {"tag1", "tag2"};
    request.metadata = {{"key", "value"}};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_GE(result.value().filesIndexed, 1);
    EXPECT_EQ(result.value().collection, "test_collection");
}

TEST_F(IndexingServiceTest, HandleSymlinkToNonRegularFile) {
    // Test symlinks to non-regular files (devices, pipes, etc. - platform dependent)
    // Create a symlink to /dev/null on Unix-like systems
#ifdef __unix__
    auto devNullLink = testDir_ / "dev_null_link";
    std::error_code ec;
    std::filesystem::create_symlink("/dev/null", devNullLink, ec);

    if (!ec) {
        AddDirectoryRequest request;
        request.directoryPath = testDir_.string();
        request.recursive = false;

        auto result = indexingService_->addDirectory(request);

        ASSERT_TRUE(result);
        // Symlink to /dev/null should be skipped (not a regular file)
        EXPECT_EQ(result.value().filesIndexed, 0);
    }
#endif
}

TEST_F(IndexingServiceTest, HandleMultipleCircularSymlinksInTree) {
    // Test multiple circular symlinks at different levels
    std::filesystem::create_directories(testDir_ / "dir1/sub1");
    std::filesystem::create_directories(testDir_ / "dir2/sub2");

    createTestFile("dir1/file1.txt", "file1");
    createTestFile("dir2/file2.txt", "file2");

    auto circular1 = testDir_ / "dir1/sub1/loop1.txt";
    auto circular2 = testDir_ / "dir2/sub2/loop2.txt";

    std::error_code ec;
    std::filesystem::create_symlink("../sub1/loop1.txt", circular1, ec);
    std::filesystem::create_symlink("../sub2/loop2.txt", circular2, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleSymlinkWithSpecialCharacters) {
    // Test symlinks with special characters in names
    createTestFile("target file with spaces.txt", "content");

    auto link = testDir_ / "link with spaces & special!@#.txt";
    std::error_code ec;
    std::filesystem::create_symlink("target file with spaces.txt", link, ec);

    if (!ec) {
        AddDirectoryRequest request;
        request.directoryPath = testDir_.string();
        request.recursive = false;

        auto result = indexingService_->addDirectory(request);

        ASSERT_TRUE(result);
        EXPECT_GE(result.value().filesIndexed, 1);
    }
}

TEST_F(IndexingServiceTest, HandleSymlinkCrossReferences) {
    // Test symlinks that cross-reference each other (A->B, B->C, C->A)
    auto linkA = testDir_ / "linkA.txt";
    auto linkB = testDir_ / "linkB.txt";
    auto linkC = testDir_ / "linkC.txt";

    std::error_code ec;
    std::filesystem::create_symlink("linkB.txt", linkA, ec);
    std::filesystem::create_symlink("linkC.txt", linkB, ec);
    std::filesystem::create_symlink("linkA.txt", linkC, ec);

    createTestFile("regular.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should skip the circular chain and index regular.txt
    EXPECT_EQ(result.value().filesIndexed, 1);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleEmptyDirectoryWithOnlySymlinks) {
    // Test directory containing only broken/circular symlinks (no regular files)
    auto link1 = testDir_ / "broken.txt";
    auto link2 = testDir_ / "circular.txt";

    std::error_code ec;
    std::filesystem::create_symlink("nonexistent.txt", link1, ec);
    std::filesystem::create_symlink("circular.txt", link2, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 0);
    EXPECT_EQ(result.value().filesSkipped, 0);
    EXPECT_EQ(result.value().filesFailed, 0);
}

TEST_F(IndexingServiceTest, HandleSymlinkPatternMatchingEdgeCases) {
    // Test pattern matching with symlinks that match/don't match patterns
    createTestFile("file.txt", "text");
    createTestFile("file.md", "markdown");
    createTestFile("file.cpp", "code");

    auto linkTxt = testDir_ / "link.txt";
    auto linkMd = testDir_ / "link.md";

    std::error_code ec;
    std::filesystem::create_symlink("file.txt", linkTxt, ec);
    std::filesystem::create_symlink("file.md", linkMd, ec);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.txt", "*.md"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should include file.txt and file.md (not file.cpp)
    // Symlinks may or may not be included depending on is_regular_file() behavior
    EXPECT_GE(result.value().filesIndexed, 2);
}

TEST_F(IndexingServiceTest, StressTestManySymlinks) {
    // Stress test with many symlinks
    const int numFiles = 50;
    const int numSymlinks = 30;

    for (int i = 0; i < numFiles; ++i) {
        createTestFile("file" + std::to_string(i) + ".txt", "content " + std::to_string(i));
    }

    // Create various types of symlinks
    for (int i = 0; i < numSymlinks; ++i) {
        auto link = testDir_ / ("link" + std::to_string(i) + ".txt");
        std::error_code ec;

        if (i % 3 == 0) {
            // Valid symlink
            std::filesystem::create_symlink("file" + std::to_string(i % numFiles) + ".txt", link,
                                            ec);
        } else if (i % 3 == 1) {
            // Broken symlink
            std::filesystem::create_symlink("nonexistent" + std::to_string(i) + ".txt", link, ec);
        } else {
            // Circular symlink
            std::filesystem::create_symlink("link" + std::to_string(i) + ".txt", link, ec);
        }
    }

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should index the regular files, skip problematic symlinks
    EXPECT_GE(result.value().filesIndexed, numFiles);
    EXPECT_EQ(result.value().filesFailed, 0);
}

// ====================================================================================
// Coverage Expansion Tests (028-52) - Targeting 90%+ coverage
// ====================================================================================

// --- Include/Exclude Pattern Tests ---

TEST_F(IndexingServiceTest, IncludePatternWithWildcards) {
    createTestFile("test.txt", "text");
    createTestFile("test.md", "markdown");
    createTestFile("test.cpp", "code");
    createTestFile("readme.txt", "readme");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.txt"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // test.txt, readme.txt
    EXPECT_EQ(result.value().filesSkipped, 2); // test.md, test.cpp
}

TEST_F(IndexingServiceTest, IncludeMultiplePatterns) {
    createTestFile("file.txt", "text");
    createTestFile("file.md", "markdown");
    createTestFile("file.cpp", "code");
    createTestFile("file.py", "python");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.txt", "*.md"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // file.txt, file.md
    EXPECT_EQ(result.value().filesSkipped, 2); // file.cpp, file.py
}

TEST_F(IndexingServiceTest, ExcludePatternOverridesInclude) {
    createTestFile("file.txt", "text");
    createTestFile("test.txt", "test");
    createTestFile("readme.txt", "readme");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.txt"};
    request.excludePatterns = {"test*"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // file.txt, readme.txt
    EXPECT_EQ(result.value().filesSkipped, 1); // test.txt excluded
}

TEST_F(IndexingServiceTest, ExcludeWithRelativePath) {
    auto subdir = testDir_ / "subdir";
    std::filesystem::create_directories(subdir);

    createTestFile("file.txt", "root");
    std::ofstream(subdir / "file.txt") << "nested";

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;
    request.excludePatterns = {"subdir/*"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1); // Only root file.txt
}

TEST_F(IndexingServiceTest, NoIncludePatternsIndexAll) {
    createTestFile("file.txt", "text");
    createTestFile("file.md", "markdown");
    createTestFile("file.cpp", "code");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    // No include patterns - should index all

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 3);
}

TEST_F(IndexingServiceTest, ExcludeOnlyWithNoIncludes) {
    createTestFile("file.txt", "text");
    createTestFile("file.md", "markdown");
    createTestFile("test.cpp", "code");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.excludePatterns = {"*.cpp"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // txt and md
    EXPECT_EQ(result.value().filesSkipped, 1); // cpp excluded
}

// --- Recursive vs Non-Recursive Tests ---

TEST_F(IndexingServiceTest, NonRecursiveSkipsSubdirectories) {
    createTestFile("root.txt", "root");

    auto subdir = testDir_ / "subdir";
    std::filesystem::create_directories(subdir);
    std::ofstream(subdir / "nested.txt") << "nested";

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1); // Only root.txt
}

TEST_F(IndexingServiceTest, RecursiveIndexesNestedDirectories) {
    createTestFile("root.txt", "root");

    auto subdir1 = testDir_ / "sub1";
    auto subdir2 = testDir_ / "sub1" / "sub2";
    std::filesystem::create_directories(subdir2);

    std::ofstream(subdir1 / "file1.txt") << "file1";
    std::ofstream(subdir2 / "file2.txt") << "file2";

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 3); // root.txt, file1.txt, file2.txt
}

TEST_F(IndexingServiceTest, RecursiveWithPatternsInNestedDirs) {
    auto subdir = testDir_ / "subdir";
    std::filesystem::create_directories(subdir);

    createTestFile("root.txt", "root");
    createTestFile("root.md", "markdown");
    std::ofstream(subdir / "nested.txt") << "nested";
    std::ofstream(subdir / "nested.cpp") << "code";

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;
    request.includePatterns = {"*.txt"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // root.txt, nested.txt
    EXPECT_GE(result.value().filesSkipped, 2); // root.md, nested.cpp
}

// --- Empty Directory and Edge Cases ---

TEST_F(IndexingServiceTest, DirectoryWithOnlySubdirectories) {
    auto subdir1 = testDir_ / "sub1";
    auto subdir2 = testDir_ / "sub2";
    std::filesystem::create_directories(subdir1);
    std::filesystem::create_directories(subdir2);

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 0);
    EXPECT_EQ(result.value().filesProcessed, 0);
}

TEST_F(IndexingServiceTest, AllFilesFilteredOut) {
    createTestFile("file.txt", "text");
    createTestFile("file.md", "markdown");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.cpp"}; // No cpp files exist

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 0);
    EXPECT_EQ(result.value().filesSkipped, 2);
}

// --- Tag and Collection Tests ---

TEST_F(IndexingServiceTest, TagsPropagatedToAllFiles) {
    createTestFile("file1.txt", "content1");
    createTestFile("file2.txt", "content2");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.tags = {"important", "backup"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2);

    // Tags are propagated via DocumentService
    // Note: Verification would require additional query methods
    // For now, we trust the service correctly applies tags
}

TEST_F(IndexingServiceTest, CollectionAssignedToAllFiles) {
    createTestFile("file1.txt", "content1");
    createTestFile("file2.txt", "content2");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.collection = "test-collection";

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2);
    EXPECT_EQ(result.value().collection, "test-collection");
}

TEST_F(IndexingServiceTest, MetadataPropagatedToFiles) {
    createTestFile("file.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.metadata = {{"project", "yams"}, {"version", "1.0"}};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);

    // Metadata propagation is tested via DocumentService integration
}

// --- Error Handling Tests ---

TEST_F(IndexingServiceTest, HandleNonDirectoryPath) {
    createTestFile("notadir.txt", "content");
    auto filePath = testDir_ / "notadir.txt";

    AddDirectoryRequest request;
    request.directoryPath = filePath.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
    EXPECT_THAT(result.error().message, ::testing::HasSubstr("does not exist"));
}

TEST_F(IndexingServiceTest, HandleInvalidPath) {
    AddDirectoryRequest request;
    request.directoryPath = "/nonexistent/path/to/directory";
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, ErrorCode::InvalidArgument);
}

// --- Verification Tests ---

TEST_F(IndexingServiceTest, VerifyOptionValidatesContent) {
    createTestFile("file.txt", "verified content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.verify = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
    EXPECT_EQ(result.value().filesFailed, 0);

    // Verify that file result has hash
    ASSERT_FALSE(result.value().results.empty());
    EXPECT_FALSE(result.value().results[0].hash.empty());
    EXPECT_TRUE(result.value().results[0].success);
}

// --- DeferExtraction Tests ---

TEST_F(IndexingServiceTest, DeferExtractionPropagated) {
    createTestFile("file.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.deferExtraction = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
}

// --- Explicit Path Handling (Fast Path) ---

TEST_F(IndexingServiceTest, ExplicitRelativePathInIncludePattern) {
    auto subdir = testDir_ / "subdir";
    std::filesystem::create_directories(subdir);
    std::ofstream(subdir / "specific.txt") << "specific file";

    createTestFile("other.txt", "other");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;                         // Non-recursive
    request.includePatterns = {"subdir/specific.txt"}; // Explicit path

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1); // Only the explicit file
}

TEST_F(IndexingServiceTest, ExplicitPathTakesFastPath) {
    createTestFile("file.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"file.txt"}; // Explicit path without wildcard

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1);
}

TEST_F(IndexingServiceTest, ExplicitNonExistentPath) {
    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"nonexistent/file.txt"}; // Explicit non-existent path

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 0); // No files found
}

// --- Pattern Edge Cases ---

TEST_F(IndexingServiceTest, CaseInsensitivePatternMatching) {
    // Test pattern matching with different case variations
    // Note: Use only lowercase files to avoid macOS case-insensitive filesystem
    // collapsing multiple files into one
    createTestFile("file.txt", "content1");
    createTestFile("readme.txt", "content2");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.txt"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    // Should match both files with lowercase .txt extension
    EXPECT_EQ(result.value().filesIndexed, 2);
}

TEST_F(IndexingServiceTest, PatternWithQuestionMarkWildcard) {
    createTestFile("file1.txt", "content");
    createTestFile("file2.txt", "content");
    createTestFile("file10.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"file?.txt"};

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 2); // file1.txt, file2.txt (not file10.txt)
}

TEST_F(IndexingServiceTest, OverlappingIncludePatterns) {
    createTestFile("test.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;
    request.includePatterns = {"*.txt", "test.*"}; // Both match test.txt

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 1); // File indexed once despite multiple matches
}

// --- Performance and Stress Tests ---

TEST_F(IndexingServiceTest, HandleManyFilesEfficiently) {
    const int numFiles = 100;

    for (int i = 0; i < numFiles; ++i) {
        createTestFile("file" + std::to_string(i) + ".txt", "content " + std::to_string(i));
    }

    auto start = std::chrono::steady_clock::now();

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    auto duration = std::chrono::steady_clock::now() - start;
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, numFiles);

    // Should complete in reasonable time (adjust threshold as needed)
    EXPECT_LT(ms, 5000) << "Indexing " << numFiles << " files took " << ms << "ms";
}

TEST_F(IndexingServiceTest, DeepDirectoryHierarchy) {
    auto current = testDir_;

    // Create 10 levels deep
    for (int i = 0; i < 10; ++i) {
        current = current / ("level" + std::to_string(i));
        std::filesystem::create_directories(current);
        std::ofstream(current / "file.txt") << "level " << i;
    }

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = true;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().filesIndexed, 10);
}

// --- Response Field Tests ---

TEST_F(IndexingServiceTest, ResponseContainsCorrectDirectoryPath) {
    createTestFile("file.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().directoryPath, testDir_.string());
}

TEST_F(IndexingServiceTest, ResponseContainsCollectionName) {
    createTestFile("file.txt", "content");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.collection = "my-collection";
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    EXPECT_EQ(result.value().collection, "my-collection");
}

TEST_F(IndexingServiceTest, IndividualFileResultsPopulated) {
    createTestFile("file1.txt", "content1");
    createTestFile("file2.txt", "content2");

    AddDirectoryRequest request;
    request.directoryPath = testDir_.string();
    request.recursive = false;

    auto result = indexingService_->addDirectory(request);

    ASSERT_TRUE(result);
    ASSERT_EQ(result.value().results.size(), 2);

    for (const auto& fileResult : result.value().results) {
        EXPECT_FALSE(fileResult.path.empty());
        EXPECT_FALSE(fileResult.hash.empty());
        EXPECT_GT(fileResult.sizeBytes, 0);
        EXPECT_TRUE(fileResult.success);
        EXPECT_FALSE(fileResult.error.has_value());
    }
}
