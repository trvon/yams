#include <gtest/gtest.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/migration.h>
#include <yams/search/bk_tree.h>
#include <filesystem>
#include <thread>
#include <chrono>

using namespace yams;
using namespace yams::metadata;
using namespace yams::search;

class MetadataRepositoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use temporary database for tests
        dbPath_ = std::filesystem::temp_directory_path() / "metadata_repo_test.db";
        std::filesystem::remove(dbPath_);
        
        // Create connection pool
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        ASSERT_TRUE(initResult.has_value());
        
        // Create repository
        repository_ = std::make_unique<MetadataRepository>(*pool_);
    }
    
    void TearDown() override {
        repository_.reset();
        pool_->shutdown();
        pool_.reset();
        std::filesystem::remove(dbPath_);
    }
    
    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repository_;
};

TEST_F(MetadataRepositoryTest, InsertAndGetDocument) {
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash123";
    docInfo.fileName = "test.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto result = repository_->insertDocument(docInfo);
    ASSERT_TRUE(result.has_value());
    
    auto docId = result.value();
    EXPECT_GT(docId, 0);
    
    // Verify document was created
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    
    auto retrievedDoc = getResult.value().value();
    EXPECT_EQ(retrievedDoc.id, docId);
    EXPECT_EQ(retrievedDoc.sha256Hash, docInfo.sha256Hash);
    EXPECT_EQ(retrievedDoc.fileName, docInfo.fileName);
    EXPECT_EQ(retrievedDoc.fileSize, docInfo.fileSize);
    EXPECT_EQ(retrievedDoc.mimeType, docInfo.mimeType);
}

TEST_F(MetadataRepositoryTest, GetDocumentNotFound) {
    auto result = repository_->getDocument(999999);
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result.value().has_value());
}

TEST_F(MetadataRepositoryTest, GetDocumentByHash) {
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash456";
    docInfo.fileName = "hash_test.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    
    // Find by hash
    auto findResult = repository_->getDocumentByHash("hash456");
    ASSERT_TRUE(findResult.has_value());
    ASSERT_TRUE(findResult.value().has_value());
    
    auto foundDoc = findResult.value().value();
    EXPECT_EQ(foundDoc.sha256Hash, "hash456");
    EXPECT_EQ(foundDoc.fileName, "hash_test.txt");
}

TEST_F(MetadataRepositoryTest, UpdateDocument) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash789";
    docInfo.fileName = "original.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Update document
    docInfo.id = docId;
    docInfo.fileName = "updated.txt";
    docInfo.fileSize = 4096;
    docInfo.modifiedTime = std::chrono::system_clock::now();
    
    auto updateResult = repository_->updateDocument(docInfo);
    ASSERT_TRUE(updateResult.has_value());
    
    // Verify update
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    
    auto updatedDoc = getResult.value().value();
    EXPECT_EQ(updatedDoc.fileName, "updated.txt");
    EXPECT_EQ(updatedDoc.fileSize, 4096);
}

TEST_F(MetadataRepositoryTest, DeleteDocument) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "hash999";
    docInfo.fileName = "delete_me.txt";
    docInfo.fileSize = 512;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Delete document
    auto deleteResult = repository_->deleteDocument(docId);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    EXPECT_FALSE(getResult.value().has_value());
}

TEST_F(MetadataRepositoryTest, SetAndGetMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "metadata_test";
    docInfo.fileName = "meta.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Set metadata
    MetadataValue value("Test Author");
    
    auto setResult = repository_->setMetadata(docId, "author", value);
    ASSERT_TRUE(setResult.has_value());
    
    // Get metadata
    auto getResult = repository_->getMetadata(docId, "author");
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    
    auto retrievedValue = getResult.value().value();
    EXPECT_EQ(retrievedValue.asString(), "Test Author");
}

TEST_F(MetadataRepositoryTest, GetAllMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "all_metadata_test";
    docInfo.fileName = "meta_all.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Set multiple metadata values
    MetadataValue authorValue("John Doe");
    repository_->setMetadata(docId, "author", authorValue);
    
    MetadataValue yearValue(int64_t(2024));
    repository_->setMetadata(docId, "year", yearValue);
    
    MetadataValue ratingValue(4.5);
    repository_->setMetadata(docId, "rating", ratingValue);
    
    // Get all metadata
    auto getAllResult = repository_->getAllMetadata(docId);
    ASSERT_TRUE(getAllResult.has_value());
    
    auto metadata = getAllResult.value();
    EXPECT_EQ(metadata.size(), 3);
    
    EXPECT_EQ(metadata["author"].asString(), "John Doe");
    EXPECT_EQ(metadata["year"].asInteger(), 2024);
    EXPECT_FLOAT_EQ(metadata["rating"].asReal(), 4.5);
}

TEST_F(MetadataRepositoryTest, RemoveMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.sha256Hash = "remove_metadata_test";
    docInfo.fileName = "remove.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Set metadata
    MetadataValue value("Temporary");
    repository_->setMetadata(docId, "temp", value);
    
    // Verify it exists
    auto getResult = repository_->getMetadata(docId, "temp");
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    
    // Remove metadata
    auto removeResult = repository_->removeMetadata(docId, "temp");
    ASSERT_TRUE(removeResult.has_value());
    
    // Verify removal
    getResult = repository_->getMetadata(docId, "temp");
    ASSERT_TRUE(getResult.has_value());
    EXPECT_FALSE(getResult.value().has_value());
}

TEST_F(MetadataRepositoryTest, SearchFunctionality) {
    // Create document with content
    DocumentInfo docInfo;
    docInfo.sha256Hash = "search_test";
    docInfo.fileName = "search.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdTime = std::chrono::system_clock::now();
    docInfo.modifiedTime = docInfo.createdTime;
    
    auto createResult = repository_->insertDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Index document content
    auto indexResult = repository_->indexDocumentContent(
        docId, "Test Document", "This is a test document with searchable content", "text/plain");
    ASSERT_TRUE(indexResult.has_value());
    
    // Perform search
    auto searchResult = repository_->search("test", 10, 0);
    ASSERT_TRUE(searchResult.has_value());
    
    auto results = searchResult.value();
    EXPECT_GT(results.results.size(), 0);
}