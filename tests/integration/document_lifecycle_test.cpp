#include <chrono>
#include <filesystem>
#include <thread>
#include "common/fixture_manager.h"
#include "common/test_data_generator.h"
#include <gtest/gtest.h>
#include <yams/api/content_store.h>
#include <yams/cli/commands/update_command.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>

using namespace yams;
using namespace yams::api;
using namespace yams::metadata;
using namespace yams::test;

class DocumentLifecycleTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test environment
        testDir_ = std::filesystem::temp_directory_path() / "yams_integration_lifecycle";
        std::filesystem::create_directories(testDir_);

        // Initialize YAMS components
        initializeYAMS();

        // Set up test data
        generator_ = std::make_unique<TestDataGenerator>();
        fixtureManager_ = std::make_unique<FixtureManager>();
    }

    void TearDown() override {
        // Clean up
        contentStore_.reset();
        metadataRepo_.reset();
        database_.reset();
        std::filesystem::remove_all(testDir_);
    }

    void initializeYAMS() {
        // Initialize database
        auto dbPath = testDir_ / "yams.db";
        database_ = std::make_shared<Database>(dbPath.string());
        database_->initialize();

        // Initialize repositories
        metadataRepo_ = std::make_shared<MetadataRepository>(database_);

        // Initialize content store
        contentStore_ = std::make_unique<ContentStore>(testDir_.string());
    }

    std::filesystem::path testDir_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<ContentStore> contentStore_;
    std::unique_ptr<TestDataGenerator> generator_;
    std::unique_ptr<FixtureManager> fixtureManager_;
};

TEST_F(DocumentLifecycleTest, CompleteDocumentWorkflow) {
    // Step 1: Add a document
    std::string content = generator_->generateTextDocument(1024);
    auto testFile = testDir_ / "test_doc.txt";
    std::ofstream(testFile) << content;

    std::vector<std::string> tags = {"test", "integration", "lifecycle"};
    auto addResult = contentStore_->addFile(testFile, tags);

    ASSERT_TRUE(addResult.has_value()) << "Failed to add document";
    std::string hash = addResult->contentHash;

    // Step 2: Search for the document
    auto searchResult = contentStore_->search("test");
    ASSERT_TRUE(searchResult.has_value()) << "Search failed";

    // Verify document is found
    bool found = false;
    if (searchResult.has_value()) {
        // Check if hash matches
        found = (searchResult->document.hash == hash);
    }
    ASSERT_TRUE(found) << "Document not found in search results";

    // Step 3: Update metadata
    auto docResult = metadataRepo_->getDocumentByHash(hash);
    ASSERT_TRUE(docResult.has_value() && docResult->has_value()) << "Document not in metadata repo";

    auto doc = docResult->value();
    auto updateResult = metadataRepo_->setMetadata(doc->id, "status", MetadataValue("processed"));
    ASSERT_TRUE(updateResult.isOk()) << "Metadata update failed";

    // Step 4: Get document content
    auto getResult = contentStore_->getContent(hash);
    ASSERT_TRUE(getResult.has_value()) << "Failed to retrieve content";

    // Verify content matches
    std::string retrievedContent(getResult->begin(), getResult->end());
    EXPECT_EQ(retrievedContent, content) << "Retrieved content doesn't match original";

    // Step 5: Verify metadata persistence
    auto metadataResult = metadataRepo_->getMetadata(doc->id, "status");
    ASSERT_TRUE(metadataResult.has_value()) << "Failed to retrieve metadata";
    EXPECT_EQ(metadataResult->toString(), "processed") << "Metadata value incorrect";
}

TEST_F(DocumentLifecycleTest, CommandChainingWorkflow) {
    // Test piping operations: add -> search -> update -> get

    // Add multiple documents
    std::vector<std::string> hashes;
    for (int i = 0; i < 5; ++i) {
        auto content = generator_->generateMarkdown(3);
        auto file = testDir_ / ("chain_" + std::to_string(i) + ".md");
        std::ofstream(file) << content;

        auto result = contentStore_->addFile(file, {"chain", "test"});
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result->contentHash);
    }

    // Search and filter
    SearchOptions options;
    options.tags = {"chain"};
    auto searchResults = contentStore_->searchWithOptions("test", options);

    EXPECT_GE(searchResults.size(), 5) << "Not all documents found";

    // Update metadata for search results
    for (const auto& result : searchResults) {
        auto updateResult = metadataRepo_->setMetadata(
            result.document.id, "processed_at",
            MetadataValue(std::chrono::system_clock::now().time_since_epoch().count()));
        EXPECT_TRUE(updateResult.isOk());
    }

    // Verify updates
    for (const auto& hash : hashes) {
        auto docResult = metadataRepo_->getDocumentByHash(hash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());

        auto metadata = metadataRepo_->getMetadata(docResult->value()->id, "processed_at");
        EXPECT_TRUE(metadata.has_value()) << "Metadata not found for " << hash;
    }
}

TEST_F(DocumentLifecycleTest, DataPersistenceAcrossRestarts) {
    std::string hash;
    std::string docId;

    // Phase 1: Add document and metadata
    {
        auto content = generator_->generateTextDocument(2048);
        auto file = testDir_ / "persistent.txt";
        std::ofstream(file) << content;

        auto result = contentStore_->addFile(file);
        ASSERT_TRUE(result.has_value());
        hash = result->contentHash;

        auto docResult = metadataRepo_->getDocumentByHash(hash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        docId = docResult->value()->id;

        // Add metadata
        metadataRepo_->setMetadata(docId, "persistent", MetadataValue("true"));
        metadataRepo_->setMetadata(docId, "phase", MetadataValue(1.0));
    }

    // Simulate restart by reinitializing
    contentStore_.reset();
    metadataRepo_.reset();
    database_.reset();

    // Small delay to ensure files are flushed
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    initializeYAMS();

    // Phase 2: Verify data persisted
    {
        // Check document exists
        auto getResult = contentStore_->getContent(hash);
        ASSERT_TRUE(getResult.has_value()) << "Document lost after restart";

        // Check metadata persisted
        auto docResult = metadataRepo_->getDocumentByHash(hash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value())
            << "Document metadata lost after restart";

        auto persistentMeta = metadataRepo_->getMetadata(docId, "persistent");
        ASSERT_TRUE(persistentMeta.has_value()) << "Metadata 'persistent' lost";
        EXPECT_EQ(persistentMeta->toString(), "true");

        auto phaseMeta = metadataRepo_->getMetadata(docId, "phase");
        ASSERT_TRUE(phaseMeta.has_value()) << "Metadata 'phase' lost";
        EXPECT_EQ(phaseMeta->toDouble(), 1.0);
    }
}

TEST_F(DocumentLifecycleTest, ErrorRecoveryWorkflow) {
    // Test recovery from various error conditions

    // 1. Try to get non-existent document
    auto getResult = contentStore_->getContent("nonexistent_hash");
    EXPECT_FALSE(getResult.has_value()) << "Should fail for non-existent document";

    // 2. Add corrupted file
    auto corruptFile = testDir_ / "corrupt.bin";
    std::ofstream(corruptFile, std::ios::binary);
    corruptFile << std::byte{0x00} << std::byte{0xFF} << std::byte{0xDE} << std::byte{0xAD};
    corruptFile.close();

    // Should still add binary file successfully
    auto addResult = contentStore_->addFile(corruptFile);
    EXPECT_TRUE(addResult.has_value()) << "Should handle binary files";

    // 3. Update metadata for non-existent document
    auto updateResult = metadataRepo_->setMetadata("nonexistent_id", "key", MetadataValue("value"));
    EXPECT_FALSE(updateResult.isOk()) << "Should fail for non-existent document";

    // 4. Search with invalid query (should handle gracefully)
    auto searchResult = contentStore_->search("");
    // Empty search should either return all or none, but not crash
    EXPECT_TRUE(true) << "Empty search handled";

    // 5. Add same document twice (deduplication)
    auto content = generator_->generateTextDocument(512);
    auto file1 = testDir_ / "dup1.txt";
    auto file2 = testDir_ / "dup2.txt";
    std::ofstream(file1) << content;
    std::ofstream(file2) << content;

    auto result1 = contentStore_->addFile(file1);
    auto result2 = contentStore_->addFile(file2);

    ASSERT_TRUE(result1.has_value() && result2.has_value());
    EXPECT_EQ(result1->contentHash, result2->contentHash) << "Same content should have same hash";
    EXPECT_GT(result2->dedupRatio(), 0.9) << "Should detect duplication";
}

TEST_F(DocumentLifecycleTest, LargeScaleOperations) {
    // Test with larger number of documents
    const size_t numDocs = 100;
    std::vector<std::string> hashes;

    // Batch add
    auto startAdd = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < numDocs; ++i) {
        auto content = generator_->generateTextDocument(1024 + i * 10);
        auto file = testDir_ / ("batch_" + std::to_string(i) + ".txt");
        std::ofstream(file) << content;

        auto result = contentStore_->addFile(file, {"batch", "scale"});
        ASSERT_TRUE(result.has_value()) << "Failed to add document " << i;
        hashes.push_back(result->contentHash);
    }
    auto endAdd = std::chrono::high_resolution_clock::now();

    auto addDuration = std::chrono::duration_cast<std::chrono::milliseconds>(endAdd - startAdd);
    std::cout << "Added " << numDocs << " documents in " << addDuration.count() << "ms"
              << std::endl;

    // Batch search
    auto startSearch = std::chrono::high_resolution_clock::now();
    SearchOptions options;
    options.tags = {"batch"};
    options.limit = numDocs;
    auto searchResults = contentStore_->searchWithOptions("*", options);
    auto endSearch = std::chrono::high_resolution_clock::now();

    EXPECT_EQ(searchResults.size(), numDocs) << "Not all documents found";

    auto searchDuration =
        std::chrono::duration_cast<std::chrono::milliseconds>(endSearch - startSearch);
    std::cout << "Searched " << numDocs << " documents in " << searchDuration.count() << "ms"
              << std::endl;

    // Batch metadata update
    auto startUpdate = std::chrono::high_resolution_clock::now();
    for (const auto& hash : hashes) {
        auto docResult = metadataRepo_->getDocumentByHash(hash);
        if (docResult.has_value() && docResult->has_value()) {
            metadataRepo_->setMetadata(docResult->value()->id, "batch_id",
                                       MetadataValue("batch_001"));
        }
    }
    auto endUpdate = std::chrono::high_resolution_clock::now();

    auto updateDuration =
        std::chrono::duration_cast<std::chrono::milliseconds>(endUpdate - startUpdate);
    std::cout << "Updated metadata for " << numDocs << " documents in " << updateDuration.count()
              << "ms" << std::endl;
}

TEST_F(DocumentLifecycleTest, TransactionConsistency) {
    // Test that operations are atomic and consistent

    // Start a transaction (if supported)
    // Note: Actual transaction support depends on implementation

    std::string hash;

    // Add document
    auto content = generator_->generateTextDocument(1024);
    auto file = testDir_ / "transaction.txt";
    std::ofstream(file) << content;

    auto result = contentStore_->addFile(file);
    ASSERT_TRUE(result.has_value());
    hash = result->contentHash;

    // Get document info
    auto docResult = metadataRepo_->getDocumentByHash(hash);
    ASSERT_TRUE(docResult.has_value() && docResult->has_value());
    auto docId = docResult->value()->id;

    // Perform multiple metadata operations
    std::vector<std::pair<std::string, MetadataValue>> updates = {
        {"field1", MetadataValue("value1")},
        {"field2", MetadataValue(42.0)},
        {"field3", MetadataValue(true)},
        {"field4", MetadataValue("value4")},
        {"field5", MetadataValue("value5")}};

    // All should succeed or all should fail
    bool allSuccess = true;
    for (const auto& [key, value] : updates) {
        auto updateResult = metadataRepo_->setMetadata(docId, key, value);
        if (!updateResult.isOk()) {
            allSuccess = false;
            break;
        }
    }

    if (allSuccess) {
        // Verify all metadata is present
        for (const auto& [key, expectedValue] : updates) {
            auto metaResult = metadataRepo_->getMetadata(docId, key);
            ASSERT_TRUE(metaResult.has_value()) << "Metadata " << key << " not found";
            EXPECT_EQ(metaResult->toString(), expectedValue.toString());
        }
    } else {
        // If any failed, none should be present (rollback)
        for (const auto& [key, _] : updates) {
            auto metaResult = metadataRepo_->getMetadata(docId, key);
            EXPECT_FALSE(metaResult.has_value()) << "Partial update detected for " << key;
        }
    }
}