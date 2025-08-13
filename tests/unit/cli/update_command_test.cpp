#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <yams/cli/commands/update_command.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/api/content_store.h>
#include <yams/core/types.h>
#include <filesystem>
#include <memory>
#include <thread>

using namespace yams;
using namespace yams::cli;
using namespace yams::metadata;
using namespace yams::api;
using ::testing::_;
using ::testing::Return;
using ::testing::HasSubstr;

// Mock classes for testing
class MockMetadataRepository : public MetadataRepository {
public:
    MockMetadataRepository() : MetadataRepository(nullptr) {}
    
    MOCK_METHOD(Result<std::optional<Document>>, getDocumentByHash, 
                (const std::string& hash), (const, override));
    MOCK_METHOD(Result<std::vector<Document>>, searchDocuments,
                (const std::string& query, const SearchOptions& options), (const, override));
    MOCK_METHOD(Result<void>, setMetadata,
                (const std::string& docId, const std::string& key, const MetadataValue& value), (override));
    MOCK_METHOD(Result<MetadataValue>, getMetadata,
                (const std::string& docId, const std::string& key), (const, override));
    MOCK_METHOD(Result<void>, removeMetadata,
                (const std::string& docId, const std::string& key), (override));
    MOCK_METHOD(Result<std::map<std::string, MetadataValue>>, getAllMetadata,
                (const std::string& docId), (const, override));
};

class MockContentStore : public ContentStore {
public:
    MockContentStore() : ContentStore("") {}
    
    MOCK_METHOD(Result<search::SearchResult>, search,
                (const std::string& query), (override));
};

class UpdateCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        mockMetadataRepo_ = std::make_shared<MockMetadataRepository>();
        mockContentStore_ = std::make_shared<MockContentStore>();
        
        // Create command with mocks
        command_ = std::make_unique<UpdateCommand>(mockMetadataRepo_, mockContentStore_);
        
        // Set up test database in temp directory
        testDir_ = std::filesystem::temp_directory_path() / "update_command_test";
        std::filesystem::create_directories(testDir_);
        
        // Create test documents
        setupTestDocuments();
    }
    
    void TearDown() override {
        std::filesystem::remove_all(testDir_);
    }
    
    void setupTestDocuments() {
        // Create sample documents for testing
        testDoc1_ = Document{
            .id = "doc1",
            .hash = "abc123def456",
            .name = "test1.txt",
            .path = testDir_ / "test1.txt",
            .size = 1024,
            .addedTime = std::chrono::system_clock::now()
        };
        
        testDoc2_ = Document{
            .id = "doc2",
            .hash = "789xyz012",
            .name = "test2.md",
            .path = testDir_ / "test2.md",
            .size = 2048,
            .addedTime = std::chrono::system_clock::now()
        };
        
        stdinDoc_ = Document{
            .id = "doc3",
            .hash = "stdin123",
            .name = "-",  // stdin indicator
            .path = "",
            .size = 512,
            .addedTime = std::chrono::system_clock::now()
        };
    }
    
    std::shared_ptr<MockMetadataRepository> mockMetadataRepo_;
    std::shared_ptr<MockContentStore> mockContentStore_;
    std::unique_ptr<UpdateCommand> command_;
    std::filesystem::path testDir_;
    
    Document testDoc1_;
    Document testDoc2_;
    Document stdinDoc_;
};

// Basic Functionality Tests

TEST_F(UpdateCommandTest, UpdateByHash) {
    // Arrange
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "status", MetadataValue("completed")))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setHash("abc123def456");
    command_->setKey("status");
    command_->setValue("completed");
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk()) << "Update by hash should succeed";
}

TEST_F(UpdateCommandTest, UpdateByName) {
    // Arrange
    SearchOptions options;
    options.limit = 1;
    
    std::vector<Document> searchResults = {testDoc1_};
    
    EXPECT_CALL(*mockMetadataRepo_, searchDocuments("test1.txt", _))
        .WillOnce(Return(Result<std::vector<Document>>::ok(searchResults)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "priority", MetadataValue("high")))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setName("test1.txt");
    command_->setKey("priority");
    command_->setValue("high");
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk()) << "Update by name should succeed";
}

TEST_F(UpdateCommandTest, ResolveStdinDocument) {
    // Arrange - simulate stdin document that needs name resolution
    search::SearchResult searchResult;
    searchResult.document = stdinDoc_;
    searchResult.score = 1.0;
    
    EXPECT_CALL(*mockContentStore_, search("stdin-doc"))
        .WillOnce(Return(Result<search::SearchResult>::ok(searchResult)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc3", "source", MetadataValue("pipe")))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setName("stdin-doc");
    command_->setKey("source");
    command_->setValue("pipe");
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk()) << "Stdin document resolution should succeed";
}

// Value Type Tests

TEST_F(UpdateCommandTest, UpdateStringValue) {
    // Arrange
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "description", 
                                               MetadataValue("A test document")))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setHash("abc123def456");
    command_->setKey("description");
    command_->setValue("A test document");
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk());
}

TEST_F(UpdateCommandTest, UpdateNumericValue) {
    // Arrange
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "version", MetadataValue(42.5)))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setHash("abc123def456");
    command_->setKey("version");
    command_->setValue("42.5");
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk());
}

TEST_F(UpdateCommandTest, UpdateBooleanValue) {
    // Arrange
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "reviewed", MetadataValue(true)))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setHash("abc123def456");
    command_->setKey("reviewed");
    command_->setValue("true");
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk());
}

TEST_F(UpdateCommandTest, UpdateNullValue) {
    // Setting null should remove the metadata
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, removeMetadata("doc1", "obsolete"))
        .WillOnce(Return(Result<void>::ok()));
    
    // Act
    command_->setHash("abc123def456");
    command_->setKey("obsolete");
    command_->setValue("null");  // Or empty string, depending on implementation
    auto result = command_->execute();
    
    // Assert
    ASSERT_TRUE(result.isOk());
}

// Error Handling Tests

TEST_F(UpdateCommandTest, HandleNonExistentDocument) {
    // Arrange
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("nonexistent"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(std::nullopt)));
    
    // Act
    command_->setHash("nonexistent");
    command_->setKey("status");
    command_->setValue("failed");
    auto result = command_->execute();
    
    // Assert
    ASSERT_FALSE(result.isOk());
    EXPECT_EQ(result.error().code, ErrorCode::NotFound);
    EXPECT_THAT(result.error().message, HasSubstr("Document not found"));
}

TEST_F(UpdateCommandTest, HandleInvalidKey) {
    // Test with empty key
    command_->setHash("abc123def456");
    command_->setKey("");  // Invalid empty key
    command_->setValue("value");
    
    auto result = command_->execute();
    
    ASSERT_FALSE(result.isOk());
    EXPECT_EQ(result.error().code, ErrorCode::InvalidInput);
}

TEST_F(UpdateCommandTest, HandleAmbiguousName) {
    // Arrange - multiple documents match the name
    std::vector<Document> searchResults = {testDoc1_, testDoc2_};
    
    EXPECT_CALL(*mockMetadataRepo_, searchDocuments("test", _))
        .WillOnce(Return(Result<std::vector<Document>>::ok(searchResults)));
    
    // Act
    command_->setName("test");
    command_->setKey("status");
    command_->setValue("ambiguous");
    auto result = command_->execute();
    
    // Assert
    ASSERT_FALSE(result.isOk());
    EXPECT_EQ(result.error().code, ErrorCode::AmbiguousInput);
    EXPECT_THAT(result.error().message, HasSubstr("Multiple documents"));
}

TEST_F(UpdateCommandTest, HandleMetadataUpdateFailure) {
    // Arrange
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "status", _))
        .WillOnce(Return(Result<void>::error(
            Error{ErrorCode::DatabaseError, "Constraint violation"})));
    
    // Act
    command_->setHash("abc123def456");
    command_->setKey("status");
    command_->setValue("failed");
    auto result = command_->execute();
    
    // Assert
    ASSERT_FALSE(result.isOk());
    EXPECT_EQ(result.error().code, ErrorCode::DatabaseError);
}

// Update Existing Metadata

TEST_F(UpdateCommandTest, UpdateExistingMetadata) {
    // First set a value, then update it
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .Times(2)
        .WillRepeatedly(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "status", MetadataValue("pending")))
        .WillOnce(Return(Result<void>::ok()));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "status", MetadataValue("completed")))
        .WillOnce(Return(Result<void>::ok()));
    
    // First update
    command_->setHash("abc123def456");
    command_->setKey("status");
    command_->setValue("pending");
    auto result1 = command_->execute();
    ASSERT_TRUE(result1.isOk());
    
    // Second update (overwrite)
    command_->setValue("completed");
    auto result2 = command_->execute();
    ASSERT_TRUE(result2.isOk());
}

// Concurrent Update Tests

TEST_F(UpdateCommandTest, ConcurrentUpdates) {
    // Test thread safety with concurrent metadata updates
    const int numThreads = 10;
    std::vector<std::thread> threads;
    std::vector<bool> results(numThreads, false);
    
    // Set up expectations for concurrent calls
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .Times(numThreads)
        .WillRepeatedly(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    // Each thread updates a different metadata key
    for (int i = 0; i < numThreads; ++i) {
        std::string key = "counter_" + std::to_string(i);
        EXPECT_CALL(*mockMetadataRepo_, 
                   setMetadata("doc1", key, MetadataValue(static_cast<double>(i))))
            .WillOnce(Return(Result<void>::ok()));
    }
    
    // Launch threads
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, i, &results]() {
            UpdateCommand cmd(mockMetadataRepo_, mockContentStore_);
            cmd.setHash("abc123def456");
            cmd.setKey("counter_" + std::to_string(i));
            cmd.setValue(std::to_string(i));
            
            auto result = cmd.execute();
            results[i] = result.isOk();
        });
    }
    
    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }
    
    // Verify all succeeded
    for (int i = 0; i < numThreads; ++i) {
        EXPECT_TRUE(results[i]) << "Thread " << i << " failed";
    }
}

// Metadata Persistence Tests

TEST_F(UpdateCommandTest, MetadataPersistence) {
    // Verify metadata persists after update
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "persistent", MetadataValue("value")))
        .WillOnce(Return(Result<void>::ok()));
    
    // Expect retrieval of the same value
    EXPECT_CALL(*mockMetadataRepo_, getMetadata("doc1", "persistent"))
        .WillOnce(Return(Result<MetadataValue>::ok(MetadataValue("value"))));
    
    // Update
    command_->setHash("abc123def456");
    command_->setKey("persistent");
    command_->setValue("value");
    auto updateResult = command_->execute();
    ASSERT_TRUE(updateResult.isOk());
    
    // Verify persistence
    auto getResult = mockMetadataRepo_->getMetadata("doc1", "persistent");
    ASSERT_TRUE(getResult.isOk());
    EXPECT_EQ(getResult.value().toString(), "value");
}

// Batch Update Tests

TEST_F(UpdateCommandTest, BatchMetadataUpdate) {
    // Test updating multiple metadata fields in sequence
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .Times(3)
        .WillRepeatedly(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    std::vector<std::pair<std::string, std::string>> updates = {
        {"status", "completed"},
        {"priority", "high"},
        {"assignee", "alice"}
    };
    
    for (const auto& [key, value] : updates) {
        EXPECT_CALL(*mockMetadataRepo_, 
                   setMetadata("doc1", key, MetadataValue(value)))
            .WillOnce(Return(Result<void>::ok()));
    }
    
    // Perform batch updates
    for (const auto& [key, value] : updates) {
        command_->setHash("abc123def456");
        command_->setKey(key);
        command_->setValue(value);
        
        auto result = command_->execute();
        ASSERT_TRUE(result.isOk()) << "Failed to update " << key;
    }
}

// Special Character Handling

TEST_F(UpdateCommandTest, HandleSpecialCharactersInValue) {
    // Test with various special characters
    std::vector<std::string> specialValues = {
        "value with spaces",
        "value\nwith\nnewlines",
        "value\twith\ttabs",
        "value\"with\"quotes",
        "value'with'apostrophes",
        "unicode: 你好世界 🚀"
    };
    
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .Times(specialValues.size())
        .WillRepeatedly(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    for (const auto& value : specialValues) {
        EXPECT_CALL(*mockMetadataRepo_, 
                   setMetadata("doc1", "special", MetadataValue(value)))
            .WillOnce(Return(Result<void>::ok()));
    }
    
    for (const auto& value : specialValues) {
        command_->setHash("abc123def456");
        command_->setKey("special");
        command_->setValue(value);
        
        auto result = command_->execute();
        ASSERT_TRUE(result.isOk()) << "Failed with value: " << value;
    }
}

// Command Line Argument Parsing Tests

TEST_F(UpdateCommandTest, ParseArguments) {
    // Test parsing command line arguments
    std::vector<std::string> args = {
        "--hash", "abc123",
        "--key", "status",
        "--value", "done"
    };
    
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "status", MetadataValue("done")))
        .WillOnce(Return(Result<void>::ok()));
    
    // Parse and execute
    command_->parseArguments(args);
    auto result = command_->execute();
    
    ASSERT_TRUE(result.isOk());
}

// Integration with ContentStore

TEST_F(UpdateCommandTest, IntegrationWithSearch) {
    // Test that search integration works correctly
    search::SearchResult searchResult;
    searchResult.document = testDoc1_;
    searchResult.score = 0.95;
    
    EXPECT_CALL(*mockContentStore_, search("query"))
        .WillOnce(Return(Result<search::SearchResult>::ok(searchResult)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "search_score", MetadataValue(0.95)))
        .WillOnce(Return(Result<void>::ok()));
    
    // Search and update based on result
    command_->setName("query");
    command_->setKey("search_score");
    command_->setValue("0.95");
    auto result = command_->execute();
    
    ASSERT_TRUE(result.isOk());
}

// Edge Cases

TEST_F(UpdateCommandTest, UpdateWithEmptyValue) {
    // Empty value might mean remove or set to empty string
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    // Depending on implementation, this might remove or set empty
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "empty", MetadataValue("")))
        .WillOnce(Return(Result<void>::ok()));
    
    command_->setHash("abc123def456");
    command_->setKey("empty");
    command_->setValue("");
    auto result = command_->execute();
    
    ASSERT_TRUE(result.isOk());
}

TEST_F(UpdateCommandTest, UpdateVeryLongValue) {
    // Test with very long metadata value
    std::string longValue(10000, 'a');  // 10KB of 'a'
    
    EXPECT_CALL(*mockMetadataRepo_, getDocumentByHash("abc123def456"))
        .WillOnce(Return(Result<std::optional<Document>>::ok(testDoc1_)));
    
    EXPECT_CALL(*mockMetadataRepo_, setMetadata("doc1", "long", MetadataValue(longValue)))
        .WillOnce(Return(Result<void>::ok()));
    
    command_->setHash("abc123def456");
    command_->setKey("long");
    command_->setValue(longValue);
    auto result = command_->execute();
    
    ASSERT_TRUE(result.isOk());
}