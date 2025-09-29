#include <cstdlib>
#include <filesystem>
#include <sstream>
#include <vector>
#include "common/test_data_generator.h"
#include <gtest/gtest.h>
#include <yams/api/content_store.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/search_executor.h>

using namespace yams;
using namespace yams::api;
using namespace yams::metadata;
using namespace yams::test;

class MultiCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        testDir_ = std::filesystem::temp_directory_path() / "yams_multi_command_test";
        std::filesystem::create_directories(testDir_);

        initializeYAMS();
        generator_ = std::make_unique<TestDataGenerator>();
    }

    void TearDown() override {
        contentStore_.reset();
        metadataRepo_.reset();
        database_.reset();
        std::filesystem::remove_all(testDir_);
    }

    void initializeYAMS() {
        auto dbPath = testDir_ / "yams.db";
        database_ = std::make_shared<Database>(dbPath.string());
        database_->initialize();
        metadataRepo_ = std::make_shared<MetadataRepository>(database_);
        contentStore_ = std::make_unique<ContentStore>(testDir_.string());
    }

    std::filesystem::path testDir_;
    std::shared_ptr<Database> database_;
    std::shared_ptr<MetadataRepository> metadataRepo_;
    std::unique_ptr<ContentStore> contentStore_;
    std::unique_ptr<TestDataGenerator> generator_;
};

TEST_F(MultiCommandTest, AddAndSearchWorkflow) {
    // Add multiple documents with different tags
    std::vector<std::string> categories = {"docs", "code", "config", "test", "data"};
    std::map<std::string, std::vector<std::string>> categoryHashes;

    for (const auto& category : categories) {
        for (int i = 0; i < 10; ++i) {
            auto content = generator_->generateTextDocument(1024);
            content += " Category: " + category + " Item: " + std::to_string(i);

            auto file = testDir_ / (category + "_" + std::to_string(i) + ".txt");
            std::ofstream(file) << content;

            auto result = contentStore_->addFile(file, {category, "multi-test"});
            ASSERT_TRUE(result.has_value());
            categoryHashes[category].push_back(result->contentHash);
        }
    }

    // Search by category tag
    for (const auto& category : categories) {
        search::SearchOptions options;
        options.tags = {category};
        auto results = contentStore_->searchWithOptions("Category", options);

        EXPECT_GE(results.size(), 10) << "Not all documents found for category: " << category;

        // Verify results are from correct category
        for (const auto& result : results) {
            auto content = contentStore_->getContent(result.document.hash);
            ASSERT_TRUE(content.has_value());
            std::string contentStr(content->begin(), content->end());
            EXPECT_TRUE(contentStr.find("Category: " + category) != std::string::npos);
        }
    }
}

TEST_F(MultiCommandTest, MetadataFilteringWorkflow) {
    // Add documents with various metadata
    struct DocInfo {
        std::string name;
        std::string status;
        int priority;
        std::string category;
    };

    std::vector<DocInfo> docs = {
        {"doc1.txt", "pending", 1, "urgent"},   {"doc2.txt", "active", 2, "normal"},
        {"doc3.txt", "completed", 1, "urgent"}, {"doc4.txt", "pending", 3, "low"},
        {"doc5.txt", "active", 1, "urgent"},    {"doc6.txt", "completed", 2, "normal"},
        {"doc7.txt", "pending", 2, "normal"},   {"doc8.txt", "active", 3, "low"},
        {"doc9.txt", "completed", 3, "low"},    {"doc10.txt", "pending", 1, "urgent"}};

    std::map<std::string, std::string> docHashes;

    // Add documents with metadata
    for (const auto& doc : docs) {
        auto content = generator_->generateTextDocument(512);
        auto file = testDir_ / doc.name;
        std::ofstream(file) << content;

        auto result = contentStore_->addFile(file);
        ASSERT_TRUE(result.has_value());
        docHashes[doc.name] = result->contentHash;

        // Get document and set metadata
        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());

        auto docId = docResult->value()->id;
        metadataRepo_->setMetadata(docId, "status", MetadataValue(doc.status));
        metadataRepo_->setMetadata(docId, "priority",
                                   MetadataValue(static_cast<double>(doc.priority)));
        metadataRepo_->setMetadata(docId, "category", MetadataValue(doc.category));
    }

    // Query with different filters

    // 1. Find all urgent pending items
    search::SearchOptions urgentPending;
    urgentPending.metadataFilter = "status:pending AND category:urgent";
    auto urgentPendingResults = contentStore_->searchWithOptions("*", urgentPending);
    EXPECT_EQ(urgentPendingResults.size(), 2); // doc1 and doc10

    // 2. Find high priority items (priority = 1)
    search::SearchOptions highPriority;
    highPriority.metadataFilter = "priority:1";
    auto highPriorityResults = contentStore_->searchWithOptions("*", highPriority);
    EXPECT_EQ(highPriorityResults.size(), 4); // doc1, doc3, doc5, doc10

    // 3. Find completed items
    search::SearchOptions completed;
    completed.metadataFilter = "status:completed";
    auto completedResults = contentStore_->searchWithOptions("*", completed);
    EXPECT_EQ(completedResults.size(), 3); // doc3, doc6, doc9
}

TEST_F(MultiCommandTest, PipelineOperations) {
    // Simulate: add | search | update | get pipeline

    // Step 1: Add documents
    std::vector<std::string> addedHashes;
    for (int i = 0; i < 5; ++i) {
        auto content = generator_->generateMarkdown(2);
        content += "\nPipeline test document " + std::to_string(i);

        auto file = testDir_ / ("pipeline_" + std::to_string(i) + ".md");
        std::ofstream(file) << content;

        auto result = contentStore_->addFile(file, {"pipeline"});
        ASSERT_TRUE(result.has_value());
        addedHashes.push_back(result->contentHash);
    }

    // Step 2: Search for pipeline documents
    search::SearchOptions searchOpts;
    searchOpts.tags = {"pipeline"};
    auto searchResults = contentStore_->searchWithOptions("Pipeline", searchOpts);
    ASSERT_GE(searchResults.size(), 5);

    // Step 3: Update metadata for search results
    int updateCount = 0;
    for (const auto& result : searchResults) {
        auto docResult = metadataRepo_->getDocumentByHash(result.document.hash);
        if (docResult.has_value() && docResult->has_value()) {
            auto updateResult = metadataRepo_->setMetadata(docResult->value()->id, "pipeline_stage",
                                                           MetadataValue("processed"));
            if (updateResult.isOk()) {
                updateCount++;
            }
        }
    }
    EXPECT_EQ(updateCount, searchResults.size());

    // Step 4: Get documents and verify updates
    for (const auto& hash : addedHashes) {
        auto content = contentStore_->getContent(hash);
        ASSERT_TRUE(content.has_value());

        auto docResult = metadataRepo_->getDocumentByHash(hash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());

        auto metadata = metadataRepo_->getMetadata(docResult->value()->id, "pipeline_stage");
        ASSERT_TRUE(metadata.has_value());
        EXPECT_EQ(metadata->toString(), "processed");
    }
}

TEST_F(MultiCommandTest, BatchOperationsWithRollback) {
    // Test batch operations with potential rollback scenarios

    std::vector<std::string> hashes;
    std::vector<std::string> docIds;

    // Add batch of documents
    for (int i = 0; i < 20; ++i) {
        auto content = generator_->generateTextDocument(1024);
        auto file = testDir_ / ("batch_" + std::to_string(i) + ".txt");
        std::ofstream(file) << content;

        auto result = contentStore_->addFile(file, {"batch"});
        ASSERT_TRUE(result.has_value());
        hashes.push_back(result->contentHash);

        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());
        docIds.push_back(docResult->value()->id);
    }

    // Attempt batch metadata update
    bool updateSuccess = true;
    std::vector<std::pair<std::string, std::string>> updates;

    for (size_t i = 0; i < docIds.size(); ++i) {
        auto result = metadataRepo_->setMetadata(docIds[i], "batch_id", MetadataValue("batch_001"));

        if (result.isOk()) {
            updates.push_back({docIds[i], "batch_id"});
        } else {
            updateSuccess = false;
            break;
        }
    }

    if (!updateSuccess) {
        // Rollback: remove metadata from successfully updated documents
        for (const auto& [docId, key] : updates) {
            metadataRepo_->removeMetadata(docId, key);
        }

        // Verify rollback
        for (const auto& [docId, key] : updates) {
            auto metadata = metadataRepo_->getMetadata(docId, key);
            EXPECT_FALSE(metadata.has_value()) << "Rollback failed for " << docId;
        }
    } else {
        // Verify all updates succeeded
        for (const auto& docId : docIds) {
            auto metadata = metadataRepo_->getMetadata(docId, "batch_id");
            ASSERT_TRUE(metadata.has_value());
            EXPECT_EQ(metadata->toString(), "batch_001");
        }
    }
}

TEST_F(MultiCommandTest, ComplexQueryChaining) {
    // Test complex query combinations

    // Setup: Add documents with rich metadata
    struct Document {
        std::string content;
        std::string type;
        std::string author;
        int year;
        double score;
        std::vector<std::string> tags;
    };

    std::vector<Document> documents = {
        {generator_->generateTextDocument(1024), "article", "alice", 2023, 4.5, {"research", "ai"}},
        {generator_->generateTextDocument(1024), "report", "bob", 2024, 3.8, {"analysis", "data"}},
        {generator_->generateTextDocument(1024),
         "article",
         "charlie",
         2023,
         4.2,
         {"research", "ml"}},
        {generator_->generateTextDocument(1024), "guide", "alice", 2024, 4.9, {"tutorial", "ai"}},
        {generator_->generateTextDocument(1024),
         "report",
         "diana",
         2022,
         3.5,
         {"analysis", "stats"}},
        {generator_->generateTextDocument(1024), "article", "bob", 2024, 4.7, {"research", "data"}},
        {generator_->generateTextDocument(1024), "guide", "charlie", 2023, 4.1, {"tutorial", "ml"}},
        {generator_->generateTextDocument(1024), "report", "alice", 2024, 4.3, {"analysis", "ai"}}};

    // Add documents with metadata
    for (size_t i = 0; i < documents.size(); ++i) {
        const auto& doc = documents[i];

        auto file = testDir_ / ("complex_" + std::to_string(i) + ".txt");
        std::ofstream(file) << doc.content;

        auto result = contentStore_->addFile(file, doc.tags);
        ASSERT_TRUE(result.has_value());

        auto docResult = metadataRepo_->getDocumentByHash(result->contentHash);
        ASSERT_TRUE(docResult.has_value() && docResult->has_value());

        auto docId = docResult->value()->id;
        metadataRepo_->setMetadata(docId, "type", MetadataValue(doc.type));
        metadataRepo_->setMetadata(docId, "author", MetadataValue(doc.author));
        metadataRepo_->setMetadata(docId, "year", MetadataValue(static_cast<double>(doc.year)));
        metadataRepo_->setMetadata(docId, "score", MetadataValue(doc.score));
    }

    // Complex query 1: Articles by alice with score > 4.0
    search::SearchOptions query1;
    query1.metadataFilter = "type:article AND author:alice AND score:>4.0";
    auto results1 = contentStore_->searchWithOptions("*", query1);
    EXPECT_EQ(results1.size(), 1);

    // Complex query 2: 2024 documents with AI tag
    search::SearchOptions query2;
    query2.metadataFilter = "year:2024";
    query2.tags = {"ai"};
    auto results2 = contentStore_->searchWithOptions("*", query2);
    EXPECT_EQ(results2.size(), 2); // Guide and report by alice in 2024

    // Complex query 3: High-scoring research articles
    search::SearchOptions query3;
    query3.metadataFilter = "type:article AND score:>4.0";
    query3.tags = {"research"};
    auto results3 = contentStore_->searchWithOptions("*", query3);
    EXPECT_GE(results3.size(), 2);
}

TEST_F(MultiCommandTest, ErrorPropagation) {
    // Test error handling across command chains

    // Try to search before adding any documents
    auto emptySearch = contentStore_->search("nonexistent");
    // Should handle gracefully, not crash

    // Add a document
    auto content = generator_->generateTextDocument(512);
    auto file = testDir_ / "error_test.txt";
    std::ofstream(file) << content;

    auto result = contentStore_->addFile(file);
    ASSERT_TRUE(result.has_value());
    auto hash = result->contentHash;

    // Try to update metadata with invalid key
    auto docResult = metadataRepo_->getDocumentByHash(hash);
    ASSERT_TRUE(docResult.has_value() && docResult->has_value());

    // Empty key should fail
    auto invalidUpdate = metadataRepo_->setMetadata(docResult->value()->id,
                                                    "", // Invalid empty key
                                                    MetadataValue("value"));
    EXPECT_FALSE(invalidUpdate.isOk());

    // Try to get non-existent document
    auto invalidGet = contentStore_->getContent("invalid_hash_12345");
    EXPECT_FALSE(invalidGet.has_value());

    // Try to search with invalid metadata filter
    search::SearchOptions invalidOptions;
    invalidOptions.metadataFilter = "((invalid syntax";
    auto invalidSearch = contentStore_->searchWithOptions("*", invalidOptions);
    // Should handle parse error gracefully

    // Verify original document is still accessible
    auto validGet = contentStore_->getContent(hash);
    ASSERT_TRUE(validGet.has_value());
    EXPECT_EQ(std::string(validGet->begin(), validGet->end()), content);
}

// Final lightweight stress: metadataRepo count loop to catch flakiness.
TEST_F(MultiCommandTest, StressTail) {
    auto stress_iters = []() {
        if (const char* s = std::getenv("YAMS_STRESS_ITERS")) {
            int v = std::atoi(s);
            if (v > 0 && v < 100000)
                return v;
        }
        return 100;
    }();
    for (int i = 0; i < stress_iters; ++i) {
        auto c = metadataRepo_->getDocumentCount();
        ASSERT_TRUE(c.isOk());
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}
