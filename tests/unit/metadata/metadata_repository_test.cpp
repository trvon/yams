#include <gtest/gtest.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
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
        
        // Create database
        database_ = std::make_unique<Database>();
        auto result = database_->open(dbPath_.string(), ConnectionMode::Create);
        ASSERT_TRUE(result.has_value());
        
        // Run migrations
        MigrationManager migrationManager(*database_);
        auto initResult = migrationManager.initialize();
        ASSERT_TRUE(initResult.has_value());
        
        migrationManager.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        auto migrateResult = migrationManager.migrate();
        ASSERT_TRUE(migrateResult.has_value());
        
        // Create repository
        repository_ = std::make_unique<MetadataRepository>(*database_);
    }
    
    void TearDown() override {
        repository_.reset();
        database_.reset();
        std::filesystem::remove(dbPath_);
    }
    
    std::filesystem::path dbPath_;
    std::unique_ptr<Database> database_;
    std::unique_ptr<MetadataRepository> repository_;
};

TEST_F(MetadataRepositoryTest, CreateDocument) {
    DocumentInfo docInfo;
    docInfo.contentHash = "hash123";
    docInfo.fileName = "test.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdAt = std::chrono::system_clock::now();
    docInfo.modifiedAt = docInfo.createdAt;
    
    auto result = repository_->createDocument(docInfo);
    ASSERT_TRUE(result.has_value());
    
    auto docId = result.value();
    EXPECT_FALSE(docId.empty());
    
    // Verify document was created
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    
    auto retrievedDoc = getResult.value();
    EXPECT_EQ(retrievedDoc.id, docId);
    EXPECT_EQ(retrievedDoc.contentHash, docInfo.contentHash);
    EXPECT_EQ(retrievedDoc.fileName, docInfo.fileName);
    EXPECT_EQ(retrievedDoc.fileSize, docInfo.fileSize);
    EXPECT_EQ(retrievedDoc.mimeType, docInfo.mimeType);
}

TEST_F(MetadataRepositoryTest, GetDocumentNotFound) {
    auto result = repository_->getDocument("nonexistent");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, yams::ErrorCode::NotFound);
}

TEST_F(MetadataRepositoryTest, UpdateDocument) {
    // Create document
    DocumentInfo docInfo;
    docInfo.contentHash = "hash456";
    docInfo.fileName = "original.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdAt = std::chrono::system_clock::now();
    docInfo.modifiedAt = docInfo.createdAt;
    
    auto createResult = repository_->createDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Update document
    docInfo.id = docId;
    docInfo.fileName = "updated.txt";
    docInfo.fileSize = 4096;
    docInfo.modifiedAt = std::chrono::system_clock::now();
    
    auto updateResult = repository_->updateDocument(docInfo);
    ASSERT_TRUE(updateResult.has_value());
    
    // Verify update
    auto getResult = repository_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    
    auto updatedDoc = getResult.value();
    EXPECT_EQ(updatedDoc.fileName, "updated.txt");
    EXPECT_EQ(updatedDoc.fileSize, 4096);
}

TEST_F(MetadataRepositoryTest, DeleteDocument) {
    // Create document
    DocumentInfo docInfo;
    docInfo.contentHash = "hash789";
    docInfo.fileName = "delete_me.txt";
    docInfo.fileSize = 512;
    docInfo.mimeType = "text/plain";
    docInfo.createdAt = std::chrono::system_clock::now();
    docInfo.modifiedAt = docInfo.createdAt;
    
    auto createResult = repository_->createDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Delete document
    auto deleteResult = repository_->deleteDocument(docId);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    auto getResult = repository_->getDocument(docId);
    ASSERT_FALSE(getResult.has_value());
    EXPECT_EQ(getResult.error().code, yams::ErrorCode::NotFound);
}

TEST_F(MetadataRepositoryTest, SetAndGetMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.contentHash = "hash_meta";
    docInfo.fileName = "metadata_test.txt";
    docInfo.fileSize = 1024;
    docInfo.mimeType = "text/plain";
    docInfo.createdAt = std::chrono::system_clock::now();
    docInfo.modifiedAt = docInfo.createdAt;
    
    auto createResult = repository_->createDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Set metadata
    MetadataValue value;
    value.stringValue = "Test Author";
    
    auto setResult = repository_->setMetadata(docId, "author", value);
    ASSERT_TRUE(setResult.has_value());
    
    // Get metadata
    auto getResult = repository_->getMetadata(docId, "author");
    ASSERT_TRUE(getResult.has_value());
    
    auto retrievedValue = getResult.value();
    EXPECT_EQ(retrievedValue.stringValue, "Test Author");
}

TEST_F(MetadataRepositoryTest, GetAllMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.contentHash = "hash_all_meta";
    docInfo.fileName = "all_metadata.txt";
    docInfo.fileSize = 2048;
    docInfo.mimeType = "text/plain";
    docInfo.createdAt = std::chrono::system_clock::now();
    docInfo.modifiedAt = docInfo.createdAt;
    
    auto createResult = repository_->createDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Set multiple metadata values
    MetadataValue authorValue;
    authorValue.stringValue = "John Doe";
    repository_->setMetadata(docId, "author", authorValue);
    
    MetadataValue yearValue;
    yearValue.intValue = 2024;
    repository_->setMetadata(docId, "year", yearValue);
    
    MetadataValue ratingValue;
    ratingValue.realValue = 4.5;
    repository_->setMetadata(docId, "rating", ratingValue);
    
    // Get all metadata
    auto getAllResult = repository_->getAllMetadata(docId);
    ASSERT_TRUE(getAllResult.has_value());
    
    auto metadata = getAllResult.value();
    EXPECT_EQ(metadata.size(), 3);
    
    EXPECT_EQ(metadata["author"].stringValue, "John Doe");
    EXPECT_EQ(metadata["year"].intValue, 2024);
    EXPECT_FLOAT_EQ(metadata["rating"].realValue, 4.5);
}

TEST_F(MetadataRepositoryTest, DeleteMetadata) {
    // Create document
    DocumentInfo docInfo;
    docInfo.contentHash = "hash_del_meta";
    docInfo.fileName = "delete_metadata.txt";
    docInfo.fileSize = 512;
    docInfo.mimeType = "text/plain";
    docInfo.createdAt = std::chrono::system_clock::now();
    docInfo.modifiedAt = docInfo.createdAt;
    
    auto createResult = repository_->createDocument(docInfo);
    ASSERT_TRUE(createResult.has_value());
    auto docId = createResult.value();
    
    // Set metadata
    MetadataValue value;
    value.stringValue = "Temporary";
    repository_->setMetadata(docId, "temp", value);
    
    // Verify it exists
    auto getResult = repository_->getMetadata(docId, "temp");
    ASSERT_TRUE(getResult.has_value());
    
    // Delete metadata
    auto deleteResult = repository_->deleteMetadata(docId, "temp");
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    getResult = repository_->getMetadata(docId, "temp");
    ASSERT_FALSE(getResult.has_value());
}

TEST_F(MetadataRepositoryTest, FindByContentHash) {
    // Create multiple documents with same content hash
    std::string contentHash = "duplicate_hash";
    
    for (int i = 0; i < 3; ++i) {
        DocumentInfo docInfo;
        docInfo.contentHash = contentHash;
        docInfo.fileName = "file_" + std::to_string(i) + ".txt";
        docInfo.fileSize = 1024 * (i + 1);
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto result = repository_->createDocument(docInfo);
        ASSERT_TRUE(result.has_value());
    }
    
    // Find by content hash
    auto findResult = repository_->findByContentHash(contentHash);
    ASSERT_TRUE(findResult.has_value());
    
    auto documents = findResult.value();
    EXPECT_EQ(documents.size(), 3);
    
    for (const auto& doc : documents) {
        EXPECT_EQ(doc.contentHash, contentHash);
    }
}

TEST_F(MetadataRepositoryTest, FindByMetadata) {
    // Create documents with metadata
    std::vector<std::string> docIds;
    
    for (int i = 0; i < 5; ++i) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + std::to_string(i);
        docInfo.fileName = "doc_" + std::to_string(i) + ".txt";
        docInfo.fileSize = 1024;
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto createResult = repository_->createDocument(docInfo);
        ASSERT_TRUE(createResult.has_value());
        docIds.push_back(createResult.value());
        
        // Add metadata
        MetadataValue categoryValue;
        categoryValue.stringValue = (i < 3) ? "important" : "regular";
        repository_->setMetadata(docIds.back(), "category", categoryValue);
        
        MetadataValue priorityValue;
        priorityValue.intValue = i;
        repository_->setMetadata(docIds.back(), "priority", priorityValue);
    }
    
    // Find by metadata value
    MetadataValue searchValue;
    searchValue.stringValue = "important";
    
    auto findResult = repository_->findByMetadata("category", searchValue);
    ASSERT_TRUE(findResult.has_value());
    
    auto documents = findResult.value();
    EXPECT_EQ(documents.size(), 3);
    
    for (const auto& doc : documents) {
        auto metaResult = repository_->getMetadata(doc.id, "category");
        ASSERT_TRUE(metaResult.has_value());
        EXPECT_EQ(metaResult.value().stringValue, "important");
    }
}

TEST_F(MetadataRepositoryTest, FullTextSearch) {
    // Index some documents
    std::vector<std::pair<std::string, std::string>> testDocs = {
        {"doc1", "The quick brown fox jumps over the lazy dog"},
        {"doc2", "Machine learning algorithms revolutionize data analysis"},
        {"doc3", "The lazy cat sleeps under the warm sun"},
        {"doc4", "Data science combines statistics and programming"},
        {"doc5", "Quick results require efficient algorithms"}
    };
    
    for (const auto& [docId, content] : testDocs) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + docId;
        docInfo.fileName = docId + ".txt";
        docInfo.fileSize = content.size();
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto createResult = repository_->createDocument(docInfo);
        ASSERT_TRUE(createResult.has_value());
        
        auto indexResult = repository_->indexDocumentContent(
            createResult.value(), docInfo.fileName, content, "text/plain");
        ASSERT_TRUE(indexResult.has_value());
    }
    
    // Search for "lazy"
    auto searchResult = repository_->fullTextSearch("lazy");
    ASSERT_TRUE(searchResult.has_value());
    
    auto results = searchResult.value();
    EXPECT_GE(results.size(), 2); // Should find doc1 and doc3
    
    // Search for "algorithms"
    searchResult = repository_->fullTextSearch("algorithms");
    ASSERT_TRUE(searchResult.has_value());
    
    results = searchResult.value();
    EXPECT_GE(results.size(), 2); // Should find doc2 and doc5
    
    // Search for phrase
    searchResult = repository_->fullTextSearch("\"machine learning\"");
    ASSERT_TRUE(searchResult.has_value());
    
    results = searchResult.value();
    EXPECT_GE(results.size(), 1); // Should find doc2
}

TEST_F(MetadataRepositoryTest, FuzzySearchBasic) {
    // Create test documents
    std::vector<std::pair<std::string, std::string>> testDocs = {
        {"hello_world.txt", "Basic hello world program"},
        {"help_file.txt", "Help documentation for users"},
        {"test_data.json", "Test data in JSON format"},
        {"search_algorithm.cpp", "Implementation of search algorithms"},
        {"machine_learning.py", "Machine learning models and training"}
    };
    
    for (const auto& [fileName, content] : testDocs) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + fileName;
        docInfo.fileName = fileName;
        docInfo.fileSize = content.size();
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto createResult = repository_->createDocument(docInfo);
        ASSERT_TRUE(createResult.has_value());
        
        // Index content for fuzzy search
        auto indexResult = repository_->indexDocumentContent(
            createResult.value(), fileName, content, "text/plain");
        ASSERT_TRUE(indexResult.has_value());
    }
    
    // Build fuzzy index
    auto buildResult = repository_->buildFuzzyIndex();
    ASSERT_TRUE(buildResult.has_value());
    
    // Test fuzzy search for misspelled filename
    auto searchResult = repository_->fuzzySearch("helo_world", 0.7, 10);
    ASSERT_TRUE(searchResult.has_value());
    
    auto results = searchResult.value();
    ASSERT_FALSE(results.empty());
    
    // Should find "hello_world.txt" as top result
    EXPECT_TRUE(results[0].text.find("hello_world") != std::string::npos);
    EXPECT_GT(results[0].score, 0.7);
    
    // Test fuzzy search for content
    searchResult = repository_->fuzzySearch("machne lerning", 0.6, 10);
    ASSERT_TRUE(searchResult.has_value());
    
    results = searchResult.value();
    ASSERT_FALSE(results.empty());
    
    // Should find documents with "machine learning" content
    bool foundMachineLearning = false;
    for (const auto& result : results) {
        if (result.text.find("machine") != std::string::npos ||
            result.text.find("learning") != std::string::npos) {
            foundMachineLearning = true;
            break;
        }
    }
    EXPECT_TRUE(foundMachineLearning);
}

TEST_F(MetadataRepositoryTest, FuzzySearchWithTypos) {
    // Create documents
    std::vector<std::string> fileNames = {
        "configuration.yaml",
        "settings.json",
        "preferences.xml",
        "options.ini",
        "parameters.conf"
    };
    
    for (const auto& fileName : fileNames) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + fileName;
        docInfo.fileName = fileName;
        docInfo.fileSize = 100;
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto createResult = repository_->createDocument(docInfo);
        ASSERT_TRUE(createResult.has_value());
    }
    
    // Build fuzzy index
    auto buildResult = repository_->buildFuzzyIndex();
    ASSERT_TRUE(buildResult.has_value());
    
    // Test with typos
    struct TestCase {
        std::string query;
        std::string expected;
    };
    
    std::vector<TestCase> testCases = {
        {"confguration", "configuration.yaml"},     // Missing 'i'
        {"setings", "settings.json"},               // Missing 't'
        {"preferances", "preferences.xml"},         // Misspelled
        {"optoins", "options.ini"},                // Transposed letters
        {"paremeters", "parameters.conf"}          // Missing 'a'
    };
    
    for (const auto& testCase : testCases) {
        auto searchResult = repository_->fuzzySearch(testCase.query, 0.6, 5);
        ASSERT_TRUE(searchResult.has_value());
        
        auto results = searchResult.value();
        ASSERT_FALSE(results.empty()) << "Failed to find results for: " << testCase.query;
        
        // Check if expected file is in top results
        bool found = false;
        for (const auto& result : results) {
            if (result.text.find(testCase.expected) != std::string::npos) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Expected to find " << testCase.expected 
                          << " when searching for " << testCase.query;
    }
}

TEST_F(MetadataRepositoryTest, FuzzySearchPerformance) {
    // Create a larger dataset
    const int numDocs = 500;
    
    for (int i = 0; i < numDocs; ++i) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + std::to_string(i);
        docInfo.fileName = "document_" + std::to_string(i) + ".txt";
        docInfo.fileSize = 1000 + i;
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto createResult = repository_->createDocument(docInfo);
        ASSERT_TRUE(createResult.has_value());
        
        // Add some content
        std::string content = "Content for document number " + std::to_string(i);
        if (i % 10 == 0) {
            content += " special important document";
        }
        if (i % 20 == 0) {
            content += " contains machine learning algorithms";
        }
        
        repository_->indexDocumentContent(
            createResult.value(), docInfo.fileName, content, "text/plain");
    }
    
    // Build fuzzy index
    auto buildStart = std::chrono::high_resolution_clock::now();
    auto buildResult = repository_->buildFuzzyIndex();
    auto buildEnd = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(buildResult.has_value());
    
    auto buildDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        buildEnd - buildStart);
    
    // Building index for 500 docs should complete within 1 second
    EXPECT_LT(buildDuration.count(), 1000) << "Index building took too long: " 
                                           << buildDuration.count() << "ms";
    
    // Test search performance
    auto searchStart = std::chrono::high_resolution_clock::now();
    auto searchResult = repository_->fuzzySearch("dokument", 0.7, 20);
    auto searchEnd = std::chrono::high_resolution_clock::now();
    ASSERT_TRUE(searchResult.has_value());
    
    auto searchDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        searchEnd - searchStart);
    
    // Search should complete within 100ms
    EXPECT_LT(searchDuration.count(), 100) << "Search took too long: " 
                                           << searchDuration.count() << "ms";
    
    auto results = searchResult.value();
    EXPECT_GT(results.size(), 0) << "Should find some documents";
}

TEST_F(MetadataRepositoryTest, FuzzySearchEmptyIndex) {
    // Try fuzzy search without building index
    auto searchResult = repository_->fuzzySearch("test", 0.8, 10);
    ASSERT_TRUE(searchResult.has_value());
    
    auto results = searchResult.value();
    EXPECT_TRUE(results.empty()) << "Should return empty results without index";
    
    // Build empty index
    auto buildResult = repository_->buildFuzzyIndex();
    ASSERT_TRUE(buildResult.has_value());
    
    // Search again
    searchResult = repository_->fuzzySearch("test", 0.8, 10);
    ASSERT_TRUE(searchResult.has_value());
    
    results = searchResult.value();
    EXPECT_TRUE(results.empty()) << "Should return empty results with empty index";
}

TEST_F(MetadataRepositoryTest, FuzzySearchConcurrency) {
    // Create test documents
    for (int i = 0; i < 100; ++i) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + std::to_string(i);
        docInfo.fileName = "file_" + std::to_string(i) + ".txt";
        docInfo.fileSize = 500;
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto createResult = repository_->createDocument(docInfo);
        ASSERT_TRUE(createResult.has_value());
    }
    
    // Build fuzzy index
    auto buildResult = repository_->buildFuzzyIndex();
    ASSERT_TRUE(buildResult.has_value());
    
    // Perform concurrent searches
    const int numThreads = 10;
    std::vector<std::thread> threads;
    std::atomic<int> successCount{0};
    
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([this, &successCount, i]() {
            std::string query = "file_" + std::to_string(i * 10);
            auto result = repository_->fuzzySearch(query, 0.7, 10);
            if (result.has_value() && !result.value().empty()) {
                successCount++;
            }
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    
    // All searches should succeed
    EXPECT_EQ(successCount.load(), numThreads);
}

TEST_F(MetadataRepositoryTest, ListDocumentsWithPagination) {
    // Create multiple documents
    const int totalDocs = 25;
    for (int i = 0; i < totalDocs; ++i) {
        DocumentInfo docInfo;
        docInfo.contentHash = "hash_" + std::to_string(i);
        docInfo.fileName = "file_" + std::to_string(i) + ".txt";
        docInfo.fileSize = 100 * (i + 1);
        docInfo.mimeType = "text/plain";
        docInfo.createdAt = std::chrono::system_clock::now();
        docInfo.modifiedAt = docInfo.createdAt;
        
        auto result = repository_->createDocument(docInfo);
        ASSERT_TRUE(result.has_value());
        
        // Add some metadata to test filtering
        if (i % 2 == 0) {
            MetadataValue value;
            value.stringValue = "even";
            repository_->setMetadata(result.value(), "type", value);
        }
    }
    
    // Test pagination
    auto page1 = repository_->listDocuments(10, 0);
    ASSERT_TRUE(page1.has_value());
    EXPECT_EQ(page1.value().size(), 10);
    
    auto page2 = repository_->listDocuments(10, 10);
    ASSERT_TRUE(page2.has_value());
    EXPECT_EQ(page2.value().size(), 10);
    
    auto page3 = repository_->listDocuments(10, 20);
    ASSERT_TRUE(page3.has_value());
    EXPECT_EQ(page3.value().size(), 5);
    
    // Verify no duplicates across pages
    std::set<std::string> allIds;
    for (const auto& doc : page1.value()) allIds.insert(doc.id);
    for (const auto& doc : page2.value()) allIds.insert(doc.id);
    for (const auto& doc : page3.value()) allIds.insert(doc.id);
    
    EXPECT_EQ(allIds.size(), totalDocs);
}