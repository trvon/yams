#include <gtest/gtest.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/database.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/migration.h>
#include <filesystem>
#include <chrono>

using namespace yams;
using namespace yams::metadata;

class MetadataSchemaTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use temporary database for tests
        dbPath_ = std::filesystem::temp_directory_path() / "kronos_metadata_test.db";
        std::filesystem::remove(dbPath_);
        
        // Initialize connection pool
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        ASSERT_TRUE(initResult.has_value());
        
        // Run migrations
        pool_->withConnection([](Database& db) -> Result<void> {
            MigrationManager mm(db);
            auto initResult = mm.initialize();
            if (!initResult) return initResult.error();
            
            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
            return mm.migrate();
        });
        
        // Create repository
        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }
    
    void TearDown() override {
        repo_.reset();
        pool_->shutdown();
        pool_.reset();
        std::filesystem::remove(dbPath_);
    }
    
    DocumentInfo createTestDocument(const std::string& name) {
        DocumentInfo doc;
        doc.filePath = "/test/path/" + name;
        doc.fileName = name;
        doc.fileExtension = ".txt";
        doc.fileSize = 1024;
        doc.sha256Hash = "hash_" + name;
        doc.mimeType = "text/plain";
        doc.createdTime = std::chrono::system_clock::now();
        doc.modifiedTime = doc.createdTime;
        doc.indexedTime = doc.createdTime;
        doc.contentExtracted = false;
        doc.extractionStatus = ExtractionStatus::Pending;
        return doc;
    }
    
    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
};

TEST_F(MetadataSchemaTest, DocumentCRUD) {
    // Create
    auto doc = createTestDocument("test.txt");
    auto insertResult = repo_->insertDocument(doc);
    ASSERT_TRUE(insertResult.has_value());
    int64_t docId = insertResult.value();
    EXPECT_GT(docId, 0);
    
    // Read by ID
    auto getResult = repo_->getDocument(docId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    auto retrieved = getResult.value().value();
    EXPECT_EQ(retrieved.id, docId);
    EXPECT_EQ(retrieved.fileName, doc.fileName);
    EXPECT_EQ(retrieved.sha256Hash, doc.sha256Hash);
    
    // Read by hash
    auto hashResult = repo_->getDocumentByHash(doc.sha256Hash);
    ASSERT_TRUE(hashResult.has_value());
    ASSERT_TRUE(hashResult.value().has_value());
    EXPECT_EQ(hashResult.value().value().id, docId);
    
    // Update
    retrieved.fileSize = 2048;
    retrieved.contentExtracted = true;
    retrieved.extractionStatus = ExtractionStatus::Success;
    auto updateResult = repo_->updateDocument(retrieved);
    ASSERT_TRUE(updateResult.has_value());
    
    // Verify update
    auto updatedResult = repo_->getDocument(docId);
    ASSERT_TRUE(updatedResult.has_value());
    ASSERT_TRUE(updatedResult.value().has_value());
    auto updated = updatedResult.value().value();
    EXPECT_EQ(updated.fileSize, 2048);
    EXPECT_TRUE(updated.contentExtracted);
    EXPECT_EQ(updated.extractionStatus, ExtractionStatus::Success);
    
    // Delete
    auto deleteResult = repo_->deleteDocument(docId);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    auto deletedResult = repo_->getDocument(docId);
    ASSERT_TRUE(deletedResult.has_value());
    EXPECT_FALSE(deletedResult.value().has_value());
}

TEST_F(MetadataSchemaTest, ContentCRUD) {
    // Create document first
    auto doc = createTestDocument("content_test.txt");
    auto docResult = repo_->insertDocument(doc);
    ASSERT_TRUE(docResult.has_value());
    int64_t docId = docResult.value();
    
    // Create content
    DocumentContent content;
    content.documentId = docId;
    content.contentText = "This is the extracted text content";
    content.contentLength = content.contentText.length();
    content.extractionMethod = "text_parser";
    content.language = "en";
    
    auto insertResult = repo_->insertContent(content);
    ASSERT_TRUE(insertResult.has_value());
    
    // Read content
    auto getResult = repo_->getContent(docId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    auto retrieved = getResult.value().value();
    EXPECT_EQ(retrieved.documentId, docId);
    EXPECT_EQ(retrieved.contentText, content.contentText);
    EXPECT_EQ(retrieved.language, content.language);
    
    // Update content
    retrieved.contentText = "Updated content";
    retrieved.contentLength = retrieved.contentText.length();
    auto updateResult = repo_->updateContent(retrieved);
    ASSERT_TRUE(updateResult.has_value());
    
    // Verify update
    auto updatedResult = repo_->getContent(docId);
    ASSERT_TRUE(updatedResult.has_value());
    ASSERT_TRUE(updatedResult.value().has_value());
    EXPECT_EQ(updatedResult.value().value().contentText, "Updated content");
    
    // Delete content
    auto deleteResult = repo_->deleteContent(docId);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    auto deletedResult = repo_->getContent(docId);
    ASSERT_TRUE(deletedResult.has_value());
    EXPECT_FALSE(deletedResult.value().has_value());
}

TEST_F(MetadataSchemaTest, MetadataOperations) {
    // Create document
    auto doc = createTestDocument("meta_test.txt");
    auto docResult = repo_->insertDocument(doc);
    ASSERT_TRUE(docResult.has_value());
    int64_t docId = docResult.value();
    
    // Set various metadata types
    auto stringResult = repo_->setMetadata(docId, "author", MetadataValue("John Doe"));
    ASSERT_TRUE(stringResult.has_value());
    
    auto intResult = repo_->setMetadata(docId, "version", MetadataValue(int64_t(3)));
    ASSERT_TRUE(intResult.has_value());
    
    auto doubleResult = repo_->setMetadata(docId, "score", MetadataValue(4.5));
    ASSERT_TRUE(doubleResult.has_value());
    
    auto boolResult = repo_->setMetadata(docId, "published", MetadataValue(true));
    ASSERT_TRUE(boolResult.has_value());
    
    // Get specific metadata
    auto authorResult = repo_->getMetadata(docId, "author");
    ASSERT_TRUE(authorResult.has_value());
    ASSERT_TRUE(authorResult.value().has_value());
    EXPECT_EQ(authorResult.value().value().asString(), "John Doe");
    EXPECT_EQ(authorResult.value().value().type, MetadataValueType::String);
    
    auto versionResult = repo_->getMetadata(docId, "version");
    ASSERT_TRUE(versionResult.has_value());
    ASSERT_TRUE(versionResult.value().has_value());
    EXPECT_EQ(versionResult.value().value().asInteger(), 3);
    EXPECT_EQ(versionResult.value().value().type, MetadataValueType::Integer);
    
    // Get all metadata
    auto allResult = repo_->getAllMetadata(docId);
    ASSERT_TRUE(allResult.has_value());
    auto allMeta = allResult.value();
    EXPECT_EQ(allMeta.size(), 4);
    EXPECT_TRUE(allMeta.count("author") > 0);
    EXPECT_TRUE(allMeta.count("version") > 0);
    EXPECT_TRUE(allMeta.count("score") > 0);
    EXPECT_TRUE(allMeta.count("published") > 0);
    
    // Update metadata (should replace)
    auto updateResult = repo_->setMetadata(docId, "author", MetadataValue("Jane Smith"));
    ASSERT_TRUE(updateResult.has_value());
    
    auto updatedResult = repo_->getMetadata(docId, "author");
    ASSERT_TRUE(updatedResult.has_value());
    ASSERT_TRUE(updatedResult.value().has_value());
    EXPECT_EQ(updatedResult.value().value().asString(), "Jane Smith");
    
    // Remove metadata
    auto removeResult = repo_->removeMetadata(docId, "score");
    ASSERT_TRUE(removeResult.has_value());
    
    auto removedResult = repo_->getMetadata(docId, "score");
    ASSERT_TRUE(removedResult.has_value());
    EXPECT_FALSE(removedResult.value().has_value());
}

TEST_F(MetadataSchemaTest, RelationshipOperations) {
    // Create parent and child documents
    auto parent = createTestDocument("parent.txt");
    auto parentResult = repo_->insertDocument(parent);
    ASSERT_TRUE(parentResult.has_value());
    int64_t parentId = parentResult.value();
    
    auto child1 = createTestDocument("child1.txt");
    auto child1Result = repo_->insertDocument(child1);
    ASSERT_TRUE(child1Result.has_value());
    int64_t child1Id = child1Result.value();
    
    auto child2 = createTestDocument("child2.txt");
    auto child2Result = repo_->insertDocument(child2);
    ASSERT_TRUE(child2Result.has_value());
    int64_t child2Id = child2Result.value();
    
    // Create relationships
    DocumentRelationship rel1;
    rel1.parentId = parentId;
    rel1.childId = child1Id;
    rel1.relationshipType = RelationshipType::Contains;
    rel1.createdTime = std::chrono::system_clock::now();
    
    auto rel1Result = repo_->insertRelationship(rel1);
    ASSERT_TRUE(rel1Result.has_value());
    int64_t rel1Id = rel1Result.value();
    
    DocumentRelationship rel2;
    rel2.parentId = parentId;
    rel2.childId = child2Id;
    rel2.relationshipType = RelationshipType::Contains;
    rel2.createdTime = std::chrono::system_clock::now();
    
    auto rel2Result = repo_->insertRelationship(rel2);
    ASSERT_TRUE(rel2Result.has_value());
    
    // Get relationships for parent
    auto parentRels = repo_->getRelationships(parentId);
    ASSERT_TRUE(parentRels.has_value());
    EXPECT_EQ(parentRels.value().size(), 2);
    
    // Get relationships for child
    auto childRels = repo_->getRelationships(child1Id);
    ASSERT_TRUE(childRels.has_value());
    EXPECT_EQ(childRels.value().size(), 1);
    EXPECT_EQ(childRels.value()[0].parentId, parentId);
    
    // Test relationship without parent (orphan)
    DocumentRelationship orphanRel;
    orphanRel.parentId = 0;  // No parent
    orphanRel.childId = child1Id;
    orphanRel.relationshipType = RelationshipType::Custom;
    orphanRel.customType = "orphaned";
    orphanRel.createdTime = std::chrono::system_clock::now();
    
    auto orphanResult = repo_->insertRelationship(orphanRel);
    ASSERT_TRUE(orphanResult.has_value());
    
    // Delete relationship
    auto deleteResult = repo_->deleteRelationship(rel1Id);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    auto afterDelete = repo_->getRelationships(parentId);
    ASSERT_TRUE(afterDelete.has_value());
    EXPECT_EQ(afterDelete.value().size(), 1);
}

TEST_F(MetadataSchemaTest, FullTextSearch) {
    // Create documents with content
    struct TestDoc {
        std::string name;
        std::string content;
        std::string title;
    };
    
    std::vector<TestDoc> testDocs = {
        {"doc1.txt", "The quick brown fox jumps over the lazy dog", "Animal Story"},
        {"doc2.txt", "C++ programming language features and benefits", "Programming Guide"},
        {"doc3.txt", "Quick start guide for C++ development", "Quick C++ Guide"},
        {"doc4.pdf", "Advanced programming techniques in modern C++", "Advanced C++"}
    };
    
    std::vector<int64_t> docIds;
    
    for (const auto& td : testDocs) {
        auto doc = createTestDocument(td.name);
        doc.fileExtension = td.name.substr(td.name.find_last_of('.'));
        auto docResult = repo_->insertDocument(doc);
        ASSERT_TRUE(docResult.has_value());
        int64_t docId = docResult.value();
        docIds.push_back(docId);
        
        // Insert content
        DocumentContent content;
        content.documentId = docId;
        content.contentText = td.content;
        content.contentLength = td.content.length();
        content.extractionMethod = "test";
        content.language = "en";
        
        auto contentResult = repo_->insertContent(content);
        ASSERT_TRUE(contentResult.has_value());
        
        // Index for FTS
        auto indexResult = repo_->indexDocumentContent(
            docId, td.title, td.content, doc.fileExtension
        );
        ASSERT_TRUE(indexResult.has_value());
    }
    
    // Search for "quick"
    auto quickResult = repo_->search("quick", 10, 0);
    ASSERT_TRUE(quickResult.has_value());
    auto quickSearch = quickResult.value();
    if (!quickSearch.isSuccess()) {
        FAIL() << "Search failed with error: " << quickSearch.errorMessage;
    }
    EXPECT_EQ(quickSearch.results.size(), 2);  // doc1 and doc3
    
    // Search for "programming" (avoiding FTS5 issues with special characters)
    auto cppResult = repo_->search("programming", 10, 0);
    ASSERT_TRUE(cppResult.has_value());
    auto cppSearch = cppResult.value();
    if (!cppSearch.isSuccess()) {
        FAIL() << "Search failed with error: " << cppSearch.errorMessage;
    }
    EXPECT_GE(cppSearch.results.size(), 2);  // At least doc2 and doc4
    
    // Test pagination
    auto page1Result = repo_->search("programming", 2, 0);
    ASSERT_TRUE(page1Result.has_value());
    EXPECT_EQ(page1Result.value().results.size(), 2);
    
    auto page2Result = repo_->search("programming", 2, 2);
    ASSERT_TRUE(page2Result.has_value());
    // May have 0 or 1 result depending on total matches
    
    // Remove from index
    auto removeResult = repo_->removeFromIndex(docIds[0]);
    ASSERT_TRUE(removeResult.has_value());
    
    // Verify removal
    auto afterRemove = repo_->search("quick brown fox", 10, 0);
    ASSERT_TRUE(afterRemove.has_value());
    EXPECT_EQ(afterRemove.value().results.size(), 0);
}

TEST_F(MetadataSchemaTest, SearchHistory) {
    // Create search history entries
    SearchHistoryEntry entry1;
    entry1.query = "test query";
    entry1.queryTime = std::chrono::system_clock::now();
    entry1.resultsCount = 5;
    entry1.executionTimeMs = 25;
    entry1.userContext = "user123";
    
    auto insert1 = repo_->insertSearchHistory(entry1);
    ASSERT_TRUE(insert1.has_value());
    
    // Add more entries
    for (int i = 0; i < 10; i++) {
        SearchHistoryEntry entry;
        entry.query = "query " + std::to_string(i);
        entry.queryTime = std::chrono::system_clock::now();
        entry.resultsCount = i * 2;
        entry.executionTimeMs = 10 + i;
        
        auto result = repo_->insertSearchHistory(entry);
        ASSERT_TRUE(result.has_value());
    }
    
    // Get recent searches
    auto recentResult = repo_->getRecentSearches(5);
    ASSERT_TRUE(recentResult.has_value());
    auto recent = recentResult.value();
    EXPECT_EQ(recent.size(), 5);
    
    // Verify ordering (most recent first)
    EXPECT_EQ(recent[0].query, "query 9");
    EXPECT_EQ(recent[1].query, "query 8");
}

TEST_F(MetadataSchemaTest, SavedQueries) {
    // Create saved query
    SavedQuery query1;
    query1.name = "My Search";
    query1.query = "C++ programming";
    query1.description = "Search for C++ content";
    query1.createdTime = std::chrono::system_clock::now();
    query1.lastUsed = query1.createdTime;
    query1.useCount = 0;
    
    auto insertResult = repo_->insertSavedQuery(query1);
    ASSERT_TRUE(insertResult.has_value());
    int64_t queryId = insertResult.value();
    
    // Get saved query
    auto getResult = repo_->getSavedQuery(queryId);
    ASSERT_TRUE(getResult.has_value());
    ASSERT_TRUE(getResult.value().has_value());
    auto retrieved = getResult.value().value();
    EXPECT_EQ(retrieved.name, query1.name);
    EXPECT_EQ(retrieved.query, query1.query);
    
    // Update saved query
    retrieved.useCount = 5;
    retrieved.lastUsed = std::chrono::system_clock::now();
    auto updateResult = repo_->updateSavedQuery(retrieved);
    ASSERT_TRUE(updateResult.has_value());
    
    // Get all saved queries
    auto allResult = repo_->getAllSavedQueries();
    ASSERT_TRUE(allResult.has_value());
    EXPECT_GE(allResult.value().size(), 1);
    
    // Delete saved query
    auto deleteResult = repo_->deleteSavedQuery(queryId);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify deletion
    auto deletedResult = repo_->getSavedQuery(queryId);
    ASSERT_TRUE(deletedResult.has_value());
    EXPECT_FALSE(deletedResult.value().has_value());
}

TEST_F(MetadataSchemaTest, BulkOperations) {
    // Create multiple documents
    std::vector<int64_t> docIds;
    for (int i = 0; i < 5; i++) {
        auto doc = createTestDocument("bulk_" + std::to_string(i) + ".txt");
        doc.modifiedTime = std::chrono::system_clock::now() - std::chrono::hours(i);
        auto result = repo_->insertDocument(doc);
        ASSERT_TRUE(result.has_value());
        docIds.push_back(result.value());
    }
    
    // Find by path pattern
    auto pathResult = repo_->findDocumentsByPath("%bulk%");
    ASSERT_TRUE(pathResult.has_value());
    EXPECT_EQ(pathResult.value().size(), 5);
    
    // Find by extension
    auto extResult = repo_->findDocumentsByExtension(".txt");
    ASSERT_TRUE(extResult.has_value());
    EXPECT_GE(extResult.value().size(), 5);
    
    // Find modified since
    auto since = std::chrono::system_clock::now() - std::chrono::hours(3);
    auto sinceResult = repo_->findDocumentsModifiedSince(since);
    ASSERT_TRUE(sinceResult.has_value());
    EXPECT_GE(sinceResult.value().size(), 3);
}

TEST_F(MetadataSchemaTest, Statistics) {
    // Create documents with different extensions
    std::vector<std::string> extensions = {".txt", ".pdf", ".doc", ".txt", ".pdf", ".txt"};
    std::vector<int64_t> docIds;
    
    for (size_t i = 0; i < extensions.size(); i++) {
        auto doc = createTestDocument("stats_" + std::to_string(i) + extensions[i]);
        doc.fileExtension = extensions[i];
        auto result = repo_->insertDocument(doc);
        ASSERT_TRUE(result.has_value());
        docIds.push_back(result.value());
    }
    
    // Get document count
    auto countResult = repo_->getDocumentCount();
    ASSERT_TRUE(countResult.has_value());
    EXPECT_EQ(countResult.value(), extensions.size());
    
    // Mark some as indexed
    for (size_t i = 0; i < 3; i++) {
        auto doc = repo_->getDocument(docIds[i]);
        ASSERT_TRUE(doc.has_value() && doc.value().has_value());
        auto docInfo = doc.value().value();
        docInfo.contentExtracted = true;
        repo_->updateDocument(docInfo);
    }
    
    // Get indexed count
    auto indexedResult = repo_->getIndexedDocumentCount();
    ASSERT_TRUE(indexedResult.has_value());
    EXPECT_EQ(indexedResult.value(), 3);
    
    // Get counts by extension
    auto extCountResult = repo_->getDocumentCountsByExtension();
    ASSERT_TRUE(extCountResult.has_value());
    auto extCounts = extCountResult.value();
    EXPECT_EQ(extCounts[".txt"], 3);
    EXPECT_EQ(extCounts[".pdf"], 2);
    EXPECT_EQ(extCounts[".doc"], 1);
}

TEST_F(MetadataSchemaTest, CascadingDeletes) {
    // Create document with all related data
    auto doc = createTestDocument("cascade_test.txt");
    auto docResult = repo_->insertDocument(doc);
    ASSERT_TRUE(docResult.has_value());
    int64_t docId = docResult.value();
    
    // Add content
    DocumentContent content;
    content.documentId = docId;
    content.contentText = "Test content";
    content.contentLength = content.contentText.length();
    content.extractionMethod = "test";
    content.language = "en";
    repo_->insertContent(content);
    
    // Add metadata
    repo_->setMetadata(docId, "test_key", MetadataValue("test_value"));
    
    // Add relationship
    DocumentRelationship rel;
    rel.parentId = docId;
    rel.childId = docId;  // Self-reference for test
    rel.relationshipType = RelationshipType::Custom;
    rel.customType = "self";
    rel.createdTime = std::chrono::system_clock::now();
    repo_->insertRelationship(rel);
    
    // Delete document - should cascade to all related data
    auto deleteResult = repo_->deleteDocument(docId);
    ASSERT_TRUE(deleteResult.has_value());
    
    // Verify cascading deletes
    auto contentResult = repo_->getContent(docId);
    ASSERT_TRUE(contentResult.has_value());
    EXPECT_FALSE(contentResult.value().has_value());
    
    auto metaResult = repo_->getAllMetadata(docId);
    ASSERT_TRUE(metaResult.has_value());
    EXPECT_EQ(metaResult.value().size(), 0);
    
    auto relResult = repo_->getRelationships(docId);
    ASSERT_TRUE(relResult.has_value());
    EXPECT_EQ(relResult.value().size(), 0);
}

TEST_F(MetadataSchemaTest, QueryBuilder) {
    // Create test documents
    std::vector<DocumentInfo> docs = {
        createTestDocument("doc1.txt"),
        createTestDocument("doc2.pdf"),
        createTestDocument("doc3.txt"),
        createTestDocument("doc4.doc")
    };
    
    // Set different attributes
    docs[0].modifiedTime = std::chrono::system_clock::now() - std::chrono::hours(1);
    docs[1].modifiedTime = std::chrono::system_clock::now() - std::chrono::hours(2);
    docs[2].modifiedTime = std::chrono::system_clock::now() - std::chrono::hours(3);
    docs[3].modifiedTime = std::chrono::system_clock::now() - std::chrono::hours(4);
    
    for (auto& doc : docs) {
        auto result = repo_->insertDocument(doc);
        ASSERT_TRUE(result.has_value());
    }
    
    // Test query builder
    MetadataQueryBuilder qb;
    auto query = qb.withExtension(".txt")
                   .modifiedAfter(std::chrono::system_clock::now() - std::chrono::hours(2))
                   .orderByModified(false)
                   .limit(10)
                   .buildQuery();
    
    EXPECT_TRUE(query.find("file_extension = ?") != std::string::npos);
    EXPECT_TRUE(query.find("modified_time >= ?") != std::string::npos);
    EXPECT_TRUE(query.find("ORDER BY modified_time DESC") != std::string::npos);
    EXPECT_TRUE(query.find("LIMIT 10") != std::string::npos);
    
    auto params = qb.getParameters();
    EXPECT_EQ(params.size(), 2);
    EXPECT_EQ(params[0], ".txt");
}

TEST_F(MetadataSchemaTest, UniqueConstraints) {
    // Test unique SHA256 hash constraint
    auto doc1 = createTestDocument("unique1.txt");
    doc1.sha256Hash = "unique_hash_123";
    auto result1 = repo_->insertDocument(doc1);
    ASSERT_TRUE(result1.has_value());
    
    // Try to insert document with same hash
    auto doc2 = createTestDocument("unique2.txt");
    doc2.sha256Hash = "unique_hash_123";  // Same hash
    auto result2 = repo_->insertDocument(doc2);
    EXPECT_FALSE(result2.has_value());  // Should fail due to unique constraint
    
    // Test unique metadata per document
    int64_t docId = result1.value();
    auto meta1 = repo_->setMetadata(docId, "unique_key", MetadataValue("value1"));
    ASSERT_TRUE(meta1.has_value());
    
    // Update should work (INSERT OR REPLACE)
    auto meta2 = repo_->setMetadata(docId, "unique_key", MetadataValue("value2"));
    ASSERT_TRUE(meta2.has_value());
    
    // Verify it was updated, not duplicated
    auto getMeta = repo_->getMetadata(docId, "unique_key");
    ASSERT_TRUE(getMeta.has_value());
    ASSERT_TRUE(getMeta.value().has_value());
    EXPECT_EQ(getMeta.value().value().asString(), "value2");
}

TEST_F(MetadataSchemaTest, ExtractionStatusHandling) {
    // Test all extraction status values
    std::vector<ExtractionStatus> statuses = {
        ExtractionStatus::Pending,
        ExtractionStatus::Success,
        ExtractionStatus::Failed,
        ExtractionStatus::Skipped
    };
    
    std::vector<int64_t> docIds;
    
    for (auto status : statuses) {
        auto doc = createTestDocument("status_" + ExtractionStatusUtils::toString(status) + ".txt");
        doc.extractionStatus = status;
        if (status == ExtractionStatus::Failed) {
            doc.extractionError = "Test error message";
        }
        
        auto result = repo_->insertDocument(doc);
        ASSERT_TRUE(result.has_value());
        docIds.push_back(result.value());
    }
    
    // Retrieve and verify
    for (size_t i = 0; i < statuses.size(); i++) {
        auto result = repo_->getDocument(docIds[i]);
        ASSERT_TRUE(result.has_value());
        ASSERT_TRUE(result.value().has_value());
        auto doc = result.value().value();
        EXPECT_EQ(doc.extractionStatus, statuses[i]);
        if (statuses[i] == ExtractionStatus::Failed) {
            EXPECT_EQ(doc.extractionError, "Test error message");
        }
    }
}

TEST_F(MetadataSchemaTest, PerformanceMetrics) {
    // Measure insertion performance
    const int numDocs = 1000;
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < numDocs; i++) {
        auto doc = createTestDocument("perf_" + std::to_string(i) + ".txt");
        doc.sha256Hash = "hash_perf_" + std::to_string(i);
        auto result = repo_->insertDocument(doc);
        ASSERT_TRUE(result.has_value());
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto insertTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should be able to insert 1000 documents in under 1 second
    EXPECT_LT(insertTime.count(), 1000);
    
    // Measure query performance
    start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 100; i++) {
        auto result = repo_->getDocumentByHash("hash_perf_" + std::to_string(i * 10));
        ASSERT_TRUE(result.has_value());
    }
    
    end = std::chrono::high_resolution_clock::now();
    auto queryTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    // Should be able to query 100 documents by hash in under 100ms
    EXPECT_LT(queryTime.count(), 100);
}