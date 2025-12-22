/**
 * @file metadata_schema_catch2_test.cpp
 * @brief Comprehensive Catch2 tests for metadata schema, CRUD operations, FTS, and migrations
 */

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/metadata/query_helpers.h>

using namespace yams;
using namespace yams::metadata;

namespace {

struct MetadataSchemaFixture {
    std::filesystem::path dbPath_;
    std::unique_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;

    MetadataSchemaFixture() {
        // Use temporary database for tests; allow override for restricted environments
        const char* t = std::getenv("YAMS_TEST_TMPDIR");
        auto base = (t && *t) ? std::filesystem::path(t) : std::filesystem::temp_directory_path();
        std::error_code ec;
        std::filesystem::create_directories(base, ec);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath_ = base / (std::string("kronos_metadata_test_") + std::to_string(ts) + ".db");
        std::filesystem::remove(dbPath_);

        // Initialize connection pool
        ConnectionPoolConfig config;
        config.minConnections = 1;
        config.maxConnections = 2;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        // Run migrations
        pool_->withConnection([](Database& db) -> Result<void> {
            MigrationManager mm(db);
            auto initResult = mm.initialize();
            if (!initResult)
                return initResult.error();

            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
            return mm.migrate();
        });

        // Create repository
        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~MetadataSchemaFixture() {
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
        doc.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        doc.modifiedTime = doc.createdTime;
        doc.indexedTime = doc.createdTime;
        doc.contentExtracted = false;
        doc.extractionStatus = ExtractionStatus::Pending;
        return doc;
    }
};

} // namespace

TEST_CASE_METHOD(MetadataSchemaFixture, "Document CRUD operations", "[unit][metadata][schema][crud]") {
    SECTION("Create document") {
        auto doc = createTestDocument("test.txt");
        auto insertResult = repo_->insertDocument(doc);
        REQUIRE(insertResult.has_value());
        int64_t docId = insertResult.value();
        CHECK(docId > 0);
    }

    SECTION("Read document by ID") {
        auto doc = createTestDocument("test.txt");
        auto insertResult = repo_->insertDocument(doc);
        REQUIRE(insertResult.has_value());
        int64_t docId = insertResult.value();

        auto getResult = repo_->getDocument(docId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();
        CHECK(retrieved.id == docId);
        CHECK(retrieved.fileName == doc.fileName);
        CHECK(retrieved.sha256Hash == doc.sha256Hash);
    }

    SECTION("Read document by hash") {
        auto doc = createTestDocument("test.txt");
        auto insertResult = repo_->insertDocument(doc);
        REQUIRE(insertResult.has_value());
        int64_t docId = insertResult.value();

        auto hashResult = repo_->getDocumentByHash(doc.sha256Hash);
        REQUIRE(hashResult.has_value());
        REQUIRE(hashResult.value().has_value());
        CHECK(hashResult.value().value().id == docId);
    }

    SECTION("Update document") {
        auto doc = createTestDocument("test.txt");
        auto insertResult = repo_->insertDocument(doc);
        REQUIRE(insertResult.has_value());
        int64_t docId = insertResult.value();

        auto getResult = repo_->getDocument(docId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();

        retrieved.fileSize = 2048;
        retrieved.contentExtracted = true;
        retrieved.extractionStatus = ExtractionStatus::Success;
        auto updateResult = repo_->updateDocument(retrieved);
        REQUIRE(updateResult.has_value());

        auto updatedResult = repo_->getDocument(docId);
        REQUIRE(updatedResult.has_value());
        REQUIRE(updatedResult.value().has_value());
        auto updated = updatedResult.value().value();
        CHECK(updated.fileSize == 2048);
        CHECK(updated.contentExtracted);
        CHECK(updated.extractionStatus == ExtractionStatus::Success);
    }

    SECTION("Delete document") {
        auto doc = createTestDocument("test.txt");
        auto insertResult = repo_->insertDocument(doc);
        REQUIRE(insertResult.has_value());
        int64_t docId = insertResult.value();

        auto deleteResult = repo_->deleteDocument(docId);
        REQUIRE(deleteResult.has_value());

        auto deletedResult = repo_->getDocument(docId);
        REQUIRE(deletedResult.has_value());
        CHECK_FALSE(deletedResult.value().has_value());
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Content CRUD operations", "[unit][metadata][schema][content]") {
    auto doc = createTestDocument("content_test.txt");
    auto docResult = repo_->insertDocument(doc);
    REQUIRE(docResult.has_value());
    int64_t docId = docResult.value();

    SECTION("Create content") {
        DocumentContent content;
        content.documentId = docId;
        content.contentText = "This is the extracted text content";
        content.contentLength = content.contentText.length();
        content.extractionMethod = "text_parser";
        content.language = "en";

        auto insertResult = repo_->insertContent(content);
        REQUIRE(insertResult.has_value());
    }

    SECTION("Read content") {
        DocumentContent content;
        content.documentId = docId;
        content.contentText = "This is the extracted text content";
        content.contentLength = content.contentText.length();
        content.extractionMethod = "text_parser";
        content.language = "en";

        auto insertResult = repo_->insertContent(content);
        REQUIRE(insertResult.has_value());

        auto getResult = repo_->getContent(docId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();
        CHECK(retrieved.documentId == docId);
        CHECK(retrieved.contentText == content.contentText);
        CHECK(retrieved.language == content.language);
    }

    SECTION("Update content") {
        DocumentContent content;
        content.documentId = docId;
        content.contentText = "Original content";
        content.contentLength = content.contentText.length();
        content.extractionMethod = "text_parser";
        content.language = "en";

        auto insertResult = repo_->insertContent(content);
        REQUIRE(insertResult.has_value());

        auto getResult = repo_->getContent(docId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();

        retrieved.contentText = "Updated content";
        retrieved.contentLength = retrieved.contentText.length();
        auto updateResult = repo_->updateContent(retrieved);
        REQUIRE(updateResult.has_value());

        auto updatedResult = repo_->getContent(docId);
        REQUIRE(updatedResult.has_value());
        REQUIRE(updatedResult.value().has_value());
        CHECK(updatedResult.value().value().contentText == "Updated content");
    }

    SECTION("Delete content") {
        DocumentContent content;
        content.documentId = docId;
        content.contentText = "Content to delete";
        content.contentLength = content.contentText.length();
        content.extractionMethod = "text_parser";
        content.language = "en";

        auto insertResult = repo_->insertContent(content);
        REQUIRE(insertResult.has_value());

        auto deleteResult = repo_->deleteContent(docId);
        REQUIRE(deleteResult.has_value());

        auto deletedResult = repo_->getContent(docId);
        REQUIRE(deletedResult.has_value());
        CHECK_FALSE(deletedResult.value().has_value());
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Metadata operations", "[unit][metadata][schema][metadata]") {
    auto doc = createTestDocument("meta_test.txt");
    auto docResult = repo_->insertDocument(doc);
    REQUIRE(docResult.has_value());
    int64_t docId = docResult.value();

    SECTION("Set and get string metadata") {
        auto stringResult = repo_->setMetadata(docId, "author", MetadataValue("John Doe"));
        REQUIRE(stringResult.has_value());

        auto authorResult = repo_->getMetadata(docId, "author");
        REQUIRE(authorResult.has_value());
        REQUIRE(authorResult.value().has_value());
        CHECK(authorResult.value().value().asString() == "John Doe");
        CHECK(authorResult.value().value().type == MetadataValueType::String);
    }

    SECTION("Set and get integer metadata") {
        auto intResult = repo_->setMetadata(docId, "version", MetadataValue(int64_t(3)));
        REQUIRE(intResult.has_value());

        auto versionResult = repo_->getMetadata(docId, "version");
        REQUIRE(versionResult.has_value());
        REQUIRE(versionResult.value().has_value());
        CHECK(versionResult.value().value().asInteger() == 3);
        CHECK(versionResult.value().value().type == MetadataValueType::Integer);
    }

    SECTION("Set and get double metadata") {
        auto doubleResult = repo_->setMetadata(docId, "score", MetadataValue(4.5));
        REQUIRE(doubleResult.has_value());

        auto scoreResult = repo_->getMetadata(docId, "score");
        REQUIRE(scoreResult.has_value());
        REQUIRE(scoreResult.value().has_value());
        CHECK(scoreResult.value().value().asReal() == 4.5);
    }

    SECTION("Set and get bool metadata") {
        auto boolResult = repo_->setMetadata(docId, "published", MetadataValue(true));
        REQUIRE(boolResult.has_value());

        auto publishedResult = repo_->getMetadata(docId, "published");
        REQUIRE(publishedResult.has_value());
        REQUIRE(publishedResult.value().has_value());
        CHECK(publishedResult.value().value().asBoolean() == true);
    }

    SECTION("Get all metadata") {
        repo_->setMetadata(docId, "author", MetadataValue("John Doe"));
        repo_->setMetadata(docId, "version", MetadataValue(int64_t(3)));
        repo_->setMetadata(docId, "score", MetadataValue(4.5));
        repo_->setMetadata(docId, "published", MetadataValue(true));

        auto allResult = repo_->getAllMetadata(docId);
        REQUIRE(allResult.has_value());
        auto allMeta = allResult.value();
        CHECK(allMeta.size() == 4);
        CHECK(allMeta.count("author") > 0);
        CHECK(allMeta.count("version") > 0);
        CHECK(allMeta.count("score") > 0);
        CHECK(allMeta.count("published") > 0);
    }

    SECTION("Update metadata replaces value") {
        auto meta1 = repo_->setMetadata(docId, "author", MetadataValue("value1"));
        REQUIRE(meta1.has_value());

        auto meta2 = repo_->setMetadata(docId, "author", MetadataValue("value2"));
        REQUIRE(meta2.has_value());

        auto getMeta = repo_->getMetadata(docId, "author");
        REQUIRE(getMeta.has_value());
        REQUIRE(getMeta.value().has_value());
        CHECK(getMeta.value().value().asString() == "value2");
    }

    SECTION("Remove metadata") {
        auto setResult = repo_->setMetadata(docId, "score", MetadataValue(4.5));
        REQUIRE(setResult.has_value());

        auto removeResult = repo_->removeMetadata(docId, "score");
        REQUIRE(removeResult.has_value());

        auto removedResult = repo_->getMetadata(docId, "score");
        REQUIRE(removedResult.has_value());
        CHECK_FALSE(removedResult.value().has_value());
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Relationship operations", "[unit][metadata][schema][relationship]") {
    auto parent = createTestDocument("parent.txt");
    auto parentResult = repo_->insertDocument(parent);
    REQUIRE(parentResult.has_value());
    int64_t parentId = parentResult.value();

    auto child1 = createTestDocument("child1.txt");
    auto child1Result = repo_->insertDocument(child1);
    REQUIRE(child1Result.has_value());
    int64_t child1Id = child1Result.value();

    auto child2 = createTestDocument("child2.txt");
    auto child2Result = repo_->insertDocument(child2);
    REQUIRE(child2Result.has_value());
    int64_t child2Id = child2Result.value();

    SECTION("Create and get relationships") {
        DocumentRelationship rel1;
        rel1.parentId = parentId;
        rel1.childId = child1Id;
        rel1.relationshipType = RelationshipType::Contains;
        rel1.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto rel1Result = repo_->insertRelationship(rel1);
        REQUIRE(rel1Result.has_value());

        DocumentRelationship rel2;
        rel2.parentId = parentId;
        rel2.childId = child2Id;
        rel2.relationshipType = RelationshipType::Contains;
        rel2.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto rel2Result = repo_->insertRelationship(rel2);
        REQUIRE(rel2Result.has_value());

        auto parentRels = repo_->getRelationships(parentId);
        REQUIRE(parentRels.has_value());
        CHECK(parentRels.value().size() == 2);

        auto childRels = repo_->getRelationships(child1Id);
        REQUIRE(childRels.has_value());
        CHECK(childRels.value().size() == 1);
        CHECK(childRels.value()[0].parentId == parentId);
    }

    SECTION("Orphan relationship") {
        DocumentRelationship orphanRel;
        orphanRel.parentId = 0; // No parent
        orphanRel.childId = child1Id;
        orphanRel.relationshipType = RelationshipType::Custom;
        orphanRel.customType = "orphaned";
        orphanRel.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto orphanResult = repo_->insertRelationship(orphanRel);
        REQUIRE(orphanResult.has_value());
    }

    SECTION("Delete relationship") {
        DocumentRelationship rel;
        rel.parentId = parentId;
        rel.childId = child1Id;
        rel.relationshipType = RelationshipType::Contains;
        rel.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto relResult = repo_->insertRelationship(rel);
        REQUIRE(relResult.has_value());
        int64_t relId = relResult.value();

        auto deleteResult = repo_->deleteRelationship(relId);
        REQUIRE(deleteResult.has_value());

        auto afterDelete = repo_->getRelationships(parentId);
        REQUIRE(afterDelete.has_value());
        CHECK(afterDelete.value().size() == 0);
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Full text search", "[unit][metadata][schema][fts]") {
    struct TestDoc {
        std::string name;
        std::string content;
        std::string title;
    };

    std::vector<TestDoc> testDocs = {
        {"doc1.txt", "The quick brown fox jumps over the lazy dog", "Animal Story"},
        {"doc2.txt", "C++ programming language features and benefits", "Programming Guide"},
        {"doc3.txt", "Quick start guide for C++ development", "Quick C++ Guide"},
        {"doc4.pdf", "Advanced programming techniques in modern C++", "Advanced C++"}};

    std::vector<int64_t> docIds;

    for (const auto& td : testDocs) {
        auto doc = createTestDocument(td.name);
        doc.fileExtension = td.name.substr(td.name.find_last_of('.'));
        auto docResult = repo_->insertDocument(doc);
        REQUIRE(docResult.has_value());
        int64_t docId = docResult.value();
        docIds.push_back(docId);

        DocumentContent content;
        content.documentId = docId;
        content.contentText = td.content;
        content.contentLength = td.content.length();
        content.extractionMethod = "test";
        content.language = "en";

        auto contentResult = repo_->insertContent(content);
        REQUIRE(contentResult.has_value());

        auto indexResult =
            repo_->indexDocumentContent(docId, td.title, td.content, doc.fileExtension);
        REQUIRE(indexResult.has_value());
    }

    SECTION("Search for quick") {
        auto quickResult = repo_->search("quick", 10, 0);
        REQUIRE(quickResult.has_value());
        auto quickSearch = quickResult.value();
        REQUIRE(quickSearch.isSuccess());
        CHECK(quickSearch.results.size() == 2); // doc1 and doc3
    }

    SECTION("Search for programming") {
        auto cppResult = repo_->search("programming", 10, 0);
        REQUIRE(cppResult.has_value());
        auto cppSearch = cppResult.value();
        REQUIRE(cppSearch.isSuccess());
        CHECK(cppSearch.results.size() >= 2); // At least doc2 and doc4
    }

    SECTION("Test pagination") {
        auto page1Result = repo_->search("programming", 2, 0);
        REQUIRE(page1Result.has_value());
        CHECK(page1Result.value().results.size() == 2);

        auto page2Result = repo_->search("programming", 2, 2);
        REQUIRE(page2Result.has_value());
        // May have 0 or 1 result depending on total matches
    }

    SECTION("Remove from index") {
        auto removeResult = repo_->removeFromIndex(docIds[0]);
        REQUIRE(removeResult.has_value());

        auto afterRemove = repo_->search("quick brown fox", 10, 0);
        REQUIRE(afterRemove.has_value());
        CHECK(afterRemove.value().results.size() == 0);
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Search history", "[unit][metadata][schema][search-history]") {
    SECTION("Insert and retrieve search history") {
        SearchHistoryEntry entry1;
        entry1.query = "test query";
        entry1.queryTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        entry1.resultsCount = 5;
        entry1.executionTimeMs = 25;
        entry1.userContext = "user123";

        auto insert1 = repo_->insertSearchHistory(entry1);
        REQUIRE(insert1.has_value());

        for (int i = 0; i < 10; i++) {
            SearchHistoryEntry entry;
            entry.query = "query " + std::to_string(i);
            entry.queryTime =
                std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
            entry.resultsCount = i * 2;
            entry.executionTimeMs = 10 + i;

            auto result = repo_->insertSearchHistory(entry);
            REQUIRE(result.has_value());
        }

        auto recentResult = repo_->getRecentSearches(5);
        REQUIRE(recentResult.has_value());
        auto recent = recentResult.value();
        CHECK(recent.size() == 5);

        // Verify ordering (most recent first)
        CHECK(recent[0].query == "query 9");
        CHECK(recent[1].query == "query 8");
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Saved queries", "[unit][metadata][schema][saved-queries]") {
    SavedQuery query1;
    query1.name = "My Search";
    query1.query = "C++ programming";
    query1.description = "Search for C++ content";
    query1.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    query1.lastUsed = query1.createdTime;
    query1.useCount = 0;

    auto insertResult = repo_->insertSavedQuery(query1);
    REQUIRE(insertResult.has_value());
    int64_t queryId = insertResult.value();

    SECTION("Get saved query") {
        auto getResult = repo_->getSavedQuery(queryId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();
        CHECK(retrieved.name == query1.name);
        CHECK(retrieved.query == query1.query);
    }

    SECTION("Update saved query") {
        auto getResult = repo_->getSavedQuery(queryId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();

        retrieved.useCount = 5;
        retrieved.lastUsed = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        auto updateResult = repo_->updateSavedQuery(retrieved);
        REQUIRE(updateResult.has_value());
    }

    SECTION("Get all saved queries") {
        auto allResult = repo_->getAllSavedQueries();
        REQUIRE(allResult.has_value());
        CHECK(allResult.value().size() >= 1);
    }

    SECTION("Delete saved query") {
        auto deleteResult = repo_->deleteSavedQuery(queryId);
        REQUIRE(deleteResult.has_value());

        auto deletedResult = repo_->getSavedQuery(queryId);
        REQUIRE(deletedResult.has_value());
        CHECK_FALSE(deletedResult.value().has_value());
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Bulk operations", "[unit][metadata][schema][bulk]") {
    std::vector<int64_t> docIds;
    for (int i = 0; i < 5; i++) {
        auto doc = createTestDocument("bulk_" + std::to_string(i) + ".txt");
        doc.modifiedTime = std::chrono::floor<std::chrono::seconds>(
            std::chrono::system_clock::now() - std::chrono::hours(i));
        auto result = repo_->insertDocument(doc);
        REQUIRE(result.has_value());
        docIds.push_back(result.value());
    }

    SECTION("Find by path pattern") {
        auto pathResult = metadata::queryDocumentsByPattern(*repo_, "%bulk%");
        REQUIRE(pathResult.has_value());
        CHECK(pathResult.value().size() == 5);
    }

    SECTION("Find by extension") {
        auto extResult = repo_->findDocumentsByExtension(".txt");
        REQUIRE(extResult.has_value());
        CHECK(extResult.value().size() >= 5);
    }

    SECTION("Find modified since") {
        auto since = std::chrono::system_clock::now() - std::chrono::hours(3);
        auto sinceResult = repo_->findDocumentsModifiedSince(since);
        REQUIRE(sinceResult.has_value());
        CHECK(sinceResult.value().size() >= 3);
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Statistics", "[unit][metadata][schema][stats]") {
    std::vector<std::string> extensions = {".txt", ".pdf", ".doc", ".txt", ".pdf", ".txt"};
    std::vector<int64_t> docIds;

    for (size_t i = 0; i < extensions.size(); i++) {
        auto doc = createTestDocument("stats_" + std::to_string(i) + extensions[i]);
        doc.fileExtension = extensions[i];
        auto result = repo_->insertDocument(doc);
        REQUIRE(result.has_value());
        docIds.push_back(result.value());
    }

    SECTION("Get document count") {
        auto countResult = repo_->getDocumentCount();
        REQUIRE(countResult.has_value());
        CHECK(countResult.value() == extensions.size());
    }

    SECTION("Get indexed document count") {
        // Mark some as indexed
        for (size_t i = 0; i < 3; i++) {
            auto doc = repo_->getDocument(docIds[i]);
            REQUIRE((doc.has_value() && doc.value().has_value()));
            auto docInfo = doc.value().value();
            docInfo.contentExtracted = true;
            repo_->updateDocument(docInfo);
        }

        // Embeddings-based, expected 0 in this test
        auto embeddingIndexedResult = repo_->getIndexedDocumentCount();
        REQUIRE(embeddingIndexedResult.has_value());
        CHECK(embeddingIndexedResult.value() == 0);

        auto extractedCount = repo_->getContentExtractedDocumentCount();
        REQUIRE(extractedCount.has_value());
        CHECK(extractedCount.value() == 3);
    }

    SECTION("Get counts by extension") {
        auto extCountResult = repo_->getDocumentCountsByExtension();
        REQUIRE(extCountResult.has_value());
        auto extCounts = extCountResult.value();
        CHECK(extCounts[".txt"] == 3);
        CHECK(extCounts[".pdf"] == 2);
        CHECK(extCounts[".doc"] == 1);
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Cascading deletes", "[unit][metadata][schema][cascade]") {
    auto doc = createTestDocument("cascade_test.txt");
    auto docResult = repo_->insertDocument(doc);
    REQUIRE(docResult.has_value());
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
    rel.childId = docId; // Self-reference for test
    rel.relationshipType = RelationshipType::Custom;
    rel.customType = "self";
    rel.createdTime = std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
    repo_->insertRelationship(rel);

    // Delete document - should cascade to all related data
    auto deleteResult = repo_->deleteDocument(docId);
    REQUIRE(deleteResult.has_value());

    // Verify cascading deletes
    auto contentResult = repo_->getContent(docId);
    REQUIRE(contentResult.has_value());
    CHECK_FALSE(contentResult.value().has_value());

    auto metaResult = repo_->getAllMetadata(docId);
    REQUIRE(metaResult.has_value());
    CHECK(metaResult.value().size() == 0);

    auto relResult = repo_->getRelationships(docId);
    REQUIRE(relResult.has_value());
    CHECK(relResult.value().size() == 0);
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Query builder", "[unit][metadata][schema][query-builder]") {
    std::vector<DocumentInfo> docs = {
        createTestDocument("doc1.txt"), createTestDocument("doc2.pdf"),
        createTestDocument("doc3.txt"), createTestDocument("doc4.doc")};

    docs[0].modifiedTime = std::chrono::floor<std::chrono::seconds>(
        std::chrono::system_clock::now() - std::chrono::hours(1));
    docs[1].modifiedTime = std::chrono::floor<std::chrono::seconds>(
        std::chrono::system_clock::now() - std::chrono::hours(2));
    docs[2].modifiedTime = std::chrono::floor<std::chrono::seconds>(
        std::chrono::system_clock::now() - std::chrono::hours(3));
    docs[3].modifiedTime = std::chrono::floor<std::chrono::seconds>(
        std::chrono::system_clock::now() - std::chrono::hours(4));

    for (auto& doc : docs) {
        auto result = repo_->insertDocument(doc);
        REQUIRE(result.has_value());
    }

    MetadataQueryBuilder qb;
    auto query = qb.withExtension(".txt")
                     .modifiedAfter(std::chrono::system_clock::now() - std::chrono::hours(2))
                     .orderByModified(false)
                     .limit(10)
                     .buildQuery();

    CHECK(query.find("file_extension = ?") != std::string::npos);
    CHECK(query.find("modified_time >= ?") != std::string::npos);
    CHECK(query.find("ORDER BY modified_time DESC") != std::string::npos);
    CHECK(query.find("LIMIT 10") != std::string::npos);

    auto params = qb.getParameters();
    CHECK(params.size() == 2);
    CHECK(params[0] == ".txt");
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Unique constraints", "[unit][metadata][schema][constraints]") {
    SECTION("Unique SHA256 hash constraint") {
        auto doc1 = createTestDocument("unique1.txt");
        doc1.sha256Hash = "unique_hash_123";
        auto result1 = repo_->insertDocument(doc1);
        REQUIRE(result1.has_value());

        // Try to insert document with same hash - should return existing doc ID
        auto doc2 = createTestDocument("unique2.txt");
        doc2.sha256Hash = "unique_hash_123"; // Same hash
        auto result2 = repo_->insertDocument(doc2);
        // Implementation uses INSERT OR IGNORE, so it returns the existing document's ID
        REQUIRE(result2.has_value());
        CHECK(result1.value() == result2.value());
    }

    SECTION("Unique metadata per document") {
        auto doc = createTestDocument("meta_unique.txt");
        auto docResult = repo_->insertDocument(doc);
        REQUIRE(docResult.has_value());
        int64_t docId = docResult.value();

        auto meta1 = repo_->setMetadata(docId, "unique_key", MetadataValue("value1"));
        REQUIRE(meta1.has_value());

        // Update should work (INSERT OR REPLACE)
        auto meta2 = repo_->setMetadata(docId, "unique_key", MetadataValue("value2"));
        REQUIRE(meta2.has_value());

        // Verify it was updated, not duplicated
        auto getMeta = repo_->getMetadata(docId, "unique_key");
        REQUIRE(getMeta.has_value());
        REQUIRE(getMeta.value().has_value());
        CHECK(getMeta.value().value().asString() == "value2");
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Extraction status handling", "[unit][metadata][schema][extraction]") {
    std::vector<ExtractionStatus> statuses = {ExtractionStatus::Pending, ExtractionStatus::Success,
                                              ExtractionStatus::Failed, ExtractionStatus::Skipped};

    std::vector<int64_t> docIds;

    for (auto status : statuses) {
        auto doc = createTestDocument("status_" + ExtractionStatusUtils::toString(status) + ".txt");
        doc.extractionStatus = status;
        if (status == ExtractionStatus::Failed) {
            doc.extractionError = "Test error message";
        }

        auto result = repo_->insertDocument(doc);
        REQUIRE(result.has_value());
        docIds.push_back(result.value());
    }

    // Retrieve and verify
    for (size_t i = 0; i < statuses.size(); i++) {
        auto result = repo_->getDocument(docIds[i]);
        REQUIRE(result.has_value());
        REQUIRE(result.value().has_value());
        auto doc = result.value().value();
        CHECK(doc.extractionStatus == statuses[i]);
        if (statuses[i] == ExtractionStatus::Failed) {
            CHECK(doc.extractionError == "Test error message");
        }
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Performance metrics", "[unit][metadata][schema][performance]") {
    const int numDocs = 1000;
    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < numDocs; i++) {
        auto doc = createTestDocument("perf_" + std::to_string(i) + ".txt");
        doc.sha256Hash = "hash_perf_" + std::to_string(i);
        auto result = repo_->insertDocument(doc);
        REQUIRE(result.has_value());
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto insertTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should be able to insert 1000 documents in under 5 seconds
    // (Windows I/O can be slower than Linux/macOS)
    CHECK(insertTime.count() < 5000);

    // Measure query performance
    start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 100; i++) {
        auto result = repo_->getDocumentByHash("hash_perf_" + std::to_string(i * 10));
        REQUIRE(result.has_value());
    }

    end = std::chrono::high_resolution_clock::now();
    auto queryTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should be able to query 100 documents by hash in under 100ms
    CHECK(queryTime.count() < 100);
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Migration version 16 hash", "[unit][metadata][schema][migration]") {
    auto result = pool_->withConnection([](Database& db) -> Result<void> {
        MigrationManager mm(db);
        auto initResult = mm.initialize();
        if (!initResult)
            return initResult.error();

        // Check current version is at least 16
        auto versionResult = mm.getCurrentVersion();
        if (!versionResult)
            return versionResult.error();

        int currentVersion = versionResult.value();
        if (currentVersion < 16) {
            return Error{ErrorCode::InvalidData, "Migration v16 not applied"};
        }

        return Result<void>();
    });

    REQUIRE(result.has_value());
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Symbol metadata schema", "[unit][metadata][schema][symbol]") {
    auto result = pool_->withConnection([](Database& db) -> Result<void> {
        // Check table exists
        auto stmt = db.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='symbol_metadata'");
        if (!stmt)
            return stmt.error();
        auto step = stmt.value().step();
        if (!step)
            return step.error();
        if (!step.value())
            return Error{ErrorCode::NotFound, "symbol_metadata table not found"};

        // Check all required columns exist
        const char* columns[] = {"symbol_id",      "document_hash", "file_path",   "symbol_name",
                                 "qualified_name", "kind",          "start_line",  "end_line",
                                 "start_offset",   "end_offset",    "return_type", "parameters",
                                 "documentation"};

        for (const char* col : columns) {
            auto colStmt =
                db.prepare("SELECT " + std::string(col) + " FROM symbol_metadata LIMIT 0");
            if (!colStmt) {
                return Error{ErrorCode::NotFound, std::string("Column not found: ") + col};
            }
        }

        // Check indices exist
        const char* indices[] = {"idx_symbol_name", "idx_symbol_document", "idx_symbol_file_path",
                                 "idx_symbol_kind", "idx_symbol_qualified"};

        for (const char* idx : indices) {
            auto idxStmt =
                db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name=?");
            if (!idxStmt)
                return idxStmt.error();
            if (auto r = idxStmt.value().bind(1, idx); !r)
                return r.error();
            auto idxStep = idxStmt.value().step();
            if (!idxStep)
                return idxStep.error();
            if (!idxStep.value()) {
                return Error{ErrorCode::NotFound, std::string("Index not found: ") + idx};
            }
        }

        return Result<void>();
    });

    REQUIRE(result.has_value());
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Symbol metadata insert and query", "[unit][metadata][schema][symbol]") {
    // Create a test document first to satisfy FK constraint
    auto doc = createTestDocument("test_symbol.cpp");
    doc.sha256Hash = "test_hash_for_symbol";
    auto insertDoc = repo_->insertDocument(doc);
    REQUIRE(insertDoc.has_value());

    // Insert multiple symbols with different kinds
    auto insertResult = pool_->withConnection([](Database& db) -> Result<void> {
        const char* symbols[][7] = {{"test_hash_for_symbol", "/test/file.cpp", "myFunction",
                                     "ns::myFunction", "function", "10", "20"},
                                    {"test_hash_for_symbol", "/test/file.cpp", "MyClass",
                                     "ns::MyClass", "class", "25", "50"},
                                    {"test_hash_for_symbol", "/test/file.cpp", "helper",
                                     "ns::MyClass::helper", "method", "30", "35"}};

        for (auto& sym : symbols) {
            auto stmt = db.prepare(R"(
                INSERT INTO symbol_metadata
                (document_hash, file_path, symbol_name, qualified_name, kind, start_line, end_line)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            )");
            if (!stmt)
                return stmt.error();

            for (int i = 0; i < 7; ++i) {
                if (i < 5) {
                    if (auto r = stmt.value().bind(i + 1, sym[i]); !r)
                        return r.error();
                } else {
                    if (auto r = stmt.value().bind(i + 1, std::atoi(sym[i])); !r)
                        return r.error();
                }
            }

            if (auto r = stmt.value().execute(); !r)
                return r.error();
        }

        return Result<void>();
    });

    REQUIRE(insertResult.has_value());

    SECTION("Query by document hash") {
        auto hashQuery = pool_->withConnection([](Database& db) -> Result<int> {
            auto stmt = db.prepare("SELECT COUNT(*) FROM symbol_metadata WHERE document_hash=?");
            if (!stmt)
                return stmt.error();
            if (auto r = stmt.value().bind(1, "test_hash_for_symbol"); !r)
                return r.error();
            auto step = stmt.value().step();
            if (!step)
                return step.error();
            if (!step.value())
                return Error{ErrorCode::NotFound, "No symbols found"};
            return stmt.value().getInt(0);
        });

        REQUIRE(hashQuery.has_value());
        CHECK(hashQuery.value() == 3);
    }

    SECTION("Query by symbol name") {
        auto nameQuery = pool_->withConnection([](Database& db) -> Result<std::string> {
            auto stmt = db.prepare("SELECT qualified_name FROM symbol_metadata WHERE symbol_name=?");
            if (!stmt)
                return stmt.error();
            if (auto r = stmt.value().bind(1, "myFunction"); !r)
                return r.error();
            auto step = stmt.value().step();
            if (!step)
                return step.error();
            if (!step.value())
                return Error{ErrorCode::NotFound, "Symbol not found"};
            return stmt.value().getString(0);
        });

        REQUIRE(nameQuery.has_value());
        CHECK(nameQuery.value() == "ns::myFunction");
    }

    SECTION("Query by kind") {
        auto kindQuery = pool_->withConnection([](Database& db) -> Result<int> {
            auto stmt = db.prepare("SELECT COUNT(*) FROM symbol_metadata WHERE kind=?");
            if (!stmt)
                return stmt.error();
            if (auto r = stmt.value().bind(1, "function"); !r)
                return r.error();
            auto step = stmt.value().step();
            if (!step)
                return step.error();
            if (!step.value())
                return 0;
            return stmt.value().getInt(0);
        });

        REQUIRE(kindQuery.has_value());
        CHECK(kindQuery.value() == 1);
    }

    SECTION("Query by file path") {
        auto pathQuery = pool_->withConnection([](Database& db) -> Result<int> {
            auto stmt = db.prepare("SELECT COUNT(*) FROM symbol_metadata WHERE file_path=?");
            if (!stmt)
                return stmt.error();
            if (auto r = stmt.value().bind(1, "/test/file.cpp"); !r)
                return r.error();
            auto step = stmt.value().step();
            if (!step)
                return step.error();
            if (!step.value())
                return 0;
            return stmt.value().getInt(0);
        });

        REQUIRE(pathQuery.has_value());
        CHECK(pathQuery.value() == 3);
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Symbol metadata foreign key constraint", "[unit][metadata][schema][symbol][fk]") {
    // Try to insert symbol with non-existent document hash (should fail)
    auto result = pool_->withConnection([](Database& db) -> Result<void> {
        auto stmt = db.prepare(R"(
            INSERT INTO symbol_metadata
            (document_hash, file_path, symbol_name, qualified_name, kind, start_line, end_line)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        )");
        if (!stmt)
            return stmt.error();

        if (auto r = stmt.value().bind(1, "nonexistent_hash"); !r)
            return r.error();
        if (auto r = stmt.value().bind(2, "/test/file.cpp"); !r)
            return r.error();
        if (auto r = stmt.value().bind(3, "someFunc"); !r)
            return r.error();
        if (auto r = stmt.value().bind(4, "someFunc"); !r)
            return r.error();
        if (auto r = stmt.value().bind(5, "function"); !r)
            return r.error();
        if (auto r = stmt.value().bind(6, 1); !r)
            return r.error();
        if (auto r = stmt.value().bind(7, 10); !r)
            return r.error();

        return stmt.value().execute();
    });

    // Should fail due to FK constraint
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().message.find("constraint") != std::string::npos);
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Repair status handling", "[unit][metadata][schema][repair]") {
    std::vector<RepairStatus> statuses = {RepairStatus::Pending, RepairStatus::Processing,
                                          RepairStatus::Completed, RepairStatus::Failed,
                                          RepairStatus::Skipped};

    std::vector<int64_t> docIds;

    for (auto status : statuses) {
        auto doc = createTestDocument("repair_" + RepairStatusUtils::toString(status) + ".txt");
        auto result = repo_->insertDocument(doc);
        REQUIRE(result.has_value());
        docIds.push_back(result.value());
    }

    // Update repair status for each document
    for (size_t i = 0; i < statuses.size(); i++) {
        auto doc = repo_->getDocument(docIds[i]);
        REQUIRE((doc.has_value() && doc.value().has_value()));
        auto hash = doc.value().value().sha256Hash;

        auto updateResult = repo_->updateDocumentRepairStatus(hash, statuses[i]);
        REQUIRE(updateResult.has_value());
    }

    // Retrieve and verify
    for (size_t i = 0; i < statuses.size(); i++) {
        auto result = repo_->getDocument(docIds[i]);
        REQUIRE(result.has_value());
        REQUIRE(result.value().has_value());
        auto doc = result.value().value();
        CHECK(doc.repairStatus == statuses[i]);
        CHECK(doc.repairAttempts >= 1);
    }
}

TEST_CASE("Repair status string conversion", "[unit][metadata][schema][repair]") {
    SECTION("toString") {
        CHECK(RepairStatusUtils::toString(RepairStatus::Pending) == "pending");
        CHECK(RepairStatusUtils::toString(RepairStatus::Processing) == "processing");
        CHECK(RepairStatusUtils::toString(RepairStatus::Completed) == "completed");
        CHECK(RepairStatusUtils::toString(RepairStatus::Failed) == "failed");
        CHECK(RepairStatusUtils::toString(RepairStatus::Skipped) == "skipped");
    }

    SECTION("fromString") {
        CHECK(RepairStatusUtils::fromString("pending") == RepairStatus::Pending);
        CHECK(RepairStatusUtils::fromString("processing") == RepairStatus::Processing);
        CHECK(RepairStatusUtils::fromString("completed") == RepairStatus::Completed);
        CHECK(RepairStatusUtils::fromString("failed") == RepairStatus::Failed);
        CHECK(RepairStatusUtils::fromString("skipped") == RepairStatus::Skipped);
        CHECK(RepairStatusUtils::fromString("unknown") == RepairStatus::Pending);
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Migration version 21 repair tracking", "[unit][metadata][schema][migration]") {
    auto result = pool_->withConnection([](Database& db) -> Result<void> {
        MigrationManager mm(db);
        auto initResult = mm.initialize();
        if (!initResult)
            return initResult.error();

        // Check current version is at least 21
        auto versionResult = mm.getCurrentVersion();
        if (!versionResult)
            return versionResult.error();

        int currentVersion = versionResult.value();
        if (currentVersion < 21) {
            return Error{ErrorCode::InvalidData, "Migration v21 not applied"};
        }

        // Check repair tracking columns exist
        const char* columns[] = {"repair_status", "repair_attempted_at", "repair_attempts"};
        for (const char* col : columns) {
            auto colStmt = db.prepare("SELECT " + std::string(col) + " FROM documents LIMIT 0");
            if (!colStmt) {
                return Error{ErrorCode::NotFound, std::string("Column not found: ") + col};
            }
        }

        // Check index exists
        auto idxStmt = db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name=?");
        if (!idxStmt)
            return idxStmt.error();
        if (auto r = idxStmt.value().bind(1, "idx_documents_repair_status"); !r)
            return r.error();
        auto idxStep = idxStmt.value().step();
        if (!idxStep)
            return idxStep.error();
        if (!idxStep.value()) {
            return Error{ErrorCode::NotFound, "Index idx_documents_repair_status not found"};
        }

        return Result<void>();
    });

    REQUIRE(result.has_value());
}
