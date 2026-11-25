/**
 * @file metadata_schema_catch2_test.cpp
 * @brief Comprehensive Catch2 tests for metadata schema, CRUD operations, FTS, and migrations
 */

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <utility>

#include "../../common/metadata_test_db.h"

#include <spdlog/spdlog.h>
#include <catch2/catch_test_macros.hpp>
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
        static const bool kSuppressInfoLogs = [] {
            spdlog::set_level(spdlog::level::warn);
            return true;
        }();
        (void)kSuppressInfoLogs;

        dbPath_ = yams::test::migrated_metadata_db_template().clone("kronos_metadata_test_");

        // Initialize connection pool
        // Use higher connection count to prevent pool exhaustion with nested queries
        ConnectionPoolConfig config;
        config.minConnections = 2;
        config.maxConnections = 4;
        pool_ = std::make_unique<ConnectionPool>(dbPath_.string(), config);
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        // Create repository against the pre-migrated snapshot.
        repo_ = std::make_unique<MetadataRepository>(
            *pool_, nullptr, MetadataRepository::SchemaBootstrapMode::AssumeReady);
    }

    ~MetadataSchemaFixture() {
        repo_.reset();
        pool_->shutdown();
        pool_.reset();
        yams::test::remove_sqlite_artifacts(dbPath_);
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

std::filesystem::path makeStandaloneMigrationDbPath(std::string_view prefix) {
    return yams::test::make_temp_sqlite_path(prefix);
}

struct StandaloneMigrationDb {
    std::filesystem::path dbPath;
    Database db;
    MigrationManager mm;

    explicit StandaloneMigrationDb(std::string_view prefix, bool registerBuiltins = true)
        : dbPath(makeStandaloneMigrationDbPath(prefix)), mm(db) {
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
        auto openResult = db.open(dbPath.string(), ConnectionMode::Create);
        REQUIRE(openResult.has_value());
        auto initResult = mm.initialize();
        REQUIRE(initResult.has_value());
        if (registerBuiltins) {
            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
        }
    }

    ~StandaloneMigrationDb() {
        db.close();
        std::error_code ec;
        std::filesystem::remove(dbPath, ec);
    }
};

template <typename... Args>
Result<void> executeBound(Database& db, const std::string& sql, const Args&... args) {
    auto stmtResult = db.prepare(sql);
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto stmt = std::move(stmtResult).value();
    if (auto bindResult = stmt.bindAll(args...); !bindResult) {
        return bindResult.error();
    }
    return stmt.execute();
}

template <typename... Args>
Result<int64_t> querySingleInt64(Database& db, const std::string& sql, const Args&... args) {
    auto stmtResult = db.prepare(sql);
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto stmt = std::move(stmtResult).value();
    if (auto bindResult = stmt.bindAll(args...); !bindResult) {
        return bindResult.error();
    }

    auto stepResult = stmt.step();
    if (!stepResult) {
        return stepResult.error();
    }
    if (!stepResult.value()) {
        return Error{ErrorCode::NotFound, "Expected row"};
    }
    return stmt.getInt64(0);
}

Result<bool> queryPlanContains(Database& db, const std::string& sql, std::string_view needle) {
    auto stmtResult = db.prepare("EXPLAIN QUERY PLAN " + sql);
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto stmt = std::move(stmtResult).value();
    while (true) {
        auto stepResult = stmt.step();
        if (!stepResult) {
            return stepResult.error();
        }
        if (!stepResult.value()) {
            return false;
        }
        if (stmt.getString(3).find(needle) != std::string::npos) {
            return true;
        }
    }
}

template <typename... Args>
Result<std::optional<std::string>> queryOptionalString(Database& db, const std::string& sql,
                                                       const Args&... args) {
    auto stmtResult = db.prepare(sql);
    if (!stmtResult) {
        return stmtResult.error();
    }

    auto stmt = std::move(stmtResult).value();
    if (auto bindResult = stmt.bindAll(args...); !bindResult) {
        return bindResult.error();
    }

    auto stepResult = stmt.step();
    if (!stepResult) {
        return stepResult.error();
    }
    if (!stepResult.value()) {
        return std::optional<std::string>{};
    }
    if (stmt.isNull(0)) {
        return std::optional<std::string>{};
    }
    return std::optional<std::string>{stmt.getString(0)};
}

} // namespace

TEST_CASE_METHOD(MetadataSchemaFixture, "Content CRUD operations",
                 "[unit][metadata][schema][content]") {
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
        CHECK((retrieved.documentId == docId));
        CHECK((retrieved.contentText == content.contentText));
        CHECK((retrieved.language == content.language));
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
        CHECK((updatedResult.value().value().contentText == "Updated content"));
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

TEST_CASE_METHOD(MetadataSchemaFixture, "Metadata operations",
                 "[unit][metadata][schema][metadata]") {
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
        CHECK((authorResult.value().value().asString() == "John Doe"));
        CHECK((authorResult.value().value().type == MetadataValueType::String));
    }

    SECTION("Set and get integer metadata") {
        auto intResult = repo_->setMetadata(docId, "version", MetadataValue(int64_t(3)));
        REQUIRE(intResult.has_value());

        auto versionResult = repo_->getMetadata(docId, "version");
        REQUIRE(versionResult.has_value());
        REQUIRE(versionResult.value().has_value());
        CHECK((versionResult.value().value().asInteger() == 3));
        CHECK((versionResult.value().value().type == MetadataValueType::Integer));
    }

    SECTION("Set and get double metadata") {
        auto doubleResult = repo_->setMetadata(docId, "score", MetadataValue(4.5));
        REQUIRE(doubleResult.has_value());

        auto scoreResult = repo_->getMetadata(docId, "score");
        REQUIRE(scoreResult.has_value());
        REQUIRE(scoreResult.value().has_value());
        CHECK((scoreResult.value().value().asReal() == 4.5));
    }

    SECTION("Set and get bool metadata") {
        auto boolResult = repo_->setMetadata(docId, "published", MetadataValue(true));
        REQUIRE(boolResult.has_value());

        auto publishedResult = repo_->getMetadata(docId, "published");
        REQUIRE(publishedResult.has_value());
        REQUIRE(publishedResult.value().has_value());
        CHECK((publishedResult.value().value().asBoolean() == true));
    }

    SECTION("Get all metadata") {
        repo_->setMetadata(docId, "author", MetadataValue("John Doe"));
        repo_->setMetadata(docId, "version", MetadataValue(int64_t(3)));
        repo_->setMetadata(docId, "score", MetadataValue(4.5));
        repo_->setMetadata(docId, "published", MetadataValue(true));

        auto allResult = repo_->getAllMetadata(docId);
        REQUIRE(allResult.has_value());
        auto allMeta = allResult.value();
        CHECK((allMeta.size() == 4));
        CHECK((allMeta.count("author") > 0));
        CHECK((allMeta.count("version") > 0));
        CHECK((allMeta.count("score") > 0));
        CHECK((allMeta.count("published") > 0));
    }

    SECTION("Update metadata replaces value") {
        auto meta1 = repo_->setMetadata(docId, "author", MetadataValue("value1"));
        REQUIRE(meta1.has_value());

        auto meta2 = repo_->setMetadata(docId, "author", MetadataValue("value2"));
        REQUIRE(meta2.has_value());

        auto getMeta = repo_->getMetadata(docId, "author");
        REQUIRE(getMeta.has_value());
        REQUIRE(getMeta.value().has_value());
        CHECK((getMeta.value().value().asString() == "value2"));
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

TEST_CASE_METHOD(MetadataSchemaFixture, "Relationship operations",
                 "[unit][metadata][schema][relationship]") {
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
        rel1.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto rel1Result = repo_->insertRelationship(rel1);
        REQUIRE(rel1Result.has_value());

        DocumentRelationship rel2;
        rel2.parentId = parentId;
        rel2.childId = child2Id;
        rel2.relationshipType = RelationshipType::Contains;
        rel2.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto rel2Result = repo_->insertRelationship(rel2);
        REQUIRE(rel2Result.has_value());

        auto parentRels = repo_->getRelationships(parentId);
        REQUIRE(parentRels.has_value());
        CHECK((parentRels.value().size() == 2));

        auto childRels = repo_->getRelationships(child1Id);
        REQUIRE(childRels.has_value());
        CHECK((childRels.value().size() == 1));
        CHECK((childRels.value()[0].parentId == parentId));
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

    SECTION("Duplicate relationship is idempotent") {
        DocumentRelationship rel;
        rel.parentId = parentId;
        rel.childId = child1Id;
        rel.relationshipType = RelationshipType::VersionOf;
        rel.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto first = repo_->insertRelationship(rel);
        REQUIRE(first.has_value());

        auto second = repo_->insertRelationship(rel);
        REQUIRE(second.has_value());

        auto parentRels = repo_->getRelationships(parentId);
        REQUIRE(parentRels.has_value());
        CHECK((parentRels.value().size() == 1));
    }

    SECTION("Delete relationship") {
        DocumentRelationship rel;
        rel.parentId = parentId;
        rel.childId = child1Id;
        rel.relationshipType = RelationshipType::Contains;
        rel.createdTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());

        auto relResult = repo_->insertRelationship(rel);
        REQUIRE(relResult.has_value());
        int64_t relId = relResult.value();

        auto deleteResult = repo_->deleteRelationship(relId);
        REQUIRE(deleteResult.has_value());

        auto afterDelete = repo_->getRelationships(parentId);
        REQUIRE(afterDelete.has_value());
        CHECK((afterDelete.value().size() == 0));
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
        CHECK((quickSearch.results.size() == 2)); // doc1 and doc3
    }

    SECTION("Search for programming") {
        auto cppResult = repo_->search("programming", 10, 0);
        REQUIRE(cppResult.has_value());
        auto cppSearch = cppResult.value();
        REQUIRE(cppSearch.isSuccess());
        CHECK((cppSearch.results.size() >= 2)); // At least doc2 and doc4
    }

    SECTION("Test pagination") {
        auto page1Result = repo_->search("programming", 2, 0);
        REQUIRE(page1Result.has_value());
        CHECK((page1Result.value().results.size() == 2));

        auto page2Result = repo_->search("programming", 2, 2);
        REQUIRE(page2Result.has_value());
        // May have 0 or 1 result depending on total matches
    }

    SECTION("Remove from index") {
        auto removeResult = repo_->removeFromIndex(docIds[0]);
        REQUIRE(removeResult.has_value());

        // Use "lazy dog" which only appears in doc1 - avoids fuzzy OR fallback
        // matching doc3 which contains "quick" (part of "quick brown fox")
        auto afterRemove = repo_->search("lazy dog", 10, 0);
        REQUIRE(afterRemove.has_value());
        CHECK((afterRemove.value().results.size() == 0));
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Search history",
                 "[unit][metadata][schema][search-history]") {
    SECTION("Insert and retrieve search history") {
        SearchHistoryEntry entry1;
        entry1.query = "test query";
        entry1.queryTime =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
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
            entry.resultsCount = static_cast<std::int64_t>(i) * 2;
            entry.executionTimeMs = 10 + i;

            auto result = repo_->insertSearchHistory(entry);
            REQUIRE(result.has_value());
        }

        auto recentResult = repo_->getRecentSearches(5);
        REQUIRE(recentResult.has_value());
        auto recent = recentResult.value();
        CHECK((recent.size() == 5));

        // Verify ordering (most recent first)
        CHECK((recent[0].query == "query 9"));
        CHECK((recent[1].query == "query 8"));
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Saved queries",
                 "[unit][metadata][schema][saved-queries]") {
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
        CHECK((retrieved.name == query1.name));
        CHECK((retrieved.query == query1.query));
    }

    SECTION("Update saved query") {
        auto getResult = repo_->getSavedQuery(queryId);
        REQUIRE(getResult.has_value());
        REQUIRE(getResult.value().has_value());
        auto retrieved = getResult.value().value();

        retrieved.useCount = 5;
        retrieved.lastUsed =
            std::chrono::floor<std::chrono::seconds>(std::chrono::system_clock::now());
        auto updateResult = repo_->updateSavedQuery(retrieved);
        REQUIRE(updateResult.has_value());
    }

    SECTION("Get all saved queries") {
        auto allResult = repo_->getAllSavedQueries();
        REQUIRE(allResult.has_value());
        CHECK((allResult.value().size() >= 1));
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
        CHECK((pathResult.value().size() == 5));
    }

    SECTION("Find by extension") {
        auto extResult = repo_->findDocumentsByExtension(".txt");
        REQUIRE(extResult.has_value());
        CHECK((extResult.value().size() >= 5));
    }

    SECTION("Find modified since") {
        auto since = std::chrono::system_clock::now() - std::chrono::hours(3);
        auto sinceResult = repo_->findDocumentsModifiedSince(since);
        REQUIRE(sinceResult.has_value());
        CHECK((sinceResult.value().size() >= 3));
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
        CHECK((static_cast<std::size_t>(countResult.value()) == extensions.size()));
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
        CHECK((embeddingIndexedResult.value() == 0));

        auto extractedCount = repo_->getContentExtractedDocumentCount();
        REQUIRE(extractedCount.has_value());
        CHECK((extractedCount.value() == 3));
    }

    SECTION("Get counts by extension") {
        auto extCountResult = repo_->getDocumentCountsByExtension();
        REQUIRE(extCountResult.has_value());
        auto extCounts = extCountResult.value();
        CHECK((extCounts[".txt"] == 3));
        CHECK((extCounts[".pdf"] == 2));
        CHECK((extCounts[".doc"] == 1));
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Unique constraints",
                 "[unit][metadata][schema][constraints]") {
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
        CHECK((result1.value() == result2.value()));
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
        CHECK((getMeta.value().value().asString() == "value2"));
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Extraction status handling",
                 "[unit][metadata][schema][extraction]") {
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
        CHECK((doc.extractionStatus == statuses[i]));
        if (statuses[i] == ExtractionStatus::Failed) {
            CHECK((doc.extractionError == "Test error message"));
        }
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Performance metrics",
                 "[unit][metadata][schema][performance][slow]") {
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
    CHECK((insertTime.count() < 5000));

    // Measure query performance
    start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < 100; i++) {
        auto result = repo_->getDocumentByHash("hash_perf_" + std::to_string(i * 10));
        REQUIRE(result.has_value());
    }

    end = std::chrono::high_resolution_clock::now();
    auto queryTime = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should be able to query 100 documents by hash in under 100ms
    CHECK((queryTime.count() < 100));
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Migration version 16 hash",
                 "[unit][metadata][schema][migration]") {
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

TEST_CASE_METHOD(MetadataSchemaFixture, "Repair status handling",
                 "[unit][metadata][schema][repair]") {
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
        CHECK((doc.repairStatus == statuses[i]));
        CHECK((doc.repairAttempts >= 1));
    }
}

TEST_CASE("Repair status string conversion", "[unit][metadata][schema][repair]") {
    SECTION("toString") {
        CHECK((RepairStatusUtils::toString(RepairStatus::Pending) == "pending"));
        CHECK((RepairStatusUtils::toString(RepairStatus::Processing) == "processing"));
        CHECK((RepairStatusUtils::toString(RepairStatus::Completed) == "completed"));
        CHECK((RepairStatusUtils::toString(RepairStatus::Failed) == "failed"));
        CHECK((RepairStatusUtils::toString(RepairStatus::Skipped) == "skipped"));
    }

    SECTION("fromString") {
        CHECK((RepairStatusUtils::fromString("pending") == RepairStatus::Pending));
        CHECK((RepairStatusUtils::fromString("processing") == RepairStatus::Processing));
        CHECK((RepairStatusUtils::fromString("completed") == RepairStatus::Completed));
        CHECK((RepairStatusUtils::fromString("failed") == RepairStatus::Failed));
        CHECK((RepairStatusUtils::fromString("skipped") == RepairStatus::Skipped));
        CHECK((RepairStatusUtils::fromString("unknown") == RepairStatus::Pending));
    }
}

TEST_CASE_METHOD(MetadataSchemaFixture, "Migration version 21 repair tracking",
                 "[unit][metadata][schema][migration]") {
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

TEST_CASE_METHOD(MetadataSchemaFixture, "Migration version 31 semantic duplicate schema",
                 "[unit][metadata][schema][migration]") {
    auto result = pool_->withConnection([](Database& db) -> Result<void> {
        MigrationManager mm(db);
        auto initResult = mm.initialize();
        if (!initResult)
            return initResult.error();

        auto versionResult = mm.getCurrentVersion();
        if (!versionResult)
            return versionResult.error();
        if (versionResult.value() < 31) {
            return Error{ErrorCode::InvalidData, "Migration v31 not applied"};
        }

        const char* groupColumns[] = {"group_key",
                                      "algorithm_version",
                                      "status",
                                      "review_state",
                                      "canonical_document_id",
                                      "member_count",
                                      "max_pair_score",
                                      "threshold",
                                      "evidence_json",
                                      "created_at",
                                      "updated_at",
                                      "last_computed_at"};
        for (const char* col : groupColumns) {
            auto stmt = db.prepare("SELECT " + std::string(col) +
                                   " FROM semantic_duplicate_groups LIMIT 0");
            if (!stmt) {
                return Error{ErrorCode::NotFound, std::string("Column not found: ") + col};
            }
        }

        const char* memberColumns[] = {"group_id", "document_id", "role",       "pair_score",
                                       "decision", "reason",      "created_at", "updated_at"};
        for (const char* col : memberColumns) {
            auto stmt = db.prepare("SELECT " + std::string(col) +
                                   " FROM semantic_duplicate_group_members LIMIT 0");
            if (!stmt) {
                return Error{ErrorCode::NotFound, std::string("Column not found: ") + col};
            }
        }

        const char* indices[] = {"idx_semantic_duplicate_groups_status",
                                 "idx_semantic_duplicate_groups_canonical_document",
                                 "idx_semantic_duplicate_group_members_group",
                                 "idx_semantic_duplicate_group_members_document"};
        for (const char* idx : indices) {
            auto idxStmt =
                db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name=?");
            if (!idxStmt)
                return idxStmt.error();
            if (auto r = idxStmt.value().bind(1, idx); !r)
                return r.error();
            auto step = idxStmt.value().step();
            if (!step)
                return step.error();
            if (!step.value()) {
                return Error{ErrorCode::NotFound, std::string("Index not found: ") + idx};
            }
        }

        return Result<void>();
    });

    REQUIRE(result.has_value());
}

TEST_CASE("MigrationBuilder composes reversible migration definitions",
          "[unit][metadata][schema][migration]") {
    auto migration = MigrationBuilder(99, "builder smoke test")
                         .createTable("widgets")
                         .addColumn("widgets", "name", "TEXT", false)
                         .createIndex("idx_widgets_name", "widgets", {"name"}, true)
                         .dropIndex("idx_widgets_old")
                         .renameTable("widgets", "gadgets")
                         .renameColumn("gadgets", "name", "display_name")
                         .dropColumn("gadgets", "legacy_column")
                         .dropTable("gadgets")
                         .up("PRAGMA optimize;")
                         .down("PRAGMA wal_checkpoint(TRUNCATE);")
                         .wrapInTransaction(false)
                         .build();

    CHECK((migration.version == 99));
    CHECK((migration.name == "builder smoke test"));
    CHECK_FALSE(migration.wrapInTransaction);
    CHECK((migration.upSQL.find("CREATE TABLE widgets") != std::string::npos));
    CHECK((migration.upSQL.find("ALTER TABLE widgets ADD COLUMN name TEXT NOT NULL") !=
           std::string::npos));
    CHECK((migration.upSQL.find("CREATE UNIQUE INDEX idx_widgets_name") != std::string::npos));
    CHECK((migration.upSQL.find("DROP INDEX IF EXISTS idx_widgets_old") != std::string::npos));
    CHECK((migration.upSQL.find("ALTER TABLE widgets RENAME TO gadgets") != std::string::npos));
    CHECK((migration.upSQL.find("ALTER TABLE gadgets RENAME COLUMN name TO display_name") !=
           std::string::npos));
    CHECK((migration.upSQL.find("legacy_column") == std::string::npos));
    CHECK((migration.upSQL.find("DROP TABLE IF EXISTS gadgets") != std::string::npos));
    CHECK((migration.upSQL.find("PRAGMA optimize;") != std::string::npos));
    CHECK((migration.downSQL.find("PRAGMA wal_checkpoint(TRUNCATE);") != std::string::npos));
    CHECK((migration.downSQL.find("DROP INDEX IF EXISTS idx_widgets_name") != std::string::npos));
}

TEST_CASE("MigrationManager records failed migrations and keeps readiness pending",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_manager_failure", false};

    Migration okMigration;
    okMigration.version = 1;
    okMigration.name = "create widgets";
    okMigration.upSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
    okMigration.downSQL = "DROP TABLE IF EXISTS widgets;";
    okMigration.created = std::chrono::system_clock::now();

    Migration failingMigration;
    failingMigration.version = 2;
    failingMigration.name = "broken migration";
    failingMigration.upSQL = "CREATE TABLE broken (";
    failingMigration.downSQL = "DROP TABLE IF EXISTS broken;";
    failingMigration.created = std::chrono::system_clock::now();

    std::vector<std::string> progressNames;
    db.mm.setProgressCallback(
        [&progressNames](int, int, const std::string& name) { progressNames.push_back(name); });
    db.mm.registerMigration(okMigration);
    db.mm.registerMigration(failingMigration);

    CHECK((db.mm.getLatestVersion() == 2));
    auto initialNeedsMigration = db.mm.needsMigration();
    REQUIRE(initialNeedsMigration.has_value());
    CHECK((initialNeedsMigration.value()));

    auto migrateResult = db.mm.migrate();
    REQUIRE_FALSE(migrateResult.has_value());
    CHECK((progressNames.size() == 2));
    CHECK((progressNames[0] == "create widgets"));
    CHECK((progressNames[1] == "broken migration"));

    auto currentVersion = db.mm.getCurrentVersion();
    REQUIRE(currentVersion.has_value());
    CHECK((currentVersion.value() == 1));

    auto historyResult = db.mm.getHistory();
    REQUIRE(historyResult.has_value());
    bool sawSuccess = false;
    bool sawFailure = false;
    for (const auto& entry : historyResult.value()) {
        if (entry.version == 1 && entry.success) {
            sawSuccess = true;
        }
        if (entry.version == 2 && !entry.success && entry.name == "broken migration") {
            sawFailure = true;
        }
    }
    CHECK((sawSuccess));
    CHECK((sawFailure));

    auto integrityResult = db.mm.verifyIntegrity();
    REQUIRE(integrityResult.has_value());

    auto stillNeedsMigration = db.mm.needsMigration();
    REQUIRE(stillNeedsMigration.has_value());
    CHECK((stillNeedsMigration.value()));
}

TEST_CASE("Migration version 26 deduplicates symbol metadata before adding uniqueness",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_v26_symbol_uniqueness"};
    REQUIRE(db.mm.migrateTo(25).has_value());

    REQUIRE(executeBound(db.db,
                         R"(
            INSERT INTO documents (
                file_path, file_name, file_extension, file_size, sha256_hash, mime_type,
                created_time, modified_time, indexed_time, content_extracted, extraction_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )",
                         "/tmp/symbol.cpp", "symbol.cpp", ".cpp", int64_t{128}, "doc-hash-v26",
                         "text/x-c++src", int64_t{1}, int64_t{1}, int64_t{1}, 1, "success")
                .has_value());

    REQUIRE(db.db
                .execute(R"(
        DROP TABLE IF EXISTS symbol_metadata;
        CREATE TABLE symbol_metadata (
            symbol_id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_hash TEXT NOT NULL,
            file_path TEXT NOT NULL,
            symbol_name TEXT NOT NULL,
            qualified_name TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_line INTEGER,
            end_line INTEGER,
            start_offset INTEGER,
            end_offset INTEGER,
            return_type TEXT,
            parameters TEXT,
            documentation TEXT,
            FOREIGN KEY (document_hash) REFERENCES documents(sha256_hash) ON DELETE CASCADE
        );
    )")
                .has_value());

    REQUIRE(executeBound(db.db,
                         R"(
            INSERT INTO symbol_metadata (
                document_hash, file_path, symbol_name, qualified_name, kind, start_line, end_line
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        )",
                         "doc-hash-v26", "/tmp/symbol.cpp", "duplicate", "ns::duplicate",
                         "function", 10, 12)
                .has_value());
    REQUIRE(executeBound(db.db,
                         R"(
            INSERT INTO symbol_metadata (
                document_hash, file_path, symbol_name, qualified_name, kind, start_line, end_line
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        )",
                         "doc-hash-v26", "/tmp/symbol.cpp", "duplicate_latest", "ns::duplicate",
                         "function", 20, 24)
                .has_value());

    auto preCount = querySingleInt64(db.db, "SELECT COUNT(*) FROM symbol_metadata");
    REQUIRE(preCount.has_value());
    CHECK((preCount.value() == 2));

    REQUIRE(db.mm.migrateTo(26).has_value());

    auto postCount = querySingleInt64(db.db, "SELECT COUNT(*) FROM symbol_metadata");
    REQUIRE(postCount.has_value());
    CHECK((postCount.value() == 1));

    auto survivingStartLine = querySingleInt64(
        db.db,
        "SELECT start_line FROM symbol_metadata WHERE document_hash = ? AND qualified_name = ?",
        "doc-hash-v26", "ns::duplicate");
    REQUIRE(survivingStartLine.has_value());
    CHECK((survivingStartLine.value() == 20));

    auto duplicateInsert = executeBound(db.db,
                                        R"(
            INSERT INTO symbol_metadata (
                document_hash, file_path, symbol_name, qualified_name, kind, start_line, end_line
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        )",
                                        "doc-hash-v26", "/tmp/symbol.cpp", "duplicate_again",
                                        "ns::duplicate", "function", 30, 32);
    REQUIRE_FALSE(duplicateInsert.has_value());
    CHECK((duplicateInsert.error().message.find("constraint") != std::string::npos));
}

TEST_CASE("Migration version 27 backfills metadata value counts and maintains trigger state",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_v27_value_counts"};
    REQUIRE(db.mm.migrateTo(26).has_value());

    for (int64_t docId = 1; docId <= 3; ++docId) {
        const auto suffix = std::to_string(docId);
        REQUIRE(executeBound(db.db,
                             R"(
                INSERT INTO documents (
                    id, file_path, file_name, file_extension, file_size, sha256_hash, mime_type,
                    created_time, modified_time, indexed_time, content_extracted, extraction_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            )",
                             docId, "/tmp/doc" + suffix + ".txt", "doc" + suffix + ".txt", ".txt",
                             int64_t{64}, "doc-hash-v27-" + suffix, "text/plain", int64_t{1},
                             int64_t{1}, int64_t{1}, 1, "success")
                    .has_value());
    }

    REQUIRE(executeBound(
                db.db,
                "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?)",
                int64_t{1}, "author", "alice", "string")
                .has_value());
    REQUIRE(executeBound(
                db.db,
                "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?)",
                int64_t{2}, "author", "alice", "string")
                .has_value());
    REQUIRE(executeBound(
                db.db,
                "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?)",
                int64_t{3}, "author", "bob", "string")
                .has_value());
    REQUIRE(executeBound(
                db.db,
                "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?)",
                int64_t{3}, "empty", "", "string")
                .has_value());

    REQUIRE(db.mm.migrateTo(27).has_value());

    const auto countSql =
        "SELECT COALESCE((SELECT count FROM metadata_value_counts WHERE key = ? AND value = ?), 0)";
    auto aliceCount = querySingleInt64(db.db, countSql, "author", "alice");
    auto bobCount = querySingleInt64(db.db, countSql, "author", "bob");
    auto emptyCount = querySingleInt64(db.db, countSql, "empty", "");
    REQUIRE(aliceCount.has_value());
    REQUIRE(bobCount.has_value());
    REQUIRE(emptyCount.has_value());
    CHECK((aliceCount.value() == 2));
    CHECK((bobCount.value() == 1));
    CHECK((emptyCount.value() == 0));

    REQUIRE(executeBound(db.db, "UPDATE metadata SET value = ? WHERE document_id = ? AND key = ?",
                         "alice", int64_t{3}, "author")
                .has_value());
    aliceCount = querySingleInt64(db.db, countSql, "author", "alice");
    bobCount = querySingleInt64(db.db, countSql, "author", "bob");
    REQUIRE(aliceCount.has_value());
    REQUIRE(bobCount.has_value());
    CHECK((aliceCount.value() == 3));
    CHECK((bobCount.value() == 0));

    REQUIRE(executeBound(db.db, "DELETE FROM metadata WHERE document_id = ? AND key = ?",
                         int64_t{1}, "author")
                .has_value());
    aliceCount = querySingleInt64(db.db, countSql, "author", "alice");
    REQUIRE(aliceCount.has_value());
    CHECK((aliceCount.value() == 2));

    REQUIRE(executeBound(
                db.db,
                "INSERT INTO metadata (document_id, key, value, value_type) VALUES (?, ?, ?, ?)",
                int64_t{1}, "topic", "sqlite", "string")
                .has_value());
    auto topicCount = querySingleInt64(db.db, countSql, "topic", "sqlite");
    REQUIRE(topicCount.has_value());
    CHECK((topicCount.value() == 1));
}

TEST_CASE("Migration version 30 rewrites legacy knowledge-graph path node prefixes",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_v30_path_prefixes"};
    REQUIRE(db.mm.migrateTo(29).has_value());

    REQUIRE(executeBound(
                db.db,
                "INSERT INTO kg_nodes (node_key, label, type, created_time) VALUES (?, ?, ?, ?)",
                "file:/tmp/a.cpp", "a.cpp", "file", int64_t{1})
                .has_value());
    REQUIRE(executeBound(
                db.db,
                "INSERT INTO kg_nodes (node_key, label, type, created_time) VALUES (?, ?, ?, ?)",
                "dir:/tmp", "/tmp", "directory", int64_t{1})
                .has_value());
    REQUIRE(executeBound(
                db.db,
                "INSERT INTO kg_nodes (node_key, label, type, created_time) VALUES (?, ?, ?, ?)",
                "path:file:/tmp/keep.cpp", "keep.cpp", "file", int64_t{1})
                .has_value());

    REQUIRE(db.mm.migrateTo(30).has_value());

    auto migratedFile = querySingleInt64(db.db, "SELECT COUNT(*) FROM kg_nodes WHERE node_key = ?",
                                         "path:file:/tmp/a.cpp");
    auto migratedDir = querySingleInt64(db.db, "SELECT COUNT(*) FROM kg_nodes WHERE node_key = ?",
                                        "path:dir:/tmp");
    auto preservedModern = querySingleInt64(
        db.db, "SELECT COUNT(*) FROM kg_nodes WHERE node_key = ?", "path:file:/tmp/keep.cpp");
    auto remainingLegacy = querySingleInt64(
        db.db,
        "SELECT COUNT(*) FROM kg_nodes WHERE node_key LIKE 'file:%' OR node_key LIKE 'dir:%'");
    REQUIRE(migratedFile.has_value());
    REQUIRE(migratedDir.has_value());
    REQUIRE(preservedModern.has_value());
    REQUIRE(remainingLegacy.has_value());
    CHECK((migratedFile.value() == 1));
    CHECK((migratedDir.value() == 1));
    CHECK((preservedModern.value() == 1));
    CHECK((remainingLegacy.value() == 0));
}

TEST_CASE("Migration version 35 narrows the documents path FTS update trigger",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_v35_documents_au"};
    REQUIRE(db.mm.migrateTo(34).has_value());

    REQUIRE(db.db
                .execute(R"(
        DROP TRIGGER IF EXISTS documents_au;
        CREATE TRIGGER documents_au
        AFTER UPDATE ON documents BEGIN
            INSERT INTO documents_path_fts(documents_path_fts, rowid, file_path)
            VALUES('delete', old.id, old.file_path);
            INSERT INTO documents_path_fts(rowid, file_path)
            VALUES(new.id, new.file_path);
        END;
    )")
                .has_value());

    auto beforeSql = queryOptionalString(
        db.db, "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?", "documents_au");
    REQUIRE(beforeSql.has_value());
    REQUIRE(beforeSql.value().has_value());
    CHECK(
        (beforeSql.value()->find("WHEN old.file_path IS NOT new.file_path") == std::string::npos));

    REQUIRE(db.mm.migrateTo(35).has_value());

    auto narrowedSql = queryOptionalString(
        db.db, "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?", "documents_au");
    REQUIRE(narrowedSql.has_value());
    REQUIRE(narrowedSql.value().has_value());
    CHECK((narrowedSql.value()->find("WHEN old.file_path IS NOT new.file_path") !=
           std::string::npos));

    REQUIRE(db.mm.rollbackTo(34).has_value());

    auto rolledBackSql = queryOptionalString(
        db.db, "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?", "documents_au");
    REQUIRE(rolledBackSql.has_value());
    REQUIRE(rolledBackSql.value().has_value());
    CHECK((rolledBackSql.value()->find("WHEN old.file_path IS NOT new.file_path") ==
           std::string::npos));
}

TEST_CASE("Migration version 36 adds indexed trigram symbol lookup",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_v36_symbol_fts"};
    REQUIRE(db.mm.migrateTo(35).has_value());

    REQUIRE(db.db
                .execute(R"(
        INSERT INTO documents (sha256_hash, file_name, file_path, file_size, mime_type)
        VALUES ('symbol-fts-doc', 'coordinator.cpp', '/tmp/coordinator.cpp', 10, 'text/plain');
        INSERT INTO symbol_metadata (
            document_hash, file_path, symbol_name, qualified_name, kind
        ) VALUES (
            'symbol-fts-doc', '/tmp/coordinator.cpp', 'WriteCoordinator',
            'yams::daemon::WriteCoordinator', 'class'
        );
    )")
                .has_value());

    REQUIRE(db.mm.migrateTo(36).has_value());

    auto ftsExists = db.db.tableExists("symbol_metadata_fts");
    REQUIRE(ftsExists.has_value());
    CHECK(ftsExists.value());

    auto count = querySingleInt64(
        db.db, "SELECT COUNT(*) FROM symbol_metadata_fts WHERE symbol_name LIKE '%Coordinator%'");
    REQUIRE(count.has_value());
    CHECK((count.value() == 1));

    {
        auto planResult = db.db.prepare("EXPLAIN QUERY PLAN SELECT rowid FROM symbol_metadata_fts "
                                        "WHERE symbol_name LIKE '%Coordinator%'");
        REQUIRE(planResult.has_value());
        auto plan = std::move(planResult).value();
        bool usesVirtualIndex = false;
        while (true) {
            auto step = plan.step();
            REQUIRE(step.has_value());
            if (!step.value())
                break;
            usesVirtualIndex = usesVirtualIndex ||
                               plan.getString(3).find("VIRTUAL TABLE INDEX") != std::string::npos;
        }
        CHECK(usesVirtualIndex);
    }

    REQUIRE(db.mm.rollbackTo(35).has_value());
    auto removed = db.db.tableExists("symbol_metadata_fts");
    REQUIRE(removed.has_value());
    CHECK_FALSE(removed.value());
}

TEST_CASE("Migration version 37 indexes document graph cleanup predicates",
          "[unit][metadata][schema][migration][kg]") {
    StandaloneMigrationDb db{"migration_v37_kg_cleanup_indexes"};
    REQUIRE(db.mm.migrateTo(36).has_value());
    REQUIRE(db.db
                .execute(R"(
        INSERT INTO kg_nodes (node_key, label, type, properties)
        VALUES ('opaque:before-migration', 'opaque', 'test', 'not-json');
    )")
                .has_value());
    REQUIRE(db.mm.migrateTo(37).has_value());

    const std::array expectedIndexes{
        "idx_kg_nodes_document_hash",
        "idx_kg_nodes_file_path",
        "idx_kg_nodes_source_file",
        "idx_kg_edges_source_file",
    };
    for (const auto* index : expectedIndexes) {
        auto count = querySingleInt64(
            db.db, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index);
        REQUIRE(count.has_value());
        CHECK((count.value() == 1));
    }

    // Graph properties predate the JSON convention and may contain opaque plugin text. The
    // cleanup indexes must not make otherwise-valid node or edge writes fail.
    REQUIRE(db.db
                .execute(R"(
        INSERT INTO kg_nodes (node_key, label, type, properties)
        VALUES ('opaque:a', 'a', 'test', 'not-json'),
               ('opaque:b', 'b', 'test', 'also-not-json');
        INSERT INTO kg_edges (src_node_id, dst_node_id, relation, properties)
        SELECT src.id, dst.id, 'opaque', 'edge-not-json'
        FROM kg_nodes src, kg_nodes dst
        WHERE src.node_key = 'opaque:a' AND dst.node_key = 'opaque:b';
    )")
                .has_value());

    auto documentHashPlan =
        queryPlanContains(db.db,
                          "DELETE FROM kg_nodes WHERE CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.document_hash') END = 'hash'",
                          "idx_kg_nodes_document_hash");
    REQUIRE(documentHashPlan.has_value());
    CHECK(documentHashPlan.value());

    auto sourceNodePlan =
        queryPlanContains(db.db,
                          "DELETE FROM kg_nodes WHERE CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.file_path') END = '/tmp/a.cpp' "
                          "OR CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.source_file') END = '/tmp/a.cpp'",
                          "MULTI-INDEX OR");
    REQUIRE(sourceNodePlan.has_value());
    CHECK(sourceNodePlan.value());

    auto sourceEdgePlan =
        queryPlanContains(db.db,
                          "DELETE FROM kg_edges WHERE CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.source_file') END = '/tmp/a.cpp'",
                          "idx_kg_edges_source_file");
    REQUIRE(sourceEdgePlan.has_value());
    CHECK(sourceEdgePlan.value());

    REQUIRE(db.mm.rollbackTo(36).has_value());
    for (const auto* index : expectedIndexes) {
        auto count = querySingleInt64(
            db.db, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?", index);
        REQUIRE(count.has_value());
        CHECK((count.value() == 0));
    }
}

TEST_CASE("Migration version 47 rebuilds graph cleanup indexes left unguarded",
          "[unit][metadata][schema][migration][kg]") {
    StandaloneMigrationDb db{"migration_v47_kg_cleanup_rebuild"};
    REQUIRE(db.mm.migrateTo(36).has_value());
    // Databases from before the json_valid guard already held these names on the bare
    // expression, so v37's IF NOT EXISTS kept them.
    REQUIRE(db.db
                .execute(R"(
        CREATE INDEX idx_kg_nodes_document_hash ON kg_nodes(json_extract(properties, '$.document_hash'))
            WHERE json_extract(properties, '$.document_hash') IS NOT NULL;
        CREATE INDEX idx_kg_nodes_file_path ON kg_nodes(json_extract(properties, '$.file_path'))
            WHERE json_extract(properties, '$.file_path') IS NOT NULL;
        CREATE INDEX idx_kg_nodes_source_file ON kg_nodes(json_extract(properties, '$.source_file'))
            WHERE json_extract(properties, '$.source_file') IS NOT NULL;
        CREATE INDEX idx_kg_edges_source_file ON kg_edges(json_extract(properties, '$.source_file'))
            WHERE json_extract(properties, '$.source_file') IS NOT NULL;
    )")
                .has_value());
    REQUIRE(db.mm.migrateTo(46).has_value());

    const char* kEdgeDelete = "DELETE FROM kg_edges WHERE CASE WHEN json_valid(properties) "
                              "THEN json_extract(properties, '$.source_file') END = '/tmp/a.cpp'";
    auto before = queryPlanContains(db.db, kEdgeDelete, "idx_kg_edges_source_file");
    REQUIRE(before.has_value());
    CHECK_FALSE(before.value());

    REQUIRE(db.mm.migrateTo(47).has_value());

    auto edgePlan = queryPlanContains(db.db, kEdgeDelete, "idx_kg_edges_source_file");
    REQUIRE(edgePlan.has_value());
    CHECK(edgePlan.value());
    auto hashPlan =
        queryPlanContains(db.db,
                          "DELETE FROM kg_nodes WHERE CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.document_hash') END = 'hash'",
                          "idx_kg_nodes_document_hash");
    REQUIRE(hashPlan.has_value());
    CHECK(hashPlan.value());
    auto pathPlan =
        queryPlanContains(db.db,
                          "DELETE FROM kg_nodes WHERE CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.file_path') END = '/tmp/a.cpp' "
                          "OR CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.source_file') END = '/tmp/a.cpp'",
                          "MULTI-INDEX OR");
    REQUIRE(pathPlan.has_value());
    CHECK(pathPlan.value());

    // The bare indexes rejected opaque properties; the guarded ones accept them.
    CHECK(db.db
              .execute("INSERT INTO kg_nodes (node_key, label, type, properties) "
                       "VALUES ('opaque:v47', 'o', 'test', 'not-json')")
              .has_value());
}

TEST_CASE("Migration version 47 leaves guarded graph cleanup indexes in place",
          "[unit][metadata][schema][migration][kg]") {
    StandaloneMigrationDb db{"migration_v47_kg_cleanup_noop"};
    REQUIRE(db.mm.migrateTo(46).has_value());
    auto before = querySingleInt64(
        db.db, "SELECT rootpage FROM sqlite_master WHERE type = 'index' AND name = ?",
        "idx_kg_edges_source_file");
    REQUIRE(before.has_value());
    REQUIRE(db.mm.migrateTo(47).has_value());
    auto after = querySingleInt64(
        db.db, "SELECT rootpage FROM sqlite_master WHERE type = 'index' AND name = ?",
        "idx_kg_edges_source_file");
    REQUIRE(after.has_value());
    CHECK((before.value() == after.value()));
}

TEST_CASE("Migration version 47 removes topology snapshots too large to read",
          "[unit][metadata][schema][migration][kg]") {
    StandaloneMigrationDb db{"migration_v47_oversized_topology"};
    REQUIRE(db.mm.migrateTo(36).has_value());
    REQUIRE(db.db
                .execute(R"(
        CREATE INDEX idx_kg_nodes_document_hash ON kg_nodes(json_extract(properties, '$.document_hash'))
            WHERE json_extract(properties, '$.document_hash') IS NOT NULL;
        CREATE INDEX idx_kg_nodes_file_path ON kg_nodes(json_extract(properties, '$.file_path'))
            WHERE json_extract(properties, '$.file_path') IS NOT NULL;
        CREATE INDEX idx_kg_nodes_source_file ON kg_nodes(json_extract(properties, '$.source_file'))
            WHERE json_extract(properties, '$.source_file') IS NOT NULL;
        CREATE INDEX idx_kg_edges_source_file ON kg_edges(json_extract(properties, '$.source_file'))
            WHERE json_extract(properties, '$.source_file') IS NOT NULL;
    )")
                .has_value());
    REQUIRE(db.mm.migrateTo(46).has_value());

    // A real store hit SQLite's 1e9-byte length limit; lowering the limit on the migrating
    // connection reproduces that at test scale. The rows are written under the default limit.
    constexpr int kLimit = 64 * 1024;
    const std::string bigPayload =
        R"({"format":"zstd_binary_v1","data_b64":")" + std::string(4 * kLimit, 'A') + R"("})";
    const std::string insert =
        "INSERT INTO kg_nodes (node_key, label, type, properties) VALUES (?, ?, ?, ?)";
    REQUIRE(executeBound(db.db, insert, std::string("doc:a"), std::string("a"),
                         std::string("document"), std::string(R"({"document_hash":"h1"})"))
                .has_value());
    REQUIRE(executeBound(db.db, insert, std::string("topology:snapshot:s0"), std::string("s0"),
                         std::string("topology_snapshot"), std::string(R"({"data_b64":"AA"})"))
                .has_value());
    REQUIRE(executeBound(db.db, insert, std::string("topology:snapshot:s1"), std::string("s1"),
                         std::string("topology_snapshot"), bigPayload)
                .has_value());
    REQUIRE(executeBound(db.db, insert, std::string("topology:roads:s1"), std::string("s1"),
                         std::string("topology_road_snapshot"), std::string(R"({"roads":[]})"))
                .has_value());
    REQUIRE(executeBound(db.db, insert, std::string("topology:snapshot:latest"),
                         std::string("latest_topology_snapshot"),
                         std::string("topology_snapshot_pointer"),
                         std::string(R"({"snapshot_id":"s1"})"))
                .has_value());
    REQUIRE(db.db
                .execute("INSERT INTO kg_edges (src_node_id, dst_node_id, relation, properties) "
                         "SELECT a.id, b.id, 'rel', '{}' FROM kg_nodes a, kg_nodes b "
                         "WHERE a.node_key = 'doc:a' AND b.node_key = 'topology:snapshot:s0'")
                .has_value());

    sqlite3_limit(db.db.rawHandle(), SQLITE_LIMIT_LENGTH, kLimit);

    // The failure mode: maintaining an expression index on properties reads the old value.
    auto plainDelete =
        db.db.execute("DELETE FROM kg_nodes WHERE node_key = 'topology:snapshot:s1'");
    REQUIRE_FALSE(plainDelete.has_value());
    CHECK(plainDelete.error().message.find("string or blob too big") != std::string::npos);

    auto migrated = db.mm.migrateTo(47);
    INFO((migrated ? std::string("ok") : migrated.error().message));
    REQUIRE(migrated.has_value());

    auto countKey = [&](const char* key) {
        auto count =
            querySingleInt64(db.db, "SELECT COUNT(*) FROM kg_nodes WHERE node_key = ?", key);
        REQUIRE(count.has_value());
        return count.value();
    };
    CHECK((countKey("topology:snapshot:s1") == 0));
    CHECK((countKey("topology:roads:s1") == 0));
    CHECK((countKey("topology:snapshot:latest") == 0));
    CHECK((countKey("topology:snapshot:s0") == 1));
    CHECK((countKey("doc:a") == 1));
    auto edges = querySingleInt64(db.db, "SELECT COUNT(*) FROM kg_edges");
    REQUIRE(edges.has_value());
    CHECK((edges.value() == 1));

    for (const char* index : {"idx_kg_nodes_document_hash", "idx_kg_nodes_file_path",
                              "idx_kg_nodes_source_file", "idx_kg_edges_source_file"}) {
        auto guarded = querySingleInt64(db.db,
                                        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' "
                                        "AND name = ? AND sql LIKE '%json_valid%'",
                                        index);
        REQUIRE(guarded.has_value());
        CHECK((guarded.value() == 1));
    }
    auto hashPlan =
        queryPlanContains(db.db,
                          "SELECT id FROM kg_nodes WHERE CASE WHEN json_valid(properties) "
                          "THEN json_extract(properties, '$.document_hash') END = 'h1'",
                          "idx_kg_nodes_document_hash");
    REQUIRE(hashPlan.has_value());
    CHECK(hashPlan.value());

    // Re-running v47 finds nothing to remove and keeps the indexes.
    REQUIRE(db.mm.rollbackTo(46).has_value());
    REQUIRE(db.mm.migrateTo(47).has_value());
    CHECK((countKey("topology:snapshot:s0") == 1));
    CHECK((countKey("doc:a") == 1));
}

TEST_CASE("MigrationManager names the failed migration and a next step",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_failure_description", false};

    Migration create;
    create.version = 1;
    create.name = "create widgets";
    create.upSQL = "CREATE TABLE widgets (id INTEGER PRIMARY KEY, body TEXT);"
                   "INSERT INTO widgets (body) VALUES ('{\"a\":\"" +
                   std::string(8192, 'x') + "\"}');";
    create.created = std::chrono::system_clock::now();

    Migration index;
    index.version = 2;
    index.name = "index widget bodies";
    index.upSQL = "CREATE INDEX idx_widgets_a ON widgets(json_extract(body, '$.a'));";
    index.created = std::chrono::system_clock::now();

    db.mm.registerMigration(create);
    db.mm.registerMigration(index);
    CHECK(db.mm.describePendingFailure().empty());

    REQUIRE(db.mm.migrateTo(1).has_value());
    sqlite3_limit(db.db.rawHandle(), SQLITE_LIMIT_LENGTH, 1024);
    REQUIRE_FALSE(db.mm.migrate().has_value());

    const std::string text = db.mm.describePendingFailure();
    INFO(text);
    CHECK(text.rfind("migration 2 (index widget bodies) failed: ", 0) == 0);
    CHECK(text.find("string or blob too big") != std::string::npos);
    CHECK(text.find("docs/guide.md") != std::string::npos);

    // Once it succeeds, nothing is pending.
    sqlite3_limit(db.db.rawHandle(), SQLITE_LIMIT_LENGTH, 1000000000);
    REQUIRE(db.mm.migrate().has_value());
    CHECK(db.mm.describePendingFailure().empty());
}

TEST_CASE("Migration version 46 empties a grep trigram index built before NUL handling",
          "[unit][metadata][schema][migration]") {
    StandaloneMigrationDb db{"migration_v46_grep_trigram_reset"};
    REQUIRE(db.mm.migrateTo(45).has_value());
    auto hasIndex = db.db.tableExists("grep_trigram_fts");
    REQUIRE(hasIndex.has_value());
    if (!hasIndex.value()) {
        SKIP("SQLite build lacks the FTS5 trigram tokenizer");
    }

    REQUIRE(executeBound(db.db,
                         R"(
            INSERT INTO documents (
                file_path, file_name, file_extension, file_size, sha256_hash, mime_type,
                created_time, modified_time, indexed_time, content_extracted, extraction_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        )",
                         "/tmp/a.out", "a.out", "", int64_t{64}, "doc-hash-v46",
                         "application/x-executable", int64_t{1}, int64_t{1}, int64_t{1}, 1,
                         "success")
                .has_value());
    auto docId =
        querySingleInt64(db.db, "SELECT id FROM documents WHERE sha256_hash = ?", "doc-hash-v46");
    REQUIRE(docId.has_value());
    REQUIRE(executeBound(db.db, "INSERT INTO grep_trigram_fts(rowid, body) VALUES (?, ?)",
                         docId.value(), "header only")
                .has_value());
    REQUIRE(executeBound(db.db,
                         "INSERT INTO grep_trigram_coverage (document_id, sha256_hash, "
                         "indexed_time) VALUES (?, ?, ?)",
                         docId.value(), "doc-hash-v46", int64_t{1})
                .has_value());

    REQUIRE(db.mm.migrateTo(46).has_value());

    auto coverage = querySingleInt64(db.db, "SELECT COUNT(*) FROM grep_trigram_coverage");
    REQUIRE(coverage.has_value());
    CHECK(coverage.value() == 0);
    auto hits = querySingleInt64(
        db.db, "SELECT COUNT(*) FROM grep_trigram_fts WHERE grep_trigram_fts MATCH '\"hea\"'");
    REQUIRE(hits.has_value());
    CHECK(hits.value() == 0);
}
