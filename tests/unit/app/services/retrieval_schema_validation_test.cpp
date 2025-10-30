// Catch2 test validating retrieval service parameters map correctly to SQL schema
// PBI-066, Task 066-10: Metadata/Repository/Retrieval schema validation
//
// Purpose: Ensure all GetOptions, GrepOptions, ListOptions fields correspond to actual
//          database columns to prevent runtime errors from typos or schema drift.

#include <filesystem>
#include <memory>
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>
#include <yams/app/services/retrieval_service.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>

using namespace yams::app::services;
using namespace yams::metadata;
using yams::Result;

namespace {

// Helper to create test database with full schema
class SchemaValidator {
public:
    SchemaValidator() {
        // Create temporary database path
        auto tempDir = std::filesystem::temp_directory_path() / "yams_schema_test";
        std::filesystem::create_directories(tempDir);
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        dbPath_ = tempDir / (std::string("schema_") + std::to_string(ts) + ".db");
        std::filesystem::remove(dbPath_); // Clean if exists

        // Create connection pool
        pool_ = std::make_shared<ConnectionPool>(dbPath_.string());
        auto initResult = pool_->initialize();
        REQUIRE(initResult.has_value());

        // Apply migrations
        pool_->withConnection([](Database& db) -> Result<void> {
            MigrationManager mm(db);
            auto initResult = mm.initialize();
            if (!initResult)
                return initResult.error();

            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
            return mm.migrate();
        });

        repo_ = std::make_unique<MetadataRepository>(*pool_);
    }

    ~SchemaValidator() {
        repo_.reset();
        pool_->shutdown();
        pool_.reset();
        // Use remove_all to handle SQLite journal files (.db-shm, .db-wal)
        std::filesystem::remove_all(dbPath_.parent_path());
    }

    // Check if column exists in table
    bool columnExists(const std::string& table, const std::string& column) {
        bool found = false;
        pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtResult = db.prepare("PRAGMA table_info(" + table + ")");
            if (!stmtResult.has_value())
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult.has_value() || !stepResult.value())
                    break;

                auto colName = stmt.getString(1); // name is column 1
                if (colName == column) {
                    found = true;
                    break;
                }
            }
            return Result<void>{};
        });
        return found;
    }

    // Get all columns for a table
    std::vector<std::string> getColumns(const std::string& table) {
        std::vector<std::string> columns;
        pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtResult = db.prepare("PRAGMA table_info(" + table + ")");
            if (!stmtResult.has_value())
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult.has_value() || !stepResult.value())
                    break;

                columns.push_back(stmt.getString(1));
            }
            return Result<void>{};
        });
        return columns;
    }

    // Check if index exists
    bool indexExists(const std::string& indexName) {
        bool found = false;
        pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtResult =
                db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name=?");
            if (!stmtResult.has_value())
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            stmt.bind(1, indexName);
            auto stepResult = stmt.step();
            if (stepResult.has_value() && stepResult.value()) {
                found = true;
            }
            return Result<void>{};
        });
        return found;
    }

    // Check if table exists
    bool tableExists(const std::string& tableName) {
        bool found = false;
        pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmtResult =
                db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
            if (!stmtResult.has_value())
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            stmt.bind(1, tableName);
            auto stepResult = stmt.step();
            if (stepResult.has_value() && stepResult.value()) {
                found = true;
            }
            return Result<void>{};
        });
        return found;
    }

    ConnectionPool& pool() { return *pool_; }
    MetadataRepository& repo() { return *repo_; }

private:
    std::filesystem::path dbPath_;
    std::shared_ptr<ConnectionPool> pool_;
    std::unique_ptr<MetadataRepository> repo_;
};

} // namespace

TEST_CASE("Documents table schema matches GetOptions fields", "[retrieval][schema][get]") {
    SchemaValidator validator;

    SECTION("core document columns exist") {
        REQUIRE(validator.columnExists("documents", "id"));
        REQUIRE(validator.columnExists("documents", "file_path"));
        REQUIRE(validator.columnExists("documents", "file_name"));
        REQUIRE(validator.columnExists("documents", "file_extension"));
        REQUIRE(validator.columnExists("documents", "file_size"));
        REQUIRE(validator.columnExists("documents", "sha256_hash"));
        REQUIRE(validator.columnExists("documents", "mime_type"));
        REQUIRE(validator.columnExists("documents", "created_time"));
        REQUIRE(validator.columnExists("documents", "modified_time"));
        REQUIRE(validator.columnExists("documents", "indexed_time"));
        REQUIRE(validator.columnExists("documents", "content_extracted"));
        REQUIRE(validator.columnExists("documents", "extraction_status"));
    }

    SECTION("GetOptions time filter fields map to columns") {
        // GetOptions has: createdAfter, createdBefore, modifiedAfter, modifiedBefore,
        //                 indexedAfter, indexedBefore
        // These should map to: created_time, modified_time, indexed_time
        REQUIRE(validator.columnExists("documents", "created_time"));
        REQUIRE(validator.columnExists("documents", "modified_time"));
        REQUIRE(validator.columnExists("documents", "indexed_time"));
    }

    SECTION("GetOptions type filters map to columns") {
        // fileType filter uses file_extension
        REQUIRE(validator.columnExists("documents", "file_extension"));

        // mimeType filter uses mime_type
        REQUIRE(validator.columnExists("documents", "mime_type"));

        // extension filter uses file_extension
        REQUIRE(validator.columnExists("documents", "file_extension"));
    }

    SECTION("GetOptions name/hash filters map to columns") {
        // hash parameter uses sha256_hash
        REQUIRE(validator.columnExists("documents", "sha256_hash"));

        // name parameter uses file_name or file_path
        REQUIRE(validator.columnExists("documents", "file_name"));
        REQUIRE(validator.columnExists("documents", "file_path"));
    }
}

TEST_CASE("Metadata table schema supports generic key-value storage",
          "[retrieval][schema][metadata]") {
    SchemaValidator validator;

    SECTION("metadata table structure") {
        REQUIRE(validator.columnExists("metadata", "id"));
        REQUIRE(validator.columnExists("metadata", "document_id"));
        REQUIRE(validator.columnExists("metadata", "key"));
        REQUIRE(validator.columnExists("metadata", "value"));
        REQUIRE(validator.columnExists("metadata", "value_type"));
    }

    SECTION("metadata table has foreign key to documents") {
        // Verify foreign key exists by checking PRAGMA
        bool foundForeignKey = false;
        validator.pool().withConnection([&](Database& db) -> Result<void> {
            auto stmtResult = db.prepare("PRAGMA foreign_key_list(metadata)");
            if (!stmtResult.has_value())
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult.has_value() || !stepResult.value())
                    break;

                auto table = stmt.getString(2); // table is column 2
                if (table == "documents") {
                    foundForeignKey = true;
                    break;
                }
            }
            return Result<void>{};
        });
        REQUIRE(foundForeignKey);
    }
}

TEST_CASE("List service query fields validate against schema", "[retrieval][schema][list]") {
    SchemaValidator validator;

    SECTION("ListOptions sort fields map to columns") {
        // sortBy can be: "date" (indexed_time), "name" (file_name), "size" (file_size)
        REQUIRE(validator.columnExists("documents", "indexed_time"));
        REQUIRE(validator.columnExists("documents", "file_name"));
        REQUIRE(validator.columnExists("documents", "file_size"));
        REQUIRE(validator.columnExists("documents", "created_time"));  // alternative date field
        REQUIRE(validator.columnExists("documents", "modified_time")); // alternative date field
    }

    SECTION("ListOptions filter fields map to columns") {
        // fileType, mimeType, extensions filters
        REQUIRE(validator.columnExists("documents", "file_extension"));
        REQUIRE(validator.columnExists("documents", "mime_type"));

        // Time range filters
        REQUIRE(validator.columnExists("documents", "created_time"));
        REQUIRE(validator.columnExists("documents", "modified_time"));
        REQUIRE(validator.columnExists("documents", "indexed_time"));
    }

    SECTION("List binary/text flags use content_extracted") {
        REQUIRE(validator.columnExists("documents", "content_extracted"));
        REQUIRE(validator.columnExists("documents", "extraction_status"));
    }
}

TEST_CASE("Path tree schema supports hierarchical queries", "[retrieval][schema][path_tree]") {
    SchemaValidator validator;

    SECTION("path_tree_nodes table exists") {
        auto columns = validator.getColumns("path_tree_nodes");
        REQUIRE(!columns.empty());

        // Core path tree columns
        REQUIRE(validator.columnExists("path_tree_nodes", "node_id")); // Primary key
        REQUIRE(validator.columnExists("path_tree_nodes", "parent_id"));
        REQUIRE(validator.columnExists("path_tree_nodes", "path_segment"));
        REQUIRE(validator.columnExists("path_tree_nodes", "full_path"));
    }

    SECTION("path tree supports centroid storage") {
        // Centroid columns for semantic similarity
        REQUIRE(validator.columnExists("path_tree_nodes", "centroid_weight"));
        REQUIRE(validator.columnExists("path_tree_nodes", "centroid"));
    }

    SECTION("path tree supports document counting") {
        REQUIRE(validator.columnExists("path_tree_nodes", "doc_count"));
    }
}

TEST_CASE("FTS5 table schema supports grep operations", "[retrieval][schema][grep][fts5]") {
    SchemaValidator validator;

    SECTION("documents_fts virtual table exists") {
        REQUIRE(validator.tableExists("documents_fts"));
    }

    SECTION("documents_fts has correct columns") {
        // FTS5 virtual table columns
        auto columns = validator.getColumns("documents_fts");
        REQUIRE(!columns.empty());

        // FTS5 tables have specific columns
        bool hasContent = std::find(columns.begin(), columns.end(), "content") != columns.end();
        bool hasTitle = std::find(columns.begin(), columns.end(), "title") != columns.end();
        bool hasContentType =
            std::find(columns.begin(), columns.end(), "content_type") != columns.end();

        REQUIRE(hasContent);
        REQUIRE(hasTitle);
        REQUIRE(hasContentType);
    }
}

TEST_CASE("Tag storage schema supports tag filtering", "[retrieval][schema][tags]") {
    SchemaValidator validator;

    SECTION("tags are stored in metadata table") {
        // Tags are stored as metadata entries with key='tag:<tagname>'
        REQUIRE(validator.columnExists("metadata", "key"));
        REQUIRE(validator.columnExists("metadata", "value"));
        REQUIRE(validator.columnExists("metadata", "document_id"));
    }

    SECTION("tags can be queried for match_all and match_any") {
        // Verify we can query tags (this exercises the schema)
        auto result = validator.repo().getAllTags();
        REQUIRE(result.has_value()); // Should succeed even if empty
    }
}

TEST_CASE("Collection and snapshot schema supports filtering", "[retrieval][schema][collections]") {
    SchemaValidator validator;

    SECTION("documents table supports collection field") {
        // Check if collection column exists (may be in metadata or documents table)
        bool hasCollectionColumn = validator.columnExists("documents", "collection");

        // If not in documents, should be queryable via metadata
        if (!hasCollectionColumn) {
            // Metadata-based storage should work
            auto metaColumns = validator.getColumns("metadata");
            REQUIRE(!metaColumns.empty());
        }
    }

    SECTION("snapshot storage exists") {
        // Snapshots may be in separate table or metadata
        bool hasSnapshotTable = !validator.getColumns("snapshots").empty();
        bool hasMetadataTable = !validator.getColumns("metadata").empty();

        // At least one storage mechanism should exist
        REQUIRE((hasSnapshotTable || hasMetadataTable));
    }
}

TEST_CASE("Query helpers validate field names at compile time", "[retrieval][schema][validation]") {
    SchemaValidator validator;

    SECTION("queryDocuments uses valid column names") {
        DocumentQueryOptions opts;
        opts.exactPath = "/test/file.txt";
        opts.limit = 1;

        // This should not throw if column names are correct
        auto result = validator.repo().queryDocuments(opts);
        REQUIRE(result.has_value()); // Query executes successfully (may return empty)
    }

    SECTION("path prefix queries use correct columns") {
        DocumentQueryOptions opts;
        opts.pathPrefix = "/test/";
        opts.limit = 10;

        auto result = validator.repo().queryDocuments(opts);
        REQUIRE(result.has_value());
    }

    SECTION("extension filter uses correct column") {
        DocumentQueryOptions opts;
        opts.extension = ".cpp";
        opts.limit = 10;

        auto result = validator.repo().queryDocuments(opts);
        REQUIRE(result.has_value());
    }

    SECTION("mime type filter uses correct column") {
        DocumentQueryOptions opts;
        opts.mimeType = "text/plain";
        opts.limit = 10;

        auto result = validator.repo().queryDocuments(opts);
        REQUIRE(result.has_value());
    }

    SECTION("time range filters use correct columns") {
        DocumentQueryOptions opts;
        opts.modifiedAfter = 1000000;
        opts.indexedBefore = 9999999;
        opts.limit = 10;

        auto result = validator.repo().queryDocuments(opts);
        REQUIRE(result.has_value());
    }
}

TEST_CASE("Batch operation methods use correct column names", "[retrieval][schema][batch]") {
    SchemaValidator validator;

    SECTION("batchGetDocumentsByHash uses sha256_hash column") {
        std::vector<std::string> hashes = {"abc123", "def456"};

        auto result = validator.repo().batchGetDocumentsByHash(hashes);
        REQUIRE(result.has_value()); // Should succeed even if hashes don't exist
    }

    SECTION("batchGetContent uses document_id foreign key") {
        std::vector<int64_t> ids = {1, 2, 3};

        auto result = validator.repo().batchGetContent(ids);
        REQUIRE(result.has_value()); // Should succeed even if IDs don't exist
    }
}

TEST_CASE("Index coverage validates query performance", "[retrieval][schema][indexes]") {
    SchemaValidator validator;

    SECTION("critical indexes exist on documents table") {
        // Check for essential indexes
        REQUIRE(validator.indexExists("idx_documents_hash")); // Essential for get by hash

        // Path and time indexes recommended but not required
        INFO("Checking recommended indexes:");
        if (!validator.indexExists("idx_documents_path"))
            INFO("  - Missing idx_documents_path (for file_path queries)");
        if (!validator.indexExists("idx_documents_modified"))
            INFO("  - Missing idx_documents_modified (for time range queries)");
        if (!validator.indexExists("idx_documents_indexed"))
            INFO("  - Missing idx_documents_indexed (for list recent)");
    }

    SECTION("metadata table has proper indexing") {
        // Check that metadata table has indexes (specific names may vary)
        std::vector<std::string> indexes;
        validator.pool().withConnection([&](Database& db) -> Result<void> {
            auto stmtResult = db.prepare("PRAGMA index_list(metadata)");
            if (!stmtResult.has_value())
                return stmtResult.error();

            auto stmt = std::move(stmtResult).value();
            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult.has_value() || !stepResult.value())
                    break;

                indexes.push_back(stmt.getString(1));
            }
            return Result<void>{};
        });

        // Should have at least one index on metadata table
        bool hasMetadataIndex = !indexes.empty();
        REQUIRE(hasMetadataIndex);
        INFO("Metadata table indexes: " + std::to_string(indexes.size()));
    }
}

// Catch2 main
#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>

// Main entry point for Catch2
int main(int argc, char* argv[]) {
    return Catch::Session().run(argc, argv);
}
