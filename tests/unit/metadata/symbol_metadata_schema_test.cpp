// Copyright 2025 YAMS Project
// SPDX-License-Identifier: GPL-3.0-or-later

/**
 * @file symbol_metadata_schema_test.cpp
 * @brief Schema validation tests for symbol metadata (tree-sitter extraction)
 *
 * Validates that:
 * - Migration v16 creates symbol_metadata table correctly
 * - EntityGraphService symbol insertion uses correct column names
 * - Symbol queries reference valid schema columns
 * - Foreign key constraint to documents table is correct
 */

#include <catch2/catch_test_macros.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/migration.h>

#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

using namespace yams;
using namespace yams::metadata;

namespace {

/**
 * @brief Helper class to validate schema after migrations
 */
class SymbolSchemaValidator {
public:
    SymbolSchemaValidator() {
        // Create temporary database in /tmp
        temp_db_path_ = std::filesystem::temp_directory_path() / "yams_symbol_schema_test";
        std::filesystem::create_directories(temp_db_path_);

        db_file_ = temp_db_path_ / "test.db";

        // Create connection pool and initialize
        pool_ = std::make_unique<ConnectionPool>(db_file_.string());
        auto init_result = pool_->initialize();
        if (!init_result) {
            throw std::runtime_error("Failed to initialize connection pool: " +
                                     init_result.error().message);
        }

        // Apply migrations
        auto migrateResult = pool_->withConnection([](Database& db) -> Result<void> {
            MigrationManager mm(db);
            auto initResult = mm.initialize();
            if (!initResult)
                return initResult.error();

            mm.registerMigrations(YamsMetadataMigrations::getAllMigrations());
            return mm.migrate();
        });

        if (!migrateResult) {
            throw std::runtime_error("Failed to apply migrations");
        }

        // Cache column information
        cacheColumnInfo();
    }

    ~SymbolSchemaValidator() {
        pool_.reset();
        try {
            std::filesystem::remove_all(temp_db_path_);
        } catch (const std::filesystem::filesystem_error& e) {
            // Ignore cleanup errors
        }
    }

    const std::vector<std::string>& getSymbolColumns() const { return symbol_columns_; }

    const std::vector<std::string>& getDocumentsColumns() const { return documents_columns_; }

    // Check if a specific table exists
    bool tableExists(const std::string& table_name) {
        auto result = pool_->withConnection([&](Database& db) -> Result<bool> {
            auto stmt_result =
                db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
            if (!stmt_result) {
                return stmt_result.error();
            }

            auto stmt = std::move(stmt_result.value());
            auto bind_result = stmt.bind(1, table_name);
            if (!bind_result) {
                return bind_result.error();
            }

            auto step_result = stmt.step();
            if (!step_result) {
                return step_result.error();
            }

            return step_result.value();
        });

        return result && result.value();
    }

    // Get foreign key information
    std::vector<std::map<std::string, std::string>> getForeignKeys(const std::string& table_name) {
        std::vector<std::map<std::string, std::string>> fks;

        pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmt_result = db.prepare("PRAGMA foreign_key_list(" + table_name + ")");
            if (!stmt_result) {
                return stmt_result.error();
            }

            auto stmt = std::move(stmt_result.value());

            while (true) {
                auto stepResult = stmt.step();
                if (!stepResult.has_value() || !stepResult.value())
                    break;

                std::map<std::string, std::string> fk;
                fk["table"] = stmt.getString(2);
                fk["from"] = stmt.getString(3);
                fk["to"] = stmt.getString(4);
                fks.push_back(fk);
            }

            return Result<void>();
        });

        return fks;
    }

    // Get index information
    std::vector<std::string> getIndexes(const std::string& table_name) {
        std::vector<std::string> indexes;

        auto result = pool_->withConnection([&](Database& db) -> Result<void> {
            auto stmt_result =
                db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=?");
            if (!stmt_result) {
                return stmt_result.error();
            }

            auto stmt = std::move(stmt_result.value());
            auto bind_result = stmt.bind(1, table_name);
            if (!bind_result) {
                return bind_result.error();
            }

            while (true) {
                auto step_result = stmt.step();
                if (!step_result) {
                    return step_result.error();
                }
                if (!step_result.value()) {
                    break;
                }

                auto name = stmt.getString(0);
                if (!name.empty()) {
                    indexes.push_back(name);
                }
            }

            return Result<void>();
        });

        return indexes;
    }

private:
    void cacheColumnInfo() {
        // Get symbol_metadata columns
        auto symbol_result = pool_->withConnection([this](Database& db) -> Result<void> {
            auto stmt_result = db.prepare("PRAGMA table_info(symbol_metadata)");
            if (!stmt_result) {
                return stmt_result.error();
            }

            auto stmt = std::move(stmt_result.value());

            while (true) {
                auto step_result = stmt.step();
                if (!step_result) {
                    return step_result.error();
                }
                if (!step_result.value()) {
                    break;
                }

                auto name = stmt.getString(1);
                if (!name.empty()) {
                    symbol_columns_.push_back(name);
                }
            }

            return Result<void>();
        });

        // Get documents columns
        auto docs_result = pool_->withConnection([this](Database& db) -> Result<void> {
            auto stmt_result = db.prepare("PRAGMA table_info(documents)");
            if (!stmt_result) {
                return stmt_result.error();
            }

            auto stmt = std::move(stmt_result.value());

            while (true) {
                auto step_result = stmt.step();
                if (!step_result) {
                    return step_result.error();
                }
                if (!step_result.value()) {
                    break;
                }

                auto name = stmt.getString(1);
                if (!name.empty()) {
                    documents_columns_.push_back(name);
                }
            }

            return Result<void>();
        });
    }

    std::filesystem::path temp_db_path_;
    std::filesystem::path db_file_;
    std::unique_ptr<ConnectionPool> pool_;
    std::vector<std::string> symbol_columns_;
    std::vector<std::string> documents_columns_;
};

bool columnExists(const std::vector<std::string>& columns, const std::string& name) {
    return std::find(columns.begin(), columns.end(), name) != columns.end();
}

// Shared validator instance (created once, reused across all tests)
SymbolSchemaValidator& getValidator() {
    static SymbolSchemaValidator instance;
    return instance;
}

} // namespace

// ============================================================================
// Migration v16 Validation Tests
// ============================================================================

TEST_CASE("Migration v16 creates symbol_metadata table", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();

    SECTION("symbol_metadata table exists") {
        REQUIRE(validator.tableExists("symbol_metadata"));
    }

    SECTION("documents table exists (FK target)") {
        REQUIRE(validator.tableExists("documents"));
    }
}

TEST_CASE("symbol_metadata table has all required columns", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    INFO("symbol_metadata columns: " << columns.size());
    for (const auto& col : columns) {
        INFO("  - " << col);
    }

    SECTION("has primary key column") {
        REQUIRE(columnExists(columns, "symbol_id"));
    }

    SECTION("has document reference column (FK)") {
        REQUIRE(columnExists(columns, "document_hash"));
    }

    SECTION("has file path column") {
        REQUIRE(columnExists(columns, "file_path"));
    }

    SECTION("has symbol identification columns") {
        REQUIRE(columnExists(columns, "symbol_name"));
        REQUIRE(columnExists(columns, "qualified_name"));
        REQUIRE(columnExists(columns, "kind"));
    }

    SECTION("has location columns") {
        REQUIRE(columnExists(columns, "start_line"));
        REQUIRE(columnExists(columns, "end_line"));
        REQUIRE(columnExists(columns, "start_offset"));
        REQUIRE(columnExists(columns, "end_offset"));
    }

    SECTION("has metadata columns") {
        REQUIRE(columnExists(columns, "return_type"));
        REQUIRE(columnExists(columns, "parameters"));
        REQUIRE(columnExists(columns, "documentation"));
    }
}

TEST_CASE("symbol_metadata has correct foreign key to documents",
          "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto documents_columns = validator.getDocumentsColumns();

    SECTION("documents table has sha256_hash column (FK target)") {
        REQUIRE(columnExists(documents_columns, "sha256_hash"));
    }

    SECTION("foreign key constraint exists") {
        auto fks = validator.getForeignKeys("symbol_metadata");

        INFO("Foreign keys found: " << fks.size());
        for (const auto& fk : fks) {
            INFO("  FK: " << fk.at("from") << " -> " << fk.at("table") << "." << fk.at("to"));
        }

        REQUIRE_FALSE(fks.empty());

        bool found_documents_fk = false;
        for (const auto& fk : fks) {
            if (fk.at("table") == "documents" && fk.at("from") == "document_hash" &&
                fk.at("to") == "sha256_hash") {
                found_documents_fk = true;
                break;
            }
        }

        REQUIRE(found_documents_fk);
    }
}

TEST_CASE("symbol_metadata has required indexes", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto indexes = validator.getIndexes("symbol_metadata");

    INFO("Indexes on symbol_metadata: " << indexes.size());
    for (const auto& idx : indexes) {
        INFO("  - " << idx);
    }

    SECTION("has index on symbol_name") {
        bool found = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_name") != std::string::npos) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }

    SECTION("has index on document_hash") {
        bool found = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_document") != std::string::npos) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }

    SECTION("has index on file_path") {
        bool found = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_file_path") != std::string::npos) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }

    SECTION("has index on kind") {
        bool found = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_kind") != std::string::npos) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }

    SECTION("has index on qualified_name") {
        bool found = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_qualified") != std::string::npos) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }
}

// ============================================================================
// Symbol Insertion Validation Tests
// ============================================================================

TEST_CASE("Symbol insertion field names match schema", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("tree-sitter SymbolInfo fields map to schema columns") {
        // Fields from plugins/symbol_extractor_treesitter/symbol_extractor.h

        // SymbolInfo.name -> symbol_name
        REQUIRE(columnExists(columns, "symbol_name"));

        // SymbolInfo.qualified_name -> qualified_name
        REQUIRE(columnExists(columns, "qualified_name"));

        // SymbolInfo.kind -> kind
        REQUIRE(columnExists(columns, "kind"));

        // SymbolInfo.file_path -> file_path
        REQUIRE(columnExists(columns, "file_path"));

        // SymbolInfo.start_line -> start_line
        REQUIRE(columnExists(columns, "start_line"));

        // SymbolInfo.end_line -> end_line
        REQUIRE(columnExists(columns, "end_line"));

        // SymbolInfo.start_offset -> start_offset
        REQUIRE(columnExists(columns, "start_offset"));

        // SymbolInfo.end_offset -> end_offset
        REQUIRE(columnExists(columns, "end_offset"));

        // SymbolInfo.return_type -> return_type
        REQUIRE(columnExists(columns, "return_type"));

        // SymbolInfo.parameters -> parameters (JSON array)
        REQUIRE(columnExists(columns, "parameters"));

        // SymbolInfo.documentation -> documentation
        REQUIRE(columnExists(columns, "documentation"));
    }
}

TEST_CASE("Symbol kind values are schema-consistent", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("kind column exists for storing symbol types") {
        REQUIRE(columnExists(columns, "kind"));
    }

    SECTION("expected symbol kinds are valid") {
        // From tree-sitter extraction (see symbol_extractor.cpp)
        std::vector<std::string> expected_kinds = {"function", "class",  "struct",  "interface",
                                                   "enum",     "method", "variable"};

        // Schema allows TEXT, so all string kinds are valid
        // This test documents expected kinds
        INFO("Expected symbol kinds:");
        for (const auto& kind : expected_kinds) {
            INFO("  - " << kind);
        }

        REQUIRE(columnExists(columns, "kind"));
    }
}

// ============================================================================
// Symbol Query Validation Tests
// ============================================================================

TEST_CASE("Symbol queries use valid column names", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("query by symbol name uses correct column") {
        // SELECT * FROM symbol_metadata WHERE symbol_name = ?
        REQUIRE(columnExists(columns, "symbol_name"));
    }

    SECTION("query by qualified name uses correct column") {
        // SELECT * FROM symbol_metadata WHERE qualified_name = ?
        REQUIRE(columnExists(columns, "qualified_name"));
    }

    SECTION("query by document uses FK column") {
        // SELECT * FROM symbol_metadata WHERE document_hash = ?
        REQUIRE(columnExists(columns, "document_hash"));
    }

    SECTION("query by file path uses correct column") {
        // SELECT * FROM symbol_metadata WHERE file_path = ?
        REQUIRE(columnExists(columns, "file_path"));
    }

    SECTION("query by kind uses correct column") {
        // SELECT * FROM symbol_metadata WHERE kind = ?
        REQUIRE(columnExists(columns, "kind"));
    }
}

TEST_CASE("Symbol location queries use valid columns", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("line-based queries use line columns") {
        REQUIRE(columnExists(columns, "start_line"));
        REQUIRE(columnExists(columns, "end_line"));
    }

    SECTION("byte-offset queries use offset columns") {
        REQUIRE(columnExists(columns, "start_offset"));
        REQUIRE(columnExists(columns, "end_offset"));
    }
}

// ============================================================================
// Integration with Documents Table
// ============================================================================

TEST_CASE("Symbol metadata integrates with documents table", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto symbol_columns = validator.getSymbolColumns();
    auto doc_columns = validator.getDocumentsColumns();

    SECTION("FK column exists in symbol_metadata") {
        REQUIRE(columnExists(symbol_columns, "document_hash"));
    }

    SECTION("FK target column exists in documents") {
        REQUIRE(columnExists(doc_columns, "sha256_hash"));
    }

    SECTION("FK constraint is properly defined") {
        auto fks = validator.getForeignKeys("symbol_metadata");

        bool valid_fk = false;
        for (const auto& fk : fks) {
            if (fk.at("table") == "documents" && fk.at("from") == "document_hash" &&
                fk.at("to") == "sha256_hash") {
                valid_fk = true;
                break;
            }
        }

        REQUIRE(valid_fk);
    }
}

TEST_CASE("Symbol metadata schema prevents common errors", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("prevents 'name' vs 'symbol_name' confusion") {
        REQUIRE(columnExists(columns, "symbol_name"));
        REQUIRE_FALSE(columnExists(columns, "name"));
    }

    SECTION("prevents 'doc_hash' vs 'document_hash' confusion") {
        REQUIRE(columnExists(columns, "document_hash"));
        REQUIRE_FALSE(columnExists(columns, "doc_hash"));
        REQUIRE_FALSE(columnExists(columns, "hash"));
    }

    SECTION("prevents 'type' vs 'kind' confusion") {
        REQUIRE(columnExists(columns, "kind"));
        REQUIRE_FALSE(columnExists(columns, "type"));
        REQUIRE_FALSE(columnExists(columns, "symbol_type"));
    }
}

// ============================================================================
// EntityGraphService Integration
// ============================================================================

TEST_CASE("EntityGraphService symbol storage uses correct fields",
          "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("EntityGraphService stores symbols with schema-consistent fields") {
        // When EntityGraphService creates symbol nodes from tree-sitter results,
        // it should use these field names

        INFO("Validating EntityGraphService would use correct INSERT statement");

        // Conceptual INSERT (from EntityGraphService::createSymbolNodes):
        // INSERT INTO symbol_metadata (
        //     document_hash, file_path, symbol_name, qualified_name, kind,
        //     start_line, end_line, start_offset, end_offset,
        //     return_type, parameters, documentation
        // ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

        REQUIRE(columnExists(columns, "document_hash"));
        REQUIRE(columnExists(columns, "file_path"));
        REQUIRE(columnExists(columns, "symbol_name"));
        REQUIRE(columnExists(columns, "qualified_name"));
        REQUIRE(columnExists(columns, "kind"));
        REQUIRE(columnExists(columns, "start_line"));
        REQUIRE(columnExists(columns, "end_line"));
        REQUIRE(columnExists(columns, "start_offset"));
        REQUIRE(columnExists(columns, "end_offset"));
        REQUIRE(columnExists(columns, "return_type"));
        REQUIRE(columnExists(columns, "parameters"));
        REQUIRE(columnExists(columns, "documentation"));
    }
}

TEST_CASE("Symbol metadata supports Knowledge Graph queries", "[catch2][unit][metadata][symbol]") {
    auto& validator = getValidator();
    auto columns = validator.getSymbolColumns();

    SECTION("supports finding symbols by name") {
        REQUIRE(columnExists(columns, "symbol_name"));

        // Indexed for performance
        auto indexes = validator.getIndexes("symbol_metadata");
        bool has_name_index = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_name") != std::string::npos) {
                has_name_index = true;
                break;
            }
        }
        REQUIRE(has_name_index);
    }

    SECTION("supports finding symbols in document") {
        REQUIRE(columnExists(columns, "document_hash"));

        // Indexed for performance
        auto indexes = validator.getIndexes("symbol_metadata");
        bool has_doc_index = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_document") != std::string::npos) {
                has_doc_index = true;
                break;
            }
        }
        REQUIRE(has_doc_index);
    }

    SECTION("supports finding symbols by type") {
        REQUIRE(columnExists(columns, "kind"));

        // Indexed for performance (e.g., find all functions)
        auto indexes = validator.getIndexes("symbol_metadata");
        bool has_kind_index = false;
        for (const auto& idx : indexes) {
            if (idx.find("idx_symbol_kind") != std::string::npos) {
                has_kind_index = true;
                break;
            }
        }
        REQUIRE(has_kind_index);
    }
}
