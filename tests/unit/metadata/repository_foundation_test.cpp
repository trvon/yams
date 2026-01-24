// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

/**
 * @file repository_foundation_test.cpp
 * @brief Unit tests for MetadataRepository refactor foundation (Phase 1)
 *
 * Tests entity_traits.hpp, result_helpers.hpp, and crud_ops.hpp
 */

#include <catch2/catch_test_macros.hpp>

#include "src/metadata/repository/crud_ops.hpp"
#include "src/metadata/repository/entity_traits.hpp"
#include "src/metadata/repository/result_helpers.hpp"

#include <yams/core/types.h>
#include <yams/metadata/document_metadata.h>

using namespace yams;
using namespace yams::metadata;
using namespace yams::metadata::repository;

// ============================================================================
// Entity Traits Tests
// ============================================================================

TEST_CASE("EntityTraits DocumentInfo has correct table name", "[entity_traits]") {
    REQUIRE(EntityTraits<DocumentInfo>::table == "documents");
    REQUIRE(EntityTraits<DocumentInfo>::primary_key == "id");
}

TEST_CASE("EntityTraits DocumentContent has correct table name", "[entity_traits]") {
    REQUIRE(EntityTraits<DocumentContent>::table == "document_content");
    REQUIRE(EntityTraits<DocumentContent>::primary_key == "document_id");
}

TEST_CASE("EntityTraits SearchHistoryEntry has correct table name", "[entity_traits]") {
    REQUIRE(EntityTraits<SearchHistoryEntry>::table == "search_history");
    REQUIRE(EntityTraits<SearchHistoryEntry>::primary_key == "id");
}

TEST_CASE("buildSelectSql generates valid SQL", "[entity_traits]") {
    auto sql = buildSelectSql<SearchHistoryEntry>();
    REQUIRE(sql.find("SELECT") != std::string::npos);
    REQUIRE(sql.find("FROM search_history") != std::string::npos);
}

TEST_CASE("buildInsertSql generates valid SQL", "[entity_traits]") {
    auto sql = buildInsertSql<SearchHistoryEntry>();
    REQUIRE(sql.find("INSERT INTO search_history") != std::string::npos);
    REQUIRE(sql.find("VALUES") != std::string::npos);
}

// ============================================================================
// Result Helpers Tests
// ============================================================================

TEST_CASE("YAMS_TRY returns error on failure", "[result_helpers]") {
    auto failingFunc = []() -> Result<void> { return Error{ErrorCode::NotFound, "test error"}; };

    auto testFunc = [&]() -> Result<void> {
        YAMS_TRY(failingFunc());
        return {}; // Should not reach here
    };

    auto result = testFunc();
    REQUIRE(!result.has_value());
    REQUIRE(result.error().code == ErrorCode::NotFound);
}

TEST_CASE("YAMS_TRY passes through on success", "[result_helpers]") {
    auto successFunc = []() -> Result<void> { return {}; };

    auto testFunc = [&]() -> Result<int> {
        YAMS_TRY(successFunc());
        return 42;
    };

    auto result = testFunc();
    REQUIRE(result.has_value());
    REQUIRE(result.value() == 42);
}

TEST_CASE("YAMS_TRY_UNWRAP extracts value on success", "[result_helpers]") {
    auto getValue = []() -> Result<int> { return 123; };

    auto testFunc = [&]() -> Result<int> {
        YAMS_TRY_UNWRAP(val, getValue());
        return val * 2;
    };

    auto result = testFunc();
    REQUIRE(result.has_value());
    REQUIRE(result.value() == 246);
}

TEST_CASE("YAMS_TRY_UNWRAP returns error on failure", "[result_helpers]") {
    auto getValue = []() -> Result<int> { return Error{ErrorCode::InvalidArgument, "bad input"}; };

    auto testFunc = [&]() -> Result<int> {
        YAMS_TRY_UNWRAP(val, getValue());
        return val * 2; // Should not reach here
    };

    auto result = testFunc();
    REQUIRE(!result.has_value());
    REQUIRE(result.error().code == ErrorCode::InvalidArgument);
}

TEST_CASE("transform maps Result value", "[result_helpers]") {
    Result<int> success = 10;
    auto transformed = transform(std::move(success), [](int x) { return x * 2; });
    REQUIRE(transformed.has_value());
    REQUIRE(transformed.value() == 20);
}

TEST_CASE("transform propagates error", "[result_helpers]") {
    Result<int> failure = Error{ErrorCode::NotFound};
    auto transformed = transform(std::move(failure), [](int x) { return x * 2; });
    REQUIRE(!transformed.has_value());
    REQUIRE(transformed.error().code == ErrorCode::NotFound);
}

TEST_CASE("value_or returns value on success", "[result_helpers]") {
    Result<int> success = 42;
    REQUIRE(value_or(std::move(success), 0) == 42);
}

TEST_CASE("value_or returns default on failure", "[result_helpers]") {
    Result<int> failure = Error{ErrorCode::NotFound};
    REQUIRE(value_or(std::move(failure), 99) == 99);
}

TEST_CASE("to_optional converts success to optional", "[result_helpers]") {
    Result<int> success = 42;
    auto opt = to_optional(std::move(success));
    REQUIRE(opt.has_value());
    REQUIRE(opt.value() == 42);
}

TEST_CASE("to_optional converts failure to nullopt", "[result_helpers]") {
    Result<int> failure = Error{ErrorCode::NotFound};
    auto opt = to_optional(std::move(failure));
    REQUIRE(!opt.has_value());
}

TEST_CASE("scope_exit runs on scope exit", "[result_helpers]") {
    bool executed = false;
    {
        auto guard = scope_exit([&]() { executed = true; });
        REQUIRE(!executed);
    }
    REQUIRE(executed);
}

TEST_CASE("scope_exit can be dismissed", "[result_helpers]") {
    bool executed = false;
    {
        auto guard = scope_exit([&]() { executed = true; });
        guard.dismiss();
    }
    REQUIRE(!executed);
}

// ============================================================================
// HasEntityTraits Concept Tests
// ============================================================================

TEST_CASE("HasEntityTraits concept validates correctly", "[entity_traits]") {
    STATIC_REQUIRE(HasEntityTraits<DocumentInfo>);
    STATIC_REQUIRE(HasEntityTraits<DocumentContent>);
    STATIC_REQUIRE(HasEntityTraits<SearchHistoryEntry>);
    STATIC_REQUIRE(HasEntityTraits<SavedQuery>);
    STATIC_REQUIRE(HasEntityTraits<DocumentRelationship>);
    STATIC_REQUIRE(HasEntityTraits<MetadataEntry>);
}

// ============================================================================
// Phase 5A: New Entity Traits Tests
// ============================================================================

TEST_CASE("EntityTraits DocumentRelationship has correct table name", "[entity_traits][phase5]") {
    REQUIRE(EntityTraits<DocumentRelationship>::table == "document_relationships");
    REQUIRE(EntityTraits<DocumentRelationship>::primary_key == "id");
}

TEST_CASE("EntityTraits MetadataEntry has correct table name", "[entity_traits][phase5]") {
    REQUIRE(EntityTraits<MetadataEntry>::table == "metadata");
    REQUIRE(EntityTraits<MetadataEntry>::primary_key == "document_id");
}

TEST_CASE("DocumentRelationship column count is correct", "[entity_traits][phase5]") {
    // id, parent_id, child_id, relationship_type, created_time
    constexpr auto count = columnCount<DocumentRelationship>();
    STATIC_REQUIRE(count == 5);
}

TEST_CASE("MetadataEntry column count is correct", "[entity_traits][phase5]") {
    // document_id, key, value, value_type
    constexpr auto count = columnCount<MetadataEntry>();
    STATIC_REQUIRE(count == 4);
}

TEST_CASE("MetadataEntry columns have valid names", "[entity_traits][phase5]") {
    using namespace sql_validation;
    STATIC_REQUIRE(validateEntityTraits<MetadataEntry>());
    STATIC_REQUIRE(entity_traits_valid<MetadataEntry>);
}

TEST_CASE("DocumentRelationship columns have valid names", "[entity_traits][phase5]") {
    using namespace sql_validation;
    STATIC_REQUIRE(validateEntityTraits<DocumentRelationship>());
    STATIC_REQUIRE(entity_traits_valid<DocumentRelationship>);
}

// ============================================================================
// Phase 4: Compile-Time Column Index Tests
// ============================================================================

TEST_CASE("columnIndex finds column by name at compile time", "[entity_traits][phase4]") {
    // DocumentInfo columns
    constexpr int idIdx = columnIndex<DocumentInfo>("id");
    constexpr int filePathIdx = columnIndex<DocumentInfo>("file_path");
    constexpr int fileNameIdx = columnIndex<DocumentInfo>("file_name");

    STATIC_REQUIRE(idIdx == 0);
    STATIC_REQUIRE(filePathIdx == 1);
    STATIC_REQUIRE(fileNameIdx == 2);

    // SearchHistoryEntry columns
    constexpr int queryIdx = columnIndex<SearchHistoryEntry>("query");
    constexpr int queryTimeIdx = columnIndex<SearchHistoryEntry>("query_time");

    STATIC_REQUIRE(queryIdx == 1);
    STATIC_REQUIRE(queryTimeIdx == 2);
}

TEST_CASE("columnIndex returns -1 for non-existent column", "[entity_traits][phase4]") {
    constexpr int notFound = columnIndex<DocumentInfo>("nonexistent_column");
    STATIC_REQUIRE(notFound == -1);
}

TEST_CASE("columnCount returns correct count", "[entity_traits][phase4]") {
    constexpr auto docCount = columnCount<DocumentInfo>();
    constexpr auto contentCount = columnCount<DocumentContent>();
    constexpr auto historyCount = columnCount<SearchHistoryEntry>();

    STATIC_REQUIRE(docCount == 9);     // id, file_path, file_name, file_extension, file_size,
                                       // sha256_hash, mime_type, path_prefix, path_depth
    STATIC_REQUIRE(contentCount == 5); // document_id, content_text, content_length,
                                       // extraction_method, language
    STATIC_REQUIRE(historyCount == 6); // id, query, query_time, results_count, execution_time_ms,
                                       // user_context
}

TEST_CASE("columnName returns name by index", "[entity_traits][phase4]") {
    constexpr auto name0 = columnName<DocumentInfo, 0>();
    constexpr auto name1 = columnName<DocumentInfo, 1>();
    constexpr auto name2 = columnName<DocumentInfo, 2>();

    STATIC_REQUIRE(name0 == "id");
    STATIC_REQUIRE(name1 == "file_path");
    STATIC_REQUIRE(name2 == "file_name");
}

TEST_CASE("hasColumn checks column existence", "[entity_traits][phase4]") {
    STATIC_REQUIRE(hasColumn<DocumentInfo>("id"));
    STATIC_REQUIRE(hasColumn<DocumentInfo>("file_path"));
    STATIC_REQUIRE(hasColumn<DocumentInfo>("sha256_hash"));
    STATIC_REQUIRE(!hasColumn<DocumentInfo>("nonexistent"));
    STATIC_REQUIRE(!hasColumn<DocumentInfo>(""));
}

TEST_CASE("primaryKeyIndex returns correct index", "[entity_traits][phase4]") {
    constexpr int docPkIdx = primaryKeyIndex<DocumentInfo>();
    constexpr int contentPkIdx = primaryKeyIndex<DocumentContent>();
    constexpr int historyPkIdx = primaryKeyIndex<SearchHistoryEntry>();

    STATIC_REQUIRE(docPkIdx == 0);     // "id" is first column
    STATIC_REQUIRE(contentPkIdx == 0); // "document_id" is first column
    STATIC_REQUIRE(historyPkIdx == 0); // "id" is first column
}

// ============================================================================
// Phase 4: SQL Validation Tests
// ============================================================================

TEST_CASE("isValidIdentifier validates SQL identifiers", "[sql_validation][phase4]") {
    using namespace sql_validation;

    // Valid identifiers
    STATIC_REQUIRE(isValidIdentifier("id"));
    STATIC_REQUIRE(isValidIdentifier("file_path"));
    STATIC_REQUIRE(isValidIdentifier("Column123"));
    STATIC_REQUIRE(isValidIdentifier("_private"));
    STATIC_REQUIRE(isValidIdentifier("A"));

    // Invalid identifiers
    STATIC_REQUIRE(!isValidIdentifier(""));
    STATIC_REQUIRE(!isValidIdentifier("123abc"));        // Starts with digit
    STATIC_REQUIRE(!isValidIdentifier("has-dash"));      // Contains dash
    STATIC_REQUIRE(!isValidIdentifier("has space"));     // Contains space
    STATIC_REQUIRE(!isValidIdentifier("has.dot"));       // Contains dot
    STATIC_REQUIRE(!isValidIdentifier("has;semicolon")); // Contains semicolon
}

TEST_CASE("isSqlKeyword detects SQL keywords", "[sql_validation][phase4]") {
    using namespace sql_validation;

    // Keywords (case-insensitive)
    STATIC_REQUIRE(isSqlKeyword("SELECT"));
    STATIC_REQUIRE(isSqlKeyword("select"));
    STATIC_REQUIRE(isSqlKeyword("INSERT"));
    STATIC_REQUIRE(isSqlKeyword("DELETE"));
    STATIC_REQUIRE(isSqlKeyword("DROP"));
    STATIC_REQUIRE(isSqlKeyword("UNION"));

    // Not keywords
    STATIC_REQUIRE(!isSqlKeyword("id"));
    STATIC_REQUIRE(!isSqlKeyword("name"));
    STATIC_REQUIRE(!isSqlKeyword("documents"));
}

TEST_CASE("validateEntityTraits validates all entity traits", "[sql_validation][phase4]") {
    using namespace sql_validation;

    // All our defined entities should be valid
    STATIC_REQUIRE(validateEntityTraits<DocumentInfo>());
    STATIC_REQUIRE(validateEntityTraits<DocumentContent>());
    STATIC_REQUIRE(validateEntityTraits<SearchHistoryEntry>());
    STATIC_REQUIRE(validateEntityTraits<SavedQuery>());
}

TEST_CASE("entity_traits_valid constant is true for valid entities", "[sql_validation][phase4]") {
    using namespace sql_validation;

    STATIC_REQUIRE(entity_traits_valid<DocumentInfo>);
    STATIC_REQUIRE(entity_traits_valid<DocumentContent>);
    STATIC_REQUIRE(entity_traits_valid<SearchHistoryEntry>);
    STATIC_REQUIRE(entity_traits_valid<SavedQuery>);
}
