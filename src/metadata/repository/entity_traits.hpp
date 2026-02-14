// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

/**
 * @file entity_traits.hpp
 * @brief Entity trait definitions for type-safe database operations
 *
 * Provides compile-time metadata for database entities using C++20 concepts.
 * Enables generic CRUD operations with automatic bind/extract.
 *
 * @see ADR-0004: MetadataRepository Refactor with Templates and Metaprogramming
 */

#include <yams/core/cpp23_features.hpp>
#include <yams/core/types.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>

#include "result_helpers.hpp"

#include <concepts>
#include <string_view>
#include <tuple>
#include <type_traits>

namespace yams::metadata::repository {

// ============================================================================
// SQL Type Concepts
// ============================================================================

/**
 * @brief Concept: Types that can be bound to SQL statements
 *
 * These types have direct bind() overloads in Statement class.
 */
template <typename T>
concept SqlBindable =
    std::same_as<std::remove_cvref_t<T>, int64_t> || std::same_as<std::remove_cvref_t<T>, int> ||
    std::same_as<std::remove_cvref_t<T>, double> || std::same_as<std::remove_cvref_t<T>, float> ||
    std::same_as<std::remove_cvref_t<T>, std::string> ||
    std::same_as<std::remove_cvref_t<T>, std::string_view> ||
    std::same_as<std::remove_cvref_t<T>, std::chrono::sys_seconds> ||
    std::same_as<std::remove_cvref_t<T>, std::nullptr_t>;

/**
 * @brief Concept: Types that can be extracted from SQL result columns
 */
template <typename T>
concept SqlExtractable = std::same_as<T, int64_t> || std::same_as<T, int> ||
                         std::same_as<T, double> || std::same_as<T, std::string> ||
                         std::same_as<T, std::chrono::sys_seconds> || std::same_as<T, bool>;

// ============================================================================
// Column Descriptor
// ============================================================================

/**
 * @brief Describes a database column with its name and member pointer
 *
 * @tparam T The column's C++ type
 * @tparam Entity The entity class this column belongs to
 */
template <typename T, typename Entity, bool Nullable = false, bool PrimaryKey = false,
          bool AutoIncrement = false>
struct Column {
    std::string_view name; ///< Column name in database
    T Entity::* member;    ///< Pointer to member for automatic bind/extract

    static constexpr bool kNullable = Nullable;
    static constexpr bool kIsPrimaryKey = PrimaryKey;
    static constexpr bool kAutoIncrement = AutoIncrement;

    constexpr Column(std::string_view n, T Entity::* m) : name(n), member(m) {}
};

// ============================================================================
// Entity Traits Base
// ============================================================================

/**
 * @brief Primary template for entity traits (must be specialized)
 *
 * Specializations define:
 * - table: The database table name
 * - primary_key: The primary key column name
 * - columns: Tuple of Column descriptors
 * - extract(): Custom extraction function (for complex fields)
 */
template <typename T> struct EntityTraits;

/**
 * @brief Concept: Types that have valid EntityTraits defined
 */
template <typename T>
concept HasEntityTraits = requires {
    { EntityTraits<T>::table } -> std::convertible_to<std::string_view>;
    { EntityTraits<T>::primary_key } -> std::convertible_to<std::string_view>;
    typename EntityTraits<T>::columns_tuple;
};

// ============================================================================
// Column Extraction Helpers
// ============================================================================

namespace detail {

/**
 * @brief Extract a single column value from a Statement
 */
template <typename T> T extractColumn(const Statement& stmt, int column) {
    if constexpr (std::same_as<T, int64_t>) {
        return stmt.getInt64(column);
    } else if constexpr (std::same_as<T, int>) {
        return stmt.getInt(column);
    } else if constexpr (std::same_as<T, double>) {
        return stmt.getDouble(column);
    } else if constexpr (std::same_as<T, std::string>) {
        return stmt.getString(column);
    } else if constexpr (std::same_as<T, std::chrono::sys_seconds>) {
        return stmt.getTime(column);
    } else if constexpr (std::same_as<T, bool>) {
        return stmt.getInt(column) != 0;
    } else {
        static_assert(sizeof(T) == 0, "Unsupported column type for extraction");
    }
}

/**
 * @brief Extract columns into entity using fold expression with index_sequence
 */
template <HasEntityTraits Entity, std::size_t... Is>
void extractColumnsImpl(const Statement& stmt, Entity& e, std::index_sequence<Is...>) {
    const auto& cols = EntityTraits<Entity>::columns;
    // Fold expression: extract each column into corresponding member
    ((e.*(std::get<Is>(cols).member) =
          extractColumn<std::remove_reference_t<decltype(e.*(std::get<Is>(cols).member))>>(stmt,
                                                                                           Is)),
     ...);
}

} // namespace detail

/**
 * @brief Extract all columns from Statement into an entity
 *
 * Uses compile-time column metadata to map statement columns to struct members.
 *
 * @param stmt The executed statement positioned at a result row
 * @param e The entity to populate
 */
template <HasEntityTraits Entity> void extractColumns(const Statement& stmt, Entity& e) {
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    detail::extractColumnsImpl(stmt, e, std::make_index_sequence<N>{});
}

// ============================================================================
// Column Binding Helpers
// ============================================================================

namespace detail {

/**
 * @brief Bind entity columns to statement using fold expression
 */
template <HasEntityTraits Entity, std::size_t... Is>
Result<void> bindColumnsImpl(Statement& stmt, const Entity& e, std::index_sequence<Is...>,
                             int startIndex = 1) {
    const auto& cols = EntityTraits<Entity>::columns;
    int idx = startIndex;
    Result<void> result;

    // Fold expression: bind each member value, short-circuit on error
    // Skip auto-increment primary key columns
    (([&] {
         if (!result.has_value())
             return; // Short-circuit if error
         using Col = std::tuple_element_t<Is, typename EntityTraits<Entity>::columns_tuple>;
         const auto& col = std::get<Is>(cols);
         if constexpr (Col::kAutoIncrement && Col::kIsPrimaryKey) {
             // Skip auto-increment primary keys
             return;
         }
         result = stmt.bind(idx++, e.*(col.member));
     }()),
     ...);

    return result;
}

} // namespace detail

/**
 * @brief Bind entity columns to a prepared statement
 *
 * Binds all non-auto-increment columns in order. Useful for INSERT operations.
 *
 * @param stmt The prepared statement
 * @param e The entity with values to bind
 * @param startIndex Starting bind index (default: 1)
 * @return Result indicating success or bind error
 */
template <HasEntityTraits Entity>
Result<void> bindEntityColumns(Statement& stmt, const Entity& e, int startIndex = 1) {
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    return detail::bindColumnsImpl(stmt, e, std::make_index_sequence<N>{}, startIndex);
}

// ============================================================================
// Compile-Time Column Index Computation (Phase 4)
// ============================================================================

namespace detail {

/**
 * @brief Find column index by name at compile time
 *
 * Returns the index of the column with the given name, or -1 if not found.
 * This enables compile-time validation of column references.
 *
 * @tparam Entity The entity type with EntityTraits
 * @param name The column name to find
 * @return Column index (0-based) or -1 if not found
 *
 * Example:
 * @code
 * constexpr int idx = columnIndex<DocumentInfo>("file_path");
 * static_assert(idx >= 0, "Column not found");
 * @endcode
 */
template <HasEntityTraits Entity, std::size_t... Is>
constexpr int columnIndexImpl(std::string_view name, std::index_sequence<Is...>) {
    constexpr auto& cols = EntityTraits<Entity>::columns;
    int result = -1;
    // Use void cast to suppress unused expression warning from fold expression
    (void)((std::get<Is>(cols).name == name ? (result = static_cast<int>(Is), true) : false) ||
           ...);
    return result;
}

} // namespace detail

/**
 * @brief Get column index by name at compile time
 */
template <HasEntityTraits Entity> constexpr int columnIndex(std::string_view name) {
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    return detail::columnIndexImpl<Entity>(name, std::make_index_sequence<N>{});
}

/**
 * @brief Get column count for an entity
 */
template <HasEntityTraits Entity> constexpr std::size_t columnCount() {
    return std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
}

/**
 * @brief Get column name by index at compile time
 */
template <HasEntityTraits Entity, std::size_t Index> constexpr std::string_view columnName() {
    static_assert(Index < std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>,
                  "Column index out of bounds");
    return std::get<Index>(EntityTraits<Entity>::columns).name;
}

/**
 * @brief Check if column exists by name at compile time
 */
template <HasEntityTraits Entity> constexpr bool hasColumn(std::string_view name) {
    return columnIndex<Entity>(name) >= 0;
}

/**
 * @brief Get index of primary key column
 */
template <HasEntityTraits Entity> constexpr int primaryKeyIndex() {
    return columnIndex<Entity>(EntityTraits<Entity>::primary_key);
}

// ============================================================================
// Constexpr SQL Validation (Phase 4)
// ============================================================================

namespace sql_validation {

/**
 * @brief Check if a string contains only valid SQL identifier characters
 *
 * Valid: a-z, A-Z, 0-9, _
 */
constexpr bool isValidIdentifier(std::string_view s) {
    if (s.empty())
        return false;
    // First character can't be a digit
    if (s[0] >= '0' && s[0] <= '9')
        return false;
    for (char c : s) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              c == '_')) {
            return false;
        }
    }
    return true;
}

/**
 * @brief Check if string looks like a SQL keyword
 */
constexpr bool isSqlKeyword(std::string_view s) {
    // Common SQL keywords to detect potential injection
    constexpr std::string_view keywords[] = {"SELECT", "INSERT", "UPDATE", "DELETE", "DROP",
                                             "CREATE", "ALTER",  "UNION",  "WHERE",  "FROM",
                                             "INTO",   "VALUES", "SET",    "AND",    "OR"};
    for (auto kw : keywords) {
        if (s.size() == kw.size()) {
            bool match = true;
            for (size_t i = 0; i < s.size() && match; ++i) {
                char c1 = s[i];
                char c2 = kw[i];
                // Case-insensitive comparison
                if (c1 >= 'a' && c1 <= 'z')
                    c1 -= 32;
                if (c2 >= 'a' && c2 <= 'z')
                    c2 -= 32;
                match = (c1 == c2);
            }
            if (match)
                return true;
        }
    }
    return false;
}

/**
 * @brief Validate that all column names in entity traits are valid SQL identifiers
 */
template <HasEntityTraits Entity, std::size_t... Is>
constexpr bool validateColumnNamesImpl(std::index_sequence<Is...>) {
    constexpr auto& cols = EntityTraits<Entity>::columns;
    return (isValidIdentifier(std::get<Is>(cols).name) && ...);
}

template <HasEntityTraits Entity> constexpr bool validateColumnNames() {
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    return validateColumnNamesImpl<Entity>(std::make_index_sequence<N>{});
}

/**
 * @brief Validate entity traits at compile time
 *
 * Checks:
 * - Table name is valid identifier
 * - Primary key name is valid identifier
 * - All column names are valid identifiers
 * - Primary key column exists
 */
template <HasEntityTraits Entity> constexpr bool validateEntityTraits() {
    // Check table name
    if (!isValidIdentifier(EntityTraits<Entity>::table))
        return false;

    // Check primary key name
    if (!isValidIdentifier(EntityTraits<Entity>::primary_key))
        return false;

    // Check all column names
    if (!validateColumnNames<Entity>())
        return false;

    // Check that primary key column exists
    if (columnIndex<Entity>(EntityTraits<Entity>::primary_key) < 0)
        return false;

    return true;
}

/**
 * @brief Static assertion helper for entity validation
 *
 * Use in entity traits specializations to catch errors at compile time.
 *
 * Example:
 * @code
 * template<>
 * struct EntityTraits<MyEntity> {
 *     // ... trait definitions ...
 *     static_assert(sql_validation::validateEntityTraits<MyEntity>(),
 *                   "Invalid entity traits");
 * };
 * @endcode
 */
template <HasEntityTraits Entity>
inline constexpr bool entity_traits_valid = validateEntityTraits<Entity>();

} // namespace sql_validation

// ============================================================================
// SQL Generation Helpers
// ============================================================================

/**
 * @brief Generate SELECT column list at compile time
 */
template <HasEntityTraits Entity> constexpr std::string buildColumnList() {
    std::string result;
    constexpr auto& cols = EntityTraits<Entity>::columns;
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;

    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
        ((result += (Is > 0 ? ", " : ""), result += std::string(std::get<Is>(cols).name)), ...);
    }(std::make_index_sequence<N>{});

    return result;
}

/**
 * @brief Generate basic SELECT statement
 */
template <HasEntityTraits Entity> std::string buildSelectSql() {
    return "SELECT " + buildColumnList<Entity>() + " FROM " +
           std::string(EntityTraits<Entity>::table);
}

/**
 * @brief Generate INSERT statement with placeholders
 *
 * Skips auto-increment primary key columns.
 */
template <HasEntityTraits Entity> std::string buildInsertSql() {
    std::string columns;
    std::string placeholders;
    const auto& cols = EntityTraits<Entity>::columns;
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    bool first = true;

    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
        (([&] {
             using Col = std::tuple_element_t<Is, typename EntityTraits<Entity>::columns_tuple>;
             const auto& col = std::get<Is>(cols);
             if constexpr (Col::kAutoIncrement && Col::kIsPrimaryKey) {
                 return; // Skip auto-increment primary keys
             }
             if (!first) {
                 columns += ", ";
                 placeholders += ", ";
             }
             columns += std::string(col.name);
             placeholders += "?";
             first = false;
         }()),
         ...);
    }(std::make_index_sequence<N>{});

    return "INSERT INTO " + std::string(EntityTraits<Entity>::table) + " (" + columns +
           ") VALUES (" + placeholders + ")";
}

/**
 * @brief Generate UPDATE statement with placeholders
 *
 * Sets all non-primary-key columns, with WHERE on primary key.
 */
template <HasEntityTraits Entity> std::string buildUpdateSql() {
    std::string setClauses;
    const auto& cols = EntityTraits<Entity>::columns;
    constexpr auto N = std::tuple_size_v<typename EntityTraits<Entity>::columns_tuple>;
    bool first = true;

    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
        (([&] {
             using Col = std::tuple_element_t<Is, typename EntityTraits<Entity>::columns_tuple>;
             const auto& col = std::get<Is>(cols);
             if constexpr (Col::kIsPrimaryKey) {
                 return; // Skip primary key in SET clause
             }
             if (!first)
                 setClauses += ", ";
             setClauses += std::string(col.name) + " = ?";
             first = false;
         }()),
         ...);
    }(std::make_index_sequence<N>{});

    return "UPDATE " + std::string(EntityTraits<Entity>::table) + " SET " + setClauses + " WHERE " +
           std::string(EntityTraits<Entity>::primary_key) + " = ?";
}

// ============================================================================
// Entity Traits Specializations
// ============================================================================

/**
 * @brief EntityTraits specialization for DocumentInfo
 *
 * Maps the documents table with all columns needed for CRUD operations.
 */
template <> struct EntityTraits<DocumentInfo> {
    static constexpr std::string_view table = "documents";
    static constexpr std::string_view primary_key = "id";

    // Core columns for basic CRUD (not all columns - complex ones use custom extraction)
    using columns_tuple = std::tuple<Column<int64_t, DocumentInfo, false, true, true>, // id
                                     Column<std::string, DocumentInfo>,                // file_path
                                     Column<std::string, DocumentInfo>,                // file_name
                                     Column<std::string, DocumentInfo>, // file_extension
                                     Column<int64_t, DocumentInfo>,     // file_size
                                     Column<std::string, DocumentInfo>, // sha256_hash
                                     Column<std::string, DocumentInfo>, // mime_type
                                     Column<std::string, DocumentInfo>, // path_prefix
                                     Column<int, DocumentInfo>          // path_depth
                                     >;

    static constexpr columns_tuple columns{
        Column<int64_t, DocumentInfo, false, true, true>{"id", &DocumentInfo::id},
        Column<std::string, DocumentInfo>{"file_path", &DocumentInfo::filePath},
        Column<std::string, DocumentInfo>{"file_name", &DocumentInfo::fileName},
        Column<std::string, DocumentInfo>{"file_extension", &DocumentInfo::fileExtension},
        Column<int64_t, DocumentInfo>{"file_size", &DocumentInfo::fileSize},
        Column<std::string, DocumentInfo>{"sha256_hash", &DocumentInfo::sha256Hash},
        Column<std::string, DocumentInfo>{"mime_type", &DocumentInfo::mimeType},
        Column<std::string, DocumentInfo>{"path_prefix", &DocumentInfo::pathPrefix},
        Column<int, DocumentInfo>{"path_depth", &DocumentInfo::pathDepth}};

    /**
     * @brief Custom extraction for DocumentInfo with all fields
     *
     * Handles complex fields like timestamps, enums, and optional fields
     * that aren't covered by the simple column tuple.
     */
    static DocumentInfo extract(const Statement& stmt, int startCol = 0) {
        DocumentInfo info;

        // Extract core columns using trait-based extraction
        info.id = stmt.getInt64(startCol + 0);
        info.filePath = stmt.getString(startCol + 1);
        info.fileName = stmt.getString(startCol + 2);
        info.fileExtension = stmt.getString(startCol + 3);
        info.fileSize = stmt.getInt64(startCol + 4);
        info.sha256Hash = stmt.getString(startCol + 5);
        info.mimeType = stmt.getString(startCol + 6);
        info.pathPrefix = stmt.getString(startCol + 7);
        info.pathDepth = stmt.getInt(startCol + 8);

        // Extended columns (when full column list is selected)
        // These are optional - caller must ensure statement has enough columns
        if (stmt.columnCount() > startCol + 9) {
            info.reversePath = stmt.getString(startCol + 9);
            info.pathHash = stmt.getString(startCol + 10);
            info.parentHash = stmt.getString(startCol + 11);
            info.createdTime = stmt.getTime(startCol + 12);
            info.modifiedTime = stmt.getTime(startCol + 13);
            info.indexedTime = stmt.getTime(startCol + 14);
            info.contentExtracted = stmt.getInt(startCol + 15) != 0;

            // Extraction status (enum)
            auto statusStr = stmt.getString(startCol + 16);
            info.extractionStatus = ExtractionStatusUtils::fromString(statusStr);

            if (!stmt.isNull(startCol + 17)) {
                info.extractionError = stmt.getString(startCol + 17);
            }

            // Repair tracking (if columns exist)
            if (stmt.columnCount() > startCol + 18) {
                auto repairStatusStr = stmt.getString(startCol + 18);
                info.repairStatus = RepairStatusUtils::fromString(repairStatusStr);
                info.repairAttemptedAt = stmt.getTime(startCol + 19);
                info.repairAttempts = stmt.getInt(startCol + 20);
            }
        }

        return info;
    }
};

/**
 * @brief EntityTraits specialization for DocumentContent
 */
template <> struct EntityTraits<DocumentContent> {
    static constexpr std::string_view table = "document_content";
    static constexpr std::string_view primary_key = "document_id";

    using columns_tuple = std::tuple<Column<int64_t, DocumentContent, false, true>, // document_id
                                     Column<std::string, DocumentContent>,          // content_text
                                     Column<int64_t, DocumentContent>,     // content_length
                                     Column<std::string, DocumentContent>, // extraction_method
                                     Column<std::string, DocumentContent>  // language
                                     >;

    static constexpr columns_tuple columns{
        Column<int64_t, DocumentContent, false, true>{"document_id", &DocumentContent::documentId},
        Column<std::string, DocumentContent>{"content_text", &DocumentContent::contentText},
        Column<int64_t, DocumentContent>{"content_length", &DocumentContent::contentLength},
        Column<std::string, DocumentContent>{"extraction_method",
                                             &DocumentContent::extractionMethod},
        Column<std::string, DocumentContent>{"language", &DocumentContent::language}};

    static DocumentContent extract(const Statement& stmt, int startCol = 0) {
        DocumentContent content;
        content.documentId = stmt.getInt64(startCol + 0);
        content.contentText = stmt.getString(startCol + 1);
        content.contentLength = stmt.getInt64(startCol + 2);
        content.extractionMethod = stmt.getString(startCol + 3);
        content.language = stmt.getString(startCol + 4);
        return content;
    }
};

/**
 * @brief EntityTraits specialization for SearchHistoryEntry
 */
template <> struct EntityTraits<SearchHistoryEntry> {
    static constexpr std::string_view table = "search_history";
    static constexpr std::string_view primary_key = "id";

    using columns_tuple =
        std::tuple<Column<int64_t, SearchHistoryEntry, false, true, true>, // id
                   Column<std::string, SearchHistoryEntry>,                // query
                   Column<std::chrono::sys_seconds, SearchHistoryEntry>,   // query_time
                   Column<int64_t, SearchHistoryEntry>,                    // results_count
                   Column<int64_t, SearchHistoryEntry>,                    // execution_time_ms
                   Column<std::string, SearchHistoryEntry, true>           // user_context
                   >;

    static constexpr columns_tuple columns{
        Column<int64_t, SearchHistoryEntry, false, true, true>{"id", &SearchHistoryEntry::id},
        Column<std::string, SearchHistoryEntry>{"query", &SearchHistoryEntry::query},
        Column<std::chrono::sys_seconds, SearchHistoryEntry>{"query_time",
                                                             &SearchHistoryEntry::queryTime},
        Column<int64_t, SearchHistoryEntry>{"results_count", &SearchHistoryEntry::resultsCount},
        Column<int64_t, SearchHistoryEntry>{"execution_time_ms",
                                            &SearchHistoryEntry::executionTimeMs},
        Column<std::string, SearchHistoryEntry, true>{"user_context",
                                                      &SearchHistoryEntry::userContext}};

    static SearchHistoryEntry extract(const Statement& stmt, int startCol = 0) {
        SearchHistoryEntry entry;
        entry.id = stmt.getInt64(startCol + 0);
        entry.query = stmt.getString(startCol + 1);
        entry.queryTime = stmt.getTime(startCol + 2);
        entry.resultsCount = stmt.getInt64(startCol + 3);
        entry.executionTimeMs = stmt.getInt64(startCol + 4);
        if (!stmt.isNull(startCol + 5)) {
            entry.userContext = stmt.getString(startCol + 5);
        }
        return entry;
    }
};

/**
 * @brief EntityTraits specialization for FeedbackEvent
 */
template <> struct EntityTraits<FeedbackEvent> {
    static constexpr std::string_view table = "feedback_events";
    static constexpr std::string_view primary_key = "id";

    using columns_tuple = std::tuple<Column<int64_t, FeedbackEvent, false, true, true>, // id
                                     Column<std::string, FeedbackEvent>,                // event_id
                                     Column<std::string, FeedbackEvent>,                // trace_id
                                     Column<std::chrono::sys_seconds, FeedbackEvent>, // created_at
                                     Column<std::string, FeedbackEvent>,              // source
                                     Column<std::string, FeedbackEvent>,              // event_type
                                     Column<std::string, FeedbackEvent, true> // payload_json
                                     >;

    static constexpr columns_tuple columns{
        Column<int64_t, FeedbackEvent, false, true, true>{"id", &FeedbackEvent::id},
        Column<std::string, FeedbackEvent>{"event_id", &FeedbackEvent::eventId},
        Column<std::string, FeedbackEvent>{"trace_id", &FeedbackEvent::traceId},
        Column<std::chrono::sys_seconds, FeedbackEvent>{"created_at", &FeedbackEvent::createdAt},
        Column<std::string, FeedbackEvent>{"source", &FeedbackEvent::source},
        Column<std::string, FeedbackEvent>{"event_type", &FeedbackEvent::eventType},
        Column<std::string, FeedbackEvent, true>{"payload_json", &FeedbackEvent::payloadJson}};

    static FeedbackEvent extract(const Statement& stmt, int startCol = 0) {
        FeedbackEvent event;
        event.id = stmt.getInt64(startCol + 0);
        event.eventId = stmt.getString(startCol + 1);
        event.traceId = stmt.getString(startCol + 2);
        event.createdAt = stmt.getTime(startCol + 3);
        event.source = stmt.getString(startCol + 4);
        event.eventType = stmt.getString(startCol + 5);
        if (!stmt.isNull(startCol + 6)) {
            event.payloadJson = stmt.getString(startCol + 6);
        }
        return event;
    }
};

/**
 * @brief EntityTraits specialization for SavedQuery
 */
template <> struct EntityTraits<SavedQuery> {
    static constexpr std::string_view table = "saved_queries";
    static constexpr std::string_view primary_key = "id";

    using columns_tuple = std::tuple<Column<int64_t, SavedQuery, false, true, true>, // id
                                     Column<std::string, SavedQuery>,                // name
                                     Column<std::string, SavedQuery>,                // query
                                     Column<std::string, SavedQuery, true>,          // description
                                     Column<std::chrono::sys_seconds, SavedQuery>,   // created_time
                                     Column<std::chrono::sys_seconds, SavedQuery>,   // last_used
                                     Column<int64_t, SavedQuery>                     // use_count
                                     >;

    static constexpr columns_tuple columns{
        Column<int64_t, SavedQuery, false, true, true>{"id", &SavedQuery::id},
        Column<std::string, SavedQuery>{"name", &SavedQuery::name},
        Column<std::string, SavedQuery>{"query", &SavedQuery::query},
        Column<std::string, SavedQuery, true>{"description", &SavedQuery::description},
        Column<std::chrono::sys_seconds, SavedQuery>{"created_time", &SavedQuery::createdTime},
        Column<std::chrono::sys_seconds, SavedQuery>{"last_used", &SavedQuery::lastUsed},
        Column<int64_t, SavedQuery>{"use_count", &SavedQuery::useCount}};

    static SavedQuery extract(const Statement& stmt, int startCol = 0) {
        SavedQuery query;
        query.id = stmt.getInt64(startCol + 0);
        query.name = stmt.getString(startCol + 1);
        query.query = stmt.getString(startCol + 2);
        if (!stmt.isNull(startCol + 3)) {
            query.description = stmt.getString(startCol + 3);
        }
        query.createdTime = stmt.getTime(startCol + 4);
        query.lastUsed = stmt.getTime(startCol + 5);
        query.useCount = stmt.getInt64(startCol + 6);
        return query;
    }
};

/**
 * @brief Simple struct for metadata table CRUD operations
 *
 * Maps the metadata table which stores key-value pairs per document.
 * Note: The primary key is composite (document_id, key), handled via upsert.
 */
struct MetadataEntry {
    int64_t documentId{0};
    std::string key;
    std::string value;
    std::string valueType;
};

/**
 * @brief EntityTraits specialization for DocumentRelationship
 *
 * Maps the document_relationships table for parent-child relationships.
 */
template <> struct EntityTraits<DocumentRelationship> {
    static constexpr std::string_view table = "document_relationships";
    static constexpr std::string_view primary_key = "id";

    // Note: relationship_type is stored as string but maps to enum+customType
    // We store a string representation for the column tuple, but extract handles the conversion
    using columns_tuple =
        std::tuple<Column<int64_t, DocumentRelationship, false, true, true>, // id
                   Column<int64_t, DocumentRelationship, true>,              // parent_id
                   Column<int64_t, DocumentRelationship>,                    // child_id
                   Column<std::string, DocumentRelationship>,                // relationship_type
                   Column<std::chrono::sys_seconds, DocumentRelationship>    // created_time
                   >;

    static constexpr columns_tuple columns{
        Column<int64_t, DocumentRelationship, false, true, true>{"id", &DocumentRelationship::id},
        Column<int64_t, DocumentRelationship, true>{"parent_id", &DocumentRelationship::parentId},
        Column<int64_t, DocumentRelationship>{"child_id", &DocumentRelationship::childId},
        Column<std::string, DocumentRelationship>{"relationship_type",
                                                  nullptr}, // extract handles this
        Column<std::chrono::sys_seconds, DocumentRelationship>{"created_time",
                                                               &DocumentRelationship::createdTime}};

    /**
     * @brief Custom extraction for DocumentRelationship
     *
     * Handles the relationship_type string to enum conversion.
     * Column order: id, parent_id, child_id, relationship_type, created_time
     */
    static DocumentRelationship extract(const Statement& stmt, int startCol = 0) {
        DocumentRelationship rel;
        rel.id = stmt.getInt64(startCol + 0);
        if (!stmt.isNull(startCol + 1)) {
            rel.parentId = stmt.getInt64(startCol + 1);
        }
        rel.childId = stmt.getInt64(startCol + 2);
        rel.setRelationshipTypeFromString(stmt.getString(startCol + 3));
        rel.createdTime = stmt.getTime(startCol + 4);
        return rel;
    }

    /**
     * @brief Custom bind for DocumentRelationship
     *
     * Handles the relationship_type enum to string conversion.
     */
    static Result<void> bind(Statement& stmt, const DocumentRelationship& rel, int startIdx = 1) {
        // Skip id for inserts (auto-increment)
        if (rel.parentId != 0) {
            YAMS_TRY(stmt.bind(startIdx++, rel.parentId));
        } else {
            YAMS_TRY(stmt.bind(startIdx++, nullptr));
        }
        YAMS_TRY(stmt.bind(startIdx++, rel.childId));
        YAMS_TRY(stmt.bind(startIdx++, rel.getRelationshipTypeString()));
        YAMS_TRY(stmt.bind(startIdx++, rel.createdTime));
        return {};
    }
};

/**
 * @brief EntityTraits specialization for MetadataEntry
 *
 * Maps the metadata table for document key-value pairs.
 * Note: Primary key is composite (document_id, key), so insert uses REPLACE.
 */
template <> struct EntityTraits<MetadataEntry> {
    static constexpr std::string_view table = "metadata";
    static constexpr std::string_view primary_key = "document_id"; // Part of composite PK

    using columns_tuple = std::tuple<Column<int64_t, MetadataEntry, false, true>, // document_id
                                     Column<std::string, MetadataEntry>,          // key
                                     Column<std::string, MetadataEntry>,          // value
                                     Column<std::string, MetadataEntry>           // value_type
                                     >;

    static constexpr columns_tuple columns{
        Column<int64_t, MetadataEntry, false, true>{"document_id", &MetadataEntry::documentId},
        Column<std::string, MetadataEntry>{"key", &MetadataEntry::key},
        Column<std::string, MetadataEntry>{"value", &MetadataEntry::value},
        Column<std::string, MetadataEntry>{"value_type", &MetadataEntry::valueType}};

    static MetadataEntry extract(const Statement& stmt, int startCol = 0) {
        MetadataEntry entry;
        entry.documentId = stmt.getInt64(startCol + 0);
        entry.key = stmt.getString(startCol + 1);
        entry.value = stmt.getString(startCol + 2);
        entry.valueType = stmt.getString(startCol + 3);
        return entry;
    }
};

// TODO: Add EntityTraits<PathTreeNode> when needed
// PathTreeNode is defined in metadata_repository.h and has a BLOB centroid field
// that requires special handling for extraction.

} // namespace yams::metadata::repository
