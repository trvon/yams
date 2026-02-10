// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#pragma once

/**
 * @file crud_ops.hpp
 * @brief Generic CRUD operations using entity traits
 *
 * Provides template-based CRUD (Create, Read, Update, Delete) operations
 * that work with any entity type that has EntityTraits defined.
 *
 * @see ADR-0004: MetadataRepository Refactor with Templates and Metaprogramming
 */

#include "entity_traits.hpp"
#include "result_helpers.hpp"

#include <yams/core/cpp23_features.hpp>
#include <yams/core/types.h>
#include <yams/metadata/database.h>

#include <optional>
#include <string>
#include <vector>

namespace yams::metadata::repository {

// ============================================================================
// Generic CRUD Operations Class
// ============================================================================

/**
 * @brief Generic CRUD operations for entities with EntityTraits
 *
 * This class template provides standard database operations for any entity
 * type that has a valid EntityTraits specialization defined.
 *
 * Features:
 * - Type-safe operations using C++20 concepts
 * - Automatic SQL generation from traits
 * - Prepared statement caching for performance
 * - Consistent error handling with Result<T>
 *
 * Example usage:
 * @code
 * CrudOps<DocumentInfo> docOps;
 *
 * // Get by ID
 * auto result = docOps.getById(db, 123);
 *
 * // Insert
 * DocumentInfo doc;
 * doc.filePath = "/path/to/file";
 * auto insertResult = docOps.insert(db, doc);
 *
 * // Query with condition
 * auto docs = docOps.query(db, "file_extension = ?", ".cpp");
 * @endcode
 *
 * @tparam Entity The entity type (must satisfy HasEntityTraits concept)
 */
template <HasEntityTraits Entity> class CrudOps {
public:
    using Traits = EntityTraits<Entity>;

    // ========================================================================
    // Read Operations
    // ========================================================================

    /**
     * @brief Get entity by primary key
     *
     * @param db Database connection
     * @param id Primary key value
     * @return Result containing optional entity (empty if not found)
     */
    Result<std::optional<Entity>> getById(Database& db, int64_t id) {
        static const std::string sql =
            buildSelectSql<Entity>() + " WHERE " + std::string(Traits::primary_key) + " = ?";

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));
        YAMS_TRY(stmt->bind(1, id));
        YAMS_TRY_UNWRAP(hasRow, stmt->step());

        if (!hasRow) {
            return std::optional<Entity>{};
        }

        return std::optional<Entity>{Traits::extract(*stmt)};
    }

    /**
     * @brief Get all entities (with optional limit)
     *
     * @param db Database connection
     * @param limit Maximum number of entities to return (0 = no limit)
     * @return Result containing vector of entities
     */
    Result<std::vector<Entity>> getAll(Database& db, int limit = 0) {
        std::string sql = buildSelectSql<Entity>();
        if (limit > 0) {
            sql += " LIMIT " + std::to_string(limit);
        }

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        std::vector<Entity> results;
        while (true) {
            YAMS_TRY_UNWRAP(hasRow, stmt->step());
            if (!hasRow)
                break;
            results.push_back(Traits::extract(*stmt));
        }

        return results;
    }

    /**
     * @brief Query entities with a WHERE condition
     *
     * @param db Database connection
     * @param whereClause WHERE clause without "WHERE" keyword (e.g., "status = ?")
     * @param args Bind arguments for placeholders
     * @return Result containing vector of matching entities
     *
     * Example:
     * @code
     * auto results = ops.query(db, "file_extension = ? AND file_size > ?", ".cpp", 1000);
     * @endcode
     */
    template <typename... Args>
    Result<std::vector<Entity>> query(Database& db, const std::string& whereClause,
                                      Args&&... args) {
        std::string sql = buildSelectSql<Entity>() + " WHERE " + whereClause;

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        if constexpr (sizeof...(args) > 0) {
            YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
        }

        std::vector<Entity> results;
        while (true) {
            YAMS_TRY_UNWRAP(hasRow, stmt->step());
            if (!hasRow)
                break;
            results.push_back(Traits::extract(*stmt));
        }

        return results;
    }

    /**
     * @brief Get single entity by column value
     *
     * Convenience method for common lookups like getByPath, getByHash, etc.
     *
     * @param db Database connection
     * @param columnName Column to search (must be valid identifier)
     * @param value Value to match
     * @return Result containing optional entity (empty if not found)
     *
     * Example:
     * @code
     * auto doc = ops.getByColumn(db, "file_path", "/path/to/file.txt");
     * auto doc2 = ops.getByColumn(db, "sha256_hash", hashValue);
     * @endcode
     */
    template <typename T>
    Result<std::optional<Entity>> getByColumn(Database& db, const std::string& columnName,
                                              const T& value) {
        std::string sql = buildSelectSql<Entity>() + " WHERE " + columnName + " = ? LIMIT 1";

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));
        YAMS_TRY(stmt->bind(1, value));
        YAMS_TRY_UNWRAP(hasRow, stmt->step());

        if (!hasRow) {
            return std::optional<Entity>{};
        }

        return std::optional<Entity>{Traits::extract(*stmt)};
    }

    /**
     * @brief Query single entity with a WHERE condition
     *
     * @param db Database connection
     * @param whereClause WHERE clause without "WHERE" keyword
     * @param args Bind arguments for placeholders
     * @return Result containing optional entity (empty if not found)
     */
    template <typename... Args>
    Result<std::optional<Entity>> queryOne(Database& db, const std::string& whereClause,
                                           Args&&... args) {
        std::string sql = buildSelectSql<Entity>() + " WHERE " + whereClause + " LIMIT 1";

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        if constexpr (sizeof...(args) > 0) {
            YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
        }

        YAMS_TRY_UNWRAP(hasRow, stmt->step());

        if (!hasRow) {
            return std::optional<Entity>{};
        }

        return std::optional<Entity>{Traits::extract(*stmt)};
    }

    /**
     * @brief Get entities by multiple IDs using IN clause
     *
     * Efficiently queries multiple entities in a single SQL statement.
     * Automatically chunks large ID lists to avoid SQLite parameter limits.
     *
     * @param db Database connection
     * @param ids Vector of primary key values
     * @param chunkSize Maximum IDs per query (default: 500)
     * @return Result containing vector of found entities (may be fewer than ids.size())
     */
    Result<std::vector<Entity>> getByIds(Database& db, const std::vector<int64_t>& ids,
                                         size_t chunkSize = 500) {
        if (ids.empty()) {
            return std::vector<Entity>{};
        }

        std::vector<Entity> results;
        results.reserve(ids.size());

        // SQLite has ~999 parameter limit, chunk to stay safe
        const size_t actualChunkSize = std::min(chunkSize, size_t{900});

        for (size_t start = 0; start < ids.size(); start += actualChunkSize) {
            const size_t end = std::min(start + actualChunkSize, ids.size());
            const size_t batchSize = end - start;

            // Build IN clause: SELECT ... WHERE pk IN (?, ?, ...)
            std::string placeholders;
            for (size_t i = 0; i < batchSize; ++i) {
                if (i > 0)
                    placeholders += ", ";
                placeholders += "?";
            }

            std::string sql = buildSelectSql<Entity>() + " WHERE " +
                              std::string(Traits::primary_key) + " IN (" + placeholders + ")";

            YAMS_TRY_UNWRAP(stmt, db.prepare(sql));

            int idx = 1;
            for (size_t i = start; i < end; ++i) {
                YAMS_TRY(stmt.bind(idx++, ids[i]));
            }

            while (true) {
                YAMS_TRY_UNWRAP(hasRow, stmt.step());
                if (!hasRow)
                    break;
                results.push_back(Traits::extract(stmt));
            }
        }

        return results;
    }

    /**
     * @brief Get all entities with ORDER BY and optional LIMIT
     *
     * @param db Database connection
     * @param orderBy ORDER BY clause without "ORDER BY" (e.g., "created_time DESC")
     * @param limit Maximum number of results (0 = no limit)
     * @return Result containing vector of entities
     */
    Result<std::vector<Entity>> getAllOrdered(Database& db, const std::string& orderBy,
                                              int limit = 0) {
        std::string sql = buildSelectSql<Entity>();
        if (!orderBy.empty()) {
            sql += " ORDER BY " + orderBy;
        }
        if (limit > 0) {
            sql += " LIMIT " + std::to_string(limit);
        }

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        std::vector<Entity> results;
        while (true) {
            YAMS_TRY_UNWRAP(hasRow, stmt->step());
            if (!hasRow)
                break;
            results.push_back(Traits::extract(*stmt));
        }

        return results;
    }

    /**
     * @brief Query entities with WHERE, ORDER BY, and LIMIT
     *
     * @param db Database connection
     * @param whereClause WHERE clause without "WHERE" keyword
     * @param orderBy ORDER BY clause without "ORDER BY" keyword (can be empty)
     * @param limit Maximum results (0 = no limit)
     * @param args Bind arguments for WHERE placeholders
     * @return Result containing vector of matching entities
     */
    template <typename... Args>
    Result<std::vector<Entity>> queryOrdered(Database& db, const std::string& whereClause,
                                             const std::string& orderBy, int limit,
                                             Args&&... args) {
        std::string sql = buildSelectSql<Entity>() + " WHERE " + whereClause;
        if (!orderBy.empty()) {
            sql += " ORDER BY " + orderBy;
        }
        if (limit > 0) {
            sql += " LIMIT " + std::to_string(limit);
        }

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        if constexpr (sizeof...(args) > 0) {
            YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
        }

        std::vector<Entity> results;
        while (true) {
            YAMS_TRY_UNWRAP(hasRow, stmt->step());
            if (!hasRow)
                break;
            results.push_back(Traits::extract(*stmt));
        }

        return results;
    }

    /**
     * @brief Check if entity exists by primary key
     *
     * @param db Database connection
     * @param id Primary key value
     * @return Result<bool> true if exists
     */
    Result<bool> exists(Database& db, int64_t id) {
        static const std::string sql = "SELECT 1 FROM " + std::string(Traits::table) + " WHERE " +
                                       std::string(Traits::primary_key) + " = ? LIMIT 1";

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));
        YAMS_TRY(stmt->bind(1, id));
        YAMS_TRY_UNWRAP(hasRow, stmt->step());

        return hasRow;
    }

    /**
     * @brief Count entities matching a condition
     *
     * @param db Database connection
     * @param whereClause Optional WHERE clause (empty = count all)
     * @param args Bind arguments for placeholders
     * @return Result<int64_t> count
     */
    template <typename... Args>
    Result<int64_t> count(Database& db, const std::string& whereClause = "", Args&&... args) {
        std::string sql = "SELECT COUNT(*) FROM " + std::string(Traits::table);
        if (!whereClause.empty()) {
            sql += " WHERE " + whereClause;
        }

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        if constexpr (sizeof...(args) > 0) {
            YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
        }

        YAMS_TRY_UNWRAP(hasRow, stmt->step());
        if (!hasRow) {
            return int64_t{0};
        }

        return stmt->getInt64(0);
    }

    // ========================================================================
    // Write Operations
    // ========================================================================

    /**
     * @brief Insert a new entity
     *
     * @param db Database connection
     * @param entity Entity to insert
     * @return Result<int64_t> the inserted row ID
     */
    Result<int64_t> insert(Database& db, const Entity& entity) {
        static const std::string sql = buildInsertSql<Entity>();

        // Use prepare() not prepareCached() for INSERT to avoid statement reuse issues
        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));
        YAMS_TRY(bindEntityColumns(stmt, entity));
        YAMS_TRY(stmt.execute());

        return db.lastInsertRowId();
    }

    /**
     * @brief Update an existing entity
     *
     * Uses the entity's primary key to identify the row to update.
     *
     * @param db Database connection
     * @param entity Entity with updated values
     * @return Result<void>
     */
    Result<void> update(Database& db, const Entity& entity) {
        static const std::string sql = buildUpdateSql<Entity>();

        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));

        // Bind all non-PK columns first, then PK for WHERE clause
        int idx = 1;

        constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;

        auto bindByPkFlag = [&]<std::size_t... Is>(std::index_sequence<Is...>, bool wantPk) {
            Result<void> bindResult{};
            (([&] {
                 if (!bindResult) {
                     return;
                 }
                 using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                 const auto& col = std::get<Is>(Traits::columns);
                 if (Col::kIsPrimaryKey != wantPk) {
                     return;
                 }
                 bindResult = stmt.bind(idx++, entity.*(col.member));
             }()),
             ...);
            return bindResult;
        };

        // Bind SET columns (non-PK) then WHERE clause (PK)
        YAMS_TRY(bindByPkFlag(std::make_index_sequence<N>{}, false));
        YAMS_TRY(bindByPkFlag(std::make_index_sequence<N>{}, true));

        return stmt.execute();
    }

    /**
     * @brief Delete entity by primary key
     *
     * @param db Database connection
     * @param id Primary key value
     * @return Result<void>
     */
    Result<void> deleteById(Database& db, int64_t id) {
        static const std::string sql = "DELETE FROM " + std::string(Traits::table) + " WHERE " +
                                       std::string(Traits::primary_key) + " = ?";

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));
        YAMS_TRY(stmt->bind(1, id));

        return stmt->execute();
    }

    /**
     * @brief Delete entities matching a condition
     *
     * @param db Database connection
     * @param whereClause WHERE clause without "WHERE" keyword
     * @param args Bind arguments for placeholders
     * @return Result<int> number of deleted rows
     */
    template <typename... Args>
    Result<int> deleteWhere(Database& db, const std::string& whereClause, Args&&... args) {
        std::string sql = "DELETE FROM " + std::string(Traits::table) + " WHERE " + whereClause;

        YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

        if constexpr (sizeof...(args) > 0) {
            YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
        }

        YAMS_TRY(stmt->execute());

        return db.changes();
    }

    // ========================================================================
    // Batch Operations
    // ========================================================================

    /**
     * @brief Insert multiple entities in a transaction
     *
     * @param db Database connection
     * @param entities Vector of entities to insert
     * @return Result<std::vector<int64_t>> vector of inserted row IDs
     */
    Result<std::vector<int64_t>> insertBatch(Database& db, const std::vector<Entity>& entities) {
        if (entities.empty()) {
            return std::vector<int64_t>{};
        }

        static const std::string sql = buildInsertSql<Entity>();
        std::vector<int64_t> ids;
        ids.reserve(entities.size());

        YAMS_TRY(db.beginTransaction());
        auto rollback = scope_exit([&db]() { db.rollback(); });

        for (const auto& entity : entities) {
            YAMS_TRY_UNWRAP(stmt, db.prepare(sql));
            YAMS_TRY(bindEntityColumns(stmt, entity));
            YAMS_TRY(stmt.execute());
            ids.push_back(db.lastInsertRowId());
        }

        rollback.dismiss();
        YAMS_TRY(db.commit());

        return ids;
    }

    /**
     * @brief Delete multiple entities by IDs
     *
     * @param db Database connection
     * @param ids Vector of primary key values to delete
     * @return Result<int> number of deleted rows
     */
    Result<int> deleteBatch(Database& db, const std::vector<int64_t>& ids) {
        if (ids.empty()) {
            return 0;
        }

        // Build IN clause: DELETE FROM table WHERE pk IN (?, ?, ...)
        std::string placeholders;
        for (size_t i = 0; i < ids.size(); ++i) {
            if (i > 0)
                placeholders += ", ";
            placeholders += "?";
        }

        std::string sql = "DELETE FROM " + std::string(Traits::table) + " WHERE " +
                          std::string(Traits::primary_key) + " IN (" + placeholders + ")";

        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));

        int idx = 1;
        for (int64_t id : ids) {
            YAMS_TRY(stmt.bind(idx++, id));
        }

        YAMS_TRY(stmt.execute());

        return db.changes();
    }

    // ========================================================================
    // Optimized Batch Operations (Phase 4)
    // ========================================================================

    /**
     * @brief Insert multiple entities with prepared statement reuse
     *
     * More efficient than insertBatch() as it reuses a single prepared statement.
     * The statement is reset and rebound for each entity instead of being recreated.
     *
     * @param db Database connection
     * @param entities Vector of entities to insert
     * @return Result<std::vector<int64_t>> vector of inserted row IDs
     */
    Result<std::vector<int64_t>> insertBatchOptimized(Database& db,
                                                      const std::vector<Entity>& entities) {
        if (entities.empty()) {
            return std::vector<int64_t>{};
        }

        static const std::string sql = buildInsertSql<Entity>();
        std::vector<int64_t> ids;
        ids.reserve(entities.size());

        YAMS_TRY(db.beginTransaction());
        auto rollback = scope_exit([&db]() { db.rollback(); });

        // Prepare statement once, reuse for all entities
        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));

        for (const auto& entity : entities) {
            YAMS_TRY(stmt.reset());
            YAMS_TRY(bindEntityColumns(stmt, entity));
            YAMS_TRY(stmt.execute());
            ids.push_back(db.lastInsertRowId());
        }

        rollback.dismiss();
        YAMS_TRY(db.commit());

        return ids;
    }

    /**
     * @brief Insert multiple entities using multi-row VALUES syntax
     *
     * Generates: INSERT INTO table (cols) VALUES (...), (...), ...
     * Most efficient for bulk inserts as it's a single SQL statement.
     *
     * Note: SQLite has a limit on number of parameters (default 999).
     * This method automatically chunks large batches.
     *
     * @param db Database connection
     * @param entities Vector of entities to insert
     * @param chunkSize Maximum rows per INSERT statement (default: 100)
     * @return Result<int64_t> number of rows inserted
     */
    Result<int64_t> insertMultiRow(Database& db, const std::vector<Entity>& entities,
                                   size_t chunkSize = 100) {
        if (entities.empty()) {
            return int64_t{0};
        }

        // Count non-auto-increment columns for parameter calculation
        constexpr auto& cols = Traits::columns;
        constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;
        constexpr size_t paramCount = []() constexpr {
            size_t count = 0;
            [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (([&] {
                     using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                     if constexpr (!(Col::kAutoIncrement && Col::kIsPrimaryKey)) {
                         ++count;
                     }
                 }()),
                 ...);
            }(std::make_index_sequence<N>{});
            return count;
        }();

        // Ensure chunk size doesn't exceed SQLite parameter limit
        constexpr size_t maxParams = 999;
        const size_t maxChunk = std::min(chunkSize, maxParams / paramCount);
        const size_t actualChunkSize = std::max(size_t{1}, maxChunk);

        YAMS_TRY(db.beginTransaction());
        auto rollback = scope_exit([&db]() { db.rollback(); });

        int64_t totalInserted = 0;

        for (size_t start = 0; start < entities.size(); start += actualChunkSize) {
            const size_t end = std::min(start + actualChunkSize, entities.size());
            const size_t batchSize = end - start;

            // Build multi-row INSERT SQL
            std::string columns;
            std::string singleRow;
            bool first = true;

            [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                (([&] {
                     using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                     constexpr auto& col = std::get<Is>(cols);
                     if constexpr (!(Col::kAutoIncrement && Col::kIsPrimaryKey)) {
                         if (!first) {
                             columns += ", ";
                             singleRow += ", ";
                         }
                         columns += std::string(col.name);
                         singleRow += "?";
                         first = false;
                     }
                 }()),
                 ...);
            }(std::make_index_sequence<N>{});

            std::string values;
            for (size_t i = 0; i < batchSize; ++i) {
                if (i > 0)
                    values += ", ";
                values += "(" + singleRow + ")";
            }

            std::string sql =
                "INSERT INTO " + std::string(Traits::table) + " (" + columns + ") VALUES " + values;

            YAMS_TRY_UNWRAP(stmt, db.prepare(sql));

            // Bind all entities in this chunk
            int idx = 1;
            for (size_t i = start; i < end; ++i) {
                const auto& entity = entities[i];
                [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                    (([&] {
                         using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                         constexpr auto& col = std::get<Is>(cols);
                         if constexpr (!(Col::kAutoIncrement && Col::kIsPrimaryKey)) {
                             stmt.bind(idx++, entity.*(col.member));
                         }
                     }()),
                     ...);
                }(std::make_index_sequence<N>{});
            }

            YAMS_TRY(stmt.execute());
            totalInserted += static_cast<int64_t>(batchSize);
        }

        rollback.dismiss();
        YAMS_TRY(db.commit());

        return totalInserted;
    }

    /**
     * @brief Upsert (INSERT OR REPLACE) multiple entities
     *
     * Inserts new rows or replaces existing ones based on primary key.
     *
     * @param db Database connection
     * @param entities Vector of entities to upsert
     * @return Result<int64_t> number of rows affected
     */
    Result<int64_t> upsertBatch(Database& db, const std::vector<Entity>& entities) {
        if (entities.empty()) {
            return int64_t{0};
        }

        // Build INSERT OR REPLACE statement
        std::string columns;
        std::string placeholders;
        constexpr auto& cols = Traits::columns;
        constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;
        bool first = true;

        [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                 constexpr auto& col = std::get<Is>(cols);
                 // For upsert, include all columns including PK
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

        // Batch using a multi-row INSERT OR REPLACE to reduce per-row overhead.
        // Keep a conservative parameter cap (SQLite default is 999).
        constexpr size_t kMaxParamsPerStmt = 900;
        const size_t colsPerRow = static_cast<size_t>(N);
        const size_t maxRowsPerStmt = std::max<size_t>(1, kMaxParamsPerStmt / colsPerRow);

        YAMS_TRY(db.beginTransaction());
        auto rollback = scope_exit([&db]() { db.rollback(); });

        int64_t affected = 0;
        for (size_t start = 0; start < entities.size(); start += maxRowsPerStmt) {
            const size_t end = std::min(start + maxRowsPerStmt, entities.size());
            const size_t batchSize = end - start;

            // Build VALUES list: (?,?,?,?),(?,?,?,?),...
            std::string values;
            values.reserve(batchSize * (placeholders.size() + 3));
            for (size_t i = 0; i < batchSize; ++i) {
                if (i > 0)
                    values += ", ";
                values += "(" + placeholders + ")";
            }

            std::string sql = "INSERT OR REPLACE INTO " + std::string(Traits::table) + " (" +
                              columns + ") VALUES " + values;

            YAMS_TRY_UNWRAP(stmt, db.prepare(sql));

            int idx = 1;
            for (size_t i = start; i < end; ++i) {
                const auto& entity = entities[i];
                Result<void> bindResult{};

                [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                    (([&] {
                         if (!bindResult) {
                             return;
                         }
                         const auto& col = std::get<Is>(cols);
                         bindResult = stmt.bind(idx++, entity.*(col.member));
                     }()),
                     ...);
                }(std::make_index_sequence<N>{});

                YAMS_TRY(bindResult);
            }

            YAMS_TRY(stmt.execute());
            affected += static_cast<int64_t>(batchSize);
        }

        rollback.dismiss();
        YAMS_TRY(db.commit());

        return affected;
    }

    /**
     * @brief Upsert single entity using ON CONFLICT DO UPDATE
     *
     * Uses SQLite 3.24+ syntax: INSERT ... ON CONFLICT(column) DO UPDATE SET ...
     * Preserves rowid (unlike INSERT OR REPLACE which deletes + reinserts).
     *
     * @param db Database connection
     * @param entity Entity to upsert
     * @param conflictColumn Column name for conflict detection (e.g., "document_id")
     * @return Result<void>
     */
    Result<void> upsertOnConflict(Database& db, const Entity& entity,
                                  std::string_view conflictColumn) {
        constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;

        // Build column list and placeholders
        std::string columns;
        std::string placeholders;
        std::string updateSet;
        bool first = true;
        bool firstUpdate = true;

        [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                 const auto& col = std::get<Is>(Traits::columns);
                 // Skip auto-increment PK for INSERT
                 using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                 if (!(Col::kAutoIncrement && Col::kIsPrimaryKey)) {
                     if (!first) {
                         columns += ", ";
                         placeholders += ", ";
                     }
                     columns += std::string(col.name);
                     placeholders += "?";
                     first = false;

                     // Build UPDATE SET clause (exclude conflict column)
                     if (std::string_view(col.name) != conflictColumn) {
                         if (!firstUpdate) {
                             updateSet += ", ";
                         }
                         updateSet +=
                             std::string(col.name) + " = excluded." + std::string(col.name);
                         firstUpdate = false;
                     }
                 }
             }()),
             ...);
        }(std::make_index_sequence<N>{});

        std::string sql = "INSERT INTO " + std::string(Traits::table) + " (" + columns +
                          ") VALUES (" + placeholders + ") ON CONFLICT(" +
                          std::string(conflictColumn) + ") DO UPDATE SET " + updateSet;

        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));
        YAMS_TRY(bindEntityColumns(stmt, entity));

        return stmt.execute();
    }

    /**
     * @brief Upsert multiple entities using ON CONFLICT DO UPDATE
     *
     * Batch version with transaction for performance.
     *
     * @param db Database connection
     * @param entities Vector of entities to upsert
     * @param conflictColumn Column name for conflict detection
     * @return Result<int64_t> number of rows affected
     */
    Result<int64_t> upsertOnConflictBatch(Database& db, const std::vector<Entity>& entities,
                                          std::string_view conflictColumn) {
        if (entities.empty()) {
            return int64_t{0};
        }

        constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;

        // Build SQL once
        std::string columns;
        std::string placeholders;
        std::string updateSet;
        bool first = true;
        bool firstUpdate = true;

        [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            (([&] {
                 const auto& col = std::get<Is>(Traits::columns);
                 using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                 if (!(Col::kAutoIncrement && Col::kIsPrimaryKey)) {
                     if (!first) {
                         columns += ", ";
                         placeholders += ", ";
                     }
                     columns += std::string(col.name);
                     placeholders += "?";
                     first = false;

                     if (std::string_view(col.name) != conflictColumn) {
                         if (!firstUpdate) {
                             updateSet += ", ";
                         }
                         updateSet +=
                             std::string(col.name) + " = excluded." + std::string(col.name);
                         firstUpdate = false;
                     }
                 }
             }()),
             ...);
        }(std::make_index_sequence<N>{});

        std::string sql = "INSERT INTO " + std::string(Traits::table) + " (" + columns +
                          ") VALUES (" + placeholders + ") ON CONFLICT(" +
                          std::string(conflictColumn) + ") DO UPDATE SET " + updateSet;

        YAMS_TRY(db.beginTransaction());
        auto rollback = scope_exit([&db]() { db.rollback(); });

        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));
        int64_t affected = 0;

        for (const auto& entity : entities) {
            YAMS_TRY(stmt.reset());
            YAMS_TRY(bindEntityColumns(stmt, entity));
            YAMS_TRY(stmt.execute());
            ++affected;
        }

        rollback.dismiss();
        YAMS_TRY(db.commit());

        return affected;
    }

    /**
     * @brief Update multiple entities in a batch
     *
     * @param db Database connection
     * @param entities Vector of entities to update (uses primary key for WHERE)
     * @return Result<int64_t> number of rows updated
     */
    Result<int64_t> updateBatch(Database& db, const std::vector<Entity>& entities) {
        if (entities.empty()) {
            return int64_t{0};
        }

        static const std::string sql = buildUpdateSql<Entity>();

        YAMS_TRY(db.beginTransaction());
        auto rollback = scope_exit([&db]() { db.rollback(); });

        YAMS_TRY_UNWRAP(stmt, db.prepare(sql));
        int64_t updated = 0;

        for (const auto& entity : entities) {
            YAMS_TRY(stmt.reset());

            // Bind SET columns (non-PK) first, then PK for WHERE clause
            int idx = 1;

            constexpr auto N = std::tuple_size_v<typename Traits::columns_tuple>;

            auto bindByPkFlag = [&]<std::size_t... Is>(std::index_sequence<Is...>, bool wantPk) {
                Result<void> bindResult{};
                (([&] {
                     if (!bindResult) {
                         return;
                     }
                     using Col = std::tuple_element_t<Is, typename Traits::columns_tuple>;
                     const auto& col = std::get<Is>(Traits::columns);
                     if (Col::kIsPrimaryKey != wantPk) {
                         return;
                     }
                     bindResult = stmt.bind(idx++, entity.*(col.member));
                 }()),
                 ...);
                return bindResult;
            };

            YAMS_TRY(bindByPkFlag(std::make_index_sequence<N>{}, false));
            YAMS_TRY(bindByPkFlag(std::make_index_sequence<N>{}, true));

            YAMS_TRY(stmt.execute());
            updated += db.changes();
        }

        rollback.dismiss();
        YAMS_TRY(db.commit());

        return updated;
    }
};

// ============================================================================
// Query Callback Pattern
// ============================================================================

/**
 * @brief Execute a query and call a callback for each result row
 *
 * Useful for processing large result sets without loading all into memory.
 *
 * @param db Database connection
 * @param sql Full SQL query
 * @param callback Function called for each row (receives Statement& for column access)
 * @param args Bind arguments for placeholders
 * @return Result<int64_t> number of rows processed
 *
 * Example:
 * @code
 * int64_t count = 0;
 * auto result = forEachRow(db, "SELECT id, name FROM users WHERE active = ?",
 *     [&count](Statement& stmt) {
 *         spdlog::info("User {}: {}", stmt.getInt64(0), stmt.getString(1));
 *         count++;
 *     },
 *     true);  // active = true
 * @endcode
 */
template <typename Callback, typename... Args>
Result<int64_t> forEachRow(Database& db, const std::string& sql, Callback&& callback,
                           Args&&... args) {
    YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

    if constexpr (sizeof...(args) > 0) {
        YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
    }

    int64_t rowCount = 0;
    while (true) {
        YAMS_TRY_UNWRAP(hasRow, stmt->step());
        if (!hasRow)
            break;
        callback(*stmt);
        ++rowCount;
    }

    return rowCount;
}

/**
 * @brief Execute a query and collect results using an extractor function
 *
 * @param db Database connection
 * @param sql Full SQL query
 * @param extractor Function that extracts T from Statement
 * @param args Bind arguments for placeholders
 * @return Result<std::vector<T>> collected results
 *
 * Example:
 * @code
 * auto names = collectRows<std::string>(db,
 *     "SELECT name FROM users WHERE active = ?",
 *     [](Statement& stmt) { return stmt.getString(0); },
 *     true);
 * @endcode
 */
template <typename T, typename Extractor, typename... Args>
Result<std::vector<T>> collectRows(Database& db, const std::string& sql, Extractor&& extractor,
                                   Args&&... args) {
    YAMS_TRY_UNWRAP(stmt, db.prepareCached(sql));

    if constexpr (sizeof...(args) > 0) {
        YAMS_TRY(stmt->bindAll(std::forward<Args>(args)...));
    }

    std::vector<T> results;
    while (true) {
        YAMS_TRY_UNWRAP(hasRow, stmt->step());
        if (!hasRow)
            break;
        results.push_back(extractor(*stmt));
    }

    return results;
}

// ============================================================================
// Transaction Helper
// ============================================================================

/**
 * @brief Execute a function within a transaction
 *
 * Automatically commits on success, rolls back on error or exception.
 *
 * @param db Database connection
 * @param func Function to execute (must return Result<T>)
 * @return Result<T> from func, or error if transaction failed
 *
 * Example:
 * @code
 * auto result = withTransaction(db, [&]() -> Result<void> {
 *     YAMS_TRY(ops.insert(db, entity1));
 *     YAMS_TRY(ops.insert(db, entity2));
 *     return {};
 * });
 * @endcode
 */
template <typename Func> auto withTransaction(Database& db, Func&& func) -> decltype(func()) {
    YAMS_TRY(db.beginTransaction());
    auto rollback = scope_exit([&db]() {
        if (db.inTransaction()) {
            db.rollback();
        }
    });

    auto result = func();

    if (result.has_value()) {
        rollback.dismiss();
        YAMS_TRY(db.commit());
    }

    return result;
}

} // namespace yams::metadata::repository
