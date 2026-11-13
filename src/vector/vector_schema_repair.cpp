#include <yams/vector/vector_schema_repair.h>

#include <exception>
#include <initializer_list>
#include <optional>
#include <string>
#include <system_error>

#include <sqlite3.h>

#include <yams/vector/sqlite_vec_backend.h>

namespace yams::vector {

namespace {

std::filesystem::path vectorsDbPath(const std::filesystem::path& dataDir) {
    return dataDir / "vectors.db";
}

Result<void> openBackend(SqliteVecBackend& backend, const std::filesystem::path& dbPath) {
    auto open = backend.initialize(dbPath.string());
    if (!open)
        return Error{ErrorCode::DatabaseError, open.error().message};
    (void)backend.ensureVecLoaded();
    return Result<void>();
}

// Drop and recreate every vector table on an open backend; the caller closes it.
Result<void> dropAndCreateTables(SqliteVecBackend& backend, std::size_t dim) {
    if (auto dropped = backend.dropTables(); !dropped)
        return dropped;
    return backend.createTables(dim);
}

Error sqliteError(sqlite3* db, const std::string& action) {
    return Error{ErrorCode::DatabaseError, action + ": " + sqlite3_errmsg(db)};
}

// Run a single-value boolean query (`SELECT EXISTS(...)`), binding `dim` to every `?1`.
// Any prepare or step result other than one row followed by SQLITE_DONE is an error.
Result<bool> queryFlag(sqlite3* db, const char* sql, std::optional<std::size_t> dim) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        auto err =
            sqliteError(db, std::string("Failed to prepare vector schema probe '") + sql + "'");
        sqlite3_finalize(stmt);
        return err;
    }
    if (dim)
        sqlite3_bind_int64(stmt, 1, static_cast<sqlite3_int64>(*dim));

    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        auto err = sqliteError(db, std::string("Vector schema probe '") + sql + "' failed");
        sqlite3_finalize(stmt);
        return err;
    }
    const bool flag = sqlite3_column_int64(stmt, 0) != 0;
    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        auto err = sqliteError(db, std::string("Vector schema probe '") + sql + "' failed");
        sqlite3_finalize(stmt);
        return err;
    }
    sqlite3_finalize(stmt);
    return flag;
}

Result<bool> tableExists(sqlite3* db, const char* table) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db,
                           "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' "
                           "AND name = ?1)",
                           -1, &stmt, nullptr) != SQLITE_OK) {
        auto err = sqliteError(db, "Failed to prepare vector table lookup");
        sqlite3_finalize(stmt);
        return err;
    }
    sqlite3_bind_text(stmt, 1, table, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    const bool present = rc == SQLITE_ROW && sqlite3_column_int64(stmt, 0) != 0;
    if (rc == SQLITE_ROW)
        rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE)
        return sqliteError(db, std::string("Vector table lookup for '") + table + "' failed");
    return present;
}

// What the vector tables currently hold, relative to the target dimension.
struct SchemaState {
    bool vectorsHasRows = false; // `vectors` exists and holds at least one row
    VectorTableGroups stale;     // groups with a row at a dimension other than the target
};

// Early-exit probes: only "any rows?" and "any row at another dimension?" are asked, so a
// matching corpus costs an index lookup on idx_vectors_embedding_dim rather than a full scan.
// entity_vectors has no dimension column or index; its probe stops at the first mismatch.
// The backend is private to the calling function, so reading through its handle is unshared.
Result<SchemaState> probeSchemaState(SqliteVecBackend& backend, std::size_t targetDim) {
    SchemaState state;
    sqlite3* db = backend.getDbHandle();
    if (db == nullptr)
        return Error{ErrorCode::NotInitialized, "Vector database not initialized"};

    auto hasVectors = tableExists(db, "vectors");
    if (!hasVectors)
        return hasVectors.error();
    if (hasVectors.value()) {
        auto rows = queryFlag(db, "SELECT EXISTS(SELECT 1 FROM vectors)", std::nullopt);
        if (!rows)
            return rows.error();
        state.vectorsHasRows = rows.value();

        auto stale = queryFlag(db,
                               "SELECT EXISTS(SELECT 1 FROM vectors WHERE embedding_dim > 0 "
                               "AND embedding_dim != ?1) OR "
                               "EXISTS(SELECT 1 FROM vectors WHERE embedding_dim IS NULL "
                               "AND LENGTH(embedding) / 4 > 0 AND LENGTH(embedding) / 4 != ?1)",
                               targetDim);
        if (!stale)
            return stale.error();
        state.stale.vectors = stale.value();
    }

    auto hasEntity = tableExists(db, "entity_vectors");
    if (!hasEntity)
        return hasEntity.error();
    if (hasEntity.value()) {
        auto stale = queryFlag(db,
                               "SELECT EXISTS(SELECT 1 FROM entity_vectors WHERE "
                               "LENGTH(embedding) / 4 > 0 AND LENGTH(embedding) / 4 != ?1)",
                               targetDim);
        if (!stale)
            return stale.error();
        state.stale.entityVectors = stale.value();
    }

    // The PQ index is derived from `vectors` rows (codes are keyed by its rowid), so it goes
    // whenever `vectors` does.
    state.stale.simeonPq = state.stale.vectors;
    for (const char* table : {"simeon_pq_meta", "simeon_pq_codes"}) {
        if (state.stale.simeonPq)
            break;
        auto present = tableExists(db, table);
        if (!present)
            return present.error();
        if (!present.value())
            continue;
        const std::string sql =
            std::string("SELECT EXISTS(SELECT 1 FROM ") + table + " WHERE dim != ?1)";
        auto stale = queryFlag(db, sql.c_str(), targetDim);
        if (!stale)
            return stale.error();
        state.stale.simeonPq = stale.value();
    }
    return state;
}

} // namespace

Result<void> ensureVectorSchemaAligned(const std::filesystem::path& dataDir, std::size_t targetDim,
                                       bool recreateIfMismatch) {
    try {
        const auto dbPath = vectorsDbPath(dataDir);
        std::error_code ec;
        std::filesystem::create_directories(dbPath.parent_path(), ec);

        SqliteVecBackend backend;
        if (auto open = openBackend(backend, dbPath); !open)
            return open;

        auto state = probeSchemaState(backend, targetDim);
        if (!state) {
            backend.close();
            return state.error();
        }

        Result<void> result;
        // Missing or empty `vectors`: (re)create the tables at the target, whatever the state of
        // the other groups. CREATE IF NOT EXISTS keeps existing tables and their rows.
        bool create = !state.value().vectorsHasRows;
        if (recreateIfMismatch && state.value().stale.any()) {
            result = backend.dropTableGroups(state.value().stale);
            create = true;
        }
        if (result && create)
            result = backend.createTables(targetDim);
        backend.close();
        return result;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

Result<void> recreateVectorTables(const std::filesystem::path& dataDir, std::size_t dim) {
    try {
        SqliteVecBackend backend;
        if (auto open = openBackend(backend, vectorsDbPath(dataDir)); !open)
            return open;

        auto result = dropAndCreateTables(backend, dim);
        backend.close();
        return result;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

} // namespace yams::vector
