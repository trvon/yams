#include <yams/vector/vector_schema_repair.h>

#include <cstdint>
#include <exception>
#include <set>
#include <string>
#include <system_error>
#include <vector>

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

// Collect the positive values of a single-column query. Returns false when the statement cannot
// be prepared (missing table or column), leaving `out` untouched.
bool collectDimensions(sqlite3* db, const char* sql, std::set<std::size_t>& out) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        sqlite3_finalize(stmt);
        return false;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const std::int64_t dim = sqlite3_column_int64(stmt, 0);
        if (dim > 0)
            out.insert(static_cast<std::size_t>(dim));
    }
    sqlite3_finalize(stmt);
    return true;
}

// Embedding dimensions recorded by rows, per table group.
struct StoredDimensions {
    std::set<std::size_t> vectors; // `vectors`
    std::set<std::size_t> entity;  // `entity_vectors`
    std::set<std::size_t> pq;      // `simeon_pq_meta` + `simeon_pq_codes`
};

StoredDimensions probeStoredDimensions(SqliteVecBackend& backend) {
    StoredDimensions stored;
    sqlite3* db = backend.getDbHandle();

    const bool probed =
        db != nullptr &&
        collectDimensions(db,
                          "SELECT DISTINCT COALESCE(embedding_dim, LENGTH(embedding) / 4) "
                          "FROM vectors",
                          stored.vectors);
    if (!probed) {
        // Schema without a readable embedding_dim column: fall back to the backend's probe.
        if (auto dim = backend.getStoredEmbeddingDimension())
            stored.vectors.insert(*dim);
    }

    if (db != nullptr) {
        (void)collectDimensions(db, "SELECT DISTINCT LENGTH(embedding) / 4 FROM entity_vectors",
                                stored.entity);
        (void)collectDimensions(db, "SELECT DISTINCT dim FROM simeon_pq_meta", stored.pq);
        (void)collectDimensions(db, "SELECT DISTINCT dim FROM simeon_pq_codes", stored.pq);
    }
    return stored;
}

bool hasOtherDimension(const std::set<std::size_t>& dims, std::size_t targetDim) {
    for (auto dim : dims) {
        if (dim != targetDim)
            return true;
    }
    return false;
}

// Table groups that hold rows at a dimension other than the target.
struct StaleGroups {
    bool vectors = false;
    bool entity = false;
    bool pq = false;

    bool any() const { return vectors || entity || pq; }
};

StaleGroups findStaleGroups(const StoredDimensions& stored, std::size_t targetDim) {
    StaleGroups stale;
    stale.vectors = hasOtherDimension(stored.vectors, targetDim);
    stale.entity = hasOtherDimension(stored.entity, targetDim);
    // The PQ index is derived from `vectors` rows (codes are keyed by its rowid), so it goes
    // whenever `vectors` does.
    stale.pq = stale.vectors || hasOtherDimension(stored.pq, targetDim);
    return stale;
}

Result<void> execSql(sqlite3* db, const std::string& sql) {
    char* errMsg = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &errMsg) != SQLITE_OK) {
        std::string err = errMsg ? errMsg : "Unknown error";
        sqlite3_free(errMsg);
        return Error{ErrorCode::DatabaseError, err};
    }
    return Result<void>();
}

// Tables derived from `vectors`: per-dimension vec0 virtual tables and legacy HNSW shadow
// tables. Mirrors the selection in SqliteVecBackend::dropTables.
std::vector<std::string> vectorsDerivedTables(sqlite3* db) {
    std::vector<std::string> names;
    const char* sql =
        "SELECT name FROM sqlite_master WHERE type='table' AND "
        "(name LIKE 'vectors_%_hnsw_meta' OR name LIKE 'vectors_%_hnsw_nodes' OR "
        " name = 'vectors_hnsw_meta' OR name = 'vectors_hnsw_nodes' OR "
        " (name LIKE 'vectors\\_%\\_vec0' ESCAPE '\\' AND sql LIKE 'CREATE VIRTUAL TABLE%'))";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (const auto* name = sqlite3_column_text(stmt, 0))
                names.emplace_back(reinterpret_cast<const char*>(name));
        }
    }
    sqlite3_finalize(stmt);
    return names;
}

// Drop only the stale table groups in one transaction, then recreate the dropped tables at
// `targetDim` (CREATE IF NOT EXISTS keeps the rest). The caller closes the backend.
Result<void> dropStaleAndCreateTables(SqliteVecBackend& backend, const StaleGroups& stale,
                                      std::size_t targetDim) {
    sqlite3* db = backend.getDbHandle();
    if (db == nullptr)
        return Error{ErrorCode::NotInitialized, "Database not initialized"};

    std::vector<std::string> drops;
    if (stale.vectors) {
        for (const auto& name : vectorsDerivedTables(db))
            drops.push_back("DROP TABLE IF EXISTS \"" + name + "\"");
        drops.emplace_back("DROP TABLE IF EXISTS vectors");
    }
    if (stale.entity)
        drops.emplace_back("DROP TABLE IF EXISTS entity_vectors");
    if (stale.pq) {
        drops.emplace_back("DROP TABLE IF EXISTS simeon_pq_meta");
        drops.emplace_back("DROP TABLE IF EXISTS simeon_pq_codes");
    }

    if (auto begin = execSql(db, "BEGIN IMMEDIATE"); !begin)
        return Error{ErrorCode::DatabaseError,
                     "Failed to begin drop transaction: " + begin.error().message};
    for (const auto& sql : drops) {
        if (auto dropped = execSql(db, sql); !dropped) {
            (void)execSql(db, "ROLLBACK");
            return Error{ErrorCode::DatabaseError,
                         "Failed to drop tables: " + dropped.error().message};
        }
    }
    if (auto commit = execSql(db, "COMMIT"); !commit) {
        (void)execSql(db, "ROLLBACK");
        return Error{ErrorCode::DatabaseError,
                     "Failed to commit table drop: " + commit.error().message};
    }
    return backend.createTables(targetDim);
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

        const auto stored = probeStoredDimensions(backend);
        const auto stale = findStaleGroups(stored, targetDim);

        Result<void> result;
        if (stale.any()) {
            if (recreateIfMismatch)
                result = dropStaleAndCreateTables(backend, stale, targetDim);
        } else if (stored.vectors.empty()) {
            result = backend.createTables(targetDim);
        }
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
