#include <yams/vector/vector_schema_repair.h>

#include <cstdint>
#include <exception>
#include <set>
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

// Drop and recreate the vector tables on an open backend; the caller closes it.
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

// Every embedding dimension recorded by rows of the vector tables.
struct StoredDimensions {
    bool vectorsHasRows = false; // `vectors` holds at least one row with a dimension
    std::set<std::size_t> all;   // dimensions across vectors, entity_vectors and simeon_pq_*
};

StoredDimensions probeStoredDimensions(SqliteVecBackend& backend) {
    StoredDimensions stored;
    sqlite3* db = backend.getDbHandle();

    std::set<std::size_t> vectorsDims;
    const bool probed =
        db != nullptr &&
        collectDimensions(db,
                          "SELECT DISTINCT COALESCE(embedding_dim, LENGTH(embedding) / 4) "
                          "FROM vectors",
                          vectorsDims);
    if (!probed) {
        // Schema without a readable embedding_dim column: fall back to the backend's probe.
        if (auto dim = backend.getStoredEmbeddingDimension())
            vectorsDims.insert(*dim);
    }
    stored.vectorsHasRows = !vectorsDims.empty();
    stored.all = vectorsDims;

    if (db != nullptr) {
        (void)collectDimensions(db, "SELECT DISTINCT LENGTH(embedding) / 4 FROM entity_vectors",
                                stored.all);
        (void)collectDimensions(db, "SELECT DISTINCT dim FROM simeon_pq_meta", stored.all);
        (void)collectDimensions(db, "SELECT DISTINCT dim FROM simeon_pq_codes", stored.all);
    }
    return stored;
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
        bool mismatch = false;
        for (auto dim : stored.all) {
            if (dim != targetDim) {
                mismatch = true;
                break;
            }
        }

        Result<void> result;
        if (mismatch) {
            if (recreateIfMismatch)
                result = dropAndCreateTables(backend, targetDim);
        } else if (!stored.vectorsHasRows) {
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
