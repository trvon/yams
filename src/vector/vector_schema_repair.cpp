#include <yams/vector/vector_schema_repair.h>

#include <exception>
#include <system_error>

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

        Result<void> result;
        const auto stored = backend.getStoredEmbeddingDimension();
        if (!stored.has_value()) {
            result = backend.createTables(targetDim);
        } else if (*stored != targetDim && recreateIfMismatch) {
            result = dropAndCreateTables(backend, targetDim);
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
