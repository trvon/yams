#pragma once

#include <filesystem>
#include <yams/core/types.h>
#include <yams/vector/sqlite_vec_backend.h>

namespace yams::integrity {

// Ensure vectors.db schema matches targetDim. If DB missing, creates tables.
// When recreateIfMismatch=true and stored dimension differs, drops and recreates tables.
// Best-effort; header-only inline to avoid extra link deps.
inline Result<void> ensureVectorSchemaAligned(const std::filesystem::path& dataDir,
                                              std::size_t targetDim, bool recreateIfMismatch) {
    try {
        namespace fs = std::filesystem;
        fs::path dbPath = dataDir / "vectors.db";
        std::error_code ec;
        fs::create_directories(dbPath.parent_path(), ec);
        yams::vector::SqliteVecBackend be;
        auto open = be.initialize(dbPath.string());
        if (!open)
            return Error{ErrorCode::DatabaseError, open.error().message};
        (void)be.ensureVecLoaded();
        auto stored = be.getStoredEmbeddingDimension();
        if (!stored.has_value()) {
            auto cr = be.createTables(targetDim);
            be.close();
            return cr;
        }
        if (*stored != targetDim && recreateIfMismatch) {
            auto dr = be.dropTables();
            if (!dr) {
                be.close();
                return dr;
            }
            auto cr = be.createTables(targetDim);
            be.close();
            return cr;
        }
        be.close();
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

} // namespace yams::integrity
