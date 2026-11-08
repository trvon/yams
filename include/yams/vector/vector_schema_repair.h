#pragma once

#include <cstddef>
#include <filesystem>

#include <yams/core/types.h>

namespace yams::vector {

/// Ensure `<dataDir>/vectors.db` holds vector tables for `targetDim`.
///
/// - Creates `dataDir` and the tables when the database or its tables are missing.
/// - The stored dimension is probed from existing rows, so an empty `vectors` table counts as
///   missing and is (idempotently) created at `targetDim`.
/// - When rows exist at a different dimension: with `recreateIfMismatch` the tables are dropped
///   and recreated empty at `targetDim`; without it the database is left untouched and the call
///   still succeeds.
/// - A matching dimension is a no-op.
///
/// Errors: DatabaseError when the database cannot be opened, the backend's error when dropping
/// or creating tables fails, InternalError when an exception escapes the backend.
Result<void> ensureVectorSchemaAligned(const std::filesystem::path& dataDir, std::size_t targetDim,
                                       bool recreateIfMismatch);

/// Unconditionally drop every vector table in `<dataDir>/vectors.db` and recreate them empty at
/// `dim` (embedding-space migration). Existing vectors are discarded even when the stored
/// dimension already equals `dim`. Does not create `dataDir`.
Result<void> recreateVectorTables(const std::filesystem::path& dataDir, std::size_t dim);

} // namespace yams::vector
