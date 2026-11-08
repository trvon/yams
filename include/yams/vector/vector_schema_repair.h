#pragma once

#include <cstddef>
#include <filesystem>

#include <yams/core/types.h>

namespace yams::vector {

/// Ensure `<dataDir>/vectors.db` holds vector tables for `targetDim`. Always creates `dataDir`.
///
/// Dimensions are probed from rows, not from the schema: every distinct dimension recorded in
/// `vectors` (embedding_dim, else blob length), `entity_vectors` (blob length) and the
/// `simeon_pq_meta` / `simeon_pq_codes` tables (their `dim` column).
///
/// Tables are judged per group: `vectors` (with its derived vec0 / legacy HNSW tables),
/// `entity_vectors`, and `simeon_pq_meta` + `simeon_pq_codes` as one unit. A group is stale when
/// any of its rows is at a dimension other than `targetDim`; the PQ group is also stale whenever
/// `vectors` is, since its codes are derived from `vectors` rows.
///
/// - Some group stale: with `recreateIfMismatch` only the stale groups are dropped, then the
///   missing tables are recreated empty at `targetDim`; groups with matching rows or no rows keep
///   their rows. Without it the database is left untouched, including rows at other
///   dimensions, and the call still succeeds.
/// - No mismatch and `vectors` has no rows (or does not exist): creates the tables
///   at `targetDim` (CREATE IF NOT EXISTS, so existing rows are kept).
/// - No mismatch and `vectors` has rows: no-op.
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
