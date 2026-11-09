#pragma once

#include <cstddef>
#include <filesystem>

#include <yams/core/types.h>

namespace yams::vector {

/// Ensure `<dataDir>/vectors.db` holds vector tables for `targetDim`. Always creates `dataDir`.
///
/// Dimensions are probed from rows, not from the schema: `vectors` (embedding_dim, else blob
/// length), `entity_vectors` (blob length) and `simeon_pq_meta` / `simeon_pq_codes` (`dim`).
/// Tables are judged per group (see VectorTableGroups): `vectors` with its derived tables,
/// `entity_vectors`, and the two Simeon PQ tables as one unit. A group is stale when any of its
/// rows is at a dimension other than `targetDim`; the PQ group is also stale whenever `vectors`
/// is, since its codes are derived from `vectors` rows.
///
/// - Stale groups and `recreateIfMismatch`: only the stale groups are dropped (bumping the
///   vector index generation when `vectors` goes), then the tables are recreated at
///   `targetDim`. Groups with matching rows or no rows keep their rows.
/// - Stale groups without `recreateIfMismatch`: nothing is dropped, rows at other dimensions
///   stay, and the call still succeeds.
/// - `vectors` missing or empty: the tables are created at `targetDim` regardless of the other
///   groups (CREATE IF NOT EXISTS, plus the persistence schema), so existing rows are kept.
/// - Otherwise (no stale group, `vectors` has rows): no-op.
///
/// Errors: DatabaseError when the database cannot be opened, a probe fails to prepare or step
/// (e.g. busy, I/O error, corruption, unexpected schema), or dropping or creating tables fails;
/// InternalError when an exception escapes the backend.
Result<void> ensureVectorSchemaAligned(const std::filesystem::path& dataDir, std::size_t targetDim,
                                       bool recreateIfMismatch);

/// Unconditionally drop every vector table in `<dataDir>/vectors.db` and recreate them empty at
/// `dim` (embedding-space migration). Existing vectors are discarded even when the stored
/// dimension already equals `dim`. Does not create `dataDir`.
Result<void> recreateVectorTables(const std::filesystem::path& dataDir, std::size_t dim);

} // namespace yams::vector
