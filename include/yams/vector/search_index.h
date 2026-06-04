#pragma once

#include <yams/core/types.h>

namespace yams::vector {

/**
 * @brief Search-index lifecycle independent of data CRUD.
 *
 * Separated from IVectorBackend so callers that only need index maintenance
 * (e.g., daemon startup, health checks) can depend on this contract alone.
 */
class ISearchIndex {
public:
    virtual ~ISearchIndex() = default;

    /// Build or rebuild the search index from stored vectors.
    virtual Result<void> buildIndex() = 0;

    /// Ensure a persisted or in-memory search index is ready for queries.
    virtual Result<void> prepareSearchIndex() = 0;

    /// Check whether a reusable persisted search index already exists.
    virtual Result<bool> hasReusablePersistedSearchIndex() = 0;

    /// Optimize storage and indices (compaction, vacuum, etc.).
    virtual Result<void> optimize() = 0;

    /// Persist an already-prepared search index.
    virtual Result<void> persistIndex() = 0;
};

} // namespace yams::vector
