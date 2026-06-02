#pragma once

#include <functional>
#include <yams/core/types.h>
#include <yams/vector/search_index.h>

namespace yams::vector {

class ISearchIndex;

/**
 * @brief Focused manager for search-index lifecycle, separated from vector CRUD.
 *
 * Wraps a non-owning ISearchIndex and exposes only the index operations:
 * build, prepare, persist, check-reusable, optimize. Callers that only need to
 * manage the search index (daemon startup, health checks, compaction) can depend
 * on this contract without linking to CRUD, entity, or quantizer concerns.
 */
class VectorIndexManager {
public:
    /// Construct from an existing non-owning index facade.
    explicit VectorIndexManager(
        ISearchIndex& index, std::function<bool()> isInitialized = [] { return true; });

    VectorIndexManager(const VectorIndexManager&) = delete;
    VectorIndexManager& operator=(const VectorIndexManager&) = delete;
    VectorIndexManager(VectorIndexManager&&) noexcept = default;
    VectorIndexManager& operator=(VectorIndexManager&&) noexcept = default;

    /// Build or rebuild the search index.
    Result<void> buildIndex();

    /// Ensure a persisted or in-memory search index is ready.
    Result<void> prepareSearchIndex();

    /// Check whether a reusable persisted index exists.
    Result<bool> hasReusablePersistedSearchIndex();

    /// Optimize storage and indices.
    Result<void> optimize();

    /// Persist an already-prepared search index.
    Result<void> persistIndex();

    /// Check whether the backend is initialized.
    bool isInitialized() const;

private:
    ISearchIndex* index_ = nullptr;
    std::function<bool()> isInitialized_;
};

} // namespace yams::vector
