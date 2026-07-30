#pragma once

#include <yams/vector/entity_store.h>
#include <yams/vector/search_index.h>
#include <yams/vector/vector_store.h>
#include <yams/vector/vector_types.h>

namespace yams::vector {

/**
 * @brief Composite interface combining all vector-backend narrow contracts.
 *
 * Inherits from IVectorStore (CRUD+search), ISearchIndex (index lifecycle),
 * and IEntityStore (entity vectors).
 *
 * New code should prefer the narrow contracts directly. This composite exists
 * for backward compatibility.
 */
class IVectorBackend : public IVectorStore, public ISearchIndex, public IEntityStore {
public:
    ~IVectorBackend() override = default;
};

} // namespace yams::vector
