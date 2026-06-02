#pragma once

#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/vector/vector_types.h>

namespace yams::vector {

/**
 * @brief Narrow interface for entity (symbol/function/class) vector storage.
 *
 * Entity vectors are a separate concern from document vectors; this contract
 * lets callers (entity graph, symbol extractors) depend only on the operations
 * they need.
 */
class IEntityStore {
public:
    virtual ~IEntityStore() = default;

    virtual Result<void> insertEntityVector(const EntityVectorRecord& record) = 0;
    virtual Result<void>
    insertEntityVectorsBatch(const std::vector<EntityVectorRecord>& records) = 0;
    virtual Result<void> deleteEntityVectorsByNode(const std::string& node_key) = 0;
    virtual Result<void> deleteEntityVectorsByDocument(const std::string& document_hash) = 0;

    virtual Result<std::vector<EntityVectorRecord>>
    searchEntities(const std::vector<float>& query_embedding,
                   const EntitySearchParams& params = {}) = 0;

    virtual Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByNode(const std::string& node_key) = 0;
    virtual Result<std::vector<EntityVectorRecord>>
    getEntityVectorsByDocument(const std::string& document_hash) = 0;

    virtual Result<bool> hasEntityEmbedding(const std::string& node_key) = 0;
    virtual Result<size_t> getEntityVectorCount() = 0;
    virtual Result<void> markEntityAsStale(const std::string& node_key) = 0;
};

} // namespace yams::vector
