#pragma once

#include <memory>

#include <yams/core/types.h>
#include <yams/search/hybrid_search_engine.h>

namespace yams {
namespace metadata {
class KnowledgeGraphStore; // forward declaration to avoid heavy includes
} // namespace metadata
} // namespace yams

namespace yams::search {

/**
 * Factory for constructing a default-configured HybridSearchEngine.
 *
 * Notes:
 * - This factory only wires components; it does not own the underlying indices or stores.
 * - If a KG scorer is provided (or a KG store variant is used), the engine will be configured
 *   to use KG joint scoring according to the supplied HybridSearchConfig.
 * - Implementations should call initialize() on the engine before returning.
 */
class HybridSearchFactory {
public:
    /**
     * Get a reasonable default HybridSearchConfig with KG enabled and conservative weights.
     * Callers may adjust as needed and then pass to create(...).
     *
     * Defaults (subject to change as we tune):
     *   vector_weight        = 0.60
     *   keyword_weight       = 0.35
     *   kg_entity_weight     = 0.03
     *   structural_weight    = 0.02
     *   enable_kg            = true
     *   kg_max_neighbors     = 32
     *   kg_max_hops          = 1
     *   kg_budget_ms         = 20
     *   generate_explanations = true
     */
    static HybridSearchConfig defaultConfig();

    /**
     * Build a HybridSearchEngine with provided vector and keyword engines.
     * Optionally attaches a pre-constructed KG scorer.
     *
     * The returned engine will be initialized() on success.
     */
    static Result<std::shared_ptr<HybridSearchEngine>> create(
        std::shared_ptr<vector::VectorIndexManager> vectorIndex,
        std::shared_ptr<KeywordSearchEngine> keywordEngine,
        const HybridSearchConfig& config,
        std::shared_ptr<KGScorer> kgScorer = nullptr);

    /**
     * Build a HybridSearchEngine and attach a default KG scorer backed by the given KG store.
     * Implementations may use a simple local-first scorer for entity overlap + 1-hop prior.
     *
     * The returned engine will be initialized() on success.
     */
    static Result<std::shared_ptr<HybridSearchEngine>> createWithKGStore(
        std::shared_ptr<vector::VectorIndexManager> vectorIndex,
        std::shared_ptr<KeywordSearchEngine> keywordEngine,
        const HybridSearchConfig& config,
        std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore);
};

} // namespace yams::search