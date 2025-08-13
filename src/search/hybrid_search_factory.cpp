#include <yams/search/hybrid_search_factory.h>

#include <yams/core/types.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/kg_scorer.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <memory>
#include <utility>

namespace yams::search {

// Forward declaration for the simple scorer factory implemented in the KG scorer TU.
std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store);

HybridSearchConfig HybridSearchFactory::defaultConfig() {
    HybridSearchConfig cfg;

    // Conservative defaults that keep KG contributions small but active by default.
    cfg.vector_weight       = 0.60f;
    cfg.keyword_weight      = 0.35f;
    cfg.kg_entity_weight    = 0.03f;
    cfg.structural_weight   = 0.02f;

    // KG settings (local-first)
    cfg.enable_kg           = true;
    cfg.kg_max_neighbors    = 32;
    cfg.kg_max_hops         = 1;
    cfg.kg_budget_ms        = std::chrono::milliseconds(20);

    // Helpful during bring-up; callers can disable for production.
    cfg.generate_explanations = true;

    // Ensure weights sum to 1.0
    cfg.normalizeWeights();
    return cfg;
}

Result<std::shared_ptr<HybridSearchEngine>> HybridSearchFactory::create(
    std::shared_ptr<vector::VectorIndexManager> vectorIndex,
    std::shared_ptr<KeywordSearchEngine> keywordEngine,
    const HybridSearchConfig& config,
    std::shared_ptr<KGScorer> kgScorer)
{
    if (!vectorIndex || !keywordEngine) {
        return Error{ErrorCode::InvalidArgument, "HybridSearchFactory::create: null engine dependency"};
    }

    // Build engine
    auto engine = std::make_shared<HybridSearchEngine>(
        std::move(vectorIndex),
        std::move(keywordEngine),
        config
    );

    // Attach KG scorer if provided and enabled
    if (config.enable_kg && kgScorer) {
        engine->setKGScorer(std::move(kgScorer));
    }

    // Initialize engine
    auto init = engine->initialize();
    if (!init) {
        return init.error();
    }

    return engine;
}

Result<std::shared_ptr<HybridSearchEngine>> HybridSearchFactory::createWithKGStore(
    std::shared_ptr<vector::VectorIndexManager> vectorIndex,
    std::shared_ptr<KeywordSearchEngine> keywordEngine,
    const HybridSearchConfig& config,
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore)
{
    if (!kgStore && config.enable_kg) {
        // If KG is requested but no store, surface a clear error.
        return Error{ErrorCode::InvalidArgument, "HybridSearchFactory::createWithKGStore: null KG store"};
    }

    std::shared_ptr<KGScorer> scorer;
    if (config.enable_kg && kgStore) {
        scorer = makeSimpleKGScorer(std::move(kgStore));
    }

    return create(std::move(vectorIndex), std::move(keywordEngine), config, std::move(scorer));
}

} // namespace yams::search