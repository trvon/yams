#include <yams/search/hybrid_search_factory.h>

#include <yams/core/types.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/kg_scorer.h>
#include <yams/vector/embedding_generator.h>

#include <spdlog/spdlog.h>
#include <filesystem>
#include <memory>
#include <utility>

namespace yams::search {

// Forward declaration for the simple scorer factory implemented in the KG scorer TU.
std::shared_ptr<KGScorer>
makeSimpleKGScorer(std::shared_ptr<yams::metadata::KnowledgeGraphStore> store);

std::shared_ptr<vector::EmbeddingGenerator> HybridSearchFactory::createDefaultEmbeddingGenerator() {
    namespace fs = std::filesystem;

    // Check for available models in ~/.yams/models
    const char* home = std::getenv("HOME");
    if (!home) {
        spdlog::debug(
            "HOME environment variable not set, cannot create default embedding generator");
        return nullptr;
    }

    fs::path modelsPath = fs::path(home) / ".yams" / "models";
    if (!fs::exists(modelsPath)) {
        spdlog::debug("Models directory does not exist: {}", modelsPath.string());
        return nullptr;
    }

    // Prefer all-MiniLM-L6-v2 for efficiency, fallback to all-mpnet-base-v2
    std::vector<std::string> preferredModels = {"all-MiniLM-L6-v2", "all-mpnet-base-v2"};
    std::string selectedModel;

    for (const auto& modelName : preferredModels) {
        fs::path modelPath = modelsPath / modelName / "model.onnx";
        if (fs::exists(modelPath)) {
            selectedModel = modelName;
            break;
        }
    }

    // If no preferred model, try any available model
    if (selectedModel.empty()) {
        for (const auto& entry : fs::directory_iterator(modelsPath)) {
            if (entry.is_directory()) {
                fs::path modelFile = entry.path() / "model.onnx";
                if (fs::exists(modelFile)) {
                    selectedModel = entry.path().filename().string();
                    break;
                }
            }
        }
    }

    if (selectedModel.empty()) {
        spdlog::debug("No embedding models found in {}", modelsPath.string());
        return nullptr;
    }

    // Create embedding generator with selected model
    vector::EmbeddingConfig config;
    config.model_path = (modelsPath / selectedModel / "model.onnx").string();
    config.model_name = selectedModel;

    // Set dimensions based on model
    if (selectedModel == "all-MiniLM-L6-v2") {
        config.embedding_dim = 384;
    } else if (selectedModel == "all-mpnet-base-v2") {
        config.embedding_dim = 768;
    } else {
        config.embedding_dim = 384; // Default fallback
    }

    auto generator = std::make_shared<vector::EmbeddingGenerator>(config);
    if (generator->initialize()) {
        spdlog::info("Created default embedding generator with model: {}", selectedModel);
        return generator;
    }

    spdlog::warn("Failed to initialize embedding generator with model: {}", selectedModel);
    return nullptr;
}

HybridSearchConfig HybridSearchFactory::defaultConfig() {
    HybridSearchConfig cfg;

    // Balanced defaults with enhanced KG contribution for better entity recognition
    cfg.vector_weight = 0.55f;     // Slightly reduced to make room for KG
    cfg.keyword_weight = 0.30f;    // Still important for exact matches
    cfg.kg_entity_weight = 0.10f;  // Increased from 0.03 for better entity/relationship awareness
    cfg.structural_weight = 0.05f; // Increased for better document structure understanding

    // Enhanced KG settings for better traversal
    cfg.enable_kg = true;
    cfg.kg_max_neighbors = 50;                        // Increased from 32 for broader context
    cfg.kg_max_hops = 2;                              // Increased from 1 for deeper relationships
    cfg.kg_budget_ms = std::chrono::milliseconds(50); // Increased from 20ms for better traversal

    // Helpful during bring-up; callers can disable for production.
    cfg.generate_explanations = true;

    // Ensure weights sum to 1.0
    cfg.normalizeWeights();
    return cfg;
}

Result<std::shared_ptr<HybridSearchEngine>>
HybridSearchFactory::create(std::shared_ptr<vector::VectorIndexManager> vectorIndex,
                            std::shared_ptr<KeywordSearchEngine> keywordEngine,
                            const HybridSearchConfig& config, std::shared_ptr<KGScorer> kgScorer,
                            std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator) {
    if (!vectorIndex || !keywordEngine) {
        return Error{ErrorCode::InvalidArgument,
                     "HybridSearchFactory::create: null engine dependency"};
    }

    // Create default embedding generator if none provided
    if (!embeddingGenerator) {
        embeddingGenerator = createDefaultEmbeddingGenerator();
        // Note: It's OK if this returns nullptr - engine will fallback to zero vectors
    }

    // Build engine with embedding generator
    auto engine = std::make_shared<HybridSearchEngine>(
        std::move(vectorIndex), std::move(keywordEngine), config, std::move(embeddingGenerator));

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
    std::shared_ptr<KeywordSearchEngine> keywordEngine, const HybridSearchConfig& config,
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGenerator) {
    if (!kgStore && config.enable_kg) {
        // If KG is requested but no store, surface a clear error.
        return Error{ErrorCode::InvalidArgument,
                     "HybridSearchFactory::createWithKGStore: null KG store"};
    }

    std::shared_ptr<KGScorer> scorer;
    if (config.enable_kg && kgStore) {
        scorer = makeSimpleKGScorer(std::move(kgStore));
    }

    return create(std::move(vectorIndex), std::move(keywordEngine), config, std::move(scorer),
                  std::move(embeddingGenerator));
}

} // namespace yams::search