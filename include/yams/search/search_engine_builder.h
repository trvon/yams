#pragma once

#include <yams/core/types.h>
#include <yams/search/hybrid_search_engine.h>

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace yams {
namespace metadata {
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace metadata

namespace vector {
class VectorIndexManager;
class EmbeddingGenerator;
} // namespace vector
} // namespace yams

namespace yams::search {

/**
 * MetadataKeywordAdapter
 *
 * Lightweight keyword engine that delegates keyword search to the MetadataRepository
 * (FTS5-backed) instead of maintaining a separate in-memory inverted index.
 *
 * Notes:
 * - This adapter is intended for production usage where the SQLite FTS index is the
 *   source of truth for keyword search.
 * - Vector fusion is still handled by HybridSearchEngine using VectorIndexManager.
 *
 * Implementation is provided in the corresponding .cpp.
 */
class MetadataKeywordAdapter final : public KeywordSearchEngine {
public:
    explicit MetadataKeywordAdapter(std::shared_ptr<yams::metadata::MetadataRepository> repo);
    ~MetadataKeywordAdapter() override = default;

    // Search interface
    Result<std::vector<KeywordSearchResult>>
    search(const std::string& query, size_t k,
           const yams::vector::SearchFilter* filter = nullptr) override;

    Result<std::vector<std::vector<KeywordSearchResult>>>
    batchSearch(const std::vector<std::string>& queries, size_t k,
                const yams::vector::SearchFilter* filter = nullptr) override;

    // Index management ops are no-ops for the adapter (backed by DB index)
    Result<void> addDocument(const std::string& id, const std::string& content,
                             const std::map<std::string, std::string>& metadata = {}) override;

    Result<void> removeDocument(const std::string& id) override;

    Result<void> updateDocument(const std::string& id, const std::string& content,
                                const std::map<std::string, std::string>& metadata = {}) override;

    Result<void>
    addDocuments(const std::vector<std::string>& ids, const std::vector<std::string>& contents,
                 const std::vector<std::map<std::string, std::string>>& metadata = {}) override;

    Result<void> buildIndex() override;
    Result<void> optimizeIndex() override;
    Result<void> clearIndex() override;
    Result<void> saveIndex(const std::string& path) override;
    Result<void> loadIndex(const std::string& path) override;

    // Basic stats (delegated/estimated)
    size_t getDocumentCount() const override;
    size_t getTermCount() const override;
    size_t getIndexSize() const override;

    // Analysis helpers (simple tokenization fallback)
    std::vector<std::string> analyzeQuery(const std::string& query) const override;
    std::vector<std::string> extractKeywords(const std::string& text) const override;

private:
    std::shared_ptr<yams::metadata::MetadataRepository> repo_;
};

/**
 * SearchEngineBuilder
 *
 * Central builder for composing a HybridSearchEngine in a single place so both CLI
 * and MCP server can share composition logic. Handles:
 * - Wiring VectorIndexManager + MetadataKeywordAdapter
 * - Optional KG scorer injection when a KnowledgeGraphStore is available
 * - Conservative, enabled-by-default KG settings
 *
 * Remote/hybrid failover is scoped via Mode and RemoteConfig (client implementation
 * can be provided separately).
 */
class SearchEngineBuilder {
public:
    enum class Mode {
        Embedded, // Always use in-process HybridSearchEngine
        Remote,   // Always call remote search service
        Hybrid    // Prefer embedded; fail over to remote
    };

    struct RemoteConfig {
        std::string base_url = "http://127.0.0.1:8081";
        std::string health_path = "/health";
        std::chrono::milliseconds timeout{1500};
        bool enable_failover = true;
    };

    struct BuildOptions {
        Mode mode = Mode::Embedded;
        RemoteConfig remote{};

        // HybridSearch configuration with conservative KG defaults enabled
        HybridSearchConfig hybrid{};
        // Convenience: default-initialize to tuned conservative config
        static BuildOptions makeDefault() {
            BuildOptions o{};
            // Enable knowledge graph by default while preserving the original conservative
            // weight distribution so existing relevance tests remain stable. Users wanting
            // stronger KG influence can adjust weights via config (search.hybrid.*).
            o.hybrid.vector_weight = 0.40f;
            o.hybrid.keyword_weight = 0.60f;
            o.hybrid.kg_entity_weight = 0.0f;  // Keep zero so enabling KG does not change scores
            o.hybrid.structural_weight = 0.0f; // Structural prior off by default (weight=0)

            // Formerly false — now true so hybrid search will attempt to attach a KG scorer
            // when a KnowledgeGraphStore is available. With zero KG weights this is a no-op
            // for scoring until the user increases kg_entity_weight or structural_weight.
            o.hybrid.enable_kg = true;
            o.hybrid.kg_max_neighbors = 32;
            o.hybrid.kg_max_hops = 1;
            o.hybrid.kg_budget_ms = std::chrono::milliseconds{20};
            o.hybrid.generate_explanations = true;

            // Enable parallel search for better performance
            o.hybrid.parallel_search = true;
            o.hybrid.num_threads = 0; // Auto-detect optimal thread count

            // Normalize to ensure weights sum to 1.0
            o.hybrid.normalizeWeights();
            return o;
        }
    };

public:
    SearchEngineBuilder() = default;

    SearchEngineBuilder& withVectorIndex(std::shared_ptr<yams::vector::VectorIndexManager> vim) {
        vectorIndex_ = std::move(vim);
        return *this;
    }

    SearchEngineBuilder&
    withMetadataRepo(std::shared_ptr<yams::metadata::MetadataRepository> repo) {
        metadataRepo_ = std::move(repo);
        return *this;
    }

    SearchEngineBuilder& withKGStore(std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg) {
        kgStore_ = std::move(kg);
        return *this;
    }

    SearchEngineBuilder&
    withEmbeddingGenerator(std::shared_ptr<yams::vector::EmbeddingGenerator> gen) {
        embeddingGenerator_ = std::move(gen);
        return *this;
    }

    // Build embedded engine only (no remote fallback)
    Result<std::shared_ptr<HybridSearchEngine>>
    buildEmbedded(const BuildOptions& options = BuildOptions::makeDefault());

    // Build according to requested mode. For Remote/Hybrid, returns an embedded engine when
    // possible. A remote client/facade can be layered on top by the caller if desired.
    Result<std::shared_ptr<HybridSearchEngine>>
    buildWithMode(const BuildOptions& options = BuildOptions::makeDefault()) {
        // For now, builder returns the embedded engine even in Hybrid/Remote modes.
        // The remote facade/client is composed at a higher layer.
        return buildEmbedded(options);
    }

private:
    // Creates a KeywordSearchEngine backed by MetadataRepository (FTS)
    Result<std::shared_ptr<KeywordSearchEngine>> makeKeywordEngine();

    // Optionally creates a KG scorer bound to the provided store, honoring hybrid config
    Result<std::shared_ptr<KGScorer>> makeKGScorerIfEnabled(const HybridSearchConfig& cfg);

private:
    std::shared_ptr<yams::vector::VectorIndexManager> vectorIndex_;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<yams::vector::EmbeddingGenerator> embeddingGenerator_;
};

} // namespace yams::search
