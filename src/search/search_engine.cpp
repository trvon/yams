#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>

namespace yams::search {

// ============================================================================
// ComponentQueryExecutor Implementation
// ============================================================================

ComponentQueryExecutor::ComponentQueryExecutor(yams::metadata::ConnectionPool& pool,
                                               const SearchEngineConfig& config)
    : pool_(pool), config_(config) {}

Result<std::vector<ComponentResult>>
ComponentQueryExecutor::executeAll(const std::string& query,
                                   const std::optional<std::vector<float>>& queryEmbedding) {
    // Note: This class needs access to MetadataRepository, not just ConnectionPool
    // This will be passed from SearchEngine::Impl which has the repo reference
    return Error{ErrorCode::NotImplemented,
                 "ComponentQueryExecutor needs refactoring to accept MetadataRepository"};
}

Result<std::vector<ComponentResult>> ComponentQueryExecutor::queryFTS5(const std::string& query) {
    return Error{ErrorCode::NotImplemented, "Query methods moved to SearchEngine::Impl"};
}

Result<std::vector<ComponentResult>>
ComponentQueryExecutor::queryPathTree(const std::string& query) {
    return Error{ErrorCode::NotImplemented, "Query methods moved to SearchEngine::Impl"};
}

Result<std::vector<ComponentResult>>
ComponentQueryExecutor::querySymbols(const std::string& query) {
    return Error{ErrorCode::NotImplemented, "Query methods moved to SearchEngine::Impl"};
}

Result<std::vector<ComponentResult>>
ComponentQueryExecutor::queryKnowledgeGraph(const std::string& query) {
    return Error{ErrorCode::NotImplemented, "Query methods moved to SearchEngine::Impl"};
}

Result<std::vector<ComponentResult>>
ComponentQueryExecutor::queryVectorIndex(const std::vector<float>& embedding) {
    return Error{ErrorCode::NotImplemented, "Query methods moved to SearchEngine::Impl"};
}

// ============================================================================
// ResultFusion Implementation
// ============================================================================

ResultFusion::ResultFusion(const SearchEngineConfig& config) : config_(config) {}

std::vector<SearchResult> ResultFusion::fuse(const std::vector<ComponentResult>& componentResults) {
    if (componentResults.empty()) {
        return {};
    }

    // Dispatch to fusion strategy
    switch (config_.fusionStrategy) {
        case SearchEngineConfig::FusionStrategy::WEIGHTED_SUM:
            return fuseWeightedSum(componentResults);
        case SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK:
            return fuseReciprocalRank(componentResults);
        case SearchEngineConfig::FusionStrategy::BORDA_COUNT:
            return fuseBordaCount(componentResults);
        case SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL:
            return fuseWeightedReciprocal(componentResults);
        default:
            return fuseWeightedReciprocal(componentResults);
    }
}

std::vector<SearchResult>
ResultFusion::fuseWeightedSum(const std::vector<ComponentResult>& results) {
    // Group results by document
    auto grouped = groupByDocument(results);

    // Calculate weighted sum for each document
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(grouped.size());

    for (const auto& [docHash, components] : grouped) {
        SearchResult result;
        result.document.sha256Hash = docHash;
        result.document.filePath = components[0].filePath;
        result.score = 0.0;

        // Sum weighted scores from each component
        for (const auto& comp : components) {
            float weight = getComponentWeight(comp.source);
            result.score += comp.score * weight;

            // Use first available snippet
            if (result.snippet.empty() && comp.snippet.has_value()) {
                result.snippet = comp.snippet.value();
            }
        }

        fusedResults.push_back(std::move(result));
    }

    // Sort by final score (descending)
    std::sort(fusedResults.begin(), fusedResults.end(),
              [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

    // Limit results
    if (fusedResults.size() > config_.maxResults) {
        fusedResults.resize(config_.maxResults);
    }

    return fusedResults;
}

std::vector<SearchResult>
ResultFusion::fuseReciprocalRank(const std::vector<ComponentResult>& results) {
    auto grouped = groupByDocument(results);
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(grouped.size());

    const float k = 60.0f; // RRF constant

    for (const auto& [docHash, components] : grouped) {
        SearchResult result;
        result.document.sha256Hash = docHash;
        result.document.filePath = components[0].filePath;
        result.score = 0.0;

        // Sum RRF scores: 1 / (k + rank)
        for (const auto& comp : components) {
            result.score += 1.0 / (k + static_cast<double>(comp.rank));

            if (result.snippet.empty() && comp.snippet.has_value()) {
                result.snippet = comp.snippet.value();
            }
        }

        fusedResults.push_back(std::move(result));
    }

    std::sort(fusedResults.begin(), fusedResults.end(),
              [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

    if (fusedResults.size() > config_.maxResults) {
        fusedResults.resize(config_.maxResults);
    }

    return fusedResults;
}

std::vector<SearchResult>
ResultFusion::fuseBordaCount(const std::vector<ComponentResult>& results) {
    auto grouped = groupByDocument(results);
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(grouped.size());

    for (const auto& [docHash, components] : grouped) {
        SearchResult result;
        result.document.sha256Hash = docHash;
        result.document.filePath = components[0].filePath;
        result.score = 0.0;

        // Sum Borda points (max_rank - rank) from each component
        for (const auto& comp : components) {
            float weight = getComponentWeight(comp.source);
            result.score += weight * (100.0 - static_cast<double>(comp.rank));

            if (result.snippet.empty() && comp.snippet.has_value()) {
                result.snippet = comp.snippet.value();
            }
        }

        fusedResults.push_back(std::move(result));
    }

    std::sort(fusedResults.begin(), fusedResults.end(),
              [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

    if (fusedResults.size() > config_.maxResults) {
        fusedResults.resize(config_.maxResults);
    }

    return fusedResults;
}

std::vector<SearchResult>
ResultFusion::fuseWeightedReciprocal(const std::vector<ComponentResult>& results) {
    auto grouped = groupByDocument(results);
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(grouped.size());

    const float k = 60.0f;

    for (const auto& [docHash, components] : grouped) {
        SearchResult result;
        result.document.sha256Hash = docHash;
        result.document.filePath = components[0].filePath;
        result.score = 0.0;

        // Weighted RRF: weight * (1 / (k + rank)) + weight * score
        for (const auto& comp : components) {
            float weight = getComponentWeight(comp.source);
            double rrfScore = 1.0 / (k + static_cast<double>(comp.rank));
            result.score += weight * (rrfScore + comp.score) / 2.0;

            if (result.snippet.empty() && comp.snippet.has_value()) {
                result.snippet = comp.snippet.value();
            }
        }

        fusedResults.push_back(std::move(result));
    }

    std::sort(fusedResults.begin(), fusedResults.end(),
              [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

    if (fusedResults.size() > config_.maxResults) {
        fusedResults.resize(config_.maxResults);
    }

    return fusedResults;
}

std::map<std::string, std::vector<ComponentResult>>
ResultFusion::groupByDocument(const std::vector<ComponentResult>& results) const {
    std::map<std::string, std::vector<ComponentResult>> grouped;

    for (const auto& result : results) {
        grouped[result.documentHash].push_back(result);
    }

    return grouped;
}

float ResultFusion::getComponentWeight(const std::string& source) const {
    if (source == "fts5")
        return config_.fts5Weight;
    if (source == "path_tree")
        return config_.pathTreeWeight;
    if (source == "symbol")
        return config_.symbolWeight;
    if (source == "kg")
        return config_.kgWeight;
    if (source == "vector")
        return config_.vectorWeight;
    return 0.0f;
}

// ============================================================================
// SearchEngine::Impl
// ============================================================================

class SearchEngine::Impl {
public:
    Impl(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
         std::shared_ptr<vector::VectorDatabase> vectorDb,
         std::shared_ptr<vector::VectorIndexManager> vectorIndex,
         std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
         std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
         const SearchEngineConfig& config)
        : metadataRepo_(std::move(metadataRepo)), vectorDb_(std::move(vectorDb)),
          vectorIndex_(std::move(vectorIndex)), embeddingGen_(std::move(embeddingGen)),
          kgStore_(std::move(kgStore)), config_(config) {}

    Result<std::vector<SearchResult>> search(const std::string& query, const SearchParams& params);

    void setConfig(const SearchEngineConfig& config) { config_ = config; }

    const SearchEngineConfig& getConfig() const { return config_; }

    const SearchEngine::Statistics& getStatistics() const { return stats_; }

    void resetStatistics() {
        // Reset all atomic counters to zero
        stats_.totalQueries.store(0, std::memory_order_relaxed);
        stats_.successfulQueries.store(0, std::memory_order_relaxed);
        stats_.failedQueries.store(0, std::memory_order_relaxed);

        stats_.fts5Queries.store(0, std::memory_order_relaxed);
        stats_.pathTreeQueries.store(0, std::memory_order_relaxed);
        stats_.symbolQueries.store(0, std::memory_order_relaxed);
        stats_.kgQueries.store(0, std::memory_order_relaxed);
        stats_.vectorQueries.store(0, std::memory_order_relaxed);

        stats_.totalQueryTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgQueryTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgFts5TimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgPathTreeTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgSymbolTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgKgTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgVectorTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgResultsPerQuery.store(0, std::memory_order_relaxed);
        stats_.avgComponentsPerResult.store(0, std::memory_order_relaxed);
    }

    Result<void> healthCheck();

private:
    // Component query methods
    Result<std::vector<ComponentResult>> queryFTS5(const std::string& query);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query);
    Result<std::vector<ComponentResult>> querySymbols(const std::string& query);
    Result<std::vector<ComponentResult>> queryKnowledgeGraph(const std::string& query);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding);

    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
    std::shared_ptr<vector::VectorIndexManager> vectorIndex_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGen_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    SearchEngineConfig config_;
    mutable SearchEngine::Statistics stats_;
};

Result<std::vector<SearchResult>> SearchEngine::Impl::search(const std::string& query,
                                                             const SearchParams& params) {
    auto startTime = std::chrono::steady_clock::now();
    stats_.totalQueries.fetch_add(1, std::memory_order_relaxed);

    // Generate query embedding if vector search is enabled
    std::optional<std::vector<float>> queryEmbedding;
    if (config_.vectorWeight > 0.0f && embeddingGen_) {
        try {
            auto embResult = embeddingGen_->generateEmbedding(query);
            if (!embResult.empty()) {
                queryEmbedding = std::move(embResult);
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to generate query embedding: {}", e.what());
        }
    }

    // Execute component queries (sequential for now, parallel TBD)
    std::vector<ComponentResult> allComponentResults;

    // FTS5 search
    if (config_.fts5Weight > 0.0f) {
        auto fts5Start = std::chrono::steady_clock::now();
        auto fts5Results = queryFTS5(query);
        auto fts5End = std::chrono::steady_clock::now();

        if (fts5Results) {
            allComponentResults.insert(allComponentResults.end(), fts5Results.value().begin(),
                                       fts5Results.value().end());
            stats_.fts5Queries.fetch_add(1, std::memory_order_relaxed);

            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(fts5End - fts5Start).count();
            stats_.avgFts5TimeMicros.store(duration, std::memory_order_relaxed);
        }
    }

    // Knowledge Graph search
    if (config_.kgWeight > 0.0f && kgStore_) {
        auto kgStart = std::chrono::steady_clock::now();
        auto kgResults = queryKnowledgeGraph(query);
        auto kgEnd = std::chrono::steady_clock::now();

        if (kgResults) {
            allComponentResults.insert(allComponentResults.end(), kgResults.value().begin(),
                                       kgResults.value().end());
            stats_.kgQueries.fetch_add(1, std::memory_order_relaxed);

            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(kgEnd - kgStart).count();
            stats_.avgKgTimeMicros.store(duration, std::memory_order_relaxed);
        }
    }

    // Path tree search
    if (config_.pathTreeWeight > 0.0f) {
        auto pathStart = std::chrono::steady_clock::now();
        auto pathResults = queryPathTree(query);
        auto pathEnd = std::chrono::steady_clock::now();

        if (pathResults) {
            allComponentResults.insert(allComponentResults.end(), pathResults.value().begin(),
                                       pathResults.value().end());
            stats_.pathTreeQueries.fetch_add(1, std::memory_order_relaxed);

            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(pathEnd - pathStart).count();
            stats_.avgPathTreeTimeMicros.store(duration, std::memory_order_relaxed);
        }
    }

    // Symbol metadata search
    if (config_.symbolWeight > 0.0f) {
        auto symbolStart = std::chrono::steady_clock::now();
        auto symbolResults = querySymbols(query);
        auto symbolEnd = std::chrono::steady_clock::now();

        if (symbolResults) {
            allComponentResults.insert(allComponentResults.end(), symbolResults.value().begin(),
                                       symbolResults.value().end());
            stats_.symbolQueries.fetch_add(1, std::memory_order_relaxed);

            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(symbolEnd - symbolStart)
                    .count();
            stats_.avgSymbolTimeMicros.store(duration, std::memory_order_relaxed);
        }
    }

    // Vector search (if query embedding was generated)
    if (config_.vectorWeight > 0.0f && queryEmbedding.has_value() && vectorIndex_) {
        auto vectorStart = std::chrono::steady_clock::now();
        auto vectorResults = queryVectorIndex(queryEmbedding.value());
        auto vectorEnd = std::chrono::steady_clock::now();

        if (vectorResults) {
            allComponentResults.insert(allComponentResults.end(), vectorResults.value().begin(),
                                       vectorResults.value().end());
            stats_.vectorQueries.fetch_add(1, std::memory_order_relaxed);

            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(vectorEnd - vectorStart)
                    .count();
            stats_.avgVectorTimeMicros.store(duration, std::memory_order_relaxed);
        }
    }

    // Fuse results
    ResultFusion fusion(config_);
    auto fusedResults = fusion.fuse(allComponentResults);

    // Update statistics
    auto endTime = std::chrono::steady_clock::now();
    auto durationMicros =
        std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count();

    stats_.successfulQueries.fetch_add(1, std::memory_order_relaxed);
    stats_.totalQueryTimeMicros.fetch_add(durationMicros, std::memory_order_relaxed);

    uint64_t totalQueries = stats_.totalQueries.load(std::memory_order_relaxed);
    if (totalQueries > 0) {
        stats_.avgQueryTimeMicros.store(
            stats_.totalQueryTimeMicros.load(std::memory_order_relaxed) / totalQueries,
            std::memory_order_relaxed);
    }

    return fusedResults;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryFTS5(const std::string& query) {
    std::vector<ComponentResult> results;

    try {
        // Use MetadataRepository::search() for FTS5 queries
        auto searchResults = metadataRepo_->search(query, config_.fts5MaxResults, 0);

        if (!searchResults) {
            spdlog::debug("FTS5 search failed: {}", searchResults.error().message);
            return results; // Return empty on error, don't fail entire search
        }

        const auto& sr = searchResults.value();

        for (size_t rank = 0; rank < sr.results.size(); ++rank) {
            const auto& searchResult = sr.results[rank];

            ComponentResult result;
            result.documentHash = searchResult.document.sha256Hash;
            result.filePath = searchResult.document.filePath;

            // BM25 score is negative (lower = better), normalize to [0, 1]
            result.score = std::max(
                0.0f, 1.0f / (1.0f + static_cast<float>(std::abs(searchResult.score)) / 10.0f));

            result.source = "fts5";
            result.rank = rank;
            result.snippet = searchResult.snippet.empty()
                                 ? std::nullopt
                                 : std::optional<std::string>(searchResult.snippet);

            results.push_back(std::move(result));
        }

    } catch (const std::exception& e) {
        spdlog::warn("FTS5 query exception: {}", e.what());
        return results; // Return empty on error
    }

    return results;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryPathTree(const std::string& query) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_) {
        return results;
    }

    try {
        // Strategy: Search for documents whose paths match the query string
        // Use path prefix matching and path component matching

        // Query for documents with matching path components
        yams::metadata::DocumentQueryOptions options;
        options.containsFragment = query; // Use query as path fragment
        options.limit = config_.pathTreeMaxResults;

        auto docResults = metadataRepo_->queryDocuments(options);
        if (!docResults) {
            spdlog::debug("Path tree query failed: {}", docResults.error().message);
            return results;
        }

        // Convert to ComponentResults
        for (size_t rank = 0; rank < docResults.value().size(); ++rank) {
            const auto& doc = docResults.value()[rank];

            ComponentResult result;
            result.documentHash = doc.sha256Hash;
            result.filePath = doc.filePath;

            // Score based on path match quality
            // Exact match = 1.0, partial match = lower score
            std::string lowerPath = doc.filePath;
            std::string lowerQuery = query;
            std::transform(lowerPath.begin(), lowerPath.end(), lowerPath.begin(), ::tolower);
            std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);

            if (lowerPath.find(lowerQuery) != std::string::npos) {
                // Calculate score based on match position and length
                size_t pos = lowerPath.find(lowerQuery);
                float positionScore = 1.0f - (static_cast<float>(pos) / lowerPath.length());
                float lengthScore = static_cast<float>(lowerQuery.length()) / lowerPath.length();
                result.score = (positionScore * 0.3f + lengthScore * 0.7f);
            } else {
                result.score = 0.5f; // Partial match
            }

            result.source = "path_tree";
            result.rank = rank;
            result.snippet = std::optional<std::string>(doc.filePath);
            result.debugInfo["path"] = doc.filePath;
            result.debugInfo["path_depth"] = std::to_string(doc.pathDepth);

            results.push_back(std::move(result));
        }

        spdlog::debug("Path tree query returned {} results for query: {}", results.size(), query);

    } catch (const std::exception& e) {
        spdlog::warn("Path tree query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::querySymbols(const std::string& query) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_) {
        return results;
    }

    // TODO: This is a simplified implementation. Ideally, MetadataRepository should
    // provide a searchSymbols() method for proper abstraction.

    try {
        // For now, we'll use a simple approach: search for documents that might
        // contain symbols matching the query by using FTS5 on content
        // A proper implementation would query the symbol_metadata table directly

        // Use FTS5 to find documents with matching content as a proxy for symbol search
        // This is not ideal but works until we have proper symbol search in MetadataRepository
        auto fts5Results = metadataRepo_->search(query, config_.symbolMaxResults, 0);
        if (!fts5Results) {
            spdlog::debug("Symbol search (via FTS5) failed: {}", fts5Results.error().message);
            return results;
        }

        // Convert FTS5 results to ComponentResults with symbol source
        for (size_t rank = 0; rank < fts5Results.value().results.size(); ++rank) {
            const auto& searchResult = fts5Results.value().results[rank];

            ComponentResult result;
            result.documentHash = searchResult.document.sha256Hash;
            result.filePath = searchResult.document.filePath;

            // Lower score than direct FTS5 since this is a proxy search
            result.score = std::max(
                0.0f, 0.8f / (1.0f + static_cast<float>(std::abs(searchResult.score)) / 10.0f));

            result.source = "symbol";
            result.rank = rank;
            result.snippet = searchResult.snippet.empty()
                                 ? std::nullopt
                                 : std::optional<std::string>(searchResult.snippet);
            result.debugInfo["match_type"] = "content_proxy";
            result.debugInfo["note"] = "Using FTS5 proxy - proper symbol table query needed";

            results.push_back(std::move(result));
        }

        spdlog::debug("Symbol query (via FTS5 proxy) returned {} results for query: {}",
                      results.size(), query);

    } catch (const std::exception& e) {
        spdlog::warn("Symbol query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryKnowledgeGraph(const std::string& query) {
    std::vector<ComponentResult> results;

    if (!kgStore_) {
        // KG store not available, skip
        return results;
    }

    try {
        // Strategy: Use KnowledgeGraphStore to resolve aliases matching the query
        // Try fuzzy matching first (includes FTS5)
        auto aliasResults = kgStore_->resolveAliasFuzzy(query, config_.kgMaxResults);
        if (!aliasResults || aliasResults.value().empty()) {
            // Fallback to exact match
            aliasResults = kgStore_->resolveAliasExact(query, config_.kgMaxResults);
            if (!aliasResults) {
                spdlog::debug("KG alias resolution failed: {}", aliasResults.error().message);
                return results;
            }
        }

        // For now, return results based on alias resolution scores
        // A full implementation would need to traverse from nodes to documents
        // This requires querying doc_entities by node_id, which isn't directly exposed
        // TODO: Add getDocEntitiesByNode() method to KnowledgeGraphStore interface

        size_t rank = 0;
        for (const auto& aliasRes : aliasResults.value()) {
            if (rank >= config_.kgMaxResults) {
                break;
            }

            // For now, we can't easily map from node_id to documents without
            // additional API methods. This is a simplified placeholder.
            // A proper implementation needs:
            // 1. Query doc_entities WHERE node_id = aliasRes.nodeId
            // 2. For each doc_entity, resolve document_id to DocumentInfo
            // 3. Group by document and aggregate scores

            spdlog::debug("KG: Found node {} with score {} for query '{}'", aliasRes.nodeId,
                          aliasRes.score, query);
            rank++;
        }

        spdlog::debug("KG query returned {} alias matches for query: {}",
                      aliasResults.value().size(), query);

    } catch (const std::exception& e) {
        spdlog::warn("KG query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding) {
    std::vector<ComponentResult> results;

    if (!vectorIndex_) {
        return results;
    }

    try {
        // Use VectorIndexManager::search()
        vector::SearchFilter filter;
        filter.min_similarity = config_.similarityThreshold;

        auto vectorResults = vectorIndex_->search(embedding, config_.vectorMaxResults, filter);

        if (!vectorResults) {
            spdlog::debug("Vector search failed: {}", vectorResults.error().message);
            return results;
        }

        for (size_t rank = 0; rank < vectorResults.value().size(); ++rank) {
            const auto& vr = vectorResults.value()[rank];

            ComponentResult result;
            result.documentHash = vr.id; // Assuming id is document hash
            result.score = vr.similarity;
            result.source = "vector";
            result.rank = rank;

            results.push_back(std::move(result));
        }

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<void> SearchEngine::Impl::healthCheck() {
    // Check metadata repository
    if (!metadataRepo_) {
        return Error{ErrorCode::InvalidState, "Metadata repository not initialized"};
    }

    // Check vector database
    if (config_.vectorWeight > 0.0f && !vectorDb_) {
        return Error{ErrorCode::InvalidState, "Vector database not initialized"};
    }

    // Check vector index
    if (config_.vectorWeight > 0.0f && !vectorIndex_) {
        return Error{ErrorCode::InvalidState, "Vector index not initialized"};
    }

    // Check embedding generator
    if (config_.vectorWeight > 0.0f && !embeddingGen_) {
        return Error{ErrorCode::InvalidState, "Embedding generator not initialized"};
    }

    return {};
}

// ============================================================================
// SearchEngine Public API
// ============================================================================

SearchEngine::SearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                           std::shared_ptr<vector::VectorDatabase> vectorDb,
                           std::shared_ptr<vector::VectorIndexManager> vectorIndex,
                           std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                           std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                           const SearchEngineConfig& config)
    : pImpl_(std::make_unique<Impl>(std::move(metadataRepo), std::move(vectorDb),
                                    std::move(vectorIndex), std::move(embeddingGen),
                                    std::move(kgStore), config)) {}

SearchEngine::~SearchEngine() = default;

SearchEngine::SearchEngine(SearchEngine&&) noexcept = default;
SearchEngine& SearchEngine::operator=(SearchEngine&&) noexcept = default;

Result<std::vector<SearchResult>> SearchEngine::search(const std::string& query,
                                                       const SearchParams& params) {
    return pImpl_->search(query, params);
}

void SearchEngine::setConfig(const SearchEngineConfig& config) {
    pImpl_->setConfig(config);
}

const SearchEngineConfig& SearchEngine::getConfig() const {
    return pImpl_->getConfig();
}

const SearchEngine::Statistics& SearchEngine::getStatistics() const {
    return pImpl_->getStatistics();
}

void SearchEngine::resetStatistics() {
    pImpl_->resetStatistics();
}

Result<void> SearchEngine::healthCheck() {
    return pImpl_->healthCheck();
}

// Factory function
std::unique_ptr<SearchEngine>
createSearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                   std::shared_ptr<vector::VectorDatabase> vectorDb,
                   std::shared_ptr<vector::VectorIndexManager> vectorIndex,
                   std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                   std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                   const SearchEngineConfig& config) {
    return std::make_unique<SearchEngine>(std::move(metadataRepo), std::move(vectorDb),
                                          std::move(vectorIndex), std::move(embeddingGen),
                                          std::move(kgStore), config);
}

} // namespace yams::search
