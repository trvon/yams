#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <future>
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
    fusedResults.reserve(std::min(grouped.size(), config_.maxResults));

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

    // Use partial_sort if we only need top maxResults (faster than full sort)
    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(
            fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
            fusedResults.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }

    return fusedResults;
}

std::vector<SearchResult>
ResultFusion::fuseReciprocalRank(const std::vector<ComponentResult>& results) {
    auto grouped = groupByDocument(results);
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(std::min(grouped.size(), config_.maxResults));

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

    // Use partial_sort for better performance when maxResults << total
    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(
            fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
            fusedResults.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }

    return fusedResults;
}

std::vector<SearchResult>
ResultFusion::fuseBordaCount(const std::vector<ComponentResult>& results) {
    auto grouped = groupByDocument(results);
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(std::min(grouped.size(), config_.maxResults));

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

    // Use partial_sort for better performance when maxResults << total
    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(
            fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
            fusedResults.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }

    return fusedResults;
}

std::vector<SearchResult>
ResultFusion::fuseWeightedReciprocal(const std::vector<ComponentResult>& results) {
    auto grouped = groupByDocument(results);
    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(std::min(grouped.size(), config_.maxResults));

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

    // Use partial_sort for better performance when maxResults << total
    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(
            fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
            fusedResults.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
    }

    return fusedResults;
}

std::unordered_map<std::string, std::vector<ComponentResult>>
ResultFusion::groupByDocument(const std::vector<ComponentResult>& results) const {
    std::unordered_map<std::string, std::vector<ComponentResult>> grouped;
    grouped.reserve(results.size()); // Worst case: each result is unique document

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
    if (source == "tag")
        return config_.tagWeight;
    if (source == "metadata")
        return config_.metadataWeight;
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
        stats_.tagQueries.store(0, std::memory_order_relaxed);
        stats_.metadataQueries.store(0, std::memory_order_relaxed);

        stats_.totalQueryTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgQueryTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgFts5TimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgPathTreeTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgSymbolTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgKgTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgVectorTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgTagTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgMetadataTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgResultsPerQuery.store(0, std::memory_order_relaxed);
        stats_.avgComponentsPerResult.store(0, std::memory_order_relaxed);
    }

    Result<void> healthCheck();

    Result<SearchResponse> searchWithResponse(const std::string& query,
                                              const SearchParams& params = {});

private:
    Result<SearchResponse> searchInternal(const std::string& query, const SearchParams& params);

    Result<std::vector<ComponentResult>> queryFTS5(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> querySymbols(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryKnowledgeGraph(const std::string& query,
                                                             size_t limit);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding,
                                                          size_t limit);
    Result<std::vector<ComponentResult>> queryTags(const std::vector<std::string>& tags,
                                                   bool matchAll, size_t limit);
    Result<std::vector<ComponentResult>> queryMetadata(const SearchParams& params, size_t limit);

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
    auto response = searchInternal(query, params);
    if (!response) {
        return Error{response.error().code, response.error().message};
    }
    return response.value().results;
}

Result<SearchResponse> SearchEngine::Impl::searchWithResponse(const std::string& query,
                                                              const SearchParams& params) {
    return searchInternal(query, params);
}

Result<SearchResponse> SearchEngine::Impl::searchInternal(const std::string& query,
                                                          const SearchParams& params) {
    auto startTime = std::chrono::steady_clock::now();
    stats_.totalQueries.fetch_add(1, std::memory_order_relaxed);

    SearchResponse response;
    std::vector<std::string> timedOut;
    std::vector<std::string> failed;
    std::vector<std::string> contributing;

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

    const size_t userLimit =
        params.limit > 0 ? static_cast<size_t>(params.limit) : config_.maxResults;
    const size_t componentCap = std::max(userLimit * 3, static_cast<size_t>(50));

    SearchEngineConfig workingConfig = config_;
    workingConfig.fts5MaxResults = std::min(config_.fts5MaxResults, componentCap);
    workingConfig.pathTreeMaxResults = std::min(config_.pathTreeMaxResults, componentCap);
    workingConfig.symbolMaxResults = std::min(config_.symbolMaxResults, componentCap);
    workingConfig.kgMaxResults = std::min(config_.kgMaxResults, componentCap);
    workingConfig.vectorMaxResults = std::min(config_.vectorMaxResults, componentCap);
    workingConfig.tagMaxResults = std::min(config_.tagMaxResults, componentCap);
    workingConfig.metadataMaxResults = std::min(config_.metadataMaxResults, componentCap);
    workingConfig.maxResults = userLimit;

    spdlog::debug("Search limit optimization: userLimit={}, componentCap={}", userLimit,
                  componentCap);

    std::vector<ComponentResult> allComponentResults;
    size_t estimatedResults = 0;
    if (workingConfig.fts5Weight > 0.0f)
        estimatedResults += workingConfig.fts5MaxResults;
    if (workingConfig.kgWeight > 0.0f)
        estimatedResults += workingConfig.kgMaxResults;
    if (workingConfig.pathTreeWeight > 0.0f)
        estimatedResults += workingConfig.pathTreeMaxResults;
    if (workingConfig.symbolWeight > 0.0f)
        estimatedResults += workingConfig.symbolMaxResults;
    if (workingConfig.vectorWeight > 0.0f)
        estimatedResults += workingConfig.vectorMaxResults;
    if (workingConfig.tagWeight > 0.0f)
        estimatedResults += workingConfig.tagMaxResults;
    if (workingConfig.metadataWeight > 0.0f)
        estimatedResults += workingConfig.metadataMaxResults;
    allComponentResults.reserve(estimatedResults);

    if (config_.enableParallelExecution) {
        std::future<Result<std::vector<ComponentResult>>> fts5Future;
        std::future<Result<std::vector<ComponentResult>>> kgFuture;
        std::future<Result<std::vector<ComponentResult>>> pathFuture;
        std::future<Result<std::vector<ComponentResult>>> symbolFuture;
        std::future<Result<std::vector<ComponentResult>>> vectorFuture;
        std::future<Result<std::vector<ComponentResult>>> tagFuture;
        std::future<Result<std::vector<ComponentResult>>> metaFuture;

        if (config_.fts5Weight > 0.0f) {
            fts5Future = std::async(std::launch::async, [this, &query, &workingConfig]() {
                return queryFTS5(query, workingConfig.fts5MaxResults);
            });
        }

        if (config_.kgWeight > 0.0f && kgStore_) {
            kgFuture = std::async(std::launch::async, [this, &query, &workingConfig]() {
                return queryKnowledgeGraph(query, workingConfig.kgMaxResults);
            });
        }

        if (config_.pathTreeWeight > 0.0f) {
            pathFuture = std::async(std::launch::async, [this, &query, &workingConfig]() {
                return queryPathTree(query, workingConfig.pathTreeMaxResults);
            });
        }

        if (config_.symbolWeight > 0.0f) {
            symbolFuture = std::async(std::launch::async, [this, &query, &workingConfig]() {
                return querySymbols(query, workingConfig.symbolMaxResults);
            });
        }

        if (config_.vectorWeight > 0.0f && queryEmbedding.has_value() && vectorIndex_) {
            vectorFuture =
                std::async(std::launch::async, [this, &queryEmbedding, &workingConfig]() {
                    return queryVectorIndex(queryEmbedding.value(), workingConfig.vectorMaxResults);
                });
        }

        if (config_.tagWeight > 0.0f && !params.tags.empty()) {
            tagFuture = std::async(std::launch::async, [this, &params, &workingConfig]() {
                return queryTags(params.tags, params.matchAllTags, workingConfig.tagMaxResults);
            });
        }

        if (config_.metadataWeight > 0.0f) {
            metaFuture = std::async(std::launch::async, [this, &params, &workingConfig]() {
                return queryMetadata(params, workingConfig.metadataMaxResults);
            });
        }

        enum class ComponentStatus { Success, Failed, TimedOut };

        auto collectResults = [&](auto& future, const char* name, std::atomic<uint64_t>& queryCount,
                                  std::atomic<uint64_t>& avgTime) -> ComponentStatus {
            if (!future.valid())
                return ComponentStatus::Success;

            auto waitStart = std::chrono::steady_clock::now();
            auto status = future.wait_for(config_.componentTimeout);

            if (status == std::future_status::ready) {
                try {
                    auto results = future.get();
                    auto waitEnd = std::chrono::steady_clock::now();
                    auto duration =
                        std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart)
                            .count();

                    if (results) {
                        if (!results.value().empty()) {
                            allComponentResults.insert(allComponentResults.end(),
                                                       results.value().begin(),
                                                       results.value().end());
                            contributing.push_back(name);
                        }
                        queryCount.fetch_add(1, std::memory_order_relaxed);
                        avgTime.store(duration, std::memory_order_relaxed);
                        return ComponentStatus::Success;
                    } else {
                        spdlog::debug("Parallel {} query returned error: {}", name,
                                      results.error().message);
                        return ComponentStatus::Failed;
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Parallel {} query failed: {}", name, e.what());
                    return ComponentStatus::Failed;
                }
            } else {
                spdlog::warn("Parallel {} query timed out after {} ms", name,
                             config_.componentTimeout.count());
                stats_.timedOutQueries.fetch_add(1, std::memory_order_relaxed);
                return ComponentStatus::TimedOut;
            }
        };

        auto handleStatus = [&](ComponentStatus status, const char* name) {
            if (status == ComponentStatus::Failed) {
                failed.push_back(name);
            } else if (status == ComponentStatus::TimedOut) {
                timedOut.push_back(name);
            }
        };

        handleStatus(
            collectResults(fts5Future, "fts5", stats_.fts5Queries, stats_.avgFts5TimeMicros),
            "fts5");
        handleStatus(collectResults(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros),
                     "kg");
        handleStatus(collectResults(pathFuture, "path", stats_.pathTreeQueries,
                                    stats_.avgPathTreeTimeMicros),
                     "path");
        handleStatus(collectResults(symbolFuture, "symbol", stats_.symbolQueries,
                                    stats_.avgSymbolTimeMicros),
                     "symbol");
        handleStatus(collectResults(vectorFuture, "vector", stats_.vectorQueries,
                                    stats_.avgVectorTimeMicros),
                     "vector");
        handleStatus(collectResults(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros),
                     "tag");
        handleStatus(collectResults(metaFuture, "metadata", stats_.metadataQueries,
                                    stats_.avgMetadataTimeMicros),
                     "metadata");

    } else {
        auto runSequential = [&](auto queryFn, const char* name, float weight,
                                 std::atomic<uint64_t>& queryCount,
                                 std::atomic<uint64_t>& avgTime) {
            if (weight <= 0.0f)
                return;

            auto start = std::chrono::steady_clock::now();
            auto results = queryFn();
            auto end = std::chrono::steady_clock::now();

            if (results) {
                if (!results.value().empty()) {
                    allComponentResults.insert(allComponentResults.end(), results.value().begin(),
                                               results.value().end());
                    contributing.push_back(name);
                }
                queryCount.fetch_add(1, std::memory_order_relaxed);
                auto duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                avgTime.store(duration, std::memory_order_relaxed);
            } else {
                failed.push_back(name);
            }
        };

        runSequential([&]() { return queryFTS5(query, workingConfig.fts5MaxResults); }, "fts5",
                      config_.fts5Weight, stats_.fts5Queries, stats_.avgFts5TimeMicros);

        if (kgStore_) {
            runSequential([&]() { return queryKnowledgeGraph(query, workingConfig.kgMaxResults); },
                          "kg", config_.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros);
        }

        runSequential([&]() { return queryPathTree(query, workingConfig.pathTreeMaxResults); },
                      "path", config_.pathTreeWeight, stats_.pathTreeQueries,
                      stats_.avgPathTreeTimeMicros);

        runSequential([&]() { return querySymbols(query, workingConfig.symbolMaxResults); },
                      "symbol", config_.symbolWeight, stats_.symbolQueries,
                      stats_.avgSymbolTimeMicros);

        if (queryEmbedding.has_value() && vectorIndex_) {
            runSequential(
                [&]() {
                    return queryVectorIndex(queryEmbedding.value(), workingConfig.vectorMaxResults);
                },
                "vector", config_.vectorWeight, stats_.vectorQueries, stats_.avgVectorTimeMicros);
        }

        if (!params.tags.empty()) {
            runSequential(
                [&]() {
                    return queryTags(params.tags, params.matchAllTags, workingConfig.tagMaxResults);
                },
                "tag", config_.tagWeight, stats_.tagQueries, stats_.avgTagTimeMicros);
        }

        runSequential([&]() { return queryMetadata(params, workingConfig.metadataMaxResults); },
                      "metadata", config_.metadataWeight, stats_.metadataQueries,
                      stats_.avgMetadataTimeMicros);
    }

    ResultFusion fusion(workingConfig);
    response.results = fusion.fuse(allComponentResults);
    response.timedOutComponents = std::move(timedOut);
    response.failedComponents = std::move(failed);
    response.contributingComponents = std::move(contributing);
    response.isDegraded =
        !response.timedOutComponents.empty() || !response.failedComponents.empty();

    auto endTime = std::chrono::steady_clock::now();
    response.executionTimeMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

    stats_.successfulQueries.fetch_add(1, std::memory_order_relaxed);
    auto durationMicros =
        std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime).count();
    stats_.totalQueryTimeMicros.fetch_add(durationMicros, std::memory_order_relaxed);

    uint64_t totalQueries = stats_.totalQueries.load(std::memory_order_relaxed);
    if (totalQueries > 0) {
        stats_.avgQueryTimeMicros.store(
            stats_.totalQueryTimeMicros.load(std::memory_order_relaxed) / totalQueries,
            std::memory_order_relaxed);
    }

    if (response.isDegraded && response.hasResults()) {
        spdlog::info("Search returned {} results (degraded: {} timed out, {} failed)",
                     response.results.size(), response.timedOutComponents.size(),
                     response.failedComponents.size());
    }

    return response;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryFTS5(const std::string& query,
                                                                   size_t limit) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_) {
        return results;
    }

    try {
        auto searchResults = metadataRepo_->search(query, limit, 0);

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

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryPathTree(const std::string& query,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_) {
        return results;
    }

    try {
        yams::metadata::DocumentQueryOptions options;
        options.containsFragment = query;
        options.limit = static_cast<int>(limit);

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

Result<std::vector<ComponentResult>> SearchEngine::Impl::querySymbols(const std::string& query,
                                                                      size_t limit) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_) {
        return results;
    }

    try {
        auto fts5Results = metadataRepo_->search(query, limit * 2, 0);
        if (!fts5Results) {
            spdlog::debug("Symbol search failed: {}", fts5Results.error().message);
            return results;
        }

        for (size_t rank = 0; rank < fts5Results.value().results.size(); ++rank) {
            if (results.size() >= limit) {
                break;
            }

            const auto& searchResult = fts5Results.value().results[rank];
            const auto& filePath = searchResult.document.filePath;

            auto pruneCategory = magic::getPruneCategory(filePath);
            bool isCodeFile = pruneCategory == magic::PruneCategory::BuildObject ||
                              pruneCategory == magic::PruneCategory::None;

            float scoreMultiplier = isCodeFile ? 1.0f : 0.5f;

            bool queryLooksLikeSymbol = query.find('_') != std::string::npos ||
                                        query.find("::") != std::string::npos ||
                                        (query.length() > 1 && std::isupper(query[0]));

            if (queryLooksLikeSymbol) {
                scoreMultiplier *= 1.2f;
            }

            ComponentResult result;
            result.documentHash = searchResult.document.sha256Hash;
            result.filePath = filePath;
            result.score = std::max(
                0.0f, scoreMultiplier /
                          (1.0f + static_cast<float>(std::abs(searchResult.score)) / 10.0f));
            result.source = "symbol";
            result.rank = results.size();
            result.snippet = searchResult.snippet.empty()
                                 ? std::nullopt
                                 : std::optional<std::string>(searchResult.snippet);

            results.push_back(std::move(result));
        }

        spdlog::debug("Symbol query returned {} results for query: {}", results.size(), query);

    } catch (const std::exception& e) {
        spdlog::warn("Symbol query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryKnowledgeGraph(const std::string& query, size_t limit) {
    std::vector<ComponentResult> results;

    if (!kgStore_) {
        return results;
    }

    try {
        auto aliasResults = kgStore_->resolveAliasFuzzy(query, limit);
        if (!aliasResults || aliasResults.value().empty()) {
            aliasResults = kgStore_->resolveAliasExact(query, limit);
            if (!aliasResults) {
                spdlog::debug("KG alias resolution failed: {}", aliasResults.error().message);
                return results;
            }
        }

        std::unordered_map<std::string, size_t> docHashToResultIndex;

        for (const auto& aliasRes : aliasResults.value()) {
            if (results.size() >= limit) {
                break;
            }

            auto nodeResult = kgStore_->getNodeById(aliasRes.nodeId);
            if (!nodeResult || !nodeResult.value().has_value()) {
                continue;
            }

            const auto& node = nodeResult.value().value();
            std::string searchTerm = node.label.value_or(node.nodeKey);

            auto docResults = metadataRepo_->search(searchTerm, 10, 0);
            if (!docResults) {
                continue;
            }

            for (const auto& searchResult : docResults.value().results) {
                if (results.size() >= limit) {
                    break;
                }

                const std::string& docHash = searchResult.document.sha256Hash;

                auto it = docHashToResultIndex.find(docHash);
                if (it != docHashToResultIndex.end()) {
                    results[it->second].score += aliasRes.score * 0.3f;
                    continue;
                }

                ComponentResult result;
                result.documentHash = docHash;
                result.filePath = searchResult.document.filePath;
                result.score = aliasRes.score * 0.8f;
                result.source = "kg";
                result.rank = results.size();
                result.snippet = searchResult.snippet.empty()
                                     ? std::optional<std::string>(searchTerm)
                                     : std::optional<std::string>(searchResult.snippet);
                result.debugInfo["node_id"] = std::to_string(aliasRes.nodeId);
                result.debugInfo["node_key"] = node.nodeKey;
                if (node.type.has_value()) {
                    result.debugInfo["node_type"] = node.type.value();
                }

                docHashToResultIndex[docHash] = results.size();
                results.push_back(std::move(result));
            }
        }

        spdlog::debug("KG query returned {} document results for query: {}", results.size(), query);

    } catch (const std::exception& e) {
        spdlog::warn("KG query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding, size_t limit) {
    std::vector<ComponentResult> results;

    if (!vectorIndex_) {
        return results;
    }

    try {
        vector::SearchFilter filter;
        filter.min_similarity = config_.similarityThreshold;

        auto vectorResults = vectorIndex_->search(embedding, limit, filter);

        if (!vectorResults) {
            spdlog::debug("Vector search failed: {}", vectorResults.error().message);
            return results;
        }

        for (size_t rank = 0; rank < vectorResults.value().size(); ++rank) {
            const auto& vr = vectorResults.value()[rank];

            ComponentResult result;
            result.documentHash = vr.id;
            result.score = vr.similarity;
            result.source = "vector";
            result.rank = rank;

            // Resolve filePath from document hash via metadata repository
            if (metadataRepo_) {
                auto docResult = metadataRepo_->findDocumentsByHashPrefix(vr.id, 1);
                if (docResult && !docResult.value().empty()) {
                    result.filePath = docResult.value()[0].filePath;
                }
            }

            results.push_back(std::move(result));
        }

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryTags(const std::vector<std::string>& tags, bool matchAll, size_t limit) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_ || tags.empty()) {
        return results;
    }

    try {
        auto tagResults = metadataRepo_->findDocumentsByTags(tags, matchAll);
        if (!tagResults) {
            spdlog::debug("Tag search failed: {}", tagResults.error().message);
            return results;
        }

        for (size_t rank = 0; rank < tagResults.value().size() && rank < limit; ++rank) {
            const auto& doc = tagResults.value()[rank];

            ComponentResult result;
            result.documentHash = doc.sha256Hash;
            result.filePath = doc.filePath;

            // Score based on how many requested tags the document has
            // For matchAll=true, all docs have all tags, so score = 1.0
            // For matchAll=false, score based on tag overlap
            if (matchAll) {
                result.score = 1.0f;
            } else {
                auto docTagsResult = metadataRepo_->getDocumentTags(doc.id);
                if (docTagsResult) {
                    std::set<std::string> docTagSet(docTagsResult.value().begin(),
                                                    docTagsResult.value().end());
                    size_t matchCount = 0;
                    for (const auto& tag : tags) {
                        if (docTagSet.count(tag) > 0) {
                            matchCount++;
                        }
                    }
                    result.score = static_cast<float>(matchCount) / static_cast<float>(tags.size());
                } else {
                    result.score = 0.5f;
                }
            }

            result.source = "tag";
            result.rank = rank;
            result.debugInfo["matched_tags"] = std::to_string(tags.size());

            results.push_back(std::move(result));
        }

        spdlog::debug("Tag query returned {} results for {} tags (matchAll={})", results.size(),
                      tags.size(), matchAll);

    } catch (const std::exception& e) {
        spdlog::warn("Tag query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryMetadata(const SearchParams& params,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;

    if (!metadataRepo_) {
        return results;
    }

    bool hasFilters = params.mimeType.has_value() || params.extension.has_value() ||
                      params.modifiedAfter.has_value() || params.modifiedBefore.has_value();

    if (!hasFilters) {
        return results;
    }

    try {
        yams::metadata::DocumentQueryOptions options;
        options.mimeType = params.mimeType;
        options.extension = params.extension;
        options.modifiedAfter = params.modifiedAfter;
        options.modifiedBefore = params.modifiedBefore;
        options.limit = static_cast<int>(limit);

        auto docResults = metadataRepo_->queryDocuments(options);
        if (!docResults) {
            spdlog::debug("Metadata query failed: {}", docResults.error().message);
            return results;
        }

        for (size_t rank = 0; rank < docResults.value().size(); ++rank) {
            const auto& doc = docResults.value()[rank];

            ComponentResult result;
            result.documentHash = doc.sha256Hash;
            result.filePath = doc.filePath;

            // Score based on how many filters matched (all docs returned match all filters)
            int filterCount = 0;
            if (params.mimeType.has_value())
                filterCount++;
            if (params.extension.has_value())
                filterCount++;
            if (params.modifiedAfter.has_value())
                filterCount++;
            if (params.modifiedBefore.has_value())
                filterCount++;

            result.score = 1.0f; // All returned docs fully match the filters
            result.source = "metadata";
            result.rank = rank;
            result.debugInfo["filter_count"] = std::to_string(filterCount);
            if (params.mimeType.has_value()) {
                result.debugInfo["mime_type"] = params.mimeType.value();
            }
            if (params.extension.has_value()) {
                result.debugInfo["extension"] = params.extension.value();
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Metadata query returned {} results", results.size());

    } catch (const std::exception& e) {
        spdlog::warn("Metadata query exception: {}", e.what());
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

Result<SearchResponse> SearchEngine::searchWithResponse(const std::string& query,
                                                        const SearchParams& params) {
    return pImpl_->searchWithResponse(query, params);
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
