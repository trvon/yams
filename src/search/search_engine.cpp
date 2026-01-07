#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include <yams/core/cpp23_features.hpp>
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

#include <boost/asio/post.hpp>

#include "yams/profiling.h"

#if YAMS_HAS_FLAT_MAP
#include <flat_map>
#endif

#if YAMS_HAS_RANGES
#include <ranges>
#endif

namespace yams::search {
namespace compat {

#if YAMS_HAS_FLAT_MAP
template <typename Key, typename Value, typename Compare = std::less<Key>,
          typename KeyContainer = std::vector<Key>, typename MappedContainer = std::vector<Value>>
using flat_map = std::flat_map<Key, Value, Compare, KeyContainer, MappedContainer>;

template <typename Map> inline void reserve_if_needed(Map&, size_t) {
    // std::flat_map doesn't have reserve(), do nothing
}
#else
template <typename Key, typename Value, typename Compare = std::less<Key>>
using flat_map = std::unordered_map<Key, Value>;

template <typename Map> inline void reserve_if_needed(Map& m, size_t n) {
    m.reserve(n);
}
#endif

} // namespace compat

namespace {

template <typename Work>
auto postWork(Work work, const std::optional<boost::asio::any_io_executor>& executor)
    -> std::future<decltype(work())> {
    using ResultType = decltype(work());
    std::packaged_task<ResultType()> task(std::move(work));
    auto future = task.get_future();
    if (executor) {
        boost::asio::post(*executor, [task = std::move(task)]() mutable { task(); });
    } else {
        // Fire-and-forget async task - intentionally discard the future
        (void)std::async(std::launch::async, [task = std::move(task)]() mutable { task(); });
    }
    return future;
}

} // namespace

#if YAMS_HAS_RANGES
namespace ranges_helpers {
template <typename Range, typename Predicate>
auto filter_not_empty(const Range& range, Predicate pred) {
    return range | std::views::filter(pred) |
           std::views::transform([](const auto& elem) { return elem; });
}
} // namespace ranges_helpers
#endif

// ============================================================================
// ResultFusion Implementation
// ============================================================================

ResultFusion::ResultFusion(const SearchEngineConfig& config) : config_(config) {}

std::vector<SearchResult> ResultFusion::fuse(const std::vector<ComponentResult>& componentResults) {
    if (componentResults.empty()) [[unlikely]] {
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

// All fusion strategies now use fuseSinglePass (defined in header as template).
// This replaces the previous 3-pass pattern (groupByDocument -> iterate -> sort)
// with a single-pass accumulation directly into result map, then one sort.

std::vector<SearchResult>
ResultFusion::fuseWeightedSum(const std::vector<ComponentResult>& results) {
    return fuseSinglePass(results, [this](const ComponentResult& comp) {
        return comp.score * getComponentWeight(comp.source);
    });
}

std::vector<SearchResult>
ResultFusion::fuseReciprocalRank(const std::vector<ComponentResult>& results) {
    const float k = config_.rrfK;
    return fuseSinglePass(results, [k](const ComponentResult& comp) {
        return 1.0 / (k + static_cast<double>(comp.rank));
    });
}

std::vector<SearchResult>
ResultFusion::fuseBordaCount(const std::vector<ComponentResult>& results) {
    return fuseSinglePass(results, [this](const ComponentResult& comp) {
        return getComponentWeight(comp.source) * (100.0 - static_cast<double>(comp.rank));
    });
}

std::vector<SearchResult>
ResultFusion::fuseWeightedReciprocal(const std::vector<ComponentResult>& results) {
    const float k = config_.rrfK;
    return fuseSinglePass(results, [this, k](const ComponentResult& comp) {
        // Weighted RRF with score boost:
        // - RRF provides rank-based fusion across components
        // - Score provides a multiplicative boost to reward high-confidence matches
        // - Formula: weight * rrfScore * (1 + score) where score is in [0,1]
        //   This gives rank-1 with score=0.9 a 1.9x boost vs rank-1 with score=0
        float weight = getComponentWeight(comp.source);
        double rrfScore = 1.0 / (k + static_cast<double>(comp.rank));
        double scoreBoost = 1.0 + std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
        return weight * rrfScore * scoreBoost;
    });
}

float ResultFusion::getComponentWeight(const std::string& source) const {
    if (source == "text")
        return config_.textWeight;
    if (source == "path_tree")
        return config_.pathTreeWeight;
    if (source == "kg")
        return config_.kgWeight;
    if (source == "vector")
        return config_.vectorWeight;
    if (source == "entity_vector")
        return config_.entityVectorWeight;
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
         std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
         std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
         const SearchEngineConfig& config)
        : metadataRepo_(std::move(metadataRepo)), vectorDb_(std::move(vectorDb)),
          embeddingGen_(std::move(embeddingGen)), kgStore_(std::move(kgStore)), config_(config) {}

    Result<std::vector<SearchResult>> search(const std::string& query, const SearchParams& params);

    void setConfig(const SearchEngineConfig& config) { config_ = config; }

    const SearchEngineConfig& getConfig() const { return config_; }

    const SearchEngine::Statistics& getStatistics() const { return stats_; }

    void resetStatistics() {
        stats_.totalQueries.store(0, std::memory_order_relaxed);
        stats_.successfulQueries.store(0, std::memory_order_relaxed);
        stats_.failedQueries.store(0, std::memory_order_relaxed);

        stats_.textQueries.store(0, std::memory_order_relaxed);
        stats_.pathTreeQueries.store(0, std::memory_order_relaxed);
        stats_.kgQueries.store(0, std::memory_order_relaxed);
        stats_.vectorQueries.store(0, std::memory_order_relaxed);
        stats_.entityVectorQueries.store(0, std::memory_order_relaxed);
        stats_.tagQueries.store(0, std::memory_order_relaxed);
        stats_.metadataQueries.store(0, std::memory_order_relaxed);

        stats_.totalQueryTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgQueryTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgTextTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgPathTreeTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgKgTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgVectorTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgEntityVectorTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgTagTimeMicros.store(0, std::memory_order_relaxed);
        stats_.avgMetadataTimeMicros.store(0, std::memory_order_relaxed);

        stats_.avgResultsPerQuery.store(0, std::memory_order_relaxed);
        stats_.avgComponentsPerResult.store(0, std::memory_order_relaxed);
    }

    Result<void> healthCheck();

    Result<SearchResponse> searchWithResponse(const std::string& query,
                                              const SearchParams& params = {});

    void setExecutor(std::optional<boost::asio::any_io_executor> executor) {
        executor_ = std::move(executor);
    }

private:
    Result<SearchResponse> searchInternal(const std::string& query, const SearchParams& params);

    Result<std::vector<ComponentResult>> queryFullText(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryKnowledgeGraph(const std::string& query,
                                                             size_t limit);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding,
                                                          size_t limit);
    Result<std::vector<ComponentResult>> queryEntityVectors(const std::vector<float>& embedding,
                                                            size_t limit);
    Result<std::vector<ComponentResult>> queryTags(const std::vector<std::string>& tags,
                                                   bool matchAll, size_t limit);
    Result<std::vector<ComponentResult>> queryMetadata(const SearchParams& params, size_t limit);

    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGen_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::optional<boost::asio::any_io_executor> executor_;
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
    YAMS_ZONE_SCOPED_N("search_engine::execute");
    YAMS_FRAME_MARK();

    auto startTime = std::chrono::steady_clock::now();
    stats_.totalQueries.fetch_add(1, std::memory_order_relaxed);

    SearchResponse response;
    std::vector<std::string> timedOut;
    std::vector<std::string> failed;
    std::vector<std::string> contributing;
    std::vector<std::string> skipped;
    std::map<std::string, int64_t> componentTiming;

    // Generate embedding upfront if needed (required for vector searches)
    std::optional<std::vector<float>> queryEmbedding;
    const bool needsEmbedding = (config_.vectorWeight > 0.0f || config_.entityVectorWeight > 0.0f);
    if (needsEmbedding && embeddingGen_) {
        auto embStart = std::chrono::steady_clock::now();
        try {
            auto embResult = embeddingGen_->generateEmbedding(query);
            if (!embResult.empty()) {
                queryEmbedding = std::move(embResult);
            }
        } catch (const std::exception& e) {
            spdlog::warn("Failed to generate query embedding: {}", e.what());
        }
        auto embEnd = std::chrono::steady_clock::now();
        componentTiming["embedding"] =
            std::chrono::duration_cast<std::chrono::microseconds>(embEnd - embStart).count();
    }

    const size_t userLimit =
        params.limit > 0 ? static_cast<size_t>(params.limit) : config_.maxResults;
    const size_t componentCap = std::max(userLimit * 3, static_cast<size_t>(50));

    SearchEngineConfig workingConfig = config_;
    workingConfig.textMaxResults = std::min(config_.textMaxResults, componentCap);
    workingConfig.pathTreeMaxResults = std::min(config_.pathTreeMaxResults, componentCap);
    workingConfig.kgMaxResults = std::min(config_.kgMaxResults, componentCap);
    workingConfig.vectorMaxResults = std::min(config_.vectorMaxResults, componentCap);
    workingConfig.entityVectorMaxResults = std::min(config_.entityVectorMaxResults, componentCap);
    workingConfig.tagMaxResults = std::min(config_.tagMaxResults, componentCap);
    workingConfig.metadataMaxResults = std::min(config_.metadataMaxResults, componentCap);
    workingConfig.maxResults = userLimit;

    spdlog::debug("Search limit optimization: userLimit={}, componentCap={}, tiered={}", userLimit,
                  componentCap, config_.enableTieredSearch);

    std::vector<ComponentResult> allComponentResults;
    size_t estimatedResults = 0;
    if (workingConfig.textWeight > 0.0f)
        estimatedResults += workingConfig.textMaxResults;
    if (workingConfig.kgWeight > 0.0f)
        estimatedResults += workingConfig.kgMaxResults;
    if (workingConfig.pathTreeWeight > 0.0f)
        estimatedResults += workingConfig.pathTreeMaxResults;
    if (workingConfig.vectorWeight > 0.0f)
        estimatedResults += workingConfig.vectorMaxResults;
    if (workingConfig.entityVectorWeight > 0.0f)
        estimatedResults += workingConfig.entityVectorMaxResults;
    if (workingConfig.tagWeight > 0.0f)
        estimatedResults += workingConfig.tagMaxResults;
    if (workingConfig.metadataWeight > 0.0f)
        estimatedResults += workingConfig.metadataMaxResults;
    allComponentResults.reserve(estimatedResults);

    // Early termination helper: check if we have enough quality results
    const size_t earlyTerminationMinResults =
        config_.earlyTerminationMinResults > 0 ? config_.earlyTerminationMinResults : userLimit;

    auto shouldTerminateEarly = [&]() -> bool {
        if (!config_.enableTieredSearch)
            return false;
        if (allComponentResults.size() < earlyTerminationMinResults)
            return false;

        // Check if we have high-quality results (top score above threshold)
        float maxScore = 0.0f;
        for (const auto& r : allComponentResults) {
            maxScore = std::max(maxScore, r.score);
        }
        return maxScore >= config_.earlyTerminationQualityThreshold;
    };

    // Component result collection helper with timing
    enum class ComponentStatus { Success, Failed, TimedOut };

    auto collectResults = [&](auto& future, const char* name, std::atomic<uint64_t>& queryCount,
                              std::atomic<uint64_t>& avgTime) -> ComponentStatus {
        if (!future.valid())
            return ComponentStatus::Success;

        auto waitStart = std::chrono::steady_clock::now();

        // componentTimeout of 0 means no timeout (wait indefinitely)
        std::future_status status;
        if (config_.componentTimeout.count() == 0) {
            future.wait(); // Wait indefinitely
            status = std::future_status::ready;
        } else {
            status = future.wait_for(config_.componentTimeout);
        }

        if (status == std::future_status::ready) {
            try {
                auto results = future.get();
                auto waitEnd = std::chrono::steady_clock::now();
                auto duration =
                    std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart)
                        .count();

                componentTiming[name] = duration;

                if (results) {
                    if (!results.value().empty()) {
                        allComponentResults.insert(allComponentResults.end(),
                                                   results.value().begin(), results.value().end());
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

    bool usedEarlyTermination = false;

    if (config_.enableParallelExecution) [[likely]] {
        if (config_.enableTieredSearch) {
            // =====================================================================
            // TIERED SEARCH (PBI-075): Run components in tiers with early termination
            // Tier 1 (fast, indexed): text, path, vector (HNSW)
            // Tier 2 (medium): KG alias resolution
            // Tier 3 (slow, disabled by default): entity vectors (brute-force)
            // =====================================================================
            YAMS_ZONE_SCOPED_N("search_engine::tiered_parallel");

            // --- TIER 1: Fast indexed searches (run in parallel) ---
            {
                YAMS_ZONE_SCOPED_N("search_engine::tier1_fast");
                std::future<Result<std::vector<ComponentResult>>> textFuture;
                std::future<Result<std::vector<ComponentResult>>> pathFuture;
                std::future<Result<std::vector<ComponentResult>>> vectorFuture;
                std::future<Result<std::vector<ComponentResult>>> tagFuture;
                std::future<Result<std::vector<ComponentResult>>> metaFuture;

                if (config_.textWeight > 0.0f) {
                    textFuture = postWork(
                        [this, &query, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::text");
                            return queryFullText(query, workingConfig.textMaxResults);
                        },
                        executor_);
                }

                if (config_.pathTreeWeight > 0.0f) {
                    pathFuture = postWork(
                        [this, &query, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::path");
                            return queryPathTree(query, workingConfig.pathTreeMaxResults);
                        },
                        executor_);
                }

                if (config_.vectorWeight > 0.0f && queryEmbedding.has_value() && vectorDb_) {
                    vectorFuture = postWork(
                        [this, &queryEmbedding, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::vector");
                            return queryVectorIndex(queryEmbedding.value(),
                                                    workingConfig.vectorMaxResults);
                        },
                        executor_);
                }

                // Tags and metadata are modifiers - run them in Tier 1 as well
                if (config_.tagWeight > 0.0f && !params.tags.empty()) {
                    tagFuture = postWork(
                        [this, &params, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::tag");
                            return queryTags(params.tags, params.matchAllTags,
                                             workingConfig.tagMaxResults);
                        },
                        executor_);
                }

                if (config_.metadataWeight > 0.0f) {
                    metaFuture = postWork(
                        [this, &params, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::metadata");
                            return queryMetadata(params, workingConfig.metadataMaxResults);
                        },
                        executor_);
                }

                // Collect Tier 1 results
                handleStatus(collectResults(textFuture, "text", stats_.textQueries,
                                            stats_.avgTextTimeMicros),
                             "text");
                handleStatus(collectResults(pathFuture, "path", stats_.pathTreeQueries,
                                            stats_.avgPathTreeTimeMicros),
                             "path");
                handleStatus(collectResults(vectorFuture, "vector", stats_.vectorQueries,
                                            stats_.avgVectorTimeMicros),
                             "vector");
                handleStatus(
                    collectResults(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros),
                    "tag");
                handleStatus(collectResults(metaFuture, "metadata", stats_.metadataQueries,
                                            stats_.avgMetadataTimeMicros),
                             "metadata");
            }

            // Check for early termination after Tier 1
            if (shouldTerminateEarly()) {
                spdlog::debug(
                    "Early termination after Tier 1: {} results, skipping KG and entity_vector",
                    allComponentResults.size());
                usedEarlyTermination = true;
                if (config_.kgWeight > 0.0f && kgStore_) {
                    skipped.push_back("kg");
                }
                if (config_.entityVectorWeight > 0.0f && vectorDb_) {
                    skipped.push_back("entity_vector");
                }
            } else {
                // --- TIER 2: Knowledge Graph search (medium cost) ---
                if (config_.kgWeight > 0.0f && kgStore_) {
                    YAMS_ZONE_SCOPED_N("search_engine::tier2_kg");
                    std::future<Result<std::vector<ComponentResult>>> kgFuture;

                    kgFuture = postWork(
                        [this, &query, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::kg");
                            return queryKnowledgeGraph(query, workingConfig.kgMaxResults);
                        },
                        executor_);

                    handleStatus(
                        collectResults(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros),
                        "kg");
                }

                // Check for early termination after Tier 2
                if (shouldTerminateEarly()) {
                    spdlog::debug(
                        "Early termination after Tier 2: {} results, skipping entity_vector",
                        allComponentResults.size());
                    usedEarlyTermination = true;
                    if (config_.entityVectorWeight > 0.0f && vectorDb_) {
                        skipped.push_back("entity_vector");
                    }
                } else {
                    // --- TIER 3: Entity vectors (slow, brute-force) ---
                    // Only run if explicitly enabled (weight > 0)
                    if (config_.entityVectorWeight > 0.0f && queryEmbedding.has_value() &&
                        vectorDb_) {
                        YAMS_ZONE_SCOPED_N("search_engine::tier3_entity_vector");
                        std::future<Result<std::vector<ComponentResult>>> entityVectorFuture;

                        entityVectorFuture = postWork(
                            [this, &queryEmbedding, &workingConfig]() {
                                YAMS_ZONE_SCOPED_N("component::entity_vector");
                                return queryEntityVectors(queryEmbedding.value(),
                                                          workingConfig.entityVectorMaxResults);
                            },
                            executor_);

                        handleStatus(collectResults(entityVectorFuture, "entity_vector",
                                                    stats_.entityVectorQueries,
                                                    stats_.avgEntityVectorTimeMicros),
                                     "entity_vector");
                    }
                }
            }
        } else {
            // =====================================================================
            // LEGACY: All components in parallel (no tiering)
            // =====================================================================
            YAMS_ZONE_SCOPED_N("search_engine::fanout_parallel");
            std::future<Result<std::vector<ComponentResult>>> textFuture;
            std::future<Result<std::vector<ComponentResult>>> kgFuture;
            std::future<Result<std::vector<ComponentResult>>> pathFuture;
            std::future<Result<std::vector<ComponentResult>>> vectorFuture;
            std::future<Result<std::vector<ComponentResult>>> entityVectorFuture;
            std::future<Result<std::vector<ComponentResult>>> tagFuture;
            std::future<Result<std::vector<ComponentResult>>> metaFuture;

            if (config_.textWeight > 0.0f) {
                textFuture = postWork(
                    [this, &query, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::text");
                        return queryFullText(query, workingConfig.textMaxResults);
                    },
                    executor_);
            }

            if (config_.kgWeight > 0.0f && kgStore_) {
                kgFuture = postWork(
                    [this, &query, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::kg");
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults);
                    },
                    executor_);
            }

            if (config_.pathTreeWeight > 0.0f) {
                pathFuture = postWork(
                    [this, &query, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::path");
                        return queryPathTree(query, workingConfig.pathTreeMaxResults);
                    },
                    executor_);
            }

            if (config_.vectorWeight > 0.0f && queryEmbedding.has_value() && vectorDb_) {
                vectorFuture = postWork(
                    [this, &queryEmbedding, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::vector");
                        return queryVectorIndex(queryEmbedding.value(),
                                                workingConfig.vectorMaxResults);
                    },
                    executor_);
            }

            if (config_.entityVectorWeight > 0.0f && queryEmbedding.has_value() && vectorDb_) {
                entityVectorFuture = postWork(
                    [this, &queryEmbedding, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::entity_vector");
                        return queryEntityVectors(queryEmbedding.value(),
                                                  workingConfig.entityVectorMaxResults);
                    },
                    executor_);
            }

            if (config_.tagWeight > 0.0f && !params.tags.empty()) {
                tagFuture = postWork(
                    [this, &params, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::tag");
                        return queryTags(params.tags, params.matchAllTags,
                                         workingConfig.tagMaxResults);
                    },
                    executor_);
            }

            if (config_.metadataWeight > 0.0f) {
                metaFuture = postWork(
                    [this, &params, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::metadata");
                        return queryMetadata(params, workingConfig.metadataMaxResults);
                    },
                    executor_);
            }

            handleStatus(
                collectResults(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros),
                "text");
            handleStatus(collectResults(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros),
                         "kg");
            handleStatus(collectResults(pathFuture, "path", stats_.pathTreeQueries,
                                        stats_.avgPathTreeTimeMicros),
                         "path");
            handleStatus(collectResults(vectorFuture, "vector", stats_.vectorQueries,
                                        stats_.avgVectorTimeMicros),
                         "vector");
            handleStatus(collectResults(entityVectorFuture, "entity_vector",
                                        stats_.entityVectorQueries,
                                        stats_.avgEntityVectorTimeMicros),
                         "entity_vector");
            handleStatus(
                collectResults(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros),
                "tag");
            handleStatus(collectResults(metaFuture, "metadata", stats_.metadataQueries,
                                        stats_.avgMetadataTimeMicros),
                         "metadata");
        }

    } else {
        auto runSequential = [&](auto queryFn, const char* name, float weight,
                                 std::atomic<uint64_t>& queryCount,
                                 std::atomic<uint64_t>& avgTime) {
            if (weight <= 0.0f)
                return;

            YAMS_ZONE_SCOPED_N(name);
            auto start = std::chrono::steady_clock::now();
            auto results = queryFn();
            auto end = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            componentTiming[name] = duration;

            if (results) {
                if (!results.value().empty()) {
                    allComponentResults.insert(allComponentResults.end(), results.value().begin(),
                                               results.value().end());
                    contributing.push_back(name);
                }
                queryCount.fetch_add(1, std::memory_order_relaxed);
                avgTime.store(duration, std::memory_order_relaxed);
            } else {
                failed.push_back(name);
            }
        };

        runSequential([&]() { return queryFullText(query, workingConfig.textMaxResults); }, "text",
                      config_.textWeight, stats_.textQueries, stats_.avgTextTimeMicros);

        if (kgStore_) {
            runSequential([&]() { return queryKnowledgeGraph(query, workingConfig.kgMaxResults); },
                          "kg", config_.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros);
        }

        runSequential([&]() { return queryPathTree(query, workingConfig.pathTreeMaxResults); },
                      "path", config_.pathTreeWeight, stats_.pathTreeQueries,
                      stats_.avgPathTreeTimeMicros);

        if (queryEmbedding.has_value() && vectorDb_) {
            runSequential(
                [&]() {
                    return queryVectorIndex(queryEmbedding.value(), workingConfig.vectorMaxResults);
                },
                "vector", config_.vectorWeight, stats_.vectorQueries, stats_.avgVectorTimeMicros);

            runSequential(
                [&]() {
                    return queryEntityVectors(queryEmbedding.value(),
                                              workingConfig.entityVectorMaxResults);
                },
                "entity_vector", config_.entityVectorWeight, stats_.entityVectorQueries,
                stats_.avgEntityVectorTimeMicros);
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

    YAMS_PLOT("component_results_count", static_cast<int64_t>(allComponentResults.size()));

    // Opt-in diagnostic: help explain "0 results" situations by showing per-component
    // hit counts (pre-fusion) and whether an embedding was available.
    if (const char* env = std::getenv("YAMS_SEARCH_DIAG"); env && std::string(env) == "1") {
        const bool embeddingsAvailable = queryEmbedding.has_value();
        spdlog::warn("[search_diag] query='{}' components: contributing={} failed={} timed_out={} "
                     "skipped={} early_termination={} embedding={} pre_fusion_total={} "
                     "weights(text={},vector={},entity_vector={},kg={},path={},tag={},meta={})",
                     query, contributing.size(), failed.size(), timedOut.size(), skipped.size(),
                     usedEarlyTermination ? "yes" : "no", embeddingsAvailable ? "yes" : "no",
                     allComponentResults.size(), workingConfig.textWeight,
                     workingConfig.vectorWeight, workingConfig.entityVectorWeight,
                     workingConfig.kgWeight, workingConfig.pathTreeWeight, workingConfig.tagWeight,
                     workingConfig.metadataWeight);
        // Log per-component timing if available
        if (!componentTiming.empty()) {
            std::string timingStr;
            for (const auto& [name, micros] : componentTiming) {
                if (!timingStr.empty())
                    timingStr += ", ";
                timingStr += fmt::format("{}={}us", name, micros);
            }
            spdlog::warn("[search_diag] timing: {}", timingStr);
        }
    }

    ResultFusion fusion(workingConfig);
    {
        YAMS_ZONE_SCOPED_N("fusion::results");
        response.results = fusion.fuse(allComponentResults);
    }

    if (!response.results.empty()) {
        std::unordered_map<std::string, size_t> extCounts;
        for (const auto& r : response.results) {
            const std::string& path = r.document.filePath;
            auto pos = path.rfind('.');
            std::string ext = (pos != std::string::npos) ? path.substr(pos) : "(no ext)";
            extCounts[ext]++;
        }

        std::vector<std::pair<std::string, size_t>> sortedExts(extCounts.begin(), extCounts.end());
        std::sort(sortedExts.begin(), sortedExts.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        constexpr size_t kMaxFacetValues = 10;
        SearchFacet facet;
        facet.name = "extension";
        facet.displayName = "File Type";
        for (size_t i = 0; i < std::min(sortedExts.size(), kMaxFacetValues); ++i) {
            SearchFacet::FacetValue fv;
            fv.value = sortedExts[i].first;
            fv.display = sortedExts[i].first;
            fv.count = sortedExts[i].second;
            facet.values.push_back(std::move(fv));
        }
        facet.totalValues = sortedExts.size();
        response.facets.push_back(std::move(facet));
    }

    response.timedOutComponents = std::move(timedOut);
    response.failedComponents = std::move(failed);
    response.contributingComponents = std::move(contributing);
    response.skippedComponents = std::move(skipped);
    response.usedEarlyTermination = usedEarlyTermination;
    if (config_.includeComponentTiming) {
        response.componentTimingMicros = std::move(componentTiming);
    }
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

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryPathTree(const std::string& query,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

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

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryFullText(const std::string& query,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_) {
        return results;
    }

    try {
        auto fts5Results = metadataRepo_->search(query, limit, 0);
        if (!fts5Results) {
            spdlog::debug("FTS5 search failed: {}", fts5Results.error().message);
            return results;
        }

        for (size_t rank = 0; rank < fts5Results.value().results.size(); ++rank) {
            const auto& searchResult = fts5Results.value().results[rank];
            const auto& filePath = searchResult.document.filePath;

            auto pruneCategory = magic::getPruneCategory(filePath);
            bool isCodeFile = pruneCategory == magic::PruneCategory::BuildObject ||
                              pruneCategory == magic::PruneCategory::None;

            float scoreMultiplier = isCodeFile ? 1.0f : 0.5f;

            // Note: Symbol-based ranking boost removed (yams-c1ot.2)
            // SymbolEnricher in search_service.cpp provides better KG-based boost

            ComponentResult result;
            result.documentHash = searchResult.document.sha256Hash;
            result.filePath = filePath;
            // FTS5 BM25 scores are negative (more negative = better match)
            // Convert to [0, 1] range: score of -25 → 1.0, score of 0 → 0.0
            // Using sigmoid-like transformation: 1 / (1 + exp(score/5))
            // This preserves ranking order and normalizes to [0, 1]
            float rawScore = static_cast<float>(searchResult.score);
            float normalizedScore = 1.0f / (1.0f + std::exp(rawScore / 5.0f));
            result.score = scoreMultiplier * normalizedScore;
            result.source = "text";
            result.rank = rank;
            result.snippet = searchResult.snippet.empty()
                                 ? std::nullopt
                                 : std::optional<std::string>(searchResult.snippet);

            results.push_back(std::move(result));
        }

        spdlog::debug("Full-text query returned {} results for query: {}", results.size(), query);

        if (const char* env = std::getenv("YAMS_SEARCH_DIAG"); env && std::string(env) == "1") {
            spdlog::warn("[search_diag] text_hits={} limit={} query='{}'", results.size(), limit,
                         query);
        }

    } catch (const std::exception& e) {
        spdlog::warn("Full-text query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryKnowledgeGraph(const std::string& query, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

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

        const auto& aliases = aliasResults.value();
        if (aliases.empty()) {
            return results;
        }

        // Batch fetch all nodes (single DB query instead of N)
        std::vector<std::int64_t> nodeIds;
        nodeIds.reserve(aliases.size());
        for (const auto& alias : aliases) {
            nodeIds.push_back(alias.nodeId);
        }

        auto nodesResult = kgStore_->getNodesByIds(nodeIds);
        if (!nodesResult) {
            spdlog::debug("KG batch node fetch failed: {}", nodesResult.error().message);
            return results;
        }

        // Build nodeId -> node map for quick lookup
        compat::flat_map<std::int64_t, size_t> nodeIndexMap;
        compat::reserve_if_needed(nodeIndexMap, nodesResult.value().size());
        for (size_t idx = 0; idx < nodesResult.value().size(); ++idx) {
            nodeIndexMap[nodesResult.value()[idx].id] = idx;
        }

        // Collect search terms and build term -> alias info mapping
        // This allows us to do a single batch FTS5 query instead of N queries
        const size_t maxAliasesToProcess = std::min(aliases.size(), limit);
        std::vector<std::string> searchTerms;
        searchTerms.reserve(maxAliasesToProcess);

        // Map: searchTerm -> (aliasIndex, nodeIndex) for score attribution
        std::unordered_map<std::string, std::vector<std::pair<size_t, size_t>>> termToAliasInfo;

        for (size_t i = 0; i < maxAliasesToProcess; ++i) {
            const auto& aliasRes = aliases[i];
            auto nodeIt = nodeIndexMap.find(aliasRes.nodeId);
            if (nodeIt == nodeIndexMap.end()) {
                continue;
            }

            const auto& node = nodesResult.value()[nodeIt->second];
            std::string searchTerm = node.label.value_or(node.nodeKey);

            // Track which aliases map to this term (for score attribution)
            termToAliasInfo[searchTerm].emplace_back(i, nodeIt->second);

            // Only add unique terms to the search list
            if (termToAliasInfo[searchTerm].size() == 1) {
                searchTerms.push_back(searchTerm);
            }
        }

        if (searchTerms.empty()) {
            return results;
        }

        // Build batch OR query: "term1" OR "term2" OR "term3"
        // Escape each term by wrapping in quotes (handles special chars)
        std::string batchQuery;
        for (size_t i = 0; i < searchTerms.size(); ++i) {
            if (i > 0) {
                batchQuery += " OR ";
            }
            // Quote the term to handle special characters
            batchQuery += '"';
            for (char c : searchTerms[i]) {
                if (c == '"') {
                    batchQuery += "\"\""; // Escape quotes by doubling
                } else {
                    batchQuery += c;
                }
            }
            batchQuery += '"';
        }

        // Single FTS5 query instead of N queries
        // Request more results since we're batching multiple terms
        const size_t batchLimit = std::min(limit * 3, static_cast<size_t>(200));
        auto docResults = metadataRepo_->search(batchQuery, batchLimit, 0);
        if (!docResults) {
            spdlog::debug("KG batch FTS5 search failed: {}", docResults.error().message);
            return results;
        }

        // Process results and attribute scores back to originating aliases
        std::unordered_map<std::string, size_t> docHashToResultIndex;
        docHashToResultIndex.reserve(limit);

        for (const auto& searchResult : docResults.value().results) {
            if (results.size() >= limit) {
                break;
            }

            const std::string& docHash = searchResult.document.sha256Hash;

            // Find which search term(s) this result likely matched
            // Use the snippet or fall back to first matching term
            float bestScore = 0.0f;
            std::string bestTerm;
            size_t bestNodeIdx = 0;

            for (const auto& term : searchTerms) {
                auto it = termToAliasInfo.find(term);
                if (it == termToAliasInfo.end())
                    continue;

                // Check if this term appears in snippet or path (heuristic for match attribution)
                bool likelyMatch = false;
                if (!searchResult.snippet.empty() &&
                    searchResult.snippet.find(term) != std::string::npos) {
                    likelyMatch = true;
                } else if (searchResult.document.filePath.find(term) != std::string::npos) {
                    likelyMatch = true;
                }

                if (likelyMatch || bestScore == 0.0f) {
                    // Use the highest-scoring alias for this term
                    for (const auto& [aliasIdx, nodeIdx] : it->second) {
                        float aliasScore = aliases[aliasIdx].score;
                        if (aliasScore > bestScore) {
                            bestScore = aliasScore;
                            bestTerm = term;
                            bestNodeIdx = nodeIdx;
                        }
                    }
                }
            }

            // If no term matched heuristically, use first alias's score
            if (bestScore == 0.0f && !termToAliasInfo.empty()) {
                const auto& firstTerm = searchTerms[0];
                const auto& firstInfo = termToAliasInfo[firstTerm][0];
                bestScore = aliases[firstInfo.first].score;
                bestTerm = firstTerm;
                bestNodeIdx = firstInfo.second;
            }

            auto existingIt = docHashToResultIndex.find(docHash);
            if (existingIt != docHashToResultIndex.end()) {
                // Boost existing result
                results[existingIt->second].score += bestScore * 0.3f;
                continue;
            }

            const auto& node = nodesResult.value()[bestNodeIdx];

            ComponentResult result;
            result.documentHash = docHash;
            result.filePath = searchResult.document.filePath;
            result.score = bestScore * 0.8f;
            result.source = "kg";
            result.rank = results.size();
            result.snippet = searchResult.snippet.empty()
                                 ? std::optional<std::string>(bestTerm)
                                 : std::optional<std::string>(searchResult.snippet);
            result.debugInfo["node_id"] = std::to_string(node.id);
            result.debugInfo["node_key"] = node.nodeKey;
            if (node.type.has_value()) {
                result.debugInfo["node_type"] = node.type.value();
            }

            docHashToResultIndex[docHash] = results.size();
            results.push_back(std::move(result));
        }

        spdlog::debug("KG query returned {} document results for query: {} (batch: {} terms)",
                      results.size(), query, searchTerms.size());

    } catch (const std::exception& e) {
        spdlog::warn("KG query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    // Vector search using VectorDatabase (sqlite-vec)
    // sqlite-vec provides efficient cosine similarity search via vec_distance_cosine()
    // Benchmarks show <30ms for 100K vectors which is acceptable for most use cases

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::VectorSearchParams params;
        params.k = limit;
        params.similarity_threshold = config_.similarityThreshold;

        auto vectorRecords = vectorDb_->search(embedding, params);

        if (vectorRecords.empty()) {
            return results;
        }

        // Batch fetch document info for all hashes (single DB query instead of N)
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(vectorRecords.size());
            for (const auto& vr : vectorRecords) {
                hashes.push_back(vr.document_hash);
            }

            auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
            if (docMapResult) {
                compat::reserve_if_needed(hashToPath, docMapResult.value().size());
                for (const auto& [hash, docInfo] : docMapResult.value()) {
                    hashToPath[hash] = docInfo.filePath;
                }
            }
        }

        for (size_t rank = 0; rank < vectorRecords.size(); ++rank) {
            const auto& vr = vectorRecords[rank];

            ComponentResult result;
            result.documentHash = vr.document_hash;
            result.score = vr.relevance_score;
            result.source = "vector";
            result.rank = rank;

            if (auto it = hashToPath.find(vr.document_hash); it != hashToPath.end()) {
                result.filePath = it->second;
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Vector search returned {} results", results.size());

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryEntityVectors(const std::vector<float>& embedding, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    // Entity vector search finds semantically similar symbols (functions, classes, etc.)
    // This enables queries like "authentication handler" to find AuthMiddleware::handleRequest

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::EntitySearchParams params;
        params.k = limit;
        params.similarity_threshold = config_.similarityThreshold;
        params.include_embeddings = false; // Don't need embeddings in results

        auto entityRecords = vectorDb_->searchEntities(embedding, params);

        if (entityRecords.empty()) {
            return results;
        }

        // Batch fetch document info for all hashes (single DB query instead of N)
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(entityRecords.size());
            for (const auto& er : entityRecords) {
                if (!er.document_hash.empty()) {
                    hashes.push_back(er.document_hash);
                }
            }

            if (!hashes.empty()) {
                auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
                if (docMapResult) {
                    compat::reserve_if_needed(hashToPath, docMapResult.value().size());
                    for (const auto& [hash, docInfo] : docMapResult.value()) {
                        hashToPath[hash] = docInfo.filePath;
                    }
                }
            }
        }

        for (size_t rank = 0; rank < entityRecords.size(); ++rank) {
            const auto& er = entityRecords[rank];

            ComponentResult result;
            result.documentHash = er.document_hash;
            result.score = er.relevance_score;
            result.source = "entity_vector";
            result.rank = rank;

            // Use file_path from entity record, or look up from metadata
            if (!er.file_path.empty()) {
                result.filePath = er.file_path;
            } else if (auto it = hashToPath.find(er.document_hash); it != hashToPath.end()) {
                result.filePath = it->second;
            }

            // Create snippet from entity info: qualified_name or node_key
            if (!er.qualified_name.empty()) {
                result.snippet = er.qualified_name;
            } else if (!er.node_key.empty()) {
                result.snippet = er.node_key;
            }

            // Debug info for entity search results
            result.debugInfo["node_key"] = er.node_key;
            if (!er.node_type.empty()) {
                result.debugInfo["node_type"] = er.node_type;
            }
            if (!er.qualified_name.empty()) {
                result.debugInfo["qualified_name"] = er.qualified_name;
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Entity vector search returned {} results", results.size());

    } catch (const std::exception& e) {
        spdlog::warn("Entity vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryTags(const std::vector<std::string>& tags, bool matchAll, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

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

            // Score: matchAll=1.0, matchAny uses position-based decay
            // (avoids N database calls to fetch tags per document)
            result.score = matchAll ? 1.0f : 1.0f / (1.0f + 0.1f * static_cast<float>(rank));

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
    results.reserve(limit);

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
                           std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                           std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                           const SearchEngineConfig& config)
    : pImpl_(std::make_unique<Impl>(std::move(metadataRepo), std::move(vectorDb),
                                    std::move(embeddingGen), std::move(kgStore), config)) {}

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

void SearchEngine::setExecutor(std::optional<boost::asio::any_io_executor> executor) {
    pImpl_->setExecutor(std::move(executor));
}

// Factory function
std::unique_ptr<SearchEngine>
createSearchEngine(std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo,
                   std::shared_ptr<vector::VectorDatabase> vectorDb,
                   std::shared_ptr<vector::EmbeddingGenerator> embeddingGen,
                   std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore,
                   const SearchEngineConfig& config) {
    return std::make_unique<SearchEngine>(std::move(metadataRepo), std::move(vectorDb),
                                          std::move(embeddingGen), std::move(kgStore), config);
}

} // namespace yams::search
