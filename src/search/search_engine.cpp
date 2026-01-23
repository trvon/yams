#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/knowledge_graph_store.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <future>
#include <set>
#include <sstream>
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
        // No executor available - run synchronously on current thread.
        // NOTE: std::async futures block in destructor, so discarding one
        // would actually block anyway. Better to be explicit about sync execution.
        task();
    }
    return future;
}

std::vector<std::string> tokenizeLower(const std::string& input) {
    std::string normalized = input;
    for (char& c : normalized) {
        if (c == '\\') {
            c = '/';
        }
    }
    std::vector<std::string> tokens;
    std::string current;
    for (unsigned char c : normalized) {
        if (std::isalnum(c)) {
            current.push_back(static_cast<char>(std::tolower(c)));
        } else {
            if (!current.empty()) {
                tokens.push_back(std::move(current));
                current.clear();
            }
        }
    }
    if (!current.empty()) {
        tokens.push_back(std::move(current));
    }
    return tokens;
}

std::string truncateSnippet(const std::string& content, size_t maxLen) {
    if (content.empty()) {
        return {};
    }
    if (content.size() <= maxLen) {
        return content;
    }
    std::string out = content.substr(0, maxLen);
    out.append("...");
    return out;
}

std::string_view::size_type ci_find(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return 0;
    }
    if (needle.size() > haystack.size()) {
        return std::string_view::npos;
    }

    for (std::string_view::size_type i = 0; i <= haystack.size() - needle.size(); ++i) {
        bool match = true;
        for (std::string_view::size_type j = 0; j < needle.size(); ++j) {
            unsigned char c1 = static_cast<unsigned char>(haystack[i + j]);
            unsigned char c2 = static_cast<unsigned char>(needle[j]);
            if (std::tolower(c1) != std::tolower(c2)) {
                match = false;
                break;
            }
        }
        if (match) {
            return i;
        }
    }
    return std::string_view::npos;
}

bool cpuSupportsSimdSearch() {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#if defined(__clang__) || defined(__GNUC__)
    return __builtin_cpu_supports("avx2") || __builtin_cpu_supports("sse2");
#else
    return false;
#endif
#elif defined(__aarch64__) || defined(__ARM_NEON)
    return true;
#else
    return false;
#endif
}

bool containsFast(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    if (needle.size() > haystack.size()) {
        return false;
    }
    static const bool useSimd = cpuSupportsSimdSearch();
    if (!useSimd) {
        return haystack.find(needle) != std::string_view::npos;
    }
    const char* data = haystack.data();
    const size_t haySize = haystack.size();
    const size_t needleSize = needle.size();
    const char first = needle.front();

    const char* scan = data;
    size_t remaining = haySize;
    while (remaining >= needleSize) {
        const void* found = std::memchr(scan, first, remaining - needleSize + 1);
        if (!found) {
            return false;
        }
        const char* candidate = static_cast<const char*>(found);
        if (std::memcmp(candidate, needle.data(), needleSize) == 0) {
            return true;
        }
        const size_t consumed = static_cast<size_t>(candidate - scan) + 1;
        scan = candidate + 1;
        remaining -= consumed;
    }
    return false;
}

float normalizedBm25Score(double rawScore, float divisor, double minScore, double maxScore) {
    if (maxScore > minScore) {
        const double norm = (rawScore - minScore) / (maxScore - minScore);
        return std::clamp(static_cast<float>(1.0 - norm), 0.0f, 1.0f);
    }
    return std::clamp(static_cast<float>(-rawScore) / divisor, 0.0f, 1.0f);
}

float filenamePathBoost(const std::string& query, const std::string& filePath,
                        const std::string& fileName) {
    const auto queryTokens = tokenizeLower(query);
    if (queryTokens.empty()) {
        return 1.0f;
    }

    const auto nameTokens = tokenizeLower(fileName);
    const auto pathTokens = tokenizeLower(filePath);
    if (nameTokens.empty() && pathTokens.empty()) {
        return 1.0f;
    }

    std::unordered_set<std::string> nameSet(nameTokens.begin(), nameTokens.end());
    std::unordered_set<std::string> pathSet(pathTokens.begin(), pathTokens.end());

    std::size_t nameMatches = 0;
    std::size_t pathMatches = 0;
    for (const auto& tok : queryTokens) {
        if (nameSet.count(tok)) {
            nameMatches++;
        } else if (pathSet.count(tok)) {
            pathMatches++;
        } else {
            for (const auto& nameTok : nameTokens) {
                if (nameTok.rfind(tok, 0) == 0) {
                    nameMatches++;
                    break;
                }
            }
            if (nameMatches == 0) {
                for (const auto& pathTok : pathTokens) {
                    if (pathTok.rfind(tok, 0) == 0) {
                        pathMatches++;
                        break;
                    }
                }
            }
        }
    }

    if (nameMatches > 0) {
        return 1.0f + std::min(2.0f, 0.5f + static_cast<float>(nameMatches) * 0.5f);
    }
    if (pathMatches > 0) {
        return 1.0f + std::min(1.0f, 0.25f + static_cast<float>(pathMatches) * 0.25f);
    }
    return 1.0f;
}

enum class QueryIntent { Code, Path, Prose, Mixed };

constexpr const char* queryIntentToString(QueryIntent intent) {
    switch (intent) {
        case QueryIntent::Code:
            return "code";
        case QueryIntent::Path:
            return "path";
        case QueryIntent::Prose:
            return "prose";
        case QueryIntent::Mixed:
            return "mixed";
    }
    return "mixed";
}

bool hasCamelCase(const std::string& input) {
    bool hasLower = false;
    bool hasUpper = false;
    for (unsigned char c : input) {
        if (std::islower(c)) {
            hasLower = true;
        } else if (std::isupper(c)) {
            hasUpper = true;
        }
        if (hasLower && hasUpper) {
            return true;
        }
    }
    return false;
}

bool hasFileExtension(std::string_view input) {
    const auto dot = input.rfind('.');
    if (dot == std::string_view::npos || dot == 0 || dot + 1 >= input.size()) {
        return false;
    }
    const auto ext = input.substr(dot + 1);
    if (ext.size() > 5) {
        return false;
    }
    for (unsigned char c : ext) {
        if (!std::isalnum(c)) {
            return false;
        }
    }
    return true;
}

QueryIntent detectQueryIntent(const std::string& query) {
    if (query.empty()) {
        return QueryIntent::Mixed;
    }

    const bool hasPathSeparator =
        query.find('/') != std::string::npos || query.find('\\') != std::string::npos;
    const bool hasPathPrefix = query.rfind("./", 0) == 0 || query.rfind("../", 0) == 0;
    const bool hasCodeSig =
        query.find("::") != std::string::npos || query.find("->") != std::string::npos ||
        query.find("#") != std::string::npos || query.find("_") != std::string::npos;
    const bool hasExt = hasFileExtension(query);
    const bool hasCamel = hasCamelCase(query);

    if (hasPathSeparator || hasPathPrefix) {
        return QueryIntent::Path;
    }

    if (hasCodeSig || hasCamel || hasExt) {
        return QueryIntent::Code;
    }

    const auto tokens = tokenizeLower(query);
    if (tokens.size() >= 3) {
        return QueryIntent::Prose;
    }

    return QueryIntent::Mixed;
}

void applyIntentWeights(SearchEngineConfig& config, QueryIntent intent) {
    auto scale = [](float& weight, float factor) {
        weight = std::clamp(weight * factor, 0.0f, 1.0f);
    };

    switch (intent) {
        case QueryIntent::Path:
            scale(config.pathTreeWeight, 1.8f);
            scale(config.textWeight, 0.8f);
            scale(config.vectorWeight, 0.7f);
            scale(config.entityVectorWeight, 0.8f);
            scale(config.kgWeight, 0.8f);
            scale(config.tagWeight, 0.9f);
            break;
        case QueryIntent::Code:
            scale(config.pathTreeWeight, 1.5f);
            scale(config.entityVectorWeight, 1.5f);
            scale(config.textWeight, 0.8f);
            scale(config.vectorWeight, 0.7f);
            scale(config.kgWeight, 0.9f);
            break;
        case QueryIntent::Prose:
            scale(config.textWeight, 1.2f);
            scale(config.vectorWeight, 1.2f);
            scale(config.pathTreeWeight, 0.6f);
            scale(config.entityVectorWeight, 0.6f);
            scale(config.kgWeight, 0.8f);
            break;
        case QueryIntent::Mixed:
            break;
    }
}

std::vector<std::string> tokenizeKgQuery(std::string_view query) {
    static const std::unordered_set<std::string> kStopwords = {
        "the", "a",   "an",   "and", "or",   "not",  "to",    "of",   "in",
        "on",  "for", "with", "by",  "from", "is",   "are",   "was",  "were",
        "be",  "as",  "at",   "it",  "this", "that", "these", "those"};

    std::vector<std::string> tokens = tokenizeLower(std::string(query));
    tokens.erase(std::remove_if(tokens.begin(), tokens.end(),
                                [](const std::string& token) {
                                    return token.size() < 3 ||
                                           kStopwords.find(token) != kStopwords.end();
                                }),
                 tokens.end());
    return tokens;
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

    switch (config_.fusionStrategy) {
        case SearchEngineConfig::FusionStrategy::WEIGHTED_SUM:
            return fuseWeightedSum(componentResults);
        case SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK:
            return fuseReciprocalRank(componentResults);
        case SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL:
            return fuseWeightedReciprocal(componentResults);
        case SearchEngineConfig::FusionStrategy::COMB_MNZ:
            return fuseCombMNZ(componentResults);
    }
    return fuseCombMNZ(componentResults); // Default fallback
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
        const double rank = static_cast<double>(comp.rank) + 1.0; // RRF uses 1-based ranks
        return 1.0 / (k + rank);
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
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double scoreBoost = 1.0 + std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
        return weight * rrfScore * scoreBoost;
    });
}

std::vector<SearchResult> ResultFusion::fuseCombMNZ(const std::vector<ComponentResult>& results) {
    struct Accumulator {
        double score = 0.0;
        size_t componentCount = 0;
        std::string filePath;
        std::string snippet;
    };
    std::unordered_map<std::string, Accumulator> accumMap;
    accumMap.reserve(results.size());

    const float k = config_.rrfK;

    for (const auto& comp : results) {
        auto& acc = accumMap[comp.documentHash];

        if (acc.componentCount == 0) {
            acc.filePath = comp.filePath;
            if (comp.snippet.has_value()) {
                acc.snippet = comp.snippet.value();
            }
        }

        float weight = getComponentWeight(comp.source);
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double contribution = weight * rrfScore;

        acc.score += contribution;
        acc.componentCount++;
    }

    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(accumMap.size());

    for (auto& entry : accumMap) {
        SearchResult r;
        r.document.sha256Hash = entry.first;
        r.document.filePath = std::move(entry.second.filePath);
        r.score = static_cast<float>(entry.second.score * entry.second.componentCount);
        r.snippet = std::move(entry.second.snippet);
        fusedResults.push_back(std::move(r));
    }

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

float ResultFusion::getComponentWeight(ComponentResult::Source source) const {
    switch (source) {
        case ComponentResult::Source::Text:
            return config_.textWeight;
        case ComponentResult::Source::PathTree:
            return config_.pathTreeWeight;
        case ComponentResult::Source::KnowledgeGraph:
            return config_.kgWeight;
        case ComponentResult::Source::Vector:
            return config_.vectorWeight;
        case ComponentResult::Source::EntityVector:
            return config_.entityVectorWeight;
        case ComponentResult::Source::Tag:
            return config_.tagWeight;
        case ComponentResult::Source::Metadata:
            return config_.metadataWeight;
        case ComponentResult::Source::Symbol:
        case ComponentResult::Source::Unknown:
            return 0.0f;
    }
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

    void setConceptExtractor(EntityExtractionFunc extractor) {
        conceptExtractor_ = std::move(extractor);
    }

    void setReranker(std::shared_ptr<IReranker> reranker) { reranker_ = std::move(reranker); }

private:
    Result<SearchResponse> searchInternal(const std::string& query, const SearchParams& params);

    Result<std::vector<ComponentResult>> queryFullText(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryKnowledgeGraph(const std::string& query,
                                                             size_t limit);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding,
                                                          size_t limit);
    Result<std::vector<ComponentResult>>
    queryVectorIndex(const std::vector<float>& embedding, size_t limit,
                     const std::unordered_set<std::string>& candidates);
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
    EntityExtractionFunc conceptExtractor_; // GLiNER concept extractor (optional)
    std::shared_ptr<IReranker> reranker_;   // Cross-encoder reranker (optional)
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

    // Start embedding generation as async task (runs in parallel with Tier 1 components)
    std::optional<std::vector<float>> queryEmbedding;
    const bool needsEmbedding = (config_.vectorWeight > 0.0f || config_.entityVectorWeight > 0.0f);
    std::future<std::vector<float>> embeddingFuture;
    std::chrono::steady_clock::time_point embStart;
    if (needsEmbedding && embeddingGen_) {
        embStart = std::chrono::steady_clock::now();
        // Launch embedding generation in parallel - will be awaited when Tier 2/vector needs it
        embeddingFuture = postWork(
            [this, &query]() {
                YAMS_ZONE_SCOPED_N("embedding::generate_async");
                return embeddingGen_->generateEmbedding(query);
            },
            executor_);
    }

    std::future<Result<QueryConceptResult>> conceptFuture;
    std::chrono::steady_clock::time_point conceptStart;
    if (conceptExtractor_) {
        conceptStart = std::chrono::steady_clock::now();
        conceptFuture = postWork(
            [this, &query]() {
                YAMS_ZONE_SCOPED_N("concepts::extract_async");
                return conceptExtractor_(query, {});
            },
            executor_);
    }

    // Helper to await embedding result when needed (called before vector search)
    auto awaitEmbedding = [&]() {
        if (embeddingFuture.valid() && !queryEmbedding.has_value()) {
            try {
                auto embResult = embeddingFuture.get();
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
    };

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

    spdlog::debug("Search limit optimization: userLimit={}, componentCap={}", userLimit,
                  componentCap);

    const QueryIntent intent = detectQueryIntent(query);
    applyIntentWeights(workingConfig, intent);
    spdlog::debug("Query intent: {}", queryIntentToString(intent));

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

    if (config_.enableParallelExecution) [[likely]] {
        YAMS_ZONE_SCOPED_N("search_engine::fanout_parallel");
        std::future<Result<std::vector<ComponentResult>>> textFuture;
        std::future<Result<std::vector<ComponentResult>>> kgFuture;
        std::future<Result<std::vector<ComponentResult>>> pathFuture;
        std::future<Result<std::vector<ComponentResult>>> vectorFuture;
        std::future<Result<std::vector<ComponentResult>>> entityVectorFuture;
        std::future<Result<std::vector<ComponentResult>>> tagFuture;
        std::future<Result<std::vector<ComponentResult>>> metaFuture;

        auto schedule = [&](const char* name, float weight, std::atomic<uint64_t>& queryCount,
                            std::atomic<uint64_t>& avgTime,
                            auto&& fn) -> std::future<Result<std::vector<ComponentResult>>> {
            if (weight <= 0.0f) {
                return {};
            }
            return postWork(std::forward<decltype(fn)>(fn), executor_);
        };

        auto collectIf = [&](std::future<Result<std::vector<ComponentResult>>>& future,
                             const char* name, std::atomic<uint64_t>& queryCount,
                             std::atomic<uint64_t>& avgTime) {
            handleStatus(collectResults(future, name, queryCount, avgTime), name);
        };

        if (config_.enableTieredExecution) {
            // === TIERED EXECUTION ===
            // Tier 1: Fast text-based components (FTS5 + path_tree)
            // Tier 2: Slower semantic components (vector) - only if Tier 1 insufficient
            YAMS_ZONE_SCOPED_N("search_engine::tiered_execution");

            // --- TIER 1: Text + Path (fast, high precision) ---
            textFuture = schedule("text", config_.textWeight, stats_.textQueries,
                                  stats_.avgTextTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::text");
                                      return queryFullText(query, workingConfig.textMaxResults);
                                  });

            pathFuture = schedule("path", config_.pathTreeWeight, stats_.pathTreeQueries,
                                  stats_.avgPathTreeTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::path");
                                      return queryPathTree(query, workingConfig.pathTreeMaxResults);
                                  });

            // Tags and metadata are also fast, run in Tier 1
            if (!params.tags.empty()) {
                tagFuture = schedule("tag", config_.tagWeight, stats_.tagQueries,
                                     stats_.avgTagTimeMicros, [this, &params, &workingConfig]() {
                                         YAMS_ZONE_SCOPED_N("component::tag");
                                         return queryTags(params.tags, params.matchAllTags,
                                                          workingConfig.tagMaxResults);
                                     });
            }

            metaFuture =
                schedule("metadata", config_.metadataWeight, stats_.metadataQueries,
                         stats_.avgMetadataTimeMicros, [this, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(params, workingConfig.metadataMaxResults);
                         });

            // Collect Tier 1 results
            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);

            // Extract Tier 1 candidate document hashes for narrowing vector search
            std::unordered_set<std::string> tier1Candidates;
            for (const auto& r : allComponentResults) {
                tier1Candidates.insert(r.documentHash);
            }

            spdlog::debug("Tiered search: {} unique candidates from Tier 1",
                          tier1Candidates.size());

            // --- TIER 2: Vector search NARROWED to Tier 1 candidates ---
            // Always run vector search (never skip), but filter to Tier 1 candidates when
            // appropriate
            YAMS_ZONE_SCOPED_N("search_engine::tier2_semantic");

            // Await embedding result (was started in parallel with Tier 1)
            awaitEmbedding();

            // Decide whether to narrow vector search to Tier 1 candidates
            // Narrow if: config enabled AND Tier 1 has enough candidates
            const bool shouldNarrow = config_.tieredNarrowVectorSearch &&
                                      tier1Candidates.size() >= config_.tieredMinCandidates;

            if (queryEmbedding.has_value() && vectorDb_) {
                vectorFuture = schedule(
                    "vector", config_.vectorWeight, stats_.vectorQueries,
                    stats_.avgVectorTimeMicros,
                    [this, &queryEmbedding, &workingConfig, &tier1Candidates, shouldNarrow]() {
                        YAMS_ZONE_SCOPED_N("component::vector");
                        if (shouldNarrow) {
                            return queryVectorIndex(queryEmbedding.value(),
                                                    workingConfig.vectorMaxResults,
                                                    tier1Candidates); // Narrowed search
                        } else {
                            return queryVectorIndex(queryEmbedding.value(),
                                                    workingConfig.vectorMaxResults); // Full search
                        }
                    });

                entityVectorFuture = schedule(
                    "entity_vector", config_.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::entity_vector");
                        return queryEntityVectors(queryEmbedding.value(),
                                                  workingConfig.entityVectorMaxResults);
                    });
            }

            if (kgStore_) {
                kgFuture =
                    schedule("kg", config_.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                             [this, &query, &workingConfig]() {
                                 YAMS_ZONE_SCOPED_N("component::kg");
                                 return queryKnowledgeGraph(query, workingConfig.kgMaxResults);
                             });
            }

            // Collect Tier 2 results (always collect, never skip)
            collectIf(vectorFuture, "vector", stats_.vectorQueries, stats_.avgVectorTimeMicros);
            collectIf(entityVectorFuture, "entity_vector", stats_.entityVectorQueries,
                      stats_.avgEntityVectorTimeMicros);
            collectIf(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros);
        } else {
            // === FLAT PARALLEL EXECUTION (original behavior) ===
            // All components run in parallel
            textFuture = schedule("text", config_.textWeight, stats_.textQueries,
                                  stats_.avgTextTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::text");
                                      return queryFullText(query, workingConfig.textMaxResults);
                                  });

            if (kgStore_) {
                kgFuture =
                    schedule("kg", config_.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                             [this, &query, &workingConfig]() {
                                 YAMS_ZONE_SCOPED_N("component::kg");
                                 return queryKnowledgeGraph(query, workingConfig.kgMaxResults);
                             });
            }

            pathFuture = schedule("path", config_.pathTreeWeight, stats_.pathTreeQueries,
                                  stats_.avgPathTreeTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::path");
                                      return queryPathTree(query, workingConfig.pathTreeMaxResults);
                                  });

            // NOTE: Vector components scheduled below after embedding is ready
            // This allows text/kg/path to run in parallel with embedding generation

            if (!params.tags.empty()) {
                tagFuture = schedule("tag", config_.tagWeight, stats_.tagQueries,
                                     stats_.avgTagTimeMicros, [this, &params, &workingConfig]() {
                                         YAMS_ZONE_SCOPED_N("component::tag");
                                         return queryTags(params.tags, params.matchAllTags,
                                                          workingConfig.tagMaxResults);
                                     });
            }

            metaFuture =
                schedule("metadata", config_.metadataWeight, stats_.metadataQueries,
                         stats_.avgMetadataTimeMicros, [this, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(params, workingConfig.metadataMaxResults);
                         });

            // Await embedding (ran in parallel with text/kg/path above) then schedule vector
            awaitEmbedding();
            if (queryEmbedding.has_value() && vectorDb_) {
                vectorFuture =
                    schedule("vector", config_.vectorWeight, stats_.vectorQueries,
                             stats_.avgVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                                 YAMS_ZONE_SCOPED_N("component::vector");
                                 return queryVectorIndex(queryEmbedding.value(),
                                                         workingConfig.vectorMaxResults);
                             });

                entityVectorFuture = schedule(
                    "entity_vector", config_.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::entity_vector");
                        return queryEntityVectors(queryEmbedding.value(),
                                                  workingConfig.entityVectorMaxResults);
                    });
            }

            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(vectorFuture, "vector", stats_.vectorQueries, stats_.avgVectorTimeMicros);
            collectIf(entityVectorFuture, "entity_vector", stats_.entityVectorQueries,
                      stats_.avgEntityVectorTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);
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

        // Await embedding (ran in parallel with sequential components above)
        awaitEmbedding();
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
                     "skipped={} embedding={} pre_fusion_total={} "
                     "weights(text={},vector={},entity_vector={},kg={},path={},tag={},meta={})",
                     query, contributing.size(), failed.size(), timedOut.size(), skipped.size(),
                     embeddingsAvailable ? "yes" : "no", allComponentResults.size(),
                     workingConfig.textWeight, workingConfig.vectorWeight,
                     workingConfig.entityVectorWeight, workingConfig.kgWeight,
                     workingConfig.pathTreeWeight, workingConfig.tagWeight,
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

    std::vector<QueryConcept> concepts;
    if (conceptFuture.valid() &&
        conceptFuture.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        auto conceptEnd = std::chrono::steady_clock::now();
        componentTiming["concepts"] =
            std::chrono::duration_cast<std::chrono::microseconds>(conceptEnd - conceptStart)
                .count();
        auto conceptResult = conceptFuture.get();
        if (conceptResult) {
            const auto& extracted = conceptResult.value().concepts;
            concepts.reserve(extracted.size());
            for (const auto& conceptItem : extracted) {
                if (conceptItem.confidence >= workingConfig.conceptMinConfidence) {
                    concepts.push_back(conceptItem);
                }
            }
            if (concepts.size() > workingConfig.conceptMaxCount) {
                concepts.resize(workingConfig.conceptMaxCount);
            }
        }
    }

    // Cross-encoder reranking: second-stage ranking for improved relevance
    const bool rerankAvailable = reranker_ && reranker_->isReady();
    if (!config_.enableReranking && rerankAvailable) {
        spdlog::debug("[reranker] Auto-enabled (model detected)");
    }
    if (rerankAvailable && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("reranking");
        const size_t rerankWindow = std::min(config_.rerankTopK, response.results.size());

        bool skipRerank = false;
        if (rerankWindow >= 2 && config_.rerankScoreGapThreshold > 0.0f) {
            const double scoreGap = response.results[0].score - response.results[1].score;
            if (scoreGap >= static_cast<double>(config_.rerankScoreGapThreshold)) {
                spdlog::debug("[reranker] Skipping rerank (score gap {:.4f} >= {:.4f})", scoreGap,
                              config_.rerankScoreGapThreshold);
                skipRerank = true;
            }
        }

        if (!skipRerank) {
            // Extract document snippets for reranking
            std::vector<std::string> snippets;
            std::vector<size_t> rerankIndices;
            snippets.reserve(rerankWindow);
            rerankIndices.reserve(rerankWindow);
            for (size_t i = 0; i < rerankWindow; ++i) {
                // Only rerank when we have real text; skip file-path-only samples.
                if (!response.results[i].snippet.empty()) {
                    snippets.push_back(truncateSnippet(response.results[i].snippet,
                                                       config_.rerankSnippetMaxChars));
                    rerankIndices.push_back(i);
                } else {
                    spdlog::debug("[reranker] Skipping doc {} (no snippet available)", i);
                }
            }

            if (!snippets.empty()) {
                auto rerankResult = reranker_->scoreDocuments(query, snippets);
                if (rerankResult) {
                    const auto& scores = rerankResult.value();
                    spdlog::debug("[reranker] Reranked {} documents", scores.size());

                    // Apply reranker scores to eligible results.
                    for (size_t i = 0; i < scores.size() && i < rerankIndices.size(); ++i) {
                        const size_t idx = rerankIndices[i];
                        double originalScore = response.results[idx].score;
                        double rerankScore = static_cast<double>(scores[i]);

                        if (config_.rerankReplaceScores) {
                            // Replace entirely with reranker score
                            response.results[idx].score = rerankScore;
                        } else {
                            // Blend: final = rerank * weight + original * (1 - weight)
                            response.results[idx].score =
                                rerankScore * config_.rerankWeight +
                                originalScore * (1.0 - config_.rerankWeight);
                        }
                        response.results[idx].rerankerScore = rerankScore;
                    }

                    // Re-sort by new scores (only the top window needs sorting)
                    std::sort(response.results.begin(),
                              response.results.begin() + static_cast<ptrdiff_t>(rerankWindow),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });

                    contributing.push_back("reranker");
                } else {
                    spdlog::warn("[reranker] Reranking failed: {}", rerankResult.error().message);
                }
            } else {
                spdlog::debug("[reranker] Skipping rerank: no snippets available");
            }
        }
    } else if (config_.enableReranking && !rerankAvailable) {
        spdlog::debug("[reranker] Unavailable; falling back to fused scores");
    }

    if (!concepts.empty() && workingConfig.conceptBoostWeight > 0.0f &&
        workingConfig.conceptMaxBoost > 0.0f && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("concepts::boost");
        std::vector<std::string> conceptTerms;
        conceptTerms.reserve(concepts.size());
        for (const auto& conceptItem : concepts) {
            if (conceptItem.text.empty()) {
                continue;
            }
            std::string lowered = conceptItem.text;
            std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            conceptTerms.push_back(std::move(lowered));
        }
        if (!conceptTerms.empty()) {
            std::sort(conceptTerms.begin(), conceptTerms.end());
            conceptTerms.erase(std::unique(conceptTerms.begin(), conceptTerms.end()),
                               conceptTerms.end());
        }

        if (!conceptTerms.empty()) {
            const size_t totalResults = response.results.size();
            const size_t scanLimit = std::min(workingConfig.conceptMaxScanResults, totalResults);
            if (scanLimit > 0) {
                std::vector<uint32_t> matchCounts(scanLimit, 0);
                const size_t minChunkSize = std::max<size_t>(1, config_.minChunkSizeForParallel);
                const size_t maxThreads = std::max<size_t>(1, std::thread::hardware_concurrency());
                const size_t chunkTarget = (scanLimit + minChunkSize - 1) / minChunkSize;
                const size_t numThreads = std::min(maxThreads, std::max<size_t>(1, chunkTarget));
                const bool useParallelBoost = numThreads > 1;

                auto computeMatches = [&](size_t start, size_t end) {
                    std::vector<uint32_t> matches;
                    matches.reserve(end - start);
                    for (size_t idx = start; idx < end; ++idx) {
                        const auto& result = response.results[idx];
                        uint32_t count = 0;
                        for (const auto& term : conceptTerms) {
                            if (!term.empty() && (containsFast(result.snippet, term) ||
                                                  containsFast(result.document.fileName, term))) {
                                count++;
                            }
                        }
                        matches.push_back(count);
                    }
                    return matches;
                };

                if (useParallelBoost) {
                    const size_t chunkSize = (scanLimit + numThreads - 1) / numThreads;
                    std::vector<std::future<std::vector<uint32_t>>> futures;
                    futures.reserve(numThreads);
                    for (size_t i = 0; i < numThreads; ++i) {
                        const size_t start = i * chunkSize;
                        const size_t end = std::min(start + chunkSize, scanLimit);
                        if (start >= end) {
                            break;
                        }
                        futures.push_back(
                            std::async(std::launch::async, [start, end, &computeMatches]() {
                                return computeMatches(start, end);
                            }));
                    }

                    size_t offset = 0;
                    for (auto& future : futures) {
                        auto matches = future.get();
                        for (size_t i = 0; i < matches.size(); ++i) {
                            matchCounts[offset + i] = matches[i];
                        }
                        offset += matches.size();
                    }
                } else {
                    auto matches = computeMatches(0, scanLimit);
                    for (size_t i = 0; i < matches.size(); ++i) {
                        matchCounts[i] = matches[i];
                    }
                }

                bool boosted = false;
                float boostBudget = workingConfig.conceptMaxBoost;
                for (size_t i = 0; i < scanLimit; ++i) {
                    if (boostBudget <= 0.0f) {
                        break;
                    }
                    const uint32_t matchCount = matchCounts[i];
                    if (matchCount == 0) {
                        continue;
                    }
                    const float desiredBoost =
                        workingConfig.conceptBoostWeight * static_cast<float>(matchCount);
                    const float appliedBoost = std::min(desiredBoost, boostBudget);
                    response.results[i].score *= (1.0f + appliedBoost);
                    boostBudget -= appliedBoost;
                    boosted = true;
                }

                if (boosted) {
                    std::sort(response.results.begin(), response.results.end(),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });
                }
            }
        }
    }

    if (!response.results.empty()) {
        std::unordered_map<std::string_view, size_t> extCounts;
        for (const auto& r : response.results) {
            std::string_view path = r.document.filePath;
            auto pos = path.rfind('.');
            std::string_view ext = (pos != std::string_view::npos) ? path.substr(pos) : "no ext";
            extCounts[ext]++;
        }

        std::vector<std::pair<std::string, size_t>> sortedExts;
        sortedExts.reserve(extCounts.size());
        for (auto& kv : extCounts) {
            sortedExts.emplace_back(std::string(kv.first), kv.second);
        }
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
    response.usedEarlyTermination = false;
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

            std::string_view pathView = doc.filePath;
            std::string_view queryView = query;

            size_t pos = ci_find(pathView, queryView);

            if (pos != std::string_view::npos) {
                float positionScore = 1.0f - (static_cast<float>(pos) / pathView.length());
                float lengthScore = static_cast<float>(queryView.length()) / pathView.length();
                result.score = (positionScore * 0.3f + lengthScore * 0.7f);
            } else {
                result.score = 0.5f;
            }

            result.source = ComponentResult::Source::PathTree;
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

        double minBm25 = 0.0;
        double maxBm25 = 0.0;
        bool bm25RangeInitialized = false;
        for (const auto& sr : fts5Results.value().results) {
            double score = sr.score;
            if (!bm25RangeInitialized) {
                minBm25 = score;
                maxBm25 = score;
                bm25RangeInitialized = true;
            } else {
                minBm25 = std::min(minBm25, score);
                maxBm25 = std::max(maxBm25, score);
            }
        }

        for (size_t rank = 0; rank < fts5Results.value().results.size(); ++rank) {
            const auto& searchResult = fts5Results.value().results[rank];
            const auto& filePath = searchResult.document.filePath;
            const auto& fileName = searchResult.document.fileName;

            auto pruneCategory = magic::getPruneCategory(filePath);
            bool isCodeFile = pruneCategory == magic::PruneCategory::BuildObject ||
                              pruneCategory == magic::PruneCategory::None;

            float scoreMultiplier = isCodeFile ? 1.0f : 0.5f;
            scoreMultiplier *= filenamePathBoost(query, filePath, fileName);

            ComponentResult result;
            result.documentHash = searchResult.document.sha256Hash;
            result.filePath = filePath;
            float rawScore = static_cast<float>(searchResult.score);
            float normalizedScore =
                normalizedBm25Score(rawScore, config_.bm25NormDivisor, minBm25, maxBm25);
            result.score = std::clamp(scoreMultiplier * normalizedScore, 0.0f, 1.0f);
            result.source = ComponentResult::Source::Text;
            result.rank = rank;
            result.snippet = searchResult.snippet.empty()
                                 ? std::nullopt
                                 : std::optional<std::string>(searchResult.snippet);
            result.debugInfo["score_multiplier"] = fmt::format("{:.3f}", scoreMultiplier);

            results.push_back(std::move(result));
        }

        spdlog::info("queryFullText: {} results for query '{}' (limit={})", results.size(),
                     query.substr(0, 50), limit);

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
        // Tokenize query into individual terms for alias lookup.
        // Lowercase + stopword filtering improves KG precision.
        std::vector<std::string> queryTokens = tokenizeKgQuery(query);

        if (queryTokens.empty()) {
            return results;
        }

        // Collect aliases from all tokens with score tracking
        std::vector<metadata::AliasResolution> aliases;
        std::unordered_map<int64_t, float> nodeIdToScore; // Track best score per node

        // Limit aliases per token to avoid explosion
        const size_t aliasesPerToken = std::max(size_t(3), limit / queryTokens.size());

        for (const auto& tok : queryTokens) {
            auto aliasResults = kgStore_->resolveAliasExact(tok, aliasesPerToken);
            bool usedFuzzy = false;
            if (!aliasResults || aliasResults.value().empty()) {
                aliasResults = kgStore_->resolveAliasFuzzy(tok, aliasesPerToken);
                usedFuzzy = true;
            }

            if (aliasResults && !aliasResults.value().empty()) {
                for (const auto& alias : aliasResults.value()) {
                    const float score = usedFuzzy ? alias.score * 0.8f : alias.score;
                    // Track best score for each node (multiple tokens may match same node)
                    auto it = nodeIdToScore.find(alias.nodeId);
                    if (it == nodeIdToScore.end()) {
                        nodeIdToScore[alias.nodeId] = score;
                        aliases.push_back(alias);
                    } else {
                        // Boost score if multiple tokens match same node
                        it->second = std::min(1.0f, it->second + score * 0.5f);
                    }
                }
            }
        }

        spdlog::debug("KG: {} tokens -> {} unique aliases from {} query terms", queryTokens.size(),
                      aliases.size(), queryTokens.size());
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
            result.source = ComponentResult::Source::KnowledgeGraph;
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

        spdlog::info("queryKnowledgeGraph: {} results for query '{}' (batch: {} terms)",
                     results.size(), query.substr(0, 50), searchTerms.size());

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
            result.source = ComponentResult::Source::Vector;
            result.rank = rank;

            if (auto it = hashToPath.find(vr.document_hash); it != hashToPath.end()) {
                result.filePath = it->second;
            }
            if (!vr.content.empty()) {
                result.snippet = truncateSnippet(vr.content, 200);
            }

            results.push_back(std::move(result));
        }

        spdlog::info("queryVectorIndex: {} results (limit={}, threshold={})", results.size(), limit,
                     config_.similarityThreshold);

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding, size_t limit,
                                     const std::unordered_set<std::string>& candidates) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::VectorSearchParams params;
        params.k = limit;
        params.similarity_threshold = config_.similarityThreshold;
        params.candidate_hashes = candidates; // Narrow to Tier 1 candidates

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
            result.source = ComponentResult::Source::Vector;
            result.rank = rank;

            if (auto it = hashToPath.find(vr.document_hash); it != hashToPath.end()) {
                result.filePath = it->second;
            }
            if (!vr.content.empty()) {
                result.snippet = truncateSnippet(vr.content, 200);
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Vector search (narrowed to {} candidates) returned {} results",
                      candidates.size(), results.size());

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
            result.source = ComponentResult::Source::EntityVector;
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

            result.source = ComponentResult::Source::Tag;
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
            result.source = ComponentResult::Source::Metadata;
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

void SearchEngine::setConceptExtractor(EntityExtractionFunc extractor) {
    pImpl_->setConceptExtractor(std::move(extractor));
}

void SearchEngine::setReranker(std::shared_ptr<IReranker> reranker) {
    pImpl_->setReranker(std::move(reranker));
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
