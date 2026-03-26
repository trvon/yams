#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/graph_expansion.h>
#include <yams/search/kg_scorer.h>
#include <yams/search/kg_scorer_simple.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_text_utils.h>
#include <yams/search/search_tracing.h>
#include <yams/search/search_tuner.h>
#include <yams/search/vector_reranker.h>
#include <yams/search/turboquant_packed_reranker.h>
#include <yams/vector/compressed_ann.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>

#include <nlohmann/json.hpp>
#include <boost/asio/post.hpp>

#include "yams/profiling.h"
#include <yams/app/services/simd_memmem.hpp>

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

using json = nlohmann::json;

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

std::string makeHeadTailSnippet(std::string_view content, size_t maxLen) {
    if (content.size() <= maxLen) {
        return std::string(content);
    }
    if (maxLen <= 3) {
        return std::string(content.substr(0, maxLen));
    }

    const size_t bodyBudget = maxLen - 3;
    const size_t prefixLen = bodyBudget / 2;
    const size_t suffixLen = bodyBudget - prefixLen;

    std::string out;
    out.reserve(maxLen);
    out.append(content.substr(0, prefixLen));
    out.append("...");
    out.append(content.substr(content.size() - suffixLen, suffixLen));
    return out;
}

std::string buildRerankSnippet(const std::string& query, const std::string& content,
                               size_t maxLen) {
    if (content.empty() || maxLen == 0) {
        return {};
    }
    if (content.size() <= maxLen) {
        return content;
    }

    struct TokenHit {
        size_t pos = 0;
        size_t len = 0;
    };

    const auto queryTokens = tokenizeLower(query);
    const std::string loweredContent = toLowerCopy(content);
    std::vector<TokenHit> hits;
    hits.reserve(queryTokens.size());

    for (const auto& token : queryTokens) {
        if (token.size() < 3) {
            continue;
        }
        size_t pos = loweredContent.find(token);
        while (pos != std::string::npos) {
            hits.push_back(TokenHit{pos, token.size()});
            pos = loweredContent.find(token, pos + 1);
        }
    }

    if (hits.empty()) {
        if (content.size() <= maxLen + maxLen / 2) {
            return truncateSnippet(content, maxLen);
        }
        return makeHeadTailSnippet(content, maxLen);
    }

    size_t bestStart = 0;
    size_t bestHitCount = 0;
    size_t bestCovered = 0;
    for (const auto& hit : hits) {
        const size_t start = hit.pos > maxLen / 3 ? hit.pos - maxLen / 3 : 0;
        const size_t end = std::min(content.size(), start + maxLen);

        size_t hitCount = 0;
        size_t covered = 0;
        for (const auto& candidate : hits) {
            if (candidate.pos >= start && candidate.pos < end) {
                hitCount++;
                covered += candidate.len;
            }
        }

        if (hitCount > bestHitCount || (hitCount == bestHitCount && covered > bestCovered)) {
            bestStart = start;
            bestHitCount = hitCount;
            bestCovered = covered;
        }
    }

    const bool clippedLeft = bestStart > 0;
    const size_t ellipsisBudget = clippedLeft ? 3 : 0;
    if (ellipsisBudget >= maxLen) {
        return truncateSnippet(content, maxLen);
    }

    const size_t bodyBudget = maxLen - ellipsisBudget;
    const size_t bodyEnd = std::min(content.size(), bestStart + bodyBudget);

    std::string out;
    out.reserve(maxLen);
    if (clippedLeft) {
        out.append("...");
    }
    out.append(content.substr(bestStart, bodyEnd - bestStart));
    if (out.size() < maxLen && bodyEnd < content.size()) {
        const size_t remaining = maxLen - out.size();
        out.append(content.substr(bodyEnd, remaining));
    }
    return out;
}

bool hasQueryTokenHit(const std::string& query, const std::string& content) {
    const auto queryTokens = tokenizeLower(query);
    const std::string loweredContent = toLowerCopy(content);
    for (const auto& token : queryTokens) {
        if (token.size() < 3) {
            continue;
        }
        if (loweredContent.find(token) != std::string::npos) {
            return true;
        }
    }
    return false;
}

std::vector<std::string> buildRerankPassages(const std::string& query, const std::string& content,
                                             size_t maxLen) {
    std::vector<std::string> passages;
    if (content.empty() || maxLen == 0) {
        return passages;
    }

    const std::string primary = buildRerankSnippet(query, content, maxLen);
    if (!primary.empty()) {
        passages.push_back(primary);
    }

    if (!hasQueryTokenHit(query, content) && content.size() > maxLen * 2) {
        struct Segment {
            size_t start = 0;
            std::string text;
        };

        std::vector<Segment> segments;
        size_t segmentStart = 0;
        for (size_t i = 0; i < content.size(); ++i) {
            const char ch = content[i];
            if (ch != '.' && ch != ';' && ch != '\n') {
                continue;
            }

            size_t rawStart = segmentStart;
            size_t rawEnd = i + 1;
            while (rawStart < rawEnd &&
                   std::isspace(static_cast<unsigned char>(content[rawStart]))) {
                ++rawStart;
            }
            while (rawEnd > rawStart &&
                   std::isspace(static_cast<unsigned char>(content[rawEnd - 1]))) {
                --rawEnd;
            }

            if (rawEnd > rawStart) {
                segments.push_back(Segment{rawStart, content.substr(rawStart, rawEnd - rawStart)});
            }
            segmentStart = i + 1;
        }
        if (segmentStart < content.size()) {
            size_t rawStart = segmentStart;
            size_t rawEnd = content.size();
            while (rawStart < rawEnd &&
                   std::isspace(static_cast<unsigned char>(content[rawStart]))) {
                ++rawStart;
            }
            while (rawEnd > rawStart &&
                   std::isspace(static_cast<unsigned char>(content[rawEnd - 1]))) {
                --rawEnd;
            }
            if (rawEnd > rawStart) {
                segments.push_back(Segment{rawStart, content.substr(rawStart, rawEnd - rawStart)});
            }
        }

        const size_t prefixCoverage = std::min(content.size(), maxLen);
        size_t supplementalCount = 0;
        for (auto it = segments.rbegin(); it != segments.rend() && supplementalCount < 3; ++it) {
            if (it->start < prefixCoverage / 2) {
                continue;
            }
            std::string passage = truncateSnippet(it->text, maxLen);
            if (passage.empty() ||
                std::find(passages.begin(), passages.end(), passage) != passages.end()) {
                continue;
            }
            passages.push_back(std::move(passage));
            supplementalCount++;
        }

        if (supplementalCount == 0) {
            const size_t maxStart = content.size() > maxLen ? content.size() - maxLen : 0;
            std::vector<size_t> supplementalStarts = {
                (content.size() - maxLen) / 2,
                std::min(maxStart, (content.size() * 2) / 3 > maxLen / 2
                                       ? (content.size() * 2) / 3 - maxLen / 2
                                       : size_t(0)),
            };

            for (size_t start : supplementalStarts) {
                const std::string passage = content.substr(start, maxLen);
                if (!passage.empty() &&
                    std::find(passages.begin(), passages.end(), passage) == passages.end()) {
                    passages.push_back(passage);
                }
            }
        }
    }

    return passages;
}

struct QueryExpansionStats {
    size_t generatedSubPhrases = 0;
    size_t subPhraseClauseCount = 0;
    size_t subPhraseFtsHitCount = 0;
    size_t subPhraseFtsAddedCount = 0;
    size_t aggressiveClauseCount = 0;
    size_t aggressiveFtsHitCount = 0;
    size_t aggressiveFtsAddedCount = 0;
    size_t graphExpansionTermCount = 0;
    size_t graphExpansionFtsHitCount = 0;
    size_t graphExpansionFtsAddedCount = 0;
    size_t graphTextBlockedLowScoreCount = 0;
};

bool envFlagEnabled(const char* name) {
    if (const char* env = std::getenv(name)) {
        std::string value(env);
        std::transform(value.begin(), value.end(), value.begin(), [](char c) {
            return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        });
        return value == "1" || value == "true" || value == "yes" || value == "on";
    }
    return false;
}

size_t envSizeTOrDefault(const char* name, size_t defaultValue, size_t minValue, size_t maxValue) {
    if (const char* env = std::getenv(name); env && *env) {
        try {
            const auto parsed = static_cast<size_t>(std::stoull(env));
            return std::clamp(parsed, minValue, maxValue);
        } catch (...) {
        }
    }
    return std::clamp(defaultValue, minValue, maxValue);
}

struct PreFusionDocSignal {
    bool hasAnchoring = false;
    bool hasVector = false;
    double maxVectorRaw = 0.0;
    size_t bestVectorRank = std::numeric_limits<size_t>::max();
    std::unordered_set<ComponentResult::Source> sources;
};

using PreFusionSignalMap = std::unordered_map<std::string, PreFusionDocSignal>;

std::vector<GraphExpansionSeedDoc>
collectGraphSeedDocs(const std::vector<ComponentResult>& componentResults, size_t maxDocs) {
    struct SeedAccumulator {
        std::string documentHash;
        std::string filePath;
        float score = 0.0f;
    };

    std::unordered_map<std::string, SeedAccumulator> bestByDoc;
    bestByDoc.reserve(componentResults.size());
    for (const auto& comp : componentResults) {
        const std::string docKey = documentIdForTrace(comp.filePath, comp.documentHash);
        if (docKey.empty()) {
            continue;
        }

        float sourceBoost = 1.0f;
        switch (comp.source) {
            case ComponentResult::Source::Text:
            case ComponentResult::Source::GraphText:
            case ComponentResult::Source::Vector:
            case ComponentResult::Source::GraphVector:
            case ComponentResult::Source::EntityVector:
            case ComponentResult::Source::KnowledgeGraph:
                sourceBoost = 1.0f;
                break;
            case ComponentResult::Source::PathTree:
                sourceBoost = 0.70f;
                break;
            case ComponentResult::Source::Symbol:
                sourceBoost = 0.80f;
                break;
            case ComponentResult::Source::Tag:
            case ComponentResult::Source::Metadata:
                sourceBoost = 0.60f;
                break;
            case ComponentResult::Source::Unknown:
                sourceBoost = 0.50f;
                break;
        }
        const float weightedScore = comp.score * sourceBoost;

        auto it = bestByDoc.find(docKey);
        if (it == bestByDoc.end() || weightedScore > it->second.score) {
            bestByDoc[docKey] = SeedAccumulator{comp.documentHash, comp.filePath, weightedScore};
        }
    }

    std::vector<GraphExpansionSeedDoc> docs;
    docs.reserve(bestByDoc.size());
    for (const auto& [_, seed] : bestByDoc) {
        docs.push_back({seed.documentHash, seed.filePath, seed.score});
    }
    std::stable_sort(docs.begin(), docs.end(),
                     [](const auto& a, const auto& b) { return a.score > b.score; });
    if (docs.size() > maxDocs) {
        docs.resize(maxDocs);
    }
    return docs;
}

PreFusionSignalMap buildPreFusionSignalMap(const std::vector<ComponentResult>& componentResults) {
    PreFusionSignalMap signals;
    signals.reserve(componentResults.size());

    for (const auto& comp : componentResults) {
        const std::string docId = documentIdForTrace(comp.filePath, comp.documentHash);
        if (docId.empty()) {
            continue;
        }

        auto& signal = signals[docId];
        signal.sources.insert(comp.source);
        if (isVectorComponent(comp.source)) {
            signal.hasVector = true;
            signal.maxVectorRaw = std::max(signal.maxVectorRaw,
                                           std::clamp(static_cast<double>(comp.score), 0.0, 1.0));
            signal.bestVectorRank = std::min(signal.bestVectorRank, comp.rank);
        }
        if (isTextAnchoringComponent(comp.source)) {
            signal.hasAnchoring = true;
        }
    }

    return signals;
}

std::string_view::size_type ci_find(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return 0;
    }
    if (needle.size() > haystack.size()) {
        return std::string_view::npos;
    }

    // Pre-lowercase the needle once, then use SIMD-accelerated CI memmem.
    std::string needleLower(needle);
    std::transform(needleLower.begin(), needleLower.end(), needleLower.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    size_t pos = yams::app::services::simdMemmemCI(haystack.data(), haystack.size(),
                                                   needleLower.data(), needleLower.size());
    return (pos == yams::app::services::kMemmemNpos) ? std::string_view::npos : pos;
}

constexpr auto kRerankerErrorCooldown = std::chrono::seconds(60);

bool isRerankerCooldownError(ErrorCode code) {
    return code == ErrorCode::NotImplemented || code == ErrorCode::InvalidState;
}

bool containsFast(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    if (needle.size() > haystack.size()) {
        return false;
    }
    return yams::app::services::simdMemmem(haystack, needle) != yams::app::services::kMemmemNpos;
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

double scoreBasedRerankSignal(const SearchResult& result, QueryIntent intent) {
    const double text = result.keywordScore.value_or(0.0);
    const double vector = result.vectorScore.value_or(0.0);
    const double graphText = result.graphTextScore.value_or(0.0);
    const double graphVector = result.graphVectorScore.value_or(0.0);
    const double kg = result.kgScore.value_or(0.0);
    const double path = result.pathScore.value_or(0.0);
    const double symbol = result.symbolScore.value_or(0.0);
    const double tag = result.tagScore.value_or(0.0);

    const bool hasText = text > 0.0 || graphText > 0.0;
    const bool hasVector = vector > 0.0 || graphVector > 0.0;
    const bool hasPath = path > 0.0;
    const bool hasSymbol = symbol > 0.0;
    const bool hasKg = kg > 0.0;

    double signal = result.score;
    if (hasText && hasVector) {
        signal += 0.12;
    }
    if (hasText && hasKg) {
        signal += 0.05;
    }
    if (hasText && hasPath) {
        signal += 0.03;
    }
    if (hasSymbol && hasVector) {
        signal += 0.04;
    }
    if (tag > 0.0) {
        signal += std::min(0.02, tag * 0.2);
    }

    switch (intent) {
        case QueryIntent::Code:
            signal += path * 0.10 + symbol * 0.10;
            break;
        case QueryIntent::Path:
            signal += path * 0.15;
            break;
        case QueryIntent::Prose:
            signal += text * 0.08 + vector * 0.04 + kg * 0.04;
            break;
        case QueryIntent::Mixed:
            signal += text * 0.05 + vector * 0.05 + kg * 0.03 + path * 0.02;
            break;
    }

    return signal;
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
    if (!config.enableIntentAdaptiveWeighting) {
        return;
    }

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
            scale(config.textWeight, 1.25f);
            scale(config.vectorWeight, 0.9f);
            scale(config.pathTreeWeight, 0.6f);
            scale(config.entityVectorWeight, 0.6f);
            scale(config.kgWeight, 0.8f);
            break;
        case QueryIntent::Mixed:
            break;
    }
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
        double scoreScale = 1.0;
        if (config_.enableFieldAwareWeightedRrf) {
            scoreScale = 0.60;
            switch (comp.source) {
                case ComponentResult::Source::Text:
                case ComponentResult::Source::GraphText:
                    scoreScale = 1.00;
                    break;
                case ComponentResult::Source::PathTree:
                    scoreScale = 0.85;
                    break;
                case ComponentResult::Source::KnowledgeGraph:
                    scoreScale = 0.80;
                    break;
                case ComponentResult::Source::Tag:
                case ComponentResult::Source::Metadata:
                    scoreScale = 0.65;
                    break;
                case ComponentResult::Source::Vector:
                case ComponentResult::Source::GraphVector:
                    scoreScale = 0.45;
                    break;
                case ComponentResult::Source::EntityVector:
                    scoreScale = 0.35;
                    break;
                case ComponentResult::Source::Symbol:
                    scoreScale = 0.75;
                    break;
                case ComponentResult::Source::Unknown:
                    scoreScale = 0.60;
                    break;
            }
        }

        double scoreBoost =
            1.0 + scoreScale * std::clamp(static_cast<double>(comp.score), 0.0, 1.0);
        return weight * rrfScore * scoreBoost;
    });
}

std::vector<SearchResult> ResultFusion::fuseCombMNZ(const std::vector<ComponentResult>& results) {
    struct Accumulator {
        double score = 0.0;
        size_t componentCount = 0;
        size_t bestTextRank = std::numeric_limits<size_t>::max();
        size_t bestVectorRank = std::numeric_limits<size_t>::max();
        bool hasAnchoring = false;
        double maxVectorRaw = 0.0;
        double keywordScore = 0.0;
        double pathScore = 0.0;
        double tagScore = 0.0;
        double symbolScore = 0.0;
        double graphTextScore = 0.0;
        double vectorScore = 0.0;
        double graphVectorScore = 0.0;
        std::string documentHash;
        std::string filePath;
        std::string snippet;
    };
    std::unordered_map<std::string, Accumulator> accumMap;
    accumMap.reserve(results.size());

    const float k = config_.rrfK;
    const auto dedupKeyForComponent = [this](const ComponentResult& comp) {
        if (config_.enablePathDedupInFusion && !comp.filePath.empty()) {
            return std::string("path:") + comp.filePath;
        }
        if (!comp.documentHash.empty()) {
            return std::string("hash:") + comp.documentHash;
        }
        if (!comp.filePath.empty()) {
            return std::string("path:") + comp.filePath;
        }
        return std::string("unknown:");
    };

    for (const auto& comp : results) {
        const std::string dedupKey = dedupKeyForComponent(comp);
        auto& acc = accumMap[dedupKey];

        if (acc.componentCount == 0) {
            acc.documentHash = comp.documentHash;
            acc.filePath = comp.filePath;
            if (comp.snippet.has_value()) {
                acc.snippet = comp.snippet.value();
            }
        } else if (acc.filePath.empty() && !comp.filePath.empty()) {
            acc.filePath = comp.filePath;
        }

        if (acc.snippet.empty() && comp.snippet.has_value()) {
            acc.snippet = comp.snippet.value();
        }

        if (comp.source == ComponentResult::Source::Text) {
            acc.bestTextRank = std::min(acc.bestTextRank, comp.rank);
        }
        if (isTextAnchoringComponent(comp.source)) {
            acc.hasAnchoring = true;
        }
        if (isVectorComponent(comp.source)) {
            acc.maxVectorRaw =
                std::max(acc.maxVectorRaw, std::clamp(static_cast<double>(comp.score), 0.0, 1.0));
            acc.bestVectorRank = std::min(acc.bestVectorRank, comp.rank);
        }

        float weight = getComponentWeight(comp.source);
        const double rank = static_cast<double>(comp.rank) + 1.0;
        double rrfScore = 1.0 / (k + rank);
        double contribution = weight * rrfScore;

        acc.score += contribution;
        acc.componentCount++;

        switch (comp.source) {
            case ComponentResult::Source::Text:
                acc.keywordScore += contribution;
                break;
            case ComponentResult::Source::GraphText:
                acc.graphTextScore += contribution;
                break;
            case ComponentResult::Source::PathTree:
                acc.pathScore += contribution;
                break;
            case ComponentResult::Source::Tag:
            case ComponentResult::Source::Metadata:
                acc.tagScore += contribution;
                break;
            case ComponentResult::Source::Symbol:
                acc.symbolScore += contribution;
                break;
            case ComponentResult::Source::Vector:
            case ComponentResult::Source::EntityVector:
                acc.vectorScore += contribution;
                break;
            case ComponentResult::Source::GraphVector:
                acc.graphVectorScore += contribution;
                break;
            case ComponentResult::Source::KnowledgeGraph:
            case ComponentResult::Source::Unknown:
                break;
        }
    }

    std::vector<SearchResult> fusedResults;
    fusedResults.reserve(accumMap.size());
    std::vector<std::pair<double, SearchResult>> semanticRescueReserve;
    semanticRescueReserve.reserve(std::min(config_.semanticRescueSlots, accumMap.size()));
    std::unordered_map<std::string, double> rawVectorScoreByDedupKey;
    rawVectorScoreByDedupKey.reserve(accumMap.size());

    for (auto& entry : accumMap) {
        rawVectorScoreByDedupKey[entry.first] = entry.second.maxVectorRaw;

        SearchResult r;
        r.document.sha256Hash = std::move(entry.second.documentHash);
        r.document.filePath = std::move(entry.second.filePath);
        r.score = static_cast<float>(entry.second.score * entry.second.componentCount);
        if (entry.second.keywordScore > 0.0) {
            r.keywordScore = entry.second.keywordScore;
        }
        if (entry.second.pathScore > 0.0) {
            r.pathScore = entry.second.pathScore;
        }
        if (entry.second.tagScore > 0.0) {
            r.tagScore = entry.second.tagScore;
        }
        if (entry.second.symbolScore > 0.0) {
            r.symbolScore = entry.second.symbolScore;
        }
        if (entry.second.graphTextScore > 0.0) {
            r.graphTextScore = entry.second.graphTextScore;
        }
        if (entry.second.vectorScore > 0.0) {
            r.vectorScore = entry.second.vectorScore;
        }
        if (entry.second.graphVectorScore > 0.0) {
            r.graphVectorScore = entry.second.graphVectorScore;
        }

        if (config_.lexicalFloorBoost > 0.0f &&
            entry.second.bestTextRank != std::numeric_limits<size_t>::max()) {
            const bool floorEnabledForRank = (config_.lexicalFloorTopN == 0) ||
                                             (entry.second.bestTextRank < config_.lexicalFloorTopN);
            if (floorEnabledForRank) {
                const double floorBoost =
                    std::clamp(static_cast<double>(config_.lexicalFloorBoost), 0.0, 1.0) /
                    (1.0 + static_cast<double>(entry.second.bestTextRank));
                r.score += floorBoost;
            }
        }

        if (entry.second.maxVectorRaw > 0.0 && !entry.second.hasAnchoring) {
            const double vectorOnlyThreshold =
                std::clamp(static_cast<double>(config_.vectorOnlyThreshold), 0.0, 1.0);
            const double nearMissSlack =
                std::clamp(static_cast<double>(config_.vectorOnlyNearMissSlack), 0.0, 1.0);
            const double nearMissPenalty =
                std::clamp(static_cast<double>(config_.vectorOnlyNearMissPenalty), 0.0, 1.0);
            const double semanticRescueMinVector =
                std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
            const bool strongRelief = strongVectorOnlyReliefEligible(
                config_, entry.second.maxVectorRaw, entry.second.bestVectorRank);
            const double effectivePenalty = effectiveVectorOnlyPenalty(
                config_, entry.second.maxVectorRaw, entry.second.bestVectorRank);
            const bool semanticRescueEligible =
                config_.semanticRescueSlots > 0 &&
                entry.second.maxVectorRaw >= semanticRescueMinVector;

            if (entry.second.maxVectorRaw < vectorOnlyThreshold) {
                const bool reserveEnabled = config_.vectorOnlyNearMissReserve > 0;
                const bool isNearMiss =
                    reserveEnabled && vectorOnlyThreshold > 0.0 &&
                    entry.second.maxVectorRaw + nearMissSlack >= vectorOnlyThreshold;
                if (!isNearMiss && !strongRelief && !semanticRescueEligible) {
                    continue;
                }

                if (strongRelief) {
                    r.score = static_cast<float>(r.score * effectivePenalty);
                } else {
                    if (semanticRescueEligible && !isNearMiss) {
                        r.score = static_cast<float>(r.score * effectivePenalty);
                        semanticRescueReserve.emplace_back(entry.second.maxVectorRaw, std::move(r));
                        continue;
                    }

                    const double thresholdRatio =
                        vectorOnlyThreshold > 0.0
                            ? std::clamp(entry.second.maxVectorRaw / vectorOnlyThreshold, 0.0, 1.0)
                            : std::clamp(entry.second.maxVectorRaw, 0.0, 1.0);
                    r.score = static_cast<float>(r.score * effectivePenalty * nearMissPenalty *
                                                 thresholdRatio);
                }
            } else {
                r.score = static_cast<float>(r.score * effectivePenalty);
            }
        }

        r.snippet = std::move(entry.second.snippet);
        fusedResults.push_back(std::move(r));
    }

    if (!semanticRescueReserve.empty()) {
        std::sort(semanticRescueReserve.begin(), semanticRescueReserve.end(),
                  [](const auto& a, const auto& b) {
                      if (a.first != b.first) {
                          return a.first > b.first;
                      }
                      return a.second.score > b.second.score;
                  });

        for (auto& [_, result] : semanticRescueReserve) {
            fusedResults.push_back(std::move(result));
        }
    }

    const auto dedupKeyForSearchResult = [this](const SearchResult& r) {
        if (config_.enablePathDedupInFusion && !r.document.filePath.empty()) {
            return std::string("path:") + r.document.filePath;
        }
        if (!r.document.sha256Hash.empty()) {
            return std::string("hash:") + r.document.sha256Hash;
        }
        if (!r.document.filePath.empty()) {
            return std::string("path:") + r.document.filePath;
        }
        return std::string("unknown:");
    };

    const auto rawVectorScoreForResult = [&rawVectorScoreByDedupKey,
                                          &dedupKeyForSearchResult](const SearchResult& r) {
        if (auto it = rawVectorScoreByDedupKey.find(dedupKeyForSearchResult(r));
            it != rawVectorScoreByDedupKey.end()) {
            return it->second;
        }
        return 0.0;
    };

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto isVectorOnlyRescueCandidate =
        [this, &lexicalAnchorScore, &rawVectorScoreForResult](const SearchResult& r) -> bool {
        const double lexical = lexicalAnchorScore(r);
        const double vector = rawVectorScoreForResult(r);
        return lexical <= 0.0 &&
               vector >= std::max(0.0, static_cast<double>(config_.semanticRescueMinVectorScore));
    };

    const auto lexicalAwareLess = [this, &lexicalAnchorScore](const SearchResult& a,
                                                              const SearchResult& b) {
        const double scoreDiff = a.score - b.score;
        const double tieEpsilon =
            std::max(0.0, static_cast<double>(config_.lexicalTieBreakEpsilon));

        if (!config_.enableLexicalTieBreak || std::abs(scoreDiff) > tieEpsilon) {
            if (a.score != b.score) {
                return a.score > b.score;
            }
        } else {
            const double lexicalA = lexicalAnchorScore(a);
            const double lexicalB = lexicalAnchorScore(b);
            if (lexicalA != lexicalB) {
                return lexicalA > lexicalB;
            }

            const double keywordA = a.keywordScore.value_or(0.0);
            const double keywordB = b.keywordScore.value_or(0.0);
            if (keywordA != keywordB) {
                return keywordA > keywordB;
            }

            const double vectorA = a.vectorScore.value_or(0.0);
            const double vectorB = b.vectorScore.value_or(0.0);
            if (vectorA != vectorB) {
                return vectorA < vectorB;
            }
        }

        if (a.document.filePath != b.document.filePath) {
            return a.document.filePath < b.document.filePath;
        }
        return a.document.sha256Hash < b.document.sha256Hash;
    };

    const auto semanticRescueBetter = [&rawVectorScoreForResult, &lexicalAwareLess](
                                          const SearchResult& a, const SearchResult& b) {
        const double rawVectorA = rawVectorScoreForResult(a);
        const double rawVectorB = rawVectorScoreForResult(b);
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }
        return lexicalAwareLess(a, b);
    };

    const auto applySemanticRescueWindow = [&]() {
        if (config_.semanticRescueSlots == 0 || fusedResults.empty()) {
            return;
        }

        const size_t topK = std::min((config_.enableReranking && config_.rerankTopK > 0)
                                         ? std::min(config_.maxResults, config_.rerankTopK)
                                         : config_.maxResults,
                                     fusedResults.size());
        if (topK == 0 || topK >= fusedResults.size()) {
            return;
        }

        const size_t rescueTarget = std::min(config_.semanticRescueSlots, topK);
        size_t rescuePresent = 0;
        for (size_t i = 0; i < topK; ++i) {
            if (isVectorOnlyRescueCandidate(fusedResults[i])) {
                rescuePresent++;
            }
        }

        while (rescuePresent < rescueTarget) {
            size_t bestTailIndex = fusedResults.size();
            for (size_t i = topK; i < fusedResults.size(); ++i) {
                if (!isVectorOnlyRescueCandidate(fusedResults[i])) {
                    continue;
                }
                if (bestTailIndex >= fusedResults.size() ||
                    semanticRescueBetter(fusedResults[i], fusedResults[bestTailIndex])) {
                    bestTailIndex = i;
                }
            }
            if (bestTailIndex >= fusedResults.size()) {
                break;
            }

            size_t victimIndex = topK;
            for (size_t i = topK; i > 0; --i) {
                const size_t idx = i - 1;
                if (!isVectorOnlyRescueCandidate(fusedResults[idx])) {
                    victimIndex = idx;
                    break;
                }
            }
            if (victimIndex >= topK) {
                break;
            }

            std::swap(fusedResults[victimIndex], fusedResults[bestTailIndex]);
            rescuePresent++;
        }

        std::sort(fusedResults.begin(), fusedResults.begin() + static_cast<ptrdiff_t>(topK),
                  lexicalAwareLess);
    };

    if (fusedResults.size() > config_.maxResults) {
        std::partial_sort(fusedResults.begin(),
                          fusedResults.begin() + static_cast<ptrdiff_t>(config_.maxResults),
                          fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();

        fusedResults.resize(config_.maxResults);
    } else {
        std::sort(fusedResults.begin(), fusedResults.end(), lexicalAwareLess);
        applySemanticRescueWindow();
    }

    return fusedResults;
}

float ResultFusion::getComponentWeight(ComponentResult::Source source) const {
    return componentSourceWeight(config_, source);
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
          embeddingGen_(std::move(embeddingGen)), kgStore_(std::move(kgStore)), config_(config) {
        if (kgStore_) {
            kgScorer_ = makeSimpleKGScorer(kgStore_);
        }
    }

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

    void setSearchTuner(std::shared_ptr<SearchTuner> tuner) { tuner_ = std::move(tuner); }

private:
    Result<SearchResponse> searchInternal(const std::string& query, const SearchParams& params);

    Result<std::vector<ComponentResult>>
    queryFullText(const std::string& query, const SearchEngineConfig& config, size_t limit,
                  QueryExpansionStats* expansionStats = nullptr,
                  const std::vector<GraphExpansionTerm>* graphExpansionTerms = nullptr);
    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>> queryKnowledgeGraph(const std::string& query,
                                                             size_t limit);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding,
                                                          const SearchEngineConfig& config,
                                                          size_t limit);
    Result<std::vector<ComponentResult>>
    queryVectorIndex(const std::vector<float>& embedding, const SearchEngineConfig& config,
                     size_t limit, const std::unordered_set<std::string>& candidates);
    Result<std::vector<ComponentResult>> queryEntityVectors(const std::vector<float>& embedding,
                                                            const SearchEngineConfig& config,
                                                            size_t limit);

    // Compressed ANN traversal using CompressedANNIndex over packed TurboQuant codes
    Result<std::vector<ComponentResult>> queryCompressedANN(const std::vector<float>& embedding,
                                                            const SearchEngineConfig& config,
                                                            size_t limit);
    Result<std::vector<ComponentResult>> queryTags(const std::vector<std::string>& tags,
                                                   bool matchAll, size_t limit);
    Result<std::vector<ComponentResult>> queryMetadata(const SearchParams& params, size_t limit);
    std::unordered_map<std::string, float> lookupQueryTermIdf(const std::string& query) const;
    std::vector<std::string> generateQuerySubPhrases(const std::string& query,
                                                     size_t maxPhrases) const;

    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGen_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<KGScorer> kgScorer_;
    std::optional<boost::asio::any_io_executor> executor_;
    SearchEngineConfig config_;
    mutable SearchEngine::Statistics stats_;
    EntityExtractionFunc conceptExtractor_;               // GLiNER concept extractor (optional)
    std::shared_ptr<IReranker> reranker_;                 // Cross-encoder reranker (optional)
    std::shared_ptr<IVectorReranker> turboQuantReranker_; // TurboQuant vector reranker (optional)

    // Compressed ANN index (built lazily from packed vectors when enabled)
    std::unique_ptr<vector::CompressedANNIndex> compressedAnnIndex_;
    bool compressedAnnIndexReady_ = false; // True once build() has been called
    std::shared_ptr<SearchTuner> tuner_;   // Adaptive runtime tuner (optional)
    std::atomic<int64_t> rerankerCooldownUntilMicros_{0};
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

std::unordered_map<std::string, float>
SearchEngine::Impl::lookupQueryTermIdf(const std::string& query) const {
    std::unordered_map<std::string, float> idfByToken;
    if (!metadataRepo_) {
        return idfByToken;
    }

    auto tokens = tokenizeQueryTokens(query);
    std::vector<std::string> uniqueTerms;
    uniqueTerms.reserve(tokens.size());
    std::unordered_set<std::string> seen;
    seen.reserve(tokens.size());
    for (const auto& token : tokens) {
        if (token.normalized.size() < 2) {
            continue;
        }
        if (seen.insert(token.normalized).second) {
            uniqueTerms.push_back(token.normalized);
        }
    }

    if (uniqueTerms.empty()) {
        return idfByToken;
    }

    auto idfResult = metadataRepo_->getTermIDFBatch(uniqueTerms);
    if (idfResult) {
        idfByToken = std::move(idfResult.value());
    } else {
        spdlog::debug("lookupQueryTermIdf: IDF batch lookup failed: {}", idfResult.error().message);
    }
    return idfByToken;
}

std::vector<std::string> SearchEngine::Impl::generateQuerySubPhrases(const std::string& query,
                                                                     size_t maxPhrases) const {
    if (maxPhrases == 0) {
        return {};
    }

    auto idfByToken = lookupQueryTermIdf(query);

    return generateAnchoredSubPhrases(query, maxPhrases,
                                      idfByToken.empty() ? nullptr : &idfByToken);
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
    const bool stageTraceEnabled = envFlagEnabled("YAMS_SEARCH_STAGE_TRACE");
    std::vector<std::string> preFusionDocIds;
    PreFusionSignalMap preFusionSignals;
    std::vector<SearchResult> postFusionSnapshot;
    std::vector<SearchResult> graphlessPostFusionSnapshot;
    std::vector<SearchResult> postGraphSnapshot;
    bool graphRerankApplied = false;
    bool crossRerankApplied = false;
    size_t graphWindowGuardReplacementCount = 0;
    size_t graphWindowCapReplacementCount = 0;
    size_t graphMatchedCandidates = 0;
    size_t graphPositiveSignalCandidates = 0;
    size_t graphBoostedDocs = 0;
    float graphMaxSignal = 0.0f;
    size_t graphQueryConceptCount = 0;
    json rerankWindowTrace = json::array();
    QueryExpansionStats textExpansionStats;
    size_t multiVectorGeneratedPhrases = 0;
    size_t multiVectorPhraseHits = 0;
    size_t multiVectorAddedNewCount = 0;
    size_t multiVectorReplacedBaseCount = 0;
    size_t graphDocExpansionTermCount = 0;
    size_t graphDocExpansionFtsHitCount = 0;
    size_t graphDocExpansionFtsAddedCount = 0;
    size_t graphVectorGeneratedTerms = 0;
    size_t graphVectorRawHitCount = 0;
    size_t graphVectorAddedNewCount = 0;
    size_t graphVectorReplacedBaseCount = 0;
    size_t graphVectorBlockedUncorroboratedCount = 0;
    size_t graphVectorBlockedMissingTextAnchorCount = 0;
    size_t graphVectorBlockedMissingBaselineTextAnchorCount = 0;
    bool weakQueryFanoutBoostApplied = false;
    size_t effectiveVectorMaxResults = 0;
    size_t effectiveEntityVectorMaxResults = 0;
    SearchEngineConfig workingConfig = config_;

    if (tuner_) {
        workingConfig = tuner_->getConfig();
    }

    SearchTraceCollector traceCollector(workingConfig);

    // Embedding generation may be launched eagerly or lazily depending on tiering strategy.
    std::optional<std::vector<float>> queryEmbedding;
    const bool needsEmbedding =
        (workingConfig.vectorWeight > 0.0f || workingConfig.entityVectorWeight > 0.0f);
    std::future<std::vector<float>> embeddingFuture;
    std::chrono::steady_clock::time_point embStart;
    bool embeddingStarted = false;
    auto launchEmbeddingIfNeeded = [&]() {
        if (!embeddingStarted && needsEmbedding && embeddingGen_) {
            traceCollector.markStageAttempted("embedding");
            embStart = std::chrono::steady_clock::now();
            embeddingFuture = postWork(
                [this, &query]() {
                    YAMS_ZONE_SCOPED_N("embedding::generate_async");
                    return embeddingGen_->generateEmbedding(query);
                },
                executor_);
            embeddingStarted = true;
        }
    };

    // Preserve overlap for default behavior. When adaptive fallback is enabled in tiered mode,
    // delay embedding work until we know Tier 2 is truly needed.
    if (!(workingConfig.enableTieredExecution && workingConfig.enableAdaptiveVectorFallback)) {
        launchEmbeddingIfNeeded();
    }

    std::future<Result<QueryConceptResult>> conceptFuture;
    std::chrono::steady_clock::time_point conceptStart;
    std::vector<QueryConcept> concepts;
    bool conceptsMaterialized = false;
    std::vector<GraphExpansionTerm> graphExpansionTerms;
    std::vector<GraphExpansionTerm> docSeedGraphTerms;
    bool graphExpansionMaterialized = false;
    size_t graphQueryNeighborSeedDocCount = 0;
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
        launchEmbeddingIfNeeded();
        if (embeddingFuture.valid() && !queryEmbedding.has_value()) {
            try {
                auto embResult = embeddingFuture.get();
                if (!embResult.empty()) {
                    queryEmbedding = std::move(embResult);
                }
            } catch (const std::exception& e) {
                spdlog::warn("Failed to generate query embedding: {}", e.what());
                auto embEnd = std::chrono::steady_clock::now();
                traceCollector.markStageFailure(
                    "embedding",
                    std::chrono::duration_cast<std::chrono::microseconds>(embEnd - embStart)
                        .count());
            }
            auto embEnd = std::chrono::steady_clock::now();
            componentTiming["embedding"] =
                std::chrono::duration_cast<std::chrono::microseconds>(embEnd - embStart).count();
            std::vector<ComponentResult> embeddingMarker;
            if (queryEmbedding.has_value()) {
                embeddingMarker.push_back(ComponentResult{.score = 1.0f});
            }
            traceCollector.markStageResult("embedding", embeddingMarker,
                                           componentTiming["embedding"],
                                           queryEmbedding.has_value());
        }
    };

    const size_t userLimit =
        params.limit > 0 ? static_cast<size_t>(params.limit) : workingConfig.maxResults;
    const size_t semanticRescueProbeWindow =
        workingConfig.semanticRescueSlots > 0
            ? std::max<size_t>(200, workingConfig.semanticRescueSlots * 100)
            : size_t(0);
    const size_t autoFusionLimit = std::max(
        userLimit, std::max(std::max(workingConfig.rerankTopK, workingConfig.graphRerankTopN),
                            workingConfig.rerankTopK + semanticRescueProbeWindow));
    const size_t fusionCandidateLimit = workingConfig.fusionCandidateLimit > 0
                                            ? workingConfig.fusionCandidateLimit
                                            : autoFusionLimit;

    traceCollector.markStageConfigured("embedding", needsEmbedding && embeddingGen_ != nullptr);
    traceCollector.markStageConfigured("concepts", conceptExtractor_ != nullptr);

    workingConfig.maxResults = fusionCandidateLimit;
    traceCollector.markStageConfigured("text", workingConfig.textWeight > 0.0f);
    traceCollector.markStageConfigured("kg", workingConfig.kgWeight > 0.0f && kgStore_ != nullptr);
    traceCollector.markStageConfigured("path", workingConfig.pathTreeWeight > 0.0f);
    traceCollector.markStageConfigured("vector", workingConfig.vectorWeight > 0.0f);
    traceCollector.markStageConfigured("entity_vector", workingConfig.entityVectorWeight > 0.0f);
    traceCollector.markStageConfigured("tag",
                                       workingConfig.tagWeight > 0.0f && !params.tags.empty());
    traceCollector.markStageConfigured("metadata", workingConfig.metadataWeight > 0.0f);
    traceCollector.markStageConfigured("multi_vector", workingConfig.enableMultiVectorQuery ||
                                                           workingConfig.enableGraphQueryExpansion);
    traceCollector.markStageConfigured("graph_rerank",
                                       workingConfig.enableGraphRerank && kgScorer_ != nullptr);
    traceCollector.markStageConfigured("reranker",
                                       workingConfig.enableReranking && reranker_ != nullptr);

    auto materializeConcepts = [&](bool waitIfConfigured) {
        if (conceptsMaterialized || !conceptFuture.valid()) {
            return;
        }

        std::future_status conceptStatus = std::future_status::deferred;
        if (waitIfConfigured) {
            if (workingConfig.componentTimeout.count() > 0) {
                conceptStatus = conceptFuture.wait_for(workingConfig.componentTimeout);
            } else {
                conceptFuture.wait();
                conceptStatus = std::future_status::ready;
            }
        } else {
            conceptStatus = conceptFuture.wait_for(std::chrono::seconds(0));
        }

        if (conceptStatus == std::future_status::ready) {
            auto conceptEnd = std::chrono::steady_clock::now();
            componentTiming["concepts"] =
                std::chrono::duration_cast<std::chrono::microseconds>(conceptEnd - conceptStart)
                    .count();
            auto conceptResult = conceptFuture.get();
            conceptsMaterialized = true;
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
            std::vector<ComponentResult> conceptMarker;
            if (!concepts.empty()) {
                conceptMarker.push_back(ComponentResult{.score = 1.0f});
            }
            traceCollector.markStageResult("concepts", conceptMarker, componentTiming["concepts"],
                                           !concepts.empty());
        } else if (waitIfConfigured && conceptStatus == std::future_status::timeout) {
            timedOut.push_back("concepts");
            conceptsMaterialized = true;
            traceCollector.markStageTimeout("concepts");
        }

        if (conceptsMaterialized && concepts.empty()) {
            auto idfByToken = lookupQueryTermIdf(query);
            concepts =
                generateFallbackQueryConcepts(query, idfByToken, workingConfig.conceptMaxCount);
            if (!concepts.empty()) {
                spdlog::debug("concepts: generated {} fallback query concepts for '{}'",
                              concepts.size(), query.substr(0, 60));
            }
        }
    };

    auto materializeGraphExpansionTerms = [&](bool waitForConcepts) {
        if (graphExpansionMaterialized || !workingConfig.enableGraphQueryExpansion) {
            return;
        }
        if (waitForConcepts) {
            materializeConcepts(true);
        }
        graphExpansionTerms = generateGraphExpansionTerms(
            kgStore_, query, concepts,
            GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                 .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                 .maxNeighbors = workingConfig.graphMaxNeighbors});

        if (vectorDb_ && embeddingGen_ && workingConfig.graphExpansionQueryNeighborK > 0) {
            awaitEmbedding();
            if (queryEmbedding.has_value()) {
                yams::vector::VectorSearchParams params;
                params.k = workingConfig.graphExpansionQueryNeighborK + 6;
                params.similarity_threshold = workingConfig.graphExpansionQueryNeighborMinScore;
                auto neighbors = vectorDb_->search(queryEmbedding.value(), params);

                std::vector<GraphExpansionSeedDoc> seedDocs;
                seedDocs.reserve(workingConfig.graphExpansionQueryNeighborK);
                std::unordered_set<std::string> seenHashes;
                seenHashes.reserve(workingConfig.graphExpansionQueryNeighborK * 2);
                for (const auto& rec : neighbors) {
                    if (seedDocs.size() >= workingConfig.graphExpansionQueryNeighborK) {
                        break;
                    }
                    if (rec.level != yams::vector::EmbeddingLevel::DOCUMENT ||
                        rec.document_hash.empty() || !seenHashes.insert(rec.document_hash).second) {
                        continue;
                    }
                    auto pathIt = rec.metadata.find("path");
                    seedDocs.push_back(
                        {.documentHash = rec.document_hash,
                         .filePath = pathIt != rec.metadata.end() ? pathIt->second : "",
                         .score = rec.relevance_score});
                }
                graphQueryNeighborSeedDocCount = seedDocs.size();

                if (!seedDocs.empty()) {
                    auto neighborTerms = generateGraphExpansionTermsFromDocuments(
                        kgStore_, query, concepts, seedDocs,
                        GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                             .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                             .maxNeighbors = workingConfig.graphMaxNeighbors});
                    for (const auto& term : neighborTerms) {
                        auto it = std::find_if(
                            graphExpansionTerms.begin(), graphExpansionTerms.end(),
                            [&](const auto& existing) { return existing.text == term.text; });
                        if (it == graphExpansionTerms.end()) {
                            graphExpansionTerms.push_back(term);
                        } else {
                            it->score = std::max(it->score, term.score);
                        }
                    }
                    std::stable_sort(
                        graphExpansionTerms.begin(), graphExpansionTerms.end(),
                        [](const auto& a, const auto& b) { return a.score > b.score; });
                    if (graphExpansionTerms.size() > workingConfig.graphExpansionMaxTerms) {
                        graphExpansionTerms.resize(workingConfig.graphExpansionMaxTerms);
                    }
                }
            }
        }
        graphExpansionMaterialized = true;
    };

    if (workingConfig.enableGraphQueryExpansion && workingConfig.waitForConceptExtraction) {
        materializeGraphExpansionTerms(true);
    }

    spdlog::debug("Search limit: userLimit={}, fusionCandidateLimit={}, textMax={}, vectorMax={}",
                  userLimit, fusionCandidateLimit, workingConfig.textMaxResults,
                  workingConfig.vectorMaxResults);

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

        traceCollector.markStageAttempted(name);

        auto waitStart = std::chrono::steady_clock::now();

        // componentTimeout of 0 means no timeout (wait indefinitely)
        std::future_status status;
        if (workingConfig.componentTimeout.count() == 0) {
            future.wait(); // Wait indefinitely
            status = std::future_status::ready;
        } else {
            status = future.wait_for(workingConfig.componentTimeout);
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
                    traceCollector.markStageResult(name, results.value(), duration,
                                                   !results.value().empty());
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
                    traceCollector.markStageFailure(name, duration);
                    return ComponentStatus::Failed;
                }
            } catch (const std::exception& e) {
                spdlog::warn("Parallel {} query failed: {}", name, e.what());
                auto waitEnd = std::chrono::steady_clock::now();
                traceCollector.markStageFailure(
                    name, std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart)
                              .count());
                return ComponentStatus::Failed;
            }
        } else {
            spdlog::warn("Parallel {} query timed out after {} ms", name,
                         workingConfig.componentTimeout.count());
            stats_.timedOutQueries.fetch_add(1, std::memory_order_relaxed);
            auto waitEnd = std::chrono::steady_clock::now();
            traceCollector.markStageTimeout(
                name,
                std::chrono::duration_cast<std::chrono::microseconds>(waitEnd - waitStart).count());
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

    if (workingConfig.enableParallelExecution) [[likely]] {
        YAMS_ZONE_SCOPED_N("search_engine::fanout_parallel");
        std::future<Result<std::vector<ComponentResult>>> textFuture;
        std::future<Result<std::vector<ComponentResult>>> kgFuture;
        std::future<Result<std::vector<ComponentResult>>> pathFuture;
        std::future<Result<std::vector<ComponentResult>>> vectorFuture;
        std::future<Result<std::vector<ComponentResult>>> entityVectorFuture;
        std::future<Result<std::vector<ComponentResult>>> tagFuture;
        std::future<Result<std::vector<ComponentResult>>> metaFuture;
        std::future<Result<std::vector<ComponentResult>>> compressedAnnFuture;

        auto schedule = [&]([[maybe_unused]] const char* name, [[maybe_unused]] float weight,
                            [[maybe_unused]] std::atomic<uint64_t>& queryCount,
                            [[maybe_unused]] std::atomic<uint64_t>& avgTime,
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

        if (workingConfig.enableTieredExecution) {
            // === TIERED EXECUTION ===
            // Tier 1: Fast text-based components (FTS5 + path_tree)
            // Tier 2: Slower semantic components (vector) - only if Tier 1 insufficient
            YAMS_ZONE_SCOPED_N("search_engine::tiered_execution");

            // --- TIER 1: Text + Path (fast, high precision) ---
            textFuture = schedule(
                "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros,
                [this, &query, &workingConfig, &textExpansionStats, &graphExpansionTerms]() {
                    YAMS_ZONE_SCOPED_N("component::text");
                    return queryFullText(query, workingConfig, workingConfig.textMaxResults,
                                         &textExpansionStats, &graphExpansionTerms);
                });

            pathFuture = schedule("path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                                  stats_.avgPathTreeTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::path");
                                      return queryPathTree(query, workingConfig.pathTreeMaxResults);
                                  });

            // Tags and metadata are also fast, run in Tier 1
            if (!params.tags.empty()) {
                tagFuture = schedule("tag", workingConfig.tagWeight, stats_.tagQueries,
                                     stats_.avgTagTimeMicros, [this, &params, &workingConfig]() {
                                         YAMS_ZONE_SCOPED_N("component::tag");
                                         return queryTags(params.tags, params.matchAllTags,
                                                          workingConfig.tagMaxResults);
                                     });
            }

            metaFuture =
                schedule("metadata", workingConfig.metadataWeight, stats_.metadataQueries,
                         stats_.avgMetadataTimeMicros, [this, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(params, workingConfig.metadataMaxResults);
                         });

            // Collect Tier 1 results
            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);

            // Extract Tier 1 candidate hashes only when needed (narrowing or adaptive fallback).
            const bool needTier1Candidates = workingConfig.tieredNarrowVectorSearch ||
                                             workingConfig.enableAdaptiveVectorFallback;
            std::unordered_set<std::string> tier1Candidates;
            if (needTier1Candidates) {
                tier1Candidates.reserve(allComponentResults.size());
                for (const auto& r : allComponentResults) {
                    if (!r.documentHash.empty()) {
                        tier1Candidates.insert(r.documentHash);
                    }
                }
            }

            const size_t tier1CandidateCount = tier1Candidates.size();
            if (needTier1Candidates) {
                spdlog::debug("Tiered search: {} unique candidates from Tier 1",
                              tier1CandidateCount);
            }

            // --- TIER 2: Vector search NARROWED to Tier 1 candidates ---
            // Always run vector search (never skip), but filter to Tier 1 candidates when
            // appropriate
            YAMS_ZONE_SCOPED_N("search_engine::tier2_semantic");

            const size_t adaptiveSkipMinHits =
                (workingConfig.adaptiveVectorSkipMinTier1Hits > 0)
                    ? workingConfig.adaptiveVectorSkipMinTier1Hits
                    : std::max<size_t>(workingConfig.maxResults * 2, static_cast<size_t>(50));

            size_t tier1TextHits = 0;
            float tier1TopTextScore = 0.0f;
            for (const auto& componentResult : allComponentResults) {
                if (componentResult.source == ComponentResult::Source::Text) {
                    tier1TextHits++;
                    tier1TopTextScore = std::max(tier1TopTextScore, componentResult.score);
                }
            }

            bool hasStrongTextSignal = true;
            if (workingConfig.adaptiveVectorSkipRequireTextSignal) {
                hasStrongTextSignal =
                    tier1TextHits >= workingConfig.adaptiveVectorSkipMinTextHits &&
                    tier1TopTextScore >= workingConfig.adaptiveVectorSkipMinTopTextScore;
            }

            effectiveVectorMaxResults = workingConfig.vectorMaxResults;
            effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
            const bool weakTier1Query = !hasStrongTextSignal || tier1TextHits == 0;
            if (workingConfig.enableWeakQueryFanoutBoost && weakTier1Query) {
                effectiveVectorMaxResults = static_cast<size_t>(
                    std::ceil(static_cast<double>(workingConfig.vectorMaxResults) *
                              workingConfig.weakQueryVectorFanoutMultiplier));
                effectiveEntityVectorMaxResults = static_cast<size_t>(
                    std::ceil(static_cast<double>(workingConfig.entityVectorMaxResults) *
                              workingConfig.weakQueryEntityVectorFanoutMultiplier));
                weakQueryFanoutBoostApplied = true;
                spdlog::debug(
                    "Tiered search: weak-query fanout boost applied (text_hits={}, "
                    "top_text_score={:.3f}, vector_max={} -> {}, entity_vector_max={} -> {})",
                    tier1TextHits, tier1TopTextScore, workingConfig.vectorMaxResults,
                    effectiveVectorMaxResults, workingConfig.entityVectorMaxResults,
                    effectiveEntityVectorMaxResults);
            }

            const bool shouldSkipSemantic = workingConfig.enableAdaptiveVectorFallback &&
                                            (tier1CandidateCount >= adaptiveSkipMinHits) &&
                                            hasStrongTextSignal;

            if (shouldSkipSemantic) {
                skipped.push_back("vector");
                skipped.push_back("entity_vector");
                traceCollector.markStageSkipped("vector", "adaptive_vector_skip");
                traceCollector.markStageSkipped("entity_vector", "adaptive_vector_skip");
                spdlog::debug("Tiered search: skipping embedding/vector tier (tier1 candidates={} "
                              ">= threshold={}, text_hits={}, top_text_score={:.3f})",
                              tier1CandidateCount, adaptiveSkipMinHits, tier1TextHits,
                              tier1TopTextScore);
            } else {
                if (workingConfig.enableAdaptiveVectorFallback &&
                    tier1CandidateCount >= adaptiveSkipMinHits && !hasStrongTextSignal) {
                    spdlog::debug("Tiered search: retaining semantic tier due to weak text signal "
                                  "(tier1 candidates={}, text_hits={}, top_text_score={:.3f}, "
                                  "min_text_hits={}, min_top_text_score={:.3f})",
                                  tier1CandidateCount, tier1TextHits, tier1TopTextScore,
                                  workingConfig.adaptiveVectorSkipMinTextHits,
                                  workingConfig.adaptiveVectorSkipMinTopTextScore);
                }
                // Await embedding result (eager or lazily started depending on config).
                awaitEmbedding();
            }

            // Decide whether to narrow vector search to Tier 1 candidates
            // Narrow if: config enabled AND Tier 1 has enough candidates
            const bool shouldNarrow = workingConfig.tieredNarrowVectorSearch &&
                                      tier1CandidateCount >= workingConfig.tieredMinCandidates;

            if (!shouldSkipSemantic && queryEmbedding.has_value() && vectorDb_) {
                vectorFuture = schedule(
                    "vector", workingConfig.vectorWeight, stats_.vectorQueries,
                    stats_.avgVectorTimeMicros,
                    [this, &queryEmbedding, &tier1Candidates, &workingConfig, shouldNarrow,
                     effectiveVectorMaxResults]() {
                        YAMS_ZONE_SCOPED_N("component::vector");
                        if (shouldNarrow) {
                            return queryVectorIndex(queryEmbedding.value(), workingConfig,
                                                    effectiveVectorMaxResults,
                                                    tier1Candidates); // Narrowed search
                        } else {
                            return queryVectorIndex(queryEmbedding.value(), workingConfig,
                                                    effectiveVectorMaxResults); // Full search
                        }
                    });

                entityVectorFuture = schedule(
                    "entity_vector", workingConfig.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros,
                    [this, &queryEmbedding, &workingConfig, effectiveEntityVectorMaxResults]() {
                        YAMS_ZONE_SCOPED_N("component::entity_vector");
                        return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                  effectiveEntityVectorMaxResults);
                    });
            }

            if (kgStore_) {
                kgFuture =
                    schedule("kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
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
            textFuture = schedule(
                "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros,
                [this, &query, &workingConfig, &textExpansionStats, &graphExpansionTerms]() {
                    YAMS_ZONE_SCOPED_N("component::text");
                    return queryFullText(query, workingConfig, workingConfig.textMaxResults,
                                         &textExpansionStats, &graphExpansionTerms);
                });

            if (kgStore_) {
                kgFuture =
                    schedule("kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                             [this, &query, &workingConfig]() {
                                 YAMS_ZONE_SCOPED_N("component::kg");
                                 return queryKnowledgeGraph(query, workingConfig.kgMaxResults);
                             });
            }

            pathFuture = schedule("path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                                  stats_.avgPathTreeTimeMicros, [this, &query, &workingConfig]() {
                                      YAMS_ZONE_SCOPED_N("component::path");
                                      return queryPathTree(query, workingConfig.pathTreeMaxResults);
                                  });

            // NOTE: Vector components scheduled below after embedding is ready
            // This allows text/kg/path to run in parallel with embedding generation

            if (!params.tags.empty()) {
                tagFuture = schedule("tag", workingConfig.tagWeight, stats_.tagQueries,
                                     stats_.avgTagTimeMicros, [this, &params, &workingConfig]() {
                                         YAMS_ZONE_SCOPED_N("component::tag");
                                         return queryTags(params.tags, params.matchAllTags,
                                                          workingConfig.tagMaxResults);
                                     });
            }

            metaFuture =
                schedule("metadata", workingConfig.metadataWeight, stats_.metadataQueries,
                         stats_.avgMetadataTimeMicros, [this, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(params, workingConfig.metadataMaxResults);
                         });

            // Await embedding (ran in parallel with text/kg/path above) then schedule vector
            awaitEmbedding();
            if (queryEmbedding.has_value() && vectorDb_) {
                vectorFuture =
                    schedule("vector", workingConfig.vectorWeight, stats_.vectorQueries,
                             stats_.avgVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                                 YAMS_ZONE_SCOPED_N("component::vector");
                                 return queryVectorIndex(queryEmbedding.value(), workingConfig,
                                                         workingConfig.vectorMaxResults);
                             });

                entityVectorFuture = schedule(
                    "entity_vector", workingConfig.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                        YAMS_ZONE_SCOPED_N("component::entity_vector");
                        return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                  workingConfig.entityVectorMaxResults);
                    });

                // Compressed ANN traversal: runs alongside vector search, inserts into
                // allComponentResults
                if (workingConfig.enableCompressedANN) {
                    compressedAnnFuture = schedule(
                        "compressed_ann", workingConfig.vectorWeight, stats_.vectorQueries,
                        stats_.avgVectorTimeMicros, [this, &queryEmbedding, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::compressed_ann");
                            return queryCompressedANN(queryEmbedding.value(), workingConfig,
                                                      workingConfig.compressedAnnTopK);
                        });
                }
            }

            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(vectorFuture, "vector", stats_.vectorQueries, stats_.avgVectorTimeMicros);
            collectIf(entityVectorFuture, "entity_vector", stats_.entityVectorQueries,
                      stats_.avgEntityVectorTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);
            collectIf(compressedAnnFuture, "compressed_ann", stats_.vectorQueries,
                      stats_.avgVectorTimeMicros);
        }
    } else {
        auto runSequential = [&](auto queryFn, const char* name, float weight,
                                 std::atomic<uint64_t>& queryCount,
                                 std::atomic<uint64_t>& avgTime) {
            if (weight <= 0.0f) {
                traceCollector.markStageSkipped(name, "disabled_by_weight");
                return;
            }

            traceCollector.markStageAttempted(name);
            YAMS_ZONE_SCOPED_N(name);
            auto start = std::chrono::steady_clock::now();
            auto results = queryFn();
            auto end = std::chrono::steady_clock::now();
            auto duration =
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

            componentTiming[name] = duration;

            if (results) {
                traceCollector.markStageResult(name, results.value(), duration,
                                               !results.value().empty());
                if (!results.value().empty()) {
                    allComponentResults.insert(allComponentResults.end(), results.value().begin(),
                                               results.value().end());
                    contributing.push_back(name);
                }
                queryCount.fetch_add(1, std::memory_order_relaxed);
                avgTime.store(duration, std::memory_order_relaxed);
            } else {
                failed.push_back(name);
                traceCollector.markStageFailure(name, duration);
            }
        };

        runSequential(
            [&]() {
                return queryFullText(query, workingConfig, workingConfig.textMaxResults,
                                     &textExpansionStats, &graphExpansionTerms);
            },
            "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros);

        if (kgStore_) {
            runSequential([&]() { return queryKnowledgeGraph(query, workingConfig.kgMaxResults); },
                          "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros);
        }

        runSequential([&]() { return queryPathTree(query, workingConfig.pathTreeMaxResults); },
                      "path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                      stats_.avgPathTreeTimeMicros);

        // Await embedding (ran in parallel with sequential components above)
        awaitEmbedding();
        if (queryEmbedding.has_value() && vectorDb_) {
            runSequential(
                [&]() {
                    return queryVectorIndex(queryEmbedding.value(), workingConfig,
                                            workingConfig.vectorMaxResults);
                },
                "vector", workingConfig.vectorWeight, stats_.vectorQueries,
                stats_.avgVectorTimeMicros);

            runSequential(
                [&]() {
                    return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                              workingConfig.entityVectorMaxResults);
                },
                "entity_vector", workingConfig.entityVectorWeight, stats_.entityVectorQueries,
                stats_.avgEntityVectorTimeMicros);
        }

        if (!params.tags.empty()) {
            runSequential(
                [&]() {
                    return queryTags(params.tags, params.matchAllTags, workingConfig.tagMaxResults);
                },
                "tag", workingConfig.tagWeight, stats_.tagQueries, stats_.avgTagTimeMicros);
        }

        runSequential([&]() { return queryMetadata(params, workingConfig.metadataMaxResults); },
                      "metadata", workingConfig.metadataWeight, stats_.metadataQueries,
                      stats_.avgMetadataTimeMicros);
    }

    if (workingConfig.enableGraphQueryExpansion && kgStore_ && metadataRepo_ &&
        !allComponentResults.empty()) {
        const auto seedDocs =
            collectGraphSeedDocs(allComponentResults, workingConfig.graphExpansionMaxSeeds * 2);
        docSeedGraphTerms = generateGraphExpansionTermsFromDocuments(
            kgStore_, query, concepts, seedDocs,
            GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                 .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                 .maxNeighbors = workingConfig.graphMaxNeighbors});
        graphDocExpansionTermCount = docSeedGraphTerms.size();

        if (!docSeedGraphTerms.empty()) {
            std::unordered_set<std::string> existingDocIds;
            existingDocIds.reserve(allComponentResults.size() * 2);
            for (const auto& comp : allComponentResults) {
                const auto docId = documentIdForTrace(comp.filePath, comp.documentHash);
                if (!docId.empty()) {
                    existingDocIds.insert(docId);
                }
            }

            for (const auto& term : docSeedGraphTerms) {
                auto searchResults =
                    metadataRepo_->search(term.text, workingConfig.textMaxResults, 0);
                if (!searchResults) {
                    continue;
                }
                graphDocExpansionFtsHitCount += searchResults.value().results.size();

                double minBm25 = 0.0;
                double maxBm25 = 0.0;
                bool rangeInitialized = false;
                for (const auto& sr : searchResults.value().results) {
                    if (!rangeInitialized) {
                        minBm25 = maxBm25 = sr.score;
                        rangeInitialized = true;
                    } else {
                        minBm25 = std::min(minBm25, sr.score);
                        maxBm25 = std::max(maxBm25, sr.score);
                    }
                }

                const size_t startRank = allComponentResults.size();
                for (size_t rank = 0; rank < searchResults.value().results.size(); ++rank) {
                    const auto& sr = searchResults.value().results[rank];
                    const auto docId =
                        documentIdForTrace(sr.document.filePath, sr.document.sha256Hash);
                    if (!docId.empty() && existingDocIds.contains(docId)) {
                        continue;
                    }

                    ComponentResult cr;
                    cr.documentHash = sr.document.sha256Hash;
                    cr.filePath = sr.document.filePath;
                    cr.score =
                        std::clamp(workingConfig.graphExpansionFtsPenalty *
                                       std::clamp(term.score, 0.2f, 1.0f) *
                                       normalizedBm25Score(sr.score, workingConfig.bm25NormDivisor,
                                                           minBm25, maxBm25),
                                   0.0f, 1.0f);
                    if (cr.score < workingConfig.graphTextMinAdmissionScore) {
                        ++textExpansionStats.graphTextBlockedLowScoreCount;
                        continue;
                    }
                    cr.source = ComponentResult::Source::GraphText;
                    cr.rank = startRank + rank;
                    cr.snippet =
                        sr.snippet.empty() ? std::nullopt : std::optional<std::string>(sr.snippet);
                    cr.debugInfo["graph_doc_expansion_term"] = term.text;
                    allComponentResults.push_back(std::move(cr));
                    if (!docId.empty()) {
                        existingDocIds.insert(docId);
                    }
                    ++graphDocExpansionFtsAddedCount;
                }
            }

            if (graphDocExpansionFtsAddedCount > 0) {
                contributing.push_back("graph_doc_text");
            }
            spdlog::debug(
                "graph_doc_text: seed_docs={} terms={} raw_hits={} added={} for query '{}'",
                seedDocs.size(), docSeedGraphTerms.size(), graphDocExpansionFtsHitCount,
                graphDocExpansionFtsAddedCount, query.substr(0, 60));
        }
    }

    // ============================================================================
    // Auxiliary vector expansion: sub-phrases and graph-derived labels both run additional
    // vector searches and merge into the vector component before fusion.
    // ============================================================================
    if ((workingConfig.enableMultiVectorQuery || workingConfig.enableGraphQueryExpansion) &&
        queryEmbedding.has_value() && vectorDb_ && embeddingGen_) {
        YAMS_ZONE_SCOPED_N("multi_vector::sub_phrase_search");
        auto mvStart = std::chrono::steady_clock::now();

        auto subPhrases = generateQuerySubPhrases(query, workingConfig.multiVectorMaxPhrases);
        if (!workingConfig.enableMultiVectorQuery) {
            subPhrases.clear();
        }
        multiVectorGeneratedPhrases = subPhrases.size();
        if (!graphExpansionMaterialized) {
            materializeGraphExpansionTerms(workingConfig.waitForConceptExtraction);
        }
        auto graphTerms = graphExpansionTerms;
        for (const auto& term : docSeedGraphTerms) {
            auto it = std::find_if(graphTerms.begin(), graphTerms.end(), [&](const auto& existing) {
                return existing.text == term.text;
            });
            if (it == graphTerms.end()) {
                graphTerms.push_back(term);
            } else {
                it->score = std::max(it->score, term.score);
            }
        }
        std::stable_sort(graphTerms.begin(), graphTerms.end(),
                         [](const auto& a, const auto& b) { return a.score > b.score; });
        if (graphTerms.size() > workingConfig.graphExpansionMaxTerms) {
            graphTerms.resize(workingConfig.graphExpansionMaxTerms);
        }
        if (!workingConfig.enableGraphQueryExpansion) {
            graphTerms.clear();
        }
        graphVectorGeneratedTerms = graphTerms.size();
        size_t multiVecHits = 0;
        size_t baseVectorCount = 0;
        std::vector<ComponentResult> mergedVectorResults;
        std::vector<ComponentResult> mergedGraphVectorResults;

        if (!subPhrases.empty() || !graphTerms.empty()) {
            spdlog::debug("multi_vector: generated {} sub-phrases from query '{}'",
                          subPhrases.size(), query.substr(0, 60));
            spdlog::debug("graph_vector: generated {} graph terms from query '{}'",
                          graphTerms.size(), query.substr(0, 60));

            std::vector<ComponentResult> nonVectorResults;
            nonVectorResults.reserve(allComponentResults.size());

            std::unordered_map<std::string, size_t> bestVectorByHash;
            bestVectorByHash.reserve(workingConfig.vectorMaxResults * 2);
            std::unordered_map<std::string, size_t> bestGraphVectorByHash;
            bestGraphVectorByHash.reserve(workingConfig.vectorMaxResults * 2);
            std::unordered_set<std::string> graphVectorCorroboratedHashes;
            graphVectorCorroboratedHashes.reserve(allComponentResults.size() * 2);
            std::unordered_set<std::string> graphVectorTextAnchoredHashes;
            graphVectorTextAnchoredHashes.reserve(allComponentResults.size() * 2);
            std::unordered_set<std::string> graphVectorBaselineTextAnchoredHashes;
            graphVectorBaselineTextAnchoredHashes.reserve(allComponentResults.size() * 2);

            for (const auto& cr : allComponentResults) {
                if (cr.source == ComponentResult::Source::Vector ||
                    cr.source == ComponentResult::Source::EntityVector) {
                    if (cr.documentHash.empty()) {
                        continue;
                    }

                    ++baseVectorCount;
                    graphVectorCorroboratedHashes.insert(cr.documentHash);
                    auto it = bestVectorByHash.find(cr.documentHash);
                    if (it == bestVectorByHash.end()) {
                        bestVectorByHash.emplace(cr.documentHash, mergedVectorResults.size());
                        mergedVectorResults.push_back(cr);
                    } else if (cr.score > mergedVectorResults[it->second].score) {
                        mergedVectorResults[it->second] = cr;
                    }
                    continue;
                }
                if (cr.source == ComponentResult::Source::GraphVector) {
                    if (cr.documentHash.empty()) {
                        continue;
                    }
                    auto it = bestGraphVectorByHash.find(cr.documentHash);
                    if (it == bestGraphVectorByHash.end()) {
                        bestGraphVectorByHash.emplace(cr.documentHash,
                                                      mergedGraphVectorResults.size());
                        mergedGraphVectorResults.push_back(cr);
                    } else if (cr.score > mergedGraphVectorResults[it->second].score) {
                        mergedGraphVectorResults[it->second] = cr;
                    }
                    continue;
                }
                {
                    if (!cr.documentHash.empty()) {
                        if (isTextAnchoringComponent(cr.source) ||
                            cr.source == ComponentResult::Source::KnowledgeGraph) {
                            graphVectorCorroboratedHashes.insert(cr.documentHash);
                        }
                        if (cr.source == ComponentResult::Source::Text ||
                            cr.source == ComponentResult::Source::GraphText ||
                            cr.source == ComponentResult::Source::PathTree ||
                            cr.source == ComponentResult::Source::KnowledgeGraph ||
                            cr.source == ComponentResult::Source::Tag ||
                            cr.source == ComponentResult::Source::Metadata ||
                            cr.source == ComponentResult::Source::Symbol) {
                            graphVectorTextAnchoredHashes.insert(cr.documentHash);
                        }
                        if (cr.source == ComponentResult::Source::Text ||
                            cr.source == ComponentResult::Source::PathTree ||
                            cr.source == ComponentResult::Source::KnowledgeGraph ||
                            cr.source == ComponentResult::Source::Symbol) {
                            graphVectorBaselineTextAnchoredHashes.insert(cr.documentHash);
                        }
                    }
                    nonVectorResults.push_back(cr);
                }
            }

            const float decay = std::clamp(workingConfig.multiVectorScoreDecay, 0.1f, 1.0f);
            const float graphPenalty =
                std::clamp(workingConfig.graphExpansionVectorPenalty, 0.1f, 1.0f);

            for (size_t pi = 0; pi < subPhrases.size(); ++pi) {
                try {
                    auto subEmbedding = embeddingGen_->generateEmbedding(subPhrases[pi]);
                    if (subEmbedding.empty()) {
                        continue;
                    }

                    auto subResults = queryVectorIndex(subEmbedding, workingConfig,
                                                       workingConfig.vectorMaxResults);
                    if (!subResults || subResults.value().empty()) {
                        continue;
                    }
                    multiVectorPhraseHits += subResults.value().size();

                    for (const auto& rawResult : subResults.value()) {
                        ComponentResult cr = rawResult;
                        if (cr.documentHash.empty()) {
                            continue;
                        }

                        cr.score *= decay;
                        cr.debugInfo["multi_vector_phrase"] = subPhrases[pi];
                        cr.debugInfo["multi_vector_phrase_idx"] = std::to_string(pi);

                        auto it = bestVectorByHash.find(cr.documentHash);
                        if (it == bestVectorByHash.end()) {
                            bestVectorByHash.emplace(cr.documentHash, mergedVectorResults.size());
                            mergedVectorResults.push_back(std::move(cr));
                            ++multiVecHits;
                            ++multiVectorAddedNewCount;
                        } else if (cr.score > mergedVectorResults[it->second].score) {
                            mergedVectorResults[it->second] = std::move(cr);
                            ++multiVecHits;
                            ++multiVectorReplacedBaseCount;
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("multi_vector: sub-phrase embedding failed for '{}': {}",
                                 subPhrases[pi], e.what());
                }
            }

            for (size_t gi = 0; gi < graphTerms.size(); ++gi) {
                try {
                    auto graphEmbedding = embeddingGen_->generateEmbedding(graphTerms[gi].text);
                    if (graphEmbedding.empty()) {
                        continue;
                    }
                    auto graphResults = queryVectorIndex(graphEmbedding, workingConfig,
                                                         workingConfig.vectorMaxResults);
                    if (!graphResults || graphResults.value().empty()) {
                        continue;
                    }
                    graphVectorRawHitCount += graphResults.value().size();

                    for (const auto& rawResult : graphResults.value()) {
                        ComponentResult cr = rawResult;
                        if (cr.documentHash.empty()) {
                            continue;
                        }
                        if (workingConfig.graphVectorRequireCorroboration &&
                            !graphVectorCorroboratedHashes.contains(cr.documentHash)) {
                            ++graphVectorBlockedUncorroboratedCount;
                            continue;
                        }
                        if (workingConfig.graphVectorRequireTextAnchoring &&
                            !graphVectorTextAnchoredHashes.contains(cr.documentHash)) {
                            ++graphVectorBlockedMissingTextAnchorCount;
                            continue;
                        }
                        if (workingConfig.graphVectorRequireBaselineTextAnchoring &&
                            !graphVectorBaselineTextAnchoredHashes.contains(cr.documentHash)) {
                            ++graphVectorBlockedMissingBaselineTextAnchorCount;
                            continue;
                        }
                        cr.source = ComponentResult::Source::GraphVector;
                        cr.score *= (graphPenalty * std::clamp(graphTerms[gi].score, 0.2f, 1.0f));
                        cr.debugInfo["graph_vector_term"] = graphTerms[gi].text;
                        cr.debugInfo["graph_vector_term_idx"] = std::to_string(gi);

                        auto it = bestGraphVectorByHash.find(cr.documentHash);
                        if (it == bestGraphVectorByHash.end()) {
                            bestGraphVectorByHash.emplace(cr.documentHash,
                                                          mergedGraphVectorResults.size());
                            mergedGraphVectorResults.push_back(std::move(cr));
                            ++graphVectorAddedNewCount;
                        } else if (cr.score > mergedGraphVectorResults[it->second].score) {
                            mergedGraphVectorResults[it->second] = std::move(cr);
                            ++graphVectorReplacedBaseCount;
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("graph_vector: term embedding failed for '{}': {}",
                                 graphTerms[gi].text, e.what());
                }
            }

            std::sort(mergedVectorResults.begin(), mergedVectorResults.end(),
                      [](const auto& a, const auto& b) { return a.score > b.score; });
            if (mergedVectorResults.size() > workingConfig.vectorMaxResults) {
                mergedVectorResults.resize(workingConfig.vectorMaxResults);
            }
            for (size_t i = 0; i < mergedVectorResults.size(); ++i) {
                mergedVectorResults[i].rank = i;
            }

            std::sort(mergedGraphVectorResults.begin(), mergedGraphVectorResults.end(),
                      [](const auto& a, const auto& b) { return a.score > b.score; });
            if (mergedGraphVectorResults.size() > workingConfig.vectorMaxResults) {
                mergedGraphVectorResults.resize(workingConfig.vectorMaxResults);
            }
            for (size_t i = 0; i < mergedGraphVectorResults.size(); ++i) {
                mergedGraphVectorResults[i].rank = i;
            }

            allComponentResults = std::move(nonVectorResults);
            allComponentResults.insert(allComponentResults.end(), mergedVectorResults.begin(),
                                       mergedVectorResults.end());
            allComponentResults.insert(allComponentResults.end(), mergedGraphVectorResults.begin(),
                                       mergedGraphVectorResults.end());
        }

        auto mvEnd = std::chrono::steady_clock::now();
        auto mvDuration =
            std::chrono::duration_cast<std::chrono::microseconds>(mvEnd - mvStart).count();
        componentTiming["multi_vector"] = mvDuration;
        if (multiVecHits > 0) {
            contributing.push_back("multi_vector");
        }
        if (graphVectorAddedNewCount > 0 || graphVectorReplacedBaseCount > 0) {
            contributing.push_back("graph_vector");
        }
        std::vector<ComponentResult> traceMultiVectorResults;
        for (const auto& comp : allComponentResults) {
            if (comp.source == ComponentResult::Source::Vector ||
                comp.source == ComponentResult::Source::EntityVector ||
                comp.source == ComponentResult::Source::GraphVector) {
                traceMultiVectorResults.push_back(comp);
            }
        }
        traceCollector.markStageResult("multi_vector", traceMultiVectorResults, mvDuration,
                                       multiVecHits > 0 || graphVectorAddedNewCount > 0 ||
                                           graphVectorReplacedBaseCount > 0);
        spdlog::debug(
            "multi_vector: merged {} vector updates from {} sub-phrases "
            "(raw_hits={}, added={}, replaced={}, base_vectors={}, final_components={}) in {}us",
            multiVecHits, subPhrases.size(), multiVectorPhraseHits, multiVectorAddedNewCount,
            multiVectorReplacedBaseCount, baseVectorCount, allComponentResults.size(), mvDuration);
        spdlog::debug(
            "graph_vector: merged graph terms={} raw_hits={} added={} replaced={} in {}us",
            graphTerms.size(), graphVectorRawHitCount, graphVectorAddedNewCount,
            graphVectorReplacedBaseCount, mvDuration);
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

    preFusionDocIds = collectUniqueComponentDocIds(allComponentResults);
    if (stageTraceEnabled || workingConfig.semanticRescueSlots > 0) {
        preFusionSignals = buildPreFusionSignalMap(allComponentResults);
    }

    ResultFusion fusion(workingConfig);
    {
        YAMS_ZONE_SCOPED_N("fusion::results");
        response.results = fusion.fuse(allComponentResults);
    }

    // TurboQuant packed-code reranking: operates on fused results using compressed codes
    // without full float reconstruction. Runs after fusion but before graph reranking.
    bool turboQuantRerankApplied = false;
    if (workingConfig.enableTurboQuantRerank && queryEmbedding.has_value() && vectorDb_) {
        const size_t window =
            std::min(workingConfig.turboQuantRerankWindow, response.results.size());
        if (window > 0) {
            // Lazy-init the reranker (built once per SearchEngine lifetime)
            if (!turboQuantReranker_) {
                TurboQuantPackedRerankerConfig cfg;
                cfg.dimension = workingConfig.turboQuantRerankDim;
                cfg.bits_per_channel = workingConfig.turboQuantRerankBits;
                cfg.seed = 42;
                cfg.rerank_weight = workingConfig.turboQuantRerankWeight;
                cfg.require_packed_codes = workingConfig.turboQuantRerankOnlyWhenPackedAvailable;
                turboQuantReranker_ = createTurboQuantPackedReranker(cfg);
            }
            if (turboQuantReranker_ && turboQuantReranker_->isReady()) {
                YAMS_ZONE_SCOPED_N("turboquant::rerank");
                // Transform query once
                yams::vector::TurboQuantConfig tq_cfg;
                tq_cfg.dimension = workingConfig.turboQuantRerankDim;
                tq_cfg.bits_per_channel = workingConfig.turboQuantRerankBits;
                tq_cfg.seed = 42;
                yams::vector::TurboQuantMSE quantizer(tq_cfg);
                auto y_q = quantizer.transformQuery(queryEmbedding.value());

                // Collect top-N candidates
                std::vector<SearchResult> topN;
                topN.reserve(window);
                for (size_t i = 0; i < window; ++i) {
                    topN.push_back(response.results[i]);
                }

                // Build reranker input
                VectorRerankInput input;
                input.transformed_query = std::move(y_q);
                for (size_t i = 0; i < topN.size(); ++i) {
                    const auto& doc = topN[i].document;
                    // Try to get vector records for this document
                    auto records = vectorDb_->getVectorsByDocument(doc.sha256Hash);
                    if (!records.empty()) {
                        // Use the best-matching chunk for this document
                        input.candidates[topN[i].document.filePath] = records[0];
                        input.initial_scores[topN[i].document.filePath] =
                            static_cast<float>(topN[i].score);
                    }
                }

                // Run reranking
                auto rerankResult = turboQuantReranker_->rerank(input);
                if (rerankResult) {
                    const auto& ranked = rerankResult.value();
                    // Update reranker scores in the top-N results
                    for (size_t i = 0; i < window; ++i) {
                        const auto& doc = topN[i].document;
                        for (const auto& rc : ranked.candidates) {
                            if (rc.chunk_id == doc.filePath) {
                                response.results[i].rerankerScore =
                                    static_cast<double>(rc.rerank_score);
                                // Blend score into the result score if rerankReplaceScores is false
                                if (!workingConfig.rerankReplaceScores) {
                                    const float w = workingConfig.turboQuantRerankWeight;
                                    response.results[i].score =
                                        (1.0 - w) * response.results[i].score +
                                        w * static_cast<double>(rc.rerank_score);
                                }
                                break;
                            }
                        }
                    }
                    turboQuantRerankApplied = ranked.packed_candidates_scored > 0;
                }
            }
        }
    }

    const bool hasGraphSources =
        std::any_of(allComponentResults.begin(), allComponentResults.end(), [](const auto& comp) {
            return comp.source == ComponentResult::Source::GraphText ||
                   comp.source == ComponentResult::Source::GraphVector;
        });
    if ((stageTraceEnabled || (workingConfig.enableGraphFusionWindowGuard && hasGraphSources))) {
        postFusionSnapshot = response.results;
        std::vector<ComponentResult> graphlessComponentResults;
        graphlessComponentResults.reserve(allComponentResults.size());
        for (const auto& comp : allComponentResults) {
            if (comp.source == ComponentResult::Source::GraphText ||
                comp.source == ComponentResult::Source::GraphVector) {
                continue;
            }
            graphlessComponentResults.push_back(comp);
        }
        if (graphlessComponentResults.size() != allComponentResults.size()) {
            graphlessPostFusionSnapshot = fusion.fuse(graphlessComponentResults);
        } else {
            graphlessPostFusionSnapshot = postFusionSnapshot;
        }
    }

    if (workingConfig.enableGraphFusionWindowGuard && hasGraphSources &&
        !graphlessPostFusionSnapshot.empty()) {
        const size_t guardWindow =
            std::min(response.results.size(),
                     std::min(graphlessPostFusionSnapshot.size(),
                              (workingConfig.enableReranking && workingConfig.rerankTopK > 0)
                                  ? workingConfig.rerankTopK
                                  : size_t(50)));
        const size_t guardDepth =
            std::min(graphlessPostFusionSnapshot.size(),
                     guardWindow * workingConfig.graphFusionGuardDepthMultiplier);
        const auto graphlessTopIds =
            collectRankedResultDocIds(graphlessPostFusionSnapshot, guardWindow);
        const auto graphlessDepthIds =
            collectRankedResultDocIds(graphlessPostFusionSnapshot, guardDepth);
        const auto actualTopIds = collectRankedResultDocIds(response.results, guardWindow);
        std::unordered_set<std::string> graphlessTopSet(graphlessTopIds.begin(),
                                                        graphlessTopIds.end());
        std::unordered_set<std::string> graphlessDepthSet(graphlessDepthIds.begin(),
                                                          graphlessDepthIds.end());
        std::unordered_set<std::string> actualTopSet(actualTopIds.begin(), actualTopIds.end());
        std::unordered_map<std::string, SearchResult> graphlessById;
        graphlessById.reserve(guardDepth);
        for (size_t i = 0; i < guardDepth; ++i) {
            const auto docId =
                documentIdForTrace(graphlessPostFusionSnapshot[i].document.filePath,
                                   graphlessPostFusionSnapshot[i].document.sha256Hash);
            if (!docId.empty()) {
                graphlessById.emplace(docId, graphlessPostFusionSnapshot[i]);
            }
        }

        std::vector<std::string> displacedGraphlessIds;
        for (const auto& docId : graphlessTopIds) {
            if (!actualTopSet.contains(docId)) {
                displacedGraphlessIds.push_back(docId);
            }
        }

        size_t displacedCursor = 0;
        for (size_t i = 0; i < guardWindow && displacedCursor < displacedGraphlessIds.size(); ++i) {
            const auto actualId = documentIdForTrace(response.results[i].document.filePath,
                                                     response.results[i].document.sha256Hash);
            if (graphlessTopSet.contains(actualId)) {
                continue;
            }
            if (graphlessDepthSet.contains(actualId)) {
                continue; // allow graph to promote graphless near-misses
            }

            const auto& restoreId = displacedGraphlessIds[displacedCursor++];
            auto restoreIt = graphlessById.find(restoreId);
            if (restoreIt == graphlessById.end()) {
                continue;
            }
            response.results[i] = restoreIt->second;
            ++graphWindowGuardReplacementCount;
        }
        if (stageTraceEnabled) {
            postFusionSnapshot = response.results;
        }
    }

    if (workingConfig.graphMaxAddedInFusionWindow > 0 && hasGraphSources &&
        !graphlessPostFusionSnapshot.empty()) {
        const size_t guardWindow =
            std::min(response.results.size(),
                     std::min(graphlessPostFusionSnapshot.size(),
                              (workingConfig.enableReranking && workingConfig.rerankTopK > 0)
                                  ? workingConfig.rerankTopK
                                  : size_t(50)));
        const auto graphlessTopIds =
            collectRankedResultDocIds(graphlessPostFusionSnapshot, guardWindow);
        const auto actualTopIds = collectRankedResultDocIds(response.results, guardWindow);
        std::unordered_set<std::string> graphlessTopSet(graphlessTopIds.begin(),
                                                        graphlessTopIds.end());
        std::unordered_map<std::string, SearchResult> graphlessById;
        graphlessById.reserve(guardWindow);
        for (size_t i = 0; i < guardWindow; ++i) {
            const auto docId =
                documentIdForTrace(graphlessPostFusionSnapshot[i].document.filePath,
                                   graphlessPostFusionSnapshot[i].document.sha256Hash);
            if (!docId.empty()) {
                graphlessById.emplace(docId, graphlessPostFusionSnapshot[i]);
            }
        }

        std::vector<size_t> graphAddedIndices;
        graphAddedIndices.reserve(guardWindow);
        for (size_t i = 0; i < guardWindow; ++i) {
            const auto actualId = documentIdForTrace(response.results[i].document.filePath,
                                                     response.results[i].document.sha256Hash);
            if (!graphlessTopSet.contains(actualId)) {
                graphAddedIndices.push_back(i);
            }
        }

        if (graphAddedIndices.size() > workingConfig.graphMaxAddedInFusionWindow) {
            std::vector<std::string> restoreIds;
            restoreIds.reserve(graphAddedIndices.size());
            for (const auto& docId : graphlessTopIds) {
                const bool present =
                    std::any_of(actualTopIds.begin(), actualTopIds.end(),
                                [&](const auto& actual) { return actual == docId; });
                if (!present) {
                    restoreIds.push_back(docId);
                }
            }

            const size_t replacements =
                std::min(restoreIds.size(),
                         graphAddedIndices.size() - workingConfig.graphMaxAddedInFusionWindow);
            for (size_t ri = 0; ri < replacements; ++ri) {
                const size_t replaceIndex = graphAddedIndices[graphAddedIndices.size() - 1 - ri];
                auto restoreIt = graphlessById.find(restoreIds[ri]);
                if (restoreIt == graphlessById.end()) {
                    continue;
                }
                response.results[replaceIndex] = restoreIt->second;
                ++graphWindowCapReplacementCount;
            }
            if (stageTraceEnabled) {
                postFusionSnapshot = response.results;
            }
        }
    }

    if (workingConfig.enableGraphRerank && kgScorer_ && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("graph::rerank");
        const auto graphRerankStart = std::chrono::steady_clock::now();

        materializeConcepts(workingConfig.graphUseQueryConcepts &&
                            workingConfig.waitForConceptExtraction);
        graphQueryConceptCount = concepts.size();

        const size_t rerankWindow =
            std::min(workingConfig.graphRerankTopN, response.results.size());
        if (rerankWindow > 0) {
            KGScoringConfig graphCfg = kgScorer_->getConfig();
            graphCfg.max_neighbors = workingConfig.graphMaxNeighbors;
            graphCfg.max_hops = workingConfig.graphMaxHops;
            graphCfg.budget =
                std::chrono::milliseconds(std::max(0, workingConfig.graphScoringBudgetMs));
            graphCfg.enable_path_enumeration = workingConfig.graphEnablePathEnumeration;
            graphCfg.max_paths = workingConfig.graphMaxPaths;
            graphCfg.hop_decay = std::clamp(workingConfig.graphHopDecay, 0.0f, 1.0f);
            kgScorer_->setConfig(graphCfg);

            std::vector<std::string> candidateIds;
            candidateIds.reserve(rerankWindow);
            for (size_t i = 0; i < rerankWindow; ++i) {
                const auto& res = response.results[i];
                const std::string docId =
                    documentIdForTrace(res.document.filePath, res.document.sha256Hash);
                if (!docId.empty()) {
                    candidateIds.push_back(docId);
                } else if (!res.document.sha256Hash.empty()) {
                    candidateIds.push_back(res.document.sha256Hash);
                } else {
                    candidateIds.push_back(res.document.filePath);
                }
            }

            std::string graphQuery = query;
            if (workingConfig.graphUseQueryConcepts && !concepts.empty()) {
                std::unordered_set<std::string> seenConcepts;
                for (const auto& conceptItem : concepts) {
                    if (conceptItem.text.empty() || !seenConcepts.insert(conceptItem.text).second) {
                        continue;
                    }
                    if (graphQuery.find(conceptItem.text) == std::string::npos) {
                        graphQuery.push_back(' ');
                        graphQuery += conceptItem.text;
                    }
                }
            }

            auto graphScoresResult = kgScorer_->score(graphQuery, candidateIds);
            if (graphScoresResult) {
                const auto& graphScores = graphScoresResult.value();
                const auto graphExplanations = kgScorer_->getLastExplanations();
                bool boosted = false;
                const float minSignal = std::max(0.0f, workingConfig.graphRerankMinSignal);
                const float maxBoost = std::max(0.0f, workingConfig.graphRerankMaxBoost);
                const float rerankWeight = std::max(0.0f, workingConfig.graphRerankWeight);

                std::vector<float> rawSignals(rerankWindow, 0.0f);
                float maxRawSignal = 0.0f;
                size_t topSignalIndex = rerankWindow;

                for (size_t i = 0; i < rerankWindow; ++i) {
                    const auto& candidateId = candidateIds[i];
                    auto scoreIt = graphScores.find(candidateId);
                    if (scoreIt == graphScores.end()) {
                        continue;
                    }
                    graphMatchedCandidates++;

                    const KGScore& kgScore = scoreIt->second;
                    const auto getFeature = [&kgScore](const char* key) {
                        auto featureIt = kgScore.features.find(key);
                        return featureIt != kgScore.features.end() ? featureIt->second : 0.0f;
                    };

                    const float queryCoverage =
                        std::clamp(getFeature("feature_query_coverage_ratio"), 0.0f, 1.0f);
                    const float pathSupport =
                        std::clamp(getFeature("feature_path_support_score"), 0.0f, 1.0f);

                    // Composite graph relevance signal.
                    const float rawSignal =
                        std::clamp(kgScore.entity * 0.45f + kgScore.structural * 0.25f +
                                       queryCoverage * 0.20f + pathSupport * 0.10f,
                                   0.0f, 1.0f);
                    rawSignals[i] = rawSignal;
                    maxRawSignal = std::max(maxRawSignal, rawSignal);
                    if (rawSignal > 0.0f) {
                        graphPositiveSignalCandidates++;
                    }
                    if (topSignalIndex >= rerankWindow || rawSignal > rawSignals[topSignalIndex]) {
                        topSignalIndex = i;
                    }
                }
                graphMaxSignal = maxRawSignal;

                for (size_t i = 0; i < rerankWindow; ++i) {
                    const auto& candidateId = candidateIds[i];
                    auto scoreIt = graphScores.find(candidateId);
                    if (scoreIt == graphScores.end()) {
                        continue;
                    }

                    const float signal = rawSignals[i];
                    if (signal < minSignal) {
                        continue;
                    }

                    const float normalizedSignal =
                        maxRawSignal > 0.0f ? signal / maxRawSignal : 0.0f;
                    const float effectiveSignal =
                        std::clamp(signal * 0.6f + normalizedSignal * 0.4f, 0.0f, 1.0f);

                    const float boost = std::min(maxBoost, rerankWeight * effectiveSignal);
                    if (boost <= 0.0f) {
                        continue;
                    }

                    response.results[i].score *= (1.0 + static_cast<double>(boost));
                    response.results[i].kgScore = response.results[i].kgScore.value_or(0.0) + boost;
                    boosted = true;
                    graphBoostedDocs++;
                }

                if (!boosted && workingConfig.graphFallbackToTopSignal &&
                    topSignalIndex < rerankWindow && rawSignals[topSignalIndex] > 0.0f) {
                    const float fallbackBoost =
                        std::min(maxBoost * 0.5f, rerankWeight * rawSignals[topSignalIndex]);
                    if (fallbackBoost > 0.0f) {
                        response.results[topSignalIndex].score *=
                            (1.0 + static_cast<double>(fallbackBoost));
                        response.results[topSignalIndex].kgScore =
                            response.results[topSignalIndex].kgScore.value_or(0.0) + fallbackBoost;
                        boosted = true;
                        graphBoostedDocs++;
                    }
                }

                if (boosted) {
                    std::sort(response.results.begin(), response.results.end(),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });
                    graphRerankApplied = true;
                    contributing.push_back("graph_rerank");
                } else {
                    skipped.push_back("graph_rerank");
                    traceCollector.markStageSkipped("graph_rerank", "no_positive_graph_signal");
                }

                if (stageTraceEnabled) {
                    json graphExplainJson = json::array();
                    for (size_t i = 0; i < std::min(graphExplanations.size(), rerankWindow); ++i) {
                        const auto& expl = graphExplanations[i];
                        graphExplainJson.push_back({
                            {"doc_id", expl.id},
                            {"components", expl.components},
                            {"reasons", expl.reasons},
                        });
                    }
                    response.debugStats["graph_explanations_json"] = graphExplainJson.dump();
                    response.debugStats["graph_doc_probe_json"] =
                        buildGraphDocProbeJson(kgStore_, response.results, rerankWindow).dump();
                }
            } else {
                failed.push_back("graph_rerank");
                traceCollector.markStageFailure("graph_rerank");
                spdlog::debug("[graph_rerank] KG scoring failed: {}",
                              graphScoresResult.error().message);
            }
        }

        const auto graphRerankEnd = std::chrono::steady_clock::now();
        componentTiming["graph_rerank"] =
            std::chrono::duration_cast<std::chrono::microseconds>(graphRerankEnd - graphRerankStart)
                .count();
        if (graphRerankApplied) {
            traceCollector.markStageResult("graph_rerank", {}, componentTiming["graph_rerank"],
                                           true);
        }
    }

    if (stageTraceEnabled) {
        postGraphSnapshot = response.results;
    }

    materializeConcepts(workingConfig.waitForConceptExtraction);

    // Cross-encoder reranking: second-stage ranking for improved relevance
    const bool rerankAvailable = reranker_ && reranker_->isReady();
    if (!workingConfig.enableReranking && rerankAvailable && !response.results.empty()) {
        skipped.push_back("reranker");
        traceCollector.markStageSkipped("reranker", "disabled_in_config");
    }
    if (workingConfig.enableReranking && rerankAvailable && !response.results.empty()) {
        YAMS_ZONE_SCOPED_N("reranking");
        traceCollector.markStageAttempted("reranker");
        const auto rerankStart = std::chrono::steady_clock::now();
        const size_t rerankProbeWindow =
            workingConfig.semanticRescueSlots > 0
                ? std::max(workingConfig.rerankTopK,
                           workingConfig.rerankTopK +
                               std::max<size_t>(200, workingConfig.semanticRescueSlots * 100))
                : workingConfig.rerankTopK;
        const size_t rerankWindow = std::min(rerankProbeWindow, response.results.size());

        if (workingConfig.useScoreBasedReranking && rerankWindow > 1) {
            std::stable_sort(response.results.begin(),
                             response.results.begin() + static_cast<ptrdiff_t>(rerankWindow),
                             [&](const SearchResult& a, const SearchResult& b) {
                                 return scoreBasedRerankSignal(a, intent) >
                                        scoreBasedRerankSignal(b, intent);
                             });
            contributing.push_back("score_rerank");
        }

        const int64_t nowMicros = std::chrono::duration_cast<std::chrono::microseconds>(
                                      std::chrono::steady_clock::now().time_since_epoch())
                                      .count();
        const int64_t cooldownUntilMicros =
            rerankerCooldownUntilMicros_.load(std::memory_order_relaxed);
        const bool rerankerCoolingDown = cooldownUntilMicros > nowMicros;

        if (rerankerCoolingDown) {
            skipped.push_back("reranker");
            traceCollector.markStageSkipped("reranker", "cooldown_active");
            const int64_t remainingMs = (cooldownUntilMicros - nowMicros) / 1000;
            spdlog::debug("[reranker] Cooldown active; skipping rerank ({} ms remaining)",
                          std::max<int64_t>(remainingMs, 0));
        }

        bool skipRerank = false;
        if (!rerankerCoolingDown && rerankWindow >= 2 &&
            workingConfig.rerankScoreGapThreshold > 0.0f) {
            const double scoreGap = response.results[0].score - response.results[1].score;
            if (scoreGap >= static_cast<double>(workingConfig.rerankScoreGapThreshold)) {
                spdlog::debug("[reranker] Skipping rerank (score gap {:.4f} >= {:.4f})", scoreGap,
                              workingConfig.rerankScoreGapThreshold);
                skipRerank = true;
                traceCollector.markStageSkipped("reranker", "score_gap_guard");
            }
        }

        if (!rerankerCoolingDown && !skipRerank) {
            std::vector<SearchResult> preRerankSnapshot;
            if (stageTraceEnabled) {
                preRerankSnapshot.assign(response.results.begin(),
                                         response.results.begin() +
                                             static_cast<ptrdiff_t>(rerankWindow));
            }

            // Extract document snippets for reranking.
            // Fallback order: fused snippet -> metadata content preview -> source file.
            // Metadata preview is preferred because benchmark/warm-cache file paths may
            // point at transient ingestion paths that no longer exist.
            std::vector<std::string> snippets;
            std::vector<size_t> rerankPassageDocIndices;
            snippets.reserve(rerankWindow);
            rerankPassageDocIndices.reserve(rerankWindow);

            std::unordered_map<size_t, std::string> metadataPreviewByIndex;
            if (metadataRepo_ && rerankWindow > 0) {
                std::unordered_map<std::string, std::vector<size_t>> hashToIndices;
                hashToIndices.reserve(rerankWindow);
                for (size_t i = 0; i < rerankWindow; ++i) {
                    const bool snippetLooksLikePath =
                        !response.results[i].snippet.empty() &&
                        response.results[i].snippet == response.results[i].document.filePath;
                    if (!response.results[i].snippet.empty() && !snippetLooksLikePath) {
                        continue;
                    }
                    const std::string& hash = response.results[i].document.sha256Hash;
                    if (!hash.empty()) {
                        hashToIndices[hash].push_back(i);
                    }
                }

                if (!hashToIndices.empty()) {
                    std::vector<std::string> hashes;
                    hashes.reserve(hashToIndices.size());
                    for (const auto& [hash, _] : hashToIndices) {
                        hashes.push_back(hash);
                    }

                    auto docMapResult = metadataRepo_->batchGetDocumentsByHash(hashes);
                    if (docMapResult) {
                        const size_t previewLimit = std::max<size_t>(
                            workingConfig.rerankSnippetMaxChars + 64,
                            std::max<size_t>(workingConfig.rerankSnippetMaxChars * 4, 1024));
                        std::vector<int64_t> docIds;
                        std::unordered_map<int64_t, std::vector<size_t>> docIdToIndices;
                        docIds.reserve(docMapResult.value().size());
                        docIdToIndices.reserve(docMapResult.value().size());

                        for (const auto& [hash, docInfo] : docMapResult.value()) {
                            if (docInfo.id <= 0) {
                                continue;
                            }
                            auto it = hashToIndices.find(hash);
                            if (it == hashToIndices.end()) {
                                continue;
                            }
                            docIds.push_back(docInfo.id);
                            docIdToIndices.emplace(docInfo.id, it->second);
                        }

                        if (!docIds.empty()) {
                            auto previewResult = metadataRepo_->batchGetContentPreview(
                                docIds, static_cast<int>(previewLimit), 0);
                            if (previewResult) {
                                metadataPreviewByIndex.reserve(rerankWindow);
                                for (const auto& [docId, preview] : previewResult.value()) {
                                    if (preview.empty()) {
                                        continue;
                                    }
                                    auto indicesIt = docIdToIndices.find(docId);
                                    if (indicesIt == docIdToIndices.end()) {
                                        continue;
                                    }
                                    for (size_t idx : indicesIt->second) {
                                        metadataPreviewByIndex[idx] = preview;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            for (size_t i = 0; i < rerankWindow; ++i) {
                std::string text;
                const bool snippetLooksLikePath =
                    !response.results[i].snippet.empty() &&
                    response.results[i].snippet == response.results[i].document.filePath;
                if (!response.results[i].snippet.empty() && !snippetLooksLikePath) {
                    text = response.results[i].snippet;
                } else if (auto it = metadataPreviewByIndex.find(i);
                           it != metadataPreviewByIndex.end()) {
                    text = it->second;
                } else if (!response.results[i].snippet.empty()) {
                    text = response.results[i].snippet;
                } else if (!response.results[i].document.filePath.empty()) {
                    // Fallback: read content from source file on disk
                    std::ifstream ifs(response.results[i].document.filePath,
                                      std::ios::in | std::ios::binary);
                    if (ifs) {
                        const size_t readLimit = workingConfig.rerankSnippetMaxChars + 64;
                        std::string buf(readLimit, '\0');
                        ifs.read(buf.data(), static_cast<std::streamsize>(readLimit));
                        buf.resize(static_cast<size_t>(ifs.gcount()));
                        if (!buf.empty()) {
                            text = std::move(buf);
                            spdlog::debug("[reranker] Loaded {} bytes from file for doc {}",
                                          text.size(), i);
                        }
                    }
                }

                if (!text.empty()) {
                    auto passages =
                        buildRerankPassages(query, text, workingConfig.rerankSnippetMaxChars);
                    for (auto& passage : passages) {
                        snippets.push_back(std::move(passage));
                        rerankPassageDocIndices.push_back(i);
                    }
                } else {
                    spdlog::debug("[reranker] Skipping doc {} (no snippet or file content)", i);
                }
            }

            if (!snippets.empty()) {
                auto rerankResult = reranker_->scoreDocuments(query, snippets);
                if (rerankResult) {
                    rerankerCooldownUntilMicros_.store(0, std::memory_order_relaxed);
                    const auto& scores = rerankResult.value();
                    spdlog::debug("[reranker] Reranked {} documents", scores.size());

                    // Diagnostic: log score distribution for debugging reranker quality
                    if (!scores.empty()) {
                        float minScore = *std::min_element(scores.begin(), scores.end());
                        float maxScore = *std::max_element(scores.begin(), scores.end());
                        float sumScore = 0.0f;
                        for (float s : scores)
                            sumScore += s;
                        spdlog::info("[reranker] Score distribution: n={} min={:.4f} max={:.4f} "
                                     "mean={:.4f}",
                                     scores.size(), minScore, maxScore, sumScore / scores.size());
                    }

                    // Apply reranker scores to eligible results.
                    // Compute effective blend weight (may be adaptive).
                    double effectiveWeight = workingConfig.rerankWeight;
                    if (!workingConfig.rerankReplaceScores && workingConfig.rerankAdaptiveBlend &&
                        !scores.empty()) {
                        float maxRerankScore = *std::max_element(scores.begin(), scores.end());
                        // Scale weight by reranker confidence, floor prevents near-zero
                        effectiveWeight = std::clamp(
                            static_cast<double>(workingConfig.rerankWeight) * maxRerankScore,
                            static_cast<double>(workingConfig.rerankAdaptiveFloor),
                            static_cast<double>(workingConfig.rerankWeight));
                        spdlog::debug("[reranker] Adaptive blend: maxScore={:.4f} "
                                      "effectiveWeight={:.4f} (base={:.3f})",
                                      maxRerankScore, effectiveWeight, workingConfig.rerankWeight);
                    }

                    std::vector<double> bestRerankScoreByDoc(rerankWindow, 0.0);
                    std::vector<bool> rerankScorePresentByDoc(rerankWindow, false);
                    for (size_t i = 0; i < scores.size() && i < rerankPassageDocIndices.size();
                         ++i) {
                        const size_t idx = rerankPassageDocIndices[i];
                        if (idx >= rerankWindow) {
                            continue;
                        }
                        const double rerankScore = static_cast<double>(scores[i]);
                        if (!rerankScorePresentByDoc[idx] ||
                            rerankScore > bestRerankScoreByDoc[idx]) {
                            bestRerankScoreByDoc[idx] = rerankScore;
                            rerankScorePresentByDoc[idx] = true;
                        }
                    }

                    for (size_t idx = 0; idx < rerankWindow; ++idx) {
                        if (!rerankScorePresentByDoc[idx]) {
                            continue;
                        }
                        const double originalScore = response.results[idx].score;
                        const double rerankScore = bestRerankScoreByDoc[idx];

                        if (workingConfig.rerankReplaceScores) {
                            response.results[idx].score = rerankScore;
                        } else {
                            response.results[idx].score = rerankScore * effectiveWeight +
                                                          originalScore * (1.0 - effectiveWeight);
                        }
                        response.results[idx].rerankerScore = rerankScore;
                    }

                    // Re-sort by new scores (only the top window needs sorting)
                    std::sort(response.results.begin(),
                              response.results.begin() + static_cast<ptrdiff_t>(rerankWindow),
                              [](const SearchResult& a, const SearchResult& b) {
                                  return a.score > b.score;
                              });

                    if (stageTraceEnabled) {
                        rerankWindowTrace = json::array();
                        std::unordered_map<std::string, size_t> finalRanks;
                        finalRanks.reserve(rerankWindow);
                        for (size_t i = 0; i < rerankWindow; ++i) {
                            finalRanks[documentIdForTrace(
                                response.results[i].document.filePath,
                                response.results[i].document.sha256Hash)] = i + 1;
                        }

                        for (size_t i = 0; i < preRerankSnapshot.size(); ++i) {
                            const auto& before = preRerankSnapshot[i];
                            const std::string docId = documentIdForTrace(
                                before.document.filePath, before.document.sha256Hash);
                            json entry = {
                                {"doc_id", docId},
                                {"pre_rank", i + 1},
                                {"original_score", before.score},
                                {"keyword_score", before.keywordScore.value_or(0.0)},
                                {"vector_score", before.vectorScore.value_or(0.0)},
                                {"kg_score", before.kgScore.value_or(0.0)},
                                {"path_score", before.pathScore.value_or(0.0)},
                                {"tag_score", before.tagScore.value_or(0.0)},
                                {"symbol_score", before.symbolScore.value_or(0.0)},
                            };
                            bool found = false;
                            for (size_t j = 0; j < rerankWindow; ++j) {
                                const auto& after = response.results[j];
                                if (documentIdForTrace(after.document.filePath,
                                                       after.document.sha256Hash) == docId) {
                                    entry["final_rank"] = j + 1;
                                    entry["final_score"] = after.score;
                                    entry["reranker_score"] = after.rerankerScore.value_or(0.0);
                                    found = true;
                                    break;
                                }
                            }
                            if (!found) {
                                entry["final_rank"] = nullptr;
                                entry["final_score"] = nullptr;
                                entry["reranker_score"] = nullptr;
                            }
                            rerankWindowTrace.push_back(std::move(entry));
                        }
                    }

                    crossRerankApplied = true;
                    contributing.push_back("reranker");
                    auto rerankEnd = std::chrono::steady_clock::now();
                    const auto rerankDuration =
                        std::chrono::duration_cast<std::chrono::microseconds>(rerankEnd -
                                                                              rerankStart)
                            .count();
                    componentTiming["reranker"] = rerankDuration;
                    traceCollector.markStageResult("reranker", {}, rerankDuration, true);
                } else {
                    if (isRerankerCooldownError(rerankResult.error().code)) {
                        const int64_t nextCooldown =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                (std::chrono::steady_clock::now() + kRerankerErrorCooldown)
                                    .time_since_epoch())
                                .count();
                        rerankerCooldownUntilMicros_.store(nextCooldown, std::memory_order_relaxed);
                        spdlog::warn(
                            "[reranker] Reranking unavailable (code={}): {}. Cooling down "
                            "for {}s",
                            static_cast<int>(rerankResult.error().code),
                            rerankResult.error().message,
                            std::chrono::duration_cast<std::chrono::seconds>(kRerankerErrorCooldown)
                                .count());
                    } else {
                        spdlog::warn("[reranker] Reranking failed: {}",
                                     rerankResult.error().message);
                    }
                    auto rerankEnd = std::chrono::steady_clock::now();
                    traceCollector.markStageFailure(
                        "reranker", std::chrono::duration_cast<std::chrono::microseconds>(
                                        rerankEnd - rerankStart)
                                        .count());
                }
            } else {
                spdlog::debug("[reranker] Skipping rerank: no snippets available");
                auto rerankEnd = std::chrono::steady_clock::now();
                traceCollector.markStageSkipped("reranker", "no_snippets_available");
                componentTiming["reranker"] =
                    std::chrono::duration_cast<std::chrono::microseconds>(rerankEnd - rerankStart)
                        .count();
            }
        }
    } else if (workingConfig.enableReranking && !rerankAvailable) {
        spdlog::debug("[reranker] Unavailable; falling back to fused scores");
        traceCollector.markStageSkipped("reranker", "unavailable");
    }

    const size_t compactRerankWindow = std::min(
        response.results.size(), (workingConfig.enableReranking && workingConfig.rerankTopK > 0)
                                     ? workingConfig.rerankTopK
                                     : size_t(0));
    if (effectiveVectorMaxResults == 0) {
        effectiveVectorMaxResults = workingConfig.vectorMaxResults;
    }
    if (effectiveEntityVectorMaxResults == 0) {
        effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
    }
    response.debugStats["compact_pre_fusion_count"] = std::to_string(preFusionDocIds.size());
    response.debugStats["compact_post_fusion_count"] = std::to_string(
        postFusionSnapshot.empty() ? response.results.size() : postFusionSnapshot.size());
    response.debugStats["compact_rerank_window_count"] = std::to_string(compactRerankWindow);
    response.debugStats["compact_final_count"] = std::to_string(response.results.size());
    response.debugStats["compact_effective_vector_max_results"] =
        std::to_string(effectiveVectorMaxResults);
    response.debugStats["compact_effective_entity_vector_max_results"] =
        std::to_string(effectiveEntityVectorMaxResults);
    response.debugStats["compact_weak_query_fanout_boost_applied"] =
        weakQueryFanoutBoostApplied ? "1" : "0";

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
                const size_t minChunkSize =
                    std::max<size_t>(1, workingConfig.minChunkSizeForParallel);
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

    const auto lexicalAnchorScore = [](const SearchResult& r) {
        return r.keywordScore.value_or(0.0) + r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
               r.symbolScore.value_or(0.0);
    };

    const auto docIdForResult = [](const SearchResult& result) {
        return documentIdForTrace(result.document.filePath, result.document.sha256Hash);
    };

    const double semanticRescueMinVectorScore =
        std::max(0.0, static_cast<double>(workingConfig.semanticRescueMinVectorScore));

    std::unordered_map<std::string, size_t> currentScoreRankByDoc;
    if (workingConfig.semanticRescueSlots > 0 && !response.results.empty()) {
        std::vector<size_t> rankOrder(response.results.size());
        std::iota(rankOrder.begin(), rankOrder.end(), 0);
        std::sort(rankOrder.begin(), rankOrder.end(), [&](size_t lhs, size_t rhs) {
            const auto& a = response.results[lhs];
            const auto& b = response.results[rhs];
            if (a.score != b.score) {
                return a.score > b.score;
            }
            return documentIdForTrace(a.document.filePath, a.document.sha256Hash) <
                   documentIdForTrace(b.document.filePath, b.document.sha256Hash);
        });

        for (size_t i = 0; i < rankOrder.size(); ++i) {
            const auto& result = response.results[rankOrder[i]];
            const std::string docId =
                documentIdForTrace(result.document.filePath, result.document.sha256Hash);
            if (!docId.empty()) {
                currentScoreRankByDoc.emplace(docId, i + 1);
            }
        }
    }

    const auto semanticRescueSignalForResult =
        [&preFusionSignals,
         &docIdForResult](const SearchResult& result) -> const PreFusionDocSignal* {
        const std::string docId = docIdForResult(result);
        if (docId.empty()) {
            return nullptr;
        }
        auto it = preFusionSignals.find(docId);
        return it != preFusionSignals.end() ? &it->second : nullptr;
    };

    const auto isFinalSemanticRescueCandidate =
        [&lexicalAnchorScore, &semanticRescueSignalForResult,
         semanticRescueMinVectorScore](const SearchResult& result) {
            const auto* signal = semanticRescueSignalForResult(result);
            return signal != nullptr && !signal->hasAnchoring && signal->hasVector &&
                   signal->maxVectorRaw >= semanticRescueMinVectorScore &&
                   lexicalAnchorScore(result) <= 0.0;
        };

    const size_t buriedVectorRankThreshold =
        (intent == QueryIntent::Prose && workingConfig.enableReranking &&
         workingConfig.rerankTopK > 0)
            ? std::max<size_t>(150, workingConfig.rerankTopK * 3)
            : std::numeric_limits<size_t>::max();
    const size_t buriedScoreRankThreshold =
        buriedVectorRankThreshold != std::numeric_limits<size_t>::max() &&
                buriedVectorRankThreshold > 10
            ? buriedVectorRankThreshold - 10
            : std::numeric_limits<size_t>::max();

    const auto isBuriedFinalSemanticRescueCandidate =
        [&isFinalSemanticRescueCandidate, &semanticRescueSignalForResult, &docIdForResult,
         &currentScoreRankByDoc, buriedVectorRankThreshold,
         buriedScoreRankThreshold](const SearchResult& result) {
            if (!isFinalSemanticRescueCandidate(result)) {
                return false;
            }
            const auto* signal = semanticRescueSignalForResult(result);
            if (signal == nullptr || result.rerankerScore.value_or(0.0) < 9e-4 ||
                signal->maxVectorRaw < 0.79) {
                return false;
            }

            const std::string docId = docIdForResult(result);
            const auto rankIt = currentScoreRankByDoc.find(docId);
            const size_t currentScoreRank =
                rankIt != currentScoreRankByDoc.end() ? rankIt->second : 0;

            return currentScoreRank >= buriedScoreRankThreshold &&
                   signal->bestVectorRank != std::numeric_limits<size_t>::max() &&
                   signal->bestVectorRank >= buriedVectorRankThreshold;
        };

    const auto finalSemanticRescueBetter = [&semanticRescueSignalForResult, &docIdForResult](
                                               const SearchResult& a, const SearchResult& b) {
        const bool rerankCoveredA = a.rerankerScore.has_value();
        const bool rerankCoveredB = b.rerankerScore.has_value();
        if (rerankCoveredA != rerankCoveredB) {
            return rerankCoveredA;
        }

        const double rerankA = a.rerankerScore.value_or(-1.0);
        const double rerankB = b.rerankerScore.value_or(-1.0);
        if (rerankA != rerankB) {
            return rerankA > rerankB;
        }

        const auto* signalA = semanticRescueSignalForResult(a);
        const auto* signalB = semanticRescueSignalForResult(b);
        const double rawVectorA = signalA != nullptr ? signalA->maxVectorRaw : 0.0;
        const double rawVectorB = signalB != nullptr ? signalB->maxVectorRaw : 0.0;
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }

        if (a.score != b.score) {
            return a.score > b.score;
        }

        return docIdForResult(a) < docIdForResult(b);
    };

    const auto buriedSemanticRescueBetter = [&semanticRescueSignalForResult, &docIdForResult](
                                                const SearchResult& a, const SearchResult& b) {
        const bool rerankCoveredA = a.rerankerScore.has_value();
        const bool rerankCoveredB = b.rerankerScore.has_value();
        if (rerankCoveredA != rerankCoveredB) {
            return rerankCoveredA;
        }

        constexpr double kBuriedRerankCap = 1e-3;
        constexpr double kBuriedRerankTieEpsilon = 2e-4;
        const double rerankA = std::min(a.rerankerScore.value_or(-1.0), kBuriedRerankCap);
        const double rerankB = std::min(b.rerankerScore.value_or(-1.0), kBuriedRerankCap);
        if (std::abs(rerankA - rerankB) > kBuriedRerankTieEpsilon) {
            return rerankA > rerankB;
        }

        const auto* signalA = semanticRescueSignalForResult(a);
        const auto* signalB = semanticRescueSignalForResult(b);
        const double rawVectorA = signalA != nullptr ? signalA->maxVectorRaw : 0.0;
        const double rawVectorB = signalB != nullptr ? signalB->maxVectorRaw : 0.0;
        if (rawVectorA != rawVectorB) {
            return rawVectorA > rawVectorB;
        }

        const double fullRerankA = a.rerankerScore.value_or(-1.0);
        const double fullRerankB = b.rerankerScore.value_or(-1.0);
        if (fullRerankA != fullRerankB) {
            return fullRerankA > fullRerankB;
        }

        if (a.score != b.score) {
            return a.score > b.score;
        }

        return docIdForResult(a) < docIdForResult(b);
    };

    std::vector<std::string> semanticRescuePromotedDocIds;
    std::vector<std::string> semanticRescueDisplacedDocIds;
    std::vector<std::string> buriedSemanticRescuePromotedDocIds;
    std::vector<std::string> buriedSemanticRescueDisplacedDocIds;

    // Enforce user-visible limit after all post-fusion stages have had a chance to reorder/boost.
    if (response.results.size() > userLimit) {
        std::partial_sort(
            response.results.begin(), response.results.begin() + static_cast<ptrdiff_t>(userLimit),
            response.results.end(),
            [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

        if (workingConfig.semanticRescueSlots > 0 && userLimit > 0) {
            const size_t finalWindow = std::min(userLimit, response.results.size());
            const size_t rescueTarget = std::min(workingConfig.semanticRescueSlots, finalWindow);
            size_t rescuePresent = 0;
            for (size_t i = 0; i < finalWindow; ++i) {
                if (isFinalSemanticRescueCandidate(response.results[i])) {
                    rescuePresent++;
                }
            }

            while (rescuePresent < rescueTarget) {
                size_t bestTailIndex = response.results.size();
                for (size_t i = finalWindow; i < response.results.size(); ++i) {
                    if (!isFinalSemanticRescueCandidate(response.results[i])) {
                        continue;
                    }
                    if (bestTailIndex >= response.results.size() ||
                        finalSemanticRescueBetter(response.results[i],
                                                  response.results[bestTailIndex])) {
                        bestTailIndex = i;
                    }
                }
                if (bestTailIndex >= response.results.size()) {
                    break;
                }

                size_t victimIndex = finalWindow;
                for (size_t i = finalWindow; i > 0; --i) {
                    const size_t idx = i - 1;
                    if (!isFinalSemanticRescueCandidate(response.results[idx])) {
                        victimIndex = idx;
                        break;
                    }
                }
                if (victimIndex >= finalWindow) {
                    break;
                }

                const std::string promotedId = docIdForResult(response.results[bestTailIndex]);
                const std::string displacedId = docIdForResult(response.results[victimIndex]);
                if (!promotedId.empty()) {
                    semanticRescuePromotedDocIds.push_back(promotedId);
                }
                if (!displacedId.empty()) {
                    semanticRescueDisplacedDocIds.push_back(displacedId);
                }

                std::swap(response.results[victimIndex], response.results[bestTailIndex]);
                rescuePresent++;
            }

            std::sort(
                response.results.begin(),
                response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

            const size_t buriedRescueTarget =
                buriedVectorRankThreshold != std::numeric_limits<size_t>::max() ? size_t(1)
                                                                                : size_t(0);
            size_t buriedRescuePresent = 0;
            for (size_t i = 0; i < finalWindow; ++i) {
                if (isBuriedFinalSemanticRescueCandidate(response.results[i])) {
                    buriedRescuePresent++;
                }
            }

            while (buriedRescuePresent < buriedRescueTarget) {
                size_t bestTailIndex = response.results.size();
                for (size_t i = finalWindow; i < response.results.size(); ++i) {
                    if (!isBuriedFinalSemanticRescueCandidate(response.results[i])) {
                        continue;
                    }
                    if (bestTailIndex >= response.results.size() ||
                        buriedSemanticRescueBetter(response.results[i],
                                                   response.results[bestTailIndex])) {
                        bestTailIndex = i;
                    }
                }
                if (bestTailIndex >= response.results.size()) {
                    break;
                }

                size_t victimIndex = finalWindow;
                for (size_t i = finalWindow; i > 0; --i) {
                    const size_t idx = i - 1;
                    if (!isBuriedFinalSemanticRescueCandidate(response.results[idx])) {
                        victimIndex = idx;
                        break;
                    }
                }
                if (victimIndex >= finalWindow) {
                    break;
                }

                const std::string promotedId = docIdForResult(response.results[bestTailIndex]);
                const std::string displacedId = docIdForResult(response.results[victimIndex]);
                if (!promotedId.empty()) {
                    buriedSemanticRescuePromotedDocIds.push_back(promotedId);
                }
                if (!displacedId.empty()) {
                    buriedSemanticRescueDisplacedDocIds.push_back(displacedId);
                }

                std::swap(response.results[victimIndex], response.results[bestTailIndex]);
                buriedRescuePresent++;
            }

            if (buriedRescueTarget > 0) {
                std::sort(
                    response.results.begin(),
                    response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                    [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });
            }
        }

        response.results.resize(userLimit);
    }

    size_t semanticRescueFinalCount = 0;
    std::vector<std::string> semanticRescueFinalDocIds;
    if (workingConfig.semanticRescueSlots > 0 && !response.results.empty()) {
        for (const auto& result : response.results) {
            if (isFinalSemanticRescueCandidate(result)) {
                semanticRescueFinalCount++;
                const std::string docId = docIdForResult(result);
                if (!docId.empty()) {
                    semanticRescueFinalDocIds.push_back(docId);
                }
            }
        }
    }
    const size_t semanticRescueTarget =
        std::min(workingConfig.semanticRescueSlots, response.results.size());
    const double semanticRescueRate = semanticRescueTarget > 0
                                          ? static_cast<double>(semanticRescueFinalCount) /
                                                static_cast<double>(semanticRescueTarget)
                                          : 0.0;

    response.debugStats["semantic_rescue_enabled"] =
        workingConfig.semanticRescueSlots > 0 ? "1" : "0";
    response.debugStats["semantic_rescue_slots"] =
        std::to_string(workingConfig.semanticRescueSlots);
    response.debugStats["semantic_rescue_target"] = std::to_string(semanticRescueTarget);
    response.debugStats["semantic_rescue_final_count"] = std::to_string(semanticRescueFinalCount);
    response.debugStats["semantic_rescue_final_doc_ids"] = joinWithTab(semanticRescueFinalDocIds);
    response.debugStats["semantic_rescue_promoted_doc_ids"] =
        joinWithTab(semanticRescuePromotedDocIds);
    response.debugStats["semantic_rescue_displaced_doc_ids"] =
        joinWithTab(semanticRescueDisplacedDocIds);
    response.debugStats["semantic_rescue_buried_promoted_doc_ids"] =
        joinWithTab(buriedSemanticRescuePromotedDocIds);
    response.debugStats["semantic_rescue_buried_displaced_doc_ids"] =
        joinWithTab(buriedSemanticRescueDisplacedDocIds);
    response.debugStats["semantic_rescue_rate"] = fmt::format("{:.3f}", semanticRescueRate);
    response.debugStats["multi_vector_generated_phrases"] =
        std::to_string(multiVectorGeneratedPhrases);
    response.debugStats["multi_vector_raw_hit_count"] = std::to_string(multiVectorPhraseHits);
    response.debugStats["multi_vector_added_new_count"] = std::to_string(multiVectorAddedNewCount);
    response.debugStats["multi_vector_replaced_base_count"] =
        std::to_string(multiVectorReplacedBaseCount);
    response.debugStats["subphrase_generated_count"] =
        std::to_string(textExpansionStats.generatedSubPhrases);
    response.debugStats["subphrase_clause_count"] =
        std::to_string(textExpansionStats.subPhraseClauseCount);
    response.debugStats["subphrase_fts_hit_count"] =
        std::to_string(textExpansionStats.subPhraseFtsHitCount);
    response.debugStats["subphrase_fts_added_count"] =
        std::to_string(textExpansionStats.subPhraseFtsAddedCount);
    response.debugStats["aggressive_fts_clause_count"] =
        std::to_string(textExpansionStats.aggressiveClauseCount);
    response.debugStats["aggressive_fts_hit_count"] =
        std::to_string(textExpansionStats.aggressiveFtsHitCount);
    response.debugStats["aggressive_fts_added_count"] =
        std::to_string(textExpansionStats.aggressiveFtsAddedCount);
    response.debugStats["graph_expansion_term_count"] =
        std::to_string(textExpansionStats.graphExpansionTermCount);
    response.debugStats["graph_expansion_fts_hit_count"] =
        std::to_string(textExpansionStats.graphExpansionFtsHitCount);
    response.debugStats["graph_expansion_fts_added_count"] =
        std::to_string(textExpansionStats.graphExpansionFtsAddedCount);
    response.debugStats["graph_doc_expansion_term_count"] =
        std::to_string(graphDocExpansionTermCount);
    response.debugStats["graph_doc_expansion_fts_hit_count"] =
        std::to_string(graphDocExpansionFtsHitCount);
    response.debugStats["graph_doc_expansion_fts_added_count"] =
        std::to_string(graphDocExpansionFtsAddedCount);
    response.debugStats["graph_text_blocked_low_score_count"] =
        std::to_string(textExpansionStats.graphTextBlockedLowScoreCount);
    if (!docSeedGraphTerms.empty()) {
        json graphDocTerms = json::array();
        for (const auto& term : docSeedGraphTerms) {
            graphDocTerms.push_back({{"text", term.text}, {"score", term.score}});
        }
        response.debugStats["graph_doc_expansion_terms_json"] = graphDocTerms.dump();
    }
    response.debugStats["graph_vector_generated_terms"] = std::to_string(graphVectorGeneratedTerms);
    response.debugStats["graph_vector_raw_hit_count"] = std::to_string(graphVectorRawHitCount);
    response.debugStats["graph_vector_added_new_count"] = std::to_string(graphVectorAddedNewCount);
    response.debugStats["graph_vector_replaced_base_count"] =
        std::to_string(graphVectorReplacedBaseCount);
    response.debugStats["graph_vector_blocked_uncorroborated_count"] =
        std::to_string(graphVectorBlockedUncorroboratedCount);
    response.debugStats["graph_vector_blocked_missing_text_anchor_count"] =
        std::to_string(graphVectorBlockedMissingTextAnchorCount);
    response.debugStats["graph_vector_blocked_missing_baseline_text_anchor_count"] =
        std::to_string(graphVectorBlockedMissingBaselineTextAnchorCount);
    response.debugStats["strong_vector_only_relief_enabled"] =
        workingConfig.enableStrongVectorOnlyRelief ? "1" : "0";
    response.debugStats["strong_vector_only_min_score"] =
        fmt::format("{:.3f}", workingConfig.strongVectorOnlyMinScore);
    response.debugStats["strong_vector_only_top_rank"] =
        std::to_string(workingConfig.strongVectorOnlyTopRank);
    response.debugStats["strong_vector_only_penalty"] =
        fmt::format("{:.3f}", workingConfig.strongVectorOnlyPenalty);
    response.debugStats["graph_query_concept_count"] = std::to_string(graphQueryConceptCount);
    response.debugStats["graph_query_neighbor_seed_docs"] =
        std::to_string(graphQueryNeighborSeedDocCount);
    response.debugStats["graph_window_guard_replacement_count"] =
        std::to_string(graphWindowGuardReplacementCount);
    response.debugStats["graph_window_cap_replacement_count"] =
        std::to_string(graphWindowCapReplacementCount);
    response.debugStats["graph_matched_candidates"] = std::to_string(graphMatchedCandidates);
    response.debugStats["graph_positive_signal_candidates"] =
        std::to_string(graphPositiveSignalCandidates);
    response.debugStats["graph_boosted_docs"] = std::to_string(graphBoostedDocs);
    response.debugStats["graph_max_signal"] = fmt::format("{:.4f}", graphMaxSignal);
    response.debugStats["rerank_window_trace_json"] = rerankWindowTrace.dump();
    if (workingConfig.semanticRescueSlots > 0 && !response.results.empty()) {
        spdlog::debug(
            "[semantic_rescue] final_count={} target={} rate={:.3f} min_vector_score={:.4f}",
            semanticRescueFinalCount, semanticRescueTarget, semanticRescueRate,
            workingConfig.semanticRescueMinVectorScore);
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
    if (workingConfig.includeComponentTiming) {
        response.componentTimingMicros = std::move(componentTiming);
    }
    response.isDegraded =
        !response.timedOutComponents.empty() || !response.failedComponents.empty();

    if (stageTraceEnabled) {
        const size_t traceTopDefault =
            std::max(userLimit, std::max(workingConfig.rerankTopK, workingConfig.graphRerankTopN));
        const size_t traceTopCount =
            envSizeTOrDefault("YAMS_SEARCH_STAGE_TRACE_TOP_N", traceTopDefault, 1, 10000);
        const size_t componentTopDefault = std::min<size_t>(traceTopCount, 25);
        const size_t componentTopCount =
            std::min(traceTopCount, envSizeTOrDefault("YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N",
                                                      componentTopDefault, 1, 10000));

        const std::vector<std::string> postFusionAllDocIds =
            collectRankedResultDocIds(postFusionSnapshot);
        const std::vector<std::string> graphlessPostFusionAllDocIds = collectRankedResultDocIds(
            graphlessPostFusionSnapshot.empty() ? postFusionSnapshot : graphlessPostFusionSnapshot);
        const std::vector<std::string> postGraphAllDocIds = collectRankedResultDocIds(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot);
        const std::vector<std::string> finalAllDocIds = collectRankedResultDocIds(response.results);

        const std::vector<std::string> fusionDroppedDocIds =
            setDifferenceIds(preFusionDocIds, postFusionAllDocIds);
        const std::vector<std::string> graphAddedPostFusionDocIds =
            setDifferenceIds(postFusionAllDocIds, graphlessPostFusionAllDocIds);
        const std::vector<std::string> graphDisplacedPostFusionDocIds =
            setDifferenceIds(graphlessPostFusionAllDocIds, postFusionAllDocIds);

        size_t vectorOnlyDocs = 0;
        size_t vectorOnlyBelowThreshold = 0;
        size_t vectorOnlyAboveThreshold = 0;
        size_t vectorOnlyNearMissEligible = 0;
        size_t strongVectorOnlyDocs = 0;
        size_t strongVectorOnlyScoreEligibleDocs = 0;
        size_t strongVectorOnlyRankEligibleDocs = 0;
        size_t anchorAndVectorDocs = 0;
        size_t anchorOnlyDocs = 0;
        std::vector<std::pair<std::string, double>> vectorOnlyBelowDocs;
        std::vector<std::pair<std::string, double>> vectorOnlyAboveDocs;

        const double nearMissSlack =
            std::clamp(static_cast<double>(workingConfig.vectorOnlyNearMissSlack), 0.0, 1.0);
        const bool nearMissReserveEnabled = workingConfig.vectorOnlyNearMissReserve > 0;

        for (const auto& [docId, signal] : preFusionSignals) {
            if (signal.hasAnchoring && signal.hasVector) {
                anchorAndVectorDocs++;
            } else if (signal.hasAnchoring && !signal.hasVector) {
                anchorOnlyDocs++;
            }

            if (signal.hasVector && !signal.hasAnchoring) {
                vectorOnlyDocs++;
                const bool scoreEligible =
                    workingConfig.enableStrongVectorOnlyRelief &&
                    signal.maxVectorRaw >=
                        static_cast<double>(workingConfig.strongVectorOnlyMinScore);
                const bool rankEligible =
                    workingConfig.enableStrongVectorOnlyRelief &&
                    workingConfig.strongVectorOnlyTopRank > 0 &&
                    signal.bestVectorRank != std::numeric_limits<size_t>::max() &&
                    signal.bestVectorRank < workingConfig.strongVectorOnlyTopRank;
                if (scoreEligible) {
                    strongVectorOnlyScoreEligibleDocs++;
                }
                if (rankEligible) {
                    strongVectorOnlyRankEligibleDocs++;
                }
                if (scoreEligible || rankEligible) {
                    strongVectorOnlyDocs++;
                }

                if (signal.maxVectorRaw < static_cast<double>(workingConfig.vectorOnlyThreshold)) {
                    vectorOnlyBelowThreshold++;
                    vectorOnlyBelowDocs.emplace_back(docId, signal.maxVectorRaw);
                    if (nearMissReserveEnabled &&
                        signal.maxVectorRaw + nearMissSlack >=
                            static_cast<double>(workingConfig.vectorOnlyThreshold)) {
                        vectorOnlyNearMissEligible++;
                    }
                } else {
                    vectorOnlyAboveThreshold++;
                    vectorOnlyAboveDocs.emplace_back(docId, signal.maxVectorRaw);
                }
            }
        }

        auto byScoreDesc = [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;
            }
            return a.first < b.first;
        };
        std::sort(vectorOnlyBelowDocs.begin(), vectorOnlyBelowDocs.end(), byScoreDesc);
        std::sort(vectorOnlyAboveDocs.begin(), vectorOnlyAboveDocs.end(), byScoreDesc);

        std::vector<std::string> vectorOnlyBelowTop;
        std::vector<std::string> vectorOnlyAboveTop;
        vectorOnlyBelowTop.reserve(std::min(componentTopCount, vectorOnlyBelowDocs.size()));
        vectorOnlyAboveTop.reserve(std::min(componentTopCount, vectorOnlyAboveDocs.size()));
        for (size_t i = 0; i < std::min(componentTopCount, vectorOnlyBelowDocs.size()); ++i) {
            vectorOnlyBelowTop.push_back(vectorOnlyBelowDocs[i].first);
        }
        for (size_t i = 0; i < std::min(componentTopCount, vectorOnlyAboveDocs.size()); ++i) {
            vectorOnlyAboveTop.push_back(vectorOnlyAboveDocs[i].first);
        }

        const json componentSummary =
            buildComponentHitSummaryJson(allComponentResults, componentTopCount);
        const json fusionTopSummary = buildFusionTopSummaryJson(postFusionSnapshot, traceTopCount);
        const json graphlessFusionTopSummary = buildFusionTopSummaryJson(
            graphlessPostFusionSnapshot.empty() ? postFusionSnapshot : graphlessPostFusionSnapshot,
            traceTopCount);
        const json graphTopSummary = buildFusionTopSummaryJson(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot, traceTopCount);
        const json finalTopSummary = buildFusionTopSummaryJson(response.results, traceTopCount);
        const json graphDisplacementSummary = {
            {"graph_added_post_fusion_count", graphAddedPostFusionDocIds.size()},
            {"graph_displaced_post_fusion_count", graphDisplacedPostFusionDocIds.size()},
            {"graph_added_post_fusion_doc_ids", graphAddedPostFusionDocIds},
            {"graph_displaced_post_fusion_doc_ids", graphDisplacedPostFusionDocIds},
        };
        const json preFusionSignalSummary = {
            {"vector_only_docs", vectorOnlyDocs},
            {"vector_only_below_threshold", vectorOnlyBelowThreshold},
            {"vector_only_above_threshold", vectorOnlyAboveThreshold},
            {"vector_only_near_miss_eligible", vectorOnlyNearMissEligible},
            {"anchor_and_vector_docs", anchorAndVectorDocs},
            {"anchor_only_docs", anchorOnlyDocs},
            {"vector_only_threshold", workingConfig.vectorOnlyThreshold},
            {"vector_only_penalty", workingConfig.vectorOnlyPenalty},
            {"strong_vector_only_relief_enabled", workingConfig.enableStrongVectorOnlyRelief},
            {"strong_vector_only_min_score", workingConfig.strongVectorOnlyMinScore},
            {"strong_vector_only_top_rank", workingConfig.strongVectorOnlyTopRank},
            {"strong_vector_only_penalty", workingConfig.strongVectorOnlyPenalty},
            {"strong_vector_only_docs", strongVectorOnlyDocs},
            {"strong_vector_only_score_eligible_docs", strongVectorOnlyScoreEligibleDocs},
            {"strong_vector_only_rank_eligible_docs", strongVectorOnlyRankEligibleDocs},
            {"vector_only_near_miss_reserve", workingConfig.vectorOnlyNearMissReserve},
            {"vector_only_near_miss_slack", workingConfig.vectorOnlyNearMissSlack},
            {"vector_only_near_miss_penalty", workingConfig.vectorOnlyNearMissPenalty},
            {"semantic_rescue_slots", workingConfig.semanticRescueSlots},
            {"semantic_rescue_min_vector_score", workingConfig.semanticRescueMinVectorScore},
            {"semantic_rescue_target", semanticRescueTarget},
            {"semantic_rescue_final_count", semanticRescueFinalCount},
            {"semantic_rescue_rate", semanticRescueRate},
            {"adaptive_vector_fallback", workingConfig.enableAdaptiveVectorFallback},
            {"adaptive_vector_skip_min_tier1_hits", workingConfig.adaptiveVectorSkipMinTier1Hits},
            {"adaptive_vector_skip_require_text_signal",
             workingConfig.adaptiveVectorSkipRequireTextSignal},
            {"adaptive_vector_skip_min_text_hits", workingConfig.adaptiveVectorSkipMinTextHits},
            {"adaptive_vector_skip_min_top_text_score",
             workingConfig.adaptiveVectorSkipMinTopTextScore},
            {"vector_only_below_top_doc_ids", vectorOnlyBelowTop},
            {"vector_only_above_top_doc_ids", vectorOnlyAboveTop},
        };

        response.debugStats["trace_enabled"] = "1";
        response.debugStats["trace_query_intent"] = queryIntentToString(intent);
        response.debugStats["trace_user_limit"] = std::to_string(userLimit);
        response.debugStats["trace_fusion_candidate_limit"] = std::to_string(fusionCandidateLimit);
        response.debugStats["trace_top_window"] = std::to_string(traceTopCount);
        response.debugStats["trace_top_window_default"] = std::to_string(traceTopDefault);
        response.debugStats["trace_component_top_window"] = std::to_string(componentTopCount);
        response.debugStats["trace_component_top_window_default"] =
            std::to_string(componentTopDefault);
        response.debugStats["trace_graph_rerank_applied"] = graphRerankApplied ? "1" : "0";
        response.debugStats["trace_cross_rerank_applied"] = crossRerankApplied ? "1" : "0";

        response.debugStats["trace_pre_fusion_unique_count"] =
            std::to_string(preFusionDocIds.size());
        response.debugStats["trace_post_fusion_count"] = std::to_string(postFusionAllDocIds.size());
        response.debugStats["trace_post_graph_count"] = std::to_string(postGraphAllDocIds.size());
        response.debugStats["trace_final_count"] = std::to_string(finalAllDocIds.size());
        response.debugStats["trace_fusion_dropped_count"] =
            std::to_string(fusionDroppedDocIds.size());

        response.debugStats["trace_pre_fusion_doc_ids"] = joinWithTab(preFusionDocIds);
        response.debugStats["trace_post_fusion_doc_ids"] = joinWithTab(postFusionAllDocIds);
        response.debugStats["trace_graphless_post_fusion_doc_ids"] =
            joinWithTab(graphlessPostFusionAllDocIds);
        response.debugStats["trace_post_graph_doc_ids"] = joinWithTab(postGraphAllDocIds);
        response.debugStats["trace_final_doc_ids"] = joinWithTab(finalAllDocIds);
        response.debugStats["trace_fusion_dropped_doc_ids"] = joinWithTab(fusionDroppedDocIds);
        response.debugStats["trace_graph_added_post_fusion_doc_ids"] =
            joinWithTab(graphAddedPostFusionDocIds);
        response.debugStats["trace_graph_displaced_post_fusion_doc_ids"] =
            joinWithTab(graphDisplacedPostFusionDocIds);

        response.debugStats["trace_post_fusion_top_doc_ids"] =
            joinWithTab(collectRankedResultDocIds(postFusionSnapshot, traceTopCount));
        response.debugStats["trace_post_graph_top_doc_ids"] = joinWithTab(collectRankedResultDocIds(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot, traceTopCount));
        response.debugStats["trace_final_top_doc_ids"] =
            joinWithTab(collectRankedResultDocIds(response.results, traceTopCount));

        response.debugStats["trace_component_hits_json"] = componentSummary.dump();
        response.debugStats["trace_prefusion_signal_summary_json"] = preFusionSignalSummary.dump();
        response.debugStats["trace_fusion_top_json"] = fusionTopSummary.dump();
        response.debugStats["trace_graphless_fusion_top_json"] = graphlessFusionTopSummary.dump();
        response.debugStats["trace_post_graph_top_json"] = graphTopSummary.dump();
        response.debugStats["trace_final_top_json"] = finalTopSummary.dump();
        response.debugStats["trace_graph_displacement_summary_json"] =
            graphDisplacementSummary.dump();
    }

    response.debugStats["trace_stage_summary_json"] = traceCollector.buildStageSummaryJson().dump();
    response.debugStats["trace_fusion_source_summary_json"] =
        traceCollector
            .buildFusionSourceSummaryJson(allComponentResults, response.results,
                                          std::max<size_t>(userLimit, size_t{25}))
            .dump();

    if (tuner_) {
        SearchTuner::RuntimeTelemetry telemetry;
        telemetry.latencyMs =
            static_cast<double>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - startTime)
                                    .count());
        telemetry.finalResultCount = response.results.size();
        telemetry.topWindow = std::max<size_t>(userLimit, size_t{25});

        try {
            auto stageSummary = traceCollector.buildStageSummaryJson();
            if (stageSummary.is_object()) {
                for (const auto& [name, data] : stageSummary.items()) {
                    if (!data.is_object()) {
                        continue;
                    }
                    SearchTuner::RuntimeStageSignal signal;
                    signal.enabled = data.value("enabled", false);
                    signal.attempted = data.value("attempted", false);
                    signal.contributed = data.value("contributed", false);
                    signal.skipped = data.value("skipped", false);
                    signal.durationMs = data.value("duration_ms", 0.0);
                    signal.rawHitCount = data.value("raw_hit_count", 0UL);
                    signal.uniqueDocCount = data.value("unique_doc_count", 0UL);
                    telemetry.stages.emplace(name, signal);
                }
            }
        } catch (...) {
        }

        try {
            auto fusionSummary = traceCollector.buildFusionSourceSummaryJson(
                allComponentResults, response.results, std::max<size_t>(userLimit, size_t{25}));
            if (fusionSummary.is_object()) {
                for (const auto& [name, data] : fusionSummary.items()) {
                    if (!data.is_object()) {
                        continue;
                    }
                    SearchTuner::RuntimeFusionSignal signal;
                    signal.enabled = data.value("enabled", false);
                    signal.contributedToFinal = data.value("contributed_to_final", false);
                    signal.configuredWeight = data.value("weight", 0.0);
                    signal.finalScoreMass = data.value("final_score_mass", 0.0);
                    signal.finalTopDocCount = data.value("final_top_doc_count", 0UL);
                    signal.rawHitCount = data.value("raw_hit_count", 0UL);
                    signal.uniqueDocCount = data.value("unique_doc_count", 0UL);
                    telemetry.fusionSources.emplace(name, signal);
                }
            }
        } catch (...) {
        }

        tuner_->observe(telemetry);
        const auto tunerState = tuner_->adaptiveStateToJson();
        response.debugStats["tuner_adaptive_active"] = "1";
        response.debugStats["tuner_decision_reason"] =
            tunerState.value("last_decision", std::string{"unknown"});
        response.debugStats["tuner_adjustments_json"] = tunerState.dump();
        response.debugStats["tuner_runtime_config_json"] = tuner_->getParams().toJson().dump();
    } else {
        response.debugStats["tuner_adaptive_active"] = "0";
    }

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
        spdlog::debug("Search returned {} results (degraded: {} timed out, {} failed)",
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

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryFullText(const std::string& query, const SearchEngineConfig& config,
                                  size_t limit, QueryExpansionStats* expansionStats,
                                  const std::vector<GraphExpansionTerm>* graphExpansionTerms) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_) {
        return results;
    }

    try {
        const QueryIntent queryIntent = detectQueryIntent(query);
        float nonCodeFileMultiplier = 1.0f;
        if (config.enableIntentAdaptiveWeighting) {
            switch (queryIntent) {
                case QueryIntent::Code:
                    nonCodeFileMultiplier = 0.5f;
                    break;
                case QueryIntent::Path:
                    nonCodeFileMultiplier = 0.65f;
                    break;
                case QueryIntent::Prose:
                    nonCodeFileMultiplier = 1.0f;
                    break;
                case QueryIntent::Mixed:
                    nonCodeFileMultiplier = 0.80f;
                    break;
            }
        }

        auto appendResults = [&](const yams::metadata::SearchResults& searchResults,
                                 float scorePenalty, bool dedupe,
                                 std::unordered_set<std::string>* seenHashes,
                                 ComponentResult::Source source = ComponentResult::Source::Text) {
            double minBm25 = 0.0;
            double maxBm25 = 0.0;
            bool bm25RangeInitialized = false;
            for (const auto& sr : searchResults.results) {
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

            const size_t startRank = results.size();
            for (size_t rank = 0; rank < searchResults.results.size(); ++rank) {
                const auto& searchResult = searchResults.results[rank];
                if (dedupe && seenHashes != nullptr &&
                    seenHashes->contains(searchResult.document.sha256Hash)) {
                    continue;
                }

                const auto& filePath = searchResult.document.filePath;
                const auto& fileName = searchResult.document.fileName;

                auto pruneCategory = magic::getPruneCategory(filePath);
                bool isCodeFile = pruneCategory == magic::PruneCategory::BuildObject ||
                                  pruneCategory == magic::PruneCategory::None;

                float scoreMultiplier = isCodeFile ? 1.0f : nonCodeFileMultiplier;
                scoreMultiplier *= filenamePathBoost(query, filePath, fileName);
                scoreMultiplier *= scorePenalty;

                ComponentResult result;
                result.documentHash = searchResult.document.sha256Hash;
                result.filePath = filePath;
                float rawScore = static_cast<float>(searchResult.score);
                float normalizedScore =
                    normalizedBm25Score(rawScore, config.bm25NormDivisor, minBm25, maxBm25);
                result.score = std::clamp(scoreMultiplier * normalizedScore, 0.0f, 1.0f);
                if (source == ComponentResult::Source::GraphText &&
                    result.score < config.graphTextMinAdmissionScore) {
                    if (expansionStats != nullptr) {
                        ++expansionStats->graphTextBlockedLowScoreCount;
                    }
                    continue;
                }
                result.source = source;
                result.rank = startRank + rank;
                result.snippet = searchResult.snippet.empty()
                                     ? std::nullopt
                                     : std::optional<std::string>(searchResult.snippet);
                result.debugInfo["score_multiplier"] = fmt::format("{:.3f}", scoreMultiplier);

                if (seenHashes != nullptr) {
                    seenHashes->insert(result.documentHash);
                }
                results.push_back(std::move(result));
            }
        };

        auto fts5Results = metadataRepo_->search(query, limit, 0);
        size_t baseFtsHitCount = 0;
        const bool baseFtsSucceeded = static_cast<bool>(fts5Results);
        if (!baseFtsSucceeded) {
            spdlog::debug("FTS5 search failed, continuing with fallback expansion: {}",
                          fts5Results.error().message);
        }

        std::unordered_set<std::string> seenHashes;
        seenHashes.reserve(limit * 2);
        if (baseFtsSucceeded) {
            baseFtsHitCount = fts5Results.value().results.size();
            appendResults(fts5Results.value(), 1.0f, true, &seenHashes);
        }

        if (config.enableLexicalExpansion && baseFtsHitCount < config.lexicalExpansionMinHits) {
            std::vector<std::string> tokens = tokenizeLower(query);
            std::vector<std::string> expansionTerms;
            expansionTerms.reserve(tokens.size());
            std::unordered_set<std::string> uniqueTokens;
            uniqueTokens.reserve(tokens.size());

            for (const auto& token : tokens) {
                if (token.size() < 3) {
                    continue;
                }
                if (uniqueTokens.insert(token).second) {
                    expansionTerms.push_back(token);
                }
                if (expansionTerms.size() >= 6) {
                    break;
                }
            }

            if (expansionTerms.size() >= 2) {
                std::string expandedQuery;
                expandedQuery.reserve(expansionTerms.size() * 8);
                for (size_t i = 0; i < expansionTerms.size(); ++i) {
                    if (i > 0) {
                        expandedQuery += " OR ";
                    }
                    expandedQuery += expansionTerms[i];
                }

                auto expandedResults = metadataRepo_->search(expandedQuery, limit, 0);
                if (expandedResults) {
                    const float penalty =
                        std::clamp(config.lexicalExpansionScorePenalty, 0.1f, 1.0f);
                    appendResults(expandedResults.value(), penalty, true, &seenHashes);
                    spdlog::debug("queryFullText lexical expansion: base_hits={} expanded_hits={} "
                                  "query='{}' expanded='{}'",
                                  baseFtsHitCount, results.size(), query, expandedQuery);
                }
            }
        }

        if (config.enableSubPhraseExpansion && baseFtsHitCount < config.subPhraseExpansionMinHits) {
            const size_t maxSubPhrases = std::max<size_t>(config.multiVectorMaxPhrases, 3);
            auto subPhrases = generateQuerySubPhrases(query, maxSubPhrases);
            if (expansionStats != nullptr) {
                expansionStats->generatedSubPhrases = subPhrases.size();
            }

            std::vector<std::string> clauses;
            clauses.reserve(subPhrases.size());
            std::unordered_set<std::string> seenClauses;
            seenClauses.reserve(subPhrases.size());

            for (const auto& phrase : subPhrases) {
                auto tokens = tokenizeQueryTokens(phrase);
                if (tokens.size() < 2) {
                    continue;
                }

                std::string clause = "(";
                for (size_t i = 0; i < tokens.size(); ++i) {
                    if (i > 0) {
                        clause += " AND ";
                    }
                    clause += tokens[i].normalized;
                }
                clause += ")";

                if (seenClauses.insert(clause).second) {
                    clauses.push_back(std::move(clause));
                }
            }

            if (expansionStats != nullptr) {
                expansionStats->subPhraseClauseCount = clauses.size();
            }

            if (!clauses.empty()) {
                std::string expandedQuery;
                for (size_t i = 0; i < clauses.size(); ++i) {
                    if (i > 0) {
                        expandedQuery += " OR ";
                    }
                    expandedQuery += clauses[i];
                }

                auto expandedResults = metadataRepo_->search(expandedQuery, limit, 0);
                if (expandedResults) {
                    if (expansionStats != nullptr) {
                        expansionStats->subPhraseFtsHitCount =
                            expandedResults.value().results.size();
                    }
                    const float penalty = std::clamp(config.subPhraseExpansionPenalty, 0.1f, 1.0f);
                    const size_t beforeAppend = results.size();
                    appendResults(expandedResults.value(), penalty, true, &seenHashes);
                    if (expansionStats != nullptr) {
                        expansionStats->subPhraseFtsAddedCount =
                            results.size() > beforeAppend ? (results.size() - beforeAppend) : 0;
                    }
                    spdlog::debug("queryFullText sub-phrase expansion: base_hits={} "
                                  "expanded_hits={} query='{}' expanded='{}'",
                                  baseFtsHitCount, results.size(), query, expandedQuery);
                }
            }
        }

        if ((!baseFtsSucceeded || baseFtsHitCount == 0 || results.size() < 3) && metadataRepo_) {
            const size_t maxAggressiveClauses =
                (!baseFtsSucceeded || baseFtsHitCount == 0) ? 10 : 6;
            auto aggressiveClauses = generateAggressiveFtsFallbackClauses(
                query, maxAggressiveClauses, lookupQueryTermIdf(query));
            if (expansionStats != nullptr) {
                expansionStats->aggressiveClauseCount = aggressiveClauses.size();
            }

            size_t totalAggressiveHits = 0;
            const size_t beforeAggressive = results.size();
            for (const auto& clause : aggressiveClauses) {
                auto clauseResults = metadataRepo_->search(clause.query, limit, 0);
                if (!clauseResults) {
                    continue;
                }
                totalAggressiveHits += clauseResults.value().results.size();
                appendResults(clauseResults.value(), clause.penalty, true, &seenHashes);
            }

            if (expansionStats != nullptr) {
                expansionStats->aggressiveFtsHitCount = totalAggressiveHits;
                expansionStats->aggressiveFtsAddedCount =
                    results.size() > beforeAggressive ? (results.size() - beforeAggressive) : 0;
            }

            if (!aggressiveClauses.empty()) {
                std::vector<std::string> debugClauses;
                debugClauses.reserve(aggressiveClauses.size());
                for (const auto& clause : aggressiveClauses) {
                    debugClauses.push_back(clause.query);
                }
                spdlog::debug("queryFullText aggressive fallback: base_succeeded={} base_hits={} "
                              "clauses={} added_hits={} query='{}' clauses='{}'",
                              baseFtsSucceeded ? 1 : 0, baseFtsHitCount, aggressiveClauses.size(),
                              results.size(), query, fmt::join(debugClauses, " || "));
            }
        }

        if (config.enableGraphQueryExpansion && baseFtsHitCount < config.graphExpansionMinHits) {
            std::vector<GraphExpansionTerm> graphTerms;
            if (graphExpansionTerms != nullptr) {
                graphTerms = *graphExpansionTerms;
            } else {
                graphTerms = generateGraphExpansionTerms(
                    kgStore_, query, {},
                    GraphExpansionConfig{.maxTerms = config.graphExpansionMaxTerms,
                                         .maxSeeds = config.graphExpansionMaxSeeds,
                                         .maxNeighbors = config.graphMaxNeighbors});
            }
            if (expansionStats != nullptr) {
                expansionStats->graphExpansionTermCount = graphTerms.size();
            }

            size_t totalGraphHits = 0;
            const size_t beforeGraph = results.size();
            for (const auto& term : graphTerms) {
                auto graphResults = metadataRepo_->search(term.text, limit, 0);
                if (!graphResults) {
                    continue;
                }
                totalGraphHits += graphResults.value().results.size();
                const float penalty =
                    std::clamp(config.graphExpansionFtsPenalty * std::clamp(term.score, 0.2f, 1.0f),
                               0.1f, 1.0f);
                appendResults(graphResults.value(), penalty, true, &seenHashes,
                              ComponentResult::Source::GraphText);
            }

            if (expansionStats != nullptr) {
                expansionStats->graphExpansionFtsHitCount = totalGraphHits;
                expansionStats->graphExpansionFtsAddedCount =
                    results.size() > beforeGraph ? (results.size() - beforeGraph) : 0;
            }

            if (!graphTerms.empty()) {
                std::vector<std::string> debugTerms;
                debugTerms.reserve(graphTerms.size());
                for (const auto& term : graphTerms) {
                    debugTerms.push_back(term.text);
                }
                spdlog::debug("queryFullText graph expansion: base_hits={} graph_terms={} "
                              "graph_hits={} final_hits={} query='{}' terms='{}'",
                              baseFtsHitCount, graphTerms.size(), totalGraphHits, results.size(),
                              query, fmt::join(debugTerms, " || "));
            }
        }

        spdlog::debug("queryFullText: {} results for query '{}' (limit={})", results.size(),
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
            if (!aliasResults || aliasResults.value().empty()) {
                auto labelMatches = kgStore_->searchNodesByLabel(tok, aliasesPerToken, 0);
                if (labelMatches && !labelMatches.value().empty()) {
                    for (const auto& node : labelMatches.value()) {
                        const float score = tok.find(' ') != std::string::npos ? 0.90f : 0.75f;
                        auto it = nodeIdToScore.find(node.id);
                        if (it == nodeIdToScore.end()) {
                            metadata::AliasResolution alias;
                            alias.nodeId = node.id;
                            alias.score = score;
                            nodeIdToScore[node.id] = score;
                            aliases.push_back(std::move(alias));
                        } else {
                            it->second = std::min(1.0f, it->second + score * 0.5f);
                        }
                    }
                    continue;
                }

                auto fuzzyResults = kgStore_->resolveAliasFuzzy(tok, aliasesPerToken);
                if (fuzzyResults && !fuzzyResults.value().empty()) {
                    for (const auto& alias : fuzzyResults.value()) {
                        const float score = alias.score * 0.8f;
                        auto it = nodeIdToScore.find(alias.nodeId);
                        if (it == nodeIdToScore.end()) {
                            nodeIdToScore[alias.nodeId] = score;
                            aliases.push_back(alias);
                        } else {
                            it->second = std::min(1.0f, it->second + score * 0.5f);
                        }
                    }
                }
                continue;
            }

            for (const auto& alias : aliasResults.value()) {
                const float score = alias.score;
                auto it = nodeIdToScore.find(alias.nodeId);
                if (it == nodeIdToScore.end()) {
                    nodeIdToScore[alias.nodeId] = score;
                    aliases.push_back(alias);
                } else {
                    it->second = std::min(1.0f, it->second + score * 0.5f);
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
                    ci_find(searchResult.snippet, term) != std::string::npos) {
                    likelyMatch = true;
                } else if (ci_find(searchResult.document.filePath, term) != std::string::npos) {
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

        spdlog::debug("queryKnowledgeGraph: {} results for query '{}' (batch: {} terms)",
                      results.size(), query.substr(0, 50), searchTerms.size());

    } catch (const std::exception& e) {
        spdlog::warn("KG query exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding,
                                     const SearchEngineConfig& config, size_t limit) {
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
        params.similarity_threshold = config.similarityThreshold;

        auto vectorRecords = vectorDb_->search(embedding, params);

        // Keep only the best semantic hit per document to avoid dense/chunked documents
        // accumulating disproportionate score during fusion.
        if (!vectorRecords.empty()) {
            std::unordered_map<std::string, size_t> bestByHash;
            bestByHash.reserve(vectorRecords.size());

            std::vector<vector::VectorRecord> deduped;
            deduped.reserve(vectorRecords.size());

            for (const auto& vr : vectorRecords) {
                if (vr.document_hash.empty()) {
                    continue;
                }
                auto it = bestByHash.find(vr.document_hash);
                if (it == bestByHash.end()) {
                    bestByHash[vr.document_hash] = deduped.size();
                    deduped.push_back(vr);
                } else if (vr.relevance_score > deduped[it->second].relevance_score) {
                    deduped[it->second] = vr;
                }
            }

            std::sort(deduped.begin(), deduped.end(), [](const auto& a, const auto& b) {
                return a.relevance_score > b.relevance_score;
            });
            if (deduped.size() > limit) {
                deduped.resize(limit);
            }
            vectorRecords = std::move(deduped);
        }

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

        spdlog::debug("queryVectorIndex: {} results (limit={}, threshold={})", results.size(),
                      limit, config.similarityThreshold);

    } catch (const std::exception& e) {
        spdlog::warn("Vector search exception: {}", e.what());
        return results;
    }

    return results;
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding,
                                     const SearchEngineConfig& config, size_t limit,
                                     const std::unordered_set<std::string>& candidates) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!vectorDb_) {
        return results;
    }

    try {
        vector::VectorSearchParams params;
        params.k = limit;
        params.similarity_threshold = config.similarityThreshold;
        params.candidate_hashes = candidates; // Narrow to Tier 1 candidates

        auto vectorRecords = vectorDb_->search(embedding, params);

        // Keep only the best semantic hit per document to avoid dense/chunked documents
        // accumulating disproportionate score during fusion.
        if (!vectorRecords.empty()) {
            std::unordered_map<std::string, size_t> bestByHash;
            bestByHash.reserve(vectorRecords.size());

            std::vector<vector::VectorRecord> deduped;
            deduped.reserve(vectorRecords.size());

            for (const auto& vr : vectorRecords) {
                if (vr.document_hash.empty()) {
                    continue;
                }
                auto it = bestByHash.find(vr.document_hash);
                if (it == bestByHash.end()) {
                    bestByHash[vr.document_hash] = deduped.size();
                    deduped.push_back(vr);
                } else if (vr.relevance_score > deduped[it->second].relevance_score) {
                    deduped[it->second] = vr;
                }
            }

            std::sort(deduped.begin(), deduped.end(), [](const auto& a, const auto& b) {
                return a.relevance_score > b.relevance_score;
            });
            if (deduped.size() > limit) {
                deduped.resize(limit);
            }
            vectorRecords = std::move(deduped);
        }

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
SearchEngine::Impl::queryEntityVectors(const std::vector<float>& embedding,
                                       const SearchEngineConfig& config, size_t limit) {
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
        params.similarity_threshold = config.similarityThreshold;
        params.include_embeddings = false; // Don't need embeddings in results

        auto entityRecords = vectorDb_->searchEntities(embedding, params);

        // Keep only the best entity hit per source document to reduce ranking noise from
        // documents with many similar symbols.
        if (!entityRecords.empty()) {
            std::unordered_map<std::string, size_t> bestByHash;
            bestByHash.reserve(entityRecords.size());

            std::vector<vector::EntityVectorRecord> deduped;
            deduped.reserve(entityRecords.size());

            for (const auto& er : entityRecords) {
                if (er.document_hash.empty()) {
                    continue;
                }
                auto it = bestByHash.find(er.document_hash);
                if (it == bestByHash.end()) {
                    bestByHash[er.document_hash] = deduped.size();
                    deduped.push_back(er);
                } else if (er.relevance_score > deduped[it->second].relevance_score) {
                    deduped[it->second] = er;
                }
            }

            std::sort(deduped.begin(), deduped.end(), [](const auto& a, const auto& b) {
                return a.relevance_score > b.relevance_score;
            });
            if (deduped.size() > limit) {
                deduped.resize(limit);
            }
            entityRecords = std::move(deduped);
        }

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
SearchEngine::Impl::queryCompressedANN(const std::vector<float>& embedding,
                                       const SearchEngineConfig& config, size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    ++stats_.compressedAnnQueries;

    if (!vectorDb_) {
        ++stats_.compressedAnnFallback;
        return results;
    }

    try {
        // Lazy-build or rebuild the compressed ANN index if needed
        if (!compressedAnnIndex_ || !compressedAnnIndexReady_) {
            // Build compressed ANN index from all packed vectors in the database
            yams::vector::CompressedANNIndex::Config ann_cfg;
            ann_cfg.dimension = config.compressedAnnDim;
            ann_cfg.bits_per_channel = config.compressedAnnBits;
            ann_cfg.seed = 42;
            ann_cfg.m = 16;
            ann_cfg.ef_search = config.compressedAnnEfSearch;
            ann_cfg.max_elements = vectorDb_->getVectorCount();

            compressedAnnIndex_ = std::make_unique<yams::vector::CompressedANNIndex>(ann_cfg);

            // Collect all vectors using entity search with large k
            size_t total = vectorDb_->getVectorCount();
            if (total > 0) {
                vector::EntitySearchParams params;
                params.k = std::min(total, size_t(10000));
                params.similarity_threshold = -1.0f; // Accept all
                params.include_embeddings = true;

                auto entity_records = vectorDb_->searchEntities(embedding, params);
                if (!entity_records.empty()) {
                    size_t idx = 0;
                    for (const auto& rec : entity_records) {
                        if (rec.embedding.empty())
                            continue;
                        yams::vector::TurboQuantConfig tq_cfg;
                        tq_cfg.dimension = config.compressedAnnDim;
                        tq_cfg.bits_per_channel = config.compressedAnnBits;
                        tq_cfg.seed = 42;
                        yams::vector::TurboQuantMSE tq(tq_cfg);
                        auto packed = tq.packedEncode(rec.embedding);
                        compressedAnnIndex_->add(idx++, packed);
                    }
                }
            }

            auto build_result = compressedAnnIndex_->build();
            if (!build_result) {
                spdlog::warn("[CompressedANN] Build failed: {}", build_result.error().message);
                ++stats_.compressedAnnBuildErrors;
                ++stats_.compressedAnnFallback;
                return results;
            }

            compressedAnnIndexReady_ = true;
            spdlog::info("[CompressedANN] Built index: {} vectors, {} bytes",
                         compressedAnnIndex_->size(), compressedAnnIndex_->memoryBytes());
        }

        if (!compressedAnnIndexReady_ || compressedAnnIndex_->size() == 0) {
            ++stats_.compressedAnnFallback;
            return results;
        }

        // Search using compressed ANN with telemetry
        size_t candidate_count = 0;
        float decode_escapes = 0.0f;
        auto ann_search_results = compressedAnnIndex_->searchWithStats(
            embedding, limit, &candidate_count, &decode_escapes);

        stats_.compressedAnnCandidateCount += candidate_count;
        stats_.compressedAnnDecodeEscapes += static_cast<uint64_t>(decode_escapes);

        if (ann_search_results.results.empty()) {
            ++stats_.compressedAnnFallback;
            return results;
        }

        ++stats_.compressedAnnSucceeded;

        // Batch-fetch document metadata
        compat::flat_map<std::string, std::string> hashToPath;
        if (metadataRepo_) {
            std::vector<std::string> hashes;
            hashes.reserve(ann_search_results.results.size());
            for (const auto& r : ann_search_results.results) {
                hashes.push_back(std::to_string(r.id));
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

        for (size_t rank = 0; rank < ann_search_results.results.size(); ++rank) {
            const auto& r = ann_search_results.results[rank];
            ComponentResult cr;
            cr.source = ComponentResult::Source::Vector;
            cr.documentHash = std::to_string(r.id);
            auto it = hashToPath.find(cr.documentHash);
            cr.filePath = (it != hashToPath.end()) ? it->second : cr.documentHash;
            cr.score = r.score;
            cr.rank = rank;
            cr.debugInfo["compressed_ann"] = "true";
            cr.debugInfo["candidate_count"] = std::to_string(candidate_count);
            results.push_back(cr);
        }
    } catch (const std::exception& e) {
        spdlog::warn("Compressed ANN search exception: {}", e.what());
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

void SearchEngine::setSearchTuner(std::shared_ptr<SearchTuner> tuner) {
    pImpl_->setSearchTuner(std::move(tuner));
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
