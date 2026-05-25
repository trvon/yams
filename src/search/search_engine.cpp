#include <yams/search/search_engine.h>

#include <spdlog/spdlog.h>
#include "search_lexical_pipeline_internal.h"
#include "search_vector_pipeline_internal.h"
#include <yams/core/cpp23_features.hpp>
#include <yams/core/magic_numbers.hpp>
#include <yams/crypto/hasher.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/concept_resolver.h>
#include <yams/search/graph_expansion.h>
#include <yams/search/kg_scorer.h>
#include <yams/search/kg_scorer_simple.h>
#include <yams/search/lexical_scoring.h>
#include <yams/search/meta_path_router.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_router.h>
#include <yams/search/query_text_utils.h>
#include <yams/search/search_execution_context.h>
#include <yams/search/search_tracing.h>
#include <yams/search/search_tuner.h>
#include <yams/search/simeon_lexical_backend.h>
#include <yams/search/tuner_mab.h>
#include <yams/search/tuning_features.h>
#include <yams/search/tuning_pipeline.h>
#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_metadata_store.h>
#include <yams/vector/simeon_embedding_backend.h>
#include <yams/vector/vector_database.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

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

std::optional<std::int64_t>
resolveGraphCandidateDocumentId(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                                std::string_view candidateId) {
    if (!kgStore || candidateId.empty()) {
        return std::nullopt;
    }

    auto byHash = kgStore->getDocumentIdByHash(candidateId);
    if (byHash && byHash.value().has_value()) {
        return byHash.value();
    }

    auto byPath = kgStore->getDocumentIdByPath(candidateId);
    if (byPath && byPath.value().has_value()) {
        return byPath.value();
    }

    return std::nullopt;
}

std::optional<std::int64_t>
resolveGraphCandidateNodeId(const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                            std::string_view candidateId) {
    if (!kgStore) {
        return std::nullopt;
    }

    auto directNode = kgStore->getNodeByKey(std::string(candidateId));
    if (directNode && directNode.value().has_value()) {
        return directNode.value()->id; // NOLINT(bugprone-unchecked-optional-access) — guarded above
    }

    auto docId = resolveGraphCandidateDocumentId(kgStore, candidateId);
    if (!docId.has_value()) {
        return std::nullopt;
    }

    auto docHash = kgStore->getDocumentHashById(*docId);
    if (!docHash || !docHash.value().has_value() ||
        docHash.value()->empty()) { // NOLINT(bugprone-unchecked-optional-access) — short-circuit:
                                    // .value() only called when docHash is non-empty
        return std::nullopt;
    }

    auto node = kgStore->getNodeByKey(
        "doc:" + *docHash.value()); // NOLINT(bugprone-unchecked-optional-access) — guarded above
    if (!node || !node.value().has_value()) {
        auto fallbackNode = kgStore->getNodeByKey("doc:" + std::string(candidateId));
        if (fallbackNode && fallbackNode.value().has_value()) {
            return fallbackNode.value()
                ->id; // NOLINT(bugprone-unchecked-optional-access) — guarded above
        }
        return std::nullopt;
    }
    return node.value()->id; // NOLINT(bugprone-unchecked-optional-access) — guarded above
}

std::vector<float> computeReciprocalCommunitySupport(
    const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
    const std::vector<std::string>& candidateIds, std::size_t maxNeighbors,
    float referenceCommunitySize = 0.0f, std::size_t* supportedDocCountOut = nullptr,
    std::size_t* edgeCountOut = nullptr, std::size_t* largestCommunityOut = nullptr,
    float decayHalfLifeDays = 0.0f, float minEdgeWeight = 0.0f) {
    std::vector<float> support(candidateIds.size(), 0.0f);
    if (!kgStore || candidateIds.size() < 2) {
        return support;
    }

    std::unordered_map<std::int64_t, std::size_t> indexByNodeId;
    indexByNodeId.reserve(candidateIds.size());
    for (std::size_t i = 0; i < candidateIds.size(); ++i) {
        auto nodeId = resolveGraphCandidateNodeId(kgStore, candidateIds[i]);
        if (nodeId.has_value()) {
            indexByNodeId.emplace(*nodeId, i);
        }
    }
    if (indexByNodeId.size() < 2) {
        return support;
    }

    // P8: compute decayed weight from (edge.weight, edge.createdTime).
    // halfLife==0 disables decay so the effective weight is just edge.weight,
    // which preserves pre-P8 binary behavior when minEdgeWeight==0 (any
    // reciprocal link counts) and every incoming edge has weight>0.
    const std::int64_t nowSeconds = std::chrono::duration_cast<std::chrono::seconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
    const double kLn2 = 0.6931471805599453;
    auto decayedWeight = [&](const yams::metadata::KGEdge& edge) -> float {
        float w = edge.weight > 0.0f ? edge.weight : 0.0f;
        if (decayHalfLifeDays > 0.0f && edge.createdTime.has_value()) {
            const double ageSeconds = static_cast<double>(nowSeconds - *edge.createdTime);
            if (ageSeconds > 0.0) {
                const double ageDays = ageSeconds / 86400.0;
                const double factor =
                    std::exp(-kLn2 * ageDays / static_cast<double>(decayHalfLifeDays));
                w = static_cast<float>(static_cast<double>(w) * factor);
            }
        }
        return w;
    };

    // outgoingWeight[i][j] is the decayed weight of the edge from i->j, if any.
    std::vector<std::unordered_map<std::size_t, float>> outgoingWeight(candidateIds.size());
    std::vector<std::vector<std::size_t>> reciprocalAdj(candidateIds.size());

    for (const auto& [nodeId, idx] : indexByNodeId) {
        auto edgesResult =
            kgStore->getEdgesFrom(nodeId, std::string_view("semantic_neighbor"), maxNeighbors, 0);
        if (!edgesResult) {
            continue;
        }
        for (const auto& edge : edgesResult.value()) {
            auto it = indexByNodeId.find(edge.dstNodeId);
            if (it == indexByNodeId.end() || it->second == idx) {
                continue;
            }
            const float w = decayedWeight(edge);
            if (w < minEdgeWeight) {
                continue;
            }
            auto& slot = outgoingWeight[idx][it->second];
            slot = std::max(slot, w);
        }
    }

    std::size_t reciprocalEdgeCount = 0;
    for (std::size_t i = 0; i < outgoingWeight.size(); ++i) {
        for (const auto& [neighbor, w] : outgoingWeight[i]) {
            if (neighbor <= i) {
                continue;
            }
            auto reverseIt = outgoingWeight[neighbor].find(i);
            if (reverseIt != outgoingWeight[neighbor].end()) {
                reciprocalAdj[i].push_back(neighbor);
                reciprocalAdj[neighbor].push_back(i);
                ++reciprocalEdgeCount;
            }
        }
    }

    std::vector<bool> visited(candidateIds.size(), false);
    std::size_t supportedDocCount = 0;
    std::size_t largestCommunity = 0;
    for (std::size_t i = 0; i < reciprocalAdj.size(); ++i) {
        if (visited[i] || reciprocalAdj[i].empty()) {
            continue;
        }
        std::vector<std::size_t> component;
        std::vector<std::size_t> stack = {i};
        visited[i] = true;
        while (!stack.empty()) {
            const auto cur = stack.back();
            stack.pop_back();
            component.push_back(cur);
            for (const auto neighbor : reciprocalAdj[cur]) {
                if (visited[neighbor]) {
                    continue;
                }
                visited[neighbor] = true;
                stack.push_back(neighbor);
            }
        }

        if (component.size() < 2) {
            continue;
        }

        supportedDocCount += component.size();
        largestCommunity = std::max(largestCommunity, component.size());
        // Corpus-adaptive normalization: when a reference community size is supplied, normalize
        // against it so the absolute signal magnitude is stable across candidate-set widths.
        // Fall back to the legacy candidate-set-adaptive denominator when no reference is given.
        const float denom = referenceCommunitySize > 1.0f
                                ? referenceCommunitySize - 1.0f
                                : static_cast<float>(candidateIds.size() - 1);
        const float normalized =
            std::clamp(static_cast<float>(component.size() - 1) / denom, 0.0f, 1.0f);
        for (const auto member : component) {
            support[member] = std::max(support[member], normalized);
        }
    }

    if (supportedDocCountOut) {
        *supportedDocCountOut = supportedDocCount;
    }
    if (edgeCountOut) {
        *edgeCountOut = reciprocalEdgeCount;
    }
    if (largestCommunityOut) {
        *largestCommunityOut = largestCommunity;
    }
    return support;
}

bool envFlagEnabled(const char* name) {
    if (const char* env = std::getenv(name)) { // NOLINT(concurrency-mt-unsafe) — read-only; env
                                               // vars not modified concurrently
        std::string value(env);
        std::transform(value.begin(), value.end(), value.begin(), [](char c) {
            return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        });
        return value == "1" || value == "true" || value == "yes" || value == "on";
    }
    return false;
}

size_t envSizeTOrDefault(const char* name, size_t defaultValue, size_t minValue, size_t maxValue) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe) — read-only; env vars not modified concurrently
    if (const char* env = std::getenv(name); env && *env) {
        size_t parsed{};
        const auto* end = env + std::strlen(env);
        auto [ptr, ec] = std::from_chars(env, end, parsed);
        if (ec == std::errc{} && ptr == end) {
            return std::clamp(parsed, minValue, maxValue);
        }
    }
    return std::clamp(defaultValue, minValue, maxValue);
}

struct PreFusionDocSignal {
    bool hasAnchoring = false;
    bool hasVector = false;
    double maxVectorRaw = 0.0;
    size_t bestVectorRank = std::numeric_limits<size_t>::max();
    size_t bestTextRank = std::numeric_limits<size_t>::max();
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
            case ComponentResult::Source::SimeonText:
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
            case ComponentResult::Source::Anchor:
                sourceBoost = 0.70f;
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
        if (comp.source == ComponentResult::Source::Text ||
            comp.source == ComponentResult::Source::SimeonText) {
            signal.bestTextRank = std::min(signal.bestTextRank, comp.rank);
        }
    }

    return signals;
}

// ── Path-seed helpers (merged from CorpusAdapter) ────────────────────────

std::string lowerCopy(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    for (unsigned char ch : in) {
        out.push_back(static_cast<char>(std::tolower(ch)));
    }
    return out;
}

std::string trimCopy(std::string_view in) {
    while (!in.empty() && std::isspace(static_cast<unsigned char>(in.front()))) {
        in.remove_prefix(1);
    }
    while (!in.empty() && std::isspace(static_cast<unsigned char>(in.back()))) {
        in.remove_suffix(1);
    }
    return std::string(in);
}

bool looksStructured(std::string_view query) {
    return query.find('=') != std::string_view::npos || query.find('/') != std::string_view::npos ||
           query.find('\\') != std::string_view::npos ||
           query.find('.') != std::string_view::npos || query.find('#') != std::string_view::npos;
}

bool isWordChar(unsigned char ch) noexcept {
    return std::isalnum(ch) || ch == '_' || ch == '-' || ch == '.' || ch == '/' || ch == '\\' ||
           ch == '#';
}

const std::unordered_set<std::string_view>& englishStopwords() {
    static const std::unordered_set<std::string_view> kStopwords = {
        "a",    "an",   "and", "are",  "as",    "at",    "be",     "by",    "can", "could", "do",
        "does", "did",  "for", "from", "has",   "have",  "how",    "i",     "in",  "into",  "is",
        "it",   "lets", "of",  "on",   "or",    "our",   "should", "that",  "the", "their", "this",
        "to",   "us",   "we",  "what", "where", "which", "with",   "would", "you", "your"};
    return kStopwords;
}

bool isStopword(std::string_view token) {
    return englishStopwords().contains(token);
}

std::vector<std::string> tokenizeQueryTerms(std::string_view query) {
    std::vector<std::string> terms;
    std::size_t pos = 0;
    while (pos < query.size()) {
        while (pos < query.size() && !isWordChar(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        const std::size_t start = pos;
        while (pos < query.size() && isWordChar(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        if (pos > start) {
            std::string term = lowerCopy(query.substr(start, pos - start));
            while (!term.empty() &&
                   (term.front() == '.' || term.front() == '-' || term.front() == '#')) {
                term.erase(term.begin());
            }
            while (!term.empty() &&
                   (term.back() == '.' || term.back() == '-' || term.back() == '#')) {
                term.pop_back();
            }
            if (term.size() > 1)
                terms.push_back(std::move(term));
        }
    }
    return terms;
}

std::vector<std::pair<std::string, std::string>>
parseMetadataFiltersFromQuery(std::string_view query) {
    std::vector<std::pair<std::string, std::string>> filters;
    std::size_t pos = 0;
    while (pos < query.size()) {
        while (pos < query.size() && std::isspace(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        const std::size_t keyStart = pos;
        while (pos < query.size() && query[pos] != '=' &&
               !std::isspace(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        if (pos >= query.size() || query[pos] != '=' || pos == keyStart) {
            while (pos < query.size() && !std::isspace(static_cast<unsigned char>(query[pos]))) {
                ++pos;
            }
            continue;
        }
        std::string key(query.substr(keyStart, pos - keyStart));
        ++pos; // '='
        const std::size_t valueStart = pos;
        while (pos < query.size() && !std::isspace(static_cast<unsigned char>(query[pos]))) {
            ++pos;
        }
        if (pos > valueStart) {
            filters.emplace_back(std::move(key),
                                 std::string(query.substr(valueStart, pos - valueStart)));
        }
    }
    return filters;
}

struct PathQuerySeed {
    std::string text;
    std::string kind;
    float weight{1.0f};
};

void addUniquePathSeed(std::vector<PathQuerySeed>& seeds, std::unordered_set<std::string>& seen,
                       std::string text, std::string kind, float weight) {
    text = trimCopy(text);
    if (text.size() < 2)
        return;
    std::string key = lowerCopy(text);
    if (!seen.insert(key).second)
        return;
    seeds.push_back(PathQuerySeed{std::move(text), std::move(kind), weight});
}

std::vector<PathQuerySeed> buildPathSeedsFromQuery(std::string_view query,
                                                   std::size_t maxSeeds = 8) {
    std::vector<PathQuerySeed> seeds;
    std::unordered_set<std::string> seen;
    seeds.reserve(maxSeeds);

    const std::string whole = trimCopy(query);
    if ((looksStructured(query) || query.size() <= 64) && !whole.empty()) {
        addUniquePathSeed(seeds, seen, whole, "whole_query", looksStructured(query) ? 1.0f : 0.82f);
    }

    for (const auto& term : tokenizeQueryTerms(query)) {
        if (seeds.size() >= maxSeeds)
            break;
        if (term.find('=') != std::string::npos)
            continue;
        const bool pathish =
            term.find('/') != std::string::npos || term.find('\\') != std::string::npos ||
            term.find('.') != std::string::npos || term.find('_') != std::string::npos ||
            term.find('-') != std::string::npos || term.find('#') != std::string::npos;
        if (pathish && term.size() >= 3) {
            addUniquePathSeed(seeds, seen, term, "structured_token", 0.95f);
        }
    }

    std::vector<std::string> contentTerms;
    for (auto& term : tokenizeQueryTerms(query)) {
        if (term.find('=') != std::string::npos || term.size() < 3 || isStopword(term))
            continue;
        std::size_t start = 0;
        for (std::size_t i = 0; i <= term.size(); ++i) {
            if (i == term.size() || term[i] == '/' || term[i] == '\\' || term[i] == '.' ||
                term[i] == '_' || term[i] == '-') {
                if (i > start + 2)
                    contentTerms.push_back(term.substr(start, i - start));
                start = i + 1;
            }
        }
        contentTerms.push_back(std::move(term));
    }

    for (const auto& term : contentTerms) {
        if (seeds.size() >= maxSeeds)
            break;
        addUniquePathSeed(seeds, seen, term, "content_term", 0.70f);
    }

    for (std::size_t i = 0; i < contentTerms.size() && seeds.size() < maxSeeds; ++i) {
        std::string phrase;
        for (std::size_t j = i; j < std::min(contentTerms.size(), i + 3) && seeds.size() < maxSeeds;
             ++j) {
            if (!phrase.empty())
                phrase.push_back(' ');
            phrase += contentTerms[j];
            if (j > i && phrase.size() <= 48) {
                addUniquePathSeed(seeds, seen, phrase, "content_phrase", 0.62f);
            }
        }
    }

    return seeds;
}

// ── End path-seed helpers ────────────────────────────────────────────────

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

bool containsFast(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return true;
    }
    if (needle.size() > haystack.size()) {
        return false;
    }
    return yams::app::services::simdMemmem(haystack, needle) != yams::app::services::kMemmemNpos;
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

    void setSearchTuner(std::shared_ptr<SearchTuner> tuner) { tuner_ = std::move(tuner); }

    void setCrossReranker(SearchEngine::CrossRerankScorer scorer) {
        crossReranker_ = std::move(scorer);
    }

    void setSimeonLexicalBackend(std::unique_ptr<SimeonLexicalBackend> backend) {
        simeonLexical_ = std::move(backend);
        if (simeonLexical_ && metadataRepo_) {
            auto build = simeonLexical_->buildAsync(metadataRepo_);
            if (!build) {
                spdlog::warn("[simeon-lexical] buildAsync failed: {}", build.error().message);
            }
        }
    }
    std::shared_ptr<SearchTuner> getSearchTuner() const { return tuner_; }

    SearchEngine::SimeonLexicalStatus getSimeonLexicalStatus() const {
        SearchEngine::SimeonLexicalStatus status;
        if (!simeonLexical_) {
            return status;
        }
        status.configured = true;
        status.ready = simeonLexical_->ready();
        status.building = simeonLexical_->building();
        status.fragmentGeometryReady = simeonLexical_->fragmentGeometryReady();
        status.conceptMiningEnabled = simeonLexical_->concept_mining_enabled();
        status.docCount = simeonLexical_->doc_count();
        status.conceptCount = simeonLexical_->concept_count();
        return status;
    }

private:
    Result<SearchResponse> searchInternal(const std::string& query, const SearchParams& params);

    Result<std::vector<ComponentResult>>
    queryFullText(const std::string& query, QueryIntent queryIntent,
                  const SearchEngineConfig& config, size_t limit,
                  QueryExpansionStats* expansionStats = nullptr,
                  const std::vector<GraphExpansionTerm>* graphExpansionTerms = nullptr,
                  std::string* simeonRouteRecipe = nullptr);

    Result<std::vector<ComponentResult>> queryPathTree(const std::string& query, size_t limit);
    Result<std::vector<ComponentResult>>
    queryKnowledgeGraph(const std::string& query, size_t limit,
                        const std::vector<QueryConcept>* concepts = nullptr);
    Result<std::vector<ComponentResult>> queryVectorIndex(const std::vector<float>& embedding,
                                                          const SearchEngineConfig& config,
                                                          size_t limit);
    Result<std::vector<ComponentResult>>
    queryVectorIndex(const std::vector<float>& embedding, const SearchEngineConfig& config,
                     size_t limit, const std::unordered_set<std::string>& candidates);
    Result<std::vector<ComponentResult>> queryEntityVectors(const std::vector<float>& embedding,
                                                            const SearchEngineConfig& config,
                                                            size_t limit);

    Result<std::vector<ComponentResult>> queryTags(const std::vector<std::string>& tags,
                                                   bool matchAll, size_t limit);
    Result<std::vector<ComponentResult>> queryMetadata(const std::string& query,
                                                       const SearchParams& params, size_t limit);

    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<vector::VectorDatabase> vectorDb_;
    std::shared_ptr<vector::EmbeddingGenerator> embeddingGen_;
    std::shared_ptr<yams::metadata::KnowledgeGraphStore> kgStore_;
    std::shared_ptr<KGScorer> kgScorer_;
    std::optional<boost::asio::any_io_executor> executor_;
    SearchEngineConfig config_;
    mutable SearchEngine::Statistics stats_;
    EntityExtractionFunc conceptExtractor_; // GLiNER concept extractor (optional)
    std::shared_ptr<SearchTuner> tuner_;    // Adaptive runtime tuner (optional)
    SearchEngine::CrossRerankScorer crossReranker_;
    std::unique_ptr<SimeonLexicalBackend> simeonLexical_;
    // Per-profile simeon bandit arms. Each corpus profile learns independently
    // which simeon recipe works best via UCB1 from proxy rewards. Training-free.
    using ProfileKey = std::string;
    std::mutex simeonBanditsMutex_;
    std::unordered_map<ProfileKey, TunerMAB> simeonBandits_;
    bool simeonBanditsInitialized_ = false;
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
    std::string simeonRouteRecipe;
    std::string selectedSimeonBanditArm;
    ProfileKey selectedSimeonBanditProfile;

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
    size_t fusionDroppedDocCount = 0;
    size_t anchoredPreFusionDocCount = 0;
    size_t anchoredFusionDroppedDocCount = 0;
    size_t topTextPreFusionDocCount = 0;
    size_t topTextFusionDroppedDocCount = 0;
    size_t vectorOnlyDocs = 0;
    size_t vectorOnlyBelowThreshold = 0;
    size_t vectorOnlyAboveThreshold = 0;
    size_t vectorOnlyNearMissEligible = 0;
    bool graphRerankApplied = false;
    bool crossRerankApplied = false;
    double rerankGuardScoreGap = 0.0;
    bool rerankGuardCompetitiveAnchoredEvidence = false;
    std::vector<std::string> rerankGuardAnchoredDocIds;
    size_t graphWindowGuardReplacementCount = 0;
    size_t graphWindowCapReplacementCount = 0;
    size_t graphRerankGuardReplacementCount = 0;
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
    bool relaxedVectorRetryEnabled = false;
    std::atomic<bool> relaxedVectorRetryApplied{false};
    std::atomic<bool> relaxedVectorRetryAttempted{false};
    std::atomic<int> relaxedVectorPrimaryHitCount{0};
    std::atomic<int> relaxedVectorRetryThresholdMilli{0};
    bool weakQueryFanoutBoostApplied = false;
    size_t effectiveVectorMaxResults = 0;
    size_t effectiveEntityVectorMaxResults = 0;
    SearchEngineConfig workingConfig = config_;

    if (tuner_) {
        workingConfig = tuner_->getConfig();
    }

    // Snapshot the per-query search execution context once so the tuner
    // and the downstream budget / freshness reads see the same
    // topologyEpoch (previous version loaded twice and could observe a
    // torn read under concurrent topology rebuilds).
    const SearchExecutionContext searchExecutionContext = currentSearchExecutionContext();

    // R2: construct a TuningContext populated with corpus-slow features,
    // query token stats, and the topology epoch fingerprint. The context is
    // passed to both getParams() and observe() so the contextual policy
    // (R4+) can condition on it. SearchTuner (rules) ignores the context.
    TuningContext tuningCtx;
    if (tuner_) {
        fillCorpusFeatures(tuningCtx, tuner_->corpusStats());
    }
    fillQueryTokenFeature(tuningCtx, query);
    tuningCtx.topologyEpoch = searchExecutionContext.freshness.topologyEpoch;

    std::optional<TuningState> baselineState;
    TunedParams baseParams;
    if (tuner_) {
        baselineState = tuner_->currentState();
        baseParams = tuner_->getParams(tuningCtx);
    } else {
        baseParams = seedTunedParamsFromConfig(workingConfig);
    }

    const auto routingStart = std::chrono::steady_clock::now();
    QueryPolicyResolution policy =
        resolveQueryPolicy(query, workingConfig, baseParams, baselineState, params.semanticOnly);
    response.componentTimingMicros["query_routing"] =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                              routingStart)
            .count();
    if (tuner_) {
        response.componentTimingMicros["community_detection"] =
            response.componentTimingMicros.at("query_routing");
    }

    const QueryRouteDecision routeDecision = std::move(policy.routeDecision);
    const QueryIntent intent = routeDecision.intent.label;
    const QueryRetrievalMode retrievalMode = routeDecision.retrievalMode.label;
    const auto effectiveZoomLevel = policy.effectiveZoomLevel;
    const bool zoomLevelInferredFromIntent = policy.zoomLevelInferredFromIntent;
    workingConfig = std::move(policy.config);

    // Select a Simeon bandit arm after tuner/policy resolution so the arm is
    // request-local and cannot leak through shared config_ state.
    if (simeonLexical_ && simeonLexical_->ready()) {
        std::lock_guard<std::mutex> lock(simeonBanditsMutex_);
        if (!simeonBanditsInitialized_) {
            std::vector<TunerMAB::Arm> arms;
            arms.push_back({"sab_smooth", 0.0, {}});
            arms.push_back({"sab_smooth_rm3_adaptive", 0.0, {}});
            arms.push_back({"sab_smooth_rm3_diverse", 0.0, {}});
            if (simeonLexical_->hasStrategyRouter()) {
                arms.push_back({"keyphrase", 0.0, {}});
                arms.push_back({"lead_field", 0.0, {}});
            }
            using CP = SearchEngineConfig::CorpusProfile;
            for (auto profile : {CP::CODE, CP::PROSE, CP::DOCS, CP::MIXED, CP::CUSTOM}) {
                std::vector<TunerMAB::Arm> profileArms = arms;
                auto key = SearchEngineConfig::corpusProfileToString(profile);
                simeonBandits_[key].setArms(std::move(profileArms));
            }
            simeonBanditsInitialized_ = true;
        }

        ProfileKey profileKey =
            std::string{SearchEngineConfig::corpusProfileToString(workingConfig.corpusProfile)};
        auto it = simeonBandits_.find(profileKey);
        if (it == simeonBandits_.end()) {
            profileKey = "MIXED";
            it = simeonBandits_.find(profileKey);
        }
        if (it != simeonBandits_.end()) {
            auto armIdx = it->second.selectArm();
            if (armIdx.has_value() && *armIdx < it->second.arms().size()) {
                selectedSimeonBanditArm = it->second.arms()[*armIdx].id;
                selectedSimeonBanditProfile = std::move(profileKey);
                workingConfig.simeonBanditArm = selectedSimeonBanditArm;
            }
        }
    }

    // R2/audit: route-derived query-fast features populated after
    // classification so R4+ contextual policies see real signals instead
    // of zero-defaulted fields. Vector-path = any retrieval mode that
    // touches the semantic tier; KG anchors = a community route matched
    // or an explicit override landed.
    tuningCtx.queryHasVectorPath = (retrievalMode == QueryRetrievalMode::Semantic ||
                                    retrievalMode == QueryRetrievalMode::Hybrid)
                                       ? std::uint8_t{1}
                                       : std::uint8_t{0};
    tuningCtx.queryHasKgAnchors =
        (routeDecision.community.has_value() || policy.communityOverride.has_value())
            ? std::uint8_t{1}
            : std::uint8_t{0};

    if (policy.communityOverride.has_value() && baselineState.has_value()) {
        const std::string overrideState = tuningStateToString(*policy.communityOverride);
        response.debugStats["community_override"] = overrideState;
        spdlog::debug("[Search] community override: global={} → {} (routing={}μs, query='{}')",
                      tuningStateToString(*baselineState), overrideState,
                      response.componentTimingMicros.at("query_routing"), query);
    }

    // Long prose queries can miss the semantic tier entirely when the default threshold
    // is tuned for code/navigation queries. Retry only after a zero-hit semantic search.
    const size_t queryTokenCount = tokenizeLower(query).size();
    const bool shortQueryBudgeted = searchExecutionContext.shortQueryBudgeted;
    const bool pressureBudgeted = searchExecutionContext.pressureBudgeted;
    const auto& freshness = searchExecutionContext.freshness;
    const auto& topologyOverlayHashes = searchExecutionContext.topologyOverlayHashes;
    const bool corpusWarming = freshness.corpusWarming();
    const bool semanticBudgetActive = shortQueryBudgeted || pressureBudgeted;
    const bool delayEmbeddingUntilTier1 = semanticBudgetActive;
    response.debugStats["semantic_budget_short_query"] = shortQueryBudgeted ? "1" : "0";
    response.debugStats["semantic_budget_pressure"] = pressureBudgeted ? "1" : "0";
    response.debugStats["corpus_warming"] = corpusWarming ? "1" : "0";
    response.debugStats["ingest_queued"] = std::to_string(freshness.ingestQueued);
    response.debugStats["ingest_inflight"] = std::to_string(freshness.ingestInFlight);
    response.debugStats["post_ingest_queued"] = std::to_string(freshness.postIngestQueued);
    response.debugStats["post_ingest_inflight"] = std::to_string(freshness.postIngestInFlight);
    response.debugStats["lexical_delta_pending_docs"] =
        std::to_string(freshness.lexicalDeltaPendingDocs);
    response.debugStats["lexical_delta_queued_epoch"] =
        std::to_string(freshness.lexicalDeltaQueuedEpoch);
    response.debugStats["lexical_delta_published_epoch"] =
        std::to_string(freshness.lexicalDeltaPublishedEpoch);
    response.debugStats["lexical_delta_published_docs"] =
        std::to_string(freshness.lexicalDeltaPublishedDocs);
    response.debugStats["search_engine_ready"] = freshness.lexicalReady ? "1" : "0";
    response.debugStats["search_engine_awaiting_drain"] = freshness.awaitingDrain ? "1" : "0";
    response.debugStats["vector_ready"] = freshness.vectorReady ? "1" : "0";
    response.debugStats["kg_ready"] = freshness.kgReady ? "1" : "0";
    response.debugStats["topology_ready"] = freshness.topologyReady ? "1" : "0";
    response.debugStats["topology_epoch"] = std::to_string(freshness.topologyEpoch);
    response.debugStats["topology_overlay_hashes"] = std::to_string(topologyOverlayHashes.size());
    response.debugStats["semantic_budget_delay_embedding"] = delayEmbeddingUntilTier1 ? "1" : "0";
    if (shortQueryBudgeted) {
        workingConfig.vectorMaxResults = std::min(workingConfig.vectorMaxResults, size_t{8});
        workingConfig.entityVectorMaxResults =
            std::min(workingConfig.entityVectorMaxResults, size_t{4});
        workingConfig.kgMaxResults = 0;
        workingConfig.kgWeight = 0.0f;
        workingConfig.enableGraphRerank = false;
        workingConfig.graphRerankTopN = 0;
        workingConfig.graphScoringBudgetMs = 0;
        response.debugStats["semantic_budget_mode"] = "short_query";
    }
    if (pressureBudgeted) {
        workingConfig.vectorMaxResults = std::min(workingConfig.vectorMaxResults, size_t{16});
        workingConfig.entityVectorMaxResults =
            std::min(workingConfig.entityVectorMaxResults, size_t{8});
        workingConfig.kgMaxResults = std::min(workingConfig.kgMaxResults, size_t{24});
        workingConfig.graphRerankTopN = std::min(workingConfig.graphRerankTopN, size_t{10});
        workingConfig.graphScoringBudgetMs = std::min(workingConfig.graphScoringBudgetMs, 4);
        response.debugStats["semantic_budget_mode"] =
            shortQueryBudgeted ? "short_query+pressure" : "pressure";
    }
    if (corpusWarming && !workingConfig.bypassCorpusWarmingGate) {
        workingConfig.enableGraphRerank = false;
        workingConfig.graphRerankTopN = 0;
        workingConfig.graphScoringBudgetMs = 0;
        workingConfig.kgMaxResults = 0;
        workingConfig.kgWeight = 0.0f;
        response.debugStats["semantic_budget_mode"] =
            response.debugStats.contains("semantic_budget_mode")
                ? response.debugStats["semantic_budget_mode"] + "+warming"
                : "warming";
    }
    relaxedVectorRetryEnabled = intent == QueryIntent::Prose && queryTokenCount >= 6 &&
                                workingConfig.similarityThreshold > 0.20f;

    const auto queryVectorWithRelaxedRetry =
        [this, &workingConfig, relaxedVectorRetryEnabled, &relaxedVectorRetryApplied,
         &relaxedVectorRetryAttempted, &relaxedVectorPrimaryHitCount,
         &relaxedVectorRetryThresholdMilli](const std::vector<float>& embedding, size_t limit,
                                            const std::unordered_set<std::string>* candidates =
                                                nullptr) -> Result<std::vector<ComponentResult>> {
        const auto runVectorQuery = [&](const SearchEngineConfig& config) {
            return candidates != nullptr ? queryVectorIndex(embedding, config, limit, *candidates)
                                         : queryVectorIndex(embedding, config, limit);
        };

        auto primary = runVectorQuery(workingConfig);
        if (primary) {
            relaxedVectorPrimaryHitCount.store(static_cast<int>(primary.value().size()),
                                               std::memory_order_relaxed);
        }
        if (!relaxedVectorRetryEnabled || !primary || !primary.value().empty()) {
            return primary;
        }

        SearchEngineConfig relaxedConfig = workingConfig;
        const float relaxedThreshold = std::min(workingConfig.similarityThreshold, 0.20f);
        if (!(relaxedThreshold + 1e-6f < workingConfig.similarityThreshold)) {
            return primary;
        }

        relaxedVectorRetryAttempted.store(true, std::memory_order_relaxed);
        relaxedConfig.similarityThreshold = relaxedThreshold;
        auto retried = runVectorQuery(relaxedConfig);
        if (retried && !retried.value().empty()) {
            relaxedVectorRetryApplied.store(true, std::memory_order_relaxed);
            relaxedVectorRetryThresholdMilli.store(
                static_cast<int>(std::lround(static_cast<double>(relaxedThreshold) * 1000.0)),
                std::memory_order_relaxed);
            spdlog::debug(
                "Vector search retry: relaxed threshold {:.3f} -> {:.3f} yielded {} results",
                workingConfig.similarityThreshold, relaxedThreshold, retried.value().size());
            return retried;
        }

        return primary;
    };

    spdlog::debug("Query intent: {}, retrieval={}, zoom={} ({})", queryIntentToString(intent),
                  queryRetrievalModeToString(retrievalMode),
                  SearchEngineConfig::navigationZoomLevelToString(effectiveZoomLevel),
                  zoomLevelInferredFromIntent ? "intent_auto" : "configured");

    SearchTraceCollector traceCollector(workingConfig);

    // Embedding generation may be launched eagerly or lazily depending on tiering strategy.
    std::optional<std::vector<float>> queryEmbedding;
    const bool needsEmbedding =
        (workingConfig.vectorWeight > 0.0f || workingConfig.entityVectorWeight > 0.0f);
    std::future<std::vector<float>> embeddingFuture;
    std::chrono::steady_clock::time_point embStart;
    bool embeddingStarted = false;
    bool embeddingAwaited = false;
    bool embeddingFailed = false;
    std::string embeddingStatus = needsEmbedding ? "not_started" : "not_needed";
    const size_t vectorDbEmbeddingDim = vectorDb_ ? vectorDb_->getConfig().embedding_dim : 0;
    size_t queryEmbeddingDim = 0;
    std::string semanticTierSkipReason;
    std::string vectorTierSkipReason;
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
            embeddingStatus = "launched";
        }
    };

    // Preserve overlap with the lexical tiers unless daemon-aware budgeting asks us to
    // prove lexical strength before paying semantic costs.
    if (!delayEmbeddingUntilTier1) {
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
    if (conceptExtractor_ && !params.semanticOnly &&
        workingConfig.conceptExtractionBackend ==
            SearchEngineConfig::ConceptExtractionBackend::GlinerWithFallback) {
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
            embeddingAwaited = true;
            try {
                auto embResult = embeddingFuture.get();
                if (!embResult.empty()) {
                    queryEmbedding = std::move(embResult);
                    embeddingStatus = "ready";
                } else {
                    embeddingStatus = "empty_result";
                }
            } catch (const std::exception& e) {
                spdlog::warn("Failed to generate query embedding: {}", e.what());
                embeddingFailed = true;
                embeddingStatus = "failed";
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
    traceCollector.markStageConfigured("cross_rerank", workingConfig.enableReranking &&
                                                           workingConfig.rerankTopK > 0);

    auto hasVectorTierDimMismatch = [&]() {
        if (!queryEmbedding.has_value() || !vectorDb_) {
            return false;
        }
        queryEmbeddingDim = queryEmbedding.value().size();
        if (queryEmbeddingDim == 0 || vectorDbEmbeddingDim == 0 ||
            queryEmbeddingDim == vectorDbEmbeddingDim) {
            return false;
        }
        if (vectorTierSkipReason.empty()) {
            vectorTierSkipReason =
                "embedding_dim_mismatch(db=" + std::to_string(vectorDbEmbeddingDim) +
                ",query=" + std::to_string(queryEmbeddingDim) + ")";
        }
        return true;
    };

    auto markVectorTierDimMismatch = [&]() {
        if (vectorTierSkipReason.empty()) {
            return;
        }
        traceCollector.markStageSkipped("vector", vectorTierSkipReason);
        traceCollector.markStageSkipped("entity_vector", vectorTierSkipReason);
    };

    auto computeLexicalEvidence = [&](const std::vector<ComponentResult>& componentResults) {
        size_t lexicalHits = 0;
        float topTextScore = 0.0f;
        for (const auto& componentResult : componentResults) {
            if (componentResult.source == ComponentResult::Source::Text ||
                componentResult.source == ComponentResult::Source::SimeonText) {
                ++lexicalHits;
                topTextScore = std::max(topTextScore, componentResult.score);
            } else if (componentResult.source == ComponentResult::Source::PathTree) {
                ++lexicalHits;
            }
        }
        return std::pair<size_t, float>{lexicalHits, topTextScore};
    };

    auto recordSemanticBudgetLexicalEvidence =
        [&](const std::vector<ComponentResult>& componentResults) {
            const auto [lexicalHits, topTextScore] = computeLexicalEvidence(componentResults);
            response.debugStats["semantic_budget_lexical_hits"] = std::to_string(lexicalHits);
            response.debugStats["semantic_budget_top_text_score"] =
                fmt::format("{:.4f}", topTextScore);
            return std::pair<size_t, float>{lexicalHits, topTextScore};
        };

    auto materializeConcepts = [&](bool waitIfConfigured) {
        if (conceptsMaterialized) {
            return;
        }

        if (conceptFuture.valid()) {
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
            } else if (waitIfConfigured && conceptStatus == std::future_status::timeout) {
                timedOut.push_back("concepts");
                conceptsMaterialized = true;
                traceCollector.markStageTimeout("concepts");
            }
        } else {
            // Concept backend is Fallback — no GLiNER future. Mark as
            // materialized so the fallback enrichment below runs.
            conceptsMaterialized = true;
        }

        if (conceptsMaterialized && concepts.size() < workingConfig.conceptMaxCount) {
            enrichWithFallbackConcepts(query, concepts, workingConfig.conceptMaxCount);
        }

        if (conceptsMaterialized) {
            std::vector<ComponentResult> conceptMarker;
            if (!concepts.empty()) {
                conceptMarker.push_back(ComponentResult{.score = 1.0f});
            }
            traceCollector.markStageResult("concepts", conceptMarker, componentTiming["concepts"],
                                           !concepts.empty());
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

                // Only let query-neighbor docs elaborate an already graph-grounded query.
                // Pure vector-neighbor fanout can drift and perturb ranking without any
                // graph/text anchor from the query itself.
                if (!seedDocs.empty() && !graphExpansionTerms.empty()) {
                    auto neighborTerms = generateGraphExpansionTermsFromDocuments(
                        kgStore_, query, concepts, seedDocs,
                        GraphExpansionConfig{.maxTerms = workingConfig.graphExpansionMaxTerms,
                                             .maxSeeds = workingConfig.graphExpansionMaxSeeds,
                                             .maxNeighbors = workingConfig.graphMaxNeighbors});
                    std::unordered_map<std::string, size_t> termIndex;
                    termIndex.reserve(graphExpansionTerms.size());
                    for (size_t i = 0; i < graphExpansionTerms.size(); ++i) {
                        termIndex[graphExpansionTerms[i].text] = i;
                    }
                    for (const auto& term : neighborTerms) {
                        if (auto it = termIndex.find(term.text); it != termIndex.end()) {
                            graphExpansionTerms[it->second].score =
                                std::max(graphExpansionTerms[it->second].score, term.score);
                        } else {
                            termIndex[term.text] = graphExpansionTerms.size();
                            graphExpansionTerms.push_back(term);
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

    std::vector<ComponentResult> allComponentResults;
    std::unordered_set<std::string> topologyMedoidHashes;
    bool topologyWeakQueryRoutingApplied = false;
    bool topologyWeakQueryNarrowApplied = false;
    size_t topologyWeakQueryRoutedClusters = 0;
    size_t topologyWeakQueryRoutedDocs = 0;
    size_t topologyWeakQueryAddedCandidates = 0;
    size_t estimatedResults = 0;
    if (workingConfig.textWeight > 0.0f || workingConfig.simeonTextWeight > 0.0f)
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
                [this, &query, intent, &workingConfig, &textExpansionStats, &graphExpansionTerms,
                 &simeonRouteRecipe]() {
                    YAMS_ZONE_SCOPED_N("component::text");
                    return queryFullText(query, intent, workingConfig, workingConfig.textMaxResults,
                                         &textExpansionStats, &graphExpansionTerms,
                                         &simeonRouteRecipe);
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
                         stats_.avgMetadataTimeMicros, [this, &query, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(query, params, workingConfig.metadataMaxResults);
                         });

            // Collect Tier 1 results
            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);

            const bool needTier1Candidates = workingConfig.tieredNarrowVectorSearch;
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

            std::unordered_set<std::string> tier2Candidates = tier1Candidates;

            // --- TIER 2: Vector search is a peer retriever; always run when the embedding
            // leg is available. Narrowing to Tier 1 candidates is still an opt-in knob.
            YAMS_ZONE_SCOPED_N("search_engine::tier2_semantic");

            size_t tier1TextHits = 0;
            float tier1TopTextScore = 0.0f;
            for (const auto& componentResult : allComponentResults) {
                if (componentResult.source == ComponentResult::Source::Text ||
                    componentResult.source == ComponentResult::Source::SimeonText) {
                    tier1TextHits++;
                    tier1TopTextScore = std::max(tier1TopTextScore, componentResult.score);
                }
            }

            const bool hasStrongTextSignal =
                tier1TextHits >= workingConfig.weakQueryMinTextHits &&
                tier1TopTextScore >= workingConfig.weakQueryMinTopTextScore;

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

            // F3a: G1 "adaptive vector skip" removed. Vector is a peer retriever — it
            // always runs when the embedding leg is available. Budget guards remain
            // (short-query / memory pressure) because they are a resource concern, not
            // a quality gate.
            const bool semanticBudgetCapApplied = semanticBudgetActive;
            if (semanticBudgetCapApplied) {
                effectiveVectorMaxResults = std::min(
                    effectiveVectorMaxResults,
                    std::max<std::size_t>(1, workingConfig.semanticBudgetVectorMaxResults));
                effectiveEntityVectorMaxResults = std::min(
                    effectiveEntityVectorMaxResults,
                    std::max<std::size_t>(1, workingConfig.semanticBudgetEntityVectorMaxResults));
                response.debugStats["semantic_budget_vector_cap"] =
                    std::to_string(effectiveVectorMaxResults);
                response.debugStats["semantic_budget_entity_vector_cap"] =
                    std::to_string(effectiveEntityVectorMaxResults);
                spdlog::debug("Tiered search: semantic budget cap applied (vector_max={}, "
                              "entity_vector_max={}, text_hits={}, top_text_score={:.3f})",
                              effectiveVectorMaxResults, effectiveEntityVectorMaxResults,
                              tier1TextHits, tier1TopTextScore);
            }

            // Always materialize the query embedding when semantic search is configured so later
            // post-fusion steps are not blocked by adaptive lexical skipping.
            awaitEmbedding();

            if (workingConfig.enableTopologyWeakQueryRouting && weakTier1Query &&
                freshness.topologyReady && metadataRepo_ && kgStore_) {
                auto routeStart = std::chrono::steady_clock::now();
                yams::topology::MetadataKgTopologyArtifactStore topologyStore(metadataRepo_,
                                                                              kgStore_);
                auto latestTopology = topologyStore.loadLatest();
                if (latestTopology && latestTopology.value().has_value()) {
                    std::vector<std::string> seedHashes;
                    seedHashes.reserve(tier1Candidates.size());
                    for (const auto& hash : tier1Candidates) {
                        seedHashes.push_back(hash);
                    }

                    yams::topology::TopologyRouteRequest routeRequest;
                    routeRequest.queryText = query;
                    routeRequest.seedDocumentHashes = std::move(seedHashes);
                    routeRequest.limit = workingConfig.topologyMaxClusters;
                    routeRequest.weakQueryOnly = true;
                    routeRequest.scoringMode = yams::topology::RouteScoringMode::Current;
                    routeRequest.sparseDenseAlpha = 0.5F;
                    if (queryEmbedding.has_value()) {
                        routeRequest.queryEmbedding = queryEmbedding.value();
                    }

                    yams::topology::SparseGuidedClusterRouter router;
                    auto routes = router.route(routeRequest, latestTopology.value().value());
                    if (routes) {
                        topologyWeakQueryRoutedClusters = routes.value().size();
                        for (const auto& route : routes.value()) {
                            if (route.medoidDocumentHash.has_value()) {
                                topologyMedoidHashes.insert(route.medoidDocumentHash.value());
                            }
                            const auto clusterIt = std::find_if(
                                latestTopology.value()->clusters.begin(),
                                latestTopology.value()->clusters.end(),
                                [&route](const yams::topology::ClusterArtifact& cluster) {
                                    return cluster.clusterId == route.clusterId;
                                });
                            if (clusterIt == latestTopology.value()->clusters.end()) {
                                continue;
                            }
                            for (const auto& hash : clusterIt->memberDocumentHashes) {
                                if (topologyWeakQueryRoutedDocs >= workingConfig.topologyMaxDocs) {
                                    break;
                                }
                                ++topologyWeakQueryRoutedDocs;
                                if (tier2Candidates.insert(hash).second) {
                                    ++topologyWeakQueryAddedCandidates;
                                }
                            }
                            if (topologyWeakQueryRoutedDocs >= workingConfig.topologyMaxDocs) {
                                break;
                            }
                        }
                        topologyWeakQueryRoutingApplied = topologyWeakQueryRoutedClusters > 0;
                        topologyWeakQueryNarrowApplied = topologyWeakQueryAddedCandidates > 0;
                    } else {
                        spdlog::debug("Topology weak-query routing failed: {}",
                                      routes.error().message);
                    }
                } else if (!latestTopology) {
                    spdlog::debug("Topology weak-query snapshot load failed: {}",
                                  latestTopology.error().message);
                }
                response.componentTimingMicros["topology_weak_query"] =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - routeStart)
                        .count();
            }

            const std::unordered_set<std::string>* vectorSearchNarrowSet =
                topologyWeakQueryNarrowApplied ? &tier2Candidates : nullptr;

            // Decide whether to narrow vector search to Tier 1 candidates
            // Narrow if: config enabled AND Tier 1 has enough candidates
            const bool shouldNarrow = (vectorSearchNarrowSet != nullptr) ||
                                      (workingConfig.tieredNarrowVectorSearch &&
                                       tier2Candidates.size() >= workingConfig.tieredMinCandidates);

            response.debugStats["topology_weak_query_total_candidates"] =
                std::to_string(tier2Candidates.size());
            response.debugStats["topology_weak_query_enabled"] =
                workingConfig.enableTopologyWeakQueryRouting ? "1" : "0";
            response.debugStats["topology_weak_query_applied"] =
                topologyWeakQueryRoutingApplied ? "1" : "0";
            response.debugStats["topology_weak_query_narrow_applied"] =
                topologyWeakQueryNarrowApplied ? "1" : "0";
            response.debugStats["topology_weak_query_routed_clusters"] =
                std::to_string(topologyWeakQueryRoutedClusters);
            response.debugStats["topology_weak_query_routed_docs"] =
                std::to_string(topologyWeakQueryRoutedDocs);
            response.debugStats["topology_weak_query_added_candidates"] =
                std::to_string(topologyWeakQueryAddedCandidates);

            traceCollector.recordStageCounter("vector", "budget_guard_skip", 0);
            traceCollector.recordStageCounter("vector", "budget_guard_cap_applied",
                                              semanticBudgetCapApplied ? 1 : 0);
            traceCollector.recordStageCounter("vector", "should_narrow_applied",
                                              shouldNarrow ? 1 : 0);
            traceCollector.recordStageCounter("vector", "effective_max_results",
                                              static_cast<std::int64_t>(effectiveVectorMaxResults));
            traceCollector.recordStageCounter("vector", "weak_query_fanout_boost_applied",
                                              weakQueryFanoutBoostApplied ? 1 : 0);
            traceCollector.recordStageCounter("vector", "tier2_candidates_size",
                                              static_cast<std::int64_t>(tier2Candidates.size()));
            traceCollector.recordStageCounter("vector", "tier1_text_hits",
                                              static_cast<std::int64_t>(tier1TextHits));
            traceCollector.recordStageCounter(
                "vector", "tier1_top_text_score_milli",
                static_cast<std::int64_t>(
                    std::llround(static_cast<double>(tier1TopTextScore) * 1000.0)));

            if (queryEmbedding.has_value() && vectorDb_ && !hasVectorTierDimMismatch()) {
                vectorFuture = schedule(
                    "vector", workingConfig.vectorWeight, stats_.vectorQueries,
                    stats_.avgVectorTimeMicros,
                    [&queryEmbedding, &tier2Candidates, vectorSearchNarrowSet, shouldNarrow,
                     effectiveVectorMaxResults, &queryVectorWithRelaxedRetry]() {
                        YAMS_ZONE_SCOPED_N("component::vector");
                        if (shouldNarrow) {
                            const auto* narrowSet =
                                vectorSearchNarrowSet ? vectorSearchNarrowSet : &tier2Candidates;
                            return queryVectorWithRelaxedRetry(
                                queryEmbedding.value(), effectiveVectorMaxResults, narrowSet);
                        } else {
                            return queryVectorWithRelaxedRetry(queryEmbedding.value(),
                                                               effectiveVectorMaxResults);
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
            } else if (hasVectorTierDimMismatch()) {
                markVectorTierDimMismatch();
            }

            if (kgStore_) {
                if (workingConfig.graphUseQueryConcepts && workingConfig.waitForConceptExtraction) {
                    materializeConcepts(true);
                }
                kgFuture = schedule(
                    "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                    [this, &query, &workingConfig, &concepts]() {
                        YAMS_ZONE_SCOPED_N("component::kg");
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults, &concepts);
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
                [this, &query, intent, &workingConfig, &textExpansionStats, &graphExpansionTerms,
                 &simeonRouteRecipe]() {
                    YAMS_ZONE_SCOPED_N("component::text");
                    return queryFullText(query, intent, workingConfig, workingConfig.textMaxResults,
                                         &textExpansionStats, &graphExpansionTerms,
                                         &simeonRouteRecipe);
                });

            const bool deferSemanticStages = semanticBudgetActive;
            if (kgStore_ && !deferSemanticStages) {
                if (workingConfig.graphUseQueryConcepts && workingConfig.waitForConceptExtraction) {
                    materializeConcepts(true);
                }
                kgFuture = schedule(
                    "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros,
                    [this, &query, &workingConfig, &concepts]() {
                        YAMS_ZONE_SCOPED_N("component::kg");
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults, &concepts);
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
                         stats_.avgMetadataTimeMicros, [this, &query, &params, &workingConfig]() {
                             YAMS_ZONE_SCOPED_N("component::metadata");
                             return queryMetadata(query, params, workingConfig.metadataMaxResults);
                         });

            collectIf(textFuture, "text", stats_.textQueries, stats_.avgTextTimeMicros);
            collectIf(pathFuture, "path", stats_.pathTreeQueries, stats_.avgPathTreeTimeMicros);
            collectIf(tagFuture, "tag", stats_.tagQueries, stats_.avgTagTimeMicros);
            collectIf(metaFuture, "metadata", stats_.metadataQueries, stats_.avgMetadataTimeMicros);

            const auto [budgetLexicalHits, budgetTopTextScore] =
                deferSemanticStages ? recordSemanticBudgetLexicalEvidence(allComponentResults)
                                    : std::pair<size_t, float>{0, 0.0f};
            const bool strongBudgetLexical =
                deferSemanticStages && (budgetLexicalHits >= 5 || budgetTopTextScore >= 0.20f);
            if (deferSemanticStages && strongBudgetLexical) {
                semanticTierSkipReason = "budget_guard_strong_lexical";
                response.debugStats["semantic_budget_skip_reason"] = semanticTierSkipReason;
                skipped.push_back("kg");
                traceCollector.markStageSkipped("kg", semanticTierSkipReason);
            }
            {
                if (kgStore_ && !(deferSemanticStages && strongBudgetLexical)) {
                    if (workingConfig.graphUseQueryConcepts &&
                        workingConfig.waitForConceptExtraction) {
                        materializeConcepts(true);
                    }
                    kgFuture = schedule("kg", workingConfig.kgWeight, stats_.kgQueries,
                                        stats_.avgKgTimeMicros,
                                        [this, &query, &workingConfig, &concepts]() {
                                            YAMS_ZONE_SCOPED_N("component::kg");
                                            return queryKnowledgeGraph(
                                                query, workingConfig.kgMaxResults, &concepts);
                                        });
                }

                // Await embedding (ran in parallel with early lexical stages unless budgeted)
                // then schedule vector work if still needed.
                awaitEmbedding();
                if (queryEmbedding.has_value() && vectorDb_ && !hasVectorTierDimMismatch()) {
                    effectiveVectorMaxResults = workingConfig.vectorMaxResults;
                    effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
                    vectorFuture =
                        schedule("vector", workingConfig.vectorWeight, stats_.vectorQueries,
                                 stats_.avgVectorTimeMicros,
                                 [&queryEmbedding, &workingConfig, &queryVectorWithRelaxedRetry]() {
                                     YAMS_ZONE_SCOPED_N("component::vector");
                                     return queryVectorWithRelaxedRetry(
                                         queryEmbedding.value(), workingConfig.vectorMaxResults);
                                 });

                    entityVectorFuture = schedule(
                        "entity_vector", workingConfig.entityVectorWeight,
                        stats_.entityVectorQueries, stats_.avgEntityVectorTimeMicros,
                        [this, &queryEmbedding, &workingConfig]() {
                            YAMS_ZONE_SCOPED_N("component::entity_vector");
                            return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                      workingConfig.entityVectorMaxResults);
                        });

                } else if (hasVectorTierDimMismatch()) {
                    markVectorTierDimMismatch();
                }

                collectIf(kgFuture, "kg", stats_.kgQueries, stats_.avgKgTimeMicros);
                collectIf(vectorFuture, "vector", stats_.vectorQueries, stats_.avgVectorTimeMicros);
                collectIf(entityVectorFuture, "entity_vector", stats_.entityVectorQueries,
                          stats_.avgEntityVectorTimeMicros);
            }
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
                return queryFullText(query, intent, workingConfig, workingConfig.textMaxResults,
                                     &textExpansionStats, &graphExpansionTerms, &simeonRouteRecipe);
            },
            "text", workingConfig.textWeight, stats_.textQueries, stats_.avgTextTimeMicros);

        runSequential([&]() { return queryPathTree(query, workingConfig.pathTreeMaxResults); },
                      "path", workingConfig.pathTreeWeight, stats_.pathTreeQueries,
                      stats_.avgPathTreeTimeMicros);

        if (!params.tags.empty()) {
            runSequential(
                [&]() {
                    return queryTags(params.tags, params.matchAllTags, workingConfig.tagMaxResults);
                },
                "tag", workingConfig.tagWeight, stats_.tagQueries, stats_.avgTagTimeMicros);
        }

        runSequential(
            [&]() { return queryMetadata(query, params, workingConfig.metadataMaxResults); },
            "metadata", workingConfig.metadataWeight, stats_.metadataQueries,
            stats_.avgMetadataTimeMicros);

        const auto [budgetLexicalHits, budgetTopTextScore] =
            semanticBudgetActive ? recordSemanticBudgetLexicalEvidence(allComponentResults)
                                 : std::pair<size_t, float>{0, 0.0f};
        const bool strongBudgetLexical =
            semanticBudgetActive && (budgetLexicalHits >= 5 || budgetTopTextScore >= 0.20f);
        if (semanticBudgetActive && strongBudgetLexical) {
            semanticTierSkipReason = "budget_guard_strong_lexical";
            response.debugStats["semantic_budget_skip_reason"] = semanticTierSkipReason;
            skipped.push_back("kg");
            traceCollector.markStageSkipped("kg", semanticTierSkipReason);
        }
        {
            if (kgStore_ && !(semanticBudgetActive && strongBudgetLexical)) {
                if (workingConfig.graphUseQueryConcepts && workingConfig.waitForConceptExtraction) {
                    materializeConcepts(true);
                }
                runSequential(
                    [&]() {
                        return queryKnowledgeGraph(query, workingConfig.kgMaxResults, &concepts);
                    },
                    "kg", workingConfig.kgWeight, stats_.kgQueries, stats_.avgKgTimeMicros);
            }

            // Await embedding after lexical stages if budgeted, otherwise it may already be ready.
            awaitEmbedding();
            if (queryEmbedding.has_value() && vectorDb_ && !hasVectorTierDimMismatch()) {
                effectiveVectorMaxResults = workingConfig.vectorMaxResults;
                effectiveEntityVectorMaxResults = workingConfig.entityVectorMaxResults;
                if (semanticBudgetActive) {
                    effectiveVectorMaxResults = std::min(
                        effectiveVectorMaxResults,
                        std::max<std::size_t>(1, workingConfig.semanticBudgetVectorMaxResults));
                    effectiveEntityVectorMaxResults =
                        std::min(effectiveEntityVectorMaxResults,
                                 std::max<std::size_t>(
                                     1, workingConfig.semanticBudgetEntityVectorMaxResults));
                    response.debugStats["semantic_budget_vector_cap"] =
                        std::to_string(effectiveVectorMaxResults);
                    response.debugStats["semantic_budget_entity_vector_cap"] =
                        std::to_string(effectiveEntityVectorMaxResults);
                }
                runSequential(
                    [&]() {
                        return queryVectorWithRelaxedRetry(queryEmbedding.value(),
                                                           effectiveVectorMaxResults);
                    },
                    "vector", workingConfig.vectorWeight, stats_.vectorQueries,
                    stats_.avgVectorTimeMicros);

                runSequential(
                    [&]() {
                        return queryEntityVectors(queryEmbedding.value(), workingConfig,
                                                  effectiveEntityVectorMaxResults);
                    },
                    "entity_vector", workingConfig.entityVectorWeight, stats_.entityVectorQueries,
                    stats_.avgEntityVectorTimeMicros);

            } else if (hasVectorTierDimMismatch()) {
                markVectorTierDimMismatch();
            }
        }
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

            const auto graphDocStats = detail::appendGraphDocumentExpansionTerms(
                metadataRepo_, workingConfig, docSeedGraphTerms, existingDocIds,
                allComponentResults, &textExpansionStats);
            graphDocExpansionFtsHitCount += graphDocStats.rawHitCount;
            graphDocExpansionFtsAddedCount += graphDocStats.addedCount;

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

        auto subPhrases = detail::generateQuerySubPhrases(metadataRepo_, query,
                                                          workingConfig.multiVectorMaxPhrases);
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
        if (!workingConfig.enableGraphQueryExpansion ||
            textExpansionStats.graphExpansionFtsAddedCount == 0) {
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
                            cr.source == ComponentResult::Source::SimeonText ||
                            cr.source == ComponentResult::Source::GraphText ||
                            cr.source == ComponentResult::Source::PathTree ||
                            cr.source == ComponentResult::Source::KnowledgeGraph ||
                            cr.source == ComponentResult::Source::Tag ||
                            cr.source == ComponentResult::Source::Metadata ||
                            cr.source == ComponentResult::Source::Symbol) {
                            graphVectorTextAnchoredHashes.insert(cr.documentHash);
                        }
                        if (cr.source == ComponentResult::Source::Text ||
                            cr.source == ComponentResult::Source::SimeonText ||
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
    // NOLINTNEXTLINE(concurrency-mt-unsafe) — debug env var; not modified concurrently
    if (const char* env = std::getenv("YAMS_SEARCH_DIAG"); env && std::string(env) == "1") {
        const bool embeddingsAvailable = queryEmbedding.has_value();
        spdlog::warn("[search_diag] query='{}' components: contributing={} failed={} timed_out={} "
                     "skipped={} embedding={} pre_fusion_total={} "
                     "weights(text={},simeon_text={},vector={},entity_vector={},kg={},path={},tag={"
                     "},meta={})",
                     query, contributing.size(), failed.size(), timedOut.size(), skipped.size(),
                     embeddingsAvailable ? "yes" : "no", allComponentResults.size(),
                     workingConfig.textWeight, workingConfig.simeonTextWeight,
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

    preFusionDocIds = collectUniqueComponentDocIds(allComponentResults);
    preFusionSignals = buildPreFusionSignalMap(allComponentResults);

    // P7: adaptive convex-fusion switch. Once the SearchTuner reports convergence
    // and the user has opted in via enableAdaptiveFusion, override the configured
    // fusion strategy to CONVEX. The convex dispatch itself falls back to RRF on
    // any exception so this is fail-soft.
    bool adaptiveFusionApplied = false;
    bool tunerConvergedForFusion = false;
    if (tuner_) {
        tunerConvergedForFusion = tuner_->hasConverged();
    }
    if (workingConfig.enableAdaptiveFusion && tunerConvergedForFusion &&
        workingConfig.fusionStrategy != SearchEngineConfig::FusionStrategy::CONVEX) {
        workingConfig.fusionStrategy = SearchEngineConfig::FusionStrategy::CONVEX;
        adaptiveFusionApplied = true;
    }
    response.debugStats["fusion_strategy"] =
        SearchEngineConfig::fusionStrategyToString(workingConfig.fusionStrategy);
    response.debugStats["fusion_adaptive_applied"] = adaptiveFusionApplied ? "1" : "0";
    response.debugStats["search_tuner_converged"] = tunerConvergedForFusion ? "1" : "0";
    // Joins per-query traces back to the active simeon encoder recipe (env
    // controlled). When the cascade fusion is selected the recipe identity
    // includes the fusion knobs so retrieval-quality runs can be attributed
    // end-to-end.
    {
        std::string recipe = vector::simeonRecipeLabel();
        if (workingConfig.fusionStrategy ==
            SearchEngineConfig::FusionStrategy::WEIGHTED_LINEAR_ZSCORE) {
            recipe += "+linear_a";
            // emit alpha as e.g. "075" for 0.75 to match the bench row label
            const int a100 = static_cast<int>(std::lround(
                std::clamp(static_cast<double>(workingConfig.weightedLinearZScoreAlpha), 0.0, 1.0) *
                100.0));
            char buf[8];
            std::snprintf(buf, sizeof(buf), "%03d", a100);
            recipe += buf;
            recipe += "_pool";
            recipe += std::to_string(workingConfig.weightedLinearZScorePoolSize);
        }
        response.debugStats["simeon_recipe"] = recipe;
        if (!simeonRouteRecipe.empty()) {
            response.debugStats["simeon_route"] = simeonRouteRecipe;
        }
        if (!selectedSimeonBanditArm.empty()) {
            response.debugStats["simeon_bandit_arm"] = selectedSimeonBanditArm;
        }
        if (!selectedSimeonBanditProfile.empty()) {
            response.debugStats["simeon_bandit_profile"] = selectedSimeonBanditProfile;
        }
    }

    // Record simeon bandit reward for the arm selected this query.
    // Training-free: uses proxy signals (result density per ms).
    if (!selectedSimeonBanditArm.empty() && !selectedSimeonBanditProfile.empty()) {
        double elapsedMs =
            std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - startTime)
                .count();
        double resultCount = static_cast<double>(response.results.size());
        double resultRate = std::clamp(resultCount / std::max(1.0, elapsedMs) * 10.0, 0.0, 0.95);
        double armBonus = (selectedSimeonBanditArm.find("rm3") != std::string::npos) ? 0.05 : 0.0;
        double reward = std::clamp(resultRate + armBonus, 0.0, 1.0);

        std::lock_guard<std::mutex> lock(simeonBanditsMutex_);
        auto profileIt = simeonBandits_.find(selectedSimeonBanditProfile);
        if (profileIt == simeonBandits_.end()) {
            profileIt = simeonBandits_.find("MIXED");
        }
        if (profileIt != simeonBandits_.end()) {
            auto& bandit = profileIt->second;
            std::optional<std::size_t> armIdx;
            const auto& arms = bandit.arms();
            for (std::size_t i = 0; i < arms.size(); ++i) {
                if (arms[i].id == selectedSimeonBanditArm) {
                    armIdx = i;
                    break;
                }
            }
            if (armIdx.has_value()) {
                bandit.recordReward(*armIdx, reward, TunerMAB::RewardSource::Proxy);
            }
        }
    }

    // Per-query vectorOnlyThreshold relaxation: when a query produces no
    // anchored/text results at all, lower the threshold so vector-only docs
    // are not needlessly filtered.  This helps benchmark corpora (SciFact)
    // where semantic similarity dominates and lexical anchoring is scarce.
    bool noAnchorThresholdRelaxed = false;
    if (workingConfig.vectorOnlyThreshold > 0.50f) {
        bool hasAnyAnchor = false;
        for (const auto& cr : allComponentResults) {
            if (isTextAnchoringComponent(cr.source)) {
                hasAnyAnchor = true;
                break;
            }
        }
        if (!hasAnyAnchor && !allComponentResults.empty()) {
            workingConfig.vectorOnlyThreshold = 0.50f;
            noAnchorThresholdRelaxed = true;
        }
    }

    ResultFusion fusion(workingConfig);
    {
        YAMS_ZONE_SCOPED_N("fusion::results");
        response.results = fusion.fuse(allComponentResults);
    }
    if (!response.results.empty()) {
        postFusionSnapshot = response.results;
    }
    if (!topologyMedoidHashes.empty() && workingConfig.topologyMedoidBoost > 0.0f &&
        !response.results.empty()) {
        bool needsSort = false;
        size_t boostedMedoids = 0;
        const double boost = 1.0 + static_cast<double>(workingConfig.topologyMedoidBoost);
        for (auto& result : response.results) {
            if (topologyMedoidHashes.contains(result.document.sha256Hash)) {
                result.score *= boost;
                needsSort = true;
                ++boostedMedoids;
            }
        }
        if (needsSort) {
            std::stable_sort(response.results.begin(), response.results.end(),
                             [](const auto& a, const auto& b) { return a.score > b.score; });
        }
        response.debugStats["topology_medoid_boosted"] = std::to_string(boostedMedoids);
    }
    // Phase Y: multi-meta-path post-fusion boost (PathSim-style fixed weights).
    // Each enabled meta-path scores candidates independently; sum contributes a
    // continuous boost on top of the result list. Off by default → no-op.
    if (workingConfig.enableMetaPathRouting && !response.results.empty() &&
        queryEmbedding.has_value() && !queryEmbedding->empty()) {
        try {
            auto mp =
                computeMetaPathBoosts(queryEmbedding.value(), vectorDb_, kgStore_, workingConfig);
            if (!mp.docBoost.empty()) {
                bool needsSort = false;
                std::size_t boostedCount = 0;
                for (auto& r : response.results) {
                    auto it = mp.docBoost.find(r.document.sha256Hash);
                    if (it == mp.docBoost.end()) {
                        continue;
                    }
                    const float factor = 1.0f + workingConfig.metaPathBoostAlpha * it->second;
                    if (factor != 1.0f) {
                        r.score *= factor;
                        needsSort = true;
                        ++boostedCount;
                    }
                }
                if (needsSort) {
                    std::stable_sort(
                        response.results.begin(), response.results.end(),
                        [](const auto& a, const auto& b) { return a.score > b.score; });
                }
                response.debugStats["meta_path_seed_count"] = std::to_string(mp.seedDocCount);
                response.debugStats["meta_path_boosted_count"] = std::to_string(boostedCount);
                response.debugStats["meta_path_sem_hits"] = std::to_string(mp.semHits);
                response.debugStats["meta_path_call_hits"] = std::to_string(mp.callHits);
                response.debugStats["meta_path_def_hits"] = std::to_string(mp.defHits);
            }
        } catch (const std::exception& e) {
            spdlog::warn("[search] meta-path routing threw: {}", e.what());
        } catch (...) {
            // Swallow — Phase Y is opt-in; never crash search.
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

    // Diagnostic: log graph reranker gate status once per benchmark run.
    {
        static std::atomic<bool> gsLogged{false};
        if (!gsLogged.exchange(true, std::memory_order_relaxed)) {
            // Verify entities are in the KG by querying the store directly.
            size_t firstDocEntityCount = 0;
            if (kgScorer_ && !response.results.empty()) {
                const auto& first = response.results[0];
                std::string hash = first.document.sha256Hash;
                if (auto docIdRes = kgStore_->getDocumentIdByHash(hash)) {
                    auto entities =
                        kgStore_->getDocEntitiesForDocument(docIdRes.value().value(), 5, 0);
                    if (entities) {
                        firstDocEntityCount = entities.value().size();
                    }
                }
            }
            spdlog::debug(
                "[graph_rerank-gate] enable={} kgScorer={} results={} resultSize={} "
                "shortQueryBudgeted={} corpusWarming={} bypassWarmingGate={} "
                "lexicalReady={} awaitingDrain={} postIngestInflight={} postIngestQueued={} "
                "firstDocEntities={}",
                workingConfig.enableGraphRerank ? 1 : 0, kgScorer_ ? 1 : 0,
                !response.results.empty() ? 1 : 0, response.results.size(),
                shortQueryBudgeted ? 1 : 0, corpusWarming ? 1 : 0,
                workingConfig.bypassCorpusWarmingGate ? 1 : 0, freshness.lexicalReady ? 1 : 0,
                freshness.awaitingDrain ? 1 : 0, freshness.postIngestInFlight,
                freshness.postIngestQueued, firstDocEntityCount);
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
                if (!res.document.sha256Hash.empty()) {
                    candidateIds.push_back(res.document.sha256Hash);
                } else if (!res.document.filePath.empty()) {
                    candidateIds.push_back(res.document.filePath);
                } else {
                    candidateIds.push_back(
                        documentIdForTrace(res.document.filePath, res.document.sha256Hash));
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
                const float communityWeight =
                    std::clamp(workingConfig.graphCommunityWeight, 0.0f, 1.0f);
                const float baseSignalWeight = std::max(0.0f, 1.0f - communityWeight);
                std::size_t graphCommunitySupportedDocs = 0;
                std::size_t graphCommunityEdges = 0;
                std::size_t graphLargestCommunity = 0;
                double graphCommunitySignalMass = 0.0;
                std::size_t graphCommunityBoostedDocs = 0;
                const auto communitySupport = computeReciprocalCommunitySupport(
                    kgStore_, candidateIds,
                    std::max<std::size_t>(8, workingConfig.graphMaxNeighbors),
                    workingConfig.graphCommunityReferenceSize, &graphCommunitySupportedDocs,
                    &graphCommunityEdges, &graphLargestCommunity,
                    workingConfig.graphCommunityDecayHalfLifeDays,
                    workingConfig.graphCommunityMinEdgeWeight);

                std::vector<float> rawSignals(rerankWindow, 0.0f);
                std::vector<float> lexicalAnchors(rerankWindow, 0.0f);
                std::vector<float> queryCoverages(rerankWindow, 0.0f);
                float maxRawSignal = 0.0f;
                float maxLexicalAnchor = 0.0f;
                size_t topSignalIndex = rerankWindow;

                for (size_t i = 0; i < rerankWindow; ++i) {
                    const auto& candidateId = candidateIds[i];
                    const auto& result = response.results[i];
                    auto scoreIt = graphScores.find(candidateId);
                    if (scoreIt != graphScores.end()) {
                        graphMatchedCandidates++;
                    }

                    const auto getFeature = [&scoreIt, &graphScores](const char* key) {
                        if (scoreIt == graphScores.end()) {
                            return 0.0f;
                        }
                        auto featureIt = scoreIt->second.features.find(key);
                        return featureIt != scoreIt->second.features.end() ? featureIt->second
                                                                           : 0.0f;
                    };

                    const float queryCoverage =
                        std::clamp(getFeature("feature_query_coverage_ratio"), 0.0f, 1.0f);
                    queryCoverages[i] = queryCoverage;
                    const float pathSupport =
                        std::clamp(getFeature("feature_path_support_score"), 0.0f, 1.0f);
                    const float entitySignal =
                        scoreIt != graphScores.end() ? scoreIt->second.entity : 0.0f;
                    const float structuralSignal =
                        scoreIt != graphScores.end() ? scoreIt->second.structural : 0.0f;
                    const float communitySignal = i < communitySupport.size()
                                                      ? std::clamp(communitySupport[i], 0.0f, 1.0f)
                                                      : 0.0f;
                    const float lexicalAnchor = std::clamp(
                        static_cast<float>(
                            result.keywordScore.value_or(0.0) + result.pathScore.value_or(0.0) +
                            result.tagScore.value_or(0.0) + result.symbolScore.value_or(0.0)),
                        0.0f, 1.0f);
                    lexicalAnchors[i] = lexicalAnchor;
                    maxLexicalAnchor = std::max(maxLexicalAnchor, lexicalAnchor);
                    graphCommunitySignalMass += communitySignal;

                    // Composite graph relevance signal (per-profile weights).
                    const float rawSignal =
                        std::clamp((entitySignal * workingConfig.graphEntitySignalWeight +
                                    structuralSignal * workingConfig.graphStructuralSignalWeight +
                                    queryCoverage * workingConfig.graphCoverageSignalWeight +
                                    pathSupport * workingConfig.graphPathSignalWeight) *
                                           baseSignalWeight +
                                       communitySignal * communityWeight,
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
                    const float lexicalAnchorRatio =
                        maxLexicalAnchor > 0.0f ? lexicalAnchors[i] / maxLexicalAnchor : 0.0f;
                    const float corrobFloor = workingConfig.graphCorroborationFloor;
                    const float corroboration =
                        std::clamp(corrobFloor + (1.0f - corrobFloor) * std::max(lexicalAnchorRatio,
                                                                                 queryCoverages[i]),
                                   0.0f, 1.0f);
                    const float rankPrior = 1.0f / std::sqrt(1.0f + static_cast<float>(i));
                    const float guardedSignal =
                        std::clamp(effectiveSignal * corroboration * rankPrior, 0.0f, 1.0f);

                    const float boost = std::min(maxBoost, rerankWeight * guardedSignal);
                    if (boost <= 0.0f) {
                        continue;
                    }

                    response.results[i].score *= (1.0 + static_cast<double>(boost));
                    response.results[i].kgScore = response.results[i].kgScore.value_or(0.0) + boost;
                    boosted = true;
                    graphBoostedDocs++;
                    if (communityWeight > 0.0f && i < communitySupport.size() &&
                        communitySupport[i] > 0.0f) {
                        graphCommunityBoostedDocs++;
                    }
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
                        const float communitySignal =
                            i < communitySupport.size()
                                ? std::clamp(communitySupport[i], 0.0f, 1.0f)
                                : 0.0f;
                        graphExplainJson.push_back({
                            {"doc_id", expl.id},
                            {"components", expl.components},
                            {"community_signal", communitySignal},
                            {"reasons", expl.reasons},
                        });
                    }
                    response.debugStats["graph_explanations_json"] = graphExplainJson.dump();
                    response.debugStats["graph_doc_probe_json"] =
                        buildGraphDocProbeJson(kgStore_, response.results, rerankWindow).dump();
                }
                response.debugStats["graph_community_supported_docs"] =
                    std::to_string(graphCommunitySupportedDocs);
                response.debugStats["graph_community_edge_count"] =
                    std::to_string(graphCommunityEdges);
                response.debugStats["graph_community_largest_size"] =
                    std::to_string(graphLargestCommunity);
                response.debugStats["graph_community_signal_mass"] =
                    fmt::format("{:.4f}", graphCommunitySignalMass);
                response.debugStats["graph_community_boosted_docs"] =
                    std::to_string(graphCommunityBoostedDocs);
                response.debugStats["graph_community_weight"] =
                    fmt::format("{:.4f}", communityWeight);
            } else {
                failed.push_back("graph_rerank");
                traceCollector.markStageFailure("graph_rerank");
                spdlog::debug("[graph_rerank] KG scoring failed: {}",
                              graphScoresResult.error().message);
            }
        }

        if (graphRerankApplied && !graphlessPostFusionSnapshot.empty()) {
            const size_t guardWindow =
                std::min(response.results.size(), std::min(graphlessPostFusionSnapshot.size(),
                                                           workingConfig.graphRerankTopN));
            if (guardWindow > 0) {
                const auto lexicalAnchorScore = [](const SearchResult& r) {
                    return r.keywordScore.value_or(0.0) + r.graphTextScore.value_or(0.0) +
                           r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) +
                           r.symbolScore.value_or(0.0);
                };

                std::vector<std::string> protectedIds;
                protectedIds.reserve(guardWindow);
                std::unordered_map<std::string, SearchResult> graphlessById;
                graphlessById.reserve(guardWindow);
                for (size_t i = 0; i < guardWindow; ++i) {
                    const auto docId =
                        documentIdForTrace(graphlessPostFusionSnapshot[i].document.filePath,
                                           graphlessPostFusionSnapshot[i].document.sha256Hash);
                    if (docId.empty() ||
                        lexicalAnchorScore(graphlessPostFusionSnapshot[i]) <= 0.0) {
                        continue;
                    }
                    protectedIds.push_back(docId);
                    graphlessById.emplace(docId, graphlessPostFusionSnapshot[i]);
                }

                if (!protectedIds.empty()) {
                    const auto actualTopIds =
                        collectRankedResultDocIds(response.results, guardWindow);
                    std::unordered_set<std::string> protectedSet(protectedIds.begin(),
                                                                 protectedIds.end());
                    std::unordered_set<std::string> actualTopSet(actualTopIds.begin(),
                                                                 actualTopIds.end());

                    std::vector<std::string> displacedProtectedIds;
                    displacedProtectedIds.reserve(protectedIds.size());
                    for (const auto& docId : protectedIds) {
                        if (!actualTopSet.contains(docId)) {
                            displacedProtectedIds.push_back(docId);
                        }
                    }

                    size_t displacedCursor = 0;
                    for (size_t i = 0;
                         i < guardWindow && displacedCursor < displacedProtectedIds.size(); ++i) {
                        const auto actualId =
                            documentIdForTrace(response.results[i].document.filePath,
                                               response.results[i].document.sha256Hash);
                        if (protectedSet.contains(actualId)) {
                            continue;
                        }

                        auto restoreIt =
                            graphlessById.find(displacedProtectedIds[displacedCursor++]);
                        if (restoreIt == graphlessById.end()) {
                            continue;
                        }
                        response.results[i] = restoreIt->second;
                        ++graphRerankGuardReplacementCount;
                    }
                }
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

        // Diagnostic: log graph reranker activity periodically so we can trace
        // whether the KG entities are producing useful signal at query time.
        {
            static std::atomic<uint64_t> grQueryCount{0};
            const auto n = grQueryCount.fetch_add(1, std::memory_order_relaxed) + 1;
            if ((n % 50) == 0 || n == 1) {
                spdlog::info("[graph_rerank] n={} window={} matched={} applied={} concepts={} "
                             "skipped={} failed={} budget_ms={} elapsed_us={}",
                             n, rerankWindow, graphMatchedCandidates, graphRerankApplied ? 1 : 0,
                             graphQueryConceptCount, skipped.size(), failed.size(),
                             workingConfig.graphScoringBudgetMs, componentTiming["graph_rerank"]);
            }
        }
    }

    if (stageTraceEnabled) {
        postGraphSnapshot = response.results;
    }

    if (workingConfig.enableReranking && workingConfig.rerankTopK > 0) {
        const auto crossStart = std::chrono::steady_clock::now();
        auto finishCrossMicros = [&]() {
            return std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - crossStart)
                .count();
        };
        auto skipCrossRerank = [&](std::string reason) {
            const auto duration = finishCrossMicros();
            componentTiming["cross_rerank"] = duration;
            response.debugStats["cross_rerank_skip_reason"] = reason;
            skipped.push_back("cross_rerank");
            traceCollector.markStageSkipped("cross_rerank", std::move(reason));
        };

        const size_t rerankWindow = std::min(workingConfig.rerankTopK, response.results.size());
        response.debugStats["cross_rerank_configured"] = "1";
        response.debugStats["cross_rerank_available"] = crossReranker_ ? "1" : "0";
        response.debugStats["cross_rerank_window"] = std::to_string(rerankWindow);
        response.debugStats["cross_rerank_snippet_max_chars"] =
            std::to_string(workingConfig.rerankSnippetMaxChars);

        if (rerankWindow == 0) {
            skipCrossRerank("no_candidates");
        } else if (!crossReranker_) {
            skipCrossRerank("reranker_unavailable");
        } else {
            traceCollector.markStageAttempted("cross_rerank");

            const size_t textLimit = workingConfig.rerankSnippetMaxChars;
            auto appendPart = [](std::string& out, const std::string& part) {
                if (part.empty()) {
                    return;
                }
                if (!out.empty()) {
                    out.push_back('\n');
                }
                out += part;
            };
            auto buildRerankText = [&](const SearchResult& result) {
                std::string text;
                appendPart(text, result.document.fileName);
                appendPart(text, result.document.filePath);
                appendPart(text, result.snippet);
                if (metadataRepo_ && result.document.id > 0 && text.size() < textLimit) {
                    auto contentResult = metadataRepo_->getContent(result.document.id);
                    if (contentResult && contentResult.value()) {
                        const auto& content = contentResult.value()->contentText;
                        if (!content.empty()) {
                            const size_t remaining =
                                textLimit > text.size() ? textLimit - text.size() : size_t{0};
                            appendPart(text, content.substr(0, remaining));
                        }
                    }
                }
                if (text.size() > textLimit) {
                    text.resize(textLimit);
                }
                return text;
            };

            std::vector<std::string> rerankTexts;
            rerankTexts.reserve(rerankWindow);
            std::vector<double> originalScores;
            originalScores.reserve(rerankWindow);
            for (size_t i = 0; i < rerankWindow; ++i) {
                rerankTexts.push_back(buildRerankText(response.results[i]));
                originalScores.push_back(response.results[i].score);
            }

            auto scoreResult = crossReranker_(query, rerankTexts);
            const auto duration = finishCrossMicros();
            componentTiming["cross_rerank"] = duration;

            if (!scoreResult) {
                response.debugStats["cross_rerank_error"] = scoreResult.error().message;
                if (scoreResult.error().code == ErrorCode::NotImplemented ||
                    scoreResult.error().code == ErrorCode::NotInitialized) {
                    skipCrossRerank("reranker_unavailable");
                } else {
                    failed.push_back("cross_rerank");
                    traceCollector.markStageFailure("cross_rerank", duration);
                }
            } else if (scoreResult.value().size() != rerankWindow) {
                failed.push_back("cross_rerank");
                response.debugStats["cross_rerank_error"] = "score_size_mismatch";
                traceCollector.markStageFailure("cross_rerank", duration);
            } else {
                const auto& scores = scoreResult.value();
                auto [minIt, maxIt] = std::minmax_element(scores.begin(), scores.end());
                const float minScore =
                    minIt != scores.end() && std::isfinite(*minIt) ? *minIt : 0.0f;
                const float maxScore =
                    maxIt != scores.end() && std::isfinite(*maxIt) ? *maxIt : 0.0f;
                float secondScore = 0.0f;
                for (float score : scores) {
                    if (!std::isfinite(score) || score >= maxScore) {
                        continue;
                    }
                    secondScore = std::max(secondScore, score);
                }
                rerankGuardScoreGap = static_cast<double>(maxScore - secondScore);

                if (!(maxScore > minScore)) {
                    skipCrossRerank("no_score_variance");
                } else if (rerankGuardScoreGap <
                           static_cast<double>(
                               std::max(0.0f, workingConfig.rerankScoreGapThreshold))) {
                    skipCrossRerank("score_gap_below_threshold");
                } else {
                    auto [origMinIt, origMaxIt] =
                        std::minmax_element(originalScores.begin(), originalScores.end());
                    const double origMin = origMinIt != originalScores.end() ? *origMinIt : 0.0;
                    const double origMax = origMaxIt != originalScores.end() ? *origMaxIt : 0.0;
                    const double rerankRange = static_cast<double>(maxScore - minScore);
                    const double originalRange = origMax - origMin;
                    const double blend =
                        std::clamp(static_cast<double>(workingConfig.rerankBlendWeight), 0.0, 1.0);

                    std::vector<ComponentResult> rerankComponents;
                    rerankComponents.reserve(rerankWindow);
                    for (size_t i = 0; i < rerankWindow; ++i) {
                        const double rerankNorm = std::clamp(
                            (static_cast<double>(scores[i]) - minScore) / rerankRange, 0.0, 1.0);
                        const double originalNorm =
                            originalRange > 0.0
                                ? std::clamp((originalScores[i] - origMin) / originalRange, 0.0,
                                             1.0)
                                : 1.0;
                        const double finalScore =
                            workingConfig.rerankReplaceScores
                                ? rerankNorm
                                : blend * rerankNorm + (1.0 - blend) * originalNorm;

                        rerankWindowTrace.push_back({
                            {"doc_id", documentIdForTrace(response.results[i].document.filePath,
                                                          response.results[i].document.sha256Hash)},
                            {"original_score", originalScores[i]},
                            {"rerank_score", scores[i]},
                            {"rerank_norm", rerankNorm},
                            {"final_score", finalScore},
                        });
                        response.results[i].score = finalScore;

                        rerankComponents.push_back(ComponentResult{
                            .documentHash = response.results[i].document.sha256Hash,
                            .filePath = response.results[i].document.filePath,
                            .score = static_cast<float>(rerankNorm),
                            .source = ComponentResult::Source::Unknown,
                            .rank = i,
                            .snippet =
                                response.results[i].snippet.empty()
                                    ? std::nullopt
                                    : std::optional<std::string>(response.results[i].snippet),
                        });
                    }

                    std::stable_sort(response.results.begin(),
                                     response.results.begin() +
                                         static_cast<std::ptrdiff_t>(rerankWindow),
                                     [](const SearchResult& a, const SearchResult& b) {
                                         return a.score > b.score;
                                     });
                    crossRerankApplied = true;
                    contributing.push_back("cross_rerank");
                    response.debugStats["cross_rerank_skip_reason"] = "";
                    response.debugStats["cross_rerank_blend_weight"] = fmt::format("{:.3f}", blend);
                    response.debugStats["cross_rerank_replace_scores"] =
                        workingConfig.rerankReplaceScores ? "1" : "0";
                    response.debugStats["cross_rerank_score_gap"] =
                        fmt::format("{:.6f}", rerankGuardScoreGap);
                    traceCollector.markStageResult("cross_rerank", rerankComponents, duration,
                                                   true);
                }
            }
        }
    } else {
        response.debugStats["cross_rerank_configured"] = "0";
        response.debugStats["cross_rerank_available"] = crossReranker_ ? "1" : "0";
    }

    materializeConcepts(workingConfig.waitForConceptExtraction);

    const size_t compactRerankWindow = 0;
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
    response.debugStats["embedding_status"] = embeddingStatus;
    response.debugStats["vector_db_dim"] = std::to_string(vectorDbEmbeddingDim);
    response.debugStats["query_embedding_dim"] = std::to_string(queryEmbeddingDim);
    response.debugStats["vector_tier_skip_reason"] = vectorTierSkipReason;

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
        return r.keywordScore.value_or(0.0) + r.graphTextScore.value_or(0.0) +
               r.pathScore.value_or(0.0) + r.tagScore.value_or(0.0) + r.symbolScore.value_or(0.0);
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
        (intent == QueryIntent::Prose && workingConfig.rerankTopK > 0)
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
            if (signal == nullptr || signal->maxVectorRaw < 0.79) {
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

    const auto evidenceRescueScore = [&lexicalAnchorScore](const SearchResult& result) {
        const double lexical = lexicalAnchorScore(result);
        const double vector = result.vectorScore.value_or(0.0);
        const double kg = result.kgScore.value_or(0.0);
        const double bestSignal =
            std::max({lexical, vector, kg, result.pathScore.value_or(0.0),
                      result.symbolScore.value_or(0.0), result.tagScore.value_or(0.0)});
        const double componentBonus = (lexical > 0.0 && vector > 0.0) ? 0.01 : 0.0;
        return bestSignal + componentBonus;
    };

    const double finalEvidenceRescueMinScore =
        std::max(0.0, static_cast<double>(workingConfig.fusionEvidenceRescueMinScore));
    const auto isFinalEvidenceRescueCandidate = [finalEvidenceRescueMinScore,
                                                 &evidenceRescueScore](const SearchResult& result) {
        return evidenceRescueScore(result) >= finalEvidenceRescueMinScore;
    };

    const auto finalEvidenceRescueBetter =
        [&evidenceRescueScore, &docIdForResult](const SearchResult& a, const SearchResult& b) {
            const double evidenceA = evidenceRescueScore(a);
            const double evidenceB = evidenceRescueScore(b);
            if (evidenceA != evidenceB) {
                return evidenceA > evidenceB;
            }
            if (a.score != b.score) {
                return a.score > b.score;
            }
            return docIdForResult(a) < docIdForResult(b);
        };

    const auto finalLexicalAwareLess = [&lexicalAnchorScore, &docIdForResult, &workingConfig](
                                           const SearchResult& a, const SearchResult& b) {
        const double scoreDiff = static_cast<double>(a.score) - static_cast<double>(b.score);
        const double tieEpsilon =
            std::max(0.0, static_cast<double>(workingConfig.lexicalTieBreakEpsilon));

        if (!workingConfig.enableLexicalTieBreak || std::abs(scoreDiff) > tieEpsilon) {
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

        return docIdForResult(a) < docIdForResult(b);
    };

    std::vector<std::string> semanticRescuePromotedDocIds;
    std::vector<std::string> semanticRescueDisplacedDocIds;
    std::vector<std::string> buriedSemanticRescuePromotedDocIds;
    std::vector<std::string> buriedSemanticRescueDisplacedDocIds;
    std::vector<std::string> evidenceRescuePromotedDocIds;
    std::vector<std::string> evidenceRescueDisplacedDocIds;

    // Enforce user-visible limit after all post-fusion stages have had a chance to reorder/boost.
    if (response.results.size() > userLimit) {
        std::partial_sort(response.results.begin(),
                          response.results.begin() + static_cast<ptrdiff_t>(userLimit),
                          response.results.end(), finalLexicalAwareLess);

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

            std::sort(response.results.begin(),
                      response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                      finalLexicalAwareLess);

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
                std::sort(response.results.begin(),
                          response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                          finalLexicalAwareLess);
            }
        }

        if (workingConfig.fusionEvidenceRescueSlots > 0 && userLimit > 0) {
            const size_t finalWindow = std::min(userLimit, response.results.size());
            const size_t rescueTarget =
                std::min(workingConfig.fusionEvidenceRescueSlots, finalWindow);
            size_t rescuePresent = 0;
            for (size_t i = 0; i < finalWindow; ++i) {
                if (isFinalEvidenceRescueCandidate(response.results[i])) {
                    rescuePresent++;
                }
            }

            while (rescuePresent < rescueTarget) {
                size_t bestTailIndex = response.results.size();
                for (size_t i = finalWindow; i < response.results.size(); ++i) {
                    if (!isFinalEvidenceRescueCandidate(response.results[i])) {
                        continue;
                    }
                    if (bestTailIndex >= response.results.size() ||
                        finalEvidenceRescueBetter(response.results[i],
                                                  response.results[bestTailIndex])) {
                        bestTailIndex = i;
                    }
                }
                if (bestTailIndex >= response.results.size()) {
                    break;
                }

                size_t victimIndex = finalWindow;
                double victimEvidence = std::numeric_limits<double>::max();
                for (size_t i = 0; i < finalWindow; ++i) {
                    const double evidence = evidenceRescueScore(response.results[i]);
                    if (evidence < victimEvidence) {
                        victimEvidence = evidence;
                        victimIndex = i;
                    }
                }
                if (victimIndex >= finalWindow) {
                    break;
                }

                const double bestTailEvidence =
                    evidenceRescueScore(response.results[bestTailIndex]);
                if (bestTailEvidence <= victimEvidence) {
                    break;
                }

                const std::string promotedId = docIdForResult(response.results[bestTailIndex]);
                const std::string displacedId = docIdForResult(response.results[victimIndex]);
                if (!promotedId.empty()) {
                    evidenceRescuePromotedDocIds.push_back(promotedId);
                }
                if (!displacedId.empty()) {
                    evidenceRescueDisplacedDocIds.push_back(displacedId);
                }

                std::swap(response.results[victimIndex], response.results[bestTailIndex]);
                rescuePresent++;
            }

            std::sort(response.results.begin(),
                      response.results.begin() + static_cast<ptrdiff_t>(finalWindow),
                      finalLexicalAwareLess);
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
    response.debugStats["evidence_rescue_enabled"] =
        workingConfig.fusionEvidenceRescueSlots > 0 ? "1" : "0";
    response.debugStats["evidence_rescue_slots"] =
        std::to_string(workingConfig.fusionEvidenceRescueSlots);
    response.debugStats["evidence_rescue_min_score"] =
        fmt::format("{:.4f}", workingConfig.fusionEvidenceRescueMinScore);
    response.debugStats["evidence_rescue_promoted_doc_ids"] =
        joinWithTab(evidenceRescuePromotedDocIds);
    response.debugStats["evidence_rescue_displaced_doc_ids"] =
        joinWithTab(evidenceRescueDisplacedDocIds);
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
    response.debugStats["simeon_direct_raw_hit_count"] =
        std::to_string(textExpansionStats.simeonDirectRawHitCount);
    response.debugStats["simeon_direct_added_count"] =
        std::to_string(textExpansionStats.simeonDirectAddedCount);
    traceCollector.recordStageCounter(
        "text", "simeon_direct_raw_hit_count",
        static_cast<std::int64_t>(textExpansionStats.simeonDirectRawHitCount));
    traceCollector.recordStageCounter(
        "text", "simeon_direct_added_count",
        static_cast<std::int64_t>(textExpansionStats.simeonDirectAddedCount));
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
    response.debugStats["relaxed_vector_retry_enabled"] = relaxedVectorRetryEnabled ? "1" : "0";
    const bool relaxedRetryAttemptedFinal =
        relaxedVectorRetryAttempted.load(std::memory_order_relaxed);
    const bool relaxedRetryAppliedFinal = relaxedVectorRetryApplied.load(std::memory_order_relaxed);
    const int relaxedPrimaryHitFinal = relaxedVectorPrimaryHitCount.load(std::memory_order_relaxed);
    const int relaxedRetryThresholdMilliFinal =
        relaxedVectorRetryThresholdMilli.load(std::memory_order_relaxed);
    response.debugStats["relaxed_vector_retry_attempted"] = relaxedRetryAttemptedFinal ? "1" : "0";
    response.debugStats["relaxed_vector_retry_applied"] = relaxedRetryAppliedFinal ? "1" : "0";
    response.debugStats["relaxed_vector_primary_hit_count"] =
        std::to_string(relaxedPrimaryHitFinal);
    response.debugStats["relaxed_vector_retry_threshold"] =
        fmt::format("{:.3f}", static_cast<double>(relaxedRetryThresholdMilliFinal) / 1000.0);
    traceCollector.recordStageCounter("vector", "relaxed_retry_enabled",
                                      relaxedVectorRetryEnabled ? 1 : 0);
    traceCollector.recordStageCounter("vector", "relaxed_retry_attempted",
                                      relaxedRetryAttemptedFinal ? 1 : 0);
    traceCollector.recordStageCounter("vector", "relaxed_retry_applied",
                                      relaxedRetryAppliedFinal ? 1 : 0);
    traceCollector.recordStageCounter("vector", "relaxed_primary_hit_count",
                                      static_cast<std::int64_t>(relaxedPrimaryHitFinal));
    traceCollector.recordStageCounter("vector", "relaxed_retry_threshold_milli",
                                      static_cast<std::int64_t>(relaxedRetryThresholdMilliFinal));
    response.debugStats["graph_query_concept_count"] = std::to_string(graphQueryConceptCount);
    response.debugStats["graph_query_neighbor_seed_docs"] =
        std::to_string(graphQueryNeighborSeedDocCount);
    response.debugStats["graph_window_guard_replacement_count"] =
        std::to_string(graphWindowGuardReplacementCount);
    response.debugStats["graph_window_cap_replacement_count"] =
        std::to_string(graphWindowCapReplacementCount);
    response.debugStats["graph_rerank_guard_replacement_count"] =
        std::to_string(graphRerankGuardReplacementCount);
    response.debugStats["graph_matched_candidates"] = std::to_string(graphMatchedCandidates);
    response.debugStats["graph_positive_signal_candidates"] =
        std::to_string(graphPositiveSignalCandidates);
    response.debugStats["graph_boosted_docs"] = std::to_string(graphBoostedDocs);
    response.debugStats["graph_max_signal"] = fmt::format("{:.4f}", graphMaxSignal);
    response.debugStats.try_emplace("graph_community_supported_docs", "0");
    response.debugStats.try_emplace("graph_community_edge_count", "0");
    response.debugStats.try_emplace("graph_community_largest_size", "0");
    response.debugStats.try_emplace("graph_community_signal_mass", "0.0000");
    response.debugStats.try_emplace("graph_community_boosted_docs", "0");
    response.debugStats.try_emplace(
        "graph_community_weight",
        fmt::format("{:.4f}", std::clamp(workingConfig.graphCommunityWeight, 0.0f, 1.0f)));
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

    const std::vector<std::string> postFusionAllDocIds =
        collectRankedResultDocIds(postFusionSnapshot);
    const std::vector<std::string> fusionDroppedDocIds =
        setDifferenceIds(preFusionDocIds, postFusionAllDocIds);
    fusionDroppedDocCount = fusionDroppedDocIds.size();

    std::unordered_set<std::string> fusionDroppedSet;
    fusionDroppedSet.reserve(fusionDroppedDocIds.size());
    for (const auto& docId : fusionDroppedDocIds) {
        fusionDroppedSet.insert(docId);
    }

    const size_t lexicalFloorTopNForTelemetry =
        workingConfig.lexicalFloorTopN == 0 ? size_t{12} : workingConfig.lexicalFloorTopN;
    const bool nearMissReserveEnabled = workingConfig.vectorOnlyNearMissReserve > 0;
    const double nearMissSlack =
        std::clamp(static_cast<double>(workingConfig.vectorOnlyNearMissSlack), 0.0, 1.0);
    for (const auto& [docId, signal] : preFusionSignals) {
        if (signal.hasAnchoring) {
            anchoredPreFusionDocCount++;
            if (fusionDroppedSet.contains(docId)) {
                anchoredFusionDroppedDocCount++;
            }
        }

        if (signal.hasAnchoring) {
            if (signal.bestTextRank != std::numeric_limits<size_t>::max() &&
                signal.bestTextRank < lexicalFloorTopNForTelemetry) {
                topTextPreFusionDocCount++;
                if (fusionDroppedSet.contains(docId)) {
                    topTextFusionDroppedDocCount++;
                }
            }
        }

        if (signal.hasVector && !signal.hasAnchoring) {
            vectorOnlyDocs++;
            if (signal.maxVectorRaw < static_cast<double>(workingConfig.vectorOnlyThreshold)) {
                vectorOnlyBelowThreshold++;
                if (nearMissReserveEnabled &&
                    signal.maxVectorRaw + nearMissSlack >=
                        static_cast<double>(workingConfig.vectorOnlyThreshold)) {
                    vectorOnlyNearMissEligible++;
                }
            } else {
                vectorOnlyAboveThreshold++;
            }
        }
    }

    const auto rateString = [](size_t part, size_t total) {
        const double rate =
            total == 0 ? 0.0 : static_cast<double>(part) / static_cast<double>(total);
        return fmt::format("{:.6f}", rate);
    };
    response.debugStats["fusion_pre_fusion_unique_count"] = std::to_string(preFusionDocIds.size());
    response.debugStats["fusion_post_fusion_count"] = std::to_string(postFusionAllDocIds.size());
    response.debugStats["fusion_dropped_count"] = std::to_string(fusionDroppedDocCount);
    response.debugStats["fusion_dropped_rate"] =
        rateString(fusionDroppedDocCount, preFusionDocIds.size());
    response.debugStats["anchored_pre_fusion_count"] = std::to_string(anchoredPreFusionDocCount);
    response.debugStats["anchored_fusion_dropped_count"] =
        std::to_string(anchoredFusionDroppedDocCount);
    response.debugStats["anchored_fusion_dropped_rate"] =
        rateString(anchoredFusionDroppedDocCount, anchoredPreFusionDocCount);
    response.debugStats["top_text_pre_fusion_count"] = std::to_string(topTextPreFusionDocCount);
    response.debugStats["top_text_fusion_dropped_count"] =
        std::to_string(topTextFusionDroppedDocCount);
    response.debugStats["top_text_fusion_dropped_rate"] =
        rateString(topTextFusionDroppedDocCount, topTextPreFusionDocCount);
    response.debugStats["vector_only_docs"] = std::to_string(vectorOnlyDocs);
    response.debugStats["vector_only_below_threshold"] = std::to_string(vectorOnlyBelowThreshold);
    response.debugStats["vector_only_above_threshold"] = std::to_string(vectorOnlyAboveThreshold);
    response.debugStats["vector_only_near_miss_eligible"] =
        std::to_string(vectorOnlyNearMissEligible);
    response.debugStats["adaptive_fusion_enabled"] = workingConfig.enableAdaptiveFusion ? "1" : "0";
    response.debugStats["no_anchor_threshold_relaxed"] = noAnchorThresholdRelaxed ? "1" : "0";

    if (stageTraceEnabled) {
        const size_t traceTopDefault =
            std::max(userLimit, std::max(workingConfig.rerankTopK, workingConfig.graphRerankTopN));
        const size_t traceTopCount =
            envSizeTOrDefault("YAMS_SEARCH_STAGE_TRACE_TOP_N", traceTopDefault, 1, 10000);
        const size_t componentTopDefault = std::min<size_t>(traceTopCount, 25);
        const size_t componentTopCount =
            std::min(traceTopCount, envSizeTOrDefault("YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N",
                                                      componentTopDefault, 1, 10000));

        const std::vector<std::string> graphlessPostFusionAllDocIds = collectRankedResultDocIds(
            graphlessPostFusionSnapshot.empty() ? postFusionSnapshot : graphlessPostFusionSnapshot);
        const std::vector<std::string> postGraphAllDocIds = collectRankedResultDocIds(
            postGraphSnapshot.empty() ? postFusionSnapshot : postGraphSnapshot);
        const std::vector<std::string> finalAllDocIds = collectRankedResultDocIds(response.results);

        const std::vector<std::string> graphAddedPostFusionDocIds =
            setDifferenceIds(postFusionAllDocIds, graphlessPostFusionAllDocIds);
        const std::vector<std::string> graphDisplacedPostFusionDocIds =
            setDifferenceIds(graphlessPostFusionAllDocIds, postFusionAllDocIds);

        size_t strongVectorOnlyDocs = 0;
        size_t strongVectorOnlyScoreEligibleDocs = 0;
        size_t strongVectorOnlyRankEligibleDocs = 0;
        size_t anchorAndVectorDocs = 0;
        size_t anchorOnlyDocs = 0;
        std::vector<std::pair<std::string, double>> vectorOnlyBelowDocs;
        std::vector<std::pair<std::string, double>> vectorOnlyAboveDocs;

        for (const auto& [docId, signal] : preFusionSignals) {
            if (signal.hasAnchoring && signal.hasVector) {
                anchorAndVectorDocs++;
            } else if (signal.hasAnchoring && !signal.hasVector) {
                anchorOnlyDocs++;
            }

            if (signal.hasVector && !signal.hasAnchoring) {
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
                    vectorOnlyBelowDocs.emplace_back(docId, signal.maxVectorRaw);
                } else {
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
            {"weak_query_min_text_hits", workingConfig.weakQueryMinTextHits},
            {"weak_query_min_top_text_score", workingConfig.weakQueryMinTopTextScore},
            {"vector_only_below_top_doc_ids", vectorOnlyBelowTop},
            {"vector_only_above_top_doc_ids", vectorOnlyAboveTop},
        };

        response.debugStats["trace_enabled"] = "1";
        response.debugStats["trace_query_intent"] = queryIntentToString(intent);
        response.debugStats["trace_query_intent_reason"] = routeDecision.intent.reason;
        response.debugStats["trace_retrieval_mode"] =
            queryRetrievalModeToString(routeDecision.retrievalMode.label);
        response.debugStats["trace_retrieval_mode_reason"] = routeDecision.retrievalMode.reason;
        response.debugStats["trace_query_community"] =
            routeDecision.community.has_value()
                ? queryCommunityToString(routeDecision.community->label)
                : "none";
        response.debugStats["trace_query_community_reason"] =
            routeDecision.community.has_value() ? routeDecision.community->reason : "";
        response.debugStats["trace_zoom_level"] =
            SearchEngineConfig::navigationZoomLevelToString(effectiveZoomLevel);
        response.debugStats["trace_zoom_source"] =
            zoomLevelInferredFromIntent ? "intent_auto" : "configured";
        response.debugStats["trace_user_limit"] = std::to_string(userLimit);
        response.debugStats["trace_fusion_candidate_limit"] = std::to_string(fusionCandidateLimit);
        response.debugStats["trace_top_window"] = std::to_string(traceTopCount);
        response.debugStats["trace_top_window_default"] = std::to_string(traceTopDefault);
        response.debugStats["trace_component_top_window"] = std::to_string(componentTopCount);
        response.debugStats["trace_component_top_window_default"] =
            std::to_string(componentTopDefault);
        response.debugStats["trace_graph_rerank_applied"] = graphRerankApplied ? "1" : "0";
        response.debugStats["trace_cross_rerank_applied"] = crossRerankApplied ? "1" : "0";
        response.debugStats["trace_turboquant_rerank_applied"] = "0";
        response.debugStats["trace_rerank_guard_score_gap"] =
            fmt::format("{:.6f}", rerankGuardScoreGap);
        response.debugStats["trace_rerank_guard_has_competitive_anchored_evidence"] =
            rerankGuardCompetitiveAnchoredEvidence ? "1" : "0";
        response.debugStats["trace_rerank_guard_anchored_doc_ids"] =
            joinWithTab(rerankGuardAnchoredDocIds);

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
        telemetry.preFusionUniqueDocCount = preFusionDocIds.size();
        telemetry.postFusionDocCount = postFusionAllDocIds.size();
        telemetry.fusionDroppedDocCount = fusionDroppedDocCount;
        telemetry.anchoredPreFusionDocCount = anchoredPreFusionDocCount;
        telemetry.anchoredFusionDroppedDocCount = anchoredFusionDroppedDocCount;
        telemetry.topTextPreFusionDocCount = topTextPreFusionDocCount;
        telemetry.topTextFusionDroppedDocCount = topTextFusionDroppedDocCount;
        telemetry.vectorOnlyDocCount = vectorOnlyDocs;
        telemetry.vectorOnlyBelowThresholdCount = vectorOnlyBelowThreshold;
        telemetry.vectorOnlyAboveThresholdCount = vectorOnlyAboveThreshold;
        telemetry.vectorOnlyNearMissEligibleCount = vectorOnlyNearMissEligible;
        telemetry.semanticRescueTarget = semanticRescueTarget;
        telemetry.semanticRescueFinalCount = semanticRescueFinalCount;
        telemetry.adaptiveFusionEnabled = workingConfig.enableAdaptiveFusion;
        telemetry.zoomLevel = effectiveZoomLevel;

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
                    signal.scoreStatsValid = data.value("score_stats_valid", false);
                    signal.minScore = data.value("min_score", 0.0);
                    signal.maxScore = data.value("max_score", 0.0);
                    telemetry.stages.emplace(name, signal);
                }
            }
        } catch (...) { // NOLINT(bugprone-empty-catch) — best-effort telemetry; skip stage summary
                        // if trace data is malformed
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
        } catch (...) { // NOLINT(bugprone-empty-catch) — best-effort telemetry; skip fusion summary
                        // if trace data is malformed
        }

        tuner_->observe(tuningCtx, telemetry);
        const auto tunerState = tuner_->adaptiveStateToJson();
        response.debugStats["tuner_adaptive_active"] = "1";
        response.debugStats["tuner_decision_reason"] =
            tunerState.value("last_decision", std::string{"unknown"});
        response.debugStats["tuner_adjustments_json"] = tunerState.dump();
        response.debugStats["tuner_runtime_config_json"] = tuner_->getParams().toJson().dump();
        // R3: emit the stable bucket key so users can see which context
        // bucket drove a given query's policy decision. The rules policy
        // ignores the bucket; R5's orchestrator reads it for per-bucket
        // handoff gating.
        response.debugStats["tuner_context_bucket"] = bucketize(tuningCtx);
        response.debugStats["tuner_backend"] = "rules";
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
    if (!metadataRepo_)
        return results;

    const auto pathSeeds = buildPathSeedsFromQuery(query);
    std::unordered_map<std::string, ComponentResult> byHash;
    byHash.reserve(limit * 2);

    try {
        for (const auto& seed : pathSeeds) {
            if (byHash.size() >= limit * 2)
                break;

            yams::metadata::DocumentQueryOptions options;
            options.containsFragment = seed.text;
            options.containsUsesFts = true;
            options.limit = static_cast<int>(std::max<std::size_t>(8, limit / 2));
            auto docResults = metadataRepo_->queryDocuments(options);
            if (!docResults)
                continue;

            const std::string lowerSeed = lowerCopy(seed.text);
            for (std::size_t rank = 0; rank < docResults.value().size(); ++rank) {
                const auto& doc = docResults.value()[rank];
                if (doc.sha256Hash.empty() && doc.filePath.empty())
                    continue;

                ComponentResult item;
                item.documentHash = doc.sha256Hash;
                item.filePath = doc.filePath;
                item.source = ComponentResult::Source::PathTree;
                item.rank = rank;
                item.snippet = doc.filePath;

                const std::string lowerPath = lowerCopy(doc.filePath);
                const auto seedPos = lowerPath.find(lowerSeed);
                float score = 0.48f * seed.weight;
                if (seedPos != std::string::npos && !lowerPath.empty()) {
                    const float position =
                        1.0f - (static_cast<float>(seedPos) / static_cast<float>(lowerPath.size()));
                    const float coverage =
                        static_cast<float>(std::min(lowerSeed.size(), lowerPath.size())) /
                        static_cast<float>(std::max<std::size_t>(1, lowerPath.size()));
                    score = std::clamp(seed.weight * (0.56f + position * 0.18f + coverage * 0.26f),
                                       0.0f, 1.0f);
                }
                item.score = score;
                item.debugInfo["seed"] = seed.text;
                item.debugInfo["seed_kind"] = seed.kind;
                item.debugInfo["path"] = doc.filePath;
                item.debugInfo["path_depth"] = std::to_string(doc.pathDepth);

                const std::string& key = doc.sha256Hash.empty() ? doc.filePath : doc.sha256Hash;
                auto it = byHash.find(key);
                if (it == byHash.end() || item.score > it->second.score) {
                    byHash[key] = std::move(item);
                }
            }
        }

        results.reserve(std::min(limit, byHash.size()));
        for (auto& [_, item] : byHash) {
            results.push_back(std::move(item));
        }
        std::sort(results.begin(), results.end(),
                  [](const ComponentResult& a, const ComponentResult& b) {
                      if (a.score != b.score)
                          return a.score > b.score;
                      return a.rank < b.rank;
                  });
        if (results.size() > limit)
            results.resize(limit);
        for (std::size_t i = 0; i < results.size(); ++i)
            results[i].rank = i;

        spdlog::debug("Path tree query returned {} results for query: {}", results.size(), query);
    } catch (const std::exception& e) {
        spdlog::warn("Path tree query exception: {}", e.what());
    }

    return results;
}

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryFullText(
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, QueryExpansionStats* expansionStats,
    const std::vector<GraphExpansionTerm>* graphExpansionTerms, std::string* simeonRouteRecipe) {
    try {
        return detail::queryFullTextPipeline(
            metadataRepo_, query, queryIntent, config, limit, expansionStats,
            [this](const std::string& phraseQuery, size_t maxSubPhrases) {
                return detail::generateQuerySubPhrases(metadataRepo_, phraseQuery, maxSubPhrases);
            },
            [this](const std::string& clauseQuery, size_t maxClauses) {
                return generateAggressiveFtsFallbackClauses(
                    clauseQuery, maxClauses,
                    detail::lookupQueryTermIdf(metadataRepo_, clauseQuery));
            },
            [this, graphExpansionTerms, &query, &config]() {
                if (graphExpansionTerms != nullptr) {
                    return *graphExpansionTerms;
                }
                return generateGraphExpansionTerms(
                    kgStore_, query, {},
                    GraphExpansionConfig{.maxTerms = config.graphExpansionMaxTerms,
                                         .maxSeeds = config.graphExpansionMaxSeeds,
                                         .maxNeighbors = config.graphMaxNeighbors});
            },
            simeonLexical_.get(), simeonRouteRecipe);
    } catch (const std::exception& e) {
        spdlog::warn("Full-text query exception: {}", e.what());
        return std::vector<ComponentResult>{};
    }
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryKnowledgeGraph(const std::string& query, size_t limit,
                                        const std::vector<QueryConcept>* concepts) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!kgStore_) {
        return results;
    }

    try {
        std::vector<std::string> queryTerms;
        std::unordered_set<std::string> seenTerms;
        if (concepts != nullptr) {
            for (const auto& queryConcept : *concepts) {
                std::string normalized = normalizeEntityTextForKey(queryConcept.text);
                if (normalized.size() < 3 || !seenTerms.insert(normalized).second) {
                    continue;
                }
                queryTerms.push_back(queryConcept.text);
            }
        }

        // Fall back to token-level alias lookup only when the query was not grounded
        // to any explicit concepts.
        if (queryTerms.empty()) {
            std::vector<std::string> queryTokens = tokenizeKgQuery(query);
            for (const auto& token : queryTokens) {
                if (!seenTerms.insert(token).second) {
                    continue;
                }
                queryTerms.push_back(token);
            }
        }

        if (queryTerms.empty()) {
            return results;
        }

        // Collect aliases from all tokens with score tracking
        std::vector<metadata::AliasResolution> aliases;
        std::unordered_map<int64_t, float> nodeIdToScore; // Track best score per node

        // Limit aliases per token to avoid explosion
        const size_t aliasesPerToken = std::max(size_t(3), limit / queryTerms.size());

        for (const auto& tok : queryTerms) {
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

        spdlog::debug("KG: {} terms -> {} unique aliases", queryTerms.size(), aliases.size());
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
        auto docResults = metadataRepo_->search(batchQuery, static_cast<int>(batchLimit), 0);
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
                const bool likelyMatch =
                    (!searchResult.snippet.empty() &&
                     ci_find(searchResult.snippet, term) != std::string::npos) ||
                    ci_find(searchResult.document.filePath, term) != std::string::npos;

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
    return detail::queryVectorIndexPipeline(metadataRepo_, vectorDb_, embedding, config, limit);
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryVectorIndex(const std::vector<float>& embedding,
                                     const SearchEngineConfig& config, size_t limit,
                                     const std::unordered_set<std::string>& candidates) {
    return detail::queryVectorIndexPipeline(metadataRepo_, vectorDb_, embedding, config, limit,
                                            candidates);
}

Result<std::vector<ComponentResult>>
SearchEngine::Impl::queryEntityVectors(const std::vector<float>& embedding,
                                       const SearchEngineConfig& config, size_t limit) {
    return detail::queryEntityVectorsPipeline(metadataRepo_, vectorDb_, embedding, config, limit);
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

Result<std::vector<ComponentResult>> SearchEngine::Impl::queryMetadata(const std::string& query,
                                                                       const SearchParams& params,
                                                                       size_t limit) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo_)
        return results;

    const auto queryFilters = parseMetadataFiltersFromQuery(query);
    bool hasStructFilters = params.mimeType.has_value() || params.extension.has_value() ||
                            params.modifiedAfter.has_value() || params.modifiedBefore.has_value();

    if (!hasStructFilters && queryFilters.empty())
        return results;

    try {
        yams::metadata::DocumentQueryOptions options;
        options.mimeType = params.mimeType;
        options.extension = params.extension;
        options.modifiedAfter = params.modifiedAfter;
        options.modifiedBefore = params.modifiedBefore;
        options.limit = static_cast<int>(limit);

        for (const auto& kv : queryFilters) {
            options.metadataFilters.push_back(kv);
        }

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

            int filterCount = 0;
            if (params.mimeType.has_value())
                filterCount++;
            if (params.extension.has_value())
                filterCount++;
            if (params.modifiedAfter.has_value())
                filterCount++;
            if (params.modifiedBefore.has_value())
                filterCount++;
            filterCount += static_cast<int>(queryFilters.size());

            result.score = 1.0f;
            result.source = ComponentResult::Source::Metadata;
            result.rank = rank;
            result.debugInfo["filter_count"] = std::to_string(filterCount);
            if (params.mimeType.has_value())
                result.debugInfo["mime_type"] = params.mimeType.value();
            if (params.extension.has_value())
                result.debugInfo["extension"] = params.extension.value();
            for (const auto& kv : queryFilters) {
                result.debugInfo["meta_" + kv.first] = kv.second;
            }

            results.push_back(std::move(result));
        }

        spdlog::debug("Metadata query returned {} results", results.size());
    } catch (const std::exception& e) {
        spdlog::warn("Metadata query exception: {}", e.what());
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

void SearchEngine::setSimeonLexicalBackend(std::unique_ptr<SimeonLexicalBackend> backend) {
    pImpl_->setSimeonLexicalBackend(std::move(backend));
}

void SearchEngine::setCrossReranker(CrossRerankScorer scorer) {
    pImpl_->setCrossReranker(std::move(scorer));
}

void SearchEngine::setSearchTuner(std::shared_ptr<SearchTuner> tuner) {
    pImpl_->setSearchTuner(std::move(tuner));
}

std::shared_ptr<SearchTuner> SearchEngine::getSearchTuner() const {
    return pImpl_->getSearchTuner();
}

SearchEngine::SimeonLexicalStatus SearchEngine::getSimeonLexicalStatus() const {
    return pImpl_->getSimeonLexicalStatus();
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
