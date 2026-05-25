#include "search_lexical_pipeline_internal.h"

#include <spdlog/spdlog.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/lexical_scoring.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_text_utils.h>
#include <yams/search/search_tracing.h>
#include <yams/search/simeon_lexical_backend.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <optional>

namespace yams::search::detail {

void appendLexicalBatchAtRank(const std::string& query, QueryIntent queryIntent,
                              const SearchEngineConfig& config,
                              const yams::metadata::SearchResults& searchResults,
                              float scorePenalty, bool dedupe,
                              std::unordered_set<std::string>* seenHashes,
                              ComponentResult::Source source, std::vector<ComponentResult>& results,
                              QueryExpansionStats* expansionStats, size_t startRank);

void appendLexicalBatch(const std::string& query, QueryIntent queryIntent,
                        const SearchEngineConfig& config,
                        const yams::metadata::SearchResults& searchResults, float scorePenalty,
                        bool dedupe, std::unordered_set<std::string>* seenHashes,
                        ComponentResult::Source source, std::vector<ComponentResult>& results,
                        QueryExpansionStats* expansionStats) {
    appendLexicalBatchAtRank(query, queryIntent, config, searchResults, scorePenalty, dedupe,
                             seenHashes, source, results, expansionStats, results.size());
}

void appendLexicalBatchAtRank(const std::string& query, QueryIntent queryIntent,
                              const SearchEngineConfig& config,
                              const yams::metadata::SearchResults& searchResults,
                              float scorePenalty, bool dedupe,
                              std::unordered_set<std::string>* seenHashes,
                              ComponentResult::Source source, std::vector<ComponentResult>& results,
                              QueryExpansionStats* expansionStats, size_t startRank) {
    detail::LexicalScoringInputs inputs;
    inputs.candidates = std::span<const yams::metadata::SearchResult>(searchResults.results.data(),
                                                                      searchResults.results.size());
    inputs.query = query;
    inputs.intent = queryIntent;
    inputs.applyIntentAdaptiveWeighting = config.enableIntentAdaptiveWeighting;
    inputs.applyFilenameBoost = true;
    inputs.bm25NormDivisor = config.bm25NormDivisor;
    inputs.penalty = scorePenalty;
    inputs.sourceTag = source;
    inputs.startRank = startRank;
    if (source == ComponentResult::Source::GraphText) {
        inputs.admissionMinScore = config.graphTextMinAdmissionScore;
    }

    auto scored = detail::scoreLexicalBatch(inputs);
    if (expansionStats != nullptr && scored.admissionDroppedCount > 0) {
        expansionStats->graphTextBlockedLowScoreCount += scored.admissionDroppedCount;
    }

    for (auto& opt : scored.results) {
        if (!opt) {
            continue;
        }
        if (dedupe && seenHashes != nullptr && seenHashes->contains(opt->documentHash)) {
            continue;
        }
        if (seenHashes != nullptr) {
            seenHashes->insert(opt->documentHash);
        }
        results.push_back(std::move(*opt));
    }
}

std::optional<yams::metadata::SearchResults>
makeSimeonLexicalResults(const std::string& query, SimeonLexicalBackend* backend,
                         const yams::metadata::SearchResults& searchResults,
                         std::string* lastSimeonRouteRecipe, const SearchEngineConfig& config) {
    if (!backend || !backend->ready() || searchResults.results.empty()) {
        return std::nullopt;
    }

    std::vector<std::int64_t> ids;
    ids.reserve(searchResults.results.size());
    for (const auto& r : searchResults.results) {
        ids.push_back(r.document.id);
    }

    auto decision = [&]() -> Result<SimeonLexicalBackend::RescoreDecision> {
        if (!config.simeonBanditArm.empty()) {
            return backend->scoreBanditRouted(query, config.simeonBanditArm, ids);
        }
        if (backend->hasStrategyRouter()) {
            return backend->scoreStrategyRouted(query, ids);
        }
        return backend->scoreRouted(query, ids);
    }();
    if (!decision) {
        spdlog::debug("[simeon-lexical] score failed, keeping FTS5 scores: {}",
                      decision.error().message);
        return std::nullopt;
    }

    const auto& values = decision.value().scores;
    if (values.size() != searchResults.results.size()) {
        spdlog::warn("[simeon-lexical] score size mismatch ({} vs {}); keeping FTS5 scores",
                     values.size(), searchResults.results.size());
        return std::nullopt;
    }
    if (lastSimeonRouteRecipe != nullptr) {
        *lastSimeonRouteRecipe = decision.value().recipe_name;
    }
    auto rescored = searchResults;
    for (size_t i = 0; i < values.size(); ++i) {
        rescored.results[i].score = -static_cast<double>(values[i]);
    }
    std::sort(rescored.results.begin(), rescored.results.end(),
              [](const yams::metadata::SearchResult& a, const yams::metadata::SearchResult& b) {
                  return a.score < b.score;
              });
    return rescored;
}

void appendSimeonLexicalBatch(const std::string& query, SimeonLexicalBackend* backend,
                              std::string* lastSimeonRouteRecipe, QueryIntent queryIntent,
                              const SearchEngineConfig& config,
                              const yams::metadata::SearchResults& searchResults,
                              float scorePenalty, std::unordered_set<std::string>& simeonSeenHashes,
                              std::vector<ComponentResult>& results,
                              QueryExpansionStats* expansionStats) {
    auto simeonResults =
        makeSimeonLexicalResults(query, backend, searchResults, lastSimeonRouteRecipe, config);
    if (!simeonResults) {
        return;
    }
    const size_t simeonRankBase = simeonSeenHashes.size();
    appendLexicalBatchAtRank(query, queryIntent, config, *simeonResults, scorePenalty, true,
                             &simeonSeenHashes, ComponentResult::Source::SimeonText, results,
                             expansionStats, simeonRankBase);
}

std::optional<yams::metadata::SearchResults> makeDirectSimeonLexicalResults(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe,
    const SearchEngineConfig& config, size_t limit, QueryExpansionStats* expansionStats) {
    if (!metadataRepo || !backend || !backend->ready() || limit == 0) {
        return std::nullopt;
    }

    auto decision = backend->searchTop(query, limit, config.simeonBanditArm);
    if (!decision) {
        spdlog::debug("[simeon-lexical] direct candidate search failed: {}",
                      decision.error().message);
        return std::nullopt;
    }
    if (lastSimeonRouteRecipe != nullptr) {
        *lastSimeonRouteRecipe = decision.value().recipe_name;
    }
    if (expansionStats != nullptr) {
        expansionStats->simeonDirectRawHitCount += decision.value().candidates.size();
    }

    yams::metadata::SearchResults direct;
    direct.query = query;
    direct.results.reserve(decision.value().candidates.size());
    for (const auto& candidate : decision.value().candidates) {
        auto docResult = metadataRepo->getDocument(candidate.document_id);
        if (!docResult || !docResult.value()) {
            continue;
        }

        yams::metadata::SearchResult row;
        row.document = *docResult.value();
        row.score = -static_cast<double>(candidate.score);

        auto contentResult = metadataRepo->getContent(candidate.document_id);
        if (contentResult && contentResult.value()) {
            const auto& text = contentResult.value()->contentText;
            constexpr size_t kSnippetChars = 240;
            row.snippet = text.substr(0, std::min(kSnippetChars, text.size()));
        }
        direct.results.push_back(std::move(row));
    }

    std::sort(direct.results.begin(), direct.results.end(),
              [](const yams::metadata::SearchResult& a, const yams::metadata::SearchResult& b) {
                  return a.score < b.score;
              });
    direct.totalCount = static_cast<int64_t>(direct.results.size());
    return direct;
}

void appendDirectSimeonLexicalBatch(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe,
    QueryIntent queryIntent, const SearchEngineConfig& config, size_t limit,
    std::unordered_set<std::string>& seenHashes, std::vector<ComponentResult>& results,
    QueryExpansionStats* expansionStats) {
    auto directResults = makeDirectSimeonLexicalResults(
        metadataRepo, query, backend, lastSimeonRouteRecipe, config, limit, expansionStats);
    if (!directResults || directResults->results.empty()) {
        return;
    }

    const size_t beforeAppend = results.size();
    appendLexicalBatchAtRank(query, queryIntent, config, *directResults, 1.0f, true, &seenHashes,
                             ComponentResult::Source::SimeonText, results, expansionStats,
                             results.size());
    if (expansionStats != nullptr) {
        expansionStats->simeonDirectAddedCount +=
            results.size() > beforeAppend ? results.size() - beforeAppend : 0;
    }
}

Fts5CandidatePoolStats fetchFts5CandidatePool(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe, const std::string& query,
    QueryIntent queryIntent, const SearchEngineConfig& config, size_t limit,
    QueryExpansionStats* expansionStats, std::vector<ComponentResult>& results,
    std::unordered_set<std::string>& seenHashes,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator) {
    Fts5CandidatePoolStats stats;
    std::unordered_set<std::string> simeonSeenHashes;
    simeonSeenHashes.reserve(limit * 2);

    auto fts5Results = metadataRepo->search(query, static_cast<int>(limit), 0);
    stats.baseFtsSucceeded = static_cast<bool>(fts5Results);
    if (!stats.baseFtsSucceeded) {
        spdlog::debug("FTS5 search failed, continuing with fallback expansion: {}",
                      fts5Results.error().message);
    }

    if (stats.baseFtsSucceeded) {
        stats.baseFtsHitCount = fts5Results.value().results.size();
        appendLexicalBatch(query, queryIntent, config, fts5Results.value(), 1.0f, true, &seenHashes,
                           ComponentResult::Source::Text, results, expansionStats);
        appendSimeonLexicalBatch(query, backend, lastSimeonRouteRecipe, queryIntent, config,
                                 fts5Results.value(), 1.0f, simeonSeenHashes, results,
                                 expansionStats);
    }

    if (config.enableLexicalExpansion && stats.baseFtsHitCount < config.lexicalExpansionMinHits) {
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

            auto expandedResults = metadataRepo->search(expandedQuery, static_cast<int>(limit), 0);
            if (expandedResults) {
                const float penalty = std::clamp(config.lexicalExpansionScorePenalty, 0.1f, 1.0f);
                appendLexicalBatch(query, queryIntent, config, expandedResults.value(), penalty,
                                   true, &seenHashes, ComponentResult::Source::Text, results,
                                   expansionStats);
                appendSimeonLexicalBatch(query, backend, lastSimeonRouteRecipe, queryIntent, config,
                                         expandedResults.value(), penalty, simeonSeenHashes,
                                         results, expansionStats);
                spdlog::debug("queryFullText lexical expansion: base_hits={} expanded_hits={} "
                              "query='{}' expanded='{}'",
                              stats.baseFtsHitCount, results.size(), query, expandedQuery);
            }
        }
    }

    if (config.enableSubPhraseExpansion &&
        stats.baseFtsHitCount < config.subPhraseExpansionMinHits) {
        const size_t maxSubPhrases = std::max<size_t>(config.multiVectorMaxPhrases, 3);
        auto subPhrases = subPhraseGenerator(query, maxSubPhrases);
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

            auto expandedResults = metadataRepo->search(expandedQuery, static_cast<int>(limit), 0);
            if (expandedResults) {
                if (expansionStats != nullptr) {
                    expansionStats->subPhraseFtsHitCount = expandedResults.value().results.size();
                }
                const float penalty = std::clamp(config.subPhraseExpansionPenalty, 0.1f, 1.0f);
                const size_t beforeAppend = results.size();
                appendLexicalBatch(query, queryIntent, config, expandedResults.value(), penalty,
                                   true, &seenHashes, ComponentResult::Source::Text, results,
                                   expansionStats);
                appendSimeonLexicalBatch(query, backend, lastSimeonRouteRecipe, queryIntent, config,
                                         expandedResults.value(), penalty, simeonSeenHashes,
                                         results, expansionStats);
                if (expansionStats != nullptr) {
                    expansionStats->subPhraseFtsAddedCount =
                        results.size() > beforeAppend ? (results.size() - beforeAppend) : 0;
                }
                spdlog::debug("queryFullText sub-phrase expansion: base_hits={} "
                              "expanded_hits={} query='{}' expanded='{}'",
                              stats.baseFtsHitCount, results.size(), query, expandedQuery);
            }
        }
    }

    return stats;
}

std::unordered_map<std::string, float>
lookupQueryTermIdf(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                   const std::string& query) {
    std::unordered_map<std::string, float> idfByToken;
    if (!metadataRepo) {
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

    auto idfResult = metadataRepo->getTermIDFBatch(uniqueTerms);
    if (idfResult) {
        idfByToken = std::move(idfResult.value());
    } else {
        spdlog::debug("lookupQueryTermIdf: IDF batch lookup failed: {}", idfResult.error().message);
    }
    return idfByToken;
}

std::vector<std::string>
generateQuerySubPhrases(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                        const std::string& query, size_t maxPhrases) {
    if (maxPhrases == 0) {
        return {};
    }

    auto idfByToken = lookupQueryTermIdf(metadataRepo, query);
    return generateAnchoredSubPhrases(query, maxPhrases,
                                      idfByToken.empty() ? nullptr : &idfByToken);
}

void applyAggressiveFtsFallback(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, bool baseFtsSucceeded, size_t baseFtsHitCount,
    const std::vector<FtsFallbackClause>& aggressiveClauses, QueryExpansionStats* expansionStats,
    std::unordered_set<std::string>& seenHashes, std::vector<ComponentResult>& results) {
    if (expansionStats != nullptr) {
        expansionStats->aggressiveClauseCount = aggressiveClauses.size();
    }

    size_t totalAggressiveHits = 0;
    const size_t beforeAggressive = results.size();
    for (const auto& clause : aggressiveClauses) {
        auto clauseResults = metadataRepo->search(clause.query, static_cast<int>(limit), 0);
        if (!clauseResults) {
            continue;
        }
        totalAggressiveHits += clauseResults.value().results.size();
        appendLexicalBatch(query, queryIntent, config, clauseResults.value(), clause.penalty, true,
                           &seenHashes, ComponentResult::Source::Text, results, expansionStats);
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
                      results.size(), query, debugClauses.size());
    }
}

void applyGraphExpansionTerms(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, size_t baseFtsHitCount, const std::vector<GraphExpansionTerm>& graphTerms,
    QueryExpansionStats* expansionStats, std::unordered_set<std::string>& seenHashes,
    std::vector<ComponentResult>& results) {
    if (expansionStats != nullptr) {
        expansionStats->graphExpansionTermCount = graphTerms.size();
    }

    size_t totalGraphHits = 0;
    const size_t beforeGraph = results.size();
    for (const auto& term : graphTerms) {
        auto graphResults = metadataRepo->search(term.text, static_cast<int>(limit), 0);
        if (!graphResults) {
            continue;
        }
        totalGraphHits += graphResults.value().results.size();
        const float penalty = std::clamp(
            config.graphExpansionFtsPenalty * std::clamp(term.score, 0.2f, 1.0f), 0.1f, 1.0f);
        appendLexicalBatch(query, queryIntent, config, graphResults.value(), penalty, true,
                           &seenHashes, ComponentResult::Source::GraphText, results,
                           expansionStats);
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
                      baseFtsHitCount, graphTerms.size(), totalGraphHits, results.size(), query,
                      debugTerms.size());
    }
}

void applySubPhraseRescoring(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, const SearchEngineConfig& config, size_t limit,
    std::vector<ComponentResult>& results,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator) {
    if (results.empty()) {
        return;
    }

    std::unordered_map<std::string, size_t> rescoreIndex;
    rescoreIndex.reserve(results.size());
    for (size_t i = 0; i < results.size(); ++i) {
        rescoreIndex.emplace(results[i].documentHash, i);
    }

    const size_t maxPhrases = std::max<size_t>(config.multiVectorMaxPhrases, 4);
    auto subPhrases = subPhraseGenerator(query, maxPhrases);
    const float rescorePenalty = std::clamp(config.subPhraseScoringPenalty, 0.1f, 1.0f);

    for (const auto& phrase : subPhrases) {
        auto phraseTokens = tokenizeQueryTokens(phrase);
        if (phraseTokens.size() < 2) {
            continue;
        }

        std::string clause = "(";
        for (size_t i = 0; i < phraseTokens.size(); ++i) {
            if (i > 0) {
                clause += " AND ";
            }
            clause += phraseTokens[i].normalized;
        }
        clause += ")";

        auto phraseResults = metadataRepo->search(clause, static_cast<int>(limit), 0);
        if (!phraseResults || phraseResults.value().results.empty()) {
            continue;
        }

        const double phMax = phraseResults.value().results.front().score;
        const double phMin = phraseResults.value().results.back().score;

        for (const auto& pr : phraseResults.value().results) {
            auto it = rescoreIndex.find(pr.document.sha256Hash);
            if (it == rescoreIndex.end()) {
                continue;
            }
            float phraseNorm = detail::normalizedBm25Score(static_cast<float>(pr.score),
                                                           config.bm25NormDivisor, phMin, phMax);
            float boost = std::clamp(phraseNorm * rescorePenalty, 0.0f, 1.0f);
            results[it->second].score = std::max(results[it->second].score, boost);
        }
    }
}

std::vector<ComponentResult> queryFullTextPipeline(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, QueryExpansionStats* expansionStats,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator,
    const std::function<std::vector<FtsFallbackClause>(const std::string&, size_t)>&
        aggressiveClauseGenerator,
    const std::function<std::vector<GraphExpansionTerm>()>& graphTermProvider,
    SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe) {
    std::vector<ComponentResult> results;
    results.reserve(limit);

    if (!metadataRepo) {
        return results;
    }

    std::unordered_set<std::string> seenHashes;
    seenHashes.reserve(limit * 2);

    const auto poolStats = fetchFts5CandidatePool(metadataRepo, backend, lastSimeonRouteRecipe,
                                                  query, queryIntent, config, limit, expansionStats,
                                                  results, seenHashes, subPhraseGenerator);
    const size_t baseFtsHitCount = poolStats.baseFtsHitCount;
    const bool baseFtsSucceeded = poolStats.baseFtsSucceeded;

    const bool weakLexicalPool = baseFtsHitCount < config.weakQueryMinTextHits ||
                                 results.size() < config.weakQueryMinTextHits;
    if (weakLexicalPool) {
        appendDirectSimeonLexicalBatch(metadataRepo, query, backend, lastSimeonRouteRecipe,
                                       queryIntent, config, limit, seenHashes, results,
                                       expansionStats);
    }

    if (config.enableSubPhraseRescoring && !results.empty()) {
        applySubPhraseRescoring(metadataRepo, query, config, limit, results, subPhraseGenerator);
    }

    if (!baseFtsSucceeded || baseFtsHitCount == 0 || results.size() < 3) {
        const size_t maxAggressiveClauses = (!baseFtsSucceeded || baseFtsHitCount == 0) ? 10 : 6;
        auto aggressiveClauses = aggressiveClauseGenerator(query, maxAggressiveClauses);
        applyAggressiveFtsFallback(metadataRepo, query, queryIntent, config, limit,
                                   baseFtsSucceeded, baseFtsHitCount, aggressiveClauses,
                                   expansionStats, seenHashes, results);
    }

    if (config.enableGraphQueryExpansion && baseFtsHitCount < config.graphExpansionMinHits) {
        auto graphTerms = graphTermProvider();
        applyGraphExpansionTerms(metadataRepo, query, queryIntent, config, limit, baseFtsHitCount,
                                 graphTerms, expansionStats, seenHashes, results);
    }

    spdlog::debug("queryFullText: {} results for query '{}' (limit={})", results.size(),
                  query.substr(0, 50), limit);

    // NOLINTNEXTLINE(concurrency-mt-unsafe) — debug env var; not modified concurrently
    if (const char* env = std::getenv("YAMS_SEARCH_DIAG"); env && std::string(env) == "1") {
        spdlog::warn("[search_diag] text_hits={} limit={} query='{}'", results.size(), limit,
                     query);
    }

    return results;
}

GraphDocumentExpansionStats appendGraphDocumentExpansionTerms(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const SearchEngineConfig& config, const std::vector<GraphExpansionTerm>& graphTerms,
    std::unordered_set<std::string>& existingDocIds, std::vector<ComponentResult>& results,
    QueryExpansionStats* expansionStats) {
    GraphDocumentExpansionStats stats;

    for (const auto& term : graphTerms) {
        auto searchResults =
            metadataRepo->search(term.text, static_cast<int>(config.textMaxResults), 0);
        if (!searchResults) {
            continue;
        }
        const auto& rows = searchResults.value().results;
        stats.rawHitCount += rows.size();

        detail::LexicalScoringInputs inputs;
        inputs.candidates = std::span<const yams::metadata::SearchResult>(rows.data(), rows.size());
        inputs.applyFilenameBoost = false;
        inputs.applyIntentAdaptiveWeighting = false;
        inputs.bm25NormDivisor = config.bm25NormDivisor;
        inputs.penalty = config.graphExpansionFtsPenalty * std::clamp(term.score, 0.2f, 1.0f);
        inputs.sourceTag = ComponentResult::Source::GraphText;
        inputs.startRank = results.size();
        inputs.admissionMinScore = config.graphTextMinAdmissionScore;

        auto scored = detail::scoreLexicalBatch(inputs);

        for (size_t rank = 0; rank < rows.size(); ++rank) {
            const auto& sr = rows[rank];
            const auto docId = documentIdForTrace(sr.document.filePath, sr.document.sha256Hash);
            if (!docId.empty() && existingDocIds.contains(docId)) {
                continue;
            }
            auto& opt = scored.results[rank];
            if (!opt) {
                if (expansionStats != nullptr) {
                    ++expansionStats->graphTextBlockedLowScoreCount;
                }
                continue;
            }
            opt->debugInfo["graph_doc_expansion_term"] = term.text;
            results.push_back(std::move(*opt));
            if (!docId.empty()) {
                existingDocIds.insert(docId);
            }
            ++stats.addedCount;
        }
    }

    return stats;
}

} // namespace yams::search::detail
