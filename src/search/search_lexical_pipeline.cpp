#include "search_lexical_pipeline_internal.h"

#include <spdlog/spdlog.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/lexical_scoring.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_text_utils.h>
#include <yams/search/simeon_lexical_backend.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>

namespace yams::search::detail {

void appendLexicalBatch(const std::string& query, QueryIntent queryIntent,
                        const SearchEngineConfig& config,
                        const yams::metadata::SearchResults& searchResults, float scorePenalty,
                        bool dedupe, std::unordered_set<std::string>* seenHashes,
                        ComponentResult::Source source, std::vector<ComponentResult>& results,
                        QueryExpansionStats* expansionStats) {
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
    inputs.startRank = results.size();
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

void applySimeonLexicalRescoring(const std::string& query, SimeonLexicalBackend* backend,
                                 yams::metadata::SearchResults& searchResults,
                                 std::string* lastSimeonRouteRecipe) {
    if (!backend || !backend->ready() || searchResults.results.empty()) {
        return;
    }

    std::vector<std::int64_t> ids;
    ids.reserve(searchResults.results.size());
    for (const auto& r : searchResults.results) {
        ids.push_back(r.document.id);
    }

    auto decision = backend->scoreRouted(query, ids);
    if (!decision) {
        spdlog::debug("[simeon-lexical] score failed, keeping FTS5 scores: {}",
                      decision.error().message);
        return;
    }

    const auto& values = decision.value().scores;
    if (values.size() != searchResults.results.size()) {
        spdlog::warn("[simeon-lexical] score size mismatch ({} vs {}); keeping FTS5 scores",
                     values.size(), searchResults.results.size());
        return;
    }
    if (lastSimeonRouteRecipe != nullptr) {
        *lastSimeonRouteRecipe = decision.value().recipe_name;
    }
    for (size_t i = 0; i < values.size(); ++i) {
        searchResults.results[i].score = -static_cast<double>(values[i]);
    }
    std::sort(searchResults.results.begin(), searchResults.results.end(),
              [](const yams::metadata::SearchResult& a, const yams::metadata::SearchResult& b) {
                  return a.score < b.score;
              });
}

Fts5CandidatePoolStats fetchFts5CandidatePool(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe, const std::string& query,
    QueryIntent queryIntent, const SearchEngineConfig& config, size_t limit,
    QueryExpansionStats* expansionStats, std::vector<ComponentResult>& results,
    std::unordered_set<std::string>& seenHashes,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator) {
    Fts5CandidatePoolStats stats;

    auto fts5Results = metadataRepo->search(query, static_cast<int>(limit), 0);
    stats.baseFtsSucceeded = static_cast<bool>(fts5Results);
    if (!stats.baseFtsSucceeded) {
        spdlog::debug("FTS5 search failed, continuing with fallback expansion: {}",
                      fts5Results.error().message);
    }

    if (stats.baseFtsSucceeded) {
        stats.baseFtsHitCount = fts5Results.value().results.size();
        applySimeonLexicalRescoring(query, backend, fts5Results.value(), lastSimeonRouteRecipe);
        appendLexicalBatch(query, queryIntent, config, fts5Results.value(), 1.0f, true, &seenHashes,
                           ComponentResult::Source::Text, results, expansionStats);
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
                applySimeonLexicalRescoring(query, backend, expandedResults.value(),
                                            lastSimeonRouteRecipe);
                appendLexicalBatch(query, queryIntent, config, expandedResults.value(), penalty,
                                   true, &seenHashes, ComponentResult::Source::Text, results,
                                   expansionStats);
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
                applySimeonLexicalRescoring(query, backend, expandedResults.value(),
                                            lastSimeonRouteRecipe);
                appendLexicalBatch(query, queryIntent, config, expandedResults.value(), penalty,
                                   true, &seenHashes, ComponentResult::Source::Text, results,
                                   expansionStats);
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

} // namespace yams::search::detail
