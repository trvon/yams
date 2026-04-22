#pragma once

#include <yams/search/graph_expansion.h>
#include <yams/search/query_expansion.h>
#include <yams/search/query_router.h>
#include <yams/search/search_engine_config.h>
#include <yams/search/search_models.h>

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
class MetadataRepository;
struct SearchResults;
} // namespace yams::metadata

namespace yams::search {
class SimeonLexicalBackend;

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
} // namespace yams::search

namespace yams::search::detail {

struct Fts5CandidatePoolStats {
    size_t baseFtsHitCount = 0;
    bool baseFtsSucceeded = false;
};

struct GraphDocumentExpansionStats {
    size_t rawHitCount = 0;
    size_t addedCount = 0;
};

void appendLexicalBatch(const std::string& query, QueryIntent queryIntent,
                        const SearchEngineConfig& config,
                        const yams::metadata::SearchResults& searchResults, float scorePenalty,
                        bool dedupe, std::unordered_set<std::string>* seenHashes,
                        ComponentResult::Source source, std::vector<ComponentResult>& results,
                        QueryExpansionStats* expansionStats);

void applySimeonLexicalRescoring(const std::string& query, SimeonLexicalBackend* backend,
                                 yams::metadata::SearchResults& searchResults,
                                 std::string* lastSimeonRouteRecipe);

Fts5CandidatePoolStats fetchFts5CandidatePool(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe, const std::string& query,
    QueryIntent queryIntent, const SearchEngineConfig& config, size_t limit,
    QueryExpansionStats* expansionStats, std::vector<ComponentResult>& results,
    std::unordered_set<std::string>& seenHashes,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator);

std::unordered_map<std::string, float>
lookupQueryTermIdf(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                   const std::string& query);

std::vector<std::string>
generateQuerySubPhrases(const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                        const std::string& query, size_t maxPhrases);

void applyAggressiveFtsFallback(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, bool baseFtsSucceeded, size_t baseFtsHitCount,
    const std::vector<FtsFallbackClause>& aggressiveClauses, QueryExpansionStats* expansionStats,
    std::unordered_set<std::string>& seenHashes, std::vector<ComponentResult>& results);

void applyGraphExpansionTerms(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, size_t baseFtsHitCount, const std::vector<GraphExpansionTerm>& graphTerms,
    QueryExpansionStats* expansionStats, std::unordered_set<std::string>& seenHashes,
    std::vector<ComponentResult>& results);

void applySubPhraseRescoring(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, const SearchEngineConfig& config, size_t limit,
    std::vector<ComponentResult>& results,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator);

std::vector<ComponentResult> queryFullTextPipeline(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const std::string& query, QueryIntent queryIntent, const SearchEngineConfig& config,
    size_t limit, QueryExpansionStats* expansionStats,
    const std::function<std::vector<std::string>(const std::string&, size_t)>& subPhraseGenerator,
    const std::function<std::vector<FtsFallbackClause>(const std::string&, size_t)>&
        aggressiveClauseGenerator,
    const std::function<std::vector<GraphExpansionTerm>()>& graphTermProvider,
    SimeonLexicalBackend* backend, std::string* lastSimeonRouteRecipe);

GraphDocumentExpansionStats appendGraphDocumentExpansionTerms(
    const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
    const SearchEngineConfig& config, const std::vector<GraphExpansionTerm>& graphTerms,
    std::unordered_set<std::string>& existingDocIds, std::vector<ComponentResult>& results,
    QueryExpansionStats* expansionStats);

} // namespace yams::search::detail
