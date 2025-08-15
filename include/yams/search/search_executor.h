#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <yams/core/types.h>
#include <yams/metadata/database.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/query_parser.h>
#include <yams/search/result_ranker.h>
#include <yams/search/search_filters.h>
#include <yams/search/search_results.h>

namespace yams::search {

/**
 * @brief Search execution configuration
 */
struct SearchConfig {
    size_t maxResults = 1000;    // Maximum number of results to return
    size_t defaultPageSize = 20; // Default number of results per page
    size_t maxPageSize = 100;    // Maximum allowed page size

    // Result processing
    bool enableHighlighting = true; // Generate result highlights
    bool enableFaceting = true;     // Generate search facets
    bool enableSnippets = true;     // Generate content snippets
    size_t snippetLength = 200;     // Length of content snippets
    size_t maxHighlights = 3;       // Maximum highlights per field

    // Performance settings
    std::chrono::milliseconds timeout{30000}; // Search timeout
    bool enableQueryCache = true;             // Enable query result caching
    size_t cacheSize = 1000;                  // Maximum cached queries

    // Ranking configuration
    RankingConfig rankingConfig;

    // Default sort order
    enum class SortOrder { Relevance, DateDesc, DateAsc, TitleAsc, TitleDesc, SizeAsc, SizeDesc };

    SortOrder defaultSort = SortOrder::Relevance;
};

/**
 * @brief Search request parameters
 */
struct SearchRequest {
    std::string query; // Search query string
    size_t offset = 0; // Result offset (for pagination)
    size_t limit = 20; // Number of results to return

    // Filtering
    SearchFilters filters;

    // Sorting
    SearchConfig::SortOrder sortOrder = SearchConfig::SortOrder::Relevance;

    // Result options
    bool includeHighlights = true;
    bool includeFacets = true;
    bool includeSnippets = true;
    std::vector<std::string> facetFields = {"contentType", "language", "author"};

    // Advanced options
    bool enableQueryExpansion = false;  // Expand query with synonyms
    bool enableSpellCorrection = false; // Suggest spelling corrections
    float minRelevanceScore = 0.0f;     // Minimum relevance score
};

// Note: SearchResults is defined in search_results.h
// The old SearchResponse struct has been removed and functionality
// merged into the SearchResults class and SearchStatistics struct

/**
 * @brief Main search execution engine
 */
class SearchExecutor {
public:
    /**
     * @brief Constructor
     */
    SearchExecutor(std::shared_ptr<metadata::Database> database,
                   std::shared_ptr<metadata::MetadataRepository> metadataRepo,
                   const SearchConfig& config = {});

    /**
     * @brief Execute a search query
     */
    Result<SearchResults> search(const SearchRequest& request);

    /**
     * @brief Execute a simple text search
     */
    Result<SearchResults> search(const std::string& query, size_t offset = 0, size_t limit = 20);

    /**
     * @brief Get search suggestions/autocomplete
     */
    Result<std::vector<std::string>> getSuggestions(const std::string& partialQuery,
                                                    size_t maxSuggestions = 10);

    /**
     * @brief Get search facets for a query (without executing full search)
     */
    Result<std::vector<SearchFacet>> getFacets(const std::string& query,
                                               const std::vector<std::string>& facetFields);

    /**
     * @brief Set search configuration
     */
    void setConfig(const SearchConfig& config) { config_ = config; }

    /**
     * @brief Get current configuration
     */
    const SearchConfig& getConfig() const { return config_; }

    /**
     * @brief Get search statistics
     */
    struct SearchExecutorStats {
        size_t totalSearches = 0;
        size_t cacheHits = 0;
        size_t cacheMisses = 0;
        std::chrono::milliseconds avgSearchTime{0};
        std::chrono::milliseconds avgRankingTime{0};
        std::chrono::milliseconds maxSearchTime{0};
        size_t errorCount = 0;
    };

    SearchExecutorStats getStatistics() const { return stats_; }

    /**
     * @brief Clear search cache
     */
    void clearCache();

private:
    std::shared_ptr<metadata::Database> database_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    SearchConfig config_;

    // Components
    std::unique_ptr<QueryParser> queryParser_;
    std::unique_ptr<ResultRanker> ranker_;

    // Cache (simple implementation)
    mutable std::unordered_map<std::string, SearchResults> queryCache_;
    mutable std::vector<std::string> cacheOrder_; // For LRU eviction

    // Statistics
    mutable SearchExecutorStats stats_;

    /**
     * @brief Execute FTS5 query against database
     */
    Result<std::vector<SearchResultItem>> executeFTSQuery(const std::string& ftsQuery,
                                                          const SearchRequest& request);

    /**
     * @brief Generate result highlights
     */
    void generateHighlights(std::vector<SearchResultItem>& results, const QueryNode* queryAst);

    /**
     * @brief Generate search facets
     */
    std::vector<SearchFacet> generateFacets(const std::vector<SearchResultItem>& results,
                                            const std::vector<std::string>& facetFields);

    /**
     * @brief Generate content snippets
     */
    void generateSnippets(std::vector<SearchResultItem>& results);

    /**
     * @brief Apply post-processing filters
     */
    std::vector<SearchResultItem> applyFilters(const std::vector<SearchResultItem>& results,
                                               const SearchFilters& filters);

    /**
     * @brief Sort search results
     */
    void sortResults(std::vector<SearchResultItem>& results, SearchConfig::SortOrder sortOrder);

    /**
     * @brief Cache management
     */
    void cacheResult(const std::string& cacheKey, const SearchResults& response) const;
    std::optional<SearchResults> getCachedResult(const std::string& cacheKey) const;
    std::string generateCacheKey(const SearchRequest& request) const;
    void evictOldestCacheEntry() const;

    /**
     * @brief Error handling
     */
    SearchResults createErrorResponse(const std::string& error, const SearchRequest& request) const;

    /**
     * @brief Performance tracking
     */
    void updateStatistics(const std::chrono::milliseconds& searchTime,
                          const std::chrono::milliseconds& rankingTime, bool cacheHit) const;
};

/**
 * @brief Search executor factory for creating configured instances
 */
class SearchExecutorFactory {
public:
    /**
     * @brief Create a default search executor
     */
    static std::unique_ptr<SearchExecutor>
    create(std::shared_ptr<metadata::Database> database,
           std::shared_ptr<metadata::MetadataRepository> metadataRepo);

    /**
     * @brief Create a search executor with custom configuration
     */
    static std::unique_ptr<SearchExecutor>
    create(std::shared_ptr<metadata::Database> database,
           std::shared_ptr<metadata::MetadataRepository> metadataRepo, const SearchConfig& config);

    /**
     * @brief Create a high-performance search executor
     */
    static std::unique_ptr<SearchExecutor>
    createHighPerformance(std::shared_ptr<metadata::Database> database,
                          std::shared_ptr<metadata::MetadataRepository> metadataRepo);
};

} // namespace yams::search