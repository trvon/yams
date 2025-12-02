#pragma once

#include <yams/core/types.h>
#include <yams/search/search_engine.h>
#include <yams/vector/document_chunker.h>
#include <yams/vector/embedding_generator.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::api {

/**
 * Search modes supported by the API
 */
enum class SearchMode {
    VECTOR,  // Pure semantic/vector search
    KEYWORD, // Pure keyword/BM25 search
    HYBRID,  // Combined vector + keyword search
    AUTO     // Automatically choose best mode
};

/**
 * Sorting options for search results
 */
enum class SortBy {
    RELEVANCE,    // Sort by relevance score (default)
    DATE_CREATED, // Sort by creation date
    DATE_UPDATED, // Sort by last update date
    TITLE,        // Sort alphabetically by title
    SIZE,         // Sort by document size
    CUSTOM        // Custom sorting function
};

enum class SortOrder {
    ASCENDING, // Low to high
    DESCENDING // High to low (default)
};

/**
 * Query intent detection
 */
enum class QueryIntent {
    INFORMATIONAL, // Looking for information
    NAVIGATIONAL,  // Looking for specific page/document
    TRANSACTIONAL, // Looking to perform action
    COMPARATIVE,   // Comparing options
    UNKNOWN        // Cannot determine intent
};

/**
 * Document metadata
 */
struct DocumentMetadata {
    std::string title;
    std::string author;
    std::string source;
    std::string content_type;
    std::string language = "en";

    // Timestamps
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point updated_at;

    // Size info
    size_t file_size = 0;
    size_t word_count = 0;

    // Categories and tags
    std::vector<std::string> categories;
    std::vector<std::string> tags;

    // Custom metadata
    std::map<std::string, std::string> custom_fields;

    // Access control
    std::string access_level = "public";
    std::vector<std::string> allowed_users;
    std::vector<std::string> allowed_groups;

    // Quality metrics
    float content_quality = 1.0f;
    float popularity_score = 0.0f;

    bool isEmpty() const { return title.empty() && author.empty() && source.empty(); }
};

/**
 * Search filters
 */
struct SearchFilters {
    // Content filters
    std::vector<std::string> content_types;
    std::vector<std::string> languages;
    std::vector<std::string> categories;
    std::vector<std::string> tags;
    std::vector<std::string> authors;
    std::vector<std::string> sources;

    // Date range filters
    std::optional<std::chrono::system_clock::time_point> created_after;
    std::optional<std::chrono::system_clock::time_point> created_before;
    std::optional<std::chrono::system_clock::time_point> updated_after;
    std::optional<std::chrono::system_clock::time_point> updated_before;

    // Size filters
    std::optional<size_t> min_file_size;
    std::optional<size_t> max_file_size;
    std::optional<size_t> min_word_count;
    std::optional<size_t> max_word_count;

    // Quality filters
    std::optional<float> min_quality_score;
    std::optional<float> min_popularity_score;

    // Access control
    std::string user_id;
    std::vector<std::string> user_groups;

    // Custom filters
    std::map<std::string, std::string> custom_filters;
    std::function<bool(const DocumentMetadata&)> custom_predicate;

    bool hasFilters() const {
        return !content_types.empty() || !languages.empty() || !categories.empty() ||
               !tags.empty() || created_after.has_value() || created_before.has_value() ||
               min_file_size.has_value() || max_file_size.has_value() || !custom_filters.empty() ||
               custom_predicate != nullptr;
    }
};

/**
 * Text snippet with highlighting
 */
struct TextSnippet {
    std::string text;
    size_t start_position = 0;
    size_t end_position = 0;
    std::vector<std::pair<size_t, size_t>> highlighted_ranges;
    float relevance_score = 0.0f;
};

/**
 * Facet for result filtering
 */
struct FacetValue {
    std::string value;
    size_t count = 0;
    bool selected = false;
};

/**
 * Entity extracted from query
 */
struct Entity {
    std::string text;
    std::string type; // PERSON, LOCATION, ORGANIZATION, etc.
    float confidence = 0.0f;
    size_t start_pos = 0;
    size_t end_pos = 0;
};

/**
 * Query correction suggestion
 */
struct QueryCorrection {
    std::string suggested_query;
    float confidence = 0.0f;
    std::vector<std::string> changed_terms;
    bool is_significant_change = false;
};

/**
 * Relevance explanation for search results
 */
struct RelevanceExplanation {
    std::string description;                         ///< Human-readable explanation
    std::map<std::string, float> componentScores;    ///< Per-component score breakdown
    std::vector<std::string> matchedFeatures;        ///< Features that contributed to the match
};

/**
 * Search result
 */
struct SearchResult {
    std::string id;
    std::string title;
    std::string content;
    std::string url; // Optional URL/path

    // Scoring information
    float score = 0.0f;
    float vector_score = 0.0f;
    float keyword_score = 0.0f;
    size_t rank = 0; // Position in results (0-based)

    // Content snippets
    std::vector<TextSnippet> snippets;
    std::string summary; // Generated summary
    std::vector<std::string> highlighted_terms;

    // Metadata
    DocumentMetadata metadata;

    // Relevance explanation
    std::optional<RelevanceExplanation> explanation;

    // Interaction tracking
    size_t click_count = 0;
    size_t view_count = 0;
    double click_through_rate = 0.0;
};

/**
 * Debug information for search
 */
struct DebugInfo {
    // Query processing
    std::string original_query;
    std::string processed_query;
    std::vector<std::string> query_tokens;
    std::vector<std::string> expanded_terms;

    // Search execution
    double vector_search_time_ms = 0.0;
    double keyword_search_time_ms = 0.0;
    double fusion_time_ms = 0.0;
    double total_time_ms = 0.0;

    // Result composition
    size_t vector_results_count = 0;
    size_t keyword_results_count = 0;
    size_t duplicate_results_removed = 0;

    // Index stats
    size_t total_documents_in_index = 0;
    size_t documents_matched = 0;

    // Cache info
    bool cache_hit = false;
    std::string cache_key;
};

/**
 * Search options
 */
struct SearchOptions {
    // Result options
    size_t max_results = 10;
    bool include_snippets = true;
    bool include_metadata = true;
    bool generate_summary = false;
    bool highlight_matches = true;

    // Search behavior
    SearchMode mode = SearchMode::HYBRID;
    float vector_weight = 0.7f;
    float keyword_weight = 0.3f;

    // Result processing
    bool enable_reranking = true;
    bool remove_duplicates = true;
    SortBy sort_by = SortBy::RELEVANCE;
    SortOrder sort_order = SortOrder::DESCENDING;

    // Query processing
    bool expand_query = true;
    bool correct_spelling = true;
    bool extract_entities = false;

    // Performance
    std::chrono::milliseconds timeout{5000};

    // Debug
    bool include_debug_info = false;
    bool explain_results = false;
};

/**
 * Search request
 */
struct SearchRequest {
    std::string query;
    SearchOptions options;
    SearchFilters filters;

    // Pagination
    size_t page = 1;
    size_t results_per_page = 10;

    // Session context
    std::string session_id;
    std::string user_id;

    // A/B testing
    std::string experiment_id;
    std::string variant_id;

    // Client info
    std::string client_ip;
    std::string user_agent;

    bool isValid() const {
        return !query.empty() && results_per_page > 0 && results_per_page <= 100 && page > 0;
    }
};

/**
 * Search response
 */
struct SearchResponse {
    // Results
    std::vector<SearchResult> results;

    // Pagination
    size_t total_results = 0;
    size_t page = 1;
    size_t total_pages = 1;
    size_t results_per_page = 10;
    bool has_more_results = false;

    // Query information
    std::string processed_query;
    std::vector<std::string> expanded_terms;
    std::optional<QueryCorrection> correction;
    QueryIntent detected_intent = QueryIntent::UNKNOWN;
    std::vector<Entity> entities;

    // Facets for filtering
    std::map<std::string, std::vector<FacetValue>> facets;

    // Performance metrics
    double search_time_ms = 0.0;
    SearchMode mode_used = SearchMode::HYBRID;

    // Suggestions
    std::vector<std::string> related_queries;
    std::vector<std::string> spelling_suggestions;

    // Analytics
    std::string search_id; // Unique ID for this search
    std::chrono::system_clock::time_point timestamp;

    // Debug information
    std::optional<DebugInfo> debug_info;

    // Response status
    bool success = true;
    std::string error_message;
};

/**
 * Search analytics
 */
struct SearchAnalytics {
    // Volume metrics
    size_t total_searches = 0;
    size_t unique_queries = 0;
    size_t unique_users = 0;

    // Performance metrics
    double avg_response_time_ms = 0.0;
    double p95_response_time_ms = 0.0;
    double p99_response_time_ms = 0.0;

    // Quality metrics
    double avg_results_per_search = 0.0;
    double zero_results_rate = 0.0;
    double click_through_rate = 0.0;

    // Popular queries
    std::vector<std::pair<std::string, size_t>> top_queries;
    std::vector<std::pair<std::string, size_t>> trending_queries;
    std::vector<std::pair<std::string, size_t>> failing_queries;

    // Search modes usage
    std::map<SearchMode, size_t> mode_usage;

    // Time range
    std::chrono::system_clock::time_point start_time;
    std::chrono::system_clock::time_point end_time;
};

/**
 * Session configuration
 */
struct SessionConfig {
    std::chrono::minutes timeout{30};    // Session timeout
    bool track_history = true;           // Track query history
    bool enable_personalization = false; // Enable personalized results
    size_t max_history_size = 100;       // Max queries to remember
    bool share_across_devices = false;   // Share session across devices
};

/**
 * API configuration
 */
struct SearchAPIConfig {
    // Default search settings
    size_t default_results_per_page = 10;
    size_t max_results_per_page = 100;
    size_t max_query_length = 1000;

    // Processing settings
    bool auto_correct_spelling = true;
    bool expand_acronyms = true;
    bool detect_language = true;
    bool extract_entities = false;
    bool generate_facets = true;

    // Performance settings
    std::chrono::milliseconds default_timeout{5000};
    std::chrono::milliseconds max_timeout{30000};
    size_t max_concurrent_searches = 100;

    // Caching
    bool enable_query_cache = true;
    size_t query_cache_size = 10000;
    std::chrono::minutes cache_ttl{30};

    // Analytics
    bool track_searches = true;
    bool track_clicks = true;
    bool track_conversions = true;
    bool anonymous_tracking = true;

    // Rate limiting
    size_t rate_limit_per_minute = 60;
    size_t rate_limit_per_hour = 1000;

    // Content safety
    bool filter_inappropriate = false;
    std::vector<std::string> blocked_terms;

    // A/B testing
    bool enable_experiments = false;
    std::map<std::string, double> experiment_weights;
};

/**
 * Main Semantic Search API
 */
class SemanticSearchAPI {
public:
    SemanticSearchAPI(std::shared_ptr<search::SearchEngine> search_engine,
                      std::shared_ptr<vector::DocumentChunker> chunker,
                      std::shared_ptr<vector::EmbeddingGenerator> embedder,
                      const SearchAPIConfig& config = {});

    ~SemanticSearchAPI();

    // Non-copyable but movable
    SemanticSearchAPI(const SemanticSearchAPI&) = delete;
    SemanticSearchAPI& operator=(const SemanticSearchAPI&) = delete;
    SemanticSearchAPI(SemanticSearchAPI&&) noexcept;
    SemanticSearchAPI& operator=(SemanticSearchAPI&&) noexcept;

    // Initialization
    Result<void> initialize();
    bool isInitialized() const;
    void shutdown();

    // Document management
    Result<std::string> ingestDocument(const std::string& content,
                                       const DocumentMetadata& metadata);

    Result<std::vector<std::string>> ingestDocuments(const std::vector<std::string>& contents,
                                                     const std::vector<DocumentMetadata>& metadata);

    Result<void> updateDocument(const std::string& document_id, const std::string& content,
                                const DocumentMetadata& metadata);

    Result<void> deleteDocument(const std::string& document_id);

    Result<void> deleteDocuments(const std::vector<std::string>& document_ids);

    // Search operations
    Result<SearchResponse> search(const SearchRequest& request);

    Result<SearchResponse> searchWithSession(const SearchRequest& request,
                                             const std::string& session_id);

    // Convenience search methods
    Result<SearchResponse> semanticSearch(const std::string& query,
                                          const SearchOptions& options = {},
                                          const SearchFilters& filters = {});

    Result<SearchResponse> keywordSearch(const std::string& query,
                                         const SearchOptions& options = {},
                                         const SearchFilters& filters = {});

    Result<SearchResponse> hybridSearch(const std::string& query, const SearchOptions& options = {},
                                        const SearchFilters& filters = {});

    // Query assistance
    Result<std::vector<std::string>> getSuggestions(const std::string& partial_query,
                                                    size_t max_suggestions = 10);

    Result<std::vector<std::string>> getRelatedQueries(const std::string& query,
                                                       size_t max_related = 5);

    Result<QueryCorrection> correctQuery(const std::string& query);

    Result<std::vector<std::string>> getPopularQueries(size_t count = 10);

    // Analytics and tracking
    void trackSearch(const SearchRequest& request, const SearchResponse& response);
    void trackClick(const std::string& search_id, const std::string& result_id, size_t position);
    void trackConversion(const std::string& search_id, const std::string& result_id);

    Result<SearchAnalytics> getAnalytics(const std::chrono::system_clock::time_point& start,
                                         const std::chrono::system_clock::time_point& end);

    // Session management
    std::string createSession(const SessionConfig& config = {});
    Result<void> endSession(const std::string& session_id);
    Result<std::vector<std::string>> getSessionHistory(const std::string& session_id);

    // Index management
    Result<void> optimizeIndex();
    Result<void> rebuildIndex();
    Result<size_t> getDocumentCount() const;
    Result<size_t> getIndexSize() const;

    // Configuration
    void setConfig(const SearchAPIConfig& config);
    const SearchAPIConfig& getConfig() const;

    // Health check
    bool isHealthy() const;
    std::map<std::string, std::string> getStatus() const;

    // Error handling
    std::string getLastError() const;
    bool hasError() const;

private:
    class Impl;
    std::unique_ptr<Impl> pImpl;
};

/**
 * Query processing utilities
 */
namespace query_utils {
/**
 * Clean and normalize query text
 */
std::string normalizeQuery(const std::string& query);

/**
 * Extract keywords from query
 */
std::vector<std::string> extractKeywords(const std::string& query);

/**
 * Detect query language
 */
std::string detectLanguage(const std::string& query);

/**
 * Extract entities from query text
 */
std::vector<Entity> extractEntities(const std::string& query);

/**
 * Determine query intent
 */
QueryIntent classifyIntent(const std::string& query);

/**
 * Generate query variations
 */
std::vector<std::string> generateVariations(const std::string& query);

/**
 * Check if query is valid
 */
bool isValidQuery(const std::string& query);

/**
 * Calculate query complexity score
 */
float calculateComplexity(const std::string& query);
} // namespace query_utils

/**
 * Result formatting utilities
 */
namespace result_utils {
/**
 * Generate snippet from content
 */
std::vector<TextSnippet> generateSnippets(const std::string& content,
                                          const std::vector<std::string>& query_terms,
                                          size_t max_snippets = 3, size_t snippet_length = 150);

/**
 * Highlight query terms in text
 */
std::string highlightTerms(const std::string& text, const std::vector<std::string>& terms,
                           const std::string& start_tag = "<mark>",
                           const std::string& end_tag = "</mark>");

/**
 * Generate document summary
 */
std::string generateSummary(const std::string& content, size_t max_length = 300);

/**
 * Calculate result diversity
 */
double calculateDiversity(const std::vector<SearchResult>& results);

/**
 * Remove duplicate results
 */
std::vector<SearchResult> removeDuplicates(const std::vector<SearchResult>& results,
                                           double similarity_threshold = 0.85);
} // namespace result_utils

} // namespace yams::api