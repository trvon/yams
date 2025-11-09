#pragma once

#include <vector>
#include <yams/search/search_results.h>
#include <yams/search/search_filters.h>

namespace yams {
namespace search {

class QueryNode;

/**
 * @brief Parallel post-processor for search results
 * 
 * Parallelizes independent post-FTS5 operations (filtering, facet generation, highlighting)
 * to improve latency on large result sets (>100 items).
 * 
 * @note Only activates parallelization when result count exceeds PARALLEL_THRESHOLD
 *       to avoid overhead on small result sets.
 */
class ParallelPostProcessor {
public:
    /// Minimum result count to activate parallel processing
    static constexpr size_t PARALLEL_THRESHOLD = 100;

    /**
     * @brief Result of parallel post-processing operations
     */
    struct ProcessingResult {
        std::vector<SearchResultItem> filteredResults;
        std::vector<SearchFacet> facets;
        bool highlightsGenerated = false;
        bool snippetsGenerated = false;
    };

    /**
     * @brief Process search results with optional parallelization
     * 
     * Conditionally parallelizes the following operations based on result count:
     * - Filter application (if filters provided)
     * - Facet generation (if facet fields provided)
     * - Highlight generation (if queryAst provided, modifies results in-place)
     * - Snippet generation (always applied, modifies results in-place)
     * 
     * @param results Input results to process (will be moved)
     * @param filters Filters to apply (nullptr = skip filtering)
     * @param facetFields Fields for facet generation (empty = skip facets)
     * @param queryAst Query AST for highlight generation (nullptr = skip highlights)
     * @param snippetLength Maximum snippet length (0 = skip snippets)
     * @param maxHighlights Maximum highlights per result
     * @return ProcessingResult with filtered results and generated facets
     */
    static ProcessingResult process(
        std::vector<SearchResultItem> results,
        const SearchFilters* filters,
        const std::vector<std::string>& facetFields,
        const QueryNode* queryAst,
        size_t snippetLength,
        size_t maxHighlights);

private:
    /**
     * @brief Generate facets in parallel-safe manner
     * 
     * Aggregates field values across results to produce facet counts.
     * This operation is read-only on results, making it trivially parallelizable.
     * 
     * @param results Input results
     * @param facetFields Fields to facet on
     * @return Vector of facets with value counts
     */
    static std::vector<SearchFacet> generateFacetsParallel(
        const std::vector<SearchResultItem>& results,
        const std::vector<std::string>& facetFields);

    /**
     * @brief Generate highlights in parallel-safe manner
     * 
     * Finds query term positions in result content and titles.
     * Modifies results in-place, but each result is independent.
     * 
     * @param results Results to modify (in-place)
     * @param queryTerms Extracted query terms to highlight
     * @param maxHighlights Maximum highlights per result
     */
    static void generateHighlightsParallel(
        std::vector<SearchResultItem>& results,
        const std::vector<std::string>& queryTerms,
        size_t maxHighlights);

    /**
     * @brief Generate snippets in parallel-safe manner
     * 
     * Truncates content previews to snippet length.
     * Modifies results in-place, but each result is independent.
     * 
     * @param results Results to modify (in-place)
     * @param snippetLength Maximum snippet length
     */
    static void generateSnippetsParallel(
        std::vector<SearchResultItem>& results,
        size_t snippetLength);

    /**
     * @brief Extract query terms from AST for highlighting
     * 
     * @param queryAst Query AST node
     * @return Vector of terms to highlight
     */
    static std::vector<std::string> extractQueryTerms(const QueryNode* queryAst);
};

} // namespace search
} // namespace yams
