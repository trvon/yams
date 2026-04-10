#pragma once

#include <vector>
#include <yams/search/search_filters.h>
#include <yams/search/search_results.h>

namespace yams {
namespace search {

class QueryNode;

/**
 * @brief Parallel post-processor for search results
 *
 * Parallelizes independent post-search operations (filtering, facet generation, highlighting)
 * with daemon-aware worker budgeting.
 */
class ParallelPostProcessor {
public:
    /// Minimum result count to activate parallel processing for exact highlight/snippet work
    static constexpr size_t PARALLEL_THRESHOLD = 500;
    /// Lower threshold for facet-only processing
    static constexpr size_t FACET_ONLY_PARALLEL_THRESHOLD = 256;

    struct ProcessingResult {
        std::vector<SearchResultItem> filteredResults;
        std::vector<SearchFacet> facets;
        bool highlightsGenerated = false;
        bool snippetsGenerated = false;
        size_t workersUsed = 0;
        bool facetsApproximate = false;
        size_t facetInputCount = 0;
        size_t facetSampleCount = 0;
    };

    static ProcessingResult process(std::vector<SearchResultItem> results,
                                    const SearchFilters* filters,
                                    const std::vector<std::string>& facetFields,
                                    const QueryNode* queryAst, size_t snippetLength,
                                    size_t maxHighlights);

private:
    static std::vector<SearchFacet>
    generateFacetsParallel(const std::vector<SearchResultItem>& results,
                           const std::vector<std::string>& facetFields);

    static void generateHighlightsParallel(std::vector<SearchResultItem>& results,
                                           const std::vector<std::string>& queryTerms,
                                           size_t maxHighlights);

    static void generateSnippetsParallel(std::vector<SearchResultItem>& results,
                                         size_t snippetLength);

    static std::vector<std::string> extractQueryTerms(const QueryNode* queryAst);
};

} // namespace search
} // namespace yams
