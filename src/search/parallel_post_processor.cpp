#include <yams/search/parallel_post_processor.hpp>

#include <algorithm>
#include <future>
#include <string>
#include <unordered_map>
#include <yams/search/result_ranker.h>

namespace yams {
namespace search {

ParallelPostProcessor::ProcessingResult ParallelPostProcessor::process(
    std::vector<SearchResultItem> results,
    const SearchFilters* filters,
    const std::vector<std::string>& facetFields,
    const QueryNode* queryAst,
    size_t snippetLength,
    size_t maxHighlights) {
    
    ProcessingResult result;
    const bool useParallel = results.size() >= PARALLEL_THRESHOLD;

    if (useParallel) {
        // Parallel path: launch independent operations concurrently
        
        // 1. Apply filtering first (facets need filtered results)
        if (filters && filters->hasFilters()) {
            result.filteredResults = filters->apply(results);
        } else {
            result.filteredResults = std::move(results);
        }

        // 2. Facet generation, highlights, and snippets can run in parallel
        std::future<std::vector<SearchFacet>> facetFuture;
        if (!facetFields.empty()) {
            facetFuture = std::async(std::launch::async, [&result, &facetFields]() {
                return generateFacetsParallel(result.filteredResults, facetFields);
            });
        }

        // 3. Highlights and snippets - must work on filtered results
        // These modify results in-place but can run together
        std::future<void> highlightFuture;
        if (queryAst) {
            auto queryTerms = extractQueryTerms(queryAst);
            highlightFuture = std::async(std::launch::async,
                [&result, queryTerms = std::move(queryTerms), maxHighlights]() {
                    generateHighlightsParallel(result.filteredResults, queryTerms, maxHighlights);
                });
        }

        if (snippetLength > 0) {
            generateSnippetsParallel(result.filteredResults, snippetLength);
        }

        // Wait for highlights to complete
        if (queryAst) {
            highlightFuture.get();
            result.highlightsGenerated = true;
        }

        result.snippetsGenerated = (snippetLength > 0);

        // Get facets (already running in parallel)
        if (!facetFields.empty()) {
            result.facets = facetFuture.get();
        }

    } else {
        // Sequential path: below threshold, avoid parallel overhead

        // 1. Apply filters
        if (filters && filters->hasFilters()) {
            result.filteredResults = filters->apply(results);
        } else {
            result.filteredResults = std::move(results);
        }

        // 2. Generate facets
        if (!facetFields.empty()) {
            result.facets = generateFacetsParallel(result.filteredResults, facetFields);
        }

        // 3. Generate highlights
        if (queryAst) {
            auto queryTerms = extractQueryTerms(queryAst);
            generateHighlightsParallel(result.filteredResults, queryTerms, maxHighlights);
            result.highlightsGenerated = true;
        }

        // 4. Generate snippets
        if (snippetLength > 0) {
            generateSnippetsParallel(result.filteredResults, snippetLength);
            result.snippetsGenerated = true;
        }
    }

    return result;
}

std::vector<SearchFacet> ParallelPostProcessor::generateFacetsParallel(
    const std::vector<SearchResultItem>& results,
    const std::vector<std::string>& facetFields) {
    
    std::vector<SearchFacet> facets;
    facets.reserve(facetFields.size());

    for (const auto& fieldName : facetFields) {
        SearchFacet facet;
        facet.name = fieldName;
        facet.displayName = fieldName;

        std::unordered_map<std::string, size_t> valueCounts;

        // Count values for this facet field
        for (const auto& result : results) {
            std::string value;

            if (fieldName == "contentType") {
                value = result.contentType;
            } else if (fieldName == "language") {
                value = result.detectedLanguage;
            } else {
                // Check metadata
                auto it = result.metadata.find(fieldName);
                if (it != result.metadata.end()) {
                    value = it->second;
                }
            }

            if (!value.empty()) {
                valueCounts[value]++;
            }
        }

        // Convert to facet values
        facet.values.reserve(valueCounts.size());
        for (const auto& [value, count] : valueCounts) {
            SearchFacet::FacetValue facetValue;
            facetValue.value = value;
            facetValue.display = value;
            facetValue.count = count;
            facet.values.push_back(std::move(facetValue));
        }

        // Sort facet values by count (descending)
        std::sort(facet.values.begin(), facet.values.end(),
                  [](const SearchFacet::FacetValue& a, const SearchFacet::FacetValue& b) {
                      return a.count > b.count;
                  });

        facet.totalValues = facet.values.size();

        if (!facet.values.empty()) {
            facets.push_back(std::move(facet));
        }
    }

    return facets;
}

void ParallelPostProcessor::generateHighlightsParallel(
    std::vector<SearchResultItem>& results,
    const std::vector<std::string>& queryTerms,
    size_t maxHighlights) {
    
    for (auto& result : results) {
        // Generate highlights for content preview
        if (!result.contentPreview.empty()) {
            SearchHighlight highlight;
            highlight.field = "content";
            highlight.snippet = result.contentPreview;
            highlight.startOffset = 0;
            highlight.endOffset = result.contentPreview.length();

            // Find term positions (simplified)
            for (const auto& term : queryTerms) {
                size_t pos = result.contentPreview.find(term);
                if (pos != std::string::npos) {
                    highlight.highlights.emplace_back(pos, pos + term.length());
                }
            }

            if (!highlight.highlights.empty()) {
                result.highlights.push_back(std::move(highlight));
            }
        }

        // Generate highlights for title
        for (const auto& term : queryTerms) {
            size_t pos = result.title.find(term);
            if (pos != std::string::npos) {
                SearchHighlight titleHighlight;
                titleHighlight.field = "title";
                titleHighlight.snippet = result.title;
                titleHighlight.startOffset = 0;
                titleHighlight.endOffset = result.title.length();
                titleHighlight.highlights.emplace_back(pos, pos + term.length());

                result.highlights.push_back(std::move(titleHighlight));
                break; // Only one title highlight
            }
        }

        // Limit highlights per result
        if (result.highlights.size() > maxHighlights) {
            result.highlights.resize(maxHighlights);
        }
    }
}

void ParallelPostProcessor::generateSnippetsParallel(
    std::vector<SearchResultItem>& results,
    size_t snippetLength) {
    
    for (auto& result : results) {
        if (result.contentPreview.length() > snippetLength) {
            result.contentPreview = result.contentPreview.substr(0, snippetLength) + "...";
        }
        result.previewLength = result.contentPreview.length();
    }
}

std::vector<std::string> ParallelPostProcessor::extractQueryTerms(const QueryNode* queryAst) {
    std::vector<std::string> terms;
    
    if (!queryAst) {
        return terms;
    }

    // Use ResultRanker's term extraction (requires creating a temporary ranker)
    // This ensures consistency with how terms are extracted elsewhere
    ResultRanker ranker;
    return ranker.extractQueryTerms(queryAst);
}

} // namespace search
} // namespace yams
