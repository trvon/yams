#include <yams/search/parallel_post_processor.hpp>

#include <algorithm>
#include <future>
#include <string>
#include <unordered_map>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/search/result_ranker.h>

namespace yams {
namespace search {

namespace {

std::string facetFieldValue(const SearchResultItem& item, const std::string& field) {
    if (field == "contentType") {
        return item.contentType;
    }
    if (field == "language") {
        return item.detectedLanguage;
    }
    if (field == "extension") {
        const auto pos = item.path.rfind('.');
        if (pos != std::string::npos) {
            return item.path.substr(pos);
        }
        return {};
    }
    auto it = item.metadata.find(field);
    if (it != item.metadata.end()) {
        return it->second;
    }
    return {};
}

void generateHighlightsForItem(SearchResultItem& item, const std::vector<std::string>& queryTerms,
                               size_t maxHighlights) {
    if (queryTerms.empty()) {
        return;
    }

    if (!item.contentPreview.empty()) {
        SearchHighlight highlight;
        highlight.field = "content";
        highlight.snippet = item.contentPreview;
        highlight.startOffset = 0;
        highlight.endOffset = item.contentPreview.length();

        for (const auto& term : queryTerms) {
            const size_t pos = item.contentPreview.find(term);
            if (pos != std::string::npos) {
                highlight.highlights.emplace_back(pos, pos + term.length());
            }
        }

        if (!highlight.highlights.empty()) {
            item.highlights.push_back(std::move(highlight));
        }
    }

    for (const auto& term : queryTerms) {
        const size_t pos = item.title.find(term);
        if (pos != std::string::npos) {
            SearchHighlight titleHighlight;
            titleHighlight.field = "title";
            titleHighlight.snippet = item.title;
            titleHighlight.startOffset = 0;
            titleHighlight.endOffset = item.title.length();
            titleHighlight.highlights.emplace_back(pos, pos + term.length());

            item.highlights.push_back(std::move(titleHighlight));
            break;
        }
    }

    if (item.highlights.size() > maxHighlights) {
        item.highlights.resize(maxHighlights);
    }
}

void generateSnippetForItem(SearchResultItem& item, size_t snippetLength) {
    if (snippetLength == 0) {
        return;
    }
    if (item.contentPreview.length() > snippetLength) {
        item.contentPreview = item.contentPreview.substr(0, snippetLength) + "...";
    }
    item.previewLength = item.contentPreview.length();
}

void finalizeFacetCounts(
    const std::vector<std::string>& facetFields,
    const std::unordered_map<std::string, std::unordered_map<std::string, size_t>>& mergedCounts,
    std::vector<SearchFacet>& facets) {
    facets.clear();
    facets.reserve(facetFields.size());

    for (const auto& field : facetFields) {
        auto it = mergedCounts.find(field);
        if (it == mergedCounts.end()) {
            continue;
        }

        SearchFacet facet;
        facet.name = field;
        facet.displayName = field;
        facet.values.reserve(it->second.size());
        for (const auto& [val, count] : it->second) {
            facet.values.push_back({val, val, count, false});
        }

        std::sort(facet.values.begin(), facet.values.end(),
                  [](const auto& a, const auto& b) { return a.count > b.count; });
        facet.totalValues = facet.values.size();
        if (!facet.values.empty()) {
            facets.push_back(std::move(facet));
        }
    }
}

} // namespace

ParallelPostProcessor::ProcessingResult
ParallelPostProcessor::process(std::vector<SearchResultItem> results, const SearchFilters* filters,
                               const std::vector<std::string>& facetFields,
                               const QueryNode* queryAst, size_t snippetLength,
                               size_t maxHighlights) {
    ProcessingResult result;
    const bool useParallel = results.size() >= PARALLEL_THRESHOLD;

    const std::vector<std::string> queryTerms =
        queryAst ? extractQueryTerms(queryAst) : std::vector<std::string>{};

    if (useParallel) {
        // Parallel path: partition data into chunks and process each chunk independently
        // This avoids race conditions between highlights (read) and snippets (write)
        // and improves cache locality.

        // 1. Apply filtering first (must be done sequentially or in a separate pass to determine
        // result set)
        if (filters && filters->hasFilters()) {
            result.filteredResults = filters->apply(results);
        } else {
            result.filteredResults = std::move(results);
        }

        const size_t totalResults = result.filteredResults.size();
        if (totalResults == 0) {
            return result;
        }

        // Determine concurrency
        const unsigned int maxThreads = yams::daemon::TuneAdvisor::recommendedThreads(0.5, 0);
        unsigned int numThreads =
            std::min(maxThreads, static_cast<unsigned int>((totalResults + PARALLEL_THRESHOLD - 1) /
                                                           PARALLEL_THRESHOLD));
        if (numThreads == 0) {
            numThreads = 1;
        }
        const size_t chunkSize = (totalResults + numThreads - 1) / numThreads;

        std::vector<std::future<std::vector<SearchFacet>>> futures;

        // Launch tasks for each chunk
        for (unsigned int i = 0; i < numThreads; ++i) {
            size_t start = i * chunkSize;
            size_t end = std::min(start + chunkSize, totalResults);

            if (start >= end)
                break;

            futures.push_back(
                std::async(std::launch::async, [&result, start, end, &facetFields, &queryTerms,
                                                maxHighlights, snippetLength]() {
                    // 1. Local facet accumulation
                    std::vector<SearchFacet> localFacets;
                    if (!facetFields.empty()) {
                        // Create temporary vector view for facet generation to avoid copying items
                        // Note: generateFacetsParallel interface expects a vector, so we might need
                        // to adapt or just count manually here for efficiency.
                        // For now, we'll do manual counting to avoid vector copies.

                        std::unordered_map<std::string, std::unordered_map<std::string, size_t>>
                            fieldCounts;
                        for (size_t idx = start; idx < end; ++idx) {
                            const auto& item = result.filteredResults[idx];
                            for (const auto& field : facetFields) {
                                std::string value = facetFieldValue(item, field);
                                if (!value.empty())
                                    fieldCounts[field][value]++;
                            }
                        }

                        // Convert local counts to Facets
                        for (const auto& field : facetFields) {
                            SearchFacet facet;
                            facet.name = field;
                            facet.displayName = field;
                            auto& counts = fieldCounts[field];
                            facet.values.reserve(counts.size());
                            for (auto&& [val, count] : counts) {
                                facet.values.push_back(
                                    {std::move(val), "", count, false}); // display set later
                            }
                            localFacets.push_back(std::move(facet));
                        }
                    }

                    // 2. Highlights & Snippets (In-place modification)
                    // Must run highlights BEFORE snippets (truncation)
                    for (size_t idx = start; idx < end; ++idx) {
                        auto& item = result.filteredResults[idx];

                        // Generate highlights (reads contentPreview)
                        if (!queryTerms.empty()) {
                            // Logic from generateHighlightsParallel inlined/adapted for single item
                            generateHighlightsForItem(item, queryTerms, maxHighlights);
                        }

                        // Generate snippets (modifies contentPreview)
                        generateSnippetForItem(item, snippetLength);
                    }

                    return localFacets;
                }));
        }

        // Aggregate results
        std::unordered_map<std::string, std::unordered_map<std::string, size_t>> mergedCounts;

        for (auto& f : futures) {
            auto chunkFacets = f.get();
            for (const auto& facet : chunkFacets) {
                for (const auto& val : facet.values) {
                    mergedCounts[facet.name][val.value] += val.count;
                }
            }
        }

        // Finalize facets
        finalizeFacetCounts(facetFields, mergedCounts, result.facets);

        result.highlightsGenerated = (queryAst != nullptr);
        result.snippetsGenerated = (snippetLength > 0);

    } else {
        // Sequential path: below threshold, avoid parallel overhead

        // 1. Apply filters
        if (filters && filters->hasFilters()) {
            result.filteredResults = filters->apply(results);
        } else {
            result.filteredResults = std::move(results);
        }

        std::unordered_map<std::string, std::unordered_map<std::string, size_t>> facetCounts;
        if (!facetFields.empty()) {
            facetCounts.reserve(facetFields.size());
        }

        for (auto& item : result.filteredResults) {
            if (!facetFields.empty()) {
                for (const auto& field : facetFields) {
                    std::string value = facetFieldValue(item, field);
                    if (!value.empty()) {
                        facetCounts[field][value]++;
                    }
                }
            }
            if (!queryTerms.empty()) {
                generateHighlightsForItem(item, queryTerms, maxHighlights);
            }
            generateSnippetForItem(item, snippetLength);
        }

        if (!facetFields.empty()) {
            finalizeFacetCounts(facetFields, facetCounts, result.facets);
        }
        result.highlightsGenerated = !queryTerms.empty();
        result.snippetsGenerated = (snippetLength > 0);
    }

    return result;
}

std::vector<SearchFacet>
ParallelPostProcessor::generateFacetsParallel(const std::vector<SearchResultItem>& results,
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
            std::string value = facetFieldValue(result, fieldName);

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

void ParallelPostProcessor::generateHighlightsParallel(std::vector<SearchResultItem>& results,
                                                       const std::vector<std::string>& queryTerms,
                                                       size_t maxHighlights) {
    for (auto& result : results) {
        generateHighlightsForItem(result, queryTerms, maxHighlights);
    }
}

void ParallelPostProcessor::generateSnippetsParallel(std::vector<SearchResultItem>& results,
                                                     size_t snippetLength) {
    for (auto& result : results) {
        generateSnippetForItem(result, snippetLength);
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
