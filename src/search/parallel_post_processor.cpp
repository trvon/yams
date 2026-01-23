#include <yams/search/parallel_post_processor.hpp>

#include <algorithm>
#include <future>
#include <string>
#include <unordered_map>
#include <yams/daemon/components/TuneAdvisor.h>
#include <yams/search/result_ranker.h>

namespace yams {
namespace search {

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
                                std::string value;
                                if (field == "contentType")
                                    value = item.contentType;
                                else if (field == "language")
                                    value = item.detectedLanguage;
                                else if (field == "extension") {
                                    auto pos = item.path.rfind('.');
                                    if (pos != std::string::npos) {
                                        value = item.path.substr(pos);
                                    }
                                } else {
                                    auto it = item.metadata.find(field);
                                    if (it != item.metadata.end())
                                        value = it->second;
                                }
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
                            if (!item.contentPreview.empty()) {
                                SearchHighlight h;
                                h.field = "content";
                                h.snippet = item.contentPreview; // Copy before truncate? No,
                                                                 // highlight uses offsets
                                h.startOffset = 0;
                                h.endOffset = item.contentPreview.length();

                                for (const auto& term : queryTerms) {
                                    size_t pos = item.contentPreview.find(term);
                                    if (pos != std::string::npos) {
                                        h.highlights.emplace_back(pos, pos + term.length());
                                    }
                                }
                                if (!h.highlights.empty())
                                    item.highlights.push_back(std::move(h));
                            }

                            // Title highlights
                            for (const auto& term : queryTerms) {
                                size_t pos = item.title.find(term);
                                if (pos != std::string::npos) {
                                    SearchHighlight h;
                                    h.field = "title";
                                    h.snippet = item.title;
                                    h.startOffset = 0;
                                    h.endOffset = item.title.length();
                                    h.highlights.emplace_back(pos, pos + term.length());
                                    item.highlights.push_back(std::move(h));
                                    break;
                                }
                            }

                            if (item.highlights.size() > maxHighlights) {
                                item.highlights.resize(maxHighlights);
                            }
                        }

                        // Generate snippets (modifies contentPreview)
                        if (snippetLength > 0) {
                            if (item.contentPreview.length() > snippetLength) {
                                item.contentPreview =
                                    item.contentPreview.substr(0, snippetLength) + "...";
                            }
                            item.previewLength = item.contentPreview.length();
                        }
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
        for (const auto& field : facetFields) {
            if (mergedCounts.find(field) == mergedCounts.end())
                continue;

            SearchFacet facet;
            facet.name = field;
            facet.displayName = field;
            auto& counts = mergedCounts[field];

            for (const auto& [val, count] : counts) {
                facet.values.push_back({val, val, count, false});
            }

            // Sort
            std::sort(facet.values.begin(), facet.values.end(),
                      [](const auto& a, const auto& b) { return a.count > b.count; });

            facet.totalValues = facet.values.size();
            result.facets.push_back(std::move(facet));
        }

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
            std::string value;

            if (fieldName == "contentType") {
                value = result.contentType;
            } else if (fieldName == "language") {
                value = result.detectedLanguage;
            } else if (fieldName == "extension") {
                auto pos = result.path.rfind('.');
                if (pos != std::string::npos) {
                    value = result.path.substr(pos);
                }
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

void ParallelPostProcessor::generateHighlightsParallel(std::vector<SearchResultItem>& results,
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

void ParallelPostProcessor::generateSnippetsParallel(std::vector<SearchResultItem>& results,
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
