#include <yams/search/parallel_post_processor.hpp>

#include <algorithm>
#include <string>
#include <unordered_map>

#include <yams/search/result_ranker.h>
#include <yams/search/search_execution_context.h>

namespace yams {
namespace search {

namespace {

using FacetCountMap = std::unordered_map<std::string, std::unordered_map<std::string, size_t>>;

struct PressurePolicy {
    size_t workers{1};
    size_t facetCap{1024};
    bool highPressure{false};
    bool mediumPressure{false};
};

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

void accumulateFacetCounts(FacetCountMap& fieldCounts, const SearchResultItem& item,
                           const std::vector<std::string>& facetFields) {
    for (const auto& field : facetFields) {
        std::string value = facetFieldValue(item, field);
        if (!value.empty()) {
            fieldCounts[field][value]++;
        }
    }
}

void finalizeFacetCounts(const std::vector<std::string>& facetFields,
                         const FacetCountMap& mergedCounts, std::vector<SearchFacet>& facets) {
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

PressurePolicy determinePressurePolicy(const SearchExecutionContext& context) {
    PressurePolicy policy;
    const size_t concurrencyLimit = std::max<size_t>(1, context.concurrencyLimit);
    const bool highPressure =
        context.activeRequests >= concurrencyLimit || context.activeRequests >= 4;
    const bool mediumPressure = !highPressure && (context.activeRequests >= 2 ||
                                                  (context.activeRequests * 2) >= concurrencyLimit);
    policy.highPressure = highPressure;
    policy.mediumPressure = mediumPressure;
    if (highPressure) {
        policy.workers = 1;
        policy.facetCap = 256;
    } else if (mediumPressure) {
        policy.workers = 2;
        policy.facetCap = 512;
    } else {
        policy.workers = std::min<size_t>(4, std::max<size_t>(1, context.recommendedWorkers));
        policy.facetCap = 1024;
    }
    return policy;
}

} // namespace

ParallelPostProcessor::ProcessingResult
ParallelPostProcessor::process(std::vector<SearchResultItem> results, const SearchFilters* filters,
                               const std::vector<std::string>& facetFields,
                               const QueryNode* queryAst, size_t snippetLength,
                               size_t maxHighlights) {
    ProcessingResult result;
    const bool facetOnlyMode = !facetFields.empty() && queryAst == nullptr && snippetLength == 0;
    const SearchExecutionContext context = currentSearchExecutionContext();
    const PressurePolicy pressure = determinePressurePolicy(context);
    const std::vector<std::string> queryTerms =
        queryAst ? extractQueryTerms(queryAst) : std::vector<std::string>{};

    if (filters && filters->hasFilters()) {
        result.filteredResults = filters->apply(results);
    } else {
        result.filteredResults = std::move(results);
    }

    const size_t totalResults = result.filteredResults.size();
    result.highlightsGenerated = !queryTerms.empty();
    result.snippetsGenerated = (snippetLength > 0);
    result.facetInputCount = facetFields.empty() ? 0 : totalResults;

    if (totalResults == 0) {
        return result;
    }

    size_t facetSampleCount = totalResults;
    if (!facetFields.empty() && facetOnlyMode && context.allowApproximateFacets &&
        totalResults > pressure.facetCap) {
        facetSampleCount = pressure.facetCap;
        result.facetsApproximate = true;
    }
    result.facetSampleCount = facetFields.empty() ? 0 : facetSampleCount;

    const size_t threshold = facetOnlyMode ? FACET_ONLY_PARALLEL_THRESHOLD : PARALLEL_THRESHOLD;
    const size_t workItemCount = std::max(totalResults, facetSampleCount);
    const bool useParallel = workItemCount >= threshold && pressure.workers > 1;

    if (!useParallel) {
        FacetCountMap facetCounts;
        if (!facetFields.empty()) {
            facetCounts.reserve(facetFields.size());
        }

        for (size_t idx = 0; idx < totalResults; ++idx) {
            auto& item = result.filteredResults[idx];
            if (idx < facetSampleCount && !facetFields.empty()) {
                accumulateFacetCounts(facetCounts, item, facetFields);
            }
            if (!queryTerms.empty()) {
                generateHighlightsForItem(item, queryTerms, maxHighlights);
            }
            generateSnippetForItem(item, snippetLength);
        }

        if (!facetFields.empty()) {
            finalizeFacetCounts(facetFields, facetCounts, result.facets);
        }
        result.workersUsed = 1;
        return result;
    }

    const size_t suggestedTasks = std::max<size_t>(1, (workItemCount + threshold - 1) / threshold);
    const size_t numTasks = std::min<size_t>(pressure.workers, suggestedTasks);
    const size_t chunkSize = (totalResults + numTasks - 1) / numTasks;
    std::vector<FacetCountMap> chunkCounts;
    chunkCounts.reserve(numTasks);

    for (size_t i = 0; i < numTasks; ++i) {
        const size_t start = i * chunkSize;
        const size_t end = std::min(start + chunkSize, totalResults);
        if (start >= end) {
            break;
        }
        FacetCountMap localCounts;
        if (!facetFields.empty()) {
            localCounts.reserve(facetFields.size());
        }

        for (size_t idx = start; idx < end; ++idx) {
            auto& item = result.filteredResults[idx];
            if (idx < facetSampleCount && !facetFields.empty()) {
                accumulateFacetCounts(localCounts, item, facetFields);
            }
            if (!queryTerms.empty()) {
                generateHighlightsForItem(item, queryTerms, maxHighlights);
            }
            generateSnippetForItem(item, snippetLength);
        }
        chunkCounts.push_back(std::move(localCounts));
    }

    FacetCountMap mergedCounts;
    if (!facetFields.empty()) {
        mergedCounts.reserve(facetFields.size());
    }
    for (auto& chunkCount : chunkCounts) {
        for (auto& [field, counts] : chunkCount) {
            auto& mergedField = mergedCounts[field];
            for (auto& [value, count] : counts) {
                mergedField[value] += count;
            }
        }
    }

    if (!facetFields.empty()) {
        finalizeFacetCounts(facetFields, mergedCounts, result.facets);
    }
    result.workersUsed = chunkCounts.empty() ? 1 : chunkCounts.size();
    return result;
}

std::vector<SearchFacet>
ParallelPostProcessor::generateFacetsParallel(const std::vector<SearchResultItem>& results,
                                              const std::vector<std::string>& facetFields) {
    std::vector<SearchFacet> facets;
    FacetCountMap facetCounts;
    facetCounts.reserve(facetFields.size());
    for (const auto& result : results) {
        accumulateFacetCounts(facetCounts, result, facetFields);
    }
    finalizeFacetCounts(facetFields, facetCounts, facets);
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
    if (!queryAst) {
        return {};
    }

    ResultRanker ranker;
    return ranker.extractQueryTerms(queryAst);
}

} // namespace search
} // namespace yams
