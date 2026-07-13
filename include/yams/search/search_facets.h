#pragma once

#include <yams/search/search_results.h>

#include <cstddef>
#include <span>
#include <string>
#include <vector>

namespace yams::search {

struct FacetAggregationResult {
    std::vector<SearchFacet> facets;
    std::size_t workersUsed{0};
    bool facetsApproximate{false};
    std::size_t facetInputCount{0};
    std::size_t facetSampleCount{0};
};

[[nodiscard]] FacetAggregationResult
aggregateSearchFacets(std::span<const SearchResultItem> results,
                      std::span<const std::string> facetFields);

} // namespace yams::search
