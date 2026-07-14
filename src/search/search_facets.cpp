#include <yams/search/search_facets.h>

#include <yams/search/search_execution_context.h>

#include <algorithm>
#include <limits>
#include <string_view>
#include <unordered_map>

namespace yams::search {

namespace {

using FacetCounts = std::unordered_map<std::string, std::size_t>;

std::string facetFieldValue(const SearchResultItem& item, std::string_view field) {
    if (field == "contentType") {
        return item.contentType;
    }
    if (field == "language") {
        return item.detectedLanguage;
    }
    if (field == "extension") {
        const auto pos = item.path.rfind('.');
        return pos == std::string::npos ? std::string{} : item.path.substr(pos);
    }
    if (const auto it = item.metadata.find(std::string{field}); it != item.metadata.end()) {
        return it->second;
    }
    return {};
}

std::size_t facetSampleCap(const SearchExecutionContext& context) noexcept {
    const auto concurrencyLimit = std::max<std::uint32_t>(1, context.concurrencyLimit);
    const bool highPressure =
        context.activeRequests >= concurrencyLimit || context.activeRequests >= 4;
    if (highPressure) {
        return 256;
    }
    const bool mediumPressure =
        context.activeRequests >= 2 || (context.activeRequests * 2) >= concurrencyLimit;
    return mediumPressure ? 512 : std::numeric_limits<std::size_t>::max();
}

SearchFacet buildFacet(std::string field, const FacetCounts& counts) {
    SearchFacet facet;
    facet.name = field;
    facet.displayName = std::move(field);
    facet.values.reserve(counts.size());
    for (const auto& [value, count] : counts) {
        facet.values.push_back({value, value, count, false});
    }
    std::ranges::sort(facet.values, [](const auto& lhs, const auto& rhs) {
        if (lhs.count != rhs.count) {
            return lhs.count > rhs.count;
        }
        return lhs.value < rhs.value;
    });
    facet.totalValues = facet.values.size();
    return facet;
}

} // namespace

FacetAggregationResult aggregateSearchFacets(std::span<const SearchResultItem> results,
                                             std::span<const std::string> facetFields) {
    FacetAggregationResult out;
    if (facetFields.empty()) {
        return out;
    }

    out.facetInputCount = results.size();
    if (results.empty()) {
        return out;
    }

    const auto context = currentSearchExecutionContext();
    const auto cap = context.allowApproximateFacets ? facetSampleCap(context)
                                                    : std::numeric_limits<std::size_t>::max();
    out.facetSampleCount = std::min(results.size(), cap);
    out.facetsApproximate = out.facetSampleCount < results.size();
    out.workersUsed = 1;

    out.facets.reserve(facetFields.size());
    for (const auto& field : facetFields) {
        FacetCounts counts;
        for (const auto& item : results.first(out.facetSampleCount)) {
            auto value = facetFieldValue(item, field);
            if (!value.empty()) {
                ++counts[std::move(value)];
            }
        }
        if (!counts.empty()) {
            out.facets.push_back(buildFacet(field, counts));
        }
    }
    return out;
}

} // namespace yams::search
