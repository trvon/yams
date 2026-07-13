#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_execution_context.h>
#include <yams/search/search_facets.h>

#include <cstdint>
#include <string>
#include <vector>

using namespace yams::search;

namespace {

std::vector<SearchResultItem> makeFacetItems(std::size_t count) {
    std::vector<SearchResultItem> items;
    items.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        SearchResultItem item;
        item.documentId = static_cast<std::int64_t>(i + 1);
        item.path = (i % 2 == 0) ? "src/file" + std::to_string(i) + ".cpp"
                                 : "docs/file" + std::to_string(i) + ".md";
        items.push_back(std::move(item));
    }
    return items;
}

SearchExecutionContext makeContext(std::uint32_t active, std::uint32_t limit) {
    SearchExecutionContext context;
    context.activeRequests = active;
    context.concurrencyLimit = limit;
    context.recommendedWorkers = 4;
    context.allowApproximateFacets = true;
    return context;
}

} // namespace

TEST_CASE("Facet aggregation is exact without a pressure budget", "[search][facets][catch2]") {
    const auto items = makeFacetItems(1100);
    const std::vector<std::string> fields{"extension"};

    const auto result = aggregateSearchFacets(items, fields);

    CHECK_FALSE(result.facetsApproximate);
    CHECK(result.facetInputCount == 1100);
    CHECK(result.facetSampleCount == 1100);
    CHECK(result.workersUsed == 1);
    REQUIRE(result.facets.size() == 1);
    CHECK(result.facets.front().totalValues == 2);
}

TEST_CASE("Facet aggregation bounds work under request pressure", "[search][facets][catch2]") {
    const auto items = makeFacetItems(1200);
    const std::vector<std::string> fields{"extension"};

    SECTION("medium pressure") {
        SearchExecutionContextGuard guard(makeContext(2, 4));
        const auto result = aggregateSearchFacets(items, fields);
        CHECK(result.facetsApproximate);
        CHECK(result.facetInputCount == 1200);
        CHECK(result.facetSampleCount == 512);
        CHECK(result.workersUsed == 1);
    }

    SECTION("high pressure") {
        SearchExecutionContextGuard guard(makeContext(4, 4));
        const auto result = aggregateSearchFacets(items, fields);
        CHECK(result.facetsApproximate);
        CHECK(result.facetInputCount == 1200);
        CHECK(result.facetSampleCount == 256);
        CHECK(result.workersUsed == 1);
    }
}
