#include <catch2/catch_test_macros.hpp>

#include <yams/search/parallel_post_processor.hpp>
#include <yams/search/search_execution_context.h>

#include <string>
#include <vector>

using namespace yams::search;

namespace {

std::vector<SearchResultItem> makeItems(size_t count) {
    std::vector<SearchResultItem> items;
    items.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        SearchResultItem item;
        item.documentId = static_cast<int64_t>(i + 1);
        item.title = "Doc " + std::to_string(i);
        item.path = (i % 2 == 0) ? "src/file" + std::to_string(i) + ".cpp"
                                 : "docs/file" + std::to_string(i) + ".md";
        item.contentType = (i % 3 == 0) ? "text/plain" : "text/markdown";
        item.detectedLanguage = (i % 2 == 0) ? "cpp" : "markdown";
        item.contentPreview = "tuning manager content preview " + std::to_string(i);
        item.relevanceScore = 1.0f - static_cast<float>(i) * 0.001f;
        items.push_back(std::move(item));
    }
    return items;
}

SearchExecutionContext makeContext(std::uint32_t active, std::uint32_t limit,
                                   bool allowApproximateFacets = true,
                                   std::uint32_t recommendedWorkers = 4) {
    SearchExecutionContext context;
    context.activeRequests = active;
    context.queuedRequests = 0;
    context.concurrencyLimit = limit;
    context.recommendedWorkers = recommendedWorkers;
    context.allowApproximateFacets = allowApproximateFacets;
    return context;
}

} // namespace

TEST_CASE("ParallelPostProcessor keeps local exact facet processing by default",
          "[search][postprocessor][catch2]") {
    auto result =
        ParallelPostProcessor::process(makeItems(1100), nullptr, {"extension"}, nullptr, 0, 0);

    REQUIRE_FALSE(result.facetsApproximate);
    REQUIRE(result.facetInputCount == 1100);
    REQUIRE(result.facetSampleCount == 1100);
    REQUIRE(result.workersUsed >= 1);
    REQUIRE(result.facets.size() == 1);
}

TEST_CASE("ParallelPostProcessor approximates facet-only sampling under medium pressure",
          "[search][postprocessor][catch2]") {
    SearchExecutionContextGuard guard(makeContext(2, 4));

    auto result =
        ParallelPostProcessor::process(makeItems(1200), nullptr, {"extension"}, nullptr, 0, 0);

    REQUIRE(result.facetsApproximate);
    REQUIRE(result.facetInputCount == 1200);
    REQUIRE(result.facetSampleCount == 512);
    REQUIRE(result.workersUsed == 2);
    REQUIRE(result.facets.size() == 1);
}

TEST_CASE("ParallelPostProcessor collapses to single worker under high pressure",
          "[search][postprocessor][catch2]") {
    SearchExecutionContextGuard guard(makeContext(4, 4));

    auto result =
        ParallelPostProcessor::process(makeItems(1200), nullptr, {"extension"}, nullptr, 0, 0);

    REQUIRE(result.facetsApproximate);
    REQUIRE(result.facetSampleCount == 256);
    REQUIRE(result.workersUsed == 1);
}

TEST_CASE("ParallelPostProcessor does not approximate when snippet generation is requested",
          "[search][postprocessor][catch2]") {
    SearchExecutionContextGuard guard(makeContext(2, 4));

    auto result =
        ParallelPostProcessor::process(makeItems(1200), nullptr, {"extension"}, nullptr, 24, 0);

    REQUIRE_FALSE(result.facetsApproximate);
    REQUIRE(result.facetSampleCount == 1200);
    REQUIRE(result.snippetsGenerated);
    REQUIRE(result.filteredResults.front().contentPreview.size() <= 27);
}
