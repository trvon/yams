// Catch2 tests for search smoke
// Migrated from GTest: search_smoke_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/search/search_results.h>

using namespace yams::search;

TEST_CASE("SearchResults container basics", "[search][results][smoke][catch2]") {
    SearchResults results;
    CHECK(results.size() == 0u);

    SearchResultItem item;
    item.documentId = 1;
    SearchHighlight h;
    h.field = "content";
    h.snippet = "hello";
    h.startOffset = 0;
    h.endOffset = 5;
    item.highlights.push_back(h);
    results.addItem(item);

    CHECK(results.size() == 1u);
    auto items = results.getItems();
    REQUIRE(items.size() == 1u);
    CHECK(items[0].documentId == 1);
}
