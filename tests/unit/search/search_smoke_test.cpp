// Simple search smoke compatible with current APIs
#include <gtest/gtest.h>
#include <yams/search/search_results.h>

using namespace yams::search;

TEST(SearchSmoke, ResultsContainerBasics) {
    SearchResults results;
    EXPECT_EQ(results.size(), 0u);

    SearchResultItem item;
    item.documentId = 1;
    SearchHighlight h;
    h.field = "content";
    h.snippet = "hello";
    h.startOffset = 0;
    h.endOffset = 5;
    item.highlights.push_back(h);
    results.addItem(item);

    EXPECT_EQ(results.size(), 1u);
    auto items = results.getItems();
    ASSERT_EQ(items.size(), 1u);
    EXPECT_EQ(items[0].documentId, 1);
}
