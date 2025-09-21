#include <gtest/gtest.h>
#include <yams/search/bk_tree.h>

using namespace yams::search;

TEST(BKTreeSmoke, BasicAddSearch) {
    BKTree tree{std::make_unique<LevenshteinDistance>()};
    tree.addBatch({"hello", "hallo", "hullo", "world"});
    auto res = tree.search("hello", 1);
    ASSERT_FALSE(res.empty());
    // Ensure "hello" at distance 0 is present
    bool foundExact = false;
    for (auto& p : res) {
        if (p.first == std::string("hello") && p.second == 0) {
            foundExact = true;
            break;
        }
    }
    EXPECT_TRUE(foundExact);
}
