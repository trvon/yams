// Catch2 tests for BK-Tree smoke test
// Migrated from GTest: bk_tree_smoke_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <yams/search/bk_tree.h>

using namespace yams::search;

TEST_CASE("BKTree basic add and search", "[search][bktree][smoke][catch2]") {
    BKTree tree{std::make_unique<LevenshteinDistance>()};
    tree.addBatch({"hello", "hallo", "hullo", "world"});
    auto res = tree.search("hello", 1);
    REQUIRE_FALSE(res.empty());

    // Ensure "hello" at distance 0 is present
    bool foundExact = false;
    for (auto& p : res) {
        if (p.first == std::string("hello") && p.second == 0) {
            foundExact = true;
            break;
        }
    }
    CHECK(foundExact);
}
