#include <catch2/catch_test_macros.hpp>
#include <yams/search/search_results.h>

#include <chrono>
#include <ctime>
#include <string>

using namespace yams::search;
using yams::DocumentId;

namespace {

std::chrono::system_clock::time_point tpFromTimeT(std::time_t t) {
    return std::chrono::system_clock::from_time_t(t);
}

SearchResultItem makeItem(std::string title, float relevance,
                          std::chrono::system_clock::time_point lastModified,
                          DocumentId id = DocumentId{0}) {
    SearchResultItem item;
    item.documentId = id;
    item.title = std::move(title);
    item.relevanceScore = relevance;
    item.lastModified = lastModified;
    return item;
}

SearchResults makeFiveItems() {
    SearchResults results;
    results.addItem(makeItem("t0", 0.0f, tpFromTimeT(1000), DocumentId{0}));
    results.addItem(makeItem("t1", 0.1f, tpFromTimeT(1001), DocumentId{1}));
    results.addItem(makeItem("t2", 0.2f, tpFromTimeT(1002), DocumentId{2}));
    results.addItem(makeItem("t3", 0.3f, tpFromTimeT(1003), DocumentId{3}));
    results.addItem(makeItem("t4", 0.4f, tpFromTimeT(1004), DocumentId{4}));
    return results;
}

} // namespace

TEST_CASE("SearchResults::sortByRelevance sorts descending relevanceScore",
          "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("low", 0.1f, tpFromTimeT(1000)));
    results.addItem(makeItem("high", 0.9f, tpFromTimeT(1000)));
    results.addItem(makeItem("mid", 0.5f, tpFromTimeT(1000)));

    results.sortByRelevance();

    const auto& items = results.getItems();
    REQUIRE(items.size() == 3);
    REQUIRE(items[0].title == "high");
    REQUIRE(items[1].title == "mid");
    REQUIRE(items[2].title == "low");
    REQUIRE(items[0].relevanceScore >= items[1].relevanceScore);
    REQUIRE(items[1].relevanceScore >= items[2].relevanceScore);
}

TEST_CASE("SearchResults::sortByRelevance keeps already-sorted order",
          "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("a", 0.9f, tpFromTimeT(1000)));
    results.addItem(makeItem("b", 0.5f, tpFromTimeT(1000)));
    results.addItem(makeItem("c", 0.1f, tpFromTimeT(1000)));

    results.sortByRelevance();

    const auto& items = results.getItems();
    REQUIRE(items.size() == 3);
    REQUIRE(items[0].title == "a");
    REQUIRE(items[1].title == "b");
    REQUIRE(items[2].title == "c");
}

TEST_CASE("SearchResults::sortByDate sorts newest first", "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("old", 0.0f, tpFromTimeT(1000)));
    results.addItem(makeItem("new", 0.0f, tpFromTimeT(3000)));
    results.addItem(makeItem("mid", 0.0f, tpFromTimeT(2000)));

    results.sortByDate();

    const auto& items = results.getItems();
    REQUIRE(items.size() == 3);
    REQUIRE(items[0].title == "new");
    REQUIRE(items[1].title == "mid");
    REQUIRE(items[2].title == "old");
    REQUIRE(items[0].lastModified >= items[1].lastModified);
    REQUIRE(items[1].lastModified >= items[2].lastModified);
}

TEST_CASE("SearchResults::sortByDate leaves single item unchanged", "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("only", 0.0f, tpFromTimeT(1234), DocumentId{42}));

    results.sortByDate();

    const auto& items = results.getItems();
    REQUIRE(items.size() == 1);
    REQUIRE(items[0].title == "only");
    REQUIRE(items[0].documentId == DocumentId{42});
    REQUIRE(items[0].lastModified == tpFromTimeT(1234));
}

TEST_CASE("SearchResults::sortByTitle sorts ascending by title", "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("banana", 0.0f, tpFromTimeT(1000)));
    results.addItem(makeItem("apple", 0.0f, tpFromTimeT(1000)));
    results.addItem(makeItem("carrot", 0.0f, tpFromTimeT(1000)));

    results.sortByTitle();

    const auto& items = results.getItems();
    REQUIRE(items.size() == 3);
    REQUIRE(items[0].title == "apple");
    REQUIRE(items[1].title == "banana");
    REQUIRE(items[2].title == "carrot");
}

TEST_CASE("SearchResults::sortByTitle is case-sensitive", "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("alpha", 0.0f, tpFromTimeT(1000)));
    results.addItem(makeItem("Alpha", 0.0f, tpFromTimeT(1000)));
    results.addItem(makeItem("ALPHA", 0.0f, tpFromTimeT(1000)));

    results.sortByTitle();

    const auto& items = results.getItems();
    REQUIRE(items.size() == 3);
    REQUIRE(items[0].title == "ALPHA");
    REQUIRE(items[1].title == "Alpha");
    REQUIRE(items[2].title == "alpha");
}

TEST_CASE("SearchResults::paginate(0,2) on 5 items returns 2 items", "[search][results][catch2]") {
    auto results = makeFiveItems();

    results.paginate(0, 2);

    const auto& items = results.getItems();
    REQUIRE(items.size() == 2);
    REQUIRE(items[0].title == "t0");
    REQUIRE(items[1].title == "t1");
}

TEST_CASE("SearchResults::paginate(2,2) on 5 items returns offset 2 and 3",
          "[search][results][catch2]") {
    auto results = makeFiveItems();

    results.paginate(2, 2);

    const auto& items = results.getItems();
    REQUIRE(items.size() == 2);
    REQUIRE(items[0].title == "t2");
    REQUIRE(items[1].title == "t3");
}

TEST_CASE("SearchResults::paginate(10,2) on 5 items returns empty", "[search][results][catch2]") {
    auto results = makeFiveItems();

    results.paginate(10, 2);

    REQUIRE(results.getItems().empty());
    REQUIRE(results.size() == 0);
    REQUIRE(results.isEmpty());
}

TEST_CASE("SearchResults::paginate(0,100) on 5 items returns all", "[search][results][catch2]") {
    auto results = makeFiveItems();

    results.paginate(0, 100);

    const auto& items = results.getItems();
    REQUIRE(items.size() == 5);
    REQUIRE(items[0].title == "t0");
    REQUIRE(items[4].title == "t4");
}

TEST_CASE("SearchResults empty results: sort and paginate do not crash",
          "[search][results][catch2]") {
    SearchResults results;
    REQUIRE(results.isEmpty());
    REQUIRE(results.size() == 0);

    results.sortByRelevance();
    results.sortByDate();
    results.sortByTitle();
    results.paginate(0, 10);
    results.paginate(10, 10);

    REQUIRE(results.isEmpty());
    REQUIRE(results.getItems().empty());
}

TEST_CASE("SearchResults single item: all operations work", "[search][results][catch2]") {
    SearchResults results;
    results.addItem(makeItem("only", 0.25f, tpFromTimeT(999), DocumentId{7}));

    results.sortByRelevance();
    results.sortByDate();
    results.sortByTitle();
    REQUIRE(results.size() == 1);
    REQUIRE(results.getItems()[0].title == "only");
    REQUIRE(results.getItems()[0].documentId == DocumentId{7});

    results.paginate(0, 1);
    REQUIRE(results.size() == 1);
    REQUIRE(results.getItems()[0].title == "only");

    results.paginate(0, 100);
    REQUIRE(results.size() == 1);
    REQUIRE(results.getItems()[0].title == "only");
}
