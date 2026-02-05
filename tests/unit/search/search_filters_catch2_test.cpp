#include <catch2/catch_test_macros.hpp>
#include <yams/search/search_filters.h>

#include <chrono>
#include <string>
#include <unordered_map>

#include <utility>
#include <vector>

using namespace yams::search;

namespace {

using Clock = std::chrono::system_clock;

SearchResultItem makeDefaultItem(Clock::time_point now, std::string path = "docs/readme.md") {
    SearchResultItem item{};
    item.documentId = 1;
    item.title = "Default Title";
    item.path = std::move(path);
    item.contentType = "text/plain";
    item.fileSize = 1024;
    item.lastModified = now;
    item.indexedAt = now;
    item.relevanceScore = 0.5f;
    item.detectedLanguage = "cpp";
    item.languageConfidence = 0.9f;
    item.metadata = {{"author", "alice"}, {"tag", "unit"}};
    return item;
}

std::vector<std::string> pathsOf(const std::vector<SearchResultItem>& items) {
    std::vector<std::string> out;
    out.reserve(items.size());
    for (const auto& item : items) {
        out.push_back(item.path);
    }
    return out;
}

} // namespace

TEST_CASE("DateRangeFilter matches returns true when date is within range",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    DateRangeFilter f;
    f.from = now - std::chrono::hours(1);
    f.to = now + std::chrono::hours(1);

    REQUIRE(f.matches(now));
}

TEST_CASE("DateRangeFilter matches returns false when date is before from",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    DateRangeFilter f;
    f.from = now;

    REQUIRE_FALSE(f.matches(now - std::chrono::seconds(1)));
}

TEST_CASE("DateRangeFilter matches returns false when date is after to",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    DateRangeFilter f;
    f.to = now;

    REQUIRE_FALSE(f.matches(now + std::chrono::seconds(1)));
}

TEST_CASE("DateRangeFilter matches returns true when no bounds set",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    DateRangeFilter f;
    REQUIRE(f.matches(now - std::chrono::hours(24)));
    REQUIRE(f.matches(now));
    REQUIRE(f.matches(now + std::chrono::hours(24)));
}

TEST_CASE("SizeRangeFilter matches within range", "[search][filters][catch2]") {
    SizeRangeFilter f;
    f.minSize = 10;
    f.maxSize = 20;
    REQUIRE(f.matches(10));
    REQUIRE(f.matches(15));
    REQUIRE(f.matches(20));
}

TEST_CASE("SizeRangeFilter rejects below minSize", "[search][filters][catch2]") {
    SizeRangeFilter f;
    f.minSize = 10;
    REQUIRE_FALSE(f.matches(9));
}

TEST_CASE("SizeRangeFilter rejects above maxSize", "[search][filters][catch2]") {
    SizeRangeFilter f;
    f.maxSize = 20;
    REQUIRE_FALSE(f.matches(21));
}

TEST_CASE("SizeRangeFilter accepts when no bounds set", "[search][filters][catch2]") {
    SizeRangeFilter f;
    REQUIRE(f.matches(0));
    REQUIRE(f.matches(123456));
}

TEST_CASE("ContentTypeFilter allowedTypes filter passes matching type",
          "[search][filters][catch2]") {
    ContentTypeFilter f;
    f.allowedTypes.insert("text/plain");
    REQUIRE(f.matches("text/plain"));
}

TEST_CASE("ContentTypeFilter allowedTypes filter rejects non-matching type",
          "[search][filters][catch2]") {
    ContentTypeFilter f;
    f.allowedTypes.insert("text/plain");
    REQUIRE_FALSE(f.matches("application/pdf"));
}

TEST_CASE("ContentTypeFilter excludedTypes filter rejects matching type",
          "[search][filters][catch2]") {
    ContentTypeFilter f;
    f.excludedTypes.insert("application/pdf");
    REQUIRE_FALSE(f.matches("application/pdf"));
}

TEST_CASE("ContentTypeFilter no filters accepts everything", "[search][filters][catch2]") {
    ContentTypeFilter f;
    REQUIRE(f.matches("text/plain"));
    REQUIRE(f.matches("application/pdf"));
    REQUIRE(f.matches(""));
}

TEST_CASE("LanguageFilter matches allowed language with sufficient confidence",
          "[search][filters][catch2]") {
    LanguageFilter f;
    f.allowedLanguages.insert("cpp");
    f.minConfidence = 0.8f;
    REQUIRE(f.matches("cpp", 0.9f));
}

TEST_CASE("LanguageFilter rejects excluded language", "[search][filters][catch2]") {
    LanguageFilter f;
    f.excludedLanguages.insert("python");
    REQUIRE_FALSE(f.matches("python", 1.0f));
}

TEST_CASE("LanguageFilter rejects when confidence below minConfidence",
          "[search][filters][catch2]") {
    LanguageFilter f;
    f.allowedLanguages.insert("cpp");
    f.minConfidence = 0.8f;
    REQUIRE_FALSE(f.matches("cpp", 0.79f));
}

TEST_CASE("PathFilter excludePaths rejects matching path", "[search][filters][catch2]") {
    PathFilter f;
    f.excludePaths = {"build/"};
    REQUIRE_FALSE(f.matches("src/build/output.o"));
}

TEST_CASE("PathFilter includePaths accepts matching path", "[search][filters][catch2]") {
    PathFilter f;
    f.includePaths = {"src/"};
    REQUIRE(f.matches("src/main.cpp"));
}

TEST_CASE("PathFilter includePaths rejects non-matching path", "[search][filters][catch2]") {
    PathFilter f;
    f.includePaths = {"src/"};
    REQUIRE_FALSE(f.matches("docs/readme.md"));
}

TEST_CASE("PathFilter no paths accepts everything", "[search][filters][catch2]") {
    PathFilter f;
    REQUIRE(f.matches("src/main.cpp"));
    REQUIRE(f.matches("docs/readme.md"));
}

TEST_CASE("MetadataFilter matches when field value is in allowedValues",
          "[search][filters][catch2]") {
    MetadataFilter f;
    f.fieldName = "author";
    f.allowedValues = {"alice", "bob"};

    std::unordered_map<std::string, std::string> md{{"author", "alice"}};
    REQUIRE(f.matches(md));
}

TEST_CASE("MetadataFilter rejects when field value is in excludedValues",
          "[search][filters][catch2]") {
    MetadataFilter f;
    f.fieldName = "tag";
    f.excludedValues = {"secret"};

    std::unordered_map<std::string, std::string> md{{"tag", "secret"}};
    REQUIRE_FALSE(f.matches(md));
}

TEST_CASE("MetadataFilter missing field allows if allowedValues empty, rejects if not",
          "[search][filters][catch2]") {
    std::unordered_map<std::string, std::string> md{{"other", "x"}};

    MetadataFilter allowMissing;
    allowMissing.fieldName = "author";
    REQUIRE(allowMissing.matches(md));

    MetadataFilter requireValue;
    requireValue.fieldName = "author";
    requireValue.allowedValues = {"alice"};
    REQUIRE_FALSE(requireValue.matches(md));
}

TEST_CASE("RelevanceFilter matches within min/max range", "[search][filters][catch2]") {
    RelevanceFilter f;
    f.minScore = 0.2f;
    f.maxScore = 0.8f;
    REQUIRE(f.matches(0.2f));
    REQUIRE(f.matches(0.5f));
    REQUIRE(f.matches(0.8f));
}

TEST_CASE("RelevanceFilter rejects below min or above max", "[search][filters][catch2]") {
    RelevanceFilter f;
    f.minScore = 0.2f;
    f.maxScore = 0.8f;
    REQUIRE_FALSE(f.matches(0.19f));
    REQUIRE_FALSE(f.matches(0.81f));
}

TEST_CASE("SearchFilters composite no filters matches everything and apply returns all items",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    const auto a = makeDefaultItem(now, "src/a.cpp");
    const auto b = makeDefaultItem(now, "docs/b.txt");

    SearchFilters filters;
    REQUIRE(filters.matches(a));
    REQUIRE(filters.matches(b));

    const std::vector<SearchResultItem> items{a, b};
    const auto out = filters.apply(items);
    REQUIRE(pathsOf(out) == pathsOf(items));
}

TEST_CASE("SearchFilters composite single filter applied correctly", "[search][filters][catch2]") {
    const auto now = Clock::now();
    const auto a = makeDefaultItem(now, "src/a.cpp");
    auto b = makeDefaultItem(now, "docs/b.txt");
    b.contentType = "application/pdf";

    SearchFilters filters;
    ContentTypeFilter ct;
    ct.allowedTypes.insert("text/plain");
    filters.addContentTypeFilter(std::move(ct));

    REQUIRE(filters.matches(a));
    REQUIRE_FALSE(filters.matches(b));

    const std::vector<SearchResultItem> items{a, b};
    const auto out = filters.apply(items);
    REQUIRE(pathsOf(out) == std::vector<std::string>{"src/a.cpp"});
}

TEST_CASE("SearchFilters composite multiple filters are AND'd - all must pass",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    const auto a = makeDefaultItem(now, "src/a.cpp");
    auto b = makeDefaultItem(now, "docs/b.txt");
    b.contentType = "application/pdf";
    b.fileSize = 50;

    SearchFilters filters;

    ContentTypeFilter ct;
    ct.allowedTypes.insert("text/plain");
    filters.addContentTypeFilter(std::move(ct));

    SizeRangeFilter sz;
    sz.minSize = 100;
    filters.addSizeRangeFilter(std::move(sz));

    REQUIRE(filters.matches(a));
    REQUIRE_FALSE(filters.matches(b));

    auto c = makeDefaultItem(now, "src/c.cpp");
    c.fileSize = 10; // fails size
    REQUIRE_FALSE(filters.matches(c));
}

TEST_CASE("SearchFilters composite custom filter lambda works", "[search][filters][catch2]") {
    const auto now = Clock::now();
    const auto base = makeDefaultItem(now, "src/a.cpp");

    SearchFilters filters;
    filters.addCustomFilter([](const SearchResultItem& item) {
        return item.title.find("ok") != std::string::npos;
    });

    auto okItem = base;
    okItem.title = "ok - match";
    auto badItem = base;
    badItem.title = "nope";

    REQUIRE(filters.matches(okItem));
    REQUIRE_FALSE(filters.matches(badItem));
}

TEST_CASE("SearchFilters::apply filters a vector correctly (some pass, some don't)",
          "[search][filters][catch2]") {
    const auto now = Clock::now();

    auto a = makeDefaultItem(now, "src/a.cpp");
    a.contentType = "text/plain";
    a.relevanceScore = 0.5f;

    auto b = makeDefaultItem(now, "src/b.pdf");
    b.contentType = "application/pdf";
    b.relevanceScore = 0.9f;

    auto c = makeDefaultItem(now, "src/c.txt");
    c.contentType = "text/plain";
    c.relevanceScore = 0.1f;

    SearchFilters filters;

    ContentTypeFilter ct;
    ct.allowedTypes.insert("text/plain");
    filters.addContentTypeFilter(std::move(ct));

    RelevanceFilter rel;
    rel.minScore = 0.2f;
    rel.maxScore = 1.0f;
    filters.addRelevanceFilter(std::move(rel));

    const std::vector<SearchResultItem> items{a, b, c};
    const auto out = filters.apply(items);
    REQUIRE(pathsOf(out) == std::vector<std::string>{"src/a.cpp"});
}

TEST_CASE("SearchFilters::apply empty filter returns original vector unchanged",
          "[search][filters][catch2]") {
    const auto now = Clock::now();
    const std::vector<SearchResultItem>
        items{makeDefaultItem(now, "src/a.cpp"), makeDefaultItem(now, "src/b.cpp")};

    SearchFilters filters;
    const auto out = filters.apply(items);
    REQUIRE(pathsOf(out) == pathsOf(items));
}
