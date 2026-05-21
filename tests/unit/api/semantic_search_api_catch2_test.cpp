// Focused unit coverage for semantic search API value types and utilities.

#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <yams/api/semantic_search_api.h>

#include <algorithm>
#include <string>
#include <vector>

namespace yams::api::test {

TEST_CASE("SemanticSearchAPI request and filter validation covers edge filters",
          "[api][semantic][validation]") {
    DocumentMetadata metadata;
    CHECK(metadata.isEmpty());
    metadata.title = "Indexed note";
    CHECK_FALSE(metadata.isEmpty());

    SearchFilters filters;
    CHECK_FALSE(filters.hasFilters());
    filters.authors.push_back("alice");
    CHECK(filters.hasFilters());

    filters = SearchFilters{};
    filters.min_word_count = 10;
    CHECK(filters.hasFilters());

    filters = SearchFilters{};
    filters.user_id = "user-1";
    CHECK(filters.hasFilters());

    filters = SearchFilters{};
    filters.custom_predicate = [](const DocumentMetadata& doc) { return !doc.title.empty(); };
    CHECK(filters.hasFilters());

    SearchRequest request;
    request.query = "semantic search";
    CHECK(request.isValid());

    request.query = "   \t\n";
    CHECK_FALSE(request.isValid());

    request.query = "semantic search";
    request.results_per_page = 101;
    CHECK_FALSE(request.isValid());

    request.results_per_page = 10;
    request.page = 0;
    CHECK_FALSE(request.isValid());
}

TEST_CASE("SemanticSearchAPI query utilities normalize, classify, and extract entities",
          "[api][semantic][query-utils]") {
    using namespace query_utils;

    CHECK(normalizeQuery("  Find   YAMS Docs  ") == "find yams docs");

    const auto keywords = extractKeywords("a ai vector 3d code");
    CHECK(keywords == std::vector<std::string>{"ai", "vector", "code"});

    CHECK(detectLanguage("") == "unknown");
    CHECK(detectLanguage("semantic search") == "en");
    CHECK(detectLanguage("\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E") == "unknown");

    const auto entities = extractEntities("Visit https://example.test and email ops@example.test");
    REQUIRE(entities.size() == 2);
    CHECK(entities[0].type == "URL");
    CHECK(entities[0].text == "https://example.test");
    CHECK(entities[0].confidence == Catch::Approx(0.85f));
    CHECK(entities[1].type == "EMAIL");
    CHECK(entities[1].text == "ops@example.test");

    CHECK(classifyIntent("what is hybrid search") == QueryIntent::INFORMATIONAL);
    CHECK(classifyIntent("where is the config file") == QueryIntent::NAVIGATIONAL);
    CHECK(classifyIntent("download model weights") == QueryIntent::TRANSACTIONAL);
    CHECK(classifyIntent("compare vector vs keyword search") == QueryIntent::COMPARATIVE);

    CHECK_FALSE(isValidQuery(""));
    CHECK_FALSE(isValidQuery("  \n\t"));
    CHECK_FALSE(isValidQuery(std::string(1001, 'x')));
    CHECK(isValidQuery("find docs"));

    CHECK(calculateComplexity("") == Catch::Approx(0.0f));
    CHECK(calculateComplexity("simple query") > 0.0f);
    CHECK(calculateComplexity("field:value + filter? with punctuation!") <= 1.0f);
}

TEST_CASE("SemanticSearchAPI result utilities format snippets and summaries",
          "[api][semantic][result-utils]") {
    using namespace result_utils;

    CHECK(generateSnippets("", {"term"}).empty());
    CHECK(generateSnippets("some content", {}).empty());

    const auto noMatch = generateSnippets("abcdef ghijkl", {"missing"}, 3, 6);
    REQUIRE(noMatch.size() == 1);
    CHECK(noMatch[0].text == "abcdef");
    CHECK(noMatch[0].start_position == 0);
    CHECK(noMatch[0].end_position == 6);
    CHECK(noMatch[0].relevance_score == Catch::Approx(0.1f));

    const std::string content = "alpha beta gamma alpha beta gamma alpha beta gamma";
    const auto matches = generateSnippets(content, {"beta"}, 2, 14);
    REQUIRE_FALSE(matches.empty());
    CHECK(matches.size() <= 2);
    CHECK(matches[0].text.find("beta") != std::string::npos);
    CHECK_FALSE(matches[0].highlighted_ranges.empty());
    CHECK(matches[0].start_position < matches[0].end_position);

    CHECK(highlightTerms("Alpha beta alpha", {"alpha"}, "[", "]") == "[Alpha] beta [alpha]");
    CHECK(highlightTerms("a+b and a+b", {"a+b"}, "<b>", "</b>") == "<b>a+b</b> and <b>a+b</b>");

    CHECK(generateSummary("short content", 100) == "short content");
    CHECK(generateSummary("abcdefghijklmnopqrstuvwxyz", 10) == "abcdefghij...");
}

} // namespace yams::api::test
