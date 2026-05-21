// Focused coverage for pluggable query concept extraction.

#include <catch2/catch_test_macros.hpp>

#include <yams/search/query_concept_extractor.h>

#include <string>
#include <vector>

namespace yams::search::test {

TEST_CASE("QueryConceptExtractor returns empty results without backend",
          "[search][query-concept]") {
    QueryConceptExtractor extractor;

    CHECK_FALSE(extractor.isAvailable());

    auto empty = extractor.extractConcepts("");
    REQUIRE(empty.has_value());
    CHECK(empty.value().concepts.empty());
    CHECK_FALSE(empty.value().usedGliner);

    auto unavailable = extractor.extractConcepts("semantic search", {"technology"});
    REQUIRE(unavailable.has_value());
    CHECK(unavailable.value().concepts.empty());
    CHECK_FALSE(unavailable.value().usedGliner);
}

TEST_CASE("QueryConceptExtractor delegates query and type filters to backend",
          "[search][query-concept]") {
    std::string observedQuery;
    std::vector<std::string> observedTypes;

    QueryConceptExtractor extractor(
        [&](const std::string& query,
            const std::vector<std::string>& types) -> Result<QueryConceptResult> {
            observedQuery = query;
            observedTypes = types;

            QueryConceptResult result;
            result.usedGliner = true;
            result.extractionTimeMs = 1.5;
            result.concepts.push_back(QueryConcept{"YAMS", "technology", 0.95f, 0, 4});
            return result;
        });

    CHECK(extractor.isAvailable());

    auto result = extractor.extractConcepts("YAMS vector search", {"technology"});
    REQUIRE(result.has_value());
    REQUIRE(result.value().concepts.size() == 1);
    CHECK(result.value().usedGliner);
    CHECK(result.value().extractionTimeMs == 1.5);
    CHECK(result.value().concepts[0].text == "YAMS");
    CHECK(result.value().concepts[0].type == "technology");
    CHECK(observedQuery == "YAMS vector search");
    CHECK(observedTypes == std::vector<std::string>{"technology"});

    auto defaultTypes = extractor.extractConcepts("YAMS");
    REQUIRE(defaultTypes.has_value());
    CHECK(observedQuery == "YAMS");
    CHECK(observedTypes.empty());
}

} // namespace yams::search::test
