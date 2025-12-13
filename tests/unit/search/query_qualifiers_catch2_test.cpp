// Catch2 tests for Query Qualifiers
// Migrated from GTest: query_qualifiers_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <yams/search/query_qualifiers.hpp>

using yams::search::ExtractScope;
using yams::search::ExtractScopeType;
using yams::search::ParsedQuery;
using yams::search::parseQueryQualifiers;

TEST_CASE("Query Qualifiers - ParsesLinesRangeAndNormalizesQuery", "[search][qualifiers][catch2]") {
    const std::string q = "find error lines:1-50 in logs";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Lines);
    CHECK(parsed.scope.range == "1-50");
    CHECK(parsed.normalizedQuery == "find error in logs");
}

TEST_CASE("Query Qualifiers - ParsesPagesRangeAndNormalizesQuery", "[search][qualifiers][catch2]") {
    const std::string q = "equation pages:10-12";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Pages);
    CHECK(parsed.scope.range == "10-12");
    CHECK(parsed.normalizedQuery == "equation");
}

TEST_CASE("Query Qualifiers - ParsesSectionWithDoubleQuotes", "[search][qualifiers][catch2]") {
    const std::string q = R"(background section:"Chapter 2" details)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Section);
    CHECK(parsed.scope.section == "Chapter 2");
    CHECK(parsed.normalizedQuery == "background details");
}

TEST_CASE("Query Qualifiers - ParsesSectionWithSingleQuotes", "[search][qualifiers][catch2]") {
    const std::string q = "intro section:'Background' methods";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Section);
    CHECK(parsed.scope.section == "Background");
    CHECK(parsed.normalizedQuery == "intro methods");
}

TEST_CASE("Query Qualifiers - ParsesSelectorWithSpacesAfterColon", "[search][qualifiers][catch2]") {
    const std::string q = R"(title selector: "div.main" content)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Selector);
    CHECK(parsed.scope.selector == "div.main");
    CHECK(parsed.normalizedQuery == "title content");
}

TEST_CASE("Query Qualifiers - ParsesFiltersNameExtMime", "[search][qualifiers][catch2]") {
    const std::string q = R"(refactor name:main.cpp ext:.cpp mime:text/x-c++ source code)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::All);
    CHECK(parsed.scope.name == "main.cpp");
    CHECK(parsed.scope.ext == ".cpp");
    CHECK(parsed.scope.mime == "text/x-c++");
    CHECK(parsed.normalizedQuery == "refactor source code");
}

TEST_CASE("Query Qualifiers - MixedQualifiersAndTextNormalization",
          "[search][qualifiers][catch2]") {
    const std::string q = R"(init lines:1-50 name:main.cpp ext:.cpp mime:text/x-c++ src project)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Lines);
    CHECK(parsed.scope.range == "1-50");
    CHECK(parsed.scope.name == "main.cpp");
    CHECK(parsed.scope.ext == ".cpp");
    CHECK(parsed.scope.mime == "text/x-c++");
    CHECK(parsed.normalizedQuery == "init src project");
}

TEST_CASE("Query Qualifiers - CaseInsensitiveKeysAreAccepted", "[search][qualifiers][catch2]") {
    const std::string q = R"(topic Lines:5-7 Section:"Intro" Selector:"div#id")";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Selector);
    CHECK(parsed.scope.range == "5-7");
    CHECK(parsed.scope.section == "Intro");
    CHECK(parsed.scope.selector == "div#id");
    CHECK(parsed.normalizedQuery == "topic");
}

TEST_CASE("Query Qualifiers - OnlyQualifiersResultsInEmptyNormalizedQuery",
          "[search][qualifiers][catch2]") {
    const std::string q = R"(lines:1-3 pages:2 section:"Body")";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.normalizedQuery.empty());
    CHECK(parsed.scope.type == ExtractScopeType::Section);
    CHECK(parsed.scope.range == "2");
    CHECK(parsed.scope.section == "Body");
}

TEST_CASE("Query Qualifiers - MultipleScopesLastOneWinsForType", "[search][qualifiers][catch2]") {
    const std::string q = R"(foo lines:1-10 pages:3-4 selector:"h1.title" bar)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    CHECK(parsed.scope.type == ExtractScopeType::Selector);
    CHECK(parsed.scope.selector == "h1.title");
    CHECK(parsed.scope.range == "3-4");
    CHECK(parsed.normalizedQuery == "foo bar");
}
