#include <string>
#include <vector>
#include <gtest/gtest.h>
#include <yams/search/query_qualifiers.hpp>

using yams::search::ExtractScope;
using yams::search::ExtractScopeType;
using yams::search::ParsedQuery;
using yams::search::parseQueryQualifiers;

namespace {

TEST(QueryQualifiersTest, ParsesLinesRangeAndNormalizesQuery) {
    const std::string q = "find error lines:1-50 in logs";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Lines);
    EXPECT_EQ(parsed.scope.range, "1-50");
    // Normalized query should not contain the qualifier
    EXPECT_EQ(parsed.normalizedQuery, "find error in logs");
}

TEST(QueryQualifiersTest, ParsesPagesRangeAndNormalizesQuery) {
    const std::string q = "equation pages:10-12";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Pages);
    EXPECT_EQ(parsed.scope.range, "10-12");
    EXPECT_EQ(parsed.normalizedQuery, "equation");
}

TEST(QueryQualifiersTest, ParsesSectionWithDoubleQuotes) {
    const std::string q = R"(background section:"Chapter 2" details)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Section);
    EXPECT_EQ(parsed.scope.section, "Chapter 2");
    EXPECT_EQ(parsed.normalizedQuery, "background details");
}

TEST(QueryQualifiersTest, ParsesSectionWithSingleQuotes) {
    const std::string q = "intro section:'Background' methods";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Section);
    EXPECT_EQ(parsed.scope.section, "Background");
    EXPECT_EQ(parsed.normalizedQuery, "intro methods");
}

TEST(QueryQualifiersTest, ParsesSelectorWithSpacesAfterColon) {
    const std::string q = R"(title selector: "div.main" content)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Selector);
    EXPECT_EQ(parsed.scope.selector, "div.main");
    EXPECT_EQ(parsed.normalizedQuery, "title content");
}

TEST(QueryQualifiersTest, ParsesFiltersNameExtMime) {
    const std::string q = R"(refactor name:main.cpp ext:.cpp mime:text/x-c++ source code)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::All);
    EXPECT_EQ(parsed.scope.name, "main.cpp");
    EXPECT_EQ(parsed.scope.ext, ".cpp");
    EXPECT_EQ(parsed.scope.mime, "text/x-c++");
    EXPECT_EQ(parsed.normalizedQuery, "refactor source code");
}

TEST(QueryQualifiersTest, MixedQualifiersAndTextNormalization) {
    const std::string q = R"(init lines:1-50 name:main.cpp ext:.cpp mime:text/x-c++ src project)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Lines);
    EXPECT_EQ(parsed.scope.range, "1-50");
    EXPECT_EQ(parsed.scope.name, "main.cpp");
    EXPECT_EQ(parsed.scope.ext, ".cpp");
    EXPECT_EQ(parsed.scope.mime, "text/x-c++");
    EXPECT_EQ(parsed.normalizedQuery, "init src project");
}

TEST(QueryQualifiersTest, CaseInsensitiveKeysAreAccepted) {
    const std::string q = R"(topic Lines:5-7 Section:"Intro" Selector:"div#id")";
    ParsedQuery parsed = parseQueryQualifiers(q);

    // The last scope type parsed should be Selector
    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Selector);
    EXPECT_EQ(parsed.scope.range, "5-7");
    EXPECT_EQ(parsed.scope.section, "Intro");
    EXPECT_EQ(parsed.scope.selector, "div#id");
    EXPECT_EQ(parsed.normalizedQuery, "topic");
}

TEST(QueryQualifiersTest, KeyColonSpaceValueWithSpaces) {
    const std::string q = R"(find section: "Related Work" selector: "article.main" now)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Selector);
    EXPECT_EQ(parsed.scope.section, "Related Work");
    EXPECT_EQ(parsed.scope.selector, "article.main");
    EXPECT_EQ(parsed.normalizedQuery, "find now");
}

TEST(QueryQualifiersTest, UnquotedValuesAreAccepted) {
    const std::string q = R"(search section:Background lines:2-3 done)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Lines);
    EXPECT_EQ(parsed.scope.section, "Background");
    EXPECT_EQ(parsed.scope.range, "2-3");
    EXPECT_EQ(parsed.normalizedQuery, "search done");
}

TEST(QueryQualifiersTest, OnlyQualifiersResultsInEmptyNormalizedQuery) {
    const std::string q = R"(lines:1-3 pages:2 section:"Body")";
    ParsedQuery parsed = parseQueryQualifiers(q);

    EXPECT_TRUE(parsed.normalizedQuery.empty());
    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Section);
    EXPECT_EQ(parsed.scope.range, "2"); // last parsed range for pages:2
    EXPECT_EQ(parsed.scope.section, "Body");
}

TEST(QueryQualifiersTest, MultipleScopesLastOneWinsForType) {
    const std::string q = R"(foo lines:1-10 pages:3-4 selector:"h1.title" bar)";
    ParsedQuery parsed = parseQueryQualifiers(q);

    // The last scope is selector
    EXPECT_EQ(parsed.scope.type, ExtractScopeType::Selector);
    EXPECT_EQ(parsed.scope.selector, "h1.title");
    EXPECT_EQ(parsed.scope.range, "3-4"); // pages range still captured earlier
    EXPECT_EQ(parsed.normalizedQuery, "foo bar");
}

} // namespace