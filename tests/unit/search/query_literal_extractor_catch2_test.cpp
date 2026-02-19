#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <yams/search/query_literal_extractor.hpp>
#include <yams/search/query_ast.h>

#include <algorithm>
#include <memory>

using namespace yams::search;

namespace {

std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

} // namespace

TEST_CASE("QueryLiteralExtractor::extract() with null AST", "[search][literal_extractor][catch2]") {
    const QueryNode* ast = nullptr;
    const auto result = QueryLiteralExtractor::extract(ast);

    REQUIRE(result.terms.empty());
    REQUIRE(result.hasPureTerms == false);
    REQUIRE(result.mostSelective.empty());
    REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(1.0f, 1e-6f));
}

TEST_CASE("QueryLiteralExtractor::extract() with single TermNode",
          "[search][literal_extractor][catch2]") {
    TermNode term("hello");
    const auto result = QueryLiteralExtractor::extract(&term);

    REQUIRE(result.terms == std::vector<std::string>{"hello"});
    REQUIRE(result.hasPureTerms == true);
    REQUIRE(result.mostSelective == "hello");
    REQUIRE(result.estimatedSelectivity >= 0.0f);
    REQUIRE(result.estimatedSelectivity <= 1.0f);
}

TEST_CASE("QueryLiteralExtractor::extract() with PhraseNode",
          "[search][literal_extractor][catch2]") {
    PhraseNode phrase("exact match phrase");
    const auto result = QueryLiteralExtractor::extract(&phrase);

    REQUIRE(result.hasPureTerms == true);
    REQUIRE(result.terms == std::vector<std::string>{"exact match phrase"});
}

TEST_CASE("QueryLiteralExtractor::extract() with AndNode", "[search][literal_extractor][catch2]") {
    auto ast = std::make_unique<AndNode>(std::make_unique<TermNode>("foo"),
                                         std::make_unique<TermNode>("SpecificClassName"));
    const auto result = QueryLiteralExtractor::extract(ast.get());

    REQUIRE(result.hasPureTerms == true);
    REQUIRE(sorted(result.terms) == sorted(std::vector<std::string>{"foo", "SpecificClassName"}));
    REQUIRE(result.mostSelective == "SpecificClassName");
}

TEST_CASE("QueryLiteralExtractor::extract() with OrNode", "[search][literal_extractor][catch2]") {
    auto ast = std::make_unique<OrNode>(std::make_unique<TermNode>("alpha"),
                                        std::make_unique<TermNode>("beta"));
    const auto result = QueryLiteralExtractor::extract(ast.get());

    REQUIRE(result.hasPureTerms == true);
    REQUIRE(sorted(result.terms) == sorted(std::vector<std::string>{"alpha", "beta"}));
}

TEST_CASE("QueryLiteralExtractor::extract() with NotNode skips terms",
          "[search][literal_extractor][catch2]") {
    auto ast = std::make_unique<NotNode>(std::make_unique<TermNode>("secret"));
    const auto result = QueryLiteralExtractor::extract(ast.get());

    REQUIRE(result.terms.empty());
    REQUIRE(result.hasPureTerms == false);
    REQUIRE(result.mostSelective.empty());
}

TEST_CASE("QueryLiteralExtractor::extract() with FieldNode",
          "[search][literal_extractor][catch2]") {
    auto ast = std::make_unique<FieldNode>("author", std::make_unique<TermNode>("john"));
    const auto result = QueryLiteralExtractor::extract(ast.get());

    REQUIRE(result.hasPureTerms == true);
    REQUIRE(result.terms == std::vector<std::string>{"john"});
    REQUIRE(result.mostSelective == "john");
}

TEST_CASE("QueryLiteralExtractor::extract() with GroupNode",
          "[search][literal_extractor][catch2]") {
    auto ast = std::make_unique<GroupNode>(std::make_unique<TermNode>("grouped"));
    const auto result = QueryLiteralExtractor::extract(ast.get());

    REQUIRE(result.hasPureTerms == true);
    REQUIRE(result.terms == std::vector<std::string>{"grouped"});
}

TEST_CASE("QueryLiteralExtractor::extract() skips Wildcard/Fuzzy/Range nodes",
          "[search][literal_extractor][catch2]") {
    SECTION("WildcardNode") {
        WildcardNode node("foo*");
        const auto result = QueryLiteralExtractor::extract(&node);
        REQUIRE(result.terms.empty());
        REQUIRE(result.hasPureTerms == false);
    }

    SECTION("FuzzyNode") {
        FuzzyNode node("hello", 2);
        const auto result = QueryLiteralExtractor::extract(&node);
        REQUIRE(result.terms.empty());
        REQUIRE(result.hasPureTerms == false);
    }

    SECTION("RangeNode") {
        RangeNode node("date", "2020", "2021");
        const auto result = QueryLiteralExtractor::extract(&node);
        REQUIRE(result.terms.empty());
        REQUIRE(result.hasPureTerms == false);
    }
}

TEST_CASE("QueryLiteralExtractor scoring influences mostSelective",
          "[search][literal_extractor][catch2]") {
    SECTION("Longer terms score higher") {
        auto ast = std::make_unique<AndNode>(std::make_unique<TermNode>("tiny"),
                                             std::make_unique<TermNode>("muchlonger"));
        const auto result = QueryLiteralExtractor::extract(ast.get());
        REQUIRE(result.mostSelective == "muchlonger");
    }

    SECTION("Uppercase terms score higher") {
        auto ast = std::make_unique<AndNode>(std::make_unique<TermNode>("classname"),
                                             std::make_unique<TermNode>("ClassName"));
        const auto result = QueryLiteralExtractor::extract(ast.get());
        REQUIRE(result.mostSelective == "ClassName");
    }

    SECTION("Special characters increase score") {
        auto ast = std::make_unique<AndNode>(std::make_unique<TermNode>("abcdefg"),
                                             std::make_unique<TermNode>("abc-def"));
        const auto result = QueryLiteralExtractor::extract(ast.get());
        REQUIRE(result.mostSelective == "abc-def");
    }

    SECTION("Common short words are penalized") {
        auto ast = std::make_unique<AndNode>(std::make_unique<TermNode>("the"),
                                             std::make_unique<TermNode>("abc"));
        const auto result = QueryLiteralExtractor::extract(ast.get());
        REQUIRE(result.mostSelective == "abc");
    }
}

TEST_CASE("QueryLiteralExtractor::estimateSelectivity via extract()",
          "[search][literal_extractor][catch2]") {
    SECTION("Empty term -> 1.0") {
        TermNode term("");
        const auto result = QueryLiteralExtractor::extract(&term);
        REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(1.0f, 1e-6f));
    }

    SECTION("Short term (<3 chars) -> 1.0") {
        TermNode term("hi");
        const auto result = QueryLiteralExtractor::extract(&term);
        REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(1.0f, 1e-6f));
    }

    SECTION("Medium term (3-5 chars) -> 0.5") {
        TermNode term("hello");
        const auto result = QueryLiteralExtractor::extract(&term);
        REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(0.5f, 1e-6f));
    }

    SECTION("Long term (6-9 chars) -> 0.3") {
        TermNode term("bananas");
        const auto result = QueryLiteralExtractor::extract(&term);
        REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(0.3f, 1e-6f));
    }

    SECTION("Very long term (10+ chars) -> 0.1") {
        TermNode term("0123456789");
        const auto result = QueryLiteralExtractor::extract(&term);
        REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(0.1f, 1e-6f));
    }

    SECTION("Uppercase reduces selectivity by 0.7x") {
        TermNode term("Hello");
        const auto result = QueryLiteralExtractor::extract(&term);
        REQUIRE_THAT(result.estimatedSelectivity, Catch::Matchers::WithinAbs(0.35f, 1e-6f));
    }
}
