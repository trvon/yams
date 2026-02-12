// Catch2 tests for Query Parser and Tokenizer
// Migrated from GTest: query_parser_test.cpp

#include <catch2/catch_test_macros.hpp>

#include <iostream>
#include <memory>
#include <yams/search/query_parser.h>
#include <yams/search/query_tokenizer.h>

using namespace yams::search;

namespace {
struct QueryParserFixture {
    QueryParserFixture()
        : parser_(std::make_unique<QueryParser>()), analyzer_(std::make_unique<QueryAnalyzer>()),
          optimizer_(std::make_unique<QueryOptimizer>()) {}

    std::unique_ptr<QueryParser> parser_;
    std::unique_ptr<QueryAnalyzer> analyzer_;
    std::unique_ptr<QueryOptimizer> optimizer_;
};
} // namespace

// ============================================================================
// QueryTokenizer Tests
// ============================================================================

TEST_CASE("QueryTokenizer - Basic term tokenization", "[search][query][tokenizer][catch2]") {
    QueryTokenizer tokenizer;

    auto tokens = tokenizer.tokenize("hello world");
    REQUIRE(tokens.size() == 3); // hello, world, EOF
    CHECK(tokens[0].type == TokenType::Term);
    CHECK(tokens[0].value == "hello");
    CHECK(tokens[1].type == TokenType::Term);
    CHECK(tokens[1].value == "world");
    CHECK(tokens[2].type == TokenType::EndOfInput);
}

TEST_CASE("QueryTokenizer - Quoted string", "[search][query][tokenizer][catch2]") {
    QueryTokenizer tokenizer;

    auto tokens = tokenizer.tokenize("\"hello world\"");
    REQUIRE(tokens.size() == 2); // quoted string, EOF
    CHECK(tokens[0].type == TokenType::QuotedString);
    CHECK(tokens[0].value == "hello world");
}

TEST_CASE("QueryTokenizer - Operators", "[search][query][tokenizer][catch2]") {
    QueryTokenizer tokenizer;

    auto tokens = tokenizer.tokenize("hello AND world OR NOT test");
    REQUIRE(tokens.size() == 7); // hello, AND, world, OR, NOT, test, EOF
    CHECK(tokens[0].type == TokenType::Term);
    CHECK(tokens[1].type == TokenType::And);
    CHECK(tokens[2].type == TokenType::Term);
    CHECK(tokens[3].type == TokenType::Or);
    CHECK(tokens[4].type == TokenType::Not);
    CHECK(tokens[5].type == TokenType::Term);
}

TEST_CASE("QueryTokenizer - Field query", "[search][query][tokenizer][catch2]") {
    QueryTokenizer tokenizer;

    auto tokens = tokenizer.tokenize("title:hello");
    REQUIRE(tokens.size() == 4); // title, :, hello, EOF
    CHECK(tokens[0].type == TokenType::Term);
    CHECK(tokens[0].value == "title");
    CHECK(tokens[1].type == TokenType::Colon);
    CHECK(tokens[2].type == TokenType::Term);
    CHECK(tokens[2].value == "hello");
}

TEST_CASE("QueryTokenizer - Parentheses", "[search][query][tokenizer][catch2]") {
    QueryTokenizer tokenizer;

    auto tokens = tokenizer.tokenize("(hello OR world)");
    REQUIRE(tokens.size() == 6); // (, hello, OR, world, ), EOF
    CHECK(tokens[0].type == TokenType::LeftParen);
    CHECK(tokens[1].type == TokenType::Term);
    CHECK(tokens[2].type == TokenType::Or);
    CHECK(tokens[3].type == TokenType::Term);
    CHECK(tokens[4].type == TokenType::RightParen);
}

TEST_CASE("QueryTokenizer - NonAsciiBytes", "[search][query][tokenizer][catch2]") {
    QueryTokenizer tokenizer;

    std::string input;
    input.push_back(static_cast<char>(0xFF));
    input.push_back('a');
    input.push_back(' ');
    input.push_back(static_cast<char>(0x80));

    auto tokens = tokenizer.tokenize(input);
    REQUIRE_FALSE(tokens.empty());
    CHECK(tokens.back().type == TokenType::EndOfInput);
}

// ============================================================================
// QueryParser Tests
// ============================================================================

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseSingleTerm",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hello");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Term);

    auto termNode = static_cast<TermNode*>(ast.get());
    CHECK(termNode->getTerm() == "hello");
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParsePhrase",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("\"hello world\"");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Phrase);

    auto phraseNode = static_cast<PhraseNode*>(ast.get());
    CHECK(phraseNode->getPhrase() == "hello world");
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseAndExpression",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hello AND world");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::And);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseOrExpression",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hello OR world");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Or);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseNotExpression",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("NOT hello");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Not);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseComplexExpression",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hello AND (world OR universe)");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::And);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseFieldQuery",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("title:hello");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Field);

    auto fieldNode = static_cast<FieldNode*>(ast.get());
    CHECK(fieldNode->getField() == "title");
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseWildcard",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hel*");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Wildcard);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ParseFuzzy",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hello~2");
    REQUIRE(result.has_value());

    auto& ast = result.value();
    REQUIRE(ast != nullptr);
    CHECK(ast->getType() == QueryNodeType::Fuzzy);

    auto fuzzyNode = static_cast<FuzzyNode*>(ast.get());
    CHECK(fuzzyNode->getTerm() == "hello");
    CHECK(fuzzyNode->getDistance() == 2);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - InvalidQuery_UnmatchedParen",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("(hello world");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - InvalidQuery_EmptyString",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("");
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - ToFTS5Query",
                 "[search][query][parser][catch2]") {
    auto result = parser_->parse("hello AND world OR \"test phrase\"");
    REQUIRE(result.has_value());

    std::string fts5Query = parser_->toFTS5Query(result.value().get());
    CHECK_FALSE(fts5Query.empty());
    CHECK(fts5Query.find("hello") != std::string::npos);
    CHECK(fts5Query.find("world") != std::string::npos);
}

TEST_CASE_METHOD(QueryParserFixture, "QueryParser - Validation",
                 "[search][query][parser][catch2]") {
    SECTION("Valid query") {
        auto validation = parser_->validate("hello AND world");
        CHECK(validation.isValid);
        CHECK(validation.error.empty());
    }

    SECTION("Invalid query") {
        auto validation = parser_->validate("(hello world");
        CHECK_FALSE(validation.isValid);
        CHECK_FALSE(validation.error.empty());
    }
}

TEST_CASE_METHOD(QueryParserFixture, "QueryAnalyzer - Basic", "[search][query][analyzer][catch2]") {
    auto result = parser_->parse("hello AND world OR \"test phrase\" NOT foo*");
    REQUIRE(result.has_value());

    auto stats = analyzer_->analyze(result.value().get());

    CHECK(stats.termCount > 0);
    CHECK(stats.operatorCount > 0);
    CHECK(stats.hasWildcards);
    CHECK_FALSE(stats.terms.empty());
}

TEST_CASE_METHOD(QueryParserFixture, "QueryOptimizer - DoubleNegation",
                 "[search][query][optimizer][catch2]") {
    auto result = parser_->parse("NOT NOT hello");
    REQUIRE(result.has_value());

    auto optimized = optimizer_->optimize(std::move(result).value());
    REQUIRE(optimized != nullptr);

    // Double negation should be eliminated
    CHECK(optimized->getType() == QueryNodeType::Term);
}
