#include <gtest/gtest.h>
#include <yams/search/query_parser.h>
#include <yams/search/query_tokenizer.h>
#include <iostream>

using namespace yams::search;

class QueryParserTest : public ::testing::Test {
protected:
    void SetUp() override {
        parser_ = std::make_unique<QueryParser>();
        analyzer_ = std::make_unique<QueryAnalyzer>();
        optimizer_ = std::make_unique<QueryOptimizer>();
    }
    
    std::unique_ptr<QueryParser> parser_;
    std::unique_ptr<QueryAnalyzer> analyzer_;
    std::unique_ptr<QueryOptimizer> optimizer_;
};

TEST_F(QueryParserTest, TokenizerBasic) {
    QueryTokenizer tokenizer;
    
    // Test simple term tokenization
    auto tokens = tokenizer.tokenize("hello world");
    ASSERT_EQ(tokens.size(), 3); // hello, world, EOF
    EXPECT_EQ(tokens[0].type, TokenType::Term);
    EXPECT_EQ(tokens[0].value, "hello");
    EXPECT_EQ(tokens[1].type, TokenType::Term);
    EXPECT_EQ(tokens[1].value, "world");
    EXPECT_EQ(tokens[2].type, TokenType::EndOfInput);
}

TEST_F(QueryParserTest, TokenizerQuotedString) {
    QueryTokenizer tokenizer;
    
    auto tokens = tokenizer.tokenize("\"hello world\"");
    ASSERT_EQ(tokens.size(), 2); // quoted string, EOF
    EXPECT_EQ(tokens[0].type, TokenType::QuotedString);
    EXPECT_EQ(tokens[0].value, "hello world");
}

TEST_F(QueryParserTest, TokenizerOperators) {
    QueryTokenizer tokenizer;
    
    auto tokens = tokenizer.tokenize("hello AND world OR NOT test");
    ASSERT_EQ(tokens.size(), 7); // hello, AND, world, OR, NOT, test, EOF
    EXPECT_EQ(tokens[0].type, TokenType::Term);
    EXPECT_EQ(tokens[1].type, TokenType::And);
    EXPECT_EQ(tokens[2].type, TokenType::Term);
    EXPECT_EQ(tokens[3].type, TokenType::Or);
    EXPECT_EQ(tokens[4].type, TokenType::Not);
    EXPECT_EQ(tokens[5].type, TokenType::Term);
}

TEST_F(QueryParserTest, TokenizerFieldQuery) {
    QueryTokenizer tokenizer;
    
    auto tokens = tokenizer.tokenize("title:hello");
    ASSERT_EQ(tokens.size(), 4); // title, :, hello, EOF
    EXPECT_EQ(tokens[0].type, TokenType::Term);
    EXPECT_EQ(tokens[0].value, "title");
    EXPECT_EQ(tokens[1].type, TokenType::Colon);
    EXPECT_EQ(tokens[2].type, TokenType::Term);
    EXPECT_EQ(tokens[2].value, "hello");
}

TEST_F(QueryParserTest, TokenizerParentheses) {
    QueryTokenizer tokenizer;
    
    auto tokens = tokenizer.tokenize("(hello OR world)");
    ASSERT_EQ(tokens.size(), 6); // (, hello, OR, world, ), EOF
    EXPECT_EQ(tokens[0].type, TokenType::LeftParen);
    EXPECT_EQ(tokens[1].type, TokenType::Term);
    EXPECT_EQ(tokens[2].type, TokenType::Or);
    EXPECT_EQ(tokens[3].type, TokenType::Term);
    EXPECT_EQ(tokens[4].type, TokenType::RightParen);
}

TEST_F(QueryParserTest, ParseSingleTerm) {
    auto result = parser_->parse("hello");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Term);
    
    auto termNode = static_cast<TermNode*>(ast.get());
    EXPECT_EQ(termNode->getTerm(), "hello");
}

TEST_F(QueryParserTest, ParsePhrase) {
    auto result = parser_->parse("\"hello world\"");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Phrase);
    
    auto phraseNode = static_cast<PhraseNode*>(ast.get());
    EXPECT_EQ(phraseNode->getPhrase(), "hello world");
}

TEST_F(QueryParserTest, ParseAndOperator) {
    auto result = parser_->parse("hello AND world");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::And);
    
    auto andNode = static_cast<AndNode*>(ast.get());
    EXPECT_NE(andNode->getLeft(), nullptr);
    EXPECT_NE(andNode->getRight(), nullptr);
    
    EXPECT_EQ(andNode->getLeft()->getType(), QueryNodeType::Term);
    EXPECT_EQ(andNode->getRight()->getType(), QueryNodeType::Term);
}

TEST_F(QueryParserTest, ParseOrOperator) {
    auto result = parser_->parse("hello OR world");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Or);
    
    auto orNode = static_cast<OrNode*>(ast.get());
    EXPECT_NE(orNode->getLeft(), nullptr);
    EXPECT_NE(orNode->getRight(), nullptr);
}

TEST_F(QueryParserTest, ParseNotOperator) {
    auto result = parser_->parse("NOT hello");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Not);
    
    auto notNode = static_cast<NotNode*>(ast.get());
    EXPECT_NE(notNode->getChild(), nullptr);
    EXPECT_EQ(notNode->getChild()->getType(), QueryNodeType::Term);
}

TEST_F(QueryParserTest, ParseComplexExpression) {
    auto result = parser_->parse("(hello OR world) AND NOT test");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::And);
    
    auto andNode = static_cast<AndNode*>(ast.get());
    EXPECT_EQ(andNode->getLeft()->getType(), QueryNodeType::Group);
    EXPECT_EQ(andNode->getRight()->getType(), QueryNodeType::Not);
}

TEST_F(QueryParserTest, ParseFieldQuery) {
    auto result = parser_->parse("title:hello");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Field);
    
    auto fieldNode = static_cast<FieldNode*>(ast.get());
    EXPECT_EQ(fieldNode->getField(), "title");
    EXPECT_NE(fieldNode->getValue(), nullptr);
    EXPECT_EQ(fieldNode->getValue()->getType(), QueryNodeType::Term);
}

TEST_F(QueryParserTest, ParseFieldWithPhrase) {
    auto result = parser_->parse("title:\"hello world\"");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Field);
    
    auto fieldNode = static_cast<FieldNode*>(ast.get());
    EXPECT_EQ(fieldNode->getField(), "title");
    EXPECT_EQ(fieldNode->getValue()->getType(), QueryNodeType::Phrase);
}

TEST_F(QueryParserTest, ParseWildcard) {
    auto result = parser_->parse("hel*");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Wildcard);
    
    auto wildcardNode = static_cast<WildcardNode*>(ast.get());
    EXPECT_EQ(wildcardNode->getPattern(), "hel*");
}

TEST_F(QueryParserTest, ParseFuzzy) {
    auto result = parser_->parse("hello~2");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Fuzzy);
    
    auto fuzzyNode = static_cast<FuzzyNode*>(ast.get());
    EXPECT_EQ(fuzzyNode->getTerm(), "hello");
    EXPECT_EQ(fuzzyNode->getDistance(), 2);
}

TEST_F(QueryParserTest, ImplicitAndOperator) {
    // Default config uses AND for implicit operator
    auto result = parser_->parse("hello world");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::And);
}

TEST_F(QueryParserTest, ImplicitOrOperator) {
    QueryParserConfig config;
    config.defaultOperator = QueryParserConfig::DefaultOperator::Or;
    parser_->setConfig(config);
    
    auto result = parser_->parse("hello world");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Or);
}

TEST_F(QueryParserTest, NestedGroups) {
    auto result = parser_->parse("((a OR b) AND (c OR d))");
    ASSERT_TRUE(result.has_value());
    
    auto& ast = result.value();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->getType(), QueryNodeType::Group);
}

TEST_F(QueryParserTest, InvalidQuery_UnmatchedParen) {
    auto result = parser_->parse("(hello world");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().message.find("Expected ')'"), std::string::npos);
}

TEST_F(QueryParserTest, InvalidQuery_EmptyString) {
    auto result = parser_->parse("");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().message.find("Empty query"), std::string::npos);
}

TEST_F(QueryParserTest, InvalidQuery_InvalidField) {
    QueryParserConfig config;
    config.searchableFields = {"title", "content"};
    parser_->setConfig(config);
    
    auto result = parser_->parse("invalid_field:hello");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().message.find("not searchable"), std::string::npos);
}

TEST_F(QueryParserTest, ToFTS5Query) {
    auto result = parser_->parse("hello AND world OR \"test phrase\"");
    ASSERT_TRUE(result.has_value());
    
    std::string fts5Query = parser_->toFTS5Query(result.value().get());
    EXPECT_FALSE(fts5Query.empty());
    
    // Should contain the terms and operators
    EXPECT_NE(fts5Query.find("hello"), std::string::npos);
    EXPECT_NE(fts5Query.find("world"), std::string::npos);
    EXPECT_NE(fts5Query.find("\"test phrase\""), std::string::npos);
}

TEST_F(QueryParserTest, Validation) {
    // Valid query
    auto validation = parser_->validate("hello AND world");
    EXPECT_TRUE(validation.isValid);
    EXPECT_TRUE(validation.error.empty());
    
    // Invalid query
    validation = parser_->validate("(hello world");
    EXPECT_FALSE(validation.isValid);
    EXPECT_FALSE(validation.error.empty());
}

TEST_F(QueryParserTest, QueryAnalyzer) {
    auto result = parser_->parse("hello AND world OR \"test phrase\" NOT foo*");
    ASSERT_TRUE(result.has_value());
    
    auto stats = analyzer_->analyze(result.value().get());
    
    EXPECT_GT(stats.termCount, 0);
    EXPECT_GT(stats.operatorCount, 0);
    EXPECT_TRUE(stats.hasWildcards);
    EXPECT_FALSE(stats.terms.empty());
}

TEST_F(QueryParserTest, QueryOptimizer_DoubleNegation) {
    auto result = parser_->parse("NOT NOT hello");
    ASSERT_TRUE(result.has_value());
    
    auto optimized = optimizer_->optimize(std::move(result).value());
    ASSERT_NE(optimized, nullptr);
    
    // Double negation should be eliminated
    EXPECT_EQ(optimized->getType(), QueryNodeType::Term);
}

TEST_F(QueryParserTest, ComplexRealWorldQuery) {
    std::string query = "(title:\"machine learning\" OR content:AI) AND "
                       "NOT author:bot* AND (tags:research OR tags:tutorial)";
    
    auto result = parser_->parse(query);
    ASSERT_TRUE(result.has_value());
    
    auto stats = analyzer_->analyze(result.value().get());
    EXPECT_TRUE(stats.hasFieldQueries);
    EXPECT_TRUE(stats.hasWildcards);
    EXPECT_FALSE(stats.fields.empty());
    
    std::string fts5Query = parser_->toFTS5Query(result.value().get());
    EXPECT_FALSE(fts5Query.empty());
}