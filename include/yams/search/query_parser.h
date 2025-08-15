#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <yams/core/types.h>
#include <yams/search/query_ast.h>
#include <yams/search/query_tokenizer.h>

namespace yams::search {

/**
 * @brief Query parser configuration
 */
struct QueryParserConfig {
    // Default operator when none specified
    enum class DefaultOperator {
        And, // Terms are ANDed by default
        Or   // Terms are ORed by default
    };

    DefaultOperator defaultOperator = DefaultOperator::And;
    bool allowLeadingWildcard = false;
    bool enablePositionIncrements = true;
    bool autoGeneratePhraseQueries = false;
    int fuzzyMaxExpansions = 50;
    float fuzzyMinSimilarity = 0.5f;
    int phraseSlop = 0;

    // Fields that can be searched
    std::unordered_set<std::string> searchableFields = {"content", "title", "author", "tags",
                                                        "description"};

    // Default field when none specified
    std::string defaultField = "content";
};

/**
 * @brief Query validation result
 */
struct QueryValidation {
    bool isValid;
    std::string error;
    size_t errorPosition;

    static QueryValidation valid() { return {true, "", 0}; }

    static QueryValidation invalid(const std::string& error, size_t pos = 0) {
        return {false, error, pos};
    }
};

/**
 * @brief Main query parser class
 */
class QueryParser {
public:
    explicit QueryParser(const QueryParserConfig& config = {});

    /**
     * @brief Parse a query string into an AST
     * @param query The query string to parse
     * @return Result containing the parsed AST or an error
     */
    Result<std::unique_ptr<QueryNode>> parse(const std::string& query);

    /**
     * @brief Validate a query without fully parsing it
     * @param query The query string to validate
     * @return Validation result
     */
    QueryValidation validate(const std::string& query);

    /**
     * @brief Convert AST to FTS5 query string
     * @param node The root node of the AST
     * @return FTS5 compatible query string
     */
    std::string toFTS5Query(const QueryNode* node);

    /**
     * @brief Set parser configuration
     */
    void setConfig(const QueryParserConfig& config) { config_ = config; }

    /**
     * @brief Get current configuration
     */
    const QueryParserConfig& getConfig() const { return config_; }

private:
    QueryParserConfig config_;
    QueryTokenizer tokenizer_;
    std::vector<Token> tokens_;
    size_t currentToken_;

    /**
     * @brief Parse methods for different expression types
     */
    std::unique_ptr<QueryNode> parseExpression();
    std::unique_ptr<QueryNode> parseOrExpression();
    std::unique_ptr<QueryNode> parseAndExpression();
    std::unique_ptr<QueryNode> parseNotExpression();
    std::unique_ptr<QueryNode> parsePrimary();
    std::unique_ptr<QueryNode> parseTerm();
    std::unique_ptr<QueryNode> parseFieldQuery();
    std::unique_ptr<QueryNode> parseGroupedExpression();
    std::unique_ptr<QueryNode> parsePhrase();
    std::unique_ptr<QueryNode> parseWildcard(const std::string& term);
    std::unique_ptr<QueryNode> parseFuzzy(const std::string& term);
    std::unique_ptr<QueryNode> parseRange();

    /**
     * @brief Token navigation helpers
     */
    const Token& current() const;
    const Token& peek() const;
    bool advance();
    bool check(TokenType type) const;
    bool match(TokenType type);
    bool isAtEnd() const;

    /**
     * @brief Error handling
     */
    void throwError(const std::string& message);

    /**
     * @brief FTS5 query generation helpers
     */
    std::string escapeForFTS5(const std::string& term);
    std::string fieldToFTS5(const std::string& field, const std::string& value);
};

/**
 * @brief Query parser exception
 */
class QueryParserException : public std::runtime_error {
public:
    QueryParserException(const std::string& message, size_t position)
        : std::runtime_error(message), position_(position) {}

    size_t getPosition() const { return position_; }

private:
    size_t position_;
};

/**
 * @brief Query optimizer - optimizes parsed queries
 */
class QueryOptimizer {
public:
    /**
     * @brief Optimize a query AST
     * @param node The root node to optimize
     * @return Optimized AST
     */
    std::unique_ptr<QueryNode> optimize(std::unique_ptr<QueryNode> node);

private:
    /**
     * @brief Optimization passes
     */
    std::unique_ptr<QueryNode> eliminateDoubleNegation(std::unique_ptr<QueryNode> node);
    std::unique_ptr<QueryNode> flattenBinaryOps(std::unique_ptr<QueryNode> node);
    std::unique_ptr<QueryNode> simplifyConstants(std::unique_ptr<QueryNode> node);
    std::unique_ptr<QueryNode> reorderForPerformance(std::unique_ptr<QueryNode> node);
};

/**
 * @brief Query analyzer - provides query statistics
 */
class QueryAnalyzer {
public:
    struct QueryStats {
        size_t termCount = 0;
        size_t operatorCount = 0;
        size_t maxDepth = 0;
        bool hasWildcards = false;
        bool hasFuzzy = false;
        bool hasRanges = false;
        bool hasFieldQueries = false;
        std::vector<std::string> fields;
        std::vector<std::string> terms;
    };

    /**
     * @brief Analyze a query AST
     * @param node The root node to analyze
     * @return Query statistics
     */
    QueryStats analyze(const QueryNode* node);

private:
    void analyzeNode(const QueryNode* node, QueryStats& stats, size_t depth);
};

} // namespace yams::search