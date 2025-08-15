#pragma once

#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace yams::search {

/**
 * @brief Token types for query parsing
 */
enum class TokenType {
    Term,         // Regular search term
    QuotedString, // Phrase in quotes
    LeftParen,    // (
    RightParen,   // )
    LeftBracket,  // [
    RightBracket, // ]
    LeftBrace,    // {
    RightBrace,   // }
    Colon,        // :
    And,          // AND, &&
    Or,           // OR, ||
    Not,          // NOT, !
    Plus,         // +
    Minus,        // -
    Tilde,        // ~
    Star,         // *
    Question,     // ?
    To,           // TO
    Caret,        // ^
    EndOfInput    // End of query string
};

/**
 * @brief Token structure
 */
struct Token {
    TokenType type;
    std::string value;
    size_t position; // Position in original query string
    size_t length;   // Length of token

    Token(TokenType t, const std::string& v, size_t pos, size_t len)
        : type(t), value(v), position(pos), length(len) {}

    bool isOperator() const {
        return type == TokenType::And || type == TokenType::Or || type == TokenType::Not;
    }

    bool isTerm() const { return type == TokenType::Term || type == TokenType::QuotedString; }
};

/**
 * @brief Query tokenizer - breaks query string into tokens
 */
class QueryTokenizer {
public:
    /**
     * @brief Tokenize a query string
     * @param query The query string to tokenize
     * @return Vector of tokens
     */
    std::vector<Token> tokenize(const std::string& query);

    /**
     * @brief Configuration for tokenizer
     */
    struct Config {
        bool caseSensitive = false;
        bool allowWildcards = true;
        bool allowFuzzy = true;
        bool allowRanges = true;
        bool allowFieldQueries = true;
        bool treatPlusAsAnd = true;
        bool treatMinusAsNot = true;
    };

    /**
     * @brief Set tokenizer configuration
     */
    void setConfig(const Config& config) { config_ = config; }

    /**
     * @brief Get current configuration
     */
    const Config& getConfig() const { return config_; }

private:
    Config config_;
    std::string query_;
    size_t position_ = 0;

    /**
     * @brief Peek at current character without advancing
     */
    char peek() const;

    /**
     * @brief Peek at next character without advancing
     */
    char peekNext() const;

    /**
     * @brief Get current character and advance position
     */
    char advance();

    /**
     * @brief Check if at end of input
     */
    bool isAtEnd() const;

    /**
     * @brief Skip whitespace
     */
    void skipWhitespace();

    /**
     * @brief Read a quoted string
     */
    Token readQuotedString();

    /**
     * @brief Read a term (word)
     */
    Token readTerm();

    /**
     * @brief Read an operator
     */
    Token readOperator();

    /**
     * @brief Check if character is valid in a term
     */
    bool isTermChar(char c) const;

    /**
     * @brief Check if string is a reserved word
     */
    bool isReservedWord(const std::string& word) const;
};

/**
 * @brief Tokenizer exception
 */
class TokenizerException : public std::runtime_error {
public:
    TokenizerException(const std::string& message, size_t position)
        : std::runtime_error(message), position_(position) {}

    size_t getPosition() const { return position_; }

private:
    size_t position_;
};

} // namespace yams::search