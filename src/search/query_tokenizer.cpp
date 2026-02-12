#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <yams/search/query_tokenizer.h>

namespace yams::search {

std::vector<Token> QueryTokenizer::tokenize(const std::string& query) {
    query_ = query;
    position_ = 0;
    std::vector<Token> tokens;

    while (!isAtEnd()) {
        skipWhitespace();
        if (isAtEnd())
            break;

        size_t startPos = position_;
        char c = peek();

        // Handle quoted strings
        if (c == '"') {
            tokens.push_back(readQuotedString());
        }
        // Handle parentheses
        else if (c == '(') {
            advance();
            tokens.emplace_back(TokenType::LeftParen, "(", startPos, 1);
        } else if (c == ')') {
            advance();
            tokens.emplace_back(TokenType::RightParen, ")", startPos, 1);
        }
        // Handle brackets (for ranges)
        else if (c == '[' && config_.allowRanges) {
            advance();
            tokens.emplace_back(TokenType::LeftBracket, "[", startPos, 1);
        } else if (c == ']' && config_.allowRanges) {
            advance();
            tokens.emplace_back(TokenType::RightBracket, "]", startPos, 1);
        } else if (c == '{' && config_.allowRanges) {
            advance();
            tokens.emplace_back(TokenType::LeftBrace, "{", startPos, 1);
        } else if (c == '}' && config_.allowRanges) {
            advance();
            tokens.emplace_back(TokenType::RightBrace, "}", startPos, 1);
        }
        // Handle colon (for field queries)
        else if (c == ':' && config_.allowFieldQueries) {
            advance();
            tokens.emplace_back(TokenType::Colon, ":", startPos, 1);
        }
        // Handle plus (required term or AND)
        else if (c == '+') {
            advance();
            if (config_.treatPlusAsAnd) {
                tokens.emplace_back(TokenType::And, "+", startPos, 1);
            } else {
                tokens.emplace_back(TokenType::Plus, "+", startPos, 1);
            }
        }
        // Handle minus (prohibited term or NOT)
        else if (c == '-') {
            advance();
            if (config_.treatMinusAsNot) {
                tokens.emplace_back(TokenType::Not, "-", startPos, 1);
            } else {
                tokens.emplace_back(TokenType::Minus, "-", startPos, 1);
            }
        }
        // Handle tilde (fuzzy search)
        else if (c == '~' && config_.allowFuzzy) {
            advance();
            tokens.emplace_back(TokenType::Tilde, "~", startPos, 1);
        }
        // Handle star (wildcard)
        else if (c == '*' && config_.allowWildcards) {
            advance();
            tokens.emplace_back(TokenType::Star, "*", startPos, 1);
        }
        // Handle question mark (wildcard)
        else if (c == '?' && config_.allowWildcards) {
            advance();
            tokens.emplace_back(TokenType::Question, "?", startPos, 1);
        }
        // Handle caret (boost)
        else if (c == '^') {
            advance();
            tokens.emplace_back(TokenType::Caret, "^", startPos, 1);
        }
        // Handle exclamation mark (NOT)
        else if (c == '!') {
            advance();
            tokens.emplace_back(TokenType::Not, "!", startPos, 1);
        }
        // Handle ampersands (AND)
        else if (c == '&') {
            advance();
            if (peek() == '&') {
                advance();
                tokens.emplace_back(TokenType::And, "&&", startPos, 2);
            } else {
                throw TokenizerException("Single '&' not allowed, use '&&' for AND", startPos);
            }
        }
        // Handle pipes (OR)
        else if (c == '|') {
            advance();
            if (peek() == '|') {
                advance();
                tokens.emplace_back(TokenType::Or, "||", startPos, 2);
            } else {
                throw TokenizerException("Single '|' not allowed, use '||' for OR", startPos);
            }
        }
        // Handle terms and operators
        else {
            Token token = readTerm();

            // Check if it's a reserved word
            std::string upper = token.value;
            std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char ch) {
                return static_cast<char>(std::toupper(ch));
            });

            if (upper == "AND") {
                tokens.emplace_back(TokenType::And, token.value, token.position, token.length);
            } else if (upper == "OR") {
                tokens.emplace_back(TokenType::Or, token.value, token.position, token.length);
            } else if (upper == "NOT") {
                tokens.emplace_back(TokenType::Not, token.value, token.position, token.length);
            } else if (upper == "TO" && config_.allowRanges) {
                tokens.emplace_back(TokenType::To, token.value, token.position, token.length);
            } else {
                tokens.push_back(token);
            }
        }
    }

    tokens.emplace_back(TokenType::EndOfInput, "", query_.length(), 0);
    return tokens;
}

char QueryTokenizer::peek() const {
    if (isAtEnd())
        return '\0';
    return query_[position_];
}

char QueryTokenizer::peekNext() const {
    if (position_ + 1 >= query_.length())
        return '\0';
    return query_[position_ + 1];
}

char QueryTokenizer::advance() {
    if (isAtEnd())
        return '\0';
    return query_[position_++];
}

bool QueryTokenizer::isAtEnd() const {
    return position_ >= query_.length();
}

void QueryTokenizer::skipWhitespace() {
    while (!isAtEnd() && std::isspace(static_cast<unsigned char>(peek()))) {
        advance();
    }
}

Token QueryTokenizer::readQuotedString() {
    size_t startPos = position_;
    advance(); // Skip opening quote

    std::string value;
    while (!isAtEnd() && peek() != '"') {
        if (peek() == '\\' && peekNext() == '"') {
            advance();          // Skip backslash
            value += advance(); // Add quote
        } else {
            value += advance();
        }
    }

    if (isAtEnd()) {
        throw TokenizerException("Unterminated quoted string", startPos);
    }

    advance(); // Skip closing quote
    return Token(TokenType::QuotedString, value, startPos, position_ - startPos);
}

Token QueryTokenizer::readTerm() {
    size_t startPos = position_;
    std::string value;

    while (!isAtEnd() && isTermChar(peek())) {
        value += advance();
    }

    // Check if wildcards immediately follow the term
    while (!isAtEnd() && (peek() == '*' || peek() == '?') && config_.allowWildcards) {
        // If we have characters before the wildcard, include it in the term
        if (!value.empty()) {
            value += advance();
        } else {
            // Standalone wildcard, break
            break;
        }
    }

    if (value.empty()) {
        throw TokenizerException("Empty term", startPos);
    }

    return Token(TokenType::Term, value, startPos, value.length());
}

bool QueryTokenizer::isTermChar(char c) const {
    // Special characters that end a term
    static const std::unordered_set<char> specialChars = {' ', '\t', '\n', '\r', '(', ')', '[',
                                                          ']', '{',  '}',  ':',  '"', '&', '|',
                                                          '!', '+',  '-',  '~',  '*', '?', '^'};

    return specialChars.find(c) == specialChars.end();
}

bool QueryTokenizer::isReservedWord(const std::string& word) const {
    static const std::unordered_set<std::string> reserved = {"AND", "OR", "NOT", "TO"};

    std::string upper = word;
    std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return reserved.find(upper) != reserved.end();
}

} // namespace yams::search