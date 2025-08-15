#include <algorithm>
#include <sstream>
#include <stack>
#include <yams/search/query_parser.h>

namespace yams::search {

QueryParser::QueryParser(const QueryParserConfig& config) : config_(config), currentToken_(0) {
    QueryTokenizer::Config tokConfig;
    tokConfig.allowWildcards = true;
    tokConfig.allowFuzzy = true;
    tokConfig.allowRanges = true;
    tokConfig.allowFieldQueries = true;
    tokenizer_.setConfig(tokConfig);
}

Result<std::unique_ptr<QueryNode>> QueryParser::parse(const std::string& query) {
    if (query.empty()) {
        return Error{ErrorCode::InvalidArgument, "Empty query string"};
    }

    try {
        // Tokenize the query
        tokens_ = tokenizer_.tokenize(query);
        currentToken_ = 0;

        // Parse the expression
        auto ast = parseExpression();

        // Ensure we've consumed all tokens
        if (!isAtEnd() && current().type != TokenType::EndOfInput) {
            throwError("Unexpected token at end of query");
        }

        return ast;

    } catch (const TokenizerException& e) {
        return Error{ErrorCode::InvalidArgument, "Tokenizer error: " + std::string(e.what())};
    } catch (const QueryParserException& e) {
        return Error{ErrorCode::InvalidArgument, "Parser error: " + std::string(e.what())};
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, "Unexpected error: " + std::string(e.what())};
    }
}

QueryValidation QueryParser::validate(const std::string& query) {
    auto result = parse(query);
    if (result) {
        return QueryValidation::valid();
    } else {
        return QueryValidation::invalid(result.error().message);
    }
}

std::string QueryParser::toFTS5Query(const QueryNode* node) {
    if (!node)
        return "";

    switch (node->getType()) {
        case QueryNodeType::Term: {
            auto termNode = static_cast<const TermNode*>(node);
            return escapeForFTS5(termNode->getTerm());
        }

        case QueryNodeType::Phrase: {
            auto phraseNode = static_cast<const PhraseNode*>(node);
            return "\"" + escapeForFTS5(phraseNode->getPhrase()) + "\"";
        }

        case QueryNodeType::And: {
            auto andNode = static_cast<const AndNode*>(node);
            return "(" + toFTS5Query(andNode->getLeft()) + " " + toFTS5Query(andNode->getRight()) +
                   ")";
        }

        case QueryNodeType::Or: {
            auto orNode = static_cast<const OrNode*>(node);
            return "(" + toFTS5Query(orNode->getLeft()) + " OR " + toFTS5Query(orNode->getRight()) +
                   ")";
        }

        case QueryNodeType::Not: {
            auto notNode = static_cast<const NotNode*>(node);
            return "NOT " + toFTS5Query(notNode->getChild());
        }

        case QueryNodeType::Field: {
            auto fieldNode = static_cast<const FieldNode*>(node);
            return fieldToFTS5(fieldNode->getField(), toFTS5Query(fieldNode->getValue()));
        }

        case QueryNodeType::Wildcard: {
            auto wildcardNode = static_cast<const WildcardNode*>(node);
            return wildcardNode->getPattern();
        }

        case QueryNodeType::Group: {
            auto groupNode = static_cast<const GroupNode*>(node);
            return "(" + toFTS5Query(groupNode->getChild()) + ")";
        }

        default:
            return node->toString();
    }
}

std::unique_ptr<QueryNode> QueryParser::parseExpression() {
    return parseOrExpression();
}

std::unique_ptr<QueryNode> QueryParser::parseOrExpression() {
    auto left = parseAndExpression();

    while (match(TokenType::Or)) {
        auto right = parseAndExpression();
        left = std::make_unique<OrNode>(std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<QueryNode> QueryParser::parseAndExpression() {
    auto left = parseNotExpression();

    while (current().type != TokenType::Or && current().type != TokenType::RightParen &&
           current().type != TokenType::EndOfInput) {
        // Implicit AND or explicit AND
        if (match(TokenType::And)) {
            auto right = parseNotExpression();
            left = std::make_unique<AndNode>(std::move(left), std::move(right));
        } else if (current().isTerm() || current().type == TokenType::LeftParen ||
                   current().type == TokenType::Not) {
            // Implicit AND based on default operator
            auto right = parseNotExpression();
            if (config_.defaultOperator == QueryParserConfig::DefaultOperator::And) {
                left = std::make_unique<AndNode>(std::move(left), std::move(right));
            } else {
                left = std::make_unique<OrNode>(std::move(left), std::move(right));
            }
        } else {
            break;
        }
    }

    return left;
}

std::unique_ptr<QueryNode> QueryParser::parseNotExpression() {
    if (match(TokenType::Not)) {
        return std::make_unique<NotNode>(parseNotExpression());
    }
    return parsePrimary();
}

std::unique_ptr<QueryNode> QueryParser::parsePrimary() {
    // Handle grouped expressions
    if (match(TokenType::LeftParen)) {
        auto expr = parseExpression();
        if (!match(TokenType::RightParen)) {
            throwError("Expected ')' after grouped expression");
        }
        return std::make_unique<GroupNode>(std::move(expr));
    }

    // Handle quoted phrases
    if (current().type == TokenType::QuotedString) {
        return parsePhrase();
    }

    // Handle terms (including field queries)
    if (current().type == TokenType::Term) {
        // Check for field query
        size_t savedPos = currentToken_;
        std::string term = current().value;
        advance();

        if (match(TokenType::Colon)) {
            // It's a field query
            currentToken_ = savedPos;
            return parseFieldQuery();
        } else {
            // Regular term, might have wildcards or fuzzy
            currentToken_ = savedPos;
            return parseTerm();
        }
    }

    throwError("Expected term, phrase, or grouped expression");
    return nullptr;
}

std::unique_ptr<QueryNode> QueryParser::parseTerm() {
    if (current().type != TokenType::Term) {
        throwError("Expected term");
    }

    std::string term = current().value;
    advance();

    // Check for wildcards in the term itself
    if (term.find('*') != std::string::npos || term.find('?') != std::string::npos) {
        return std::make_unique<WildcardNode>(term);
    }

    // Check if next token is a wildcard
    if (current().type == TokenType::Star || current().type == TokenType::Question) {
        // Append wildcards to the term
        while (current().type == TokenType::Star || current().type == TokenType::Question) {
            term += current().value;
            advance();
        }
        return std::make_unique<WildcardNode>(term);
    }

    // Check for fuzzy modifier
    if (match(TokenType::Tilde)) {
        int distance = 2; // Default fuzzy distance
        if (current().type == TokenType::Term) {
            // Try to parse distance
            try {
                distance = std::stoi(current().value);
                advance();
            } catch (...) {
                // Not a number, keep default
            }
        }
        return std::make_unique<FuzzyNode>(term, distance);
    }

    return std::make_unique<TermNode>(term);
}

std::unique_ptr<QueryNode> QueryParser::parseFieldQuery() {
    if (current().type != TokenType::Term) {
        throwError("Expected field name");
    }

    std::string field = current().value;
    advance();

    if (!match(TokenType::Colon)) {
        throwError("Expected ':' after field name");
    }

    // Check if field is searchable
    if (config_.searchableFields.find(field) == config_.searchableFields.end()) {
        throwError("Field '" + field + "' is not searchable");
    }

    // Parse the field value
    std::unique_ptr<QueryNode> value;

    if (current().type == TokenType::QuotedString) {
        value = parsePhrase();
    } else if (current().type == TokenType::LeftBracket || current().type == TokenType::LeftBrace) {
        value = parseRange();
    } else if (current().type == TokenType::Term) {
        value = parseTerm();
    } else if (current().type == TokenType::LeftParen) {
        advance();
        value = parseExpression();
        if (!match(TokenType::RightParen)) {
            throwError("Expected ')' after field value expression");
        }
    } else {
        throwError("Expected field value");
    }

    return std::make_unique<FieldNode>(field, std::move(value));
}

std::unique_ptr<QueryNode> QueryParser::parsePhrase() {
    if (current().type != TokenType::QuotedString) {
        throwError("Expected quoted string");
    }

    std::string phrase = current().value;
    advance();

    return std::make_unique<PhraseNode>(phrase);
}

std::unique_ptr<QueryNode> QueryParser::parseRange() {
    bool includeLower = false;
    bool includeUpper = false;

    if (match(TokenType::LeftBracket)) {
        includeLower = true;
    } else if (match(TokenType::LeftBrace)) {
        includeLower = false;
    } else {
        throwError("Expected '[' or '{' for range query");
    }

    if (current().type != TokenType::Term) {
        throwError("Expected lower bound for range");
    }
    std::string lower = current().value;
    advance();

    if (!match(TokenType::To)) {
        throwError("Expected 'TO' in range query");
    }

    if (current().type != TokenType::Term) {
        throwError("Expected upper bound for range");
    }
    std::string upper = current().value;
    advance();

    if (match(TokenType::RightBracket)) {
        includeUpper = true;
    } else if (match(TokenType::RightBrace)) {
        includeUpper = false;
    } else {
        throwError("Expected ']' or '}' for range query");
    }

    // Range queries need a field context, handled by parent
    return std::make_unique<RangeNode>("", lower, upper, includeLower, includeUpper);
}

const Token& QueryParser::current() const {
    if (currentToken_ >= tokens_.size()) {
        static Token endToken(TokenType::EndOfInput, "", 0, 0);
        return endToken;
    }
    return tokens_[currentToken_];
}

const Token& QueryParser::peek() const {
    if (currentToken_ + 1 >= tokens_.size()) {
        static Token endToken(TokenType::EndOfInput, "", 0, 0);
        return endToken;
    }
    return tokens_[currentToken_ + 1];
}

bool QueryParser::advance() {
    if (currentToken_ < tokens_.size()) {
        currentToken_++;
        return true;
    }
    return false;
}

bool QueryParser::check(TokenType type) const {
    return current().type == type;
}

bool QueryParser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

bool QueryParser::isAtEnd() const {
    return currentToken_ >= tokens_.size() || current().type == TokenType::EndOfInput;
}

void QueryParser::throwError(const std::string& message) {
    size_t position = current().position;
    throw QueryParserException(message + " at position " + std::to_string(position), position);
}

std::string QueryParser::escapeForFTS5(const std::string& term) {
    std::string escaped;
    for (char c : term) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    return escaped;
}

std::string QueryParser::fieldToFTS5(const std::string& field, const std::string& value) {
    // FTS5 uses column:value syntax
    return field + ":" + value;
}

// QueryOptimizer implementation

std::unique_ptr<QueryNode> QueryOptimizer::optimize(std::unique_ptr<QueryNode> node) {
    if (!node)
        return node;

    node = eliminateDoubleNegation(std::move(node));
    node = flattenBinaryOps(std::move(node));
    node = simplifyConstants(std::move(node));
    node = reorderForPerformance(std::move(node));

    return node;
}

std::unique_ptr<QueryNode>
QueryOptimizer::eliminateDoubleNegation(std::unique_ptr<QueryNode> node) {
    if (!node)
        return node;

    if (node->getType() == QueryNodeType::Not) {
        auto notNode = static_cast<NotNode*>(node.get());
        auto child = const_cast<QueryNode*>(notNode->getChild());

        if (child && child->getType() == QueryNodeType::Not) {
            // Double negation - extract the child's child
            auto innerNot = static_cast<NotNode*>(child);
            return innerNot->getChild()->clone();
        }
    }

    return node;
}

std::unique_ptr<QueryNode> QueryOptimizer::flattenBinaryOps(std::unique_ptr<QueryNode> node) {
    // TODO: Implement flattening of nested AND/OR operations
    return node;
}

std::unique_ptr<QueryNode> QueryOptimizer::simplifyConstants(std::unique_ptr<QueryNode> node) {
    // TODO: Implement simplification of constant expressions
    return node;
}

std::unique_ptr<QueryNode> QueryOptimizer::reorderForPerformance(std::unique_ptr<QueryNode> node) {
    // TODO: Reorder terms to optimize query execution
    // For example, put more selective terms first
    return node;
}

// QueryAnalyzer implementation

QueryAnalyzer::QueryStats QueryAnalyzer::analyze(const QueryNode* node) {
    QueryStats stats;
    if (node) {
        analyzeNode(node, stats, 0);
    }
    return stats;
}

void QueryAnalyzer::analyzeNode(const QueryNode* node, QueryStats& stats, size_t depth) {
    if (!node)
        return;

    stats.maxDepth = std::max(stats.maxDepth, depth);

    switch (node->getType()) {
        case QueryNodeType::Term: {
            stats.termCount++;
            auto termNode = static_cast<const TermNode*>(node);
            stats.terms.push_back(termNode->getTerm());
            break;
        }

        case QueryNodeType::Phrase: {
            stats.termCount++;
            auto phraseNode = static_cast<const PhraseNode*>(node);
            stats.terms.push_back(phraseNode->getPhrase());
            break;
        }

        case QueryNodeType::And:
        case QueryNodeType::Or: {
            stats.operatorCount++;
            auto binOp = static_cast<const BinaryOpNode*>(node);
            analyzeNode(binOp->getLeft(), stats, depth + 1);
            analyzeNode(binOp->getRight(), stats, depth + 1);
            break;
        }

        case QueryNodeType::Not: {
            stats.operatorCount++;
            auto notNode = static_cast<const NotNode*>(node);
            analyzeNode(notNode->getChild(), stats, depth + 1);
            break;
        }

        case QueryNodeType::Field: {
            stats.hasFieldQueries = true;
            auto fieldNode = static_cast<const FieldNode*>(node);
            stats.fields.push_back(fieldNode->getField());
            analyzeNode(fieldNode->getValue(), stats, depth + 1);
            break;
        }

        case QueryNodeType::Wildcard: {
            stats.hasWildcards = true;
            stats.termCount++;
            break;
        }

        case QueryNodeType::Fuzzy: {
            stats.hasFuzzy = true;
            stats.termCount++;
            auto fuzzyNode = static_cast<const FuzzyNode*>(node);
            stats.terms.push_back(fuzzyNode->getTerm());
            break;
        }

        case QueryNodeType::Range: {
            stats.hasRanges = true;
            break;
        }

        case QueryNodeType::Group: {
            auto groupNode = static_cast<const GroupNode*>(node);
            analyzeNode(groupNode->getChild(), stats, depth + 1);
            break;
        }
    }
}

} // namespace yams::search