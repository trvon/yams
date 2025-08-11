#pragma once

#include <yams/core/types.h>
#include <memory>
#include <string>
#include <vector>
#include <variant>

namespace yams::search {

/**
 * @brief Types of query nodes in the AST
 */
enum class QueryNodeType {
    Term,       // Single search term
    Phrase,     // Phrase search (quoted text)
    And,        // AND operator
    Or,         // OR operator
    Not,        // NOT operator
    Field,      // Field-specific search
    Wildcard,   // Wildcard search
    Fuzzy,      // Fuzzy search
    Range,      // Range query
    Group       // Grouped expression
};

/**
 * @brief Base class for query AST nodes
 */
class QueryNode {
public:
    virtual ~QueryNode() = default;
    virtual QueryNodeType getType() const = 0;
    virtual std::string toString() const = 0;
    virtual std::unique_ptr<QueryNode> clone() const = 0;
};

/**
 * @brief Term node - represents a single search term
 */
class TermNode : public QueryNode {
public:
    explicit TermNode(const std::string& term) : term_(term) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Term; }
    std::string toString() const override { return term_; }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<TermNode>(term_);
    }
    
    const std::string& getTerm() const { return term_; }
    
private:
    std::string term_;
};

/**
 * @brief Phrase node - represents a phrase search
 */
class PhraseNode : public QueryNode {
public:
    explicit PhraseNode(const std::string& phrase) : phrase_(phrase) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Phrase; }
    std::string toString() const override { return "\"" + phrase_ + "\""; }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<PhraseNode>(phrase_);
    }
    
    const std::string& getPhrase() const { return phrase_; }
    
private:
    std::string phrase_;
};

/**
 * @brief Binary operator node base class
 */
class BinaryOpNode : public QueryNode {
public:
    BinaryOpNode(std::unique_ptr<QueryNode> left, 
                 std::unique_ptr<QueryNode> right)
        : left_(std::move(left)), right_(std::move(right)) {}
    
    const QueryNode* getLeft() const { return left_.get(); }
    const QueryNode* getRight() const { return right_.get(); }
    
protected:
    std::unique_ptr<QueryNode> left_;
    std::unique_ptr<QueryNode> right_;
};

/**
 * @brief AND operator node
 */
class AndNode : public BinaryOpNode {
public:
    using BinaryOpNode::BinaryOpNode;
    
    QueryNodeType getType() const override { return QueryNodeType::And; }
    std::string toString() const override {
        return "(" + left_->toString() + " AND " + right_->toString() + ")";
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<AndNode>(left_->clone(), right_->clone());
    }
};

/**
 * @brief OR operator node
 */
class OrNode : public BinaryOpNode {
public:
    using BinaryOpNode::BinaryOpNode;
    
    QueryNodeType getType() const override { return QueryNodeType::Or; }
    std::string toString() const override {
        return "(" + left_->toString() + " OR " + right_->toString() + ")";
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<OrNode>(left_->clone(), right_->clone());
    }
};

/**
 * @brief NOT operator node
 */
class NotNode : public QueryNode {
public:
    explicit NotNode(std::unique_ptr<QueryNode> child)
        : child_(std::move(child)) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Not; }
    std::string toString() const override {
        return "NOT " + child_->toString();
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<NotNode>(child_->clone());
    }
    
    const QueryNode* getChild() const { return child_.get(); }
    
private:
    std::unique_ptr<QueryNode> child_;
};

/**
 * @brief Field-specific search node
 */
class FieldNode : public QueryNode {
public:
    FieldNode(const std::string& field, std::unique_ptr<QueryNode> value)
        : field_(field), value_(std::move(value)) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Field; }
    std::string toString() const override {
        return field_ + ":" + value_->toString();
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<FieldNode>(field_, value_->clone());
    }
    
    const std::string& getField() const { return field_; }
    const QueryNode* getValue() const { return value_.get(); }
    
private:
    std::string field_;
    std::unique_ptr<QueryNode> value_;
};

/**
 * @brief Wildcard search node
 */
class WildcardNode : public QueryNode {
public:
    explicit WildcardNode(const std::string& pattern) : pattern_(pattern) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Wildcard; }
    std::string toString() const override { return pattern_; }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<WildcardNode>(pattern_);
    }
    
    const std::string& getPattern() const { return pattern_; }
    
private:
    std::string pattern_;
};

/**
 * @brief Fuzzy search node
 */
class FuzzyNode : public QueryNode {
public:
    FuzzyNode(const std::string& term, int distance = 2)
        : term_(term), distance_(distance) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Fuzzy; }
    std::string toString() const override {
        return term_ + "~" + std::to_string(distance_);
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<FuzzyNode>(term_, distance_);
    }
    
    const std::string& getTerm() const { return term_; }
    int getDistance() const { return distance_; }
    
private:
    std::string term_;
    int distance_;
};

/**
 * @brief Range query node
 */
class RangeNode : public QueryNode {
public:
    RangeNode(const std::string& field,
              const std::string& lower,
              const std::string& upper,
              bool includeLower = true,
              bool includeUpper = true)
        : field_(field), lower_(lower), upper_(upper),
          includeLower_(includeLower), includeUpper_(includeUpper) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Range; }
    std::string toString() const override {
        std::string left = includeLower_ ? "[" : "{";
        std::string right = includeUpper_ ? "]" : "}";
        return field_ + ":" + left + lower_ + " TO " + upper_ + right;
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<RangeNode>(field_, lower_, upper_, 
                                          includeLower_, includeUpper_);
    }
    
    const std::string& getField() const { return field_; }
    const std::string& getLower() const { return lower_; }
    const std::string& getUpper() const { return upper_; }
    bool isIncludeLower() const { return includeLower_; }
    bool isIncludeUpper() const { return includeUpper_; }
    
private:
    std::string field_;
    std::string lower_;
    std::string upper_;
    bool includeLower_;
    bool includeUpper_;
};

/**
 * @brief Group node - represents a grouped expression
 */
class GroupNode : public QueryNode {
public:
    explicit GroupNode(std::unique_ptr<QueryNode> child)
        : child_(std::move(child)) {}
    
    QueryNodeType getType() const override { return QueryNodeType::Group; }
    std::string toString() const override {
        return "(" + child_->toString() + ")";
    }
    std::unique_ptr<QueryNode> clone() const override {
        return std::make_unique<GroupNode>(child_->clone());
    }
    
    const QueryNode* getChild() const { return child_.get(); }
    
private:
    std::unique_ptr<QueryNode> child_;
};

} // namespace yams::search