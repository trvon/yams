#include <algorithm>
#include <cctype>
#include <yams/search/query_literal_extractor.hpp>

namespace yams::search {

QueryLiteralExtractor::QueryLiterals QueryLiteralExtractor::extract(const QueryNode* queryAst) {
    QueryLiterals result;

    if (!queryAst) {
        return result;
    }

    // Collect all literal terms from AST
    collectTerms(queryAst, result.terms, result.hasPureTerms);

    if (result.terms.empty()) {
        return result;
    }

    // Select most selective term
    result.mostSelective = selectMostSelective(result.terms);
    result.estimatedSelectivity = estimateSelectivity(result.mostSelective);

    return result;
}

void QueryLiteralExtractor::collectTerms(const QueryNode* node, std::vector<std::string>& terms,
                                         bool& hasPureTerms) {
    if (!node) {
        return;
    }

    switch (node->getType()) {
        case QueryNodeType::Term: {
            auto* termNode = static_cast<const TermNode*>(node);
            terms.push_back(termNode->getTerm());
            hasPureTerms = true;
            break;
        }

        case QueryNodeType::Phrase: {
            auto* phraseNode = static_cast<const PhraseNode*>(node);
            // Phrases are highly selective - add the full phrase
            terms.push_back(phraseNode->getPhrase());
            hasPureTerms = true;
            break;
        }

        case QueryNodeType::And: {
            auto* andNode = static_cast<const AndNode*>(node);
            collectTerms(andNode->getLeft(), terms, hasPureTerms);
            collectTerms(andNode->getRight(), terms, hasPureTerms);
            break;
        }

        case QueryNodeType::Or: {
            auto* orNode = static_cast<const OrNode*>(node);
            // For OR, collect from both sides but they're less selective
            collectTerms(orNode->getLeft(), terms, hasPureTerms);
            collectTerms(orNode->getRight(), terms, hasPureTerms);
            break;
        }

        case QueryNodeType::Not: {
            // Skip NOT terms - they're exclusions, not useful for pre-filtering
            break;
        }

        case QueryNodeType::Field: {
            auto* fieldNode = static_cast<const FieldNode*>(node);
            // Collect terms from field value
            collectTerms(fieldNode->getValue(), terms, hasPureTerms);
            break;
        }

        case QueryNodeType::Group: {
            auto* groupNode = static_cast<const GroupNode*>(node);
            collectTerms(groupNode->getChild(), terms, hasPureTerms);
            break;
        }

        case QueryNodeType::Wildcard:
        case QueryNodeType::Fuzzy:
        case QueryNodeType::Range:
            // Skip fuzzy/wildcard/range - not useful for literal pre-filtering
            break;
    }
}

std::string QueryLiteralExtractor::selectMostSelective(const std::vector<std::string>& terms) {
    if (terms.empty()) {
        return "";
    }

    if (terms.size() == 1) {
        return terms[0];
    }

    // Score each term and select highest
    std::string best = terms[0];
    int bestScore = scoreTermSelectivity(best);

    for (size_t i = 1; i < terms.size(); ++i) {
        int score = scoreTermSelectivity(terms[i]);
        if (score > bestScore) {
            bestScore = score;
            best = terms[i];
        }
    }

    return best;
}

int QueryLiteralExtractor::scoreTermSelectivity(const std::string& term) {
    if (term.empty()) {
        return 0;
    }

    int score = 0;

    // Longer terms are more selective
    score += static_cast<int>(term.length()) * 10;

    // Terms with uppercase letters are more selective (CamelCase, CLASS_NAME)
    int uppercaseCount = 0;
    for (char c : term) {
        if (std::isupper(static_cast<unsigned char>(c))) {
            uppercaseCount++;
        }
    }
    score += uppercaseCount * 20;

    // Terms with non-alphanumeric chars are more selective (file.cpp, my-function)
    int specialCount = 0;
    for (char c : term) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != ' ') {
            specialCount++;
        }
    }
    score += specialCount * 15;

    // Penalize very common short words
    if (term.length() <= 3) {
        static const std::vector<std::string> commonWords = {
            "the", "and", "for", "are", "but", "not", "you", "all", "can", "her",
            "was", "one", "our", "out", "has", "had", "his", "how", "its", "may"};
        std::string lower = term;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (std::find(commonWords.begin(), commonWords.end(), lower) != commonWords.end()) {
            score -= 100; // Heavy penalty for common words
        }
    }

    return score;
}

float QueryLiteralExtractor::estimateSelectivity(const std::string& term) {
    if (term.empty()) {
        return 1.0f; // Matches everything
    }

    // Heuristic: longer, more complex terms match fewer documents
    // This is a rough estimate for optimization decisions

    float selectivity = 1.0f;

    // Length factor: longer = more selective
    if (term.length() >= 10) {
        selectivity *= 0.1f; // Very selective
    } else if (term.length() >= 6) {
        selectivity *= 0.3f; // Moderately selective
    } else if (term.length() >= 3) {
        selectivity *= 0.5f; // Somewhat selective
    }
    // Short terms (<3 chars) remain at 1.0

    // Uppercase factor
    bool hasUpper = false;
    for (char c : term) {
        if (std::isupper(static_cast<unsigned char>(c))) {
            hasUpper = true;
            break;
        }
    }
    if (hasUpper) {
        selectivity *= 0.7f; // More selective
    }

    return selectivity;
}

} // namespace yams::search
