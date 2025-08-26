#include <algorithm>
#include <cmath>
#include <regex>
#include <sstream>
#include <unordered_set>
#include <yams/search/result_ranker.h>

namespace yams::search {

ResultRanker::ResultRanker(const RankingConfig& config) : config_(config) {}

float ResultRanker::calculateScore(const SearchResultItem& item, const QueryNode* query) const {
    if (!query)
        return 0.0f;

    float score = 0.0f;

    // Extract query terms for analysis
    auto queryTerms = extractQueryTerms(query);

    // Calculate TF-IDF component
    float tfIdfScore = 0.0f;
    for (const auto& term : queryTerms) {
        tfIdfScore += calculateTFIDF(term, item.termFrequency);
    }

    // Apply field boosts based on highlights
    float fieldBoostScore = 0.0f;
    for (const auto& highlight : item.highlights) {
        fieldBoostScore += getFieldBoost(highlight.field);
    }

    // Calculate freshness score
    float freshnessScore = calculateFreshnessScore(item.lastModified);

    // Calculate size score
    float sizeScore = calculateSizeScore(item.fileSize);

    // Calculate position score
    float positionScore = calculatePositionScore(item.highlights);

    // Calculate language confidence boost
    float languageScore = item.languageConfidence * config_.languageConfidenceWeight;

    // Calculate phrase and exact match bonuses
    float phraseBonus = calculatePhraseBonus(item, query);
    float exactMatchBonus = calculateExactMatchBonus(item, query);

    // Combine all components
    score = tfIdfScore + fieldBoostScore + freshnessScore * config_.freshnessWeight +
            sizeScore * config_.sizeWeight + positionScore * config_.positionWeight +
            languageScore + phraseBonus + exactMatchBonus;

    return normalizeScore(score);
}

void ResultRanker::rankResults(std::vector<SearchResultItem>& results,
                               const QueryNode* query) const {
    // Calculate scores for all results
    for (auto& result : results) {
        result.relevanceScore = calculateScore(result, query);
    }

    // Sort by relevance score (descending)
    std::sort(results.begin(), results.end(),
              [](const SearchResultItem& a, const SearchResultItem& b) {
                  return a.relevanceScore > b.relevanceScore;
              });
}

float ResultRanker::calculateTFIDF(const std::string& term, float termFrequency,
                                   const std::string& field) const {
    if (termFrequency == 0.0f)
        return 0.0f;

    // Calculate TF (Term Frequency) component
    float tf = termFrequency * config_.termFrequencyWeight;

    // Calculate IDF (Inverse Document Frequency) component
    float idf = corpusStats_.getIDF(term) * config_.inverseDocumentFrequencyWeight;

    // Apply field boost
    float fieldBoost = getFieldBoost(field);

    return tf * idf * fieldBoost;
}

float ResultRanker::calculateFreshnessScore(
    const std::chrono::system_clock::time_point& lastModified) const {
    auto now = std::chrono::system_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::hours>(now - lastModified).count();

    // Documents get less fresh over time, with a maximum boost for very recent documents
    float freshnessScore =
        std::exp(-static_cast<float>(age) / (24.0f * 30.0f)); // Decay over 30 days
    return std::min(freshnessScore, config_.maxFreshnessBoost);
}

float ResultRanker::calculateSizeScore(size_t fileSize) const {
    if (fileSize == 0)
        return 0.0f;

    // Documents near the optimal size get higher scores
    float sizeDiff =
        std::abs(static_cast<float>(fileSize) - static_cast<float>(config_.optimalSize));
    float sizeRatio = sizeDiff / static_cast<float>(config_.optimalSize);

    // Use inverse exponential decay - closer to optimal size = higher score
    return std::exp(-sizeRatio);
}

float ResultRanker::calculatePositionScore(const std::vector<SearchHighlight>& highlights) const {
    if (highlights.empty())
        return 0.0f;

    float positionScore = 0.0f;

    for (const auto& highlight : highlights) {
        // Earlier positions (lower offset) get higher scores
        float normalizedPosition =
            1.0f / (1.0f + static_cast<float>(highlight.startOffset) / 1000.0f);
        positionScore += normalizedPosition;
    }

    return positionScore / static_cast<float>(highlights.size()); // Average position score
}

float ResultRanker::calculatePhraseBonus(const SearchResultItem& item,
                                         const QueryNode* query) const {
    // This is a simplified implementation - would need more sophisticated phrase matching
    // For now, check if any highlights contain multiple consecutive terms

    auto queryTerms = extractQueryTerms(query);
    if (queryTerms.size() < 2)
        return 0.0f;

    for (const auto& highlight : item.highlights) {
        // Simple check: if highlight contains multiple query terms close together
        size_t termsFound = 0;
        for (const auto& term : queryTerms) {
            if (highlight.snippet.find(term) != std::string::npos) {
                termsFound++;
            }
        }

        if (termsFound >= 2) {
            return config_.phraseMatchBonus;
        }
    }

    return 0.0f;
}

float ResultRanker::calculateExactMatchBonus(const SearchResultItem& item,
                                             const QueryNode* query) const {
    auto queryTerms = extractQueryTerms(query);

    // Check if title or any highlight contains exact query terms
    for (const auto& term : queryTerms) {
        if (item.title.find(term) != std::string::npos) {
            return config_.exactMatchBonus;
        }

        for (const auto& highlight : item.highlights) {
            if (highlight.snippet.find(term) != std::string::npos) {
                return config_.exactMatchBonus;
            }
        }
    }

    return 0.0f;
}

std::vector<std::string> ResultRanker::extractQueryTerms(const QueryNode* query) const {
    std::vector<std::string> terms;
    extractTermsRecursive(query, terms);
    return terms;
}

void ResultRanker::extractTermsRecursive(const QueryNode* node,
                                         std::vector<std::string>& terms) const {
    if (!node)
        return;

    switch (node->getType()) {
        case QueryNodeType::Term: {
            auto termNode = static_cast<const TermNode*>(node);
            terms.push_back(termNode->getTerm());
            break;
        }

        case QueryNodeType::Phrase: {
            auto phraseNode = static_cast<const PhraseNode*>(node);
            // Split phrase into individual terms
            std::istringstream iss(phraseNode->getPhrase());
            std::string word;
            while (iss >> word) {
                terms.push_back(word);
            }
            break;
        }

        case QueryNodeType::And:
        case QueryNodeType::Or: {
            auto binOp = static_cast<const BinaryOpNode*>(node);
            extractTermsRecursive(binOp->getLeft(), terms);
            extractTermsRecursive(binOp->getRight(), terms);
            break;
        }

        case QueryNodeType::Not: {
            auto notNode = static_cast<const NotNode*>(node);
            extractTermsRecursive(notNode->getChild(), terms);
            break;
        }

        case QueryNodeType::Field: {
            auto fieldNode = static_cast<const FieldNode*>(node);
            extractTermsRecursive(fieldNode->getValue(), terms);
            break;
        }

        case QueryNodeType::Wildcard: {
            auto wildcardNode = static_cast<const WildcardNode*>(node);
            // Remove wildcards from pattern for term extraction
            std::string pattern = wildcardNode->getPattern();
            pattern.erase(std::remove(pattern.begin(), pattern.end(), '*'), pattern.end());
            pattern.erase(std::remove(pattern.begin(), pattern.end(), '?'), pattern.end());
            if (!pattern.empty()) {
                terms.push_back(pattern);
            }
            break;
        }

        case QueryNodeType::Fuzzy: {
            auto fuzzyNode = static_cast<const FuzzyNode*>(node);
            terms.push_back(fuzzyNode->getTerm());
            break;
        }

        case QueryNodeType::Group: {
            auto groupNode = static_cast<const GroupNode*>(node);
            extractTermsRecursive(groupNode->getChild(), terms);
            break;
        }

        default:
            break;
    }
}

void ResultRanker::updateTermFrequencies(const std::string& documentText,
                                         [[maybe_unused]] const std::string& documentId) {
    // Simple term extraction - split by whitespace and punctuation
    std::regex wordRegex(R"(\b\w+\b)");
    std::sregex_iterator iter(documentText.begin(), documentText.end(), wordRegex);
    std::sregex_iterator end;

    std::unordered_set<std::string> documentTerms; // Unique terms in this document

    for (; iter != end; ++iter) {
        std::string term = iter->str();
        std::transform(term.begin(), term.end(), term.begin(), ::tolower);
        documentTerms.insert(term);
    }

    // Update corpus statistics
    for (const auto& term : documentTerms) {
        corpusStats_.termDocumentFrequency[term]++;
    }

    corpusStats_.totalDocuments++;
}

// AdvancedRanker implementation

AdvancedRanker::AdvancedRanker(RankingAlgorithm algorithm) : algorithm_(algorithm) {}

void AdvancedRanker::rankResults(std::vector<SearchResultItem>& results,
                                 const QueryNode* query) const {
    switch (algorithm_) {
        case RankingAlgorithm::TfIdf:
            baseRanker_.rankResults(results, query);
            break;

        case RankingAlgorithm::BM25:
            for (auto& result : results) {
                result.relevanceScore = calculateBM25Score(result, query);
            }
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.relevanceScore > b.relevanceScore;
                      });
            break;

        case RankingAlgorithm::Cosine:
            for (auto& result : results) {
                result.relevanceScore = calculateCosineScore(result, query);
            }
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.relevanceScore > b.relevanceScore;
                      });
            break;

        case RankingAlgorithm::Hybrid:
            for (auto& result : results) {
                result.relevanceScore = calculateHybridScore(result, query);
            }
            std::sort(results.begin(), results.end(),
                      [](const SearchResultItem& a, const SearchResultItem& b) {
                          return a.relevanceScore > b.relevanceScore;
                      });
            break;
    }
}

float AdvancedRanker::calculateBM25Score(const SearchResultItem& item,
                                         const QueryNode* query) const {
    // Simplified BM25 implementation
    float k1 = 1.2f;
    float b = 0.75f;
    float avgDocLength = 1000.0f; // Would need to calculate from corpus

    auto queryTerms = baseRanker_.extractQueryTerms(query);
    float score = 0.0f;

    for ([[maybe_unused]] const auto& term : queryTerms) {
        float tf = item.termFrequency;
        float idf = std::log((100.0f + 1.0f) / (1.0f + 1.0f)); // Simplified IDF

        float numerator = tf * (k1 + 1.0f);
        float denominator =
            tf + k1 * (1.0f - b + b * (static_cast<float>(item.fileSize) / avgDocLength));

        score += idf * (numerator / denominator);
    }

    return score;
}

float AdvancedRanker::calculateCosineScore(const SearchResultItem& item,
                                           const QueryNode* query) const {
    // Simplified cosine similarity - would need proper vector space implementation
    return baseRanker_.calculateScore(item, query) * 0.8f; // Placeholder
}

float AdvancedRanker::calculateHybridScore(const SearchResultItem& item,
                                           const QueryNode* query) const {
    float tfIdfScore = baseRanker_.calculateScore(item, query);
    float bm25Score = calculateBM25Score(item, query);
    float cosineScore = calculateCosineScore(item, query);

    // Weighted combination
    return 0.5f * tfIdfScore + 0.3f * bm25Score + 0.2f * cosineScore;
}

} // namespace yams::search