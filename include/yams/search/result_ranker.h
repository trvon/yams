#pragma once

#include <yams/search/search_results.h>
#include <yams/search/query_ast.h>
#include <unordered_map>
#include <vector>
#include <string>

namespace yams::search {

/**
 * @brief Configuration for result ranking
 */
struct RankingConfig {
    // TF-IDF weights
    float termFrequencyWeight = 1.0f;
    float inverseDocumentFrequencyWeight = 1.0f;
    
    // Field-specific boosts
    std::unordered_map<std::string, float> fieldBoosts = {
        {"title", 2.0f},
        {"content", 1.0f},
        {"tags", 1.5f},
        {"description", 1.2f}
    };
    
    // Document freshness boost
    float freshnessWeight = 0.1f;
    float maxFreshnessBoost = 0.2f;
    
    // Document size penalty/boost
    float sizeWeight = 0.05f;
    size_t optimalSize = 10000; // Bytes - documents near this size get slight boost
    
    // Language confidence boost
    float languageConfidenceWeight = 0.1f;
    
    // Phrase match bonus
    float phraseMatchBonus = 0.3f;
    
    // Exact match bonus
    float exactMatchBonus = 0.5f;
    
    // Position-based scoring (earlier matches score higher)
    float positionWeight = 0.1f;
};

/**
 * @brief Statistics about the document corpus (for IDF calculation)
 */
struct CorpusStatistics {
    size_t totalDocuments = 0;
    std::unordered_map<std::string, size_t> termDocumentFrequency;
    
    /**
     * @brief Get IDF score for a term
     */
    float getIDF(const std::string& term) const {
        auto it = termDocumentFrequency.find(term);
        if (it == termDocumentFrequency.end() || it->second == 0) {
            return 0.0f;
        }
        
        return std::log(static_cast<float>(totalDocuments) / static_cast<float>(it->second));
    }
};

/**
 * @brief Result ranking and scoring engine
 */
class ResultRanker {
public:
    explicit ResultRanker(const RankingConfig& config = {});
    
    /**
     * @brief Set ranking configuration
     */
    void setConfig(const RankingConfig& config) { config_ = config; }
    
    /**
     * @brief Get current configuration
     */
    const RankingConfig& getConfig() const { return config_; }
    
    /**
     * @brief Set corpus statistics for IDF calculation
     */
    void setCorpusStatistics(const CorpusStatistics& stats) { corpusStats_ = stats; }
    
    /**
     * @brief Calculate relevance score for a search result
     */
    float calculateScore(const SearchResultItem& item, const QueryNode* query) const;
    
    /**
     * @brief Rank a collection of search results
     */
    void rankResults(std::vector<SearchResultItem>& results, const QueryNode* query) const;
    
    /**
     * @brief Calculate TF-IDF score
     */
    float calculateTFIDF(const std::string& term, 
                         float termFrequency,
                         const std::string& field = "content") const;
    
    /**
     * @brief Calculate freshness score based on document age
     */
    float calculateFreshnessScore(const std::chrono::system_clock::time_point& lastModified) const;
    
    /**
     * @brief Calculate size score (documents near optimal size get higher score)
     */
    float calculateSizeScore(size_t fileSize) const;
    
    /**
     * @brief Calculate position-based score (earlier matches get higher score)
     */
    float calculatePositionScore(const std::vector<SearchHighlight>& highlights) const;
    
    /**
     * @brief Calculate phrase match bonus
     */
    float calculatePhraseBonus(const SearchResultItem& item, const QueryNode* query) const;
    
    /**
     * @brief Calculate exact match bonus
     */
    float calculateExactMatchBonus(const SearchResultItem& item, const QueryNode* query) const;
    
    /**
     * @brief Extract terms from query for scoring
     */
    std::vector<std::string> extractQueryTerms(const QueryNode* query) const;
    
    /**
     * @brief Update term frequencies for a document (for corpus statistics)
     */
    void updateTermFrequencies(const std::string& documentText, 
                              const std::string& documentId);

private:
    RankingConfig config_;
    CorpusStatistics corpusStats_;
    
    /**
     * @brief Helper method to recursively extract terms from query AST
     */
    void extractTermsRecursive(const QueryNode* node, std::vector<std::string>& terms) const;
    
    /**
     * @brief Calculate field boost multiplier
     */
    float getFieldBoost(const std::string& field) const {
        auto it = config_.fieldBoosts.find(field);
        return it != config_.fieldBoosts.end() ? it->second : 1.0f;
    }
    
    /**
     * @brief Normalize score to 0-1 range
     */
    float normalizeScore(float score) const {
        return std::tanh(score); // Use tanh for smooth normalization
    }
};

/**
 * @brief Different ranking algorithms that can be applied
 */
enum class RankingAlgorithm {
    TfIdf,          // Traditional TF-IDF
    BM25,           // Best Matching 25
    Cosine,         // Cosine similarity
    Hybrid          // Combination of multiple algorithms
};

/**
 * @brief Advanced ranking engine with multiple algorithm support
 */
class AdvancedRanker {
public:
    explicit AdvancedRanker(RankingAlgorithm algorithm = RankingAlgorithm::Hybrid);
    
    /**
     * @brief Set ranking algorithm
     */
    void setAlgorithm(RankingAlgorithm algorithm) { algorithm_ = algorithm; }
    
    /**
     * @brief Rank results using the selected algorithm
     */
    void rankResults(std::vector<SearchResultItem>& results, const QueryNode* query) const;
    
private:
    RankingAlgorithm algorithm_;
    ResultRanker baseRanker_;
    
    /**
     * @brief BM25 scoring algorithm
     */
    float calculateBM25Score(const SearchResultItem& item, const QueryNode* query) const;
    
    /**
     * @brief Cosine similarity scoring
     */
    float calculateCosineScore(const SearchResultItem& item, const QueryNode* query) const;
    
    /**
     * @brief Hybrid scoring combining multiple algorithms
     */
    float calculateHybridScore(const SearchResultItem& item, const QueryNode* query) const;
};

} // namespace yams::search