#pragma once

#include <yams/core/types.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <algorithm>
#include <limits>

namespace yams::search {

/**
 * @brief Distance metric interface for BK-tree
 */
class IDistanceMetric {
public:
    virtual ~IDistanceMetric() = default;
    virtual size_t distance(const std::string& s1, const std::string& s2) const = 0;
};

/**
 * @brief Levenshtein distance metric
 */
class LevenshteinDistance : public IDistanceMetric {
public:
    size_t distance(const std::string& s1, const std::string& s2) const override;
};

/**
 * @brief Damerau-Levenshtein distance metric (handles transpositions)
 */
class DamerauLevenshteinDistance : public IDistanceMetric {
public:
    size_t distance(const std::string& s1, const std::string& s2) const override;
};

/**
 * @brief BK-tree node
 */
struct BKNode {
    std::string value;
    std::unordered_map<size_t, std::unique_ptr<BKNode>> children;
    
    explicit BKNode(const std::string& val) : value(val) {}
};

/**
 * @brief BK-tree for fast fuzzy string matching
 * 
 * A BK-tree (Burkhard-Keller tree) is a metric tree specifically adapted to 
 * discrete metric spaces. It's particularly useful for fuzzy string matching
 * as it can find all strings within a given edit distance efficiently.
 * 
 * Average time complexity: O(log n)
 * Worst case: O(n) for pathological cases
 */
class BKTree {
public:
    /**
     * @brief Construct BK-tree with specified distance metric
     * @param metric Distance metric to use (defaults to Levenshtein)
     */
    explicit BKTree(std::unique_ptr<IDistanceMetric> metric = 
                    std::make_unique<LevenshteinDistance>());
    
    /**
     * @brief Add a string to the tree
     * @param value String to add
     */
    void add(const std::string& value);
    
    /**
     * @brief Add multiple strings to the tree
     * @param values Strings to add
     */
    void addBatch(const std::vector<std::string>& values);
    
    /**
     * @brief Search for strings within a given distance
     * @param query Query string
     * @param maxDistance Maximum edit distance
     * @return Vector of matching strings with their distances
     */
    std::vector<std::pair<std::string, size_t>> search(
        const std::string& query, 
        size_t maxDistance) const;
    
    /**
     * @brief Search for best N matches
     * @param query Query string
     * @param maxResults Maximum number of results
     * @param maxDistance Maximum edit distance to consider
     * @return Vector of best matching strings with distances, sorted by distance
     */
    std::vector<std::pair<std::string, size_t>> searchBest(
        const std::string& query,
        size_t maxResults,
        size_t maxDistance = std::numeric_limits<size_t>::max()) const;
    
    /**
     * @brief Get the number of strings in the tree
     */
    size_t size() const { return size_; }
    
    /**
     * @brief Check if tree is empty
     */
    bool empty() const { return root_ == nullptr; }
    
    /**
     * @brief Clear the tree
     */
    void clear();
    
    /**
     * @brief Get statistics about the tree
     */
    struct Stats {
        size_t nodeCount;
        size_t maxDepth;
        double averageBranching;
        size_t totalStrings;
    };
    Stats getStats() const;

private:
    std::unique_ptr<BKNode> root_;
    std::unique_ptr<IDistanceMetric> metric_;
    size_t size_ = 0;
    
    void addToNode(BKNode* node, const std::string& value);
    void searchNode(const BKNode* node, const std::string& query, 
                   size_t maxDistance, 
                   std::vector<std::pair<std::string, size_t>>& results) const;
    void collectStats(const BKNode* node, size_t depth, Stats& stats) const;
};

/**
 * @brief Trigram-based string similarity
 */
class TrigramIndex {
public:
    /**
     * @brief Add a string to the trigram index
     * @param id Unique identifier for the string
     * @param value The string to index
     */
    void add(const std::string& id, const std::string& value);
    
    /**
     * @brief Search for similar strings using trigram similarity
     * @param query Query string
     * @param threshold Minimum similarity score (0.0 to 1.0)
     * @return Vector of matching IDs with similarity scores
     */
    std::vector<std::pair<std::string, float>> search(
        const std::string& query, 
        float threshold = 0.7f) const;
    
    /**
     * @brief Clear the index
     */
    void clear();
    
    /**
     * @brief Get the number of indexed strings
     */
    size_t size() const { return stringToTrigrams_.size(); }

private:
    // Map from trigram to set of string IDs containing it
    std::unordered_map<std::string, std::vector<std::string>> trigramToStrings_;
    
    // Map from string ID to its trigrams
    std::unordered_map<std::string, std::vector<std::string>> stringToTrigrams_;
    
    // Map from string ID to original value
    std::unordered_map<std::string, std::string> idToString_;
    
    // Extract trigrams from a string
    std::vector<std::string> extractTrigrams(const std::string& str) const;
    
    // Calculate Jaccard similarity between two trigram sets
    float jaccardSimilarity(const std::vector<std::string>& set1,
                           const std::vector<std::string>& set2) const;
};

/**
 * @brief Hybrid fuzzy search combining multiple techniques
 */
class HybridFuzzySearch {
public:
    HybridFuzzySearch();
    
    /**
     * @brief Add a document to the search index
     * @param id Document identifier
     * @param title Document title
     * @param keywords Keywords or tags
     */
    void addDocument(const std::string& id, 
                    const std::string& title,
                    const std::vector<std::string>& keywords = {});
    
    /**
     * @brief Search for documents
     * @param query Search query
     * @param maxResults Maximum number of results
     * @param options Search options
     */
    struct SearchOptions {
        float minSimilarity;      // Minimum similarity score
        size_t maxEditDistance;       // Maximum edit distance for BK-tree
        bool useTrigramPrefilter;  // Use trigram index for pre-filtering
        bool useBKTree;            // Use BK-tree for edit distance
        bool usePhonetic;         // Use phonetic matching (future)
        
        SearchOptions() : 
            minSimilarity(0.7f),
            maxEditDistance(3),
            useTrigramPrefilter(true),
            useBKTree(true),
            usePhonetic(false) {}
    };
    
    struct SearchResult {
        std::string id;
        std::string title;
        float score;
        std::string matchType; // "exact", "fuzzy", "trigram", etc.
    };
    
    std::vector<SearchResult> search(const std::string& query,
                                    size_t maxResults = 10,
                                    const SearchOptions& options = SearchOptions{}) const;
    
    /**
     * @brief Clear all indices
     */
    void clear();
    
    /**
     * @brief Get index statistics
     */
    struct IndexStats {
        size_t documentCount;
        size_t bkTreeSize;
        size_t trigramIndexSize;
        BKTree::Stats bkTreeStats;
    };
    IndexStats getStats() const;

private:
    BKTree titleTree_;                    // BK-tree for titles
    BKTree keywordTree_;                  // BK-tree for keywords
    TrigramIndex titleTrigrams_;          // Trigram index for titles
    TrigramIndex keywordTrigrams_;        // Trigram index for keywords
    
    std::unordered_map<std::string, std::string> idToTitle_;
    std::unordered_map<std::string, std::vector<std::string>> idToKeywords_;
    
    // Combine and rank results from different search methods
    std::vector<SearchResult> combineResults(
        const std::vector<std::pair<std::string, float>>& trigramResults,
        const std::vector<std::pair<std::string, size_t>>& bkResults,
        size_t maxResults) const;
};

} // namespace yams::search