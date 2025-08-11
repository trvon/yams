#include <yams/search/bk_tree.h>
#include <algorithm>
#include <queue>
#include <set>
#include <spdlog/spdlog.h>

namespace yams::search {

// Levenshtein Distance Implementation
size_t LevenshteinDistance::distance(const std::string& s1, const std::string& s2) const {
    const size_t m = s1.length();
    const size_t n = s2.length();
    
    if (m == 0) return n;
    if (n == 0) return m;
    
    // Use two rows instead of full matrix for space efficiency
    std::vector<size_t> prevRow(n + 1);
    std::vector<size_t> currRow(n + 1);
    
    // Initialize first row
    for (size_t j = 0; j <= n; ++j) {
        prevRow[j] = j;
    }
    
    for (size_t i = 1; i <= m; ++i) {
        currRow[0] = i;
        
        for (size_t j = 1; j <= n; ++j) {
            size_t cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
            
            currRow[j] = std::min({
                prevRow[j] + 1,      // deletion
                currRow[j - 1] + 1,  // insertion
                prevRow[j - 1] + cost // substitution
            });
        }
        
        std::swap(prevRow, currRow);
    }
    
    return prevRow[n];
}

// Damerau-Levenshtein Distance Implementation
size_t DamerauLevenshteinDistance::distance(const std::string& s1, const std::string& s2) const {
    const size_t m = s1.length();
    const size_t n = s2.length();
    
    if (m == 0) return n;
    if (n == 0) return m;
    
    // Create a 2D matrix for dynamic programming
    std::vector<std::vector<size_t>> dp(m + 1, std::vector<size_t>(n + 1));
    
    // Initialize first column and row
    for (size_t i = 0; i <= m; ++i) {
        dp[i][0] = i;
    }
    for (size_t j = 0; j <= n; ++j) {
        dp[0][j] = j;
    }
    
    // Fill the matrix
    for (size_t i = 1; i <= m; ++i) {
        for (size_t j = 1; j <= n; ++j) {
            size_t cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
            
            dp[i][j] = std::min({
                dp[i - 1][j] + 1,      // deletion
                dp[i][j - 1] + 1,      // insertion
                dp[i - 1][j - 1] + cost // substitution
            });
            
            // Transposition
            if (i > 1 && j > 1 && 
                s1[i - 1] == s2[j - 2] && 
                s1[i - 2] == s2[j - 1]) {
                dp[i][j] = std::min(dp[i][j], dp[i - 2][j - 2] + cost);
            }
        }
    }
    
    return dp[m][n];
}

// BKTree Implementation
BKTree::BKTree(std::unique_ptr<IDistanceMetric> metric) 
    : metric_(std::move(metric)) {
}

void BKTree::add(const std::string& value) {
    if (!root_) {
        root_ = std::make_unique<BKNode>(value);
        size_++;
        return;
    }
    
    addToNode(root_.get(), value);
    size_++;
}

void BKTree::addToNode(BKNode* node, const std::string& value) {
    size_t dist = metric_->distance(node->value, value);
    
    if (dist == 0) {
        // String already exists
        return;
    }
    
    auto it = node->children.find(dist);
    if (it == node->children.end()) {
        node->children[dist] = std::make_unique<BKNode>(value);
    } else {
        addToNode(it->second.get(), value);
    }
}

void BKTree::addBatch(const std::vector<std::string>& values) {
    for (const auto& value : values) {
        add(value);
    }
}

std::vector<std::pair<std::string, size_t>> BKTree::search(
    const std::string& query, 
    size_t maxDistance) const {
    
    std::vector<std::pair<std::string, size_t>> results;
    
    if (!root_) {
        return results;
    }
    
    searchNode(root_.get(), query, maxDistance, results);
    
    // Sort by distance
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) {
                  return a.second < b.second;
              });
    
    return results;
}

void BKTree::searchNode(const BKNode* node, 
                        const std::string& query,
                        size_t maxDistance,
                        std::vector<std::pair<std::string, size_t>>& results) const {
    
    size_t dist = metric_->distance(node->value, query);
    
    if (dist <= maxDistance) {
        results.emplace_back(node->value, dist);
    }
    
    // Use triangle inequality to prune search space
    size_t minDist = (dist > maxDistance) ? dist - maxDistance : 0;
    size_t maxDist = dist + maxDistance;
    
    for (const auto& [childDist, childNode] : node->children) {
        if (childDist >= minDist && childDist <= maxDist) {
            searchNode(childNode.get(), query, maxDistance, results);
        }
    }
}

std::vector<std::pair<std::string, size_t>> BKTree::searchBest(
    const std::string& query,
    size_t maxResults,
    size_t maxDistance) const {
    
    auto results = search(query, maxDistance);
    
    if (results.size() > maxResults) {
        results.resize(maxResults);
    }
    
    return results;
}

void BKTree::clear() {
    root_.reset();
    size_ = 0;
}

BKTree::Stats BKTree::getStats() const {
    Stats stats{};
    stats.totalStrings = size_;
    
    if (root_) {
        collectStats(root_.get(), 0, stats);
    }
    
    return stats;
}

void BKTree::collectStats(const BKNode* node, size_t depth, Stats& stats) const {
    stats.nodeCount++;
    stats.maxDepth = std::max(stats.maxDepth, depth);
    stats.averageBranching += node->children.size();
    
    for (const auto& [_, child] : node->children) {
        collectStats(child.get(), depth + 1, stats);
    }
}

// TrigramIndex Implementation
std::vector<std::string> TrigramIndex::extractTrigrams(const std::string& str) const {
    std::vector<std::string> trigrams;
    
    if (str.length() < 3) {
        // For short strings, use the string itself
        trigrams.push_back(str);
        return trigrams;
    }
    
    // Add padding for better matching at boundaries
    std::string padded = "  " + str + "  ";
    
    for (size_t i = 0; i <= padded.length() - 3; ++i) {
        trigrams.push_back(padded.substr(i, 3));
    }
    
    return trigrams;
}

void TrigramIndex::add(const std::string& id, const std::string& value) {
    // Convert to lowercase for case-insensitive matching
    std::string lowerValue = value;
    std::transform(lowerValue.begin(), lowerValue.end(), lowerValue.begin(), ::tolower);
    
    auto trigrams = extractTrigrams(lowerValue);
    
    stringToTrigrams_[id] = trigrams;
    idToString_[id] = value;
    
    for (const auto& trigram : trigrams) {
        trigramToStrings_[trigram].push_back(id);
    }
}

float TrigramIndex::jaccardSimilarity(const std::vector<std::string>& set1,
                                      const std::vector<std::string>& set2) const {
    std::set<std::string> s1(set1.begin(), set1.end());
    std::set<std::string> s2(set2.begin(), set2.end());
    
    std::vector<std::string> intersection;
    std::set_intersection(s1.begin(), s1.end(),
                          s2.begin(), s2.end(),
                          std::back_inserter(intersection));
    
    std::vector<std::string> unionSet;
    std::set_union(s1.begin(), s1.end(),
                  s2.begin(), s2.end(),
                  std::back_inserter(unionSet));
    
    if (unionSet.empty()) {
        return 0.0f;
    }
    
    return static_cast<float>(intersection.size()) / static_cast<float>(unionSet.size());
}

std::vector<std::pair<std::string, float>> TrigramIndex::search(
    const std::string& query, 
    float threshold) const {
    
    std::string lowerQuery = query;
    std::transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);
    
    auto queryTrigrams = extractTrigrams(lowerQuery);
    
    // Collect candidate strings that share trigrams with query
    std::unordered_map<std::string, int> candidateCounts;
    for (const auto& trigram : queryTrigrams) {
        auto it = trigramToStrings_.find(trigram);
        if (it != trigramToStrings_.end()) {
            for (const auto& id : it->second) {
                candidateCounts[id]++;
            }
        }
    }
    
    // Calculate similarity scores
    std::vector<std::pair<std::string, float>> results;
    
    for (const auto& [id, count] : candidateCounts) {
        auto it = stringToTrigrams_.find(id);
        if (it != stringToTrigrams_.end()) {
            float similarity = jaccardSimilarity(queryTrigrams, it->second);
            
            if (similarity >= threshold) {
                results.emplace_back(id, similarity);
            }
        }
    }
    
    // Sort by similarity score
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) {
                  return a.second > b.second;
              });
    
    return results;
}

void TrigramIndex::clear() {
    trigramToStrings_.clear();
    stringToTrigrams_.clear();
    idToString_.clear();
}

// HybridFuzzySearch Implementation
HybridFuzzySearch::HybridFuzzySearch() 
    : titleTree_(std::make_unique<DamerauLevenshteinDistance>()),
      keywordTree_(std::make_unique<DamerauLevenshteinDistance>()) {
}

void HybridFuzzySearch::addDocument(const std::string& id,
                                   const std::string& title,
                                   const std::vector<std::string>& keywords) {
    // Store document info
    idToTitle_[id] = title;
    idToKeywords_[id] = keywords;
    
    // Add to BK-trees
    titleTree_.add(title);
    for (const auto& keyword : keywords) {
        keywordTree_.add(keyword);
    }
    
    // Add to trigram indices
    titleTrigrams_.add(id, title);
    for (const auto& keyword : keywords) {
        keywordTrigrams_.add(id + "_kw_" + keyword, keyword);
    }
}

std::vector<HybridFuzzySearch::SearchResult> HybridFuzzySearch::search(
    const std::string& query,
    size_t maxResults,
    const SearchOptions& options) const {
    
    std::unordered_map<std::string, SearchResult> resultMap;
    
    // Trigram pre-filtering
    if (options.useTrigramPrefilter) {
        auto trigramResults = titleTrigrams_.search(query, options.minSimilarity);
        
        for (const auto& [id, score] : trigramResults) {
            if (resultMap.find(id) == resultMap.end()) {
                SearchResult result;
                result.id = id;
                result.title = idToTitle_.at(id);
                result.score = score;
                result.matchType = "trigram";
                resultMap[id] = result;
            } else {
                resultMap[id].score = std::max(resultMap[id].score, score);
            }
        }
    }
    
    // BK-tree search
    if (options.useBKTree) {
        auto bkResults = titleTree_.search(query, options.maxEditDistance);
        
        for (const auto& [title, distance] : bkResults) {
            // Find document ID by title
            for (const auto& [id, docTitle] : idToTitle_) {
                if (docTitle == title) {
                    // Convert edit distance to similarity score
                    float maxLen = std::max(query.length(), title.length());
                    float score = 1.0f - (static_cast<float>(distance) / maxLen);
                    
                    if (score >= options.minSimilarity) {
                        if (resultMap.find(id) == resultMap.end()) {
                            SearchResult result;
                            result.id = id;
                            result.title = title;
                            result.score = score;
                            result.matchType = "fuzzy";
                            resultMap[id] = result;
                        } else {
                            resultMap[id].score = std::max(resultMap[id].score, score);
                            if (distance == 0) {
                                resultMap[id].matchType = "exact";
                            }
                        }
                    }
                    break;
                }
            }
        }
    }
    
    // Convert map to vector and sort
    std::vector<SearchResult> results;
    for (const auto& [_, result] : resultMap) {
        results.push_back(result);
    }
    
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) {
                  return a.score > b.score;
              });
    
    if (results.size() > maxResults) {
        results.resize(maxResults);
    }
    
    return results;
}

void HybridFuzzySearch::clear() {
    titleTree_.clear();
    keywordTree_.clear();
    titleTrigrams_.clear();
    keywordTrigrams_.clear();
    idToTitle_.clear();
    idToKeywords_.clear();
}

HybridFuzzySearch::IndexStats HybridFuzzySearch::getStats() const {
    IndexStats stats;
    stats.documentCount = idToTitle_.size();
    stats.bkTreeSize = titleTree_.size();
    stats.trigramIndexSize = titleTrigrams_.size();
    stats.bkTreeStats = titleTree_.getStats();
    return stats;
}

} // namespace yams::search