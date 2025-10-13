#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <yams/search/bk_tree.h>

namespace yams::search {

// Levenshtein Distance Implementation
size_t LevenshteinDistance::distance(const std::string& s1, const std::string& s2) const {
    const size_t m = s1.length();
    const size_t n = s2.length();

    if (m == 0)
        return n;
    if (n == 0)
        return m;

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
                prevRow[j] + 1,       // deletion
                currRow[j - 1] + 1,   // insertion
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

    if (m == 0)
        return n;
    if (n == 0)
        return m;

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
                dp[i - 1][j] + 1,       // deletion
                dp[i][j - 1] + 1,       // insertion
                dp[i - 1][j - 1] + cost // substitution
            });

            // Transposition
            if (i > 1 && j > 1 && s1[i - 1] == s2[j - 2] && s1[i - 2] == s2[j - 1]) {
                dp[i][j] = std::min(dp[i][j], dp[i - 2][j - 2] + cost);
            }
        }
    }

    return dp[m][n];
}

// BKTree Implementation
BKTree::BKTree(std::unique_ptr<IDistanceMetric> metric) : metric_(std::move(metric)) {}

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

std::vector<std::pair<std::string, size_t>> BKTree::search(const std::string& query,
                                                           size_t maxDistance) const {
    std::vector<std::pair<std::string, size_t>> results;

    if (!root_) {
        return results;
    }

    searchNode(root_.get(), query, maxDistance, results);

    // Sort by distance
    std::ranges::sort(results, [](const auto& a, const auto& b) { return a.second < b.second; });

    return results;
}

void BKTree::searchNode(const BKNode* node, const std::string& query, size_t maxDistance,
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

std::vector<std::pair<std::string, size_t>>
BKTree::searchBest(const std::string& query, size_t maxResults, size_t maxDistance) const {
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
    std::ranges::transform(lowerValue, lowerValue.begin(), ::tolower);

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
    std::ranges::set_intersection(s1, s2, std::back_inserter(intersection));

    std::vector<std::string> unionSet;
    std::ranges::set_union(s1, s2, std::back_inserter(unionSet));

    if (unionSet.empty()) {
        return 0.0f;
    }

    return static_cast<float>(intersection.size()) / static_cast<float>(unionSet.size());
}

std::vector<std::pair<std::string, float>> TrigramIndex::search(const std::string& query,
                                                                float threshold) const {
    std::string lowerQuery = query;
    std::ranges::transform(lowerQuery, lowerQuery.begin(), ::tolower);

    auto queryTrigrams = extractTrigrams(lowerQuery);
    // std::cerr << "[DEBUG] TrigramIndex::search query='" << query << "' trigrams=" <<
    // queryTrigrams.size()
    //           << " threshold=" << threshold << std::endl;
    // std::cerr << "[DEBUG] Total trigrams in index: " << trigramToStrings_.size() << std::endl;

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

    // std::cerr << "[DEBUG] Found " << candidateCounts.size() << " candidate documents" <<
    // std::endl;

    // Calculate similarity scores
    std::vector<std::pair<std::string, float>> results;

    for (const auto& [id, count] : candidateCounts) {
        auto it = stringToTrigrams_.find(id);
        if (it != stringToTrigrams_.end()) {
            float similarity = jaccardSimilarity(queryTrigrams, it->second);

            // For short queries, also check substring matching
            auto strIt = idToString_.find(id);
            if (strIt != idToString_.end() && query.length() <= 10) {
                std::string lowerStr = strIt->second;
                std::ranges::transform(lowerStr, lowerStr.begin(), ::tolower);

                // Check if query is a substring (case-insensitive)
                if (lowerStr.find(lowerQuery) != std::string::npos) {
                    // Boost similarity for substring matches
                    similarity = std::max(similarity, 0.7f);
                } else {
                    // Check for fuzzy substring match (allowing 1-2 char difference)
                    // This is a simple approximation
                    size_t matchCount = 0;
                    for (const auto& trigram : queryTrigrams) {
                        if (lowerStr.find(trigram.substr(1, 2)) != std::string::npos) {
                            matchCount++;
                        }
                    }
                    if (matchCount >= queryTrigrams.size() * 0.6) {
                        similarity = std::max(similarity, 0.5f);
                    }
                }
            }

            // Debug: Show similarity scores
            // std::cerr << "[DEBUG] ID=" << id << " similarity=" << similarity << " (threshold=" <<
            // threshold << ")" << std::endl;

            if (similarity >= threshold) {
                results.emplace_back(id, similarity);
            }
        }
    }

    // Sort by similarity score
    std::ranges::sort(results, [](const auto& a, const auto& b) { return a.second > b.second; });

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
      keywordTree_(std::make_unique<DamerauLevenshteinDistance>()) {}

void HybridFuzzySearch::addDocument(const std::string& id, const std::string& title,
                                    const std::vector<std::string>& keywords) {
    // Store document info
    idToTitle_[id] = title;
    idToKeywords_[id] = keywords;

    // Add to BK-trees
    titleTree_.add(title);
    for (const auto& keyword : keywords) {
        keywordTree_.add(keyword);
    }

    // Add to trigram indices - index each word separately for better matching
    // Split title into words and index each word
    std::istringstream titleStream(title);
    std::string word;
    int wordIndex = 0;
    while (titleStream >> word) {
        // Clean word - remove punctuation
        word.erase(std::ranges::remove_if(
                       word, [](char c) { return !std::isalnum(c) && c != '-' && c != '_'; })
                       .begin(),
                   word.end());

        if (!word.empty() && word.length() > 2) {
            // Index each word with document ID
            titleTrigrams_.add(id + "_w" + std::to_string(wordIndex++), word);
        }
    }

    // Also index the full title for exact/near-exact matches
    titleTrigrams_.add(id, title);

    // Index keywords individually
    for (size_t i = 0; i < keywords.size(); ++i) {
        keywordTrigrams_.add(id + "_kw" + std::to_string(i), keywords[i]);
    }
}

std::vector<HybridFuzzySearch::SearchResult>
HybridFuzzySearch::search(const std::string& query, size_t maxResults,
                          const SearchOptions& options) const {
    std::unordered_map<std::string, SearchResult> resultMap;

    // Debug: Log search parameters (disabled)
    // std::cerr << "[DEBUG] Fuzzy search for: '" << query << "' with minSimilarity="
    //           << options.minSimilarity << ", maxEditDistance=" << options.maxEditDistance <<
    //           std::endl;
    // std::cerr << "[DEBUG] Index has " << idToTitle_.size() << " documents" << std::endl;

    // Split query into words for multi-word handling (industry standard approach)
    std::vector<std::string> queryWords;
    std::istringstream queryStream(query);
    std::string word;
    while (queryStream >> word) {
        // Clean word - remove punctuation
        std::erase_if(word, [](char c) { return !std::isalnum(c) && c != '-' && c != '_'; });

        // Convert to lowercase
        std::ranges::transform(word, word.begin(), ::tolower);

        if (!word.empty() && word.length() > 2) {
            queryWords.push_back(word);
        }
    }

    // If multi-word query, handle differently
    if (queryWords.size() > 1) {
        // std::cerr << "[DEBUG] Multi-word query detected with " << queryWords.size() << " words"
        // << std::endl;

        // Track which documents match which words
        std::unordered_map<std::string, std::set<std::string>> docWordMatches;
        std::unordered_map<std::string, float> docMaxScores;

        // Search for each word independently
        for (const auto& queryWord : queryWords) {
            // std::cerr << "[DEBUG] Searching for word: '" << queryWord << "'" << std::endl;

            // Use lower similarity threshold for individual words
            float wordSimilarity = options.minSimilarity * 0.7f;

            // Search using trigrams
            if (options.useTrigramPrefilter) {
                auto trigramResults = titleTrigrams_.search(queryWord, wordSimilarity);
                auto keywordResults = keywordTrigrams_.search(queryWord, wordSimilarity);

                // Process results
                for (const auto& [id, score] : trigramResults) {
                    // Extract base document ID
                    std::string baseId = id;
                    size_t pos = id.find("_");
                    if (pos != std::string::npos) {
                        baseId = id.substr(0, pos);
                    }

                    docWordMatches[baseId].insert(queryWord);
                    docMaxScores[baseId] = std::max(docMaxScores[baseId], score);
                }

                for (const auto& [id, score] : keywordResults) {
                    // Extract base document ID
                    std::string baseId = id;
                    size_t pos = id.find("_");
                    if (pos != std::string::npos) {
                        baseId = id.substr(0, pos);
                    }

                    docWordMatches[baseId].insert(queryWord);
                    docMaxScores[baseId] = std::max(docMaxScores[baseId], score * 0.9f);
                }
            }

            // BK-tree search for exact/close matches
            if (options.useBKTree) {
                // Limit edit distance to 2 for performance (industry standard)
                size_t maxDist = std::min(options.maxEditDistance, size_t(2));
                auto bkResults = titleTree_.search(queryWord, maxDist);

                for (const auto& [title, distance] : bkResults) {
                    // Find documents with this word in title
                    for (const auto& [id, docTitle] : idToTitle_) {
                        std::string lowerTitle = docTitle;
                        std::transform(lowerTitle.begin(), lowerTitle.end(), lowerTitle.begin(),
                                       ::tolower);

                        if (lowerTitle.find(queryWord) != std::string::npos ||
                            distance <= 2) { // Allow up to 2 edits
                            float score = 1.0f - (float(distance) / float(maxDist + 1));
                            docWordMatches[id].insert(queryWord);
                            docMaxScores[id] = std::max(docMaxScores[id], score);
                        }
                    }
                }
            }
        }

        // Calculate final scores based on matched words (industry standard)
        float minShouldMatch = 0.5f; // At least 50% of words should match
        int requiredMatches = std::max(1, (int)std::ceil(queryWords.size() * minShouldMatch));

        for (const auto& [docId, matchedWords] : docWordMatches) {
            if ((int)matchedWords.size() >= requiredMatches) {
                SearchResult result;
                result.id = docId;

                // Get document title
                auto titleIt = idToTitle_.find(docId);
                if (titleIt != idToTitle_.end()) {
                    result.title = titleIt->second;
                } else {
                    result.title = "Document " + docId;
                }

                // Score based on proportion of words matched and individual scores
                float matchRatio = (float)matchedWords.size() / (float)queryWords.size();
                float avgScore = docMaxScores[docId];
                result.score = matchRatio * avgScore;

                // Boost if all words matched
                if (matchedWords.size() == queryWords.size()) {
                    result.score = std::min(1.0f, result.score * 1.2f);
                }

                result.matchType = "multi-word";
                resultMap[docId] = result;

                // std::cerr << "[DEBUG] Doc " << docId << " matched " << matchedWords.size()
                //           << "/" << queryWords.size() << " words, score=" << result.score <<
                //           std::endl;
            }
        }

    } else {
        // Single word query - use original logic

        // Trigram pre-filtering
        if (options.useTrigramPrefilter) {
            // Search in both title and keyword trigrams
            auto trigramResults = titleTrigrams_.search(query, options.minSimilarity);
            auto keywordTrigramResults = keywordTrigrams_.search(query, options.minSimilarity);

            // Debug: Log trigram search results (disabled)
            // std::cerr << "[DEBUG] Title trigram results: " << trigramResults.size() << std::endl;
            // std::cerr << "[DEBUG] Keyword trigram results: " << keywordTrigramResults.size() <<
            // std::endl;

            // Combine results from both searches
            for (const auto& [id, score] : keywordTrigramResults) {
                trigramResults.push_back(
                    {id, score * 0.9f}); // Slightly lower weight for keyword matches
            }

            for (const auto& [id, score] : trigramResults) {
                if (resultMap.find(id) == resultMap.end()) {
                    SearchResult result;
                    result.id = id;

                    // Handle regular IDs, content IDs, word IDs, and keyword IDs
                    auto titleIt = idToTitle_.find(id);
                    if (titleIt != idToTitle_.end()) {
                        result.title = titleIt->second;
                    } else {
                        // Extract base document ID from special IDs
                        std::string baseId = id;
                        size_t contentPos = id.find("_content");
                        size_t wordPos = id.find("_w");
                        size_t kwPos = id.find("_kw");

                        if (contentPos != std::string::npos) {
                            baseId = id.substr(0, contentPos);
                        } else if (wordPos != std::string::npos) {
                            baseId = id.substr(0, wordPos);
                        } else if (kwPos != std::string::npos) {
                            baseId = id.substr(0, kwPos);
                        }

                        auto baseTitleIt = idToTitle_.find(baseId);
                        if (baseTitleIt != idToTitle_.end()) {
                            result.title = baseTitleIt->second;
                        } else {
                            result.title = "Document " + baseId;
                        }

                        // Normalize ID to base document ID for result aggregation
                        result.id = baseId;
                    }

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
    } // End of else block for single-word query

    // Convert map to vector and sort
    std::vector<SearchResult> results;
    for (const auto& [_, result] : resultMap) {
        results.push_back(result);
    }

    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.score > b.score; });

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