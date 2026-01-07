#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <future>
#include <iostream>
#include <queue>
#include <set>
#include <sstream>
#include <thread>
#include <yams/search/bk_tree.h>

namespace yams::search {

size_t LevenshteinDistance::distance(const std::string& s1, const std::string& s2) const {
    const size_t m = s1.length();
    const size_t n = s2.length();

    if (m == 0)
        return n;
    if (n == 0)
        return m;

    std::vector<size_t> prevRow(n + 1);
    std::vector<size_t> currRow(n + 1);

    for (size_t j = 0; j <= n; ++j) {
        prevRow[j] = j;
    }

    for (size_t i = 1; i <= m; ++i) {
        currRow[0] = i;

        for (size_t j = 1; j <= n; ++j) {
            size_t cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;

            currRow[j] = std::min({prevRow[j] + 1, currRow[j - 1] + 1, prevRow[j - 1] + cost});
        }

        std::swap(prevRow, currRow);
    }

    return prevRow[n];
}

size_t DamerauLevenshteinDistance::distance(const std::string& s1, const std::string& s2) const {
    const size_t m = s1.length();
    const size_t n = s2.length();

    if (m == 0)
        return n;
    if (n == 0)
        return m;

    std::vector<std::vector<size_t>> dp(m + 1, std::vector<size_t>(n + 1));

    for (size_t i = 0; i <= m; ++i) {
        dp[i][0] = i;
    }
    for (size_t j = 0; j <= n; ++j) {
        dp[0][j] = j;
    }

    for (size_t i = 1; i <= m; ++i) {
        for (size_t j = 1; j <= n; ++j) {
            size_t cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;

            dp[i][j] = std::min({dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost});

            if (i > 1 && j > 1 && s1[i - 1] == s2[j - 2] && s1[i - 2] == s2[j - 1]) {
                dp[i][j] = std::min(dp[i][j], dp[i - 2][j - 2] + cost);
            }
        }
    }

    return dp[m][n];
}

BKTree::BKTree(std::unique_ptr<IDistanceMetric> metric) : metric_(std::move(metric)) {}

void BKTree::add(const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
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

    std::ranges::sort(results, [](const auto& a, const auto& b) { return a.second < b.second; });

    return results;
}

void BKTree::searchNode(const BKNode* node, const std::string& query, size_t maxDistance,
                        std::vector<std::pair<std::string, size_t>>& results) const {
    size_t dist = metric_->distance(node->value, query);

    if (dist <= maxDistance) {
        results.emplace_back(node->value, dist);
    }

    size_t minDist = (dist > maxDistance) ? dist - maxDistance : 0;
    size_t maxDist = dist + maxDistance;

    for (const auto& [childDist, childNode] : node->children) {
        if (childDist >= minDist && childDist <= maxDist) {
            searchNode(childNode.get(), query, maxDistance, results);
        }
    }
}

void BKTree::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
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

std::vector<std::string> TrigramIndex::extractTrigrams(const std::string& str) const {
    std::vector<std::string> trigrams;

    if (str.length() < 3) {
        trigrams.push_back(str);
        return trigrams;
    }

    std::string padded = "  " + str + "  ";

    for (size_t i = 0; i <= padded.length() - 3; ++i) {
        trigrams.push_back(padded.substr(i, 3));
    }

    return trigrams;
}

void TrigramIndex::add(const std::string& id, const std::string& value) {
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

    std::unordered_map<std::string, int> candidateCounts;
    for (const auto& trigram : queryTrigrams) {
        auto it = trigramToStrings_.find(trigram);
        if (it != trigramToStrings_.end()) {
            for (const auto& id : it->second) {
                candidateCounts[id]++;
            }
        }
    }

    std::vector<std::pair<std::string, float>> results;

    for (const auto& [id, count] : candidateCounts) {
        auto it = stringToTrigrams_.find(id);
        if (it != stringToTrigrams_.end()) {
            float similarity = jaccardSimilarity(queryTrigrams, it->second);

            auto strIt = idToString_.find(id);
            if (strIt != idToString_.end() && query.length() <= 10) {
                std::string lowerStr = strIt->second;
                std::ranges::transform(lowerStr, lowerStr.begin(), ::tolower);

                if (lowerStr.find(lowerQuery) != std::string::npos) {
                    similarity = std::max(similarity, 0.7f);
                } else {
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

            if (similarity >= threshold) {
                results.emplace_back(id, similarity);
            } else if (similarity >= threshold * 0.5f) {
                spdlog::debug("TrigramIndex near-miss: ID={} similarity={:.2f} (threshold={:.2f})",
                              id, similarity, threshold);
            }
        }
    }

    std::ranges::sort(results, [](const auto& a, const auto& b) { return a.second > b.second; });

    return results;
}

void TrigramIndex::clear() {
    trigramToStrings_.clear();
    stringToTrigrams_.clear();
    idToString_.clear();
}

HybridFuzzySearch::HybridFuzzySearch()
    : titleTree_(std::make_unique<DamerauLevenshteinDistance>()),
      keywordTree_(std::make_unique<DamerauLevenshteinDistance>()) {}

void HybridFuzzySearch::addDocument(const std::string& id, const std::string& title,
                                    const std::vector<std::string>& keywords) {
    idToTitle_[id] = title;
    titleToIds_[title].push_back(id); // Reverse lookup for O(1) title->ids
    idToKeywords_[id] = keywords;

    titleTree_.add(title);
    for (const auto& keyword : keywords) {
        keywordTree_.add(keyword);
    }

    std::istringstream titleStream(title);
    std::string word;
    int wordIndex = 0;
    while (titleStream >> word) {
        word.erase(std::ranges::remove_if(
                       word, [](char c) { return !std::isalnum(c) && c != '-' && c != '_'; })
                       .begin(),
                   word.end());

        if (!word.empty() && word.length() > 2) {
            titleTrigrams_.add(id + "_w" + std::to_string(wordIndex++), word);
        }
    }

    titleTrigrams_.add(id, title);

    for (size_t i = 0; i < keywords.size(); ++i) {
        keywordTrigrams_.add(id + "_kw" + std::to_string(i), keywords[i]);
    }
}

std::vector<HybridFuzzySearch::SearchResult>
HybridFuzzySearch::search(const std::string& query, size_t maxResults,
                          const SearchOptions& options) const {
    std::unordered_map<std::string, SearchResult> resultMap;
    std::vector<std::string> queryWords;
    std::istringstream queryStream(query);
    std::string word;
    while (queryStream >> word) {
        std::erase_if(word, [](char c) { return !std::isalnum(c) && c != '-' && c != '_'; });

        std::ranges::transform(word, word.begin(), ::tolower);

        if (!word.empty() && word.length() > 2) {
            queryWords.push_back(word);
        }
    }

    if (queryWords.size() > 1) {
        std::unordered_map<std::string, std::set<std::string>> docWordMatches;
        std::unordered_map<std::string, float> docMaxScores;

        for (const auto& queryWord : queryWords) {
            float adjustmentFactor = 1.0f / std::sqrt(static_cast<float>(queryWords.size()));
            float wordSimilarity = options.minSimilarity * adjustmentFactor;

            wordSimilarity = std::clamp(wordSimilarity, 0.2f, 0.7f);

            if (options.useTrigramPrefilter) {
                auto trigramResults = titleTrigrams_.search(queryWord, wordSimilarity);
                auto keywordResults = keywordTrigrams_.search(queryWord, wordSimilarity);

                for (const auto& [id, score] : trigramResults) {
                    std::string baseId = id;
                    size_t pos = id.find("_");
                    if (pos != std::string::npos) {
                        baseId = id.substr(0, pos);
                    }

                    docWordMatches[baseId].insert(queryWord);
                    docMaxScores[baseId] = std::max(docMaxScores[baseId], score);
                }

                for (const auto& [id, score] : keywordResults) {
                    std::string baseId = id;
                    size_t pos = id.find("_");
                    if (pos != std::string::npos) {
                        baseId = id.substr(0, pos);
                    }

                    docWordMatches[baseId].insert(queryWord);
                    docMaxScores[baseId] = std::max(docMaxScores[baseId], score * 0.9f);
                }
            }

            if (options.useBKTree) {
                size_t maxDist = std::min(options.maxEditDistance, size_t(2));
                auto bkResults = titleTree_.search(queryWord, maxDist);

                for (const auto& [title, distance] : bkResults) {
                    // O(1) lookup using reverse map instead of O(n) scan
                    auto it = titleToIds_.find(title);
                    if (it != titleToIds_.end()) {
                        float score = 1.0f - (float(distance) / float(maxDist + 1));
                        for (const auto& id : it->second) {
                            docWordMatches[id].insert(queryWord);
                            docMaxScores[id] = std::max(docMaxScores[id], score);
                        }
                    }
                }
            }
        }

        float minShouldMatch = 0.5f;
        int requiredMatches = std::max(1, (int)std::ceil(queryWords.size() * minShouldMatch));

        for (const auto& [docId, matchedWords] : docWordMatches) {
            if ((int)matchedWords.size() >= requiredMatches) {
                SearchResult result;
                result.id = docId;

                auto titleIt = idToTitle_.find(docId);
                if (titleIt != idToTitle_.end()) {
                    result.title = titleIt->second;
                } else {
                    result.title = "Document " + docId;
                }

                float matchRatio = (float)matchedWords.size() / (float)queryWords.size();
                float avgScore = docMaxScores[docId];
                result.score = matchRatio * avgScore;

                if (matchedWords.size() == queryWords.size()) {
                    result.score = std::min(1.0f, result.score * 1.2f);
                }

                result.matchType = "multi-word";
                resultMap[docId] = result;
            }
        }

    } else {
        // Single-word path timing
        auto singleWordStart = std::chrono::high_resolution_clock::now();

        if (options.useTrigramPrefilter) {
            auto trigramStart = std::chrono::high_resolution_clock::now();
            auto trigramResults = titleTrigrams_.search(query, options.minSimilarity);
            auto trigramMid = std::chrono::high_resolution_clock::now();
            auto keywordTrigramResults = keywordTrigrams_.search(query, options.minSimilarity);
            auto trigramEnd = std::chrono::high_resolution_clock::now();

            auto titleMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(trigramMid - trigramStart)
                    .count();
            auto keywordMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(trigramEnd - trigramMid)
                    .count();
            spdlog::info(
                "[FUZZY_PERF] Trigram search: title={}ms ({} results), keyword={}ms ({} results)",
                titleMs, trigramResults.size(), keywordMs, keywordTrigramResults.size());

            for (const auto& [id, score] : keywordTrigramResults) {
                trigramResults.push_back({id, score * 0.9f});
            }

            for (const auto& [id, score] : trigramResults) {
                if (resultMap.find(id) == resultMap.end()) {
                    SearchResult result;
                    result.id = id;

                    auto titleIt = idToTitle_.find(id);
                    if (titleIt != idToTitle_.end()) {
                        result.title = titleIt->second;
                    } else {
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

        if (options.useBKTree) {
            auto bkStart = std::chrono::high_resolution_clock::now();
            auto bkResults = titleTree_.search(query, options.maxEditDistance);
            auto bkEnd = std::chrono::high_resolution_clock::now();
            auto bkMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(bkEnd - bkStart).count();
            spdlog::info("[FUZZY_PERF] BK-tree search: {}ms ({} results, maxDist={})", bkMs,
                         bkResults.size(), options.maxEditDistance);

            for (const auto& [title, distance] : bkResults) {
                // O(1) lookup using reverse map instead of O(n) scan
                auto it = titleToIds_.find(title);
                if (it != titleToIds_.end()) {
                    float maxLen = std::max(query.length(), title.length());
                    float score = 1.0f - (static_cast<float>(distance) / maxLen);

                    if (score >= options.minSimilarity) {
                        for (const auto& id : it->second) {
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
                    }
                }
            }
        }

        auto singleWordEnd = std::chrono::high_resolution_clock::now();
        auto totalMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(singleWordEnd - singleWordStart)
                .count();
        spdlog::info("[FUZZY_PERF] Single-word total: {}ms, resultMap size: {}", totalMs,
                     resultMap.size());
    }

    std::vector<SearchResult> results;
    for (const auto& [_, result] : resultMap) {
        results.push_back(result);
    }

    std::sort(results.begin(), results.end(), [&query](const auto& a, const auto& b) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
        bool aExact = (a.title == query);
        bool bExact = (b.title == query);
        if (aExact != bExact)
            return aExact;
        return a.title.length() < b.title.length();
    });

    if (results.size() > maxResults) {
        results.resize(maxResults);
    }

    return results;
}

std::vector<std::vector<HybridFuzzySearch::SearchResult>>
HybridFuzzySearch::searchConcurrent(const std::vector<std::string>& queries, size_t maxResults,
                                    const SearchOptions& options) const {
    std::vector<std::future<std::vector<SearchResult>>> futures;
    futures.reserve(queries.size());

    for (const auto& query : queries) {
        futures.push_back(std::async(std::launch::async, [this, &query, maxResults, &options]() {
            return search(query, maxResults, options);
        }));
    }

    std::vector<std::vector<SearchResult>> allResults;
    allResults.reserve(queries.size());

    for (auto& fut : futures) {
        allResults.push_back(fut.get());
    }

    return allResults;
}

void HybridFuzzySearch::clear() {
    titleTree_.clear();
    keywordTree_.clear();
    titleTrigrams_.clear();
    keywordTrigrams_.clear();
    idToTitle_.clear();
    titleToIds_.clear();
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
