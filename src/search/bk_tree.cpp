#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
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

void BKTree::addBatchParallel(const std::vector<std::string>& values, size_t parallelThreshold) {
    if (values.size() < parallelThreshold) {
        addBatch(values);
        return;
    }

    size_t numThreads = std::thread::hardware_concurrency();
    if (numThreads == 0)
        numThreads = 4;
    numThreads = std::min(numThreads, values.size());

    size_t chunkSize = values.size() / numThreads;

    std::vector<std::future<void>> futures;
    futures.reserve(numThreads);

    std::vector<std::vector<std::string>> chunkValues(numThreads);

    for (size_t i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? values.size() : start + chunkSize;
        chunkValues[i].reserve(end - start);
        for (size_t j = start; j < end; ++j) {
            chunkValues[i].push_back(values[j]);
        }
    }

    for (size_t i = 0; i < numThreads; ++i) {
        futures.push_back(std::async(std::launch::async, [this, &chunkValues, i, numThreads]() {
            if (chunkValues[i].empty())
                return;

            std::unique_ptr<BKNode> subtreeRoot;
            size_t subtreeSize = 0;

            for (const auto& value : chunkValues[i]) {
                if (!subtreeRoot) {
                    subtreeRoot = std::make_unique<BKNode>(value);
                } else {
                    size_t dist = metric_->distance(subtreeRoot->value, value);
                    if (dist != 0) {
                        std::function<void(BKNode*, const std::string&)> insertNode =
                            [&](BKNode* node, const std::string& val) {
                                size_t d = metric_->distance(node->value, val);
                                if (d == 0)
                                    return;
                                auto itr = node->children.find(d);
                                if (itr == node->children.end()) {
                                    node->children[d] = std::make_unique<BKNode>(val);
                                } else {
                                    insertNode(itr->second.get(), val);
                                }
                            };
                        auto it = subtreeRoot->children.find(dist);
                        if (it == subtreeRoot->children.end()) {
                            subtreeRoot->children[dist] = std::make_unique<BKNode>(value);
                        } else {
                            insertNode(it->second.get(), value);
                        }
                    }
                }
                subtreeSize++;
            }

            chunkValues[i].clear();
            chunkValues[i].shrink_to_fit();

            if (subtreeRoot) {
                std::lock_guard<std::mutex> lock(mutex_);
                if (!root_) {
                    root_ = std::move(subtreeRoot);
                    size_ += subtreeSize;
                } else {
                    std::function<void(BKNode*, std::unique_ptr<BKNode>)> mergeSubtree =
                        [&](BKNode* mainNode, std::unique_ptr<BKNode> subNode) {
                            if (!subNode)
                                return;
                            size_t dist = metric_->distance(mainNode->value, subNode->value);
                            if (dist == 0)
                                return;
                            auto it = mainNode->children.find(dist);
                            if (it == mainNode->children.end()) {
                                mainNode->children[dist] = std::move(subNode);
                            } else {
                                mergeSubtree(it->second.get(), std::move(subNode));
                            }
                        };
                    mergeSubtree(root_.get(), std::move(subtreeRoot));
                    size_ += subtreeSize;
                }
            }
        }));
    }

    for (auto& fut : futures) {
        fut.get();
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

std::vector<std::pair<std::string, size_t>>
BKTree::searchBest(const std::string& query, size_t maxResults, size_t maxDistance) const {
    auto results = search(query, maxDistance);

    if (results.size() > maxResults) {
        results.resize(maxResults);
    }

    return results;
}

std::vector<std::pair<std::string, size_t>>
BKTree::searchParallel(const std::string& query, size_t maxDistance,
                       size_t parallelismThreshold) const {
    std::vector<std::pair<std::string, size_t>> results;

    if (!root_) {
        return results;
    }

    struct SearchTask {
        const BKNode* node;
        size_t minDist;
        size_t maxDist;
    };

    std::deque<SearchTask> taskQueue;
    std::mutex resultsMutex;

    size_t rootDist = metric_->distance(root_->value, query);
    if (rootDist <= maxDistance) {
        results.emplace_back(root_->value, rootDist);
    }

    size_t rootMinDist = (rootDist > maxDistance) ? rootDist - maxDistance : 0;
    size_t rootMaxDist = rootDist + maxDistance;

    for (const auto& [childDist, childNode] : root_->children) {
        if (childDist >= rootMinDist && childDist <= rootMaxDist) {
            taskQueue.push_back({childNode.get(), rootMinDist, rootMaxDist});
        }
    }

    while (!taskQueue.empty()) {
        size_t batchSize = std::min(taskQueue.size(), parallelismThreshold);

        if (batchSize == 1) {
            const auto& task = taskQueue.front();
            size_t dist = metric_->distance(task.node->value, query);

            if (dist <= maxDistance) {
                std::lock_guard<std::mutex> lock(resultsMutex);
                results.emplace_back(task.node->value, dist);
            }

            size_t minDist = (dist > maxDistance) ? dist - maxDistance : 0;
            size_t maxDist = dist + maxDistance;

            for (const auto& [childDist, childNode] : task.node->children) {
                if (childDist >= minDist && childDist <= maxDist) {
                    taskQueue.push_back({childNode.get(), minDist, maxDist});
                }
            }
            taskQueue.pop_front();
        } else {
            std::vector<std::future<void>> futures;

            for (size_t i = 0; i < batchSize && !taskQueue.empty(); ++i) {
                SearchTask task = taskQueue.front();
                taskQueue.pop_front();

                futures.push_back(
                    std::async(std::launch::async, [this, task, &results, &resultsMutex, &taskQueue,
                                                    query, maxDistance]() {
                        size_t dist = metric_->distance(task.node->value, query);

                        if (dist <= maxDistance) {
                            std::lock_guard<std::mutex> lock(resultsMutex);
                            results.emplace_back(task.node->value, dist);
                        }

                        size_t minDist = (dist > maxDistance) ? dist - maxDistance : 0;
                        size_t maxDist = dist + maxDistance;

                        std::vector<SearchTask> childTasks;
                        for (const auto& [childDist, childNode] : task.node->children) {
                            if (childDist >= minDist && childDist <= maxDist) {
                                childTasks.push_back({childNode.get(), minDist, maxDist});
                            }
                        }

                        std::lock_guard<std::mutex> lock(resultsMutex);
                        for (auto& ct : childTasks) {
                            taskQueue.push_back(std::move(ct));
                        }
                    }));
            }

            for (auto& fut : futures) {
                fut.get();
            }
        }
    }

    std::ranges::sort(results, [](const auto& a, const auto& b) { return a.second < b.second; });

    return results;
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
                    for (const auto& [id, docTitle] : idToTitle_) {
                        std::string lowerTitle = docTitle;
                        std::transform(lowerTitle.begin(), lowerTitle.end(), lowerTitle.begin(),
                                       ::tolower);

                        if (lowerTitle.find(queryWord) != std::string::npos || distance <= 2) {
                            float score = 1.0f - (float(distance) / float(maxDist + 1));
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
        if (options.useTrigramPrefilter) {
            auto trigramResults = titleTrigrams_.search(query, options.minSimilarity);
            auto keywordTrigramResults = keywordTrigrams_.search(query, options.minSimilarity);
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
            auto bkResults = titleTree_.search(query, options.maxEditDistance);

            for (const auto& [title, distance] : bkResults) {
                for (const auto& [id, docTitle] : idToTitle_) {
                    if (docTitle == title) {
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
