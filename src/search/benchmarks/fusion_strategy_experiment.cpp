/**
 * @file fusion_strategy_experiment.cpp
 * @brief Experiment to compare different fusion strategies for hybrid search
 *
 * Tests four architectural approaches:
 * 1. Two-stage retrieval: FTS5 candidates → vector re-rank (and vice versa)
 * 2. Score-gated fusion: Filter low-confidence vector results before fusion
 * 3. Dominance detection: Let high-confidence component dominate
 * 4. Query-adaptive routing: Select best component based on query analysis
 *
 * Run with:
 *   YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=scifact \
 *   ./builddir/tests/benchmarks/fusion_strategy_experiment
 */

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"
#include <yams/cli/search_runner.h>
#include <yams/daemon/client/daemon_client.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using json = nlohmann::json;

// ============================================================================
// BEIR Dataset Types (copied from retrieval_quality_bench.cpp)
// ============================================================================

struct BEIRDocument {
    std::string id;
    std::string text;
    std::string title;
};

struct BEIRQuery {
    std::string id;
    std::string text;
};

struct BEIRDataset {
    std::map<std::string, BEIRDocument> documents;
    std::map<std::string, BEIRQuery> queries;
    std::multimap<std::string, std::pair<std::string, int>> qrels;
};

struct TestQuery {
    std::string query;
    std::set<std::string> relevantFiles;
    std::set<std::string> relevantDocIds;
    std::map<std::string, int> relevanceGrades;
    bool useDocIds = false;
};

// ============================================================================
// BEIR Dataset Loading (from retrieval_quality_bench.cpp)
// ============================================================================

BEIRDataset loadBEIRDataset(const std::string& datasetName) {
    BEIRDataset dataset;
    std::string basePath;

    if (const char* env = std::getenv("YAMS_BENCH_DATASET_PATH")) {
        basePath = env;
    } else if (const char* home = std::getenv("HOME")) {
        basePath = std::string(home) + "/.cache/yams/beir/" + datasetName;
    } else {
        basePath = "/tmp/yams_beir/" + datasetName;
    }

    fs::path corpusPath = fs::path(basePath) / "corpus.jsonl";
    fs::path queriesPath = fs::path(basePath) / "queries.jsonl";
    fs::path qrelsPath = fs::path(basePath) / "qrels" / "test.tsv";

    if (!fs::exists(corpusPath)) {
        spdlog::warn("BEIR corpus not found at {}", corpusPath.string());
        return dataset;
    }

    // Load corpus
    std::ifstream corpusFile(corpusPath);
    std::string line;
    while (std::getline(corpusFile, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRDocument doc;
            doc.id = j.value("_id", "");
            doc.text = j.value("text", "");
            doc.title = j.value("title", "");
            if (!doc.id.empty()) {
                dataset.documents[doc.id] = doc;
            }
        } catch (...) {
        }
    }

    // Load queries
    if (fs::exists(queriesPath)) {
        std::ifstream queriesFile(queriesPath);
        while (std::getline(queriesFile, line)) {
            if (line.empty())
                continue;
            try {
                auto j = json::parse(line);
                BEIRQuery q;
                q.id = j.value("_id", "");
                q.text = j.value("text", "");
                if (!q.id.empty()) {
                    dataset.queries[q.id] = q;
                }
            } catch (...) {
            }
        }
    }

    // Load qrels
    if (fs::exists(qrelsPath)) {
        std::ifstream qrelsFile(qrelsPath);
        std::getline(qrelsFile, line); // skip header
        while (std::getline(qrelsFile, line)) {
            if (line.empty())
                continue;
            std::istringstream iss(line);
            std::string queryId, docId;
            int score;
            if (iss >> queryId >> docId >> score) {
                dataset.qrels.insert({queryId, {docId, score}});
            }
        }
    }

    spdlog::info("Loaded BEIR dataset '{}': {} docs, {} queries, {} qrels", datasetName,
                 dataset.documents.size(), dataset.queries.size(), dataset.qrels.size());
    return dataset;
}

// ============================================================================
// BEIR Corpus Loader
// ============================================================================

struct BEIRCorpusLoader {
    BEIRDataset dataset;
    fs::path corpusDir;

    BEIRCorpusLoader(const BEIRDataset& ds, const fs::path& dir) : dataset(ds), corpusDir(dir) {}

    void writeDocumentsAsFiles() {
        fs::create_directories(corpusDir);
        for (const auto& [id, doc] : dataset.documents) {
            fs::path filePath = corpusDir / (id + ".txt");
            std::ofstream outFile(filePath);
            if (doc.title.empty()) {
                outFile << doc.text;
            } else {
                outFile << doc.title << "\n\n" << doc.text;
            }
        }
        spdlog::info("Wrote {} documents to {}", dataset.documents.size(), corpusDir.string());
    }

    std::vector<TestQuery> generateTestQueries() {
        std::vector<TestQuery> testQueries;
        for (const auto& [queryId, query] : dataset.queries) {
            TestQuery tq;
            tq.query = query.text;
            tq.useDocIds = true;

            auto range = dataset.qrels.equal_range(queryId);
            for (auto it = range.first; it != range.second; ++it) {
                const auto& [docId, score] = it->second;
                tq.relevantDocIds.insert(docId);
                tq.relevanceGrades[docId] = score;
            }

            if (!tq.relevantDocIds.empty()) {
                testQueries.push_back(tq);
            }
        }
        return testQueries;
    }
};

// ============================================================================
// Experiment Metrics
// ============================================================================

struct ExperimentMetrics {
    double mrr = 0.0;
    double recall_at_k = 0.0;
    double precision_at_k = 0.0;
    double ndcg_at_k = 0.0;
    int num_queries = 0;
    double avg_latency_ms = 0.0;

    json toJson() const {
        return {{"mrr", mrr},
                {"recall_at_k", recall_at_k},
                {"precision_at_k", precision_at_k},
                {"ndcg_at_k", ndcg_at_k},
                {"num_queries", num_queries},
                {"avg_latency_ms", avg_latency_ms}};
    }
};

// ============================================================================
// Fusion Strategies
// ============================================================================

enum class FusionStrategy {
    BASELINE_KEYWORD,
    BASELINE_HYBRID,
    TWO_STAGE_TEXT_FIRST,
    SCORE_GATED_080,
    DOMINANCE_DETECTION,
    ADAPTIVE_ROUTING,
    HYBRID_RERANK
};

const char* strategyName(FusionStrategy s) {
    switch (s) {
        case FusionStrategy::BASELINE_KEYWORD:
            return "baseline_keyword";
        case FusionStrategy::BASELINE_HYBRID:
            return "baseline_hybrid";
        case FusionStrategy::TWO_STAGE_TEXT_FIRST:
            return "two_stage_text_first";
        case FusionStrategy::SCORE_GATED_080:
            return "score_gated_0.80";
        case FusionStrategy::DOMINANCE_DETECTION:
            return "dominance_detection";
        case FusionStrategy::ADAPTIVE_ROUTING:
            return "adaptive_routing";
        case FusionStrategy::HYBRID_RERANK:
            return "hybrid_rerank";
    }
    return "unknown";
}

// ============================================================================
// NDCG computation
// ============================================================================

double computeNDCG(const std::vector<int>& grades, int k) {
    auto dcg = [](const std::vector<int>& g, int n) {
        double sum = 0.0;
        for (int i = 0; i < std::min(n, static_cast<int>(g.size())); ++i) {
            sum += (std::pow(2.0, g[i]) - 1.0) / std::log2(i + 2.0);
        }
        return sum;
    };

    double dcg_val = dcg(grades, k);
    std::vector<int> ideal = grades;
    std::sort(ideal.begin(), ideal.end(), std::greater<int>());
    double idcg_val = dcg(ideal, k);

    return idcg_val > 0 ? dcg_val / idcg_val : 0.0;
}

// ============================================================================
// Strategy Implementations
// ============================================================================

struct QueryResult {
    std::string query;
    std::vector<std::string> returned_doc_ids;
    std::vector<float> returned_scores;
    std::set<std::string> relevant_doc_ids;
    double reciprocal_rank = 0.0;
    int64_t latency_us = 0;
};

QueryResult executeSearch(yams::daemon::DaemonClient& client, const std::string& query,
                          const std::set<std::string>& relevant_ids, int k,
                          const std::string& searchType) {
    QueryResult result;
    result.query = query;
    result.relevant_doc_ids = relevant_ids;

    auto start = std::chrono::steady_clock::now();

    yams::cli::search_runner::DaemonSearchOptions opts;
    opts.query = query;
    opts.searchType = searchType;
    opts.limit = static_cast<size_t>(k);
    opts.timeout = 5s;
    opts.symbolRank = false;

    auto searchResult =
        yams::cli::run_sync(yams::cli::search_runner::daemon_search(client, opts, true), 10s);

    auto end = std::chrono::steady_clock::now();
    result.latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    if (!searchResult) {
        return result;
    }

    for (const auto& r : searchResult.value().response.results) {
        std::string docId = fs::path(r.path).stem().string();
        result.returned_doc_ids.push_back(docId);
        result.returned_scores.push_back(static_cast<float>(r.score));

        if (result.reciprocal_rank == 0.0 && relevant_ids.count(docId)) {
            result.reciprocal_rank = 1.0 / static_cast<double>(result.returned_doc_ids.size());
        }
    }

    return result;
}

/**
 * Two-stage retrieval: FTS5 first, then re-rank candidates
 */
QueryResult executeTwoStageTextFirst(yams::daemon::DaemonClient& client, const std::string& query,
                                     const std::set<std::string>& relevant_ids, int k) {
    QueryResult result;
    result.query = query;
    result.relevant_doc_ids = relevant_ids;

    auto start = std::chrono::steady_clock::now();

    // Stage 1: Get candidates from FTS5
    yams::cli::search_runner::DaemonSearchOptions opts1;
    opts1.query = query;
    opts1.searchType = "keyword";
    opts1.limit = static_cast<size_t>(k * 3);
    opts1.timeout = 5s;

    auto stage1 =
        yams::cli::run_sync(yams::cli::search_runner::daemon_search(client, opts1, true), 10s);

    if (!stage1 || stage1.value().response.results.empty()) {
        auto end = std::chrono::steady_clock::now();
        result.latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return result;
    }

    // Stage 2: Get hybrid scores for re-ranking boost
    yams::cli::search_runner::DaemonSearchOptions opts2;
    opts2.query = query;
    opts2.searchType = "hybrid";
    opts2.limit = static_cast<size_t>(k * 3);
    opts2.timeout = 5s;

    auto stage2 =
        yams::cli::run_sync(yams::cli::search_runner::daemon_search(client, opts2, true), 10s);

    auto end = std::chrono::steady_clock::now();
    result.latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    // Build hybrid score map
    std::map<std::string, float> hybridScores;
    if (stage2) {
        for (const auto& r : stage2.value().response.results) {
            std::string docId = fs::path(r.path).stem().string();
            hybridScores[docId] = static_cast<float>(r.score);
        }
    }

    // Re-rank: text rank is primary, hybrid score is secondary boost
    struct Candidate {
        std::string docId;
        float textRankScore;
        float hybridScore;
        float combined;
    };

    std::vector<Candidate> candidates;
    float maxRank = static_cast<float>(stage1.value().response.results.size());

    for (size_t i = 0; i < stage1.value().response.results.size(); ++i) {
        const auto& r = stage1.value().response.results[i];
        std::string docId = fs::path(r.path).stem().string();

        Candidate c;
        c.docId = docId;
        c.textRankScore = 1.0f - (static_cast<float>(i) / maxRank);
        c.hybridScore = hybridScores.count(docId) ? hybridScores[docId] : 0.0f;
        // Text-dominated: 80% text rank, 20% hybrid boost
        c.combined = 0.80f * c.textRankScore + 0.20f * c.hybridScore;
        candidates.push_back(c);
    }

    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.combined > b.combined; });

    for (size_t i = 0; i < std::min(static_cast<size_t>(k), candidates.size()); ++i) {
        result.returned_doc_ids.push_back(candidates[i].docId);
        result.returned_scores.push_back(candidates[i].combined);

        if (result.reciprocal_rank == 0.0 && relevant_ids.count(candidates[i].docId)) {
            result.reciprocal_rank = 1.0 / (i + 1);
        }
    }

    return result;
}

/**
 * Score-gated fusion: Only use hybrid if scores are above threshold
 */
QueryResult executeScoreGated(yams::daemon::DaemonClient& client, const std::string& query,
                              const std::set<std::string>& relevant_ids, int k, float threshold) {
    QueryResult result;
    result.query = query;
    result.relevant_doc_ids = relevant_ids;

    auto start = std::chrono::steady_clock::now();

    // Get keyword results
    auto keywordResult = executeSearch(client, query, relevant_ids, k * 2, "keyword");

    // Get hybrid results
    auto hybridResult = executeSearch(client, query, relevant_ids, k * 2, "hybrid");

    auto end = std::chrono::steady_clock::now();
    result.latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    // Combine: keyword base, boost from high-confidence hybrid
    std::map<std::string, float> scores;
    std::set<std::string> seen;

    // Add keyword results
    float maxKeywordRank = static_cast<float>(keywordResult.returned_doc_ids.size());
    for (size_t i = 0; i < keywordResult.returned_doc_ids.size(); ++i) {
        const auto& docId = keywordResult.returned_doc_ids[i];
        float rankScore = 1.0f - (static_cast<float>(i) / std::max(1.0f, maxKeywordRank));
        scores[docId] = 0.7f * rankScore;
        seen.insert(docId);
    }

    // Add high-confidence hybrid boosts
    for (size_t i = 0; i < hybridResult.returned_doc_ids.size(); ++i) {
        const auto& docId = hybridResult.returned_doc_ids[i];
        float score = hybridResult.returned_scores[i];
        if (score >= threshold) {
            scores[docId] += 0.3f * score;
            seen.insert(docId);
        }
    }

    // Sort and return top k
    std::vector<std::pair<std::string, float>> sorted(scores.begin(), scores.end());
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    for (size_t i = 0; i < std::min(static_cast<size_t>(k), sorted.size()); ++i) {
        result.returned_doc_ids.push_back(sorted[i].first);
        result.returned_scores.push_back(sorted[i].second);

        if (result.reciprocal_rank == 0.0 && relevant_ids.count(sorted[i].first)) {
            result.reciprocal_rank = 1.0 / (i + 1);
        }
    }

    return result;
}

/**
 * Dominance detection: If one component has high confidence, let it dominate
 */
QueryResult executeDominanceDetection(yams::daemon::DaemonClient& client, const std::string& query,
                                      const std::set<std::string>& relevant_ids, int k) {
    QueryResult result;
    result.query = query;
    result.relevant_doc_ids = relevant_ids;

    auto start = std::chrono::steady_clock::now();

    auto keywordResult = executeSearch(client, query, relevant_ids, k, "keyword");
    auto hybridResult = executeSearch(client, query, relevant_ids, k, "hybrid");

    auto end = std::chrono::steady_clock::now();
    result.latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    // Compute confidence: top score + gap to second
    auto computeConfidence = [](const QueryResult& qr) {
        if (qr.returned_scores.empty())
            return 0.0f;
        float top = qr.returned_scores[0];
        float second = qr.returned_scores.size() > 1 ? qr.returned_scores[1] : 0.0f;
        return top + (top - second);
    };

    float keywordConf = computeConfidence(keywordResult);
    float hybridConf = computeConfidence(hybridResult);

    // If keyword confidence is significantly higher, use keyword
    const float dominanceRatio = 1.3f;
    const QueryResult* winner = &hybridResult;
    if (keywordConf > hybridConf * dominanceRatio) {
        winner = &keywordResult;
    }

    result.returned_doc_ids = winner->returned_doc_ids;
    result.returned_scores = winner->returned_scores;

    // Recalculate reciprocal rank
    for (size_t i = 0; i < result.returned_doc_ids.size(); ++i) {
        if (relevant_ids.count(result.returned_doc_ids[i])) {
            result.reciprocal_rank = 1.0 / (i + 1);
            break;
        }
    }

    return result;
}

/**
 * Query-adaptive routing: Analyze query to select component
 */
QueryResult executeAdaptiveRouting(yams::daemon::DaemonClient& client, const std::string& query,
                                   const std::set<std::string>& relevant_ids, int k) {
    // Count words
    int wordCount = 1;
    for (char c : query) {
        if (c == ' ')
            wordCount++;
    }

    // Check for technical/scientific terms
    std::string lower = query;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    bool hasTechnical = false;
    std::vector<std::string> techTerms = {"gene", "protein",  "cell",       "dna",
                                          "rna",  "mutation", "expression", "receptor"};
    for (const auto& t : techTerms) {
        if (lower.find(t) != std::string::npos) {
            hasTechnical = true;
            break;
        }
    }

    // Routing decision: for scientific corpus, keyword tends to work better
    std::string searchType = "keyword"; // Default to keyword for SciFact
    if (wordCount >= 10 && !hasTechnical) {
        searchType = "hybrid"; // Long non-technical queries might benefit from semantic
    }

    return executeSearch(client, query, relevant_ids, k, searchType);
}

// ============================================================================
// Simple Mock Reranker (word-overlap scoring for benchmark demonstration)
// For production, use OnnxRerankerSession from the ONNX plugin with BGE reranker
// ============================================================================

struct MockRerankerResult {
    float score;
    size_t document_index;
};

/**
 * Compute word-overlap score between query and document
 * This is a simple baseline - real cross-encoder reranking would use BGE model
 */
float computeWordOverlapScore(const std::string& query, const std::string& document) {
    auto tokenize = [](const std::string& text) {
        std::set<std::string> words;
        std::string word;
        for (char c : text) {
            if (std::isalnum(static_cast<unsigned char>(c))) {
                word += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            } else if (!word.empty()) {
                words.insert(word);
                word.clear();
            }
        }
        if (!word.empty())
            words.insert(word);
        return words;
    };

    auto queryWords = tokenize(query);
    auto docWords = tokenize(document);

    if (queryWords.empty() || docWords.empty())
        return 0.0f;

    size_t overlap = 0;
    for (const auto& w : queryWords) {
        if (docWords.count(w))
            ++overlap;
    }

    // Jaccard-like score normalized to [0, 1]
    float score = static_cast<float>(overlap) / static_cast<float>(queryWords.size());
    return std::min(1.0f, score);
}

/**
 * Mock rerank function using word overlap scoring
 * Returns document indices sorted by score (highest first)
 */
std::vector<MockRerankerResult>
mockRerank(const std::string& query, const std::vector<std::string>& documents, size_t topK = 0) {
    std::vector<MockRerankerResult> results;
    results.reserve(documents.size());

    for (size_t i = 0; i < documents.size(); ++i) {
        results.push_back({computeWordOverlapScore(query, documents[i]), i});
    }

    // Sort by score descending
    std::sort(
        results.begin(), results.end(),
        [](const MockRerankerResult& a, const MockRerankerResult& b) { return a.score > b.score; });

    // Limit to topK if specified
    if (topK > 0 && topK < results.size()) {
        results.resize(topK);
    }

    return results;
}

/**
 * Hybrid + Rerank: Get top-N candidates from hybrid, rerank with word-overlap scoring
 *
 * NOTE: This uses a simple word-overlap mock scorer for demonstration.
 * For production use, integrate OnnxRerankerSession with BGE reranker model.
 * Install model with: yams model download bge-reranker-base
 */
QueryResult executeHybridRerank(yams::daemon::DaemonClient& client, const std::string& query,
                                const std::set<std::string>& relevant_ids, int k,
                                const BEIRDataset& dataset) {
    QueryResult result;
    result.query = query;
    result.relevant_doc_ids = relevant_ids;

    auto start = std::chrono::steady_clock::now();

    // Stage 1: Get candidates from hybrid search (retrieve more than k)
    const int candidateK = k * 5; // Get 5x candidates for reranking
    auto hybridResult = executeSearch(client, query, relevant_ids, candidateK, "hybrid");

    if (hybridResult.returned_doc_ids.empty()) {
        auto end = std::chrono::steady_clock::now();
        result.latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return result;
    }

    // Stage 2: Get document texts for reranking
    std::vector<std::string> docTexts;
    std::vector<std::string> docIds;
    docTexts.reserve(hybridResult.returned_doc_ids.size());
    docIds.reserve(hybridResult.returned_doc_ids.size());

    for (const auto& docId : hybridResult.returned_doc_ids) {
        auto it = dataset.documents.find(docId);
        if (it != dataset.documents.end()) {
            std::string text = it->second.title.empty()
                                   ? it->second.text
                                   : (it->second.title + " " + it->second.text);
            // Truncate to reasonable length for reranker
            if (text.size() > 1000) {
                text = text.substr(0, 1000);
            }
            docTexts.push_back(text);
            docIds.push_back(docId);
        }
    }

    if (docTexts.empty()) {
        auto end = std::chrono::steady_clock::now();
        result.latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return result;
    }

    // Stage 3: Rerank with mock word-overlap scorer
    // (In production, use OnnxRerankerSession with BGE model)
    auto rerankResults = mockRerank(query, docTexts, static_cast<size_t>(k));

    auto end = std::chrono::steady_clock::now();
    result.latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

    // Build final results from reranked order
    for (const auto& rr : rerankResults) {
        if (rr.document_index < docIds.size()) {
            const auto& docId = docIds[rr.document_index];
            result.returned_doc_ids.push_back(docId);
            result.returned_scores.push_back(rr.score);

            if (result.reciprocal_rank == 0.0 && relevant_ids.count(docId)) {
                result.reciprocal_rank = 1.0 / static_cast<double>(result.returned_doc_ids.size());
            }
        }
    }

    return result;
}

// ============================================================================
// Experiment Runner
// ============================================================================

ExperimentMetrics runExperiment(yams::daemon::DaemonClient& client,
                                const std::vector<TestQuery>& queries, FusionStrategy strategy,
                                int k, const BEIRDataset* dataset = nullptr) {
    ExperimentMetrics metrics;
    metrics.num_queries = static_cast<int>(queries.size());

    double totalMRR = 0.0;
    double totalRecall = 0.0;
    double totalPrecision = 0.0;
    double totalNDCG = 0.0;
    int64_t totalLatency = 0;

    for (const auto& tq : queries) {
        QueryResult qr;

        switch (strategy) {
            case FusionStrategy::BASELINE_KEYWORD:
                qr = executeSearch(client, tq.query, tq.relevantDocIds, k, "keyword");
                break;
            case FusionStrategy::BASELINE_HYBRID:
                qr = executeSearch(client, tq.query, tq.relevantDocIds, k, "hybrid");
                break;
            case FusionStrategy::TWO_STAGE_TEXT_FIRST:
                qr = executeTwoStageTextFirst(client, tq.query, tq.relevantDocIds, k);
                break;
            case FusionStrategy::SCORE_GATED_080:
                qr = executeScoreGated(client, tq.query, tq.relevantDocIds, k, 0.80f);
                break;
            case FusionStrategy::DOMINANCE_DETECTION:
                qr = executeDominanceDetection(client, tq.query, tq.relevantDocIds, k);
                break;
            case FusionStrategy::ADAPTIVE_ROUTING:
                qr = executeAdaptiveRouting(client, tq.query, tq.relevantDocIds, k);
                break;
            case FusionStrategy::HYBRID_RERANK:
                if (dataset) {
                    qr = executeHybridRerank(client, tq.query, tq.relevantDocIds, k, *dataset);
                } else {
                    qr = executeSearch(client, tq.query, tq.relevantDocIds, k, "hybrid");
                }
                break;
        }

        totalMRR += qr.reciprocal_rank;
        totalLatency += qr.latency_us;

        // Compute per-query metrics
        int relevantFound = 0;
        std::vector<int> grades;
        for (const auto& docId : qr.returned_doc_ids) {
            if (tq.relevantDocIds.count(docId)) {
                relevantFound++;
                grades.push_back(tq.relevanceGrades.count(docId) ? tq.relevanceGrades.at(docId)
                                                                 : 1);
            } else {
                grades.push_back(0);
            }
        }

        double recall = tq.relevantDocIds.empty()
                            ? 0.0
                            : static_cast<double>(relevantFound) /
                                  static_cast<double>(tq.relevantDocIds.size());
        double precision = qr.returned_doc_ids.empty()
                               ? 0.0
                               : static_cast<double>(relevantFound) /
                                     static_cast<double>(qr.returned_doc_ids.size());

        totalRecall += recall;
        totalPrecision += precision;
        totalNDCG += computeNDCG(grades, k);
    }

    int n = metrics.num_queries;
    if (n > 0) {
        metrics.mrr = totalMRR / n;
        metrics.recall_at_k = totalRecall / n;
        metrics.precision_at_k = totalPrecision / n;
        metrics.ndcg_at_k = totalNDCG / n;
        metrics.avg_latency_ms = (static_cast<double>(totalLatency) / n) / 1000.0;
    }

    return metrics;
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;

    spdlog::set_level(spdlog::level::info);

    const char* datasetEnv = std::getenv("YAMS_BENCH_DATASET");
    if (!datasetEnv || std::string(datasetEnv) != "scifact") {
        std::cerr << "Usage: YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=scifact "
                  << "./fusion_strategy_experiment\n";
        return 1;
    }

    // Set SCIENTIFIC tuning override
    setenv("YAMS_TUNING_OVERRIDE", "SCIENTIFIC", 1);

    std::cout << "=======================================================\n";
    std::cout << "       FUSION STRATEGY EXPERIMENT - SciFact\n";
    std::cout << "=======================================================\n\n";

    // Load BEIR dataset
    auto dataset = loadBEIRDataset("scifact");
    if (dataset.documents.empty()) {
        std::cerr << "Failed to load SciFact dataset\n";
        std::cerr
            << "Download with: python -c \"from beir import util; util.download_and_unzip("
            << "'https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip', "
            << "'~/.cache/yams/beir/')\"\n";
        return 1;
    }

    // Setup daemon harness with vector support
    yams::test::DaemonHarness::Options harnessOpts;
    harnessOpts.isolateState = true;
    harnessOpts.useMockModelProvider = false;
    harnessOpts.autoLoadPlugins = true;
    harnessOpts.configureModelPool = true;
    harnessOpts.modelPoolLazyLoading = false;

    // Configure plugin directory
    const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR");
    if (envPluginDir) {
        harnessOpts.pluginDir = fs::path(envPluginDir);
    } else {
        harnessOpts.pluginDir = fs::current_path() / "builddir" / "plugins";
    }
    harnessOpts.preloadModels = {"all-MiniLM-L6-v2"};

    auto harness = std::make_unique<yams::test::DaemonHarness>(harnessOpts);
    if (!harness->start(30s)) {
        std::cerr << "Failed to start daemon harness\n";
        return 1;
    }

    // Write corpus to temp directory
    fs::path corpusDir = harness->rootDir() / "corpus";
    BEIRCorpusLoader loader(dataset, corpusDir);
    loader.writeDocumentsAsFiles();

    // Create client
    yams::daemon::ClientConfig clientCfg;
    clientCfg.socketPath = harness->socketPath();
    clientCfg.connectTimeout = 5s;
    clientCfg.autoStart = false;
    auto client = std::make_unique<yams::daemon::DaemonClient>(clientCfg);

    auto connectResult = yams::cli::run_sync(client->connect(), 5s);
    if (!connectResult) {
        std::cerr << "Failed to connect to daemon: " << connectResult.error().message << "\n";
        return 1;
    }

    // Ingest corpus using streamingAddDocument
    std::cout << "Ingesting corpus (" << dataset.documents.size() << " docs)...\n";
    std::cout.flush();
    yams::daemon::AddDocumentRequest addReq;
    addReq.path = corpusDir.string();
    addReq.recursive = true;
    addReq.noEmbeddings = false;
    addReq.includePatterns = {"*.txt"};

    // Large corpus needs a longer timeout (10 minutes for 5000+ docs)
    auto ingestResult = yams::cli::run_sync(client->streamingAddDocument(addReq), 600s);
    if (!ingestResult) {
        std::cerr << "Failed to ingest corpus: " << ingestResult.error().message << "\n";
        return 1;
    }
    std::cout << "Ingestion request completed.\n";
    std::cout.flush();

    // Phase 1: Wait for ingestion to complete (documents_total reaches corpus size)
    const size_t corpusSize = dataset.documents.size();
    std::cout << "Waiting for ingestion to complete (" << corpusSize << " docs)...\n";
    std::cout.flush();
    auto deadline = std::chrono::steady_clock::now() + 900s; // 15 min for large corpus
    uint64_t lastDocCount = 0;
    int stableChecks = 0;
    bool ingestionComplete = false;

    while (std::chrono::steady_clock::now() < deadline) {
        auto statusResult = yams::cli::run_sync(client->status(), 5s);
        if (statusResult) {
            uint64_t depth = statusResult.value().postIngestQueueDepth;
            uint64_t docCount = 0;
            auto it = statusResult.value().requestCounts.find("documents_total");
            if (it != statusResult.value().requestCounts.end()) {
                docCount = it->second;
            }

            if (docCount != lastDocCount) {
                std::cout << "  Documents: " << docCount << " / " << corpusSize
                          << ", queue depth: " << depth << "\n";
                lastDocCount = docCount;
                stableChecks = 0;
            } else {
                stableChecks++;
            }

            // Complete when queue empty AND we have expected doc count (or stable for 5s)
            if (depth == 0 && (docCount >= corpusSize || stableChecks >= 10)) {
                std::cout << "Ingestion complete: " << docCount << " documents indexed\n";
                ingestionComplete = true;
                break;
            }
        }
        std::this_thread::sleep_for(500ms);
    }
    if (!ingestionComplete) {
        std::cout << "Warning: Ingestion did not complete within timeout\n";
    }

    // Phase 2: Wait for embedding provider to become available
    std::cout << "Waiting for embedding provider to become available...\n";
    deadline = std::chrono::steady_clock::now() + 30s;
    bool embeddingReady = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto statusResult = yams::cli::run_sync(client->status(), 5s);
        if (statusResult && statusResult.value().embeddingAvailable) {
            std::cout << "Embedding provider available (backend: "
                      << statusResult.value().embeddingBackend
                      << ", model: " << statusResult.value().embeddingModel << ")\n";
            embeddingReady = true;
            break;
        }
        std::this_thread::sleep_for(500ms);
    }
    if (!embeddingReady) {
        std::cout << "Warning: Embedding provider did not become available\n";
    }

    // Phase 3: Wait for embeddings to be generated
    std::cout << "Waiting for embeddings to be generated (target: " << corpusSize << " docs)...\n";
    deadline = std::chrono::steady_clock::now() +
               7200s; // 2 hour timeout for large corpus with slow models
    uint64_t lastVectorCount = 0;
    int stableCount = 0;
    uint64_t embedDropped = 0;
    auto embedStartTime = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() < deadline) {
        auto statusResult = yams::cli::run_sync(client->status(), 5s);
        if (statusResult) {
            // Get vector count
            uint64_t vectorCount = 0;
            auto it = statusResult.value().requestCounts.find("vector_count");
            if (it != statusResult.value().requestCounts.end()) {
                vectorCount = it->second;
            }

            // Check post-ingest queue status (documents still being processed before embedding)
            uint64_t postIngestInFlight = 0;
            auto itPI = statusResult.value().requestCounts.find("post_ingest_inflight");
            if (itPI != statusResult.value().requestCounts.end()) {
                postIngestInFlight = itPI->second;
            }

            // Check embed queue status (jobs waiting in channel) - use bus_embed_queued!
            uint64_t embedQueued = 0;
            auto itQ = statusResult.value().requestCounts.find("bus_embed_queued");
            if (itQ != statusResult.value().requestCounts.end()) {
                embedQueued = itQ->second;
            }

            // Check embed in-flight status (jobs being processed)
            uint64_t embedInFlight = 0;
            auto itInFlight = statusResult.value().requestCounts.find("embed_in_flight");
            if (itInFlight != statusResult.value().requestCounts.end()) {
                embedInFlight = itInFlight->second;
            }

            auto itD = statusResult.value().requestCounts.find("bus_embed_dropped");
            if (itD != statusResult.value().requestCounts.end()) {
                embedDropped = itD->second;
            }

            if (vectorCount != lastVectorCount) {
                double coverage = corpusSize > 0 ? (vectorCount * 100.0 / corpusSize) : 0;
                std::cout << "  Vectors: " << vectorCount << " / " << corpusSize << " ("
                          << std::fixed << std::setprecision(1) << coverage << "%)"
                          << " | post_ingest=" << postIngestInFlight << " queue=" << embedQueued
                          << " in_flight=" << embedInFlight << " dropped=" << embedDropped << "\n";
                lastVectorCount = vectorCount;
                stableCount = 0;
            } else {
                stableCount++;
            }

            // Success: vectors >= corpusSize AND all queues drained AND stable
            // Must check post_ingest_inflight to avoid race where docs haven't reached embed
            // channel yet
            bool queueDrained = (postIngestInFlight == 0 && embedQueued == 0 && embedInFlight == 0);
            if (vectorCount >= corpusSize && queueDrained && stableCount >= 10) {
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - embedStartTime)
                                   .count();
                std::cout << "Full embedding coverage: " << vectorCount << " vectors for "
                          << corpusSize << " docs in " << elapsed << "s\n";
                break;
            }

            // If very stable (20s) with queue drained, we're done even if < corpusSize
            if (stableCount >= 40 && queueDrained && vectorCount > 0) {
                double coverage = corpusSize > 0 ? (vectorCount * 100.0 / corpusSize) : 0;
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - embedStartTime)
                                   .count();
                std::cout << "Embedding stable at " << coverage << "% coverage (" << vectorCount
                          << " vectors) after " << elapsed << "s\n";
                break;
            }
        }
        std::this_thread::sleep_for(500ms);
    }

    // Generate test queries
    auto queries = loader.generateTestQueries();

    // Limit queries
    const char* numQueriesEnv = std::getenv("YAMS_BENCH_NUM_QUERIES");
    size_t maxQueries = numQueriesEnv ? std::stoul(numQueriesEnv) : 100;
    if (queries.size() > maxQueries) {
        queries.resize(maxQueries);
    }

    std::cout << "\nRunning experiments with " << queries.size() << " queries...\n";
    std::cout << "-------------------------------------------------------\n\n";

    const int k = 10;

    // Run all strategies
    std::vector<FusionStrategy> strategies = {
        FusionStrategy::BASELINE_KEYWORD,     FusionStrategy::BASELINE_HYBRID,
        FusionStrategy::TWO_STAGE_TEXT_FIRST, FusionStrategy::SCORE_GATED_080,
        FusionStrategy::DOMINANCE_DETECTION,  FusionStrategy::ADAPTIVE_ROUTING,
        FusionStrategy::HYBRID_RERANK};

    std::map<std::string, ExperimentMetrics> results;

    for (auto strategy : strategies) {
        std::cout << "Running: " << strategyName(strategy) << "..." << std::flush;
        auto metrics = runExperiment(*client, queries, strategy, k, &dataset);
        results[strategyName(strategy)] = metrics;

        std::cout << " MRR=" << std::fixed << std::setprecision(4) << metrics.mrr << " Recall@" << k
                  << "=" << metrics.recall_at_k << "\n";
    }

    // Print summary
    std::cout << "\n=======================================================\n";
    std::cout << "                    RESULTS SUMMARY\n";
    std::cout << "=======================================================\n\n";

    std::cout << std::left << std::setw(25) << "Strategy" << std::right << std::setw(10) << "MRR"
              << std::setw(12) << "Recall@10" << std::setw(12) << "Precision" << std::setw(10)
              << "nDCG" << std::setw(12) << "Latency(ms)"
              << "\n";
    std::cout << std::string(81, '-') << "\n";

    for (const auto& [name, m] : results) {
        std::cout << std::left << std::setw(25) << name << std::right << std::fixed
                  << std::setprecision(4) << std::setw(10) << m.mrr << std::setw(12)
                  << m.recall_at_k << std::setw(12) << m.precision_at_k << std::setw(10)
                  << m.ndcg_at_k << std::setprecision(1) << std::setw(12) << m.avg_latency_ms
                  << "\n";
    }

    std::cout << "\n=======================================================\n";

    // Save JSON
    json jsonResults;
    jsonResults["dataset"] = "scifact";
    jsonResults["num_queries"] = queries.size();
    jsonResults["k"] = k;
    jsonResults["strategies"] = json::object();

    for (const auto& [name, m] : results) {
        jsonResults["strategies"][name] = m.toJson();
    }

    std::ofstream outFile("/tmp/fusion_experiment_results.json");
    outFile << jsonResults.dump(2) << "\n";
    std::cout << "\nResults saved to /tmp/fusion_experiment_results.json\n";

    return 0;
}
