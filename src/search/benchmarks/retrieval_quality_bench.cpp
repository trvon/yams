/*
  retrieval_quality_bench.cpp - RAG Retrieval Quality Benchmark

  Uses daemon-based ingestion to properly test retrieval quality with IR metrics.

  Usage:
    YAMS_TEST_SAFE_SINGLE_INSTANCE=1 ./builddir/tests/benchmarks/retrieval_quality_bench

  Environment variables:
    YAMS_TEST_SAFE_SINGLE_INSTANCE=1  - Required to prevent GlobalIOContext reset
    YAMS_BENCH_CORPUS_SIZE=N          - Number of documents to generate (default: 50)
    YAMS_BENCH_NUM_QUERIES=N          - Number of test queries (default: 10)
    YAMS_BENCH_TOPK=N                 - Retrieve top K results (default: 10)
    YAMS_BENCH_DATASET=<name>         - Use BEIR dataset (default: synthetic)
    YAMS_BENCH_DATASET_PATH=...       - Path to dataset directory

  Tuning for faster ingestion (recommended for large corpora):
    YAMS_POST_EMBED_CONCURRENT=12     - Parallel embedding workers (default: 4-8)
    YAMS_POST_EXTRACTION_CONCURRENT=12- Parallel content extraction (default: 4)
    YAMS_POST_KG_CONCURRENT=12        - Parallel KG extraction (default: 4)
    YAMS_POST_INGEST_BATCH_SIZE=24    - Documents per batch (default: 8, adaptive)
    YAMS_EMBED_CHANNEL_CAPACITY=16384 - Embedding queue capacity (default: 8192)
    YAMS_DB_POOL_MAX=48               - Database connection pool (default: 20)

  Supported BEIR datasets (download from
  https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/): scifact       - Scientific
  fact verification (300 queries, 5K docs) cqadupstack   - Community Q&A including programming (13K
  queries, 457K docs) nfcorpus      - NutritionFacts corpus (323 queries, 3.6K docs) arguana       -
  Argument retrieval (1.4K queries, 8.7K docs) scidocs       - Scientific document similarity (1K
  queries, 25K docs) fiqa          - Financial question answering (648 queries, 57K docs) quora -
  Duplicate question retrieval (10K queries, 523K docs) hotpotqa      - Multi-hop question answering
  (7.4K queries, 5.2M docs) fever         - Fact verification (6.7K queries, 5.4M docs)
    climate-fever - Climate claim verification (1.5K queries, 5.4M docs)
    dbpedia-entity- Entity retrieval (400 queries, 4.6M docs)
    trec-covid    - COVID-19 retrieval (50 queries, 171K docs)
    touche-2020   - Argument retrieval (49 queries, 382K docs)

  Example (code-related benchmark):
    mkdir -p ~/.cache/yams/benchmarks && cd ~/.cache/yams/benchmarks
    curl -L -o cqadupstack.zip
  https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/cqadupstack.zip unzip
  cqadupstack.zip YAMS_TEST_SAFE_SINGLE_INSTANCE=1 YAMS_BENCH_DATASET=cqadupstack
  ./builddir/tests/benchmarks/retrieval_quality_bench
*/

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <benchmark/benchmark.h>

#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"
#include <yams/cli/search_runner.h>
#include <yams/daemon/client/daemon_client.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <yams/compat/unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace yams;
using json = nlohmann::json;

// Helper to discover GLiNER model path for entity extraction
// Searches standard locations where yams init downloads models
static std::string discoverGlinerModelPath() {
    // Preferred model names in order
    const std::vector<std::string> model_names = {
        "gliner_small-v2.1-quantized",
        "gliner_small-v2.1",
        "gliner_medium-v2.1-quantized",
        "gliner_medium-v2.1",
    };

    // Build list of base directories to search
    std::vector<fs::path> search_dirs;

    // YAMS_GLINT_MODEL_PATH - explicit override
    if (const char* env_path = std::getenv("YAMS_GLINT_MODEL_PATH")) {
        if (fs::exists(env_path)) {
            spdlog::info("[Bench] Using GLiNER model from YAMS_GLINT_MODEL_PATH: {}", env_path);
            return env_path;
        }
    }

    // YAMS_DATA_DIR
    if (const char* data_dir = std::getenv("YAMS_DATA_DIR")) {
        search_dirs.emplace_back(fs::path(data_dir) / "models" / "gliner");
    }

    // YAMS_STORAGE
    if (const char* storage = std::getenv("YAMS_STORAGE")) {
        search_dirs.emplace_back(fs::path(storage) / "models" / "gliner");
    }

    // ~/.local/share/yams (XDG default)
    if (const char* home = std::getenv("HOME")) {
        search_dirs.emplace_back(fs::path(home) / ".local" / "share" / "yams" / "models" /
                                 "gliner");
    }

    // Development path
    search_dirs.emplace_back("/Volumes/picaso/yams/models/gliner");

    // Homebrew path
    search_dirs.emplace_back("/opt/homebrew/share/yams/models/gliner");

    // Search for models
    for (const auto& base_dir : search_dirs) {
        if (!fs::exists(base_dir)) {
            continue;
        }

        // Try preferred model names first
        for (const auto& model_name : model_names) {
            fs::path model_path = base_dir / model_name / "model.onnx";
            if (fs::exists(model_path)) {
                spdlog::info("[Bench] Found GLiNER model at: {}", model_path.string());
                return model_path.string();
            }
        }

        // Fall back to scanning directory for any model
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(base_dir, ec)) {
            if (!entry.is_directory())
                continue;
            fs::path model_path = entry.path() / "model.onnx";
            if (fs::exists(model_path)) {
                spdlog::info("[Bench] Found GLiNER model at: {}", model_path.string());
                return model_path.string();
            }
        }
    }

    spdlog::warn("[Bench] No GLiNER model found - entity extraction will use mock mode");
    return "";
}

struct BEIRDocument {
    std::string id;
    std::string title;
    std::string text;
};

struct BEIRQuery {
    std::string id;
    std::string text;
};

struct BEIRDataset {
    std::string name;
    std::map<std::string, BEIRDocument> documents;
    std::map<std::string, BEIRQuery> queries;
    std::multimap<std::string, std::pair<std::string, int>> qrels;
    fs::path basePath;
};

static Result<BEIRDataset> loadSciFactDataset(const fs::path& cacheDir) {
    const char* home = std::getenv("HOME");
    if (!home) {
        return Error{ErrorCode::InvalidArgument, "HOME environment variable not set"};
    }

    fs::path scifactDir =
        cacheDir.empty() ? fs::path(home) / ".cache" / "yams" / "benchmarks" / "scifact" : cacheDir;

    BEIRDataset dataset;
    dataset.name = "SciFact";
    dataset.basePath = scifactDir;

    fs::path corpusFile = scifactDir / "corpus.jsonl";
    fs::path queriesFile = scifactDir / "queries.jsonl";
    fs::path qrelsFile = scifactDir / "qrels" / "test.tsv"; // Standard BEIR structure

    if (!fs::exists(corpusFile) || !fs::exists(queriesFile) || !fs::exists(qrelsFile)) {
        return Error{
            ErrorCode::NotFound,
            "SciFact dataset not found. Please download manually:\n"
            "  mkdir -p ~/.cache/yams/benchmarks && cd ~/.cache/yams/benchmarks\n"
            "  curl -L -o scifact.zip "
            "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/scifact.zip\n"
            "  unzip scifact.zip"};
    }

    std::ifstream corpusIn(corpusFile);
    std::string line;
    while (std::getline(corpusIn, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRDocument doc;
            doc.id = j.value("_id", "");
            if (doc.id.empty())
                doc.id = j.value("id", "");
            doc.title = j.value("title", "");
            doc.text = j.value("text", "");
            if (!doc.id.empty() && !doc.text.empty()) {
                dataset.documents[doc.id] = doc;
            }
        } catch (const json::exception& e) {
            spdlog::warn("Failed to parse corpus line: {}", e.what());
        }
    }
    corpusIn.close();

    std::ifstream queriesIn(queriesFile);
    while (std::getline(queriesIn, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRQuery q;
            q.id = j.value("_id", "");
            if (q.id.empty())
                q.id = j.value("id", "");
            q.text = j.value("text", "");
            if (!q.id.empty() && !q.text.empty()) {
                dataset.queries[q.id] = q;
            }
        } catch (const json::exception& e) {
            spdlog::warn("Failed to parse query line: {}", e.what());
        }
    }
    queriesIn.close();

    std::ifstream qrelsIn(qrelsFile);
    bool firstLine = true;
    while (std::getline(qrelsIn, line)) {
        if (line.empty() || line[0] == '#')
            continue;
        // Skip header row
        if (firstLine) {
            firstLine = false;
            if (line.find("query-id") != std::string::npos)
                continue;
        }
        std::istringstream iss(line);
        std::string queryId, docId, scoreStr;
        // BEIR format: query-id, corpus-id, score (3 columns, no iteration)
        if (std::getline(iss, queryId, '\t') && std::getline(iss, docId, '\t') &&
            std::getline(iss, scoreStr, '\t')) {
            int score = std::stoi(scoreStr);
            dataset.qrels.emplace(queryId, std::make_pair(docId, score));
        }
    }
    qrelsIn.close();

    spdlog::info("Loaded BEIR dataset: {} documents, {} queries, {} qrels",
                 dataset.documents.size(), dataset.queries.size(), dataset.qrels.size());

    return dataset;
}

// Generic BEIR dataset loader - works with any dataset from:
// https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/
// Supported datasets: cqadupstack, nfcorpus, arguana, scidocs, fiqa, quora, hotpotqa,
//                     fever, climate-fever, dbpedia-entity, trec-covid, touche-2020
static Result<BEIRDataset> loadBEIRDataset(const std::string& datasetName,
                                           const fs::path& cacheDir) {
    const char* home = std::getenv("HOME");
    if (!home) {
        return Error{ErrorCode::InvalidArgument, "HOME environment variable not set"};
    }

    // Map dataset names to their zip file names (some differ)
    static const std::map<std::string, std::string> DATASET_ZIP_NAMES = {
        {"touche-2020", "webis-touche2020"},
        {"dbpedia-entity", "dbpedia-entity"},
    };

    std::string zipName = datasetName;
    auto zipIt = DATASET_ZIP_NAMES.find(datasetName);
    if (zipIt != DATASET_ZIP_NAMES.end()) {
        zipName = zipIt->second;
    }

    fs::path datasetDir = cacheDir.empty()
                              ? fs::path(home) / ".cache" / "yams" / "benchmarks" / datasetName
                              : cacheDir;

    BEIRDataset dataset;
    dataset.name = datasetName;
    dataset.basePath = datasetDir;

    fs::path corpusFile = datasetDir / "corpus.jsonl";
    fs::path queriesFile = datasetDir / "queries.jsonl";
    fs::path qrelsFile = datasetDir / "qrels" / "test.tsv";

    if (!fs::exists(corpusFile) || !fs::exists(queriesFile) || !fs::exists(qrelsFile)) {
        std::ostringstream oss;
        oss << datasetName << " dataset not found. Please download manually:\n"
            << "  mkdir -p ~/.cache/yams/benchmarks && cd ~/.cache/yams/benchmarks\n"
            << "  curl -L -o " << zipName << ".zip "
            << "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/" << zipName
            << ".zip\n"
            << "  unzip " << zipName << ".zip";
        if (datasetName != zipName) {
            oss << " && mv " << zipName << " " << datasetName;
        }
        return Error{ErrorCode::NotFound, oss.str()};
    }

    // Load corpus
    std::ifstream corpusIn(corpusFile);
    std::string line;
    while (std::getline(corpusIn, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRDocument doc;
            doc.id = j.value("_id", "");
            if (doc.id.empty())
                doc.id = j.value("id", "");
            doc.title = j.value("title", "");
            doc.text = j.value("text", "");
            if (!doc.id.empty() && !doc.text.empty()) {
                dataset.documents[doc.id] = doc;
            }
        } catch (const json::exception& e) {
            spdlog::warn("Failed to parse corpus line: {}", e.what());
        }
    }
    corpusIn.close();

    // Load queries
    std::ifstream queriesIn(queriesFile);
    while (std::getline(queriesIn, line)) {
        if (line.empty())
            continue;
        try {
            auto j = json::parse(line);
            BEIRQuery q;
            q.id = j.value("_id", "");
            if (q.id.empty())
                q.id = j.value("id", "");
            q.text = j.value("text", "");
            if (!q.id.empty() && !q.text.empty()) {
                dataset.queries[q.id] = q;
            }
        } catch (const json::exception& e) {
            spdlog::warn("Failed to parse query line: {}", e.what());
        }
    }
    queriesIn.close();

    // Load qrels
    std::ifstream qrelsIn(qrelsFile);
    bool firstLine = true;
    while (std::getline(qrelsIn, line)) {
        if (line.empty() || line[0] == '#')
            continue;
        if (firstLine) {
            firstLine = false;
            if (line.find("query-id") != std::string::npos)
                continue;
        }
        std::istringstream iss(line);
        std::string queryId, docId, scoreStr;
        if (std::getline(iss, queryId, '\t') && std::getline(iss, docId, '\t') &&
            std::getline(iss, scoreStr, '\t')) {
            int score = std::stoi(scoreStr);
            dataset.qrels.emplace(queryId, std::make_pair(docId, score));
        }
    }
    qrelsIn.close();

    spdlog::info("Loaded BEIR {} dataset: {} documents, {} queries, {} qrels", datasetName,
                 dataset.documents.size(), dataset.queries.size(), dataset.qrels.size());

    return dataset;
}

using yams::test::DaemonHarness;

struct TestQuery {
    std::string query;
    std::set<std::string> relevantFiles;
    std::set<std::string> relevantDocIds;
    std::map<std::string, int> relevanceGrades;
    bool useDocIds = false;
};

struct CorpusGenerator {
    std::vector<std::string> topics = {"authentication", "database", "network", "parsing",
                                       "encryption",     "testing",  "logging", "storage"};
    std::vector<std::string> terms = {"user",       "password", "token",    "session",
                                      "connection", "request",  "response", "cache",
                                      "error",      "file",     "data"};
    fs::path corpusDir;
    std::vector<std::string> createdFiles;
    std::mt19937 rng{42};

    CorpusGenerator(const fs::path& dir) : corpusDir(dir) { fs::create_directories(corpusDir); }

    void generateDocuments(int count) {
        std::uniform_int_distribution<int> topicDist(0, topics.size() - 1);
        std::uniform_int_distribution<int> termDist(0, terms.size() - 1);
        for (int i = 0; i < count; ++i) {
            std::string topicName = topics[topicDist(rng)];
            std::string content = "This document covers " + topicName + " functionality.\nThe " +
                                  topicName + " system handles ";
            for (int t = 0; t < 10; ++t)
                content += terms[termDist(rng)] + " ";
            content += "\nImplementation of " + topicName + " requires careful design.\n";
            std::string filename = topicName + "_" + std::to_string(i) + ".txt";
            std::ofstream(corpusDir / filename) << content;
            createdFiles.push_back(filename);
        }
    }

    std::vector<TestQuery> generateQueries(int numQueries) {
        std::vector<TestQuery> queries;
        std::uniform_int_distribution<int> topicDist(0, topics.size() - 1);
        for (int q = 0; q < numQueries; ++q) {
            std::string topic = topics[topicDist(rng)];
            TestQuery tq;
            tq.query = topic + " system";
            for (const auto& filename : createdFiles) {
                if (filename.find(topic) != std::string::npos) {
                    tq.relevantFiles.insert(filename);
                    tq.relevanceGrades[filename] = (filename.find(topic) == 0) ? 3 : 2;
                }
            }
            if (!tq.relevantFiles.empty())
                queries.push_back(tq);
        }
        return queries;
    }
};

struct BEIRCorpusLoader {
    BEIRDataset dataset;
    fs::path corpusDir;
    std::map<std::string, std::string> docIdToHash;

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
            outFile.close();
            docIdToHash[id] = filePath.string();
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
        spdlog::info("Generated {} test queries from BEIR dataset", testQueries.size());
        return testQueries;
    }

    std::vector<std::string> getDocumentPaths() const {
        std::vector<std::string> paths;
        for (const auto& [id, doc] : dataset.documents) {
            paths.push_back((corpusDir / (id + ".txt")).string());
        }
        return paths;
    }
};

struct RetrievalMetrics {
    double mrr = 0.0, recallAtK = 0.0, precisionAtK = 0.0, ndcgAtK = 0.0, map = 0.0;
    int numQueries = 0;
};

struct DebugLogEntry {
    std::string query;
    std::vector<std::string> relevantDocIds;
    std::vector<std::string> relevantFiles;
    std::vector<std::string> returnedPaths;
    std::vector<std::string> returnedDocIds;
    std::vector<float> returnedScores;
    std::vector<int> returnedGrades;
    std::vector<std::string> diagnostics;
    std::uint64_t attempts = 0;
    bool usedStreaming = false;
    bool usedFuzzyFlag = false;
    bool usedLiteralFlag = false;
    bool usedFuzzyRetry = false;
    bool usedLiteralTextRetry = false;
};

static std::ostream* g_debugOut = nullptr;
static std::unique_ptr<std::ofstream> g_debugFile;
static std::mutex g_debugMutex;

static void debugLogWriteJsonLine(const DebugLogEntry& e) {
    if (!g_debugOut)
        return;

    json j;
    j["query"] = e.query;
    j["relevant_doc_ids"] = e.relevantDocIds;
    j["relevant_files"] = e.relevantFiles;
    j["returned_paths"] = e.returnedPaths;
    j["returned_doc_ids"] = e.returnedDocIds;
    j["returned_scores"] = e.returnedScores;
    j["returned_grades"] = e.returnedGrades;
    j["attempts"] = e.attempts;
    j["used_streaming"] = e.usedStreaming;
    j["used_fuzzy_flag"] = e.usedFuzzyFlag;
    j["used_literal_flag"] = e.usedLiteralFlag;
    j["used_fuzzy_retry"] = e.usedFuzzyRetry;
    j["used_literal_retry"] = e.usedLiteralTextRetry;
    j["diag"] = e.diagnostics;

    std::lock_guard<std::mutex> lock(g_debugMutex);
    (*g_debugOut) << j.dump() << "\n";
    g_debugOut->flush();
}

// Store final metrics globally for summary after teardown
static RetrievalMetrics g_final_metrics;
static RetrievalMetrics g_keyword_metrics; // FTS5-only metrics for component isolation

double computeDCG(const std::vector<int>& grades, int k) {
    double dcg = 0.0;
    for (int i = 0; i < std::min(k, (int)grades.size()); ++i) {
        dcg += (std::pow(2.0, grades[i]) - 1.0) / std::log2(i + 2.0);
    }
    return dcg;
}

double computeIDCG(std::vector<int> grades, int k) {
    std::sort(grades.begin(), grades.end(), std::greater<int>());
    return computeDCG(grades, k);
}

RetrievalMetrics evaluateQueries(yams::daemon::DaemonClient& client, const fs::path& corpusDir,
                                 const std::vector<TestQuery>& queries, int k,
                                 const std::string& searchType = "hybrid") {
    RetrievalMetrics metrics;
    metrics.numQueries = queries.size();
    double totalMRR = 0.0, totalRecall = 0.0, totalPrecision = 0.0, totalNDCG = 0.0, totalMAP = 0.0;

    std::uint64_t totalAttempts = 0;
    std::uint64_t streamingCount = 0;
    std::uint64_t fuzzyRetryCount = 0;
    std::uint64_t literalRetryCount = 0;

    for (const auto& tq : queries) {
        yams::cli::search_runner::DaemonSearchOptions opts;
        opts.query = tq.query;
        opts.searchType = searchType;
        opts.limit = static_cast<std::size_t>(k);
        std::chrono::milliseconds queryTimeout{20000};
        if (const char* env = std::getenv("YAMS_BENCH_QUERY_TIMEOUT_MS")) {
            queryTimeout = std::chrono::milliseconds{
                static_cast<std::chrono::milliseconds::rep>(std::stoll(env))};
        }
        opts.timeout = queryTimeout;
        opts.symbolRank = true;
        const bool benchDiagEnabled = []() -> bool {
            if (const char* env = std::getenv("YAMS_BENCH_DIAG"); env && std::string(env) == "1") {
                return true;
            }
            return false;
        }();

        spdlog::info("Executing search query: '{}'", tq.query);
        // Wait at least as long as the query timeout, plus a small margin.
        // Using a shorter run_sync deadline can leave in-flight requests behind,
        // which can cascade into timeouts/backpressure across subsequent queries.
        auto waitBudget = std::chrono::duration_cast<std::chrono::milliseconds>(opts.timeout) +
                          std::chrono::seconds(10);
        if (waitBudget < std::chrono::seconds(10)) {
            waitBudget = std::chrono::seconds(10);
        }
        auto run = yams::cli::run_sync(yams::cli::search_runner::daemon_search(client, opts, true),
                                       waitBudget);
        if (!run) {
            spdlog::warn("Search failed for query '{}': {}", tq.query, run.error().message);
            continue;
        }

        totalAttempts += run.value().attempts;
        if (run.value().usedStreaming)
            streamingCount++;
        if (run.value().usedFuzzyRetry)
            fuzzyRetryCount++;
        if (run.value().usedLiteralTextRetry)
            literalRetryCount++;

        const auto& results = run.value().response.results;
        spdlog::info("Search returned {} results for query '{}' (attempts={}, streaming={}, "
                     "fuzzy_retry={}, literal_retry={})",
                     results.size(), tq.query, run.value().attempts, run.value().usedStreaming,
                     run.value().usedFuzzyRetry, run.value().usedLiteralTextRetry);

        // Detailed result logging for debugging retrieval quality
        if (benchDiagEnabled) {
            spdlog::info("  [{}] Expected relevant: {}", searchType,
                         fmt::join(tq.relevantDocIds, ", "));
            for (size_t i = 0; i < std::min((size_t)5, results.size()); ++i) {
                std::string filename = fs::path(results[i].path).filename().string();
                std::string docId = filename;
                if (docId.size() > 4 && docId.substr(docId.size() - 4) == ".txt") {
                    docId = docId.substr(0, docId.size() - 4);
                }
                bool relevant = tq.relevantDocIds.count(docId) > 0;
                spdlog::info("  [{}] Result {}: path='{}' docId='{}' score={:.4f} {}", searchType,
                             i, results[i].path, docId, results[i].score,
                             relevant ? "RELEVANT" : "");
            }
        }

        DebugLogEntry debugEntry;
        debugEntry.query = tq.query;
        debugEntry.attempts = run.value().attempts;
        debugEntry.usedStreaming = run.value().usedStreaming;
        debugEntry.usedFuzzyFlag = false;
        debugEntry.usedLiteralFlag = false;
        debugEntry.usedFuzzyRetry = run.value().usedFuzzyRetry;
        debugEntry.usedLiteralTextRetry = run.value().usedLiteralTextRetry;
        debugEntry.relevantDocIds.assign(tq.relevantDocIds.begin(), tq.relevantDocIds.end());
        debugEntry.relevantFiles.assign(tq.relevantFiles.begin(), tq.relevantFiles.end());

        if (benchDiagEnabled) {
            debugEntry.diagnostics.push_back("searchType=" + opts.searchType);
            debugEntry.diagnostics.push_back(std::string("symbolRank=") +
                                             (opts.symbolRank ? "true" : "false"));
            debugEntry.diagnostics.push_back("limit=" + std::to_string(opts.limit));
            debugEntry.diagnostics.push_back(
                "timeout_ms=" +
                std::to_string(
                    std::chrono::duration_cast<std::chrono::milliseconds>(opts.timeout).count()));
        }

        int firstRelevantRank = -1, numRelevantInTopK = 0, numRelevantSeen = 0;
        std::vector<int> retrievedGrades;
        double avgPrecision = 0.0;

        for (size_t i = 0; i < std::min((size_t)k, results.size()); ++i) {
            debugEntry.returnedPaths.push_back(results[i].path);
            debugEntry.returnedScores.push_back(results[i].score);

            std::string filename = fs::path(results[i].path).filename().string();
            bool isRelevant = false;
            std::string key;

            if (tq.useDocIds) {
                std::string docId = filename;
                if (docId.size() > 4 && docId.substr(docId.size() - 4) == ".txt") {
                    docId = docId.substr(0, docId.size() - 4);
                }
                debugEntry.returnedDocIds.push_back(docId);
                key = docId;
                isRelevant = tq.relevantDocIds.count(docId) > 0;
            } else {
                debugEntry.returnedDocIds.push_back(filename);
                key = filename;
                isRelevant = tq.relevantFiles.count(filename) > 0;
            }

            if (isRelevant) {
                numRelevantInTopK++;
                if (firstRelevantRank < 0)
                    firstRelevantRank = i + 1;
                numRelevantSeen++;
                avgPrecision += (double)numRelevantSeen / (i + 1);
            }
            auto gradeIt = tq.relevanceGrades.find(key);
            auto grade = (gradeIt != tq.relevanceGrades.end()) ? gradeIt->second : 0;
            retrievedGrades.push_back(grade);
            debugEntry.returnedGrades.push_back(grade);
        }

        if (benchDiagEnabled && results.empty() && tq.useDocIds && !tq.relevantDocIds.empty()) {
            // Opt-in diagnostic: show a snippet of the relevant BEIR doc file(s).
            std::size_t printed = 0;
            for (const auto& docId : tq.relevantDocIds) {
                if (printed >= 2)
                    break;
                const fs::path p = corpusDir / (docId + ".txt");
                if (!fs::exists(p)) {
                    debugEntry.diagnostics.push_back("relevant_doc_missing=" + p.string());
                    continue;
                }
                std::ifstream in(p);

                if (!in.is_open()) {
                    debugEntry.diagnostics.push_back("relevant_doc_open_failed=" + p.string());
                    continue;
                }
                std::string snippet;
                snippet.resize(240);
                in.read(snippet.data(), static_cast<std::streamsize>(snippet.size()));
                snippet.resize(static_cast<std::size_t>(in.gcount()));
                for (auto& c : snippet) {
                    if (c == '\n' || c == '\r' || c == '\t')
                        c = ' ';
                }
                debugEntry.diagnostics.push_back("relevant_doc_snippet[" + docId + "]=" + snippet);
                ++printed;
            }
        }

        // Opt-in diagnostic: run a few "shadow" queries for failing cases.
        // This does not affect scoring; it only emits diagnostic entries.
        if (benchDiagEnabled && results.empty()) {
            // 1) Try a shorter query with a few salient tokens to test whether the full
            // sentence query is failing due to strict AND semantics / tokenization.
            std::istringstream iss(tq.query);
            std::string token;
            std::vector<std::string> picked;
            picked.reserve(3);
            while (iss >> token && picked.size() < 3) {
                // Strip trailing punctuation for the diagnostic query.
                while (!token.empty() && std::ispunct(static_cast<unsigned char>(token.back()))) {
                    token.pop_back();
                }
                if (token.size() < 4)
                    continue;
                picked.push_back(token);
            }

            if (!picked.empty()) {
                std::string diagQuery;
                for (size_t i = 0; i < picked.size(); ++i) {
                    if (i)
                        diagQuery += ' ';
                    diagQuery += picked[i];
                }

                yams::cli::search_runner::DaemonSearchOptions shadow;
                shadow.query = diagQuery;
                shadow.searchType = opts.searchType;
                shadow.limit = opts.limit;
                shadow.timeout = opts.timeout;
                shadow.symbolRank = opts.symbolRank;

                DebugLogEntry shadowEntry;
                shadowEntry.query = "__diag_shadow_query__";
                shadowEntry.relevantDocIds.assign(tq.relevantDocIds.begin(),
                                                  tq.relevantDocIds.end());
                shadowEntry.diagnostics.push_back("original_query=" + tq.query);
                shadowEntry.diagnostics.push_back("shadow_query=" + diagQuery);

                auto shadowRun = yams::cli::run_sync(
                    yams::cli::search_runner::daemon_search(client, shadow, true),
                    std::chrono::duration_cast<std::chrono::milliseconds>(shadow.timeout) +
                        std::chrono::seconds(10));
                if (shadowRun) {
                    shadowEntry.attempts = shadowRun.value().attempts;
                    shadowEntry.usedStreaming = shadowRun.value().usedStreaming;
                    shadowEntry.usedFuzzyRetry = shadowRun.value().usedFuzzyRetry;
                    shadowEntry.usedLiteralTextRetry = shadowRun.value().usedLiteralTextRetry;
                    shadowEntry.returnedDocIds.push_back(
                        "result_count=" +
                        std::to_string(shadowRun.value().response.results.size()));
                    for (size_t i = 0;
                         i < std::min<size_t>(5, shadowRun.value().response.results.size()); ++i) {
                        shadowEntry.returnedPaths.push_back(
                            shadowRun.value().response.results[i].path);
                    }
                } else {
                    shadowEntry.returnedDocIds.push_back("shadow_error=" +
                                                         shadowRun.error().message);
                }

                debugLogWriteJsonLine(shadowEntry);
            }
        }

        debugLogWriteJsonLine(debugEntry);

        if (firstRelevantRank > 0)
            totalMRR += 1.0 / firstRelevantRank;
        if (tq.useDocIds) {
            if (tq.relevantDocIds.size() > 0)
                totalRecall += (double)numRelevantInTopK / tq.relevantDocIds.size();
        } else {
            if (tq.relevantFiles.size() > 0)
                totalRecall += (double)numRelevantInTopK / tq.relevantFiles.size();
        }
        if (results.size() > 0)
            totalPrecision += (double)numRelevantInTopK / std::min((size_t)k, results.size());
        if (numRelevantSeen > 0)
            totalMAP += avgPrecision / numRelevantSeen;

        std::vector<int> allGrades;
        for (const auto& [fn, grade] : tq.relevanceGrades)
            allGrades.push_back(grade);
        double dcg = computeDCG(retrievedGrades, k);
        double idcg = computeIDCG(allGrades, k);
        totalNDCG += (idcg > 0.0) ? dcg / idcg : 0.0;
    }

    if (metrics.numQueries > 0) {
        metrics.mrr = totalMRR / metrics.numQueries;
        metrics.recallAtK = totalRecall / metrics.numQueries;
        metrics.precisionAtK = totalPrecision / metrics.numQueries;
        metrics.ndcgAtK = totalNDCG / metrics.numQueries;
        metrics.map = totalMAP / metrics.numQueries;

        spdlog::info("Search execution stats: queries={} total_attempts={} avg_attempts={:.2f} "
                     "streaming={} fuzzy_retries={} literal_retries={}",
                     metrics.numQueries, totalAttempts,
                     static_cast<double>(totalAttempts) / metrics.numQueries, streamingCount,
                     fuzzyRetryCount, literalRetryCount);
    }
    return metrics;
}

struct BenchFixture {
    std::unique_ptr<DaemonHarness> harness;
    std::unique_ptr<yams::daemon::DaemonClient> client;
    std::unique_ptr<CorpusGenerator> corpus;
    std::unique_ptr<BEIRCorpusLoader> beirCorpus;
    fs::path benchCorpusDir;
    std::vector<TestQuery> queries;
    int corpusSize = 50, numQueries = 10, topK = 10;
    bool useBEIR = false;
    std::string beirDatasetName; // Name of BEIR dataset (scifact, cqadupstack, etc.)

    void setup() {
        const char* env_dataset = std::getenv("YAMS_BENCH_DATASET");
        const char* env_debug_file = std::getenv("YAMS_BENCH_DEBUG_FILE");
        if (env_debug_file && std::strlen(env_debug_file) > 0) {
            g_debugFile =
                std::make_unique<std::ofstream>(env_debug_file, std::ios::out | std::ios::app);
            if (g_debugFile->is_open()) {
                g_debugOut = g_debugFile.get();
                spdlog::info("Benchmark debug logging enabled: {}", env_debug_file);
            } else {
                spdlog::warn("Failed to open YAMS_BENCH_DEBUG_FILE={} for writing", env_debug_file);
                g_debugFile.reset();
                g_debugOut = nullptr;
            }
        }

        const char* env_path = std::getenv("YAMS_BENCH_DATASET_PATH");
        const char* env_size = std::getenv("YAMS_BENCH_CORPUS_SIZE");
        const char* env_queries = std::getenv("YAMS_BENCH_NUM_QUERIES");
        const char* env_topk = std::getenv("YAMS_BENCH_TOPK");

        if (env_dataset) {
            std::string datasetName = env_dataset;
            // Supported BEIR datasets from
            // https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/
            static const std::set<std::string> SUPPORTED_BEIR_DATASETS = {
                "scifact",        "nfcorpus",    "arguana",    "scidocs", "fiqa",
                "quora",          "cqadupstack", "hotpotqa",   "fever",   "climate-fever",
                "dbpedia-entity", "trec-covid",  "touche-2020"};
            if (SUPPORTED_BEIR_DATASETS.count(datasetName) > 0) {
                useBEIR = true;
                beirDatasetName = datasetName;
                // Set SCIENTIFIC tuning override for BEIR datasets
                // This is needed because the auto-detection uses absolute path depth
                // which fails for temp directories (pathDepthAvg >> 1.5)
                setenv("YAMS_ENABLE_ENV_OVERRIDES", "1", 1);
                setenv("YAMS_TUNING_OVERRIDE", "SCIENTIFIC", 1);
                spdlog::info("Set YAMS_TUNING_OVERRIDE=SCIENTIFIC for BEIR benchmark ({})",
                             datasetName);
            } else {
                spdlog::warn("Unknown dataset '{}', using synthetic. Supported: scifact, "
                             "cqadupstack, nfcorpus, etc.",
                             datasetName);
            }
        }

        if (env_size)
            corpusSize = std::stoi(env_size);
        if (env_queries)
            numQueries = std::stoi(env_queries);
        if (env_topk)
            topK = std::stoi(env_topk);

        spdlog::info("Setting up RAG benchmark: {} dataset, {} docs, {} queries, k={}",
                     useBEIR ? beirDatasetName + " BEIR" : "synthetic", corpusSize, numQueries,
                     topK);

        // PBI-05b: Create summary log file for important embedding metrics
        fs::path summaryLogPath = fs::temp_directory_path() / "yams_bench_summary.log";
        std::ofstream summaryLog(summaryLogPath, std::ios::trunc);
        if (summaryLog) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            summaryLog << "=== YAMS Retrieval Quality Benchmark ===" << std::endl;
            summaryLog << "Started: " << std::ctime(&time_t_now);
            summaryLog << "Dataset: " << (useBEIR ? beirDatasetName + " BEIR" : "synthetic")
                       << std::endl;
            summaryLog << "Corpus size: " << corpusSize << std::endl;
            summaryLog << "Num queries: " << numQueries << std::endl;
            summaryLog << "Top K: " << topK << std::endl;
            summaryLog << std::endl;
            summaryLog.flush();
        }
        spdlog::info("Summary log: {}", summaryLogPath.string());

        const char* disableVectors = std::getenv("YAMS_DISABLE_VECTORS");
        bool vectorsDisabled = disableVectors && std::string(disableVectors) == "1";

        DaemonHarness::Options harnessOptions;
        harnessOptions.isolateState = true;
        harnessOptions.useMockModelProvider = vectorsDisabled;
        harnessOptions.autoLoadPlugins = !vectorsDisabled;
        harnessOptions.configureModelPool = !vectorsDisabled;
        harnessOptions.modelPoolLazyLoading = false;

        if (!vectorsDisabled) {
            const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR");
            if (envPluginDir) {
                harnessOptions.pluginDir = fs::path(envPluginDir);
            } else {
                harnessOptions.pluginDir = fs::current_path() / "builddir" / "plugins";
            }
            // Use system config for model settings - no hardcoded overrides
            // The benchmark will use whatever preferred_model is set in ~/.config/yams/config.toml
            spdlog::info("Using system config for embedding model (no benchmark override)");

            // Configure Glint plugin with GLiNER model path for NL entity extraction
            std::string glinerModelPath = discoverGlinerModelPath();
            if (!glinerModelPath.empty()) {
                json glintConfig;
                glintConfig["model_path"] = glinerModelPath;
                glintConfig["threshold"] = 0.5; // Default confidence threshold
                harnessOptions.pluginConfigs["glint"] = glintConfig.dump();
                spdlog::info("Configured Glint plugin with model: {}", glinerModelPath);
            }
        } else {
            spdlog::info("Using mock model provider (YAMS_DISABLE_VECTORS=1)");
        }

        harness = std::make_unique<DaemonHarness>(harnessOptions);
        if (!harness->start(std::chrono::seconds(30)))
            throw std::runtime_error("Failed to start daemon");

        if (useBEIR) {
            fs::path cachePath = env_path ? fs::path(env_path) : fs::path();
            Result<BEIRDataset> dsResult;
            if (beirDatasetName == "scifact") {
                dsResult = loadSciFactDataset(cachePath);
            } else {
                dsResult = loadBEIRDataset(beirDatasetName, cachePath);
            }
            if (!dsResult) {
                throw std::runtime_error("Failed to load BEIR dataset: " +
                                         dsResult.error().message);
            }
            BEIRDataset fullDataset = dsResult.value();

            // Apply corpus size limit if specified via env var
            // IMPORTANT: We must include documents that are referenced by qrels,
            // otherwise we'll have queries with no relevant documents to find!
            BEIRDataset dataset;
            dataset.name = fullDataset.name;
            dataset.basePath = fullDataset.basePath;

            int maxDocs = env_size ? corpusSize : (int)fullDataset.documents.size();
            int maxQueries = env_queries ? numQueries : (int)fullDataset.queries.size();

            // Step 1: Collect all document IDs referenced by qrels (these are "relevant" docs)
            std::set<std::string> qrelDocIds;
            for (const auto& [queryId, docScore] : fullDataset.qrels) {
                qrelDocIds.insert(docScore.first);
            }
            spdlog::info("BEIR dataset has {} documents referenced by qrels", qrelDocIds.size());

            // Step 2: First include documents that are referenced by qrels (up to maxDocs)
            std::set<std::string> includedDocIds;
            for (const auto& docId : qrelDocIds) {
                if ((int)includedDocIds.size() >= maxDocs)
                    break;
                if (fullDataset.documents.count(docId) > 0) {
                    dataset.documents[docId] = fullDataset.documents.at(docId);
                    includedDocIds.insert(docId);
                }
            }
            spdlog::info("Included {} qrel-referenced documents", includedDocIds.size());

            // Step 3: Fill remaining slots with other documents (for realistic corpus size)
            for (const auto& [id, doc] : fullDataset.documents) {
                if ((int)includedDocIds.size() >= maxDocs)
                    break;
                if (includedDocIds.count(id) == 0) {
                    dataset.documents[id] = doc;
                    includedDocIds.insert(id);
                }
            }

            // Step 4: Select queries that have qrels for documents we included
            int queryCount = 0;
            for (const auto& [qid, query] : fullDataset.queries) {
                if (queryCount >= maxQueries)
                    break;
                // Check if this query has at least one relevant doc in our subset
                auto range = fullDataset.qrels.equal_range(qid);
                bool hasRelevantDoc = false;
                for (auto it = range.first; it != range.second; ++it) {
                    if (includedDocIds.count(it->second.first) > 0) {
                        hasRelevantDoc = true;
                        break;
                    }
                }
                if (hasRelevantDoc) {
                    dataset.queries[qid] = query;
                    // Copy qrels for this query
                    for (auto it = range.first; it != range.second; ++it) {
                        if (includedDocIds.count(it->second.first) > 0) {
                            dataset.qrels.emplace(qid, it->second);
                        }
                    }
                    queryCount++;
                }
            }

            spdlog::debug(
                "Limited BEIR dataset to {} docs, {} queries, {} qrels (from {} docs, {} queries)",
                dataset.documents.size(), dataset.queries.size(), dataset.qrels.size(),
                fullDataset.documents.size(), fullDataset.queries.size());

            fs::path corpusDir = harness->rootDir() / "corpus";
            beirCorpus = std::make_unique<BEIRCorpusLoader>(dataset, corpusDir);
            beirCorpus->writeDocumentsAsFiles();
            benchCorpusDir = corpusDir;
            corpusSize = dataset.documents.size();
            numQueries = dataset.queries.size();
        } else {
            benchCorpusDir = harness->rootDir() / "corpus";
            corpus = std::make_unique<CorpusGenerator>(benchCorpusDir);
            corpus->generateDocuments(corpusSize);
        }

        yams::daemon::ClientConfig clientCfg;
        // Prefer proxy socket for long-running benchmark connections; it skips
        // maxConnectionLifetime enforcement (main socket forces close at ~300s).
        {
            fs::path proxySock = harness->socketPath().parent_path() / "proxy.sock";
            if (fs::exists(proxySock)) {
                clientCfg.socketPath = proxySock;
                spdlog::info("Using proxy socket for benchmark client: {}", proxySock.string());
            } else {
                clientCfg.socketPath = harness->socketPath();
                spdlog::info("Proxy socket not found; using main socket: {}",
                             clientCfg.socketPath.string());
            }
        }
        clientCfg.connectTimeout = 5s;
        clientCfg.requestTimeout = 300s; // 5 minutes for bulk ingestion
        clientCfg.autoStart = false;
        client = std::make_unique<yams::daemon::DaemonClient>(clientCfg);

        auto connectResult = yams::cli::run_sync(client->connect(), 5s);
        if (!connectResult)
            throw std::runtime_error("Failed to connect: " + connectResult.error().message);

        // Use directory ingestion for faster bulk add via IndexingService
        fs::path corpusDir = useBEIR ? beirCorpus->corpusDir : corpus->corpusDir;
        spdlog::info("Ingesting {} documents from {}...", corpusSize, corpusDir.string());

        yams::daemon::AddDocumentRequest addReq;
        addReq.path = corpusDir.string();
        addReq.recursive = true;
        addReq.noEmbeddings = false;
        addReq.includePatterns = {"*.txt"};

        auto addResult = yams::cli::run_sync(client->streamingAddDocument(addReq), 120s);
        if (!addResult) {
            throw std::runtime_error("Failed to ingest directory: " + addResult.error().message);
        }
        spdlog::info("Directory ingestion request accepted: {}", addResult.value().message);

        // Wait for directory ingestion to complete by monitoring document count
        // Use progress-based timeout: only timeout if no progress for N seconds
        // This is more robust than a hard deadline because slow ingestion won't fail,
        // but true hangs will still be detected.
        auto progressTimeoutSec = std::chrono::seconds(
            300); // 5 minutes without progress = stalled (embedding/model load)
        if (const char* env = std::getenv("YAMS_BENCH_PROGRESS_TIMEOUT")) {
            progressTimeoutSec = std::chrono::seconds(std::stoi(env));
        }
        spdlog::info("Waiting for ingestion to complete (expecting {} documents, "
                     "progress timeout {}s)...",
                     corpusSize, progressTimeoutSec.count());

        auto lastProgressTime = std::chrono::steady_clock::now();
        auto ingestHeartbeatAt = lastProgressTime + std::chrono::seconds(5);
        uint64_t lastDocCount = 0;
        uint64_t lastIndexedDocCount = 0;
        uint64_t lastContentExtracted = 0;
        uint64_t lastPostProcessed = 0;
        uint64_t lastEmbedQueued = 0;
        uint64_t lastEmbedInFlight = 0;
        uint64_t lastVectorCount = 0;
        uint32_t lastDepth = 0;
        bool completed = false;
        int stableChecks = 0;

        // Helper to get metric from requestCounts with presence flag
        auto getMetric = [](const auto& counts,
                            const std::string& key) -> std::pair<uint64_t, bool> {
            auto it = counts.find(key);
            if (it == counts.end())
                return {0, false};
            return {it->second, true};
        };

        uint64_t lastKgInFlight = 0, lastSymbolInFlight = 0, lastEntityInFlight = 0;
        uint64_t lastExtractionInFlight = 0;

        while (true) {
            auto now = std::chrono::steady_clock::now();
            auto timeSinceProgress =
                std::chrono::duration_cast<std::chrono::seconds>(now - lastProgressTime);

            if (now >= ingestHeartbeatAt) {
                ingestHeartbeatAt = now + std::chrono::seconds(5);
                spdlog::info("Ingest wait heartbeat: since_progress={}s last_total={} "
                             "last_indexed={} last_queue={} "
                             "last_processed={}",
                             timeSinceProgress.count(), lastDocCount, lastIndexedDocCount,
                             lastDepth, lastPostProcessed);
            }

            // Check for stall (no progress for progressTimeoutSec)
            if (timeSinceProgress > progressTimeoutSec) {
                spdlog::error(
                    "Ingestion stalled - no progress for {}s (docs: total={} indexed={} target={} "
                    "queue={}, inflight: extract={} kg={} symbol={} entity={} extracted={} "
                    "processed={} embed_queue={} embed_in_flight={} vectors={})",
                    timeSinceProgress.count(), lastDocCount, lastIndexedDocCount, corpusSize,
                    lastDepth, lastExtractionInFlight, lastKgInFlight, lastSymbolInFlight,
                    lastEntityInFlight, lastContentExtracted, lastPostProcessed, lastEmbedQueued,
                    lastEmbedInFlight, lastVectorCount);
                throw std::runtime_error("Ingestion stalled - benchmark results would be invalid. "
                                         "Set YAMS_BENCH_PROGRESS_TIMEOUT=300 for slower systems. "
                                         "For faster ingestion: YAMS_POST_EMBED_CONCURRENT=12 "
                                         "YAMS_POST_EXTRACTION_CONCURRENT=12 YAMS_DB_POOL_MAX=48");
            }

            auto statusResult = yams::cli::run_sync(client->status(), 5s);
            if (statusResult) {
                const auto& counts = statusResult.value().requestCounts;
                uint32_t depth = statusResult.value().postIngestQueueDepth;
                auto [docCount, docCountPresent] = getMetric(counts, "documents_total");
                auto [indexedCount, indexedPresent] = getMetric(counts, "documents_indexed");
                auto [contentExtracted, extractedPresent] =
                    getMetric(counts, "documents_content_extracted");
                auto [postQueued, postQueuedPresent] = getMetric(counts, "post_ingest_queued");
                auto [postInflight, postInflightPresent] =
                    getMetric(counts, "post_ingest_inflight");
                auto [postProcessed, postProcessedPresent] =
                    getMetric(counts, "post_ingest_processed");
                auto [embedQueued, embedQueuedPresent] = getMetric(counts, "embed_svc_queued");
                auto [embedInFlight, embedInFlightPresent] = getMetric(counts, "embed_in_flight");
                auto [vectorCount, vectorPresent] = getMetric(counts, "vector_count");
                uint64_t queuedTotal = std::max<uint64_t>(depth, postQueued);

                // Check all processing stage in-flight counters
                auto [extractionInFlight, extractionPresent] =
                    getMetric(counts, "extraction_inflight");
                auto [kgInFlight, kgPresent] = getMetric(counts, "kg_inflight");
                auto [symbolInFlight, symbolPresent] = getMetric(counts, "symbol_inflight");
                auto [entityInFlight, entityPresent] = getMetric(counts, "entity_inflight");

                // Total in-flight across all stages (matches PostIngestQueue::totalInFlight())
                uint64_t totalInFlight =
                    extractionInFlight + kgInFlight + symbolInFlight + entityInFlight;

                bool statusChanged =
                    (queuedTotal != lastDepth || docCount != lastDocCount ||
                     indexedCount != lastIndexedDocCount ||
                     contentExtracted != lastContentExtracted ||
                     postProcessed != lastPostProcessed || kgInFlight != lastKgInFlight ||
                     symbolInFlight != lastSymbolInFlight || entityInFlight != lastEntityInFlight ||
                     extractionInFlight != lastExtractionInFlight ||
                     embedQueued != lastEmbedQueued || embedInFlight != lastEmbedInFlight ||
                     vectorCount != lastVectorCount);

                if (statusChanged) {
                    bool vectorDbReady = statusResult.value().vectorDbReady;
                    if (auto it = statusResult.value().readinessStates.find("vector_db");
                        it != statusResult.value().readinessStates.end()) {
                        vectorDbReady = it->second;
                    }
                    spdlog::debug(
                        "Documents: total={} indexed={} / {} | queue={} inflight={} | extracted={} "
                        "processed={} | extract={} kg={} symbol={} entity={} (total={}) | "
                        "embed_queue={} embed_in_flight={} vectors={} (ready={})",
                        docCount, indexedCount, corpusSize, queuedTotal, postInflight,
                        contentExtracted, postProcessed, extractionInFlight, kgInFlight,
                        symbolInFlight, entityInFlight, totalInFlight, embedQueued, embedInFlight,
                        vectorCount, (vectorDbReady ? "true" : "false"));
                    lastDepth = queuedTotal;
                    lastDocCount = docCount;
                    lastIndexedDocCount = indexedCount;
                    lastContentExtracted = contentExtracted;
                    lastPostProcessed = postProcessed;
                    lastEmbedQueued = embedQueued;
                    lastEmbedInFlight = embedInFlight;
                    lastVectorCount = vectorCount;
                    lastKgInFlight = kgInFlight;
                    lastSymbolInFlight = symbolInFlight;
                    lastEntityInFlight = entityInFlight;
                    lastExtractionInFlight = extractionInFlight;
                    stableChecks = 0;
                    lastProgressTime = now; // Reset progress timer on any change
                } else {
                    stableChecks++;
                }

                // Complete when ALL stages are idle:
                // - post_ingest_queued == 0 AND post_ingest_inflight == 0 (queue empty)
                // - totalInFlight == 0 (all stages: extraction, KG, symbol, entity)
                // - indexedCount >= corpusSize (or stable for 5+ seconds after reaching target)
                bool allStagesDrained =
                    (queuedTotal == 0 && postInflight == 0 && totalInFlight == 0);
                bool indexedReady = indexedPresent
                                        ? (indexedCount >= static_cast<uint64_t>(corpusSize))
                                        : (docCount >= static_cast<uint64_t>(corpusSize));
                bool extractedReady = extractedPresent
                                          ? (contentExtracted >= static_cast<uint64_t>(corpusSize))
                                          : true;
                bool processedReady = postProcessedPresent
                                          ? (postProcessed >= static_cast<uint64_t>(corpusSize))
                                          : true;
                bool stableAfterDrain = (stableChecks >= 6);
                if (allStagesDrained && indexedReady &&
                    (extractedReady || processedReady || stableAfterDrain)) {
                    spdlog::info("Ingestion complete: total={} indexed={} (target={}), all stages "
                                 "drained (extracted={}, processed={})",
                                 docCount, indexedCount, corpusSize, contentExtracted,
                                 postProcessed);
                    completed = true;
                    break;
                }
            } else {
                if (now >= ingestHeartbeatAt - std::chrono::seconds(5)) {
                    spdlog::warn("Status polling failed during ingest wait: {}",
                                 statusResult.error().message);
                }
            }
            std::this_thread::sleep_for(500ms);
        }

        // Wait for embedding provider to become available
        spdlog::info("Waiting for embedding provider to become available...");
        auto deadline = std::chrono::steady_clock::now() + 30s;
        bool embeddingReady = false;
        while (std::chrono::steady_clock::now() < deadline) {
            auto statusResult = yams::cli::run_sync(client->status(), 5s);
            if (statusResult && statusResult.value().embeddingAvailable) {
                spdlog::info("Embedding provider is available (backend: {}, model: {})",
                             statusResult.value().embeddingBackend,
                             statusResult.value().embeddingModel);
                embeddingReady = true;
                break;
            }
            std::this_thread::sleep_for(500ms);
        }
        if (!embeddingReady) {
            spdlog::warn("Embedding provider did not become available within 30s");
        }

        // Wait for embeddings to be generated by checking vector count
        // The embedding service processes documents asynchronously after FTS5 indexing
        // Vector count is exposed via requestCounts["vector_count"] in StatusResponse
        bool vectorsEnabled = !vectorsDisabled;

        if (vectorsEnabled) {
            spdlog::info("Waiting for embeddings to be generated (target: {} docs)...", corpusSize);

            auto embedHeartbeatAt = std::chrono::steady_clock::now() + std::chrono::seconds(5);

            // Log to summary file
            if (summaryLog) {
                summaryLog << "=== Embedding Generation ===" << std::endl;
                summaryLog << "Target: " << corpusSize << " documents" << std::endl;
            }

            deadline =
                std::chrono::steady_clock::now() + 1800s; // 30 minute timeout for large corpus
            uint64_t lastVectorCount = 0;
            int stableCount = 0;
            uint64_t embedDropped = 0;
            auto embedStartTime = std::chrono::steady_clock::now();

            // Phase 1: Wait for embedding queue to drain and vectors to reach target
            // We need vectors >= corpusSize AND queue/in-flight drained for complete coverage
            // Note: Each document may produce multiple chunk vectors, so we wait for stability
            while (std::chrono::steady_clock::now() < deadline) {
                auto now = std::chrono::steady_clock::now();
                if (now >= embedHeartbeatAt) {
                    embedHeartbeatAt = now + std::chrono::seconds(5);
                    spdlog::info("Embed wait heartbeat: polling daemon status");
                }
                auto statusResult = yams::cli::run_sync(client->status(), 5s);
                if (statusResult) {
                    bool vectorDbReady = statusResult.value().vectorDbReady;
                    if (auto it = statusResult.value().readinessStates.find("vector_db");
                        it != statusResult.value().readinessStates.end()) {
                        vectorDbReady = it->second;
                    }
                    // Access vector count from requestCounts map
                    uint64_t vectorCount = 0;
                    auto it = statusResult.value().requestCounts.find("vector_count");
                    if (it != statusResult.value().requestCounts.end()) {
                        vectorCount = it->second;
                    }

                    // Check embed queue status (jobs waiting in EmbeddingService queue)
                    // NOTE: The key is "embed_svc_queued" in requestCounts (StatusResponse),
                    // NOT "bus_embed_queued" which is in additionalStats (GetStatsResponse)
                    uint64_t embedQueued = 0;
                    auto itQ = statusResult.value().requestCounts.find("embed_svc_queued");
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

                    const bool haveQueueMetrics =
                        (itQ != statusResult.value().requestCounts.end()) ||
                        (itInFlight != statusResult.value().requestCounts.end());

                    if (vectorCount != lastVectorCount) {
                        double coverage = corpusSize > 0 ? (vectorCount * 100.0 / corpusSize) : 0;
                        spdlog::info(
                            "Vectors: {} / {} ({:.1f}%) | queue={} in_flight={} dropped={}",
                            vectorCount, corpusSize, coverage, embedQueued, embedInFlight,
                            embedDropped);

                        // Log progress to summary file
                        if (summaryLog) {
                            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - embedStartTime)
                                               .count();
                            summaryLog << "[" << elapsed << "ms] Vectors: " << vectorCount << " / "
                                       << corpusSize << " (" << std::fixed << std::setprecision(1)
                                       << coverage << "%) | queue=" << embedQueued
                                       << " in_flight=" << embedInFlight
                                       << " dropped=" << embedDropped << std::endl;
                            summaryLog.flush();
                        }

                        lastVectorCount = vectorCount;
                        stableCount = 0;
                    } else {
                        stableCount++;
                    }

                    // Success condition: vectors >= corpusSize AND queue drained AND no in-flight
                    // This ensures all chunks are embedded before running queries
                    bool queueDrained = (embedQueued == 0 && embedInFlight == 0);
                    if (haveQueueMetrics && vectorCount >= static_cast<uint64_t>(corpusSize) &&
                        queueDrained && stableCount >= 10) {
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - embedStartTime)
                                           .count();
                        spdlog::info("Full embedding coverage achieved: {} vectors for {} docs "
                                     "(queue drained, stable for {}s, vector_db_ready={})",
                                     vectorCount, corpusSize, stableCount / 2,
                                     (vectorDbReady ? "true" : "false"));
                        if (summaryLog) {
                            summaryLog << "\n*** SUCCESS: Full coverage in " << elapsed << "ms ***"
                                       << std::endl;
                            summaryLog << "Final vectors: " << vectorCount << std::endl;
                            summaryLog << "Total dropped: " << embedDropped << std::endl;
                            summaryLog.flush();
                        }
                        break;
                    }

                    // If very stable (20s) with queue drained, we're done even if < corpusSize
                    // This handles cases where some docs have no content to embed
                    // IMPORTANT: Require minimum 90% coverage to prevent premature exit from
                    // transient queue drain (e.g., between embedding batches)
                    double coverage = corpusSize > 0 ? (vectorCount * 100.0 / corpusSize) : 0;
                    constexpr double MIN_COVERAGE_THRESHOLD = 90.0;
                    if (haveQueueMetrics && stableCount >= 40 && queueDrained && vectorCount > 0 &&
                        coverage >= MIN_COVERAGE_THRESHOLD) {
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - embedStartTime)
                                           .count();
                        spdlog::info("Embedding generation complete (queue drained, stable): "
                                     "{} vectors ({:.1f}% coverage, vector_db_ready={})",
                                     vectorCount, coverage, (vectorDbReady ? "true" : "false"));
                        if (summaryLog) {
                            summaryLog << "\n*** Complete (queue drained) in " << elapsed
                                       << "ms ***" << std::endl;
                            summaryLog << "Final vectors: " << vectorCount << " (" << coverage
                                       << "%)" << std::endl;
                            summaryLog.flush();
                        }
                        break;
                    }

                    // If stable but queue NOT drained, warn about potential issues
                    if (haveQueueMetrics && stableCount >= 20 && !queueDrained && vectorCount > 0) {
                        // coverage already computed above
                        spdlog::warn("Embedding stalled at {:.1f}% but queue not drained "
                                     "(queue={}, in_flight={}). Continuing to wait...",
                                     coverage, embedQueued, embedInFlight);
                        // Don't break - keep waiting for queue to drain
                    }
                }
                std::this_thread::sleep_for(500ms);
            }

            // Final status check
            auto finalStatus = yams::cli::run_sync(client->status(), 5s);
            if (finalStatus) {
                uint64_t finalVectorCount = 0;
                auto it = finalStatus.value().requestCounts.find("vector_count");
                if (it != finalStatus.value().requestCounts.end()) {
                    finalVectorCount = it->second;
                }
                double finalCoverage = corpusSize > 0 ? (finalVectorCount * 100.0 / corpusSize) : 0;
                bool vectorDbReady = finalStatus.value().vectorDbReady;
                if (auto it = finalStatus.value().readinessStates.find("vector_db");
                    it != finalStatus.value().readinessStates.end()) {
                    vectorDbReady = it->second;
                }
                spdlog::info("Vector DB: ready={} dim={}, vectors={} ({:.1f}% coverage)",
                             (vectorDbReady ? "true" : "false"), finalStatus.value().vectorDbDim,
                             finalVectorCount, finalCoverage);

                if (finalVectorCount == 0) {
                    spdlog::warn("No vectors present after embedding wait");
                } else if (finalCoverage < 90.0) {
                    spdlog::error(
                        "WARNING: Low embedding coverage ({:.1f}%) will degrade retrieval quality!",
                        finalCoverage);
                    spdlog::error(
                        "Consider reducing corpus size or increasing embed channel capacity.");
                }

                if (!vectorDbReady) {
                    spdlog::warn("Vector DB not ready yet; waiting briefly before queries...");
                    const auto guardDeadline = std::chrono::steady_clock::now() + 30s;
                    while (std::chrono::steady_clock::now() < guardDeadline) {
                        auto st = yams::cli::run_sync(client->status(), 5s);
                        if (st) {
                            bool ready = st.value().vectorDbReady;
                            if (auto it = st.value().readinessStates.find("vector_db");
                                it != st.value().readinessStates.end()) {
                                ready = it->second;
                            }
                            if (ready) {
                                spdlog::info("Vector DB is now ready after guard wait");
                                break;
                            }
                        }
                        std::this_thread::sleep_for(500ms);
                    }
                }
            } else {
                spdlog::warn("Final status check failed after embedding wait: {}",
                             finalStatus.error().message);
            }
        } else {
            spdlog::info("Vectors disabled - skipping embedding generation wait");
        }

        // Wait for search engine to be ready (critical for hybrid search!)
        // Without this, all hybrid searches fall back to keyword-only
        spdlog::info("Waiting for search engine to be ready...");
        const auto searchEngineDeadline = std::chrono::steady_clock::now() + 60s;
        bool searchEngineReady = false;
        while (std::chrono::steady_clock::now() < searchEngineDeadline) {
            auto statusCheck = yams::cli::run_sync(client->status(), 5s);
            if (statusCheck) {
                auto it = statusCheck.value().readinessStates.find("search_engine");
                if (it != statusCheck.value().readinessStates.end() && it->second) {
                    searchEngineReady = true;
                    spdlog::info("Search engine is ready for hybrid search");
                    break;
                }
            }
            std::this_thread::sleep_for(500ms);
        }
        if (!searchEngineReady) {
            spdlog::error("Search engine not ready after 60s - hybrid searches will fall back to "
                          "keyword-only!");
            spdlog::error("This is likely due to search engine build not completing.");
        }

        // Verify document count using status metrics (avoids degraded search false negatives)
        uint64_t indexedDocCount = 0;
        auto statusResult = yams::cli::run_sync(client->status(), 5s);
        if (statusResult) {
            if (auto it = statusResult.value().requestCounts.find("documents_indexed");
                it != statusResult.value().requestCounts.end()) {
                indexedDocCount = it->second;
            } else if (auto it = statusResult.value().requestCounts.find("documents_total");
                       it != statusResult.value().requestCounts.end()) {
                indexedDocCount = it->second;
            }
        }

        if (indexedDocCount == 0) {
            yams::daemon::SearchRequest testReq;
            testReq.query = "test";
            testReq.searchType = "hybrid";
            testReq.limit = 1000;
            std::chrono::milliseconds queryTimeout{20000};
            if (const char* env = std::getenv("YAMS_BENCH_QUERY_TIMEOUT_MS")) {
                queryTimeout = std::chrono::milliseconds{
                    static_cast<std::chrono::milliseconds::rep>(std::stoll(env))};
            }
            testReq.timeout = queryTimeout;
            auto waitBudget = queryTimeout + std::chrono::seconds(10);
            if (waitBudget < std::chrono::seconds(10)) {
                waitBudget = std::chrono::seconds(10);
            }
            auto testResult = yams::cli::run_sync(client->search(testReq), waitBudget);
            indexedDocCount = testResult ? testResult.value().results.size() : 0;
        }

        spdlog::info("Verified indexed documents: {} (expected: {})", indexedDocCount, corpusSize);
        if (indexedDocCount == 0) {
            spdlog::error("NO DOCUMENTS IN INDEX! Ingestion failed completely.");
            throw std::runtime_error("No documents indexed - benchmark cannot proceed");
        }

        // Post-ingest status/stats snapshot + sanity searches.
        // Goal: distinguish "no docs" vs "docs but query returns empty".
        {
            // Log StatusResponse (it already powers our ingestion waits).
            auto statusRes = yams::cli::run_sync(client->status(), 5s);
            if (statusRes) {
                const auto& st = statusRes.value();
                uint64_t documentsTotal = 0;
                uint64_t vectorCount = 0;
                if (auto it = st.requestCounts.find("documents_total");
                    it != st.requestCounts.end()) {
                    documentsTotal = it->second;
                }
                if (auto it = st.requestCounts.find("vector_count"); it != st.requestCounts.end()) {
                    vectorCount = it->second;
                }

                // Log search tuning state (epic yams-7ez4)
                if (!st.searchTuningState.empty()) {
                    spdlog::info("Search tuning state: {} ({})", st.searchTuningState,
                                 st.searchTuningReason);
                    spdlog::info("Search tuning params:");
                    for (const auto& [k, v] : st.searchTuningParams) {
                        spdlog::info("  {}: {:.4f}", k, v);
                    }
                }

                DebugLogEntry statusEntry;
                statusEntry.query = "__post_ingest_status__";
                statusEntry.returnedPaths.push_back("documents_total=" +
                                                    std::to_string(documentsTotal));
                statusEntry.returnedPaths.push_back("vector_count=" + std::to_string(vectorCount));
                statusEntry.returnedPaths.push_back("post_ingest_queue_depth=" +
                                                    std::to_string(st.postIngestQueueDepth));
                statusEntry.returnedPaths.push_back(std::string("embedding_available=") +
                                                    (st.embeddingAvailable ? "true" : "false"));
                bool vectorDbReady = st.vectorDbReady;
                if (auto it = st.readinessStates.find("vector_db");
                    it != st.readinessStates.end()) {
                    vectorDbReady = it->second;
                }
                statusEntry.returnedPaths.push_back(std::string("vector_db_ready=") +
                                                    (vectorDbReady ? "true" : "false"));
                statusEntry.returnedPaths.push_back("vector_db_dim=" +
                                                    std::to_string(st.vectorDbDim));
                statusEntry.returnedPaths.push_back("embedding_backend=" + st.embeddingBackend);
                statusEntry.returnedPaths.push_back("embedding_model=" + st.embeddingModel);
                // Search tuning state (epic yams-7ez4)
                statusEntry.returnedPaths.push_back("search_tuning_state=" + st.searchTuningState);
                statusEntry.returnedPaths.push_back("search_tuning_reason=" +
                                                    st.searchTuningReason);
                for (const auto& [k, v] : st.searchTuningParams) {
                    statusEntry.returnedPaths.push_back("tuning_" + k + "=" + std::to_string(v));
                }
                debugLogWriteJsonLine(statusEntry);
            } else {
                spdlog::warn("Status failed (post-ingest): {}", statusRes.error().message);
            }

            // Log GetStatsResponse (extra counters);
            // we also emit structured JSON (additional_stats) for easier parsing.
            yams::daemon::GetStatsRequest statsReq;
            auto statsRes = yams::cli::run_sync(client->getStats(statsReq), 10s);
            if (statsRes) {
                const auto& st = statsRes.value();
                spdlog::info("GetStats: totalDocuments={} indexedDocuments={} vectorIndexSize={} "
                             "additionalStats.size={}",
                             st.totalDocuments, st.indexedDocuments, st.vectorIndexSize,
                             st.additionalStats.size());

                DebugLogEntry statsEntry;
                statsEntry.query = "__post_ingest_stats__";
                statsEntry.returnedPaths.push_back("totalDocuments=" +
                                                   std::to_string(st.totalDocuments));
                statsEntry.returnedPaths.push_back("indexedDocuments=" +
                                                   std::to_string(st.indexedDocuments));
                statsEntry.returnedPaths.push_back("vectorIndexSize=" +
                                                   std::to_string(st.vectorIndexSize));

                std::lock_guard<std::mutex> lock(g_debugMutex);
                if (g_debugOut) {
                    json j;
                    j["query"] = statsEntry.query;
                    j["relevant_doc_ids"] = json::array();
                    j["relevant_files"] = json::array();
                    j["returned_paths"] = statsEntry.returnedPaths;
                    j["returned_doc_ids"] = json::array();
                    j["returned_scores"] = json::array();
                    j["returned_grades"] = json::array();
                    j["attempts"] = 0;
                    j["used_streaming"] = false;
                    j["used_fuzzy_retry"] = false;
                    j["used_literal_retry"] = false;
                    j["additional_stats"] = st.additionalStats;
                    j["total_documents"] = st.totalDocuments;
                    j["indexed_documents"] = st.indexedDocuments;
                    j["vector_index_size"] = st.vectorIndexSize;

                    for (const auto& [k, v] : st.additionalStats) {
                        statsEntry.returnedPaths.push_back(k + "=" + v);
                    }
                    j["returned_paths"] = statsEntry.returnedPaths;
                    (*g_debugOut) << j.dump() << "\n";
                    g_debugOut->flush();
                }
            } else {
                spdlog::warn("GetStats failed: {}", statsRes.error().message);
            }

            auto doSanitySearch = [&](std::string label, std::string query, std::string searchType,
                                      bool symbolRank) {
                yams::cli::search_runner::DaemonSearchOptions opts;
                opts.query = std::move(query);
                opts.searchType = std::move(searchType);
                opts.limit = 25;
                std::chrono::milliseconds queryTimeout{20000};
                if (const char* env = std::getenv("YAMS_BENCH_QUERY_TIMEOUT_MS")) {
                    queryTimeout = std::chrono::milliseconds{
                        static_cast<std::chrono::milliseconds::rep>(std::stoll(env))};
                }
                opts.timeout = queryTimeout;
                opts.symbolRank = symbolRank;

                DebugLogEntry sanityEntry;
                sanityEntry.query = std::move(label);

                auto waitBudget =
                    std::chrono::duration_cast<std::chrono::milliseconds>(opts.timeout) +
                    std::chrono::seconds(10);
                if (waitBudget < std::chrono::seconds(10)) {
                    waitBudget = std::chrono::seconds(10);
                }
                auto run = yams::cli::run_sync(
                    yams::cli::search_runner::daemon_search(*client, opts, true), waitBudget);
                if (!run) {
                    sanityEntry.returnedPaths.push_back("error=" + run.error().message);
                    debugLogWriteJsonLine(sanityEntry);
                    return;
                }

                sanityEntry.attempts = run.value().attempts;
                sanityEntry.usedStreaming = run.value().usedStreaming;
                sanityEntry.usedFuzzyRetry = run.value().usedFuzzyRetry;
                sanityEntry.usedLiteralTextRetry = run.value().usedLiteralTextRetry;

                const auto& results = run.value().response.results;
                for (size_t i = 0; i < std::min<size_t>(results.size(), 10); ++i) {
                    sanityEntry.returnedPaths.push_back(results[i].path);
                }

                sanityEntry.returnedDocIds.push_back("result_count=" +
                                                     std::to_string(results.size()));
                debugLogWriteJsonLine(sanityEntry);
            };

            // Keyword search should return something if FTS is populated.
            doSanitySearch("__sanity_keyword_the__", "the", "keyword", false);

            // Hybrid search without symbol rank to avoid symbol-only decisions.
            doSanitySearch("__sanity_hybrid_the__", "the", "hybrid", false);

            // If BEIR corpus is used, easily-searchable token is doc id.
            if (useBEIR && beirCorpus && !beirCorpus->dataset.documents.empty()) {
                const auto& firstDocId = beirCorpus->dataset.documents.begin()->first;
                doSanitySearch("__sanity_keyword_docid__", firstDocId, "keyword", false);
            }
        }

        if (useBEIR) {
            queries = beirCorpus->generateTestQueries();
        } else {
            queries = corpus->generateQueries(numQueries);
        }
        spdlog::info("Generated {} test queries", queries.size());
    }

    void teardown() {
        client.reset();
        harness.reset();
        corpus.reset();

        g_debugOut = nullptr;
        g_debugFile.reset();
    }
};

static std::unique_ptr<BenchFixture> g_fixture;
void SetupFixture() {
    if (!g_fixture) {
        g_fixture = std::make_unique<BenchFixture>();
        g_fixture->setup();
    }
}
void CleanupFixture() {
    if (g_fixture) {
        g_fixture->teardown();
        g_fixture.reset();
    }
}

void BM_RetrievalQuality(benchmark::State& state) {
    SetupFixture();
    auto& fixture = *g_fixture;
    RetrievalMetrics metrics;
    for (auto _ : state) {
        metrics = evaluateQueries(*fixture.client, fixture.benchCorpusDir, fixture.queries,
                                  fixture.topK, "hybrid");
        benchmark::DoNotOptimize(metrics);
    }

    // Store metrics globally for post-teardown summary
    g_final_metrics = metrics;

    state.counters["MRR"] = metrics.mrr;
    state.counters["Recall@K"] = metrics.recallAtK;
    state.counters["Precision@K"] = metrics.precisionAtK;
    state.counters["nDCG@K"] = metrics.ndcgAtK;
    state.counters["MAP"] = metrics.map;
    state.counters["num_queries"] = metrics.numQueries;

    // Also evaluate keyword-only (FTS5) for component isolation
    // This helps diagnose whether low MRR is from FTS5 or vector search
    spdlog::info("=== Evaluating KEYWORD-ONLY (FTS5) for component isolation ===");
    g_keyword_metrics = evaluateQueries(*fixture.client, fixture.benchCorpusDir, fixture.queries,
                                        fixture.topK, "keyword");

    state.counters["MRR_keyword"] = g_keyword_metrics.mrr;
    state.counters["Recall_keyword"] = g_keyword_metrics.recallAtK;
}
// Retrieval quality is a long-running, end-to-end evaluation.
// Force a single iteration to avoid repeated full-corpus evaluations.
BENCHMARK(BM_RetrievalQuality)->Iterations(1);

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv))
        return 1;
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    CleanupFixture();

    // Re-print summary after all teardown logs
    // This ensures results are visible even when daemon teardown is verbose
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "              RETRIEVAL QUALITY BENCHMARK RESULTS\n";
    std::cout << std::string(70, '=') << "\n";
    std::cout << std::fixed << std::setprecision(4);

    // Hybrid search results
    std::cout << "\n  --- HYBRID SEARCH (text + vector) ---\n";
    std::cout << "  MRR (Mean Reciprocal Rank):     " << std::setw(10) << g_final_metrics.mrr
              << "\n";
    std::cout << "  Recall@K:                       " << std::setw(10) << g_final_metrics.recallAtK
              << "\n";
    std::cout << "  Precision@K:                    " << std::setw(10)
              << g_final_metrics.precisionAtK << "\n";
    std::cout << "  nDCG@K (Normalized DCG):        " << std::setw(10) << g_final_metrics.ndcgAtK
              << "\n";
    std::cout << "  MAP (Mean Average Precision):   " << std::setw(10) << g_final_metrics.map
              << "\n";

    // Keyword-only search results (FTS5 isolation)
    std::cout << "\n  --- KEYWORD SEARCH (FTS5 only) ---\n";
    std::cout << "  MRR (Mean Reciprocal Rank):     " << std::setw(10) << g_keyword_metrics.mrr
              << "\n";
    std::cout << "  Recall@K:                       " << std::setw(10)
              << g_keyword_metrics.recallAtK << "\n";
    std::cout << "  Precision@K:                    " << std::setw(10)
              << g_keyword_metrics.precisionAtK << "\n";
    std::cout << "  nDCG@K (Normalized DCG):        " << std::setw(10) << g_keyword_metrics.ndcgAtK
              << "\n";
    std::cout << "  MAP (Mean Average Precision):   " << std::setw(10) << g_keyword_metrics.map
              << "\n";

    // Component comparison summary
    std::cout << "\n  --- COMPONENT COMPARISON ---\n";
    std::cout << "  Number of queries evaluated:    " << std::setw(10) << g_final_metrics.numQueries
              << "\n";
    double mrrDelta = g_final_metrics.mrr - g_keyword_metrics.mrr;
    std::cout << "  MRR delta (hybrid - keyword):   " << std::setw(10) << mrrDelta
              << (mrrDelta > 0 ? " (hybrid better)" : " (keyword better)") << "\n";
    std::cout << std::string(70, '=') << "\n";

    return 0;
}
