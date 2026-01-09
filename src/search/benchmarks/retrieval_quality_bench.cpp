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
    YAMS_BENCH_DATASET=scifact         - Use BEIR dataset (default: synthetic)
    YAMS_BENCH_DATASET_PATH=...       - Path to dataset directory
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
                                 const std::vector<TestQuery>& queries, int k) {
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
        opts.searchType = "hybrid";
        opts.limit = static_cast<std::size_t>(k);
        opts.timeout = 5s;
        opts.symbolRank = true;
        const bool benchDiagEnabled = []() -> bool {
            if (const char* env = std::getenv("YAMS_BENCH_DIAG"); env && std::string(env) == "1") {
                return true;
            }
            return false;
        }();

        spdlog::info("Executing search query: '{}'", tq.query);
        auto run =
            yams::cli::run_sync(yams::cli::search_runner::daemon_search(client, opts, true), 10s);
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
                    yams::cli::search_runner::daemon_search(client, shadow, true), 10s);
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
            if (datasetName == "scifact") {
                useBEIR = true;
            }
        }

        if (env_size)
            corpusSize = std::stoi(env_size);
        if (env_queries)
            numQueries = std::stoi(env_queries);
        if (env_topk)
            topK = std::stoi(env_topk);

        spdlog::info("Setting up RAG benchmark: {} dataset, {} docs, {} queries, k={}",
                     useBEIR ? "SciFact BEIR" : "synthetic", corpusSize, numQueries, topK);

        // PBI-05b: Create summary log file for important embedding metrics
        fs::path summaryLogPath = fs::temp_directory_path() / "yams_bench_summary.log";
        std::ofstream summaryLog(summaryLogPath, std::ios::trunc);
        if (summaryLog) {
            auto now = std::chrono::system_clock::now();
            auto time_t_now = std::chrono::system_clock::to_time_t(now);
            summaryLog << "=== YAMS Retrieval Quality Benchmark ===" << std::endl;
            summaryLog << "Started: " << std::ctime(&time_t_now);
            summaryLog << "Dataset: " << (useBEIR ? "SciFact BEIR" : "synthetic") << std::endl;
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
            harnessOptions.preloadModels = {"nomic-embed-text-v1.5"};

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
            auto dsResult = loadSciFactDataset(cachePath);
            if (!dsResult) {
                throw std::runtime_error("Failed to load BEIR dataset: " +
                                         dsResult.error().message);
            }
            BEIRDataset fullDataset = dsResult.value();

            // Apply corpus size limit if specified via env var
            BEIRDataset dataset;
            dataset.name = fullDataset.name;
            dataset.basePath = fullDataset.basePath;

            int maxDocs = env_size ? corpusSize : (int)fullDataset.documents.size();
            int maxQueries = env_queries ? numQueries : (int)fullDataset.queries.size();

            // Copy limited documents
            int docCount = 0;
            for (const auto& [id, doc] : fullDataset.documents) {
                if (docCount >= maxDocs)
                    break;
                dataset.documents[id] = doc;
                docCount++;
            }

            // Copy queries that have qrels for documents we included
            std::set<std::string> includedDocIds;
            for (const auto& [id, doc] : dataset.documents) {
                includedDocIds.insert(id);
            }

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

            spdlog::info("Limited BEIR dataset to {} docs, {} queries (from {} docs, {} queries)",
                         dataset.documents.size(), dataset.queries.size(),
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
        clientCfg.socketPath = harness->socketPath();
        clientCfg.connectTimeout = 5s;
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
        spdlog::info("Waiting for ingestion to complete (expecting {} documents)...", corpusSize);
        auto deadline =
            std::chrono::steady_clock::now() + 120s; // 2 minute timeout for bulk ingestion
        uint64_t lastDocCount = 0;
        uint32_t lastDepth = 0;
        bool completed = false;
        int stableChecks = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            auto statusResult = yams::cli::run_sync(client->status(), 5s);
            if (statusResult) {
                uint32_t depth = statusResult.value().postIngestQueueDepth;
                uint64_t docCount = 0;
                auto it = statusResult.value().requestCounts.find("documents_total");
                if (it != statusResult.value().requestCounts.end()) {
                    docCount = it->second;
                }

                if (depth != lastDepth || docCount != lastDocCount) {
                    spdlog::info("Documents: {} / {}, queue depth: {}", docCount, corpusSize,
                                 depth);
                    lastDepth = depth;
                    lastDocCount = docCount;
                    stableChecks = 0;
                } else {
                    stableChecks++;
                }

                // Complete when queue is empty AND we have expected doc count (or stable for 5s)
                if (depth == 0 && (docCount >= (uint64_t)corpusSize || stableChecks >= 10)) {
                    spdlog::info("Ingestion complete: {} documents indexed", docCount);
                    completed = true;
                    break;
                }
            }
            std::this_thread::sleep_for(500ms);
        }
        if (!completed) {
            spdlog::warn("Ingestion did not complete within timeout (docs: {}, queue: {})",
                         lastDocCount, lastDepth);
        }

        // Wait for embedding provider to become available
        spdlog::info("Waiting for embedding provider to become available...");
        deadline = std::chrono::steady_clock::now() + 30s;
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
            // We need vectors >= corpusSize for complete coverage
            while (std::chrono::steady_clock::now() < deadline) {
                auto statusResult = yams::cli::run_sync(client->status(), 5s);
                if (statusResult) {
                    // Access vector count from requestCounts map
                    uint64_t vectorCount = 0;
                    auto it = statusResult.value().requestCounts.find("vector_count");
                    if (it != statusResult.value().requestCounts.end()) {
                        vectorCount = it->second;
                    }

                    // Check embed queue status
                    uint64_t embedQueued = 0;
                    uint64_t embedInFlight = 0;
                    auto itQ = statusResult.value().requestCounts.find("bus_embed_queued");
                    if (itQ != statusResult.value().requestCounts.end()) {
                        embedQueued = itQ->second;
                    }
                    auto itD = statusResult.value().requestCounts.find("bus_embed_dropped");
                    if (itD != statusResult.value().requestCounts.end()) {
                        embedDropped = itD->second;
                    }

                    if (vectorCount != lastVectorCount) {
                        double coverage = corpusSize > 0 ? (vectorCount * 100.0 / corpusSize) : 0;
                        spdlog::info("Vectors: {} / {} ({:.1f}%) | queue={} dropped={}",
                                     vectorCount, corpusSize, coverage, embedQueued, embedDropped);

                        // Log progress to summary file
                        if (summaryLog) {
                            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - embedStartTime)
                                               .count();
                            summaryLog << "[" << elapsed << "ms] Vectors: " << vectorCount << " / "
                                       << corpusSize << " (" << std::fixed << std::setprecision(1)
                                       << coverage << "%) | queue=" << embedQueued
                                       << " dropped=" << embedDropped << std::endl;
                            summaryLog.flush();
                        }

                        lastVectorCount = vectorCount;
                        stableCount = 0;
                    } else {
                        stableCount++;
                    }

                    // Success condition: vectors >= corpusSize (100% coverage)
                    if (vectorCount >= corpusSize) {
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - embedStartTime)
                                           .count();
                        spdlog::info("Full embedding coverage achieved: {} vectors for {} docs",
                                     vectorCount, corpusSize);
                        if (summaryLog) {
                            summaryLog << "\n*** SUCCESS: Full coverage in " << elapsed << "ms ***"
                                       << std::endl;
                            summaryLog << "Final vectors: " << vectorCount << std::endl;
                            summaryLog << "Total dropped: " << embedDropped << std::endl;
                            summaryLog.flush();
                        }
                        break;
                    }

                    // If stable and we have embeddings but not full coverage, check for dropped
                    if (stableCount >= 20 && vectorCount > 0) { // 10s of stability
                        double coverage = corpusSize > 0 ? (vectorCount * 100.0 / corpusSize) : 0;
                        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - embedStartTime)
                                           .count();
                        if (embedDropped > 0) {
                            // PBI-05b: With parallel EmbeddingService, drops should be rare.
                            // If they occur, log a warning and proceed (repair is background-only).
                            spdlog::warn("Embedding generation stalled at {:.1f}% coverage. "
                                         "{} jobs were dropped due to queue overflow. "
                                         "Proceeding with partial coverage - consider increasing "
                                         "embed channel capacity or reducing ingest rate.",
                                         coverage, embedDropped);
                            if (summaryLog) {
                                summaryLog << "\n*** WARNING: Stalled at " << coverage << "% after "
                                           << elapsed << "ms ***" << std::endl;
                                summaryLog << "Jobs dropped: " << embedDropped << std::endl;
                                summaryLog.flush();
                            }
                            break;
                        } else {
                            // No drops but still incomplete - might be legitimate (empty docs, etc)
                            spdlog::info("Embedding generation complete (no drops): {} vectors "
                                         "({:.1f}% coverage)",
                                         vectorCount, coverage);
                            if (summaryLog) {
                                summaryLog << "\n*** Complete (no drops) in " << elapsed << "ms ***"
                                           << std::endl;
                                summaryLog << "Final vectors: " << vectorCount << " (" << coverage
                                           << "%)" << std::endl;
                                summaryLog.flush();
                            }
                            break;
                        }
                    }
                }
                std::this_thread::sleep_for(500ms);
            }

            // Final status check
            auto finalStatus = yams::cli::run_sync(client->status(), 5s);
            if (finalStatus && finalStatus.value().vectorDbReady) {
                uint64_t finalVectorCount = 0;
                auto it = finalStatus.value().requestCounts.find("vector_count");
                if (it != finalStatus.value().requestCounts.end()) {
                    finalVectorCount = it->second;
                }
                double finalCoverage = corpusSize > 0 ? (finalVectorCount * 100.0 / corpusSize) : 0;
                spdlog::info("Vector DB ready: dim={}, vectors={} ({:.1f}% coverage)",
                             finalStatus.value().vectorDbDim, finalVectorCount, finalCoverage);

                if (finalCoverage < 90.0) {
                    spdlog::error(
                        "WARNING: Low embedding coverage ({:.1f}%) will degrade retrieval quality!",
                        finalCoverage);
                    spdlog::error(
                        "Consider reducing corpus size or increasing embed channel capacity.");
                }
            } else {
                spdlog::warn("Vector DB may not be ready or no embeddings generated");
            }
        } else {
            spdlog::info("Vectors disabled - skipping embedding generation wait");
        }

        // Verify document count using status metrics (avoids degraded search false negatives)
        uint64_t indexedDocCount = 0;
        auto statusResult = yams::cli::run_sync(client->status(), 5s);
        if (statusResult) {
            auto it = statusResult.value().requestCounts.find("documents_total");
            if (it != statusResult.value().requestCounts.end()) {
                indexedDocCount = it->second;
            }
        }

        if (indexedDocCount == 0) {
            yams::daemon::SearchRequest testReq;
            testReq.query = "test";
            testReq.searchType = "hybrid";
            testReq.limit = 1000;
            testReq.timeout = 5s;
            auto testResult = yams::cli::run_sync(client->search(testReq), 10s);
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
                statusEntry.returnedPaths.push_back(std::string("vector_db_ready=") +
                                                    (st.vectorDbReady ? "true" : "false"));
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
                opts.timeout = 5s;
                opts.symbolRank = symbolRank;

                DebugLogEntry sanityEntry;
                sanityEntry.query = std::move(label);

                auto run = yams::cli::run_sync(
                    yams::cli::search_runner::daemon_search(*client, opts, true), 10s);
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
    for (auto _ : state) {
        auto metrics =
            evaluateQueries(*fixture.client, fixture.benchCorpusDir, fixture.queries, fixture.topK);
        benchmark::DoNotOptimize(metrics);
    }
    auto metrics =
        evaluateQueries(*fixture.client, fixture.benchCorpusDir, fixture.queries, fixture.topK);

    // Store metrics globally for post-teardown summary
    g_final_metrics = metrics;

    state.counters["MRR"] = metrics.mrr;
    state.counters["Recall@K"] = metrics.recallAtK;
    state.counters["Precision@K"] = metrics.precisionAtK;
    state.counters["nDCG@K"] = metrics.ndcgAtK;
    state.counters["MAP"] = metrics.map;
    state.counters["num_queries"] = metrics.numQueries;
}
BENCHMARK(BM_RetrievalQuality);

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
    std::cout << "  Number of queries evaluated:    " << std::setw(10) << g_final_metrics.numQueries
              << "\n";
    std::cout << std::string(70, '=') << "\n";

    return 0;
}
