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
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/daemon.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <curl/curl.h>
#include <yams/compat/unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace yams;
using json = nlohmann::json;

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

static size_t writeCallback(void* contents, size_t size, size_t nmemb, void* userp) {
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

static Result<std::string> downloadFile(const std::string& url) {
    CURL* curl = curl_easy_init();
    if (!curl) {
        return Error{ErrorCode::IoError, "Failed to initialize libcurl"};
    }

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 300L);

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (res != CURLE_OK) {
        return Error{ErrorCode::NetworkError,
                     "Download failed: " + std::string(curl_easy_strerror(res))};
    }

    return response;
}

static Result<void> downloadSciFactDataset(const fs::path& cacheDir) {
    const char* home = std::getenv("HOME");
    if (!home) {
        return Error{ErrorCode::InvalidArgument, "HOME environment variable not set"};
    }

    fs::path scifactDir =
        cacheDir.empty() ? fs::path(home) / ".cache" / "yams" / "benchmarks" / "scifact" : cacheDir;

    fs::create_directories(scifactDir);

    std::vector<std::pair<std::string, std::string>> files = {
        {"https://raw.githubusercontent.com/beir-cellar/scifact/main/corpus.jsonl", "corpus.jsonl"},
        {"https://raw.githubusercontent.com/beir-cellar/scifact/main/queries.jsonl",
         "queries.jsonl"},
        {"https://raw.githubusercontent.com/beir-cellar/scifact/main/qrels/test.tsv", "qrels.tsv"}};

    for (const auto& [url, filename] : files) {
        fs::path filePath = scifactDir / filename;
        if (fs::exists(filePath)) {
            spdlog::info("Dataset file already exists: {}", filePath.string());
            continue;
        }

        spdlog::info("Downloading {} from {}", filename, url);
        auto result = downloadFile(url);
        if (!result) {
            return Error{result.error().code,
                         "Failed to download " + filename + ": " + result.error().message};
        }

        std::ofstream outFile(filePath);
        if (!outFile) {
            return Error{ErrorCode::IoError, "Failed to write " + filePath.string()};
        }
        outFile << result.value();
        outFile.close();

        spdlog::info("Downloaded {} ({} bytes)", filename, result.value().size());
    }

    spdlog::info("SciFact dataset downloaded to {}", scifactDir.string());
    return {};
}

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
    fs::path qrelsFile = scifactDir / "qrels" / "test.tsv";

    if (!fs::exists(corpusFile) || !fs::exists(queriesFile) || !fs::exists(qrelsFile)) {
        spdlog::info("SciFact dataset not found, downloading...");
        auto dlResult = downloadSciFactDataset(cacheDir);
        if (!dlResult) {
            return Error{dlResult.error().code, dlResult.error().message};
        }
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
    while (std::getline(qrelsIn, line)) {
        if (line.empty() || line[0] == '#')
            continue;
        std::istringstream iss(line);
        std::string queryId, iteration, docId, scoreStr;
        if (std::getline(iss, queryId, '\t') && std::getline(iss, iteration, '\t') &&
            std::getline(iss, docId, '\t') && std::getline(iss, scoreStr, '\t')) {
            int score = std::stoi(scoreStr);
            dataset.qrels.emplace(queryId, std::make_pair(docId, score));
        }
    }
    qrelsIn.close();

    spdlog::info("Loaded BEIR dataset: {} documents, {} queries, {} qrels",
                 dataset.documents.size(), dataset.queries.size(), dataset.qrels.size());

    return dataset;
}

class SimpleDaemonHarness {
public:
    SimpleDaemonHarness() {
        auto id = randomId();
        root_ = fs::temp_directory_path() / ("yams_bench_" + id);
        fs::create_directories(root_);
        data_ = root_ / "data";
        fs::create_directories(data_);
        sock_ = fs::path("/tmp") / ("bench_" + id + ".sock");
        pid_ = root_ / "bench.pid";
        log_ = root_ / "bench.log";
    }

    ~SimpleDaemonHarness() {
        stop();
        cleanup();
    }

    bool start() {
        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = data_;
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = true;

        // Configure real embedding provider for integration/benchmark mode
        cfg.useMockModelProvider = false;
        cfg.autoLoadPlugins = true;

        // Set plugin directory to builddir if not overridden by environment
        const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR");
        if (envPluginDir) {
            cfg.pluginDir = fs::path(envPluginDir);
        } else {
            // Default to builddir/plugins for benchmark mode
            cfg.pluginDir = fs::path("builddir") / "plugins";
        }

        // Configure model pool to preload embedding model
        cfg.modelPoolConfig.lazyLoading = false;
        cfg.modelPoolConfig.preloadModels = {"all-MiniLM-L6-v2"};

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);

        auto s = daemon_->start();
        if (!s)
            return false;

        // Start runLoop in background thread - CRITICAL for processing requests
        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });

        auto deadline = std::chrono::steady_clock::now() + 30s;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready)
                return true;
            if (lifecycle.state == yams::daemon::LifecycleState::Failed)
                return false;
        }
        return false;
    }

    void stop() {
        if (daemon_) {
            auto stopResult = daemon_->stop();
            if (!stopResult) {
                spdlog::warn("Daemon stop returned error: {}", stopResult.error().message);
            }

            // Join the runLoop thread before continuing
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
            }

            int retries = 0;
            while (daemon_->isRunning() && retries < 50) {
                std::this_thread::sleep_for(10ms);
                retries++;
            }

            const char* yams_testing = std::getenv("YAMS_TESTING");
            if (yams_testing) {
                unsetenv("YAMS_TESTING");
            }

            yams::daemon::GlobalIOContext::reset();

            if (yams_testing) {
                setenv("YAMS_TESTING", yams_testing, 1);
            }

            daemon_.reset();
            std::this_thread::sleep_for(500ms);
        }
    }

    const fs::path& socketPath() const { return sock_; }
    const fs::path& root() const { return root_; }

private:
    static std::string randomId() {
        static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, 35);
        std::string out;
        for (int i = 0; i < 8; ++i)
            out.push_back(cs[dist(rng)]);
        return out;
    }

    void cleanup() {
        std::error_code ec;
        if (!root_.empty())
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    std::thread runLoopThread_;
    fs::path root_, data_, sock_, pid_, log_;
};

struct TestQuery {
    std::string query;
    std::set<std::string> relevantFiles;
    std::set<std::string> relevantDocIds;
    std::map<std::string, int> relevanceGrades;
    bool useDocIds;
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

RetrievalMetrics evaluateQueries(yams::daemon::DaemonClient& client,
                                 const std::vector<TestQuery>& queries, int k) {
    RetrievalMetrics metrics;
    metrics.numQueries = queries.size();
    double totalMRR = 0.0, totalRecall = 0.0, totalPrecision = 0.0, totalNDCG = 0.0, totalMAP = 0.0;

    for (const auto& tq : queries) {
        yams::daemon::SearchRequest req;
        req.query = tq.query;
        req.searchType = "hybrid";
        req.limit = k;
        req.timeout = 5s;

        spdlog::info("Executing search query: '{}' (relevant files: {})", tq.query,
                     tq.relevantFiles.size());
        auto result = yams::cli::run_sync(client.search(req), 10s);
        if (!result) {
            spdlog::warn("Search failed for query '{}': {}", tq.query, result.error().message);
            continue;
        }

        const auto& results = result.value().results;
        spdlog::info("Search returned {} results for query '{}'", results.size(), tq.query);
        int firstRelevantRank = -1, numRelevantInTopK = 0, numRelevantSeen = 0;
        std::vector<int> retrievedGrades;
        double avgPrecision = 0.0;

        for (size_t i = 0; i < std::min((size_t)k, results.size()); ++i) {
            std::string filename = fs::path(results[i].path).filename().string();
            bool isRelevant = false;
            std::string key;

            if (tq.useDocIds) {
                std::string docId = filename;
                if (docId.size() > 4 && docId.substr(docId.size() - 4) == ".txt") {
                    docId = docId.substr(0, docId.size() - 4);
                }
                key = docId;
                isRelevant = tq.relevantDocIds.count(docId) > 0;
            } else {
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
            retrievedGrades.push_back((gradeIt != tq.relevanceGrades.end()) ? gradeIt->second : 0);
        }

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
    }
    return metrics;
}

struct BenchFixture {
    std::unique_ptr<SimpleDaemonHarness> harness;
    std::unique_ptr<yams::daemon::DaemonClient> client;
    std::unique_ptr<CorpusGenerator> corpus;
    std::unique_ptr<BEIRCorpusLoader> beirCorpus;
    std::vector<TestQuery> queries;
    int corpusSize = 50, numQueries = 10, topK = 10;
    bool useBEIR = false;

    void setup() {
        const char* env_dataset = std::getenv("YAMS_BENCH_DATASET");
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

        harness = std::make_unique<SimpleDaemonHarness>();
        if (!harness->start())
            throw std::runtime_error("Failed to start daemon");

        if (useBEIR) {
            fs::path cachePath = env_path ? fs::path(env_path) : fs::path();
            auto dsResult = loadSciFactDataset(cachePath);
            if (!dsResult) {
                throw std::runtime_error("Failed to load BEIR dataset: " +
                                         dsResult.error().message);
            }
            BEIRDataset dataset = dsResult.value();

            fs::path corpusDir = harness->root() / "corpus";
            beirCorpus = std::make_unique<BEIRCorpusLoader>(dataset, corpusDir);
            beirCorpus->writeDocumentsAsFiles();
            corpusSize = dataset.documents.size();
            numQueries = dataset.queries.size();
        } else {
            corpus = std::make_unique<CorpusGenerator>(harness->root() / "corpus");
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

        spdlog::info("Ingesting {} documents...", corpusSize);
        int successCount = 0, failCount = 0;
        std::vector<std::string> docPaths;

        if (useBEIR) {
            docPaths = beirCorpus->getDocumentPaths();
        } else {
            for (const auto& filename : corpus->createdFiles) {
                docPaths.push_back((corpus->corpusDir / filename).string());
            }
        }

        for (const auto& filePath : docPaths) {
            yams::daemon::AddDocumentRequest addReq;
            addReq.path = filePath;
            addReq.noEmbeddings = false;
            auto addResult = yams::cli::run_sync(client->streamingAddDocument(addReq), 30s);
            if (!addResult) {
                spdlog::warn("Failed to ingest {}: {}", filePath, addResult.error().message);
                failCount++;
            } else {
                successCount++;
                if (successCount % 10 == 0 || successCount == corpusSize) {
                    spdlog::info("Ingested {}/{} documents", successCount, corpusSize);
                }
            }
        }
        spdlog::info("Ingestion complete: {} succeeded, {} failed", successCount, failCount);

        spdlog::info("Waiting for ingestion to complete...");
        auto deadline = std::chrono::steady_clock::now() + 60s;
        uint32_t lastDepth = 0;
        bool completed = false;
        while (std::chrono::steady_clock::now() < deadline) {
            auto statusResult = yams::cli::run_sync(client->status(), 5s);
            if (statusResult) {
                uint32_t depth = statusResult.value().postIngestQueueDepth;
                if (depth != lastDepth) {
                    spdlog::info("Post-ingest queue depth: {}", depth);
                    lastDepth = depth;
                }
                if (depth == 0) {
                    spdlog::info("All documents ingested and indexed");
                    completed = true;
                    break;
                }
            }
            std::this_thread::sleep_for(200ms);
        }
        if (!completed) {
            spdlog::warn("Ingestion did not complete within 60s timeout (last depth: {})",
                         lastDepth);
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

        // Additional wait for embeddings to be generated
        // The embedding service processes documents asynchronously after FTS5 indexing
        spdlog::info("Waiting for embeddings to be generated (20s)...");
        std::this_thread::sleep_for(20s);

        // Check vector count to verify embeddings were created
        auto finalStatus = yams::cli::run_sync(client->status(), 5s);
        if (finalStatus && finalStatus.value().vectorDbReady) {
            spdlog::info("Vector DB ready: dim={}", finalStatus.value().vectorDbDim);
        } else {
            spdlog::warn("Vector DB may not be ready or no embeddings generated");
        }

        // Verify document count by doing a test search
        yams::daemon::SearchRequest testReq;
        testReq.query = "test";
        testReq.searchType = "hybrid";
        testReq.limit = 1000;
        testReq.timeout = 5s;
        auto testResult = yams::cli::run_sync(client->search(testReq), 10s);
        int indexedDocCount = testResult ? testResult.value().results.size() : 0;
        spdlog::info("Verified indexed documents: {} (expected: {})", indexedDocCount, corpusSize);
        if (indexedDocCount == 0) {
            spdlog::error("NO DOCUMENTS IN INDEX! Ingestion failed completely.");
            throw std::runtime_error("No documents indexed - benchmark cannot proceed");
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
        auto metrics = evaluateQueries(*fixture.client, fixture.queries, fixture.topK);
        benchmark::DoNotOptimize(metrics);
    }
    auto metrics = evaluateQueries(*fixture.client, fixture.queries, fixture.topK);
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
    return 0;
}
