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
#include <yams/daemon/client/asio_connection_pool.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>

#include <algorithm>
#include <cctype>
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
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
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

static Result<BEIRDataset> selectBenchmarkBEIRDataset(const std::string& datasetName,
                                                      const fs::path& cacheDir,
                                                      const std::optional<int>& maxDocsOpt,
                                                      const std::optional<int>& maxQueriesOpt) {
    Result<BEIRDataset> dsResult;
    if (datasetName == "scifact") {
        dsResult = loadSciFactDataset(cacheDir);
    } else {
        dsResult = loadBEIRDataset(datasetName, cacheDir);
    }
    if (!dsResult) {
        return dsResult;
    }

    const BEIRDataset& fullDataset = dsResult.value();
    BEIRDataset dataset;
    dataset.name = fullDataset.name;
    dataset.basePath = fullDataset.basePath;

    const int maxDocs =
        maxDocsOpt.has_value() ? *maxDocsOpt : static_cast<int>(fullDataset.documents.size());
    const int maxQueries =
        maxQueriesOpt.has_value() ? *maxQueriesOpt : static_cast<int>(fullDataset.queries.size());

    std::set<std::string> qrelDocIds;
    for (const auto& [queryId, docScore] : fullDataset.qrels) {
        (void)queryId;
        qrelDocIds.insert(docScore.first);
    }
    spdlog::info("BEIR dataset has {} documents referenced by qrels", qrelDocIds.size());

    std::set<std::string> includedDocIds;
    for (const auto& docId : qrelDocIds) {
        if (static_cast<int>(includedDocIds.size()) >= maxDocs) {
            break;
        }
        if (fullDataset.documents.count(docId) > 0) {
            dataset.documents[docId] = fullDataset.documents.at(docId);
            includedDocIds.insert(docId);
        }
    }
    spdlog::info("Included {} qrel-referenced documents", includedDocIds.size());

    for (const auto& [id, doc] : fullDataset.documents) {
        if (static_cast<int>(includedDocIds.size()) >= maxDocs) {
            break;
        }
        if (includedDocIds.count(id) == 0) {
            dataset.documents[id] = doc;
            includedDocIds.insert(id);
        }
    }

    int queryCount = 0;
    for (const auto& [qid, query] : fullDataset.queries) {
        if (queryCount >= maxQueries) {
            break;
        }
        auto range = fullDataset.qrels.equal_range(qid);
        bool hasRelevantDoc = false;
        for (auto it = range.first; it != range.second; ++it) {
            if (includedDocIds.count(it->second.first) > 0) {
                hasRelevantDoc = true;
                break;
            }
        }
        if (!hasRelevantDoc) {
            continue;
        }

        dataset.queries[qid] = query;
        for (auto it = range.first; it != range.second; ++it) {
            if (includedDocIds.count(it->second.first) > 0) {
                dataset.qrels.emplace(qid, it->second);
            }
        }
        queryCount++;
    }

    spdlog::debug(
        "Limited BEIR dataset to {} docs, {} queries, {} qrels (from {} docs, {} queries)",
        dataset.documents.size(), dataset.queries.size(), dataset.qrels.size(),
        fullDataset.documents.size(), fullDataset.queries.size());

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
        std::uniform_int_distribution<int> topicDist(0, static_cast<int>(topics.size()) - 1);
        std::uniform_int_distribution<int> termDist(0, static_cast<int>(terms.size()) - 1);
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
        std::uniform_int_distribution<int> topicDist(0, static_cast<int>(topics.size()) - 1);
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
    double duplicateRateAtK = 0.0;
    int numQueries = 0;
};

struct QueryDiagnosticsSummary {
    std::uint64_t queryCount = 0;
    std::uint64_t queryWithResultsCount = 0;
    std::uint64_t queryWithoutResultsCount = 0;
    std::uint64_t queryWithoutRelevantHitCount = 0;
    std::uint64_t queryWithRelevantInPreFusionCount = 0;
    std::uint64_t queryWithRelevantInPostFusionCount = 0;
    std::uint64_t queryWithRelevantInGraphlessPostFusionCount = 0;
    std::uint64_t queryWithRelevantInFinalWindowCount = 0;
    std::uint64_t queryWithGraphAddedRelevantPostFusionCount = 0;
    std::uint64_t queryWithGraphDisplacedRelevantPostFusionCount = 0;
    std::uint64_t queryWithTopKDuplicateCount = 0;
    std::uint64_t degradedQueryCount = 0;
    std::uint64_t traceEnabledQueryCount = 0;
    std::uint64_t graphRerankAppliedQueryCount = 0;
    std::uint64_t semanticRescueNonZeroQueryCount = 0;
    std::uint64_t missMissingPreFusionCount = 0;
    std::uint64_t missFusionCutoffCount = 0;
    std::uint64_t missRerankWindowDropCount = 0;
    std::uint64_t missTopKDropCount = 0;
    std::vector<double> semanticRescueRateSamples;
    std::vector<double> semanticRescueFinalCountSamples;
    std::vector<double> semanticRescueTargetSamples;
    std::vector<double> fusionDroppedCountSamples;
    std::vector<double> vectorOnlyDocsSamples;
    std::vector<double> vectorOnlyBelowThresholdSamples;
    std::vector<double> vectorOnlyAboveThresholdSamples;
    std::vector<double> vectorOnlyNearMissEligibleSamples;
    std::vector<double> strongVectorOnlyDocsSamples;
    std::vector<double> strongVectorOnlyScoreEligibleDocsSamples;
    std::vector<double> strongVectorOnlyRankEligibleDocsSamples;
    std::vector<double> graphAddedPostFusionSamples;
    std::vector<double> graphDisplacedPostFusionSamples;
    std::vector<double> multiVectorGeneratedPhraseSamples;
    std::vector<double> multiVectorRawHitSamples;
    std::vector<double> multiVectorAddedNewSamples;
    std::vector<double> multiVectorReplacedBaseSamples;
    std::vector<double> subPhraseGeneratedSamples;
    std::vector<double> subPhraseClauseSamples;
    std::vector<double> subPhraseFtsHitSamples;
    std::vector<double> subPhraseFtsAddedSamples;
    std::vector<double> aggressiveFtsClauseSamples;
    std::vector<double> aggressiveFtsHitSamples;
    std::vector<double> aggressiveFtsAddedSamples;
    std::unordered_map<std::string, std::vector<double>> timingSamplesMs;
};

static const std::vector<std::string>& trackedTimingKeys() {
    static const std::vector<std::string> kKeys = {
        "latency", "embedding", "concepts", "text",     "vector",       "entity_vector",
        "kg",      "path",      "tag",      "metadata", "multi_vector", "graph_rerank",
    };
    return kKeys;
}

static std::string timingStatKey(const std::string& name) {
    return std::string("timing_") + name + "_ms";
}

static std::unordered_set<std::string> splitTabSet(const std::string& value) {
    std::unordered_set<std::string> out;
    if (value.empty()) {
        return out;
    }
    size_t start = 0;
    while (start <= value.size()) {
        const size_t end = value.find('\t', start);
        const size_t len = (end == std::string::npos) ? (value.size() - start) : (end - start);
        if (len > 0) {
            out.insert(value.substr(start, len));
        }
        if (end == std::string::npos) {
            break;
        }
        start = end + 1;
    }
    return out;
}

static json buildRelevantDecisionTrace(const std::vector<std::string>& relevantDocIds,
                                       const std::vector<std::string>& returnedDocIds,
                                       const std::map<std::string, std::string>& searchStats) {
    const auto preSet = splitTabSet(searchStats.contains("trace_pre_fusion_doc_ids")
                                        ? searchStats.at("trace_pre_fusion_doc_ids")
                                        : "");
    const auto postSet = splitTabSet(searchStats.contains("trace_post_fusion_doc_ids")
                                         ? searchStats.at("trace_post_fusion_doc_ids")
                                         : "");
    const auto graphlessPostSet =
        splitTabSet(searchStats.contains("trace_graphless_post_fusion_doc_ids")
                        ? searchStats.at("trace_graphless_post_fusion_doc_ids")
                        : "");
    const auto finalWindowSet = splitTabSet(
        searchStats.contains("trace_final_doc_ids") ? searchStats.at("trace_final_doc_ids") : "");
    const std::unordered_set<std::string> returnedTopKSet(returnedDocIds.begin(),
                                                          returnedDocIds.end());

    json fusionTop = json::array();
    if (auto it = searchStats.find("trace_fusion_top_json"); it != searchStats.end()) {
        try {
            fusionTop = json::parse(it->second);
        } catch (...) {
        }
    }
    json finalTop = json::array();
    if (auto it = searchStats.find("trace_final_top_json"); it != searchStats.end()) {
        try {
            finalTop = json::parse(it->second);
        } catch (...) {
        }
    }
    json componentHits = json::object();
    if (auto it = searchStats.find("trace_component_hits_json"); it != searchStats.end()) {
        try {
            componentHits = json::parse(it->second);
        } catch (...) {
        }
    }

    const auto findDoc = [](const json& docs, const std::string& docId) -> json {
        if (!docs.is_array()) {
            return json();
        }
        for (const auto& item : docs) {
            if (item.is_object() && item.value("doc_id", "") == docId) {
                return item;
            }
        }
        return json();
    };

    json relevant = json::array();
    bool anyPre = false;
    bool anyPost = false;
    bool anyGraphlessPost = false;
    bool anyFinalWindow = false;
    bool anyReturnedTopK = false;
    json graphAddedRelevant = json::array();
    json graphDisplacedRelevant = json::array();

    for (const auto& docId : relevantDocIds) {
        const bool inPre = preSet.contains(docId);
        const bool inPost = postSet.contains(docId);
        const bool inGraphlessPost = graphlessPostSet.contains(docId);
        const bool inFinalWindow = finalWindowSet.contains(docId);
        const bool inReturnedTopK = returnedTopKSet.contains(docId);
        anyPre = anyPre || inPre;
        anyPost = anyPost || inPost;
        anyGraphlessPost = anyGraphlessPost || inGraphlessPost;
        anyFinalWindow = anyFinalWindow || inFinalWindow;
        anyReturnedTopK = anyReturnedTopK || inReturnedTopK;

        if (inPost && !inGraphlessPost) {
            graphAddedRelevant.push_back(docId);
        }
        if (inGraphlessPost && !inPost) {
            graphDisplacedRelevant.push_back(docId);
        }

        json componentSources = json::array();
        if (componentHits.is_object()) {
            for (const auto& [component, details] : componentHits.items()) {
                if (!details.is_object()) {
                    continue;
                }
                const auto topIdsIt = details.find("unique_top_doc_ids");
                if (topIdsIt == details.end() || !topIdsIt->is_array()) {
                    continue;
                }
                for (const auto& id : *topIdsIt) {
                    if (id.is_string() && id.get<std::string>() == docId) {
                        componentSources.push_back(component);
                        break;
                    }
                }
            }
        }

        json entry = {
            {"doc_id", docId},
            {"in_pre_fusion", inPre},
            {"in_post_fusion", inPost},
            {"in_graphless_post_fusion", inGraphlessPost},
            {"in_final_window", inFinalWindow},
            {"in_returned_topk", inReturnedTopK},
            {"component_top_hits", componentSources},
        };

        if (auto match = findDoc(fusionTop, docId); !match.is_null()) {
            entry["post_fusion"] = match;
        }
        if (auto match = findDoc(finalTop, docId); !match.is_null()) {
            entry["final_window"] = match;
        }
        relevant.push_back(std::move(entry));
    }

    std::string missStage = "hit";
    if (!anyReturnedTopK) {
        if (!anyPre) {
            missStage = "missing_pre_fusion";
        } else if (!anyPost) {
            missStage = "dropped_before_fusion_window";
        } else if (!anyFinalWindow) {
            missStage = "dropped_during_rerank_window";
        } else {
            missStage = "dropped_before_topk";
        }
    }

    return {
        {"trace_enabled",
         searchStats.contains("trace_enabled") && searchStats.at("trace_enabled") == "1"},
        {"graph_rerank_applied", searchStats.contains("trace_graph_rerank_applied") &&
                                     searchStats.at("trace_graph_rerank_applied") == "1"},
        {"cross_rerank_applied", searchStats.contains("trace_cross_rerank_applied") &&
                                     searchStats.at("trace_cross_rerank_applied") == "1"},
        {"any_relevant_in_pre_fusion", anyPre},
        {"any_relevant_in_post_fusion", anyPost},
        {"any_relevant_in_graphless_post_fusion", anyGraphlessPost},
        {"any_relevant_in_final_window", anyFinalWindow},
        {"any_relevant_in_returned_topk", anyReturnedTopK},
        {"graph_added_relevant_post_fusion_count", graphAddedRelevant.size()},
        {"graph_displaced_relevant_post_fusion_count", graphDisplacedRelevant.size()},
        {"graph_added_relevant_post_fusion_doc_ids", graphAddedRelevant},
        {"graph_displaced_relevant_post_fusion_doc_ids", graphDisplacedRelevant},
        {"miss_stage", missStage},
        {"relevant_docs", relevant},
    };
}

struct DebugLogEntry {
    std::string query;
    int queryIndex = -1;
    std::string searchType;
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
    std::map<std::string, std::string> searchStats;
    json extraFields;
};

struct DebugRunContext {
    std::string runId;
    std::string candidate;
    std::string dataset;
    int corpusSize = 0;
    int numQueries = 0;
    int topK = 0;
    int traceTopN = 0;
    int traceComponentTopN = 0;
    bool stageTraceEnabled = false;
    std::string debugFile;
};

static std::ostream* g_debugOut = nullptr;
static std::unique_ptr<std::ofstream> g_debugFile;
static std::mutex g_debugMutex;
static std::optional<DebugRunContext> g_debugRunContext;

static std::optional<double> parseDoubleLoose(const std::string& value) {
    try {
        return std::stod(value);
    } catch (...) {
        return std::nullopt;
    }
}

static std::optional<double> parseDoubleStat(const std::map<std::string, std::string>& stats,
                                             const std::string& key) {
    if (auto it = stats.find(key); it != stats.end()) {
        return parseDoubleLoose(it->second);
    }
    return std::nullopt;
}

static std::optional<bool> parseBoolStat(const std::map<std::string, std::string>& stats,
                                         const std::string& key) {
    if (auto it = stats.find(key); it != stats.end()) {
        std::string normalized = it->second;
        std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char c) {
            return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        });
        return normalized == "1" || normalized == "true" || normalized == "yes" ||
               normalized == "on";
    }
    return std::nullopt;
}

static double computePercentile(std::vector<double> samples, double percentile) {
    if (samples.empty()) {
        return 0.0;
    }

    percentile = std::clamp(percentile, 0.0, 1.0);
    std::sort(samples.begin(), samples.end());
    const double idx = percentile * static_cast<double>(samples.size() - 1);
    const auto lo = static_cast<std::size_t>(std::floor(idx));
    const auto hi = static_cast<std::size_t>(std::ceil(idx));
    if (lo == hi) {
        return samples[lo];
    }
    const double t = idx - static_cast<double>(lo);
    return samples[lo] * (1.0 - t) + samples[hi] * t;
}

static json summarizeSamples(const std::vector<double>& samples) {
    json out = {
        {"count", samples.size()},
        {"mean", 0.0},
        {"min", 0.0},
        {"p50", 0.0},
        {"p95", 0.0},
        {"max", 0.0},
    };

    if (samples.empty()) {
        return out;
    }

    const double sum = std::accumulate(samples.begin(), samples.end(), 0.0);
    out["mean"] = sum / static_cast<double>(samples.size());
    out["min"] = *std::min_element(samples.begin(), samples.end());
    out["p50"] = computePercentile(samples, 0.50);
    out["p95"] = computePercentile(samples, 0.95);
    out["max"] = *std::max_element(samples.begin(), samples.end());
    return out;
}

static void ingestQueryDiagnostics(QueryDiagnosticsSummary& summary,
                                   const std::map<std::string, std::string>& searchStats,
                                   const json& relevantDecisionTrace, bool degraded,
                                   bool hadResults, bool hadTopKDuplicate, int firstRelevantRank) {
    summary.queryCount++;
    if (hadResults) {
        summary.queryWithResultsCount++;
    } else {
        summary.queryWithoutResultsCount++;
    }
    if (firstRelevantRank < 0) {
        summary.queryWithoutRelevantHitCount++;
    }
    if (hadTopKDuplicate) {
        summary.queryWithTopKDuplicateCount++;
    }
    if (degraded) {
        summary.degradedQueryCount++;
    }

    if (relevantDecisionTrace.is_object()) {
        const bool anyPre = relevantDecisionTrace.value("any_relevant_in_pre_fusion", false);
        const bool anyPost = relevantDecisionTrace.value("any_relevant_in_post_fusion", false);
        const bool anyGraphlessPost =
            relevantDecisionTrace.value("any_relevant_in_graphless_post_fusion", false);
        const bool anyFinalWindow =
            relevantDecisionTrace.value("any_relevant_in_final_window", false);
        if (anyPre) {
            summary.queryWithRelevantInPreFusionCount++;
        }
        if (anyPost) {
            summary.queryWithRelevantInPostFusionCount++;
        }
        if (anyGraphlessPost) {
            summary.queryWithRelevantInGraphlessPostFusionCount++;
        }
        if (anyFinalWindow) {
            summary.queryWithRelevantInFinalWindowCount++;
        }
        if (relevantDecisionTrace.value("graph_added_relevant_post_fusion_count", 0) > 0) {
            summary.queryWithGraphAddedRelevantPostFusionCount++;
        }
        if (relevantDecisionTrace.value("graph_displaced_relevant_post_fusion_count", 0) > 0) {
            summary.queryWithGraphDisplacedRelevantPostFusionCount++;
        }
        if (firstRelevantRank < 0) {
            const std::string missStage = relevantDecisionTrace.value("miss_stage", "unknown");
            if (missStage == "missing_pre_fusion") {
                summary.missMissingPreFusionCount++;
            } else if (missStage == "dropped_before_fusion_window") {
                summary.missFusionCutoffCount++;
            } else if (missStage == "dropped_during_rerank_window") {
                summary.missRerankWindowDropCount++;
            } else if (missStage == "dropped_before_topk") {
                summary.missTopKDropCount++;
            }
        }
    }

    if (parseBoolStat(searchStats, "trace_enabled").value_or(false)) {
        summary.traceEnabledQueryCount++;
    }
    if (parseBoolStat(searchStats, "trace_graph_rerank_applied").value_or(false)) {
        summary.graphRerankAppliedQueryCount++;
    }

    if (auto v = parseDoubleStat(searchStats, "semantic_rescue_rate")) {
        summary.semanticRescueRateSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "semantic_rescue_final_count")) {
        summary.semanticRescueFinalCountSamples.push_back(*v);
        if (*v > 0.0) {
            summary.semanticRescueNonZeroQueryCount++;
        }
    }
    if (auto v = parseDoubleStat(searchStats, "semantic_rescue_target")) {
        summary.semanticRescueTargetSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "trace_fusion_dropped_count")) {
        summary.fusionDroppedCountSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "multi_vector_generated_phrases")) {
        summary.multiVectorGeneratedPhraseSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "multi_vector_raw_hit_count")) {
        summary.multiVectorRawHitSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "multi_vector_added_new_count")) {
        summary.multiVectorAddedNewSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "multi_vector_replaced_base_count")) {
        summary.multiVectorReplacedBaseSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "subphrase_generated_count")) {
        summary.subPhraseGeneratedSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "subphrase_clause_count")) {
        summary.subPhraseClauseSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "subphrase_fts_hit_count")) {
        summary.subPhraseFtsHitSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "subphrase_fts_added_count")) {
        summary.subPhraseFtsAddedSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "aggressive_fts_clause_count")) {
        summary.aggressiveFtsClauseSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "aggressive_fts_hit_count")) {
        summary.aggressiveFtsHitSamples.push_back(*v);
    }
    if (auto v = parseDoubleStat(searchStats, "aggressive_fts_added_count")) {
        summary.aggressiveFtsAddedSamples.push_back(*v);
    }

    for (const auto& timingName : trackedTimingKeys()) {
        const auto key = timingStatKey(timingName);
        if (auto v = parseDoubleStat(searchStats, key)) {
            summary.timingSamplesMs[timingName].push_back(*v);
        }
    }

    auto prefusion = searchStats.find("trace_prefusion_signal_summary_json");
    if (prefusion != searchStats.end()) {
        try {
            auto parsed = json::parse(prefusion->second);
            if (parsed.is_object()) {
                if (auto it = parsed.find("vector_only_docs"); it != parsed.end()) {
                    if (it->is_number()) {
                        summary.vectorOnlyDocsSamples.push_back(it->get<double>());
                    }
                }
                if (auto it = parsed.find("vector_only_below_threshold"); it != parsed.end()) {
                    if (it->is_number()) {
                        summary.vectorOnlyBelowThresholdSamples.push_back(it->get<double>());
                    }
                }
                if (auto it = parsed.find("vector_only_above_threshold"); it != parsed.end()) {
                    if (it->is_number()) {
                        summary.vectorOnlyAboveThresholdSamples.push_back(it->get<double>());
                    }
                }
                if (auto it = parsed.find("vector_only_near_miss_eligible"); it != parsed.end()) {
                    if (it->is_number()) {
                        summary.vectorOnlyNearMissEligibleSamples.push_back(it->get<double>());
                    }
                }
                if (auto it = parsed.find("strong_vector_only_docs"); it != parsed.end()) {
                    if (it->is_number()) {
                        summary.strongVectorOnlyDocsSamples.push_back(it->get<double>());
                    }
                }
                if (auto it = parsed.find("strong_vector_only_score_eligible_docs");
                    it != parsed.end()) {
                    if (it->is_number()) {
                        summary.strongVectorOnlyScoreEligibleDocsSamples.push_back(
                            it->get<double>());
                    }
                }
                if (auto it = parsed.find("strong_vector_only_rank_eligible_docs");
                    it != parsed.end()) {
                    if (it->is_number()) {
                        summary.strongVectorOnlyRankEligibleDocsSamples.push_back(
                            it->get<double>());
                    }
                }
            }
        } catch (...) {
        }
    }

    auto graphDisplacement = searchStats.find("trace_graph_displacement_summary_json");
    if (graphDisplacement != searchStats.end()) {
        try {
            auto parsed = json::parse(graphDisplacement->second);
            if (parsed.is_object()) {
                if (auto it = parsed.find("graph_added_post_fusion_count");
                    it != parsed.end() && it->is_number()) {
                    summary.graphAddedPostFusionSamples.push_back(it->get<double>());
                }
                if (auto it = parsed.find("graph_displaced_post_fusion_count");
                    it != parsed.end() && it->is_number()) {
                    summary.graphDisplacedPostFusionSamples.push_back(it->get<double>());
                }
            }
        } catch (...) {
        }
    }
}

static json queryDiagnosticsToJson(const QueryDiagnosticsSummary& summary) {
    const double queryCount = static_cast<double>(std::max<std::uint64_t>(summary.queryCount, 1));

    json out = {
        {"query_count", summary.queryCount},
        {"query_with_results_count", summary.queryWithResultsCount},
        {"query_without_results_count", summary.queryWithoutResultsCount},
        {"query_without_relevant_hit_count", summary.queryWithoutRelevantHitCount},
        {"query_with_relevant_in_pre_fusion_count", summary.queryWithRelevantInPreFusionCount},
        {"query_with_relevant_in_post_fusion_count", summary.queryWithRelevantInPostFusionCount},
        {"query_with_relevant_in_graphless_post_fusion_count",
         summary.queryWithRelevantInGraphlessPostFusionCount},
        {"query_with_relevant_in_final_window_count", summary.queryWithRelevantInFinalWindowCount},
        {"query_with_graph_added_relevant_post_fusion_count",
         summary.queryWithGraphAddedRelevantPostFusionCount},
        {"query_with_graph_displaced_relevant_post_fusion_count",
         summary.queryWithGraphDisplacedRelevantPostFusionCount},
        {"query_with_topk_duplicate_count", summary.queryWithTopKDuplicateCount},
        {"degraded_query_count", summary.degradedQueryCount},
        {"trace_enabled_query_count", summary.traceEnabledQueryCount},
        {"graph_rerank_applied_query_count", summary.graphRerankAppliedQueryCount},
        {"semantic_rescue_nonzero_query_count", summary.semanticRescueNonZeroQueryCount},
        {"miss_missing_pre_fusion_count", summary.missMissingPreFusionCount},
        {"miss_fusion_cutoff_count", summary.missFusionCutoffCount},
        {"miss_rerank_window_drop_count", summary.missRerankWindowDropCount},
        {"miss_topk_drop_count", summary.missTopKDropCount},
        {"query_with_results_rate",
         static_cast<double>(summary.queryWithResultsCount) / queryCount},
        {"query_without_relevant_hit_rate",
         static_cast<double>(summary.queryWithoutRelevantHitCount) / queryCount},
        {"query_with_relevant_in_pre_fusion_rate",
         static_cast<double>(summary.queryWithRelevantInPreFusionCount) / queryCount},
        {"query_with_relevant_in_post_fusion_rate",
         static_cast<double>(summary.queryWithRelevantInPostFusionCount) / queryCount},
        {"query_with_relevant_in_graphless_post_fusion_rate",
         static_cast<double>(summary.queryWithRelevantInGraphlessPostFusionCount) / queryCount},
        {"query_with_relevant_in_final_window_rate",
         static_cast<double>(summary.queryWithRelevantInFinalWindowCount) / queryCount},
        {"query_with_graph_added_relevant_post_fusion_rate",
         static_cast<double>(summary.queryWithGraphAddedRelevantPostFusionCount) / queryCount},
        {"query_with_graph_displaced_relevant_post_fusion_rate",
         static_cast<double>(summary.queryWithGraphDisplacedRelevantPostFusionCount) / queryCount},
        {"query_with_topk_duplicate_rate",
         static_cast<double>(summary.queryWithTopKDuplicateCount) / queryCount},
        {"degraded_query_rate", static_cast<double>(summary.degradedQueryCount) / queryCount},
        {"trace_coverage", static_cast<double>(summary.traceEnabledQueryCount) / queryCount},
        {"graph_rerank_apply_rate",
         static_cast<double>(summary.graphRerankAppliedQueryCount) / queryCount},
        {"semantic_rescue_nonzero_rate",
         static_cast<double>(summary.semanticRescueNonZeroQueryCount) / queryCount},
        {"miss_missing_pre_fusion_rate",
         static_cast<double>(summary.missMissingPreFusionCount) / queryCount},
        {"miss_fusion_cutoff_rate",
         static_cast<double>(summary.missFusionCutoffCount) / queryCount},
        {"miss_rerank_window_drop_rate",
         static_cast<double>(summary.missRerankWindowDropCount) / queryCount},
        {"miss_topk_drop_rate", static_cast<double>(summary.missTopKDropCount) / queryCount},
        {"semantic_rescue_rate", summarizeSamples(summary.semanticRescueRateSamples)},
        {"semantic_rescue_final_count", summarizeSamples(summary.semanticRescueFinalCountSamples)},
        {"semantic_rescue_target", summarizeSamples(summary.semanticRescueTargetSamples)},
        {"fusion_dropped_count", summarizeSamples(summary.fusionDroppedCountSamples)},
        {"vector_only_docs", summarizeSamples(summary.vectorOnlyDocsSamples)},
        {"vector_only_below_threshold", summarizeSamples(summary.vectorOnlyBelowThresholdSamples)},
        {"vector_only_above_threshold", summarizeSamples(summary.vectorOnlyAboveThresholdSamples)},
        {"vector_only_near_miss_eligible",
         summarizeSamples(summary.vectorOnlyNearMissEligibleSamples)},
        {"strong_vector_only_docs", summarizeSamples(summary.strongVectorOnlyDocsSamples)},
        {"strong_vector_only_score_eligible_docs",
         summarizeSamples(summary.strongVectorOnlyScoreEligibleDocsSamples)},
        {"strong_vector_only_rank_eligible_docs",
         summarizeSamples(summary.strongVectorOnlyRankEligibleDocsSamples)},
        {"graph_added_post_fusion_count", summarizeSamples(summary.graphAddedPostFusionSamples)},
        {"graph_displaced_post_fusion_count",
         summarizeSamples(summary.graphDisplacedPostFusionSamples)},
        {"multi_vector_generated_phrases",
         summarizeSamples(summary.multiVectorGeneratedPhraseSamples)},
        {"multi_vector_raw_hit_count", summarizeSamples(summary.multiVectorRawHitSamples)},
        {"multi_vector_added_new_count", summarizeSamples(summary.multiVectorAddedNewSamples)},
        {"multi_vector_replaced_base_count",
         summarizeSamples(summary.multiVectorReplacedBaseSamples)},
        {"subphrase_generated_count", summarizeSamples(summary.subPhraseGeneratedSamples)},
        {"subphrase_clause_count", summarizeSamples(summary.subPhraseClauseSamples)},
        {"subphrase_fts_hit_count", summarizeSamples(summary.subPhraseFtsHitSamples)},
        {"subphrase_fts_added_count", summarizeSamples(summary.subPhraseFtsAddedSamples)},
        {"aggressive_fts_clause_count", summarizeSamples(summary.aggressiveFtsClauseSamples)},
        {"aggressive_fts_hit_count", summarizeSamples(summary.aggressiveFtsHitSamples)},
        {"aggressive_fts_added_count", summarizeSamples(summary.aggressiveFtsAddedSamples)},
    };

    json timings = json::object();
    for (const auto& timingName : trackedTimingKeys()) {
        auto it = summary.timingSamplesMs.find(timingName);
        if (it != summary.timingSamplesMs.end() && !it->second.empty()) {
            timings[timingName] = summarizeSamples(it->second);
        }
    }
    out["timings_ms"] = timings;

    return out;
}

static void debugLogWriteJsonLine(const DebugLogEntry& e) {
    if (!g_debugOut)
        return;

    json j;
    j["query"] = e.query;
    if (e.queryIndex >= 0) {
        j["query_index"] = e.queryIndex;
    }
    j["search_type"] = e.searchType;
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
    j["search_stats"] = e.searchStats;
    j["diag"] = e.diagnostics;

    if (g_debugRunContext.has_value()) {
        j["run_id"] = g_debugRunContext->runId;
        j["candidate"] = g_debugRunContext->candidate;
        j["dataset"] = g_debugRunContext->dataset;
        j["corpus_size"] = g_debugRunContext->corpusSize;
        j["num_queries"] = g_debugRunContext->numQueries;
        j["top_k"] = g_debugRunContext->topK;
        j["trace_top_n"] = g_debugRunContext->traceTopN;
        j["trace_component_top_n"] = g_debugRunContext->traceComponentTopN;
        j["stage_trace_enabled"] = g_debugRunContext->stageTraceEnabled;
        if (!g_debugRunContext->debugFile.empty()) {
            j["debug_file"] = g_debugRunContext->debugFile;
        }
    }

    if (e.extraFields.is_object()) {
        for (const auto& [k, v] : e.extraFields.items()) {
            j[k] = v;
        }
    }

    std::lock_guard<std::mutex> lock(g_debugMutex);
    (*g_debugOut) << j.dump() << "\n";
    g_debugOut->flush();
}

// Store final metrics globally for summary after teardown
static RetrievalMetrics g_final_metrics;
static RetrievalMetrics g_keyword_metrics; // FTS5-only metrics for component isolation

struct EnvSetting {
    std::string key;
    std::optional<std::string> value;
};

class ScopedEnvOverrides {
public:
    explicit ScopedEnvOverrides(const std::vector<EnvSetting>& overrides) {
        previous_.reserve(overrides.size());
        for (const auto& setting : overrides) {
            PreviousValue pv;
            pv.key = setting.key;
            if (const char* existing = std::getenv(setting.key.c_str())) {
                pv.value = std::string(existing);
            }
            previous_.push_back(pv);

            if (setting.value.has_value()) {
                (void)setenv(setting.key.c_str(), setting.value->c_str(), 1);
            } else {
                (void)unsetenv(setting.key.c_str());
            }
        }
    }

    ~ScopedEnvOverrides() {
        for (const auto& previous : previous_) {
            if (previous.value.has_value()) {
                (void)setenv(previous.key.c_str(), previous.value->c_str(), 1);
            } else {
                (void)unsetenv(previous.key.c_str());
            }
        }
    }

private:
    struct PreviousValue {
        std::string key;
        std::optional<std::string> value;
    };

    std::vector<PreviousValue> previous_;
};

struct OptimizationCandidate {
    std::string name;
    std::string description;
    std::vector<EnvSetting> envOverrides;
    bool enginePolicyCandidate = false;
    bool diagnosticCandidate = false;
};

struct OptimizationRunResult {
    OptimizationCandidate candidate;
    RetrievalMetrics hybridMetrics;
    RetrievalMetrics keywordMetrics;
    QueryDiagnosticsSummary hybridDiagnostics;
    QueryDiagnosticsSummary keywordDiagnostics;
    std::string tuningState;
    std::string tuningReason;
    std::string runId;
    std::string debugFile;
    int traceTopN = 0;
    int traceComponentTopN = 0;
    bool stageTraceEnabled = false;
    double objectiveScore = 0.0;
    double hybridEvalMs = 0.0;
    double keywordEvalMs = 0.0;
    bool success = false;
    std::string errorMessage;
};

struct BenchCacheMetadata {
    std::string dataset = "synthetic";
    std::string datasetPath;
    int corpusSize = 0;
    int numQueries = 0;
    int topK = 0;
    bool useBEIR = false;
    bool vectorsDisabled = false;
    bool requireKgReady = false;
    bool graphRerankRequested = false;
    int expectedDocs = 0;
    int expectedQueries = 0;
    std::uintmax_t corpusFingerprint = 0;
    std::string status = "priming";
    int embeddedDocs = 0;
    std::uintmax_t vectorCount = 0;
};

static bool envTruthy(const char* value) {
    if (!value) {
        return false;
    }
    std::string normalized(value);
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char c) {
        return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    });
    return normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on";
}

static int parseIntEnvOrDefault(const char* key, int defaultValue, int minValue, int maxValue) {
    const int clampedDefault = std::clamp(defaultValue, minValue, maxValue);
    const char* raw = std::getenv(key);
    if (!(raw && *raw)) {
        return clampedDefault;
    }
    try {
        return std::clamp(std::stoi(raw), minValue, maxValue);
    } catch (...) {
        return clampedDefault;
    }
}

static std::string sanitizeLabelForFilename(const std::string& value) {
    std::string out;
    out.reserve(value.size());
    for (char c : value) {
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '-' || c == '_') {
            out.push_back(c);
        } else {
            out.push_back('_');
        }
    }
    return out;
}

static std::string makeRunId(const std::string& candidate) {
    (void)candidate;
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
    return std::to_string(us);
}

static std::uintmax_t fileSizeOrZero(const fs::path& path) {
    std::error_code ec;
    const auto size = fs::file_size(path, ec);
    return ec ? 0 : size;
}

static std::uintmax_t computePathFingerprint(const fs::path& path) {
    std::error_code ec;
    if (!fs::exists(path, ec)) {
        return 0;
    }

    if (fs::is_regular_file(path, ec)) {
        return fileSizeOrZero(path);
    }

    if (!fs::is_directory(path, ec)) {
        return 0;
    }

    std::uintmax_t fingerprint = 1469598103934665603ull;
    for (fs::recursive_directory_iterator it(path, ec), end; !ec && it != end; it.increment(ec)) {
        const auto entryPath = it->path();
        const auto relative = fs::relative(entryPath, path, ec);
        const std::string relativeString = ec ? entryPath.filename().string() : relative.string();
        for (unsigned char c : relativeString) {
            fingerprint ^= static_cast<std::uintmax_t>(c);
            fingerprint *= 1099511628211ull;
        }
        if (it->is_regular_file(ec)) {
            fingerprint ^= fileSizeOrZero(entryPath);
            fingerprint *= 1099511628211ull;
        }
        ec.clear();
    }
    return fingerprint;
}

static json benchCacheMetadataToJson(const BenchCacheMetadata& metadata) {
    return {
        {"dataset", metadata.dataset},
        {"dataset_path", metadata.datasetPath},
        {"corpus_size", metadata.corpusSize},
        {"num_queries", metadata.numQueries},
        {"top_k", metadata.topK},
        {"use_beir", metadata.useBEIR},
        {"vectors_disabled", metadata.vectorsDisabled},
        {"require_kg_ready", metadata.requireKgReady},
        {"graph_rerank_requested", metadata.graphRerankRequested},
        {"expected_docs", metadata.expectedDocs},
        {"expected_queries", metadata.expectedQueries},
        {"corpus_fingerprint", std::to_string(metadata.corpusFingerprint)},
        {"status", metadata.status},
        {"embedded_docs", metadata.embeddedDocs},
        {"vector_count", std::to_string(metadata.vectorCount)},
    };
}

static std::optional<BenchCacheMetadata> parseBenchCacheMetadata(const json& j) {
    if (!j.is_object()) {
        return std::nullopt;
    }

    BenchCacheMetadata metadata;
    metadata.dataset = j.value("dataset", "synthetic");
    metadata.datasetPath = j.value("dataset_path", "");
    metadata.corpusSize = j.value("corpus_size", 0);
    metadata.numQueries = j.value("num_queries", 0);
    metadata.topK = j.value("top_k", 0);
    metadata.useBEIR = j.value("use_beir", false);
    metadata.vectorsDisabled = j.value("vectors_disabled", false);
    metadata.requireKgReady = j.value("require_kg_ready", false);
    metadata.graphRerankRequested = j.value("graph_rerank_requested", false);
    metadata.expectedDocs = j.value("expected_docs", 0);
    metadata.expectedQueries = j.value("expected_queries", 0);
    metadata.status = j.value("status", std::string("priming"));
    metadata.embeddedDocs = j.value("embedded_docs", 0);

    const auto fingerprintString = j.value("corpus_fingerprint", std::string("0"));
    try {
        metadata.corpusFingerprint = static_cast<std::uintmax_t>(std::stoull(fingerprintString));
    } catch (...) {
        metadata.corpusFingerprint = 0;
    }

    const auto vectorCountString = j.value("vector_count", std::string("0"));
    try {
        metadata.vectorCount = static_cast<std::uintmax_t>(std::stoull(vectorCountString));
    } catch (...) {
        metadata.vectorCount = 0;
    }

    return metadata;
}

static Result<void> writeBenchCacheMetadata(const fs::path& warmDataDir,
                                            const BenchCacheMetadata& metadata) {
    std::error_code ec;
    fs::create_directories(warmDataDir, ec);
    if (ec) {
        return Error{ErrorCode::IOError, "Failed to create warm data dir '" + warmDataDir.string() +
                                             "': " + ec.message()};
    }

    const fs::path metadataPath = warmDataDir / "retrieval_bench_cache.json";
    std::ofstream out(metadataPath, std::ios::out | std::ios::trunc);
    if (!out.is_open()) {
        return Error{ErrorCode::IOError,
                     "Failed to write warm cache metadata: " + metadataPath.string()};
    }
    out << benchCacheMetadataToJson(metadata).dump(2) << "\n";
    return {};
}

static Result<BenchCacheMetadata> readBenchCacheMetadata(const fs::path& warmDataDir) {
    const fs::path metadataPath = warmDataDir / "retrieval_bench_cache.json";
    if (!fs::exists(metadataPath)) {
        return Error{ErrorCode::NotFound, "Warm cache metadata missing: " + metadataPath.string()};
    }

    std::ifstream in(metadataPath);
    if (!in.is_open()) {
        return Error{ErrorCode::IOError,
                     "Failed to read warm cache metadata: " + metadataPath.string()};
    }

    try {
        json j;
        in >> j;
        auto parsed = parseBenchCacheMetadata(j);
        if (!parsed.has_value()) {
            return Error{ErrorCode::InvalidData,
                         "Warm cache metadata is not a valid object: " + metadataPath.string()};
        }
        return *parsed;
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData, "Failed to parse warm cache metadata '" +
                                                 metadataPath.string() + "': " + e.what()};
    }
}

static bool benchCacheMatches(const BenchCacheMetadata& expected, const BenchCacheMetadata& actual,
                              std::string* mismatchReason) {
    auto mismatch = [&](const std::string& reason) {
        if (mismatchReason) {
            *mismatchReason = reason;
        }
        return false;
    };

    const auto datasetPathMatches = [&]() {
        if (expected.datasetPath.empty() || actual.datasetPath.empty()) {
            return true;
        }
        return expected.datasetPath == actual.datasetPath;
    };

    if (expected.dataset != actual.dataset) {
        return mismatch("dataset mismatch");
    }
    if (!datasetPathMatches()) {
        return mismatch("dataset_path mismatch");
    }
    if (expected.corpusSize != actual.corpusSize) {
        return mismatch("corpus_size mismatch");
    }
    if (expected.useBEIR != actual.useBEIR) {
        return mismatch("use_beir mismatch");
    }
    if (expected.vectorsDisabled != actual.vectorsDisabled) {
        return mismatch("vectors_disabled mismatch");
    }
    if (expected.expectedDocs != actual.expectedDocs) {
        return mismatch("expected_docs mismatch");
    }
    if (expected.corpusFingerprint != 0 && actual.corpusFingerprint != 0 &&
        expected.corpusFingerprint != actual.corpusFingerprint) {
        return mismatch("corpus_fingerprint mismatch");
    }

    if (!(actual.status == "primed" || actual.status == "partial")) {
        return mismatch("cache status not reusable");
    }

    return true;
}

static int benchCacheStatusRank(std::string_view status) {
    if (status == "primed") {
        return 2;
    }
    if (status == "partial") {
        return 1;
    }
    return 0;
}

static BenchCacheMetadata
preferStrongerBenchCacheMetadata(const BenchCacheMetadata& candidate,
                                 const std::optional<BenchCacheMetadata>& prior) {
    if (!prior.has_value()) {
        return candidate;
    }

    std::string mismatchReason;
    if (!benchCacheMatches(candidate, *prior, &mismatchReason)) {
        return candidate;
    }

    const int candidateRank = benchCacheStatusRank(candidate.status);
    const int priorRank = benchCacheStatusRank(prior->status);
    if (priorRank > candidateRank) {
        return *prior;
    }
    if (priorRank == candidateRank) {
        if (prior->embeddedDocs > candidate.embeddedDocs) {
            return *prior;
        }
        if (prior->embeddedDocs == candidate.embeddedDocs &&
            prior->vectorCount > candidate.vectorCount) {
            return *prior;
        }
    }

    return candidate;
}

static std::string canonicalPathOrEmpty(const fs::path& path) {
    if (path.empty()) {
        return "";
    }
    std::error_code ec;
    const auto canonical = fs::weakly_canonical(path, ec);
    if (!ec) {
        return canonical.string();
    }
    return path.lexically_normal().string();
}

static BenchCacheMetadata currentBenchCacheMetadata(const BenchCacheMetadata& base,
                                                    const yams::daemon::StatusResponse& status) {
    BenchCacheMetadata metadata = base;
    const auto getCount = [&status](const std::string& key) -> uint64_t {
        auto it = status.requestCounts.find(key);
        return (it == status.requestCounts.end()) ? 0ULL : it->second;
    };

    metadata.embeddedDocs = static_cast<int>(getCount("documents_embedded"));
    metadata.vectorCount = static_cast<std::uintmax_t>(getCount("vector_count"));

    const bool queueDrained = getCount("embed_svc_queued") == 0 && getCount("embed_in_flight") == 0;
    const bool fullCoverage =
        metadata.embeddedDocs >= metadata.expectedDocs && metadata.expectedDocs > 0;
    metadata.status = (queueDrained && fullCoverage) ? "primed" : "partial";
    return metadata;
}

static fs::path makeCandidateDebugPath(const fs::path& base, const std::string& candidate,
                                       const std::string& runId) {
    const auto safeCandidate = sanitizeLabelForFilename(candidate);
    const auto stem = base.stem().string();
    const auto ext = base.extension().string();
    const auto suffix = "_" + safeCandidate + "_" + runId;
    if (!stem.empty()) {
        return base.parent_path() / (stem + suffix + ext);
    }
    return base.parent_path() / (base.filename().string() + suffix);
}

static bool hasOverrideKey(const std::vector<EnvSetting>& settings, const std::string& key) {
    return std::any_of(settings.begin(), settings.end(),
                       [&](const EnvSetting& setting) { return setting.key == key; });
}

static std::vector<OptimizationCandidate> defaultOptimizationCandidates() {
    return {
        {"auto_baseline",
         "Auto tuner baseline with graph rerank enabled",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_DISABLE_SEARCH_REBUILDS", "0"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", std::nullopt},
             {"YAMS_CANDIDATE_MULTIPLIER", std::nullopt},
             {"YAMS_FUSION_STRATEGY", std::nullopt},
         }},
        {"mixed_precision_profile",
         "MIXED precision preset with lexical and vector guardrails",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
         }},
        {"mixed_precision_semantic_recall_v1",
         "MIXED_PRECISION + looser vector gate + extra semantic rescue",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
         }},
        {"mixed_precision_semantic_recall_lexical14_v1",
         "Semantic recall v1 + stronger lexical floor/tiebreak guardrails",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
         }},
        {"mixed_default",
         "Code/mixed tuned state with benchmark defaults",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
         }},
        {"mixed_precision_lexical_floor",
         "MIXED defaults + lexical floor to preserve top text hits",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "8"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.12"},
         }},
        {"mixed_precision_path_dedup",
         "MIXED defaults + path-based fusion dedup",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_PATH_DEDUP", "1"},
         }},
        {"mixed_precision_lexical_floor_path_dedup",
         "MIXED defaults + lexical floor + path dedup",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_PATH_DEDUP", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "8"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.12"},
         }},
        {"mixed_precision_lexical_floor_strong",
         "MIXED defaults + stronger lexical floor",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "12"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.20"},
         }},
        {"mixed_precision_lexical_floor_path_dedup_strong",
         "MIXED defaults + strong lexical floor + path dedup",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_PATH_DEDUP", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "12"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.20"},
         }},
        {"mixed_precision_lexical_floor_path_dedup_aggressive",
         "MIXED defaults + aggressive lexical floor + path dedup",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_PATH_DEDUP", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "20"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.30"},
         }},
        {"mixed_precision_lexical_floor_path_dedup_vector_dampen",
         "MIXED defaults + lexical floor + path dedup + lower vector influence",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_PATH_DEDUP", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "12"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.20"},
             {"YAMS_SEARCH_VECTOR_WEIGHT", "0.18"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.94"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.70"},
         }},
        {"mixed_precision_lexical_floor_strong_tiebreak",
         "MIXED stronger lexical floor with lexical tie-break",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "12"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.20"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
         }},
        {"mixed_precision_lexical_floor_path_dedup_strong_tiebreak",
         "MIXED strong lexical floor + path dedup + lexical tie-break",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_PATH_DEDUP", "1"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "12"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.20"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
         }},
        {"small_code_precision",
         "Code-heavy state with stronger lexical/entity bias",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "SMALL_CODE"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
         }},
        {"mixed_recall_push",
         "MIXED state with larger candidate pool and no vector skip",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_CANDIDATE_MULTIPLIER", "2.0"},
         }},
        {"mixed_weighted_rrf",
         "MIXED state with weighted reciprocal fusion",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
         }},
        {"mixed_precision_guardrails",
         "MIXED + weighted reciprocal with vector-only precision guardrails",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "10"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.93"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.70"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.07"},
         }},
        {"mixed_precision_mrr_recovery_rrf",
         "MIXED weighted reciprocal with softer vector-only guardrails",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.91"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.82"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"mixed_precision_rrf_neighbor_t92_p80_k14",
         "MIXED weighted reciprocal near-neighbor: stricter threshold, lower RRF k",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "14"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.80"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"mixed_precision_rrf_neighbor_t92_p78_k16",
         "MIXED weighted reciprocal near-neighbor: stricter threshold and penalty",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.78"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"mixed_precision_rrf_neighbor_t90_p84_k18",
         "MIXED weighted reciprocal near-neighbor: looser threshold, higher RRF k",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "18"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.90"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.84"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"mixed_precision_rrf_rerank10",
         "MIXED weighted reciprocal with broader rerank window",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.91"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.82"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "10"},
         }},
        {"mixed_ablation_disable_vector_gate",
         "Ablation: disable vector-only threshold/penalty",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.00"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "1.00"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"mixed_ablation_disable_intent_adaptive",
         "Ablation: disable query intent weighting",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.91"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.82"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
             {"YAMS_SEARCH_ENABLE_INTENT_ADAPTIVE", "0"},
         }},
        {"mixed_ablation_disable_tiered_execution",
         "Ablation: disable tiered execution, run flat fanout",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.91"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.82"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
             {"YAMS_SEARCH_ENABLE_TIERED_EXECUTION", "0"},
         }},
        {"mixed_ablation_disable_graph_rerank",
         "Ablation: disable graph reranking",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "0"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.91"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.82"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"mixed_ablation_disable_reranker",
         "Ablation: disable cross-encoder reranking stage",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "WEIGHTED_RECIPROCAL"},
             {"YAMS_SEARCH_RRF_K", "16"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.91"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.82"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.05"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "0"},
         }},
        {"mixed_precision_mrr_recovery_mnz",
         "MIXED CombMNZ with moderate vector-only precision guardrails",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_FUSION_STRATEGY", "COMB_MNZ"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.78"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.06"},
             {"YAMS_SEARCH_GRAPH_RERANK_WEIGHT", "0.12"},
         }},
        {"scientific_precision_guardrails",
         "SCIENTIFIC state with strict vector-only thresholding",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "SCIENTIFIC"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "0"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_RRF_K", "10"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.94"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.68"},
             {"YAMS_SEARCH_CONCEPT_BOOST_WEIGHT", "0.08"},
         }},
        {"mixed_graph_off",
         "MIXED state with graph rerank disabled",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "0"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
         }},

        // ── Tier-1 diagnostic candidates ──────────────────────────────
        // Purpose: determine if the 14 both-miss queries are recoverable
        // by expanding the candidate pool and/or the evaluation window.

        {"diag_winner_3x_candidates",
         "Winner config + 3x candidate multiplier (vector pool 150->450)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
         },
         false,
         true},
        {"diag_winner_topk20",
         "Winner config + topK=20 (recall@20 diagnostic)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_BENCH_TOPK", "20"},
         },
         false,
         true},
        {"diag_winner_topk50",
         "Winner config + topK=50 (recall@50 diagnostic)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_BENCH_TOPK", "50"},
         },
         false,
         true},
        {"diag_winner_3x_topk50",
         "Winner + 3x candidates + topK=50 (maximum retrieval diagnostic)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_BENCH_TOPK", "50"},
         },
         false,
         true},

        // ── Tier-2 cross-encoder reranker candidates ──────────────────
        // Strategy: expand pool (3x) to get ~50 candidates, then use the
        // bge-reranker-large cross-encoder to rescore and promote relevant
        // documents that the bi-encoder ranked at positions 11-49 into
        // the top-10.  Tier-1 diagnostics showed 7 of 14 both-miss
        // queries have relevant docs at ranks 11-49 — reranking should
        // recover these.
        // Current benchmark evidence: blend-60 beats blend-70 on both q20 and q300,
        // while graph rerank / semantic rescue stay effectively inactive on SciFact.
        // Treat rerank_3x_blend60_top50 as the engine-policy baseline; heavier recall probes
        // below remain diagnostics unless they win on both quality and latency.

        {"rerank_3x_replace_top50",
         "3x pool + cross-encoder rerank top-50, replace scores (max reranker impact)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-50 candidates
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "1"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend60_top50",
         "Current q300 winner: 3x pool + cross-encoder rerank top-50, blend 60/40 with fusion "
         "scores",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-50, blend scores
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         },
         true,
         false},
        {"rerank_4x_blend60_top50",
         "Pre-fusion recall probe: winner + larger candidate multiplier (4x), rerank window stays "
         "at top-50",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "4.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         },
         false,
         true},
        {"rerank_3x_blend60_top50_fuse100",
         "Pre-fusion recall probe: winner + moderately wider fusion window (fusion limit 100)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_FUSION_CANDIDATE_LIMIT", "100"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         },
         false,
         true},
        {"rerank_3x_blend60_top50_lexical16",
         "Pre-fusion recall probe: winner + milder lexical floor (top-16 / +0.24)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "16"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.24"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         },
         false,
         true},
        {"rerank_3x_blend60_top50_fuse200",
         "Pre-fusion recall probe: winner + wider fusion window (fusion limit 200), rerank window "
         "stays at top-50",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_FUSION_CANDIDATE_LIMIT", "200"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend60_top50_lexical20",
         "Pre-fusion recall probe: winner + stronger lexical floor (top-20 / +0.30)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "20"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.30"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend40_top50",
         "3x pool + cross-encoder rerank top-50, blend 40/60 (conservative)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-50, conservative blend
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.40"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_replace_top50_snippet512",
         "3x pool + cross-encoder rerank top-50, replace scores, 512-char snippets",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank top-50, larger snippets for more context
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "1"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "512"},
         }},
        // --- Faster rerank candidates (top-20) for quicker iteration ---
        {"rerank_3x_replace_top20",
         "3x pool + cross-encoder rerank top-20, replace scores",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "1"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend40_top20",
         "3x pool + cross-encoder rerank top-20, blend 40/60 (conservative)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.40"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend60_top20",
         "3x pool + cross-encoder rerank top-20, blend 60/40",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},

        // Adaptive blend: scale reranker weight by max-score confidence
        {"rerank_3x_adaptive80_top20",
         "3x pool + cross-encoder rerank top-20, adaptive blend (base 0.80, floor 0.10)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.80"},
             {"YAMS_SEARCH_RERANK_ADAPTIVE_BLEND", "1"},
             {"YAMS_SEARCH_RERANK_ADAPTIVE_BLEND_FLOOR", "0.10"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},

        {"rerank_3x_adaptive60_top20",
         "3x pool + cross-encoder rerank top-20, adaptive blend (base 0.60, floor 0.05)",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.60"},
             {"YAMS_SEARCH_RERANK_ADAPTIVE_BLEND", "1"},
             {"YAMS_SEARCH_RERANK_ADAPTIVE_BLEND_FLOOR", "0.05"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},

        {"rerank_3x_blend70_top20",
         "3x pool + cross-encoder rerank top-20, blend 70/30 with fusion scores",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-20, stronger blend
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.70"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend80_top20",
         "3x pool + cross-encoder rerank top-20, blend 80/20 with fusion scores",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-20, aggressive blend
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "20"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.80"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},
        {"rerank_3x_blend70_top50",
         "3x pool + cross-encoder rerank top-50, blend 70/30 with fusion scores",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-50, strong blend
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.70"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
         }},

        // Wide fusion window: pass many more fused results through the reranker
        {"rerank_3x_blend70_top200_fuse200",
         "3x pool + cross-encoder rerank top-200, blend 70/30, fusion window 200",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             // Cross-encoder reranking: rerank the top-200, strong blend
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "200"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.70"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
             // Wider fusion window: keep 200 fused results (vs default 50)
             {"YAMS_SEARCH_FUSION_CANDIDATE_LIMIT", "200"},
         }},

        // ── Tier 3: query expansion before rerank ───────────────────────────

        {"rerank_3x_blend70_top50_multivec",
         "3x pool + rerank top-50, blend 70/30, multi-vector sub-phrase search",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.70"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
             {"YAMS_SEARCH_MULTI_VECTOR_QUERY", "1"},
             {"YAMS_SEARCH_MULTI_VECTOR_MAX_PHRASES", "3"},
             {"YAMS_SEARCH_MULTI_VECTOR_SCORE_DECAY", "0.85"},
         }},

        {"rerank_3x_blend70_top50_subphrase_fts",
         "3x pool + rerank top-50, blend 70/30, sub-phrase FTS expansion",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.70"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
             {"YAMS_SEARCH_SUB_PHRASE_EXPANSION", "1"},
             {"YAMS_SEARCH_SUB_PHRASE_MIN_HITS", "5"},
             {"YAMS_SEARCH_SUB_PHRASE_PENALTY", "0.70"},
         }},

        {"rerank_3x_blend70_top50_multivec_subphrase",
         "3x pool + rerank top-50, blend 70/30, multi-vector + sub-phrase FTS",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.70"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
             {"YAMS_SEARCH_MULTI_VECTOR_QUERY", "1"},
             {"YAMS_SEARCH_MULTI_VECTOR_MAX_PHRASES", "3"},
             {"YAMS_SEARCH_MULTI_VECTOR_SCORE_DECAY", "0.85"},
             {"YAMS_SEARCH_SUB_PHRASE_EXPANSION", "1"},
             {"YAMS_SEARCH_SUB_PHRASE_MIN_HITS", "5"},
             {"YAMS_SEARCH_SUB_PHRASE_PENALTY", "0.70"},
         }},

        {"rerank_3x_blend50_top50_multivec_subphrase",
         "3x pool + rerank top-50, blend 50/50, multi-vector + sub-phrase FTS",
         {
             {"YAMS_ENABLE_ENV_OVERRIDES", "1"},
             {"YAMS_BENCH_FORCE_TUNING_OVERRIDE", std::nullopt},
             {"YAMS_TUNING_OVERRIDE", "MIXED_PRECISION"},
             {"YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1"},
             {"YAMS_SEARCH_VECTOR_ONLY_THRESHOLD", "0.92"},
             {"YAMS_SEARCH_VECTOR_ONLY_PENALTY", "0.65"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_SLOTS", "2"},
             {"YAMS_SEARCH_SEMANTIC_RESCUE_MIN_VECTOR_SCORE", "0.30"},
             {"YAMS_SEARCH_ENABLE_ADAPTIVE_FALLBACK", "0"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_TOPN", "14"},
             {"YAMS_SEARCH_LEXICAL_FLOOR_BOOST", "0.22"},
             {"YAMS_SEARCH_ENABLE_LEXICAL_TIEBREAK", "1"},
             {"YAMS_SEARCH_LEXICAL_TIEBREAK_EPS", "0.010"},
             {"YAMS_CANDIDATE_MULTIPLIER", "3.0"},
             {"YAMS_SEARCH_ENABLE_RERANKING", "1"},
             {"YAMS_SEARCH_RERANK_TOPK", "50"},
             {"YAMS_SEARCH_RERANK_REPLACE_SCORES", "0"},
             {"YAMS_SEARCH_RERANK_WEIGHT", "0.50"},
             {"YAMS_SEARCH_RERANK_SCORE_GAP_THRESHOLD", "0.0"},
             {"YAMS_SEARCH_RERANK_SNIPPET_MAX_CHARS", "256"},
             {"YAMS_SEARCH_MULTI_VECTOR_QUERY", "1"},
             {"YAMS_SEARCH_MULTI_VECTOR_MAX_PHRASES", "3"},
             {"YAMS_SEARCH_MULTI_VECTOR_SCORE_DECAY", "0.85"},
             {"YAMS_SEARCH_SUB_PHRASE_EXPANSION", "1"},
             {"YAMS_SEARCH_SUB_PHRASE_MIN_HITS", "5"},
             {"YAMS_SEARCH_SUB_PHRASE_PENALTY", "0.70"},
         }},
    };
}

static std::set<std::string> parseCandidateFilter(const char* raw) {
    std::set<std::string> selected;
    if (!raw || !*raw) {
        return selected;
    }

    std::string value(raw);
    std::stringstream ss(value);
    std::string token;
    while (std::getline(ss, token, ',')) {
        auto trim = [](std::string& s) {
            const auto first = s.find_first_not_of(" \t\n\r");
            if (first == std::string::npos) {
                s.clear();
                return;
            }
            const auto last = s.find_last_not_of(" \t\n\r");
            s = s.substr(first, last - first + 1);
        };
        trim(token);
        if (!token.empty()) {
            selected.insert(token);
        }
    }
    return selected;
}

enum class OptimizationProfile {
    EnginePolicy,
    Diagnostics,
    All,
};

static OptimizationProfile parseOptimizationProfile(const char* raw) {
    if (!raw || !*raw) {
        return OptimizationProfile::EnginePolicy;
    }

    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if (value == "diagnostic" || value == "diagnostics" || value == "probe" || value == "probes") {
        return OptimizationProfile::Diagnostics;
    }
    if (value == "all") {
        return OptimizationProfile::All;
    }
    return OptimizationProfile::EnginePolicy;
}

static const char* optimizationProfileName(OptimizationProfile profile) {
    switch (profile) {
        case OptimizationProfile::EnginePolicy:
            return "engine-policy";
        case OptimizationProfile::Diagnostics:
            return "diagnostics";
        case OptimizationProfile::All:
            return "all";
    }
    return "engine-policy";
}

static double computeHybridRegressionPenalty(const RetrievalMetrics& hybrid,
                                             const RetrievalMetrics& keyword) {
    const double hybridMrrLoss = std::max(0.0, keyword.mrr - hybrid.mrr);
    return std::min(0.12, hybridMrrLoss * 2.0);
}

static double computeDuplicatePenalty(const RetrievalMetrics& hybrid) {
    return std::min(0.08, std::max(0.0, hybrid.duplicateRateAtK) * 0.30);
}

static double computeLatencyPenalty(double avgHybridQueryMs) {
    // Keep latency meaningful in the 3-7s/query range seen in real SciFact runs.
    // The previous cap saturated far too early and made materially slower candidates
    // look effectively free once they crossed ~800ms/query.
    return std::clamp(std::max(0.0, avgHybridQueryMs) / 25000.0, 0.0, 0.25);
}

static std::string summarizeTimingTradeoffs(const json& hybridDiag) {
    if (!hybridDiag.is_object()) {
        return "timings=unavailable";
    }
    const auto timingsIt = hybridDiag.find("timings_ms");
    if (timingsIt == hybridDiag.end() || !timingsIt->is_object()) {
        return "timings=unavailable";
    }

    const json& timings = *timingsIt;
    std::vector<std::pair<std::string, double>> ranked;
    ranked.reserve(timings.size());
    for (const auto& [name, stats] : timings.items()) {
        if (!stats.is_object()) {
            continue;
        }
        auto meanIt = stats.find("mean");
        if (meanIt == stats.end() || !meanIt->is_number()) {
            continue;
        }
        ranked.emplace_back(name, meanIt->get<double>());
    }

    if (ranked.empty()) {
        return "timings=unavailable";
    }

    std::sort(ranked.begin(), ranked.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(1);
    const size_t limit = std::min<size_t>(3, ranked.size());
    for (size_t i = 0; i < limit; ++i) {
        if (i > 0) {
            oss << "  ";
        }
        oss << ranked[i].first << '=' << ranked[i].second << "ms";
    }
    return oss.str();
}

static double computeOptimizationObjective(const RetrievalMetrics& hybrid,
                                           const RetrievalMetrics& keyword,
                                           double avgHybridQueryMs) {
    // Accuracy-first, but keep latency meaningful so recall probes do not win by default when
    // they materially slow search without improving quality.
    const double quality =
        0.40 * hybrid.mrr + 0.30 * hybrid.ndcgAtK + 0.20 * hybrid.recallAtK + 0.10 * hybrid.map;
    const double hybridGain = std::max(0.0, hybrid.mrr - keyword.mrr);
    const double regressionPenalty = computeHybridRegressionPenalty(hybrid, keyword);
    const double duplicatePenalty = computeDuplicatePenalty(hybrid);
    const double latencyPenalty = computeLatencyPenalty(avgHybridQueryMs);
    return quality + 0.10 * hybridGain - regressionPenalty - duplicatePenalty - latencyPenalty;
}

static void appendOptimizationResultJson(const fs::path& outputFile,
                                         const OptimizationRunResult& result) {
    std::ofstream out(outputFile, std::ios::out | std::ios::app);
    if (!out.is_open()) {
        spdlog::warn("[OptLoop] Failed to open results file: {}", outputFile.string());
        return;
    }

    json j;
    j["candidate"] = result.candidate.name;
    j["description"] = result.candidate.description;
    j["success"] = result.success;
    j["objective"] = result.objectiveScore;
    j["hybrid_eval_ms"] = result.hybridEvalMs;
    j["keyword_eval_ms"] = result.keywordEvalMs;
    j["tuning_state"] = result.tuningState;
    j["tuning_reason"] = result.tuningReason;
    j["error"] = result.errorMessage;
    j["run_id"] = result.runId;
    j["debug_file"] = result.debugFile;
    j["trace_top_n"] = result.traceTopN;
    j["trace_component_top_n"] = result.traceComponentTopN;
    j["stage_trace_enabled"] = result.stageTraceEnabled;
    j["hybrid_keyword_mrr_delta"] = result.hybridMetrics.mrr - result.keywordMetrics.mrr;

    const double objectiveRegressionPenalty =
        computeHybridRegressionPenalty(result.hybridMetrics, result.keywordMetrics);
    const double objectiveDuplicatePenalty = computeDuplicatePenalty(result.hybridMetrics);
    j["objective_penalties"] = {
        {"hybrid_regression", objectiveRegressionPenalty},
        {"duplicate_rate", objectiveDuplicatePenalty},
    };

    j["hybrid"] = {
        {"mrr", result.hybridMetrics.mrr},
        {"recall_at_k", result.hybridMetrics.recallAtK},
        {"precision_at_k", result.hybridMetrics.precisionAtK},
        {"ndcg_at_k", result.hybridMetrics.ndcgAtK},
        {"map", result.hybridMetrics.map},
        {"duplicate_rate_at_k", result.hybridMetrics.duplicateRateAtK},
        {"num_queries", result.hybridMetrics.numQueries},
    };

    j["keyword"] = {
        {"mrr", result.keywordMetrics.mrr},
        {"recall_at_k", result.keywordMetrics.recallAtK},
        {"precision_at_k", result.keywordMetrics.precisionAtK},
        {"ndcg_at_k", result.keywordMetrics.ndcgAtK},
        {"map", result.keywordMetrics.map},
        {"duplicate_rate_at_k", result.keywordMetrics.duplicateRateAtK},
        {"num_queries", result.keywordMetrics.numQueries},
    };

    j["hybrid_debug_summary"] = queryDiagnosticsToJson(result.hybridDiagnostics);
    j["keyword_debug_summary"] = queryDiagnosticsToJson(result.keywordDiagnostics);

    json env = json::object();
    for (const auto& setting : result.candidate.envOverrides) {
        if (setting.value.has_value()) {
            env[setting.key] = *setting.value;
        } else {
            env[setting.key] = nullptr;
        }
    }
    j["env_overrides"] = env;

    out << j.dump() << "\n";
}

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
                                 const std::string& searchType = "hybrid",
                                 QueryDiagnosticsSummary* diagnostics = nullptr) {
    RetrievalMetrics metrics;
    metrics.numQueries = static_cast<int>(queries.size());
    double totalMRR = 0.0, totalRecall = 0.0, totalPrecision = 0.0, totalNDCG = 0.0, totalMAP = 0.0;

    std::uint64_t totalAttempts = 0;
    std::uint64_t streamingCount = 0;
    std::uint64_t fuzzyRetryCount = 0;
    std::uint64_t literalRetryCount = 0;
    std::uint64_t duplicateTopKCount = 0;
    std::uint64_t totalTopKCount = 0;

    for (std::size_t queryIndex = 0; queryIndex < queries.size(); ++queryIndex) {
        const auto& tq = queries[queryIndex];
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
        const bool benchVerbose = []() -> bool {
            if (const char* env = std::getenv("YAMS_BENCH_VERBOSE"); env) {
                std::string value(env);
                std::transform(value.begin(), value.end(), value.begin(), [](char c) {
                    return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
                });
                return value == "1" || value == "true" || value == "yes" || value == "on";
            }
            return false;
        }();

        if (benchVerbose) {
            spdlog::debug("Executing search query: '{}'", tq.query);
        }
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
        if (benchVerbose) {
            spdlog::debug("Search returned {} results for query '{}' (attempts={}, streaming={}, "
                          "fuzzy_retry={}, literal_retry={})",
                          results.size(), tq.query, run.value().attempts, run.value().usedStreaming,
                          run.value().usedFuzzyRetry, run.value().usedLiteralTextRetry);
        }

        // Detailed result logging for debugging retrieval quality
        if (benchDiagEnabled) {
            spdlog::debug("  [{}] Expected relevant: {}", searchType,
                          fmt::join(tq.relevantDocIds, ", "));
            for (size_t i = 0; i < std::min((size_t)5, results.size()); ++i) {
                std::string filename = fs::path(results[i].path).filename().string();
                std::string docId = filename;
                if (docId.size() > 4 && docId.substr(docId.size() - 4) == ".txt") {
                    docId = docId.substr(0, docId.size() - 4);
                }
                bool relevant = tq.relevantDocIds.count(docId) > 0;
                spdlog::debug("  [{}] Result {}: path='{}' docId='{}' score={:.4f} {}", searchType,
                              i, results[i].path, docId, results[i].score,
                              relevant ? "RELEVANT" : "");
            }
        }

        DebugLogEntry debugEntry;
        debugEntry.query = tq.query;
        debugEntry.queryIndex = static_cast<int>(queryIndex);
        debugEntry.searchType = searchType;
        debugEntry.attempts = run.value().attempts;
        debugEntry.usedStreaming = run.value().usedStreaming;
        debugEntry.usedFuzzyFlag = false;
        debugEntry.usedLiteralFlag = false;
        debugEntry.usedFuzzyRetry = run.value().usedFuzzyRetry;
        debugEntry.usedLiteralTextRetry = run.value().usedLiteralTextRetry;
        debugEntry.searchStats = run.value().response.searchStats;
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
        bool hadTopKDuplicate = false;
        std::unordered_set<std::string> seenTopKDocIds;
        seenTopKDocIds.reserve(std::min((size_t)k, results.size()));

        for (size_t i = 0; i < std::min((size_t)k, results.size()); ++i) {
            debugEntry.returnedPaths.push_back(results[i].path);
            debugEntry.returnedScores.push_back(static_cast<float>(results[i].score));

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

            totalTopKCount++;
            const bool isDuplicate = !seenTopKDocIds.insert(key).second;
            if (isDuplicate) {
                hadTopKDuplicate = true;
                duplicateTopKCount++;
                debugEntry.diagnostics.push_back("duplicate_topk_doc=" + key);
            }

            if (isRelevant && !isDuplicate) {
                numRelevantInTopK++;
                if (firstRelevantRank < 0)
                    firstRelevantRank = i + 1;
                numRelevantSeen++;
                avgPrecision += (double)numRelevantSeen / (i + 1);
            }
            auto gradeIt = tq.relevanceGrades.find(key);
            auto grade =
                (!isDuplicate && gradeIt != tq.relevanceGrades.end()) ? gradeIt->second : 0;
            retrievedGrades.push_back(grade);
            debugEntry.returnedGrades.push_back(grade);
        }

        const json relevantDecisionTrace = buildRelevantDecisionTrace(
            debugEntry.relevantDocIds, debugEntry.returnedDocIds, debugEntry.searchStats);
        debugEntry.extraFields["relevant_decision_trace"] = relevantDecisionTrace;

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
                shadowEntry.queryIndex = static_cast<int>(queryIndex);
                shadowEntry.searchType = searchType;
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

        if (diagnostics) {
            ingestQueryDiagnostics(*diagnostics, run.value().response.searchStats,
                                   relevantDecisionTrace, false, !results.empty(), hadTopKDuplicate,
                                   firstRelevantRank);
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
        if (totalTopKCount > 0) {
            metrics.duplicateRateAtK =
                static_cast<double>(duplicateTopKCount) / static_cast<double>(totalTopKCount);
        }

        spdlog::info("Search execution stats: queries={} total_attempts={} avg_attempts={:.2f} "
                     "streaming={} fuzzy_retries={} literal_retries={} duplicate_rate@k={:.4f}",
                     metrics.numQueries, totalAttempts,
                     static_cast<double>(totalAttempts) / metrics.numQueries, streamingCount,
                     fuzzyRetryCount, literalRetryCount, metrics.duplicateRateAtK);
    }
    return metrics;
}

struct BenchFixture {
    std::unique_ptr<DaemonHarness> harness;
    std::unique_ptr<yams::daemon::DaemonClient> client;
    std::unique_ptr<CorpusGenerator> corpus;
    std::unique_ptr<BEIRCorpusLoader> beirCorpus;
    fs::path benchCorpusDir;
    fs::path warmDataDirPath;
    std::optional<BenchCacheMetadata> warmCacheMetadata;
    std::vector<TestQuery> queries;
    int corpusSize = 50, numQueries = 10, topK = 10;
    bool useBEIR = false;
    bool warmDataDir = false;    // True when reusing a pre-ingested data directory
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
                // Default to SearchTuner auto mode so benchmark reflects dynamic tuning behavior.
                // Keep explicit override support for controlled experiments:
                //   YAMS_BENCH_FORCE_TUNING_OVERRIDE=SCIENTIFIC
                const char* forcedOverride = std::getenv("YAMS_BENCH_FORCE_TUNING_OVERRIDE");
                const char* existingOverride = std::getenv("YAMS_TUNING_OVERRIDE");
                if (forcedOverride && std::strlen(forcedOverride) > 0) {
                    setenv("YAMS_ENABLE_ENV_OVERRIDES", "1", 1);
                    setenv("YAMS_TUNING_OVERRIDE", forcedOverride, 1);
                    spdlog::info(
                        "Set YAMS_TUNING_OVERRIDE={} via YAMS_BENCH_FORCE_TUNING_OVERRIDE for "
                        "BEIR benchmark ({})",
                        forcedOverride, datasetName);
                } else if (existingOverride && std::strlen(existingOverride) > 0) {
                    spdlog::info("Using pre-set YAMS_TUNING_OVERRIDE={} for BEIR benchmark ({})",
                                 existingOverride, datasetName);
                } else {
                    spdlog::info("Using SearchTuner auto mode for BEIR benchmark ({})",
                                 datasetName);
                }
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

        auto optionalPositiveInt = [](const char* raw) -> std::optional<int> {
            if (!(raw && *raw)) {
                return std::nullopt;
            }
            const int parsed = std::stoi(raw);
            if (parsed <= 0) {
                return std::nullopt;
            }
            return parsed;
        };

        const std::optional<int> corpusLimit = optionalPositiveInt(env_size);
        const std::optional<int> queryLimit = optionalPositiveInt(env_queries);

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

        auto envFlagEnabled = [](const char* value) -> bool {
            if (!value) {
                return false;
            }
            std::string normalized(value);
            std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](char c) {
                return static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            });
            return normalized == "1" || normalized == "true" || normalized == "yes" ||
                   normalized == "on";
        };

        auto ensureEnvDefault = [](const char* key, const char* value) {
            const char* current = std::getenv(key);
            if (!(current && std::strlen(current) > 0)) {
                setenv(key, value, 0);
            }
        };

        // Benchmark determinism defaults (can still be overridden by explicit env values).
        ensureEnvDefault("YAMS_DISABLE_SEARCH_REBUILDS", "1");
        ensureEnvDefault("YAMS_SEARCH_WAIT_FOR_CONCEPTS", "1");
        ensureEnvDefault("YAMS_HNSW_RANDOM_SEED", "42");
        ensureEnvDefault("YAMS_HNSW_PARALLEL_BUILD_THRESHOLD", "0");
        // Use higher concurrency for embedding/extraction to reduce ingestion time.
        // HNSW seed + wait-for-concepts provide sufficient determinism for ranking;
        // single-threaded embedding only added wall-clock cost without improving reproducibility.
        ensureEnvDefault("YAMS_POST_EMBED_CONCURRENT", "4");
        ensureEnvDefault("YAMS_POST_EXTRACTION_CONCURRENT", "4");
        ensureEnvDefault("YAMS_POST_KG_CONCURRENT", "1");

        // Adaptive sub-batch tuning: the default 15s warning threshold causes premature
        // batch-cap collapse (8→4→1) on machines where ONNX inference is legitimately slow.
        // Raise to 60s so the adaptive logic only shrinks on true stalls, not normal latency.
        ensureEnvDefault("YAMS_EMBED_SUBBATCH_WARN_MS", "60000");

        // Benchmark default: graph rerank is ON unless explicitly overridden.
        // Canonical env key used by SearchEngineBuilder is YAMS_SEARCH_ENABLE_GRAPH_RERANK.
        const char* graphRerankCanonical = std::getenv("YAMS_SEARCH_ENABLE_GRAPH_RERANK");
        if (!(graphRerankCanonical && *graphRerankCanonical)) {
            setenv("YAMS_SEARCH_ENABLE_GRAPH_RERANK", "1", 0);
            graphRerankCanonical = std::getenv("YAMS_SEARCH_ENABLE_GRAPH_RERANK");
        }

        const bool graphRerankRequested = envFlagEnabled(graphRerankCanonical);
        bool requireKgReady = graphRerankRequested;
        if (const char* envRequireKgReady = std::getenv("YAMS_BENCH_REQUIRE_KG_READY");
            envRequireKgReady && *envRequireKgReady) {
            requireKgReady = envFlagEnabled(envRequireKgReady);
        }

        fs::path datasetPathForMetadata = env_path ? fs::path(env_path) : fs::path();
        Result<BEIRDataset> preparedBeirDataset =
            Error{ErrorCode::Unknown, "BEIR dataset not requested"};
        if (useBEIR) {
            preparedBeirDataset = selectBenchmarkBEIRDataset(
                beirDatasetName, datasetPathForMetadata, corpusLimit, queryLimit);
            if (!preparedBeirDataset) {
                throw std::runtime_error("Failed to load BEIR dataset: " +
                                         preparedBeirDataset.error().message);
            }
            corpusSize = static_cast<int>(preparedBeirDataset.value().documents.size());
            numQueries = static_cast<int>(preparedBeirDataset.value().queries.size());
        }

        BenchCacheMetadata expectedCacheMetadata;
        expectedCacheMetadata.dataset = useBEIR ? beirDatasetName : "synthetic";
        expectedCacheMetadata.datasetPath = canonicalPathOrEmpty(datasetPathForMetadata);
        expectedCacheMetadata.corpusSize = corpusSize;
        expectedCacheMetadata.numQueries = numQueries;
        expectedCacheMetadata.topK = topK;
        expectedCacheMetadata.useBEIR = useBEIR;
        expectedCacheMetadata.vectorsDisabled = vectorsDisabled;
        expectedCacheMetadata.requireKgReady = requireKgReady;
        expectedCacheMetadata.graphRerankRequested = graphRerankRequested;
        expectedCacheMetadata.expectedDocs = corpusSize;
        expectedCacheMetadata.expectedQueries = numQueries;

        spdlog::info("[Bench] Effective graph rerank env: YAMS_SEARCH_ENABLE_GRAPH_RERANK={}",
                     graphRerankCanonical ? graphRerankCanonical : "<unset>");
        spdlog::info(
            "[Bench] Determinism envs: YAMS_DISABLE_SEARCH_REBUILDS={} "
            "YAMS_SEARCH_WAIT_FOR_CONCEPTS={} YAMS_HNSW_RANDOM_SEED={} "
            "YAMS_HNSW_PARALLEL_BUILD_THRESHOLD={} YAMS_POST_EMBED_CONCURRENT={} "
            "YAMS_POST_EXTRACTION_CONCURRENT={} YAMS_POST_KG_CONCURRENT={}",
            std::getenv("YAMS_DISABLE_SEARCH_REBUILDS")
                ? std::getenv("YAMS_DISABLE_SEARCH_REBUILDS")
                : "<unset>",
            std::getenv("YAMS_SEARCH_WAIT_FOR_CONCEPTS")
                ? std::getenv("YAMS_SEARCH_WAIT_FOR_CONCEPTS")
                : "<unset>",
            std::getenv("YAMS_HNSW_RANDOM_SEED") ? std::getenv("YAMS_HNSW_RANDOM_SEED") : "<unset>",
            std::getenv("YAMS_HNSW_PARALLEL_BUILD_THRESHOLD")
                ? std::getenv("YAMS_HNSW_PARALLEL_BUILD_THRESHOLD")
                : "<unset>",
            std::getenv("YAMS_POST_EMBED_CONCURRENT") ? std::getenv("YAMS_POST_EMBED_CONCURRENT")
                                                      : "<unset>",
            std::getenv("YAMS_POST_EXTRACTION_CONCURRENT")
                ? std::getenv("YAMS_POST_EXTRACTION_CONCURRENT")
                : "<unset>",
            std::getenv("YAMS_POST_KG_CONCURRENT") ? std::getenv("YAMS_POST_KG_CONCURRENT")
                                                   : "<unset>");

        DaemonHarness::Options harnessOptions;
        harnessOptions.isolateState = true;
        harnessOptions.useMockModelProvider = vectorsDisabled;
        harnessOptions.autoLoadPlugins = !vectorsDisabled;
        harnessOptions.configureModelPool = !vectorsDisabled;
        harnessOptions.modelPoolLazyLoading = false;
        harnessOptions.pluginDirStrict = !vectorsDisabled;

        // YAMS_BENCH_DATA_DIR: reuse a pre-ingested data directory to skip
        // corpus ingestion + embedding wait on repeated benchmark runs.
        // YAMS_BENCH_WARM_CACHE_DIR: like YAMS_BENCH_DATA_DIR, but validates a metadata file and
        // auto-writes one after a cold run so repeated full-corpus benchmarks are practical.
        const char* envWarmCacheDir = std::getenv("YAMS_BENCH_WARM_CACHE_DIR");
        const char* envDataDir = std::getenv("YAMS_BENCH_DATA_DIR");
        if (envWarmCacheDir && *envWarmCacheDir) {
            fs::path cacheDir(envWarmCacheDir);
            warmDataDirPath = cacheDir;
            if (fs::exists(cacheDir)) {
                auto metadataResult = readBenchCacheMetadata(cacheDir);
                if (metadataResult) {
                    std::string mismatchReason;
                    if (benchCacheMatches(expectedCacheMetadata, metadataResult.value(),
                                          &mismatchReason)) {
                        harnessOptions.dataDir = cacheDir;
                        warmDataDir = true;
                        warmCacheMetadata = metadataResult.value();
                        spdlog::info(
                            "[Bench] Reusing warm cache dir: {} (status={}, embedded_docs={}, "
                            "vectors={})",
                            cacheDir.string(), metadataResult.value().status,
                            metadataResult.value().embeddedDocs,
                            metadataResult.value().vectorCount);
                    } else {
                        std::error_code removeEc;
                        const bool canResumePartial = metadataResult &&
                                                      metadataResult.value().status == "partial" &&
                                                      mismatchReason == "cache status not reusable";
                        if (!canResumePartial) {
                            fs::remove_all(cacheDir, removeEc);
                            if (removeEc) {
                                spdlog::warn("[Bench] Failed to clear stale warm cache dir {}: {}",
                                             cacheDir.string(), removeEc.message());
                            }
                        }
                        harnessOptions.dataDir = cacheDir;
                        if (canResumePartial) {
                            warmDataDir = true;
                            warmCacheMetadata = metadataResult.value();
                            spdlog::warn(
                                "[Bench] Warm cache dir {} is partially primed; reusing it "
                                "to resume embedding completion",
                                cacheDir.string());
                        } else {
                            spdlog::warn(
                                "[Bench] Warm cache dir {} metadata mismatch ({}); running "
                                "cold and refreshing cache",
                                cacheDir.string(), mismatchReason);
                        }
                    }
                } else {
                    harnessOptions.dataDir = cacheDir;
                    spdlog::warn("[Bench] Warm cache dir {} missing/invalid metadata ({}); "
                                 "running cold and refreshing cache",
                                 cacheDir.string(), metadataResult.error().message);
                }
            } else {
                harnessOptions.dataDir = cacheDir;
                spdlog::info("[Bench] Warm cache dir {} does not exist yet; cold run will create "
                             "it",
                             cacheDir.string());
            }
        } else if (envDataDir && *envDataDir) {
            fs::path dataDirPath(envDataDir);
            if (fs::exists(dataDirPath)) {
                harnessOptions.dataDir = dataDirPath;
                warmDataDir = true;
                warmDataDirPath = dataDirPath;
                spdlog::info("[Bench] Reusing warm data dir: {} (skip ingestion + embed wait)",
                             dataDirPath.string());
            } else {
                spdlog::warn("[Bench] YAMS_BENCH_DATA_DIR={} does not exist, ignoring", envDataDir);
            }
        }

        if (!vectorsDisabled) {
            bool resetPluginTrustFile = false;
            bool useIsolatedBenchmarkConfig = false;
            const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR");
            if (envPluginDir) {
                harnessOptions.pluginDir = fs::path(envPluginDir);
            } else {
                const fs::path cwd = fs::current_path();
                const fs::path installedPluginDir = fs::path("/opt/homebrew/lib/yams/plugins");
                const fs::path installedOnnxPlugin =
                    installedPluginDir / "libyams_onnx_plugin.dylib";
                const fs::path localPluginDir = cwd / "plugins";
                const fs::path localOnnxPlugin =
                    localPluginDir / "onnx" / "libyams_onnx_plugin.dylib";

                const fs::path root = cwd.parent_path();
                const fs::path nosanPluginDir = root / "builddir-nosan" / "plugins";
                const fs::path nosanOnnxPlugin =
                    nosanPluginDir / "onnx" / "libyams_onnx_plugin.dylib";
                const fs::path defaultPluginDir = root / "builddir" / "plugins";
                const fs::path defaultOnnxPlugin =
                    defaultPluginDir / "onnx" / "libyams_onnx_plugin.dylib";

                if (fs::exists(installedOnnxPlugin)) {
                    const fs::path stagedPluginDir =
                        fs::temp_directory_path() / "yams_retrieval_bench_plugins";
                    const fs::path stagedOnnxPlugin =
                        stagedPluginDir / installedOnnxPlugin.filename();
                    std::error_code ec;
                    fs::path installedOnnxSource = fs::weakly_canonical(installedOnnxPlugin, ec);
                    if (ec || installedOnnxSource.empty()) {
                        ec.clear();
                        installedOnnxSource = installedOnnxPlugin;
                    }
                    fs::create_directories(stagedPluginDir, ec);
                    if (ec) {
                        spdlog::warn("Failed to create staged plugin dir {}: {}",
                                     stagedPluginDir.string(), ec.message());
                        harnessOptions.pluginDir = installedPluginDir;
                    } else {
                        fs::remove(stagedOnnxPlugin, ec);
                        ec.clear();
                        fs::copy_file(installedOnnxSource, stagedOnnxPlugin,
                                      fs::copy_options::overwrite_existing, ec);
                        if (ec) {
                            spdlog::warn("Failed to stage installed ONNX plugin {} -> {}: {}",
                                         installedOnnxSource.string(), stagedOnnxPlugin.string(),
                                         ec.message());
                            harnessOptions.pluginDir = installedPluginDir;
                        } else {
                            harnessOptions.pluginDir = stagedPluginDir;
                            resetPluginTrustFile = true;
                            useIsolatedBenchmarkConfig = true;
                            spdlog::info("Using isolated installed ONNX plugin dir: {}",
                                         stagedPluginDir.string());
                        }
                    }
                } else if (fs::exists(localOnnxPlugin)) {
                    harnessOptions.pluginDir = localOnnxPlugin.parent_path();
                } else if (fs::exists(nosanOnnxPlugin)) {
                    harnessOptions.pluginDir = nosanOnnxPlugin.parent_path();
                } else if (fs::exists(defaultOnnxPlugin)) {
                    harnessOptions.pluginDir = defaultOnnxPlugin.parent_path();
                } else if (fs::exists(localPluginDir)) {
                    harnessOptions.pluginDir = localPluginDir;
                } else if (fs::exists(nosanPluginDir)) {
                    harnessOptions.pluginDir = nosanPluginDir;
                } else {
                    harnessOptions.pluginDir = defaultPluginDir;
                }
            }

            json onnxPluginConfig;
            onnxPluginConfig["preferred_model"] = "embeddinggemma-300m";
            onnxPluginConfig["reranker_model"] = "bge-reranker-base";
            harnessOptions.pluginConfigs["onnx_plugin"] = onnxPluginConfig.dump();
            spdlog::info("Configured ONNX plugin models: embedding=embeddinggemma-300m "
                         "reranker=bge-reranker-base");

            if (!std::getenv("YAMS_ONNX_RERANK_FORCE_CPU") &&
                !std::getenv("YAMS_ONNX_RERANK_FORCE_GPU")) {
                setenv("YAMS_ONNX_RERANK_FORCE_CPU", "1", 1);
                spdlog::info("Enabled CPU-preferred ONNX reranker for benchmark evaluation");
            }

            // Configure Glint plugin with GLiNER model path for NL entity extraction
            std::string glinerModelPath = discoverGlinerModelPath();
            if (!glinerModelPath.empty()) {
                json glintConfig;
                glintConfig["model_path"] = glinerModelPath;
                glintConfig["threshold"] = 0.5; // Default confidence threshold
                harnessOptions.pluginConfigs["glint"] = glintConfig.dump();
                spdlog::info("Configured Glint plugin with model: {}", glinerModelPath);
            }

            if (resetPluginTrustFile && harnessOptions.dataDir) {
                const fs::path trustFile = *harnessOptions.dataDir / "plugins.trust";
                std::error_code ec;
                fs::remove(trustFile, ec);
                if (ec && ec != std::errc::no_such_file_or_directory) {
                    spdlog::warn("Failed to reset plugin trust file {}: {}", trustFile.string(),
                                 ec.message());
                } else if (!ec) {
                    spdlog::info("Reset plugin trust file for isolated benchmark plugins: {}",
                                 trustFile.string());
                }
            }

            if (useIsolatedBenchmarkConfig) {
                const fs::path configPath =
                    fs::temp_directory_path() / "yams_retrieval_bench_config.toml";
                std::ofstream configOut(configPath, std::ios::trunc);
                if (!configOut) {
                    spdlog::warn("Failed to write isolated benchmark config: {}",
                                 configPath.string());
                } else {
                    configOut << "# Isolated benchmark config\n";
                    configOut.close();
                    harnessOptions.configPath = configPath;
                    spdlog::info("Using isolated benchmark config: {}", configPath.string());
                }
            }
        } else {
            spdlog::info("Using mock model provider (YAMS_DISABLE_VECTORS=1)");
        }

        harness = std::make_unique<DaemonHarness>(harnessOptions);
        if (!harness->start(std::chrono::seconds(30)))
            throw std::runtime_error("Failed to start daemon");

        if (useBEIR) {
            const BEIRDataset& dataset = preparedBeirDataset.value();
            fs::path corpusDir = harness->rootDir() / "corpus";
            beirCorpus = std::make_unique<BEIRCorpusLoader>(dataset, corpusDir);
            beirCorpus->writeDocumentsAsFiles();
            benchCorpusDir = corpusDir;
            corpusSize = static_cast<int>(dataset.documents.size());
            numQueries = static_cast<int>(dataset.queries.size());
            expectedCacheMetadata.expectedDocs = corpusSize;
            expectedCacheMetadata.expectedQueries = numQueries;
            expectedCacheMetadata.corpusFingerprint = computePathFingerprint(benchCorpusDir);
        } else {
            benchCorpusDir = harness->rootDir() / "corpus";
            corpus = std::make_unique<CorpusGenerator>(benchCorpusDir);
            corpus->generateDocuments(corpusSize);
            expectedCacheMetadata.corpusFingerprint = computePathFingerprint(benchCorpusDir);
        }

        yams::daemon::ClientConfig clientCfg;
        // Prefer proxy socket for long-running benchmark connections; it skips
        // maxConnectionLifetime enforcement (main socket forces close at ~300s).
        {
            // Proxy socket is derived from daemon socket stem + ".proxy.sock".
            fs::path daemonSock = harness->socketPath();
            auto base = daemonSock.stem().string();
            if (base.empty())
                base = daemonSock.filename().string();
            if (base.empty())
                base = "yams-daemon";
            fs::path proxySock = daemonSock.parent_path() / (base + ".proxy.sock");
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

        auto isEofLikeError = [](const yams::Error& error) {
            return error.message.find("[ipc:eof]") != std::string::npos ||
                   error.message.find("End of file") != std::string::npos ||
                   error.message.find("Connection closed") != std::string::npos;
        };

        auto connectClientWithProbe = [&](const char* reason) {
            if (reason && *reason) {
                spdlog::info("[Bench] Reconnecting benchmark client ({})", reason);
            }

            client = std::make_unique<yams::daemon::DaemonClient>(clientCfg);
            // Override header/body timeouts to handle slow reranker queries.
            // Default headerTimeout=30s is too short when cross-encoder reranking
            // takes 20-60s before any response header is sent.
            client->setHeaderTimeout(std::chrono::milliseconds(300000)); // 5 min
            client->setBodyTimeout(std::chrono::milliseconds(300000));   // 5 min
            auto connectResult = yams::cli::run_sync(client->connect(), 5s);
            if (!connectResult) {
                throw std::runtime_error("Failed to connect: " + connectResult.error().message);
            }

            // Probe the exact selected socket path before ingesting to catch startup races.
            constexpr int kStatusProbeAttempts = 8;
            for (int attempt = 1; attempt <= kStatusProbeAttempts; ++attempt) {
                auto statusProbe = yams::cli::run_sync(client->status(), 5s);
                if (statusProbe) {
                    if (attempt > 1) {
                        spdlog::info("[Bench] Client status probe succeeded after {} attempts",
                                     attempt);
                    }
                    return;
                }

                if (attempt == kStatusProbeAttempts) {
                    throw std::runtime_error("Client status probe failed: " +
                                             statusProbe.error().message);
                }

                const bool eofLike = isEofLikeError(statusProbe.error());
                const auto backoff = std::chrono::milliseconds(125 * attempt);
                spdlog::warn("[Bench] Client status probe attempt {}/{} failed ({}{}), retrying in "
                             "{}ms",
                             attempt, kStatusProbeAttempts, statusProbe.error().message,
                             eofLike ? ", eof-like" : "", backoff.count());

                if (eofLike) {
                    // Drop any potentially stale pooled transport before retrying.
                    client.reset();
                    yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(250));
                    auto reconnectWait = std::chrono::milliseconds(80);
                    std::this_thread::sleep_for(reconnectWait);
                    client = std::make_unique<yams::daemon::DaemonClient>(clientCfg);
                    client->setHeaderTimeout(std::chrono::milliseconds(300000));
                    client->setBodyTimeout(std::chrono::milliseconds(300000));
                    auto reconnectResult = yams::cli::run_sync(client->connect(), 5s);
                    if (!reconnectResult) {
                        throw std::runtime_error("Failed to reconnect after probe EOF: " +
                                                 reconnectResult.error().message);
                    }
                }

                std::this_thread::sleep_for(backoff);
            }
        };

        connectClientWithProbe("initial setup");

        const bool usingExternalBenchDataDir = harnessOptions.dataDir.has_value();
        auto initialStatusResult = yams::cli::run_sync(client->status(), 5s);
        const auto getInitialCount = [&](const std::string& key) -> uint64_t {
            if (!initialStatusResult) {
                return 0;
            }
            auto it = initialStatusResult.value().requestCounts.find(key);
            return (it == initialStatusResult.value().requestCounts.end()) ? 0ULL : it->second;
        };

        const bool warmCacheMetadataReusable =
            warmCacheMetadata.has_value() &&
            (warmCacheMetadata->status == "primed" || warmCacheMetadata->status == "partial");

        const uint64_t initialIndexedDocCount = [&]() -> uint64_t {
            if (warmCacheMetadataReusable) {
                return static_cast<uint64_t>(warmCacheMetadata->expectedDocs);
            }
            if (!initialStatusResult) {
                return 0;
            }
            auto itIndexed = initialStatusResult.value().requestCounts.find("documents_indexed");
            if (itIndexed != initialStatusResult.value().requestCounts.end()) {
                return itIndexed->second;
            }
            auto itTotal = initialStatusResult.value().requestCounts.find("documents_total");
            return (itTotal == initialStatusResult.value().requestCounts.end()) ? 0ULL
                                                                                : itTotal->second;
        }();

        const uint64_t initialDocsEmbedded =
            warmCacheMetadataReusable ? static_cast<uint64_t>(warmCacheMetadata->embeddedDocs)
                                      : getInitialCount("documents_embedded");
        const uint64_t initialVectorCount =
            warmCacheMetadataReusable ? static_cast<uint64_t>(warmCacheMetadata->vectorCount)
                                      : getInitialCount("vector_count");
        const uint64_t initialEmbedQueued =
            warmCacheMetadataReusable ? 0 : getInitialCount("embed_svc_queued");
        const uint64_t initialEmbedInFlight =
            warmCacheMetadataReusable ? 0 : getInitialCount("embed_in_flight");
        const bool cacheHasIndexedCorpus =
            usingExternalBenchDataDir &&
            initialIndexedDocCount >= static_cast<uint64_t>(corpusSize);
        const bool cacheHasFullEmbeddings =
            usingExternalBenchDataDir && !vectorsDisabled &&
            initialDocsEmbedded >= static_cast<uint64_t>(corpusSize) && initialEmbedQueued == 0 &&
            initialEmbedInFlight == 0;

        if (usingExternalBenchDataDir) {
            spdlog::info("[Bench] External data dir status: indexed_docs={} embedded_docs={} "
                         "vector_count={} embed_queue={} embed_in_flight={}{}",
                         initialIndexedDocCount, initialDocsEmbedded, initialVectorCount,
                         initialEmbedQueued, initialEmbedInFlight,
                         warmCacheMetadataReusable ? " (trusted metadata)" : "");
        }

        if (!cacheHasFullEmbeddings) {
            if (cacheHasIndexedCorpus) {
                spdlog::info("[Bench] Reusing indexed corpus from external data dir; skipping "
                             "directory ingest");
            }

            if (!cacheHasIndexedCorpus) {
                // Use directory ingestion for faster bulk add via IndexingService
                fs::path corpusDir = useBEIR ? beirCorpus->corpusDir : corpus->corpusDir;
                spdlog::info("Ingesting {} documents from {}...", corpusSize, corpusDir.string());

                yams::daemon::AddDocumentRequest addReq;
                addReq.path = corpusDir.string();
                addReq.recursive = true;
                addReq.noEmbeddings = false;
                addReq.includePatterns = {"*.txt"};

                Result<yams::daemon::AddDocumentResponse> addResult =
                    Error{ErrorCode::Unknown, "ingest not attempted"};
                constexpr int kIngestAttempts = 2;
                for (int ingestAttempt = 1; ingestAttempt <= kIngestAttempts; ++ingestAttempt) {
                    addResult = yams::cli::run_sync(client->streamingAddDocument(addReq), 120s);
                    if (addResult) {
                        break;
                    }

                    const bool eofLike = isEofLikeError(addResult.error());
                    if (!(eofLike && ingestAttempt < kIngestAttempts)) {
                        break;
                    }

                    spdlog::warn(
                        "[Bench] Directory ingest attempt {}/{} failed with eof-like transport "
                        "error ({}). Retrying once with a fresh client.",
                        ingestAttempt, kIngestAttempts, addResult.error().message);
                    client.reset();
                    yams::daemon::AsioConnectionPool::shutdown_all(std::chrono::milliseconds(250));
                    std::this_thread::sleep_for(std::chrono::milliseconds(120));
                    connectClientWithProbe("ingest retry after eof");
                }

                if (!addResult) {
                    throw std::runtime_error("Failed to ingest directory: " +
                                             addResult.error().message);
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

                auto ingestionLooksCompleteFromLastObserved = [&]() -> bool {
                    const bool targetReached =
                        (lastIndexedDocCount >= static_cast<uint64_t>(corpusSize)) ||
                        (lastDocCount >= static_cast<uint64_t>(corpusSize));
                    const bool allQueuesDrained =
                        (lastDepth == 0 && lastExtractionInFlight == 0 && lastKgInFlight == 0 &&
                         lastSymbolInFlight == 0 && lastEntityInFlight == 0 &&
                         lastEmbedQueued == 0 && lastEmbedInFlight == 0);
                    const bool extractionOrPostDone =
                        (lastContentExtracted >= static_cast<uint64_t>(corpusSize)) ||
                        (lastPostProcessed >= static_cast<uint64_t>(corpusSize));
                    return targetReached && allQueuesDrained && extractionOrPostDone;
                };

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
                        if (ingestionLooksCompleteFromLastObserved()) {
                            spdlog::warn(
                                "Ingestion progress timeout reached, but last observed metrics "
                                "already indicate completion; accepting completion.");
                            completed = true;
                            break;
                        }

                        // Status polls may intermittently fail even when work is done; perform one
                        // final verification poll before classifying as stalled.
                        auto finalVerify = yams::cli::run_sync(client->status(), 10s);
                        if (finalVerify) {
                            const auto& counts = finalVerify.value().requestCounts;
                            auto get = [&](const std::string& key) -> uint64_t {
                                auto it = counts.find(key);
                                return (it == counts.end()) ? 0ULL : it->second;
                            };

                            const uint64_t docsTotal = get("documents_total");
                            const uint64_t docsIndexed = get("documents_indexed");
                            const uint64_t extracted = get("documents_content_extracted");
                            const uint64_t postQueued = get("post_ingest_queued");
                            const uint64_t postInFlight = get("post_ingest_inflight");
                            const uint64_t postProcessed = get("post_ingest_processed");
                            const uint64_t extractionInFlight = get("extraction_inflight");
                            const uint64_t kgInFlight = get("kg_inflight");
                            const uint64_t symbolInFlight = get("symbol_inflight");
                            const uint64_t entityInFlight = get("entity_inflight");
                            const uint64_t embedQueued = get("embed_svc_queued");
                            const uint64_t embedInFlight = get("embed_in_flight");

                            const bool targetReached =
                                (docsIndexed >= static_cast<uint64_t>(corpusSize)) ||
                                (docsTotal >= static_cast<uint64_t>(corpusSize));
                            const bool allQueuesDrained =
                                (postQueued == 0 && postInFlight == 0 && extractionInFlight == 0 &&
                                 kgInFlight == 0 && symbolInFlight == 0 && entityInFlight == 0 &&
                                 embedQueued == 0 && embedInFlight == 0);
                            const bool extractionOrPostDone =
                                (extracted >= static_cast<uint64_t>(corpusSize)) ||
                                (postProcessed >= static_cast<uint64_t>(corpusSize));

                            if (targetReached && allQueuesDrained && extractionOrPostDone) {
                                spdlog::warn(
                                    "Ingestion progress timeout reached but final verification "
                                    "shows completion (docs_total={}, docs_indexed={}, "
                                    "extracted={}, post_processed={}); accepting completion.",
                                    docsTotal, docsIndexed, extracted, postProcessed);
                                completed = true;
                                break;
                            }
                        }

                        spdlog::error(
                            "Ingestion stalled - no progress for {}s (docs: total={} indexed={} "
                            "target={} "
                            "queue={}, inflight: extract={} kg={} symbol={} entity={} extracted={} "
                            "processed={} embed_queue={} embed_in_flight={} vectors={})",
                            timeSinceProgress.count(), lastDocCount, lastIndexedDocCount,
                            corpusSize, lastDepth, lastExtractionInFlight, lastKgInFlight,
                            lastSymbolInFlight, lastEntityInFlight, lastContentExtracted,
                            lastPostProcessed, lastEmbedQueued, lastEmbedInFlight, lastVectorCount);
                        throw std::runtime_error(
                            "Ingestion stalled - benchmark results would be invalid. "
                            "Set YAMS_BENCH_PROGRESS_TIMEOUT=300 for slower systems. "
                            "For faster ingestion: YAMS_POST_EMBED_CONCURRENT=12 "
                            "YAMS_POST_EXTRACTION_CONCURRENT=12 YAMS_DB_POOL_MAX=48");
                    }

                    auto statusResult = yams::cli::run_sync(client->status(), 5s);
                    if (statusResult) {
                        const auto& counts = statusResult.value().requestCounts;
                        uint32_t depth = statusResult.value().postIngestQueueDepth;
                        auto [docCount, docCountPresent] = getMetric(counts, "documents_total");
                        auto [indexedCount, indexedPresent] =
                            getMetric(counts, "documents_indexed");
                        auto [contentExtracted, extractedPresent] =
                            getMetric(counts, "documents_content_extracted");
                        auto [postQueued, postQueuedPresent] =
                            getMetric(counts, "post_ingest_queued");
                        auto [postInflight, postInflightPresent] =
                            getMetric(counts, "post_ingest_inflight");
                        auto [postProcessed, postProcessedPresent] =
                            getMetric(counts, "post_ingest_processed");
                        auto [embedQueued, embedQueuedPresent] =
                            getMetric(counts, "embed_svc_queued");
                        auto [embedInFlight, embedInFlightPresent] =
                            getMetric(counts, "embed_in_flight");
                        auto [vectorCount, vectorPresent] = getMetric(counts, "vector_count");
                        uint64_t queuedTotal = std::max<uint64_t>(depth, postQueued);

                        // Check all processing stage in-flight counters
                        auto [extractionInFlight, extractionPresent] =
                            getMetric(counts, "extraction_inflight");
                        auto [kgInFlight, kgPresent] = getMetric(counts, "kg_inflight");
                        auto [symbolInFlight, symbolPresent] = getMetric(counts, "symbol_inflight");
                        auto [entityInFlight, entityPresent] = getMetric(counts, "entity_inflight");

                        // Total in-flight across all stages (matches
                        // PostIngestQueue::totalInFlight())
                        uint64_t totalInFlight =
                            extractionInFlight + kgInFlight + symbolInFlight + entityInFlight;

                        bool statusChanged =
                            (queuedTotal != lastDepth || docCount != lastDocCount ||
                             indexedCount != lastIndexedDocCount ||
                             contentExtracted != lastContentExtracted ||
                             postProcessed != lastPostProcessed || kgInFlight != lastKgInFlight ||
                             symbolInFlight != lastSymbolInFlight ||
                             entityInFlight != lastEntityInFlight ||
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
                                "Documents: total={} indexed={} / {} | queue={} inflight={} | "
                                "extracted={} "
                                "processed={} | extract={} kg={} symbol={} entity={} (total={}) | "
                                "embed_queue={} embed_in_flight={} vectors={} (ready={})",
                                docCount, indexedCount, corpusSize, queuedTotal, postInflight,
                                contentExtracted, postProcessed, extractionInFlight, kgInFlight,
                                symbolInFlight, entityInFlight, totalInFlight, embedQueued,
                                embedInFlight, vectorCount, (vectorDbReady ? "true" : "false"));
                            lastDepth = static_cast<uint32_t>(queuedTotal);
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
                        // - indexedCount >= corpusSize (or stable for 5+ seconds after reaching
                        // target)
                        bool allStagesDrained =
                            (queuedTotal == 0 && postInflight == 0 && totalInFlight == 0);
                        bool indexedReady =
                            indexedPresent ? (indexedCount >= static_cast<uint64_t>(corpusSize))
                                           : (docCount >= static_cast<uint64_t>(corpusSize));
                        bool extractedReady =
                            extractedPresent
                                ? (contentExtracted >= static_cast<uint64_t>(corpusSize))
                                : true;
                        bool processedReady =
                            postProcessedPresent
                                ? (postProcessed >= static_cast<uint64_t>(corpusSize))
                                : true;
                        bool stableAfterDrain = (stableChecks >= 6);
                        if (allStagesDrained && indexedReady &&
                            (extractedReady || processedReady || stableAfterDrain)) {
                            spdlog::info(
                                "Ingestion complete: total={} indexed={} (target={}), all stages "
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

            } else if (!vectorsDisabled) {
                yams::daemon::RepairRequest repairReq;
                repairReq.repairEmbeddings = true;
                repairReq.embeddingModel = "embeddinggemma-300m";
                repairReq.foreground = true;
                repairReq.maxRetries = 3;
                spdlog::info("[Bench] Requesting embedding repair to resume warm-cache priming...");
                auto repairResult = yams::cli::run_sync(client->callRepair(repairReq), 300s);
                if (!repairResult) {
                    spdlog::warn("[Bench] Embedding repair request failed: {}",
                                 repairResult.error().message);
                } else {
                    spdlog::info("[Bench] Embedding repair queued: success={} ops={} succeeded={} "
                                 "failed={} skipped={}",
                                 repairResult.value().success, repairResult.value().totalOperations,
                                 repairResult.value().totalSucceeded,
                                 repairResult.value().totalFailed,
                                 repairResult.value().totalSkipped);
                }
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

            // Wait for embeddings to be generated by checking document-level embedding progress
            // (documents_embedded) and queue drain. We still track vector_count for row-level
            // visibility, but vector_count is not a document counter.
            bool vectorsEnabled = !vectorsDisabled;
            if (vectorsEnabled && !embeddingReady) {
                vectorsEnabled = false;
                spdlog::warn("Disabling vector wait for this benchmark run: embedding provider is "
                             "unavailable; proceeding with keyword-dominant evaluation");
                if (summaryLog) {
                    summaryLog << "Embedding provider unavailable after 30s; skipping vector wait"
                               << std::endl;
                    summaryLog.flush();
                }
            }

            if (vectorsEnabled) {
                spdlog::info("Waiting for embeddings to be generated (target: {} docs)...",
                             corpusSize);

                auto embedHeartbeatAt = std::chrono::steady_clock::now() + std::chrono::seconds(5);

                // Log to summary file
                if (summaryLog) {
                    summaryLog << "=== Embedding Generation ===" << std::endl;
                    summaryLog << "Target: " << corpusSize << " documents" << std::endl;
                }

                auto embedProgressTimeoutSec = std::chrono::seconds(600);
                if (const char* env = std::getenv("YAMS_BENCH_EMBED_PROGRESS_TIMEOUT")) {
                    embedProgressTimeoutSec = std::chrono::seconds(std::stoi(env));
                }
                auto embedMaxWaitSec = std::chrono::seconds(7200); // 2h hard cap (configurable)
                if (const char* env = std::getenv("YAMS_BENCH_EMBED_MAX_WAIT")) {
                    embedMaxWaitSec = std::chrono::seconds(std::stoi(env));
                }
                uint64_t lastVectorCount = 0;
                uint64_t lastDocsEmbeddedCount = 0;
                uint64_t lastPreparedDocsCount = 0;
                uint64_t lastPreparedChunksCount = 0;
                uint64_t lastEmbedQueuedObserved = 0;
                uint64_t lastEmbedInFlightObserved = 0;
                uint64_t lastEmbedDroppedObserved = 0;
                bool seenEmbedMetrics = false;
                int stableCount = 0;
                uint64_t embedDropped = 0;
                bool embeddingDrainSatisfied = false;
                auto embedStartTime = std::chrono::steady_clock::now();
                auto lastEmbedProgressTime = embedStartTime;
                uint64_t lastObservedVectorCount = 0;
                uint64_t lastObservedDocsEmbedded = 0;
                uint64_t lastObservedPreparedDocs = 0;
                uint64_t lastObservedPreparedChunks = 0;
                uint64_t lastObservedEmbedQueued = 0;
                uint64_t lastObservedEmbedInFlight = 0;
                uint64_t lastObservedEmbedDropped = 0;
                uint64_t initialObservedVectorCount = 0;
                uint64_t initialObservedDocsEmbedded = 0;
                bool capturedInitialEmbedSnapshot = false;

                // Phase 1: Wait for embedding queue to drain and embedded documents to reach
                // target. Uses progress-based stall detection (like ingestion wait) to avoid
                // failing while work is still actively progressing on large corpora/slow
                // accelerators.
                while (true) {
                    auto now = std::chrono::steady_clock::now();
                    auto elapsed =
                        std::chrono::duration_cast<std::chrono::seconds>(now - embedStartTime);
                    auto sinceProgress = std::chrono::duration_cast<std::chrono::seconds>(
                        now - lastEmbedProgressTime);

                    if (embedMaxWaitSec.count() > 0 && elapsed > embedMaxWaitSec) {
                        throw std::runtime_error(
                            "Embedding wait exceeded hard limit (" +
                            std::to_string(embedMaxWaitSec.count()) +
                            "s) before benchmark queries (embed_svc_queued=" +
                            std::to_string(lastObservedEmbedQueued) +
                            ", embed_in_flight=" + std::to_string(lastObservedEmbedInFlight) +
                            ", docs_embedded=" + std::to_string(lastObservedDocsEmbedded) +
                            ", vectors=" + std::to_string(lastObservedVectorCount) +
                            ", dropped=" + std::to_string(lastObservedEmbedDropped) +
                            "). Increase YAMS_BENCH_EMBED_MAX_WAIT or tune embedding concurrency.");
                    }

                    if (sinceProgress > embedProgressTimeoutSec) {
                        throw std::runtime_error(
                            "Embedding wait stalled for " +
                            std::to_string(embedProgressTimeoutSec.count()) +
                            "s before benchmark queries (embed_svc_queued=" +
                            std::to_string(lastObservedEmbedQueued) +
                            ", embed_in_flight=" + std::to_string(lastObservedEmbedInFlight) +
                            ", docs_embedded=" + std::to_string(lastObservedDocsEmbedded) +
                            ", vectors=" + std::to_string(lastObservedVectorCount) +
                            ", dropped=" + std::to_string(lastObservedEmbedDropped) +
                            "). Increase timeout/concurrency or investigate model/provider "
                            "stalls.");
                    }

                    if (now >= embedHeartbeatAt) {
                        embedHeartbeatAt = now + std::chrono::seconds(5);
                        spdlog::info("Embed wait heartbeat: elapsed={}s since_progress={}s "
                                     "last_docs_embedded={} last_vectors={} queue={} in_flight={}",
                                     elapsed.count(), sinceProgress.count(),
                                     lastObservedDocsEmbedded, lastObservedVectorCount,
                                     lastObservedEmbedQueued, lastObservedEmbedInFlight);
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

                        uint64_t docsEmbedded = 0;
                        auto itDocsEmbedded =
                            statusResult.value().requestCounts.find("documents_embedded");
                        if (itDocsEmbedded != statusResult.value().requestCounts.end()) {
                            docsEmbedded = itDocsEmbedded->second;
                        }

                        uint64_t preparedDocsQueued = 0;
                        auto itPreparedDocs = statusResult.value().requestCounts.find(
                            "bus_embed_prepared_docs_queued");
                        if (itPreparedDocs != statusResult.value().requestCounts.end()) {
                            preparedDocsQueued = itPreparedDocs->second;
                        }

                        uint64_t preparedChunksQueued = 0;
                        auto itPreparedChunks = statusResult.value().requestCounts.find(
                            "bus_embed_prepared_chunks_queued");
                        if (itPreparedChunks != statusResult.value().requestCounts.end()) {
                            preparedChunksQueued = itPreparedChunks->second;
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
                        auto itInFlight =
                            statusResult.value().requestCounts.find("embed_in_flight");
                        if (itInFlight != statusResult.value().requestCounts.end()) {
                            embedInFlight = itInFlight->second;
                        }

                        auto itD = statusResult.value().requestCounts.find("bus_embed_dropped");
                        if (itD != statusResult.value().requestCounts.end()) {
                            embedDropped = itD->second;
                        }

                        lastObservedVectorCount = vectorCount;
                        lastObservedDocsEmbedded = docsEmbedded;
                        lastObservedPreparedDocs = preparedDocsQueued;
                        lastObservedPreparedChunks = preparedChunksQueued;
                        lastObservedEmbedQueued = embedQueued;
                        lastObservedEmbedInFlight = embedInFlight;
                        lastObservedEmbedDropped = embedDropped;

                        if (!capturedInitialEmbedSnapshot) {
                            initialObservedVectorCount = vectorCount;
                            initialObservedDocsEmbedded = docsEmbedded;
                            capturedInitialEmbedSnapshot = true;
                        }

                        const bool haveQueueMetrics =
                            (itQ != statusResult.value().requestCounts.end()) ||
                            (itInFlight != statusResult.value().requestCounts.end());

                        bool metricsChanged = false;
                        if (!seenEmbedMetrics) {
                            seenEmbedMetrics = true;
                            metricsChanged = true;
                        }
                        if (vectorCount != lastVectorCount ||
                            docsEmbedded != lastDocsEmbeddedCount ||
                            preparedDocsQueued != lastPreparedDocsCount ||
                            preparedChunksQueued != lastPreparedChunksCount ||
                            embedQueued != lastEmbedQueuedObserved ||
                            embedInFlight != lastEmbedInFlightObserved) {
                            metricsChanged = true;
                        }

                        if (metricsChanged) {
                            lastEmbedProgressTime = now;
                            stableCount = 0;
                        } else {
                            stableCount++;
                        }

                        if (vectorCount != lastVectorCount ||
                            docsEmbedded != lastDocsEmbeddedCount) {
                            double docCoverage =
                                corpusSize > 0 ? (docsEmbedded * 100.0 / corpusSize) : 0;
                            double preparedAvgChunksPerDoc =
                                preparedDocsQueued > 0 ? static_cast<double>(preparedChunksQueued) /
                                                             static_cast<double>(preparedDocsQueued)
                                                       : 0.0;
                            spdlog::info(
                                "Embedding progress: docs={} / {} ({:.1f}%) vectors={} "
                                "prepared_docs={} prepared_chunks={} avg_chunks_per_doc={:.2f} "
                                "| queue={} in_flight={} dropped={}",
                                docsEmbedded, corpusSize, docCoverage, vectorCount,
                                preparedDocsQueued, preparedChunksQueued, preparedAvgChunksPerDoc,
                                embedQueued, embedInFlight, embedDropped);

                            // Log progress to summary file
                            if (summaryLog) {
                                auto elapsed =
                                    std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - embedStartTime)
                                        .count();
                                summaryLog
                                    << "[" << elapsed << "ms] DocsEmbedded: " << docsEmbedded
                                    << " / " << corpusSize << " (" << std::fixed
                                    << std::setprecision(1) << docCoverage << "%)"
                                    << " | vectors=" << vectorCount
                                    << " | prepared_docs=" << preparedDocsQueued
                                    << " prepared_chunks=" << preparedChunksQueued
                                    << " avg_chunks_per_doc=" << std::fixed << std::setprecision(2)
                                    << preparedAvgChunksPerDoc << " | queue=" << embedQueued
                                    << " in_flight=" << embedInFlight << " dropped=" << embedDropped
                                    << std::endl;
                                summaryLog.flush();
                            }

                            lastVectorCount = vectorCount;
                            lastDocsEmbeddedCount = docsEmbedded;
                            lastPreparedDocsCount = preparedDocsQueued;
                            lastPreparedChunksCount = preparedChunksQueued;
                        }

                        lastEmbedQueuedObserved = embedQueued;
                        lastEmbedInFlightObserved = embedInFlight;
                        lastEmbedDroppedObserved = embedDropped;

                        // Success condition: embedded docs >= corpusSize AND queue drained.
                        // vector_count tracks rows (chunks + doc records), not documents.
                        bool queueDrained = (embedQueued == 0 && embedInFlight == 0);
                        if (haveQueueMetrics && docsEmbedded >= static_cast<uint64_t>(corpusSize) &&
                            queueDrained && stableCount >= 10) {
                            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - embedStartTime)
                                               .count();
                            spdlog::info(
                                "Full embedding coverage achieved: {} embedded docs for {} "
                                "target docs "
                                "(queue drained, stable for {}s, vector_db_ready={})",
                                docsEmbedded, corpusSize, stableCount / 2,
                                (vectorDbReady ? "true" : "false"));
                            if (summaryLog) {
                                summaryLog << "\n*** SUCCESS: Full coverage in " << elapsed
                                           << "ms ***" << std::endl;
                                summaryLog << "Final embedded docs: " << docsEmbedded << std::endl;
                                summaryLog << "Final vectors: " << vectorCount << std::endl;
                                summaryLog << "Total dropped: " << embedDropped << std::endl;
                                summaryLog.flush();
                            }
                            embeddingDrainSatisfied = true;
                            break;
                        }

                        // If very stable (20s) with queue drained, we're done even if < corpusSize
                        // This handles cases where some docs have no content to embed
                        // IMPORTANT: Require minimum 90% coverage to prevent premature exit from
                        // transient queue drain (e.g., between embedding batches)
                        double coverage = corpusSize > 0 ? (docsEmbedded * 100.0 / corpusSize) : 0;
                        constexpr double MIN_COVERAGE_THRESHOLD = 90.0;
                        if (haveQueueMetrics && stableCount >= 40 && queueDrained &&
                            docsEmbedded > 0 && coverage >= MIN_COVERAGE_THRESHOLD) {
                            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                               std::chrono::steady_clock::now() - embedStartTime)
                                               .count();
                            spdlog::info(
                                "Embedding generation complete (queue drained, stable): {} "
                                "embedded docs ({:.1f}% coverage, vector_db_ready={})",
                                docsEmbedded, coverage, (vectorDbReady ? "true" : "false"));
                            if (summaryLog) {
                                summaryLog << "\n*** Complete (queue drained) in " << elapsed
                                           << "ms ***" << std::endl;
                                summaryLog << "Final embedded docs: " << docsEmbedded << " ("
                                           << coverage << "%)" << std::endl;
                                summaryLog << "Final vectors: " << vectorCount << std::endl;
                                summaryLog.flush();
                            }
                            embeddingDrainSatisfied = true;
                            break;
                        }

                        if (haveQueueMetrics && stableCount >= 40 && queueDrained &&
                            docsEmbedded == 0) {
                            throw std::runtime_error(
                                "Embedding failed before benchmark queries (queue "
                                "drained, no documents "
                                "embedded, dropped=" +
                                std::to_string(embedDropped) +
                                "). Check embedding provider/model availability.");
                        }

                        if (haveQueueMetrics && queueDrained && docsEmbedded == 0 &&
                            elapsed > std::chrono::seconds(60)) {
                            throw std::runtime_error(
                                "Embedding appears unavailable for this candidate (queue drained, "
                                "no "
                                "documents embedded after " +
                                std::to_string(elapsed.count()) +
                                "s, dropped=" + std::to_string(embedDropped) +
                                "). Aborting candidate to keep optimization loop progressing.");
                        }

                        // If stable but queue NOT drained, warn about potential issues
                        if (haveQueueMetrics && stableCount >= 20 && !queueDrained &&
                            docsEmbedded > 0) {
                            // coverage already computed above
                            spdlog::warn("Embedding stalled at {:.1f}% but queue not drained "
                                         "(queue={}, in_flight={}). Continuing to wait...",
                                         coverage, embedQueued, embedInFlight);
                            // Don't break - keep waiting for queue to drain
                        }
                    }
                    std::this_thread::sleep_for(500ms);
                }

                if (auto* daemon = harness->daemon()) {
                    if (auto* serviceManager = daemon->getServiceManager()) {
                        auto vectorDb = serviceManager->getVectorDatabase();
                        auto metadataRepo = serviceManager->getMetadataRepo();

                        if (vectorDb && metadataRepo) {
                            auto embeddedHashes = vectorDb->getEmbeddedDocumentHashes();
                            if (embeddedHashes.size() > lastObservedDocsEmbedded) {
                                std::vector<std::string> reconciledHashes;
                                reconciledHashes.reserve(embeddedHashes.size());
                                for (const auto& hash : embeddedHashes) {
                                    reconciledHashes.push_back(hash);
                                }
                                auto reconcileResult =
                                    metadataRepo->batchUpdateDocumentEmbeddingStatusByHashes(
                                        reconciledHashes, true,
                                        serviceManager->getEmbeddingModelName());
                                if (!reconcileResult) {
                                    spdlog::warn("Failed to reconcile embedded-doc status from "
                                                 "vector rows: {}",
                                                 reconcileResult.error().message);
                                } else {
                                    spdlog::info("Reconciled embedded-doc status from vector rows: "
                                                 "{} -> {}",
                                                 lastObservedDocsEmbedded, reconciledHashes.size());
                                    lastObservedDocsEmbedded = reconciledHashes.size();
                                }
                            }
                        }
                    }
                }

                // Final status check
                auto finalStatus = yams::cli::run_sync(client->status(), 5s);
                if (finalStatus) {
                    uint64_t finalVectorCount = 0;
                    auto it = finalStatus.value().requestCounts.find("vector_count");
                    if (it != finalStatus.value().requestCounts.end()) {
                        finalVectorCount = it->second;
                    }
                    uint64_t finalDocsEmbedded = 0;
                    auto itDocsEmbedded =
                        finalStatus.value().requestCounts.find("documents_embedded");
                    if (itDocsEmbedded != finalStatus.value().requestCounts.end()) {
                        finalDocsEmbedded = itDocsEmbedded->second;
                    }
                    uint64_t finalEmbedQueued = 0;
                    auto itQ = finalStatus.value().requestCounts.find("embed_svc_queued");
                    if (itQ != finalStatus.value().requestCounts.end()) {
                        finalEmbedQueued = itQ->second;
                    }
                    uint64_t finalEmbedInFlight = 0;
                    auto itInFlight = finalStatus.value().requestCounts.find("embed_in_flight");
                    if (itInFlight != finalStatus.value().requestCounts.end()) {
                        finalEmbedInFlight = itInFlight->second;
                    }
                    double finalCoverage =
                        corpusSize > 0 ? (finalDocsEmbedded * 100.0 / corpusSize) : 0;
                    bool vectorDbReady = finalStatus.value().vectorDbReady;
                    if (auto it = finalStatus.value().readinessStates.find("vector_db");
                        it != finalStatus.value().readinessStates.end()) {
                        vectorDbReady = it->second;
                    }
                    spdlog::info("Vector DB: ready={} dim={}, docs_embedded={} vectors={} "
                                 "({:.1f}% doc coverage)",
                                 (vectorDbReady ? "true" : "false"),
                                 finalStatus.value().vectorDbDim, finalDocsEmbedded,
                                 finalVectorCount, finalCoverage);

                    if (!embeddingDrainSatisfied) {
                        const bool queueDrained =
                            (finalEmbedQueued == 0 && finalEmbedInFlight == 0);
                        if (!queueDrained) {
                            throw std::runtime_error(
                                "Embedding drain not reached before benchmark queries "
                                "(embed_svc_queued=" +
                                std::to_string(finalEmbedQueued) +
                                ", embed_in_flight=" + std::to_string(finalEmbedInFlight) +
                                ", docs_embedded=" + std::to_string(finalDocsEmbedded) +
                                ", vectors=" + std::to_string(finalVectorCount) +
                                "). Increase timeout/concurrency or reduce corpus size to obtain "
                                "valid "
                                "results.");
                        }

                        // Queue may only be fully drained on the final status check.
                        embeddingDrainSatisfied = true;
                    }

                    if (finalDocsEmbedded == 0) {
                        spdlog::warn("No embedded documents present after embedding wait");
                    } else if (finalCoverage < 90.0) {
                        spdlog::error("WARNING: Low document embedding coverage ({:.1f}%) will "
                                      "degrade retrieval "
                                      "quality!",
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
                    if (!embeddingDrainSatisfied) {
                        throw std::runtime_error("Final status check failed after embedding wait; "
                                                 "cannot verify embedding drain. "
                                                 "Benchmark results would be unreliable.");
                    }
                }
            } else {
                spdlog::info("Vectors disabled - skipping embedding generation wait");
            }

        } else {
            spdlog::info("[Bench] Warm data dir: skipping ingestion + embedding wait");
        } // end if (!warmDataDir) / else

        if (!warmDataDirPath.empty()) {
            auto finalCacheMetadata = expectedCacheMetadata;
            auto finalStatusForCache = yams::cli::run_sync(client->status(), 5s);
            if (finalStatusForCache) {
                finalCacheMetadata =
                    currentBenchCacheMetadata(expectedCacheMetadata, finalStatusForCache.value());
            }
            finalCacheMetadata =
                preferStrongerBenchCacheMetadata(finalCacheMetadata, warmCacheMetadata);
            if (!expectedCacheMetadata.datasetPath.empty() &&
                finalCacheMetadata.datasetPath.empty()) {
                finalCacheMetadata.datasetPath = expectedCacheMetadata.datasetPath;
            }

            const auto metadataWrite = writeBenchCacheMetadata(warmDataDirPath, finalCacheMetadata);
            if (!metadataWrite) {
                spdlog::warn("[Bench] Failed to write warm cache metadata for {}: {}",
                             warmDataDirPath.string(), metadataWrite.error().message);
            } else {
                spdlog::info(
                    "[Bench] Warm cache metadata written to {} (status={}, embedded_docs={}, "
                    "vectors={})",
                    (warmDataDirPath / "retrieval_bench_cache.json").string(),
                    finalCacheMetadata.status, finalCacheMetadata.embeddedDocs,
                    finalCacheMetadata.vectorCount);
            }
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

        if (requireKgReady) {
            int kgReadyTimeoutSec = 180;
            if (const char* env = std::getenv("YAMS_BENCH_KG_READY_TIMEOUT")) {
                kgReadyTimeoutSec = std::max(0, std::stoi(env));
            }

            spdlog::info("Waiting for KG readiness before query evaluation (timeout={}s)...",
                         kgReadyTimeoutSec);

            const auto kgDeadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(kgReadyTimeoutSec);
            int stickyInflightGraceSec = 20;
            if (const char* env = std::getenv("YAMS_BENCH_KG_STICKY_INFLIGHT_GRACE_SEC")) {
                stickyInflightGraceSec = std::max(0, std::stoi(env));
            }
            const auto stickyInflightGrace = std::chrono::seconds(stickyInflightGraceSec);
            int stableReadyChecks = 0;
            bool kgReady = false;
            bool acceptedStickyInflight = false;
            bool lastHasKg = false;
            uint32_t lastQueueDepth = 0;
            uint64_t lastPostQueued = 0;
            uint64_t lastPostInflight = 0;
            uint64_t lastKgInflight = 0;
            uint64_t lastKgConsumed = 0;
            uint64_t lastSymbolInflight = 0;
            uint64_t lastEntityInflight = 0;
            uint64_t lastTitleInflight = 0;
            uint64_t lastTitleQueueDepth = 0;
            uint64_t lastExtractionInflight = 0;
            bool stickyInflightActive = false;
            uint64_t stickyInflightValue = 0;
            auto stickyInflightSince = std::chrono::steady_clock::time_point{};

            while (std::chrono::steady_clock::now() < kgDeadline) {
                const auto now = std::chrono::steady_clock::now();
                auto statusCheck = yams::cli::run_sync(client->status(), 5s);
                auto statsCheck =
                    yams::cli::run_sync(client->getStats(yams::daemon::GetStatsRequest{}), 10s);

                bool queuesDrained = false;
                if (statusCheck) {
                    const auto& st = statusCheck.value();
                    const auto getCount = [&st](const std::string& key) -> uint64_t {
                        auto it = st.requestCounts.find(key);
                        return (it == st.requestCounts.end()) ? 0ULL : it->second;
                    };

                    lastQueueDepth = st.postIngestQueueDepth;
                    lastPostQueued = getCount("post_ingest_queued");
                    lastPostInflight = getCount("post_ingest_inflight");
                    lastExtractionInflight = getCount("extraction_inflight");
                    lastKgInflight = getCount("kg_inflight");
                    lastKgConsumed = getCount("kg_consumed");
                    lastSymbolInflight = getCount("symbol_inflight");
                    lastEntityInflight = getCount("entity_inflight");
                    lastTitleInflight = getCount("title_inflight");
                    lastTitleQueueDepth = getCount("title_queue_depth");

                    queuesDrained =
                        (lastQueueDepth == 0 && lastPostQueued == 0 && lastPostInflight == 0 &&
                         lastExtractionInflight == 0 && lastKgInflight == 0 &&
                         lastSymbolInflight == 0 && lastEntityInflight == 0 &&
                         lastTitleInflight == 0 && lastTitleQueueDepth == 0);
                }

                bool hasKg = false;
                if (statsCheck) {
                    const auto& stats = statsCheck.value();
                    if (auto it = stats.additionalStats.find("corpus_stats");
                        it != stats.additionalStats.end() && !it->second.empty()) {
                        try {
                            auto corpusStats = json::parse(it->second);
                            if (corpusStats.contains("classification") &&
                                corpusStats["classification"].is_object()) {
                                hasKg = corpusStats["classification"].value("has_kg", false);
                            }
                        } catch (const json::exception&) {
                        }
                    }
                }

                lastHasKg = hasKg;
                const bool kgSignalReady = hasKg || (lastKgConsumed > 0);

                const bool queueIdle = (lastQueueDepth == 0 && lastPostQueued == 0);
                const bool stageInflightIdle =
                    (lastExtractionInflight == 0 && lastKgInflight == 0 &&
                     lastSymbolInflight == 0 && lastEntityInflight == 0 && lastTitleInflight == 0 &&
                     lastTitleQueueDepth == 0);

                bool stickyInflightReady = false;
                if (kgSignalReady && queueIdle && stageInflightIdle && lastPostInflight > 0) {
                    if (!stickyInflightActive || stickyInflightValue != lastPostInflight) {
                        stickyInflightActive = true;
                        stickyInflightValue = lastPostInflight;
                        stickyInflightSince = now;
                    } else if (now - stickyInflightSince >= stickyInflightGrace) {
                        stickyInflightReady = true;
                    }
                } else {
                    stickyInflightActive = false;
                    stickyInflightValue = 0;
                }

                if (queuesDrained && kgSignalReady) {
                    stableReadyChecks++;
                    if (stableReadyChecks >= 4) {
                        kgReady = true;
                        break;
                    }
                } else if (stickyInflightReady) {
                    kgReady = true;
                    acceptedStickyInflight = true;
                    break;
                } else {
                    stableReadyChecks = 0;
                }

                std::this_thread::sleep_for(500ms);
            }

            if (!kgReady) {
                throw std::runtime_error(
                    "KG not ready before benchmark queries (has_kg=" +
                    std::string(lastHasKg ? "true" : "false") +
                    ", kg_consumed=" + std::to_string(lastKgConsumed) +
                    ", post_ingest_queue_depth=" + std::to_string(lastQueueDepth) +
                    ", post_ingest_queued=" + std::to_string(lastPostQueued) +
                    ", post_ingest_inflight=" + std::to_string(lastPostInflight) +
                    ", extraction_inflight=" + std::to_string(lastExtractionInflight) +
                    ", kg_inflight=" + std::to_string(lastKgInflight) +
                    ", symbol_inflight=" + std::to_string(lastSymbolInflight) +
                    ", entity_inflight=" + std::to_string(lastEntityInflight) +
                    ", title_inflight=" + std::to_string(lastTitleInflight) +
                    ", title_queue_depth=" + std::to_string(lastTitleQueueDepth) +
                    "). Increase YAMS_BENCH_KG_READY_TIMEOUT or reduce ingestion load.");
            }

            if (!lastHasKg) {
                spdlog::warn(
                    "KG readiness satisfied via kg_consumed={}, but corpus_stats.has_kg=false "
                    "(low symbol_density). Graph signals may still be weak.",
                    lastKgConsumed);
            } else if (acceptedStickyInflight) {
                spdlog::warn(
                    "KG readiness accepted with sticky post_ingest_inflight={} after {}s grace "
                    "(queues drained + stage inflight idle + corpus_stats.has_kg=true).",
                    lastPostInflight, stickyInflightGraceSec);
            } else {
                spdlog::info("KG readiness satisfied (queues drained + corpus_stats.has_kg=true)");
            }
        }

        // Verify document count using status metrics (avoids degraded search false negatives)
        uint64_t indexedDocCount = 0;
        auto statusResult = yams::cli::run_sync(client->status(), 5s);
        if (warmCacheMetadata && warmCacheMetadata->status == "primed") {
            indexedDocCount = static_cast<uint64_t>(warmCacheMetadata->expectedDocs);
        } else if (statusResult) {
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
                uint64_t documentsEmbedded = 0;
                uint64_t vectorCount = 0;
                uint64_t preparedDocsQueued = 0;
                uint64_t preparedChunksQueued = 0;
                uint64_t titleInflight = 0;
                uint64_t titleQueueDepth = 0;
                uint64_t titleQueued = 0;
                uint64_t titleDropped = 0;
                uint64_t titleConsumed = 0;
                if (auto it = st.requestCounts.find("documents_total");
                    it != st.requestCounts.end()) {
                    documentsTotal = it->second;
                }
                if (auto it = st.requestCounts.find("documents_embedded");
                    it != st.requestCounts.end()) {
                    documentsEmbedded = it->second;
                }
                if (auto it = st.requestCounts.find("vector_count"); it != st.requestCounts.end()) {
                    vectorCount = it->second;
                }
                if (auto it = st.requestCounts.find("bus_embed_prepared_docs_queued");
                    it != st.requestCounts.end()) {
                    preparedDocsQueued = it->second;
                }
                if (auto it = st.requestCounts.find("bus_embed_prepared_chunks_queued");
                    it != st.requestCounts.end()) {
                    preparedChunksQueued = it->second;
                }
                if (auto it = st.requestCounts.find("title_inflight");
                    it != st.requestCounts.end()) {
                    titleInflight = it->second;
                }
                if (auto it = st.requestCounts.find("title_queue_depth");
                    it != st.requestCounts.end()) {
                    titleQueueDepth = it->second;
                }
                if (auto it = st.requestCounts.find("title_queued"); it != st.requestCounts.end()) {
                    titleQueued = it->second;
                }
                if (auto it = st.requestCounts.find("title_dropped");
                    it != st.requestCounts.end()) {
                    titleDropped = it->second;
                }
                if (auto it = st.requestCounts.find("title_consumed");
                    it != st.requestCounts.end()) {
                    titleConsumed = it->second;
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
                statusEntry.returnedPaths.push_back("documents_embedded=" +
                                                    std::to_string(documentsEmbedded));
                statusEntry.returnedPaths.push_back("vector_count=" + std::to_string(vectorCount));
                statusEntry.returnedPaths.push_back("bus_embed_prepared_docs_queued=" +
                                                    std::to_string(preparedDocsQueued));
                statusEntry.returnedPaths.push_back("bus_embed_prepared_chunks_queued=" +
                                                    std::to_string(preparedChunksQueued));
                statusEntry.returnedPaths.push_back("title_inflight=" +
                                                    std::to_string(titleInflight));
                statusEntry.returnedPaths.push_back("title_queue_depth=" +
                                                    std::to_string(titleQueueDepth));
                statusEntry.returnedPaths.push_back("title_queued=" + std::to_string(titleQueued));
                statusEntry.returnedPaths.push_back("title_dropped=" +
                                                    std::to_string(titleDropped));
                statusEntry.returnedPaths.push_back("title_consumed=" +
                                                    std::to_string(titleConsumed));
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

                double tunedKgWeight = 0.0;
                bool tunedGraphRerank = graphRerankRequested;
                if (auto it = st.searchTuningParams.find("kg_weight");
                    it != st.searchTuningParams.end()) {
                    tunedKgWeight = static_cast<double>(it->second);
                }
                if (auto it = st.searchTuningParams.find("enable_graph_rerank");
                    it != st.searchTuningParams.end()) {
                    tunedGraphRerank = it->second > 0.5;
                }
                spdlog::info("[Bench] Effective graph rerank: env={} tuned={} kg_weight={:.4f}",
                             graphRerankRequested ? "on" : "off", tunedGraphRerank ? "on" : "off",
                             tunedKgWeight);

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

                const auto appendIfPresent = [&](std::string_view key) {
                    auto it = st.additionalStats.find(std::string(key));
                    if (it != st.additionalStats.end()) {
                        statsEntry.returnedPaths.push_back(std::string(key) + "=" + it->second);
                    }
                };
                appendIfPresent("kg_doc_entities_total");
                appendIfPresent("post_title_nl_docs_processed");
                appendIfPresent("post_title_nl_docs_with_entities");
                appendIfPresent("post_title_nl_entities_extracted");
                appendIfPresent("post_deferred_doc_entities_queued");
                appendIfPresent("post_deferred_doc_entity_queue_failures");
                appendIfPresent("kg_write_doc_entities_inserted");
                appendIfPresent("kg_write_deferred_doc_entities_skipped");

                statsEntry.extraFields = {
                    {"additional_stats", st.additionalStats},
                    {"total_documents", st.totalDocuments},
                    {"indexed_documents", st.indexedDocuments},
                    {"vector_index_size", st.vectorIndexSize},
                };

                for (const auto& [k, v] : st.additionalStats) {
                    statsEntry.returnedPaths.push_back(k + "=" + v);
                }

                debugLogWriteJsonLine(statsEntry);
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

        g_debugRunContext.reset();
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

static OptimizationRunResult runOptimizationCandidate(const OptimizationCandidate& candidate,
                                                      const std::optional<fs::path>& outputFile) {
    OptimizationRunResult result;
    result.candidate = candidate;
    result.runId = makeRunId(candidate.name);

    std::vector<EnvSetting> candidateOverrides = candidate.envOverrides;
    const char* debugBaseEnv = std::getenv("YAMS_BENCH_DEBUG_FILE");
    if (debugBaseEnv && std::strlen(debugBaseEnv) > 0) {
        const fs::path basePath(debugBaseEnv);
        const fs::path candidateDebugPath =
            makeCandidateDebugPath(basePath, candidate.name, result.runId);
        result.debugFile = candidateDebugPath.string();
        candidateOverrides.push_back({"YAMS_BENCH_DEBUG_FILE", result.debugFile});

        const bool hasExplicitTrace = hasOverrideKey(candidateOverrides, "YAMS_SEARCH_STAGE_TRACE");
        if (!hasExplicitTrace && std::getenv("YAMS_SEARCH_STAGE_TRACE") == nullptr) {
            candidateOverrides.push_back({"YAMS_SEARCH_STAGE_TRACE", "1"});
            result.stageTraceEnabled = true;
        } else {
            result.stageTraceEnabled = envTruthy(std::getenv("YAMS_SEARCH_STAGE_TRACE"));
        }

        const int traceTopNDefault = parseIntEnvOrDefault("YAMS_BENCH_TRACE_TOP_N", 50, 1, 10000);
        const int traceComponentDefault = parseIntEnvOrDefault(
            "YAMS_BENCH_TRACE_COMPONENT_TOP_N", std::min(traceTopNDefault, 50), 1, 10000);

        const bool hasExplicitTraceTop =
            hasOverrideKey(candidateOverrides, "YAMS_SEARCH_STAGE_TRACE_TOP_N");
        if (!hasExplicitTraceTop && std::getenv("YAMS_SEARCH_STAGE_TRACE_TOP_N") == nullptr) {
            candidateOverrides.push_back(
                {"YAMS_SEARCH_STAGE_TRACE_TOP_N", std::to_string(traceTopNDefault)});
        }

        const bool hasExplicitComponentTop =
            hasOverrideKey(candidateOverrides, "YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N");
        if (!hasExplicitComponentTop &&
            std::getenv("YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N") == nullptr) {
            candidateOverrides.push_back(
                {"YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N", std::to_string(traceComponentDefault)});
        }
    } else {
        result.stageTraceEnabled = envTruthy(std::getenv("YAMS_SEARCH_STAGE_TRACE"));
    }

    ScopedEnvOverrides scopedEnv(candidateOverrides);
    result.traceTopN = parseIntEnvOrDefault("YAMS_SEARCH_STAGE_TRACE_TOP_N", 0, 0, 10000);
    result.traceComponentTopN =
        parseIntEnvOrDefault("YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N", 0, 0, 10000);
    g_debugRunContext = DebugRunContext{
        .runId = result.runId,
        .candidate = result.candidate.name,
        .dataset = "",
        .corpusSize = 0,
        .numQueries = 0,
        .topK = 0,
        .traceTopN = result.traceTopN,
        .traceComponentTopN = result.traceComponentTopN,
        .stageTraceEnabled = envTruthy(std::getenv("YAMS_SEARCH_STAGE_TRACE")),
        .debugFile = result.debugFile,
    };
    CleanupFixture();

    try {
        SetupFixture();
        if (!g_fixture || !g_fixture->client) {
            throw std::runtime_error("Benchmark fixture did not initialize a daemon client");
        }

        auto status = yams::cli::run_sync(g_fixture->client->status(), 5s);
        if (status) {
            result.tuningState = status.value().searchTuningState;
            result.tuningReason = status.value().searchTuningReason;
        }

        g_debugRunContext->dataset = g_fixture->useBEIR ? g_fixture->beirDatasetName : "synthetic";
        g_debugRunContext->corpusSize = g_fixture->corpusSize;
        g_debugRunContext->numQueries = static_cast<int>(g_fixture->queries.size());
        g_debugRunContext->topK = g_fixture->topK;
        g_debugRunContext->traceTopN =
            parseIntEnvOrDefault("YAMS_SEARCH_STAGE_TRACE_TOP_N", result.traceTopN, 0, 10000);
        g_debugRunContext->traceComponentTopN = parseIntEnvOrDefault(
            "YAMS_SEARCH_STAGE_TRACE_COMPONENT_TOP_N", result.traceComponentTopN, 0, 10000);
        g_debugRunContext->stageTraceEnabled = envTruthy(std::getenv("YAMS_SEARCH_STAGE_TRACE"));
        result.traceTopN = g_debugRunContext->traceTopN;
        result.traceComponentTopN = g_debugRunContext->traceComponentTopN;
        result.stageTraceEnabled = g_debugRunContext->stageTraceEnabled;

        const auto hybridStart = std::chrono::steady_clock::now();
        result.hybridMetrics =
            evaluateQueries(*g_fixture->client, g_fixture->benchCorpusDir, g_fixture->queries,
                            g_fixture->topK, "hybrid", &result.hybridDiagnostics);
        const auto hybridEnd = std::chrono::steady_clock::now();
        result.hybridEvalMs =
            std::chrono::duration<double, std::milli>(hybridEnd - hybridStart).count();

        const auto keywordStart = std::chrono::steady_clock::now();
        result.keywordMetrics =
            evaluateQueries(*g_fixture->client, g_fixture->benchCorpusDir, g_fixture->queries,
                            g_fixture->topK, "keyword", &result.keywordDiagnostics);
        const auto keywordEnd = std::chrono::steady_clock::now();
        result.keywordEvalMs =
            std::chrono::duration<double, std::milli>(keywordEnd - keywordStart).count();

        if (result.hybridMetrics.numQueries <= 0) {
            throw std::runtime_error("No benchmark queries generated for optimization candidate");
        }

        const double avgHybridQueryMs =
            result.hybridEvalMs / std::max(1, result.hybridMetrics.numQueries);
        result.objectiveScore = computeOptimizationObjective(
            result.hybridMetrics, result.keywordMetrics, avgHybridQueryMs);
        result.success = true;
    } catch (const std::exception& e) {
        result.success = false;
        result.errorMessage = e.what();
    }

    g_debugRunContext.reset();
    CleanupFixture();

    if (outputFile.has_value()) {
        appendOptimizationResultJson(*outputFile, result);
    }

    return result;
}

static int runOptimizationLoop() {
    std::vector<OptimizationCandidate> candidates = defaultOptimizationCandidates();
    const auto optProfile = parseOptimizationProfile(std::getenv("YAMS_BENCH_OPT_PROFILE"));
    const bool hasExplicitCandidateFilter =
        std::getenv("YAMS_BENCH_OPT_CANDIDATE") &&
        std::strlen(std::getenv("YAMS_BENCH_OPT_CANDIDATE")) > 0;

    if (!hasExplicitCandidateFilter) {
        std::vector<OptimizationCandidate> filteredByProfile;
        filteredByProfile.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            const bool keep =
                optProfile == OptimizationProfile::All ||
                (optProfile == OptimizationProfile::EnginePolicy &&
                 candidate.enginePolicyCandidate) ||
                (optProfile == OptimizationProfile::Diagnostics && candidate.diagnosticCandidate);
            if (keep) {
                filteredByProfile.push_back(candidate);
            }
        }
        if (!filteredByProfile.empty()) {
            spdlog::info("[OptLoop] No candidate filter set; using optimization profile '{}' with "
                         "{} candidates",
                         optimizationProfileName(optProfile), filteredByProfile.size());
            candidates = std::move(filteredByProfile);
        }
    }

    const auto selectedCandidates = parseCandidateFilter(std::getenv("YAMS_BENCH_OPT_CANDIDATE"));
    if (!selectedCandidates.empty()) {
        std::vector<OptimizationCandidate> filtered;
        filtered.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            if (selectedCandidates.count(candidate.name) > 0) {
                filtered.push_back(candidate);
            }
        }
        candidates = std::move(filtered);
        if (candidates.empty()) {
            std::cerr << "No optimization candidates matched YAMS_BENCH_OPT_CANDIDATE='"
                      << std::getenv("YAMS_BENCH_OPT_CANDIDATE") << "'\n";
            return 2;
        }
    }

    bool pruneKnownBadCandidates = true;
    if (const char* pruneEnv = std::getenv("YAMS_BENCH_OPT_PRUNE_KNOWN_BAD"); pruneEnv) {
        pruneKnownBadCandidates = envTruthy(pruneEnv);
    }

    if (pruneKnownBadCandidates && selectedCandidates.empty()) {
        static const std::unordered_set<std::string> kKnownBadCandidates = {
            "mixed_precision_rrf_neighbor_t92_p80_k14", "mixed_precision_rrf_neighbor_t92_p78_k16",
            "mixed_precision_rrf_neighbor_t90_p84_k18", "mixed_ablation_disable_tiered_execution",
            "mixed_ablation_disable_graph_rerank",
        };

        const auto beforeCount = candidates.size();
        candidates.erase(std::remove_if(candidates.begin(), candidates.end(),
                                        [](const OptimizationCandidate& candidate) {
                                            return kKnownBadCandidates.count(candidate.name) > 0;
                                        }),
                         candidates.end());

        const auto removed = beforeCount - candidates.size();
        if (removed > 0) {
            spdlog::info("[OptLoop] Pruned {} known low-value candidates (set "
                         "YAMS_BENCH_OPT_PRUNE_KNOWN_BAD=0 to disable)",
                         removed);
        }
    }

    if (const char* maxCandidatesEnv = std::getenv("YAMS_BENCH_OPT_MAX_CANDIDATES")) {
        try {
            const auto parsed = std::max(1, std::stoi(maxCandidatesEnv));
            if (static_cast<std::size_t>(parsed) < candidates.size()) {
                candidates.resize(static_cast<std::size_t>(parsed));
            }
        } catch (...) {
            spdlog::warn("[OptLoop] Invalid YAMS_BENCH_OPT_MAX_CANDIDATES='{}', using default {}",
                         maxCandidatesEnv, candidates.size());
        }
    }

    std::optional<fs::path> outputFile;
    if (const char* outputEnv = std::getenv("YAMS_BENCH_OPT_RESULTS_FILE");
        outputEnv && std::strlen(outputEnv) > 0) {
        outputFile = fs::path(outputEnv);
        const bool appendMode = envTruthy(std::getenv("YAMS_BENCH_OPT_APPEND_RESULTS"));
        std::ofstream probe(*outputFile, appendMode ? (std::ios::out | std::ios::app)
                                                    : (std::ios::out | std::ios::trunc));
        if (!probe.is_open()) {
            spdlog::warn("[OptLoop] Could not create optimization results file: {}",
                         outputFile->string());
            outputFile.reset();
        } else {
            spdlog::info("[OptLoop] Writing optimization results to {} ({})", outputFile->string(),
                         appendMode ? "append" : "truncate");
        }
    }

    std::cout << "\n" << std::string(78, '=') << "\n";
    std::cout << "            RETRIEVAL OPTIMIZATION LOOP (code/mixed first)\n";
    std::cout << std::string(78, '=') << "\n";
    std::cout << "Profile: " << optimizationProfileName(optProfile)
              << (hasExplicitCandidateFilter ? " (explicit candidate filter)" : "") << "\n";

    std::vector<OptimizationRunResult> allResults;
    allResults.reserve(candidates.size());

    for (std::size_t i = 0; i < candidates.size(); ++i) {
        const auto& candidate = candidates[i];
        spdlog::info("[OptLoop] Candidate {}/{}: {} - {}", i + 1, candidates.size(), candidate.name,
                     candidate.description);

        auto result = runOptimizationCandidate(candidate, outputFile);
        allResults.push_back(result);

        if (!result.success) {
            std::cout << "\n  [" << (i + 1) << "/" << candidates.size() << "] " << candidate.name
                      << " -> FAILED: " << result.errorMessage << "\n";
            continue;
        }

        const double avgMs = result.hybridEvalMs / std::max(1, result.hybridMetrics.numQueries);
        const double mrrDelta = result.hybridMetrics.mrr - result.keywordMetrics.mrr;
        std::cout << "\n  [" << (i + 1) << "/" << candidates.size() << "] " << candidate.name
                  << "\n";
        std::cout << "    objective=" << std::fixed << std::setprecision(4) << result.objectiveScore
                  << "  hybrid_mrr=" << result.hybridMetrics.mrr
                  << "  keyword_mrr=" << result.keywordMetrics.mrr << "  mrr_delta=" << std::showpos
                  << std::setprecision(4) << mrrDelta << std::noshowpos
                  << "  dup@k=" << std::setprecision(4) << result.hybridMetrics.duplicateRateAtK
                  << "  avg_hybrid_ms=" << std::setprecision(1) << avgMs << "\n";
        const auto hybridDiag = queryDiagnosticsToJson(result.hybridDiagnostics);
        std::cout << "    trace_cov=" << std::setprecision(3)
                  << hybridDiag["trace_coverage"].get<double>() << "  sem_rescue_mean="
                  << hybridDiag["semantic_rescue_rate"]["mean"].get<double>()
                  << "  vec_only_below_mean="
                  << hybridDiag["vector_only_below_threshold"]["mean"].get<double>()
                  << "  no_relevant_hit_rate="
                  << hybridDiag["query_without_relevant_hit_rate"].get<double>()
                  << "  graph_apply_rate=" << hybridDiag["graph_rerank_apply_rate"].get<double>()
                  << "  miss_pre_fusion_rate="
                  << hybridDiag["miss_missing_pre_fusion_rate"].get<double>() << "\n";
        std::cout << "    timing_tradeoffs=" << summarizeTimingTradeoffs(hybridDiag) << "\n";
        if (result.traceTopN > 0 || result.traceComponentTopN > 0) {
            std::cout << "    trace_top_n=" << result.traceTopN
                      << "  trace_component_top_n=" << result.traceComponentTopN << "\n";
        }
        std::cout << "    tuning_state="
                  << (result.tuningState.empty() ? "<unknown>" : result.tuningState) << "\n";
        if (!result.debugFile.empty()) {
            std::cout << "    debug_file=" << result.debugFile << "\n";
        }
    }

    std::vector<OptimizationRunResult> successful;
    for (const auto& result : allResults) {
        if (result.success) {
            successful.push_back(result);
        }
    }

    if (successful.empty()) {
        std::cout << "\nNo successful optimization candidates completed.\n";
        return 2;
    }

    std::sort(successful.begin(), successful.end(),
              [](const OptimizationRunResult& a, const OptimizationRunResult& b) {
                  return a.objectiveScore > b.objectiveScore;
              });

    const auto& best = successful.front();
    g_final_metrics = best.hybridMetrics;
    g_keyword_metrics = best.keywordMetrics;

    std::cout << "\n" << std::string(78, '-') << "\n";
    std::cout << "Top candidates by objective\n";
    std::cout << std::string(78, '-') << "\n";
    for (std::size_t i = 0; i < successful.size(); ++i) {
        const auto& result = successful[i];
        const double avgMs = result.hybridEvalMs / std::max(1, result.hybridMetrics.numQueries);
        const double mrrDelta = result.hybridMetrics.mrr - result.keywordMetrics.mrr;
        std::cout << "  " << (i + 1) << ". " << result.candidate.name
                  << "  objective=" << std::fixed << std::setprecision(4) << result.objectiveScore
                  << "  mrr=" << result.hybridMetrics.mrr
                  << "  ndcg=" << result.hybridMetrics.ndcgAtK << "  mrr_delta=" << std::showpos
                  << std::setprecision(4) << mrrDelta << std::noshowpos
                  << "  dup@k=" << std::setprecision(4) << result.hybridMetrics.duplicateRateAtK
                  << "  avg_ms=" << std::setprecision(1) << avgMs << "\n";
    }

    std::cout << "\nBest candidate: " << best.candidate.name << "\n";
    std::cout << "  " << best.candidate.description << "\n";

    std::ostringstream envLine;
    for (const auto& setting : best.candidate.envOverrides) {
        if (setting.value.has_value()) {
            envLine << setting.key << "=" << *setting.value << " ";
        }
    }
    std::cout << "  Recommended env overrides: " << envLine.str() << "\n";

    if (outputFile.has_value()) {
        std::cout << "  Saved per-run JSONL to: " << outputFile->string() << "\n";
    }

    const char* datasetEnv = std::getenv("YAMS_BENCH_DATASET");
    if (!(datasetEnv && std::strlen(datasetEnv) > 0)) {
        std::cout << "  Next: validate this winner on a BEIR dataset, e.g. "
                  << "YAMS_BENCH_DATASET=scifact YAMS_BENCH_OPT_LOOP=1 ...\n";
    }

    std::cout << std::string(78, '=') << "\n";
    return 0;
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
    if (envTruthy(std::getenv("YAMS_BENCH_OPT_LOOP"))) {
        return runOptimizationLoop();
    }

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
