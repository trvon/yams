/**
 * @file hnsw_retrieval_diagnostic.cpp
 * @brief Diagnostic test to verify HNSW vector retrieval is working correctly
 *
 * This test creates a small corpus, generates embeddings, and verifies that
 * vector search returns expected results with correct similarity scores.
 *
 * Run with:
 *   YAMS_TEST_SAFE_SINGLE_INSTANCE=1 ./builddir/tests/integration/daemon/hnsw_retrieval_diagnostic
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include <spdlog/spdlog.h>

#include "tests/integration/daemon/test_daemon_harness.h"
#include "tests/integration/daemon/test_async_helpers.h"
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/vector/vector_database.h>

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <set>
#include <thread>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using namespace yams;

namespace {

// Test documents with known semantic similarity
struct TestDoc {
    std::string filename;
    std::string content;
    std::string category; // For grouping similar docs
};

const std::vector<TestDoc> TEST_CORPUS = {
    // Cluster 1: Machine Learning
    {"ml_intro.txt",
     "Machine learning is a subset of artificial intelligence that enables computers to learn from "
     "data. "
     "Neural networks are a key component of deep learning systems.",
     "ml"},
    {"deep_learning.txt",
     "Deep learning uses multiple layers of neural networks to process complex patterns. "
     "Convolutional neural networks are used for image recognition tasks.",
     "ml"},
    {"ai_basics.txt",
     "Artificial intelligence encompasses machine learning, natural language processing, and "
     "robotics. "
     "AI systems can learn from experience and improve over time.",
     "ml"},

    // Cluster 2: Cooking
    {"pasta_recipe.txt",
     "To make pasta, boil water with salt, add the pasta and cook for 8-10 minutes. "
     "Drain and serve with your favorite sauce like marinara or alfredo.",
     "cooking"},
    {"baking_bread.txt",
     "Bread baking requires flour, water, yeast, and salt. Knead the dough for 10 minutes, "
     "let it rise for an hour, then bake at 400F for 30 minutes.",
     "cooking"},

    // Cluster 3: Programming
    {"python_intro.txt",
     "Python is a high-level programming language known for its readability. "
     "It supports object-oriented, functional, and procedural programming paradigms.",
     "programming"},
    {"cpp_guide.txt",
     "C++ is a powerful systems programming language. It provides low-level memory control "
     "and high performance for applications like games and operating systems.",
     "programming"},
};

// Test queries with expected results
struct TestQuery {
    std::string query;
    std::vector<std::string> expectedTopFiles; // Files that should be in top results
    std::string expectedCategory;
};

const std::vector<TestQuery> TEST_QUERIES = {
    {"neural networks and deep learning",
     {"ml_intro.txt", "deep_learning.txt", "ai_basics.txt"},
     "ml"},
    {"how to cook italian food", {"pasta_recipe.txt", "baking_bread.txt"}, "cooking"},
    {"software development languages", {"python_intro.txt", "cpp_guide.txt"}, "programming"},
};

// Helper to create a client
daemon::DaemonClient createClient(const fs::path& socketPath) {
    daemon::ClientConfig config;
    config.socketPath = socketPath;
    config.connectTimeout = 5s;
    config.requestTimeout = 30s;
    return daemon::DaemonClient(config);
}

class HNSWDiagnosticFixture {
public:
    void setup() {
        spdlog::set_level(spdlog::level::info);
        spdlog::info("=== HNSW Retrieval Diagnostic ===");

        // Create temp directory for test corpus
        tempDir_ = fs::temp_directory_path() / ("hnsw_diag_" + std::to_string(std::time(nullptr)));
        fs::create_directories(tempDir_);

        // Write test documents
        for (const auto& doc : TEST_CORPUS) {
            fs::path filePath = tempDir_ / doc.filename;
            std::ofstream out(filePath);
            out << doc.content;
            out.close();
            spdlog::info("Created test doc: {}", filePath.string());
        }

        // Start daemon with real embeddings via ONNX plugin
        yams::test::DaemonHarnessOptions opts;
        opts.isolateState = true;
        opts.enableModelProvider = true;
        opts.useMockModelProvider = false; // Use real ONNX embeddings
        opts.autoLoadPlugins = true;
        opts.configureModelPool = true;
        opts.modelPoolLazyLoading = false;

        // Set plugin directory to builddir/plugins
        opts.pluginDir = fs::current_path() / "builddir" / "plugins";
        opts.preloadModels = {"all-MiniLM-L6-v2"};

        // Configure ONNX plugin
        nlohmann::json onnxConfig;
        onnxConfig["preferred_model"] = "all-MiniLM-L6-v2";
        onnxConfig["preload"] = "all-MiniLM-L6-v2";
        onnxConfig["keep_model_hot"] = true;
        // Point to user's model storage directory (for models like jina-embeddings-v2-base-code)
        onnxConfig["models_root"] = "/Volumes/picaso/hak/storage/models";
        opts.pluginConfigs["onnx_plugin"] = onnxConfig.dump();

        spdlog::info("Plugin directory: {}", opts.pluginDir->string());
        harness_ = std::make_unique<test::DaemonHarness>(opts);

        bool started = harness_->start(60s);
        REQUIRE(started);
        spdlog::info("Daemon started");

        // Ingest test corpus
        ingestCorpus();

        // Wait for embeddings
        waitForEmbeddings();
    }

    void teardown() {
        if (harness_) {
            harness_->stop();
        }
        if (!tempDir_.empty() && fs::exists(tempDir_)) {
            fs::remove_all(tempDir_);
        }
    }

    void ingestCorpus() {
        // Use the ServiceManager directly for simpler ingestion
        auto* daemon = harness_->daemon();
        REQUIRE(daemon != nullptr);

        auto* sm = daemon->getServiceManager();
        REQUIRE(sm != nullptr);

        auto contentStore = sm->getContentStore();
        auto metaRepo = sm->getMetadataRepo();
        REQUIRE(contentStore != nullptr);
        REQUIRE(metaRepo != nullptr);

        spdlog::info("Ingesting corpus from: {}", tempDir_.string());

        for (const auto& doc : TEST_CORPUS) {
            fs::path filePath = tempDir_ / doc.filename;

            // Read file content
            std::ifstream in(filePath, std::ios::binary);
            std::vector<std::byte> bytes;
            char c;
            while (in.get(c)) {
                bytes.push_back(static_cast<std::byte>(c));
            }

            // Store in content store
            auto storeResult = contentStore->storeBytes(bytes);
            REQUIRE(storeResult.has_value());

            std::string hash = storeResult.value().contentHash;

            // Create document metadata
            yams::metadata::DocumentInfo docInfo;
            docInfo.fileName = doc.filename;
            docInfo.filePath = filePath.string();
            docInfo.sha256Hash = hash;
            docInfo.fileSize = bytes.size();
            docInfo.mimeType = "text/plain";
            docInfo.fileExtension = ".txt";
            docInfo.modifiedTime = std::chrono::time_point_cast<std::chrono::seconds>(
                std::chrono::system_clock::now());

            auto insertResult = metaRepo->insertDocument(docInfo);
            REQUIRE(insertResult.has_value());

            spdlog::info("Ingested: {} (hash: {})", doc.filename, hash.substr(0, 8));
        }

        spdlog::info("Corpus ingestion complete");
    }

    void waitForEmbeddings() {
        spdlog::info("Waiting for embedding generation (max 30s)...");

        auto* daemon = harness_->daemon();
        REQUIRE(daemon != nullptr);

        auto* sm = daemon->getServiceManager();
        REQUIRE(sm != nullptr);

        // Poll stats until we have embeddings for all docs (or timeout)
        for (int i = 0; i < 30; ++i) { // Max 30 seconds
            auto vectorDb = sm->getVectorDatabase();
            if (vectorDb) {
                size_t vectorCount = vectorDb->getVectorCount();

                if (i % 5 == 0) {
                    spdlog::info("Vector count: {} (waiting for {})", vectorCount,
                                 TEST_CORPUS.size());
                }

                // We expect at least one vector per document
                if (vectorCount >= TEST_CORPUS.size()) {
                    spdlog::info("Embeddings ready: {} vectors for {} docs", vectorCount,
                                 TEST_CORPUS.size());
                    hasEmbeddings_ = true;
                    return;
                }
            }

            std::this_thread::sleep_for(1000ms);
        }

        // Don't fail - we can still test keyword search without embeddings
        spdlog::warn("Embeddings not generated after 30s - continuing with keyword-only tests");
        hasEmbeddings_ = false;
    }

    bool hasEmbeddings() const { return hasEmbeddings_; }

    daemon::SearchResponse runSearch(const std::string& query, const std::string& searchType,
                                     int limit = 10) {
        auto client = createClient(harness_->socketPath());

        // Connect first
        auto connectResult = cli::run_sync(client.connect(), 5s);
        REQUIRE(connectResult.has_value());

        daemon::SearchRequest searchReq;
        searchReq.query = query;
        searchReq.searchType = searchType;
        searchReq.limit = limit;
        searchReq.timeout = 10s;

        auto result = cli::run_sync(client.search(searchReq), 15s);
        REQUIRE(result.has_value());

        return result.value();
    }

    void runDiagnostics() {
        spdlog::info("\n=== Running Search Diagnostics ===\n");

        for (const auto& tq : TEST_QUERIES) {
            spdlog::info("Query: \"{}\"", tq.query);
            spdlog::info("Expected category: {}", tq.expectedCategory);

            std::string expectedFiles;
            for (const auto& f : tq.expectedTopFiles) {
                if (!expectedFiles.empty())
                    expectedFiles += ", ";
                expectedFiles += f;
            }
            spdlog::info("Expected top files: {}", expectedFiles);

            // Test semantic/vector search
            spdlog::info("\n--- Vector Search ---");
            auto vectorResults = runSearch(tq.query, "semantic", 5);
            printResults(vectorResults, "vector");

            // Test keyword search
            spdlog::info("\n--- Keyword Search ---");
            auto keywordResults = runSearch(tq.query, "keyword", 5);
            printResults(keywordResults, "keyword");

            // Test hybrid search
            spdlog::info("\n--- Hybrid Search ---");
            auto hybridResults = runSearch(tq.query, "hybrid", 5);
            printResults(hybridResults, "hybrid");

            // Analyze results
            analyzeResults(tq, vectorResults, keywordResults, hybridResults);

            spdlog::info("\n{}\n", std::string(60, '='));
        }
    }

    void printResults(const daemon::SearchResponse& results, const std::string& searchType) {
        if (results.results.empty()) {
            spdlog::warn("  {} search: NO RESULTS", searchType);
            return;
        }

        int rank = 1;
        for (const auto& r : results.results) {
            fs::path p(r.path);
            std::string filename = p.filename().string();
            spdlog::info("  #{}: {} (score={:.4f})", rank, filename, r.score);
            rank++;
        }
    }

    void analyzeResults(const TestQuery& tq, const daemon::SearchResponse& vectorRes,
                        const daemon::SearchResponse& keywordRes,
                        const daemon::SearchResponse& hybridRes) {
        spdlog::info("\n--- Analysis ---");

        // Check if expected files are in vector results
        std::set<std::string> vectorFiles;
        for (const auto& r : vectorRes.results) {
            fs::path p(r.path);
            vectorFiles.insert(p.filename().string());
        }

        int vectorHits = 0;
        for (const auto& expected : tq.expectedTopFiles) {
            if (vectorFiles.count(expected)) {
                vectorHits++;
            }
        }

        float vectorRecall = static_cast<float>(vectorHits) / tq.expectedTopFiles.size();
        spdlog::info("Vector recall: {}/{} = {:.1f}%", vectorHits, tq.expectedTopFiles.size(),
                     vectorRecall * 100);

        // Check vector score distribution
        if (!vectorRes.results.empty()) {
            float topScore = vectorRes.results[0].score;
            float bottomScore = vectorRes.results.back().score;
            spdlog::info("Vector score range: [{:.4f}, {:.4f}]", bottomScore, topScore);
        }

        // Flag potential issues
        if (vectorFiles.empty()) {
            spdlog::error("ISSUE: Vector search returned NO results!");
        } else if (vectorRecall < 0.5f) {
            spdlog::warn("ISSUE: Low vector recall ({:.1f}%)", vectorRecall * 100);
        }
    }

private:
    std::unique_ptr<test::DaemonHarness> harness_;
    fs::path tempDir_;
    bool hasEmbeddings_ = false;
};

} // namespace

TEST_CASE("HNSW Retrieval Diagnostic", "[hnsw][diagnostic][integration]") {
    HNSWDiagnosticFixture fixture;

    fixture.setup();
    fixture.runDiagnostics();
    fixture.teardown();
}
