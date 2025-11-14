/*
  engine_comparison_bench.cpp

  Google Benchmark to compare HybridSearchEngine vs new multi-component SearchEngine.
  Measures:
  - End-to-end query latency for both engines
  - Component-level timing (FTS5, vector, path tree, symbols, KG)
  - Result fusion overhead
  - Memory usage and metadata queries
  - Throughput (queries per second)

  Configuration (environment variables):
  - YAMS_BENCH_DB_PATH     : Path to test database. Default: creates temp DB
  - YAMS_BENCH_CORPUS_SIZE : Number of documents to index. Default: "1000"
  - YAMS_BENCH_QUERY       : Query string to run. Default: "search query test"
  - YAMS_BENCH_TOPK        : Limit for results. Default: "10"
  - YAMS_BENCH_WARMUP      : Number of warmup runs. Default: "3"
  - YAMS_BENCH_ITERATIONS  : Number of iterations. Default: "100"

  Output:
  - Comparative metrics for HybridSearchEngine vs SearchEngine
  - Component breakdown timings
  - Memory and query count metrics
  - Fusion strategy performance
*/

#include <spdlog/spdlog.h>
#include <benchmark/benchmark.h>

#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/database.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/migration.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_engine.h>         // New engine
#include <yams/search/search_engine_builder.h> // For MetadataKeywordAdapter
#include <yams/vector/embedding_generator.h>
#include <yams/vector/vector_database.h>
#include <yams/vector/vector_index_manager.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

// ============================================================================
// Configuration and Utilities
// ============================================================================

std::string getEnvOr(const char* key, const std::string& fallback) {
    if (const char* v = std::getenv(key)) {
        return std::string(v);
    }
    return fallback;
}

int toIntOr(const std::string& s, int fallback) {
    try {
        return std::stoi(s);
    } catch (...) {
        return fallback;
    }
}

struct BenchConfig {
    std::string dbPath;
    int corpusSize = 1000;
    std::string query = "search query test";
    int topK = 10;
    int warmup = 3;
    int iterations = 100;
    bool useTempDb = true;
};

BenchConfig getBenchConfig() {
    BenchConfig cfg;

    std::string dbPathEnv = getEnvOr("YAMS_BENCH_DB_PATH", "");
    if (dbPathEnv.empty()) {
        // Use /tmp for better cross-platform compatibility (similar to test_daemon_harness.h)
        cfg.dbPath = "/tmp/yams_engine_bench.db";
        cfg.useTempDb = true;
    } else {
        cfg.dbPath = dbPathEnv;
        cfg.useTempDb = false;
    }

    cfg.corpusSize = toIntOr(getEnvOr("YAMS_BENCH_CORPUS_SIZE", "1000"), 1000);
    cfg.query = getEnvOr("YAMS_BENCH_QUERY", "search query test");
    cfg.topK = toIntOr(getEnvOr("YAMS_BENCH_TOPK", "10"), 10);
    cfg.warmup = toIntOr(getEnvOr("YAMS_BENCH_WARMUP", "3"), 3);
    cfg.iterations = toIntOr(getEnvOr("YAMS_BENCH_ITERATIONS", "100"), 100);

    return cfg;
}

// ============================================================================
// Test Data Generator
// ============================================================================

class TestCorpusGenerator {
public:
    TestCorpusGenerator() : rng_(std::random_device{}()) {}

    std::string generateDocument(size_t id) {
        std::ostringstream oss;
        oss << "Document " << id << " - ";

        // Generate realistic content with varying terms
        const std::vector<std::string> words = {
            "search",     "query",  "test",      "benchmark",    "performance",
            "database",   "index",  "vector",    "metadata",     "result",
            "fusion",     "engine", "component", "optimization", "latency",
            "throughput", "memory", "parallel",  "algorithm",    "ranking"};

        std::uniform_int_distribution<size_t> lengthDist(50, 200);
        std::uniform_int_distribution<size_t> wordDist(0, words.size() - 1);

        size_t wordCount = lengthDist(rng_);
        for (size_t i = 0; i < wordCount; ++i) {
            if (i > 0)
                oss << " ";
            oss << words[wordDist(rng_)];
        }

        return oss.str();
    }

    std::string generatePath(size_t id) {
        std::uniform_int_distribution<int> dirDist(1, 10);
        std::ostringstream oss;
        oss << "/test/bench/dir" << dirDist(rng_) << "/doc_" << id << ".txt";
        return oss.str();
    }

private:
    std::mt19937 rng_;
};

// ============================================================================
// Test Fixture - Shared Database Setup
// ============================================================================

class SearchEngineFixture {
public:
    SearchEngineFixture(const BenchConfig& config) : config_(config) {
        setupDatabase();
        populateCorpus();
        setupEngines();
    }

    ~SearchEngineFixture() { tearDown(); }

    yams::search::HybridSearchEngine* getHybridEngine() { return hybridEngine_.get(); }
    yams::search::SearchEngine* getNewEngine() { return newEngine_.get(); }

    const BenchConfig& getConfig() const { return config_; }

private:
    void setupDatabase() {
        spdlog::info("Setting up test database at: {}", config_.dbPath);

        // Remove old database if using temp
        if (config_.useTempDb && fs::exists(config_.dbPath)) {
            fs::remove(config_.dbPath);
        }

        // Create database
        yams::metadata::Database db;
        auto openResult = db.open(config_.dbPath, yams::metadata::ConnectionMode::Create);
        if (!openResult) {
            throw std::runtime_error("Failed to open database: " + openResult.error().message);
        }

        // Run migrations
        yams::metadata::MigrationManager migrationMgr(db);
        auto initResult = migrationMgr.initialize();
        if (!initResult) {
            throw std::runtime_error("Failed to initialize migrations: " +
                                     initResult.error().message);
        }

        // Register all YAMS metadata migrations
        migrationMgr.registerMigrations(yams::metadata::YamsMetadataMigrations::getAllMigrations());

        auto migrateResult = migrationMgr.migrate();
        if (!migrateResult) {
            throw std::runtime_error("Failed to run migrations: " + migrateResult.error().message);
        }

        db.close();

        // Create connection pool
        yams::metadata::ConnectionPoolConfig poolConfig;
        poolConfig.maxConnections = 16;
        poolConfig.minConnections = 4;

        connectionPool_ =
            std::make_shared<yams::metadata::ConnectionPool>(config_.dbPath, poolConfig);
        auto poolInitResult = connectionPool_->initialize();
        if (!poolInitResult) {
            throw std::runtime_error("Failed to initialize pool: " +
                                     poolInitResult.error().message);
        }

        // Create metadata repository (takes reference to pool)
        metadataRepo_ = std::make_shared<yams::metadata::MetadataRepository>(*connectionPool_);
    }

    void populateCorpus() {
        spdlog::info("Populating corpus with {} documents", config_.corpusSize);

        TestCorpusGenerator generator;

        for (int i = 0; i < config_.corpusSize; ++i) {
            std::string content = generator.generateDocument(i);
            std::string filePath = generator.generatePath(i);

            yams::metadata::DocumentInfo doc;
            doc.filePath = filePath;
            doc.fileName = "doc_" + std::to_string(i) + ".txt";
            doc.fileExtension = ".txt";
            doc.mimeType = "text/plain";
            doc.fileSize = content.size();
            doc.sha256Hash = "hash_" + std::to_string(i); // Simplified hash
            doc.pathDepth = 4;                            // /test/bench/dirX/doc_N.txt

            // Insert document
            auto result = metadataRepo_->insertDocument(doc);
            if (!result) {
                spdlog::warn("Failed to insert document {}: {}", i, result.error().message);
            }

            // Index content in FTS5
            if (result) {
                auto docId = result.value();
                std::string title = "Document " + std::to_string(i);
                auto indexResult =
                    metadataRepo_->indexDocumentContent(docId, title, content, "text/plain");
                if (!indexResult) {
                    spdlog::warn("Failed to index document {}: {}", i, indexResult.error().message);
                }
            }
        }

        spdlog::info("Corpus populated successfully");
    }

    void setupEngines() {
        spdlog::info("Setting up search engines");

        // Setup HybridSearchEngine with MetadataKeywordAdapter
        auto keywordAdapter = std::make_shared<yams::search::MetadataKeywordAdapter>(metadataRepo_);

        // Create a minimal VectorIndexManager (required by HybridSearchEngine)
        yams::vector::IndexConfig vectorConfig;
        vectorConfig.dimension = 384;                      // Minimal dimension
        vectorConfig.type = yams::vector::IndexType::FLAT; // Simplest index
        vectorConfig.max_elements = 100;                   // Small capacity for benchmark
        vectorConfig.enable_persistence = false;           // No persistence needed
        auto vectorIndex = std::make_shared<yams::vector::VectorIndexManager>(vectorConfig);
        auto vectorInitResult = vectorIndex->initialize();
        if (!vectorInitResult) {
            spdlog::warn("VectorIndexManager initialization failed: {}",
                         vectorInitResult.error().message);
        }

        yams::search::HybridSearchConfig hybridConfig;
        hybridConfig.keyword_weight = 1.0f; // 100% keyword weight for keyword-only testing
        hybridConfig.vector_weight = 0.0f;  // 0% vector weight
        hybridConfig.final_top_k = config_.topK;

        hybridEngine_ = std::make_shared<yams::search::HybridSearchEngine>(
            vectorIndex, keywordAdapter, hybridConfig,
            nullptr // embeddingGen - not needed
        );

        auto initResult = hybridEngine_->initialize();
        if (!initResult) {
            spdlog::warn("HybridSearchEngine initialization failed: {}",
                         initResult.error().message);
        } else {
            spdlog::info("HybridSearchEngine initialized successfully");
        }

        // Setup new SearchEngine
        yams::search::SearchEngineConfig searchConfig;
        searchConfig.fts5Weight = 0.50f;  // Higher FTS5 weight for testing without vectors
        searchConfig.vectorWeight = 0.0f; // Disable vector for now (no embeddings)
        searchConfig.pathTreeWeight = 0.20f;
        searchConfig.symbolWeight = 0.20f;
        searchConfig.kgWeight = 0.10f;
        searchConfig.maxResults = config_.topK;

        newEngine_ = std::make_shared<yams::search::SearchEngine>(
            metadataRepo_,
            nullptr, // vectorDb - not needed for this benchmark
            nullptr, // vectorIndex - not needed
            nullptr, // embeddingGen - not needed
            nullptr, // kgStore - not needed for this benchmark
            searchConfig);

        // Run health check
        auto healthResult = newEngine_->healthCheck();
        if (!healthResult) {
            spdlog::warn("Health check failed: {}", healthResult.error().message);
        } else {
            spdlog::info("New SearchEngine initialized successfully");
        }
    }

    void tearDown() {
        // Reset engines first (they may hold references to repository)
        newEngine_.reset();
        hybridEngine_.reset();

        // Reset repository (releases connections back to pool)
        metadataRepo_.reset();

        // Shutdown pool (may log, so do before exit)
        if (connectionPool_) {
            try {
                connectionPool_->shutdown();
            } catch (...) {
                // Ignore shutdown errors during cleanup
            }
            connectionPool_.reset();
        }

        // Clean up database file
        if (config_.useTempDb && fs::exists(config_.dbPath)) {
            try {
                fs::remove(config_.dbPath);
            } catch (...) {
                // Ignore cleanup errors
            }
        }
    }

    BenchConfig config_;
    std::shared_ptr<yams::metadata::ConnectionPool> connectionPool_;
    std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<yams::search::HybridSearchEngine> hybridEngine_;
    std::shared_ptr<yams::search::SearchEngine> newEngine_;
};

// Global fixture instance (initialized once)
std::unique_ptr<SearchEngineFixture> g_fixture;

void EnsureFixture() {
    if (!g_fixture) {
        auto config = getBenchConfig();
        g_fixture = std::make_unique<SearchEngineFixture>(config);
    }
}

// ============================================================================
// Benchmarks - New SearchEngine
// ============================================================================

static void BM_NewEngine_Search(benchmark::State& state) {
    EnsureFixture();
    auto* engine = g_fixture->getNewEngine();
    const auto& config = g_fixture->getConfig();

    if (!engine) {
        state.SkipWithError("New engine not available");
        return;
    }

    // Warmup
    for (int i = 0; i < config.warmup; ++i) {
        auto result = engine->search(config.query);
        benchmark::DoNotOptimize(result);
    }

    // Benchmark
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto result = engine->search(config.query);
        auto end = std::chrono::high_resolution_clock::now();

        benchmark::DoNotOptimize(result);

        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        state.SetIterationTime(elapsed.count() / 1000000.0); // Convert to seconds

        if (result && !result.value().empty()) {
            state.counters["results"] = result.value().size();
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["queries_per_sec"] =
        benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_NewEngine_Search)->UseManualTime();

static void BM_NewEngine_FusionStrategies(benchmark::State& state) {
    EnsureFixture();
    auto* engine = g_fixture->getNewEngine();
    const auto& config = g_fixture->getConfig();

    if (!engine) {
        state.SkipWithError("New engine not available");
        return;
    }

    // Test different fusion strategies
    yams::search::SearchEngineConfig::FusionStrategy strategy =
        static_cast<yams::search::SearchEngineConfig::FusionStrategy>(state.range(0));

    auto engineConfig = engine->getConfig();
    engineConfig.fusionStrategy = strategy;
    engine->setConfig(engineConfig);

    // Benchmark
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto result = engine->search(config.query);
        auto end = std::chrono::high_resolution_clock::now();

        benchmark::DoNotOptimize(result);

        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        state.SetIterationTime(elapsed.count() / 1000000.0);
    }

    state.SetLabel([strategy]() {
        switch (strategy) {
            case yams::search::SearchEngineConfig::FusionStrategy::WEIGHTED_SUM:
                return "WeightedSum";
            case yams::search::SearchEngineConfig::FusionStrategy::RECIPROCAL_RANK:
                return "ReciprocalRank";
            case yams::search::SearchEngineConfig::FusionStrategy::BORDA_COUNT:
                return "BordaCount";
            case yams::search::SearchEngineConfig::FusionStrategy::WEIGHTED_RECIPROCAL:
                return "WeightedReciprocal";
            default:
                return "Unknown";
        }
    }());
}
BENCHMARK(BM_NewEngine_FusionStrategies)
    ->Arg(0) // WEIGHTED_SUM
    ->Arg(1) // RECIPROCAL_RANK
    ->Arg(2) // BORDA_COUNT
    ->Arg(3) // WEIGHTED_RECIPROCAL
    ->UseManualTime();

static void BM_NewEngine_ComponentWeights(benchmark::State& state) {
    EnsureFixture();
    auto* engine = g_fixture->getNewEngine();
    const auto& config = g_fixture->getConfig();

    if (!engine) {
        state.SkipWithError("New engine not available");
        return;
    }

    // Test different weight configurations
    auto engineConfig = engine->getConfig();

    // Configuration variants based on state.range(0)
    switch (state.range(0)) {
        case 0: // FTS5 only
            engineConfig.fts5Weight = 1.0f;
            engineConfig.pathTreeWeight = 0.0f;
            engineConfig.symbolWeight = 0.0f;
            engineConfig.kgWeight = 0.0f;
            break;
        case 1: // Balanced
            engineConfig.fts5Weight = 0.35f;
            engineConfig.pathTreeWeight = 0.15f;
            engineConfig.symbolWeight = 0.20f;
            engineConfig.kgWeight = 0.10f;
            break;
        case 2: // Path-heavy
            engineConfig.fts5Weight = 0.20f;
            engineConfig.pathTreeWeight = 0.50f;
            engineConfig.symbolWeight = 0.20f;
            engineConfig.kgWeight = 0.10f;
            break;
    }

    engine->setConfig(engineConfig);

    // Benchmark
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto result = engine->search(config.query);
        auto end = std::chrono::high_resolution_clock::now();

        benchmark::DoNotOptimize(result);

        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        state.SetIterationTime(elapsed.count() / 1000000.0);
    }
}
BENCHMARK(BM_NewEngine_ComponentWeights)
    ->Arg(0) // FTS5 only
    ->Arg(1) // Balanced
    ->Arg(2) // Path-heavy
    ->UseManualTime();

static void BM_NewEngine_Statistics(benchmark::State& state) {
    EnsureFixture();
    auto* engine = g_fixture->getNewEngine();
    const auto& config = g_fixture->getConfig();

    if (!engine) {
        state.SkipWithError("New engine not available");
        return;
    }

    // Reset statistics
    engine->resetStatistics();

    // Run searches
    for (auto _ : state) {
        auto result = engine->search(config.query);
        benchmark::DoNotOptimize(result);
    }

    // Get final statistics
    const auto& stats = engine->getStatistics();

    state.counters["total_queries"] = stats.totalQueries.load();
    state.counters["successful_queries"] = stats.successfulQueries.load();
    state.counters["failed_queries"] = stats.failedQueries.load();
    state.counters["fts5_queries"] = stats.fts5Queries.load();
    state.counters["avg_query_time_us"] = stats.avgQueryTimeMicros.load();
}
BENCHMARK(BM_NewEngine_Statistics);

// ============================================================================
// Benchmarks - Old HybridSearchEngine (for comparison)
// ============================================================================

static void BM_OldEngine_Search(benchmark::State& state) {
    EnsureFixture();
    auto* engine = g_fixture->getHybridEngine();
    const auto& config = g_fixture->getConfig();

    if (!engine) {
        state.SkipWithError("Hybrid engine not available");
        return;
    }

    // Warmup
    for (int i = 0; i < config.warmup; ++i) {
        auto result = engine->search(config.query, config.topK);
        benchmark::DoNotOptimize(result);
    }

    // Benchmark
    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();
        auto result = engine->search(config.query, config.topK);
        auto end = std::chrono::high_resolution_clock::now();

        benchmark::DoNotOptimize(result);

        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        state.SetIterationTime(elapsed.count() / 1000000.0); // Convert to seconds

        if (result && !result.value().empty()) {
            state.counters["results"] = result.value().size();
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.counters["queries_per_sec"] =
        benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_OldEngine_Search)->UseManualTime();

static void BM_OldEngine_Statistics(benchmark::State& state) {
    EnsureFixture();
    auto* engine = g_fixture->getHybridEngine();
    const auto& config = g_fixture->getConfig();

    if (!engine) {
        state.SkipWithError("Hybrid engine not available");
        return;
    }

    // Run searches
    size_t totalResults = 0;
    size_t successfulQueries = 0;
    size_t failedQueries = 0;

    for (auto _ : state) {
        auto result = engine->search(config.query, config.topK);
        benchmark::DoNotOptimize(result);

        if (result) {
            successfulQueries++;
            totalResults += result.value().size();
        } else {
            failedQueries++;
        }
    }

    state.counters["total_queries"] = state.iterations();
    state.counters["successful_queries"] = successfulQueries;
    state.counters["failed_queries"] = failedQueries;
    state.counters["avg_results"] = static_cast<double>(totalResults) / state.iterations();
}
BENCHMARK(BM_OldEngine_Statistics);

// ============================================================================
// Head-to-Head Comparison
// ============================================================================

static void BM_Comparison_BothEngines(benchmark::State& state) {
    EnsureFixture();
    auto* oldEngine = g_fixture->getHybridEngine();
    auto* newEngine = g_fixture->getNewEngine();
    const auto& config = g_fixture->getConfig();

    if (!oldEngine || !newEngine) {
        state.SkipWithError("Both engines must be available for comparison");
        return;
    }

    bool testOldEngine = (state.range(0) == 0);

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

        if (testOldEngine) {
            auto result = oldEngine->search(config.query, config.topK);
            benchmark::DoNotOptimize(result);
        } else {
            auto result = newEngine->search(config.query);
            benchmark::DoNotOptimize(result);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        state.SetIterationTime(elapsed.count() / 1000000.0);
    }

    state.SetLabel(testOldEngine ? "HybridSearchEngine" : "SearchEngine");
    state.counters["queries_per_sec"] =
        benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_Comparison_BothEngines)
    ->Arg(0) // Old engine
    ->Arg(1) // New engine
    ->UseManualTime();

// ============================================================================
// Cleanup
// ============================================================================

// This function will be called after all benchmarks complete
void CleanupFixture() {
    g_fixture.reset();
}

} // namespace

// Register cleanup (will run after BENCHMARK_MAIN)
struct BenchmarkCleanup {
    ~BenchmarkCleanup() { CleanupFixture(); }
};

static BenchmarkCleanup g_cleanup;

BENCHMARK_MAIN();
