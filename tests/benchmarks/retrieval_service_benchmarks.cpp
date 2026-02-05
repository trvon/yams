// PBI-040: Service layer performance benchmarks
// These benchmarks test the app/services layer to catch performance regressions
// in user-facing operations like get-by-name, cat, grep, search.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#ifdef TRACY_ENABLE
#include <tracy/Tracy.hpp>
#endif

#include "../integration/daemon/test_async_helpers.h"
#include "../integration/daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/retrieval_service.h>
#include <yams/daemon/client/daemon_client.h>

using namespace yams;
using namespace yams::test;

namespace {

struct BenchConfig {
    bool embeddingsEnabled{false};
    std::string tuningProfile; // empty => use default
    bool operator==(const BenchConfig& other) const {
        return embeddingsEnabled == other.embeddingsEnabled && tuningProfile == other.tuningProfile;
    }
};

// Global daemon harness for benchmark suite (setup once, reused across benchmarks)
std::unique_ptr<DaemonHarness> g_harness;
std::unique_ptr<daemon::DaemonClient> g_client;
std::vector<std::string> g_test_docs;      // Hashes of test documents
std::vector<std::string> g_test_doc_names; // Names used for by-name retrieval
BenchConfig g_activeConfig;

std::size_t benchDocCount() {
    if (const char* env = std::getenv("YAMS_BENCH_DOC_COUNT")) {
        try {
            auto val = std::stoul(env);
            if (val > 0) {
                return val;
            }
        } catch (...) {
        }
    }
    return 500; // Default larger dataset for benchmarks
}

bool waitForSearchEngineReady(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (status) {
            auto it = status.value().readinessStates.find("search_engine");
            if (it != status.value().readinessStates.end() && it->second) {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool waitForEmbeddingDrain(std::size_t minDocCount, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    uint64_t lastVectorCount = 0;
    int stableCount = 0;
    constexpr int stableRequired = 10;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (!status) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        const auto& st = status.value();
        uint64_t vectorCount = 0;
        uint64_t embedQueued = 0;
        uint64_t embedInFlight = 0;
        uint64_t postQueued = 0;
        uint64_t postInFlight = 0;

        if (auto it = st.requestCounts.find("vector_count"); it != st.requestCounts.end()) {
            vectorCount = it->second;
        }
        if (auto it = st.requestCounts.find("embed_svc_queued"); it != st.requestCounts.end()) {
            embedQueued = it->second;
        }
        if (auto it = st.requestCounts.find("embed_in_flight"); it != st.requestCounts.end()) {
            embedInFlight = it->second;
        }
        if (auto it = st.requestCounts.find("post_ingest_queued"); it != st.requestCounts.end()) {
            postQueued = it->second;
        }
        if (auto it = st.requestCounts.find("post_ingest_inflight"); it != st.requestCounts.end()) {
            postInFlight = it->second;
        }

        const bool embedDrained = (embedQueued == 0 && embedInFlight == 0);
        const bool postDrained = (postQueued == 0 && postInFlight == 0);
        if (vectorCount != lastVectorCount || !embedDrained || !postDrained) {
            stableCount = 0;
            lastVectorCount = vectorCount;
        } else {
            ++stableCount;
        }

        if (st.vectorDbReady && embedDrained && postDrained && stableCount >= stableRequired) {
            return vectorCount > 0 || minDocCount == 0;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

struct DrainMetrics {
    uint64_t maxEmbedQueued{0};
    uint64_t maxEmbedInflight{0};
    uint64_t maxPostQueued{0};
    uint64_t maxPostInflight{0};
};

bool waitForEmbeddingDrainWithMetrics(std::size_t minDocCount, std::chrono::milliseconds timeout,
                                      DrainMetrics& metrics) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    uint64_t lastVectorCount = 0;
    int stableCount = 0;
    constexpr int stableRequired = 10;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (!status) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        const auto& st = status.value();
        uint64_t vectorCount = 0;
        uint64_t embedQueued = 0;
        uint64_t embedInFlight = 0;
        uint64_t postQueued = 0;
        uint64_t postInFlight = 0;

        if (auto it = st.requestCounts.find("vector_count"); it != st.requestCounts.end()) {
            vectorCount = it->second;
        }
        if (auto it = st.requestCounts.find("embed_svc_queued"); it != st.requestCounts.end()) {
            embedQueued = it->second;
        }
        if (auto it = st.requestCounts.find("embed_in_flight"); it != st.requestCounts.end()) {
            embedInFlight = it->second;
        }
        if (auto it = st.requestCounts.find("post_ingest_queued"); it != st.requestCounts.end()) {
            postQueued = it->second;
        }
        if (auto it = st.requestCounts.find("post_ingest_inflight"); it != st.requestCounts.end()) {
            postInFlight = it->second;
        }

        metrics.maxEmbedQueued = std::max(metrics.maxEmbedQueued, embedQueued);
        metrics.maxEmbedInflight = std::max(metrics.maxEmbedInflight, embedInFlight);
        metrics.maxPostQueued = std::max(metrics.maxPostQueued, postQueued);
        metrics.maxPostInflight = std::max(metrics.maxPostInflight, postInFlight);

        const bool embedDrained = (embedQueued == 0 && embedInFlight == 0);
        const bool postDrained = (postQueued == 0 && postInFlight == 0);
        if (vectorCount != lastVectorCount || !embedDrained || !postDrained) {
            stableCount = 0;
            lastVectorCount = vectorCount;
        } else {
            ++stableCount;
        }

        if (st.vectorDbReady && embedDrained && postDrained && stableCount >= stableRequired) {
            return vectorCount > 0 || minDocCount == 0;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

// Setup: Start daemon and add test documents
void SetupBenchmarkSuite(const BenchConfig& config = {}) {
    if (g_harness && g_activeConfig == config)
        return; // Already initialized with same config

    g_client.reset();
    g_harness.reset();
    g_test_docs.clear();
    g_test_doc_names.clear();
    g_activeConfig = config;

    std::cout << "\n=== Setting up benchmark environment ===\n";

    // Apply tuning profile/env knobs before daemon start
    if (!config.tuningProfile.empty()) {
        ::setenv("YAMS_TUNING_PROFILE", config.tuningProfile.c_str(), 1);
        std::cout << "Using tuning profile: " << config.tuningProfile << "\n";
    } else {
        ::unsetenv("YAMS_TUNING_PROFILE");
    }
    ::setenv("YAMS_BENCH_ENABLE_EMBEDDINGS", config.embeddingsEnabled ? "1" : "0", 1);

    // Start daemon
    g_harness = std::make_unique<DaemonHarness>();
    if (!g_harness->start(std::chrono::seconds(5))) {
        std::cerr << "ERROR: Failed to start daemon\n";
        std::exit(1);
    }

    // Create client
    daemon::ClientConfig cc;
    cc.socketPath = g_harness->socketPath();
    cc.autoStart = false;
    g_client = std::make_unique<daemon::DaemonClient>(cc);

    const bool embeddingsEnabled = []() {
        if (const char* env = std::getenv("YAMS_BENCH_ENABLE_EMBEDDINGS")) {
            return std::string(env) == "1";
        }
        return false;
    }();

    // Add test documents (small set for benchmarking)
    const auto docCount = static_cast<int>(benchDocCount());
    std::cout << "Adding test documents (" << docCount << ")...\n";
    for (int i = 0; i < docCount; ++i) {
        const std::string docName = "test_doc_" + std::to_string(i) + ".txt";
        auto path = g_harness->dataDir() / docName;
        std::ofstream ofs(path);
        ofs << "Test document " << i << "\n";
        ofs << "This is sample content for performance benchmarking.\n";
        ofs << "Document ID: " << i << "\n";
        // Add some variability to increase corpus size
        ofs << "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n";
        ofs << "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.\n";
        ofs << "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.\n";
        ofs.close();

        // Add via daemon
        app::services::DocumentIngestionService docSvc;
        app::services::AddOptions opts;
        opts.socketPath = g_harness->socketPath().string();
        opts.explicitDataDir = g_harness->dataDir().string();
        opts.path = path.string();
        opts.noEmbeddings = !embeddingsEnabled;

        auto result = docSvc.addViaDaemon(opts);
        if (result) {
            // AddViaDaemon returns AddDocumentResponse, check hash field
            if (!result.value().hash.empty()) {
                g_test_docs.push_back(result.value().hash);
                g_test_doc_names.push_back(docName);
            }
        }
    }

    std::cout << "Waiting for search engine readiness...\n";
    if (!waitForSearchEngineReady(std::chrono::seconds(60))) {
        std::cerr << "WARNING: Search engine not ready after 60s.\n";
    }

    if (embeddingsEnabled) {
        std::cout << "Waiting for embedding queue to drain...\n";
        auto timeout = std::chrono::milliseconds(600000);
        if (const char* env = std::getenv("YAMS_BENCH_EMBED_WAIT_MS")) {
            timeout = std::chrono::milliseconds(
                static_cast<std::chrono::milliseconds::rep>(std::stoll(env)));
        }
        DrainMetrics metrics;
        if (!waitForEmbeddingDrainWithMetrics(g_test_docs.size(), timeout, metrics)) {
            std::cerr << "WARNING: Embedding drain timeout; proceeding anyway.\n";
        }
        std::cout << "Embedding drain observed peaks: embedQueued=" << metrics.maxEmbedQueued
                  << " embedInflight=" << metrics.maxEmbedInflight
                  << " postQueued=" << metrics.maxPostQueued
                  << " postInflight=" << metrics.maxPostInflight << "\n";
    }

    std::cout << "Setup complete: " << g_test_docs.size() << " documents added\n\n";
}

void TeardownBenchmarkSuite() {
    g_client.reset();
    g_harness.reset();
    g_test_docs.clear();
    g_test_doc_names.clear();
    g_activeConfig = {};
    ::unsetenv("YAMS_TUNING_PROFILE");
    ::unsetenv("YAMS_BENCH_ENABLE_EMBEDDINGS");
}

} // anonymous namespace

// Benchmark: Get by name with ready FTS5 index (nominal case)
static void BM_RetrievalService_GetByName_FTS5Ready(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_GetByName_FTS5Ready");
#endif

    if (g_test_docs.empty()) {
        state.SkipWithError("No test documents available");
        return;
    }

    size_t success_count = 0;
    std::vector<int64_t> latencies_us;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

#ifdef TRACY_ENABLE
        {
            ZoneScopedN("getByHash");
#endif
            // Use hash-based retrieval (fastest path)
            daemon::GetRequest req;
            req.hash = g_test_docs[success_count % g_test_docs.size()];

            auto result = cli::run_sync(g_client->get(req), std::chrono::milliseconds(2000));

            if (result) {
                success_count++;
            }
#ifdef TRACY_ENABLE
        }
#endif

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    // Calculate statistics
    std::sort(latencies_us.begin(), latencies_us.end());
    auto p50 = latencies_us[latencies_us.size() / 2];
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["success_count"] = static_cast<double>(success_count);
    state.counters["p50_us"] = static_cast<double>(p50);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    // Check performance target: < 500ms (500,000 us)
    if (p95 > 500000) {
        std::cerr << "⚠️  WARNING: P95 latency " << (p95 / 1000.0) << "ms exceeds 500ms target\n";
    }
}

// Benchmark: Cat by name (CLI-equivalent retrieval path)
static void BM_RetrievalService_CatByName(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_CatByName");
#endif

    if (g_test_doc_names.empty()) {
        state.SkipWithError("No document names available for cat benchmark");
        return;
    }

    app::services::RetrievalService rsvc;
    app::services::RetrievalOptions ropts;
    ropts.socketPath = g_harness->socketPath();
    ropts.explicitDataDir = g_harness->dataDir();
    ropts.requestTimeoutMs = 5000;
    ropts.enableStreaming = false;

    size_t success_count = 0;
    std::vector<int64_t> latencies_us;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

#ifdef TRACY_ENABLE
        {
            ZoneScopedN("catByName");
#endif
            app::services::GetOptions getOpts;
            getOpts.name = g_test_doc_names[success_count % g_test_doc_names.size()];
            getOpts.byName = true;
            getOpts.raw = true;

            auto result = rsvc.get(getOpts, ropts);

            if (result && result.value().hasContent) {
                success_count++;
            }
#ifdef TRACY_ENABLE
        }
#endif

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    if (latencies_us.empty()) {
        state.SkipWithError("No samples collected for cat benchmark");
        return;
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p50 = latencies_us[latencies_us.size() / 2];
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["success_count"] = static_cast<double>(success_count);
    state.counters["p50_us"] = static_cast<double>(p50);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    if (p95 > 500000) {
        std::cerr << "⚠️  WARNING: Cat P95 latency " << (p95 / 1000.0)
                  << "ms exceeds 500ms target\n";
    }
}

// Benchmark: Search service query performance (keyword only)
static void BM_RetrievalService_Search_Keyword(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_Search_Keyword");
#endif

    std::vector<int64_t> latencies_us;
    size_t total_results = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

#ifdef TRACY_ENABLE
        {
            ZoneScopedN("searchQuery");
#endif
            daemon::SearchRequest req;
            req.query = "document";
            req.limit = 20;
            req.pathsOnly = false;
            req.searchType = "keyword";
            req.fuzzy = false;

            auto result = cli::run_sync(g_client->search(req), std::chrono::milliseconds(2000));

            if (result) {
                total_results += result.value().results.size();
            }
#ifdef TRACY_ENABLE
        }
#endif

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    if (latencies_us.empty()) {
        state.SkipWithError("No samples collected for search benchmark");
        return;
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["total_results"] = static_cast<double>(total_results);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    if (p95 > 500000) {
        std::cerr << "⚠️  WARNING: Search P95 latency " << (p95 / 1000.0)
                  << "ms exceeds 500ms target\n";
    }
}

// Benchmark: Exercise post-ingest queue under configured concurrency limits
// This benchmark ingests a batch of documents while tracking peak queue/inflight
// counts reported by the daemon. It surfaces whether tuning caps throttle
// progress (e.g., when profiles change or caps are too low).
static void BM_PostIngest_Throughput(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_PostIngest_Throughput");
#endif

    // Configure harness per-iteration (profile & embeddings)
    BenchConfig cfg;
    cfg.embeddingsEnabled = state.range(1) != 0;
    switch (state.range(2)) {
    case 1: cfg.tuningProfile = "Efficient"; break;
    case 2: cfg.tuningProfile = "Balanced"; break;
    case 3: cfg.tuningProfile = "Aggressive"; break;
    default: cfg.tuningProfile.clear(); break; // default profile
    }
    SetupBenchmarkSuite(cfg);

    // Small batch per iteration to keep runtime reasonable
    const int batchSize = static_cast<int>(state.range(0));

    for (auto _ : state) {
        state.PauseTiming();
        // Create fresh files for this iteration
        std::vector<std::filesystem::path> paths;
        paths.reserve(batchSize);
        for (int i = 0; i < batchSize; ++i) {
            const std::string docName = "bench_pi_" + std::to_string(i) + "_" +
                std::to_string(state.iterations()) + ".txt";
            auto path = g_harness->dataDir() / docName;
            std::ofstream ofs(path);
            ofs << "Post-ingest bench doc " << i << " iteration " << state.iterations() << "\n";
            ofs << std::string(1024, 'x') << "\n";
            ofs.close();
            paths.push_back(path);
        }

        app::services::DocumentIngestionService docSvc;
        app::services::AddOptions opts;
        opts.socketPath = g_harness->socketPath().string();
        opts.explicitDataDir = g_harness->dataDir().string();
        opts.noEmbeddings = true; // Focus on post-ingest stages

        DrainMetrics metrics;
        state.ResumeTiming();
        // Fire the batch
        for (auto& path : paths) {
            opts.path = path.string();
            auto result = docSvc.addViaDaemon(opts);
            benchmark::DoNotOptimize(result);
        }

        // Wait for post-ingest queue to drain to capture peak queue sizes
        state.PauseTiming();
        auto timeout = std::chrono::milliseconds(120000);
        waitForEmbeddingDrainWithMetrics(0, timeout, metrics);

        state.counters["max_embed_queued"] = static_cast<double>(metrics.maxEmbedQueued);
        state.counters["max_embed_inflight"] = static_cast<double>(metrics.maxEmbedInflight);
        state.counters["max_post_queued"] = static_cast<double>(metrics.maxPostQueued);
        state.counters["max_post_inflight"] = static_cast<double>(metrics.maxPostInflight);
    }
}
// Args: batchSize, embeddings(0/1), profile(0=default,1=Efficient,2=Balanced,3=Aggressive)
BENCHMARK(BM_PostIngest_Throughput)
    ->Args({10, 0, 0})
    ->Args({10, 0, 1})
    ->Args({10, 0, 2})
    ->Args({10, 0, 3})
    ->Args({25, 0, 0})
    ->Args({25, 0, 2})
    ->Args({25, 0, 3})
    ->Args({50, 0, 0})
    ->Args({50, 0, 2})
    ->Args({50, 1, 2})
    ->Args({25, 1, 2})
    ->Args({25, 1, 3});

// Benchmark: Search with fuzzy enabled (tests SymSpell expansion + FTS5)
static void BM_RetrievalService_Search_Fuzzy(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_Search_Fuzzy");
#endif

    std::vector<int64_t> latencies_us;
    size_t total_results = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

        daemon::SearchRequest req;
        req.query = "documnt"; // Intentional typo to test fuzzy
        req.limit = 20;
        req.pathsOnly = false;
        req.fuzzy = true;
        req.similarity = 0.6;

        auto result = cli::run_sync(g_client->search(req), std::chrono::milliseconds(2000));

        if (result) {
            total_results += result.value().results.size();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    if (latencies_us.empty()) {
        state.SkipWithError("No samples collected for fuzzy search benchmark");
        return;
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p50 = latencies_us[latencies_us.size() / 2];
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["total_results"] = static_cast<double>(total_results);
    state.counters["p50_us"] = static_cast<double>(p50);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    if (p95 > 500000) {
        std::cerr << "⚠️  WARNING: Fuzzy Search P95 latency " << (p95 / 1000.0)
                  << "ms exceeds 500ms target\n";
    }
}

// Benchmark: Hybrid search (keyword + semantic + vector)
static void BM_RetrievalService_Search_Hybrid(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_Search_Hybrid");
#endif

    std::vector<int64_t> latencies_us;
    size_t total_results = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

        daemon::SearchRequest req;
        req.query = "test document performance";
        req.limit = 20;
        req.pathsOnly = false;
        req.searchType = "hybrid";
        req.fuzzy = false;

        auto result = cli::run_sync(g_client->search(req), std::chrono::milliseconds(3000));

        if (result) {
            total_results += result.value().results.size();
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    if (latencies_us.empty()) {
        state.SkipWithError("No samples collected for hybrid search benchmark");
        return;
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p50 = latencies_us[latencies_us.size() / 2];
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["total_results"] = static_cast<double>(total_results);
    state.counters["p50_us"] = static_cast<double>(p50);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    if (p95 > 1000000) {
        std::cerr << "⚠️  WARNING: Hybrid Search P95 latency " << (p95 / 1000.0)
                  << "ms exceeds 1000ms target\n";
    }
}

// Benchmark: Get by name when document doesn't exist (tests fast-path error handling)
static void BM_RetrievalService_GetByName_NotFound(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_GetByName_NotFound");
#endif

    std::vector<int64_t> latencies_us;
    size_t fast_error_count = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

#ifdef TRACY_ENABLE
        {
            ZoneScopedN("getByHash_nonexistent");
#endif
            // Try to get nonexistent document
            daemon::GetRequest req;
            req.hash = "0000000000000000"; // Nonexistent hash

            auto result = cli::run_sync(g_client->get(req), std::chrono::milliseconds(2000));

            // Should fail quickly with NotFound
            if (!result) {
                fast_error_count++;
            }
#ifdef TRACY_ENABLE
        }
#endif

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["fast_errors"] = static_cast<double>(fast_error_count);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    // Check target: fast error < 1000ms (1,000,000 us)
    // CRITICAL: This is the 15-minute hang bug we fixed in 040-1
    if (p95 > 1000000) {
        std::cerr << "🔴 CRITICAL: P95 error latency " << (p95 / 1000.0)
                  << "ms exceeds 1000ms target (possible hang!)\n";
    }
}

// Benchmark: List documents (tests metadata query performance)
static void BM_RetrievalService_List(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_List");
#endif

    std::vector<int64_t> latencies_us;
    size_t total_docs = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

#ifdef TRACY_ENABLE
        {
            ZoneScopedN("listDocuments");
#endif
            daemon::ListRequest req;
            req.limit = 20;

            auto result = cli::run_sync(g_client->list(req), std::chrono::milliseconds(2000));

            if (result) {
                total_docs += result.value().items.size();
            }
#ifdef TRACY_ENABLE
        }
#endif

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["total_docs"] = static_cast<double>(total_docs);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);

    if (max_us > 1000000) {
        state.SkipWithError("List latency exceeded 1s — possible hang");
        return;
    }
}

// Benchmark: Grep search (tests FTS5 query + content retrieval)
static void BM_GrepService_Search(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_Grep");
#endif

    std::vector<int64_t> latencies_us;
    size_t total_matches = 0;
    size_t timeout_count = 0;
    size_t error_count = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

#ifdef TRACY_ENABLE
        {
            ZoneScopedN("grepSearch");
#endif
            daemon::GrepRequest req;
            req.pattern = "document";
            req.pathsOnly = false;

            auto result = cli::run_sync(g_client->grep(req), std::chrono::milliseconds(2000));

            if (!result) {
                const auto& err = result.error();
                if (err.code == ErrorCode::Timeout) {
                    ++timeout_count;
                } else {
                    ++error_count;
                }
            } else {
                total_matches += result.value().matches.size();
            }
#ifdef TRACY_ENABLE
        }
#endif

        auto end = std::chrono::high_resolution_clock::now();
        auto latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        latencies_us.push_back(latency_us);
    }

    if (latencies_us.empty()) {
        state.SkipWithError("No samples collected for grep benchmark");
        return;
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();

    state.counters["total_matches"] = static_cast<double>(total_matches);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);
    state.counters["timeout_count"] = static_cast<double>(timeout_count);
    state.counters["error_count"] = static_cast<double>(error_count);

    if (max_us > 1000000) {
        std::cerr << "🔴 CRITICAL: Grep latency " << (max_us / 1000.0)
                  << "ms exceeded 1s; potential hang detected\n";
        state.SkipWithError("Grep latency exceeded 1s");
        return;
    }

    if (timeout_count > 0) {
        std::cerr << "🔴 CRITICAL: Grep benchmark experienced " << timeout_count
                  << " timeout(s); potential hang detected\n";
        state.SkipWithError("Grep timeout detected");
        return;
    }

    if (error_count > 0) {
        std::cerr << "⚠️  WARNING: Grep benchmark observed " << error_count
                  << " non-timeout error(s).\n";
    }

    // Target: < 500ms for grep with sync indexing (when 040-4 is complete)
    if (p95 > 500000) {
        std::cerr << "⚠️  INFO: Grep P95 " << (p95 / 1000.0)
                  << "ms (target < 500ms with sync indexing in 040-4)\n";
    }
}

// Register benchmarks with appropriate iteration counts
BENCHMARK(BM_RetrievalService_GetByName_FTS5Ready)->Unit(benchmark::kMicrosecond)->Iterations(100);

BENCHMARK(BM_RetrievalService_CatByName)->Unit(benchmark::kMicrosecond)->Iterations(60);

BENCHMARK(BM_RetrievalService_GetByName_NotFound)->Unit(benchmark::kMicrosecond)->Iterations(10);

BENCHMARK(BM_RetrievalService_List)->Unit(benchmark::kMicrosecond)->Iterations(50);

BENCHMARK(BM_GrepService_Search)->Unit(benchmark::kMicrosecond)->Iterations(30);

BENCHMARK(BM_RetrievalService_Search_Keyword)->Unit(benchmark::kMicrosecond)->Iterations(40);

BENCHMARK(BM_RetrievalService_Search_Fuzzy)->Unit(benchmark::kMicrosecond)->Iterations(40);

BENCHMARK(BM_RetrievalService_Search_Hybrid)->Unit(benchmark::kMicrosecond)->Iterations(20);

// Custom main to setup/teardown daemon
int main(int argc, char** argv) {
    std::cout << "\n";
    std::cout << "╔═══════════════════════════════════════════════════════════╗\n";
    std::cout << "║  YAMS Service Layer Performance Benchmarks (PBI-040)     ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════╝\n";
    std::cout << "\nPerformance Targets:\n";
    std::cout << "  • GetByName (FTS5 ready):     P95 < 500ms\n";
    std::cout << "  • Cat (by name):              P95 < 500ms\n";
    std::cout << "  • GetByName (not found):      P95 < 1000ms (fast error)\n";
    std::cout << "  • Search (keyword):           P95 < 500ms\n";
    std::cout << "  • Search (fuzzy/SymSpell):    P95 < 500ms\n";
    std::cout << "  • Search (hybrid):            P95 < 1000ms\n";
    std::cout << "  • List documents:             Responsive metadata queries\n";
    std::cout << "  • Grep search:                P95 < 500ms (with sync indexing)\n";
    std::cout << "\n";

    // Setup daemon and test data
    SetupBenchmarkSuite();

    // Run benchmarks
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        TeardownBenchmarkSuite();
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();

    // Cleanup
    TeardownBenchmarkSuite();

    std::cout << "\n✅ Benchmark suite completed\n";
    std::cout << "💡 Review P95 latencies above for regressions\n";
    std::cout << "🔴 Critical warnings indicate potential hangs or severe slowdowns\n\n";

    return 0;
}
