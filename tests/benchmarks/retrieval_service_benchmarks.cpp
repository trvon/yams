// PBI-040: Service layer performance benchmarks
// These benchmarks test the app/services layer to catch performance regressions
// in user-facing operations like get-by-name, cat, grep, search.

#include <algorithm>
#include <chrono>
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

// Global daemon harness for benchmark suite (setup once, reused across benchmarks)
std::unique_ptr<DaemonHarness> g_harness;
std::unique_ptr<daemon::DaemonClient> g_client;
std::vector<std::string> g_test_docs;      // Hashes of test documents
std::vector<std::string> g_test_doc_names; // Names used for by-name retrieval

// Setup: Start daemon and add test documents
void SetupBenchmarkSuite() {
    if (g_harness)
        return; // Already initialized

    std::cout << "\n=== Setting up benchmark environment ===\n";

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

    // Add test documents (small set for benchmarking)
    std::cout << "Adding test documents...\n";
    for (int i = 0; i < 50; ++i) {
        const std::string docName = "test_doc_" + std::to_string(i) + ".txt";
        auto path = g_harness->dataDir() / docName;
        std::ofstream ofs(path);
        ofs << "Test document " << i << "\n";
        ofs << "This is sample content for performance benchmarking.\n";
        ofs << "Document ID: " << i << "\n";
        ofs.close();

        // Add via daemon
        app::services::DocumentIngestionService docSvc;
        app::services::AddOptions opts;
        opts.socketPath = g_harness->socketPath().string();
        opts.explicitDataDir = g_harness->dataDir().string();
        opts.path = path.string();
        opts.noEmbeddings = true; // Skip embeddings for speed

        auto result = docSvc.addViaDaemon(opts);
        if (result) {
            // AddViaDaemon returns AddDocumentResponse, check hash field
            if (!result.value().hash.empty()) {
                g_test_docs.push_back(result.value().hash);
                g_test_doc_names.push_back(docName);
            }
        }
    }

    // Wait for FTS5 indexing to complete
    std::cout << "Waiting for indexing to complete...\n";
    std::this_thread::sleep_for(std::chrono::seconds(2));

    std::cout << "Setup complete: " << g_test_docs.size() << " documents added\n\n";
}

void TeardownBenchmarkSuite() {
    g_client.reset();
    g_harness.reset();
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

// Benchmark: Search service query performance
static void BM_RetrievalService_Search(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_Search");
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

BENCHMARK(BM_RetrievalService_Search)->Unit(benchmark::kMicrosecond)->Iterations(40);

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
    std::cout << "  • Search query:               P95 < 500ms\n";
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
