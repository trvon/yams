// Single-client pipelining benchmark
//
// Measures how single-client ingest throughput scales with the number of
// in-flight add requests (window W) on one client process. Fills the gap
// between ingestion_throughput_bench (W=1 by construction) and
// multi_client_ingestion_bench (N OS-level clients): the 2026-06-10 scaling
// curve showed a solo client at ~320 docs/s while per-client throughput under
// 16-way concurrency reached 800+ docs/s, i.e. the solo path is round-trip
// bound with idle daemon capacity. This bench quantifies that headroom and
// tells us whether client-side pipelining/batching recovers it.
//
// Method: W coroutines are co_spawned on the GlobalIOContext executor (the
// same executor run_sync uses), each looping co_await streamingAddDocument
// until the shared document budget is exhausted. One process, one io thread —
// concurrency comes purely from interleaved in-flight requests.
//
// Environment variables:
//   YAMS_BENCH_DOCS            - Total documents per window cell (default: 400)
//   YAMS_BENCH_DOC_SIZE_BYTES  - Approximate document size (default: 2048)
//   YAMS_BENCH_WINDOWS         - Comma-separated window sizes (default: 1,2,4,8,16,32)
//   YAMS_BENCH_OUTPUT          - JSONL output path (default:
//                                bench_results/single_client_pipeline.jsonl)

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "../integration/daemon/test_daemon_harness.h"

#include <yams/app/services/document_ingestion_service.h>
#include <yams/cli/cli_sync.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace std::chrono_literals;
using yams::test::DaemonHarness;

namespace {

int readIntEnv(const char* name, int fallback, int minVal, int maxVal) {
    const char* val = std::getenv(name);
    if (!val || !*val) {
        return fallback;
    }
    char* end = nullptr;
    long parsed = std::strtol(val, &end, 10);
    if (end == nullptr || end == val || *end != '\0') {
        return fallback;
    }
    return static_cast<int>(std::clamp<long>(parsed, minVal, maxVal));
}

std::vector<int> readWindowsEnv() {
    std::vector<int> windows;
    const char* val = std::getenv("YAMS_BENCH_WINDOWS");
    std::string spec = (val && *val) ? val : "1,2,4,8,16,32";
    std::stringstream ss(spec);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        try {
            int w = std::stoi(tok);
            if (w >= 1 && w <= 256) {
                windows.push_back(w);
            }
        } catch (...) {
        }
    }
    if (windows.empty()) {
        windows = {1, 2, 4, 8, 16, 32};
    }
    return windows;
}

std::string generateContent(int window, int docIndex, std::size_t targetBytes) {
    std::mt19937 rng(static_cast<uint32_t>(window * 100003 + docIndex));
    std::uniform_int_distribution<int> word(0, 25);
    std::string out;
    out.reserve(targetBytes + 16);
    out += "pipeline bench w" + std::to_string(window) + " d" + std::to_string(docIndex) + "\n";
    while (out.size() < targetBytes) {
        for (int i = 0; i < 8 && out.size() < targetBytes; ++i) {
            out += static_cast<char>('a' + word(rng));
        }
        out += ' ';
    }
    return out;
}

struct WindowResult {
    int window{0};
    int docsRequested{0};
    int docsIngested{0};
    int failures{0};
    double elapsedSeconds{0.0};
    double docsPerSec{0.0};
    std::vector<double> latenciesUs;
};

double percentileUs(std::vector<double> values, double q) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const auto idx = std::min(values.size() - 1,
                              static_cast<std::size_t>(q * static_cast<double>(values.size() - 1)));
    return values[idx];
}

WindowResult runWindow(const std::string& socketPath, int window, int totalDocs,
                       std::size_t docBytes) {
    WindowResult result;
    result.window = window;
    result.docsRequested = totalDocs;

    std::atomic<int> nextDoc{0};
    std::atomic<int> ok{0};
    std::atomic<int> failed{0};

    struct WorkerLatencies {
        std::vector<double> values;
    };
    std::vector<WorkerLatencies> perWorker(static_cast<std::size_t>(window));

    auto executor = yams::daemon::GlobalIOContext::global_executor();

    const auto start = std::chrono::steady_clock::now();
    std::vector<std::future<void>> futures;
    futures.reserve(static_cast<std::size_t>(window));

    for (int w = 0; w < window; ++w) {
        futures.push_back(boost::asio::co_spawn(
            executor,
            [&, w]() -> boost::asio::awaitable<void> {
                yams::daemon::ClientConfig ccfg;
                ccfg.socketPath = socketPath;
                ccfg.requestTimeout = 30s;
                yams::daemon::DaemonClient client(ccfg);

                while (true) {
                    const int doc = nextDoc.fetch_add(1, std::memory_order_relaxed);
                    if (doc >= totalDocs) {
                        break;
                    }
                    yams::daemon::AddDocumentRequest req;
                    req.name = "pipeline_w" + std::to_string(window) + "_d" +
                               std::to_string(doc) + ".md";
                    req.content = generateContent(window, doc, docBytes);
                    req.tags = {"bench", "pipeline"};

                    const auto t0 = std::chrono::steady_clock::now();
                    auto res = co_await client.streamingAddDocument(req);
                    const auto us = std::chrono::duration<double, std::micro>(
                                        std::chrono::steady_clock::now() - t0)
                                        .count();
                    perWorker[static_cast<std::size_t>(w)].values.push_back(us);
                    if (res) {
                        ok.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        failed.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                co_return;
            },
            boost::asio::use_future));
    }

    for (auto& f : futures) {
        f.wait();
    }
    const auto end = std::chrono::steady_clock::now();

    result.elapsedSeconds = std::chrono::duration<double>(end - start).count();
    result.docsIngested = ok.load();
    result.failures = failed.load();
    result.docsPerSec =
        result.elapsedSeconds > 0.0
            ? static_cast<double>(result.docsIngested) / result.elapsedSeconds
            : 0.0;
    for (auto& worker : perWorker) {
        result.latenciesUs.insert(result.latenciesUs.end(), worker.values.begin(),
                                  worker.values.end());
    }
    return result;
}

void appendJsonLine(const fs::path& outputPath, const json& record) {
    if (!outputPath.parent_path().empty()) {
        fs::create_directories(outputPath.parent_path());
    }
    std::ofstream out(outputPath, std::ios::app);
    out << record.dump() << '\n';
}

std::string isoTimestamp() {
    const auto now = std::chrono::system_clock::now();
    const auto tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&tt, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

} // namespace

TEST_CASE("Single-client pipelining: addBatchAsync window scaling",
          "[!benchmark][single-client][pipeline][batch-service]") {
    const int totalDocs = readIntEnv("YAMS_BENCH_DOCS", 400, 10, 100000);
    const int docBytes = readIntEnv("YAMS_BENCH_DOC_SIZE_BYTES", 2048, 64, 1 << 20);
    const auto windows = readWindowsEnv();
    fs::path outputPath = "bench_results/single_client_pipeline.jsonl";
    if (const char* out = std::getenv("YAMS_BENCH_OUTPUT"); out && *out) {
        outputPath = out;
    }

    DaemonHarness::Options opts;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = true;
    opts.autoLoadPlugins = false;
    opts.enableAutoRepair = true;
    opts.isolateState = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(30s));

    yams::app::services::DocumentIngestionService ing;
    json cells = json::array();

    for (int window : windows) {
        std::vector<yams::app::services::AddOptions> batch;
        batch.reserve(static_cast<std::size_t>(totalDocs));
        for (int doc = 0; doc < totalDocs; ++doc) {
            yams::app::services::AddOptions aopts;
            aopts.socketPath = harness.socketPath();
            aopts.content = generateContent(window + 1000, doc, static_cast<std::size_t>(docBytes));
            aopts.name =
                "svc_batch_w" + std::to_string(window) + "_d" + std::to_string(doc) + ".md";
            aopts.tags = {"bench", "batch-service"};
            aopts.noEmbeddings = true;
            batch.push_back(std::move(aopts));
        }

        const auto start = std::chrono::steady_clock::now();
        auto fut = boost::asio::co_spawn(yams::daemon::GlobalIOContext::global_executor(),
                                         ing.addBatchAsync(batch, window),
                                         boost::asio::use_future);
        const auto batchResult = fut.get();
        const auto elapsed =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        const double docsPerSec =
            elapsed > 0.0 ? static_cast<double>(batchResult.succeeded) / elapsed : 0.0;

        std::printf("addBatchAsync window=%-3d docs/s=%8.1f  succeeded=%zu failed=%zu\n", window,
                    docsPerSec, batchResult.succeeded, batchResult.failed);

        cells.push_back(json{{"window", window},
                             {"docs_requested", totalDocs},
                             {"succeeded", batchResult.succeeded},
                             {"failed", batchResult.failed},
                             {"elapsed_seconds", elapsed},
                             {"docs_per_sec", docsPerSec}});

        CHECK(batchResult.failed == 0);
    }

    appendJsonLine(outputPath, json{{"test", "single_client_addbatch"},
                                    {"timestamp", isoTimestamp()},
                                    {"schema_version", 1},
                                    {"total_docs_per_cell", totalDocs},
                                    {"doc_size_bytes", docBytes},
                                    {"cells", std::move(cells)}});
    harness.stop();
}

TEST_CASE("Single-client pipelining: in-flight window scaling",
          "[!benchmark][single-client][pipeline]") {
    const int totalDocs = readIntEnv("YAMS_BENCH_DOCS", 400, 10, 100000);
    const int docBytes = readIntEnv("YAMS_BENCH_DOC_SIZE_BYTES", 2048, 64, 1 << 20);
    const auto windows = readWindowsEnv();
    fs::path outputPath = "bench_results/single_client_pipeline.jsonl";
    if (const char* out = std::getenv("YAMS_BENCH_OUTPUT"); out && *out) {
        outputPath = out;
    }

    DaemonHarness::Options opts;
    opts.enableModelProvider = false;
    opts.useMockModelProvider = true;
    opts.autoLoadPlugins = false;
    opts.enableAutoRepair = true;
    opts.isolateState = true;
    DaemonHarness harness(opts);
    REQUIRE(harness.start(30s));

    spdlog::info("=== Single-Client Pipeline Benchmark ===");
    spdlog::info("  docs/cell: {}  doc bytes: {}  windows: {}", totalDocs, docBytes,
                 windows.size());

    json cells = json::array();
    double w1DocsPerSec = 0.0;

    for (int window : windows) {
        auto result = runWindow(harness.socketPath().string(), window, totalDocs,
                                static_cast<std::size_t>(docBytes));
        if (window == 1) {
            w1DocsPerSec = result.docsPerSec;
        }
        const double p50 = percentileUs(result.latenciesUs, 0.50);
        const double p95 = percentileUs(result.latenciesUs, 0.95);

        std::printf("window=%-3d docs/s=%8.1f  add p50=%7.0f us  p95=%7.0f us  failures=%d\n",
                    window, result.docsPerSec, p50, p95, result.failures);

        cells.push_back(json{{"window", window},
                             {"docs_requested", result.docsRequested},
                             {"docs_ingested", result.docsIngested},
                             {"failures", result.failures},
                             {"elapsed_seconds", result.elapsedSeconds},
                             {"docs_per_sec", result.docsPerSec},
                             {"latency_p50_us", p50},
                             {"latency_p95_us", p95},
                             {"latency_count", result.latenciesUs.size()}});

        CHECK(result.failures == 0);
        CHECK(result.docsIngested == result.docsRequested);
    }

    json record{{"test", "single_client_pipeline"},
                {"timestamp", isoTimestamp()},
                {"schema_version", 1},
                {"total_docs_per_cell", totalDocs},
                {"doc_size_bytes", docBytes},
                {"w1_docs_per_sec", w1DocsPerSec},
                {"cells", std::move(cells)}};
    appendJsonLine(outputPath, record);

    harness.stop();
}
