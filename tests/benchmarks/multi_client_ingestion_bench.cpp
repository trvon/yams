// Multi-client ingestion and concurrent access benchmark
// Benchmarks the daemon's ability to handle:
// 1. Concurrent ingestion from multiple clients
// 2. Mixed read/write workloads (ingest + search + list)
// 3. Connection fairness and latency under contention
// 4. Per-client throughput scaling vs total throughput
// 5. Backpressure and queue depth behavior under multi-client load
//
// Designed for recursive tuning: run repeatedly, adjusting TuneAdvisor knobs,
// connection pool sizes, and I/O thread counts to optimize multi-client performance.
//
// Environment variables:
//   YAMS_BENCH_NUM_CLIENTS        - Number of concurrent clients (default: 4)
//   YAMS_BENCH_DOCS_PER_CLIENT    - Documents each client ingests (default: 100)
//   YAMS_BENCH_DOC_SIZE_BYTES     - Average document size in bytes (default: 2048)
//   YAMS_BENCH_SEARCH_RATIO       - Fraction of ops that are search (default: 0.2)
//   YAMS_BENCH_WARMUP_DOCS        - Docs to pre-ingest for search queries (default: 50)
//   YAMS_BENCH_OUTPUT              - JSONL output path (default: bench_results/multi_client.jsonl)
//   YAMS_BENCH_DRAIN_TIMEOUT_S    - Seconds to wait for queue drain (default: 120)
//   YAMS_BENCH_SCALING_MAX_CLIENTS - Max clients in scaling sweep (default: 32)
//   YAMS_TUNING_PROFILE           - TuneAdvisor profile (Quiet/Balanced/Performance)

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../common/test_helpers_catch2.h"
#include "../integration/daemon/test_async_helpers.h"
#include "../integration/daemon/test_daemon_harness.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/metric_keys.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace std::chrono_literals;
using namespace yams::daemon;
using namespace yams::test;

// ─────────────────────────────────────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────────────────────────────────────

namespace {

struct BenchConfig {
    int numClients{4};
    int docsPerClient{100};
    size_t docSizeBytes{2048};
    double searchRatio{0.2};
    int warmupDocs{50};
    fs::path outputPath{"bench_results/multi_client.jsonl"};
    int drainTimeoutSecs{120};
    int scalingMaxClients{32};
    std::string tuningProfile;

    static BenchConfig fromEnv() {
        BenchConfig cfg;
        if (auto* v = std::getenv("YAMS_BENCH_NUM_CLIENTS"))
            cfg.numClients = std::max(1, std::atoi(v));
        if (auto* v = std::getenv("YAMS_BENCH_DOCS_PER_CLIENT"))
            cfg.docsPerClient = std::max(1, std::atoi(v));
        if (auto* v = std::getenv("YAMS_BENCH_DOC_SIZE_BYTES"))
            cfg.docSizeBytes = std::max<size_t>(64, std::stoull(v));
        if (auto* v = std::getenv("YAMS_BENCH_SEARCH_RATIO"))
            cfg.searchRatio = std::clamp(std::stod(v), 0.0, 1.0);
        if (auto* v = std::getenv("YAMS_BENCH_WARMUP_DOCS"))
            cfg.warmupDocs = std::max(0, std::atoi(v));
        if (auto* v = std::getenv("YAMS_BENCH_OUTPUT"))
            cfg.outputPath = v;
        if (auto* v = std::getenv("YAMS_BENCH_DRAIN_TIMEOUT_S"))
            cfg.drainTimeoutSecs = std::max(10, std::atoi(v));
        if (auto* v = std::getenv("YAMS_BENCH_SCALING_MAX_CLIENTS"))
            cfg.scalingMaxClients = std::max(1, std::atoi(v));
        cfg.scalingMaxClients = std::max(cfg.scalingMaxClients, cfg.numClients * 2);
        if (auto* v = std::getenv("YAMS_TUNING_PROFILE"))
            cfg.tuningProfile = v;
        return cfg;
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Latency & throughput tracking
// ─────────────────────────────────────────────────────────────────────────────

struct OpLatency {
    std::string opType; // "add", "search", "list", "status"
    int clientId{0};
    int64_t latencyUs{0};
    bool success{false};
    std::string errorMsg;
};

struct ClientResult {
    int clientId{0};
    int docsIngested{0};
    int searchesExecuted{0};
    int listsExecuted{0};
    int statusChecks{0};
    int failures{0};
    double elapsedSeconds{0.0};
    double addThroughputDocsPerSec{0.0};
    std::vector<OpLatency> latencies;
};

struct PercentileStats {
    int64_t min{0};
    int64_t p50{0};
    int64_t p90{0};
    int64_t p95{0};
    int64_t p99{0};
    int64_t max{0};
    double mean{0.0};
    size_t count{0};

    static PercentileStats compute(std::vector<int64_t>& values) {
        PercentileStats stats;
        stats.count = values.size();
        if (values.empty())
            return stats;

        std::sort(values.begin(), values.end());
        stats.min = values.front();
        stats.max = values.back();
        stats.p50 = values[values.size() / 2];
        stats.p90 = values[static_cast<size_t>(values.size() * 0.90)];
        stats.p95 = values[static_cast<size_t>(values.size() * 0.95)];
        stats.p99 = values[static_cast<size_t>(values.size() * 0.99)];
        stats.mean =
            std::accumulate(values.begin(), values.end(), 0.0) / static_cast<double>(values.size());
        return stats;
    }

    json toJson() const {
        return json{{"count", count}, {"min_us", min}, {"p50_us", p50}, {"p90_us", p90},
                    {"p95_us", p95},  {"p99_us", p99}, {"max_us", max}, {"mean_us", mean}};
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Daemon status snapshot helper
// ─────────────────────────────────────────────────────────────────────────────

struct DaemonSnapshot {
    size_t activeConnections{0};
    size_t maxConnections{0};
    uint64_t requestsProcessed{0};
    double memoryUsageMb{0.0};
    double cpuUsagePercent{0.0};
    uint64_t postIngestQueued{0};
    uint64_t postIngestInflight{0};
    int pressureLevel{0};
    uint64_t documentsTotal{0};
    uint64_t searchActive{0};
    uint64_t searchQueued{0};
    uint64_t dbWritePoolWaiting{0};
    uint64_t dbReadPoolWaiting{0};
    uint32_t retryAfterMs{0};

    static DaemonSnapshot capture(DaemonClient& client) {
        DaemonSnapshot snap;
        auto st = yams::cli::run_sync(client.status(), 5s);
        if (!st)
            return snap;

        const auto& s = st.value();
        snap.activeConnections = s.activeConnections;
        snap.maxConnections = s.maxConnections;
        snap.requestsProcessed = s.requestsProcessed;
        snap.memoryUsageMb = s.memoryUsageMb;
        snap.cpuUsagePercent = s.cpuUsagePercent;
        snap.retryAfterMs = s.retryAfterMs;

        auto getCount = [&](std::string_view key) -> uint64_t {
            auto it = s.requestCounts.find(std::string(key));
            return (it != s.requestCounts.end()) ? it->second : 0;
        };

        snap.postIngestQueued = getCount(metrics::kPostIngestQueued);
        snap.postIngestInflight = getCount(metrics::kPostIngestInflight);
        snap.pressureLevel = static_cast<int>(getCount(metrics::kPressureLevel));
        snap.documentsTotal = getCount(metrics::kDocumentsTotal);
        snap.searchActive = getCount(metrics::kSearchActive);
        snap.searchQueued = getCount(metrics::kSearchQueued);
        snap.dbWritePoolWaiting = getCount(metrics::kDbWritePoolWaitingRequests);
        snap.dbReadPoolWaiting = getCount(metrics::kDbReadPoolWaitingRequests);
        return snap;
    }

    json toJson() const {
        return json{{"active_connections", activeConnections},
                    {"max_connections", maxConnections},
                    {"requests_processed", requestsProcessed},
                    {"memory_mb", memoryUsageMb},
                    {"cpu_pct", cpuUsagePercent},
                    {"post_ingest_queued", postIngestQueued},
                    {"post_ingest_inflight", postIngestInflight},
                    {"pressure_level", pressureLevel},
                    {"documents_total", documentsTotal},
                    {"search_active", searchActive},
                    {"search_queued", searchQueued},
                    {"db_write_pool_waiting", dbWritePoolWaiting},
                    {"db_read_pool_waiting", dbReadPoolWaiting},
                    {"retry_after_ms", retryAfterMs}};
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Time-series sampler (runs in background during benchmark)
// ─────────────────────────────────────────────────────────────────────────────

class TimeSeriesSampler {
public:
    struct Sample {
        uint64_t elapsedMs{0};
        DaemonSnapshot snap;
    };

    void start(DaemonClient& client, std::chrono::milliseconds interval = 500ms) {
        stop_ = false;
        startTime_ = std::chrono::steady_clock::now();
        thread_ = std::thread([this, &client, interval]() {
            while (!stop_) {
                Sample sample;
                sample.elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::steady_clock::now() - startTime_)
                                       .count();
                sample.snap = DaemonSnapshot::capture(client);
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    samples_.push_back(sample);
                }
                std::this_thread::sleep_for(interval);
            }
        });
    }

    void stop() {
        stop_ = true;
        if (thread_.joinable())
            thread_.join();
    }

    std::vector<Sample> getSamples() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_;
    }

private:
    std::atomic<bool> stop_{false};
    std::chrono::steady_clock::time_point startTime_;
    std::thread thread_;
    mutable std::mutex mutex_;
    std::vector<Sample> samples_;
};

// ─────────────────────────────────────────────────────────────────────────────
// Document generator
// ─────────────────────────────────────────────────────────────────────────────

std::string generateDocument(int clientId, int docIndex, size_t targetBytes) {
    std::ostringstream oss;
    oss << "# Client " << clientId << " Document " << docIndex << "\n\n";
    oss << "Created at: " << std::chrono::system_clock::now().time_since_epoch().count() << "\n\n";

    // Fill with pseudo-realistic text to reach target size
    static const char* paragraphs[] = {
        "The system architecture follows a layered design pattern with clear separation "
        "of concerns between the daemon core, service layer, and client interfaces.\n\n",
        "Performance tuning parameters are exposed through TuneAdvisor knobs allowing "
        "runtime adjustment of thread pool sizes, queue capacities, and concurrency limits.\n\n",
        "The ingestion pipeline processes documents through extraction, indexing, knowledge "
        "graph construction, symbol analysis, entity recognition, and embedding stages.\n\n",
        "Resource governance monitors memory pressure, CPU utilization, and I/O throughput "
        "to dynamically scale worker thread counts and admission control thresholds.\n\n",
        "The IPC transport uses Unix domain sockets with a binary framing protocol supporting "
        "multiplexed request/response streams with backpressure signaling.\n\n",
    };
    constexpr int numParagraphs = 5;

    int paraIdx = 0;
    while (oss.tellp() < static_cast<std::streamoff>(targetBytes)) {
        oss << paragraphs[paraIdx % numParagraphs];
        paraIdx++;
    }

    auto result = oss.str();
    if (result.size() > targetBytes)
        result.resize(targetBytes);
    return result;
}

// ─────────────────────────────────────────────────────────────────────────────
// ISO timestamp helper
// ─────────────────────────────────────────────────────────────────────────────

std::string isoTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&tt, &tm);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

std::string toLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

bool containsInsensitive(const std::string& haystackLower, std::string_view needle) {
    return haystackLower.find(std::string(needle)) != std::string::npos;
}

std::string classifyFailureMessage(const std::string& errorMsg) {
    if (errorMsg.empty()) {
        return "unknown";
    }

    const std::string lower = toLowerCopy(errorMsg);

    if (containsInsensitive(lower, "timed out") || containsInsensitive(lower, "timeout")) {
        return "timeout";
    }
    if (containsInsensitive(lower, "retry after") || containsInsensitive(lower, "backpressure")) {
        return "backpressure";
    }
    if (containsInsensitive(lower, "connection refused") ||
        containsInsensitive(lower, "no such file or directory")) {
        return "connect_refused";
    }
    if (containsInsensitive(lower, "broken pipe") || containsInsensitive(lower, "eof") ||
        containsInsensitive(lower, "connection reset")) {
        return "connection_drop";
    }
    if (containsInsensitive(lower, "cancel")) {
        return "cancelled";
    }
    if (containsInsensitive(lower, "database is locked") ||
        containsInsensitive(lower, "sql_busy")) {
        return "db_locked";
    }
    if (containsInsensitive(lower, "queue") &&
        (containsInsensitive(lower, "full") || containsInsensitive(lower, "overflow"))) {
        return "queue_full";
    }
    if (containsInsensitive(lower, "parse") || containsInsensitive(lower, "protocol") ||
        containsInsensitive(lower, "invalid response")) {
        return "protocol";
    }

    return "other";
}

// ─────────────────────────────────────────────────────────────────────────────
// Wait for queue drain
// ─────────────────────────────────────────────────────────────────────────────

bool waitForDrain(DaemonClient& client, std::chrono::seconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    int stableCount = 0;
    constexpr int stableRequired = 5;

    while (std::chrono::steady_clock::now() < deadline) {
        auto snap = DaemonSnapshot::capture(client);
        bool drained = (snap.postIngestQueued == 0 && snap.postIngestInflight == 0);
        if (drained) {
            ++stableCount;
            if (stableCount >= stableRequired)
                return true;
        } else {
            stableCount = 0;
        }
        std::this_thread::sleep_for(500ms);
    }
    return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Output helpers
// ─────────────────────────────────────────────────────────────────────────────

void emitJsonl(const fs::path& outputPath, const json& record) {
    fs::create_directories(outputPath.parent_path());
    std::ofstream out(outputPath, std::ios::app);
    if (out) {
        out << record.dump() << '\n';
    }
}

void printPercentiles(const std::string& label, const PercentileStats& stats) {
    std::cout << "  " << label << ": n=" << stats.count << " min=" << stats.min << "us"
              << " p50=" << stats.p50 << "us"
              << " p95=" << stats.p95 << "us"
              << " p99=" << stats.p99 << "us"
              << " max=" << stats.max << "us"
              << " mean=" << std::fixed << std::setprecision(0) << stats.mean << "us" << std::endl;
}

// Create harness options tuned for benchmarking:
// - Disable model provider (we don't need embeddings for ingestion benchmarks)
// - Use mock model provider as fallback
// - No auto-loaded plugins (reduce startup variance)
DaemonHarnessOptions benchHarnessOptions() {
    return DaemonHarnessOptions{
        .enableModelProvider = false,
        .useMockModelProvider = true,
        .autoLoadPlugins = false,
        .enableAutoRepair = true,
    };
}

// Startup timeout: 30s to handle TSan/debug builds where init is ~5-10x slower
constexpr auto kStartTimeout = std::chrono::seconds(30);

bool startHarnessWithRetry(DaemonHarness& harness, std::chrono::milliseconds timeout,
                           int maxAttempts = 3) {
    for (int attempt = 0; attempt < maxAttempts; ++attempt) {
        if (harness.start(timeout)) {
            return true;
        }
        harness.stop();
        std::this_thread::sleep_for(500ms);
    }
    return false;
}

} // namespace

// =============================================================================
// TEST CASES
// =============================================================================

TEST_CASE("Multi-client ingestion: baseline single client",
          "[!benchmark][multi-client][ingestion]") {
    auto cfg = BenchConfig::fromEnv();
    cfg.numClients = 1; // Force single client for baseline

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Create client
    ClientConfig ccfg;
    ccfg.socketPath = harness.socketPath();
    ccfg.autoStart = false;
    ccfg.requestTimeout = 30s;
    DaemonClient client(ccfg);

    // Ingest documents
    std::vector<int64_t> addLatencies;
    auto startTime = std::chrono::steady_clock::now();

    for (int i = 0; i < cfg.docsPerClient; ++i) {
        auto doc = generateDocument(0, i, cfg.docSizeBytes);
        AddDocumentRequest req;
        req.name = "baseline_doc_" + std::to_string(i) + ".md";
        req.content = std::move(doc);
        req.tags = {"bench", "baseline"};

        auto t0 = std::chrono::steady_clock::now();
        auto result = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
        auto t1 = std::chrono::steady_clock::now();
        auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

        if (result) {
            addLatencies.push_back(latUs);
        } else {
            WARN("Add failed at doc " << i << ": " << result.error().message);
        }
    }

    auto elapsed = std::chrono::steady_clock::now() - startTime;
    double elapsedSec = std::chrono::duration<double>(elapsed).count();
    double throughput = static_cast<double>(addLatencies.size()) / elapsedSec;

    // Wait for pipeline drain
    REQUIRE(waitForDrain(client, std::chrono::seconds(cfg.drainTimeoutSecs)));

    auto snap = DaemonSnapshot::capture(client);
    auto stats = PercentileStats::compute(addLatencies);

    std::cout << "\n=== Single Client Baseline ===\n";
    std::cout << "  Docs ingested: " << addLatencies.size() << "/" << cfg.docsPerClient << "\n";
    std::cout << "  Elapsed: " << std::fixed << std::setprecision(2) << elapsedSec << "s\n";
    std::cout << "  Throughput: " << throughput << " docs/s\n";
    printPercentiles("Add latency", stats);
    std::cout << "  Final docs_total: " << snap.documentsTotal << "\n";
    std::cout << "  Memory: " << snap.memoryUsageMb << " MB\n\n";

    CHECK(static_cast<int>(addLatencies.size()) == cfg.docsPerClient);
    CHECK(throughput > 0.0);

    // Emit JSONL record for regression tracking
    json record{
        {"timestamp", isoTimestamp()},
        {"test", "baseline_single_client"},
        {"num_clients", 1},
        {"docs_per_client", cfg.docsPerClient},
        {"doc_size_bytes", cfg.docSizeBytes},
        {"elapsed_seconds", elapsedSec},
        {"throughput_docs_per_sec", throughput},
        {"add_latency", stats.toJson()},
        {"daemon_snapshot", snap.toJson()},
        {"tuning_profile", cfg.tuningProfile},
    };
    emitJsonl(cfg.outputPath, record);
}

TEST_CASE("Multi-client ingestion: concurrent pure ingest",
          "[!benchmark][multi-client][ingestion]") {
    auto cfg = BenchConfig::fromEnv();

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Status-monitoring client (separate from worker clients)
    ClientConfig monCfg;
    monCfg.socketPath = harness.socketPath();
    monCfg.autoStart = false;
    monCfg.requestTimeout = 10s;
    DaemonClient monitorClient(monCfg);

    // Start time-series sampler
    TimeSeriesSampler sampler;
    sampler.start(monitorClient, 500ms);

    // Shared counters
    std::atomic<int> totalDocsIngested{0};
    std::atomic<int> totalFailures{0};
    std::mutex resultsMutex;
    std::vector<ClientResult> clientResults(cfg.numClients);

    auto globalStart = std::chrono::steady_clock::now();

    // Launch concurrent client threads
    std::vector<std::thread> threads;
    threads.reserve(cfg.numClients);

    for (int c = 0; c < cfg.numClients; ++c) {
        threads.emplace_back([&, c]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 30s;
            DaemonClient client(ccfg);

            ClientResult& result = clientResults[c];
            result.clientId = c;

            auto clientStart = std::chrono::steady_clock::now();

            for (int i = 0; i < cfg.docsPerClient; ++i) {
                auto doc = generateDocument(c, i, cfg.docSizeBytes);
                AddDocumentRequest req;
                req.name = "client" + std::to_string(c) + "_doc_" + std::to_string(i) + ".md";
                req.content = std::move(doc);
                req.tags = {"bench", "multi-client", "client-" + std::to_string(c)};
                req.collection = "bench_client_" + std::to_string(c);

                auto t0 = std::chrono::steady_clock::now();
                auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                OpLatency op;
                op.opType = "add";
                op.clientId = c;
                op.latencyUs = latUs;
                op.success = static_cast<bool>(res);
                if (!res)
                    op.errorMsg = res.error().message;

                result.latencies.push_back(op);

                if (res) {
                    result.docsIngested++;
                    totalDocsIngested.fetch_add(1);
                } else {
                    result.failures++;
                    totalFailures.fetch_add(1);
                }
            }

            auto clientEnd = std::chrono::steady_clock::now();
            result.elapsedSeconds = std::chrono::duration<double>(clientEnd - clientStart).count();
            result.addThroughputDocsPerSec =
                (result.docsIngested > 0)
                    ? static_cast<double>(result.docsIngested) / result.elapsedSeconds
                    : 0.0;
        });
    }

    for (auto& t : threads)
        t.join();

    sampler.stop();

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();
    double aggregateThroughput = static_cast<double>(totalDocsIngested.load()) / globalElapsed;

    // Wait for pipeline drain
    bool drained = waitForDrain(monitorClient, std::chrono::seconds(cfg.drainTimeoutSecs));
    auto finalSnap = DaemonSnapshot::capture(monitorClient);

    // Compute aggregate latency stats
    std::vector<int64_t> allAddLatencies;
    for (auto& cr : clientResults) {
        for (auto& op : cr.latencies) {
            if (op.opType == "add" && op.success)
                allAddLatencies.push_back(op.latencyUs);
        }
    }
    auto aggStats = PercentileStats::compute(allAddLatencies);

    std::map<std::string, int> failureBreakdown;
    std::map<std::string, std::string> failureSamples;
    for (const auto& cr : clientResults) {
        for (const auto& op : cr.latencies) {
            if (op.opType != "add" || op.success) {
                continue;
            }
            const auto bucket = classifyFailureMessage(op.errorMsg);
            failureBreakdown[bucket] += 1;
            if (!op.errorMsg.empty() && !failureSamples.contains(bucket)) {
                failureSamples[bucket] = op.errorMsg;
            }
        }
    }

    // Print summary
    std::cout << "\n=== Concurrent Pure Ingest (N=" << cfg.numClients << ") ===\n";
    std::cout << "  Total docs: " << totalDocsIngested.load() << "/"
              << (cfg.numClients * cfg.docsPerClient) << "\n";
    std::cout << "  Total failures: " << totalFailures.load() << "\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    std::cout << "  Aggregate throughput: " << aggregateThroughput << " docs/s\n";
    printPercentiles("Add latency (all)", aggStats);

    if (!failureBreakdown.empty()) {
        std::cout << "\n  Failure breakdown (add requests):\n";
        for (const auto& [bucket, count] : failureBreakdown) {
            std::cout << "    " << bucket << ": " << count;
            auto sampleIt = failureSamples.find(bucket);
            if (sampleIt != failureSamples.end()) {
                std::cout << " (sample: " << sampleIt->second << ")";
            }
            std::cout << "\n";
        }
    }

    std::cout << "\n  Per-client breakdown:\n";
    for (auto& cr : clientResults) {
        std::cout << "    Client " << cr.clientId << ": " << cr.docsIngested << " docs, "
                  << std::fixed << std::setprecision(1) << cr.addThroughputDocsPerSec
                  << " docs/s, failures=" << cr.failures << "\n";
    }

    // Fairness metric: coefficient of variation of per-client throughput
    {
        std::vector<double> perClientThroughput;
        for (auto& cr : clientResults) {
            if (cr.addThroughputDocsPerSec > 0)
                perClientThroughput.push_back(cr.addThroughputDocsPerSec);
        }
        if (perClientThroughput.size() > 1) {
            double mean =
                std::accumulate(perClientThroughput.begin(), perClientThroughput.end(), 0.0) /
                static_cast<double>(perClientThroughput.size());
            double variance = 0.0;
            for (auto v : perClientThroughput)
                variance += (v - mean) * (v - mean);
            variance /= static_cast<double>(perClientThroughput.size());
            double cv = (mean > 0) ? std::sqrt(variance) / mean : 0.0;
            std::cout << "  Fairness (CV of throughput): " << std::fixed << std::setprecision(3)
                      << cv << " (lower = more fair)\n";
        }
    }

    std::cout << "  Queue drained: " << (drained ? "yes" : "NO") << "\n";
    std::cout << "  Final docs_total: " << finalSnap.documentsTotal << "\n";
    std::cout << "  Final memory: " << finalSnap.memoryUsageMb << " MB\n\n";

    CHECK(totalDocsIngested.load() == cfg.numClients * cfg.docsPerClient);
    CHECK(drained);

    // Emit JSONL
    json perClient = json::array();
    for (auto& cr : clientResults) {
        perClient.push_back(json{
            {"client_id", cr.clientId},
            {"docs_ingested", cr.docsIngested},
            {"throughput_docs_per_sec", cr.addThroughputDocsPerSec},
            {"elapsed_seconds", cr.elapsedSeconds},
            {"failures", cr.failures},
        });
    }

    // Time-series data
    json timeSeries = json::array();
    for (auto& s : sampler.getSamples()) {
        timeSeries.push_back(json{{"elapsed_ms", s.elapsedMs}, {"snapshot", s.snap.toJson()}});
    }

    json failureBreakdownJson = json::object();
    for (const auto& [bucket, count] : failureBreakdown) {
        failureBreakdownJson[bucket] = count;
    }

    json failureSamplesJson = json::object();
    for (const auto& [bucket, sample] : failureSamples) {
        failureSamplesJson[bucket] = sample;
    }

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "concurrent_pure_ingest"},
        {"num_clients", cfg.numClients},
        {"docs_per_client", cfg.docsPerClient},
        {"doc_size_bytes", cfg.docSizeBytes},
        {"total_docs_ingested", totalDocsIngested.load()},
        {"total_failures", totalFailures.load()},
        {"elapsed_seconds", globalElapsed},
        {"aggregate_throughput_docs_per_sec", aggregateThroughput},
        {"add_latency", aggStats.toJson()},
        {"failure_breakdown", failureBreakdownJson},
        {"failure_samples", failureSamplesJson},
        {"per_client", perClient},
        {"daemon_snapshot_final", finalSnap.toJson()},
        {"time_series", timeSeries},
        {"tuning_profile", cfg.tuningProfile},
        {"drained", drained},
    };
    emitJsonl(cfg.outputPath, record);
}

TEST_CASE("Multi-client ingestion: mixed read/write workload",
          "[!benchmark][multi-client][mixed]") {
    auto cfg = BenchConfig::fromEnv();

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Monitor client
    ClientConfig monCfg;
    monCfg.socketPath = harness.socketPath();
    monCfg.autoStart = false;
    monCfg.requestTimeout = 10s;
    DaemonClient monitorClient(monCfg);

    // Pre-seed corpus so search queries return results
    if (cfg.warmupDocs > 0) {
        ClientConfig warmCfg;
        warmCfg.socketPath = harness.socketPath();
        warmCfg.autoStart = false;
        warmCfg.requestTimeout = 30s;
        DaemonClient warmClient(warmCfg);

        std::cout << "  Pre-seeding " << cfg.warmupDocs << " warmup docs...\n";
        for (int i = 0; i < cfg.warmupDocs; ++i) {
            AddDocumentRequest req;
            req.name = "warmup_doc_" + std::to_string(i) + ".md";
            req.content = generateDocument(99, i, cfg.docSizeBytes);
            req.tags = {"warmup"};
            auto res = yams::cli::run_sync(warmClient.streamingAddDocument(req), 30s);
            if (!res) {
                WARN("Warmup doc " << i << " failed: " << res.error().message);
            }
        }
        // Wait for warmup to settle
        waitForDrain(warmClient, 30s);
        std::this_thread::sleep_for(1s);
    }

    TimeSeriesSampler sampler;
    sampler.start(monitorClient, 500ms);

    std::atomic<int> totalAdds{0};
    std::atomic<int> totalSearches{0};
    std::atomic<int> totalLists{0};
    std::atomic<int> totalFailures{0};
    std::mutex resultsMutex;
    std::vector<ClientResult> clientResults(cfg.numClients);

    auto globalStart = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(cfg.numClients);

    for (int c = 0; c < cfg.numClients; ++c) {
        threads.emplace_back([&, c]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 30s;
            DaemonClient client(ccfg);

            ClientResult& result = clientResults[c];
            result.clientId = c;

            std::mt19937 rng(static_cast<unsigned>(c * 1000 + 42));
            std::uniform_real_distribution<double> opDist(0.0, 1.0);

            auto clientStart = std::chrono::steady_clock::now();
            int docIdx = 0;

            // Total operations = docsPerClient ingestions + proportional reads
            int totalOps = cfg.docsPerClient;
            if (cfg.searchRatio > 0.0) {
                totalOps = static_cast<int>(static_cast<double>(cfg.docsPerClient) /
                                            (1.0 - cfg.searchRatio));
            }

            for (int op = 0; op < totalOps; ++op) {
                double roll = opDist(rng);
                bool doSearch = (roll < cfg.searchRatio) && (docIdx > 0 || cfg.warmupDocs > 0);
                bool doList = (roll >= cfg.searchRatio && roll < cfg.searchRatio + 0.05);

                if (doSearch) {
                    // Search operation
                    SearchRequest req;
                    req.query = "architecture performance tuning";
                    req.limit = 5;

                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.search(req), 10s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    OpLatency lat;
                    lat.opType = "search";
                    lat.clientId = c;
                    lat.latencyUs = latUs;
                    lat.success = static_cast<bool>(res);
                    if (!res)
                        lat.errorMsg = res.error().message;
                    result.latencies.push_back(lat);

                    if (res) {
                        result.searchesExecuted++;
                        totalSearches.fetch_add(1);
                    } else {
                        result.failures++;
                        totalFailures.fetch_add(1);
                    }
                } else if (doList) {
                    // List operation
                    ListRequest req;
                    req.limit = 20;

                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.list(req), 10s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    OpLatency lat;
                    lat.opType = "list";
                    lat.clientId = c;
                    lat.latencyUs = latUs;
                    lat.success = static_cast<bool>(res);
                    if (!res)
                        lat.errorMsg = res.error().message;
                    result.latencies.push_back(lat);

                    if (res) {
                        result.listsExecuted++;
                        totalLists.fetch_add(1);
                    } else {
                        result.failures++;
                        totalFailures.fetch_add(1);
                    }
                } else {
                    // Add operation
                    if (docIdx >= cfg.docsPerClient)
                        continue; // Already ingested all docs for this client

                    auto doc = generateDocument(c, docIdx, cfg.docSizeBytes);
                    AddDocumentRequest req;
                    req.name =
                        "mixed_c" + std::to_string(c) + "_d" + std::to_string(docIdx) + ".md";
                    req.content = std::move(doc);
                    req.tags = {"bench", "mixed", "client-" + std::to_string(c)};
                    req.collection = "bench_mixed_" + std::to_string(c);

                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    OpLatency lat;
                    lat.opType = "add";
                    lat.clientId = c;
                    lat.latencyUs = latUs;
                    lat.success = static_cast<bool>(res);
                    if (!res)
                        lat.errorMsg = res.error().message;
                    result.latencies.push_back(lat);

                    if (res) {
                        result.docsIngested++;
                        totalAdds.fetch_add(1);
                    } else {
                        result.failures++;
                        totalFailures.fetch_add(1);
                    }
                    docIdx++;
                }
            }

            auto clientEnd = std::chrono::steady_clock::now();
            result.elapsedSeconds = std::chrono::duration<double>(clientEnd - clientStart).count();
            result.addThroughputDocsPerSec =
                (result.docsIngested > 0)
                    ? static_cast<double>(result.docsIngested) / result.elapsedSeconds
                    : 0.0;
        });
    }

    for (auto& t : threads)
        t.join();

    sampler.stop();

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();

    bool drained = waitForDrain(monitorClient, std::chrono::seconds(cfg.drainTimeoutSecs));
    auto finalSnap = DaemonSnapshot::capture(monitorClient);

    // Compute per-op-type latency stats
    std::vector<int64_t> addLats, searchLats, listLats;
    for (auto& cr : clientResults) {
        for (auto& op : cr.latencies) {
            if (!op.success)
                continue;
            if (op.opType == "add")
                addLats.push_back(op.latencyUs);
            else if (op.opType == "search")
                searchLats.push_back(op.latencyUs);
            else if (op.opType == "list")
                listLats.push_back(op.latencyUs);
        }
    }
    auto addStats = PercentileStats::compute(addLats);
    auto searchStats = PercentileStats::compute(searchLats);
    auto listStats = PercentileStats::compute(listLats);

    std::cout << "\n=== Mixed Read/Write Workload (N=" << cfg.numClients
              << ", search_ratio=" << cfg.searchRatio << ") ===\n";
    std::cout << "  Adds: " << totalAdds.load() << ", Searches: " << totalSearches.load()
              << ", Lists: " << totalLists.load() << ", Failures: " << totalFailures.load() << "\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    printPercentiles("Add latency", addStats);
    printPercentiles("Search latency", searchStats);
    printPercentiles("List latency", listStats);
    std::cout << "  Queue drained: " << (drained ? "yes" : "NO") << "\n";
    std::cout << "  Final docs_total: " << finalSnap.documentsTotal << "\n\n";

    CHECK(totalAdds.load() > 0);
    CHECK(drained);

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "mixed_read_write"},
        {"num_clients", cfg.numClients},
        {"docs_per_client", cfg.docsPerClient},
        {"search_ratio", cfg.searchRatio},
        {"warmup_docs", cfg.warmupDocs},
        {"total_adds", totalAdds.load()},
        {"total_searches", totalSearches.load()},
        {"total_lists", totalLists.load()},
        {"total_failures", totalFailures.load()},
        {"elapsed_seconds", globalElapsed},
        {"add_latency", addStats.toJson()},
        {"search_latency", searchStats.toJson()},
        {"list_latency", listStats.toJson()},
        {"daemon_snapshot_final", finalSnap.toJson()},
        {"tuning_profile", cfg.tuningProfile},
        {"drained", drained},
    };
    emitJsonl(cfg.outputPath, record);
}

TEST_CASE("Multi-client ingestion: scaling curve", "[!benchmark][multi-client][scaling]") {
    // Run the same workload at increasing powers-of-two clients to find scaling limits.
    auto baseCfg = BenchConfig::fromEnv();
    std::vector<int> clientCounts;
    for (int n = 1; n <= baseCfg.scalingMaxClients; n *= 2) {
        clientCounts.push_back(n);
        if (n > (std::numeric_limits<int>::max() / 2)) {
            break;
        }
    }
    if (!clientCounts.empty() && clientCounts.back() != baseCfg.scalingMaxClients) {
        clientCounts.push_back(baseCfg.scalingMaxClients);
    }

    json scalingResults = json::array();

    for (int numClients : clientCounts) {
        std::cout << "\n--- Scaling test: " << numClients << " client(s) ---\n";

        DaemonHarness harness(benchHarnessOptions());
        REQUIRE(startHarnessWithRetry(harness, kStartTimeout));

        ClientConfig monCfg;
        monCfg.socketPath = harness.socketPath();
        monCfg.autoStart = false;
        monCfg.requestTimeout = 10s;
        DaemonClient monitorClient(monCfg);

        std::atomic<int> totalDocs{0};
        std::atomic<int> totalFail{0};
        std::vector<ClientResult> results(numClients);

        auto globalStart = std::chrono::steady_clock::now();

        std::vector<std::thread> threads;
        for (int c = 0; c < numClients; ++c) {
            threads.emplace_back([&, c]() {
                ClientConfig ccfg;
                ccfg.socketPath = harness.socketPath();
                ccfg.autoStart = false;
                ccfg.requestTimeout = 30s;
                DaemonClient client(ccfg);

                auto start = std::chrono::steady_clock::now();
                for (int i = 0; i < baseCfg.docsPerClient; ++i) {
                    AddDocumentRequest req;
                    req.name = "scale_c" + std::to_string(c) + "_d" + std::to_string(i) + ".md";
                    req.content = generateDocument(c, i, baseCfg.docSizeBytes);
                    req.tags = {"bench", "scaling"};

                    auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
                    if (res) {
                        results[c].docsIngested++;
                        totalDocs.fetch_add(1);
                    } else {
                        results[c].failures++;
                        totalFail.fetch_add(1);
                    }
                }
                auto end = std::chrono::steady_clock::now();
                results[c].elapsedSeconds = std::chrono::duration<double>(end - start).count();
                results[c].addThroughputDocsPerSec =
                    results[c].docsIngested / results[c].elapsedSeconds;
            });
        }

        for (auto& t : threads)
            t.join();

        auto globalEnd = std::chrono::steady_clock::now();
        double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();
        double aggThroughput = static_cast<double>(totalDocs.load()) / globalElapsed;

        bool drained = waitForDrain(monitorClient, std::chrono::seconds(baseCfg.drainTimeoutSecs));
        auto finalSnap = DaemonSnapshot::capture(monitorClient);

        // Per-client throughput stats
        std::vector<double> perClient;
        for (auto& r : results)
            perClient.push_back(r.addThroughputDocsPerSec);

        double meanPerClient = std::accumulate(perClient.begin(), perClient.end(), 0.0) /
                               static_cast<double>(perClient.size());

        std::cout << "  Clients: " << numClients << "  Aggregate: " << std::fixed
                  << std::setprecision(1) << aggThroughput << " docs/s"
                  << "  Per-client mean: " << meanPerClient << " docs/s"
                  << "  Failures: " << totalFail.load() << "  Memory: " << finalSnap.memoryUsageMb
                  << " MB"
                  << "  Drained: " << (drained ? "yes" : "NO") << "\n";

        CHECK(totalDocs.load() == numClients * baseCfg.docsPerClient);

        scalingResults.push_back(json{
            {"num_clients", numClients},
            {"aggregate_throughput", aggThroughput},
            {"per_client_mean_throughput", meanPerClient},
            {"total_docs", totalDocs.load()},
            {"total_failures", totalFail.load()},
            {"elapsed_seconds", globalElapsed},
            {"memory_mb", finalSnap.memoryUsageMb},
            {"drained", drained},
        });

        // Harness destructor will stop the daemon
    }

    // Print scaling summary
    std::cout << "\n=== Scaling Curve Summary ===\n";
    std::cout << std::setw(10) << "Clients" << std::setw(18) << "Agg docs/s" << std::setw(18)
              << "Per-client docs/s" << std::setw(12) << "Efficiency"
              << "\n";
    double baselineThroughput = 0;
    for (auto& r : scalingResults) {
        double agg = r["aggregate_throughput"];
        double perClient = r["per_client_mean_throughput"];
        int n = r["num_clients"];
        if (n == 1)
            baselineThroughput = agg;
        double efficiency =
            (baselineThroughput > 0) ? (agg / (baselineThroughput * n)) * 100.0 : 0.0;
        std::cout << std::setw(10) << n << std::setw(18) << std::fixed << std::setprecision(1)
                  << agg << std::setw(18) << perClient << std::setw(11) << std::setprecision(1)
                  << efficiency << "%\n";
    }
    std::cout << std::endl;

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "scaling_curve"},
        {"docs_per_client", baseCfg.docsPerClient},
        {"doc_size_bytes", baseCfg.docSizeBytes},
        {"scaling_results", scalingResults},
        {"tuning_profile", baseCfg.tuningProfile},
    };
    emitJsonl(baseCfg.outputPath, record);
}

TEST_CASE("Multi-client ingestion: connection contention",
          "[!benchmark][multi-client][contention]") {
    // Stress test: many clients making rapid short-lived connections
    auto cfg = BenchConfig::fromEnv();
    int burstClients = cfg.numClients * 4; // Higher contention than normal
    int opsPerClient = 20;                 // Shorter burst per client

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Monitor
    ClientConfig monCfg;
    monCfg.socketPath = harness.socketPath();
    monCfg.autoStart = false;
    monCfg.requestTimeout = 10s;
    DaemonClient monitorClient(monCfg);

    (void)DaemonSnapshot::capture(monitorClient); // warmup connection

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};
    std::atomic<int> retryAfterCount{0};
    std::mutex latMutex;
    std::vector<int64_t> allLatencies;

    auto globalStart = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    for (int c = 0; c < burstClients; ++c) {
        threads.emplace_back([&, c]() {
            std::vector<int64_t> localLats;
            for (int i = 0; i < opsPerClient; ++i) {
                // Alternate between status, list, and add to test different op types
                ClientConfig ccfg;
                ccfg.socketPath = harness.socketPath();
                ccfg.autoStart = false;
                ccfg.requestTimeout = 10s;
                DaemonClient client(ccfg);

                auto t0 = std::chrono::steady_clock::now();
                bool success = false;

                if (i % 3 == 0) {
                    auto res = yams::cli::run_sync(client.status(), 10s);
                    success = static_cast<bool>(res);
                    if (res && res.value().retryAfterMs > 0)
                        retryAfterCount.fetch_add(1);
                } else if (i % 3 == 1) {
                    ListRequest req;
                    req.limit = 5;
                    auto res = yams::cli::run_sync(client.list(req), 10s);
                    success = static_cast<bool>(res);
                } else {
                    AddDocumentRequest req;
                    req.name = "burst_c" + std::to_string(c) + "_" + std::to_string(i) + ".md";
                    req.content = "Burst test content from client " + std::to_string(c);
                    auto res = yams::cli::run_sync(client.streamingAddDocument(req), 10s);
                    success = static_cast<bool>(res);
                }

                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                localLats.push_back(latUs);

                if (success)
                    successCount.fetch_add(1);
                else
                    failCount.fetch_add(1);
            }

            std::lock_guard<std::mutex> lock(latMutex);
            allLatencies.insert(allLatencies.end(), localLats.begin(), localLats.end());
        });
    }

    for (auto& t : threads)
        t.join();

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();
    int totalOps = burstClients * opsPerClient;
    double throughput = static_cast<double>(successCount.load()) / globalElapsed;

    auto postSnap = DaemonSnapshot::capture(monitorClient);
    auto latStats = PercentileStats::compute(allLatencies);

    std::cout << "\n=== Connection Contention (N=" << burstClients << " burst clients) ===\n";
    std::cout << "  Total ops: " << totalOps << " (success=" << successCount.load()
              << " fail=" << failCount.load() << ")\n";
    std::cout << "  Backpressure signals (retryAfter>0): " << retryAfterCount.load() << "\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    std::cout << "  Throughput: " << throughput << " ops/s\n";
    printPercentiles("Op latency", latStats);
    std::cout << "  Peak connections: " << postSnap.maxConnections << "\n";
    std::cout << "  Forced closes: " << postSnap.requestsProcessed << " total reqs processed\n\n";

    // Failures acceptable under extreme contention, but should be minority
    double failRate = static_cast<double>(failCount.load()) / static_cast<double>(totalOps);
    CHECK(failRate < 0.20); // Less than 20% failure acceptable under burst

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "connection_contention"},
        {"burst_clients", burstClients},
        {"ops_per_client", opsPerClient},
        {"total_ops", totalOps},
        {"success_count", successCount.load()},
        {"fail_count", failCount.load()},
        {"retry_after_count", retryAfterCount.load()},
        {"elapsed_seconds", globalElapsed},
        {"throughput_ops_per_sec", throughput},
        {"latency", latStats.toJson()},
        {"tuning_profile", cfg.tuningProfile},
    };
    emitJsonl(cfg.outputPath, record);
}

TEST_CASE("Multi-client ingestion: reader starvation under ingest load",
          "[!benchmark][multi-client][starvation]") {
    // Verify that readers are not starved when heavy ingestion is in progress
    auto cfg = BenchConfig::fromEnv();

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Pre-seed so readers have something to query
    {
        ClientConfig warmCfg;
        warmCfg.socketPath = harness.socketPath();
        warmCfg.autoStart = false;
        warmCfg.requestTimeout = 30s;
        DaemonClient warmClient(warmCfg);

        for (int i = 0; i < 30; ++i) {
            AddDocumentRequest req;
            req.name = "preseed_" + std::to_string(i) + ".md";
            req.content = generateDocument(0, i, cfg.docSizeBytes);
            yams::cli::run_sync(warmClient.streamingAddDocument(req), 30s);
        }
        waitForDrain(warmClient, 30s);
    }

    // Writer clients (heavy ingestion)
    int numWriters = std::max(1, cfg.numClients - 1);
    // Reader clients (continuous reads)
    int numReaders = std::max(1, cfg.numClients / 2);

    std::atomic<bool> done{false};
    std::atomic<int> writerDocs{0};
    std::atomic<int> readerOps{0};
    std::atomic<int> readerFails{0};
    std::mutex readerLatMutex;
    std::vector<int64_t> readerLatencies;

    auto globalStart = std::chrono::steady_clock::now();

    // Launch writers
    std::vector<std::thread> writerThreads;
    for (int w = 0; w < numWriters; ++w) {
        writerThreads.emplace_back([&, w]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 30s;
            DaemonClient client(ccfg);

            for (int i = 0; i < cfg.docsPerClient; ++i) {
                AddDocumentRequest req;
                req.name = "writer" + std::to_string(w) + "_" + std::to_string(i) + ".md";
                req.content = generateDocument(w, i, cfg.docSizeBytes);
                auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
                if (res)
                    writerDocs.fetch_add(1);
            }
        });
    }

    // Launch readers (run until writers finish)
    std::vector<std::thread> readerThreads;
    for (int r = 0; r < numReaders; ++r) {
        readerThreads.emplace_back([&]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 10s;
            DaemonClient client(ccfg);

            while (!done.load()) {
                auto t0 = std::chrono::steady_clock::now();
                ListRequest req;
                req.limit = 10;
                auto res = yams::cli::run_sync(client.list(req), 10s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                if (res) {
                    readerOps.fetch_add(1);
                    std::lock_guard<std::mutex> lock(readerLatMutex);
                    readerLatencies.push_back(latUs);
                } else {
                    readerFails.fetch_add(1);
                }

                // Small delay to avoid pure spin
                std::this_thread::sleep_for(10ms);
            }
        });
    }

    // Wait for writers to finish
    for (auto& t : writerThreads)
        t.join();
    done.store(true);

    // Wait for readers to stop
    for (auto& t : readerThreads)
        t.join();

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();

    auto readerStats = PercentileStats::compute(readerLatencies);

    std::cout << "\n=== Reader Starvation Test ===\n";
    std::cout << "  Writers: " << numWriters << " (ingested " << writerDocs.load() << " docs)\n";
    std::cout << "  Readers: " << numReaders << " (" << readerOps.load() << " ops, "
              << readerFails.load() << " fails)\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    printPercentiles("Reader latency", readerStats);

    // Reader p99 should stay reasonable even under write load
    // This threshold can be tuned based on baseline measurements
    if (readerStats.count > 0) {
        std::cout << "  Reader throughput: "
                  << static_cast<double>(readerOps.load()) / globalElapsed << " ops/s\n\n";
    }

    CHECK(readerOps.load() > 0);                  // Readers must make progress
    CHECK(readerFails.load() < readerOps.load()); // Most reads should succeed

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "reader_starvation"},
        {"num_writers", numWriters},
        {"num_readers", numReaders},
        {"docs_per_writer", cfg.docsPerClient},
        {"total_writer_docs", writerDocs.load()},
        {"total_reader_ops", readerOps.load()},
        {"total_reader_fails", readerFails.load()},
        {"elapsed_seconds", globalElapsed},
        {"reader_latency", readerStats.toJson()},
        {"tuning_profile", cfg.tuningProfile},
    };
    emitJsonl(cfg.outputPath, record);
}
