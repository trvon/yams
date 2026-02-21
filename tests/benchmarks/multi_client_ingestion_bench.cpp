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
//   YAMS_BENCH_DATA_DIR           - Existing YAMS data dir (for large-corpus bench)
//   YAMS_BENCH_ENABLE_PLUGINS     - Enable plugin loading: 1/true/yes (default: off)
//   YAMS_BENCH_PLUGIN_DIR         - Directory containing plugin .dylib/.so files
//   YAMS_BENCH_TRUSTED_PLUGIN_PATHS - Colon-separated trusted plugin search paths
//   YAMS_BENCH_REAL_MODEL_PROVIDER  - Use real ONNX model provider: 1/true/yes

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

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#endif

#include "../common/test_helpers_catch2.h"
#include "../integration/daemon/test_async_helpers.h"
#include "../integration/daemon/test_daemon_harness.h"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/metric_keys.h>
#include <yams/mcp/mcp_server.h>

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

    // --- Storage / corpus overrides ---
    // Point at an existing YAMS data directory (e.g. /Volumes/picaso/yams)
    // When set, the daemon serves this corpus instead of creating an empty temp db.
    std::optional<fs::path> dataDir;

    // --- Plugin / embedding config ---
    // Enable real plugin loading (ONNX embeddings, PDF extraction, etc.)
    bool enablePlugins{false};
    // Directory containing built plugin .dylib/.so files
    std::optional<fs::path> pluginDir;
    // Trusted plugin search paths (from plugins.trust)
    std::vector<fs::path> trustedPluginPaths;
    // Use real model provider instead of mock (requires ONNX runtime + models)
    bool useRealModelProvider{false};
    bool useMcpPath{false};

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

        // Storage path override: point at an existing corpus
        if (auto* v = std::getenv("YAMS_BENCH_DATA_DIR"))
            cfg.dataDir = fs::path(v);

        // Plugin / embedding configuration
        if (auto* v = std::getenv("YAMS_BENCH_ENABLE_PLUGINS")) {
            std::string s(v);
            cfg.enablePlugins = (s == "1" || s == "true" || s == "yes");
        }
        if (auto* v = std::getenv("YAMS_BENCH_PLUGIN_DIR"))
            cfg.pluginDir = fs::path(v);
        if (auto* v = std::getenv("YAMS_BENCH_TRUSTED_PLUGIN_PATHS")) {
            // Colon-separated list of paths
            std::string paths(v);
            std::istringstream iss(paths);
            std::string path;
            while (std::getline(iss, path, ':')) {
                if (!path.empty())
                    cfg.trustedPluginPaths.push_back(fs::path(path));
            }
        }
        if (auto* v = std::getenv("YAMS_BENCH_REAL_MODEL_PROVIDER")) {
            std::string s(v);
            cfg.useRealModelProvider = (s == "1" || s == "true" || s == "yes");
        }

        if (auto* v = std::getenv("YAMS_BENCH_USE_MCP")) {
            std::string s(v);
            cfg.useMcpPath = (s == "1" || s == "true" || s == "yes");
        }

        return cfg;
    }
};

class NullTransport : public yams::mcp::ITransport {
public:
    void send(const nlohmann::json&) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Disconnected;
    }
};

class MCPPipelineClient {
public:
    explicit MCPPipelineClient(const fs::path& socketPath) {
        ::setenv("YAMS_DAEMON_SOCKET_PATH", socketPath.string().c_str(), 1);
        ::setenv("YAMS_CLI_DISABLE_DAEMON_AUTOSTART", "1", 1);
        server_ = std::make_shared<yams::mcp::MCPServer>(std::make_unique<NullTransport>());
        initialize();
    }

    yams::Result<nlohmann::json> queryStep(const std::string& op, const nlohmann::json& params) {
        std::lock_guard<std::mutex> lock(mu_);
        return queryStepUnlocked(op, params);
    }

private:
    yams::Result<nlohmann::json> queryStepUnlocked(const std::string& op,
                                                   const nlohmann::json& params) {
        nlohmann::json request = nlohmann::json::object();
        request["jsonrpc"] = "2.0";
        request["id"] = requestId_++;
        request["method"] = "tools/call";
        request["params"] = nlohmann::json::object();
        request["params"]["name"] = hasQueryTool_ ? "query" : op;
        if (hasQueryTool_) {
            request["params"]["arguments"] = nlohmann::json::object();
            request["params"]["arguments"]["steps"] =
                nlohmann::json::array({{{"op", op}, {"params", params}}});
        } else {
            request["params"]["arguments"] = params.is_object() ? params : nlohmann::json::object();
        }

        auto responseResult = server_->processMessage(request);
        if (!responseResult) {
            return yams::Error{responseResult.error()};
        }

        const auto& response = responseResult.value();
        if (response.contains("error")) {
            const std::string msg = response["error"].value("message", "MCP tools/call error");
            if (hasQueryTool_ && msg.rfind("Unknown tool: query", 0) == 0) {
                hasQueryTool_ = false;
                return queryStepUnlocked(op, params);
            }
            return yams::Error{yams::ErrorCode::InvalidState, msg};
        }
        if (!response.contains("result") || !response["result"].is_object()) {
            return yams::Error{yams::ErrorCode::InvalidState,
                               "MCP tools/call missing result object"};
        }

        const auto& result = response["result"];
        if (result.value("isError", false)) {
            std::string msg{"MCP tool returned error"};
            if (result.contains("content") && result["content"].is_array() &&
                !result["content"].empty() && result["content"][0].contains("text")) {
                msg = result["content"][0]["text"].get<std::string>();
            }
            return yams::Error{yams::ErrorCode::InvalidState, msg};
        }

        if (result.contains("structuredContent") && result["structuredContent"].is_object()) {
            const auto& sc = result["structuredContent"];
            if (sc.contains("data")) {
                return sc["data"];
            }
        }

        if (result.contains("content") && result["content"].is_array() &&
            !result["content"].empty() && result["content"][0].contains("text")) {
            try {
                return nlohmann::json::parse(result["content"][0]["text"].get<std::string>());
            } catch (...) {
                return nlohmann::json{{"text", result["content"][0]["text"]}};
            }
        }

        return yams::Error{yams::ErrorCode::InvalidState, "MCP tool result had no data"};
    }

    void initialize() {
        nlohmann::json initReq = nlohmann::json::object();
        initReq["jsonrpc"] = "2.0";
        initReq["id"] = requestId_++;
        initReq["method"] = "initialize";
        initReq["params"] = nlohmann::json::object();
        initReq["params"]["protocolVersion"] = "2025-06-18";
        initReq["params"]["capabilities"] = nlohmann::json::object();
        initReq["params"]["clientInfo"] =
            nlohmann::json{{"name", "multi_client_ingestion_bench"}, {"version", "1.0"}};
        auto initRes = server_->processMessage(initReq);
        if (!initRes) {
            throw std::runtime_error("MCP initialize failed: " + initRes.error().message);
        }

        nlohmann::json initializedNotif = {{"jsonrpc", "2.0"},
                                           {"method", "notifications/initialized"},
                                           {"params", nlohmann::json::object()}};
        (void)server_->processMessage(initializedNotif);

        nlohmann::json toolsReq = nlohmann::json::object();
        toolsReq["jsonrpc"] = "2.0";
        toolsReq["id"] = requestId_++;
        toolsReq["method"] = "tools/list";
        toolsReq["params"] = nlohmann::json::object();
        auto toolsRes = server_->processMessage(toolsReq);
        if (!toolsRes) {
            throw std::runtime_error("MCP tools/list failed: " + toolsRes.error().message);
        }
        const auto& toolsResp = toolsRes.value();
        if (!toolsResp.contains("result") || !toolsResp["result"].is_object() ||
            !toolsResp["result"].contains("tools") || !toolsResp["result"]["tools"].is_array()) {
            throw std::runtime_error("MCP tools/list returned invalid shape");
        }
        hasQueryTool_ = false;
        for (const auto& tool : toolsResp["result"]["tools"]) {
            if (!tool.is_object() || !tool.contains("name") || !tool["name"].is_string()) {
                continue;
            }
            if (tool["name"].get<std::string>() == "query") {
                hasQueryTool_ = true;
                break;
            }
        }
    }

    std::shared_ptr<yams::mcp::MCPServer> server_;
    std::mutex mu_;
    bool hasQueryTool_{false};
    std::uint64_t requestId_{1};
};

class MCPPipelineClientPool {
public:
    MCPPipelineClientPool(const fs::path& socketPath, size_t poolSize) {
        const size_t size = std::max<size_t>(1, poolSize);
        clients_.reserve(size);
        for (size_t i = 0; i < size; ++i) {
            clients_.push_back(std::make_unique<MCPPipelineClient>(socketPath));
        }
    }

    yams::Result<nlohmann::json> queryStep(const std::string& op, const nlohmann::json& params) {
        const size_t idx = next_.fetch_add(1, std::memory_order_relaxed) % clients_.size();
        return clients_[idx]->queryStep(op, params);
    }

    yams::Result<nlohmann::json> queryStepForSlot(size_t slot, const std::string& op,
                                                  const nlohmann::json& params) {
        const size_t idx = slot % clients_.size();
        return clients_[idx]->queryStep(op, params);
    }

    size_t size() const { return clients_.size(); }

private:
    std::vector<std::unique_ptr<MCPPipelineClient>> clients_;
    std::atomic<size_t> next_{0};
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
#ifdef _WIN32
    gmtime_s(&tm, &tt);
#else
    gmtime_r(&tt, &tm);
#endif
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
// Overloads accept a BenchConfig to enable plugins, real models, and custom data dirs.
DaemonHarnessOptions benchHarnessOptions() {
    return DaemonHarnessOptions{
        .enableModelProvider = false,
        .useMockModelProvider = true,
        .autoLoadPlugins = false,
        .enableAutoRepair = true,
    };
}

DaemonHarnessOptions benchHarnessOptions(const BenchConfig& cfg) {
    DaemonHarnessOptions opts{
        .enableModelProvider = cfg.enablePlugins || cfg.useRealModelProvider,
        .useMockModelProvider = !cfg.useRealModelProvider,
        .autoLoadPlugins = cfg.enablePlugins,
        .enableAutoRepair = true,
        .pluginDir = cfg.pluginDir,
        .dataDir = cfg.dataDir,
        .trustedPluginPaths = cfg.trustedPluginPaths,
    };
    return opts;
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

TEST_CASE("Multi-client ingestion: 16-client concurrent mixed ops",
          "[!benchmark][multi-client][mixed-16]") {
    // 16 clients performing list, search, status, and get simultaneously while
    // background writers are ingesting. Measures per-operation-type latency and
    // throughput under realistic concurrent access patterns.
    auto cfg = BenchConfig::fromEnv();

    constexpr int kTotalClients = 16;
    constexpr int kWriterClients = 4;
    constexpr int kSearchClients = 4;
    constexpr int kListClients = 4;
    constexpr int kStatusClients = 4;

    // Each reader client runs for a fixed number of operations
    constexpr int kOpsPerReader = 200;

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Pre-seed 100 docs so search/list have data from the start
    std::vector<std::string> knownHashes;
    {
        ClientConfig warmCfg;
        warmCfg.socketPath = harness.socketPath();
        warmCfg.autoStart = false;
        warmCfg.requestTimeout = 30s;
        DaemonClient warmClient(warmCfg);

        std::cout << "  Pre-seeding 100 warmup docs...\n";
        for (int i = 0; i < 100; ++i) {
            AddDocumentRequest req;
            req.name = "mixed16_warmup_" + std::to_string(i) + ".md";
            req.content = generateDocument(99, i, cfg.docSizeBytes);
            req.tags = {"warmup", "bench"};
            auto res = yams::cli::run_sync(warmClient.streamingAddDocument(req), 30s);
            if (res && !res.value().hash.empty()) {
                knownHashes.push_back(res.value().hash);
            }
        }
        waitForDrain(warmClient, 60s);
        std::this_thread::sleep_for(1s);
        std::cout << "  Warmup complete (" << knownHashes.size() << " hashes)\n";
    }

    // Monitor client
    ClientConfig monCfg;
    monCfg.socketPath = harness.socketPath();
    monCfg.autoStart = false;
    monCfg.requestTimeout = 10s;
    DaemonClient monitorClient(monCfg);

    TimeSeriesSampler sampler;
    sampler.start(monitorClient, 500ms);

    // Per-thread latency collectors
    std::mutex searchLatMutex, listLatMutex, statusLatMutex, getLatMutex, addLatMutex;
    std::vector<int64_t> searchLatencies, listLatencies, statusLatencies, getLatencies,
        addLatencies;

    std::atomic<int> searchOps{0}, listOps{0}, statusOps{0}, getOps{0}, addOps{0};
    std::atomic<int> searchFails{0}, listFails{0}, statusFails{0}, getFails{0}, addFails{0};

    // Barrier: all threads start at the same instant
    std::atomic<bool> go{false};

    auto globalStart = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(kTotalClients);

    // --- Writer threads (4): continuous ingestion ---
    for (int w = 0; w < kWriterClients; ++w) {
        threads.emplace_back([&, w]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 30s;
            DaemonClient client(ccfg);
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < cfg.docsPerClient; ++i) {
                AddDocumentRequest req;
                req.name = "mixed16_w" + std::to_string(w) + "_" + std::to_string(i) + ".md";
                req.content = generateDocument(w, i, cfg.docSizeBytes);
                req.tags = {"bench", "mixed16", "writer-" + std::to_string(w)};

                auto t0 = std::chrono::steady_clock::now();
                auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                if (res) {
                    addOps.fetch_add(1);
                    std::lock_guard<std::mutex> lk(addLatMutex);
                    addLatencies.push_back(latUs);
                } else {
                    addFails.fetch_add(1);
                }
            }
        });
    }

    // --- Search threads (4): continuous search queries ---
    for (int s = 0; s < kSearchClients; ++s) {
        threads.emplace_back([&]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 10s;
            DaemonClient client(ccfg);
            while (!go.load())
                std::this_thread::yield();

            static const char* queries[] = {
                "architecture performance tuning",
                "ingestion pipeline extraction",
                "resource governance memory pressure",
                "thread pool concurrency limits",
                "IPC transport protocol",
            };

            for (int i = 0; i < kOpsPerReader; ++i) {
                SearchRequest req;
                req.query = queries[i % 5];
                req.limit = 10;

                auto t0 = std::chrono::steady_clock::now();
                auto res = yams::cli::run_sync(client.search(req), 10s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                if (res) {
                    searchOps.fetch_add(1);
                    std::lock_guard<std::mutex> lk(searchLatMutex);
                    searchLatencies.push_back(latUs);
                } else {
                    searchFails.fetch_add(1);
                }
            }
        });
    }

    // --- List threads (4): continuous list queries with varying filters ---
    for (int l = 0; l < kListClients; ++l) {
        threads.emplace_back([&]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 10s;
            DaemonClient client(ccfg);
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < kOpsPerReader; ++i) {
                ListRequest req;
                req.limit = (i % 3 == 0) ? 5 : (i % 3 == 1) ? 20 : 50;
                // Alternate between filtered and unfiltered
                if (i % 4 == 0) {
                    req.tags = {"warmup"};
                }

                auto t0 = std::chrono::steady_clock::now();
                auto res = yams::cli::run_sync(client.list(req), 10s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                if (res) {
                    listOps.fetch_add(1);
                    std::lock_guard<std::mutex> lk(listLatMutex);
                    listLatencies.push_back(latUs);
                } else {
                    listFails.fetch_add(1);
                }
            }
        });
    }

    // --- Status threads (4): continuous status + get queries ---
    for (int t = 0; t < kStatusClients; ++t) {
        threads.emplace_back([&, t]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 10s;
            DaemonClient client(ccfg);
            std::mt19937 rng(static_cast<unsigned>(t * 997 + 13));
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < kOpsPerReader; ++i) {
                if (i % 3 == 0) {
                    // Status query
                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.status(), 10s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    if (res) {
                        statusOps.fetch_add(1);
                        std::lock_guard<std::mutex> lk(statusLatMutex);
                        statusLatencies.push_back(latUs);
                    } else {
                        statusFails.fetch_add(1);
                    }
                } else {
                    // Get (metadata) by hash
                    std::string hash;
                    {
                        if (!knownHashes.empty()) {
                            std::uniform_int_distribution<std::size_t> dist(0,
                                                                            knownHashes.size() - 1);
                            hash = knownHashes[dist(rng)];
                        }
                    }
                    if (hash.empty()) {
                        statusOps.fetch_add(1); // count as no-op
                        continue;
                    }

                    GetRequest req;
                    req.hash = hash;
                    req.metadataOnly = true;

                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.get(req), 10s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    if (res) {
                        getOps.fetch_add(1);
                        std::lock_guard<std::mutex> lk(getLatMutex);
                        getLatencies.push_back(latUs);
                    } else {
                        getFails.fetch_add(1);
                    }
                }
            }
        });
    }

    // Release all threads simultaneously
    go.store(true);

    for (auto& t : threads)
        t.join();

    sampler.stop();

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();

    // Drain
    bool drained = waitForDrain(monitorClient, std::chrono::seconds(cfg.drainTimeoutSecs));
    auto finalSnap = DaemonSnapshot::capture(monitorClient);

    // Compute stats
    auto searchStats = PercentileStats::compute(searchLatencies);
    auto listStats = PercentileStats::compute(listLatencies);
    auto statusStats = PercentileStats::compute(statusLatencies);
    auto getStats = PercentileStats::compute(getLatencies);
    auto addStats = PercentileStats::compute(addLatencies);

    int totalOps =
        searchOps.load() + listOps.load() + statusOps.load() + getOps.load() + addOps.load();
    int totalFails = searchFails.load() + listFails.load() + statusFails.load() + getFails.load() +
                     addFails.load();
    double opsPerSec = static_cast<double>(totalOps) / globalElapsed;

    // Print summary
    std::cout << "\n=== 16-Client Concurrent Mixed Ops ===\n";
    std::cout << "  Layout: " << kWriterClients << " writers + " << kSearchClients << " searchers"
              << " + " << kListClients << " listers + " << kStatusClients << " status/get\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    std::cout << "  Total ops: " << totalOps << " (" << std::setprecision(0) << opsPerSec
              << " ops/s)"
              << "  Failures: " << totalFails << "\n\n";

    std::cout << "  Op breakdown:\n";
    std::cout << "    Add:    " << addOps.load() << " ok, " << addFails.load() << " fail\n";
    std::cout << "    Search: " << searchOps.load() << " ok, " << searchFails.load() << " fail\n";
    std::cout << "    List:   " << listOps.load() << " ok, " << listFails.load() << " fail\n";
    std::cout << "    Status: " << statusOps.load() << " ok, " << statusFails.load() << " fail\n";
    std::cout << "    Get:    " << getOps.load() << " ok, " << getFails.load() << " fail\n\n";

    printPercentiles("Add latency   ", addStats);
    printPercentiles("Search latency", searchStats);
    printPercentiles("List latency  ", listStats);
    printPercentiles("Status latency", statusStats);
    printPercentiles("Get latency   ", getStats);

    std::cout << "\n  Queue drained: " << (drained ? "yes" : "NO") << "\n";
    std::cout << "  Final docs_total: " << finalSnap.documentsTotal << "\n";
    std::cout << "  Final memory: " << finalSnap.memoryUsageMb << " MB\n\n";

    // Key assertions: all op types must make progress
    CHECK(addOps.load() > 0);
    CHECK(searchOps.load() > 0);
    CHECK(listOps.load() > 0);
    CHECK(statusOps.load() > 0);
    CHECK(getOps.load() > 0);
    CHECK(drained);

    // Emit JSONL
    json timeSeries = json::array();
    for (auto& s : sampler.getSamples()) {
        timeSeries.push_back(json{{"elapsed_ms", s.elapsedMs}, {"snapshot", s.snap.toJson()}});
    }

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "16_client_concurrent_mixed_ops"},
        {"layout", json{{"writers", kWriterClients},
                        {"searchers", kSearchClients},
                        {"listers", kListClients},
                        {"status_get", kStatusClients}}},
        {"ops_per_reader", kOpsPerReader},
        {"docs_per_writer", cfg.docsPerClient},
        {"doc_size_bytes", cfg.docSizeBytes},
        {"total_ops", totalOps},
        {"total_failures", totalFails},
        {"elapsed_seconds", globalElapsed},
        {"ops_per_sec", opsPerSec},
        {"add",
         json{{"ops", addOps.load()}, {"fails", addFails.load()}, {"latency", addStats.toJson()}}},
        {"search", json{{"ops", searchOps.load()},
                        {"fails", searchFails.load()},
                        {"latency", searchStats.toJson()}}},
        {"list", json{{"ops", listOps.load()},
                      {"fails", listFails.load()},
                      {"latency", listStats.toJson()}}},
        {"status", json{{"ops", statusOps.load()},
                        {"fails", statusFails.load()},
                        {"latency", statusStats.toJson()}}},
        {"get",
         json{{"ops", getOps.load()}, {"fails", getFails.load()}, {"latency", getStats.toJson()}}},
        {"daemon_snapshot_final", finalSnap.toJson()},
        {"time_series", timeSeries},
        {"tuning_profile", cfg.tuningProfile},
        {"drained", drained},
    };
    emitJsonl(cfg.outputPath, record);
}

// =============================================================================
// EXISTING CORPUS BENCHMARK
// =============================================================================

TEST_CASE("Multi-client ingestion: large corpus reads",
          "[!benchmark][multi-client][large-corpus]") {
    // Instrumented benchmark against an existing YAMS corpus (e.g. /Volumes/picaso/yams
    // with 48k+ docs). Does NOT ingest — hammers search/list/get/status against the
    // real corpus to measure read-path performance. Outputs:
    //   - Per-op latency traces (every operation recorded)
    //   - Live progress with rolling throughput
    //   - Memory watermarks (peak RSS, delta over time)
    //   - Error classification breakdown
    //   - Detailed JSONL with all latency traces for post-analysis
    //
    // Env overrides:
    //   YAMS_BENCH_DATA_DIR              - required: existing corpus path
    //   YAMS_BENCH_SEARCH_CLIENTS        - search thread count (default: 4)
    //   YAMS_BENCH_LIST_CLIENTS          - list thread count (default: 4)
    //   YAMS_BENCH_STATUS_GET_CLIENTS    - status/get thread count (default: 4)
    //   YAMS_BENCH_OPS_PER_CLIENT        - ops per thread (default: 50)
    //   YAMS_BENCH_OP_TIMEOUT_S          - per-op timeout in seconds (default: 60)
    auto cfg = BenchConfig::fromEnv();
    if (!cfg.dataDir) {
        SKIP("Set YAMS_BENCH_DATA_DIR to an existing YAMS corpus to run this benchmark");
    }

    // --- Configurable layout ---
    auto envInt = [](const char* name, int def) {
        if (auto* v = std::getenv(name))
            return std::max(1, std::atoi(v));
        return def;
    };
    const int kSearchClients = envInt("YAMS_BENCH_SEARCH_CLIENTS", 4);
    const int kListClients = envInt("YAMS_BENCH_LIST_CLIENTS", 4);
    const int kStatusGetClients = envInt("YAMS_BENCH_STATUS_GET_CLIENTS", 4);
    const int kTotalClients = kSearchClients + kListClients + kStatusGetClients;
    const int kOpsPerClient = envInt("YAMS_BENCH_OPS_PER_CLIENT", 50);
    const int kOpTimeoutS = envInt("YAMS_BENCH_OP_TIMEOUT_S", 60);
    const auto kOpTimeout = std::chrono::seconds(kOpTimeoutS);
    const bool useMcpPath = cfg.useMcpPath;
    const size_t mcpPoolSize = static_cast<size_t>(
        std::max(1, envInt("YAMS_BENCH_MCP_POOL_SIZE", std::min(kTotalClients, 8))));

    std::cout << "\n=== Large Corpus Read Benchmark (instrumented) ===\n";
    std::cout << "  Data dir:       " << cfg.dataDir->string() << "\n";
    std::cout << "  Layout:         " << kSearchClients << "S + " << kListClients << "L + "
              << kStatusGetClients << "G = " << kTotalClients << " threads\n";
    std::cout << "  Ops/thread:     " << kOpsPerClient << "\n";
    std::cout << "  Op timeout:     " << kOpTimeoutS << "s\n";
    std::cout << "  Transport path: " << (useMcpPath ? "mcp(query-or-direct-tools)" : "daemon-ipc")
              << "\n";
    if (useMcpPath) {
        std::cout << "  MCP pool size:  " << mcpPoolSize << "\n";
    }

    // --- Server-side timeout configuration ---
    // Stay within TuneAdvisor range clamps:
    //   ipcTimeoutMs:         [500, 600000]
    //   streamChunkTimeoutMs: [1000, 600000]
    //   searchBuildTimeout:   unclamped
    std::string timeoutStr = std::to_string(std::min(kOpTimeoutS * 2000, 600000));
#ifdef _WIN32
    if (!std::getenv("YAMS_SEARCH_BUILD_TIMEOUT_MS"))
        _putenv_s("YAMS_SEARCH_BUILD_TIMEOUT_MS", "120000");
    if (!std::getenv("YAMS_IPC_TIMEOUT_MS"))
        _putenv_s("YAMS_IPC_TIMEOUT_MS", timeoutStr.c_str());
    if (!std::getenv("YAMS_STREAM_CHUNK_TIMEOUT_MS"))
        _putenv_s("YAMS_STREAM_CHUNK_TIMEOUT_MS", timeoutStr.c_str());
#else
    if (!std::getenv("YAMS_SEARCH_BUILD_TIMEOUT_MS"))
        ::setenv("YAMS_SEARCH_BUILD_TIMEOUT_MS", "120000", 0);
    if (!std::getenv("YAMS_IPC_TIMEOUT_MS"))
        ::setenv("YAMS_IPC_TIMEOUT_MS", timeoutStr.c_str(), 0);
    if (!std::getenv("YAMS_STREAM_CHUNK_TIMEOUT_MS"))
        ::setenv("YAMS_STREAM_CHUNK_TIMEOUT_MS", timeoutStr.c_str(), 0);
#endif

    const char* ipcTimeoutEnv = std::getenv("YAMS_IPC_TIMEOUT_MS");
    const char* streamTimeoutEnv = std::getenv("YAMS_STREAM_CHUNK_TIMEOUT_MS");
    std::cout << "  IPC timeout:    " << (ipcTimeoutEnv ? ipcTimeoutEnv : "default") << " ms\n";
    std::cout << "  Stream timeout: " << (streamTimeoutEnv ? streamTimeoutEnv : "default")
              << " ms\n";

    // --- Start daemon ---
    auto opts = benchHarnessOptions(cfg);
    DaemonHarness harness(opts);
    REQUIRE(harness.start(std::chrono::seconds(300)));
    std::this_thread::sleep_for(3s); // let async init settle

    // Helper: create a client config with all timeouts aligned to kOpTimeout.
    // The default headerTimeout (30s) was causing cascading 30s stalls because
    // the client would time out waiting for the first response frame header.
    auto makeClientConfig = [&](std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) {
        ClientConfig c;
        c.socketPath = harness.socketPath();
        c.autoStart = false;
        auto t = timeout.count() > 0 ? timeout : kOpTimeout;
        c.requestTimeout = t;
        c.headerTimeout = t;
        c.bodyTimeout = t;
        return c;
    };

    // --- Discover corpus ---
    auto discoverCfg = makeClientConfig();
    DaemonClient discoverClient(discoverCfg);
    std::shared_ptr<MCPPipelineClientPool> sharedMcpPool;
    if (useMcpPath) {
        sharedMcpPool = std::make_shared<MCPPipelineClientPool>(harness.socketPath(), mcpPoolSize);
    }

    std::vector<std::string> knownHashes;
    {
        if (useMcpPath) {
            auto listRes = sharedMcpPool->queryStepForSlot(0, "list", {{"limit", 200}});
            if (listRes && listRes.value().contains("items") &&
                listRes.value()["items"].is_array()) {
                for (const auto& item : listRes.value()["items"]) {
                    if (item.contains("hash") && item["hash"].is_string()) {
                        auto hash = item["hash"].get<std::string>();
                        if (!hash.empty())
                            knownHashes.push_back(hash);
                    }
                }
            }
        } else {
            ListRequest listReq;
            listReq.limit = 200;
            auto listRes = yams::cli::run_sync(discoverClient.list(listReq), kOpTimeout);
            if (listRes) {
                for (auto& item : listRes.value().items) {
                    if (!item.hash.empty())
                        knownHashes.push_back(item.hash);
                }
            }
        }
    }

    auto initSnap = DaemonSnapshot::capture(discoverClient);
    std::cout << "  Corpus docs:    " << initSnap.documentsTotal << "\n";
    std::cout << "  Known hashes:   " << knownHashes.size() << "\n";
    std::cout << "  Initial RSS:    " << initSnap.memoryUsageMb << " MB\n";

    if (knownHashes.empty()) {
        WARN("Corpus appears empty — benchmark results may be unreliable");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 1: Single-client warmup — baseline latency without contention
    // ─────────────────────────────────────────────────────────────────────────
    std::cout << "\n--- Phase 1: Single-client warmup (5 ops each) ---\n";
    {
        auto warmCfg = makeClientConfig();
        DaemonClient warmClient(warmCfg);

        // Warm search
        for (int i = 0; i < 5; ++i) {
            auto t0 = std::chrono::steady_clock::now();
            yams::Result<nlohmann::json> res;
            if (useMcpPath) {
                res = sharedMcpPool->queryStepForSlot(
                    0, "search", {{"query", "architecture design"}, {"limit", 10}});
            } else {
                SearchRequest req;
                req.query = "architecture design";
                req.limit = 10;
                auto direct = yams::cli::run_sync(warmClient.search(req), kOpTimeout);
                if (direct) {
                    nlohmann::json j;
                    j["results"] = nlohmann::json::array();
                    for (const auto& r : direct.value().results) {
                        j["results"].push_back({{"id", r.id}, {"path", r.path}});
                    }
                    res = j;
                } else {
                    res = yams::Error{direct.error()};
                }
            }
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            std::cout << "    search[" << i << "]: " << us / 1000 << "ms" << (res ? "" : " FAIL")
                      << "\n";
        }
        // Warm list
        for (int i = 0; i < 5; ++i) {
            auto t0 = std::chrono::steady_clock::now();
            yams::Result<nlohmann::json> res;
            int items = 0;
            if (useMcpPath) {
                res = sharedMcpPool->queryStepForSlot(0, "list", {{"limit", 50}});
                if (res && res.value().contains("items") && res.value()["items"].is_array()) {
                    items = static_cast<int>(res.value()["items"].size());
                }
            } else {
                ListRequest req;
                req.limit = 50;
                auto direct = yams::cli::run_sync(warmClient.list(req), kOpTimeout);
                if (direct) {
                    items = static_cast<int>(direct.value().items.size());
                    nlohmann::json j;
                    j["items"] = nlohmann::json::array();
                    res = j;
                } else {
                    res = yams::Error{direct.error()};
                }
            }
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now() - t0)
                          .count();
            std::cout << "    list[" << i << "]:   " << us / 1000 << "ms (" << items << " items)"
                      << (res ? "" : " FAIL") << "\n";
        }
        // Warm get
        if (!knownHashes.empty()) {
            for (int i = 0; i < 5 && i < static_cast<int>(knownHashes.size()); ++i) {
                GetRequest req;
                req.hash = knownHashes[i];
                req.metadataOnly = true;
                auto t0 = std::chrono::steady_clock::now();
                yams::Result<nlohmann::json> res;
                if (useMcpPath) {
                    res = sharedMcpPool->queryStepForSlot(
                        0, "get", {{"hash", knownHashes[i]}, {"include_content", false}});
                } else {
                    auto direct = yams::cli::run_sync(warmClient.get(req), kOpTimeout);
                    if (direct) {
                        res = nlohmann::json::object();
                    } else {
                        res = yams::Error{direct.error()};
                    }
                }
                auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                              std::chrono::steady_clock::now() - t0)
                              .count();
                std::cout << "    get[" << i << "]:    " << us / 1000 << "ms"
                          << (res ? "" : " FAIL") << "\n";
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Phase 2: Concurrent load with per-op tracing
    // ─────────────────────────────────────────────────────────────────────────
    std::cout << "\n--- Phase 2: Concurrent load (" << kTotalClients << " threads x "
              << kOpsPerClient << " ops) ---\n";

    // Per-op trace record
    struct OpTrace {
        std::string opType; // "search", "list", "get", "status"
        int threadId{0};
        int opIndex{0};
        int64_t latencyUs{0};
        bool success{false};
        std::string errorMsg;
        int resultCount{0};     // items returned (search hits, list items)
        int64_t wallClockMs{0}; // ms since benchmark start
    };

    // Monitor with sampler
    auto monCfg = makeClientConfig(10s);
    DaemonClient monitorClient(monCfg);

    TimeSeriesSampler sampler;
    sampler.start(monitorClient, 1s);

    // Per-op trace collectors (one per thread for lock-free append)
    std::vector<std::vector<OpTrace>> perThreadTraces(kTotalClients);

    // Shared counters
    std::atomic<int> searchOps{0}, listOps{0}, statusOps{0}, getOps{0};
    std::atomic<int> searchFails{0}, listFails{0}, statusFails{0}, getFails{0};
    std::atomic<int> totalCompleted{0};
    const int totalExpected = kTotalClients * kOpsPerClient;

    // Live progress reporter
    std::atomic<bool> progressStop{false};
    auto progressStart = std::chrono::steady_clock::now();
    std::thread progressThread([&]() {
        int lastCompleted = 0;
        while (!progressStop.load()) {
            std::this_thread::sleep_for(2s);
            int cur = totalCompleted.load();
            auto elapsed =
                std::chrono::duration<double>(std::chrono::steady_clock::now() - progressStart)
                    .count();
            double rate = cur > 0 ? cur / elapsed : 0.0;
            int delta = cur - lastCompleted;
            lastCompleted = cur;
            auto snap = DaemonSnapshot::capture(monitorClient);
            std::cout << "  [" << std::fixed << std::setprecision(1) << elapsed << "s] " << cur
                      << "/" << totalExpected << " ops (" << std::setprecision(1) << rate
                      << " ops/s, +" << delta << ") "
                      << "RSS=" << std::setprecision(0) << snap.memoryUsageMb << "MB "
                      << "conns=" << snap.activeConnections << "/" << snap.maxConnections << " "
                      << "pressure=" << snap.pressureLevel << "\n";
        }
    });

    std::atomic<bool> go{false};
    auto globalStart = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(kTotalClients);

    // Search queries
    static const char* corpusQueries[] = {
        "architecture design patterns", "security vulnerability analysis",
        "memory management allocation", "network protocol implementation",
        "database query optimization",  "concurrent thread synchronization",
        "error handling recovery",      "configuration file parsing",
        "build system compilation",     "testing framework assertions",
    };

    int threadIdx = 0;

    // --- Search threads ---
    for (int s = 0; s < kSearchClients; ++s) {
        int tid = threadIdx++;
        perThreadTraces[tid].reserve(kOpsPerClient);
        threads.emplace_back([&, tid, s]() {
            std::optional<DaemonClient> client;
            if (!useMcpPath) {
                auto ccfg = makeClientConfig();
                client.emplace(ccfg);
            }
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < kOpsPerClient; ++i) {
                SearchRequest req;
                req.query = corpusQueries[(s * kOpsPerClient + i) % 10];
                req.limit = (i % 3 == 0) ? 5 : (i % 3 == 1) ? 10 : 25;

                auto t0 = std::chrono::steady_clock::now();
                yams::Result<nlohmann::json> res;
                if (useMcpPath) {
                    res = sharedMcpPool->queryStepForSlot(
                        static_cast<size_t>(tid), "search",
                        {{"query", req.query}, {"limit", req.limit}});
                } else {
                    auto direct = yams::cli::run_sync(client->search(req), kOpTimeout);
                    if (direct) {
                        nlohmann::json j;
                        j["results"] = nlohmann::json::array();
                        for (const auto& r : direct.value().results) {
                            j["results"].push_back({{"id", r.id}});
                        }
                        res = j;
                    } else {
                        res = yams::Error{direct.error()};
                    }
                }
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                auto wallMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(t1 - globalStart).count();

                OpTrace trace;
                trace.opType = "search";
                trace.threadId = tid;
                trace.opIndex = i;
                trace.latencyUs = latUs;
                trace.wallClockMs = wallMs;
                if (res) {
                    trace.success = true;
                    if (res.value().contains("results") && res.value()["results"].is_array()) {
                        trace.resultCount = static_cast<int>(res.value()["results"].size());
                    }
                    searchOps.fetch_add(1);
                } else {
                    trace.success = false;
                    trace.errorMsg = res.error().message;
                    searchFails.fetch_add(1);
                }
                perThreadTraces[tid].push_back(std::move(trace));
                totalCompleted.fetch_add(1);
            }
        });
    }

    // --- List threads ---
    for (int l = 0; l < kListClients; ++l) {
        int tid = threadIdx++;
        perThreadTraces[tid].reserve(kOpsPerClient);
        threads.emplace_back([&, tid]() {
            std::optional<DaemonClient> client;
            if (!useMcpPath) {
                auto ccfg = makeClientConfig();
                client.emplace(ccfg);
            }
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < kOpsPerClient; ++i) {
                ListRequest req;
                req.limit = (i % 4 == 0) ? 10 : (i % 4 == 1) ? 25 : (i % 4 == 2) ? 50 : 100;

                auto t0 = std::chrono::steady_clock::now();
                yams::Result<nlohmann::json> res;
                if (useMcpPath) {
                    res = sharedMcpPool->queryStepForSlot(static_cast<size_t>(tid), "list",
                                                          {{"limit", req.limit}});
                } else {
                    auto direct = yams::cli::run_sync(client->list(req), kOpTimeout);
                    if (direct) {
                        nlohmann::json j;
                        j["items"] = nlohmann::json::array();
                        for (const auto& it : direct.value().items) {
                            j["items"].push_back({{"hash", it.hash}});
                        }
                        res = j;
                    } else {
                        res = yams::Error{direct.error()};
                    }
                }
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                auto wallMs =
                    std::chrono::duration_cast<std::chrono::milliseconds>(t1 - globalStart).count();

                OpTrace trace;
                trace.opType = "list";
                trace.threadId = tid;
                trace.opIndex = i;
                trace.latencyUs = latUs;
                trace.wallClockMs = wallMs;
                if (res) {
                    trace.success = true;
                    if (res.value().contains("items") && res.value()["items"].is_array()) {
                        trace.resultCount = static_cast<int>(res.value()["items"].size());
                    }
                    listOps.fetch_add(1);
                } else {
                    trace.success = false;
                    trace.errorMsg = res.error().message;
                    listFails.fetch_add(1);
                }
                perThreadTraces[tid].push_back(std::move(trace));
                totalCompleted.fetch_add(1);
            }
        });
    }

    // --- Status/Get threads ---
    for (int g = 0; g < kStatusGetClients; ++g) {
        int tid = threadIdx++;
        perThreadTraces[tid].reserve(kOpsPerClient);
        threads.emplace_back([&, tid, g]() {
            std::optional<DaemonClient> client;
            if (!useMcpPath) {
                auto ccfg = makeClientConfig();
                client.emplace(ccfg);
            }
            std::mt19937 rng(static_cast<unsigned>(g * 997 + 17));
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < kOpsPerClient; ++i) {
                auto t0 = std::chrono::steady_clock::now();
                bool isStatus = (i % 4 == 0) || knownHashes.empty();

                if (isStatus) {
                    yams::Result<nlohmann::json> res;
                    if (useMcpPath) {
                        res = sharedMcpPool->queryStepForSlot(static_cast<size_t>(tid), "status",
                                                              nlohmann::json::object());
                    } else {
                        auto direct = yams::cli::run_sync(client->status(), kOpTimeout);
                        if (direct) {
                            res = nlohmann::json::object();
                        } else {
                            res = yams::Error{direct.error()};
                        }
                    }
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                    auto wallMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - globalStart)
                            .count();

                    OpTrace trace;
                    trace.opType = "status";
                    trace.threadId = tid;
                    trace.opIndex = i;
                    trace.latencyUs = latUs;
                    trace.wallClockMs = wallMs;
                    if (res) {
                        trace.success = true;
                        statusOps.fetch_add(1);
                    } else {
                        trace.success = false;
                        trace.errorMsg = res.error().message;
                        statusFails.fetch_add(1);
                    }
                    perThreadTraces[tid].push_back(std::move(trace));
                } else {
                    std::uniform_int_distribution<std::size_t> dist(0, knownHashes.size() - 1);
                    std::string hash = knownHashes[dist(rng)];
                    yams::Result<nlohmann::json> res;
                    if (useMcpPath) {
                        res = sharedMcpPool->queryStepForSlot(
                            static_cast<size_t>(tid), "get",
                            {{"hash", hash}, {"include_content", false}});
                    } else {
                        GetRequest req;
                        req.hash = hash;
                        req.metadataOnly = true;
                        auto direct = yams::cli::run_sync(client->get(req), kOpTimeout);
                        if (direct) {
                            res = nlohmann::json::object();
                        } else {
                            res = yams::Error{direct.error()};
                        }
                    }
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                    auto wallMs =
                        std::chrono::duration_cast<std::chrono::milliseconds>(t1 - globalStart)
                            .count();

                    OpTrace trace;
                    trace.opType = "get";
                    trace.threadId = tid;
                    trace.opIndex = i;
                    trace.latencyUs = latUs;
                    trace.wallClockMs = wallMs;
                    if (res) {
                        trace.success = true;
                        getOps.fetch_add(1);
                    } else {
                        trace.success = false;
                        trace.errorMsg = res.error().message;
                        getFails.fetch_add(1);
                    }
                    perThreadTraces[tid].push_back(std::move(trace));
                }
                totalCompleted.fetch_add(1);
            }
        });
    }

    go.store(true);
    for (auto& t : threads)
        t.join();

    progressStop.store(true);
    progressThread.join();
    sampler.stop();

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();

    // ─────────────────────────────────────────────────────────────────────────
    // Analysis
    // ─────────────────────────────────────────────────────────────────────────
    auto finalSnap = DaemonSnapshot::capture(monitorClient);

    // Aggregate latency vectors from per-thread traces
    std::vector<int64_t> allSearchLat, allListLat, allStatusLat, allGetLat;
    std::vector<OpTrace> allTraces;
    std::map<std::string, int> errorCategories;

    for (auto& threadTraces : perThreadTraces) {
        for (auto& tr : threadTraces) {
            allTraces.push_back(tr);
            if (tr.success) {
                if (tr.opType == "search")
                    allSearchLat.push_back(tr.latencyUs);
                else if (tr.opType == "list")
                    allListLat.push_back(tr.latencyUs);
                else if (tr.opType == "status")
                    allStatusLat.push_back(tr.latencyUs);
                else if (tr.opType == "get")
                    allGetLat.push_back(tr.latencyUs);
            } else {
                errorCategories[classifyFailureMessage(tr.errorMsg)]++;
            }
        }
    }

    auto searchStats = PercentileStats::compute(allSearchLat);
    auto listStats = PercentileStats::compute(allListLat);
    auto statusStats = PercentileStats::compute(allStatusLat);
    auto getStats = PercentileStats::compute(allGetLat);

    int totalOps = searchOps.load() + listOps.load() + statusOps.load() + getOps.load();
    int totalFails = searchFails.load() + listFails.load() + statusFails.load() + getFails.load();
    double opsPerSec = totalOps > 0 ? totalOps / globalElapsed : 0.0;

    // Memory watermarks from time series
    auto samples = sampler.getSamples();
    double peakRss = 0.0, minRss = std::numeric_limits<double>::max();
    for (auto& s : samples) {
        peakRss = std::max(peakRss, s.snap.memoryUsageMb);
        minRss = std::min(minRss, s.snap.memoryUsageMb);
    }
    if (samples.empty())
        minRss = 0.0;

    std::cout << "\n=== Large Corpus Results (instrumented) ===\n";
    std::cout << "  Corpus:         " << finalSnap.documentsTotal << " docs\n";
    std::cout << "  Layout:         " << kSearchClients << "S + " << kListClients << "L + "
              << kStatusGetClients << "G\n";
    std::cout << "  Wall time:      " << std::fixed << std::setprecision(2) << globalElapsed
              << "s\n";
    std::cout << "  Total ops:      " << totalOps << " (" << std::setprecision(1) << opsPerSec
              << " ops/s)\n";
    std::cout << "  Total failures: " << totalFails << "\n\n";

    std::cout << "  Op breakdown:\n";
    std::cout << "    Search: " << searchOps.load() << " ok, " << searchFails.load() << " fail\n";
    std::cout << "    List:   " << listOps.load() << " ok, " << listFails.load() << " fail\n";
    std::cout << "    Status: " << statusOps.load() << " ok, " << statusFails.load() << " fail\n";
    std::cout << "    Get:    " << getOps.load() << " ok, " << getFails.load() << " fail\n\n";

    printPercentiles("Search latency", searchStats);
    printPercentiles("List latency  ", listStats);
    printPercentiles("Status latency", statusStats);
    printPercentiles("Get latency   ", getStats);

    std::cout << "\n  Memory:\n";
    std::cout << "    Initial: " << std::setprecision(0) << initSnap.memoryUsageMb << " MB\n";
    std::cout << "    Peak:    " << peakRss << " MB\n";
    std::cout << "    Min:     " << minRss << " MB\n";
    std::cout << "    Final:   " << finalSnap.memoryUsageMb << " MB\n";
    std::cout << "    Delta:   " << (finalSnap.memoryUsageMb - initSnap.memoryUsageMb) << " MB\n\n";

    if (!errorCategories.empty()) {
        std::cout << "  Error categories:\n";
        for (auto& [cat, count] : errorCategories) {
            std::cout << "    " << cat << ": " << count << "\n";
        }
        std::cout << "\n";
    }

    // Hotspot analysis: find slowest ops
    std::sort(allTraces.begin(), allTraces.end(),
              [](const OpTrace& a, const OpTrace& b) { return a.latencyUs > b.latencyUs; });

    std::cout << "  Top 10 slowest operations:\n";
    for (int i = 0; i < 10 && i < static_cast<int>(allTraces.size()); ++i) {
        auto& tr = allTraces[i];
        std::cout << "    " << (i + 1) << ". " << tr.opType << " t" << tr.threadId << "#"
                  << tr.opIndex << ": " << tr.latencyUs / 1000 << "ms"
                  << (tr.success ? "" : " FAIL:" + tr.errorMsg) << " @" << tr.wallClockMs << "ms\n";
    }
    std::cout << "\n";

    // Latency distribution (histogram buckets)
    auto printHistogram = [](const std::string& label, std::vector<int64_t>& lats) {
        if (lats.empty())
            return;
        std::sort(lats.begin(), lats.end());
        // Buckets: <1ms, 1-10ms, 10-100ms, 100ms-1s, 1-10s, >10s
        int b0 = 0, b1 = 0, b2 = 0, b3 = 0, b4 = 0, b5 = 0;
        for (auto us : lats) {
            if (us < 1000)
                b0++;
            else if (us < 10000)
                b1++;
            else if (us < 100000)
                b2++;
            else if (us < 1000000)
                b3++;
            else if (us < 10000000)
                b4++;
            else
                b5++;
        }
        std::cout << "    " << label << " histogram: <1ms=" << b0 << " 1-10ms=" << b1
                  << " 10-100ms=" << b2 << " 100ms-1s=" << b3 << " 1-10s=" << b4 << " >10s=" << b5
                  << "\n";
    };

    std::cout << "  Latency distribution:\n";
    printHistogram("Search", allSearchLat);
    printHistogram("List  ", allListLat);
    printHistogram("Status", allStatusLat);
    printHistogram("Get   ", allGetLat);
    std::cout << "\n";

    CHECK(totalOps > 0);
    // Soft-fail on errors — we want the data even if some ops time out
    if (totalFails > 0) {
        WARN("Had " << totalFails << " failures out of " << totalExpected << " total ops");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // JSONL output with full traces
    // ─────────────────────────────────────────────────────────────────────────
    json timeSeries = json::array();
    for (auto& s : samples) {
        timeSeries.push_back(json{{"elapsed_ms", s.elapsedMs}, {"snapshot", s.snap.toJson()}});
    }

    // Per-op latency trace array (sorted by wall clock)
    std::sort(allTraces.begin(), allTraces.end(),
              [](const OpTrace& a, const OpTrace& b) { return a.wallClockMs < b.wallClockMs; });
    json traceArray = json::array();
    for (auto& tr : allTraces) {
        json j{{"op", tr.opType},          {"tid", tr.threadId}, {"idx", tr.opIndex},
               {"lat_us", tr.latencyUs},   {"ok", tr.success},   {"items", tr.resultCount},
               {"wall_ms", tr.wallClockMs}};
        if (!tr.errorMsg.empty())
            j["error"] = tr.errorMsg;
        traceArray.push_back(std::move(j));
    }

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "large_corpus_reads"},
        {"request_path", useMcpPath ? "mcp_query" : "daemon_ipc"},
        {"data_dir", cfg.dataDir->string()},
        {"corpus_docs", finalSnap.documentsTotal},
        {"layout", json{{"search_clients", kSearchClients},
                        {"list_clients", kListClients},
                        {"status_get_clients", kStatusGetClients},
                        {"ops_per_client", kOpsPerClient},
                        {"op_timeout_s", kOpTimeoutS}}},
        {"total_ops", totalOps},
        {"total_failures", totalFails},
        {"elapsed_seconds", globalElapsed},
        {"ops_per_sec", opsPerSec},
        {"search", json{{"ops", searchOps.load()},
                        {"fails", searchFails.load()},
                        {"latency", searchStats.toJson()}}},
        {"list", json{{"ops", listOps.load()},
                      {"fails", listFails.load()},
                      {"latency", listStats.toJson()}}},
        {"status", json{{"ops", statusOps.load()},
                        {"fails", statusFails.load()},
                        {"latency", statusStats.toJson()}}},
        {"get",
         json{{"ops", getOps.load()}, {"fails", getFails.load()}, {"latency", getStats.toJson()}}},
        {"memory", json{{"initial_mb", initSnap.memoryUsageMb},
                        {"peak_mb", peakRss},
                        {"min_mb", minRss},
                        {"final_mb", finalSnap.memoryUsageMb},
                        {"delta_mb", finalSnap.memoryUsageMb - initSnap.memoryUsageMb}}},
        {"error_categories", errorCategories},
        {"daemon_snapshot_final", finalSnap.toJson()},
        {"time_series", timeSeries},
        {"op_traces", traceArray},
        {"tuning_profile", cfg.tuningProfile},
    };
    emitJsonl(cfg.outputPath, record);

    std::cout << "  Results written to: " << cfg.outputPath.string() << "\n\n";
}

// =============================================================================
// EMBEDDINGS-ENABLED BENCHMARK
// =============================================================================

TEST_CASE("Multi-client ingestion: embeddings pipeline", "[!benchmark][multi-client][embeddings]") {
    // Full ingestion benchmark with real plugins loaded: ONNX embeddings,
    // symbol extraction, entity recognition. Measures the complete pipeline
    // including embedding generation which is the most expensive stage.
    //
    // Requires either:
    //   YAMS_BENCH_ENABLE_PLUGINS=1 with plugins and models available, or
    //   YAMS_BENCH_DATA_DIR pointing to a corpus (can combine both for
    //   ingest-into-existing-corpus testing).
    auto cfg = BenchConfig::fromEnv();
    if (!cfg.enablePlugins) {
        SKIP("Set YAMS_BENCH_ENABLE_PLUGINS=1 to run the embeddings benchmark");
    }

    std::cout << "\n=== Embeddings Pipeline Benchmark ===\n";
    std::cout << "  Plugins: enabled\n";
    std::cout << "  Real model provider: " << (cfg.useRealModelProvider ? "yes" : "mock") << "\n";
    if (cfg.pluginDir)
        std::cout << "  Plugin dir: " << cfg.pluginDir->string() << "\n";
    if (cfg.dataDir)
        std::cout << "  Data dir: " << cfg.dataDir->string() << "\n";

    constexpr int kWriterClients = 4;
    constexpr int kSearchClients = 4;
    constexpr int kTotalClients = kWriterClients + kSearchClients;

    auto opts = benchHarnessOptions(cfg);
    DaemonHarness harness(opts);
    // Plugin loading + model warmup needs extra time
    REQUIRE(harness.start(std::chrono::seconds(120)));

    // Allow async init to complete (plugin loading, model warmup, connection pools)
    std::this_thread::sleep_for(5s);

    // Monitor
    ClientConfig monCfg;
    monCfg.socketPath = harness.socketPath();
    monCfg.autoStart = false;
    monCfg.requestTimeout = 30s;
    DaemonClient monitorClient(monCfg);

    auto initSnap = DaemonSnapshot::capture(monitorClient);
    std::cout << "  Initial docs: " << initSnap.documentsTotal << "\n";
    std::cout << "  Docs per writer: " << cfg.docsPerClient << "\n";

    TimeSeriesSampler sampler;
    sampler.start(monitorClient, 1s);

    // Latency collectors
    std::mutex addLatMutex, searchLatMutex;
    std::vector<int64_t> addLatencies, searchLatencies;
    std::atomic<int> addOps{0}, searchOps{0};
    std::atomic<int> addFails{0}, searchFails{0};

    // Collect hashes for search/get during ingest
    std::mutex hashMutex;
    std::vector<std::string> ingestedHashes;

    std::atomic<bool> go{false};
    std::atomic<bool> writersFinished{false};
    auto globalStart = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    threads.reserve(kTotalClients);

    // Writer threads: ingest with embeddings pipeline active
    for (int w = 0; w < kWriterClients; ++w) {
        threads.emplace_back([&, w]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 60s; // embedding can be slow
            DaemonClient client(ccfg);
            while (!go.load())
                std::this_thread::yield();

            for (int i = 0; i < cfg.docsPerClient; ++i) {
                AddDocumentRequest req;
                req.name = "embed_w" + std::to_string(w) + "_" + std::to_string(i) + ".md";
                req.content = generateDocument(w, i, cfg.docSizeBytes);
                req.tags = {"bench", "embeddings", "writer-" + std::to_string(w)};

                auto t0 = std::chrono::steady_clock::now();
                auto res = yams::cli::run_sync(client.streamingAddDocument(req), 60s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                if (res && !res.value().hash.empty()) {
                    addOps.fetch_add(1);
                    {
                        std::lock_guard<std::mutex> lk(addLatMutex);
                        addLatencies.push_back(latUs);
                    }
                    {
                        std::lock_guard<std::mutex> lk(hashMutex);
                        ingestedHashes.push_back(res.value().hash);
                    }
                } else {
                    addFails.fetch_add(1);
                }
            }
        });
    }

    // Search threads: query while ingest + embeddings are running
    for (int s = 0; s < kSearchClients; ++s) {
        threads.emplace_back([&]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 30s;
            DaemonClient client(ccfg);
            while (!go.load())
                std::this_thread::yield();

            static const char* queries[] = {
                "architecture design",       "performance tuning parameters",
                "ingestion pipeline stages", "resource governance pressure",
                "IPC transport protocol",
            };

            // Keep searching until writers finish
            int i = 0;
            while (!writersFinished.load()) {
                SearchRequest req;
                req.query = queries[i % 5];
                req.limit = 10;

                auto t0 = std::chrono::steady_clock::now();
                auto res = yams::cli::run_sync(client.search(req), 30s);
                auto t1 = std::chrono::steady_clock::now();
                auto latUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                if (res) {
                    searchOps.fetch_add(1);
                    std::lock_guard<std::mutex> lk(searchLatMutex);
                    searchLatencies.push_back(latUs);
                } else {
                    searchFails.fetch_add(1);
                }
                ++i;
            }
        });
    }

    go.store(true);

    // Wait for writers, then signal searchers to stop
    for (int w = 0; w < kWriterClients; ++w)
        threads[w].join();
    writersFinished.store(true);

    // Join searchers
    for (int s = kWriterClients; s < kTotalClients; ++s)
        threads[s].join();

    sampler.stop();
    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();

    // Wait for embeddings pipeline to drain
    bool drained =
        waitForDrain(monitorClient, std::chrono::seconds(std::max(cfg.drainTimeoutSecs, 300)));
    auto finalSnap = DaemonSnapshot::capture(monitorClient);

    auto addStats = PercentileStats::compute(addLatencies);
    auto searchStats = PercentileStats::compute(searchLatencies);

    int totalOps = addOps.load() + searchOps.load();
    int totalFails = addFails.load() + searchFails.load();
    double addThroughput = addOps.load() > 0 ? addOps.load() / globalElapsed : 0.0;
    double searchThroughput = searchOps.load() > 0 ? searchOps.load() / globalElapsed : 0.0;

    std::cout << "\n=== Embeddings Pipeline Results ===\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    std::cout << "  Total ops: " << totalOps << "  Failures: " << totalFails << "\n\n";

    std::cout << "  Ingest:\n";
    std::cout << "    Added: " << addOps.load() << " (" << std::setprecision(1) << addThroughput
              << " docs/s)  Fails: " << addFails.load() << "\n";
    printPercentiles("    Add latency   ", addStats);

    std::cout << "\n  Search (concurrent with ingest):\n";
    std::cout << "    Queries: " << searchOps.load() << " (" << std::setprecision(1)
              << searchThroughput << " q/s)  Fails: " << searchFails.load() << "\n";
    printPercentiles("    Search latency", searchStats);

    std::cout << "\n  Pipeline:\n";
    std::cout << "    Queue drained: " << (drained ? "yes" : "NO") << "\n";
    std::cout << "    Final docs_total: " << finalSnap.documentsTotal << "\n";
    std::cout << "    Final memory: " << finalSnap.memoryUsageMb << " MB\n\n";

    CHECK(addOps.load() > 0);
    CHECK(drained);

    // Emit JSONL
    json timeSeries = json::array();
    for (auto& s : sampler.getSamples()) {
        timeSeries.push_back(json{{"elapsed_ms", s.elapsedMs}, {"snapshot", s.snap.toJson()}});
    }

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "embeddings_pipeline"},
        {"plugins_enabled", cfg.enablePlugins},
        {"real_model_provider", cfg.useRealModelProvider},
        {"data_dir", cfg.dataDir ? cfg.dataDir->string() : ""},
        {"plugin_dir", cfg.pluginDir ? cfg.pluginDir->string() : ""},
        {"writers", kWriterClients},
        {"searchers", kSearchClients},
        {"docs_per_writer", cfg.docsPerClient},
        {"doc_size_bytes", cfg.docSizeBytes},
        {"add", json{{"ops", addOps.load()},
                     {"fails", addFails.load()},
                     {"throughput_per_sec", addThroughput},
                     {"latency", addStats.toJson()}}},
        {"search", json{{"ops", searchOps.load()},
                        {"fails", searchFails.load()},
                        {"throughput_per_sec", searchThroughput},
                        {"latency", searchStats.toJson()}}},
        {"elapsed_seconds", globalElapsed},
        {"drained", drained},
        {"daemon_snapshot_final", finalSnap.toJson()},
        {"time_series", timeSeries},
        {"tuning_profile", cfg.tuningProfile},
    };
    emitJsonl(cfg.outputPath, record);
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

TEST_CASE("Multi-client ingestion: file access under ingest load",
          "[!benchmark][multi-client][access]") {
    // Validate get/cat responsiveness while concurrent ingestion is running.
    auto cfg = BenchConfig::fromEnv();

    DaemonHarness harness(benchHarnessOptions());
    REQUIRE(harness.start(kStartTimeout));

    // Seed a baseline corpus so readers always have accessible hashes.
    std::vector<std::string> knownHashes;
    std::mutex knownHashesMutex;
    {
        ClientConfig warmCfg;
        warmCfg.socketPath = harness.socketPath();
        warmCfg.autoStart = false;
        warmCfg.requestTimeout = 30s;
        DaemonClient warmClient(warmCfg);

        for (int i = 0; i < 64; ++i) {
            AddDocumentRequest req;
            req.name = "access_preseed_" + std::to_string(i) + ".md";
            req.content = generateDocument(700, i, cfg.docSizeBytes);
            req.tags = {"bench", "access-preseed"};
            auto res = yams::cli::run_sync(warmClient.streamingAddDocument(req), 30s);
            if (res && !res.value().hash.empty()) {
                std::lock_guard<std::mutex> lk(knownHashesMutex);
                knownHashes.push_back(res.value().hash);
            }
        }
        waitForDrain(warmClient, 30s);
    }

    const int numWriters = std::max(1, cfg.numClients - std::max(1, cfg.numClients / 3));
    const int numReaders = std::max(1, cfg.numClients / 3);

    std::atomic<bool> done{false};
    std::atomic<int> writerDocs{0};
    std::atomic<int> readerOps{0};
    std::atomic<int> readerFails{0};
    std::atomic<int> readerAttempts{0};
    std::atomic<int> getOps{0};
    std::atomic<int> catOps{0};
    std::mutex getLatMutex;
    std::mutex catLatMutex;
    std::vector<int64_t> getLatencies;
    std::vector<int64_t> catLatencies;

    auto pickHash = [&](std::mt19937& rng) -> std::string {
        std::lock_guard<std::mutex> lk(knownHashesMutex);
        if (knownHashes.empty()) {
            return std::string{};
        }
        std::uniform_int_distribution<std::size_t> dist(0, knownHashes.size() - 1);
        return knownHashes[dist(rng)];
    };

    auto globalStart = std::chrono::steady_clock::now();

    std::vector<std::thread> writerThreads;
    writerThreads.reserve(numWriters);
    for (int w = 0; w < numWriters; ++w) {
        writerThreads.emplace_back([&, w]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 30s;
            DaemonClient client(ccfg);

            for (int i = 0; i < cfg.docsPerClient; ++i) {
                AddDocumentRequest req;
                req.name = "access_writer" + std::to_string(w) + "_" + std::to_string(i) + ".md";
                req.content = generateDocument(w, i, cfg.docSizeBytes);
                req.tags = {"bench", "access", "writer-" + std::to_string(w)};

                auto res = yams::cli::run_sync(client.streamingAddDocument(req), 30s);
                if (res) {
                    writerDocs.fetch_add(1);
                    if (!res.value().hash.empty()) {
                        std::lock_guard<std::mutex> lk(knownHashesMutex);
                        knownHashes.push_back(res.value().hash);
                    }
                }
            }
        });
    }

    std::vector<std::thread> readerThreads;
    readerThreads.reserve(numReaders);
    for (int r = 0; r < numReaders; ++r) {
        readerThreads.emplace_back([&, r]() {
            ClientConfig ccfg;
            ccfg.socketPath = harness.socketPath();
            ccfg.autoStart = false;
            ccfg.requestTimeout = 10s;
            DaemonClient client(ccfg);

            std::mt19937 rng(static_cast<unsigned>(r * 1009 + 17));
            bool doGet = true;

            while (!done.load()) {
                std::string hash = pickHash(rng);
                if (hash.empty()) {
                    std::this_thread::sleep_for(5ms);
                    continue;
                }
                readerAttempts.fetch_add(1);

                // Alternate get (metadata-focused) and cat (content-focused)
                if (doGet) {
                    GetRequest req;
                    req.hash = hash;
                    req.metadataOnly = true;

                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.get(req), 10s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    if (res) {
                        getOps.fetch_add(1);
                        readerOps.fetch_add(1);
                        std::lock_guard<std::mutex> lk(getLatMutex);
                        getLatencies.push_back(latUs);
                    } else {
                        readerFails.fetch_add(1);
                    }
                } else {
                    CatRequest req;
                    req.hash = hash;

                    auto t0 = std::chrono::steady_clock::now();
                    auto res = yams::cli::run_sync(client.cat(req), 10s);
                    auto t1 = std::chrono::steady_clock::now();
                    auto latUs =
                        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();

                    if (res) {
                        catOps.fetch_add(1);
                        readerOps.fetch_add(1);
                        std::lock_guard<std::mutex> lk(catLatMutex);
                        catLatencies.push_back(latUs);
                    } else {
                        readerFails.fetch_add(1);
                    }
                }
                doGet = !doGet;

                std::this_thread::sleep_for(2ms);
            }
        });
    }

    for (auto& t : writerThreads)
        t.join();
    done.store(true);

    for (auto& t : readerThreads)
        t.join();

    ClientConfig monCfg;
    monCfg.socketPath = harness.socketPath();
    monCfg.autoStart = false;
    monCfg.requestTimeout = 10s;
    DaemonClient monitorClient(monCfg);
    bool drained = waitForDrain(monitorClient, std::chrono::seconds(cfg.drainTimeoutSecs));

    auto globalEnd = std::chrono::steady_clock::now();
    double globalElapsed = std::chrono::duration<double>(globalEnd - globalStart).count();

    auto getStats = PercentileStats::compute(getLatencies);
    auto catStats = PercentileStats::compute(catLatencies);

    std::cout << "\n=== File Access Under Ingest Load ===\n";
    std::cout << "  Writers: " << numWriters << " (ingested " << writerDocs.load() << " docs)\n";
    std::cout << "  Readers: " << numReaders << " (ops=" << readerOps.load()
              << ", attempts=" << readerAttempts.load() << ", get=" << getOps.load()
              << ", cat=" << catOps.load() << ", fails=" << readerFails.load() << ")\n";
    std::cout << "  Wall time: " << std::fixed << std::setprecision(2) << globalElapsed << "s\n";
    printPercentiles("Get latency", getStats);
    printPercentiles("Cat latency", catStats);
    std::cout << "  Queue drained: " << (drained ? "yes" : "NO") << "\n\n";

    CHECK(writerDocs.load() > 0);
    CHECK(readerOps.load() > 0);
    CHECK(readerFails.load() < readerAttempts.load());
    CHECK(drained);

    json record{
        {"timestamp", isoTimestamp()},
        {"test", "access_under_ingest_load"},
        {"num_clients", cfg.numClients},
        {"num_writers", numWriters},
        {"num_readers", numReaders},
        {"docs_per_writer", cfg.docsPerClient},
        {"doc_size_bytes", cfg.docSizeBytes},
        {"total_writer_docs", writerDocs.load()},
        {"reader_attempts", readerAttempts.load()},
        {"reader_ops", readerOps.load()},
        {"get_ops", getOps.load()},
        {"cat_ops", catOps.load()},
        {"reader_fails", readerFails.load()},
        {"elapsed_seconds", globalElapsed},
        {"get_latency", getStats.toJson()},
        {"cat_latency", catStats.toJson()},
        {"drained", drained},
        {"tuning_profile", cfg.tuningProfile},
    };
    emitJsonl(cfg.outputPath, record);
}
