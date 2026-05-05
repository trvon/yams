// PBI-040: Service layer performance benchmarks
// These benchmarks test the app/services layer to catch performance regressions
// in user-facing operations like get-by-name, cat, grep, search.

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <thread>
#include <vector>

#include "../common/env_compat.h"
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
    // empty => leave daemon/backend defaults in place
    std::string embeddingBackend;
    std::string tuningProfile; // empty => use default
    bool operator==(const BenchConfig& other) const {
        return embeddingsEnabled == other.embeddingsEnabled &&
               embeddingBackend == other.embeddingBackend && tuningProfile == other.tuningProfile;
    }
};

// Global daemon harness for benchmark suite (setup once, reused across benchmarks)
std::unique_ptr<DaemonHarness> g_harness;
std::unique_ptr<daemon::DaemonClient> g_client;
std::vector<std::string> g_test_docs;      // Hashes of test documents
std::vector<std::string> g_test_doc_names; // Names used for by-name retrieval
BenchConfig g_activeConfig;
std::optional<std::filesystem::path> g_activeConfigPath;
std::optional<std::string> g_savedEmbedBackendEnv;
bool g_savedEmbedBackendEnvPresent{false};
bool g_savedEmbedBackendEnvCaptured{false};
std::optional<std::string> g_savedOnnxForceCpuEnv;
bool g_savedOnnxForceCpuEnvPresent{false};
bool g_savedOnnxForceCpuEnvCaptured{false};
std::string g_observedEmbeddingBackend;
std::string g_observedEmbeddingModel;
uint32_t g_observedEmbeddingDim{0};
uint64_t g_observedVectorCount{0};
uint64_t g_embeddingRepairSucceeded{0};

std::string lowerCopy(std::string value) {
    for (auto& c : value) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return value;
}

std::string embeddingBackendForArg(int64_t arg) {
    switch (arg) {
        case 1:
            return "simeon";
        case 2:
            return "onnxruntime";
        default:
            return {};
    }
}

std::string preferredModelForBackend(const std::string& backend) {
    if (backend == "simeon") {
        return "simeon-default";
    }
    if (backend == "onnxruntime") {
        return "all-MiniLM-L6-v2";
    }
    return {};
}

std::filesystem::path resolveBenchmarkPluginDir() {
    namespace fs = std::filesystem;
    if (const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR")) {
        return fs::path(envPluginDir);
    }

    const auto cwd = fs::current_path();
    const std::vector<fs::path> candidates = {
        cwd / "build" / "debug" / "plugins",
        cwd / "builddir" / "plugins",
        cwd / "build" / "plugins",
    };
    std::error_code ec;
    for (const auto& candidate : candidates) {
        if (fs::exists(candidate, ec) && fs::is_directory(candidate, ec)) {
            return candidate;
        }
    }
    return cwd / "builddir" / "plugins";
}

std::filesystem::path writeEmbeddingBackendConfig(const BenchConfig& config) {
    namespace fs = std::filesystem;
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    auto configDir =
        fs::temp_directory_path() / ("yams_retrieval_service_bench_" + std::to_string(stamp));
    fs::create_directories(configDir);
    auto configPath = configDir / "config.toml";

    std::ofstream out(configPath);
    out << "[embeddings]\n";
    out << "backend = \"" << config.embeddingBackend << "\"\n";
    const auto preferred = preferredModelForBackend(config.embeddingBackend);
    if (!preferred.empty()) {
        out << "preferred_model = \"" << preferred << "\"\n";
    }
    out.close();

    return configPath;
}

void captureEmbedBackendEnvOnce() {
    if (g_savedEmbedBackendEnvCaptured) {
        return;
    }
    g_savedEmbedBackendEnvCaptured = true;
    if (const char* env = std::getenv("YAMS_EMBED_BACKEND")) {
        g_savedEmbedBackendEnvPresent = true;
        g_savedEmbedBackendEnv = std::string(env);
    } else {
        g_savedEmbedBackendEnvPresent = false;
        g_savedEmbedBackendEnv.reset();
    }
}

void restoreEmbedBackendEnvIfCaptured() {
    if (!g_savedEmbedBackendEnvCaptured) {
        return;
    }
    if (g_savedEmbedBackendEnvPresent && g_savedEmbedBackendEnv) {
        ::setenv("YAMS_EMBED_BACKEND", g_savedEmbedBackendEnv->c_str(), 1);
    } else {
        ::unsetenv("YAMS_EMBED_BACKEND");
    }
    g_savedEmbedBackendEnvCaptured = false;
    g_savedEmbedBackendEnvPresent = false;
    g_savedEmbedBackendEnv.reset();
}

void captureOnnxForceCpuEnvOnce() {
    if (g_savedOnnxForceCpuEnvCaptured) {
        return;
    }
    g_savedOnnxForceCpuEnvCaptured = true;
    if (const char* env = std::getenv("YAMS_ONNX_FORCE_CPU")) {
        g_savedOnnxForceCpuEnvPresent = true;
        g_savedOnnxForceCpuEnv = std::string(env);
    } else {
        g_savedOnnxForceCpuEnvPresent = false;
        g_savedOnnxForceCpuEnv.reset();
    }
}

void restoreOnnxForceCpuEnvIfCaptured() {
    if (!g_savedOnnxForceCpuEnvCaptured) {
        return;
    }
    if (g_savedOnnxForceCpuEnvPresent && g_savedOnnxForceCpuEnv) {
        ::setenv("YAMS_ONNX_FORCE_CPU", g_savedOnnxForceCpuEnv->c_str(), 1);
    } else {
        ::unsetenv("YAMS_ONNX_FORCE_CPU");
    }
    g_savedOnnxForceCpuEnvCaptured = false;
    g_savedOnnxForceCpuEnvPresent = false;
    g_savedOnnxForceCpuEnv.reset();
}

void updateObservedEmbeddingStatus(const daemon::StatusResponse& status) {
    g_observedEmbeddingBackend = status.embeddingBackend;
    g_observedEmbeddingModel = status.embeddingModel;
    g_observedEmbeddingDim = status.embeddingDim;
    if (auto it = status.requestCounts.find("vector_count"); it != status.requestCounts.end()) {
        g_observedVectorCount = it->second;
    } else {
        g_observedVectorCount = 0;
    }
}

void runBenchmarkEmbeddingRepair(const BenchConfig& config) {
    g_embeddingRepairSucceeded = 0;
    daemon::RepairRequest repairReq;
    repairReq.repairEmbeddings = true;
    repairReq.foreground = true;
    repairReq.force = true;
    repairReq.maxRetries = 1;
    repairReq.embeddingModel = preferredModelForBackend(config.embeddingBackend);

    auto timeout = std::chrono::milliseconds(180000);
    if (const char* env = std::getenv("YAMS_BENCH_REPAIR_WAIT_MS")) {
        try {
            timeout = std::chrono::milliseconds(
                static_cast<std::chrono::milliseconds::rep>(std::stoll(env)));
        } catch (...) {
        }
    }

    std::cout << "Running foreground embedding repair";
    if (!repairReq.embeddingModel.empty()) {
        std::cout << " (model=" << repairReq.embeddingModel << ")";
    }
    std::cout << "...\n";

    auto repairResult = cli::run_sync(g_client->callRepair(repairReq), timeout);
    if (!repairResult) {
        std::cerr << "WARNING: Embedding repair failed: " << repairResult.error().message << "\n";
        return;
    }

    const auto& resp = repairResult.value();
    std::cout << "Embedding repair result: success=" << (resp.success ? 1 : 0)
              << " operations=" << resp.totalOperations << " succeeded=" << resp.totalSucceeded
              << " failed=" << resp.totalFailed << " skipped=" << resp.totalSkipped << "\n";
    for (const auto& op : resp.operationResults) {
        if (op.operation == "embeddings") {
            g_embeddingRepairSucceeded = op.succeeded;
            std::cout << "Embedding repair operation: processed=" << op.processed
                      << " succeeded=" << op.succeeded << " failed=" << op.failed
                      << " skipped=" << op.skipped;
            if (!op.message.empty()) {
                std::cout << " message=" << op.message;
            }
            std::cout << "\n";
        }
    }
}

bool benchmarkFilterTargetsBackendAB(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i] ? argv[i] : "";
        if (arg.rfind("--benchmark_filter=", 0) == 0 &&
            arg.find("BackendAB") != std::string::npos) {
            return true;
        }
    }
    return false;
}

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

uint64_t getCountOrZero(const daemon::StatusResponse& st, const std::string& key) {
    if (auto it = st.requestCounts.find(key); it != st.requestCounts.end()) {
        return it->second;
    }
    return 0;
}

bool isVectorDbReady(const daemon::StatusResponse& st) {
    if (auto it = st.readinessStates.find("vector_db"); it != st.readinessStates.end()) {
        return it->second;
    }
    return st.vectorDbReady;
}

bool waitForCorpusIndexed(std::size_t expectedDocs, std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    int stableCount = 0;
    constexpr int stableRequired = 10;

    daemon::StatusResponse lastStatus;
    bool haveLastStatus = false;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (!status) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        lastStatus = status.value();
        haveLastStatus = true;

        const auto& st = lastStatus;
        const uint64_t docsTotal = getCountOrZero(st, "documents_total");
        const uint64_t docsIndexed = getCountOrZero(st, "documents_indexed");
        const uint64_t postQueued = getCountOrZero(st, "post_ingest_queued");
        const uint64_t postInflight = getCountOrZero(st, "post_ingest_inflight");

        // `documents_indexed` tracks search/FTS visibility and can lag the canonical document
        // count in overlay-backed search builds. The service benchmarks need a stable, queryable
        // corpus, not necessarily a fully backfilled FTS counter, so accept the stored document
        // count once post-ingest is drained.
        const bool countsMet =
            (expectedDocs == 0) || (docsTotal >= static_cast<uint64_t>(expectedDocs) &&
                                    (docsIndexed >= static_cast<uint64_t>(expectedDocs) ||
                                     (postQueued == 0 && postInflight == 0)));
        const bool postDrained = (postQueued == 0 && postInflight == 0);

        if (countsMet && postDrained) {
            ++stableCount;
        } else {
            stableCount = 0;
        }

        if (stableCount >= stableRequired) {
            return true;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    if (haveLastStatus) {
        const auto& st = lastStatus;
        const uint64_t docsTotal = getCountOrZero(st, "documents_total");
        const uint64_t docsIndexed = getCountOrZero(st, "documents_indexed");
        const uint64_t postQueued = getCountOrZero(st, "post_ingest_queued");
        const uint64_t postInflight = getCountOrZero(st, "post_ingest_inflight");
        const uint64_t embedQueued = getCountOrZero(st, "embed_svc_queued");
        const uint64_t embedInflight = getCountOrZero(st, "embed_in_flight");
        const uint64_t vectorCount = getCountOrZero(st, "vector_count");

        bool searchReady = false;
        if (auto it = st.readinessStates.find("search_engine"); it != st.readinessStates.end()) {
            searchReady = it->second;
        }

        std::cerr << "WARNING: Corpus readiness timeout after " << timeout.count() << "ms"
                  << " (expectedDocs=" << expectedDocs << " docsTotal=" << docsTotal
                  << " docsIndexed=" << docsIndexed << " postQueued=" << postQueued
                  << " postInflight=" << postInflight << " embedQueued=" << embedQueued
                  << " embedInflight=" << embedInflight << " vectorCount=" << vectorCount
                  << " vectorDbReady=" << (isVectorDbReady(st) ? 1 : 0)
                  << " searchReady=" << (searchReady ? 1 : 0) << ")\n";
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
        if (vectorCount != lastVectorCount || !embedDrained) {
            stableCount = 0;
            lastVectorCount = vectorCount;
        } else {
            ++stableCount;
        }

        if (isVectorDbReady(st) && embedDrained && stableCount >= stableRequired) {
            if (minDocCount == 0)
                return true;
            return vectorCount >= static_cast<uint64_t>(minDocCount);
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
        const uint64_t vectorCount = getCountOrZero(st, "vector_count");
        const uint64_t embedQueued = getCountOrZero(st, "embed_svc_queued");
        const uint64_t embedInFlight = getCountOrZero(st, "embed_in_flight");
        const uint64_t postQueued = getCountOrZero(st, "post_ingest_queued");
        const uint64_t postInFlight = getCountOrZero(st, "post_ingest_inflight");

        metrics.maxEmbedQueued = std::max(metrics.maxEmbedQueued, embedQueued);
        metrics.maxEmbedInflight = std::max(metrics.maxEmbedInflight, embedInFlight);
        metrics.maxPostQueued = std::max(metrics.maxPostQueued, postQueued);
        metrics.maxPostInflight = std::max(metrics.maxPostInflight, postInFlight);

        const bool embedDrained = (embedQueued == 0 && embedInFlight == 0);
        if (vectorCount != lastVectorCount || !embedDrained) {
            stableCount = 0;
            lastVectorCount = vectorCount;
        } else {
            ++stableCount;
        }

        const bool vectorReady = isVectorDbReady(st) || minDocCount == 0;
        if (vectorReady && embedDrained && stableCount >= stableRequired) {
            if (minDocCount == 0)
                return true;
            return vectorCount >= static_cast<uint64_t>(minDocCount);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

bool waitForVectorDbServing(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (status && isVectorDbReady(status.value())) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool waitForPostIngestDrainWithMetrics(std::chrono::milliseconds timeout, DrainMetrics& metrics) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    int stableCount = 0;
    constexpr int stableRequired = 10;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (!status) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        const auto& st = status.value();
        const uint64_t postQueued = getCountOrZero(st, "post_ingest_queued");
        const uint64_t postInflight = getCountOrZero(st, "post_ingest_inflight");
        const uint64_t embedQueued = getCountOrZero(st, "embed_svc_queued");
        const uint64_t embedInflight = getCountOrZero(st, "embed_in_flight");

        metrics.maxEmbedQueued = std::max(metrics.maxEmbedQueued, embedQueued);
        metrics.maxEmbedInflight = std::max(metrics.maxEmbedInflight, embedInflight);
        metrics.maxPostQueued = std::max(metrics.maxPostQueued, postQueued);
        metrics.maxPostInflight = std::max(metrics.maxPostInflight, postInflight);

        const bool postDrained = (postQueued == 0 && postInflight == 0);
        if (postDrained) {
            ++stableCount;
        } else {
            stableCount = 0;
        }

        if (stableCount >= stableRequired) {
            return true;
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
    g_activeConfigPath.reset();
    g_observedEmbeddingBackend.clear();
    g_observedEmbeddingModel.clear();
    g_observedEmbeddingDim = 0;
    g_observedVectorCount = 0;
    g_embeddingRepairSucceeded = 0;

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
    DaemonHarness::Options harnessOptions;
    if (config.embeddingsEnabled) {
        // Keep everything enabled: let PluginManager decide availability.
        harnessOptions.useMockModelProvider = false;
        harnessOptions.autoLoadPlugins = true;
        harnessOptions.configureModelPool = true;
        harnessOptions.modelPoolLazyLoading = false;
        harnessOptions.pluginDir = resolveBenchmarkPluginDir();
        if (!config.embeddingBackend.empty()) {
            // Backend A/B is driven through typed config so the benchmark exercises the same
            // ConfigResolver path as production. Clear the legacy env overlay while the
            // benchmark-owned config is active; restore it on teardown.
            captureEmbedBackendEnvOnce();
            ::unsetenv("YAMS_EMBED_BACKEND");
            if (config.embeddingBackend == "onnxruntime" && !std::getenv("YAMS_ONNX_FORCE_CPU")) {
                // Keep release-validation numbers comparable and avoid platform EP failures
                // (e.g. CoreML partition bugs) from masking ONNX Runtime backend selection.
                captureOnnxForceCpuEnvOnce();
                ::setenv("YAMS_ONNX_FORCE_CPU", "1", 1);
            }
            auto configPath = writeEmbeddingBackendConfig(config);
            harnessOptions.configPath = configPath;
            g_activeConfigPath = configPath;
            std::cout << "Embedding backend config: " << config.embeddingBackend
                      << " (config=" << configPath << ")\n";
        }
        std::cout << "Plugin dir: " << harnessOptions.pluginDir->string() << "\n";
    }
    g_harness = std::make_unique<DaemonHarness>(harnessOptions);
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
        ofs << "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor "
               "incididunt ut labore et dolore magna aliqua.\n";
        ofs << "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip "
               "ex ea commodo consequat.\n";
        ofs << "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu "
               "fugiat nulla pariatur.\n";
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

    // Benchmarks assume a queryable corpus; wait for post-ingest + FTS5/doc indexing to catch up.
    // With faster ingestion this can otherwise race and lead to false stalls/timeouts.
    {
        auto timeout = std::chrono::milliseconds(180000);
        if (const char* env = std::getenv("YAMS_BENCH_INDEX_WAIT_MS")) {
            timeout = std::chrono::milliseconds(
                static_cast<std::chrono::milliseconds::rep>(std::stoll(env)));
        }
        std::cout << "Waiting for corpus to be indexed (FTS5/doc counters)...\n";
        if (!waitForCorpusIndexed(g_test_docs.size(), timeout)) {
            std::cerr << "WARNING: Corpus indexing not observed as ready; proceeding anyway.\n";
        }
    }

    if (embeddingsEnabled) {
        if (!config.embeddingBackend.empty()) {
            runBenchmarkEmbeddingRepair(config);
        } else {
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

        if (auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5))) {
            updateObservedEmbeddingStatus(status.value());
            std::cout << "Embedding runtime: available="
                      << (status.value().embeddingAvailable ? 1 : 0)
                      << " backend=" << g_observedEmbeddingBackend
                      << " model=" << g_observedEmbeddingModel << " dim=" << g_observedEmbeddingDim
                      << " vectorCount=" << g_observedVectorCount << "\n";
        }
    }

    std::cout << "Setup complete: " << g_test_docs.size() << " documents added\n\n";
}

void TeardownBenchmarkSuite() {
    g_client.reset();
    g_harness.reset();
    g_test_docs.clear();
    g_test_doc_names.clear();
    g_activeConfig = {};
    g_activeConfigPath.reset();
    g_observedEmbeddingBackend.clear();
    g_observedEmbeddingModel.clear();
    g_observedEmbeddingDim = 0;
    g_observedVectorCount = 0;
    g_embeddingRepairSucceeded = 0;
    ::unsetenv("YAMS_TUNING_PROFILE");
    ::unsetenv("YAMS_BENCH_ENABLE_EMBEDDINGS");
    restoreEmbedBackendEnvIfCaptured();
    restoreOnnxForceCpuEnvIfCaptured();
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
        case 1:
            cfg.tuningProfile = "Efficient";
            break;
        case 2:
            cfg.tuningProfile = "Balanced";
            break;
        case 3:
            cfg.tuningProfile = "Aggressive";
            break;
        default:
            cfg.tuningProfile.clear();
            break; // default profile
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
            const std::string docName =
                "bench_pi_" + std::to_string(i) + "_" + std::to_string(state.iterations()) + ".txt";
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
        waitForPostIngestDrainWithMetrics(timeout, metrics);

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

// Benchmark: direct daemon embedding generation, sweeping the selected provider backend.
// This provides a stable A/B signal for provider selection and embedding throughput without
// coupling the result to vector-index rebuild policy.
static void BM_EmbeddingBackendAB_Generate(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_EmbeddingBackendAB_Generate");
#endif

    const auto backend = embeddingBackendForArg(state.range(0));
    if (backend.empty()) {
        state.SkipWithError("Unknown backend arg");
        return;
    }

    BenchConfig cfg;
    cfg.embeddingsEnabled = true;
    cfg.embeddingBackend = backend;
    SetupBenchmarkSuite(cfg);

    auto statusResult = cli::run_sync(g_client->status(), std::chrono::seconds(5));
    if (!statusResult) {
        state.SkipWithError("Unable to read daemon status for embedding backend validation");
        return;
    }
    const auto status = statusResult.value();
    updateObservedEmbeddingStatus(status);
    const auto observed = lowerCopy(status.embeddingBackend + " " + status.embeddingModel + " " +
                                    status.embeddingModelPath + " " + status.version);
    if (!status.embeddingAvailable) {
        state.SkipWithError("Embedding backend is not available");
        return;
    }
    if (backend == "simeon" && observed.find("simeon") == std::string::npos) {
        state.SkipWithError("Requested simeon but daemon did not report a Simeon provider");
        return;
    }
    if (backend == "onnxruntime" && observed.find("onnx") == std::string::npos &&
        observed.find("abimodelprovider") == std::string::npos) {
        state.SkipWithError("Requested onnxruntime but daemon did not report an ONNX/ABI provider");
        return;
    }

    const auto docCount = static_cast<std::size_t>(std::max<int64_t>(1, state.range(1)));
    std::vector<std::string> texts;
    texts.reserve(docCount);
    for (std::size_t i = 0; i < docCount; ++i) {
        texts.push_back("YAMS embedding throughput document " + std::to_string(i) +
                        " for backend " + backend +
                        ". This sample text exercises daemon batch embedding generation.");
    }
    const auto model = preferredModelForBackend(backend);
    state.SetLabel("requested=" + backend + " observed=" + status.embeddingBackend +
                   " model=" + status.embeddingModel);

    std::vector<int64_t> latencies_us;
    size_t success_count = 0;
    size_t failure_count = 0;

    for (auto _ : state) {
        auto start = std::chrono::high_resolution_clock::now();

        daemon::BatchEmbeddingRequest req;
        req.texts = texts;
        req.modelName = model;
        req.normalize = true;
        req.batchSize = texts.size();

        auto result =
            cli::run_sync(g_client->generateBatchEmbeddings(req), std::chrono::milliseconds(30000));
        if (result && result.value().successCount == texts.size()) {
            success_count += result.value().successCount;
        } else {
            ++failure_count;
        }

        auto end = std::chrono::high_resolution_clock::now();
        latencies_us.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
    }

    if (latencies_us.empty()) {
        state.SkipWithError("No samples collected for embedding backend A/B benchmark");
        return;
    }
    std::sort(latencies_us.begin(), latencies_us.end());
    auto p50 = latencies_us[latencies_us.size() / 2];
    auto p95 = latencies_us[static_cast<size_t>(latencies_us.size() * 0.95)];
    auto max_us = latencies_us.back();
    const auto totalLatencyUs =
        std::accumulate(latencies_us.begin(), latencies_us.end(), int64_t{0});
    const auto avg_us = latencies_us.empty()
                            ? int64_t{0}
                            : totalLatencyUs / static_cast<int64_t>(latencies_us.size());

    state.counters["success_count"] = static_cast<double>(success_count);
    state.counters["failure_count"] = static_cast<double>(failure_count);
    state.counters["batch_docs"] = static_cast<double>(docCount);
    state.counters["texts_per_iter"] = static_cast<double>(texts.size());
    state.counters["avg_us"] = static_cast<double>(avg_us);
    state.counters["p50_us"] = static_cast<double>(p50);
    state.counters["p95_us"] = static_cast<double>(p95);
    state.counters["max_us"] = static_cast<double>(max_us);
    state.counters["avg_docs_per_s"] =
        avg_us > 0 ? static_cast<double>(docCount) * 1'000'000.0 / static_cast<double>(avg_us)
                   : 0.0;
    state.counters["p50_docs_per_s"] =
        p50 > 0 ? static_cast<double>(docCount) * 1'000'000.0 / static_cast<double>(p50) : 0.0;
    state.counters["p95_docs_per_s"] =
        p95 > 0 ? static_cast<double>(docCount) * 1'000'000.0 / static_cast<double>(p95) : 0.0;
    state.counters["embedding_dim"] = static_cast<double>(g_observedEmbeddingDim);
    state.counters["repair_succeeded"] = static_cast<double>(g_embeddingRepairSucceeded);

    if (failure_count > 0) {
        state.SkipWithError("Embedding backend A/B generation saw daemon errors");
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

// Arg backend: 1=simeon, 2=onnxruntime.
BENCHMARK(BM_EmbeddingBackendAB_Generate)
    ->ArgNames({"backend", "docs"})
    ->Args({1, 100})
    ->Args({2, 100})
    ->Args({1, 1000})
    ->Args({2, 1000})
    ->Unit(benchmark::kMicrosecond)
    ->Iterations(3);

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

    // Setup daemon and test data. The embedding backend A/B benchmark owns its setup per backend
    // row, so skip this default harness when the filter targets only that validation pass.
    const bool deferSetupToBackendAB = benchmarkFilterTargetsBackendAB(argc, argv);
    if (!deferSetupToBackendAB) {
        SetupBenchmarkSuite();
    }

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
