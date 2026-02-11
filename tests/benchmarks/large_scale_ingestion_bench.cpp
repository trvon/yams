// Large-scale ingestion performance benchmarks
// Tests the full ingestion pipeline at production scale (10K-1M+ documents)
// Measures per-stage latency, throughput, resource usage, and backpressure behavior

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <benchmark/benchmark.h>

#ifdef TRACY_ENABLE
#include <tracy/Tracy.hpp>
#endif

#include "../common/benchmark_tracker.h"
#include "../common/test_data_generator.h"
#include "../integration/daemon/test_async_helpers.h"
#include "../integration/daemon/test_daemon_harness.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/metric_keys.h>

using namespace yams;
using namespace yams::test;
using namespace yams::daemon;

namespace {

// Forward declarations for globals used by helper waits.
extern std::unique_ptr<DaemonHarness> g_harness;
extern std::unique_ptr<daemon::DaemonClient> g_client;

uint64_t getCountOrZero(const daemon::StatusResponse& st, const std::string& key) {
    if (auto it = st.requestCounts.find(key); it != st.requestCounts.end()) {
        return it->second;
    }
    return 0;
}

bool waitForSearchEngineReady(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (status) {
            if (auto it = status.value().readinessStates.find("search_engine");
                it != status.value().readinessStates.end() && it->second) {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}

bool waitForVectorDbReady(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (status) {
            const auto& st = status.value();
            // Prefer readinessStates["vector_db"] (canonical) but keep compatibility with
            // vectorDbReady field.
            bool ready = st.vectorDbReady;
            if (auto it = st.readinessStates.find("vector_db"); it != st.readinessStates.end()) {
                ready = it->second;
            }
            if (ready) {
                return true;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

bool waitForVectorDbInitialized(std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    auto nextLog = std::chrono::steady_clock::now();
    daemon::StatusResponse lastStatus;
    bool haveLastStatus = false;
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (status) {
            const auto& st = status.value();
            lastStatus = st;
            haveLastStatus = true;
            // Initialization semantics: the vector DB can be initialized (dim set) but not
            // "serving" yet (0 vectors). For setup, we just need init to have happened.
            if (st.vectorDbInitAttempted && st.vectorDbDim > 0) {
                return true;
            }
            auto now = std::chrono::steady_clock::now();
            if (now >= nextLog) {
                const uint64_t vectorCount = getCountOrZero(st, std::string(metrics::kVectorCount));
                bool mapReady = false;
                if (auto it = st.readinessStates.find("vector_db");
                    it != st.readinessStates.end()) {
                    mapReady = it->second;
                }
                std::cerr << "[bench] vector init wait: initAttempted="
                          << (st.vectorDbInitAttempted ? 1 : 0) << " dim=" << st.vectorDbDim
                          << " vectorDbReady=" << (st.vectorDbReady ? 1 : 0)
                          << " readinessStates[vector_db]=" << (mapReady ? 1 : 0)
                          << " vector_count=" << vectorCount << "\n";
                nextLog = now + std::chrono::seconds(1);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    if (haveLastStatus) {
        const auto& st = lastStatus;
        const uint64_t vectorCount = getCountOrZero(st, std::string(metrics::kVectorCount));
        bool mapReady = false;
        if (auto it = st.readinessStates.find("vector_db"); it != st.readinessStates.end()) {
            mapReady = it->second;
        }
        std::cerr << "[bench] vector init wait timed out after " << timeout.count() << "ms"
                  << " (initAttempted=" << (st.vectorDbInitAttempted ? 1 : 0)
                  << " dim=" << st.vectorDbDim << " vectorDbReady=" << (st.vectorDbReady ? 1 : 0)
                  << " readinessStates[vector_db]=" << (mapReady ? 1 : 0)
                  << " vector_count=" << vectorCount << ")\n";
    }
    return false;
}

bool waitForCorpusIndexed(
    std::size_t expectedDocs, bool embeddingsEnabled, std::chrono::milliseconds timeout,
    std::optional<std::chrono::steady_clock::time_point> hardDeadline = std::nullopt,
    bool* hardTimedOut = nullptr) {
    if (hardTimedOut) {
        *hardTimedOut = false;
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;
    int stableCount = 0;
    int stableRequired = 10;
    auto nextLog = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    if (const char* env = std::getenv("YAMS_BENCH_CORPUS_STABLE_REQUIRED")) {
        try {
            stableRequired = std::max(1, std::stoi(env));
        } catch (...) {
        }
    }

    daemon::StatusResponse lastStatus;
    bool haveLastStatus = false;

    while (std::chrono::steady_clock::now() < deadline) {
        if (hardDeadline && std::chrono::steady_clock::now() >= *hardDeadline) {
            if (hardTimedOut) {
                *hardTimedOut = true;
            }
            std::cerr << "WARNING: Corpus indexing wait hit phase hard-timeout\n";
            return false;
        }

        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (!status) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        lastStatus = status.value();
        haveLastStatus = true;

        const auto& st = lastStatus;
        const uint64_t docsTotal = getCountOrZero(st, std::string(metrics::kDocumentsTotal));
        const uint64_t docsIndexed = getCountOrZero(st, std::string(metrics::kDocumentsIndexed));
        const uint64_t postQueued = getCountOrZero(st, std::string(metrics::kPostIngestQueued));
        const uint64_t postInflight = getCountOrZero(st, std::string(metrics::kPostIngestInflight));
        const uint64_t embedQueued = getCountOrZero(st, std::string(metrics::kEmbedQueued));
        const uint64_t embedInflight = getCountOrZero(st, std::string(metrics::kEmbedInflight));
        const uint64_t vectorCount = getCountOrZero(st, std::string(metrics::kVectorCount));

        const bool countsMet =
            (expectedDocs == 0) || (docsTotal >= static_cast<uint64_t>(expectedDocs) &&
                                    docsIndexed >= static_cast<uint64_t>(expectedDocs));
        const bool postDrained = (postQueued == 0 && postInflight == 0);
        const bool embedDrained = (!embeddingsEnabled) || (embedQueued == 0 && embedInflight == 0);
        bool vectorReady = (!embeddingsEnabled) || st.vectorDbReady;
        if (auto it = st.readinessStates.find("vector_db"); it != st.readinessStates.end()) {
            vectorReady = (!embeddingsEnabled) || it->second;
        }
        // Serving semantics: vector_db readiness is only true once vector_count > 0.
        // For corpus readiness, we actually care that vectors are produced for the corpus.
        const bool vectorMet =
            (!embeddingsEnabled) || (vectorCount >= static_cast<uint64_t>(expectedDocs));

        if (countsMet && postDrained && embedDrained && vectorReady && vectorMet) {
            ++stableCount;
        } else {
            stableCount = 0;
        }

        if (stableCount >= stableRequired) {
            return true;
        }

        auto now = std::chrono::steady_clock::now();
        if (now >= nextLog) {
            std::cerr << "[bench] corpus wait: expected=" << expectedDocs
                      << " docsTotal=" << docsTotal << " docsIndexed=" << docsIndexed
                      << " postQ=" << postQueued << " postIn=" << postInflight
                      << " embedQ=" << embedQueued << " embedIn=" << embedInflight
                      << " vectorCount=" << vectorCount
                      << " vectorDbReady=" << (st.vectorDbReady ? 1 : 0)
                      << " stable=" << stableCount << "/" << stableRequired
                      << " rss_mb=" << std::fixed << std::setprecision(1) << st.memoryUsageMb
                      << " cpu_pct=" << st.cpuUsagePercent << "\n";
            nextLog = now + std::chrono::seconds(5);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    if (haveLastStatus) {
        const auto& st = lastStatus;
        std::cerr << "WARNING: Corpus indexing readiness timeout after " << timeout.count() << "ms"
                  << " (expectedDocs=" << expectedDocs
                  << " docsTotal=" << getCountOrZero(st, std::string(metrics::kDocumentsTotal))
                  << " docsIndexed=" << getCountOrZero(st, std::string(metrics::kDocumentsIndexed))
                  << " postQueued=" << getCountOrZero(st, std::string(metrics::kPostIngestQueued))
                  << " postInflight="
                  << getCountOrZero(st, std::string(metrics::kPostIngestInflight))
                  << " embedQueued=" << getCountOrZero(st, std::string(metrics::kEmbedQueued))
                  << " embedInflight=" << getCountOrZero(st, std::string(metrics::kEmbedInflight))
                  << " vectorCount=" << getCountOrZero(st, std::string(metrics::kVectorCount))
                  << " vectorDbReady=" << (st.vectorDbReady ? 1 : 0) << ")\n";
    }
    return false;
}

struct IngestionBenchConfig {
    size_t documentCount{10000};
    bool enableEmbeddings{false};
    std::string tuningProfile;
    size_t batchSize{1000};
    double duplicationRate{0.05};
    bool operator==(const IngestionBenchConfig& other) const {
        return documentCount == other.documentCount && enableEmbeddings == other.enableEmbeddings &&
               tuningProfile == other.tuningProfile && batchSize == other.batchSize &&
               duplicationRate == other.duplicationRate;
    }
};

// Global state
std::unique_ptr<DaemonHarness> g_harness;
std::unique_ptr<daemon::DaemonClient> g_client;
IngestionBenchConfig g_activeConfig;

// Best-effort counters for time series (bench-local; daemon counters are separate).
std::atomic<uint64_t> g_bytesIngested{0};

// Time series sample structure
struct TimeSeriesSample {
    std::chrono::steady_clock::time_point timestamp;
    uint64_t elapsedMs{0};

    // Progress metrics
    uint64_t docsIngested{0};
    uint64_t docsFailed{0};
    uint64_t totalBytes{0};

    // Queue metrics from daemon status
    uint64_t postIngestQueued{0};
    uint64_t postIngestInflight{0};
    uint64_t embedQueued{0};
    uint64_t embedInflight{0};
    uint64_t embedInferActive{0};
    uint64_t embedInferOldestMs{0};
    uint64_t embedInferStarted{0};
    uint64_t embedInferCompleted{0};
    uint64_t embedInferLastMs{0};
    uint64_t embedInferMaxMs{0};
    uint64_t embedInferWarnCount{0};
    uint64_t kgQueued{0};
    uint64_t kgInflight{0};
    uint64_t symbolQueued{0};
    uint64_t symbolInflight{0};
    uint64_t entityQueued{0};
    uint64_t entityInflight{0};
    uint64_t titleQueued{0};
    uint64_t titleInflight{0};

    // Resource metrics
    uint64_t rssBytes{0};
    double cpuPercent{0.0};
    int pressureLevel{0};

    // Dropped counters for diagnostic
    uint64_t kgDropped{0};
    uint64_t symbolDropped{0};
    uint64_t entityDropped{0};
    uint64_t titleDropped{0};
    uint64_t busPostDropped{0};

    // Instantaneous throughput (docs/sec in last interval)
    double docsPerSecond{0.0};
    double bytesPerSecond{0.0};
};

// Time series g_collector
class TimeSeriesCollector {
public:
    void start(std::chrono::milliseconds interval = std::chrono::milliseconds(1000)) {
        startTime_ = std::chrono::steady_clock::now();
        stop_ = false;
        thread_ = std::thread([this, interval]() { collectLoop(interval); });
    }

    void stop() {
        stop_ = true;
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    void addSample(TimeSeriesSample sample) {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.push_back(std::move(sample));
    }

    std::vector<TimeSeriesSample> getSamples() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return samples_;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        samples_.clear();
    }

private:
    void collectLoop(std::chrono::milliseconds interval) {
        auto lastSample = collectSample();
        addSample(lastSample);

        while (!stop_) {
            std::this_thread::sleep_for(interval);
            if (stop_)
                break;

            auto sample = collectSample();

            // Calculate instantaneous throughput
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                               sample.timestamp - lastSample.timestamp)
                               .count();
            if (elapsed > 0) {
                sample.docsPerSecond =
                    static_cast<double>(sample.docsIngested - lastSample.docsIngested) * 1000.0 /
                    elapsed;
                sample.bytesPerSecond =
                    static_cast<double>(sample.totalBytes - lastSample.totalBytes) * 1000.0 /
                    elapsed;
            }
            sample.elapsedMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(sample.timestamp - startTime_)
                    .count();

            addSample(sample);
            lastSample = sample;
        }
    }

    TimeSeriesSample collectSample() {
        TimeSeriesSample sample;
        sample.timestamp = std::chrono::steady_clock::now();

        // Bench-local bytes ingested (successful addViaDaemon only).
        sample.totalBytes = g_bytesIngested.load(std::memory_order_relaxed);

        // Get daemon status
        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (status) {
            const auto& st = status.value();

            // Queue depths
            auto getCount = [&](const std::string& key) -> uint64_t {
                auto it = st.requestCounts.find(key);
                return (it != st.requestCounts.end()) ? it->second : 0;
            };

            sample.postIngestQueued = getCount(std::string(metrics::kPostIngestQueued));
            sample.postIngestInflight = getCount(std::string(metrics::kPostIngestInflight));
            sample.embedQueued = getCount(std::string(metrics::kEmbedQueued));
            sample.embedInflight = getCount(std::string(metrics::kEmbedInflight));
            sample.embedInferActive = getCount(std::string(metrics::kEmbedInferActive));
            sample.embedInferOldestMs = getCount(std::string(metrics::kEmbedInferOldestMs));
            sample.embedInferStarted = getCount(std::string(metrics::kEmbedInferStarted));
            sample.embedInferCompleted = getCount(std::string(metrics::kEmbedInferCompleted));
            sample.embedInferLastMs = getCount(std::string(metrics::kEmbedInferLastMs));
            sample.embedInferMaxMs = getCount(std::string(metrics::kEmbedInferMaxMs));
            sample.embedInferWarnCount = getCount(std::string(metrics::kEmbedInferWarnCount));
            sample.kgQueued = getCount(std::string(metrics::kKgQueueDepth));
            sample.kgInflight = getCount(std::string(metrics::kKgInflight));
            sample.symbolQueued = getCount(std::string(metrics::kSymbolQueueDepth));
            sample.symbolInflight = getCount(std::string(metrics::kSymbolInflight));
            sample.entityQueued = getCount(std::string(metrics::kEntityQueueDepth));
            sample.entityInflight = getCount(std::string(metrics::kEntityInflight));
            sample.titleQueued = getCount(std::string(metrics::kTitleQueueDepth));
            sample.titleInflight = getCount(std::string(metrics::kTitleInflight));

            // Document counts
            sample.docsIngested = getCount(std::string(metrics::kDocumentsTotal));
            sample.docsFailed = getCount(std::string(metrics::kPostIngestFailed));

            // Resource metrics
            sample.rssBytes = static_cast<uint64_t>(st.memoryUsageMb * 1024 * 1024);
            sample.cpuPercent = st.cpuUsagePercent;
            sample.pressureLevel = static_cast<int>(getCount(std::string(metrics::kPressureLevel)));

            // Dropped counters
            sample.kgDropped = getCount(std::string(metrics::kKgDropped));
            sample.symbolDropped = getCount(std::string(metrics::kSymbolDropped));
            sample.entityDropped = getCount(std::string(metrics::kEntityDropped));
            sample.titleDropped = getCount(std::string(metrics::kTitleDropped));

            // Bus dropped
            sample.busPostDropped = getCount(std::string(metrics::kBusPostDropped));
        }

        return sample;
    }

    std::atomic<bool> stop_{false};
    std::thread thread_;
    std::vector<TimeSeriesSample> samples_;
    mutable std::mutex mutex_;
    std::chrono::steady_clock::time_point startTime_{std::chrono::steady_clock::now()};
};

// Global g_collector
TimeSeriesCollector g_collector;

// Environment-based configuration
size_t getDocCount() {
    if (const char* env = std::getenv("YAMS_BENCH_DOC_COUNT")) {
        try {
            auto val = std::stoull(env);
            if (val > 0)
                return val;
        } catch (...) {
        }
    }
    return 10000; // Default 10K
}

bool getEnableEmbeddings() {
    if (const char* env = std::getenv("YAMS_BENCH_ENABLE_EMBEDDINGS")) {
        return std::string(env) == "1";
    }
    return false;
}

std::string getTuningProfile() {
    if (const char* env = std::getenv("YAMS_TUNING_PROFILE")) {
        return std::string(env);
    }
    return "";
}

std::string getBenchEmbedProfile() {
    if (const char* env = std::getenv("YAMS_BENCH_EMBED_PROFILE")) {
        return std::string(env);
    }
    return "";
}

double getDuplicationRate() {
    if (const char* env = std::getenv("YAMS_BENCH_DUPLICATION_RATE")) {
        try {
            return std::stod(env);
        } catch (...) {
        }
    }
    return 0.05; // 5% default
}

// Setup harness with configuration
void SetupHarness(const IngestionBenchConfig& config) {
    if (g_harness && g_activeConfig == config) {
        return; // Already initialized
    }

    g_harness.reset();
    g_client.reset();
    g_activeConfig = config;

    std::cout << "\n=== Setting up ingestion benchmark environment ===\n";
    std::cout << "Document count: " << config.documentCount << "\n";
    std::cout << "Enable embeddings: " << (config.enableEmbeddings ? "yes" : "no") << "\n";
    std::cout << "Tuning profile: "
              << (config.tuningProfile.empty() ? "default" : config.tuningProfile) << "\n";
    std::cout << "Duplication rate: " << (config.duplicationRate * 100) << "%\n";

    const std::string embedProfile = getBenchEmbedProfile();
    if (!embedProfile.empty()) {
        std::cout << "Embed benchmark profile: " << embedProfile << "\n";
    }

    // Set environment variables
    if (!config.tuningProfile.empty()) {
        ::setenv("YAMS_TUNING_PROFILE", config.tuningProfile.c_str(), 1);
    } else {
        ::unsetenv("YAMS_TUNING_PROFILE");
    }
    ::setenv("YAMS_BENCH_ENABLE_EMBEDDINGS", config.enableEmbeddings ? "1" : "0", 1);
    if (!embedProfile.empty()) {
        ::setenv("YAMS_BENCH_EMBED_PROFILE", embedProfile.c_str(), 1);
    }

    // Disable automatic search engine rebuilds by default for benchmark determinism.
    // Rebuilds compete with ingestion and can dominate runtime on large corpora.
    // Set YAMS_BENCH_ENABLE_SEARCH_REBUILDS=1 to allow rebuilds.
    bool enableRebuilds = false;
    if (const char* env = std::getenv("YAMS_BENCH_ENABLE_SEARCH_REBUILDS")) {
        enableRebuilds = (std::string(env) == "1");
    }
    if (!enableRebuilds) {
        ::setenv("YAMS_DISABLE_SEARCH_REBUILDS", "1", 1);
    }

    // Start daemon
    DaemonHarness::Options harnessOptions;
    // AutoRepair/RepairService can compete with ingestion at high scale (per-hash DB checks).
    // Disable for benchmark determinism and throughput analysis.
    harnessOptions.enableAutoRepair = false;
    if (config.enableEmbeddings) {
        // Keep everything enabled: let PluginManager decide availability.
        harnessOptions.useMockModelProvider = false;
        harnessOptions.autoLoadPlugins = true;
        harnessOptions.configureModelPool = true;
        harnessOptions.modelPoolLazyLoading = false;
        if (const char* envPluginDir = std::getenv("YAMS_PLUGIN_DIR")) {
            harnessOptions.pluginDir = std::filesystem::path(envPluginDir);
        } else {
            harnessOptions.pluginDir = std::filesystem::current_path() / "builddir" / "plugins";
        }
    }
    g_harness = std::make_unique<DaemonHarness>(harnessOptions);
    if (!g_harness->start(std::chrono::seconds(30))) {
        std::cerr << "ERROR: Failed to start daemon\n";
        std::exit(1);
    }

    // Create client
    daemon::ClientConfig cc;
    cc.socketPath = g_harness->socketPath();
    cc.autoStart = false;
    g_client = std::make_unique<daemon::DaemonClient>(cc);

    std::cout << "Waiting for search engine readiness...\n";
    if (!waitForSearchEngineReady(std::chrono::seconds(60))) {
        std::cerr << "WARNING: Search engine not ready after 60s.\n";
    }

    if (config.enableEmbeddings) {
        auto timeout = std::chrono::milliseconds(600000);
        if (const char* env = std::getenv("YAMS_BENCH_VECTOR_READY_WAIT_MS")) {
            timeout = std::chrono::milliseconds(
                static_cast<std::chrono::milliseconds::rep>(std::stoll(env)));
        }
        std::cout << "Waiting for vector DB initialization...\n";
        if (!waitForVectorDbInitialized(timeout)) {
            std::cerr << "WARNING: Vector DB not initialized after " << timeout.count() << "ms.\n";
            std::cerr
                << "         (If this persists, check plugin autoload/model provider logs.)\n";
        }
    }

    std::cout << "Daemon started successfully\n\n";
}

void TeardownHarness() {
    g_collector.stop();
    g_client.reset();
    g_harness.reset();
    g_activeConfig = {};
    ::unsetenv("YAMS_TUNING_PROFILE");
    ::unsetenv("YAMS_BENCH_ENABLE_EMBEDDINGS");
    ::unsetenv("YAMS_BENCH_EMBED_PROFILE");
}

// Wait for all queues to drain
enum class DrainScope {
    Full,
    PostEmbed,
    None,
};

struct DrainSnapshot {
    uint64_t totalQueued{0};
    uint64_t totalInflight{0};
    uint64_t postQueued{0};
    uint64_t postInflight{0};
    uint64_t embedQueued{0};
    uint64_t embedInflight{0};
    uint64_t kgQueued{0};
    uint64_t kgInflight{0};
    uint64_t symbolQueued{0};
    uint64_t symbolInflight{0};
    uint64_t entityQueued{0};
    uint64_t entityInflight{0};
    uint64_t titleQueued{0};
    uint64_t titleInflight{0};
    int stableCount{0};
    int stableRequired{0};
    uint64_t polls{0};
    bool hitTimeout{false};
    bool hitHardTimeout{false};
};

DrainScope getDrainScope() {
    if (const char* env = std::getenv("YAMS_BENCH_DRAIN_SCOPE")) {
        const std::string v(env);
        if (v == "none")
            return DrainScope::None;
        if (v == "post_embed")
            return DrainScope::PostEmbed;
        if (v == "full")
            return DrainScope::Full;
    }
    return DrainScope::Full;
}

int getDrainStableRequired() {
    int stableRequired = 10;
    if (const char* env = std::getenv("YAMS_BENCH_DRAIN_STABLE_REQUIRED")) {
        try {
            stableRequired = std::max(1, std::stoi(env));
        } catch (...) {
        }
    }
    return stableRequired;
}

bool waitForDrain(std::chrono::milliseconds timeout, bool embeddingsEnabled,
                  std::optional<std::chrono::steady_clock::time_point> hardDeadline = std::nullopt,
                  bool* hardTimedOut = nullptr, DrainSnapshot* snapshotOut = nullptr) {
    if (hardTimedOut) {
        *hardTimedOut = false;
    }

    const auto scope = getDrainScope();
    if (scope == DrainScope::None) {
        return true;
    }

    auto deadline = std::chrono::steady_clock::now() + timeout;
    int stableCount = 0;
    const int stableRequired = getDrainStableRequired();
    DrainSnapshot snapshot;
    snapshot.stableRequired = stableRequired;
    auto nextLog = std::chrono::steady_clock::now() + std::chrono::seconds(5);

    while (std::chrono::steady_clock::now() < deadline) {
        snapshot.polls += 1;
        if (hardDeadline && std::chrono::steady_clock::now() >= *hardDeadline) {
            if (hardTimedOut) {
                *hardTimedOut = true;
            }
            snapshot.hitHardTimeout = true;
            snapshot.hitTimeout = true;
            snapshot.stableCount = stableCount;
            if (snapshotOut) {
                *snapshotOut = snapshot;
            }
            std::cerr << "WARNING: Drain wait hit phase hard-timeout\n";
            return false;
        }

        auto status = cli::run_sync(g_client->status(), std::chrono::seconds(5));
        if (!status) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        const auto& st = status.value();

        auto getCount = [&](const std::string& key) -> uint64_t {
            auto it = st.requestCounts.find(key);
            return (it != st.requestCounts.end()) ? it->second : 0;
        };

        uint64_t totalQueued = 0;
        uint64_t totalInflight = 0;

        // For embedding perf sweeps, it's useful to drain only the embed-critical stages.
        // Full drain remains available for end-to-end pipeline benchmarking.
        const uint64_t postQueued = getCount(std::string(metrics::kPostIngestQueued));
        const uint64_t postInflight = getCount(std::string(metrics::kPostIngestInflight));
        totalQueued += postQueued;
        totalInflight += postInflight;

        uint64_t embedQueued = 0;
        uint64_t embedInflight = 0;

        if (embeddingsEnabled) {
            embedQueued = getCount(std::string(metrics::kEmbedQueued));
            embedInflight = getCount(std::string(metrics::kEmbedInflight));
            totalQueued += embedQueued;
            totalInflight += embedInflight;
        }

        uint64_t kgQueued = 0, kgInflight = 0;
        uint64_t symbolQueued = 0, symbolInflight = 0;
        uint64_t entityQueued = 0, entityInflight = 0;
        uint64_t titleQueued = 0, titleInflight = 0;

        if (scope == DrainScope::Full) {
            kgQueued = getCount(std::string(metrics::kKgQueueDepth));
            symbolQueued = getCount(std::string(metrics::kSymbolQueueDepth));
            entityQueued = getCount(std::string(metrics::kEntityQueueDepth));
            titleQueued = getCount(std::string(metrics::kTitleQueueDepth));

            kgInflight = getCount(std::string(metrics::kKgInflight));
            symbolInflight = getCount(std::string(metrics::kSymbolInflight));
            entityInflight = getCount(std::string(metrics::kEntityInflight));
            titleInflight = getCount(std::string(metrics::kTitleInflight));

            totalQueued += kgQueued + symbolQueued + entityQueued + titleQueued;
            totalInflight += kgInflight + symbolInflight + entityInflight + titleInflight;
        }

        snapshot.totalQueued = totalQueued;
        snapshot.totalInflight = totalInflight;
        snapshot.postQueued = postQueued;
        snapshot.postInflight = postInflight;
        snapshot.embedQueued = embedQueued;
        snapshot.embedInflight = embedInflight;
        snapshot.kgQueued = kgQueued;
        snapshot.kgInflight = kgInflight;
        snapshot.symbolQueued = symbolQueued;
        snapshot.symbolInflight = symbolInflight;
        snapshot.entityQueued = entityQueued;
        snapshot.entityInflight = entityInflight;
        snapshot.titleQueued = titleQueued;
        snapshot.titleInflight = titleInflight;

        if (totalQueued == 0 && totalInflight == 0) {
            if (++stableCount >= stableRequired) {
                snapshot.stableCount = stableCount;
                if (snapshotOut) {
                    *snapshotOut = snapshot;
                }
                return true;
            }
        } else {
            stableCount = 0;
        }
        snapshot.stableCount = stableCount;

        auto now = std::chrono::steady_clock::now();
        if (now >= nextLog) {
            std::cerr << "[bench] drain wait: scope="
                      << (scope == DrainScope::Full
                              ? "full"
                              : (scope == DrainScope::PostEmbed ? "post_embed" : "none"))
                      << " queued=" << totalQueued << " inflight=" << totalInflight
                      << " postQ=" << postQueued << " postIn=" << postInflight
                      << " embedQ=" << embedQueued << " embedIn=" << embedInflight;
            if (scope == DrainScope::Full) {
                std::cerr << " kgQ=" << kgQueued << " kgIn=" << kgInflight
                          << " symQ=" << symbolQueued << " symIn=" << symbolInflight
                          << " entQ=" << entityQueued << " entIn=" << entityInflight
                          << " titleQ=" << titleQueued << " titleIn=" << titleInflight;
            }
            std::cerr << " stable=" << stableCount << "/" << stableRequired
                      << " rss_mb=" << std::fixed << std::setprecision(1) << st.memoryUsageMb
                      << " cpu_pct=" << st.cpuUsagePercent << "\n";
            nextLog = now + std::chrono::seconds(5);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    snapshot.hitTimeout = true;
    snapshot.stableCount = stableCount;
    if (snapshotOut) {
        *snapshotOut = snapshot;
    }

    return false;
}

// Generate documents with realistic size distribution
std::vector<std::pair<std::filesystem::path, std::string>>
generateDocuments(const std::filesystem::path& dataDir, size_t count, double duplicationRate) {
    TestDataGenerator generator;
    std::vector<std::pair<std::filesystem::path, std::string>> docs;
    docs.reserve(count);

    // Power-law size distribution
    std::vector<size_t> sizeBuckets = {1024, 4096, 16384, 65536, 262144};
    std::vector<double> sizeWeights = {0.40, 0.30, 0.20, 0.08, 0.02};

    std::random_device rd;
    std::mt19937 gen(rd());
    std::discrete_distribution<> sizeDist(sizeWeights.begin(), sizeWeights.end());
    std::bernoulli_distribution dupDist(duplicationRate);

    for (size_t i = 0; i < count; ++i) {
        std::string filename = "bench_doc_" + std::to_string(i) + ".txt";
        auto path = dataDir / filename;

        // Apply duplication: reuse bytes from an existing file (no in-memory corpus).
        if (!docs.empty() && dupDist(gen)) {
            std::uniform_int_distribution<size_t> pick(0, docs.size() - 1);
            const auto& srcPath = docs[pick(gen)].first;
            std::error_code ec;
            std::filesystem::copy_file(srcPath, path,
                                       std::filesystem::copy_options::overwrite_existing, ec);
            if (ec) {
                // Fallback: read/write if copy_file fails (e.g., cross-device quirks).
                std::ifstream ifs(srcPath, std::ios::binary);
                std::ofstream ofs(path, std::ios::binary);
                ofs << ifs.rdbuf();
            }
        } else {
            // Generate new document
            size_t size = sizeBuckets[sizeDist(gen)];
            std::string content = generator.generateTextDocument(size, "benchmark");
            std::ofstream ofs(path);
            ofs << content;
        }

        docs.emplace_back(path, filename);
    }

    return docs;
}

// Write time series to CSV
void writeTimeSeriesCsv(const std::vector<TimeSeriesSample>& samples,
                        const std::filesystem::path& path) {
    std::ofstream csv(path);
    csv << "elapsed_ms,docs_ingested,docs_failed,total_bytes,"
        << "post_queued,post_inflight,embed_queued,embed_inflight,"
        << "embed_infer_active,embed_infer_oldest_ms,embed_infer_started,embed_infer_completed,"
        << "embed_infer_last_ms,embed_infer_max_ms,embed_infer_warn_count,"
        << "kg_queued,kg_inflight,symbol_queued,symbol_inflight,"
        << "entity_queued,entity_inflight,title_queued,title_inflight,"
        << "rss_bytes,cpu_percent,pressure_level,"
        << "kg_dropped,symbol_dropped,entity_dropped,title_dropped,bus_post_dropped,"
        << "docs_per_sec,bytes_per_sec\n";

    for (const auto& s : samples) {
        csv << s.elapsedMs << "," << s.docsIngested << "," << s.docsFailed << "," << s.totalBytes
            << "," << s.postIngestQueued << "," << s.postIngestInflight << "," << s.embedQueued
            << "," << s.embedInflight << "," << s.embedInferActive << "," << s.embedInferOldestMs
            << "," << s.embedInferStarted << "," << s.embedInferCompleted << ","
            << s.embedInferLastMs << "," << s.embedInferMaxMs << "," << s.embedInferWarnCount << ","
            << s.kgQueued << "," << s.kgInflight << "," << s.symbolQueued << "," << s.symbolInflight
            << "," << s.entityQueued << "," << s.entityInflight << "," << s.titleQueued << ","
            << s.titleInflight << "," << s.rssBytes << "," << s.cpuPercent << "," << s.pressureLevel
            << "," << s.kgDropped << "," << s.symbolDropped << "," << s.entityDropped << ","
            << s.titleDropped << "," << s.busPostDropped << "," << s.docsPerSecond << ","
            << s.bytesPerSecond << "\n";
    }
}

// Main ingestion benchmark
static void BM_LargeScaleIngestion(benchmark::State& state) {
#ifdef TRACY_ENABLE
    ZoneScopedN("BM_LargeScaleIngestion");
#endif

    // Parse benchmark arguments
    IngestionBenchConfig config;
    config.documentCount = static_cast<size_t>(state.range(0));
    config.enableEmbeddings = state.range(1) != 0;
    switch (state.range(2)) {
        case 1:
            config.tuningProfile = "Efficient";
            break;
        case 2:
            config.tuningProfile = "Balanced";
            break;
        case 3:
            config.tuningProfile = "Aggressive";
            break;
        default:
            config.tuningProfile = "";
            break;
    }
    config.duplicationRate = getDuplicationRate();

    // Setup harness
    SetupHarness(config);

    for (auto _ : state) {
        state.PauseTiming();

        // Clear previous data
        g_collector.clear();
        g_collector.start(std::chrono::milliseconds(1000));
        g_bytesIngested.store(0, std::memory_order_relaxed);

        // Generate documents
        std::cout << "Generating " << config.documentCount << " documents...\n";
        auto docs =
            generateDocuments(g_harness->dataDir(), config.documentCount, config.duplicationRate);

        // Setup ingestion service
        app::services::DocumentIngestionService docSvc;
        app::services::AddOptions opts;
        opts.socketPath = g_harness->socketPath().string();
        opts.explicitDataDir = g_harness->dataDir().string();
        opts.noEmbeddings = !config.enableEmbeddings;

        auto startTime = std::chrono::steady_clock::now();
        auto ingestStartTime = startTime;
        size_t successCount = 0;
        size_t failCount = 0;
        std::unordered_set<std::string> uniqueHashes;
        uniqueHashes.reserve(config.documentCount);
        uint64_t totalBytes = 0;

        state.ResumeTiming();

        ingestStartTime = std::chrono::steady_clock::now();

        // Ingest documents in batches
        const size_t batchSize = config.batchSize;
        for (size_t i = 0; i < docs.size(); i += batchSize) {
            size_t end = std::min(i + batchSize, docs.size());

            for (size_t j = i; j < end; ++j) {
                opts.path = docs[j].first.string();

                auto result = docSvc.addViaDaemon(opts);
                if (result && !result.value().hash.empty()) {
                    ++successCount;
                    uniqueHashes.insert(result.value().hash);
                    // Real bytes (best-effort)
                    std::error_code ec;
                    const uint64_t fileBytes =
                        static_cast<uint64_t>(std::filesystem::file_size(docs[j].first, ec));
                    if (!ec && fileBytes > 0) {
                        totalBytes += fileBytes;
                        g_bytesIngested.fetch_add(fileBytes, std::memory_order_relaxed);
                    }
                } else {
                    ++failCount;
                }
            }

            // Small yield between batches
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        const auto ingestEndTime = std::chrono::steady_clock::now();

        state.PauseTiming();

        // Wait for post-ingest queue to drain
        std::cout << "Waiting for post-ingest pipeline to drain...\n";
        const auto waitPhaseStart = std::chrono::steady_clock::now();
        auto drainTimeout = std::chrono::milliseconds(
            config.enableEmbeddings ? 1200000 : 300000); // 20 min with embeddings, 5 min without
        auto phaseHardTimeout = std::chrono::milliseconds(0);
        if (const char* env = std::getenv("YAMS_BENCH_PHASE_TIMEOUT_MS")) {
            try {
                auto parsed = static_cast<std::chrono::milliseconds::rep>(std::stoll(env));
                if (parsed > 0) {
                    phaseHardTimeout = std::chrono::milliseconds(parsed);
                }
            } catch (...) {
            }
        }
        std::optional<std::chrono::steady_clock::time_point> phaseDeadline;
        if (phaseHardTimeout.count() > 0) {
            phaseDeadline = waitPhaseStart + phaseHardTimeout;
            std::cout << "Phase hard-timeout enabled: " << phaseHardTimeout.count() << "ms\n";
        }

        if (const char* env = std::getenv("YAMS_BENCH_DRAIN_WAIT_MS")) {
            try {
                drainTimeout = std::chrono::milliseconds(
                    static_cast<std::chrono::milliseconds::rep>(std::stoll(env)));
            } catch (...) {
            }
        }

        bool hardTimeoutTriggered = false;
        DrainSnapshot drainSnapshot;
        bool drainOk = waitForDrain(drainTimeout, config.enableEmbeddings, phaseDeadline,
                                    &hardTimeoutTriggered, &drainSnapshot);
        if (!drainOk) {
            std::cerr << "WARNING: Pipeline drain timeout\n";
        }

        // Ensure cached counters and indexing are truly caught up before recording final
        // metrics. Ingest can complete far faster than post-ingest/FTS5/vector indexing.
        // IMPORTANT: expected corpus size should be unique document hashes, not attempted adds.
        // Bench data generation can include duplicates (deduped by CAS hash), so using
        // config.documentCount would cause false readiness timeouts.
        const std::size_t expectedUniqueDocs = uniqueHashes.size();
        bool corpusReadyOk = true;
        bool corpusCheckSkipped = false;
        if (const char* env = std::getenv("YAMS_BENCH_SKIP_CORPUS_READY")) {
            if (std::string(env) != "1") {
                if (hardTimeoutTriggered) {
                    corpusCheckSkipped = true;
                    std::cerr << "WARNING: Skipping corpus readiness due to phase hard-timeout\n";
                } else {
                    corpusReadyOk =
                        waitForCorpusIndexed(expectedUniqueDocs, config.enableEmbeddings,
                                             drainTimeout, phaseDeadline, &hardTimeoutTriggered);
                }
            }
        } else {
            if (hardTimeoutTriggered) {
                corpusCheckSkipped = true;
                std::cerr << "WARNING: Skipping corpus readiness due to phase hard-timeout\n";
            } else {
                corpusReadyOk =
                    waitForCorpusIndexed(expectedUniqueDocs, config.enableEmbeddings, drainTimeout,
                                         phaseDeadline, &hardTimeoutTriggered);
            }
        }

        const auto waitPhaseEnd = std::chrono::steady_clock::now();
        const auto waitPhaseDurationMs =
            std::chrono::duration_cast<std::chrono::milliseconds>(waitPhaseEnd - waitPhaseStart)
                .count();

        g_collector.stop();
        auto endTime = std::chrono::steady_clock::now();
        auto totalDuration =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        auto ingestDuration =
            std::chrono::duration_cast<std::chrono::milliseconds>(ingestEndTime - ingestStartTime);

        // Calculate metrics
        double throughput = 0.0;
        if (totalDuration.count() > 0) {
            throughput = static_cast<double>(successCount) * 1000.0 / totalDuration.count();
        }
        double ingestThroughput = 0.0;
        if (ingestDuration.count() > 0) {
            ingestThroughput = static_cast<double>(successCount) * 1000.0 / ingestDuration.count();
        }
        double bytesPerSec = 0.0;
        if (totalDuration.count() > 0) {
            bytesPerSec = static_cast<double>(totalBytes) * 1000.0 / totalDuration.count();
        }
        double ingestBytesPerSec = 0.0;
        if (ingestDuration.count() > 0) {
            ingestBytesPerSec = static_cast<double>(totalBytes) * 1000.0 / ingestDuration.count();
        }

        // Get final stats
        auto samples = g_collector.getSamples();

        // Find peak values
        uint64_t peakPostQueued = 0, peakEmbedQueued = 0;
        uint64_t peakEmbedInflight = 0;
        uint64_t peakEmbedInferActive = 0;
        uint64_t peakEmbedInferOldestMs = 0;
        uint64_t peakEmbedInferLastMs = 0;
        uint64_t peakEmbedInferMaxMs = 0;
        uint64_t peakRss = 0;
        double maxCpu = 0.0;
        int maxPressure = 0;
        double embedBacklogSeconds = 0.0;
        double embedBacklogSingleWorkerSeconds = 0.0;

        uint64_t endEmbedQueued = 0;
        uint64_t endEmbedInflight = 0;
        uint64_t endEmbedInferActive = 0;
        uint64_t endEmbedInferOldestMs = 0;
        uint64_t endEmbedInferLastMs = 0;
        uint64_t endEmbedInferMaxMs = 0;
        uint64_t endEmbedInferStarted = 0;
        uint64_t endEmbedInferCompleted = 0;
        uint64_t endEmbedInferWarnCount = 0;
        uint64_t firstEmbedInferStarted = 0;
        uint64_t firstEmbedInferCompleted = 0;
        uint64_t firstEmbedInferWarnCount = 0;
        bool firstSeen = false;

        uint64_t prevElapsedMs = 0;
        for (const auto& s : samples) {
            peakPostQueued = std::max(peakPostQueued, s.postIngestQueued);
            peakEmbedQueued = std::max(peakEmbedQueued, s.embedQueued);
            peakEmbedInflight = std::max(peakEmbedInflight, s.embedInflight);
            peakEmbedInferActive = std::max(peakEmbedInferActive, s.embedInferActive);
            peakEmbedInferOldestMs = std::max(peakEmbedInferOldestMs, s.embedInferOldestMs);
            peakEmbedInferLastMs = std::max(peakEmbedInferLastMs, s.embedInferLastMs);
            peakEmbedInferMaxMs = std::max(peakEmbedInferMaxMs, s.embedInferMaxMs);
            peakRss = std::max(peakRss, s.rssBytes);
            maxCpu = std::max(maxCpu, s.cpuPercent);
            maxPressure = std::max(maxPressure, s.pressureLevel);

            if (!firstSeen) {
                firstSeen = true;
                firstEmbedInferStarted = s.embedInferStarted;
                firstEmbedInferCompleted = s.embedInferCompleted;
                firstEmbedInferWarnCount = s.embedInferWarnCount;
            }

            uint64_t deltaMs = (s.elapsedMs > prevElapsedMs) ? (s.elapsedMs - prevElapsedMs) : 0;
            prevElapsedMs = s.elapsedMs;
            if (deltaMs > 0 && s.embedQueued > 0) {
                embedBacklogSeconds += static_cast<double>(deltaMs) / 1000.0;
                if (s.embedInflight <= 1) {
                    embedBacklogSingleWorkerSeconds += static_cast<double>(deltaMs) / 1000.0;
                }
            }

            endEmbedQueued = s.embedQueued;
            endEmbedInflight = s.embedInflight;
            endEmbedInferActive = s.embedInferActive;
            endEmbedInferOldestMs = s.embedInferOldestMs;
            endEmbedInferLastMs = s.embedInferLastMs;
            endEmbedInferMaxMs = s.embedInferMaxMs;
            endEmbedInferStarted = s.embedInferStarted;
            endEmbedInferCompleted = s.embedInferCompleted;
            endEmbedInferWarnCount = s.embedInferWarnCount;
        }

        const uint64_t inferStartedDelta = (endEmbedInferStarted >= firstEmbedInferStarted)
                                               ? (endEmbedInferStarted - firstEmbedInferStarted)
                                               : 0;
        const uint64_t inferCompletedDelta =
            (endEmbedInferCompleted >= firstEmbedInferCompleted)
                ? (endEmbedInferCompleted - firstEmbedInferCompleted)
                : 0;
        const uint64_t inferWarnDelta = (endEmbedInferWarnCount >= firstEmbedInferWarnCount)
                                            ? (endEmbedInferWarnCount - firstEmbedInferWarnCount)
                                            : 0;

        // Output counters
        state.counters["docs_total"] = static_cast<double>(config.documentCount);
        state.counters["docs_success"] = static_cast<double>(successCount);
        state.counters["docs_failed"] = static_cast<double>(failCount);
        state.counters["docs_unique"] = static_cast<double>(expectedUniqueDocs);
        state.counters["throughput_dps"] = throughput;
        state.counters["ingest_throughput_dps"] = ingestThroughput;
        state.counters["bytes_total"] = static_cast<double>(totalBytes);
        state.counters["throughput_Bps"] = bytesPerSec;
        state.counters["ingest_throughput_Bps"] = ingestBytesPerSec;
        state.counters["duration_ms"] = static_cast<double>(totalDuration.count());
        state.counters["ingest_duration_ms"] = static_cast<double>(ingestDuration.count());
        state.counters["peak_post_queued"] = static_cast<double>(peakPostQueued);
        state.counters["peak_embed_queued"] = static_cast<double>(peakEmbedQueued);
        state.counters["peak_embed_inflight"] = static_cast<double>(peakEmbedInflight);
        state.counters["peak_embed_infer_active"] = static_cast<double>(peakEmbedInferActive);
        state.counters["peak_embed_infer_oldest_ms"] = static_cast<double>(peakEmbedInferOldestMs);
        state.counters["peak_embed_infer_last_ms"] = static_cast<double>(peakEmbedInferLastMs);
        state.counters["peak_embed_infer_max_ms"] = static_cast<double>(peakEmbedInferMaxMs);
        state.counters["peak_rss_mb"] = static_cast<double>(peakRss / (1024 * 1024));
        state.counters["max_cpu_pct"] = maxCpu;
        state.counters["max_pressure"] = static_cast<double>(maxPressure);
        state.counters["embed_backlog_sec"] = embedBacklogSeconds;
        state.counters["embed_backlog_single_worker_sec"] = embedBacklogSingleWorkerSeconds;
        state.counters["end_embed_queued"] = static_cast<double>(endEmbedQueued);
        state.counters["end_embed_inflight"] = static_cast<double>(endEmbedInflight);
        state.counters["end_embed_infer_active"] = static_cast<double>(endEmbedInferActive);
        state.counters["end_embed_infer_oldest_ms"] = static_cast<double>(endEmbedInferOldestMs);
        state.counters["end_embed_infer_last_ms"] = static_cast<double>(endEmbedInferLastMs);
        state.counters["end_embed_infer_max_ms"] = static_cast<double>(endEmbedInferMaxMs);
        state.counters["embed_infer_started_delta"] = static_cast<double>(inferStartedDelta);
        state.counters["embed_infer_completed_delta"] = static_cast<double>(inferCompletedDelta);
        state.counters["embed_infer_warn_delta"] = static_cast<double>(inferWarnDelta);
        state.counters["drain_last_total_queued"] = static_cast<double>(drainSnapshot.totalQueued);
        state.counters["drain_last_total_inflight"] =
            static_cast<double>(drainSnapshot.totalInflight);
        state.counters["drain_last_embed_queued"] = static_cast<double>(drainSnapshot.embedQueued);
        state.counters["drain_last_embed_inflight"] =
            static_cast<double>(drainSnapshot.embedInflight);
        state.counters["drain_last_post_queued"] = static_cast<double>(drainSnapshot.postQueued);
        state.counters["drain_last_post_inflight"] =
            static_cast<double>(drainSnapshot.postInflight);
        state.counters["drain_poll_count"] = static_cast<double>(drainSnapshot.polls);
        state.counters["drain_stable_count"] = static_cast<double>(drainSnapshot.stableCount);
        state.counters["drain_hard_timeout"] = drainSnapshot.hitHardTimeout ? 1.0 : 0.0;
        state.counters["wait_phase_ms"] = static_cast<double>(waitPhaseDurationMs);
        state.counters["drain_timeout"] = drainOk ? 0.0 : 1.0;
        state.counters["corpus_timeout"] = corpusReadyOk ? 0.0 : 1.0;
        state.counters["phase_timeout"] = hardTimeoutTriggered ? 1.0 : 0.0;
        state.counters["corpus_check_skipped"] = corpusCheckSkipped ? 1.0 : 0.0;

        // Write time series CSV
        std::filesystem::path outDir = g_harness->dataDir().parent_path();
        if (const char* env = std::getenv("YAMS_BENCH_OUT_DIR")) {
            if (std::string(env).size() > 0) {
                outDir = std::filesystem::path(env);
            }
        }
        std::error_code outEc;
        std::filesystem::create_directories(outDir, outEc);

        std::string fileName = "ingestion_timeseries_" + std::to_string(config.documentCount);
        if (const char* env = std::getenv("YAMS_BENCH_RUN_ID")) {
            const std::string runId(env);
            if (!runId.empty()) {
                fileName += "_" + runId;
            }
        }
        fileName += ".csv";

        std::filesystem::path csvPath = outDir / fileName;
        writeTimeSeriesCsv(samples, csvPath);
        std::cout << "Time series written to: " << csvPath << "\n";

        std::cout << "Completed: " << successCount << "/" << config.documentCount << " docs in "
                  << (totalDuration.count() / 1000.0) << "s"
                  << " (" << throughput << " docs/sec, " << (bytesPerSec / (1024.0 * 1024.0))
                  << " MiB/sec)\n";

        // Cleanup documents
        for (const auto& doc : docs) {
            std::error_code ec;
            std::filesystem::remove(doc.first, ec);
        }

        // Ensure benchmark timing is running when the iteration ends.
        // Google Benchmark asserts if an iteration finishes with timing paused.
        state.ResumeTiming();
    }
}

// Benchmark configurations
// Args: documentCount, enableEmbeddings(0/1),
// profile(0=default,1=Efficient,2=Balanced,3=Aggressive)

// Tier 0: Embedding smoke test (tiny corpus; completes even on slow inference)
BENCHMARK(BM_LargeScaleIngestion)
    ->Args({200, 0, 2}) // 200, no embeddings, Balanced
    ->Args({200, 1, 2}) // 200, with embeddings, Balanced
    ->Args({100, 1, 2}) // 100, with embeddings, Balanced
    ->Args({10, 1, 2})  // 10, with embeddings, Balanced
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1);

// Tier 1: Quick validation (10K docs, ~2-5 min)
BENCHMARK(BM_LargeScaleIngestion)
    ->Args({10000, 0, 0}) // 10K, no embeddings, default
    ->Args({10000, 0, 2}) // 10K, no embeddings, Balanced
    ->Args({10000, 1, 2}) // 10K, with embeddings, Balanced
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1);

// Tier 2: Standard benchmark (100K docs, ~15-30 min)
BENCHMARK(BM_LargeScaleIngestion)
    ->Args({100000, 0, 2}) // 100K, no embeddings, Balanced
    ->Args({100000, 0, 3}) // 100K, no embeddings, Aggressive
    ->Args({100000, 1, 3}) // 100K, with embeddings, Aggressive
    ->Unit(benchmark::kMillisecond)
    ->Iterations(1);

// Tier 3: Stress test (1M docs, ~2-4 hours) - commented out by default
// BENCHMARK(BM_LargeScaleIngestion)
//     ->Args({1000000, 0, 3})  // 1M, no embeddings, Aggressive
//     ->Args({1000000, 1, 3})  // 1M, with embeddings, Aggressive
//     ->Unit(benchmark::kMillisecond)
//     ->Iterations(1);

} // anonymous namespace

int main(int argc, char** argv) {
    const bool listOnly = [&]() {
        for (int i = 1; i < argc; ++i) {
            if (std::string(argv[i]) == "--benchmark_list_tests")
                return true;
        }
        return false;
    }();

    std::cout << "\n";
    std::cout << "╔═══════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  YAMS Large-Scale Ingestion Benchmarks                           ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════════════╝\n";
    std::cout << "\nEnvironment Variables:\n";
    std::cout << "  YAMS_BENCH_DOC_COUNT=N          Override document count\n";
    std::cout << "  YAMS_BENCH_ENABLE_EMBEDDINGS=1  Enable embedding generation\n";
    std::cout << "  YAMS_TUNING_PROFILE=NAME        Set profile (Efficient/Balanced/Aggressive)\n";
    std::cout << "  YAMS_BENCH_EMBED_PROFILE=NAME   Embedding profile (safe|balanced)\n";
    std::cout << "  YAMS_BENCH_DUPLICATION_RATE=0.05 Set duplication rate (0.0-1.0)\n";
    std::cout << "  YAMS_BENCH_DRAIN_SCOPE=full|post_embed|none\n";
    std::cout << "  YAMS_BENCH_DRAIN_WAIT_MS=N          Override drain wait timeout\n";
    std::cout << "  YAMS_BENCH_DRAIN_STABLE_REQUIRED=N  Stable samples needed for drain\n";
    std::cout << "  YAMS_BENCH_CORPUS_STABLE_REQUIRED=N Stable samples needed for corpus ready\n";
    std::cout << "  YAMS_BENCH_SKIP_CORPUS_READY=1      Skip corpus indexed readiness wait\n";
    std::cout << "  YAMS_BENCH_SKIP_DEFAULT_SETUP=1     Don't pre-start daemon before benchmarks\n";
    std::cout
        << "  YAMS_BENCH_ENABLE_SEARCH_REBUILDS=1 Allow search engine rebuilds during bench\n";
    std::cout << "  YAMS_BENCH_OUT_DIR=PATH             Write CSV time series into PATH\n";
    std::cout << "  YAMS_BENCH_RUN_ID=ID                Suffix for CSV filename\n";
    std::cout << "\nEmbedding Chunk Tuning (daemon env):\n";
    std::cout
        << "  "
           "YAMS_EMBED_CHUNK_STRATEGY=fixed|sentence|paragraph|recursive|sliding_window|markdown\n";
    std::cout << "  YAMS_EMBED_CHUNK_TARGET=N        Target size (chars by default; tokens if "
                 "USE_TOKENS=1)\n";
    std::cout << "  YAMS_EMBED_CHUNK_MIN=N           Minimum chunk size\n";
    std::cout << "  YAMS_EMBED_CHUNK_MAX=N           Maximum chunk size\n";
    std::cout << "  YAMS_EMBED_CHUNK_OVERLAP=N       Overlap size (0 disables)\n";
    std::cout << "  YAMS_EMBED_CHUNK_OVERLAP_PCT=F   Overlap percentage (0 disables)\n";
    std::cout
        << "  YAMS_EMBED_CHUNK_USE_TOKENS=0/1  Interpret sizes as tokens (roughly 4 chars/token)\n";
    std::cout << "  YAMS_EMBED_CHUNK_PRESERVE_SENTENCES=0/1\n";
    std::cout << "  YAMS_EMBED_DEBUG_TIMINGS=1       Log per-job chunk/infer timings\n";
    std::cout << "  YAMS_EMBED_TIMING_WARN_MS=5000   Also log when phases exceed this\n";
    std::cout << "\nTiers:\n";
    std::cout << "  Tier 0 (10 docs):    Embedding smoke test (~1-3 min)\n";
    std::cout << "  Tier 1 (10K docs):   Quick validation (~2-5 min)\n";
    std::cout << "  Tier 2 (100K docs):  Standard benchmark (~15-30 min)\n";
    std::cout << "  Tier 3 (1M docs):    Stress test (~2-4 hours, manual enable)\n";
    std::cout << "\nMetrics:\n";
    std::cout << "  - Throughput: docs/sec\n";
    std::cout << "  - Queue depths: Peak queued/inflight per stage\n";
    std::cout << "  - Resource: Peak RSS, CPU%, pressure level\n";
    std::cout << "  - Time series: CSV output for plotting\n\n";

    // When only listing tests, avoid starting the daemon (expensive, and can trigger
    // model warmup/shutdown races in plugin backends).
    if (!listOnly) {
        // Setup default harness (will be reconfigured per benchmark)
        if (const char* env = std::getenv("YAMS_BENCH_SKIP_DEFAULT_SETUP")) {
            if (std::string(env) != "1") {
                IngestionBenchConfig defaultConfig;
                defaultConfig.documentCount = getDocCount();
                defaultConfig.enableEmbeddings = getEnableEmbeddings();
                defaultConfig.tuningProfile = getTuningProfile();
                SetupHarness(defaultConfig);
            }
        } else {
            IngestionBenchConfig defaultConfig;
            defaultConfig.documentCount = getDocCount();
            defaultConfig.enableEmbeddings = getEnableEmbeddings();
            defaultConfig.tuningProfile = getTuningProfile();
            SetupHarness(defaultConfig);
        }
    }

    // Run benchmarks
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        if (!listOnly) {
            TeardownHarness();
        }
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();

    // Cleanup
    if (!listOnly) {
        TeardownHarness();
    }

    std::cout << "\nLarge-scale ingestion benchmarks completed\n";
    std::cout << "Review throughput and queue metrics above\n";
    std::cout << "Time series CSV files available for analysis\n\n";

    return 0;
}
