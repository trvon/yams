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
#include <string>
#include <thread>
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

    // Instantaneous throughput (docs/sec in last interval)
    double docsPerSecond{0.0};
    double bytesPerSecond{0.0};
};

// Time series g_collector
class TimeSeriesCollector {
public:
    void start(std::chrono::milliseconds interval = std::chrono::milliseconds(1000)) {
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

            // Pressure level from governor
            if (auto it = st.requestCounts.find("pressure_level"); it != st.requestCounts.end()) {
                sample.pressureLevel = static_cast<int>(it->second);
            }
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

    // Set environment variables
    if (!config.tuningProfile.empty()) {
        ::setenv("YAMS_TUNING_PROFILE", config.tuningProfile.c_str(), 1);
    } else {
        ::unsetenv("YAMS_TUNING_PROFILE");
    }
    ::setenv("YAMS_BENCH_ENABLE_EMBEDDINGS", config.enableEmbeddings ? "1" : "0", 1);

    // Start daemon
    g_harness = std::make_unique<DaemonHarness>();
    if (!g_harness->start(std::chrono::seconds(30))) {
        std::cerr << "ERROR: Failed to start daemon\n";
        std::exit(1);
    }

    // Create client
    daemon::ClientConfig cc;
    cc.socketPath = g_harness->socketPath();
    cc.autoStart = false;
    g_client = std::make_unique<daemon::DaemonClient>(cc);

    std::cout << "Daemon started successfully\n\n";
}

void TeardownHarness() {
    g_collector.stop();
    g_client.reset();
    g_harness.reset();
    g_activeConfig = {};
    ::unsetenv("YAMS_TUNING_PROFILE");
    ::unsetenv("YAMS_BENCH_ENABLE_EMBEDDINGS");
}

// Wait for all queues to drain
bool waitForDrain(std::chrono::milliseconds timeout) {
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

        auto getCount = [&](const std::string& key) -> uint64_t {
            auto it = st.requestCounts.find(key);
            return (it != st.requestCounts.end()) ? it->second : 0;
        };

        uint64_t totalQueued = getCount(std::string(metrics::kPostIngestQueued)) +
                               getCount(std::string(metrics::kEmbedQueued)) +
                               getCount(std::string(metrics::kKgQueueDepth)) +
                               getCount(std::string(metrics::kSymbolQueueDepth)) +
                               getCount(std::string(metrics::kEntityQueueDepth)) +
                               getCount(std::string(metrics::kTitleQueueDepth));

        uint64_t totalInflight = getCount(std::string(metrics::kPostIngestInflight)) +
                                 getCount(std::string(metrics::kEmbedInflight)) +
                                 getCount(std::string(metrics::kKgInflight)) +
                                 getCount(std::string(metrics::kSymbolInflight)) +
                                 getCount(std::string(metrics::kEntityInflight)) +
                                 getCount(std::string(metrics::kTitleInflight));

        if (totalQueued == 0 && totalInflight == 0) {
            if (++stableCount >= stableRequired) {
                return true;
            }
        } else {
            stableCount = 0;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
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
        std::string content;

        // Apply duplication
        if (!docs.empty() && dupDist(gen)) {
            // Copy a previous document
            std::uniform_int_distribution<size_t> pick(0, docs.size() - 1);
            content = generator.generateTextDocument(sizeBuckets[sizeDist(gen)], "duplicated");
        } else {
            // Generate new document
            size_t size = sizeBuckets[sizeDist(gen)];
            content = generator.generateTextDocument(size, "benchmark");
        }

        std::string filename = "bench_doc_" + std::to_string(i) + ".txt";
        auto path = dataDir / filename;

        std::ofstream ofs(path);
        ofs << content;
        ofs.close();

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
        << "kg_queued,kg_inflight,symbol_queued,symbol_inflight,"
        << "entity_queued,entity_inflight,title_queued,title_inflight,"
        << "rss_bytes,cpu_percent,pressure_level,docs_per_sec,bytes_per_sec\n";

    for (const auto& s : samples) {
        csv << s.elapsedMs << "," << s.docsIngested << "," << s.docsFailed << "," << s.totalBytes
            << "," << s.postIngestQueued << "," << s.postIngestInflight << "," << s.embedQueued
            << "," << s.embedInflight << "," << s.kgQueued << "," << s.kgInflight << ","
            << s.symbolQueued << "," << s.symbolInflight << "," << s.entityQueued << ","
            << s.entityInflight << "," << s.titleQueued << "," << s.titleInflight << ","
            << s.rssBytes << "," << s.cpuPercent << "," << s.pressureLevel << "," << s.docsPerSecond
            << "," << s.bytesPerSecond << "\n";
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
        size_t successCount = 0;
        size_t failCount = 0;
        uint64_t totalBytes = 0;

        state.ResumeTiming();

        // Ingest documents in batches
        const size_t batchSize = config.batchSize;
        for (size_t i = 0; i < docs.size(); i += batchSize) {
            size_t end = std::min(i + batchSize, docs.size());

            for (size_t j = i; j < end; ++j) {
                opts.path = docs[j].first.string();

                auto result = docSvc.addViaDaemon(opts);
                if (result && !result.value().hash.empty()) {
                    ++successCount;
                    // Approximate bytes
                    totalBytes += 1024; // Average size estimate
                } else {
                    ++failCount;
                }
            }

            // Small yield between batches
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        state.PauseTiming();

        // Wait for post-ingest queue to drain
        std::cout << "Waiting for post-ingest pipeline to drain...\n";
        auto drainTimeout = std::chrono::milliseconds(
            config.enableEmbeddings ? 1200000 : 300000); // 20 min with embeddings, 5 min without

        if (!waitForDrain(drainTimeout)) {
            std::cerr << "WARNING: Pipeline drain timeout\n";
        }

        g_collector.stop();
        auto endTime = std::chrono::steady_clock::now();
        auto totalDuration =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        // Calculate metrics
        double throughput = 0.0;
        if (totalDuration.count() > 0) {
            throughput = static_cast<double>(successCount) * 1000.0 / totalDuration.count();
        }

        // Get final stats
        auto samples = g_collector.getSamples();

        // Find peak values
        uint64_t peakPostQueued = 0, peakEmbedQueued = 0;
        uint64_t peakRss = 0;
        double maxCpu = 0.0;
        int maxPressure = 0;

        for (const auto& s : samples) {
            peakPostQueued = std::max(peakPostQueued, s.postIngestQueued);
            peakEmbedQueued = std::max(peakEmbedQueued, s.embedQueued);
            peakRss = std::max(peakRss, s.rssBytes);
            maxCpu = std::max(maxCpu, s.cpuPercent);
            maxPressure = std::max(maxPressure, s.pressureLevel);
        }

        // Output counters
        state.counters["docs_total"] = static_cast<double>(config.documentCount);
        state.counters["docs_success"] = static_cast<double>(successCount);
        state.counters["docs_failed"] = static_cast<double>(failCount);
        state.counters["throughput_dps"] = throughput;
        state.counters["duration_ms"] = static_cast<double>(totalDuration.count());
        state.counters["peak_post_queued"] = static_cast<double>(peakPostQueued);
        state.counters["peak_embed_queued"] = static_cast<double>(peakEmbedQueued);
        state.counters["peak_rss_mb"] = static_cast<double>(peakRss / (1024 * 1024));
        state.counters["max_cpu_pct"] = maxCpu;
        state.counters["max_pressure"] = static_cast<double>(maxPressure);

        // Write time series CSV
        std::filesystem::path csvPath =
            g_harness->dataDir().parent_path() /
            ("ingestion_timeseries_" + std::to_string(config.documentCount) + ".csv");
        writeTimeSeriesCsv(samples, csvPath);
        std::cout << "Time series written to: " << csvPath << "\n";

        std::cout << "Completed: " << successCount << "/" << config.documentCount << " docs in "
                  << (totalDuration.count() / 1000.0) << "s"
                  << " (" << throughput << " docs/sec)\n";

        // Cleanup documents
        for (const auto& doc : docs) {
            std::error_code ec;
            std::filesystem::remove(doc.first, ec);
        }
    }
}

// Benchmark configurations
// Args: documentCount, enableEmbeddings(0/1),
// profile(0=default,1=Efficient,2=Balanced,3=Aggressive)

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
    std::cout << "\n";
    std::cout << "╔═══════════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  YAMS Large-Scale Ingestion Benchmarks                           ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════════════╝\n";
    std::cout << "\nEnvironment Variables:\n";
    std::cout << "  YAMS_BENCH_DOC_COUNT=N          Override document count\n";
    std::cout << "  YAMS_BENCH_ENABLE_EMBEDDINGS=1  Enable embedding generation\n";
    std::cout << "  YAMS_TUNING_PROFILE=NAME        Set profile (Efficient/Balanced/Aggressive)\n";
    std::cout << "  YAMS_BENCH_DUPLICATION_RATE=0.05 Set duplication rate (0.0-1.0)\n";
    std::cout << "\nTiers:\n";
    std::cout << "  Tier 1 (10K docs):   Quick validation (~2-5 min)\n";
    std::cout << "  Tier 2 (100K docs):  Standard benchmark (~15-30 min)\n";
    std::cout << "  Tier 3 (1M docs):    Stress test (~2-4 hours, manual enable)\n";
    std::cout << "\nMetrics:\n";
    std::cout << "  - Throughput: docs/sec\n";
    std::cout << "  - Queue depths: Peak queued/inflight per stage\n";
    std::cout << "  - Resource: Peak RSS, CPU%, pressure level\n";
    std::cout << "  - Time series: CSV output for plotting\n\n";

    // Setup default harness (will be reconfigured per benchmark)
    IngestionBenchConfig defaultConfig;
    defaultConfig.documentCount = getDocCount();
    defaultConfig.enableEmbeddings = getEnableEmbeddings();
    defaultConfig.tuningProfile = getTuningProfile();
    SetupHarness(defaultConfig);

    // Run benchmarks
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        TeardownHarness();
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();

    // Cleanup
    TeardownHarness();

    std::cout << "\n✅ Large-scale ingestion benchmarks completed\n";
    std::cout << "💡 Review throughput and queue metrics above\n";
    std::cout << "📊 Time series CSV files available for analysis\n\n";

    return 0;
}
