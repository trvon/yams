/*
  ingestion_e2e_bench.cpp - End-to-End Ingestion Pipeline Benchmark (PBI-004)

  Measures complete ingestion pipeline performance with per-stage timing,
  queue depth monitoring, and throughput characterization.

  Usage:
    YAMS_TEST_SAFE_SINGLE_INSTANCE=1 ./builddir/tests/benchmarks/ingestion_e2e_bench

  Environment variables:
    YAMS_TEST_SAFE_SINGLE_INSTANCE=1  - Required to prevent GlobalIOContext reset
    YAMS_BENCH_CORPUS_SIZE=N          - Number of documents to ingest (default: 100)
    YAMS_BENCH_DOC_SIZE=N             - Document size in bytes (default: 1000)
    YAMS_BENCH_POLL_INTERVAL_MS=N     - Queue polling interval (default: 100)
    YAMS_BENCH_OUTPUT=path            - JSON output file (default: stdout)

  Metrics collected:
    - Total pipeline duration (start → all stages complete)
    - Per-stage latency: metadata, FTS5, embeddings, KG
    - Queue depths over time (store_document_tasks, embed_jobs, fts5_jobs, postIngest)
    - Throughput (docs/sec) and success/fail counts
    - WorkCoordinator thread pool utilization
*/

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include "tests/integration/daemon/test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/daemon.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <yams/compat/unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using json = nlohmann::json;

// ============================================================================
// SimpleDaemonHarness - Minimal daemon lifecycle for benchmarking
// ============================================================================

class SimpleDaemonHarness {
public:
    SimpleDaemonHarness() {
        auto id = randomId();
        root_ = fs::temp_directory_path() / ("yams_e2e_bench_" + id);
        fs::create_directories(root_);
        data_ = root_ / "data";
        fs::create_directories(data_);
        sock_ = fs::path("/tmp") / ("e2e_bench_" + id + ".sock");
        pid_ = root_ / "bench.pid";
        log_ = root_ / "bench.log";
    }

    ~SimpleDaemonHarness() {
        stop();
        cleanup();
    }

    bool start() {
        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = data_;
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = true;

        // Use real model provider with plugins for accurate end-to-end benchmark
        cfg.useMockModelProvider = false;
        cfg.autoLoadPlugins = true;

        // Configure model pool for real embedding generation
        cfg.modelPoolConfig.lazyLoading = false;
        cfg.modelPoolConfig.preloadModels = {"all-MiniLM-L6-v2"};

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);

        auto s = daemon_->start();
        if (!s) {
            spdlog::error("Failed to start daemon: {}", s.error().message);
            return false;
        }

        auto deadline = std::chrono::steady_clock::now() + 30s;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
                spdlog::info("Daemon ready at socket: {}", sock_.string());
                return true;
            }
            if (lifecycle.state == yams::daemon::LifecycleState::Failed) {
                spdlog::error("Daemon failed to start");
                return false;
            }
        }
        spdlog::error("Daemon startup timeout");
        return false;
    }

    void stop() {
        if (daemon_) {
            spdlog::info("[SimpleDaemonHarness] Stopping daemon (running={})...",
                         daemon_->isRunning());

            auto stopResult = daemon_->stop();
            if (!stopResult) {
                spdlog::warn("[SimpleDaemonHarness] Daemon stop returned error: {}",
                             stopResult.error().message);
            }

            // Wait for daemon to report it's no longer running
            int isRunningRetries = 0;
            while (daemon_->isRunning() && isRunningRetries < 50) {
                std::this_thread::sleep_for(10ms);
                isRunningRetries++;
            }
            if (isRunningRetries > 0) {
                spdlog::info("[SimpleDaemonHarness] Waited {}ms for isRunning() to become false",
                             isRunningRetries * 10);
            }

            spdlog::info("[SimpleDaemonHarness] Daemon stopped (running={}), resetting instance...",
                         daemon_->isRunning());

            // Reset GlobalIOContext to clean up threads and io_context state
            // Temporarily unset YAMS_TESTING to allow reset() to actually work
            const char* yams_testing = std::getenv("YAMS_TESTING");
            const char* yams_safe = std::getenv("YAMS_TEST_SAFE_SINGLE_INSTANCE");
            if (yams_testing) {
                unsetenv("YAMS_TESTING");
            }
            if (yams_safe) {
                unsetenv("YAMS_TEST_SAFE_SINGLE_INSTANCE");
            }

            yams::daemon::GlobalIOContext::reset();
            spdlog::info("[SimpleDaemonHarness] GlobalIOContext reset complete");

            // Restore environment variables
            if (yams_testing) {
                setenv("YAMS_TESTING", yams_testing, 1);
            }
            if (yams_safe) {
                setenv("YAMS_TEST_SAFE_SINGLE_INSTANCE", yams_safe, 1);
            }

            daemon_.reset();

            // Allow OS to fully release thread resources (macOS needs this)
            // Real model provider + plugins create additional threads beyond base daemon
            std::this_thread::sleep_for(500ms);
        }
    }

    const fs::path& socketPath() const { return sock_; }
    const fs::path& root() const { return root_; }
    const fs::path& dataDir() const { return data_; }
    yams::daemon::YamsDaemon* daemon() const { return daemon_.get(); }

private:
    static std::string randomId() {
        static const char* cs = "abcdefghijklmnopqrstuvwxyz0123456789";
        thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, 35);
        std::string out;
        for (int i = 0; i < 8; ++i)
            out.push_back(cs[dist(rng)]);
        return out;
    }

    void cleanup() {
        std::error_code ec;
        if (!root_.empty())
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<yams::daemon::YamsDaemon> daemon_;
    fs::path root_, data_, sock_, pid_, log_;
};

// ============================================================================
// CorpusGenerator - Generate synthetic documents for benchmarking
// ============================================================================

struct CorpusGenerator {
    fs::path corpusDir;
    std::vector<std::string> createdFiles;
    std::mt19937 rng{42}; // Fixed seed for reproducibility
    size_t docSize;

    CorpusGenerator(const fs::path& dir, size_t size) : corpusDir(dir), docSize(size) {
        fs::create_directories(corpusDir);
    }

    void generateDocuments(int count) {
        spdlog::info("Generating {} documents of ~{} bytes each", count, docSize);
        std::uniform_int_distribution<int> wordDist(0, 999);

        for (int i = 0; i < count; ++i) {
            std::ostringstream content;
            content << "Document " << i << " - Synthetic Test Corpus\n\n";

            // Generate content to approximately match docSize
            size_t written = content.str().size();
            while (written < docSize) {
                content << "word" << wordDist(rng) << " ";
                written += 10; // Approximate word length
                if (written % 100 == 0) {
                    content << "\n";
                }
            }

            std::string filename = "doc_" + std::to_string(i) + ".txt";
            std::ofstream(corpusDir / filename) << content.str();
            createdFiles.push_back(filename);

            if ((i + 1) % 10 == 0 || (i + 1) == count) {
                spdlog::debug("Generated {}/{} documents", i + 1, count);
            }
        }
        spdlog::info("Corpus generation complete: {} files", createdFiles.size());
    }
};

// ============================================================================
// QueueSnapshot - Capture queue depths and counters at a point in time
// Uses InternalEventBus::instance() to access daemon-internal state
// ============================================================================

struct QueueSnapshot {
    int64_t timestamp_ms;
    // Queue depths (actual items in queues)
    uint64_t store_document_tasks;
    uint64_t embed_jobs;
    uint64_t fts5_jobs;
    uint64_t post_ingest;
    // Cumulative counters (for stage completion detection)
    uint64_t embed_queued;
    uint64_t embed_consumed;
    uint64_t embed_dropped;
    uint64_t fts5_queued;
    uint64_t fts5_consumed;
    uint64_t fts5_dropped;
    uint64_t post_queued;
    uint64_t post_consumed;
    uint64_t post_dropped;

    json toJson() const {
        return json{
            {"timestamp_ms", timestamp_ms},
            {"queue_depths",
             {{"store_document_tasks", store_document_tasks},
              {"embed_jobs", embed_jobs},
              {"fts5_jobs", fts5_jobs},
              {"post_ingest", post_ingest}}},
            {"counters",
             {{"embed",
               {{"queued", embed_queued},
                {"consumed", embed_consumed},
                {"dropped", embed_dropped}}},
              {"fts5",
               {{"queued", fts5_queued}, {"consumed", fts5_consumed}, {"dropped", fts5_dropped}}},
              {"post",
               {{"queued", post_queued},
                {"consumed", post_consumed},
                {"dropped", post_dropped}}}}}};
    }
};

// ============================================================================
// StageMetrics - Per-stage completion tracking
// ============================================================================

struct StageMetrics {
    std::string name;
    int64_t start_ms = 0;
    int64_t end_ms = 0;
    uint64_t count = 0;
    uint64_t failures = 0;

    int64_t duration_ms() const { return end_ms > start_ms ? end_ms - start_ms : 0; }

    json toJson() const {
        return json{{"name", name},
                    {"duration_ms", duration_ms()},
                    {"count", count},
                    {"failures", failures}};
    }
};

// ============================================================================
// BenchmarkResult - Complete benchmark output
// ============================================================================

struct BenchmarkResult {
    // Test configuration
    int corpus_size = 0;
    int doc_size = 0;
    int poll_interval_ms = 0;
    std::string timestamp;

    // Overall metrics
    int64_t total_duration_ms = 0;
    double throughput_docs_per_sec = 0.0;

    // Per-stage metrics
    StageMetrics metadata_storage;
    StageMetrics fts5_extraction;
    StageMetrics embedding_generation;
    StageMetrics kg_extraction;

    // Queue monitoring
    std::vector<QueueSnapshot> queue_samples;
    uint64_t max_store_document_tasks = 0;
    uint64_t max_embed_jobs = 0;
    uint64_t max_fts5_jobs = 0;
    uint64_t max_post_ingest = 0;
    uint64_t dropped_batches = 0;

    // WorkCoordinator metrics
    size_t thread_count = 0;

    json toJson() const {
        json j;
        j["test_config"] = {{"corpus_size", corpus_size},
                            {"doc_size", doc_size},
                            {"poll_interval_ms", poll_interval_ms},
                            {"timestamp", timestamp}};

        j["total_duration_ms"] = total_duration_ms;
        j["throughput_docs_per_sec"] = throughput_docs_per_sec;

        j["stages"] = {{"metadata_storage", metadata_storage.toJson()},
                       {"fts5_extraction", fts5_extraction.toJson()},
                       {"embedding_generation", embedding_generation.toJson()},
                       {"kg_extraction", kg_extraction.toJson()}};

        j["queues"] = {{"max_store_document_tasks", max_store_document_tasks},
                       {"max_embed_jobs", max_embed_jobs},
                       {"max_fts5_jobs", max_fts5_jobs},
                       {"max_post_ingest", max_post_ingest},
                       {"dropped_batches", dropped_batches},
                       {"sample_count", queue_samples.size()}};

        // Include first/last 5 samples for debugging
        if (!queue_samples.empty()) {
            json samples_head = json::array();
            json samples_tail = json::array();
            for (size_t i = 0; i < std::min<size_t>(5, queue_samples.size()); ++i) {
                samples_head.push_back(queue_samples[i].toJson());
            }
            size_t tail_start = queue_samples.size() > 5 ? queue_samples.size() - 5 : 0;
            for (size_t i = tail_start; i < queue_samples.size(); ++i) {
                samples_tail.push_back(queue_samples[i].toJson());
            }
            j["queues"]["samples_head"] = samples_head;
            j["queues"]["samples_tail"] = samples_tail;
        }

        j["work_coordinator"] = {{"thread_count", thread_count}};

        return j;
    }
};

// ============================================================================
// Helper: Get current timestamp in milliseconds
// ============================================================================

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::string nowIso8601() {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);
    std::ostringstream oss;
    oss << std::put_time(std::gmtime(&time_t), "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

// ============================================================================
// captureQueueSnapshot - Read queue depths directly from InternalEventBus
// ============================================================================

QueueSnapshot captureQueueSnapshot() {
    using namespace yams::daemon;
    auto& bus = InternalEventBus::instance();

    QueueSnapshot snap;
    snap.timestamp_ms = nowMs();

    // Get actual queue depths by accessing channels
    if (auto q = bus.get_or_create_channel<InternalEventBus::StoreDocumentTask>(
            "store_document_tasks", 4096)) {
        // Note: SpscQueue doesn't expose size(), so we'll rely on counters for now
        // Queue depth approximation: queued - consumed
        snap.store_document_tasks = 0; // TODO: Need size() method on SpscQueue
    }

    if (auto q = bus.get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs", 4096)) {
        snap.embed_jobs = 0; // TODO: Need size() method
    }

    if (auto q = bus.get_or_create_channel<InternalEventBus::Fts5Job>("fts5_jobs", 4096)) {
        snap.fts5_jobs = 0; // TODO: Need size() method
    }

    if (auto q = bus.get_or_create_channel<InternalEventBus::PostIngestTask>("post_ingest", 4096)) {
        snap.post_ingest = 0; // TODO: Need size() method
    }

    // Get cumulative counters (these work for completion detection)
    snap.embed_queued = bus.embedQueued();
    snap.embed_consumed = bus.embedConsumed();
    snap.embed_dropped = bus.embedDropped();

    snap.fts5_queued = bus.fts5Queued();
    snap.fts5_consumed = bus.fts5Consumed();
    snap.fts5_dropped = bus.fts5Dropped();

    snap.post_queued = bus.postQueued();
    snap.post_consumed = bus.postConsumed();
    snap.post_dropped = bus.postDropped();

    return snap;
}

// ============================================================================
// runBenchmark - Main benchmark logic
// ============================================================================

BenchmarkResult runBenchmark(int corpusSize, int docSize, int pollIntervalMs) {
    BenchmarkResult result;
    result.corpus_size = corpusSize;
    result.doc_size = docSize;
    result.poll_interval_ms = pollIntervalMs;
    result.timestamp = nowIso8601();

    spdlog::info("=== Ingestion E2E Benchmark ===");
    spdlog::info("Corpus size: {}", corpusSize);
    spdlog::info("Doc size: {} bytes", docSize);
    spdlog::info("Poll interval: {} ms", pollIntervalMs);

    // Phase 1: Start daemon
    spdlog::info("Phase 1: Starting daemon...");
    SimpleDaemonHarness harness;
    if (!harness.start()) {
        spdlog::error("Failed to start daemon");
        return result;
    }

    // Phase 2: Generate corpus
    spdlog::info("Phase 2: Generating test corpus...");
    fs::path corpusDir = harness.root() / "corpus";
    CorpusGenerator corpus(corpusDir, docSize);
    corpus.generateDocuments(corpusSize);

    // Phase 3: Connect to daemon
    spdlog::info("Phase 3: Connecting to daemon...");
    yams::daemon::ClientConfig clientConfig;
    clientConfig.socketPath = harness.socketPath();
    clientConfig.autoStart = false; // Daemon already started by harness
    auto client = std::make_shared<yams::daemon::DaemonClient>(clientConfig);

    // Actually connect to the daemon socket
    auto connectResult = yams::cli::run_sync(client->connect(), std::chrono::seconds(5));
    if (!connectResult) {
        spdlog::error("Failed to connect to daemon: {}", connectResult.error().message);
        return result; // Return empty result on connection failure
    }
    spdlog::info("Connected to daemon at {}", harness.socketPath().string());

    // Log model provider status
    auto serviceManager = harness.daemon()->getServiceManager();
    if (serviceManager) {
        auto modelProvider = serviceManager->getModelProvider();
        if (modelProvider && modelProvider->isAvailable()) {
            auto loadedModels = modelProvider->getLoadedModels();
            spdlog::info("Model provider: {} model(s) loaded: {}", loadedModels.size(),
                         loadedModels.empty() ? "none"
                                              : fmt::format("{}", fmt::join(loadedModels, ", ")));
        } else {
            spdlog::warn("Model provider not available");
        }
    }

    // Capture baseline counters (start from zero)
    auto& bus = yams::daemon::InternalEventBus::instance();
    uint64_t baselineEmbedConsumed = bus.embedConsumed();
    uint64_t baselineEmbedDropped = bus.embedDropped();
    uint64_t baselinePostConsumed = bus.postConsumed();
    uint64_t baselinePostDropped = bus.postDropped();
    spdlog::info("Baseline counters: embed={}, post={}", baselineEmbedConsumed,
                 baselinePostConsumed);

    // Phase 4: Ingest documents
    spdlog::info("Phase 4: Ingesting {} documents...", corpusSize);
    int64_t ingestStartMs = nowMs();
    result.metadata_storage.name = "metadata_storage";
    result.metadata_storage.start_ms = ingestStartMs;

    int successCount = 0, failCount = 0;
    for (const auto& filename : corpus.createdFiles) {
        yams::daemon::AddDocumentRequest addReq;
        addReq.path = (corpusDir / filename).string();
        addReq.noEmbeddings = false; // Full pipeline

        auto addResult = yams::cli::run_sync(client->streamingAddDocument(addReq), 30s);
        if (!addResult) {
            spdlog::warn("Failed to ingest {}: {}", filename, addResult.error().message);
            failCount++;
        } else {
            successCount++;
            if (successCount % 10 == 0 || successCount == corpusSize) {
                spdlog::info("Ingested {}/{} documents", successCount, corpusSize);
            }
        }
    }

    int64_t ingestEndMs = nowMs();
    result.metadata_storage.end_ms = ingestEndMs;
    result.metadata_storage.count = successCount;
    result.metadata_storage.failures = failCount;

    spdlog::info("Ingestion complete: {} succeeded, {} failed in {} ms", successCount, failCount,
                 ingestEndMs - ingestStartMs);

    // Phase 5: Monitor pipeline completion via InternalEventBus
    spdlog::info("Phase 5: Monitoring pipeline completion...");

    // Capture initial state
    QueueSnapshot initialSnap = captureQueueSnapshot();
    spdlog::info("Initial state: embed_queued={} fts5_queued={} post_queued={}",
                 initialSnap.embed_queued, initialSnap.fts5_queued, initialSnap.post_queued);

    // Expected counts
    // Note: FTS5 indexing happens synchronously in post-ingest metadata stage (not via jobs)
    // So we only track post-ingest (metadata+FTS5) and embeddings completion
    uint64_t expectedPost = successCount;
    uint64_t expectedEmbed = successCount;

    // Poll until all stages complete or timeout
    auto pipelineDeadline = std::chrono::steady_clock::now() + 60s;
    bool allStagesComplete = false;

    while (std::chrono::steady_clock::now() < pipelineDeadline && !allStagesComplete) {
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));

        QueueSnapshot snap = captureQueueSnapshot();
        result.queue_samples.push_back(snap);

        // Update max queue depths (approximated by queued - consumed)
        // Use signed arithmetic to avoid underflow, then clamp to 0
        auto fts5Depth =
            snap.fts5_queued > snap.fts5_consumed ? snap.fts5_queued - snap.fts5_consumed : 0;
        auto embedDepth =
            snap.embed_queued > snap.embed_consumed ? snap.embed_queued - snap.embed_consumed : 0;
        auto postDepth =
            snap.post_queued > snap.post_consumed ? snap.post_queued - snap.post_consumed : 0;

        result.max_fts5_jobs = std::max(result.max_fts5_jobs, fts5Depth);
        result.max_embed_jobs = std::max(result.max_embed_jobs, embedDepth);
        result.max_post_ingest = std::max(result.max_post_ingest, postDepth);

        // Track dropped batches (excluding warmup baseline)
        uint64_t embedDroppedDelta = snap.embed_dropped > baselineEmbedDropped
                                         ? snap.embed_dropped - baselineEmbedDropped
                                         : 0;
        uint64_t postDroppedDelta =
            snap.post_dropped > baselinePostDropped ? snap.post_dropped - baselinePostDropped : 0;
        result.dropped_batches = snap.fts5_dropped + embedDroppedDelta + postDroppedDelta;

        // Check if all stages have processed all documents (relative to baseline)
        // FTS5 happens in post-ingest, so we only check post and embed completion
        uint64_t embedProcessedDelta = (snap.embed_consumed - baselineEmbedConsumed) +
                                       (snap.embed_dropped - baselineEmbedDropped);
        uint64_t postProcessedDelta =
            (snap.post_consumed - baselinePostConsumed) + (snap.post_dropped - baselinePostDropped);

        bool embedComplete = embedProcessedDelta >= expectedEmbed;
        bool postComplete = postProcessedDelta >= expectedPost;

        allStagesComplete = embedComplete && postComplete;

        if ((snap.timestamp_ms - initialSnap.timestamp_ms) % 1000 < pollIntervalMs) {
            spdlog::info("Pipeline status: post={}/{} (includes FTS5) embed={}/{}",
                         postProcessedDelta, expectedPost, embedProcessedDelta, expectedEmbed);
        }

        if (allStagesComplete) {
            // Update stage metrics (use deltas to exclude warmup)
            int64_t now = nowMs();

            result.fts5_extraction.name = "fts5_extraction";
            result.fts5_extraction.start_ms = ingestStartMs;
            result.fts5_extraction.end_ms = now;
            result.fts5_extraction.count =
                snap.fts5_consumed; // FTS5 counter not affected by warmup
            result.fts5_extraction.failures = snap.fts5_dropped;

            result.embedding_generation.name = "embedding_generation";
            result.embedding_generation.start_ms = ingestStartMs;
            result.embedding_generation.end_ms = now;
            result.embedding_generation.count = snap.embed_consumed - baselineEmbedConsumed;
            result.embedding_generation.failures = snap.embed_dropped - baselineEmbedDropped;

            result.kg_extraction.name = "kg_extraction";
            result.kg_extraction.start_ms = ingestStartMs;
            result.kg_extraction.end_ms = now;
            result.kg_extraction.count = snap.post_consumed - baselinePostConsumed;
            result.kg_extraction.failures = snap.post_dropped - baselinePostDropped;

            spdlog::info("All pipeline stages complete!");
            break;
        }
    }

    if (!allStagesComplete) {
        spdlog::warn("Pipeline completion timeout after 60s");
    }

    int64_t totalEndMs = nowMs();
    result.total_duration_ms = totalEndMs - ingestStartMs;
    result.throughput_docs_per_sec = successCount / (result.total_duration_ms / 1000.0);

    spdlog::info("=== Benchmark Complete ===");
    spdlog::info("Total duration: {} ms", result.total_duration_ms);
    spdlog::info("Throughput: {:.2f} docs/sec", result.throughput_docs_per_sec);
    spdlog::info("Dropped batches: {}", result.dropped_batches);

    return result;
}

// ============================================================================
// main - Entry point
// ============================================================================

int main(int argc, char* argv[]) {
    // Set log level
    const char* logLevel = std::getenv("YAMS_LOG_LEVEL");
    if (logLevel && std::string(logLevel) == "debug") {
        spdlog::set_level(spdlog::level::debug);
    } else {
        spdlog::set_level(spdlog::level::info);
    }

    // Read configuration from environment
    int corpusSize = 100;
    if (const char* env = std::getenv("YAMS_BENCH_CORPUS_SIZE")) {
        corpusSize = std::atoi(env);
    }

    int docSize = 1000;
    if (const char* env = std::getenv("YAMS_BENCH_DOC_SIZE")) {
        docSize = std::atoi(env);
    }

    int pollInterval = 100;
    if (const char* env = std::getenv("YAMS_BENCH_POLL_INTERVAL_MS")) {
        pollInterval = std::atoi(env);
    }

    const char* outputPath = std::getenv("YAMS_BENCH_OUTPUT");

    // Run benchmark
    BenchmarkResult result = runBenchmark(corpusSize, docSize, pollInterval);

    // Output results
    json output = result.toJson();
    std::string jsonStr = output.dump(2);

    if (outputPath) {
        std::ofstream outFile(outputPath);
        outFile << jsonStr << std::endl;
        spdlog::info("Results written to: {}", outputPath);
    } else {
        std::cout << jsonStr << std::endl;
    }

    return result.metadata_storage.failures > 0 ? 1 : 0;
}
