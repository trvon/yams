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
    YAMS_BENCH_INGEST_MODE=MODE       - directory, pipelined_single_file, or
                                        single_file_serial (default)
    YAMS_BENCH_INGEST_CONCURRENCY=N   - Client count for pipelined_single_file
                                        (default: 4)
    YAMS_BENCH_OUTPUT=path            - JSON output file (default: stdout)
    YAMS_BENCH_DISABLE_KG=1           - Disable KG post-ingest enrichment for ablation
    YAMS_BENCH_SEARCH_PROBES=0        - Disable fixed post-ingest search probes

  Metrics collected:
    - Total pipeline duration (start → all stages complete)
    - Per-stage latency: metadata, FTS5, embeddings, KG
    - Queue depths over time (store_document_tasks, embed_jobs, fts5_jobs, postIngest)
    - Throughput (docs/sec) and success/fail counts
    - WorkCoordinator thread pool utilization
    - Fixed post-ingest keyword/semantic/hybrid search probe impact
*/

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <fmt/ranges.h>

#include "tests/integration/daemon/test_async_helpers.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/api/content_store.h>
#include <yams/app/services/services.hpp>
#include <yams/common/fs_utils.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/reference_counter_writer.h>
#include <yams/vector/simeon_embedding_backend.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <yams/compat/unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using json = nlohmann::json;

namespace {

int64_t nonNegativeDeltaMs(int64_t endMs, int64_t startMs) noexcept {
    return endMs > startMs ? endMs - startMs : 0;
}

uint64_t counterDelta(uint64_t current, uint64_t baseline) noexcept {
    return current > baseline ? current - baseline : 0;
}

uint64_t saturatedAdd(uint64_t lhs, uint64_t rhs) noexcept {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

bool envFlagEnabled(const char* name) {
    const char* raw = std::getenv(name);
    if (!raw || !*raw) {
        return false;
    }
    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

} // namespace

// ============================================================================
// SimpleDaemonHarness - Minimal daemon lifecycle for benchmarking
// ============================================================================

class SimpleDaemonHarness {
public:
    SimpleDaemonHarness() {
        auto id = randomId();
        root_ = fs::temp_directory_path() / ("yams_e2e_bench_" + id);
        yams::common::ensureDirectories(root_);
        data_ = root_ / "data";
        yams::common::ensureDirectories(data_);
        sock_ = fs::path("/tmp") / ("e2e_bench_" + id + ".sock");
        pid_ = root_ / "bench.pid";
        log_ = root_ / "bench.log";
    }

    ~SimpleDaemonHarness() {
        stop();
        cleanup();
    }

    bool start() {
        // Save original working directory and change to temp dir
        // This prevents session auto-watch from indexing CWD
        originalCwd_ = fs::current_path();
        fs::current_path(root_);

        // Also isolate session state to prevent global session watch from triggering
        originalXdgState_ = std::getenv("XDG_STATE_HOME") ? std::getenv("XDG_STATE_HOME") : "";
        setenv("XDG_STATE_HOME", (root_ / "state").string().c_str(), 1);
        yams::common::ensureDirectories(root_ / "state" / "yams" / "sessions");

        yams::daemon::DaemonConfig cfg;
        cfg.dataDir = data_;
        cfg.socketPath = sock_;
        cfg.pidFile = pid_;
        cfg.logFile = log_;
        cfg.enableModelProvider = true;
        // Disable auto-repair during benchmarks to avoid background work (per-hash DB checks,
        // vector cleanup, etc.) affecting ingestion measurements.
        cfg.enableAutoRepair = false;

        // Benchmark embedding-mode contract:
        // - YAMS_BENCH_FORCE_MOCK_EMBEDDINGS=1 forces mock model provider
        // - YAMS_DISABLE_VECTORS=1 also implies mock/FTS-only mode
        const char* forceMockEmbeddings = std::getenv("YAMS_BENCH_FORCE_MOCK_EMBEDDINGS");
        const bool mockEmbeddingsRequested =
            forceMockEmbeddings && std::string(forceMockEmbeddings) == "1";
        const char* disableVectors = std::getenv("YAMS_DISABLE_VECTORS");
        bool vectorsDisabled = disableVectors && std::string(disableVectors) == "1";
        vectorsDisabled = vectorsDisabled || mockEmbeddingsRequested;

        const char* benchModelEnv = std::getenv("YAMS_BENCH_EMBED_MODEL");
        std::string benchModel = benchModelEnv && *benchModelEnv ? benchModelEnv : "simeon-default";
        const bool useSimeon = benchModel == "simeon-default" || benchModel == "simeon";

        if (useSimeon && !std::getenv("YAMS_EMBED_BACKEND")) {
            setenv("YAMS_EMBED_BACKEND", "simeon", 1);
        }

        if (vectorsDisabled) {
            cfg.useMockModelProvider = true;
            cfg.autoLoadPlugins = false;
            spdlog::info("Using mock model provider (YAMS_BENCH_FORCE_MOCK_EMBEDDINGS={} "
                         "YAMS_DISABLE_VECTORS={})",
                         mockEmbeddingsRequested ? 1 : 0, disableVectors ? disableVectors : "0");
        } else if (useSimeon) {
            // Simeon is built in and deterministic, so it is the preferred default for
            // ingestion profiling. Avoid plugin startup and ONNX model load noise.
            cfg.useMockModelProvider = false;
            cfg.autoLoadPlugins = false;
            cfg.modelPoolConfig.lazyLoading = false;
            cfg.modelPoolConfig.preloadModels = {"simeon-default"};
            spdlog::info("Using Simeon embedding provider for ingestion benchmark");
        } else {
            // Use real model provider with plugins for accurate end-to-end benchmark
            cfg.useMockModelProvider = false;
            cfg.autoLoadPlugins = true;

            // Configure model pool for real embedding generation
            cfg.modelPoolConfig.lazyLoading = false;
            cfg.modelPoolConfig.preloadModels = {benchModel};
        }

        daemon_ = std::make_unique<yams::daemon::YamsDaemon>(cfg);

        auto s = daemon_->start();
        if (!s) {
            spdlog::error("Failed to start daemon: {}", s.error().message);
            return false;
        }

        // Start runLoop in background thread - CRITICAL for processing requests
        runLoopThread_ = std::thread([this]() { daemon_->runLoop(); });

        auto deadline = std::chrono::steady_clock::now() + 30s;
        while (std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(100ms);
            auto lifecycle = daemon_->getLifecycle().snapshot();
            if (lifecycle.state == yams::daemon::LifecycleState::Ready) {
                spdlog::info("Daemon ready at socket: {}", sock_.string());
                if (!vectorsDisabled) {
                    if (auto* sm = daemon_->getServiceManager()) {
                        auto ready = sm->ensureEmbeddingModelReadySync(
                            useSimeon ? "simeon-default" : benchModel, {}, 10000, false, false);
                        if (!ready) {
                            spdlog::warn("Embedding model not ready: {}", ready.error().message);
                        }
                    }
                }
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

            // Join the runLoop thread before continuing
            if (runLoopThread_.joinable()) {
                runLoopThread_.join();
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

        // Restore original working directory
        if (!originalCwd_.empty()) {
            std::error_code ec;
            fs::current_path(originalCwd_, ec);
        }

        // Restore original XDG_STATE_HOME
        if (originalXdgState_.empty()) {
            unsetenv("XDG_STATE_HOME");
        } else {
            setenv("XDG_STATE_HOME", originalXdgState_.c_str(), 1);
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
    std::thread runLoopThread_;
    fs::path root_, data_, sock_, pid_, log_;
    fs::path originalCwd_;
    std::string originalXdgState_;
};

// ============================================================================
// CorpusGenerator - Generate synthetic documents for benchmarking
// ============================================================================

struct CorpusGenerator {
    static constexpr std::uint32_t kSeed = 42;
    static constexpr std::uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
    static constexpr std::uint64_t kFnvPrime = 1099511628211ULL;

    fs::path corpusDir;
    std::vector<std::string> createdFiles;
    std::mt19937 rng{kSeed};
    size_t docSize;
    std::uint64_t fingerprintValue{kFnvOffsetBasis};

    CorpusGenerator(const fs::path& dir, size_t size) : corpusDir(dir), docSize(size) {
        yams::common::ensureDirectories(corpusDir);
    }

    void hashBytes(std::string_view bytes) {
        for (const unsigned char byte : bytes) {
            fingerprintValue ^= byte;
            fingerprintValue *= kFnvPrime;
        }
    }

    std::string fingerprint() const {
        std::ostringstream out;
        out << "fnv1a64:" << std::hex << std::setfill('0') << std::setw(16) << fingerprintValue;
        return out.str();
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
            const std::string document = content.str();
            std::ofstream(corpusDir / filename) << document;
            hashBytes(filename);
            hashBytes(document);
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
    int64_t timestamp_ms = 0;
    // Queue depths (actual items in queues)
    uint64_t store_document_tasks = 0;
    uint64_t embed_jobs = 0;
    uint64_t fts5_jobs = 0;
    uint64_t post_ingest = 0;
    // Cumulative counters (for stage completion detection)
    uint64_t embed_queued = 0;
    uint64_t embed_consumed = 0;
    uint64_t embed_dropped = 0;
    uint64_t embed_prepared_docs_queued = 0;
    uint64_t embed_prepared_chunks_queued = 0;
    uint64_t embed_hash_only_docs_queued = 0;
    uint64_t kg_jobs = 0;
    uint64_t symbol_jobs = 0;
    uint64_t entity_jobs = 0;
    uint64_t title_jobs = 0;
    uint64_t fts5_queued = 0;
    uint64_t fts5_consumed = 0;
    uint64_t fts5_dropped = 0;
    uint64_t post_queued = 0;
    uint64_t post_consumed = 0;
    uint64_t post_dropped = 0;
    uint64_t kg_queued = 0;
    uint64_t kg_consumed = 0;
    uint64_t kg_dropped = 0;
    uint64_t symbol_queued = 0;
    uint64_t symbol_consumed = 0;
    uint64_t symbol_dropped = 0;
    uint64_t entity_queued = 0;
    uint64_t entity_consumed = 0;
    uint64_t entity_dropped = 0;
    uint64_t title_queued = 0;
    uint64_t title_consumed = 0;
    uint64_t title_dropped = 0;

    json toJson() const {
        return json{
            {"timestamp_ms", timestamp_ms},
            {"queue_depths",
             {{"store_document_tasks", store_document_tasks},
              {"embed_jobs", embed_jobs},
              {"fts5_jobs", fts5_jobs},
              {"post_ingest", post_ingest}}},
            {"enrichment_depths",
             {{"kg_jobs", kg_jobs},
              {"symbol_jobs", symbol_jobs},
              {"entity_jobs", entity_jobs},
              {"title_jobs", title_jobs}}},
            {"counters",
             {{"embed",
               {{"queued", embed_queued},
                {"consumed", embed_consumed},
                {"dropped", embed_dropped},
                {"prepared_docs_queued", embed_prepared_docs_queued},
                {"prepared_chunks_queued", embed_prepared_chunks_queued},
                {"hash_only_docs_queued", embed_hash_only_docs_queued}}},
              {"fts5",
               {{"queued", fts5_queued}, {"consumed", fts5_consumed}, {"dropped", fts5_dropped}}},
              {"post",
               {{"queued", post_queued}, {"consumed", post_consumed}, {"dropped", post_dropped}}},
              {"kg", {{"queued", kg_queued}, {"consumed", kg_consumed}, {"dropped", kg_dropped}}},
              {"symbol",
               {{"queued", symbol_queued},
                {"consumed", symbol_consumed},
                {"dropped", symbol_dropped}}},
              {"entity",
               {{"queued", entity_queued},
                {"consumed", entity_consumed},
                {"dropped", entity_dropped}}},
              {"title",
               {{"queued", title_queued},
                {"consumed", title_consumed},
                {"dropped", title_dropped}}}}}};
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

    int64_t duration_ms() const { return nonNegativeDeltaMs(end_ms, start_ms); }

    json toJson() const {
        return json{{"name", name},
                    {"duration_ms", duration_ms()},
                    {"count", count},
                    {"failures", failures}};
    }
};

struct SearchImpactProbe {
    std::string name;
    std::string query;
    std::string search_type;
    bool required_by_contract = false;
    bool skipped = false;
    std::string skip_reason;
    bool ok = false;
    std::string error;
    int64_t wall_ms = 0;
    int64_t completed_ms = 0;
    int64_t daemon_elapsed_ms = 0;
    uint64_t total_count = 0;
    uint64_t returned_count = 0;
    std::vector<std::string> top_ids;

    json toJson() const {
        return json{{"name", name},
                    {"query", query},
                    {"search_type", search_type},
                    {"required_by_contract", required_by_contract},
                    {"skipped", skipped},
                    {"skip_reason", skip_reason},
                    {"ok", ok},
                    {"error", error},
                    {"wall_ms", wall_ms},
                    {"completed_ms", completed_ms},
                    {"daemon_elapsed_ms", daemon_elapsed_ms},
                    {"total_count", total_count},
                    {"returned_count", returned_count},
                    {"top_ids", top_ids}};
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
    std::uint32_t corpus_seed = CorpusGenerator::kSeed;
    std::string corpus_fingerprint;
    std::string ingest_mode = "single_file_serial";
    std::size_t ingest_concurrency = 1;
    std::string timestamp;
    std::string embedding_model = "simeon-default";
    std::string embedding_backend = "simeon";
    std::string simeon_recipe;
    bool kg_enabled = true;

    // Overall metrics
    int64_t total_duration_ms = 0;
    double throughput_docs_per_sec = 0.0;
    bool pipeline_complete = false;
    uint64_t expected_post = 0;
    uint64_t expected_embed = 0;
    uint64_t expected_kg = 0;
    uint64_t observed_post = 0;
    uint64_t observed_embed = 0;
    uint64_t observed_kg = 0;
    bool stages_settled = false;
    bool enrichment_drained = false;
    bool write_coordinator_flushed = false;
    int64_t admission_ms = 0;
    int64_t storage_ready_ms = 0;
    int64_t pipeline_drain_ms = 0;
    int64_t enrichment_ready_ms = 0;
    int64_t searchability_ready_ms = 0;

    // Per-stage metrics
    StageMetrics metadata_storage;
    StageMetrics fts5_extraction;
    StageMetrics embedding_generation;
    StageMetrics kg_extraction;
    std::unordered_map<std::string, yams::daemon::EmbeddingService::PhaseTiming>
        embedding_phase_timings;
    std::unordered_map<std::string, yams::app::services::DocumentStorePhaseTiming>
        document_store_phase_timings;
    std::unordered_map<std::string, yams::api::ContentStorePhaseTiming> content_store_phase_timings;
    std::unordered_map<std::string, yams::metadata::MetadataInsertPhaseTiming>
        metadata_insert_phase_timings;
    std::unordered_map<std::string, yams::storage::RefCounterCommitPhaseTiming>
        ref_counter_commit_phase_timings;
    std::unordered_map<std::string, yams::storage::RefCounterWriterTiming>
        ref_counter_writer_timings;
    std::unordered_map<std::string, yams::storage::RefCounterWriterValueMetric>
        ref_counter_writer_value_metrics;
    yams::daemon::PostIngestQueue::MetricsSnapshot post_ingest_metrics;
    std::vector<SearchImpactProbe> search_impact;

    // Queue monitoring
    std::vector<QueueSnapshot> queue_samples;
    uint64_t max_store_document_tasks = 0;
    uint64_t max_embed_jobs = 0;
    uint64_t max_fts5_jobs = 0;
    uint64_t max_post_ingest = 0;
    uint64_t max_kg_jobs = 0;
    uint64_t max_symbol_jobs = 0;
    uint64_t max_entity_jobs = 0;
    uint64_t max_title_jobs = 0;
    uint64_t final_kg_queued = 0;
    uint64_t final_kg_consumed = 0;
    uint64_t final_kg_dropped = 0;
    uint64_t final_symbol_queued = 0;
    uint64_t final_symbol_consumed = 0;
    uint64_t final_symbol_dropped = 0;
    uint64_t final_entity_queued = 0;
    uint64_t final_entity_consumed = 0;
    uint64_t final_entity_dropped = 0;
    uint64_t final_title_queued = 0;
    uint64_t final_title_consumed = 0;
    uint64_t final_title_dropped = 0;
    uint64_t dropped_batches = 0;

    // WorkCoordinator metrics
    size_t thread_count = 0;
    bool has_write_coordinator_stats = false;
    yams::daemon::WriteCoordinator::Stats write_coordinator_stats;

    json toJson() const {
        json j;
        j["test_config"] = {{"corpus_size", corpus_size},
                            {"doc_size", doc_size},
                            {"poll_interval_ms", poll_interval_ms},
                            {"corpus_seed", corpus_seed},
                            {"corpus_fingerprint", corpus_fingerprint},
                            {"ingest_mode", ingest_mode},
                            {"ingest_concurrency", ingest_concurrency},
                            {"timestamp", timestamp},
                            {"embedding_model", embedding_model},
                            {"embedding_backend", embedding_backend},
                            {"simeon_recipe", simeon_recipe},
                            {"kg_enabled", kg_enabled}};

        j["total_duration_ms"] = total_duration_ms;
        j["throughput_docs_per_sec"] = throughput_docs_per_sec;
        j["pipeline_status"] = {{"complete", pipeline_complete},
                                {"stages_settled", stages_settled},
                                {"enrichment_drained", enrichment_drained},
                                {"write_coordinator_flushed", write_coordinator_flushed},
                                {"expected_post", expected_post},
                                {"observed_post", observed_post},
                                {"expected_embed", expected_embed},
                                {"observed_embed", observed_embed},
                                {"expected_kg", expected_kg},
                                {"observed_kg", observed_kg}};
        j["phase_timings"] = {{"admission_ms", admission_ms},
                              {"storage_ready_ms", storage_ready_ms},
                              {"pipeline_drain_ms", pipeline_drain_ms},
                              {"enrichment_ready_ms", enrichment_ready_ms},
                              {"searchability_ready_ms", searchability_ready_ms}};

        j["stages"] = {{"metadata_storage", metadata_storage.toJson()},
                       {"fts5_extraction", fts5_extraction.toJson()},
                       {"embedding_generation", embedding_generation.toJson()},
                       {"kg_extraction", kg_extraction.toJson()}};

        json phaseTimings = json::object();
        for (const auto& [phase, timing] : embedding_phase_timings) {
            phaseTimings[phase] = {{"calls", timing.calls},
                                   {"total_ms", timing.totalMs},
                                   {"max_ms", timing.maxMs},
                                   {"avg_ms", timing.calls == 0
                                                  ? 0.0
                                                  : static_cast<double>(timing.totalMs) /
                                                        static_cast<double>(timing.calls)}};
        }
        j["embedding_phase_timings"] = std::move(phaseTimings);

        json docStoreTimings = json::object();
        for (const auto& [phase, timing] : document_store_phase_timings) {
            docStoreTimings[phase] = {{"calls", timing.calls},
                                      {"total_ms", timing.totalMs},
                                      {"max_ms", timing.maxMs},
                                      {"avg_ms", timing.calls == 0
                                                     ? 0.0
                                                     : static_cast<double>(timing.totalMs) /
                                                           static_cast<double>(timing.calls)}};
        }
        j["document_store_phase_timings"] = std::move(docStoreTimings);

        json contentStoreTimings = json::object();
        for (const auto& [phase, timing] : content_store_phase_timings) {
            contentStoreTimings[phase] = {
                {"calls", timing.calls},
                {"total_ms", timing.totalMs},
                {"max_ms", timing.maxMs},
                {"avg_ms", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalMs) /
                                                   static_cast<double>(timing.calls)},
                {"total_us", timing.totalUs},
                {"max_us", timing.maxUs},
                {"avg_us", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   static_cast<double>(timing.calls)}};
        }
        j["content_store_phase_timings"] = std::move(contentStoreTimings);

        json metadataInsertTimings = json::object();
        for (const auto& [phase, timing] : metadata_insert_phase_timings) {
            metadataInsertTimings[phase] = {
                {"calls", timing.calls},
                {"total_us", timing.totalUs},
                {"max_us", timing.maxUs},
                {"avg_us", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   static_cast<double>(timing.calls)}};
        }
        j["metadata_insert_phase_timings"] = std::move(metadataInsertTimings);

        json refCounterCommitTimings = json::object();
        for (const auto& [phase, timing] : ref_counter_commit_phase_timings) {
            refCounterCommitTimings[phase] = {
                {"calls", timing.calls},
                {"total_us", timing.totalUs},
                {"max_us", timing.maxUs},
                {"avg_us", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   static_cast<double>(timing.calls)}};
        }
        j["ref_counter_commit_phase_timings"] = std::move(refCounterCommitTimings);

        json refCounterWriterTimings = json::object();
        for (const auto& [phase, timing] : ref_counter_writer_timings) {
            refCounterWriterTimings[phase] = {
                {"calls", timing.calls},
                {"total_us", timing.totalUs},
                {"max_us", timing.maxUs},
                {"avg_us", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   static_cast<double>(timing.calls)}};
        }
        j["ref_counter_writer_timings"] = std::move(refCounterWriterTimings);

        json refCounterWriterValueMetrics = json::object();
        for (const auto& [metric, value] : ref_counter_writer_value_metrics) {
            refCounterWriterValueMetrics[metric] = {
                {"calls", value.calls},
                {"total", value.total},
                {"max", value.max},
                {"avg", value.calls == 0
                            ? 0.0
                            : static_cast<double>(value.total) / static_cast<double>(value.calls)}};
        }
        j["ref_counter_writer_value_metrics"] = std::move(refCounterWriterValueMetrics);

        json postTimings = json::object();
        for (const auto& [phase, timing] : post_ingest_metrics.timings) {
            postTimings[phase] = {
                {"calls", timing.calls},
                {"total_ms", timing.totalMs},
                {"max_ms", timing.maxMs},
                {"avg_ms", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalMs) /
                                                   static_cast<double>(timing.calls)},
                {"total_us", timing.totalUs},
                {"max_us", timing.maxUs},
                {"avg_us", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   static_cast<double>(timing.calls)},
                {"first_start_ms", timing.firstStartMs},
                {"last_end_ms", timing.lastEndMs},
                {"span_ms", nonNegativeDeltaMs(timing.lastEndMs, timing.firstStartMs)}};
        }
        j["post_ingest_phase_timings"] = std::move(postTimings);
        json searchImpact = json::array();
        for (const auto& probe : search_impact) {
            searchImpact.push_back(probe.toJson());
        }
        j["search_impact"] = std::move(searchImpact);

        j["post_ingest_batch_metrics"] = {
            {"extraction_batches", post_ingest_metrics.batches.extractionBatches},
            {"extraction_tasks", post_ingest_metrics.batches.extractionTasks},
            {"extraction_successes", post_ingest_metrics.batches.extractionSuccesses},
            {"extraction_failures", post_ingest_metrics.batches.extractionFailures},
            {"embed_jobs_emitted", post_ingest_metrics.batches.embedJobsEmitted},
            {"embed_docs_emitted", post_ingest_metrics.batches.embedDocsEmitted},
            {"embed_prepared_docs_emitted", post_ingest_metrics.batches.embedPreparedDocsEmitted},
            {"embed_hash_only_docs_emitted", post_ingest_metrics.batches.embedHashOnlyDocsEmitted},
            {"content_index_calls", post_ingest_metrics.batches.contentIndexCalls},
            {"content_index_entries", post_ingest_metrics.batches.contentIndexEntries},
            {"content_index_chunks", post_ingest_metrics.batches.contentIndexChunks},
            {"content_index_max_entries", post_ingest_metrics.batches.contentIndexMaxEntries},
            {"content_index_max_chunk_entries",
             post_ingest_metrics.batches.contentIndexMaxChunkEntries},
            {"content_index_avg_entries_per_call",
             post_ingest_metrics.batches.contentIndexCalls == 0
                 ? 0.0
                 : static_cast<double>(post_ingest_metrics.batches.contentIndexEntries) /
                       static_cast<double>(post_ingest_metrics.batches.contentIndexCalls)},
            {"content_index_avg_entries_per_chunk",
             post_ingest_metrics.batches.contentIndexChunks == 0
                 ? 0.0
                 : static_cast<double>(post_ingest_metrics.batches.contentIndexEntries) /
                       static_cast<double>(post_ingest_metrics.batches.contentIndexChunks)}};

        j["queues"] = {{"max_store_document_tasks", max_store_document_tasks},
                       {"max_embed_jobs", max_embed_jobs},
                       {"max_fts5_jobs", max_fts5_jobs},
                       {"max_post_ingest", max_post_ingest},
                       {"max_kg_jobs", max_kg_jobs},
                       {"max_symbol_jobs", max_symbol_jobs},
                       {"max_entity_jobs", max_entity_jobs},
                       {"max_title_jobs", max_title_jobs},
                       {"dropped_batches", dropped_batches},
                       {"sample_count", queue_samples.size()}};
        j["enrichment_status"] = {{"kg",
                                   {{"queued", final_kg_queued},
                                    {"consumed", final_kg_consumed},
                                    {"dropped", final_kg_dropped}}},
                                  {"symbol",
                                   {{"queued", final_symbol_queued},
                                    {"consumed", final_symbol_consumed},
                                    {"dropped", final_symbol_dropped}}},
                                  {"entity",
                                   {{"queued", final_entity_queued},
                                    {"consumed", final_entity_consumed},
                                    {"dropped", final_entity_dropped}}},
                                  {"title",
                                   {{"queued", final_title_queued},
                                    {"consumed", final_title_consumed},
                                    {"dropped", final_title_dropped}}}};

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

        if (has_write_coordinator_stats) {
            const auto& stats = write_coordinator_stats;
            j["write_coordinator"] = {
                {"batches_enqueued", stats.batchesEnqueued},
                {"batches_committed", stats.batchesCommitted},
                {"ops_applied", stats.opsApplied},
                {"commit_errors", stats.commitErrors},
                {"nodes_upserted", stats.nodesUpserted},
                {"edges_added", stats.edgesAdded},
                {"edges_coalesced", stats.edgesCoalesced},
                {"aliases_added", stats.aliasesAdded},
                {"doc_entities_added", stats.docEntitiesAdded},
                {"symbols_upserted", stats.symbolsUpserted},
                {"node_key_lookups_batched", stats.nodeKeyLookupsBatched},
                {"max_queue_depth", stats.maxQueueDepth},
                {"capacity_rejections", stats.capacityRejections},
                {"forced_enqueues_over_capacity", stats.forcedEnqueuesOverCapacity},
                {"max_batch_apply_ms", stats.maxBatchApplyMs},
                {"max_batch_queue_wait_ms", stats.maxBatchQueueWaitMs},
                {"max_batch_excess_queue_wait_ms", stats.maxBatchExcessQueueWaitMs}};

            json hotSources = json::array();
            for (const auto& source : stats.hotSources) {
                hotSources.push_back({{"source", source.source},
                                      {"batches", source.batches},
                                      {"ops", source.ops},
                                      {"errors", source.errors},
                                      {"total_queue_wait_ms", source.totalQueueWaitMs},
                                      {"max_queue_wait_ms", source.maxQueueWaitMs},
                                      {"max_excess_queue_wait_ms", source.maxExcessQueueWaitMs},
                                      {"total_apply_ms", source.totalApplyMs},
                                      {"max_apply_ms", source.maxApplyMs}});
            }
            j["write_coordinator"]["hot_sources"] = std::move(hotSources);
        }

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
        snap.store_document_tasks = q->size_approx();
    }

    if (auto q = bus.get_or_create_channel<InternalEventBus::EmbedJob>("embed_jobs", 4096)) {
        snap.embed_jobs = q->size_approx();
    }

    if (auto q = bus.get_or_create_channel<InternalEventBus::Fts5Job>("fts5_jobs", 4096)) {
        snap.fts5_jobs = q->size_approx();
    }

    if (auto q = bus.get_or_create_channel<InternalEventBus::PostIngestTask>("post_ingest", 4096)) {
        snap.post_ingest = q->size_approx();
    }
    if (auto q = bus.get_channel<InternalEventBus::KgJob>("kg_jobs")) {
        snap.kg_jobs = q->size_approx();
    }
    if (auto q = bus.get_channel<InternalEventBus::SymbolExtractionJob>("symbol_extraction")) {
        snap.symbol_jobs = q->size_approx();
    }
    if (auto q = bus.get_channel<InternalEventBus::EntityExtractionJob>("entity_extraction")) {
        snap.entity_jobs = q->size_approx();
    }
    if (auto q = bus.get_channel<InternalEventBus::TitleExtractionJob>("title_extraction")) {
        snap.title_jobs = q->size_approx();
    }

    // Get cumulative counters (these work for completion detection)
    snap.embed_queued = bus.embedQueued();
    snap.embed_consumed = bus.embedConsumed();
    snap.embed_dropped = bus.embedDropped();
    snap.embed_prepared_docs_queued = bus.embedPreparedDocsQueued();
    snap.embed_prepared_chunks_queued = bus.embedPreparedChunksQueued();
    snap.embed_hash_only_docs_queued = bus.embedHashOnlyDocsQueued();

    snap.fts5_queued = bus.fts5Queued();
    snap.fts5_consumed = bus.fts5Consumed();
    snap.fts5_dropped = bus.fts5Dropped();

    snap.post_queued = bus.postQueued();
    snap.post_consumed = bus.postConsumed();
    snap.post_dropped = bus.postDropped();
    snap.kg_queued = bus.kgQueued();
    snap.kg_consumed = bus.kgConsumed();
    snap.kg_dropped = bus.kgDropped();
    snap.symbol_queued = bus.symbolQueued();
    snap.symbol_consumed = bus.symbolConsumed();
    snap.symbol_dropped = bus.symbolDropped();
    snap.entity_queued = bus.entityQueued();
    snap.entity_consumed = bus.entityConsumed();
    snap.entity_dropped = bus.entityDropped();
    snap.title_queued = bus.titleQueued();
    snap.title_consumed = bus.titleConsumed();
    snap.title_dropped = bus.titleDropped();

    return snap;
}

std::vector<SearchImpactProbe>
runSearchImpactProbes(const std::shared_ptr<yams::daemon::DaemonClient>& client,
                      bool vectorsDisabled, bool kgEnabled, bool pipelineComplete) {
    if (const char* searchProbes = std::getenv("YAMS_BENCH_SEARCH_PROBES");
        searchProbes && std::string(searchProbes) == "0") {
        return {};
    }

    std::vector<SearchImpactProbe> probes;
    probes.push_back({.name = "keyword",
                      .query = "Synthetic Test Corpus",
                      .search_type = "keyword",
                      .required_by_contract = pipelineComplete});
    probes.push_back({.name = "semantic",
                      .query = "synthetic document corpus",
                      .search_type = "semantic",
                      .required_by_contract = pipelineComplete && !vectorsDisabled});
    probes.push_back({.name = "graph_rerank_hybrid",
                      .query = "synthetic document corpus",
                      .search_type = "hybrid",
                      .required_by_contract = pipelineComplete && !vectorsDisabled && kgEnabled});

    for (auto& probe : probes) {
        if (probe.name == "semantic" && vectorsDisabled) {
            probe.skipped = true;
            probe.skip_reason = "vectors_disabled";
            continue;
        }
        if (probe.name == "graph_rerank_hybrid" && !kgEnabled) {
            probe.skipped = true;
            probe.skip_reason = "kg_disabled";
            continue;
        }
        if (probe.name == "graph_rerank_hybrid" && vectorsDisabled) {
            probe.skipped = true;
            probe.skip_reason = "vectors_disabled";
            continue;
        }
        if (!pipelineComplete) {
            probe.skipped = true;
            probe.skip_reason = "pipeline_incomplete";
            continue;
        }

        yams::daemon::SearchRequest req;
        req.query = probe.query;
        req.searchType = probe.search_type;
        req.limit = 10;
        req.useSession = false;
        req.globalSearch = true;
        req.showHash = true;
        req.timeout = std::chrono::milliseconds(10000);

        const auto start = nowMs();
        auto response = yams::cli::run_sync(client->search(req), 15s);
        const auto end = nowMs();
        probe.wall_ms = nonNegativeDeltaMs(end, start);
        probe.completed_ms = end;

        if (!response) {
            probe.ok = false;
            probe.error = response.error().message;
            continue;
        }

        const auto& value = response.value();
        probe.ok = true;
        probe.daemon_elapsed_ms = value.elapsed.count();
        probe.total_count = static_cast<uint64_t>(value.totalCount);
        probe.returned_count = static_cast<uint64_t>(value.results.size());
        for (const auto& item : value.results) {
            if (!item.id.empty()) {
                probe.top_ids.push_back(item.id);
            }
            if (probe.top_ids.size() >= 5) {
                break;
            }
        }
    }

    return probes;
}

// ============================================================================
// runBenchmark - Main benchmark logic
// ============================================================================

BenchmarkResult runBenchmark(int corpusSize, int docSize, int pollIntervalMs,
                             std::string ingestMode, std::size_t ingestConcurrency) {
    BenchmarkResult result;
    result.corpus_size = corpusSize;
    result.doc_size = docSize;
    result.poll_interval_ms = pollIntervalMs;
    result.ingest_mode = std::move(ingestMode);
    result.ingest_concurrency = std::max<std::size_t>(1, ingestConcurrency);
    result.timestamp = nowIso8601();
    if (const char* env = std::getenv("YAMS_BENCH_EMBED_MODEL"); env && *env) {
        result.embedding_model = env;
    }
    const char* forceMockEmbeddings = std::getenv("YAMS_BENCH_FORCE_MOCK_EMBEDDINGS");
    const bool mockEmbeddingsRequested =
        forceMockEmbeddings && std::string(forceMockEmbeddings) == "1";
    const char* disableVectors = std::getenv("YAMS_DISABLE_VECTORS");
    const bool vectorsDisabled =
        mockEmbeddingsRequested || (disableVectors && std::string(disableVectors) == "1");
    const bool simeonModel =
        result.embedding_model == "simeon-default" || result.embedding_model == "simeon";
    result.embedding_backend = vectorsDisabled ? "mock" : (simeonModel ? "simeon" : "onnxruntime");
    result.simeon_recipe =
        (!vectorsDisabled && simeonModel) ? yams::vector::simeonRecipeLabel() : "";
    result.kg_enabled = !envFlagEnabled("YAMS_BENCH_DISABLE_KG");

    spdlog::info("=== Ingestion E2E Benchmark ===");
    spdlog::info("Corpus size: {}", corpusSize);
    spdlog::info("Doc size: {} bytes", docSize);
    spdlog::info("Poll interval: {} ms", pollIntervalMs);
    spdlog::info("Ingest mode: {} (concurrency={})", result.ingest_mode, result.ingest_concurrency);
    spdlog::info("KG enrichment: {}", result.kg_enabled ? "enabled" : "disabled");

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
    result.corpus_fingerprint = corpus.fingerprint();

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
                         loadedModels.empty() ? "none" : std::to_string(loadedModels.size()));
        } else {
            spdlog::warn("Model provider not available");
        }
        serviceManager->resetEmbeddingPhaseTimings();
        yams::app::services::resetDocumentStorePhaseTimings();
        yams::api::resetContentStorePhaseTimings();
        yams::metadata::resetMetadataInsertPhaseTimings();
        yams::storage::resetRefCounterCommitPhaseTimings();
        yams::storage::resetRefCounterWriterMetrics();
        if (auto postIngest = serviceManager->getPostIngestQueue()) {
            postIngest->resetMetrics();
            if (!result.kg_enabled) {
                postIngest->pauseStage(yams::daemon::PostIngestQueue::Stage::KnowledgeGraph);
            }
        }
    }

    // Capture baseline counters (start from zero)
    auto& bus = yams::daemon::InternalEventBus::instance();
    uint64_t baselineEmbedConsumed = bus.embedConsumed();
    uint64_t baselineEmbedDropped = bus.embedDropped();
    uint64_t baselinePostConsumed = bus.postConsumed();
    uint64_t baselinePostDropped = bus.postDropped();
    uint64_t baselineKgConsumed = bus.kgConsumed();
    uint64_t baselineKgDropped = bus.kgDropped();
    spdlog::info("Baseline counters: embed={}, post={}, kg={}", baselineEmbedConsumed,
                 baselinePostConsumed, baselineKgConsumed);

    // Phase 4: Ingest documents
    spdlog::info("Phase 4: Ingesting {} documents...", corpusSize);
    int64_t ingestStartMs = nowMs();
    result.metadata_storage.name = "metadata_storage";
    result.metadata_storage.start_ms = ingestStartMs;

    std::atomic<int> successCount{0};
    std::atomic<int> failCount{0};
    const auto addSingleFile = [&](yams::daemon::DaemonClient& addClient,
                                   const std::string& filename) {
        yams::daemon::AddDocumentRequest addReq;
        addReq.path = (corpusDir / filename).string();
        addReq.noEmbeddings = vectorsDisabled;

        auto addResult = yams::cli::run_sync(addClient.streamingAddDocument(addReq), 30s);
        if (!addResult) {
            spdlog::warn("Failed to ingest {}: {}", filename, addResult.error().message);
            failCount.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        const int completed = successCount.fetch_add(1, std::memory_order_relaxed) + 1;
        if (completed % 10 == 0 || completed == corpusSize) {
            spdlog::info("Ingested {}/{} documents", completed, corpusSize);
        }
    };

    if (result.ingest_mode == "directory") {
        // Directory admission is one recursive request regardless of the caller's
        // generic concurrency default. Keep the experiment identity truthful.
        result.ingest_concurrency = 1;
        yams::daemon::AddDocumentRequest addReq;
        addReq.path = corpusDir.string();
        addReq.recursive = true;
        addReq.noEmbeddings = vectorsDisabled;
        addReq.includePatterns = {"*.txt"};
        auto addResult = yams::cli::run_sync(client->streamingAddDocument(addReq), 120s);
        if (!addResult) {
            spdlog::warn("Directory ingestion failed: {}", addResult.error().message);
            failCount.store(corpusSize, std::memory_order_relaxed);
        } else {
            successCount.store(corpusSize, std::memory_order_relaxed);
            spdlog::info("Directory ingestion accepted for {} documents: {}", corpusSize,
                         addResult.value().message);
        }
    } else if (result.ingest_mode == "pipelined_single_file") {
        const std::size_t workerCount =
            std::min<std::size_t>(result.ingest_concurrency, corpus.createdFiles.size());
        std::atomic<std::size_t> nextFile{0};
        std::vector<std::thread> workers;
        workers.reserve(workerCount);
        for (std::size_t worker = 0; worker < workerCount; ++worker) {
            workers.emplace_back([&, worker]() {
                auto workerClient = std::make_shared<yams::daemon::DaemonClient>(clientConfig);
                auto workerConnect = yams::cli::run_sync(workerClient->connect(), 5s);
                if (!workerConnect) {
                    spdlog::warn("Pipelined ingest worker {} failed to connect: {}", worker,
                                 workerConnect.error().message);
                    while (nextFile.fetch_add(1, std::memory_order_relaxed) <
                           corpus.createdFiles.size()) {
                        failCount.fetch_add(1, std::memory_order_relaxed);
                    }
                    return;
                }
                while (true) {
                    const std::size_t index = nextFile.fetch_add(1, std::memory_order_relaxed);
                    if (index >= corpus.createdFiles.size()) {
                        break;
                    }
                    addSingleFile(*workerClient, corpus.createdFiles[index]);
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
    } else {
        result.ingest_mode = "single_file_serial";
        result.ingest_concurrency = 1;
        for (const auto& filename : corpus.createdFiles) {
            addSingleFile(*client, filename);
        }
    }

    int64_t ingestEndMs = nowMs();
    result.metadata_storage.end_ms = ingestEndMs;
    result.admission_ms = nonNegativeDeltaMs(ingestEndMs, ingestStartMs);
    result.metadata_storage.count = static_cast<std::uint64_t>(successCount.load());
    result.metadata_storage.failures = static_cast<std::uint64_t>(failCount.load());

    spdlog::info("Ingestion admission complete: {} succeeded, {} failed in {} ms",
                 successCount.load(), failCount.load(),
                 nonNegativeDeltaMs(ingestEndMs, ingestStartMs));

    // Phase 5: Monitor pipeline completion via InternalEventBus
    spdlog::info("Phase 5: Monitoring pipeline completion...");

    // Capture initial state
    QueueSnapshot initialSnap = captureQueueSnapshot();
    spdlog::info("Initial state: embed_queued={} fts5_queued={} post_queued={}",
                 initialSnap.embed_queued, initialSnap.fts5_queued, initialSnap.post_queued);

    // Expected counts
    // Note: FTS5 indexing happens synchronously in post-ingest metadata stage (not via jobs)
    // So we only track post-ingest (metadata+FTS5) and embeddings completion
    uint64_t expectedPost = static_cast<std::uint64_t>(successCount.load());
    uint64_t expectedEmbed = vectorsDisabled ? 0 : static_cast<std::uint64_t>(successCount.load());
    uint64_t expectedKg = result.kg_enabled ? static_cast<std::uint64_t>(successCount.load()) : 0;
    result.expected_post = expectedPost;
    result.expected_embed = expectedEmbed;
    result.expected_kg = expectedKg;

    // Poll until every expected item is consumed or explicitly dropped. Drops settle the
    // workload but never satisfy the lossless completion contract.
    auto pipelineDeadline = std::chrono::steady_clock::now() + 60s;
    bool allStagesComplete = false;
    bool allStagesSettled = false;
    QueueSnapshot lastSnap = initialSnap;
    uint64_t lastEmbedConsumedDelta = 0;
    uint64_t lastPostConsumedDelta = 0;
    uint64_t lastEmbedDroppedDelta = 0;
    uint64_t lastKgConsumedDelta = 0;
    uint64_t lastKgDroppedDelta = 0;

    while (std::chrono::steady_clock::now() < pipelineDeadline && !allStagesSettled) {
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));

        QueueSnapshot snap = captureQueueSnapshot();
        result.queue_samples.push_back(snap);
        lastSnap = snap;

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
        result.max_kg_jobs = std::max(result.max_kg_jobs, snap.kg_jobs);
        result.max_symbol_jobs = std::max(result.max_symbol_jobs, snap.symbol_jobs);
        result.max_entity_jobs = std::max(result.max_entity_jobs, snap.entity_jobs);
        result.max_title_jobs = std::max(result.max_title_jobs, snap.title_jobs);

        // Track dropped batches (excluding warmup baseline)
        uint64_t embedDroppedDelta = counterDelta(snap.embed_dropped, baselineEmbedDropped);
        uint64_t postDroppedDelta = counterDelta(snap.post_dropped, baselinePostDropped);
        uint64_t kgDroppedDelta = counterDelta(snap.kg_dropped, baselineKgDropped);
        const uint64_t countedKgDroppedDelta = result.kg_enabled ? kgDroppedDelta : 0;
        result.dropped_batches = saturatedAdd(
            saturatedAdd(saturatedAdd(snap.fts5_dropped, embedDroppedDelta), postDroppedDelta),
            countedKgDroppedDelta);

        const uint64_t embedConsumedDelta =
            counterDelta(snap.embed_consumed, baselineEmbedConsumed);
        const uint64_t postConsumedDelta = counterDelta(snap.post_consumed, baselinePostConsumed);
        const uint64_t kgConsumedDelta =
            result.kg_enabled ? counterDelta(snap.kg_consumed, baselineKgConsumed) : 0;
        lastEmbedConsumedDelta = embedConsumedDelta;
        lastPostConsumedDelta = postConsumedDelta;
        lastKgConsumedDelta = kgConsumedDelta;
        lastEmbedDroppedDelta = embedDroppedDelta;
        lastKgDroppedDelta = kgDroppedDelta;

        // Completion is exact: consuming more work than was admitted indicates a replay or
        // duplicate enqueue. Keep the settled checks below permissive so the benchmark can stop
        // and report the over-count instead of waiting for a timeout.
        const bool embedComplete = embedConsumedDelta == expectedEmbed;
        const bool postComplete = postConsumedDelta == expectedPost;
        const bool kgComplete = kgConsumedDelta == expectedKg;
        const bool embedSettled =
            saturatedAdd(embedConsumedDelta, embedDroppedDelta) >= expectedEmbed;
        const bool postSettled = saturatedAdd(postConsumedDelta, postDroppedDelta) >= expectedPost;
        const bool kgSettled =
            saturatedAdd(kgConsumedDelta, result.kg_enabled ? kgDroppedDelta : 0) >= expectedKg;

        allStagesComplete =
            embedComplete && postComplete && kgComplete && result.dropped_batches == 0;
        allStagesSettled = embedSettled && postSettled && kgSettled;

        if (nonNegativeDeltaMs(snap.timestamp_ms, initialSnap.timestamp_ms) % 1000 <
            pollIntervalMs) {
            spdlog::info("Pipeline status: post={}/{} (dropped={}) embed={}/{} (dropped={}) "
                         "kg={}/{} (dropped={})",
                         postConsumedDelta, expectedPost, postDroppedDelta, embedConsumedDelta,
                         expectedEmbed, embedDroppedDelta, kgConsumedDelta, expectedKg,
                         kgDroppedDelta);
        }

        if (allStagesSettled) {
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
            result.embedding_generation.count =
                counterDelta(snap.embed_consumed, baselineEmbedConsumed);
            result.embedding_generation.failures = embedDroppedDelta;

            result.kg_extraction.name = "kg_extraction";
            result.kg_extraction.start_ms = ingestStartMs;
            result.kg_extraction.end_ms = now;
            result.kg_extraction.count = counterDelta(snap.kg_consumed, baselineKgConsumed);
            result.kg_extraction.failures = kgDroppedDelta;

            spdlog::info("All pipeline stages settled (lossless={})", allStagesComplete);
            break;
        }
    }

    result.stages_settled = allStagesSettled;
    const int64_t storageReadyEndMs = nowMs();
    result.storage_ready_ms = nonNegativeDeltaMs(storageReadyEndMs, ingestStartMs);
    result.observed_post = lastPostConsumedDelta;
    result.observed_embed = lastEmbedConsumedDelta;
    result.observed_kg = lastKgConsumedDelta;

    if (!allStagesSettled) {
        spdlog::warn("Pipeline settlement timeout after 60s (post={}/{} embed={}/{} kg={}/{})",
                     lastPostConsumedDelta, expectedPost, lastEmbedConsumedDelta, expectedEmbed,
                     lastKgConsumedDelta, expectedKg);

        // Preserve partial pipeline evidence instead of leaving stage metrics at zero. A timeout is
        // still a failure, but partial counters identify which PostIngestQueue branch stalled.
        const int64_t now = nowMs();
        result.fts5_extraction.name = "fts5_extraction";
        result.fts5_extraction.start_ms = ingestStartMs;
        result.fts5_extraction.end_ms = now;
        result.fts5_extraction.count = lastSnap.fts5_consumed;
        result.fts5_extraction.failures = lastSnap.fts5_dropped;

        result.embedding_generation.name = "embedding_generation";
        result.embedding_generation.start_ms = ingestStartMs;
        result.embedding_generation.end_ms = now;
        result.embedding_generation.count =
            counterDelta(lastSnap.embed_consumed, baselineEmbedConsumed);
        result.embedding_generation.failures = lastEmbedDroppedDelta;

        result.kg_extraction.name = "kg_extraction";
        result.kg_extraction.start_ms = ingestStartMs;
        result.kg_extraction.end_ms = now;
        result.kg_extraction.count = counterDelta(lastSnap.kg_consumed, baselineKgConsumed);
        result.kg_extraction.failures = lastKgDroppedDelta;
    }

    // The core counters can settle before entity/title work and deferred KG writes commit.
    // Wait for every PostIngestQueue stage to drain, then flush the WriteCoordinator.
    result.enrichment_drained = false;
    const auto enrichmentDeadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < enrichmentDeadline) {
        lastSnap = captureQueueSnapshot();
        bool postIngestIdle = true;
        if (serviceManager) {
            if (auto postIngest = serviceManager->getPostIngestQueue()) {
                postIngestIdle =
                    postIngest->size() == 0 && postIngest->kgQueueDepth() == 0 &&
                    postIngest->symbolQueueDepth() == 0 && postIngest->entityQueueDepth() == 0 &&
                    postIngest->titleQueueDepth() == 0 && postIngest->totalInFlight() == 0;
            }
        }
        const bool embeddingIdle =
            lastSnap.embed_jobs == 0 &&
            (!serviceManager || serviceManager->getEmbeddingInFlightJobs() == 0);
        if (postIngestIdle && embeddingIdle) {
            result.enrichment_drained = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }

    result.write_coordinator_flushed = true;
    if (serviceManager) {
        if (auto* writeCoordinator = serviceManager->getWriteCoordinator()) {
            auto flushResult = writeCoordinator->flush(30s);
            result.write_coordinator_flushed = static_cast<bool>(flushResult);
            result.write_coordinator_stats = writeCoordinator->getStats();
            result.has_write_coordinator_stats = true;
            if (!flushResult) {
                spdlog::warn("WriteCoordinator flush failed: {}", flushResult.error().message);
            }
        }
    }

    lastSnap = captureQueueSnapshot();
    result.final_kg_queued = lastSnap.kg_queued;
    result.final_kg_consumed = lastSnap.kg_consumed;
    result.final_kg_dropped = lastSnap.kg_dropped;
    result.final_symbol_queued = lastSnap.symbol_queued;
    result.final_symbol_consumed = lastSnap.symbol_consumed;
    result.final_symbol_dropped = lastSnap.symbol_dropped;
    result.final_entity_queued = lastSnap.entity_queued;
    result.final_entity_consumed = lastSnap.entity_consumed;
    result.final_entity_dropped = lastSnap.entity_dropped;
    result.final_title_queued = lastSnap.title_queued;
    result.final_title_consumed = lastSnap.title_consumed;
    result.final_title_dropped = lastSnap.title_dropped;
    result.dropped_batches = saturatedAdd(
        result.dropped_batches,
        saturatedAdd(result.final_symbol_dropped,
                     saturatedAdd(result.final_entity_dropped, result.final_title_dropped)));

    result.pipeline_complete = allStagesComplete && result.enrichment_drained &&
                               result.write_coordinator_flushed && result.dropped_batches == 0 &&
                               failCount.load(std::memory_order_relaxed) == 0;
    const int64_t enrichmentEndMs = nowMs();
    result.pipeline_drain_ms = nonNegativeDeltaMs(enrichmentEndMs, storageReadyEndMs);
    result.enrichment_ready_ms = nonNegativeDeltaMs(enrichmentEndMs, ingestStartMs);
    result.total_duration_ms = result.enrichment_ready_ms;
    result.throughput_docs_per_sec =
        result.total_duration_ms > 0
            ? static_cast<double>(successCount.load()) / (result.total_duration_ms / 1000.0)
            : 0.0;

    spdlog::info("Phase 6: Running fixed search-impact probes...");
    result.search_impact =
        runSearchImpactProbes(client, vectorsDisabled, result.kg_enabled, result.pipeline_complete);
    for (const auto& probe : result.search_impact) {
        if (probe.name == "keyword" && probe.ok &&
            probe.total_count >= static_cast<std::uint64_t>(successCount.load())) {
            result.searchability_ready_ms = nonNegativeDeltaMs(probe.completed_ms, ingestStartMs);
            break;
        }
    }

    if (serviceManager) {
        result.embedding_phase_timings = serviceManager->getEmbeddingPhaseTimingsSnapshot();
        result.document_store_phase_timings =
            yams::app::services::getDocumentStorePhaseTimingsSnapshot();
        result.content_store_phase_timings = yams::api::getContentStorePhaseTimingsSnapshot();
        result.metadata_insert_phase_timings =
            yams::metadata::getMetadataInsertPhaseTimingsSnapshot();
        result.ref_counter_commit_phase_timings =
            yams::storage::getRefCounterCommitPhaseTimingsSnapshot();
        result.ref_counter_writer_timings = yams::storage::getRefCounterWriterTimingsSnapshot();
        result.ref_counter_writer_value_metrics =
            yams::storage::getRefCounterWriterValueMetricsSnapshot();
        if (auto postIngest = serviceManager->getPostIngestQueue()) {
            result.post_ingest_metrics = postIngest->metricsSnapshot();
        }
    }

    spdlog::info("=== Benchmark Complete ===");
    spdlog::info("Total duration: {} ms", result.total_duration_ms);
    spdlog::info("Throughput: {:.2f} docs/sec", result.throughput_docs_per_sec);
    spdlog::info("Dropped batches: {}", result.dropped_batches);

    return result;
}

// ============================================================================
// main - Entry point
// ============================================================================

int main(int /*argc*/, char* /*argv*/[]) {
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

    std::string ingestMode = "single_file_serial";
    if (const char* env = std::getenv("YAMS_BENCH_INGEST_MODE")) {
        ingestMode = env;
    }

    std::size_t ingestConcurrency = 4;
    if (const char* env = std::getenv("YAMS_BENCH_INGEST_CONCURRENCY")) {
        ingestConcurrency = static_cast<std::size_t>(std::max(1, std::atoi(env)));
    }

    const char* outputPath = std::getenv("YAMS_BENCH_OUTPUT");

    // Run benchmark
    BenchmarkResult result =
        runBenchmark(corpusSize, docSize, pollInterval, ingestMode, ingestConcurrency);

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

    return result.metadata_storage.failures > 0 || !result.pipeline_complete ||
                   result.dropped_batches > 0
               ? 1
               : 0;
}
