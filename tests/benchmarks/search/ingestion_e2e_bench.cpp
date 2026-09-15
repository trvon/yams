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
    YAMS_BENCH_INGEST_CONCURRENCY=N   - In-flight window for pipelined_single_file
                                        (default: 4)
    YAMS_BENCH_POST_INGEST_COALESCE_MS=N
                                      - Typed post-ingest coalescing window (default: 2)
    YAMS_BENCH_POST_INGEST_BATCH_SIZE=N
                                      - Existing typed batch-size axis (default: product value)
    YAMS_BENCH_OUTPUT=path            - JSON output file (default: stdout)
    YAMS_BENCH_DISABLE_KG=1           - Disable KG post-ingest enrichment for ablation
    YAMS_BENCH_SEARCH_PROBES=0        - Disable fixed post-ingest search probes
    YAMS_BENCH_PHASE=initial|replay|resume
                                      - Select lifecycle contract (default: initial)
    YAMS_BENCH_FIXTURE_ROOT=path      - Externally owned yams_e2e_bench_* temp root used across
                                        fresh processes; caller must remove it

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

#include "tests/common/test_helpers_catch2.h"
#include "tests/integration/daemon/test_async_helpers.h"
#include "tests/integration/daemon/test_daemon_harness.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <yams/api/content_store.h>
#include <yams/app/services/document_ingestion_service.h>
#include <yams/app/services/services.hpp>
#include <yams/common/fs_utils.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/InternalEventBus.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/VectorIndexCoordinator.h>
#include <yams/daemon/components/WriteCoordinator.h>
#include <yams/daemon/daemon.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/storage/reference_counter.h>
#include <yams/storage/reference_counter_writer.h>
#include <yams/vector/simeon_embedding_backend.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>
#include <yams/compat/unistd.h>

#if defined(__APPLE__) || defined(__linux__)
#include <sys/resource.h>
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using json = nlohmann::json;

namespace {

constexpr const char* kSimeonModel = "simeon-default";

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

std::string fnvFingerprint(std::uint64_t value) {
    std::ostringstream out;
    out << "fnv1a64:" << std::hex << std::setfill('0') << std::setw(16) << value;
    return out.str();
}

class Fnv1a64 {
public:
    void addBytes(std::string_view value) {
        addInteger(value.size());
        for (const auto byte : value) {
            addByte(static_cast<unsigned char>(byte));
        }
    }

    void addInteger(std::uint64_t value) {
        for (unsigned shift = 0; shift < 64; shift += 8) {
            addByte(static_cast<unsigned char>((value >> shift) & 0xffU));
        }
    }

    void addFloat(float value) { addInteger(std::bit_cast<std::uint32_t>(value)); }

    [[nodiscard]] std::string fingerprint() const { return fnvFingerprint(value_); }

private:
    void addByte(unsigned char byte) {
        value_ ^= byte;
        value_ *= 1099511628211ULL;
    }

    std::uint64_t value_{14695981039346656037ULL};
};

std::uint64_t percentileUs(std::vector<std::uint64_t> samples, double percentile) {
    if (samples.empty()) {
        return 0;
    }
    std::sort(samples.begin(), samples.end());
    const auto rank =
        static_cast<std::size_t>(std::ceil(percentile * static_cast<double>(samples.size())));
    return samples[std::min(samples.size() - 1, rank > 0 ? rank - 1 : 0)];
}

std::uint64_t peakRssBytes() noexcept {
#if defined(__APPLE__) || defined(__linux__)
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return 0;
    }
#if defined(__APPLE__)
    return static_cast<std::uint64_t>(usage.ru_maxrss);
#else
    return static_cast<std::uint64_t>(usage.ru_maxrss) * 1024ULL;
#endif
#else
    return 0;
#endif
}

std::uint64_t sidecarBytes(const fs::path& dataDir, std::string_view suffix) {
    std::uint64_t total = 0;
    std::error_code ec;
    for (fs::recursive_directory_iterator it(dataDir, ec), end; !ec && it != end;
         it.increment(ec)) {
        if (!it->is_regular_file(ec)) {
            continue;
        }
        const auto name = it->path().filename().string();
        if (name.size() < suffix.size() ||
            name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
            continue;
        }
        const auto size = it->file_size(ec);
        if (!ec) {
            total = saturatedAdd(total, static_cast<std::uint64_t>(size));
        }
    }
    return total;
}

bool hasSidecars(const fs::path& dataDir) {
    std::error_code ec;
    for (fs::recursive_directory_iterator it(dataDir, ec), end; !ec && it != end;
         it.increment(ec)) {
        const auto name = it->path().filename().string();
        if (name.ends_with("-wal") || name.ends_with("-shm")) {
            return true;
        }
    }
    return false;
}

bool envFlagEnabled(const char* name) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') {
        return false;
    }
    std::string value(raw);
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return value == "1" || value == "true" || value == "yes" || value == "on";
}

bool pathWithin(const fs::path& candidate, const fs::path& root) {
    const auto relative = candidate.lexically_relative(root);
    return !relative.empty() && relative.native() != "." && *relative.begin() != "..";
}

std::optional<fs::path> externalFixtureRoot() {
    const char* configured = std::getenv("YAMS_BENCH_FIXTURE_ROOT");
    if (configured == nullptr || *configured == '\0') {
        return std::nullopt;
    }

    auto candidate = fs::weakly_canonical(fs::absolute(configured));
    const auto tempRoot = fs::weakly_canonical(fs::temp_directory_path());
    bool isolated = pathWithin(candidate, tempRoot);
#ifndef _WIN32
    isolated = isolated || pathWithin(candidate, fs::weakly_canonical("/tmp"));
#endif
    const auto name = candidate.filename().string();
    if (!isolated || !name.starts_with("yams_e2e_bench_")) {
        throw std::runtime_error(
            "YAMS_BENCH_FIXTURE_ROOT must be an isolated yams_e2e_bench_* temp directory");
    }
    return candidate;
}

std::string benchPhase() {
    const char* configured = std::getenv("YAMS_BENCH_PHASE");
    const std::string phase = configured != nullptr && *configured != '\0' ? configured : "initial";
    if (phase != "initial" && phase != "replay" && phase != "resume") {
        throw std::runtime_error("YAMS_BENCH_PHASE must be initial, replay, or resume");
    }
    return phase;
}

int benchEnvInt(const char* name, int fallback, int minimum, int maximum) {
    const char* raw = std::getenv(name);
    if (raw == nullptr || *raw == '\0') {
        return fallback;
    }
    const std::string_view value(raw);
    int parsed = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), parsed);
    return error == std::errc{} && end == value.data() + value.size() && parsed >= minimum &&
                   parsed <= maximum
               ? parsed
               : fallback;
}

std::uint32_t benchPostIngestCoalesceMs() {
    constexpr std::uint32_t kMaxCoalesceMs = 20;
    const auto defaultValue = yams::daemon::TuningConfig{}.postIngestCoalesceMs;
    const char* raw = std::getenv("YAMS_BENCH_POST_INGEST_COALESCE_MS");
    if (!raw || !*raw) {
        return defaultValue;
    }
    try {
        const auto parsed = std::stoul(raw);
        return parsed <= kMaxCoalesceMs ? static_cast<std::uint32_t>(parsed) : defaultValue;
    } catch (const std::exception&) {
        return defaultValue;
    }
}

std::uint32_t benchPostIngestBatchSize() {
    constexpr std::uint32_t kMaxBatchSize = 256;
    const auto defaultValue = yams::daemon::TuneAdvisor::postIngestBatchSize();
    const char* raw = std::getenv("YAMS_BENCH_POST_INGEST_BATCH_SIZE");
    if (!raw || !*raw) {
        return defaultValue;
    }
    try {
        const auto parsed = std::stoul(raw);
        return parsed >= 1 && parsed <= kMaxBatchSize ? static_cast<std::uint32_t>(parsed)
                                                      : defaultValue;
    } catch (const std::exception&) {
        return defaultValue;
    }
}

} // namespace

// ============================================================================
// SimpleDaemonHarness - Minimal daemon lifecycle for benchmarking
// ============================================================================

class SimpleDaemonHarness {
public:
    SimpleDaemonHarness() {
        if (const auto configuredRoot = externalFixtureRoot()) {
            root_ = *configuredRoot;
            externallyOwnedRoot_ = true;
        } else {
            auto id = randomId();
#ifdef _WIN32
            const auto tempBase = fs::temp_directory_path();
#else
            // Keep Unix-domain socket paths well below sockaddr_un::sun_path limits while placing
            // every fixture artifact under one directory for deterministic cleanup.
            const auto tempBase = fs::path("/tmp");
#endif
            root_ = tempBase / ("yams_e2e_bench_" + id);
        }
        yams::common::ensureDirectories(root_);
        data_ = root_ / "data";
        yams::common::ensureDirectories(data_);
        sock_ = root_ / "daemon.sock";
        pid_ = root_ / "bench.pid";
        log_ = root_ / "bench.log";
    }

    ~SimpleDaemonHarness() noexcept {
        try {
            stop();
        } catch (...) {
            std::fputs("SimpleDaemonHarness stop failed during teardown\n", stderr);
        }
        try {
            cleanup();
        } catch (...) {
            std::fputs("SimpleDaemonHarness cleanup failed during teardown\n", stderr);
        }
    }

    SimpleDaemonHarness(const SimpleDaemonHarness&) = delete;
    SimpleDaemonHarness& operator=(const SimpleDaemonHarness&) = delete;

    bool start() {
        if (harness_ || !savedEnvironment_.empty()) {
            spdlog::error("SimpleDaemonHarness start called while a prior start is still active");
            return false;
        }

        // Save original working directory and change to temp dir
        // This prevents session auto-watch from indexing CWD
        originalCwd_ = fs::current_path();
        fs::current_path(root_);

        // Isolate every daemon-owned path. Plugin/model discovery must not fall through to the
        // installed daemon's config, cache, corpus, or embedding selection.
        saveAndUnsetEnvironment("YAMS_CONFIG_PATH");
        saveAndUnsetEnvironment("YAMS_CONFIG");
        saveAndUnsetEnvironment("YAMS_EMBED_BACKEND");
        saveAndUnsetEnvironment("YAMS_PREFERRED_MODEL");
        saveAndSetEnvironment("XDG_STATE_HOME", root_ / "state");
        saveAndSetEnvironment("XDG_CONFIG_HOME", root_ / "config");
        saveAndSetEnvironment("XDG_CACHE_HOME", root_ / "cache");
        saveAndSetEnvironment("YAMS_DATA_DIR", data_);
        yams::common::ensureDirectories(root_ / "state" / "yams" / "sessions");

        const auto configPath = root_ / "config" / "yams" / "config.toml";
        yams::common::ensureDirectories(configPath.parent_path());
        {
            std::ofstream configFile(configPath);
            if (!configFile) {
                spdlog::error("Failed to create isolated benchmark config: {}",
                              configPath.string());
                return false;
            }
            configFile << "config_version = 3\n"
                          "[embeddings]\n"
                          "backend = \"simeon\"\n"
                          "preferred_model = \"simeon-default\"\n";
        }

        // Benchmark embedding-mode contract:
        // - YAMS_BENCH_FORCE_MOCK_EMBEDDINGS=1 forces mock model provider
        // - YAMS_DISABLE_VECTORS=1 also implies mock/FTS-only mode
        const char* forceMockEmbeddings = std::getenv("YAMS_BENCH_FORCE_MOCK_EMBEDDINGS");
        const bool mockEmbeddingsRequested =
            forceMockEmbeddings && std::string(forceMockEmbeddings) == "1";
        const char* disableVectors = std::getenv("YAMS_DISABLE_VECTORS");
        bool vectorsDisabled = disableVectors && std::string(disableVectors) == "1";
        vectorsDisabled = vectorsDisabled || mockEmbeddingsRequested;

        yams::test::DaemonHarness::Options options;
        options.rootDir = root_;
        options.preserveRoot = true;
        options.dataDir = data_;
        options.socketPath = sock_;
        options.pidFile = pid_;
        options.logFile = log_;
        options.configPath = configPath;
        options.enableModelProvider = true;
        options.enableAutoRepair = false;
        options.requireReadyLifecycle = true;
        options.configureDaemon = [](yams::daemon::DaemonConfig& config) {
            config.tuning.postIngestCoalesceMs = benchPostIngestCoalesceMs();
        };

        glinerRequested_ = !envFlagEnabled("YAMS_DISABLE_GLINER_TITLES");
        if (const char* pluginDir = std::getenv("YAMS_BENCH_GLINT_PLUGIN_DIR");
            glinerRequested_ && pluginDir && *pluginDir && fs::is_directory(pluginDir)) {
            options.pluginDir = fs::absolute(pluginDir);
            options.pluginDirStrict = true;
            options.trustedPluginPaths = {*options.pluginDir};
            pluginAutoloadEnabled_ = true;
        }

        if (vectorsDisabled) {
            options.useMockModelProvider = true;
            options.autoLoadPlugins = false;
            spdlog::info("Using mock model provider (YAMS_BENCH_FORCE_MOCK_EMBEDDINGS={} "
                         "YAMS_DISABLE_VECTORS={})",
                         mockEmbeddingsRequested ? 1 : 0, disableVectors ? disableVectors : "0");
        } else {
            // Simeon is the benchmark's fixed production-default embedding provider. Optional
            // Glint autoload exercises the independent GLiNER title stage without changing the
            // embedding space.
            options.useMockModelProvider = false;
            options.autoLoadPlugins = pluginAutoloadEnabled_;
            options.configureModelPool = true;
            options.modelPoolLazyLoading = false;
            options.preloadModels = {kSimeonModel};
            spdlog::info("Using Simeon embedding provider for ingestion benchmark");
        }

        harness_ = std::make_unique<yams::test::DaemonHarness>(std::move(options));
        if (!harness_->startWithRetry(30s, 2, [](yams::daemon::YamsDaemon*) {})) {
            spdlog::error("Failed to start shared daemon harness");
            return false;
        }

        spdlog::info("Daemon ready at socket: {}", sock_.string());
        if (!vectorsDisabled) {
            if (auto* sm = harness_->daemon()->getServiceManager()) {
                auto ready =
                    sm->ensureEmbeddingModelReadySync(kSimeonModel, {}, 10000, false, false);
                if (!ready) {
                    spdlog::warn("Embedding model not ready: {}", ready.error().message);
                }
            }
        }
        return true;
    }

    void stop() {
        if (harness_) {
            harness_->stop();
            shutdownSucceeded_ = harness_->shutdownSucceeded();
            harness_.reset();
        }

        // Restore original working directory only after the shared harness has released daemon
        // resources and restored its own scoped process state.
        if (!originalCwd_.empty()) {
            std::error_code ec;
            fs::current_path(originalCwd_, ec);
        }
        restoreEnvironment();
    }

    [[nodiscard]] const fs::path& socketPath() const { return sock_; }
    [[nodiscard]] const fs::path& root() const { return root_; }
    [[nodiscard]] const fs::path& dataDir() const { return data_; }
    [[nodiscard]] const fs::path& pidPath() const { return pid_; }
    [[nodiscard]] bool glinerRequested() const { return glinerRequested_; }
    [[nodiscard]] bool pluginAutoloadEnabled() const { return pluginAutoloadEnabled_; }
    [[nodiscard]] bool shutdownSucceeded() const { return shutdownSucceeded_; }
    [[nodiscard]] bool stopRequested() const {
        return harness_ && harness_->daemon() && harness_->daemon()->isStopRequested();
    }
    [[nodiscard]] yams::daemon::YamsDaemon* daemon() const {
        return harness_ ? harness_->daemon() : nullptr;
    }

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

    void saveAndSetEnvironment(const std::string& key, const fs::path& value) {
        savedEnvironment_.emplace_back(key, value.string());
    }

    void saveAndUnsetEnvironment(const std::string& key) {
        savedEnvironment_.emplace_back(key, std::nullopt);
    }

    void restoreEnvironment() {
        while (!savedEnvironment_.empty()) {
            savedEnvironment_.pop_back();
        }
    }

    void cleanup() {
        std::error_code ec;
        if (!root_.empty() && !externallyOwnedRoot_)
            fs::remove_all(root_, ec);
    }

    std::unique_ptr<yams::test::DaemonHarness> harness_;
    fs::path root_, data_, sock_, pid_, log_;
    fs::path originalCwd_;
    std::vector<yams::test::ScopedEnvVar> savedEnvironment_;
    bool glinerRequested_{false};
    bool pluginAutoloadEnabled_{false};
    bool shutdownSucceeded_{false};
    bool externallyOwnedRoot_{false};
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
    std::uint64_t generatedBytes{0};
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
            generatedBytes = saturatedAdd(generatedBytes, document.size());
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

struct VectorOracleSnapshot {
    bool complete{false};
    std::string error;
    std::uint64_t storedVectors{0};
    std::uint64_t fetchedVectors{0};
    std::uint64_t embeddedDocuments{0};
    std::uint64_t chunkVectors{0};
    std::uint64_t documentVectors{0};
    std::uint64_t invalidDimensions{0};
    std::uint64_t nonFiniteValues{0};
    std::uint64_t searchResultCount{0};
    std::uint64_t nonFiniteSearchScores{0};
    std::string fingerprint;
    std::string searchFingerprint;
};

VectorOracleSnapshot captureVectorOracle(const yams::vector::VectorDatabase& vectorDb,
                                         std::size_t expectedDim) {
    VectorOracleSnapshot snapshot;
    snapshot.storedVectors = vectorDb.getVectorCount();
    auto hashesResult = vectorDb.getEmbeddedDocumentHashesChecked();
    if (!hashesResult) {
        snapshot.error = hashesResult.error().message;
        return snapshot;
    }

    std::vector<std::string> hashes(hashesResult.value().begin(), hashesResult.value().end());
    std::sort(hashes.begin(), hashes.end());
    snapshot.embeddedDocuments = hashes.size();

    Fnv1a64 fingerprint;
    fingerprint.addInteger(expectedDim);
    std::vector<float> canonicalQuery;
    for (const auto& hash : hashes) {
        auto records = vectorDb.getVectorsByDocument(hash);
        std::sort(records.begin(), records.end(), [](const auto& lhs, const auto& rhs) {
            if (lhs.chunk_id != rhs.chunk_id) {
                return lhs.chunk_id < rhs.chunk_id;
            }
            if (lhs.document_hash != rhs.document_hash) {
                return lhs.document_hash < rhs.document_hash;
            }
            if (lhs.level != rhs.level) {
                return static_cast<int>(lhs.level) < static_cast<int>(rhs.level);
            }
            if (lhs.start_offset != rhs.start_offset) {
                return lhs.start_offset < rhs.start_offset;
            }
            if (lhs.end_offset != rhs.end_offset) {
                return lhs.end_offset < rhs.end_offset;
            }
            if (lhs.model_id != rhs.model_id) {
                return lhs.model_id < rhs.model_id;
            }
            if (lhs.model_version != rhs.model_version) {
                return lhs.model_version < rhs.model_version;
            }
            const auto sharedSize = std::min(lhs.embedding.size(), rhs.embedding.size());
            for (std::size_t index = 0; index < sharedSize; ++index) {
                const auto lhsBits = std::bit_cast<std::uint32_t>(lhs.embedding[index]);
                const auto rhsBits = std::bit_cast<std::uint32_t>(rhs.embedding[index]);
                if (lhsBits != rhsBits) {
                    return lhsBits < rhsBits;
                }
            }
            return lhs.embedding.size() < rhs.embedding.size();
        });
        fingerprint.addBytes(hash);
        fingerprint.addInteger(records.size());
        for (const auto& record : records) {
            fingerprint.addBytes(record.chunk_id);
            fingerprint.addBytes(record.document_hash);
            fingerprint.addInteger(static_cast<std::uint64_t>(record.level));
            fingerprint.addInteger(record.start_offset);
            fingerprint.addInteger(record.end_offset);
            fingerprint.addBytes(record.model_id);
            fingerprint.addBytes(record.model_version);
            fingerprint.addInteger(record.embedding.size());
            if (record.embedding.size() != expectedDim) {
                ++snapshot.invalidDimensions;
            }
            for (const auto value : record.embedding) {
                if (!std::isfinite(value)) {
                    ++snapshot.nonFiniteValues;
                }
                fingerprint.addFloat(value);
            }
            if (canonicalQuery.empty() && record.embedding.size() == expectedDim) {
                canonicalQuery = record.embedding;
            }
            ++snapshot.fetchedVectors;
            if (record.level == yams::vector::EmbeddingLevel::DOCUMENT) {
                ++snapshot.documentVectors;
            } else {
                ++snapshot.chunkVectors;
            }
        }
    }
    snapshot.fingerprint = fingerprint.fingerprint();
    if (!canonicalQuery.empty()) {
        yams::vector::VectorSearchParams params;
        params.k = 10;
        params.similarity_threshold = -1.0F;
        const auto results = vectorDb.search(canonicalQuery, params);
        Fnv1a64 searchFingerprint;
        searchFingerprint.addInteger(results.size());
        for (const auto& record : results) {
            searchFingerprint.addBytes(record.chunk_id);
            searchFingerprint.addBytes(record.document_hash);
            if (!std::isfinite(record.relevance_score)) {
                ++snapshot.nonFiniteSearchScores;
            }
            searchFingerprint.addFloat(record.relevance_score);
        }
        snapshot.searchResultCount = results.size();
        snapshot.searchFingerprint = searchFingerprint.fingerprint();
    }
    snapshot.complete = snapshot.fetchedVectors == snapshot.storedVectors &&
                        snapshot.invalidDimensions == 0 && snapshot.nonFiniteValues == 0 &&
                        snapshot.searchResultCount > 0 && snapshot.nonFiniteSearchScores == 0 &&
                        snapshot.error.empty();
    return snapshot;
}

struct VectorPersistenceSnapshot {
    bool readable{false};
    std::string error;
    std::int64_t authoritativeGeneration{-1};
    std::int64_t sourceGeneration{-1};
    std::uint64_t persistedVectorCount{0};
    std::uint64_t codeCount{0};
    bool trained{false};
};

VectorPersistenceSnapshot captureVectorPersistence(const fs::path& databasePath,
                                                   std::size_t dimension) {
    VectorPersistenceSnapshot snapshot;
    sqlite3* db = nullptr;
    const int openRc = sqlite3_open_v2(databasePath.string().c_str(), &db,
                                       SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, nullptr);
    if (openRc != SQLITE_OK || db == nullptr) {
        snapshot.error = db ? sqlite3_errmsg(db) : "sqlite open failed";
        if (db) {
            sqlite3_close(db);
        }
        return snapshot;
    }

    auto queryOne = [&](const char* sql, std::int64_t bindValue,
                        std::vector<std::int64_t>& values) -> bool {
        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK) {
            snapshot.error = sqlite3_errmsg(db);
            return false;
        }
        if (bindValue >= 0) {
            sqlite3_bind_int64(stmt, 1, bindValue);
        }
        const int stepRc = sqlite3_step(stmt);
        if (stepRc == SQLITE_ROW) {
            const int columns = sqlite3_column_count(stmt);
            values.reserve(columns);
            for (int column = 0; column < columns; ++column) {
                values.push_back(sqlite3_column_int64(stmt, column));
            }
        } else if (stepRc != SQLITE_DONE) {
            snapshot.error = sqlite3_errmsg(db);
        }
        sqlite3_finalize(stmt);
        return stepRc == SQLITE_ROW;
    };

    std::vector<std::int64_t> generation;
    const bool generationOk =
        queryOne("SELECT generation FROM vector_index_generation WHERE id = 1", -1, generation);
    std::vector<std::int64_t> pqMeta;
    const bool pqOk = queryOne(
        "SELECT source_generation, trained, vector_count FROM simeon_pq_meta WHERE dim = ?",
        static_cast<std::int64_t>(dimension), pqMeta);
    std::vector<std::int64_t> codes;
    const bool codesOk = queryOne("SELECT COUNT(*) FROM simeon_pq_codes WHERE dim = ?",
                                  static_cast<std::int64_t>(dimension), codes);
    sqlite3_close(db);

    if (generationOk && !generation.empty()) {
        snapshot.authoritativeGeneration = generation[0];
    }
    if (pqOk && pqMeta.size() == 3) {
        snapshot.sourceGeneration = pqMeta[0];
        snapshot.trained = pqMeta[1] != 0;
        snapshot.persistedVectorCount =
            static_cast<std::uint64_t>(std::max<std::int64_t>(0, pqMeta[2]));
    }
    if (codesOk && !codes.empty()) {
        snapshot.codeCount = static_cast<std::uint64_t>(std::max<std::int64_t>(0, codes[0]));
    }
    snapshot.readable = generationOk && pqOk && codesOk;
    return snapshot;
}

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

struct BenchmarkEmbeddingPhaseTiming {
    static constexpr std::size_t kMaxSamples = 4096;

    std::uint64_t calls{0};
    std::uint64_t totalUs{0};
    std::uint64_t maxUs{0};
    std::vector<std::uint64_t> samplesUs;
    bool samplesTruncated{false};
};

class BenchmarkEmbeddingTimingSink final : public yams::daemon::EmbeddingPhaseTimingSink {
public:
    void record(std::string_view phase, std::uint64_t elapsedUs) override {
        std::lock_guard lock(mutex_);
        auto& timing = timings_[std::string(phase)];
        ++timing.calls;
        timing.totalUs += elapsedUs;
        timing.maxUs = std::max(timing.maxUs, elapsedUs);
        if (timing.samplesUs.size() < BenchmarkEmbeddingPhaseTiming::kMaxSamples) {
            timing.samplesUs.push_back(elapsedUs);
        } else {
            timing.samplesTruncated = true;
        }
    }

    std::unordered_map<std::string, BenchmarkEmbeddingPhaseTiming> snapshot() const {
        std::lock_guard lock(mutex_);
        return timings_;
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, BenchmarkEmbeddingPhaseTiming> timings_;
};

struct SearchImpactProbe {
    std::string name;
    std::string query;
    std::string search_type;
    bool required_by_contract = false;
    bool skipped = false;
    std::string skip_reason{};
    bool ok = false;
    std::string error{};
    int64_t wall_ms = 0;
    int64_t completed_ms = 0;
    int64_t daemon_elapsed_ms = 0;
    uint64_t total_count = 0;
    uint64_t returned_count = 0;
    std::vector<std::string> top_ids{};
    std::vector<std::string> top_paths{};
    std::vector<double> top_scores{};
    bool scores_finite = true;

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
                    {"top_ids", top_ids},
                    {"top_paths", top_paths},
                    {"top_scores", top_scores},
                    {"scores_finite", scores_finite}};
    }
};

struct AddDispatchMetrics {
    uint64_t samples = 0;
    uint64_t total_us = 0;
    uint64_t max_us = 0;
    uint64_t fingerprint_total_us = 0;
    uint64_t fingerprint_max_us = 0;
    uint64_t enqueue_total_us = 0;
    uint64_t enqueue_max_us = 0;

    json toJson() const {
        const auto average = [this](uint64_t total) {
            return samples == 0 ? 0.0 : static_cast<double>(total) / static_cast<double>(samples);
        };
        return json{{"samples", samples},
                    {"total_us", total_us},
                    {"max_us", max_us},
                    {"avg_us", average(total_us)},
                    {"fingerprint_total_us", fingerprint_total_us},
                    {"fingerprint_max_us", fingerprint_max_us},
                    {"fingerprint_avg_us", average(fingerprint_total_us)},
                    {"enqueue_total_us", enqueue_total_us},
                    {"enqueue_max_us", enqueue_max_us},
                    {"enqueue_avg_us", average(enqueue_total_us)}};
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
    std::string fixture_phase = "initial";
    std::uint32_t post_ingest_coalesce_ms = 0;
    std::uint32_t post_ingest_batch_size = 0;
    std::uint64_t enrichment_timeout_ms = 30000;
    std::string timestamp;
    std::string embedding_model = "simeon-default";
    std::string embedding_backend = "simeon";
    std::string embedding_provider;
    std::string embedding_provider_version;
    std::string embedding_model_uri;
    std::string embedding_space_identity;
    std::size_t embedding_dimension = 0;
    std::string simeon_recipe;
    std::uint64_t generated_bytes = 0;
    bool kg_enabled = true;
    bool gliner_requested = false;
    bool plugin_autoload_enabled = false;
    bool gliner_stage_active = false;
    std::string gliner_model_path;

    // Overall metrics
    int64_t total_duration_ms = 0;
    double throughput_docs_per_sec = 0.0;
    double throughput_mib_per_sec = 0.0;
    bool pipeline_complete = false;
    uint64_t expected_post = 0;
    uint64_t expected_embed = 0;
    uint64_t expected_kg = 0;
    uint64_t expected_title = 0;
    uint64_t observed_post = 0;
    uint64_t observed_embed = 0;
    uint64_t observed_kg = 0;
    uint64_t observed_title = 0;
    bool stages_settled = false;
    bool enrichment_drained = false;
    bool write_coordinator_flushed = false;
    int64_t admission_ms = 0;
    int64_t storage_ready_ms = 0;
    int64_t pipeline_drain_ms = 0;
    int64_t enrichment_ready_ms = 0;
    int64_t vector_index_finalize_ms = 0;
    int64_t searchability_ready_ms = 0;
    int64_t daemon_startup_ms = 0;
    int64_t shutdown_ms = 0;
    int64_t lifecycle_ms = 0;
    std::uint64_t cpu_ms = 0;
    std::uint64_t peak_rss_before_bytes = 0;
    std::uint64_t peak_rss_after_bytes = 0;
    std::uint64_t wal_bytes_before = 0;
    std::uint64_t wal_bytes_after = 0;
    std::uint64_t wal_growth_bytes = 0;
    std::uint64_t db_lock_errors = 0;
    bool shutdown_succeeded = false;
    bool socket_removed = false;
    bool pid_removed = false;
    bool sidecars_removed = false;
    AddDispatchMetrics add_dispatch_metrics;

    std::uint64_t stored_documents = 0;
    bool stored_document_count_readable = false;
    std::string stored_document_count_error;
    VectorOracleSnapshot vector_oracle;
    bool vector_rebuild_ok = false;
    std::string vector_rebuild_error;
    bool vector_index_reusable = false;
    yams::daemon::VectorIndexTelemetry vector_index_telemetry;
    VectorPersistenceSnapshot vector_persistence;
    std::string search_result_fingerprint;
    std::string search_score_fingerprint;
    bool startup_oracle_required = false;
    bool startup_oracle_complete = false;
    std::uint64_t startup_stored_documents = 0;
    bool startup_stored_document_count_readable = false;
    bool startup_vector_reusable = false;
    VectorOracleSnapshot startup_vector_oracle;
    yams::daemon::VectorIndexTelemetry startup_vector_index_telemetry;
    std::vector<SearchImpactProbe> startup_search_impact;
    std::string startup_search_result_fingerprint;
    std::string startup_search_score_fingerprint;

    // Per-stage metrics
    StageMetrics metadata_storage;
    StageMetrics fts5_extraction;
    StageMetrics embedding_generation;
    StageMetrics kg_extraction;
    std::unordered_map<std::string, BenchmarkEmbeddingPhaseTiming> embedding_phase_timings;
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
    yams::metadata::MetadataInsertWriter::MetricsSnapshot metadata_insert_writer_metrics;
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

    [[nodiscard]] json toJson() const {
        json j;
        j["test_config"] = {{"corpus_size", corpus_size},
                            {"doc_size", doc_size},
                            {"poll_interval_ms", poll_interval_ms},
                            {"corpus_seed", corpus_seed},
                            {"corpus_fingerprint", corpus_fingerprint},
                            {"ingest_mode", ingest_mode},
                            {"ingest_concurrency", ingest_concurrency},
                            {"fixture_phase", fixture_phase},
                            {"post_ingest_coalesce_ms", post_ingest_coalesce_ms},
                            {"post_ingest_batch_size", post_ingest_batch_size},
                            {"enrichment_timeout_ms", enrichment_timeout_ms},
                            {"timestamp", timestamp},
                            {"embedding_model", embedding_model},
                            {"embedding_backend", embedding_backend},
                            {"embedding_provider", embedding_provider},
                            {"embedding_provider_version", embedding_provider_version},
                            {"embedding_model_uri", embedding_model_uri},
                            {"embedding_space_identity", embedding_space_identity},
                            {"embedding_dimension", embedding_dimension},
                            {"simeon_recipe", simeon_recipe},
                            {"generated_bytes", generated_bytes},
                            {"kg_enabled", kg_enabled},
                            {"gliner_requested", gliner_requested},
                            {"plugin_autoload_enabled", plugin_autoload_enabled},
                            {"gliner_stage_active", gliner_stage_active},
                            {"gliner_model_path", gliner_model_path}};

        j["total_duration_ms"] = total_duration_ms;
        j["throughput_docs_per_sec"] = throughput_docs_per_sec;
        j["throughput_mib_per_sec"] = throughput_mib_per_sec;
        j["pipeline_status"] = {{"complete", pipeline_complete},
                                {"stages_settled", stages_settled},
                                {"enrichment_drained", enrichment_drained},
                                {"write_coordinator_flushed", write_coordinator_flushed},
                                {"expected_post", expected_post},
                                {"observed_post", observed_post},
                                {"expected_embed", expected_embed},
                                {"observed_embed", observed_embed},
                                {"expected_kg", expected_kg},
                                {"observed_kg", observed_kg},
                                {"expected_title", expected_title},
                                {"observed_title", observed_title}};
        j["phase_timings"] = {{"daemon_startup_ms", daemon_startup_ms},
                              {"admission_ms", admission_ms},
                              {"storage_ready_ms", storage_ready_ms},
                              {"pipeline_drain_ms", pipeline_drain_ms},
                              {"enrichment_ready_ms", enrichment_ready_ms},
                              {"vector_index_finalize_ms", vector_index_finalize_ms},
                              {"searchability_ready_ms", searchability_ready_ms},
                              {"shutdown_ms", shutdown_ms},
                              {"lifecycle_ms", lifecycle_ms}};
        j["storage_oracle"] = {
            {"stored_documents", stored_documents},
            {"stored_document_count_readable", stored_document_count_readable},
            {"stored_document_count_error", stored_document_count_error},
            {"stored_vectors", vector_oracle.storedVectors},
            {"fetched_vectors", vector_oracle.fetchedVectors},
            {"embedded_documents", vector_oracle.embeddedDocuments},
            {"chunk_vectors", vector_oracle.chunkVectors},
            {"document_vectors", vector_oracle.documentVectors},
            {"invalid_vector_dimensions", vector_oracle.invalidDimensions},
            {"non_finite_vector_values", vector_oracle.nonFiniteValues},
            {"vector_search_result_count", vector_oracle.searchResultCount},
            {"non_finite_vector_search_scores", vector_oracle.nonFiniteSearchScores},
            {"vector_search_fingerprint", vector_oracle.searchFingerprint},
            {"vector_oracle_complete", vector_oracle.complete},
            {"vector_oracle_error", vector_oracle.error},
            {"embedding_fingerprint", vector_oracle.fingerprint},
            {"search_result_fingerprint", search_result_fingerprint},
            {"search_score_fingerprint", search_score_fingerprint}};
        j["restart_oracle"] = {
            {"required", startup_oracle_required},
            {"complete", startup_oracle_complete},
            {"stored_documents", startup_stored_documents},
            {"stored_document_count_readable", startup_stored_document_count_readable},
            {"vector_reusable", startup_vector_reusable},
            {"stored_vectors", startup_vector_oracle.storedVectors},
            {"embedded_documents", startup_vector_oracle.embeddedDocuments},
            {"invalid_vector_dimensions", startup_vector_oracle.invalidDimensions},
            {"non_finite_vector_values", startup_vector_oracle.nonFiniteValues},
            {"vector_search_result_count", startup_vector_oracle.searchResultCount},
            {"non_finite_vector_search_scores", startup_vector_oracle.nonFiniteSearchScores},
            {"vector_search_fingerprint", startup_vector_oracle.searchFingerprint},
            {"vector_oracle_complete", startup_vector_oracle.complete},
            {"embedding_fingerprint", startup_vector_oracle.fingerprint},
            {"search_result_fingerprint", startup_search_result_fingerprint},
            {"search_score_fingerprint", startup_search_score_fingerprint},
            {"coordinator_ready", startup_vector_index_telemetry.ready},
            {"coordinator_rebuilding", startup_vector_index_telemetry.rebuilding},
            {"coordinator_epoch", startup_vector_index_telemetry.rebuildEpoch}};
        j["vector_index"] = {
            {"rebuild_ok", vector_rebuild_ok},
            {"rebuild_error", vector_rebuild_error},
            {"reusable_persisted_index", vector_index_reusable},
            {"coordinator_epoch", vector_index_telemetry.rebuildEpoch},
            {"coordinator_pending_reasons", vector_index_telemetry.pendingReasons},
            {"coordinator_active_bulk_scopes", vector_index_telemetry.activeBulkScopes},
            {"coordinator_rebuilding", vector_index_telemetry.rebuilding},
            {"coordinator_ready", vector_index_telemetry.ready},
            {"coordinator_progress_pct", vector_index_telemetry.progressPct},
            {"persistence_readable", vector_persistence.readable},
            {"persistence_error", vector_persistence.error},
            {"authoritative_generation", vector_persistence.authoritativeGeneration},
            {"source_generation", vector_persistence.sourceGeneration},
            {"trained", vector_persistence.trained},
            {"persisted_vector_count", vector_persistence.persistedVectorCount},
            {"code_count", vector_persistence.codeCount}};
        j["resources"] = {{"cpu_ms", cpu_ms},
                          {"peak_rss_before_bytes", peak_rss_before_bytes},
                          {"peak_rss_after_bytes", peak_rss_after_bytes},
                          {"wal_bytes_before", wal_bytes_before},
                          {"wal_bytes_after", wal_bytes_after},
                          {"wal_growth_bytes", wal_growth_bytes},
                          {"db_lock_errors", db_lock_errors},
                          {"shutdown_succeeded", shutdown_succeeded},
                          {"socket_removed", socket_removed},
                          {"pid_removed", pid_removed},
                          {"sidecars_removed", sidecars_removed}};
        j["add_dispatch_metrics"] = add_dispatch_metrics.toJson();

        j["stages"] = {{"metadata_storage", metadata_storage.toJson()},
                       {"fts5_extraction", fts5_extraction.toJson()},
                       {"embedding_generation", embedding_generation.toJson()},
                       {"kg_extraction", kg_extraction.toJson()}};

        json phaseTimings = json::object();
        for (const auto& [phase, timing] : embedding_phase_timings) {
            phaseTimings[phase] = {
                {"calls", timing.calls},
                {"total_ms", timing.totalUs / 1000},
                {"max_ms", timing.maxUs / 1000},
                {"avg_ms", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   (1000.0 * static_cast<double>(timing.calls))},
                {"total_us", timing.totalUs},
                {"max_us", timing.maxUs},
                {"avg_us", timing.calls == 0 ? 0.0
                                             : static_cast<double>(timing.totalUs) /
                                                   static_cast<double>(timing.calls)},
                {"p50_us", percentileUs(timing.samplesUs, 0.50)},
                {"p95_us", percentileUs(timing.samplesUs, 0.95)},
                {"p99_us", percentileUs(timing.samplesUs, 0.99)},
                {"samples", timing.samplesUs.size()},
                {"samples_truncated", timing.samplesTruncated}};
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

        j["metadata_insert_writer_metrics"] = {
            {"submitted_items", metadata_insert_writer_metrics.submittedItems},
            {"completed_items", metadata_insert_writer_metrics.completedItems},
            {"rejected_items", metadata_insert_writer_metrics.rejectedItems},
            {"batches", metadata_insert_writer_metrics.batches},
            {"batch_items", metadata_insert_writer_metrics.batchItems},
            {"max_batch_size", metadata_insert_writer_metrics.maxBatchSize},
            {"avg_batch_size",
             metadata_insert_writer_metrics.batches == 0
                 ? 0.0
                 : static_cast<double>(metadata_insert_writer_metrics.batchItems) /
                       static_cast<double>(metadata_insert_writer_metrics.batches)},
            {"queue_wait_samples", metadata_insert_writer_metrics.queueWaitSamples},
            {"queue_wait_total_us", metadata_insert_writer_metrics.queueWaitTotalUs},
            {"queue_wait_max_us", metadata_insert_writer_metrics.queueWaitMaxUs},
            {"queue_wait_avg_us",
             metadata_insert_writer_metrics.queueWaitSamples == 0
                 ? 0.0
                 : static_cast<double>(metadata_insert_writer_metrics.queueWaitTotalUs) /
                       static_cast<double>(metadata_insert_writer_metrics.queueWaitSamples)},
            {"batch_apply_samples", metadata_insert_writer_metrics.batchApplySamples},
            {"batch_apply_total_us", metadata_insert_writer_metrics.batchApplyTotalUs},
            {"batch_apply_max_us", metadata_insert_writer_metrics.batchApplyMaxUs},
            {"batch_apply_avg_us",
             metadata_insert_writer_metrics.batchApplySamples == 0
                 ? 0.0
                 : static_cast<double>(metadata_insert_writer_metrics.batchApplyTotalUs) /
                       static_cast<double>(metadata_insert_writer_metrics.batchApplySamples)},
            {"failed_batches", metadata_insert_writer_metrics.failedBatches},
            {"fallback_items", metadata_insert_writer_metrics.fallbackItems}};

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
        json startupSearchImpact = json::array();
        for (const auto& probe : startup_search_impact) {
            startupSearchImpact.push_back(probe.toJson());
        }
        j["startup_search_impact"] = std::move(startupSearchImpact);

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
        searchProbes != nullptr && std::string(searchProbes) == "0") {
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
        probe.daemon_elapsed_ms = value.elapsed.count();
        probe.total_count = static_cast<uint64_t>(value.totalCount);
        probe.returned_count = static_cast<uint64_t>(value.results.size());
        for (const auto& item : value.results) {
            probe.top_ids.push_back(item.id);
            probe.top_paths.push_back(item.path.empty() ? std::string{}
                                                        : fs::path(item.path).filename().string());
            probe.top_scores.push_back(item.score);
            probe.scores_finite = probe.scores_finite && std::isfinite(item.score);
            if (probe.top_paths.size() >= 5) {
                break;
            }
        }
        probe.ok =
            probe.scores_finite && !probe.top_paths.empty() &&
            std::ranges::all_of(probe.top_paths, [](const auto& path) { return !path.empty(); });
        if (!probe.ok) {
            probe.error = "search returned no stable document paths";
        }
    }

    return probes;
}

std::string fingerprintSearchImpact(const std::vector<SearchImpactProbe>& probes) {
    Fnv1a64 fingerprint;
    for (const auto& probe : probes) {
        fingerprint.addBytes(probe.name);
        fingerprint.addInteger(probe.ok ? 1 : 0);
        fingerprint.addInteger(probe.total_count);
        fingerprint.addInteger(probe.top_paths.size());
        for (const auto& path : probe.top_paths) {
            fingerprint.addBytes(path);
        }
    }
    return fingerprint.fingerprint();
}

std::string fingerprintSearchScores(const std::vector<SearchImpactProbe>& probes) {
    Fnv1a64 fingerprint;
    for (const auto& probe : probes) {
        fingerprint.addBytes(probe.name);
        fingerprint.addInteger(probe.top_scores.size());
        for (const double score : probe.top_scores) {
            fingerprint.addInteger(std::bit_cast<std::uint64_t>(score));
        }
    }
    return fingerprint.fingerprint();
}

boost::asio::awaitable<yams::Result<yams::app::services::BatchAddResult>>
submitBatchAsync(yams::app::services::DocumentIngestionService& service,
                 const std::vector<yams::app::services::AddOptions>& batch, int requestWindow) {
    co_return co_await service.addBatchAsync(batch, requestWindow);
}

// ============================================================================
// runBenchmark - Main benchmark logic
// ============================================================================

BenchmarkResult runBenchmark(int corpusSize, int docSize, int pollIntervalMs,
                             std::string ingestMode, std::size_t ingestConcurrency) {
    const auto _lifecycleStart = std::chrono::steady_clock::now();
    const auto _cpuStart = std::clock();
    yams::daemon::TuneAdvisor::setPostIngestBatchSize(benchPostIngestBatchSize());

    BenchmarkResult result;
    auto embeddingTimingSink = std::make_shared<BenchmarkEmbeddingTimingSink>();
    result.peak_rss_before_bytes = peakRssBytes();
    result.corpus_size = corpusSize;
    result.doc_size = docSize;
    result.poll_interval_ms = pollIntervalMs;
    result.ingest_mode = std::move(ingestMode);
    result.ingest_concurrency = std::max<std::size_t>(1, ingestConcurrency);
    result.fixture_phase = benchPhase();
    result.post_ingest_coalesce_ms = benchPostIngestCoalesceMs();
    result.post_ingest_batch_size = yams::daemon::TuneAdvisor::postIngestBatchSize();
    result.timestamp = nowIso8601();
    const char* forceMockEmbeddings = std::getenv("YAMS_BENCH_FORCE_MOCK_EMBEDDINGS");
    const bool mockEmbeddingsRequested =
        forceMockEmbeddings != nullptr && std::string(forceMockEmbeddings) == "1";
    const char* disableVectors = std::getenv("YAMS_DISABLE_VECTORS");
    const bool vectorsDisabled = mockEmbeddingsRequested ||
                                 (disableVectors != nullptr && std::string(disableVectors) == "1");
    if (vectorsDisabled) {
        result.embedding_backend = "mock";
    }
    result.simeon_recipe = !vectorsDisabled ? yams::vector::simeonRecipeLabel() : "";
    result.kg_enabled = !envFlagEnabled("YAMS_BENCH_DISABLE_KG");

    spdlog::info("=== Ingestion E2E Benchmark ===");
    spdlog::info("Corpus size: {}", corpusSize);
    spdlog::info("Doc size: {} bytes", docSize);
    spdlog::info("Poll interval: {} ms", pollIntervalMs);
    spdlog::info("Ingest mode: {} (concurrency={})", result.ingest_mode, result.ingest_concurrency);
    spdlog::info("Fixture phase: {}", result.fixture_phase);
    spdlog::info("Post-ingest coalesce window: {} ms", result.post_ingest_coalesce_ms);
    spdlog::info("Post-ingest batch size: {}", result.post_ingest_batch_size);
    spdlog::info("KG enrichment: {}", result.kg_enabled ? "enabled" : "disabled");

    // Phase 1: Start daemon
    spdlog::info("Phase 1: Starting daemon...");
    SimpleDaemonHarness harness;
    const auto startupStart = std::chrono::steady_clock::now();
    if (!harness.start()) {
        spdlog::error("Failed to start daemon");
        return result;
    }
    result.daemon_startup_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::steady_clock::now() - startupStart)
                                   .count();
    result.gliner_requested = harness.glinerRequested();
    result.plugin_autoload_enabled = harness.pluginAutoloadEnabled();
    if (const char* modelPath = std::getenv("YAMS_GLINT_MODEL_PATH"); modelPath != nullptr) {
        result.gliner_model_path = modelPath;
    }
    result.wal_bytes_before = sidecarBytes(harness.dataDir(), "-wal");

    const auto stopHarness = [&](bool capturePersistence) {
        const auto shutdownStart = std::chrono::steady_clock::now();
        harness.stop();
        result.shutdown_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - shutdownStart)
                                 .count();
        result.shutdown_succeeded = harness.shutdownSucceeded();
        result.socket_removed = !fs::exists(harness.socketPath());
        result.pid_removed = !fs::exists(harness.pidPath());
        result.sidecars_removed = !hasSidecars(harness.dataDir());
        if (capturePersistence && !vectorsDisabled) {
            result.vector_persistence = captureVectorPersistence(harness.dataDir() / "vectors.db",
                                                                 result.embedding_dimension);
        }
    };

    // Phase 2: Generate corpus
    spdlog::info("Phase 2: Generating test corpus...");
    fs::path corpusDir = harness.root() / "corpus";
    CorpusGenerator corpus(corpusDir, docSize);
    corpus.generateDocuments(corpusSize);
    result.corpus_fingerprint = corpus.fingerprint();
    result.generated_bytes = corpus.generatedBytes;

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
    auto* serviceManager = harness.daemon()->getServiceManager();
    if (serviceManager != nullptr) {
        auto modelProvider = serviceManager->getModelProvider();
        if (modelProvider && modelProvider->isAvailable()) {
            auto loadedModels = modelProvider->getLoadedModels();
            spdlog::info("Model provider: {} model(s) loaded: {}", loadedModels.size(),
                         loadedModels.empty() ? "none" : std::to_string(loadedModels.size()));
            result.embedding_provider = modelProvider->getProviderName();
            result.embedding_provider_version = modelProvider->getProviderVersion();
            result.embedding_dimension = modelProvider->getEmbeddingDim(result.embedding_model);
            result.embedding_space_identity =
                modelProvider->getEmbeddingSpaceIdentity(result.embedding_model);
            if (auto info = modelProvider->getModelInfo(result.embedding_model); info) {
                result.embedding_model_uri = info.value().path;
            } else {
                result.embedding_model_uri = "simeon://" + result.embedding_model;
            }
        } else {
            spdlog::warn("Model provider not available");
        }

        if (result.fixture_phase != "initial") {
            result.startup_oracle_required = result.fixture_phase == "replay";
            if (auto metadataRepo = serviceManager->getMetadataRepo()) {
                if (auto countResult = metadataRepo->getDocumentCount(); countResult) {
                    result.startup_stored_documents =
                        static_cast<std::uint64_t>(std::max<std::int64_t>(0, countResult.value()));
                    result.startup_stored_document_count_readable = true;
                }
            }

            if (!vectorsDisabled) {
                if (auto vectorDb = serviceManager->getVectorDatabase()) {
                    if (result.startup_oracle_required) {
                        const auto reuseDeadline = std::chrono::steady_clock::now() + 30s;
                        while (!vectorDb->hasReusablePersistedSearchIndex() &&
                               std::chrono::steady_clock::now() < reuseDeadline) {
                            std::this_thread::sleep_for(20ms);
                        }
                    }
                    result.startup_vector_reusable = vectorDb->hasReusablePersistedSearchIndex();
                    result.startup_vector_oracle =
                        captureVectorOracle(*vectorDb, result.embedding_dimension);
                }
                if (auto coordinator = serviceManager->getVectorIndexCoordinator()) {
                    result.startup_vector_index_telemetry = coordinator->snapshot();
                }
            }

            const bool startupStorageComplete =
                result.startup_stored_document_count_readable &&
                result.startup_stored_documents == static_cast<std::uint64_t>(corpusSize);
            const bool startupVectorComplete =
                vectorsDisabled ||
                (result.startup_vector_reusable && result.startup_vector_oracle.complete &&
                 result.startup_vector_oracle.embeddedDocuments ==
                     static_cast<std::uint64_t>(corpusSize));
            if (result.startup_oracle_required) {
                result.startup_search_impact =
                    runSearchImpactProbes(client, vectorsDisabled, result.kg_enabled,
                                          startupStorageComplete && startupVectorComplete);
                result.startup_search_result_fingerprint =
                    fingerprintSearchImpact(result.startup_search_impact);
                result.startup_search_score_fingerprint =
                    fingerprintSearchScores(result.startup_search_impact);
            }
            bool startupSearchComplete = true;
            for (const auto& probe : result.startup_search_impact) {
                startupSearchComplete =
                    startupSearchComplete && (!probe.required_by_contract || probe.ok);
            }
            result.startup_oracle_complete =
                startupStorageComplete && startupVectorComplete && startupSearchComplete;
        }

        serviceManager->setEmbeddingPhaseTimingSink(embeddingTimingSink);
        yams::app::services::resetDocumentStorePhaseTimings();
        yams::api::resetContentStorePhaseTimings();
        yams::metadata::resetMetadataInsertPhaseTimings();
        yams::storage::resetRefCounterCommitPhaseTimings();
        yams::storage::resetRefCounterWriterMetrics();
        auto appContext = serviceManager->getAppContext();
        if (appContext.metadataInsertWriter) {
            appContext.metadataInsertWriter->resetMetrics();
        }
        if (auto postIngest = serviceManager->getPostIngestQueue()) {
            postIngest->resetMetrics();
            result.gliner_stage_active =
                postIngest->hasTitleExtractor() && !envFlagEnabled("YAMS_DISABLE_GLINER_TITLES");
            if (!result.kg_enabled) {
                postIngest->setKnowledgeGraphEnabled(false);
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
    uint64_t baselineTitleQueued = bus.titleQueued();
    uint64_t baselineTitleConsumed = bus.titleConsumed();
    uint64_t baselineTitleDropped = bus.titleDropped();
    spdlog::info("Baseline counters: embed={}, post={}, kg={}, title={}", baselineEmbedConsumed,
                 baselinePostConsumed, baselineKgConsumed, baselineTitleConsumed);

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
        const std::size_t requestWindow =
            std::min<std::size_t>(result.ingest_concurrency, corpus.createdFiles.size());
        std::vector<yams::app::services::AddOptions> batch;
        batch.reserve(corpus.createdFiles.size());
        for (const auto& filename : corpus.createdFiles) {
            yams::app::services::AddOptions options;
            options.path = (corpusDir / filename).string();
            options.noEmbeddings = vectorsDisabled;
            batch.push_back(std::move(options));
        }

        yams::app::services::DocumentIngestionService ingestionService(client);
        auto batchResult = yams::cli::run_sync(
            submitBatchAsync(ingestionService, batch, static_cast<int>(requestWindow)), 120s);
        if (!batchResult) {
            spdlog::warn("Pipelined ingestion failed: {}", batchResult.error().message);
            failCount.store(corpusSize, std::memory_order_relaxed);
        } else {
            successCount.store(static_cast<int>(batchResult.value().succeeded),
                               std::memory_order_relaxed);
            failCount.store(static_cast<int>(batchResult.value().failed),
                            std::memory_order_relaxed);
        }
    } else {
        result.ingest_mode = "single_file_serial";
        result.ingest_concurrency = 1;
        for (const auto& filename : corpus.createdFiles) {
            if (harness.stopRequested()) {
                break;
            }
            addSingleFile(*client, filename);
        }
    }

    int64_t ingestEndMs = nowMs();
    result.metadata_storage.end_ms = ingestEndMs;
    result.admission_ms = nonNegativeDeltaMs(ingestEndMs, ingestStartMs);
    result.metadata_storage.count = static_cast<std::uint64_t>(successCount.load());
    result.metadata_storage.failures = static_cast<std::uint64_t>(failCount.load());
    const auto& addStats = harness.daemon()->getState().stats;
    result.add_dispatch_metrics = {
        .samples = addStats.addDispatchSamples.load(std::memory_order_relaxed),
        .total_us = addStats.addDispatchTotalUs.load(std::memory_order_relaxed),
        .max_us = addStats.addDispatchMaxUs.load(std::memory_order_relaxed),
        .fingerprint_total_us = addStats.addFingerprintTotalUs.load(std::memory_order_relaxed),
        .fingerprint_max_us = addStats.addFingerprintMaxUs.load(std::memory_order_relaxed),
        .enqueue_total_us = addStats.addEnqueueTotalUs.load(std::memory_order_relaxed),
        .enqueue_max_us = addStats.addEnqueueMaxUs.load(std::memory_order_relaxed)};

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
    uint64_t expectedTitle =
        result.gliner_stage_active ? static_cast<std::uint64_t>(successCount.load()) : 0;
    result.expected_post = expectedPost;
    result.expected_embed = expectedEmbed;
    result.expected_kg = expectedKg;
    result.expected_title = expectedTitle;
    constexpr std::uint64_t kBaseEnrichmentTimeoutMs = 30000;
    constexpr std::uint64_t kPerTitleTimeoutMs = 60000;
    constexpr std::uint64_t kMaxEnrichmentTimeoutMs = 2ULL * 60ULL * 60ULL * 1000ULL;
    result.enrichment_timeout_ms =
        std::min(kMaxEnrichmentTimeoutMs,
                 saturatedAdd(kBaseEnrichmentTimeoutMs, expectedTitle * kPerTitleTimeoutMs));

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

    while (std::chrono::steady_clock::now() < pipelineDeadline && !allStagesSettled &&
           !harness.stopRequested()) {
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
    const auto enrichmentDeadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(result.enrichment_timeout_ms);
    while (std::chrono::steady_clock::now() < enrichmentDeadline && !harness.stopRequested()) {
        lastSnap = captureQueueSnapshot();
        bool postIngestIdle = true;
        if (serviceManager != nullptr) {
            if (auto postIngest = serviceManager->getPostIngestQueue()) {
                postIngestIdle =
                    postIngest->size() == 0 && postIngest->kgQueueDepth() == 0 &&
                    postIngest->symbolQueueDepth() == 0 && postIngest->entityQueueDepth() == 0 &&
                    postIngest->titleQueueDepth() == 0 && postIngest->totalInFlight() == 0;
            }
        }
        const bool embeddingIdle =
            lastSnap.embed_jobs == 0 &&
            (serviceManager == nullptr || serviceManager->getEmbeddingInFlightJobs() == 0);
        if (postIngestIdle && embeddingIdle) {
            result.enrichment_drained = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
    }

    if (harness.stopRequested()) {
        lastSnap = captureQueueSnapshot();
        result.observed_post = counterDelta(lastSnap.post_consumed, baselinePostConsumed);
        result.observed_embed = counterDelta(lastSnap.embed_consumed, baselineEmbedConsumed);
        result.observed_kg = counterDelta(lastSnap.kg_consumed, baselineKgConsumed);
        result.observed_title = counterDelta(lastSnap.title_consumed, baselineTitleConsumed);
        spdlog::warn("Termination requested; stopping isolated benchmark fixture");
        stopHarness(false);
        return result;
    }

    result.write_coordinator_flushed = true;
    if (serviceManager != nullptr) {
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
    result.final_title_queued = counterDelta(lastSnap.title_queued, baselineTitleQueued);
    result.final_title_consumed = counterDelta(lastSnap.title_consumed, baselineTitleConsumed);
    result.final_title_dropped = counterDelta(lastSnap.title_dropped, baselineTitleDropped);
    result.observed_title = result.final_title_consumed;
    result.dropped_batches = saturatedAdd(
        result.dropped_batches,
        saturatedAdd(result.final_symbol_dropped,
                     saturatedAdd(result.final_entity_dropped, result.final_title_dropped)));

    const bool titleComplete = result.observed_title == result.expected_title;
    result.pipeline_complete = allStagesComplete && titleComplete && result.enrichment_drained &&
                               result.write_coordinator_flushed && result.dropped_batches == 0 &&
                               failCount.load(std::memory_order_relaxed) == 0;
    const int64_t enrichmentEndMs = nowMs();
    result.pipeline_drain_ms = nonNegativeDeltaMs(enrichmentEndMs, storageReadyEndMs);
    result.enrichment_ready_ms = nonNegativeDeltaMs(enrichmentEndMs, ingestStartMs);

    if (serviceManager != nullptr) {
        if (auto metadataRepo = serviceManager->getMetadataRepo()) {
            auto countResult = metadataRepo->getDocumentCount();
            if (countResult) {
                result.stored_documents =
                    static_cast<std::uint64_t>(std::max<std::int64_t>(0, countResult.value()));
                result.stored_document_count_readable = true;
            } else {
                result.stored_document_count_error = countResult.error().message;
            }
        }

        if (!vectorsDisabled) {
            const auto vectorFinalizeStart = std::chrono::steady_clock::now();
            if (auto coordinator = serviceManager->getVectorIndexCoordinator()) {
                auto rebuildResult = yams::cli::run_sync(
                    coordinator->requestRebuild(yams::daemon::RebuildReason::EmbeddingBatch), 300s);
                result.vector_rebuild_ok = static_cast<bool>(rebuildResult);
                if (!rebuildResult) {
                    result.vector_rebuild_error = rebuildResult.error().message;
                }
                result.vector_index_telemetry = coordinator->snapshot();
            } else {
                result.vector_rebuild_error = "vector index coordinator unavailable";
            }
            result.vector_index_finalize_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - vectorFinalizeStart)
                    .count();

            if (auto vectorDb = serviceManager->getVectorDatabase()) {
                result.vector_index_reusable = vectorDb->hasReusablePersistedSearchIndex();
                result.vector_oracle = captureVectorOracle(*vectorDb, result.embedding_dimension);
            } else {
                result.vector_oracle.error = "vector database unavailable";
            }
        } else {
            result.vector_rebuild_ok = true;
            result.vector_oracle.complete = true;
        }
    }

    const bool corePipelineComplete = result.pipeline_complete;
    spdlog::info("Phase 6: Running fixed search-impact probes...");
    result.search_impact =
        runSearchImpactProbes(client, vectorsDisabled, result.kg_enabled, corePipelineComplete);
    result.search_result_fingerprint = fingerprintSearchImpact(result.search_impact);
    result.search_score_fingerprint = fingerprintSearchScores(result.search_impact);
    bool searchOracleComplete = true;
    for (const auto& probe : result.search_impact) {
        if (!probe.required_by_contract) {
            continue;
        }
        searchOracleComplete = searchOracleComplete && probe.ok;
        if (probe.ok) {
            result.searchability_ready_ms =
                std::max(result.searchability_ready_ms,
                         nonNegativeDeltaMs(probe.completed_ms, ingestStartMs));
        }
    }

    const auto expectedStoredDocuments = static_cast<std::uint64_t>(corpusSize);
    const bool documentOracleComplete =
        result.stored_document_count_readable && result.stored_documents == expectedStoredDocuments;
    const bool vectorOracleComplete =
        vectorsDisabled || (result.vector_rebuild_ok && result.vector_oracle.complete &&
                            result.vector_oracle.embeddedDocuments == expectedStoredDocuments);
    const bool glinerOracleComplete = !result.gliner_requested || result.gliner_stage_active;
    result.pipeline_complete = result.pipeline_complete && documentOracleComplete &&
                               vectorOracleComplete && glinerOracleComplete && searchOracleComplete;
    if (result.startup_oracle_required) {
        const bool replayFingerprintsStable =
            result.startup_vector_oracle.fingerprint == result.vector_oracle.fingerprint &&
            result.startup_vector_oracle.searchFingerprint ==
                result.vector_oracle.searchFingerprint &&
            result.startup_search_result_fingerprint == result.search_result_fingerprint &&
            result.startup_search_score_fingerprint == result.search_score_fingerprint;
        result.pipeline_complete =
            result.pipeline_complete && result.startup_oracle_complete && replayFingerprintsStable;
    }

    result.total_duration_ms = std::max(result.enrichment_ready_ms, result.searchability_ready_ms);
    const double elapsedSeconds = static_cast<double>(result.total_duration_ms) / 1000.0;
    result.throughput_docs_per_sec =
        elapsedSeconds > 0.0 ? static_cast<double>(successCount.load()) / elapsedSeconds : 0.0;
    result.throughput_mib_per_sec =
        elapsedSeconds > 0.0
            ? (static_cast<double>(result.generated_bytes) / (1024.0 * 1024.0)) / elapsedSeconds
            : 0.0;

    if (serviceManager != nullptr) {
        result.embedding_phase_timings = embeddingTimingSink->snapshot();
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
        auto appContext = serviceManager->getAppContext();
        if (appContext.metadataInsertWriter) {
            result.metadata_insert_writer_metrics =
                appContext.metadataInsertWriter->metricsSnapshot();
        }
        if (auto postIngest = serviceManager->getPostIngestQueue()) {
            result.post_ingest_metrics = postIngest->metricsSnapshot();
        }
        result.db_lock_errors =
            harness.daemon()->getState().stats.dbLockErrors.load(std::memory_order_relaxed);
    }

    result.wal_bytes_after = sidecarBytes(harness.dataDir(), "-wal");
    result.wal_growth_bytes = result.wal_bytes_after > result.wal_bytes_before
                                  ? result.wal_bytes_after - result.wal_bytes_before
                                  : 0;

    stopHarness(true);

    result.peak_rss_after_bytes = peakRssBytes();
    const auto cpuEnd = std::clock();
    if (_cpuStart != static_cast<std::clock_t>(-1) && cpuEnd != static_cast<std::clock_t>(-1) &&
        cpuEnd >= _cpuStart) {
        result.cpu_ms = static_cast<std::uint64_t>(
            (static_cast<double>(cpuEnd - _cpuStart) * 1000.0) / CLOCKS_PER_SEC);
    }
    result.lifecycle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - _lifecycleStart)
                              .count();

    const bool cleanShutdown = result.shutdown_succeeded && result.socket_removed &&
                               result.pid_removed && result.sidecars_removed;
    result.pipeline_complete = result.pipeline_complete && cleanShutdown;

    spdlog::info("=== Benchmark Complete ===");
    spdlog::info("Total duration: {} ms", result.total_duration_ms);
    spdlog::info("Throughput: {:.2f} docs/sec ({:.2f} MiB/sec)", result.throughput_docs_per_sec,
                 result.throughput_mib_per_sec);
    spdlog::info("Dropped batches: {}", result.dropped_batches);

    return result;
}

// ============================================================================
// main - Entry point
// ============================================================================

int main(int /*argc*/, char* /*argv*/[]) noexcept {
    try {
        // Set log level
        const char* logLevel = std::getenv("YAMS_LOG_LEVEL");
        if (logLevel != nullptr && std::string(logLevel) == "debug") {
            spdlog::set_level(spdlog::level::debug);
        } else {
            spdlog::set_level(spdlog::level::info);
        }

        // Read configuration from environment
        const int corpusSize = benchEnvInt("YAMS_BENCH_CORPUS_SIZE", 100, 1, 1000000);
        const int docSize = benchEnvInt("YAMS_BENCH_DOC_SIZE", 1000, 1, 100 * 1024 * 1024);
        const int pollInterval = benchEnvInt("YAMS_BENCH_POLL_INTERVAL_MS", 100, 1, 60000);

        std::string ingestMode = "single_file_serial";
        if (const char* env = std::getenv("YAMS_BENCH_INGEST_MODE")) {
            ingestMode = env;
        }

        const auto ingestConcurrency =
            static_cast<std::size_t>(benchEnvInt("YAMS_BENCH_INGEST_CONCURRENCY", 4, 1, 1024));

        const char* outputPath = std::getenv("YAMS_BENCH_OUTPUT");

        BenchmarkResult result =
            runBenchmark(corpusSize, docSize, pollInterval, ingestMode, ingestConcurrency);
        const std::string jsonStr = result.toJson().dump(2);

        if (outputPath != nullptr) {
            std::ofstream outFile(outputPath);
            outFile << jsonStr << '\n';
            spdlog::info("Results written to: {}", outputPath);
        } else {
            std::cout << jsonStr << '\n';
        }

        return result.metadata_storage.failures > 0 || !result.pipeline_complete ||
                       result.dropped_batches > 0
                   ? 1
                   : 0;
    } catch (const std::exception& error) {
        spdlog::error("Ingestion E2E benchmark failed: {}", error.what());
        return 2;
    } catch (...) {
        spdlog::error("Ingestion E2E benchmark failed with an unknown exception");
        return 2;
    }
}
