// Metadata/SQLite contention benchmark
//
// Profiles concurrent metadata reads/writes against the SQLite metadata DB,
// with optional TRUNCATE checkpoints and long-lived read transactions to force
// realistic lock pressure. This fills the gap between:
//   - write_coordinator_bench (daemon single-writer throughput)
//   - multi_client_ingestion_bench (end-to-end daemon contention)
// by directly stressing the metadata repository + SQLite connection pools.
//
// Environment variables:
//   YAMS_BENCH_SEED_DOCS             - Initial seeded documents (default: 2000)
//   YAMS_BENCH_WRITER_THREADS        - Concurrent metadata writer threads (default: 2)
//   YAMS_BENCH_READER_THREADS        - Concurrent metadata reader threads (default: 8)
//   YAMS_BENCH_LONG_READERS          - Long-lived read transaction threads (default: 1)
//   YAMS_BENCH_LONG_READ_HOLD_MS     - How long each long reader holds tx (default: 100)
//   YAMS_BENCH_RUNTIME_S             - Timed run duration per repeat (default: 8)
//   YAMS_BENCH_REPEAT                - Full repeats (default: 1)
//   YAMS_BENCH_METADATA_BATCH        - setMetadataBatch entries per write op (default: 8)
//   YAMS_BENCH_STATUS_BATCH          - repair-status hashes per write op (default: 64)
//   YAMS_BENCH_READ_BATCH            - getMetadataForDocuments ids per read op (default: 16)
//   YAMS_BENCH_CHECKPOINT_EVERY_MS   - TRUNCATE checkpoint cadence, 0 disables (default: 500)
//   YAMS_BENCH_DB_BUSY_TIMEOUT_MS    - SQLite busy timeout (default: 100)
//   YAMS_BENCH_DUAL_POOL             - 1 => separate read-only pool, 0 => single pool (default: 1)
//   YAMS_BENCH_EXTERNAL_LOCKER       - 1 => separate writer periodically holds BEGIN IMMEDIATE
//                                      long enough to trigger metadata retry exhaustion (default:
//                                      0)
//   YAMS_BENCH_EXTERNAL_LOCK_HOLD_MS - Hold time for injected writer lock (default: 2500)
//   YAMS_BENCH_EXTERNAL_LOCK_EVERY_MS- Period between injected lock attempts (default: 250)
//   YAMS_BENCH_OUTPUT                - JSONL output path (default:
//                                      bench_results/metadata_contention.jsonl)
//   YAMS_BENCH_WRITE_MODE            - mixed|insert_document|metadata_single|metadata_batch|
//                                      batch_content|feedback_event|repair_status|
//                                      extraction_status|download_metadata_burst|
//                                      document_update_burst|post_ingest_batch_content
//                                      (default: mixed)

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include "../common/test_helpers_catch2.h"
#include <yams/app/services/download_metadata_entries.hpp>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/document_metadata.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/storage/sqlite_retry.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace std::chrono_literals;
using yams::metadata::BatchContentEntry;
using yams::metadata::ConnectionPool;
using yams::metadata::ConnectionPoolConfig;
using yams::metadata::ConnectionPriority;
using yams::metadata::Database;
using yams::metadata::DocumentInfo;
using yams::metadata::FeedbackEvent;
using yams::metadata::MetadataOpScope;
using yams::metadata::MetadataRepository;
using yams::metadata::MetadataValue;
using yams::storage::sqlite_retry::isBusyOrLockedMessage;

namespace {

struct BenchConfig {
    int seedDocs{2000};
    int writerThreads{2};
    int readerThreads{8};
    int longReaders{1};
    int longReadHoldMs{100};
    int runtimeSeconds{8};
    int repeat{1};
    int metadataBatchSize{8};
    int statusBatchSize{64};
    int readBatchSize{16};
    int checkpointEveryMs{500};
    int dbBusyTimeoutMs{100};
    bool dualPool{true};
    bool externalLocker{false};
    int externalLockHoldMs{2500};
    int externalLockEveryMs{250};
    std::string outputPath{"bench_results/metadata_contention.jsonl"};
    int writeMode{-1};
    std::string writeModeName{"mixed"};
};

struct OpStats {
    uint64_t attempts{0};
    uint64_t success{0};
    uint64_t errors{0};
    uint64_t lockErrors{0};
    std::vector<int64_t> latenciesUs;
};

struct WorkerStats {
    OpStats writes;
    OpStats insertWrites;
    OpStats metadataSingleWrites;
    OpStats metadataBatchWrites;
    OpStats downloadMetadataWrites;
    OpStats documentUpdateWrites;
    OpStats batchContentWrites;
    OpStats feedbackWrites;
    OpStats repairWrites;
    OpStats extractionWrites;
    OpStats reads;
    OpStats checkpoints;
    OpStats longReads;
    OpStats externalLocks;
};

struct LatencySummary {
    size_t count{0};
    double meanUs{0.0};
    double p50Us{0.0};
    double p95Us{0.0};
    double p99Us{0.0};
    double maxUs{0.0};
};

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

bool readBoolEnv(const char* name, bool fallback) {
    const char* val = std::getenv(name);
    if (!val || !*val) {
        return fallback;
    }
    std::string s(val);
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (s == "1" || s == "true" || s == "yes" || s == "on") {
        return true;
    }
    if (s == "0" || s == "false" || s == "no" || s == "off") {
        return false;
    }
    return fallback;
}

std::pair<int, std::string> readWriteModeEnv() {
    const char* val = std::getenv("YAMS_BENCH_WRITE_MODE");
    if (!val || !*val) {
        return {-1, "mixed"};
    }
    std::string s(val);
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (s == "mixed")
        return {-1, "mixed"};
    if (s == "insert_document")
        return {0, s};
    if (s == "metadata_batch")
        return {1, s};
    if (s == "batch_content")
        return {2, s};
    if (s == "feedback_event")
        return {3, s};
    if (s == "repair_status")
        return {4, s};
    if (s == "extraction_status")
        return {5, s};
    if (s == "metadata_single")
        return {6, s};
    if (s == "download_metadata_burst")
        return {7, s};
    if (s == "document_update_burst")
        return {8, s};
    if (s == "post_ingest_batch_content")
        return {9, s};
    return {-1, "mixed"};
}

BenchConfig loadConfig() {
    BenchConfig cfg;
    cfg.seedDocs = readIntEnv("YAMS_BENCH_SEED_DOCS", 2000, 100, 200000);
    cfg.writerThreads = readIntEnv("YAMS_BENCH_WRITER_THREADS", 2, 1, 64);
    cfg.readerThreads = readIntEnv("YAMS_BENCH_READER_THREADS", 8, 1, 128);
    cfg.longReaders = readIntEnv("YAMS_BENCH_LONG_READERS", 1, 0, 32);
    cfg.longReadHoldMs = readIntEnv("YAMS_BENCH_LONG_READ_HOLD_MS", 100, 0, 5000);
    cfg.runtimeSeconds = readIntEnv("YAMS_BENCH_RUNTIME_S", 8, 1, 300);
    cfg.repeat = readIntEnv("YAMS_BENCH_REPEAT", 1, 1, 20);
    cfg.metadataBatchSize = readIntEnv("YAMS_BENCH_METADATA_BATCH", 8, 1, 128);
    cfg.statusBatchSize = readIntEnv("YAMS_BENCH_STATUS_BATCH", 64, 1, 1024);
    cfg.readBatchSize = readIntEnv("YAMS_BENCH_READ_BATCH", 16, 1, 256);
    cfg.checkpointEveryMs = readIntEnv("YAMS_BENCH_CHECKPOINT_EVERY_MS", 500, 0, 60000);
    cfg.dbBusyTimeoutMs = readIntEnv("YAMS_BENCH_DB_BUSY_TIMEOUT_MS", 100, 0, 30000);
    cfg.dualPool = readBoolEnv("YAMS_BENCH_DUAL_POOL", true);
    cfg.externalLocker = readBoolEnv("YAMS_BENCH_EXTERNAL_LOCKER", false);
    cfg.externalLockHoldMs = readIntEnv("YAMS_BENCH_EXTERNAL_LOCK_HOLD_MS", 2500, 1, 60000);
    cfg.externalLockEveryMs = readIntEnv("YAMS_BENCH_EXTERNAL_LOCK_EVERY_MS", 250, 0, 60000);
    if (const char* out = std::getenv("YAMS_BENCH_OUTPUT"); out && *out) {
        cfg.outputPath = out;
    }
    auto [writeMode, writeModeName] = readWriteModeEnv();
    cfg.writeMode = writeMode;
    cfg.writeModeName = std::move(writeModeName);
    return cfg;
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

void ensureOutputDir(const fs::path& path) {
    if (!path.parent_path().empty()) {
        fs::create_directories(path.parent_path());
    }
}

std::uintmax_t walBytes(const fs::path& dbPath) {
    const fs::path walPath = dbPath.string() + "-wal";
    std::error_code ec;
    if (!fs::exists(walPath, ec)) {
        return 0;
    }
    return fs::file_size(walPath, ec);
}

DocumentInfo makeDocument(const fs::path& root, int writerId, uint64_t seq) {
    DocumentInfo doc;
    doc.fileName = "writer_" + std::to_string(writerId) + "_" + std::to_string(seq) + ".txt";
    doc.filePath = (root / "docs" / doc.fileName).string();
    doc.fileExtension = ".txt";
    doc.fileSize = 1024 + static_cast<int64_t>(seq % 2048);
    doc.sha256Hash = "hash_writer_" + std::to_string(writerId) + "_" + std::to_string(seq);
    doc.mimeType = "text/plain";
    const auto now =
        std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    doc.createdTime = now;
    doc.modifiedTime = now;
    doc.indexedTime = now;
    doc.contentExtracted = true;
    doc.extractionStatus = yams::metadata::ExtractionStatus::Success;
    yams::metadata::populatePathDerivedFields(doc);
    return doc;
}

std::vector<std::pair<std::string, MetadataValue>> makeInsertTags(int writerId, uint64_t seq) {
    std::vector<std::pair<std::string, MetadataValue>> tags;
    tags.emplace_back("owner", MetadataValue("bench_writer_" + std::to_string(writerId)));
    tags.emplace_back("seq", MetadataValue(std::to_string(seq)));
    tags.emplace_back("phase", MetadataValue("insert"));
    return tags;
}

bool isContentionError(std::string_view message) {
    return isBusyOrLockedMessage(message) ||
           message.find("max retries exceeded") != std::string_view::npos;
}

void noteResult(OpStats& stats, bool ok, std::optional<std::string_view> errorMessage,
                int64_t latencyUs) {
    stats.attempts++;
    if (ok) {
        stats.success++;
    } else {
        stats.errors++;
        if (errorMessage && isContentionError(*errorMessage)) {
            stats.lockErrors++;
        }
    }
    stats.latenciesUs.push_back(latencyUs);
}

void mergeOpStats(OpStats& dst, const OpStats& src) {
    dst.attempts += src.attempts;
    dst.success += src.success;
    dst.errors += src.errors;
    dst.lockErrors += src.lockErrors;
    if (!src.latenciesUs.empty()) {
        dst.latenciesUs.insert(dst.latenciesUs.end(), src.latenciesUs.begin(),
                               src.latenciesUs.end());
    }
}

void mergeWorkerStats(WorkerStats& dst, const WorkerStats& src) {
    mergeOpStats(dst.writes, src.writes);
    mergeOpStats(dst.insertWrites, src.insertWrites);
    mergeOpStats(dst.metadataSingleWrites, src.metadataSingleWrites);
    mergeOpStats(dst.metadataBatchWrites, src.metadataBatchWrites);
    mergeOpStats(dst.downloadMetadataWrites, src.downloadMetadataWrites);
    mergeOpStats(dst.documentUpdateWrites, src.documentUpdateWrites);
    mergeOpStats(dst.batchContentWrites, src.batchContentWrites);
    mergeOpStats(dst.feedbackWrites, src.feedbackWrites);
    mergeOpStats(dst.repairWrites, src.repairWrites);
    mergeOpStats(dst.extractionWrites, src.extractionWrites);
    mergeOpStats(dst.reads, src.reads);
    mergeOpStats(dst.checkpoints, src.checkpoints);
    mergeOpStats(dst.longReads, src.longReads);
    mergeOpStats(dst.externalLocks, src.externalLocks);
}

LatencySummary summarizeLatencies(std::vector<int64_t> values) {
    LatencySummary out;
    if (values.empty()) {
        return out;
    }
    std::sort(values.begin(), values.end());
    out.count = values.size();
    const auto sum = std::accumulate(values.begin(), values.end(), 0.0);
    out.meanUs = sum / static_cast<double>(values.size());
    auto pct = [&](double q) {
        const size_t idx = std::min(
            values.size() - 1, static_cast<size_t>(q * static_cast<double>(values.size() - 1)));
        return static_cast<double>(values[idx]);
    };
    out.p50Us = pct(0.50);
    out.p95Us = pct(0.95);
    out.p99Us = pct(0.99);
    out.maxUs = static_cast<double>(values.back());
    return out;
}

json latencyJson(const OpStats& stats) {
    const auto summary = summarizeLatencies(stats.latenciesUs);
    return json{{"attempts", stats.attempts},      {"success", stats.success},
                {"errors", stats.errors},          {"lock_errors", stats.lockErrors},
                {"latency_count", summary.count},  {"latency_mean_us", summary.meanUs},
                {"latency_p50_us", summary.p50Us}, {"latency_p95_us", summary.p95Us},
                {"latency_p99_us", summary.p99Us}, {"latency_max_us", summary.maxUs}};
}

json poolStatsJson(const ConnectionPool::Stats& stats) {
    return json{{"total_connections", stats.totalConnections},
                {"available_connections", stats.availableConnections},
                {"active_connections", stats.activeConnections},
                {"waiting_requests", stats.waitingRequests},
                {"max_observed_waiting", stats.maxObservedWaiting},
                {"total_wait_micros", stats.totalWaitMicros},
                {"timeout_count", stats.timeoutCount},
                {"total_acquired", stats.totalAcquired},
                {"total_released", stats.totalReleased},
                {"failed_acquisitions", stats.failedAcquisitions},
                {"slow_holder_count", stats.slowHolderCount},
                {"max_holder_micros", stats.maxHolderMicros}};
}

void appendJsonLine(const fs::path& outputPath, const json& record) {
    ensureOutputDir(outputPath);
    std::ofstream out(outputPath, std::ios::app);
    out << record.dump() << '\n';
    out.flush();
}

} // namespace

TEST_CASE("MetadataRepository SQLite contention profile", "[bench][metadata][sqlite][contention]") {
    const BenchConfig cfg = loadConfig();

    spdlog::info("=== Metadata/SQLite Contention Benchmark ===");
    spdlog::info("  seedDocs:           {}", cfg.seedDocs);
    spdlog::info("  writerThreads:      {}", cfg.writerThreads);
    spdlog::info("  readerThreads:      {}", cfg.readerThreads);
    spdlog::info("  longReaders:        {}", cfg.longReaders);
    spdlog::info("  longReadHoldMs:     {}", cfg.longReadHoldMs);
    spdlog::info("  runtimeSeconds:     {}", cfg.runtimeSeconds);
    spdlog::info("  repeat:             {}", cfg.repeat);
    spdlog::info("  metadataBatchSize:  {}", cfg.metadataBatchSize);
    spdlog::info("  statusBatchSize:    {}", cfg.statusBatchSize);
    spdlog::info("  readBatchSize:      {}", cfg.readBatchSize);
    spdlog::info("  checkpointEveryMs:  {}", cfg.checkpointEveryMs);
    spdlog::info("  dbBusyTimeoutMs:    {}", cfg.dbBusyTimeoutMs);
    spdlog::info("  dualPool:           {}", cfg.dualPool);
    spdlog::info("  externalLocker:     {}", cfg.externalLocker);
    spdlog::info("  externalLockHoldMs: {}", cfg.externalLockHoldMs);
    spdlog::info("  externalLockEveryMs:{}", cfg.externalLockEveryMs);
    spdlog::info("  writeMode:          {}", cfg.writeModeName);
    spdlog::info("  output:             {}", cfg.outputPath);

    for (int rep = 0; rep < cfg.repeat; ++rep) {
        const fs::path root = yams::test::make_temp_dir("yams_bench_metadata_contention_");
        const fs::path dbPath = root / "metadata.db";

        ConnectionPoolConfig writeCfg;
        writeCfg.minConnections = 1;
        writeCfg.maxConnections = 1;
        writeCfg.busyTimeout = std::chrono::milliseconds(cfg.dbBusyTimeoutMs);
        writeCfg.enableWAL = true;

        ConnectionPool writePool(dbPath.string(), writeCfg);
        REQUIRE(writePool.initialize().has_value());
        writePool.setSlowHolderThreshold(std::chrono::milliseconds(250));

        std::unique_ptr<ConnectionPool> readPool;
        if (cfg.dualPool) {
            ConnectionPoolConfig readCfg;
            readCfg.minConnections = std::min<size_t>(std::max(2, cfg.readerThreads), 8);
            readCfg.maxConnections =
                std::max<size_t>(readCfg.minConnections,
                                 static_cast<size_t>(cfg.readerThreads + cfg.longReaders + 2));
            readCfg.busyTimeout = std::chrono::milliseconds(cfg.dbBusyTimeoutMs);
            readCfg.enableWAL = true;
            readCfg.readOnly = true;
            readPool = std::make_unique<ConnectionPool>(dbPath.string(), readCfg);
            REQUIRE(readPool->initialize().has_value());
        }

        MetadataRepository repo(writePool, readPool ? readPool.get() : nullptr);
        if (readPool) {
            readPool->refreshAll();
        }

        std::unique_ptr<ConnectionPool> externalLockPool;
        if (cfg.externalLocker) {
            ConnectionPoolConfig externalCfg;
            externalCfg.minConnections = 1;
            externalCfg.maxConnections = 1;
            externalCfg.busyTimeout = std::chrono::milliseconds(cfg.dbBusyTimeoutMs);
            externalCfg.enableWAL = true;
            externalLockPool = std::make_unique<ConnectionPool>(dbPath.string(), externalCfg);
            REQUIRE(externalLockPool->initialize().has_value());
            externalLockPool->setSlowHolderThreshold(std::chrono::milliseconds(250));
        }

        std::vector<int64_t> docIds;
        std::vector<std::string> hashes;
        std::vector<std::string> paths;
        docIds.reserve(static_cast<size_t>(cfg.seedDocs));
        hashes.reserve(static_cast<size_t>(cfg.seedDocs));
        paths.reserve(static_cast<size_t>(cfg.seedDocs));

        for (int i = 0; i < cfg.seedDocs; ++i) {
            auto doc = makeDocument(root, -1, static_cast<uint64_t>(i));
            doc.fileName = "seed_" + std::to_string(i) + ".txt";
            doc.filePath = (root / "seed" / doc.fileName).string();
            doc.sha256Hash = "seed_hash_" + std::to_string(i);
            yams::metadata::populatePathDerivedFields(doc);

            std::vector<std::pair<std::string, MetadataValue>> tags;
            tags.emplace_back("bucket", MetadataValue("seed"));
            tags.emplace_back("ordinal", MetadataValue(std::to_string(i)));
            tags.emplace_back("hot_key", MetadataValue("v0"));
            auto inserted = repo.insertDocumentWithMetadata(doc, tags);
            REQUIRE(inserted.has_value());
            docIds.push_back(inserted.value());
            hashes.push_back(doc.sha256Hash);
            paths.push_back(doc.filePath);
        }

        const auto walBefore = walBytes(dbPath);
        std::chrono::steady_clock::time_point start;
        std::chrono::steady_clock::time_point deadline;
        std::atomic<bool> stop{false};
        std::atomic<bool> runStarted{false};
        std::atomic<int> readyWorkers{0};

        WorkerStats aggregate;
        std::mutex aggregateMutex;
        std::vector<std::thread> threads;
        const size_t reservedThreads =
            static_cast<size_t>(cfg.writerThreads) + static_cast<size_t>(cfg.readerThreads) +
            static_cast<size_t>(cfg.longReaders) + (cfg.checkpointEveryMs > 0 ? 1U : 0U) +
            (cfg.externalLocker ? 1U : 0U);
        threads.reserve(reservedThreads);
        const int expectedWorkers = static_cast<int>(reservedThreads);
        auto waitForRunStart = [&]() {
            readyWorkers.fetch_add(1, std::memory_order_release);
            while (!runStarted.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(1ms);
            }
        };

        for (int writerId = 0; writerId < cfg.writerThreads; ++writerId) {
            threads.emplace_back([&, writerId]() {
                WorkerStats local;
                uint64_t seq = 0;
                waitForRunStart();
                while (!stop.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() < deadline) {
                    const auto t0 = std::chrono::steady_clock::now();
                    const auto mode =
                        cfg.writeMode >= 0 ? cfg.writeMode : static_cast<int>(seq % 6);
                    if (mode == 0) {
                        MetadataOpScope op("bench_insert_document_with_metadata");
                        auto doc = makeDocument(root, writerId, seq);
                        auto result =
                            repo.insertDocumentWithMetadata(doc, makeInsertTags(writerId, seq));
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.insertWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 1) {
                        MetadataOpScope op("bench_set_metadata_batch");
                        std::vector<std::tuple<int64_t, std::string, MetadataValue>> entries;
                        entries.reserve(static_cast<size_t>(cfg.metadataBatchSize));
                        for (int j = 0; j < cfg.metadataBatchSize; ++j) {
                            const auto idx =
                                static_cast<size_t>((seq + static_cast<uint64_t>(j) +
                                                     static_cast<uint64_t>(writerId) * 17) %
                                                    docIds.size());
                            entries.emplace_back(docIds[idx], "hot_key_" + std::to_string(j % 4),
                                                 MetadataValue("w" + std::to_string(writerId) +
                                                               "_" + std::to_string(seq) + "_" +
                                                               std::to_string(j)));
                        }
                        auto result = repo.setMetadataBatch(entries);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.metadataBatchWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 2) {
                        MetadataOpScope op("bench_batch_insert_content_and_index");
                        std::vector<BatchContentEntry> entries;
                        entries.reserve(static_cast<size_t>(cfg.metadataBatchSize));
                        for (int j = 0; j < cfg.metadataBatchSize; ++j) {
                            const auto idx =
                                static_cast<size_t>((seq + static_cast<uint64_t>(j) * 13 +
                                                     static_cast<uint64_t>(writerId) * 23) %
                                                    docIds.size());
                            BatchContentEntry entry;
                            entry.documentId = docIds[idx];
                            entry.title = "bench title " + std::to_string(writerId) + "_" +
                                          std::to_string(seq) + "_" + std::to_string(j);
                            entry.contentText = "bench content body " + std::to_string(writerId) +
                                                "_" + std::to_string(seq) + "_" + std::to_string(j);
                            entry.mimeType = "text/plain";
                            entry.extractionMethod = "bench";
                            entry.language = "en";
                            entry.priorStateKnown = true;
                            entry.priorContentExtracted = true;
                            entry.priorExtractionStatus = yams::metadata::ExtractionStatus::Success;
                            entries.push_back(std::move(entry));
                        }
                        auto result = repo.batchInsertContentAndIndex(entries);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.batchContentWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 3) {
                        MetadataOpScope op("bench_insert_feedback_event");
                        FeedbackEvent event;
                        event.eventId =
                            "bench_event_" + std::to_string(writerId) + "_" + std::to_string(seq);
                        event.traceId = "bench_trace_" + std::to_string(writerId % 3);
                        event.createdAt = std::chrono::floor<std::chrono::seconds>(
                            std::chrono::system_clock::now());
                        event.source = "bench";
                        event.eventType = "retrieval_served";
                        event.payloadJson = std::string{"{\"writer\":"} + std::to_string(writerId) +
                                            ",\"seq\":" + std::to_string(seq) + "}";
                        auto result = repo.insertFeedbackEvent(event);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.feedbackWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 4) {
                        MetadataOpScope op("bench_batch_update_repair_statuses");
                        std::vector<std::string> batchHashes;
                        batchHashes.reserve(static_cast<size_t>(cfg.statusBatchSize));
                        for (int j = 0; j < cfg.statusBatchSize; ++j) {
                            const auto idx =
                                static_cast<size_t>((seq + static_cast<uint64_t>(j) * 11 +
                                                     static_cast<uint64_t>(writerId) * 29) %
                                                    hashes.size());
                            batchHashes.push_back(hashes[idx]);
                        }
                        auto result = repo.batchUpdateDocumentRepairStatuses(
                            batchHashes, yams::metadata::RepairStatus::Processing);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.repairWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 6) {
                        MetadataOpScope op("bench_set_metadata_single");
                        const auto idx = static_cast<size_t>(
                            (seq + static_cast<uint64_t>(writerId) * 37) % docIds.size());
                        auto result =
                            repo.setMetadata(docIds[idx], "hot_single_" + std::to_string(seq % 4),
                                             MetadataValue("single_" + std::to_string(writerId) +
                                                           "_" + std::to_string(seq)));
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.metadataSingleWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 7) {
                        MetadataOpScope op("bench_download_metadata_burst");
                        const auto idx = static_cast<size_t>(
                            (seq + static_cast<uint64_t>(writerId) * 41) % docIds.size());

                        yams::app::services::DownloadServiceRequest request;
                        request.tags = {"bench-user-tag", "bench-user-tag-2"};
                        request.metadata = {{"custom_key", "bench_value_" + std::to_string(seq)}};

                        yams::downloader::FinalResult downloadResult;
                        downloadResult.url = "https://bench.example.test/files/" +
                                             std::to_string(writerId) + "/" + std::to_string(seq) +
                                             ".txt";
                        downloadResult.httpStatus = 200;
                        downloadResult.etag =
                            "etag_" + std::to_string(writerId) + "_" + std::to_string(seq);
                        downloadResult.lastModified = "Mon, 01 Jan 2024 00:00:00 GMT";
                        downloadResult.checksumOk = true;
                        downloadResult.contentType = "text/plain";
                        downloadResult.suggestedName = "bench-file.txt";

                        auto result = repo.setMetadataBatch(
                            buildDownloadMetadataEntries(docIds[idx], request, downloadResult));
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.downloadMetadataWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 8) {
                        MetadataOpScope op("bench_document_update_burst");
                        const auto idx = static_cast<size_t>(
                            (seq + static_cast<uint64_t>(writerId) * 43) % docIds.size());
                        std::vector<std::tuple<int64_t, std::string, MetadataValue>> entries;
                        entries.reserve(5);
                        entries.emplace_back(docIds[idx], "author",
                                             MetadataValue("author_" + std::to_string(writerId)));
                        entries.emplace_back(docIds[idx], "topic",
                                             MetadataValue("topic_" + std::to_string(seq)));
                        entries.emplace_back(docIds[idx], "topic",
                                             MetadataValue("topic_final_" + std::to_string(seq)));
                        entries.emplace_back(docIds[idx], "tag:bench-tag-a",
                                             MetadataValue("bench-tag-a"));
                        entries.emplace_back(docIds[idx], "tag:bench-tag-b",
                                             MetadataValue("bench-tag-b"));
                        auto result = repo.setMetadataBatch(entries);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.documentUpdateWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 9) {
                        MetadataOpScope op("daemon_post_ingest_batch_content");
                        std::vector<BatchContentEntry> entries;
                        entries.reserve(static_cast<size_t>(cfg.metadataBatchSize));
                        for (int j = 0; j < cfg.metadataBatchSize; ++j) {
                            const auto idx =
                                static_cast<size_t>((seq + static_cast<uint64_t>(j) * 13 +
                                                     static_cast<uint64_t>(writerId) * 23) %
                                                    docIds.size());
                            BatchContentEntry entry;
                            entry.documentId = docIds[idx];
                            entry.title = "bench title " + std::to_string(writerId) + "_" +
                                          std::to_string(seq) + "_" + std::to_string(j);
                            entry.contentText = "bench content body " + std::to_string(writerId) +
                                                "_" + std::to_string(seq) + "_" + std::to_string(j);
                            entry.mimeType = "text/plain";
                            entry.extractionMethod = "bench";
                            entry.language = "en";
                            entry.priorStateKnown = true;
                            entry.priorContentExtracted = true;
                            entry.priorExtractionStatus = yams::metadata::ExtractionStatus::Success;
                            entries.push_back(std::move(entry));
                        }
                        auto result = repo.batchInsertContentAndIndex(entries);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.batchContentWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else {
                        MetadataOpScope op("bench_update_extraction_status");
                        const auto idx = static_cast<size_t>(
                            (seq + static_cast<uint64_t>(writerId) * 31) % docIds.size());
                        const bool extracted = (seq % 8) != 3;
                        const auto status = extracted ? yams::metadata::ExtractionStatus::Success
                                                      : yams::metadata::ExtractionStatus::Failed;
                        const std::string error = extracted ? std::string{} : "bench_lock_probe";
                        auto result = repo.updateDocumentExtractionStatus(docIds[idx], extracted,
                                                                          status, error);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.writes, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                        noteResult(local.extractionWrites, result.has_value(),
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    }
                    ++seq;
                }
                std::lock_guard<std::mutex> lock(aggregateMutex);
                mergeWorkerStats(aggregate, local);
            });
        }

        for (int readerId = 0; readerId < cfg.readerThreads; ++readerId) {
            threads.emplace_back([&, readerId]() {
                WorkerStats local;
                uint64_t seq = static_cast<uint64_t>(readerId) * 101;
                waitForRunStart();
                while (!stop.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() < deadline) {
                    const auto t0 = std::chrono::steady_clock::now();
                    const auto mode = seq % 3;
                    if (mode == 0) {
                        MetadataOpScope op("bench_find_document_by_path");
                        auto result = repo.findDocumentByExactPath(paths[seq % paths.size()]);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        const bool ok = result.has_value() && result.value().has_value();
                        noteResult(local.reads, ok,
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else if (mode == 1) {
                        MetadataOpScope op("bench_get_document_by_hash");
                        auto result = repo.getDocumentByHash(hashes[seq % hashes.size()]);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        const bool ok = result.has_value() && result.value().has_value();
                        noteResult(local.reads, ok,
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    } else {
                        MetadataOpScope op("bench_get_metadata_batch");
                        std::vector<int64_t> batch;
                        batch.reserve(static_cast<size_t>(cfg.readBatchSize));
                        for (int j = 0; j < cfg.readBatchSize; ++j) {
                            const auto idx = static_cast<size_t>(
                                (seq + static_cast<uint64_t>(j) * 7) % docIds.size());
                            batch.push_back(docIds[idx]);
                        }
                        auto result = repo.getMetadataForDocuments(batch);
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        const bool ok = result.has_value();
                        noteResult(local.reads, ok,
                                   result ? std::nullopt
                                          : std::optional<std::string_view>(result.error().message),
                                   us);
                    }
                    ++seq;
                }
                std::lock_guard<std::mutex> lock(aggregateMutex);
                mergeWorkerStats(aggregate, local);
            });
        }

        ConnectionPool* longReadPool = readPool ? readPool.get() : &writePool;
        for (int longReaderId = 0; longReaderId < cfg.longReaders; ++longReaderId) {
            threads.emplace_back([&, longReaderId]() {
                WorkerStats local;
                waitForRunStart();
                while (!stop.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() < deadline) {
                    const auto t0 = std::chrono::steady_clock::now();
                    MetadataOpScope op("bench_long_read_tx");
                    auto connResult =
                        longReadPool->acquire(5s, ConnectionPriority::High, "bench_long_read_tx");
                    if (!connResult) {
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.longReads, false,
                                   std::optional<std::string_view>(connResult.error().message), us);
                        continue;
                    }

                    std::unique_ptr<yams::metadata::PooledConnection> conn =
                        std::move(connResult).value();
                    Database& db = **conn;
                    auto begin = db.execute("BEGIN");
                    if (!begin) {
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.longReads, false,
                                   std::optional<std::string_view>(begin.error().message), us);
                        continue;
                    }

                    bool ok = true;
                    std::optional<std::string_view> errorMessage;
                    auto stmtResult =
                        db.prepare("SELECT d.id, d.file_path, m.key, m.value FROM documents d "
                                   "LEFT JOIN metadata m ON m.document_id = d.id "
                                   "ORDER BY d.id LIMIT 64");
                    if (!stmtResult) {
                        ok = false;
                        errorMessage = stmtResult.error().message;
                    } else {
                        yams::metadata::Statement stmt = std::move(stmtResult).value();
                        while (true) {
                            auto step = stmt.step();
                            if (!step) {
                                ok = false;
                                errorMessage = step.error().message;
                                break;
                            }
                            if (!step.value()) {
                                break;
                            }
                            const auto rowId = stmt.getInt64(0);
                            (void)rowId;
                        }
                    }

                    if (cfg.longReadHoldMs > 0) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.longReadHoldMs));
                    }

                    auto commit = db.execute("COMMIT");
                    if (!commit && ok) {
                        ok = false;
                        errorMessage = commit.error().message;
                    }
                    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now() - t0)
                                        .count();
                    noteResult(local.longReads, ok, errorMessage, us);

                    if (!ok) {
                        db.execute("ROLLBACK");
                    }
                    (void)longReaderId;
                }
                std::lock_guard<std::mutex> lock(aggregateMutex);
                mergeWorkerStats(aggregate, local);
            });
        }

        if (cfg.externalLocker) {
            threads.emplace_back([&]() {
                WorkerStats local;
                waitForRunStart();
                auto nextLockAttempt = start + std::chrono::milliseconds(cfg.externalLockEveryMs);
                while (!stop.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() < deadline) {
                    if (cfg.externalLockEveryMs > 0) {
                        std::this_thread::sleep_until(nextLockAttempt);
                        nextLockAttempt += std::chrono::milliseconds(cfg.externalLockEveryMs);
                    }
                    if (stop.load(std::memory_order_relaxed) ||
                        std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }

                    const auto t0 = std::chrono::steady_clock::now();
                    auto connResult = externalLockPool->acquire(5s, ConnectionPriority::Normal,
                                                                "bench_external_locker");
                    if (!connResult) {
                        const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                            std::chrono::steady_clock::now() - t0)
                                            .count();
                        noteResult(local.externalLocks, false,
                                   std::optional<std::string_view>(connResult.error().message), us);
                        continue;
                    }

                    std::unique_ptr<yams::metadata::PooledConnection> conn =
                        std::move(connResult).value();
                    Database& db = **conn;
                    bool ok = true;
                    bool transactionStarted = false;
                    std::optional<std::string_view> errorMessage;
                    auto begin = db.execute("BEGIN IMMEDIATE");
                    if (!begin) {
                        ok = false;
                        errorMessage = begin.error().message;
                    } else {
                        transactionStarted = true;
                    }

                    if (ok) {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(cfg.externalLockHoldMs));
                        auto commit = db.execute("COMMIT");
                        if (!commit) {
                            ok = false;
                            errorMessage = commit.error().message;
                        } else {
                            transactionStarted = false;
                        }
                    }

                    if (!ok && transactionStarted) {
                        db.execute("ROLLBACK");
                    }

                    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now() - t0)
                                        .count();
                    noteResult(local.externalLocks, ok, errorMessage, us);
                }
                std::lock_guard<std::mutex> lock(aggregateMutex);
                mergeWorkerStats(aggregate, local);
            });
        }

        if (cfg.checkpointEveryMs > 0) {
            threads.emplace_back([&]() {
                WorkerStats local;
                waitForRunStart();
                auto nextCheckpoint = start + std::chrono::milliseconds(cfg.checkpointEveryMs);
                while (!stop.load(std::memory_order_relaxed) &&
                       std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::sleep_until(nextCheckpoint);
                    nextCheckpoint += std::chrono::milliseconds(cfg.checkpointEveryMs);
                    if (stop.load(std::memory_order_relaxed) ||
                        std::chrono::steady_clock::now() >= deadline) {
                        break;
                    }
                    const auto t0 = std::chrono::steady_clock::now();
                    MetadataOpScope op("bench_checkpoint_truncate");
                    auto result = repo.checkpointWalTruncate();
                    const auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                                        std::chrono::steady_clock::now() - t0)
                                        .count();
                    noteResult(local.checkpoints, result.has_value(),
                               result ? std::nullopt
                                      : std::optional<std::string_view>(result.error().message),
                               us);
                }
                std::lock_guard<std::mutex> lock(aggregateMutex);
                mergeWorkerStats(aggregate, local);
            });
        }

        while (readyWorkers.load(std::memory_order_acquire) < expectedWorkers) {
            std::this_thread::sleep_for(1ms);
        }
        start = std::chrono::steady_clock::now();
        deadline = start + std::chrono::seconds(cfg.runtimeSeconds);
        runStarted.store(true, std::memory_order_release);

        for (auto& thread : threads) {
            thread.join();
        }
        stop.store(true, std::memory_order_relaxed);

        const auto totalWallSeconds =
            std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        const auto activeElapsedSeconds = std::chrono::duration<double>(deadline - start).count();
        const auto cleanupTailSeconds = std::max(0.0, totalWallSeconds - activeElapsedSeconds);
        const auto walAfter = walBytes(dbPath);
        const auto writePoolStats = writePool.getStats();
        const auto readPoolStats =
            readPool ? std::optional<ConnectionPool::Stats>(readPool->getStats()) : std::nullopt;

        INFO("write p95(us): " << latencyJson(aggregate.writes)["latency_p95_us"]);
        INFO("read p95(us): " << latencyJson(aggregate.reads)["latency_p95_us"]);
        INFO("metadata-single lock errors: " << aggregate.metadataSingleWrites.lockErrors);
        INFO("metadata-batch lock errors: " << aggregate.metadataBatchWrites.lockErrors);
        INFO("download-metadata lock errors: " << aggregate.downloadMetadataWrites.lockErrors);
        INFO("document-update lock errors: " << aggregate.documentUpdateWrites.lockErrors);
        INFO("content-index lock errors: " << aggregate.batchContentWrites.lockErrors);
        INFO("feedback lock errors: " << aggregate.feedbackWrites.lockErrors);
        INFO("repair lock errors: " << aggregate.repairWrites.lockErrors);
        INFO("extraction lock errors: " << aggregate.extractionWrites.lockErrors);
        CHECK((aggregate.writes.success > 0));
        CHECK((aggregate.reads.success > 0));
        if (cfg.externalLocker) {
            CHECK((aggregate.externalLocks.success > 0));
            CHECK(
                (aggregate.writes.lockErrors > 0 || aggregate.metadataSingleWrites.lockErrors > 0 ||
                 aggregate.metadataBatchWrites.lockErrors > 0 ||
                 aggregate.downloadMetadataWrites.lockErrors > 0 ||
                 aggregate.documentUpdateWrites.lockErrors > 0 ||
                 aggregate.batchContentWrites.lockErrors > 0 ||
                 aggregate.feedbackWrites.lockErrors > 0 || aggregate.repairWrites.lockErrors > 0 ||
                 aggregate.extractionWrites.lockErrors > 0));
        }

        json record{{"timestamp", isoTimestamp()},
                    {"repeat", rep + 1},
                    {"duration_seconds", activeElapsedSeconds},
                    {"total_wall_seconds", totalWallSeconds},
                    {"cleanup_tail_seconds", cleanupTailSeconds},
                    {"db_path", dbPath.string()},
                    {"dual_pool", cfg.dualPool},
                    {"seed_docs", cfg.seedDocs},
                    {"writer_threads", cfg.writerThreads},
                    {"reader_threads", cfg.readerThreads},
                    {"long_readers", cfg.longReaders},
                    {"long_read_hold_ms", cfg.longReadHoldMs},
                    {"metadata_batch_size", cfg.metadataBatchSize},
                    {"status_batch_size", cfg.statusBatchSize},
                    {"read_batch_size", cfg.readBatchSize},
                    {"checkpoint_every_ms", cfg.checkpointEveryMs},
                    {"db_busy_timeout_ms", cfg.dbBusyTimeoutMs},
                    {"external_locker", cfg.externalLocker},
                    {"external_lock_hold_ms", cfg.externalLockHoldMs},
                    {"external_lock_every_ms", cfg.externalLockEveryMs},
                    {"worker_start_barrier", true},
                    {"write_mode", cfg.writeModeName},
                    {"wal_bytes_before", walBefore},
                    {"wal_bytes_after", walAfter},
                    {"writes", latencyJson(aggregate.writes)},
                    {"insert_document_writes", latencyJson(aggregate.insertWrites)},
                    {"metadata_single_writes", latencyJson(aggregate.metadataSingleWrites)},
                    {"metadata_batch_writes", latencyJson(aggregate.metadataBatchWrites)},
                    {"download_metadata_writes", latencyJson(aggregate.downloadMetadataWrites)},
                    {"document_update_writes", latencyJson(aggregate.documentUpdateWrites)},
                    {"batch_content_writes", latencyJson(aggregate.batchContentWrites)},
                    {"feedback_event_writes", latencyJson(aggregate.feedbackWrites)},
                    {"repair_status_writes", latencyJson(aggregate.repairWrites)},
                    {"extraction_status_writes", latencyJson(aggregate.extractionWrites)},
                    {"reads", latencyJson(aggregate.reads)},
                    {"checkpoints", latencyJson(aggregate.checkpoints)},
                    {"long_reads", latencyJson(aggregate.longReads)},
                    {"external_locks", latencyJson(aggregate.externalLocks)},
                    {"write_pool", poolStatsJson(writePoolStats)}};

        if (readPoolStats.has_value()) {
            record["read_pool"] = poolStatsJson(*readPoolStats);
        }

        appendJsonLine(cfg.outputPath, record);

        std::cout << "[metadata-contention] repeat=" << (rep + 1) << " writes/s="
                  << (activeElapsedSeconds > 0.0
                          ? static_cast<double>(aggregate.writes.success) / activeElapsedSeconds
                          : 0.0)
                  << " reads/s="
                  << (activeElapsedSeconds > 0.0
                          ? static_cast<double>(aggregate.reads.success) / activeElapsedSeconds
                          : 0.0)
                  << " write_p95_us="
                  << latencyJson(aggregate.writes)["latency_p95_us"].get<double>()
                  << " insert_lock_errors=" << aggregate.insertWrites.lockErrors
                  << " metadata_single_lock_errors=" << aggregate.metadataSingleWrites.lockErrors
                  << " metadata_lock_errors=" << aggregate.metadataBatchWrites.lockErrors
                  << " download_metadata_lock_errors="
                  << aggregate.downloadMetadataWrites.lockErrors
                  << " document_update_lock_errors=" << aggregate.documentUpdateWrites.lockErrors
                  << " batch_content_lock_errors=" << aggregate.batchContentWrites.lockErrors
                  << " feedback_lock_errors=" << aggregate.feedbackWrites.lockErrors
                  << " repair_lock_errors=" << aggregate.repairWrites.lockErrors
                  << " extraction_lock_errors=" << aggregate.extractionWrites.lockErrors
                  << " read_p95_us=" << latencyJson(aggregate.reads)["latency_p95_us"].get<double>()
                  << " checkpoint_p95_us="
                  << latencyJson(aggregate.checkpoints)["latency_p95_us"].get<double>()
                  << " cleanup_tail_s=" << cleanupTailSeconds << std::endl;

        if (externalLockPool) {
            externalLockPool->shutdown();
        }
        if (readPool) {
            readPool->shutdown();
        }
        writePool.shutdown();
        std::error_code ec;
        fs::remove_all(root, ec);
    }
}
