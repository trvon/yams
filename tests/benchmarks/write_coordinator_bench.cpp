// WriteCoordinator throughput and versioning benchmark
//
// Profiles the daemon's single-writer coordinator under configurable load,
// measuring batch throughput, queue wait, apply latency, and version-churn
// impact on coordinator stats.
//
// Environment variables:
//   YAMS_BENCH_NUM_FILES          - Number of files per ingest batch (default: 100)
//   YAMS_BENCH_FILE_SIZE_BYTES    - Average file size in bytes (default: 1024)
//   YAMS_BENCH_VERSION_ITERATIONS - Re-ingest iterations for version churn (default: 3)
//   YAMS_BENCH_REPEAT             - Number of full runs (default: 1)
//   YAMS_BENCH_OUTPUT             - JSONL output path (default:
//   bench_results/write_coordinator.jsonl)

#define CATCH_CONFIG_MAIN
#include <catch2/catch_session.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include "../common/test_helpers_catch2.h"
#include "../integration/daemon/test_daemon_harness.h"
#include "bench_utils.h"
#include <yams/app/services/document_ingestion_service.h>
#include <yams/daemon/components/ServiceManager.h>
#include <yams/daemon/components/WriteCoordinator.h>

namespace fs = std::filesystem;
using json = nlohmann::json;
using namespace std::chrono_literals;
using namespace yams::daemon;
using namespace yams::test;

namespace {

struct BenchConfig {
    int numFiles{100};
    int fileSizeBytes{1024};
    int versionIterations{3};
    int repeat{1};
    std::string outputPath{"bench_results/write_coordinator.jsonl"};
};

using yams::bench::isoTimestamp;
using yams::bench::readIntEnv;

BenchConfig loadConfig() {
    BenchConfig cfg;
    cfg.numFiles = readIntEnv("YAMS_BENCH_NUM_FILES", 100, 1, 50000);
    cfg.fileSizeBytes = readIntEnv("YAMS_BENCH_FILE_SIZE_BYTES", 1024, 64, 1024 * 1024);
    cfg.versionIterations = readIntEnv("YAMS_BENCH_VERSION_ITERATIONS", 3, 1, 50);
    cfg.repeat = readIntEnv("YAMS_BENCH_REPEAT", 1, 1, 20);
    if (const char* out = std::getenv("YAMS_BENCH_OUTPUT"))
        cfg.outputPath = out;
    return cfg;
}

void ensureOutputDir(const fs::path& path) {
    if (!path.parent_path().empty())
        fs::create_directories(path.parent_path());
}

std::string randomContent(std::size_t bytes, std::mt19937_64& rng) {
    std::uniform_int_distribution<int> dist('a', 'z');
    std::string content(bytes, '\0');
    for (std::size_t i = 0; i < bytes; ++i)
        content[i] = static_cast<char>(dist(rng));
    return content;
}

struct CoordinatorSnapshot {
    std::uint64_t batchesEnqueued{0};
    std::uint64_t batchesCommitted{0};
    std::uint64_t opsApplied{0};
    std::uint64_t commitErrors{0};
    std::uint64_t documentsInserted{0};
    std::uint64_t metadataEntriesSet{0};
    std::uint64_t relationshipsInserted{0};
    std::uint64_t nodesUpserted{0};
    std::uint64_t edgesAdded{0};
    std::uint64_t maxBatchQueueWaitMs{0};
    std::uint64_t maxBatchApplyMs{0};
    std::vector<WriteCoordinator::Stats::Hotspot> hotSources;

    static CoordinatorSnapshot from(const WriteCoordinator::Stats& s) {
        CoordinatorSnapshot snap;
        snap.batchesEnqueued = s.batchesEnqueued;
        snap.batchesCommitted = s.batchesCommitted;
        snap.opsApplied = s.opsApplied;
        snap.commitErrors = s.commitErrors;
        snap.documentsInserted = s.documentsInserted;
        snap.metadataEntriesSet = s.metadataEntriesSet;
        snap.relationshipsInserted = s.relationshipsInserted;
        snap.nodesUpserted = s.nodesUpserted;
        snap.edgesAdded = s.edgesAdded;
        snap.maxBatchQueueWaitMs = s.maxBatchQueueWaitMs;
        snap.maxBatchApplyMs = s.maxBatchApplyMs;
        snap.hotSources = s.hotSources;
        return snap;
    }

    CoordinatorSnapshot delta(const CoordinatorSnapshot& prev) const {
        CoordinatorSnapshot d;
        d.batchesEnqueued = batchesEnqueued - prev.batchesEnqueued;
        d.batchesCommitted = batchesCommitted - prev.batchesCommitted;
        d.opsApplied = opsApplied - prev.opsApplied;
        d.commitErrors = commitErrors - prev.commitErrors;
        d.documentsInserted = documentsInserted - prev.documentsInserted;
        d.metadataEntriesSet = metadataEntriesSet - prev.metadataEntriesSet;
        d.relationshipsInserted = relationshipsInserted - prev.relationshipsInserted;
        d.nodesUpserted = nodesUpserted - prev.nodesUpserted;
        d.edgesAdded = edgesAdded - prev.edgesAdded;
        d.maxBatchQueueWaitMs = maxBatchQueueWaitMs;
        d.maxBatchApplyMs = maxBatchApplyMs;
        d.hotSources = hotSources;
        return d;
    }

    json toJson() const {
        json j{
            {"batches_enqueued", batchesEnqueued},
            {"batches_committed", batchesCommitted},
            {"ops_applied", opsApplied},
            {"commit_errors", commitErrors},
            {"documents_inserted", documentsInserted},
            {"metadata_entries_set", metadataEntriesSet},
            {"relationships_inserted", relationshipsInserted},
            {"nodes_upserted", nodesUpserted},
            {"edges_added", edgesAdded},
            {"max_batch_queue_wait_ms", maxBatchQueueWaitMs},
            {"max_batch_apply_ms", maxBatchApplyMs},
        };
        if (!hotSources.empty()) {
            json sources = json::array();
            for (const auto& hs : hotSources) {
                sources.push_back({
                    {"source", hs.source},
                    {"batches", hs.batches},
                    {"ops", hs.ops},
                    {"errors", hs.errors},
                    {"total_queue_wait_ms", hs.totalQueueWaitMs},
                    {"max_queue_wait_ms", hs.maxQueueWaitMs},
                    {"total_apply_ms", hs.totalApplyMs},
                    {"max_apply_ms", hs.maxApplyMs},
                });
            }
            j["hot_sources"] = std::move(sources);
        }
        return j;
    }
};

void writeRecord(std::ofstream& out, const json& record) {
    out << record.dump() << '\n';
    out.flush();
}

void sleepMs(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

} // namespace

TEST_CASE("WriteCoordinator throughput and versioning profile", "[bench][write-coordinator]") {
    const auto cfg = loadConfig();

    spdlog::info("=== WriteCoordinator Benchmark ===");
    spdlog::info("  numFiles:          {}", cfg.numFiles);
    spdlog::info("  fileSizeBytes:     {}", cfg.fileSizeBytes);
    spdlog::info("  versionIterations: {}", cfg.versionIterations);
    spdlog::info("  repeat:            {}", cfg.repeat);
    spdlog::info("  output:            {}", cfg.outputPath);

    ensureOutputDir(cfg.outputPath);
    std::ofstream outFile(cfg.outputPath, std::ios::app);
    REQUIRE(outFile.is_open());

    for (int rep = 0; rep < cfg.repeat; ++rep) {
        spdlog::info("--- Repeat {}/{} ---", rep + 1, cfg.repeat);

        DaemonHarness::Options harnessOpts;
        harnessOpts.enableModelProvider = false;
        harnessOpts.useMockModelProvider = false;
        harnessOpts.enableAutoRepair = false;
        harnessOpts.isolateState = true;
        DaemonHarness harness(std::move(harnessOpts));
        REQUIRE(harness.start(30s));

        auto* daemon = harness.daemon();
        REQUIRE(daemon != nullptr);
        auto* sm = daemon->getServiceManager();
        REQUIRE(sm != nullptr);
        auto* wc = sm->getWriteCoordinator();
        REQUIRE(wc != nullptr);

        // Create temp directory with test files
        fs::path workDir = harness.rootDir() / "work";
        fs::create_directories(workDir);

        std::mt19937_64 rng(static_cast<std::uint64_t>(rep) * 12345 + 67890);
        std::vector<fs::path> files;
        files.reserve(static_cast<std::size_t>(cfg.numFiles));

        for (int i = 0; i < cfg.numFiles; ++i) {
            std::ostringstream fname;
            fname << "file_" << std::setfill('0') << std::setw(5) << i << ".txt";
            fs::path fp = workDir / fname.str();
            auto content = randomContent(static_cast<std::size_t>(cfg.fileSizeBytes), rng);
            std::ofstream ofs(fp);
            ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
            ofs.close();
            files.push_back(fp);
        }

        spdlog::info("Created {} files in {}", cfg.numFiles, workDir.string());

        yams::app::services::DocumentIngestionService ingestion;
        auto makeOpts = [&](bool /*isRepeat*/) -> yams::app::services::AddOptions {
            yams::app::services::AddOptions opts;
            opts.socketPath = harness.socketPath();
            opts.explicitDataDir = harness.dataDir();
            opts.path = workDir.string();
            opts.recursive = true;
            opts.noEmbeddings = true;
            opts.timeoutMs = 60000;
            opts.retries = 1;
            return opts;
        };

        // ── Phase 1: Cold ingest (first-time, no versioning) ──
        {
            spdlog::info("Phase 1: cold ingest {} files", cfg.numFiles);
            auto snapBefore = CoordinatorSnapshot::from(wc->getStats());

            auto t0 = std::chrono::steady_clock::now();
            auto addResult = ingestion.addViaDaemon(makeOpts(false));
            auto t1 = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(t1 - t0).count();

            sleepMs(500);
            wc->flush(60s);

            auto snapAfter = CoordinatorSnapshot::from(wc->getStats());

            json record{
                {"timestamp", isoTimestamp()},
                {"repeat", rep},
                {"phase", "cold_ingest"},
                {"elapsed_sec", elapsed},
                {"files_ingested", cfg.numFiles},
                {"file_size_bytes", cfg.fileSizeBytes},
                {"coordinator", snapAfter.delta(snapBefore).toJson()},
            };
            if (addResult) {
                record["documents_added"] = addResult.value().documentsAdded;
                record["documents_skipped"] = addResult.value().documentsSkipped;
            } else {
                record["error"] = addResult.error().message;
            }
            writeRecord(outFile, record);

            auto delta = snapAfter.delta(snapBefore);
            spdlog::info("  elapsed={:.3f}s  docsInserted={}  metadataSet={}  "
                         "relationshipsInserted={}  nodesUpserted={}  edgesAdded={}  "
                         "maxQueueWait={}ms  maxApply={}ms",
                         elapsed, delta.documentsInserted, delta.metadataEntriesSet,
                         delta.relationshipsInserted, delta.nodesUpserted, delta.edgesAdded,
                         delta.maxBatchQueueWaitMs, delta.maxBatchApplyMs);
        }

        // ── Phase 2: Version churn (re-ingest same paths) ──
        {
            spdlog::info("Phase 2: version churn ({} iterations of {} files)",
                         cfg.versionIterations, cfg.numFiles);

            for (int vi = 0; vi < cfg.versionIterations; ++vi) {
                // Write new content to the same files
                for (int i = 0; i < cfg.numFiles; ++i) {
                    auto content = randomContent(static_cast<std::size_t>(cfg.fileSizeBytes), rng);
                    std::ofstream ofs(files[static_cast<std::size_t>(i)]);
                    ofs.write(content.data(), static_cast<std::streamsize>(content.size()));
                    ofs.close();
                }

                auto snapBefore = CoordinatorSnapshot::from(wc->getStats());

                auto t0 = std::chrono::steady_clock::now();
                auto addResult = ingestion.addViaDaemon(makeOpts(true));
                auto t1 = std::chrono::steady_clock::now();
                double elapsed = std::chrono::duration<double>(t1 - t0).count();

                sleepMs(200);
                wc->flush(60s);

                auto snapAfter = CoordinatorSnapshot::from(wc->getStats());
                auto delta = snapAfter.delta(snapBefore);

                json record{
                    {"timestamp", isoTimestamp()},
                    {"repeat", rep},
                    {"phase", "version_churn"},
                    {"version_iter", vi},
                    {"elapsed_sec", elapsed},
                    {"files_ingested", cfg.numFiles},
                    {"file_size_bytes", cfg.fileSizeBytes},
                    {"coordinator", delta.toJson()},
                };
                if (addResult) {
                    record["documents_added"] = addResult.value().documentsAdded;
                    record["documents_skipped"] = addResult.value().documentsSkipped;
                } else {
                    record["error"] = addResult.error().message;
                }
                writeRecord(outFile, record);

                spdlog::info("  iter={} elapsed={:.3f}s  docsInserted={}  metadataSet={}  "
                             "relationshipsInserted={}  nodesUpserted={}  edgesAdded={}",
                             vi, elapsed, delta.documentsInserted, delta.metadataEntriesSet,
                             delta.relationshipsInserted, delta.nodesUpserted, delta.edgesAdded);
            }
        }

        // ── Phase 3: Final coordinator summary ──
        {
            sleepMs(500);
            wc->flush(60s);

            auto finalStats = CoordinatorSnapshot::from(wc->getStats());
            json record{
                {"timestamp", isoTimestamp()},
                {"repeat", rep},
                {"phase", "final_stats"},
                {"coordinator", finalStats.toJson()},
            };
            writeRecord(outFile, record);

            spdlog::info("Phase 3: final coordinator totals");
            spdlog::info("  batchesEnqueued={}  batchesCommitted={}  opsApplied={}  "
                         "commitErrors={}",
                         finalStats.batchesEnqueued, finalStats.batchesCommitted,
                         finalStats.opsApplied, finalStats.commitErrors);
            if (!finalStats.hotSources.empty()) {
                spdlog::info("  top hot sources:");
                for (std::size_t i = 0; i < std::min<std::size_t>(finalStats.hotSources.size(), 5);
                     ++i) {
                    const auto& hs = finalStats.hotSources[i];
                    spdlog::info("    [{}] {} batches={} ops={} totalApply={}ms maxApply={}ms", i,
                                 hs.source, hs.batches, hs.ops, hs.totalApplyMs, hs.maxApplyMs);
                }
            }
        }
    }

    outFile.close();
    spdlog::info("Results written to {}", cfg.outputPath);
}
