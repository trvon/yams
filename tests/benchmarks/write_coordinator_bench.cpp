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
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/profiling.h>

#include <boost/asio/io_context.hpp>

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
    std::uint64_t repairStatusesUpdated{0};
    std::uint64_t metadataEntriesSet{0};
    std::uint64_t extractionStatusesUpdated{0};
    std::uint64_t embeddingStatusesUpdated{0};
    std::uint64_t symbolExtractionStatesUpdated{0};
    std::uint64_t relationshipsInserted{0};
    std::uint64_t symSpellTermsAdded{0};
    std::uint64_t nodesUpserted{0};
    std::uint64_t edgesAdded{0};
    std::uint64_t maxBatchQueueWaitMs{0};
    std::uint64_t maxBatchExcessQueueWaitMs{0};
    std::uint64_t maxBatchApplyMs{0};
    std::vector<WriteCoordinator::Stats::Hotspot> hotSources;

    static CoordinatorSnapshot from(const WriteCoordinator::Stats& s) {
        CoordinatorSnapshot snap;
        snap.batchesEnqueued = s.batchesEnqueued;
        snap.batchesCommitted = s.batchesCommitted;
        snap.opsApplied = s.opsApplied;
        snap.commitErrors = s.commitErrors;
        snap.documentsInserted = s.documentsInserted;
        snap.repairStatusesUpdated = s.repairStatusesUpdated;
        snap.metadataEntriesSet = s.metadataEntriesSet;
        snap.extractionStatusesUpdated = s.extractionStatusesUpdated;
        snap.embeddingStatusesUpdated = s.embeddingStatusesUpdated;
        snap.symbolExtractionStatesUpdated = s.symbolExtractionStatesUpdated;
        snap.relationshipsInserted = s.relationshipsInserted;
        snap.symSpellTermsAdded = s.symSpellTermsAdded;
        snap.nodesUpserted = s.nodesUpserted;
        snap.edgesAdded = s.edgesAdded;
        snap.maxBatchQueueWaitMs = s.maxBatchQueueWaitMs;
        snap.maxBatchExcessQueueWaitMs = s.maxBatchExcessQueueWaitMs;
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
        d.repairStatusesUpdated = repairStatusesUpdated - prev.repairStatusesUpdated;
        d.metadataEntriesSet = metadataEntriesSet - prev.metadataEntriesSet;
        d.extractionStatusesUpdated = extractionStatusesUpdated - prev.extractionStatusesUpdated;
        d.embeddingStatusesUpdated = embeddingStatusesUpdated - prev.embeddingStatusesUpdated;
        d.symbolExtractionStatesUpdated =
            symbolExtractionStatesUpdated - prev.symbolExtractionStatesUpdated;
        d.relationshipsInserted = relationshipsInserted - prev.relationshipsInserted;
        d.symSpellTermsAdded = symSpellTermsAdded - prev.symSpellTermsAdded;
        d.nodesUpserted = nodesUpserted - prev.nodesUpserted;
        d.edgesAdded = edgesAdded - prev.edgesAdded;
        d.maxBatchQueueWaitMs = maxBatchQueueWaitMs;
        d.maxBatchExcessQueueWaitMs = maxBatchExcessQueueWaitMs;
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
            {"repair_statuses_updated", repairStatusesUpdated},
            {"metadata_entries_set", metadataEntriesSet},
            {"extraction_statuses_updated", extractionStatusesUpdated},
            {"embedding_statuses_updated", embeddingStatusesUpdated},
            {"symbol_extraction_states_updated", symbolExtractionStatesUpdated},
            {"relationships_inserted", relationshipsInserted},
            {"symspell_terms_added", symSpellTermsAdded},
            {"nodes_upserted", nodesUpserted},
            {"edges_added", edgesAdded},
            {"max_batch_queue_wait_ms", maxBatchQueueWaitMs},
            {"max_batch_excess_queue_wait_ms", maxBatchExcessQueueWaitMs},
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
                    {"max_excess_queue_wait_ms", hs.maxExcessQueueWaitMs},
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
    YAMS_ZONE_SCOPED_N("Bench::WriteCoordinator");
    YAMS_SET_THREAD_NAME("bench-write-coordinator");
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
        YAMS_ZONE_SCOPED_N("Bench::WriteCoordinator::repeat");
        YAMS_FRAME_MARK_NAMED("write_coordinator_repeat");
        YAMS_PLOT("bench.write_coordinator.repeat", static_cast<int64_t>(rep));
        spdlog::info("--- Repeat {}/{} ---", rep + 1, cfg.repeat);

        DaemonHarness::Options harnessOpts;
        harnessOpts.enableModelProvider = false;
        harnessOpts.useMockModelProvider = false;
        harnessOpts.enableAutoRepair = false;
        harnessOpts.isolateState = true;
        DaemonHarness harness(std::move(harnessOpts));
        REQUIRE(harness.start(30s));

        auto* daemon = harness.daemon();
        REQUIRE((daemon != nullptr));
        auto* sm = daemon->getServiceManager();
        REQUIRE((sm != nullptr));
        auto* wc = sm->getWriteCoordinator();
        REQUIRE((wc != nullptr));

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
            YAMS_ZONE_SCOPED_N("Bench::WriteCoordinator::cold_ingest");
            YAMS_PLOT("bench.write_coordinator.files", static_cast<int64_t>(cfg.numFiles));
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
            YAMS_ZONE_SCOPED_N("Bench::WriteCoordinator::version_churn");
            spdlog::info("Phase 2: version churn ({} iterations of {} files)",
                         cfg.versionIterations, cfg.numFiles);

            for (int vi = 0; vi < cfg.versionIterations; ++vi) {
                YAMS_ZONE_SCOPED_N("Bench::WriteCoordinator::version_iteration");
                YAMS_PLOT("bench.write_coordinator.version_iter", static_cast<int64_t>(vi));
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
            YAMS_ZONE_SCOPED_N("Bench::WriteCoordinator::final_stats");
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

TEST_CASE("WriteCoordinator KG dedup apply benchmark", "[bench][write-coordinator][kg-dedup]") {
    YAMS_ZONE_SCOPED_N("Bench::WriteCoordinatorKgDedup");
    const int batches = readIntEnv("YAMS_BENCH_KG_BATCHES", 200, 1, 20000);
    const int edgesPerBatch = readIntEnv("YAMS_BENCH_KG_EDGES_PER_BATCH", 30, 1, 1000);
    const int dupPct = readIntEnv("YAMS_BENCH_KG_DUP_PCT", 60, 0, 100);
    const int nodePool = readIntEnv("YAMS_BENCH_KG_NODE_POOL", 2000, 10, 100000);
    const int repeat = readIntEnv("YAMS_BENCH_REPEAT", 3, 1, 20);
    const int hotPool = std::max(2, nodePool / 20);

    std::string outputPath = "bench_results/write_coordinator_kg_dedup.jsonl";
    if (const char* out = std::getenv("YAMS_BENCH_OUTPUT"))
        outputPath = out;
    ensureOutputDir(outputPath);
    std::ofstream outFile(outputPath, std::ios::app);
    REQUIRE(outFile.is_open());

    auto runScenario = [&](bool dedup, int rep) -> json {
        fs::path dir = fs::temp_directory_path() /
                       ("wc_kg_dedup_bench_" +
                        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(dir);
        auto dbPath = (dir / "kg.db").string();

        yams::metadata::KnowledgeGraphStoreConfig kgCfg;
        kgCfg.enable_alias_fts = false;
        auto kgRes = yams::metadata::makeSqliteKnowledgeGraphStore(dbPath, kgCfg);
        REQUIRE(kgRes);
        std::shared_ptr<yams::metadata::KnowledgeGraphStore> kg = std::move(kgRes.value());

        std::vector<yams::metadata::KGNode> nodes;
        nodes.reserve(static_cast<std::size_t>(nodePool));
        for (int i = 0; i < nodePool; ++i) {
            yams::metadata::KGNode node;
            node.nodeKey = "bench:node:" + std::to_string(i);
            node.type = "entity";
            nodes.push_back(std::move(node));
        }
        REQUIRE(kg->upsertNodes(nodes));

        boost::asio::io_context io;
        WriteCoordinator::Config config;
        config.maxBatchSize = 50;
        config.maxBatchDelayMs = std::chrono::milliseconds(1);
        config.channelCapacity = static_cast<std::size_t>(batches) + 8;
        config.kgDedupEnabled = dedup;
        WriteCoordinator coordinator(io, kg, {}, config);
        coordinator.start();
        std::thread writerLoop([&io] { io.run(); });

        std::mt19937_64 rng(0xBE7CDEDBull + static_cast<std::uint64_t>(rep));
        std::uniform_int_distribution<int> pctDist(0, 99);
        std::uniform_int_distribution<int> hotDist(0, hotPool - 1);
        std::uniform_int_distribution<int> coldDist(0, nodePool - 1);
        std::uniform_real_distribution<float> weightDist(0.1f, 2.0f);

        const auto pickKey = [&]() {
            const int idx = pctDist(rng) < dupPct ? hotDist(rng) : coldDist(rng);
            return "bench:node:" + std::to_string(idx);
        };

        const auto start = std::chrono::steady_clock::now();
        for (int b = 0; b < batches; ++b) {
            auto batch = std::make_unique<WriteBatch>();
            batch->source = "bench/kg_dedup/" + std::to_string(b);
            AddDeferredEdgesOp op;
            op.edges.reserve(static_cast<std::size_t>(edgesPerBatch));
            for (int e = 0; e < edgesPerBatch; ++e) {
                DeferredEdgeOp edge;
                edge.srcNodeKey = pickKey();
                edge.dstNodeKey = pickKey();
                edge.relation = "bench_rel";
                edge.weight = weightDist(rng);
                op.edges.push_back(std::move(edge));
            }
            batch->ops.emplace_back(std::move(op));
            coordinator.enqueue(std::move(batch));
        }
        auto flushResult = coordinator.flush(std::chrono::minutes(5));
        const auto wallMs = std::chrono::duration<double, std::milli>(
                                std::chrono::steady_clock::now() - start)
                                .count();
        REQUIRE(flushResult.has_value());

        const auto stats = coordinator.getStats();
        coordinator.shutdown();
        io.stop();
        if (writerLoop.joinable())
            writerLoop.join();
        kg.reset();
        std::error_code ec;
        fs::remove_all(dir, ec);

        const double offered = static_cast<double>(batches) * edgesPerBatch;
        json j{{"benchmark", "kg_dedup_apply"},
               {"timestamp", isoTimestamp()},
               {"rep", rep},
               {"dedup", dedup},
               {"batches", batches},
               {"edges_per_batch", edgesPerBatch},
               {"dup_pct", dupPct},
               {"node_pool", nodePool},
               {"wall_ms", wallMs},
               {"edges_offered_per_sec", offered / (wallMs / 1000.0)},
               {"edges_added", stats.edgesAdded},
               {"edges_coalesced", stats.edgesCoalesced},
               {"node_key_lookups_batched", stats.nodeKeyLookupsBatched},
               {"max_batch_apply_ms", stats.maxBatchApplyMs}};
        return j;
    };

    for (int rep = 0; rep < repeat; ++rep) {
        for (bool dedup : {false, true}) {
            auto record = runScenario(dedup, rep);
            writeRecord(outFile, record);
            spdlog::info("[kg_dedup_apply] rep={} dedup={} wall_ms={:.1f} offered_eps={:.0f} "
                         "added={} coalesced={} key_lookups={} max_apply_ms={}",
                         rep, record["dedup"].get<bool>(), record["wall_ms"].get<double>(),
                         record["edges_offered_per_sec"].get<double>(),
                         record["edges_added"].get<std::uint64_t>(),
                         record["edges_coalesced"].get<std::uint64_t>(),
                         record["node_key_lookups_batched"].get<std::uint64_t>(),
                         record["max_batch_apply_ms"].get<std::uint64_t>());
        }
    }
}
