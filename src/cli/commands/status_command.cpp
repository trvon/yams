#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <future>
#include <iomanip>
#include <iostream>
#include <vector>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/daemon/client/global_io_context.h>

#ifdef __unix__
#include <sys/stat.h>
#endif
#include <atomic>
#include <chrono>
#include <future>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_migration.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/profiling.h>
#include <yams/vector/embedding_service.h>
#include <yams/vector/vector_database.h>

namespace yams::cli {

namespace fs = std::filesystem;

class StatusCommand : public ICommand {
public:
    std::string getName() const override { return "status"; }

    std::string getDescription() const override {
        return "Show quick system status and health overview";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("status", getDescription());

        cmd->add_flag("--json", jsonOutput_, "Output in JSON format");
        cmd->add_flag("-v,--verbose", verbose_, "Show detailed information");
        cmd->add_flag("--no-physical", noPhysical_,
                      "Skip physical size fallback scan; use daemon stats only");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Status command failed: {}", result.error().message);
                std::exit(1);
            }
        });

        // Provide a 'stats' alias that maps to the same implementation
        // Mirrors flags and behavior to ease migration from the old stats command
        auto* stats = app.add_subcommand("stats", getDescription());
        stats->add_flag("--json", jsonOutput_, "Output in JSON format");
        stats->add_flag("-v,--verbose", verbose_, "Show detailed information");
        stats->add_flag("--no-physical", noPhysical_,
                        "Skip physical size fallback scan; use daemon stats only");
        stats->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Stats (alias) failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        std::promise<Result<void>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            yams::daemon::GlobalIOContext::global_executor(),
            [this, &prom]() -> boost::asio::awaitable<void> {
                auto r = co_await this->executeAsync();
                prom.set_value(std::move(r));
                co_return;
            },
            boost::asio::detached);
        return fut.get();
    }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        YAMS_ZONE_SCOPED_N("StatusCommand::execute");
        try {
            // Try daemon-first for quick status snapshot
            {
                yams::daemon::ClientConfig cfg;
                if (cli_->hasExplicitDataDir()) {
                    cfg.dataDir = cli_->getDataPath();
                }
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (leaseRes) {
                    auto leaseHandle = std::move(leaseRes.value());
                    auto& client = **leaseHandle;
                    // Always request non-detailed status to avoid any daemon-side
                    // physical scanning. Verbose mode will format these same fields.
                    yams::daemon::StatusRequest sreq;
                    sreq.detailed = false;
                    auto st = co_await client.call(sreq);
                    if (st) {
                        auto s = st.value();
                        auto getCount = [&](const char* key) -> uint64_t {
                            auto it = s.requestCounts.find(key);
                            return it != s.requestCounts.end() ? it->second : 0ULL;
                        };
                        std::string svcSummary;
                        std::string waitingSummary;
                        if (jsonOutput_) {
                            nlohmann::json j;
                            j["running"] = s.running;
                            j["ready"] = s.ready;
                            j["lifecycle_state"] =
                                s.overallStatus.empty()
                                    ? (s.ready ? "ready" : (s.running ? "starting" : "stopped"))
                                    : s.overallStatus;
                            j["version"] = s.version;
                            j["uptimeSeconds"] = s.uptimeSeconds;
                            j["requestsProcessed"] = s.requestsProcessed;
                            j["activeConnections"] = s.activeConnections;
                            j["memoryUsageMb"] = s.memoryUsageMb;
                            j["cpuUsagePercent"] = s.cpuUsagePercent;
                            if (s.retryAfterMs > 0)
                                j["retryAfterMs"] = s.retryAfterMs;
                            j["fsmTransitions"] = s.fsmTransitions;
                            j["fsmHeaderReads"] = s.fsmHeaderReads;
                            j["fsmPayloadReads"] = s.fsmPayloadReads;
                            j["fsmPayloadWrites"] = s.fsmPayloadWrites;
                            j["fsmBytesSent"] = s.fsmBytesSent;
                            j["fsmBytesReceived"] = s.fsmBytesReceived;
                            j["muxActiveHandlers"] = s.muxActiveHandlers;
                            j["muxQueuedBytes"] = s.muxQueuedBytes;
                            j["muxWriterBudgetBytes"] = s.muxWriterBudgetBytes;
                            j["ipcPoolSize"] = s.ipcPoolSize;
                            j["ioPoolSize"] = s.ioPoolSize;
                            {
                                nlohmann::json worker = nlohmann::json::object();
                                worker["threads"] = getCount("worker_threads");
                                worker["active"] = getCount("worker_active");
                                worker["queued"] = getCount("worker_queued");
                                j["worker"] = std::move(worker);
                            }
                            {
                                nlohmann::json pools = nlohmann::json::object();
                                pools["ipc"] = s.ipcPoolSize;
                                pools["io"] = s.ioPoolSize;
                                j["pools"] = std::move(pools);
                            }
                            if (!s.readinessStates.empty()) {
                                nlohmann::json rj = nlohmann::json::object();
                                bool pluginsDegraded = false;
                                bool embeddingDegraded = false;
                                for (const auto& [k, v] : s.readinessStates) {
                                    rj[k] = v;
                                    if (k == std::string("plugins_degraded"))
                                        pluginsDegraded = v;
                                    if (k == std::string("embedding_degraded"))
                                        embeddingDegraded = v;
                                }
                                j["readiness"] = std::move(rj);
                                // Convenience summary for clients
                                nlohmann::json dj = nlohmann::json::object();
                                dj["plugins"] = pluginsDegraded;
                                dj["embedding"] = embeddingDegraded;
                                j["degraded"] = std::move(dj);
                            }
                            if (!s.initProgress.empty()) {
                                nlohmann::json pj = nlohmann::json::object();
                                for (const auto& [k, v] : s.initProgress)
                                    pj[k] = v;
                                j["initProgress"] = std::move(pj);
                            }
                            if (!s.providers.empty()) {
                                nlohmann::json pv = nlohmann::json::array();
                                for (const auto& p : s.providers) {
                                    nlohmann::json rec;
                                    rec["name"] = p.name;
                                    rec["ready"] = p.ready;
                                    rec["degraded"] = p.degraded;
                                    rec["error"] = p.error;
                                    rec["models_loaded"] = p.modelsLoaded;
                                    rec["is_provider"] = p.isProvider;
                                    pv.push_back(std::move(rec));
                                }
                                j["plugins"] = std::move(pv);
                            }
                            // Include storage stats: render only daemon-provided counts (no local
                            // scans)
                            try {
                                // Prefer authoritative metadata counts; fallback to storage object
                                // count
                                uint64_t docs = getCount("documents_total");
                                if (docs == 0)
                                    docs = getCount("storage_documents");
                                uint64_t idxDocs = getCount("documents_indexed");
                                uint64_t logical = getCount("storage_logical_bytes");
                                // Prefer aggregated physical total if available
                                uint64_t physical = getCount("physical_total_bytes");
                                if (physical == 0)
                                    physical = getCount("storage_physical_bytes");
                                uint64_t dedupSaved = getCount("cas_dedup_saved_bytes");

                                if (docs > 0)
                                    j["totalDocuments"] = docs;
                                if (idxDocs > 0)
                                    j["indexedDocuments"] = idxDocs;
                                j["logicalBytes"] = logical;
                                if (physical > 0)
                                    j["physicalBytes"] = physical;
                                // Only compute savings when physical is known
                                if (physical > 0 && logical > 0) {
                                    uint64_t spaceSaved =
                                        (logical > physical) ? (logical - physical) : 0ULL;
                                    double spaceSavedPct = static_cast<double>(spaceSaved) * 100.0 /
                                                           static_cast<double>(logical);
                                    if (spaceSaved > 0)
                                        j["spaceSavingsBytes"] = spaceSaved;
                                    j["spaceSavingsPercent"] = spaceSavedPct;
                                }
                                if (dedupSaved > 0)
                                    j["deduplicatedBytes"] = dedupSaved;
                            } catch (...) {
                            }

                            // Embedding runtime telemetry
                            {
                                nlohmann::json er;
                                er["available"] = s.embeddingAvailable;
                                er["backend"] = s.embeddingBackend;
                                er["model"] = s.embeddingModel;
                                er["path"] = s.embeddingModelPath;
                                er["dim"] = s.embeddingDim;
                                er["intra_threads"] = s.embeddingThreadsIntra;
                                er["inter_threads"] = s.embeddingThreadsInter;
                                j["embedding"] = std::move(er);
                            }
                            // Embedding runtime telemetry (daemon)
                            {
                                nlohmann::json er = nlohmann::json::object();
                                er["available"] = s.embeddingAvailable;
                                er["backend"] = s.embeddingBackend;
                                er["model"] = s.embeddingModel;
                                er["dim"] = s.embeddingDim;
                                er["intra_threads"] = s.embeddingThreadsIntra;
                                er["inter_threads"] = s.embeddingThreadsInter;
                                j["embedding"] = std::move(er);
                            }
                            {
                                nlohmann::json sr = nlohmann::json::object();
                                sr["active"] = s.searchMetrics.active;
                                sr["queued"] = s.searchMetrics.queued;
                                sr["executed"] = s.searchMetrics.executed;
                                sr["cache_hit_rate"] = s.searchMetrics.cacheHitRate;
                                sr["avg_latency_us"] = s.searchMetrics.avgLatencyUs;
                                sr["concurrency_limit"] = s.searchMetrics.concurrencyLimit;
                                j["search"] = std::move(sr);
                            }
                            // Post-ingest gauges (from requestCounts)
                            {
                                nlohmann::json pj;
                                pj["threads"] = getCount("post_ingest_threads");
                                pj["queued"] = getCount("post_ingest_queued");
                                pj["inflight"] = getCount("post_ingest_inflight");
                                pj["capacity"] = getCount("post_ingest_capacity");
                                pj["rate_sec_ema"] = getCount("post_ingest_rate_sec_ema");
                                pj["latency_ms_ema"] = getCount("post_ingest_latency_ms_ema");
                                j["post_ingest"] = std::move(pj);
                            }
                            std::cout << j.dump(2) << std::endl;
                        } else {
                            std::cout << "== DAEMON STATUS ==\n";
                            std::cout << "RUN  : " << (s.running ? "yes" : "no") << "\n";
                            std::cout << "VER  : " << s.version << "\n";
                            std::cout
                                << "STATE: "
                                << (s.overallStatus.empty()
                                        ? (s.ready ? "Ready" : (s.running ? "Starting" : "Stopped"))
                                        : s.overallStatus)
                                << "\n";
                            std::cout << "UP   : " << s.uptimeSeconds << "s\n";
                            std::cout << "REQ  : " << s.requestsProcessed
                                      << "  ACT: " << s.activeConnections << "\n";
                            if (s.retryAfterMs > 0) {
                                std::cout << "BACK : retry-after=" << s.retryAfterMs << "ms\n";
                            }
                            std::cout << "MEM  : " << std::fixed << std::setprecision(1)
                                      << s.memoryUsageMb << "MB  CPU: " << (int)s.cpuUsagePercent
                                      << "%\n";
                            std::cout << "POOL : ipc=" << s.ipcPoolSize << ", io=" << s.ioPoolSize
                                      << "\n";
                            std::cout << "SEARCH: act=" << s.searchMetrics.active
                                      << ", queued=" << s.searchMetrics.queued
                                      << ", hit=" << std::fixed << std::setprecision(1)
                                      << (s.searchMetrics.cacheHitRate * 100.0)
                                      << "% , lat=" << s.searchMetrics.avgLatencyUs << "us"
                                      << ", limit=" << s.searchMetrics.concurrencyLimit << "\n";
                            std::cout.unsetf(std::ios::floatfield);
                            std::cout << "WORK : thr=" << getCount("worker_threads")
                                      << ", act=" << getCount("worker_active")
                                      << ", queued=" << getCount("worker_queued") << "\n";
                            // Surface degraded flags prominently when present
                            try {
                                auto it_pd = s.readinessStates.find("plugins_degraded");
                                auto it_ed = s.readinessStates.find("embedding_degraded");
                                if ((it_pd != s.readinessStates.end() && it_pd->second) ||
                                    (it_ed != s.readinessStates.end() && it_ed->second)) {
                                    std::cout << "DEGD : "
                                              << (it_pd != s.readinessStates.end() && it_pd->second
                                                      ? "plugins "
                                                      : "")
                                              << (it_ed != s.readinessStates.end() && it_ed->second
                                                      ? "embeddings"
                                                      : "")
                                              << "\n";
                                }
                            } catch (...) {
                            }
                            if (!s.ready && !waitingSummary.empty()) {
                                std::cout << "WAIT : " << waitingSummary << "\n";
                            }
                            // Storage stats (daemon metrics only; no local scans)
                            {
                                try {
                                    uint64_t docs = 0, logical = 0, physical = 0;
                                    auto itDocs = s.requestCounts.find("storage_documents");
                                    if (itDocs != s.requestCounts.end())
                                        docs = itDocs->second;
                                    auto itLogical = s.requestCounts.find("storage_logical_bytes");
                                    if (itLogical != s.requestCounts.end())
                                        logical = itLogical->second;
                                    auto itPhysical =
                                        s.requestCounts.find("storage_physical_bytes");
                                    if (itPhysical != s.requestCounts.end())
                                        physical = itPhysical->second;

                                    bool storageOk = (physical > 0 || logical > 0 || docs > 0);

                                    auto humanSize = [&](uint64_t b) {
                                        const char* u[] = {"B", "KB", "MB", "GB", "TB"};
                                        double v = static_cast<double>(b);
                                        int i = 0;
                                        while (v >= 1024.0 && i < 4) {
                                            v /= 1024.0;
                                            ++i;
                                        }
                                        std::ostringstream os;
                                        os << std::fixed << std::setprecision(v < 10 ? 1 : 0) << v
                                           << u[i];
                                        return os.str();
                                    };

                                    uint64_t saved = 0ULL;
                                    int pct = 0;
                                    if (physical > 0 && logical > 0) {
                                        saved = (logical > physical) ? (logical - physical) : 0ULL;
                                        if (logical > 0)
                                            pct = static_cast<int>((saved * 100.0) / logical);
                                    }
                                    std::cout << "STOR : " << (storageOk ? "ok" : "unknown")
                                              << ", docs=" << docs
                                              << ", logical=" << humanSize(logical);
                                    if (physical > 0) {
                                        std::cout << ", physical=" << humanSize(physical)
                                                  << ", saved=" << humanSize(saved) << " (" << pct
                                                  << "%)";
                                    }
                                    std::cout << "\n";

                                } catch (...) {
                                    std::cout << "STOR : error reading stats\n";
                                }
                            }

                            // Short plugins summary (typed providers)
                            if (!s.providers.empty()) {
                                std::cout << "PLUG : " << s.providers.size() << " loaded";
                                // Identify adopted provider and its state
                                for (const auto& p : s.providers) {
                                    if (p.isProvider) {
                                        std::cout << ", provider='" << p.name << "'";
                                        if (!p.ready)
                                            std::cout << " [not-ready]";
                                        if (p.degraded)
                                            std::cout << " [degraded]";
                                        if (p.modelsLoaded > 0)
                                            std::cout << ", models=" << p.modelsLoaded;
                                        if (!p.error.empty())
                                            std::cout << ", error=\"" << p.error << "\"";
                                        break;
                                    }
                                }
                                std::cout << "\n";
                            }
                            // Embedding runtime summary (prefer readiness when daemon omits fields)
                            bool embedAvail = s.embeddingAvailable;
                            try {
                                auto it = s.readinessStates.find("vector_embeddings_available");
                                if (it != s.readinessStates.end())
                                    embedAvail = it->second;
                            } catch (...) {
                            }
                            std::cout << "EMBED: " << (embedAvail ? "available" : "unavailable");
                            if (!s.embeddingBackend.empty())
                                std::cout << ", backend=" << s.embeddingBackend;
                            if (!s.embeddingModel.empty())
                                std::cout << ", model='" << s.embeddingModel << "'";
                            if (!s.embeddingModelPath.empty())
                                std::cout << ", path='" << s.embeddingModelPath << "'";
                            if (s.embeddingDim > 0)
                                std::cout << ", dim=" << s.embeddingDim;
                            if (s.embeddingThreadsIntra > 0 || s.embeddingThreadsInter > 0)
                                std::cout << ", threads=" << s.embeddingThreadsIntra << "/"
                                          << s.embeddingThreadsInter;
                            std::cout << "\n";
                        }
                        // In verbose mode, pull vector DB quick stats (size + rows) from daemon
                        // stats
                        if (!jsonOutput_ && verbose_) {
                            std::cout << "\n== VERBOSE STATS ==\n";
                            try {
                                auto getU64 = [&](const std::string& key) -> uint64_t {
                                    auto it = s.requestCounts.find(key);
                                    return it != s.requestCounts.end() ? it->second : 0;
                                };

                                auto humanSize = [&](uint64_t b) {
                                    const char* u[] = {"B", "KB", "MB", "GB", "TB"};
                                    double v = static_cast<double>(b);
                                    int i = 0;
                                    while (v >= 1024.0 && i < 4) {
                                        v /= 1024.0;
                                        ++i;
                                    }
                                    std::ostringstream os;
                                    os << std::fixed << std::setprecision(v < 10 ? 1 : 0) << v
                                       << u[i];
                                    return os.str();
                                };

                                // Post-ingest pipeline
                                {
                                    uint64_t pit = getU64("post_ingest_threads");
                                    uint64_t piq = getU64("post_ingest_queued");
                                    uint64_t pii = getU64("post_ingest_inflight");
                                    uint64_t pic = getU64("post_ingest_capacity");
                                    uint64_t pil = getU64("post_ingest_latency_ms_ema");
                                    uint64_t pir = getU64("post_ingest_rate_sec_ema");
                                    std::cout << "POST : threads=" << pit << ", queued=" << piq
                                              << ", inflight=" << pii << ", cap=" << pic
                                              << ", rate=" << pir << "/s, lat=" << pil << "ms\n";
                                }

                                std::cout << "SEARCH: active=" << s.searchMetrics.active
                                          << ", queued=" << s.searchMetrics.queued
                                          << ", executed=" << s.searchMetrics.executed
                                          << ", cache_hit=" << std::fixed << std::setprecision(1)
                                          << (s.searchMetrics.cacheHitRate * 100.0)
                                          << "%, lat=" << s.searchMetrics.avgLatencyUs
                                          << "us, limit=" << s.searchMetrics.concurrencyLimit
                                          << "\n";
                                std::cout.unsetf(std::ios::floatfield);

                                uint64_t casPhys = getU64("cas_physical_bytes");
                                uint64_t total = getU64("physical_total_bytes");

                                if (casPhys > 0 || total > 0) {
                                    uint64_t logical = getU64("storage_logical_bytes");
                                    uint64_t casRaw = getU64("cas_unique_raw_bytes");
                                    if (casRaw == 0)
                                        casRaw = logical;
                                    uint64_t dedupSaved = getU64("cas_dedup_saved_bytes");
                                    uint64_t compSaved = getU64("cas_compress_saved_bytes");
                                    uint64_t meta = getU64("metadata_physical_bytes");
                                    uint64_t idx = getU64("index_physical_bytes");
                                    uint64_t vec = getU64("vector_physical_bytes");
                                    uint64_t tmp = getU64("logs_tmp_physical_bytes");
                                    uint64_t casSaved = dedupSaved + compSaved;

                                    std::cout << "--- Storage Breakdown ---\n";
                                    std::cout << "CAS (Content Store):\n";
                                    std::cout << "  Ingested (logical) : " << humanSize(casRaw)
                                              << "\n";
                                    std::cout << "  Physical           : " << humanSize(casPhys)
                                              << "\n";
                                    std::cout << "  Savings            : " << humanSize(casSaved)
                                              << " (dedup=" << humanSize(dedupSaved)
                                              << ", compress=" << humanSize(compSaved) << ")\n";
                                    std::cout << "Overhead:\n";
                                    std::cout << "  Metadata DB        : " << humanSize(meta)
                                              << "\n";
                                    std::cout << "  Search Index       : " << humanSize(idx)
                                              << "\n";
                                    std::cout << "  Vector Store       : " << humanSize(vec)
                                              << "\n";
                                    std::cout << "  Logs & Temp        : " << humanSize(tmp)
                                              << "\n";
                                    std::cout << "-------------------------\n";
                                    std::cout << "Total Physical Usage : " << humanSize(total)
                                              << "\n";
                                }

                                bool vecReady = false;
                                try {
                                    auto it = s.readinessStates.find("vector_index");
                                    if (it != s.readinessStates.end())
                                        vecReady = it->second;
                                } catch (...) {
                                }

                                std::string line = "VECDB: ";
                                uint64_t vecBytes = getU64("vector_physical_bytes");
                                if (vecBytes > 0) {
                                    line += "Initialized - " + humanSize(vecBytes);
                                } else {
                                    line += vecReady ? "Initialized" : "Not initialized";
                                }
                                uint64_t vecRows = getU64("vector_rows");
                                if (vecRows > 0) {
                                    line += " (" + std::to_string(vecRows) + " rows)";
                                }
                                std::cout << line << "\n";

                            } catch (const std::exception& e) {
                                std::cout << "Error fetching verbose stats: " << e.what() << "\n";
                            }
                        }
                        co_return Result<void>();
                    }
                }
            }
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                if (jsonOutput_) {
                    std::cout
                        << R"({"status": "not_initialized", "message": "YAMS not initialized"})"
                        << std::endl;
                } else {
                    std::cout << "YAMS System Status\n";
                    std::cout << "==================\n";
                    std::cout << "✗ Not initialized\n";
                    std::cout << "  → Run 'yams init' to get started\n";
                }
                co_return Result<void>();
            }
            auto store = cli_->getContentStore();
            auto metadataRepo = cli_->getMetadataRepository();
            if (!store || !metadataRepo)
                co_return Error{ErrorCode::NotInitialized, "Failed to access storage components"};
            StatusInfo status = gatherStatusInfo(store, metadataRepo);
            if (jsonOutput_)
                outputJson(status);
            else
                outputText(status);
            co_return Result<void>();
        } catch (const std::exception& e) {
            co_return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    bool jsonOutput_ = false;
    bool verbose_ = false;
    // Default to using daemon-reported physical sizes only; do not local-scan unless opted in
    bool noPhysical_ = true;

    struct StatusInfo {
        // Storage
        bool storageHealthy = false;
        uint64_t totalDocuments = 0;
        uint64_t totalSize = 0;
        std::string storagePath;

        // Configuration
        bool configMigrationNeeded = false;
        std::string configVersion;
        std::string configPath;

        // Models
        std::vector<std::string> availableModels;
        bool hasModels = false;

        // Embeddings
        bool autoGenerationEnabled = false;
        uint64_t embeddingCount = 0;
        bool vectorDbHealthy = false;
        std::string preferredModel;

        // Worker pool (daemon)
        uint64_t workerThreads = 0;
        uint64_t workerActive = 0;
        uint64_t workerQueued = 0;
        uint64_t workerUtilPct = 0;

        // Collected advice (built after gathering raw stats)
        yams::cli::RecommendationBuilder advice;
        // Internal legacy warnings gathered conditionally (verbose or critical); will be
        // migrated into advice builder generation path
        std::vector<std::string> legacyWarnings;
    };

    StatusInfo gatherStatusInfo(std::shared_ptr<api::IContentStore> store,
                                std::shared_ptr<metadata::IMetadataRepository> metadataRepo) {
        StatusInfo info;

        // Storage information
        try {
            auto stats = store->getStats();
            info.storageHealthy = true;
            info.totalDocuments = stats.totalObjects;
            info.totalSize = stats.totalBytes;
            info.storagePath = cli_->getDataPath().string();
        } catch (const std::exception& e) {
            info.storageHealthy = false;
            info.legacyWarnings.push_back("Storage access failed: " + std::string(e.what()));
        }

        // Configuration migration status
        try {
            const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
            const char* homeEnv = std::getenv("HOME");

            fs::path configPath;
            if (xdgConfigHome) {
                configPath = fs::path(xdgConfigHome) / "yams" / "config.toml";
            } else if (homeEnv) {
                configPath = fs::path(homeEnv) / ".config" / "yams" / "config.toml";
            } else {
                configPath = fs::path("~/.config") / "yams" / "config.toml";
            }

            info.configPath = configPath.string();

            config::ConfigMigrator migrator;
            auto needsResult = migrator.needsMigration(configPath);

            if (needsResult) {
                info.configMigrationNeeded = needsResult.value();

                // Get current version
                auto versionResult = migrator.getConfigVersion(configPath);
                if (versionResult) {
                    info.configVersion = versionResult.value().toString();
                } else {
                    info.configVersion = "1.0.0";
                }
            } else {
                // Error checking migration - assume v1 if config exists
                if (fs::exists(configPath)) {
                    info.configMigrationNeeded = true;
                    info.configVersion = "1.0.0 (assumed)";
                } else {
                    info.configMigrationNeeded = false;
                    info.configVersion = "none";
                }
            }
        } catch (const std::exception& e) {
            info.configMigrationNeeded = false;
            info.configVersion = "unknown";
            if (verbose_) {
                info.legacyWarnings.push_back("Config migration check failed: " +
                                              std::string(e.what()));
            }
        }

        // Check available models
        const char* home = std::getenv("HOME");
        if (home) {
            fs::path modelsPath = fs::path(home) / ".yams" / "models";
            if (fs::exists(modelsPath)) {
                for (const auto& entry : fs::directory_iterator(modelsPath)) {
                    if (entry.is_directory()) {
                        fs::path modelFile = entry.path() / "model.onnx";
                        if (fs::exists(modelFile)) {
                            info.availableModels.push_back(entry.path().filename().string());
                        }
                    }
                }
            }
        }
        info.hasModels = !info.availableModels.empty();

        // Check embedding configuration
        std::unique_ptr<vector::EmbeddingService> embeddingService;

        if (store && metadataRepo) {
            embeddingService = std::make_unique<vector::EmbeddingService>(store, metadataRepo,
                                                                          cli_->getDataPath());
        }

        info.autoGenerationEnabled = embeddingService && embeddingService->isAvailable();
        info.preferredModel = info.hasModels ? info.availableModels[0] : "none";

        auto leaseProbe = yams::cli::acquire_cli_daemon_client_shared(yams::daemon::ClientConfig{});
        if (leaseProbe) {
            auto leaseHandle = std::move(leaseProbe.value());
            std::promise<Result<yams::daemon::StatusResponse>> promProbe;
            auto futProbe = promProbe.get_future();
            auto workProbe = [leaseHandle, &promProbe]() mutable -> boost::asio::awaitable<void> {
                auto& client = **leaseHandle;
                yams::daemon::StatusRequest sreq;
                sreq.detailed = false;
                auto sr = co_await client.call(sreq);
                promProbe.set_value(std::move(sr));
                co_return;
            };
            boost::asio::co_spawn(yams::daemon::GlobalIOContext::global_executor(), workProbe(),
                                  boost::asio::detached);
            if (futProbe.wait_for(std::chrono::milliseconds(800)) == std::future_status::ready) {
                auto sres = futProbe.get();
                if (sres) {
                    const auto& s = sres.value();
                    auto itT = s.requestCounts.find("worker_threads");
                    auto itA = s.requestCounts.find("worker_active");
                    auto itQ = s.requestCounts.find("worker_queued");
                    if (itT != s.requestCounts.end())
                        info.workerThreads = itT->second;
                    if (itA != s.requestCounts.end())
                        info.workerActive = itA->second;
                    if (itQ != s.requestCounts.end())
                        info.workerQueued = itQ->second;
                    if (info.workerThreads > 0)
                        info.workerUtilPct =
                            static_cast<uint64_t>((100.0 * info.workerActive) / info.workerThreads);
                }
            }
        }

        // Check vector database status
        try {
            auto dbPath = (cli_->getDataPath() / "vectors.db");
            if (!std::filesystem::exists(dbPath)) {
                // Do not create vectors.db during status; report absence only
                info.vectorDbHealthy = true;
            } else {
                vector::VectorDatabaseConfig vdbConfig;
                vdbConfig.database_path = dbPath.string();
                // Use a conservative default; existing table schema determines dimension
                vdbConfig.embedding_dim = 384;
                auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
                if (vectorDb->initialize()) {
                    info.vectorDbHealthy = true;
                    if (vectorDb->tableExists()) {
                        info.embeddingCount = vectorDb->getVectorCount();
                    }
                }
            }
        } catch (const std::exception& e) {
            info.vectorDbHealthy = false;
            if (verbose_) {
                info.legacyWarnings.push_back("Vector database check failed: " +
                                              std::string(e.what()));
            }
        }

        // Generate structured recommendations/advice
        generateRecommendations(info);

        return info;
    }

    void generateRecommendations(StatusInfo& info) {
        // Taxonomy codes (STATUS_ prefix) adopted per unified plan
        if (info.configMigrationNeeded) {
            info.advice.warning("STATUS_CONFIG_MIGRATION",
                                "Update configuration: yams config migrate",
                                "New configuration format available");
        }
        if (!info.hasModels) {
            info.advice.warning(
                "STATUS_NO_MODELS",
                "Download an embedding model: yams model --download all-MiniLM-L6-v2",
                "Semantic search and embeddings require a model");
        }
        if (info.hasModels && !info.autoGenerationEnabled) {
            info.advice.info("STATUS_EMB_AUTO_DISABLED",
                             "Enable auto-embedding: yams config embeddings enable",
                             "Keeps embeddings up to date automatically");
        }
        if (info.totalDocuments > 0 && info.embeddingCount == 0) {
            info.advice.warning(
                "STATUS_EMB_NONE",
                "Generate embeddings for existing documents: yams repair --embeddings",
                "Semantic relevance requires vector representations");
        }
        if (info.totalDocuments > 100 && info.embeddingCount < info.totalDocuments / 2) {
            info.advice.info(
                "STATUS_EMB_LOW_COVERAGE",
                "Many documents lack embeddings - consider running: yams repair --embeddings",
                "Higher coverage improves semantic recall");
        }
        // Promote legacyWarnings into structured advice (Warning severity) preserving message.
        for (const auto& w : info.legacyWarnings) {
            // Map specific known substrings to stable codes; fallback generic.
            std::string code = "STATUS_MISC_WARNING";
            if (w.find("Storage access failed") != std::string::npos)
                code = "STATUS_STORAGE_UNHEALTHY";
            else if (w.find("Config migration check failed") != std::string::npos)
                code = "STATUS_CONFIG_CHECK_FAILED";
            else if (w.find("Vector database check failed") != std::string::npos)
                code = "STATUS_VECTOR_DB_ERROR";
            info.advice.warning(code, w);
        }
    }

    void outputJson(const StatusInfo& info) {
        std::cout << "{\n";
        std::cout << "  \"storage\": {\n";
        std::cout << "    \"healthy\": " << (info.storageHealthy ? "true" : "false") << ",\n";
        std::cout << "    \"totalDocuments\": " << info.totalDocuments << ",\n";
        std::cout << "    \"totalSize\": " << info.totalSize << ",\n";
        std::cout << "    \"path\": \"" << info.storagePath << "\"\n";
        std::cout << "  },\n";
        std::cout << "  \"configuration\": {\n";
        std::cout << "    \"migrationNeeded\": " << (info.configMigrationNeeded ? "true" : "false")
                  << ",\n";
        std::cout << "    \"version\": \"" << info.configVersion << "\",\n";
        std::cout << "    \"path\": \"" << info.configPath << "\"\n";
        std::cout << "  },\n";
        std::cout << "  \"models\": {\n";
        std::cout << "    \"available\": [";
        for (size_t i = 0; i < info.availableModels.size(); ++i) {
            if (i > 0)
                std::cout << ", ";
            std::cout << "\"" << info.availableModels[i] << "\"";
        }
        std::cout << "],\n";
        std::cout << "    \"hasModels\": " << (info.hasModels ? "true" : "false") << "\n";
        std::cout << "  },\n";
        std::cout << "  \"embeddings\": {\n";
        std::cout << "    \"autoGenerationEnabled\": "
                  << (info.autoGenerationEnabled ? "true" : "false") << ",\n";
        std::cout << "    \"embeddingCount\": " << info.embeddingCount << ",\n";
        std::cout << "    \"vectorDbHealthy\": " << (info.vectorDbHealthy ? "true" : "false")
                  << ",\n";
        std::cout << "    \"preferredModel\": \"" << info.preferredModel << "\"\n";
        std::cout << "  },\n";
        std::cout << "  \"advice\": " << yams::cli::recommendationsToJson(info.advice) << "\n";
        std::cout << "}\n";
    }

    void outputText(const StatusInfo& info) {
        using namespace yams::cli::ui;

        std::cout << title_banner("YAMS System Status") << "\n\n";

        auto severityLabel = [](std::string label, const char* color, std::string icon) {
            return colorize(icon + " " + std::move(label), color);
        };

        auto humanCount = [](uint64_t value) {
            if (value < 1000)
                return std::to_string(value);
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(value < 10000 ? 1 : 0)
                << static_cast<double>(value) / 1000.0 << "k";
            return oss.str();
        };

        const bool configWarn = info.configMigrationNeeded;
        const bool embeddingsWarn = !info.autoGenerationEnabled;
        const bool modelsWarn = !info.hasModels;
        const bool vectorWarn = !info.vectorDbHealthy;
        const bool storageOk = info.storageHealthy;

        std::vector<Row> overview;
        overview.push_back(
            {severityLabel("Storage", storageOk ? Ansi::GREEN : Ansi::RED, storageOk ? "✓" : "✗"),
             storageOk ? ("Healthy · " + formatSize(info.totalSize) + " · " +
                          humanCount(info.totalDocuments) + " docs")
                       : std::string("Check logs for storage errors"),
             info.storagePath.empty() ? std::string{} : std::string{"path: " + info.storagePath}});

        overview.push_back(
            {severityLabel("Configuration", configWarn ? Ansi::YELLOW : Ansi::GREEN,
                           configWarn ? "⚠" : "✓"),
             std::string("v") + info.configVersion +
                 (configWarn ? " · migration recommended" : " · up-to-date"),
             info.configPath.empty() ? std::string{} : std::string{"config: " + info.configPath}});

        overview.push_back({severityLabel("Models", modelsWarn ? Ansi::YELLOW : Ansi::GREEN,
                                          modelsWarn ? "⚠" : "✓"),
                            info.hasModels
                                ? (std::to_string(info.availableModels.size()) + " available" +
                                   (info.availableModels.size() > 0
                                        ? " (" +
                                              [&]() {
                                                  std::ostringstream oss;
                                                  for (size_t i = 0;
                                                       i < info.availableModels.size(); ++i) {
                                                      if (i)
                                                          oss << ", ";
                                                      oss << info.availableModels[i];
                                                  }
                                                  return oss.str();
                                              }() +
                                              ")"
                                        : std::string()))
                                : std::string("No embedding models detected"),
                            std::string{}});

        const bool hasEmbeddings = info.embeddingCount > 0;
        const bool embeddingsCritical = (info.totalDocuments > 0 && !hasEmbeddings);
        std::ostringstream embVal;
        embVal << (info.autoGenerationEnabled ? "Auto-build enabled" : "Auto-build disabled");
        if (info.embeddingCount > 0) {
            embVal << " · " << humanCount(info.embeddingCount) << " vectors";
            if (info.totalDocuments > 0) {
                const double coverage =
                    static_cast<double>(info.embeddingCount) /
                    static_cast<double>(std::max<uint64_t>(1, info.totalDocuments));
                embVal << " · " << std::fixed << std::setprecision(0) << (coverage * 100.0)
                       << "% coverage";
            }
        } else if (info.totalDocuments > 0) {
            embVal << " · 0 of " << humanCount(info.totalDocuments) << " documents";
        }

        const char* embeddingColor =
            embeddingsCritical
                ? Ansi::RED
                : ((info.autoGenerationEnabled && hasEmbeddings) ? Ansi::GREEN : Ansi::YELLOW);
        const char* embeddingIcon = embeddingsCritical                              ? "✗"
                                    : (info.autoGenerationEnabled && hasEmbeddings) ? "✓"
                                                                                    : "⚠";

        overview.push_back(
            {severityLabel("Embeddings", embeddingColor, embeddingIcon), embVal.str(),
             info.preferredModel == "none" ? std::string{}
                                           : std::string{"preferred: " + info.preferredModel}});

        overview.push_back({severityLabel("Vector DB", vectorWarn ? Ansi::YELLOW : Ansi::GREEN,
                                          vectorWarn ? "⚠" : "✓"),
                            info.vectorDbHealthy ? "Operational" : "Not available", std::string{}});

        std::cout << section_header("Overview") << "\n\n";
        render_rows(std::cout, overview);

        std::vector<Row> worker;
        if (info.workerThreads > 0) {
            std::ostringstream details;
            details << info.workerThreads << " threads · " << info.workerActive << " active · "
                    << info.workerQueued << " queued";
            std::string util = std::to_string(info.workerUtilPct) + "%";
            const char* utilColor = info.workerUtilPct >= 85
                                        ? (info.workerUtilPct >= 95 ? Ansi::RED : Ansi::YELLOW)
                                        : Ansi::GREEN;
            const char* workerIcon = "✓";
            if (info.workerUtilPct >= 95)
                workerIcon = "✗";
            else if (info.workerUtilPct >= 85)
                workerIcon = "⚠";
            worker.push_back({severityLabel("Worker Pool", utilColor, workerIcon),
                              colorize(util, utilColor), details.str()});
        }
        if (!worker.empty()) {
            std::cout << "\n" << section_header("Background Processing") << "\n\n";
            render_rows(std::cout, worker);
        }

        if (!info.advice.empty()) {
            std::cout << "\n" << section_header("Recommendations") << "\n\n";
            yams::cli::printRecommendationsText(info.advice, std::cout);
        }

        std::cout << "\n"
                  << colorize("→ For daemon telemetry run 'yams daemon status -d'", Ansi::DIM)
                  << "\n";

        // Add hint to run doctor if embeddings have issues
        const bool embeddingIssues = !info.vectorDbHealthy ||
                                     (info.totalDocuments > 0 && info.embeddingCount == 0) ||
                                     !info.autoGenerationEnabled;
        if (embeddingIssues) {
            std::cout << colorize("→ For configuration issues run 'yams doctor'", Ansi::YELLOW)
                      << "\n";
        }
    }

    std::string formatSize(uint64_t bytes) const {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unitIndex = 0;
        double size = static_cast<double>(bytes);

        while (size >= 1024 && unitIndex < 4) {
            size /= 1024;
            unitIndex++;
        }

        std::ostringstream oss;
        if (unitIndex == 0) {
            oss << bytes << " B";
        } else {
            oss << std::fixed << std::setprecision(1) << size << " " << units[unitIndex];
        }
        return oss.str();
    }
};

// Factory function
std::unique_ptr<ICommand> createStatusCommand() {
    return std::make_unique<StatusCommand>();
}

} // namespace yams::cli
