#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/version.hpp>

namespace yams::cli {

using json = nlohmann::json;

class StatsCommand : public ICommand {
public:
    std::string getName() const override { return "stats"; }

    std::string getDescription() const override {
        return "Display storage statistics and health metrics";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;

        auto* cmd = app.add_subcommand("stats", getDescription());

        cmd->add_option("--format", format_, "Output format: text, json")
            ->default_val("text")
            ->check(CLI::IsMember({"text", "json"}));

        // Simplified UX options
        cmd->add_flag("--local", localOnly_, "Force local-only statistics (do not contact daemon)");
        cmd->add_flag("--watch", watchMode_, "Live refresh (service dashboard)");
        cmd->add_flag("-v,--verbose", verbose_,
                      "Show comprehensive technical details and diagnostics");
        cmd->add_flag("--color", forceColor_, "Force-enable ANSI colors in output");
        cmd->add_flag("--no-color", noColor_, "Disable ANSI colors in output");
        cmd->add_flag("--daemon-only", daemonOnly_, "Use daemon results only (no local fallback)");

        // Nested subcommand: yams stats vectors
        auto* vectors = cmd->add_subcommand("vectors", "Show vector database statistics");
        vectors->add_option("--format", format_, "Output format: text, json")
            ->default_val("text")
            ->check(CLI::IsMember({"text", "json"}));
        vectors->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        vectors->add_flag("--daemon-only", daemonOnly_,
                          "Use daemon results only (no local fallback)");
        vectors->callback([this]() {
            vectorsMode_ = true;
            auto r = this->executeVectors();
            if (!r) {
                spdlog::error("Stats (vectors) failed: {}", r.error().message);
                std::exit(1);
            }
        });

        // System help: explain fields/metrics shown by stats
        auto* helpCmd = cmd->add_subcommand("help", "Show system metrics help for stats output");
        helpCmd->callback([this]() { renderSystemHelp(); });

        cmd->callback([this]() {
            auto r = this->execute();
            if (!r) {
                spdlog::error("Stats command failed: {}", r.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        // Temporary mitigation: force local-only stats until daemon path bug is fixed
        return executeLocal();

        // Apply color mode overrides
        if (forceColor_) {
            yams::cli::ui::set_color_mode(yams::cli::ui::ColorMode::ForceOn);
        } else if (noColor_) {
            yams::cli::ui::set_color_mode(yams::cli::ui::ColorMode::ForceOff);
        } else {
            yams::cli::ui::clear_color_override();
        }

        // If local-only mode requested, skip daemon entirely
        if (localOnly_) {
            return executeLocal();
        }

        // Readiness check removed to prevent blocking on unready services

        // Build daemon request using simplified protocol
        yams::daemon::GetStatsRequest dreq;
        dreq.detailed = verbose_;        // Verbose mode requests detailed stats
        dreq.includeCache = true;        // Always include cache info
        dreq.includeHealth = true;       // Always include health (now default)
        dreq.showFileTypes = verbose_;   // Only in verbose mode
        dreq.showCompression = verbose_; // Only in verbose mode
        dreq.showDuplicates = verbose_;  // Only in verbose mode
        dreq.showDedup = verbose_;       // Only in verbose mode
        dreq.showPerformance = verbose_; // Only in verbose mode

        // Render lambda for results
        auto render = [&](const yams::daemon::GetStatsResponse& resp) -> Result<void> {
            if (!localOnly_ && resp.totalDocuments == 0 && resp.totalSize == 0) {
                return executeLocal();
            }
            if (format_ == "json") {
                return renderJSON(resp);
            } else {
                return renderText(resp);
            }
        };

        // Prefer a direct daemon call (no pooling) for determinism, mirroring list_command
        // Falls back to local stats if the daemon path fails for any reason.
        auto fallback = [&]() -> Result<void> { return executeLocal(); };

        try {
            yams::daemon::ClientConfig cfg;
            cfg.dataDir = cli_->getDataPath();
            cfg.enableChunkedResponses = false; // stats is small; avoid streaming complexity
            cfg.singleUseConnections = true;    // keep isolation to avoid cross-talk

            // Fast-fallback mode: non-verbose, non-watch, text, and not daemon-only.
            // Use a more tolerant timeout to avoid spurious local fallbacks, and align with
            // DaemonClient defaults and status/doctor behavior.
            bool shortMode = (!verbose_ && !watchMode_ && format_ != "json" && !daemonOnly_);
            auto reqTimeout =
                shortMode ? std::chrono::milliseconds(5000) : std::chrono::milliseconds(30000);
            cfg.requestTimeout = reqTimeout;
            // Make connection/header/body timeouts explicit to reduce edge-case stalls
            cfg.connectTimeout = std::chrono::milliseconds(shortMode ? 2000 : 3000);
            cfg.headerTimeout = std::chrono::milliseconds(shortMode ? 5000 : 15000);
            cfg.bodyTimeout = std::chrono::milliseconds(shortMode ? 15000 : 30000);

            yams::daemon::DaemonClient client(cfg);

            std::promise<Result<yams::daemon::GetStatsResponse>> prom;
            auto fut = prom.get_future();
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&]() -> boost::asio::awaitable<void> {
                    auto r = co_await client.call(dreq);
                    prom.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);
            if (fut.wait_for(reqTimeout) == std::future_status::ready) {
                auto result = fut.get();
                if (result) {
                    const auto& resp = result.value();

                    // When daemon reports not_ready in shortMode, silently fallback to local
                    if (shortMode) {
                        try {
                            auto it = resp.additionalStats.find("not_ready");
                            if (it != resp.additionalStats.end() && it->second == "true") {
                                return executeLocal();
                            }
                            auto itj = resp.additionalStats.find("json");
                            if (itj != resp.additionalStats.end()) {
                                auto j = json::parse(itj->second);
                                if (j.contains("not_ready") && j["not_ready"].get<bool>()) {
                                    return executeLocal();
                                }
                            }
                        } catch (...) {
                            // ignore parse errors and render as-is
                        }
                    }

                    return render(resp);
                }
            }
            // On failure, choose fallback only when not daemon-only
            return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats unavailable"} : fallback();
        } catch (...) {
            return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats exception"} : fallback();
        }

        // Close execute()
    }

    // Native coroutine implementation to avoid promise bridging when runner is async
    boost::asio::awaitable<Result<void>> executeAsync() override {
        // Temporary mitigation: force local-only stats until daemon path bug is fixed
        co_return executeLocal();

        // Apply color mode overrides (same as execute)
        if (forceColor_) {
            yams::cli::ui::set_color_mode(yams::cli::ui::ColorMode::ForceOn);
        } else if (noColor_) {
            yams::cli::ui::set_color_mode(yams::cli::ui::ColorMode::ForceOff);
        } else {
            yams::cli::ui::clear_color_override();
        }

        if (localOnly_)
            co_return executeLocal();

        yams::daemon::GetStatsRequest dreq;
        dreq.detailed = verbose_;
        dreq.includeCache = true;
        dreq.includeHealth = true;
        dreq.showFileTypes = verbose_;
        dreq.showCompression = verbose_;
        dreq.showDuplicates = verbose_;
        dreq.showDedup = verbose_;
        dreq.showPerformance = verbose_;

        auto render = [&](const yams::daemon::GetStatsResponse& resp) -> Result<void> {
            if (!localOnly_ && resp.totalDocuments == 0 && resp.totalSize == 0) {
                return executeLocal();
            }
            return (format_ == "json") ? renderJSON(resp) : renderText(resp);
        };

        try {
            yams::daemon::ClientConfig cfg;
            cfg.dataDir = cli_->getDataPath();
            cfg.enableChunkedResponses = false;
            cfg.singleUseConnections = true;
            cfg.requestTimeout = std::chrono::milliseconds(30000);
            cfg.connectTimeout = std::chrono::milliseconds(3000);
            cfg.headerTimeout = std::chrono::milliseconds(15000);
            cfg.bodyTimeout = std::chrono::milliseconds(30000);
            yams::daemon::DaemonClient client(cfg);
            auto result = co_await client.call(dreq);
            if (result)
                co_return render(result.value());
            co_return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats unavailable"}
                                  : executeLocal();
        } catch (...) {
            co_return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats exception"}
                                  : executeLocal();
        }
    }

private:
    void renderSystemHelp() {
        std::cout << "YAMS System Metrics (stats)\n";
        std::cout << "===========================\n\n";
        std::cout << "Core Counters:\n";
        std::cout << "  - totalDocuments: Number of documents indexed\n";
        std::cout << "  - totalSize: Total logical size of stored content\n";
        std::cout << "  - indexedDocuments: Documents with embeddings/index visibility\n";
        std::cout << "  - vectorIndexSize: On-disk size of the vector index (bytes)\n\n";
        std::cout << "Streaming Metrics (daemon):\n";
        std::cout << "  - time_to_first_byte_ms: Time from header to first non-keepalive\n";
        std::cout << "  - batches_emitted: Number of response chunks sent\n";
        std::cout << "  - keepalive_count: Keepalive frames sent during compute\n\n";
        std::cout << "Grep/List/Retrieval Modes:\n";
        std::cout << "  - Grep hot/cold: hot uses extracted text; cold scans CAS\n";
        std::cout << "  - List paths-only: avoids snippet/metadata hydration\n";
        std::cout << "  - Retrieval auto: prefers extracted text when present\n\n";
        std::cout << "Env Knobs (optional):\n";
        std::cout << "  - YAMS_KEEPALIVE_MS: Streaming keepalive cadence\n";
        std::cout << "  - YAMS_GREP_FIRST_BATCH_MAX_WAIT_MS: Grep first-burst window\n";
        std::cout << "  - YAMS_GREP_BATCH_SIZE: Grep chunk batch size override\n";
        std::cout << "  - YAMS_LIST_MODE, YAMS_GREP_MODE, YAMS_RETRIEVAL_MODE: "
                     "hot_only|cold_only|auto\n\n";
        std::cout << "Per-document Overrides:\n";
        std::cout << "  - Tag/metadata force_cold=true: Force cold path for a document\n\n";
        std::cout << "Tips:\n";
        std::cout << "  - Use 'yams stats --format json' for scriptable output\n";
        std::cout << "  - Use 'yams stats vectors' to focus on embedding coverage\n";
    }
    Result<void> executeVectors() { return executeVectorsLocal(); }

    Result<void> executeVectorsLocal() {
        // Original local vector stats implementation
        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }

        namespace fs = std::filesystem;
        fs::path storagePath = cli_->getDataPath();
        fs::path vdbPath = storagePath / "vectors.db";

        bool exists = fs::exists(vdbPath);
        size_t vectorCount = 0;

        // Vector DB analysis logic here - simplified for now
        if (format_ == "json") {
            json j;
            j["vectorDb"] = {
                {"path", vdbPath.string()}, {"exists", exists}, {"embeddings", vectorCount}};
            std::cout << j.dump(2) << std::endl;
        } else {
            std::cout << "== VECTORS ==\n";
            std::cout << "PATH : " << vdbPath.string() << "\n";
            std::cout << "EXIST: " << (exists ? "yes" : "no") << "\n";
            std::cout << "COUNT: " << vectorCount << "\n";
        }

        return Result<void>();
    }

    Result<void> executeLocal() {
        // Fallback to original local implementation when daemon unavailable
        // This preserves all the rich functionality from the original stats command
        spdlog::info("Daemon unavailable, using local statistics");

        auto ensured = cli_->ensureStorageInitialized();
        if (!ensured) {
            return ensured;
        }

        auto store = cli_->getContentStore();
        if (!store) {
            return Error{ErrorCode::NotInitialized, "Content store not initialized"};
        }

        // Get basic statistics
        auto stats = store->getStats();

        // Calculate storage size and provide basic output
        // Also check vector DB file size for local approximations
        namespace fs = std::filesystem;
        fs::path vdbPath = cli_->getDataPath() / "vectors.db";
        uint64_t vdbSize = 0;
        std::error_code ec;
        if (fs::exists(vdbPath, ec) && !ec) {
            vdbSize = fs::file_size(vdbPath, ec);
            if (ec)
                vdbSize = 0;
        }

        if (format_ == "json") {
            json output;
            output["storage"]["total_documents"] = stats.totalObjects;
            output["storage"]["total_size"] = stats.totalBytes;
            output["storage"]["unique_blocks"] = stats.uniqueBlocks;
            if (vdbSize > 0)
                output["storage"]["vector_index_size"] = vdbSize;
            std::cout << output.dump(2) << std::endl;
        } else {
            // Retro, compact output
            std::cout << "== STORAGE (LOCAL) ==\n";
            std::cout << "DOCS : " << formatNumber(stats.totalObjects) << "\n";
            std::cout << "SIZE : " << formatSize(stats.totalBytes) << "\n";
            std::cout << "BLOCK: " << formatNumber(stats.uniqueBlocks) << "\n";
            if (vdbSize > 0) {
                std::cout << "VECDB: " << formatSize(vdbSize) << "\n";
            }
        }

        return Result<void>();
    }

    // ===================
    // Formatting Helpers
    // ===================

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
            oss << std::fixed << std::setprecision(2) << size << " " << units[unitIndex];
        }
        return oss.str();
    }

    std::string formatNumber(uint64_t num) const {
        std::string result = std::to_string(num);
        int insertPos = result.length() - 3;
        while (insertPos > 0) {
            result.insert(insertPos, ",");
            insertPos -= 3;
        }
        return result;
    }

    std::string formatPercentage(double value) const {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(1) << value << "%";
        return oss.str();
    }

    std::string formatUptime(long seconds) const {
        if (seconds < 60) {
            return std::to_string(seconds) + "s";
        } else if (seconds < 3600) {
            long minutes = seconds / 60;
            long remainingSeconds = seconds % 60;
            return std::to_string(minutes) + "m " + std::to_string(remainingSeconds) + "s";
        } else if (seconds < 86400) {
            long hours = seconds / 3600;
            long remainingMinutes = (seconds % 3600) / 60;
            return std::to_string(hours) + "h " + std::to_string(remainingMinutes) + "m";
        } else {
            long days = seconds / 86400;
            long remainingHours = (seconds % 86400) / 3600;
            return std::to_string(days) + "d " + std::to_string(remainingHours) + "h";
        }
    }

    void printSectionHeader(const std::string& title) const {
        std::cout << yams::cli::ui::section_header(title) << "\n\n";
    }

    // Stash readiness info for JSON output when applicable
    std::vector<std::string> pendingWaiting_;

    struct Row {
        std::string label;
        std::string value;
        std::string extra;
    };

    static size_t visibleWidth(const std::string& s) {
        size_t w = 0;
        bool inEsc = false;
        for (size_t i = 0; i < s.size(); ++i) {
            unsigned char c = s[i];
            if (c == '\x1b') {
                inEsc = true;
                continue;
            }
            if (inEsc) {
                if (c == 'm')
                    inEsc = false;
                continue;
            }
            ++w;
        }
        return w;
    }

    static std::string truncateToWidth(const std::string& s, size_t maxw) {
        if (maxw == 0)
            return "";
        if (visibleWidth(s) <= maxw)
            return s;
        std::string out;
        out.reserve(s.size());
        size_t w = 0;
        bool inEsc = false;
        for (size_t i = 0; i < s.size(); ++i) {
            unsigned char c = s[i];
            if (c == '\x1b') {
                inEsc = true;
                out.push_back(c);
                continue;
            }
            if (inEsc) {
                out.push_back(c);
                if (c == 'm')
                    inEsc = false;
                continue;
            }
            if (w + 1 > maxw - 1)
                break; // leave room for ellipsis
            out.push_back(c);
            ++w;
        }
        out += "…";
        return out;
    }

    static int detectTerminalWidth() {
        const char* cols = std::getenv("COLUMNS");
        if (cols) {
            try {
                return std::max(60, std::min(200, std::stoi(cols)));
            } catch (...) {
            }
        }
        return 100;
    }

    void renderRows(const std::vector<Row>& rows) const {
        if (rows.empty())
            return;
        int term = detectTerminalWidth();
        const int padding = 2; // spaces between columns
        size_t maxLabel = 8, maxValue = 8;
        for (const auto& r : rows) {
            maxLabel = std::max(maxLabel, visibleWidth(r.label));
            maxValue = std::max(maxValue, visibleWidth(r.value));
        }
        // Print each row, truncating as needed to fit terminal
        for (const auto& r : rows) {
            std::string l = r.label;
            std::string v = r.value;
            std::string e = r.extra;
            size_t lW = maxLabel, vW = maxValue;
            int base = 2 + (int)lW + padding + (int)vW; // indent + label + pad + value
            int need = base + (e.empty() ? 0 : padding + (int)visibleWidth(e));
            int overflow = need - term;
            if (overflow > 0) {
                // shrink extra first
                if (!e.empty()) {
                    size_t ew = visibleWidth(e);
                    size_t target = (overflow >= (int)ew) ? 0 : (ew - overflow);
                    e = truncateToWidth(e, target);
                    need = base + (e.empty() ? 0 : padding + (int)visibleWidth(e));
                    overflow = need - term;
                }
                // then value, then label, preserving minimum width 8
                if (overflow > 0 && vW > 8) {
                    size_t target = std::max((size_t)8, vW - (size_t)overflow);
                    v = truncateToWidth(v, target);
                    vW = visibleWidth(v);
                    need = 2 + (int)lW + padding + (int)vW +
                           (e.empty() ? 0 : padding + (int)visibleWidth(e));
                    overflow = need - term;
                }
                if (overflow > 0 && lW > 8) {
                    size_t target = std::max((size_t)8, lW - (size_t)overflow);
                    l = truncateToWidth(l, target);
                    lW = visibleWidth(l);
                }
            }
            std::cout << "  " << std::left << std::setw((int)lW) << l << std::string(padding, ' ')
                      << std::right << std::setw((int)vW) << v;
            if (!e.empty()) {
                std::cout << std::string(padding, ' ') << e;
            }
            std::cout << "\n";
        }
    }

    // ===================
    // Main Render Functions
    // ===================

    Result<void> renderText(const yams::daemon::GetStatsResponse& resp) {
        // Prefer daemon JSON payload if present
        uint64_t totalDocs = resp.totalDocuments;
        uint64_t totalSize = resp.totalSize;
        uint64_t indexedDocs = resp.indexedDocuments;
        uint64_t vecSize = resp.vectorIndexSize;
        uint64_t uniqueBlocks = 0;
        bool notReady = false;
        uint64_t p50 = 0, p95 = 0;
        auto itJson = resp.additionalStats.find("json");
        if (itJson != resp.additionalStats.end()) {
            try {
                auto j = json::parse(itJson->second);
                if (j.contains("total_documents"))
                    totalDocs = j["total_documents"].get<uint64_t>();
                if (j.contains("total_size"))
                    totalSize = j["total_size"].get<uint64_t>();
                if (j.contains("indexed_documents"))
                    indexedDocs = j["indexed_documents"].get<uint64_t>();
                if (j.contains("vector_index_size"))
                    vecSize = j["vector_index_size"].get<uint64_t>();
                if (j.contains("not_ready"))
                    notReady = j["not_ready"].get<bool>();
                if (j.contains("latency_p50_ms"))
                    p50 = j["latency_p50_ms"].get<uint64_t>();
                if (j.contains("latency_p95_ms"))
                    p95 = j["latency_p95_ms"].get<uint64_t>();
                if (j.contains("additional") && j["additional"].contains("unique_blocks")) {
                    // legacy, but keep if present
                    uniqueBlocks = std::stoull(j["additional"]["unique_blocks"].get<std::string>());
                }
            } catch (...) {
            }
        } else {
            auto itBlocks = resp.additionalStats.find("unique_blocks");
            if (itBlocks != resp.additionalStats.end()) {
                try {
                    uniqueBlocks = std::stoull(itBlocks->second);
                } catch (...) {
                }
            }
            auto itNotReady = resp.additionalStats.find("not_ready");
            if (itNotReady != resp.additionalStats.end())
                notReady = (itNotReady->second == "true");
        }

        if (verbose_) {
            // Default themed sections (no duplicate headers)
            renderSmartHealthSection(resp);
            renderRecommendationsSection(resp);
            // Inject latency metrics into Technical Details section via additional rows
            if (p50 > 0 || p95 > 0) {
                printSectionHeader("Technical Details");
                std::vector<yams::cli::ui::Row> rows;
                rows.push_back({"Latency p50", std::to_string(p50) + "ms", ""});
                rows.push_back({"Latency p95", std::to_string(p95) + "ms", ""});
                yams::cli::ui::render_rows(std::cout, rows);
                std::cout << "\n";
            }
            renderTechnicalDetailsSection(resp);
            renderServiceStatusSection(resp);
            // Knowledge Graph (local DB quick stats)
            try {
                auto db = cli_->getDatabase();
                if (db && db->isOpen()) {
                    auto countTable = [&](const char* sql) -> long long {
                        auto stR = db->prepare(sql);
                        if (!stR)
                            return -1;
                        auto st = std::move(stR).value();
                        auto step = st.step();
                        if (step && step.value())
                            return st.getInt64(0);
                        return -1;
                    };
                    long long nodes = countTable("SELECT COUNT(1) FROM kg_nodes");
                    long long edges = countTable("SELECT COUNT(1) FROM kg_edges");
                    long long aliases = countTable("SELECT COUNT(1) FROM kg_aliases");
                    long long embeddings = countTable("SELECT COUNT(1) FROM kg_node_embeddings");
                    long long entities = countTable("SELECT COUNT(1) FROM doc_entities");
                    std::cout << "\nKnowledge Graph:\n";
                    if ((entities <= 0 && nodes <= 0) &&
                        !(nodes >= 0 || edges >= 0 || aliases >= 0 || embeddings >= 0)) {
                        // Converted to structured recommendation (STATS_KG_EMPTY) using the
                        // existing recommendations builder path (renderRecommendationsSection)
                        // by storing a sentinel in additionalStats the builder can read.
                        // Add a synthetic flag so renderRecommendationsSection can treat it.
                        const_cast<yams::daemon::GetStatsResponse&>(resp)
                            .additionalStats["kg_empty"] = "true";
                        std::cout << "  Graph entities: 0 — run 'yams doctor repair --graph' to "
                                     "build from tags/metadata.\n";
                    } else {
                        if (nodes >= 0)
                            std::cout << "  Nodes:        " << nodes << "\n";
                        if (edges >= 0)
                            std::cout << "  Edges:        " << edges << "\n";
                        if (aliases >= 0)
                            std::cout << "  Aliases:      " << aliases << "\n";
                        if (embeddings >= 0)
                            std::cout << "  Embeddings:   " << embeddings << "\n";
                        if (entities >= 0)
                            std::cout << "  Doc entities: " << entities << "\n";
                    }
                }
            } catch (...) {
                // best-effort only
            }
        } else {
            // Strong system health summary first
            renderSmartHealthSection(resp);

            // Adaptive verbosity: include sections when service metrics or recommendations are
            // present
            auto hasServiceSpecific = [&](const yams::daemon::GetStatsResponse& r) {
                const auto& a = r.additionalStats;
                return a.count("service_contentstore") || a.count("service_metadatarepo") ||
                       a.count("service_searchexecutor") || a.count("service_embeddingservice") ||
                       a.count("daemon_version") || a.count("daemon_uptime");
            };
            auto hasRecommendationsLikely = [&](const yams::daemon::GetStatsResponse& r) {
                const auto& a = r.additionalStats;
                if (r.totalDocuments > 1000 && r.indexedDocuments < r.totalDocuments / 2)
                    return true;
                if (auto it = a.find("orphaned_blocks"); it != a.end()) {
                    try {
                        if (std::stoull(it->second) > 0ULL)
                            return true;
                    } catch (...) {
                    }
                }
                if (auto it = a.find("block_health"); it != a.end() && it->second == "critical")
                    return true;
                if (auto it = a.find("space_efficiency"); it != a.end()) {
                    try {
                        if (std::stod(it->second) < 50.0)
                            return true;
                    } catch (...) {
                    }
                }
                if (r.vectorIndexSize == 0 && r.totalDocuments > 0)
                    return true;
                return false;
            };

            if (hasRecommendationsLikely(resp)) {
                renderRecommendationsSection(resp);
            }
            if (hasServiceSpecific(resp)) {
                renderServiceStatusSection(resp);
            }

            // Retro, compact totals (always include)
            std::cout << "== STATS ==" << (notReady ? "  [INIT]" : "") << "\n";
            std::cout << "DOCS : " << formatNumber(totalDocs) << "\n";
            std::cout << "SIZE : " << formatSize(totalSize) << "\n";
            if (uniqueBlocks > 0) {
                std::cout << "BLOCK: " << formatNumber(uniqueBlocks) << "\n";
            }
            if (indexedDocs > 0 || totalDocs > 0) {
                double pct =
                    (totalDocs > 0) ? (static_cast<double>(indexedDocs) / totalDocs * 100.0) : 0.0;
                std::cout << "INDEX: " << formatNumber(indexedDocs) << " (" << std::fixed
                          << std::setprecision(1) << pct << "%)\n";
            }
            if (vecSize > 0) {
                std::cout << "VECDB: " << formatSize(vecSize) << "\n";
            }

            // Best-effort daemon backpressure snapshot removed for async-first simplicity
        }
        return Result<void>();
    }

    Result<void> renderJSON(const yams::daemon::GetStatsResponse& resp) {
        json output;

        // Storage overview
        output["storage"]["total_documents"] = resp.totalDocuments;
        output["storage"]["total_size"] = resp.totalSize;
        output["storage"]["indexed_documents"] = resp.indexedDocuments;
        output["storage"]["vector_index_size"] = resp.vectorIndexSize;
        output["storage"]["compression_ratio"] = resp.compressionRatio;

        // File types if available
        if (!resp.documentsByType.empty()) {
            json types = json::object();
            for (const auto& [type, count] : resp.documentsByType) {
                types[type] = count;
            }
            output["file_types"] = types;
        }

        // All additional stats organized by category
        if (!resp.additionalStats.empty()) {
            output["additional"] = resp.additionalStats;
        }

        // Client-side diagnostics for quick validation
        output["client"] = {
            {"ipc_pool_enabled", true},
            {"daemon_socket", yams::daemon::DaemonClient::resolveSocketPathConfigFirst().string()},
            {"version", YAMS_VERSION_STRING}};

        if (!pendingWaiting_.empty()) {
            output["waiting_on"] = pendingWaiting_;
        }

        // Daemon status supplement removed

        std::cout << output.dump(2) << std::endl;
        return Result<void>();
    }

    // ===================
    // Section Renderers
    // ===================

    void renderStorageOverview(const yams::daemon::GetStatsResponse& resp) {
        std::cout << "\nStorage Overview:\n";
        std::cout << "  Documents:  " << formatNumber(resp.totalDocuments) << "\n";
        std::cout << "  Size:       " << formatSize(resp.totalSize) << "\n";

        // Enhanced unique blocks display with ratio
        auto blocksIt = resp.additionalStats.find("unique_blocks");
        if (blocksIt != resp.additionalStats.end()) {
            uint64_t uniqueBlocks = std::stoull(blocksIt->second);
            std::cout << "  Blocks:     " << formatNumber(uniqueBlocks);

            // Show block to document ratio
            auto ratioIt = resp.additionalStats.find("block_to_doc_ratio");
            if (ratioIt != resp.additionalStats.end()) {
                double ratio = std::stod(ratioIt->second);
                if (ratio > 1.2) {
                    std::cout << " (~" << std::fixed << std::setprecision(1) << ratio
                              << " per doc)";
                }
            }
            std::cout << "\n";
        }

        // Space efficiency
        auto effIt = resp.additionalStats.find("space_efficiency");
        if (effIt != resp.additionalStats.end()) {
            double efficiency = std::stod(effIt->second);
            std::cout << "  Efficiency: " << formatPercentage(efficiency);

            // Show orphaned space if present
            auto orphanIt = resp.additionalStats.find("orphaned_bytes");
            if (orphanIt != resp.additionalStats.end()) {
                uint64_t orphanedBytes = std::stoull(orphanIt->second);
                if (orphanedBytes > 0) {
                    double orphanedPercent = 100.0 - efficiency;
                    std::cout << " (" << formatPercentage(orphanedPercent) << " orphaned)";
                }
            }
            std::cout << "\n";
        }

        double idxPct = resp.totalDocuments > 0
                            ? (double)resp.indexedDocuments / resp.totalDocuments * 100.0
                            : 0.0;
        std::cout << "  Indexed:    " << formatNumber(resp.indexedDocuments) << " ("
                  << formatPercentage(idxPct) << ")\n";
    }

    void renderSmartHealthSection(const yams::daemon::GetStatsResponse& resp) {
        std::cout << "\nSystem Health:\n";

        // One-line services summary (✓/⚠/✗)
        try {
            json j;
            bool haveJson = false;
            if (auto itj = resp.additionalStats.find("json"); itj != resp.additionalStats.end()) {
                j = json::parse(itj->second);
                haveJson = true;
            }
            auto gs = [&](const std::string& key, const std::string& good, const std::string& bad,
                          const std::string& unk) -> std::string {
                auto it = resp.additionalStats.find(key);
                std::string v = (it != resp.additionalStats.end()) ? it->second : std::string{};
                if (v.empty() && haveJson && j.contains("services") &&
                    j["services"].contains(key.substr(8))) {
                    v = j["services"][key.substr(8)].get<std::string>();
                }
                if (v == good)
                    return "✓";
                if (v == bad || v == "error" || v == "unavailable" || v == "not_initialized")
                    return "⚠";
                return unk; // unknown
            };
            std::string content = gs("service_contentstore", "running", "unavailable", "⚠");
            std::string repo = gs("service_metadatarepo", "running", "unavailable", "⚠");
            std::string search;
            {
                auto it = resp.additionalStats.find("service_searchexecutor");
                std::string v = (it != resp.additionalStats.end()) ? it->second : std::string{};
                if (v.empty() && haveJson && j.contains("services") &&
                    j["services"].contains("searchexecutor"))
                    v = j["services"]["searchexecutor"].get<std::string>();
                if (v == "available")
                    search = "✓";
                else {
                    std::string reason;
                    auto r = resp.additionalStats.find("searchexecutor_reason");
                    if (r != resp.additionalStats.end())
                        reason = r->second;
                    else if (haveJson && j.contains("services"))
                        reason = j["services"].value("searchexecutor_reason", "");
                    search = reason.empty() ? "⚠" : ("⚠ (" + reason + ")");
                }
            }
            std::string models;
            {
                std::string svc;
                auto it = resp.additionalStats.find("service_embeddingservice");
                if (it != resp.additionalStats.end())
                    svc = it->second;
                else if (haveJson && j.contains("services") &&
                         j["services"].contains("embeddingservice"))
                    svc = j["services"]["embeddingservice"].get<std::string>();
                std::string count = "";
                if (auto ml = resp.additionalStats.find("onnx_models_loaded");
                    ml != resp.additionalStats.end())
                    count = ml->second;
                else if (haveJson && j.contains("services") &&
                         j["services"].contains("onnx_models_loaded")) {
                    try {
                        if (j["services"]["onnx_models_loaded"].is_string())
                            count = j["services"]["onnx_models_loaded"].get<std::string>();
                        else if (j["services"]["onnx_models_loaded"].is_number_integer())
                            count = std::to_string(j["services"]["onnx_models_loaded"].get<int>());
                        else
                            count = j["services"]["onnx_models_loaded"].dump();
                    } catch (...) {
                        count = "unknown";
                    }
                }
                if (svc == "available") {
                    if (count == "0" || count == "\"0\"")
                        models = "⚠ (0)";
                    else
                        models = "✓";
                } else if (svc == "unavailable" || svc == "error") {
                    models = "⚠";
                } else {
                    models = "⚠";
                }
            }
            std::cout << "  Services:   " << content << " Content | " << repo << " Repo | "
                      << search << " Search | " << models << " Models\n";
        } catch (...) {
        }

        // Overall status
        auto healthStatus = resp.additionalStats.find("health_status");
        std::string status =
            (healthStatus != resp.additionalStats.end()) ? healthStatus->second : "unknown";
        std::string statusDisplay = (status == "healthy") ? "Healthy" : "Needs Attention";
        std::cout << "  Status:     " << statusDisplay << "\n";

        // Storage health with warnings
        auto orphanedBlocks = resp.additionalStats.find("orphaned_blocks");
        std::string storageStatus;
        if (orphanedBlocks != resp.additionalStats.end()) {
            uint64_t orphaned = std::stoull(orphanedBlocks->second);
            if (orphaned > 0) {
                storageStatus = "WARNING - " + formatSize(resp.totalSize) + " in " +
                                formatNumber(resp.totalDocuments) + " documents";
            } else {
                storageStatus = "OK - " + formatSize(resp.totalSize) + " in " +
                                formatNumber(resp.totalDocuments) + " documents";
            }
        } else {
            storageStatus = "OK - " + formatSize(resp.totalSize) + " in " +
                            formatNumber(resp.totalDocuments) + " documents";
        }
        std::cout << "  Storage:    " << storageStatus << "\n";

        // Block integrity status
        auto blockHealth = resp.additionalStats.find("block_health");
        if (blockHealth != resp.additionalStats.end()) {
            std::string blockStatus;
            auto orphanIt = resp.additionalStats.find("orphaned_blocks");
            if (orphanIt != resp.additionalStats.end()) {
                uint64_t orphaned = std::stoull(orphanIt->second);
                if (blockHealth->second == "critical") {
                    blockStatus =
                        "CRITICAL - " + formatNumber(orphaned) + " orphaned blocks detected";
                } else if (blockHealth->second == "warning") {
                    blockStatus =
                        "WARNING - " + formatNumber(orphaned) + " orphaned blocks detected";
                } else {
                    blockStatus = "Healthy";
                }
            } else {
                blockStatus = blockHealth->second == "healthy" ? "Healthy" : "Needs Attention";
            }
            std::cout << "  Blocks:     " << blockStatus << "\n";
        }

        // Indexing health with actionable status
        double indexedPercent = resp.totalDocuments > 0
                                    ? (double)resp.indexedDocuments / resp.totalDocuments * 100
                                    : 0.0;
        std::string indexStatus;
        if (indexedPercent == 0 && resp.totalDocuments > 0) {
            indexStatus = "ATTENTION - " + formatNumber(resp.indexedDocuments) +
                          " documents indexed (" + formatPercentage(indexedPercent) + ")";
        } else if (indexedPercent < 50) {
            indexStatus = "PARTIAL - " + formatNumber(resp.indexedDocuments) +
                          " documents indexed (" + formatPercentage(indexedPercent) + ")";
        } else {
            indexStatus = "OK - " + formatNumber(resp.indexedDocuments) + " documents indexed (" +
                          formatPercentage(indexedPercent) + ")";
        }
        std::cout << "  Indexing:   " << indexStatus << "\n";

        // Vector database status with optional row count in verbose mode
        std::string vectorStatus = resp.vectorIndexSize > 0
                                       ? "Initialized - " + formatSize(resp.vectorIndexSize)
                                       : "Not initialized";
        if (verbose_) {
            auto rowsIt = resp.additionalStats.find("vector_rows");
            if (rowsIt != resp.additionalStats.end()) {
                vectorStatus += " (" +
                                formatNumber(static_cast<uint64_t>(std::stoull(rowsIt->second))) +
                                " rows)";
            }
        }
        std::cout << "  Vector DB:  " << vectorStatus << "\n";

        // Daemon status if available
        auto uptimeIt = resp.additionalStats.find("daemon_uptime");
        if (uptimeIt != resp.additionalStats.end()) {
            long uptime = std::stoll(uptimeIt->second);
            std::string daemonStatus = "Running (uptime: " + formatUptime(uptime) + ")";
            std::cout << "  Daemon:     " << daemonStatus << "\n";
        }
    }

    void renderRecommendationsSection(const yams::daemon::GetStatsResponse& resp) {
        yams::cli::RecommendationBuilder builder;
        // Knowledge graph emptiness (direct DB probe so ordering of other sections doesn't matter)
        try {
            auto db = cli_->getDatabase();
            if (db && db->isOpen()) {
                auto count = [&](const char* sql) -> long long {
                    auto stR = db->prepare(sql);
                    if (!stR)
                        return -1;
                    auto st = std::move(stR).value();
                    auto step = st.step();
                    if (step && step.value())
                        return st.getInt64(0);
                    return -1;
                };
                long long nodes = count("SELECT COUNT(1) FROM kg_nodes");
                long long entities = count("SELECT COUNT(1) FROM doc_entities");
                if ((nodes == 0 && entities == 0) || (nodes <= 0 && entities <= 0)) {
                    builder.warning("STATS_KG_EMPTY",
                                    "Knowledge graph is empty. Index content (yams add) or repair "
                                    "graph to enable relationship traversal.",
                                    "Run 'yams doctor repair --graph'");
                }
            }
        } catch (...) {
            // best-effort only
        }
        // Orphaned blocks
        auto orphanedBlocksIt = resp.additionalStats.find("orphaned_blocks");
        auto orphanedBytesIt = resp.additionalStats.find("orphaned_bytes");
        if (orphanedBlocksIt != resp.additionalStats.end() &&
            orphanedBytesIt != resp.additionalStats.end()) {
            uint64_t orphanedBlocks = 0;
            uint64_t orphanedBytes = 0;
            try {
                orphanedBlocks = std::stoull(orphanedBlocksIt->second);
            } catch (...) {
            }
            try {
                orphanedBytes = std::stoull(orphanedBytesIt->second);
            } catch (...) {
            }
            if (orphanedBlocks > 0) {
                std::string msg = "Run 'yams repair --orphans' to clean " +
                                  formatNumber(orphanedBlocks) + " orphaned blocks";
                if (orphanedBytes > 1024ULL * 1024ULL) {
                    msg += " (~" + formatSize(orphanedBytes) + " recoverable)";
                }
                builder.warning("STATS_ORPHANED_BLOCKS", msg, "Dead data occupies space");
            }
        }
        // Block health critical
        if (auto blockHealthIt = resp.additionalStats.find("block_health");
            blockHealthIt != resp.additionalStats.end()) {
            if (blockHealthIt->second == "critical") {
                builder.warning("STATS_BLOCK_INTEGRITY_CRITICAL",
                                "Storage has critical block integrity issues - repair urgently",
                                "Run 'yams repair --integrity' soon");
            }
        }
        // Embedding coverage
        bool needEmbeddingsRec = false;
        if (resp.totalDocuments > 0) {
            if (resp.indexedDocuments == 0)
                needEmbeddingsRec = true;
            else if (resp.totalDocuments > 1000 &&
                     resp.indexedDocuments < (resp.totalDocuments / 2))
                needEmbeddingsRec = true;
        }
        if (needEmbeddingsRec) {
            builder.warning("STATS_EMBEDDINGS_MISSING",
                            "Run 'yams repair --embeddings' to generate missing embeddings",
                            "Semantic search requires vector coverage");
        }
        // Space efficiency
        if (auto effIt = resp.additionalStats.find("space_efficiency");
            effIt != resp.additionalStats.end()) {
            try {
                double efficiency = std::stod(effIt->second);
                if (efficiency < 50.0) {
                    builder.info(
                        "STATS_LOW_SPACE_EFFICIENCY",
                        "Consider running 'yams repair --optimize' to improve storage efficiency",
                        "Current space efficiency " + effIt->second + "%");
                }
            } catch (...) {
            }
        }
        // Vector DB initialization hint
        if (resp.vectorIndexSize == 0 && resp.totalDocuments > 0) {
            builder.info("STATS_VECTOR_DB_PENDING",
                         "Vector database will be created automatically on first search");
        }
        // Output
        if (builder.empty()) {
            std::cout << "\nRecommendations:\n  ✓ System is optimally configured\n";
        } else {
            yams::cli::printRecommendationsText(builder, std::cout);
        }
    }

    void renderTechnicalDetailsSection(const yams::daemon::GetStatsResponse& resp) {
        std::cout << "\nTechnical Details:\n";

        // Deduplication info
        auto uniqueBlocks = resp.additionalStats.find("unique_blocks");
        if (uniqueBlocks != resp.additionalStats.end()) {
            std::cout << "  Deduplication:  " << resp.compressionRatio << ":1 ratio, "
                      << formatNumber(std::stoull(uniqueBlocks->second)) << " unique blocks\n";
        }

        // Performance info
        auto storeOps = resp.additionalStats.find("performance_store_ops");
        if (storeOps != resp.additionalStats.end()) {
            uint64_t ops = std::stoull(storeOps->second);
            std::string perfInfo = ops > 0 ? formatNumber(ops) + " operations recorded"
                                           : "Fresh daemon (0 operations recorded)";
            std::cout << "  Performance:    " << perfInfo << "\n";
        }

        // Compression info
        std::cout << "  Compression:    " << resp.compressionRatio
                  << ":1 ratio using content-addressed storage\n";

        // Streaming metrics if available
        auto ttfb = resp.additionalStats.find("stream_ttfb_avg_ms");
        auto streams = resp.additionalStats.find("stream_total_streams");
        auto batches = resp.additionalStats.find("stream_batches_emitted");
        auto keepalives = resp.additionalStats.find("stream_keepalives");
        if (ttfb != resp.additionalStats.end() || streams != resp.additionalStats.end() ||
            batches != resp.additionalStats.end() || keepalives != resp.additionalStats.end()) {
            std::cout << "  Streaming:\n";
            if (ttfb != resp.additionalStats.end())
                std::cout << "    TTFB (avg):     " << ttfb->second << " ms\n";
            if (streams != resp.additionalStats.end())
                std::cout << "    Streams:        " << streams->second << "\n";
            if (batches != resp.additionalStats.end())
                std::cout << "    Batches:        " << batches->second << "\n";
            if (keepalives != resp.additionalStats.end())
                std::cout << "    Keepalives:     " << keepalives->second << "\n";
        }

        // Resource pools (IPC)
        auto poolSize = resp.additionalStats.find("pool_ipc_size");
        auto poolResizes = resp.additionalStats.find("pool_ipc_resizes");
        auto poolRejected = resp.additionalStats.find("pool_ipc_rejected_on_cap");
        auto poolThrottled = resp.additionalStats.find("pool_ipc_throttled_on_cooldown");
        if (poolSize != resp.additionalStats.end() || poolResizes != resp.additionalStats.end() ||
            poolRejected != resp.additionalStats.end() ||
            poolThrottled != resp.additionalStats.end()) {
            std::cout << "  Resource Pools (IPC):\n";
            if (poolSize != resp.additionalStats.end())
                std::cout << "    Size:           " << poolSize->second << "\n";
            if (poolResizes != resp.additionalStats.end())
                std::cout << "    Resizes:        " << poolResizes->second << "\n";
            if (poolRejected != resp.additionalStats.end())
                std::cout << "    Rejected@Cap:   " << poolRejected->second << "\n";
            if (poolThrottled != resp.additionalStats.end())
                std::cout << "    Throttled:      " << poolThrottled->second << "\n";
        }

        // Embedding/model usage (when available)
        auto et = resp.additionalStats.find("embed_total_texts");
        auto tok = resp.additionalStats.find("embed_total_tokens");
        auto ttot = resp.additionalStats.find("embed_total_time_ms");
        auto tavg = resp.additionalStats.find("embed_avg_time_ms");
        auto ttps = resp.additionalStats.find("embed_throughput_txtps");
        auto tkps = resp.additionalStats.find("embed_throughput_tokps");
        auto mname = resp.additionalStats.find("embed_model_name");
        auto edim = resp.additionalStats.find("embed_dim");
        if (et != resp.additionalStats.end() || tok != resp.additionalStats.end() ||
            ttot != resp.additionalStats.end() || tavg != resp.additionalStats.end() ||
            ttps != resp.additionalStats.end() || tkps != resp.additionalStats.end() ||
            mname != resp.additionalStats.end() || edim != resp.additionalStats.end()) {
            std::cout << "  Embeddings:\n";
            if (mname != resp.additionalStats.end())
                std::cout << "    Model:          " << mname->second
                          << (edim != resp.additionalStats.end() ? " (dim " + edim->second + ")"
                                                                 : "")
                          << "\n";
            if (et != resp.additionalStats.end())
                std::cout << "    Texts:          " << et->second << "\n";
            if (tok != resp.additionalStats.end())
                std::cout << "    Tokens:         " << tok->second << "\n";
            if (ttot != resp.additionalStats.end())
                std::cout << "    Time (total):   " << ttot->second << " ms\n";
            if (tavg != resp.additionalStats.end())
                std::cout << "    Time (avg):     " << tavg->second << " ms\n";
            if (ttps != resp.additionalStats.end())
                std::cout << "    Throughput:     " << ttps->second << " texts/s\n";
            if (tkps != resp.additionalStats.end())
                std::cout << "                     " << tkps->second << " tokens/s\n";
        }
    }

    void renderServiceStatusSection(const yams::daemon::GetStatsResponse& resp) {
        std::cout << "\nService Status:\n";

        // Parse daemon JSON for service keys when proto didn't carry them
        json j;
        bool haveJson = false;
        if (auto itj = resp.additionalStats.find("json"); itj != resp.additionalStats.end()) {
            try {
                j = json::parse(itj->second);
                haveJson = true;
            } catch (...) {
                haveJson = false;
            }
        }

        // Show daemon version and uptime
        auto versionIt = resp.additionalStats.find("daemon_version");
        auto uptimeIt = resp.additionalStats.find("daemon_uptime");
        if (versionIt != resp.additionalStats.end() && uptimeIt != resp.additionalStats.end()) {
            std::string uptimeStr = formatUptime(std::stoll(uptimeIt->second));
            std::cout << "  Daemon:         " << versionIt->second << " (uptime: " << uptimeStr
                      << ")\n";
        }

        // ContentStore status
        auto contentStoreIt = resp.additionalStats.find("service_contentstore");
        std::string contentStoreStatus = "Unknown";
        if (contentStoreIt != resp.additionalStats.end()) {
            if (contentStoreIt->second == "running") {
                contentStoreStatus =
                    "✓ Running (" + formatNumber(resp.totalDocuments) + " objects)";
            } else {
                contentStoreStatus = "✗ " + contentStoreIt->second;
            }
        } else {
            contentStoreStatus = "✓ Running (" + formatNumber(resp.totalDocuments) + " objects)";
        }
        std::cout << "  ContentStore:   " << contentStoreStatus << "\n";

        // MetadataRepo status
        auto metadataIt = resp.additionalStats.find("service_metadatarepo");
        std::string metadataStatus = "Unknown";
        if (metadataIt != resp.additionalStats.end()) {
            if (metadataIt->second == "running") {
                metadataStatus = "✓ Running (" + formatNumber(resp.indexedDocuments) + " indexed)";
            } else {
                metadataStatus = "✗ " + metadataIt->second;
            }
        } else {
            metadataStatus = "✓ Running (" + formatNumber(resp.indexedDocuments) + " indexed)";
        }
        std::cout << "  MetadataRepo:   " << metadataStatus << "\n";

        // Configured data directory (helps debug path mismatches)
        auto dataDirIt = resp.additionalStats.find("data_dir");
        if (dataDirIt != resp.additionalStats.end() && !dataDirIt->second.empty()) {
            std::cout << "  Data Dir:       " << dataDirIt->second << "\n";
        }

        // SearchExecutor status
        auto searchIt = resp.additionalStats.find("service_searchexecutor");
        std::string searchStatus;
        if (searchIt != resp.additionalStats.end()) {
            if (searchIt->second == "available") {
                searchStatus = "✓ Available";
            } else {
                auto reasonIt = resp.additionalStats.find("searchexecutor_reason");
                searchStatus = reasonIt != resp.additionalStats.end()
                                   ? "⚠ Unavailable (" + reasonIt->second + ")"
                                   : std::string{"⚠ Unavailable"};
            }
        } else if (haveJson && j.contains("services") && j["services"].contains("searchexecutor")) {
            auto st = j["services"]["searchexecutor"].get<std::string>();
            if (st == "available")
                searchStatus = "✓ Available";
            else {
                std::string reason = j["services"].value("searchexecutor_reason", "");
                searchStatus =
                    reason.empty() ? "⚠ Unavailable" : ("⚠ Unavailable (" + reason + ")");
            }
        } else {
            searchStatus = "⚠ Unknown";
        }
        std::cout << "  SearchExecutor: " << searchStatus << "\n";

        // Auto-repair visibility (compact summary line + details)
        auto rq = resp.additionalStats.find("repair_queue_depth");
        auto rb = resp.additionalStats.find("repair_batches_attempted");
        auto rg = resp.additionalStats.find("repair_embeddings_generated");
        auto rsk = resp.additionalStats.find("repair_embeddings_skipped");
        auto rfl = resp.additionalStats.find("repair_failed_operations");
        // Readiness progress percentage for vector index (reused as repair progress proxy)
        int repairProgressPct = -1;
        try {
            // When status JSON is present in additionalStats["json"], use it to read progress
            if (haveJson && j.contains("progress") && j["progress"].contains("vector_index")) {
                repairProgressPct = j["progress"]["vector_index"].get<int>();
            }
        } catch (...) {
        }
        if (rq != resp.additionalStats.end() || rb != resp.additionalStats.end() ||
            rg != resp.additionalStats.end() || rsk != resp.additionalStats.end() ||
            rfl != resp.additionalStats.end() || repairProgressPct >= 0) {
            std::ostringstream line;
            line << "  Repair:        ";
            if (repairProgressPct >= 0)
                line << repairProgressPct << "%  | ";
            if (rq != resp.additionalStats.end())
                line << "queue=" << rq->second << " ";
            if (rb != resp.additionalStats.end())
                line << "batches=" << rb->second << " ";
            if (rg != resp.additionalStats.end())
                line << "gen=" << rg->second << " ";
            if (rsk != resp.additionalStats.end())
                line << "skip=" << rsk->second << " ";
            if (rfl != resp.additionalStats.end())
                line << "fail=" << rfl->second;
            std::cout << line.str() << "\n";
        }

        // Client/IPC self-check diagnostics
        std::cout << "  Client:         " << YAMS_VERSION_STRING << "\n";
        std::cout << "  Client IPC:     Pool enabled, "
                  << yams::daemon::DaemonClient::resolveSocketPathConfigFirst().string() << "\n";

        // VectorDatabase detailed status
        auto vectorServiceIt = resp.additionalStats.find("service_vectordb");
        std::string vectorStatus;
        if (vectorServiceIt != resp.additionalStats.end()) {
            if (vectorServiceIt->second == "initialized") {
                vectorStatus = "✓ Initialized";
                auto vectorDocsIt = resp.additionalStats.find("vectordb_documents");
                if (vectorDocsIt != resp.additionalStats.end()) {
                    vectorStatus +=
                        " (" + formatNumber(std::stoull(vectorDocsIt->second)) + " vectors)";
                }
            } else if (vectorServiceIt->second == "not_initialized") {
                vectorStatus = "⚠ Not initialized";
            } else if (vectorServiceIt->second == "error") {
                vectorStatus = "✗ Error";
                auto errorIt = resp.additionalStats.find("vectordb_error");
                if (errorIt != resp.additionalStats.end()) {
                    yams::cli::ui::render_rows(std::cout, {{"Vector DB", "✗ Error", ""}});
                    yams::cli::ui::render_rows(std::cout, {{"", "", errorIt->second}});
                }
            } else {
                vectorStatus = "⚠ " + vectorServiceIt->second;
            }
        } else {
            vectorStatus = resp.vectorIndexSize > 0 ? "✓ Initialized" : "⚠ Not initialized";
        }

        if (vectorServiceIt == resp.additionalStats.end() || vectorServiceIt->second != "error") {
            std::cout << "  Vector DB:      " << vectorStatus << "\n";
            auto vdbPathIt = resp.additionalStats.find("vectordb_path");
            if (vdbPathIt != resp.additionalStats.end() && !vdbPathIt->second.empty()) {
                std::cout << "                  " << vdbPathIt->second << "\n";
            }
            auto vrows = resp.additionalStats.find("vector_rows");
            if (vrows != resp.additionalStats.end()) {
                std::cout << "                  Rows: " << formatNumber(std::stoull(vrows->second))
                          << "\n";
            }
            if (resp.vectorIndexSize == 0) {
                std::cout << "                  Will initialize on first search\n";
            }
        }

        // ONNX / Embeddings Provider status
        auto embeddingServiceIt = resp.additionalStats.find("service_embeddingservice");
        auto modelsLoadedIt = resp.additionalStats.find("onnx_models_loaded");
        auto modelsErrorIt = resp.additionalStats.find("onnx_models_error");
        auto onnxRtIt = resp.additionalStats.find("onnx_runtime");

        // Determine service state from proto or JSON
        std::string embeddingSvcState;
        if (embeddingServiceIt != resp.additionalStats.end()) {
            embeddingSvcState = embeddingServiceIt->second;
        } else if (haveJson && j.contains("services") &&
                   j["services"].contains("embeddingservice")) {
            embeddingSvcState = j["services"]["embeddingservice"].get<std::string>();
        }

        if (!embeddingSvcState.empty()) {
            if (embeddingSvcState == "available") {
                std::string modelsCount =
                    modelsLoadedIt != resp.additionalStats.end()
                        ? modelsLoadedIt->second
                        : (haveJson && j.contains("services") &&
                                   j["services"].contains("onnx_models_loaded")
                               ? (j["services"]["onnx_models_loaded"].is_string()
                                      ? j["services"]["onnx_models_loaded"].get<std::string>()
                                      : (j["services"]["onnx_models_loaded"].is_number_integer()
                                             ? std::to_string(
                                                   j["services"]["onnx_models_loaded"].get<int>())
                                             : j["services"]["onnx_models_loaded"].dump()))
                               : std::string{"0"});
                if (modelsCount == "0") {
                    std::cout << "  ONNX Models:    ⚠ No models loaded (0)\n";
                    std::cout << "                  Run 'yams model download --apply-config "
                                 "all-MiniLM-L6-v2'\n";
                } else {
                    std::cout << "  ONNX Models:    ✓ " << modelsCount << " models loaded\n";
                }
                // Show preferred model hints when available
                if (auto itpm = resp.additionalStats.find("preferred_model");
                    itpm != resp.additionalStats.end()) {
                    std::cout << "                  Preferred: " << itpm->second;
                    if (auto itpath = resp.additionalStats.find("preferred_model_path_exists");
                        itpath != resp.additionalStats.end()) {
                        std::cout << " (" << (itpath->second == "true" ? "found" : "missing")
                                  << ")";
                    }
                    std::cout << "\n";
                }
            } else if (embeddingSvcState == "unavailable") {
                // Distinguish runtime-disabled vs. just not yet adopted/loaded
                if (onnxRtIt != resp.additionalStats.end() && onnxRtIt->second == "disabled") {
                    std::cout << "  ONNX Models:    ✗ Runtime not compiled in\n";
                    std::cout << "                  Rebuild with ONNX or install ONNX-enabled "
                                 "binaries.\n";
                } else {
                    std::cout << "  ONNX Models:    ⚠ Service unavailable\n";
                }
                if (modelsErrorIt != resp.additionalStats.end()) {
                    std::cout << "                  " << modelsErrorIt->second << "\n";
                }
                // Print preferred model hint even when unavailable
                if (auto itpm = resp.additionalStats.find("preferred_model");
                    itpm != resp.additionalStats.end()) {
                    std::cout << "                  Preferred: " << itpm->second;
                    if (auto itpath = resp.additionalStats.find("preferred_model_path_exists");
                        itpath != resp.additionalStats.end()) {
                        std::cout << " (" << (itpath->second == "true" ? "found" : "missing")
                                  << ")";
                    }
                    std::cout << "\n";
                }
            } else if (embeddingSvcState == "error") {
                std::cout << "  ONNX Models:    ✗ Service error\n";
                if (modelsErrorIt != resp.additionalStats.end()) {
                    std::cout << "                  " << modelsErrorIt->second << "\n";
                }
            }
        } else {
            std::cout << "  ONNX Models:    ⚠ Status unknown\n";
        }

        // Embedding Service status (duplicated view for clarity)
        if (embeddingServiceIt != resp.additionalStats.end()) {
            std::string embeddingStatus;
            if (embeddingServiceIt->second == "available") {
                std::string modelsCount =
                    modelsLoadedIt != resp.additionalStats.end() ? modelsLoadedIt->second : "0";
                embeddingStatus = modelsCount == "0" ? "⚠ Unavailable (no models)" : "✓ Available";
            } else {
                embeddingStatus = "⚠ Unavailable (" + embeddingServiceIt->second + ")";
            }
            std::cout << "  Embedding Svc:  " << embeddingStatus << "\n";

            // Show dynamic batching metrics when available
            auto effTok = resp.additionalStats.find("embed_batch_effective_tokens");
            auto avgDocs = resp.additionalStats.find("embed_batch_recent_avg_docs");
            auto succ = resp.additionalStats.find("embed_batch_successes");
            auto fails = resp.additionalStats.find("embed_batch_failures");
            auto backs = resp.additionalStats.find("embed_batch_backoffs");
            if (effTok != resp.additionalStats.end() || avgDocs != resp.additionalStats.end() ||
                succ != resp.additionalStats.end() || fails != resp.additionalStats.end() ||
                backs != resp.additionalStats.end()) {
                std::ostringstream bl;
                bl << "  Batching:       ";
                if (effTok != resp.additionalStats.end())
                    bl << "budget_tokens=" << effTok->second << " ";
                if (avgDocs != resp.additionalStats.end())
                    bl << "avg_docs=" << avgDocs->second << " ";
                if (succ != resp.additionalStats.end())
                    bl << "ok=" << succ->second << " ";
                if (fails != resp.additionalStats.end())
                    bl << "fail=" << fails->second << " ";
                if (backs != resp.additionalStats.end())
                    bl << "backoff=" << backs->second;
                std::cout << bl.str() << "\n";
            }
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string format_;
    bool localOnly_ = false;   // Force local-only mode
    bool vectorsMode_ = false; // Subcommand: vectors mode
    bool watchMode_ = false;   // Live refresh mode
    bool verbose_ = false;     // Show comprehensive technical details
    bool forceColor_ = false;  // --color flag
    bool noColor_ = false;     // --no-color flag
    bool daemonOnly_ = false;  // Force daemon-only, disable local fallback
};

// Factory function
std::unique_ptr<ICommand> createStatsCommand() {
    return std::make_unique<StatsCommand>();
}

} // namespace yams::cli
