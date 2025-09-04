#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/async_bridge.h>
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
        cmd->add_flag("--daemon-only", daemonOnly_,
                      "Use daemon results only (no local fallback)");

        // Nested subcommand: yams stats vectors
        auto* vectors = cmd->add_subcommand("vectors", "Show vector database statistics");
        vectors->add_option("--format", format_, "Output format: text, json")
            ->default_val("text")
            ->check(CLI::IsMember({"text", "json"}));
        vectors->add_flag("-v,--verbose", verbose_, "Enable verbose output");
        vectors->add_flag("--daemon-only", daemonOnly_,
                          "Use daemon results only (no local fallback)");
        vectors->callback([this]() {
            auto result = executeVectors();
            if (!result) {
                spdlog::error("Stats vectors failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });

        // System help: explain fields/metrics shown by stats
        auto* helpCmd = cmd->add_subcommand("help", "Show system metrics help for stats output");
        helpCmd->callback([this]() {
            renderSystemHelp();
        });

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Stats failed: {}", result.error().message);
                throw CLI::RuntimeError(1);
            }
        });
    }

    Result<void> execute() override {
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
            // Heuristic fallback only when not forcing daemon-only
            if (!localOnly_ && !daemonOnly_ && resp.totalDocuments == 0 && resp.totalSize == 0) {
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
            cfg.requestTimeout = std::chrono::milliseconds(30000);
            yams::daemon::DaemonClient client(cfg);

            auto result = run_sync(client.call(dreq), std::chrono::seconds(30));
            if (result) {
                return render(result.value());
            }
            // On failure, choose fallback only when not daemon-only
            return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats unavailable"} : fallback();
        } catch (...) {
            return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats exception"} : fallback();
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
        std::cout << "  - YAMS_LIST_MODE, YAMS_GREP_MODE, YAMS_RETRIEVAL_MODE: hot_only|cold_only|auto\n\n";
        std::cout << "Per-document Overrides:\n";
        std::cout << "  - Tag/metadata force_cold=true: Force cold path for a document\n\n";
        std::cout << "Tips:\n";
        std::cout << "  - Use 'yams stats --format json' for scriptable output\n";
        std::cout << "  - Use 'yams stats vectors' to focus on embedding coverage\n";
    }
    Result<void> executeVectors() {
        // For now, vectors uses the same stats request but focuses on vector data
        yams::daemon::GetStatsRequest dreq;
        dreq.detailed = verbose_;
        dreq.includeCache = false; // Focus on vector-specific data

        // Render lambda for results - focus on vector-related data
        auto render = [&](const yams::daemon::GetStatsResponse& resp) -> Result<void> {
            if (format_ == "json") {
                json output;
                output["vectorDatabase"] = {{"indexedDocuments", resp.indexedDocuments},
                                            {"vectorIndexSize", resp.vectorIndexSize},
                                            {"totalDocuments", resp.totalDocuments}};

                // Extract vector-related additional stats
                json vectorStats;
                for (const auto& [key, value] : resp.additionalStats) {
                    if (key.find("vector") != std::string::npos ||
                        key.find("embedding") != std::string::npos) {
                        vectorStats[key] = value;
                    }
                }
                if (!vectorStats.empty()) {
                    output["vectorDetails"] = vectorStats;
                }

                std::cout << output.dump(2) << std::endl;
            } else {
                std::cout << "Vector Database Statistics\n";
                std::cout << "==========================\n";
                std::cout << "Indexed Documents: " << resp.indexedDocuments << "\n";
                std::cout << "Vector Index Size: " << formatSize(resp.vectorIndexSize) << "\n";
                std::cout << "Total Documents: " << resp.totalDocuments << "\n";

                if (resp.vectorIndexSize > 0 && resp.totalDocuments > 0) {
                    double coverage = (double)resp.indexedDocuments / resp.totalDocuments * 100;
                    std::cout << "Coverage: " << std::fixed << std::setprecision(1) << coverage
                              << "%\n";
                }
            }
            return Result<void>();
        };

        // Prefer a direct daemon call (no pooling) for determinism
        auto fallback = [&]() -> Result<void> { return executeVectorsLocal(); };

        try {
            yams::daemon::ClientConfig cfg;
            cfg.dataDir = cli_->getDataPath();
            cfg.enableChunkedResponses = false;
            cfg.singleUseConnections = true;
            cfg.requestTimeout = std::chrono::milliseconds(30000);
            yams::daemon::DaemonClient client(cfg);

            auto result = run_sync(client.call(dreq), std::chrono::seconds(30));
            if (result) {
                return render(result.value());
            }
            return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats (vectors) unavailable"}
                              : fallback();
        } catch (...) {
            return daemonOnly_ ? Error{ErrorCode::Unknown, "Daemon stats (vectors) exception"}
                              : fallback();
        }
    }

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
            std::cout << "Vector Database Statistics\n";
            std::cout << "==========================\n";
            std::cout << "Path: " << vdbPath.string() << "\n";
            std::cout << "Exists: " << (exists ? "Yes" : "No") << "\n";
            std::cout << "Embeddings: " << vectorCount << "\n";
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
        if (format_ == "json") {
            json output;
            output["storage"]["total_documents"] = stats.totalObjects;
            output["storage"]["total_size"] = stats.totalBytes;
            output["storage"]["unique_blocks"] = stats.uniqueBlocks;
            std::cout << output.dump(2) << std::endl;
        } else {
            std::cout << "\n=== Storage Statistics (Local) ===\n\n";
            std::cout << "  Total Documents: " << stats.totalObjects << "\n";
            std::cout << "  Total Size: " << formatSize(stats.totalBytes) << "\n";
            std::cout << "  Unique Blocks: " << stats.uniqueBlocks << "\n";
            std::cout << std::endl;
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
        // Command center banner and Storage Overview
        std::cout << yams::cli::ui::title_banner("YAMS Command Center - Storage Statistics",
                                                 yams::cli::ui::terminal_width())
                  << "\n\n";
        renderStorageOverview(resp);

        // Always show health and recommendations (new default behavior)
        renderSmartHealthSection(resp);
        renderRecommendationsSection(resp);

        // Verbose mode shows comprehensive technical details
        if (verbose_) {
            renderTechnicalDetailsSection(resp);
            renderServiceStatusSection(resp);
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

        std::cout << output.dump(2) << std::endl;
        return Result<void>();
    }

    // ===================
    // Section Renderers
    // ===================

    void renderStorageOverview(const yams::daemon::GetStatsResponse& resp) {
        printSectionHeader("Storage Overview");

        {
            std::vector<yams::cli::ui::Row> rows{
                {"Documents", formatNumber(resp.totalDocuments), ""},
                {"Size", formatSize(resp.totalSize), ""}};
            yams::cli::ui::render_rows(std::cout, rows);
        }

        // Enhanced unique blocks display with ratio
        auto blocksIt = resp.additionalStats.find("unique_blocks");
        if (blocksIt != resp.additionalStats.end()) {
            uint64_t uniqueBlocks = std::stoull(blocksIt->second);
            std::string blockInfo = formatNumber(uniqueBlocks);

            // Show block to document ratio
            auto ratioIt = resp.additionalStats.find("block_to_doc_ratio");
            if (ratioIt != resp.additionalStats.end()) {
                double ratio = std::stod(ratioIt->second);
                if (ratio > 1.2) {
                    // Format ratio with one decimal place
                    std::ostringstream oss;
                    oss << std::fixed << std::setprecision(1) << ratio;
                    blockInfo += "  (~" + oss.str() + " blocks per document)";
                }
            }
            yams::cli::ui::render_rows(std::cout, {{"Blocks (unique)", blockInfo, ""}});
        }

        // Space efficiency
        auto effIt = resp.additionalStats.find("space_efficiency");
        if (effIt != resp.additionalStats.end()) {
            double efficiency = std::stod(effIt->second);
            std::string effInfo = formatPercentage(efficiency);

            // Show orphaned space if present
            auto orphanIt = resp.additionalStats.find("orphaned_bytes");
            if (orphanIt != resp.additionalStats.end()) {
                uint64_t orphanedBytes = std::stoull(orphanIt->second);
                if (orphanedBytes > 0) {
                    double orphanedPercent = 100.0 - efficiency;
                    effInfo += "  (" + formatPercentage(orphanedPercent) + " orphaned)";
                }
            }
            std::string effBar = yams::cli::ui::progress_bar(efficiency / 100.0, 30);
            yams::cli::ui::render_rows(std::cout, {{"Space Efficiency", effInfo, effBar}});
        }

        double idxPct = resp.totalDocuments > 0
                            ? (double)resp.indexedDocuments / resp.totalDocuments * 100.0
                            : 0.0;
        std::string idxPctStr = formatPercentage(idxPct);
        yams::cli::ui::render_rows(std::cout,
                                   {{"Indexed", formatNumber(resp.indexedDocuments), idxPctStr}});

        std::cout << "\n";
    }

    void renderSmartHealthSection(const yams::daemon::GetStatsResponse& resp) {
        printSectionHeader("System Health");

        // Overall status
        auto healthStatus = resp.additionalStats.find("health_status");
        std::string status =
            (healthStatus != resp.additionalStats.end()) ? healthStatus->second : "unknown";
        std::string statusDisplay = (status == "healthy") ? "Healthy" : "Needs Attention";
        {
            std::vector<yams::cli::ui::Row> rows{{"Status", statusDisplay, ""}};
            yams::cli::ui::render_rows(std::cout, rows);
        }

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
        {
            std::vector<yams::cli::ui::Row> rows{{"Storage", storageStatus, ""}};
            yams::cli::ui::render_rows(std::cout, rows);
        }

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
            {
                std::vector<yams::cli::ui::Row> rows{{"Block Health", blockStatus, ""}};
                yams::cli::ui::render_rows(std::cout, rows);
            }
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
        {
            std::vector<yams::cli::ui::Row> rows{{"Indexing", indexStatus, ""}};
            yams::cli::ui::render_rows(std::cout, rows);
        }

        // Vector database status
        std::string vectorStatus = resp.vectorIndexSize > 0
                                       ? "Initialized - " + formatSize(resp.vectorIndexSize)
                                       : "Not initialized";
        {
            std::vector<yams::cli::ui::Row> rows{{"Vector DB", vectorStatus, ""}};
            yams::cli::ui::render_rows(std::cout, rows);
        }

        // Daemon status if available
        auto uptimeIt = resp.additionalStats.find("daemon_uptime");
        if (uptimeIt != resp.additionalStats.end()) {
            long uptime = std::stoll(uptimeIt->second);
            std::string daemonStatus = "Running (uptime: " + formatUptime(uptime) + ")";
            {
                std::vector<yams::cli::ui::Row> rows{{"Daemon", daemonStatus, ""}};
                yams::cli::ui::render_rows(std::cout, rows);
            }
        }

        std::cout << "\n";
    }

    void renderRecommendationsSection(const yams::daemon::GetStatsResponse& resp) {
        printSectionHeader("Recommendations");

        std::vector<std::string> recommendations;
        std::vector<std::string> warnings; // Higher priority items

        // Check for orphaned blocks - HIGH PRIORITY
        auto orphanedBlocksIt = resp.additionalStats.find("orphaned_blocks");
        auto orphanedBytesIt = resp.additionalStats.find("orphaned_bytes");
        if (orphanedBlocksIt != resp.additionalStats.end() &&
            orphanedBytesIt != resp.additionalStats.end()) {
            uint64_t orphanedBlocks = std::stoull(orphanedBlocksIt->second);
            uint64_t orphanedBytes = std::stoull(orphanedBytesIt->second);

            if (orphanedBlocks > 0) {
                std::string recommendation = "Run 'yams repair --orphans' to clean " +
                                             formatNumber(orphanedBlocks) + " orphaned blocks";
                if (orphanedBytes > 1024 * 1024) { // Show size if > 1MB
                    recommendation += " (~" + formatSize(orphanedBytes) + " recoverable)";
                }
                warnings.push_back(recommendation);
            }
        }

        // Check block health status
        auto blockHealthIt = resp.additionalStats.find("block_health");
        if (blockHealthIt != resp.additionalStats.end()) {
            if (blockHealthIt->second == "critical") {
                warnings.push_back("Storage has critical block integrity issues - repair urgently");
            }
        }

        // Check indexing status
        if (resp.indexedDocuments == 0 && resp.totalDocuments > 0) {
            recommendations.push_back(
                "Run 'yams repair --embeddings' to generate missing embeddings");
        }

        // Check space efficiency
        auto effIt = resp.additionalStats.find("space_efficiency");
        if (effIt != resp.additionalStats.end()) {
            double efficiency = std::stod(effIt->second);
            if (efficiency < 50.0) {
                recommendations.push_back(
                    "Consider running 'yams repair --optimize' to improve storage efficiency");
            }
        }

        // Check vector database
        if (resp.vectorIndexSize == 0 && resp.totalDocuments > 0) {
            recommendations.push_back(
                "Vector database will be created automatically on first search");
        }

        // Check for performance optimizations
        if (resp.totalDocuments > 1000 && resp.indexedDocuments < resp.totalDocuments / 2) {
            recommendations.push_back("Consider running indexing to improve search performance");
        }

        // Display warnings first (with warning symbol)
        for (const auto& warning : warnings) {
            std::cout << "  ⚠ " << warning << "\n";
        }

        // Display regular recommendations
        for (const auto& rec : recommendations) {
            std::cout << "  □ " << rec << "\n";
        }

        // If no issues found
        if (warnings.empty() && recommendations.empty()) {
            std::cout << "  ✓ System is optimally configured\n";
        }

        std::cout << "\n";
    }

    void renderTechnicalDetailsSection(const yams::daemon::GetStatsResponse& resp) {
        printSectionHeader("Technical Details");

        // Deduplication info
        std::vector<yams::cli::ui::Row> rows;
        auto uniqueBlocks = resp.additionalStats.find("unique_blocks");
        if (uniqueBlocks != resp.additionalStats.end()) {
            std::string dedupInfo = std::to_string(resp.compressionRatio) + ":1 ratio, " +
                                    formatNumber(std::stoull(uniqueBlocks->second)) +
                                    " unique blocks";
            rows.push_back({"Deduplication", dedupInfo, ""});
        }

        // Performance info
        auto storeOps = resp.additionalStats.find("performance_store_ops");
        if (storeOps != resp.additionalStats.end()) {
            uint64_t ops = std::stoull(storeOps->second);
            std::string perfInfo = ops > 0 ? formatNumber(ops) + " operations recorded"
                                           : "Fresh daemon (0 operations recorded)";
            rows.push_back({"Performance", perfInfo, ""});
        }

        // Compression info
        std::string compressionInfo =
            std::to_string(resp.compressionRatio) + ":1 ratio using content-addressed storage";
        rows.push_back({"Compression", compressionInfo, ""});

        // Streaming metrics if available
        auto ttfb = resp.additionalStats.find("stream_ttfb_avg_ms");
        auto streams = resp.additionalStats.find("stream_total_streams");
        auto batches = resp.additionalStats.find("stream_batches_emitted");
        auto keepalives = resp.additionalStats.find("stream_keepalives");
        if (ttfb != resp.additionalStats.end() || streams != resp.additionalStats.end() ||
            batches != resp.additionalStats.end() || keepalives != resp.additionalStats.end()) {
            rows.push_back({"Streaming TTFB (avg)", (ttfb!=resp.additionalStats.end()? ttfb->second : "0" ) + std::string(" ms"), ""});
            if (streams!=resp.additionalStats.end()) rows.push_back({"Streams", streams->second, ""});
            if (batches!=resp.additionalStats.end()) rows.push_back({"Batches Emitted", batches->second, ""});
            if (keepalives!=resp.additionalStats.end()) rows.push_back({"Keepalives", keepalives->second, ""});
        }

        yams::cli::ui::render_rows(std::cout, rows);
        std::cout << "\n";
    }

    void renderServiceStatusSection(const yams::daemon::GetStatsResponse& resp) {
        printSectionHeader("Service Status");

        // Show daemon version and uptime
        std::vector<yams::cli::ui::Row> statusRows;
        auto versionIt = resp.additionalStats.find("daemon_version");
        auto uptimeIt = resp.additionalStats.find("daemon_uptime");
        if (versionIt != resp.additionalStats.end() && uptimeIt != resp.additionalStats.end()) {
            std::string uptimeStr = formatUptime(std::stoll(uptimeIt->second));
            std::string daemonInfo = versionIt->second + " (uptime: " + uptimeStr + ")";
            statusRows.push_back({"Daemon", daemonInfo, ""});
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
        statusRows.push_back({"ContentStore", contentStoreStatus, ""});

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
        statusRows.push_back({"MetadataRepo", metadataStatus, ""});

        // Configured data directory (helps debug path mismatches)
        auto dataDirIt = resp.additionalStats.find("data_dir");
        if (dataDirIt != resp.additionalStats.end() && !dataDirIt->second.empty()) {
            statusRows.push_back({"Data Dir", dataDirIt->second, ""});
        }

        // SearchExecutor status
        auto searchIt = resp.additionalStats.find("service_searchexecutor");
        std::string searchStatus =
            searchIt != resp.additionalStats.end() && searchIt->second == "available"
                ? "✓ Available"
                : "⚠ Unavailable";
        statusRows.push_back({"SearchExecutor", searchStatus, ""});

        yams::cli::ui::render_rows(std::cout, statusRows);

        // Client/IPC self-check diagnostics
        yams::cli::ui::render_rows(std::cout, {{"Client", YAMS_VERSION_STRING, ""}});
        yams::cli::ui::render_rows(std::cout,
                                   {{"Client IPC", "Pool: enabled",
                                     yams::daemon::DaemonClient::resolveSocketPathConfigFirst().string()}});

        std::cout << "\n";

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
            yams::cli::ui::render_rows(std::cout, {{"Vector DB", vectorStatus, ""}});
            auto vdbPathIt = resp.additionalStats.find("vectordb_path");
            if (vdbPathIt != resp.additionalStats.end() && !vdbPathIt->second.empty()) {
                yams::cli::ui::render_rows(std::cout, {{"", vdbPathIt->second, ""}});
            }
            if (resp.vectorIndexSize == 0) {
                yams::cli::ui::render_rows(std::cout,
                                           {{"", "", "Will initialize on first search"}});
            }
        }

        // ONNX Models status
        auto embeddingServiceIt = resp.additionalStats.find("service_embeddingservice");
        auto modelsLoadedIt = resp.additionalStats.find("onnx_models_loaded");
        auto modelsErrorIt = resp.additionalStats.find("onnx_models_error");

        if (embeddingServiceIt != resp.additionalStats.end()) {
            if (embeddingServiceIt->second == "available") {
                std::string modelsCount =
                    modelsLoadedIt != resp.additionalStats.end() ? modelsLoadedIt->second : "0";
                if (modelsCount == "0") {
                    yams::cli::ui::render_rows(
                        std::cout, {{"ONNX Models", "⚠ No models loaded (0/1 available)", ""}});
                    yams::cli::ui::render_rows(
                        std::cout, {{"", "", "Run 'yams model --download all-MiniLM-L6-v2'"}});
                } else {
                    yams::cli::ui::render_rows(
                        std::cout, {{"ONNX Models", "✓ " + modelsCount + " models loaded", ""}});
                }
            } else if (embeddingServiceIt->second == "unavailable") {
                yams::cli::ui::render_rows(std::cout,
                                           {{"ONNX Models", "⚠ Service unavailable", ""}});
                if (modelsErrorIt != resp.additionalStats.end()) {
                    yams::cli::ui::render_rows(std::cout, {{"", "", modelsErrorIt->second}});
                }
            } else if (embeddingServiceIt->second == "error") {
                yams::cli::ui::render_rows(std::cout, {{"ONNX Models", "✗ Service error", ""}});
                if (modelsErrorIt != resp.additionalStats.end()) {
                    yams::cli::ui::render_rows(std::cout, {{"", "", modelsErrorIt->second}});
                }
            }
        } else {
            yams::cli::ui::render_rows(std::cout, {{"ONNX Models", "⚠ Status unknown", ""}});
        }

        // Embedding Service status
        if (embeddingServiceIt != resp.additionalStats.end()) {
            std::string embeddingStatus;
            if (embeddingServiceIt->second == "available") {
                std::string modelsCount =
                    modelsLoadedIt != resp.additionalStats.end() ? modelsLoadedIt->second : "0";
                embeddingStatus = modelsCount == "0" ? "⚠ Unavailable (no models)" : "✓ Available";
            } else {
                embeddingStatus = "⚠ Unavailable (" + embeddingServiceIt->second + ")";
            }
            yams::cli::ui::render_rows(std::cout, {{"Embedding Service", embeddingStatus, ""}});
        }

        std::cout << "\n";
    }

private:
    YamsCLI* cli_ = nullptr;
    std::string format_;
    bool localOnly_ = false;  // Force local-only mode
    bool watchMode_ = false;  // Live refresh mode
    bool verbose_ = false;    // Show comprehensive technical details
    bool forceColor_ = false; // --color flag
    bool noColor_ = false;    // --no-color flag
    bool daemonOnly_ = false; // Force daemon-only, disable local fallback
};

// Factory function
std::unique_ptr<ICommand> createStatsCommand() {
    return std::make_unique<StatsCommand>();
}

} // namespace yams::cli
