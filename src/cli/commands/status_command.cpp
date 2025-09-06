#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <iomanip>
#include <iostream>
#include <vector>

#include <yams/cli/async_bridge.h>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
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
        cmd->add_flag("--verbose", verbose_, "Show detailed information");

        cmd->callback([this]() {
            auto result = execute();
            if (!result) {
                spdlog::error("Status command failed: {}", result.error().message);
                std::exit(1);
            }
        });
    }

    Result<void> execute() override {
        YAMS_ZONE_SCOPED_N("StatusCommand::execute");

        try {
            // Try daemon-first for quick status snapshot
            {
                // Use DaemonClient with CLI data path to align storage
                yams::daemon::ClientConfig cfg;
                cfg.dataDir = cli_->getDataPath();
                yams::daemon::DaemonClient client(cfg);
                if (auto st = yams::cli::run_sync(client.status(), std::chrono::seconds(5)); st) {
                    auto s = st.value();

                    // Build services summary from stats JSON for a quick health glance
                    std::string svcSummary;
                    std::string waitingSummary;

                    // Supplement Status with daemon stats JSON to avoid zeros from proto path
                    // (StatusResponse over proto currently only carries a 'state' string)
                    try {
                        yams::daemon::GetStatsRequest sreq; // defaults are fine
                        if (auto gres =
                                yams::cli::run_sync(client.getStats(sreq), std::chrono::seconds(2));
                            gres) {
                            const auto& g = gres.value();
                            auto itJson = g.additionalStats.find("json");
                            if (itJson != g.additionalStats.end()) {
                                auto j = nlohmann::json::parse(itJson->second);
                                if (s.version.empty() && j.contains("version"))
                                    s.version = j["version"].get<std::string>();
                                if (s.uptimeSeconds == 0 && j.contains("uptime_seconds"))
                                    s.uptimeSeconds = j["uptime_seconds"].get<uint64_t>();
                                if (s.memoryUsageMb == 0 && j.contains("memory_mb"))
                                    s.memoryUsageMb = j["memory_mb"].get<double>();
                                if (s.cpuUsagePercent == 0 && j.contains("cpu_pct"))
                                    s.cpuUsagePercent = j["cpu_pct"].get<double>();

                                // Derive services summary
                                auto flag = [&](const char* key, const char* good) -> std::string {
                                    if (j.contains("services") && j["services"].contains(key)) {
                                        auto v = j["services"][key].get<std::string>();
                                        if (v == good)
                                            return "✓";
                                        if (v == std::string("available") ||
                                            v == std::string("initialized"))
                                            return "✓";
                                        return "⚠";
                                    }
                                    return "⚠";
                                };
                                std::string content = flag("contentstore", "running");
                                std::string repo = flag("metadatarepo", "running");
                                std::string search;
                                if (j.contains("services") &&
                                    j["services"].contains("searchexecutor")) {
                                    auto sv = j["services"]["searchexecutor"].get<std::string>();
                                    if (sv == "available")
                                        search = "✓";
                                    else {
                                        std::string reason =
                                            j["services"].value("searchexecutor_reason", "");
                                        search = reason.empty() ? "⚠" : ("⚠ (" + reason + ")");
                                    }
                                } else {
                                    search = "⚠";
                                }
                                std::string models;
                                if (j.contains("services") &&
                                    j["services"].contains("embeddingservice")) {
                                    auto ev = j["services"]["embeddingservice"].get<std::string>();
                                    if (ev == "available") {
                                        std::string count =
                                            j["services"].contains("onnx_models_loaded")
                                                ? j["services"]["onnx_models_loaded"].dump()
                                                : std::string{"0"};
                                        models = (count == "0" || count == "\"0\"") ? "⚠ (0)" : "✓";
                                    } else {
                                        models = "⚠";
                                    }
                                } else {
                                    models = "⚠";
                                }
                                svcSummary = content + " Content | " + repo + " Repo | " + search +
                                             " Search | " + models + " Models";

                                // Build waiting summary when not ready
                                if (!s.ready && j.contains("readiness")) {
                                    std::vector<std::string> waiting;
                                    for (auto it = j["readiness"].begin();
                                         it != j["readiness"].end(); ++it) {
                                        bool ok = false;
                                        try {
                                            ok = it.value().get<bool>();
                                        } catch (...) {
                                        }
                                        if (!ok) {
                                            std::ostringstream os;
                                            os << it.key();
                                            if (j.contains("progress") &&
                                                j["progress"].contains(it.key())) {
                                                try {
                                                    os << " (" << j["progress"][it.key()].get<int>()
                                                       << "%)";
                                                } catch (...) {
                                                }
                                            }
                                            waiting.push_back(os.str());
                                        }
                                    }
                                    if (!waiting.empty()) {
                                        std::ostringstream line;
                                        for (size_t i = 0; i < waiting.size(); ++i) {
                                            if (i)
                                                line << ", ";
                                            line << waiting[i];
                                            if (i >= 4) {
                                                line << ", …";
                                                break;
                                            }
                                        }
                                        waitingSummary = line.str();
                                    }
                                }
                            }
                        }
                    } catch (...) {
                        // best-effort supplement only
                    }
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
                        j["fsmTransitions"] = s.fsmTransitions;
                        j["fsmHeaderReads"] = s.fsmHeaderReads;
                        j["fsmPayloadReads"] = s.fsmPayloadReads;
                        j["fsmPayloadWrites"] = s.fsmPayloadWrites;
                        j["fsmBytesSent"] = s.fsmBytesSent;
                        j["fsmBytesReceived"] = s.fsmBytesReceived;
                        j["muxActiveHandlers"] = s.muxActiveHandlers;
                        j["muxQueuedBytes"] = s.muxQueuedBytes;
                        j["muxWriterBudgetBytes"] = s.muxWriterBudgetBytes;
                        if (!s.readinessStates.empty()) {
                            nlohmann::json rj = nlohmann::json::object();
                            for (const auto& [k, v] : s.readinessStates)
                                rj[k] = v;
                            j["readiness"] = std::move(rj);
                        }
                        if (!s.initProgress.empty()) {
                            nlohmann::json pj = nlohmann::json::object();
                            for (const auto& [k, v] : s.initProgress)
                                pj[k] = v;
                            j["initProgress"] = std::move(pj);
                        }
                        std::cout << j.dump(2) << std::endl;
                    } else {
                        // Retro, compact output (using supplemented values when available)
                        std::cout << "== DAEMON STATUS ==\n";
                        std::cout << "RUN  : " << (s.running ? "yes" : "no") << "\n";
                        std::cout << "VER  : " << s.version << "\n";
                        std::cout << "STATE: "
                                  << (s.overallStatus.empty()
                                          ? (s.ready ? "Ready"
                                                     : (s.running ? "Starting" : "Stopped"))
                                          : s.overallStatus)
                                  << "\n";
                        std::cout << "UP   : " << s.uptimeSeconds << "s\n";
                        std::cout << "REQ  : " << s.requestsProcessed
                                  << "  ACT: " << s.activeConnections << "\n";
                        std::cout << "MEM  : " << std::fixed << std::setprecision(1)
                                  << s.memoryUsageMb << "MB  CPU: " << (int)s.cpuUsagePercent
                                  << "%\n";
                        if (!svcSummary.empty()) {
                            std::cout << "SVC  : " << svcSummary << "\n";
                        }
                        if (!s.ready && !waitingSummary.empty()) {
                            std::cout << "WAIT : " << waitingSummary << "\n";
                        }
                    }
                    return Result<void>();
                }
            }
            // Ensure storage is initialized (will detect existing storage)
            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                // Storage doesn't exist or can't be initialized
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
                return Result<void>();
            }

            // Get system components
            auto store = cli_->getContentStore();
            auto metadataRepo = cli_->getMetadataRepository();

            if (!store || !metadataRepo) {
                return Error{ErrorCode::NotInitialized, "Failed to access storage components"};
            }

            // Gather status information
            StatusInfo status = gatherStatusInfo(store, metadataRepo);

            if (jsonOutput_) {
                outputJson(status);
            } else {
                outputText(status);
            }

            return Result<void>();

        } catch (const std::exception& e) {
            return Error{ErrorCode::Unknown, std::string(e.what())};
        }
    }

private:
    YamsCLI* cli_ = nullptr;
    bool jsonOutput_ = false;
    bool verbose_ = false;

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

        // Issues/Recommendations
        std::vector<std::string> warnings;
        std::vector<std::string> recommendations;
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
            info.warnings.push_back("Storage access failed: " + std::string(e.what()));
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
                info.warnings.push_back("Config migration check failed: " + std::string(e.what()));
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

        // Check vector database status
        try {
            vector::VectorDatabaseConfig vdbConfig;
            vdbConfig.database_path = (cli_->getDataPath() / "vectors.db").string();
            vdbConfig.embedding_dim = 384; // Default for status check

            auto vectorDb = std::make_unique<vector::VectorDatabase>(vdbConfig);
            if (vectorDb->initialize()) {
                info.vectorDbHealthy = true;
                if (vectorDb->tableExists()) {
                    info.embeddingCount = vectorDb->getVectorCount();
                }
            }
        } catch (const std::exception& e) {
            info.vectorDbHealthy = false;
            if (verbose_) {
                info.warnings.push_back("Vector database check failed: " + std::string(e.what()));
            }
        }

        // Generate recommendations
        generateRecommendations(info);

        return info;
    }

    void generateRecommendations(StatusInfo& info) {
        // Configuration migration (highest priority)
        if (info.configMigrationNeeded) {
            info.recommendations.push_back("Update configuration: yams config migrate");
        }

        if (!info.hasModels) {
            info.recommendations.push_back(
                "Download an embedding model: yams model --download all-MiniLM-L6-v2");
        }

        if (info.hasModels && !info.autoGenerationEnabled) {
            info.recommendations.push_back("Enable auto-embedding: yams config embeddings enable");
        }

        if (info.totalDocuments > 0 && info.embeddingCount == 0) {
            info.recommendations.push_back(
                "Generate embeddings for existing documents: yams repair --embeddings");
        }

        if (info.totalDocuments > 100 && info.embeddingCount < info.totalDocuments / 2) {
            info.recommendations.push_back(
                "Many documents lack embeddings - consider running: yams repair --embeddings");
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

        std::cout << "  \"recommendations\": [";
        for (size_t i = 0; i < info.recommendations.size(); ++i) {
            if (i > 0)
                std::cout << ", ";
            std::cout << "\"" << info.recommendations[i] << "\"";
        }
        std::cout << "],\n";

        std::cout << "  \"warnings\": [";
        for (size_t i = 0; i < info.warnings.size(); ++i) {
            if (i > 0)
                std::cout << ", ";
            std::cout << "\"" << info.warnings[i] << "\"";
        }
        std::cout << "]\n";
        std::cout << "}\n";
    }

    struct Row {
        std::string label;
        std::string value;
        std::string extra;
    };

    static size_t visibleWidth(const std::string& s) {
        size_t w = 0;
        bool inEsc = false;
        for (char c : s) {
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
        for (char c : s) {
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
                break;
            out.push_back(c);
            ++w;
        }
        out += "…";
        return out;
    }
    static int detectTerminalWidth() {
        const char* cols = getenv("COLUMNS");
        if (cols) {
            try {
                return std::max(60, std::min(200, std::stoi(cols)));
            } catch (...) {
            }
        }
        return 100;
    }

    static void renderRows(const std::vector<Row>& rows) {
        if (rows.empty())
            return;
        int term = detectTerminalWidth();
        const int pad = 2;
        size_t maxL = 8, maxV = 8;
        for (const auto& r : rows) {
            maxL = std::max(maxL, visibleWidth(r.label));
            maxV = std::max(maxV, visibleWidth(r.value));
        }
        for (const auto& r : rows) {
            std::string l = r.label, v = r.value, e = r.extra;
            size_t lW = maxL, vW = maxV;
            int base = 2 + (int)lW + pad + (int)vW;
            int need = base + (e.empty() ? 0 : pad + (int)visibleWidth(e));
            int over = need - term;
            if (over > 0) {
                if (!e.empty()) {
                    size_t ew = visibleWidth(e);
                    size_t tgt = (over >= (int)ew) ? 0 : ew - over;
                    e = truncateToWidth(e, tgt);
                    need = base + (e.empty() ? 0 : pad + (int)visibleWidth(e));
                    over = need - term;
                }
                if (over > 0 && vW > 8) {
                    size_t tgt = std::max((size_t)8, vW - (size_t)over);
                    v = truncateToWidth(v, tgt);
                    vW = visibleWidth(v);
                    need =
                        2 + (int)lW + pad + (int)vW + (e.empty() ? 0 : pad + (int)visibleWidth(e));
                    over = need - term;
                }
                if (over > 0 && lW > 8) {
                    size_t tgt = std::max((size_t)8, lW - (size_t)over);
                    l = truncateToWidth(l, tgt);
                    lW = visibleWidth(l);
                }
            }
            std::cout << "  " << std::left << std::setw((int)lW) << l << std::string(pad, ' ')
                      << std::right << std::setw((int)vW) << v;
            if (!e.empty())
                std::cout << std::string(pad, ' ') << e;
            std::cout << "\n";
        }
    }

    void outputText(const StatusInfo& info) {
        std::cout << "YAMS System Status\n";
        std::cout << "==================\n\n";

        std::vector<Row> rows;
        rows.push_back({std::string(info.storageHealthy ? "✓ Storage" : "✗ Storage"),
                        info.storageHealthy ? ("Healthy (" + formatSize(info.totalSize) + ", " +
                                               std::to_string(info.totalDocuments) + " documents)")
                                            : std::string("Issues detected"),
                        ""});
        rows.push_back(
            {std::string(info.configMigrationNeeded ? "⚠ Configuration" : "✓ Configuration"),
             std::string("v") + info.configVersion +
                 (info.configMigrationNeeded ? " (migration recommended)" : " (current)"),
             ""});
        rows.push_back({std::string(info.hasModels ? "✓ Models" : "⚠ Models"),
                        info.hasModels
                            ? (std::to_string(info.availableModels.size()) + " available (" +
                               [&]() {
                                   std::ostringstream oss;
                                   for (size_t i = 0; i < info.availableModels.size(); ++i) {
                                       if (i)
                                           oss << ", ";
                                       oss << info.availableModels[i];
                                   }
                                   return oss.str();
                               }() +
                               ")")
                            : std::string("No embedding models found"),
                        ""});
        rows.push_back({std::string(info.autoGenerationEnabled ? "✓ Embeddings" : "⚠ Embeddings"),
                        std::string(info.autoGenerationEnabled ? "Auto-generation enabled"
                                                               : "Auto-generation disabled") +
                            (info.embeddingCount > 0 ? (" (" + std::to_string(info.embeddingCount) +
                                                        " embeddings ready)")
                                                     : std::string()),
                        ""});
        rows.push_back(
            {std::string(info.vectorDbHealthy ? "✓ Vector DB" : "⚠ Vector DB"),
             std::string(info.vectorDbHealthy ? "Ready for semantic search" : "Not available"),
             ""});
        renderRows(rows);

        if (!info.recommendations.empty()) {
            std::cout << "\nRecommendations:\n";
            for (const auto& rec : info.recommendations) {
                std::cout << "  → " << rec << "\n";
            }
        }
        if (!info.warnings.empty() && (verbose_ || !info.storageHealthy)) {
            std::cout << "\nWarnings:\n";
            for (const auto& warning : info.warnings) {
                std::cout << "  ⚠ " << warning << "\n";
            }
        }
        std::cout << "\nFor detailed statistics: yams stats\n";
        std::cout << "For configuration help: yams config --help\n";
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
