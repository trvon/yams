#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <future>
#include <iomanip>
#include <iostream>
#include <vector>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/recommendation_util.h>
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
        std::promise<Result<void>> prom;
        auto fut = prom.get_future();
        boost::asio::co_spawn(
            boost::asio::system_executor{},
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
                cfg.dataDir = cli_->getDataPath();
                yams::daemon::DaemonClient client(cfg);
                auto st = co_await client.status();
                if (st) {
                    auto s = st.value();
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
                        std::cout << j.dump(2) << std::endl;
                    } else {
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
                        if (!s.ready && !waitingSummary.empty()) {
                            std::cout << "WAIT : " << waitingSummary << "\n";
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
                        // Embedding runtime summary
                        std::cout << "EMBED: "
                                  << (s.embeddingAvailable ? "available" : "unavailable");
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
                    // In verbose mode, pull vector DB quick stats (size + rows) from daemon stats
                    if (!jsonOutput_ && verbose_) {
                        try {
                            yams::daemon::ClientConfig scfg;
                            scfg.dataDir = cli_->getDataPath();
                            scfg.singleUseConnections = true;
                            yams::daemon::DaemonClient scli(scfg);
                            yams::daemon::GetStatsRequest greq;
                            greq.detailed = true;
                            auto gs = co_await scli.getStats(greq);
                            if (gs) {
                                const auto& resp = gs.value();
                                std::string line = "VECDB: ";
                                line += (resp.vectorIndexSize > 0)
                                            ? ("Initialized - " + formatSize(resp.vectorIndexSize))
                                            : "Not initialized";
                                auto it = resp.additionalStats.find("vector_rows");
                                if (it != resp.additionalStats.end()) {
                                    line += " (" + it->second + " rows)";
                                }
                                std::cout << line << "\n";
                            }
                        } catch (...) {
                        }
                    }
                    co_return Result<void>();
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

        // Pull worker pool metrics from daemon status (best-effort, short timeout)
        try {
            yams::daemon::DaemonClient probe{};
            std::promise<Result<yams::daemon::StatusResponse>> promProbe;
            auto futProbe = promProbe.get_future();
            auto workProbe = [&]() -> boost::asio::awaitable<void> {
                auto sr = co_await probe.status();
                promProbe.set_value(std::move(sr));
                co_return;
            };
            boost::asio::co_spawn(boost::asio::system_executor{}, workProbe(),
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
        } catch (...) {
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
        if (info.workerThreads > 0) {
            std::ostringstream oss;
            oss << "threads=" << info.workerThreads << ", active=" << info.workerActive
                << ", queued=" << info.workerQueued << ", util=" << info.workerUtilPct << "%";
            rows.push_back({"✓ Worker Pool", "Healthy", oss.str()});
        }
        renderRows(rows);

        if (!info.advice.empty()) {
            yams::cli::printRecommendationsText(info.advice, std::cout);
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
