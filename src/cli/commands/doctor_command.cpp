#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <cstdlib>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <CLI/CLI.hpp>
extern "C" {
#include <yams/plugins/model_provider_v1.h>
}

namespace yams::cli {

class DoctorCommand : public ICommand {
public:
    std::string getName() const override { return "doctor"; }
    std::string getDescription() const override {
        return "Diagnose daemon connectivity and plugin health";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* doctor = app.add_subcommand(getName(), getDescription());
        doctor->require_subcommand(0); // allow bare doctor
        doctor->add_flag("--fix", fixAllTop_, "Fix everything (embeddings + FTS5)");
        // Non-interactive fix flags
        doctor->add_flag("--fix-config-dims", fixConfigDims_,
                         "Align config embedding dims to target (non-interactive)");
        doctor->add_flag("--recreate-vectors", recreateVectors_,
                         "Drop and recreate vector tables to target dim (non-interactive)");
        doctor->add_option(
            "--dim", recreateDim_,
            "Target dimension to use with --recreate-vectors (defaults to resolved target)");
        doctor->add_flag("--stop-daemon", stopDaemon_,
                         "Attempt to stop daemon before DB operations");

        // doctor (no args) -> quick daemon + plugin scan summary OR repair when --fix
        doctor->callback([this]() { cli_->setPendingCommand(this); });

        // doctor daemon
        auto* dsub = doctor->add_subcommand("daemon", "Check daemon socket and status");
        dsub->callback([this]() { checkDaemon(); });

        // doctor plugin [NAME|PATH]
        auto* psub = doctor->add_subcommand("plugin", "Check a plugin (.so/.wasm or by name)");
        psub->add_option("target", pluginArg_, "Plugin path or logical name");
        psub->add_option("--iface", ifaceId_, "Interface ID to probe (default: model_provider_v1)");
        psub->add_option("--iface-version", ifaceVersion_, "Interface version (default: 1)");
        psub->add_flag("--no-daemon", noDaemonProbe_, "Skip daemon dry-run load");
        psub->callback([this]() {
            if (pluginArg_.empty()) {
                std::cout
                    << "target is optional now. Examples:\n"
                       "  yams doctor plugin onnx\n"
                       "  yams doctor plugin ~/.local/lib/yams/plugins/libyams_onnx_plugin.so\n\n";
                runAll();
                return;
            }
            checkPlugin(pluginArg_);
        });

        // doctor plugins (summary)
        doctor->add_subcommand("plugins", "Show plugin summary (loaded + scan)")
            ->callback([this]() { runAll(); });

        // doctor repair [--embeddings|--fts5|--graph|--all]
        auto* rsub =
            doctor->add_subcommand("repair", "Repair common issues (embeddings, FTS5, graph)");
        rsub->add_flag("--embeddings", fixEmbeddings_, "Generate missing vector embeddings");
        rsub->add_flag("--fts5", fixFts5_, "Rebuild FTS5 text index (best-effort)");
        rsub->add_flag("--graph", fixGraph_,
                       "Construct/repair knowledge graph from tags and metadata");
        rsub->add_flag("--all", fixAll_, "Run all repair operations");
        rsub->callback([this]() { runRepair(); });
    }

    Result<void> execute() override { return Result<void>(); }
    boost::asio::awaitable<Result<void>> executeAsync() override {
        // Default doctor behavior: combined summary (runAll already checks daemon)
        try {
            runAll();
        } catch (const std::exception& e) {
            std::cout << "Doctor error: " << e.what() << "\n";
            co_return Error{ErrorCode::Unknown, e.what()};
        }
        co_return Result<void>();
    }

    // Resolve embedding dimension for DB creation (config > env > generator > model > heuristic)
    std::pair<size_t, std::string> resolveEmbeddingDim() {
        namespace fs = std::filesystem;
        size_t dim = 0;
        std::string src;
        // Config
        try {
            fs::path cfgHome;
            if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
                cfgHome = fs::path(xdg);
            else if (const char* home = std::getenv("HOME"))
                cfgHome = fs::path(home) / ".config";
            else
                cfgHome = fs::path("~/.config");
            fs::path cfgPath = cfgHome / "yams" / "config.toml";
            if (fs::exists(cfgPath)) {
                std::ifstream in(cfgPath);
                std::string line;
                auto trim = [&](std::string& t) {
                    if (t.empty())
                        return;
                    t.erase(0, t.find_first_not_of(" 	"));
                    auto p = t.find_last_not_of(" 	");
                    if (p != std::string::npos)
                        t.erase(p + 1);
                };
                while (std::getline(in, line)) {
                    std::string l = line;
                    trim(l);
                    if (l.empty() || l[0] == '#')
                        continue;
                    if (l.rfind("embeddings.embedding_dim", 0) == 0 ||
                        l.find("embeddings.embedding_dim") != std::string::npos ||
                        l.find("vector_database.embedding_dim") != std::string::npos) {
                        auto eq = l.find('=');
                        if (eq != std::string::npos) {
                            std::string v = l.substr(eq + 1);
                            trim(v);
                            if (!v.empty() && v.front() == '"' && v.back() == '"')
                                v = v.substr(1, v.size() - 2);
                            try {
                                dim = static_cast<size_t>(std::stoul(v));
                                src = "config";
                            } catch (...) {
                            }
                        }
                        if (dim > 0)
                            break;
                    }
                }
            }
        } catch (...) {
        }
        if (dim > 0)
            return {dim, src};
        // Env
        try {
            if (const char* envd = std::getenv("YAMS_EMBED_DIM")) {
                dim = static_cast<size_t>(std::stoul(envd));
                src = "env";
            }
        } catch (...) {
        }
        if (dim > 0)
            return {dim, src};
        // Generator
        try {
            auto emb = cli_ ? cli_->getEmbeddingGenerator() : nullptr;
            if (emb) {
                dim = emb->getEmbeddingDimension();
                if (dim > 0)
                    src = "generator";
            }
        } catch (...) {
        }
        if (dim > 0)
            return {dim, src};
        // Model heuristic
        try {
            std::string pick;
            if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL"))
                pick = std::string(pref);
            if (pick.empty()) {
                if (const char* home = std::getenv("HOME")) {
                    fs::path models = fs::path(home) / ".yams" / "models";
                    std::error_code ec;
                    if (fs::exists(models, ec) && fs::is_directory(models, ec)) {
                        for (const auto& e : fs::directory_iterator(models, ec)) {
                            if (!e.is_directory())
                                continue;
                            if (fs::exists(e.path() / "model.onnx", ec)) {
                                pick = e.path().filename().string();
                                break;
                            }
                        }
                    }
                }
            }
            if (!pick.empty()) {
                if (pick.find("MiniLM") != std::string::npos) {
                    dim = 384;
                } else {
                    dim = 768;
                }
                src = "model";
            }
        } catch (...) {
        }
        if (dim > 0)
            return {dim, src};
        // Heuristic fallback
        dim = 384;
        src = "heuristic";
        return {dim, src};
    }

private:
    // ============ UI Helpers ============
    struct StepResult {
        std::string name;
        bool ok{false};
        std::string message; // optional detail
    };

    static void printHeader(const std::string& title) {
        std::cout << "\n" << title << "\n";
        for (size_t i = 0; i < title.size(); ++i)
            std::cout << '-';
        std::cout << "\n";
    }

    static void printStatusLine(const std::string& label, const std::string& value) {
        std::cout << "- " << label << ": " << value << "\n";
    }

    static void printSuccess(const std::string& msg) { std::cout << "✓ " << msg << "\n"; }

    static void printWarn(const std::string& msg) { std::cout << "⚠ " << msg << "\n"; }

    static void printError(const std::string& msg) { std::cout << "✗ " << msg << "\n"; }

    static void printSummary(const std::string& title, const std::vector<StepResult>& steps) {
        printHeader(title);
        for (const auto& s : steps) {
            std::cout << (s.ok ? "  ✓ " : "  ✗ ") << s.name;
            if (!s.message.empty())
                std::cout << " — " << s.message;
            std::cout << "\n";
        }
    }
    // Step helpers to make doctor logic composable
    Result<void> touchDbFile(const std::filesystem::path& dbPath) {
        try {
            namespace fs = std::filesystem;
            if (!fs::exists(dbPath)) {
                std::ofstream f(dbPath);
                f.flush();
                f.close();
            }
            if (!fs::exists(dbPath)) {
                return Error{ErrorCode::IOError, "failed to create vectors.db"};
            }
            return Result<void>();
        } catch (const std::exception& e) {
            return Error{ErrorCode::IOError, e.what()};
        }
    }

    // Legacy lock helpers kept for reference; no-ops now
    Result<void> acquireMaintenanceLock(const std::filesystem::path&) { return Result<void>(); }
    void releaseMaintenanceLock(const std::filesystem::path&) {}

    // Stop the daemon gracefully if it is running (best-effort)
    void ensureDaemonStopped() {
        try {
            yams::daemon::ClientConfig ccfg;
            if (cli_)
                ccfg.dataDir = cli_->getDataPath();
            ccfg.singleUseConnections = true;
            ccfg.requestTimeout = std::chrono::seconds(5);
            yams::daemon::DaemonClient shut(ccfg);
            std::promise<Result<void>> prom;
            auto fut = prom.get_future();
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&]() -> boost::asio::awaitable<void> {
                    auto r = co_await shut.shutdown(true);
                    prom.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);
            (void)fut.wait_for(std::chrono::seconds(6));
        } catch (...) {
        }
    }

    Result<void> openDbMinimal(yams::vector::SqliteVecBackend& be,
                               const std::filesystem::path& dbPath, int timeout_ms) {
        // Ensure minimal, fast open with deferred vec init
        setEnvIfUnset("YAMS_SQLITE_VEC_SKIP_INIT", 1);
        setEnvIfUnset("YAMS_SQLITE_MINIMAL_PRAGMAS", 1);
        auto openOpt = runWithSpinner(
            "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); }, timeout_ms);
        if (!openOpt)
            return Error{ErrorCode::Timeout, "open timeout"};
        return *openOpt;
    }

    Result<void> initVecModule(yams::vector::SqliteVecBackend& be, int timeout_ms) {
        auto vecOpt = runWithSpinner(
            "Initializing sqlite-vec", [&]() { return be.ensureVecLoaded(); }, timeout_ms);
        if (!vecOpt)
            return Error{ErrorCode::Timeout, "vec init timeout"};
        return *vecOpt;
    }

    Result<void> createVecSchema(yams::vector::SqliteVecBackend& be, size_t dim, int timeout_ms) {
        auto cr = runWithSpinner(
            "Creating vector tables", [&]() { return be.createTables(dim); }, timeout_ms);
        if (!cr)
            return Error{ErrorCode::Timeout, "create tables timeout"};
        return *cr;
    }

    Result<void> dropVecSchema(yams::vector::SqliteVecBackend& be, int timeout_ms) {
        auto dr = runWithSpinner(
            "Dropping existing vector tables", [&]() { return be.dropTables(); }, timeout_ms);
        if (!dr)
            return Error{ErrorCode::Timeout, "drop tables timeout"};
        return *dr;
    }

    static void writeVectorSentinel(const std::filesystem::path& dataDir, size_t dim) {
        try {
            namespace fs = std::filesystem;
            nlohmann::json j;
            j["embedding_dim"] = dim;
            j["schema"] = "vec0";
            j["schema_version"] = 1;
            j["updated"] = static_cast<std::int64_t>(std::time(nullptr));
            fs::path p = dataDir / "vectors_sentinel.json";
            std::ofstream out(p);
            out << j.dump(2);
        } catch (...) {
        }
    }
    // ============ Config Helpers (TOML — best-effort line edits) ============
    static std::filesystem::path resolveConfigPath() {
        namespace fs = std::filesystem;
        fs::path cfgHome;
        if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
            cfgHome = fs::path(xdg);
        else if (const char* home = std::getenv("HOME"))
            cfgHome = fs::path(home) / ".config";
        else
            cfgHome = fs::path("~/.config");
        return cfgHome / "yams" / "config.toml";
    }

    struct ConfigDims {
        std::optional<size_t> embeddings;
        std::optional<size_t> vector_db;
        std::optional<size_t> index;
    };

    static ConfigDims readConfigDims(const std::filesystem::path& cfg) {
        ConfigDims out{};
        try {
            std::ifstream in(cfg);
            if (!in)
                return out;
            std::string line;
            std::regex kvRe(R"(^\s*([A-Za-z0-9_\.]+)\s*=\s*([0-9]+))");
            while (std::getline(in, line)) {
                std::smatch m;
                if (!std::regex_search(line, m, kvRe))
                    continue;
                std::string key = m[1];
                size_t val = 0;
                try {
                    val = static_cast<size_t>(std::stoul(m[2]));
                } catch (...) {
                    continue;
                }
                if (key == "embeddings.embedding_dim")
                    out.embeddings = val;
                else if (key == "vector_database.embedding_dim")
                    out.vector_db = val;
                else if (key == "vector_index.dimension" || key == "vector_index.dimenions")
                    out.index = val;
            }
        } catch (...) {
        }
        return out;
    }

    static bool writeOrReplaceConfigDims(const std::filesystem::path& cfg, size_t dim) {
        try {
            std::vector<std::string> lines;
            lines.reserve(256);
            {
                std::ifstream in(cfg);
                if (in) {
                    std::string L;
                    while (std::getline(in, L))
                        lines.push_back(L);
                }
            }
            auto replace_or_append = [&](const std::string& key) {
                bool replaced = false;
                std::regex re(std::string("^\\s*") + key + R"(\s*=\s*[0-9]+)");
                for (auto& L : lines) {
                    if (std::regex_search(L, re)) {
                        L = key + " = " + std::to_string(dim);
                        replaced = true;
                        break;
                    }
                }
                if (!replaced)
                    lines.push_back(key + " = " + std::to_string(dim));
            };
            replace_or_append("embeddings.embedding_dim");
            replace_or_append("vector_database.embedding_dim");
            replace_or_append("vector_index.dimension");
            std::ofstream out(cfg, std::ios::trunc);
            for (auto& L : lines)
                out << L << "\n";
            return true;
        } catch (...) {
            return false;
        }
    }
    // Run a blocking function with a console spinner and timeout.
    // Returns optional result; nullopt indicates timeout.
    std::optional<Result<void>> runWithSpinner(const std::string& label,
                                               const std::function<Result<void>()>& fn,
                                               int timeout_ms) {
        using namespace std::chrono;
        auto fut = std::async(std::launch::async, fn);
        auto start = steady_clock::now();
        const char frames[] = {'|', '/', '-', '\\'};
        size_t idx = 0;
        while (true) {
            if (fut.wait_for(0ms) == std::future_status::ready) {
                auto r = fut.get();
                std::cout << "\r" << label << " ... done    \n";
                return r;
            }
            auto now = steady_clock::now();
            if (duration_cast<milliseconds>(now - start).count() >= timeout_ms) {
                std::cout << "\r" << label << " ... timeout after " << timeout_ms << " ms    \n";
                return std::nullopt;
            }
            std::cout << "\r" << label << " " << frames[idx++ % 4] << std::flush;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    static void setEnvIfUnset(const char* key, int value) {
        if (!std::getenv(key)) {
            try {
                std::string v = std::to_string(value);
                setenv(key, v.c_str(), 0);
            } catch (...) {
            }
        }
    }
    void runRepair() {
        try {
            if (fixAll_) {
                fixEmbeddings_ = true;
                fixFts5_ = true;
                fixGraph_ = true;
            }
            if (!fixEmbeddings_ && !fixFts5_ && !fixGraph_) {
                std::cout << "Nothing to repair. Use --embeddings, --fts5, --graph or --all.\n";
                return;
            }

            auto ensured = cli_->ensureStorageInitialized();
            if (!ensured) {
                std::cout << "Storage init failed: " << ensured.error().message << "\n";
                return;
            }
            auto appCtx = cli_->getAppContext();
            if (!appCtx) {
                std::cout << "AppContext unavailable\n";
                return;
            }

            // Embeddings repair
            if (fixEmbeddings_) {
                std::cout << "Repair: missing embeddings...\n";
                yams::repair::EmbeddingRepairConfig rcfg;
                rcfg.batchSize = 32;
                rcfg.skipExisting = true;
                rcfg.dataPath = cli_->getDataPath();
                auto emb = cli_->getEmbeddingGenerator();
                if (!emb) {
                    std::cout << "  Embedding generator unavailable -- ensure model provider is "
                                 "configured.\n";
                } else {
                    // Guard: abort repair if DB schema dim mismatches configured target
                    try {
                        namespace fs = std::filesystem;
                        fs::path dbPath = cli_->getDataPath() / "vectors.db";
                        auto readDbDim = [&](const fs::path& p) -> std::optional<size_t> {
                            sqlite3* db = nullptr;
                            if (sqlite3_open(p.string().c_str(), &db) != SQLITE_OK) {
                                if (db)
                                    sqlite3_close(db);
                                return std::nullopt;
                            }
                            const char* sql =
                                "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
                            sqlite3_stmt* stmt = nullptr;
                            std::optional<size_t> out{};
                            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                                if (sqlite3_step(stmt) == SQLITE_ROW) {
                                    const unsigned char* txt = sqlite3_column_text(stmt, 0);
                                    if (txt) {
                                        std::string ddl(reinterpret_cast<const char*>(txt));
                                        auto pos = ddl.find("float[");
                                        if (pos != std::string::npos) {
                                            auto end = ddl.find(']', pos);
                                            if (end != std::string::npos && end > pos + 6) {
                                                std::string num =
                                                    ddl.substr(pos + 6, end - (pos + 6));
                                                try {
                                                    out = static_cast<size_t>(std::stoul(num));
                                                } catch (...) {
                                                }
                                            }
                                        }
                                    }
                                }
                                sqlite3_finalize(stmt);
                            }
                            sqlite3_close(db);
                            return out;
                        };
                        auto dbDim = readDbDim(dbPath);
                        auto resolved = resolveEmbeddingDim();
                        size_t targetDim =
                            resolved.first ? resolved.first : emb->getEmbeddingDimension();
                        if (dbDim && *dbDim != targetDim) {
                            std::cout << "  ✗ Schema dimension mismatch (db=" << *dbDim
                                      << ", target=" << targetDim << ") — aborting repair.\n";
                            std::cout << "    Run: yams doctor (recreate vector tables), then yams "
                                         "repair --embeddings\n";
                            return;
                        }
                    } catch (...) {
                    }
                    auto missing = yams::repair::getDocumentsMissingEmbeddings(
                        appCtx->metadataRepo, cli_->getDataPath(), 0);
                    if (!missing) {
                        std::cout << "  Could not query missing embeddings: "
                                  << missing.error().message << "\n";
                    } else if (missing.value().empty()) {
                        std::cout << "  ✓ No documents missing embeddings\n";
                    } else {
                        bool attemptedDaemon = false;
                        {
                            try {
                                yams::daemon::ClientConfig cfg;
                                cfg.dataDir = cli_->getDataPath();
                                cfg.singleUseConnections = true;
                                // Configurable RPC timeout (default 60s)
                                int rpc_ms = 60000;
                                if (const char* env = std::getenv("YAMS_DOCTOR_RPC_TIMEOUT_MS")) {
                                    try {
                                        rpc_ms = std::max(1000, std::stoi(env));
                                    } catch (...) {
                                    }
                                }
                                cfg.requestTimeout = std::chrono::milliseconds(rpc_ms);
                                yams::daemon::DaemonClient client(cfg);
                                yams::daemon::EmbedDocumentsRequest ed;
                                ed.modelName = ""; // let server/provider decide
                                ed.documentHashes = missing.value();
                                ed.batchSize = static_cast<uint32_t>(rcfg.batchSize);
                                ed.skipExisting = rcfg.skipExisting;
                                std::promise<Result<yams::daemon::EmbedDocumentsResponse>> prom;
                                auto fut = prom.get_future();
                                boost::asio::co_spawn(
                                    boost::asio::system_executor{},
                                    [&]() -> boost::asio::awaitable<void> {
                                        auto r = co_await client.streamingEmbedDocuments(ed);
                                        prom.set_value(std::move(r));
                                        co_return;
                                    },
                                    boost::asio::detached);
                                // Overall wait limit (2x per-request timeout, minimum 30s)
                                int overall_ms = std::max(30000, 2 * rpc_ms);
                                auto er = (fut.wait_for(std::chrono::milliseconds(overall_ms)) ==
                                           std::future_status::ready)
                                              ? fut.get()
                                              : Result<yams::daemon::EmbedDocumentsResponse>(
                                                    Error{ErrorCode::Timeout, "embed timeout"});
                                if (er) {
                                    std::cout << "  ✓ Daemon embeddings: requested="
                                              << er.value().requested
                                              << ", embedded=" << er.value().embedded
                                              << ", skipped=" << er.value().skipped
                                              << ", failed=" << er.value().failed << "\n";
                                    attemptedDaemon = true;
                                } else {
                                    std::cout << "  ⚠ Daemon embeddings failed ("
                                              << er.error().message
                                              << ") — falling back to local mode. Use "
                                                 "'--no-daemon' to skip RPC.\n";
                                }
                            } catch (const std::exception& ex) {
                                std::cout << "  ⚠ Daemon embeddings exception (" << ex.what()
                                          << ") — falling back to local mode. Use '--no-daemon' to "
                                             "skip RPC.\n";
                            }
                        }
                        if (!attemptedDaemon) {
                            auto stats = yams::repair::repairMissingEmbeddings(
                                appCtx->store, appCtx->metadataRepo, emb, rcfg, missing.value(),
                                nullptr, appCtx->contentExtractors);
                            if (!stats) {
                                std::cout
                                    << "  ✗ Embedding repair failed: " << stats.error().message
                                    << "\n";
                            } else {
                                std::cout << "  ✓ Embeddings generated="
                                          << stats.value().embeddingsGenerated
                                          << ", skipped=" << stats.value().embeddingsSkipped
                                          << ", failed=" << stats.value().failedOperations << "\n";
                            }
                        }
                    }
                }
            }

            // Knowledge graph repair (build from tags/metadata)
            if (fixGraph_) {
                auto kg = cli_->getKnowledgeGraphStore();
                if (!kg) {
                    std::cout << "Repair: knowledge graph store unavailable — skipped.\n";
                } else {
                    auto r = repairGraph();
                    if (r) {
                        std::cout << "Repair: knowledge graph completed.\n";
                    } else {
                        std::cout << "Repair: knowledge graph failed — " << r.error().message
                                  << "\n";
                    }
                }
            }

            // FTS5 rebuild
            if (fixFts5_) {
                std::cout << "Repair: FTS5 index...\n";
                if (!appCtx->store || !appCtx->metadataRepo) {
                    std::cout << "  Store/Metadata unavailable\n";
                    return;
                }
                auto docs = appCtx->metadataRepo->findDocumentsByPath("%");
                if (!docs) {
                    std::cout << "  Query failed: " << docs.error().message << "\n";
                    return;
                }
                size_t ok = 0, fail = 0, total = docs.value().size(), cur = 0;
                for (const auto& d : docs.value()) {
                    ++cur;
                    std::string ext = d.fileExtension;
                    if (!ext.empty() && ext[0] == '.')
                        ext.erase(0, 1);
                    try {
                        auto extracted = yams::extraction::util::extractDocumentText(
                            appCtx->store, d.sha256Hash, d.mimeType, ext,
                            appCtx->contentExtractors);
                        if (extracted && !extracted->empty()) {
                            auto ir = appCtx->metadataRepo->indexDocumentContent(
                                d.id, d.fileName, *extracted, d.mimeType);
                            if (ir) {
                                (void)appCtx->metadataRepo->updateFuzzyIndex(d.id);
                                ++ok;
                            } else {
                                ++fail;
                            }
                        } else {
                            ++fail;
                        }
                    } catch (...) {
                        ++fail;
                    }
                    if (total > 0 && cur % 500 == 0)
                        std::cout << "  ..." << cur << "/" << total << "\n";
                }
                std::cout << "  ✓ FTS5 reindex complete: ok=" << ok << ", fail=" << fail << "\n";
            }
        } catch (const std::exception& e) {
            std::cout << "Doctor repair error: " << e.what() << "\n";
        }
    }

    // Build/repair knowledge graph using tags/metadata (non-destructive)
    Result<void> repairGraph();

    // Minimal daemon check: connect and get status
    void checkDaemon() {
        using namespace yams::daemon;
        try {
            ClientConfig cfg;
            if (cli_)
                cfg.dataDir = cli_->getDataPath();
            cfg.singleUseConnections = true;
            cfg.requestTimeout = std::chrono::milliseconds(3000);
            DaemonClient client(cfg);
            std::cout << "Daemon: ";
            std::promise<Result<yams::daemon::StatusResponse>> prom;
            auto fut = prom.get_future();
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&]() -> boost::asio::awaitable<void> {
                    auto r = co_await client.status();
                    prom.set_value(std::move(r));
                    co_return;
                },
                boost::asio::detached);
            auto s = (fut.wait_for(std::chrono::seconds(3)) == std::future_status::ready)
                         ? fut.get()
                         : Result<yams::daemon::StatusResponse>(
                               Error{ErrorCode::Timeout, "status timeout"});
            if (!s) {
                std::cout << "UNAVAILABLE - " << s.error().message << "\n";
                return;
            }
            const auto& st = s.value();
            std::cout << (st.ready ? "READY" : "NOT READY") << ", state="
                      << (st.lifecycleState.empty() ? st.overallStatus : st.lifecycleState)
                      << ", connections=" << st.activeConnections
                      << ", version=" << (st.version.empty() ? "unknown" : st.version) << "\n";

            // Show vector scoring availability line (brief)
            try {
                bool vecAvail = false, vecEnabled = false;
                if (auto it = st.readinessStates.find("vector_embeddings_available");
                    it != st.readinessStates.end())
                    vecAvail = it->second;
                if (auto it = st.readinessStates.find("vector_scoring_enabled");
                    it != st.readinessStates.end())
                    vecEnabled = it->second;
                if (!vecEnabled) {
                    std::cout << "  Vector scoring: disabled — "
                              << (vecAvail ? "config weight=0" : "embeddings unavailable") << "\n";
                } else {
                    std::cout << "  Vector scoring: enabled\n";
                }
            } catch (...) {
            }

            // Show resources (memory/cpu) and worker/mux details when available
            try {
                if (!st.ready) {
                    const auto& phase =
                        (st.lifecycleState.empty() ? st.overallStatus : st.lifecycleState);
                    if (st.retryAfterMs > 0) {
                        std::cout << "  Resources:    pending (daemon " << phase << ", retry after "
                                  << st.retryAfterMs << " ms)" << "\n";
                    } else {
                        std::cout << "  Resources:    pending (daemon " << phase << ")" << "\n";
                    }
                } else {
                    std::cout << "  Resources:    RAM=" << std::fixed << std::setprecision(1)
                              << st.memoryUsageMb << " MB, CPU=" << std::setprecision(0)
                              << st.cpuUsagePercent << "%\n";
                }
                auto wt = st.requestCounts.find("worker_threads");
                auto wa = st.requestCounts.find("worker_active");
                auto wq = st.requestCounts.find("worker_queued");
                if (!st.ready) {
                    std::cout << "  Workers:      pending\n";
                } else if (wt != st.requestCounts.end() || wa != st.requestCounts.end() ||
                           wq != st.requestCounts.end()) {
                    size_t threads = wt != st.requestCounts.end() ? wt->second : 0;
                    size_t active = wa != st.requestCounts.end() ? wa->second : 0;
                    size_t queued = wq != st.requestCounts.end() ? wq->second : 0;
                    std::cout << "  Workers:      threads=" << threads << " active=" << active
                              << " queued=" << queued << "\n";
                }
                if (st.muxQueuedBytes > 0)
                    std::cout << "  Mux queued: " << st.muxQueuedBytes << " bytes\n";
                if (st.retryAfterMs > 0)
                    std::cout << "  Retry hint: " << st.retryAfterMs << " ms\n";
            } catch (...) {
            }
        } catch (const std::exception& e) {
            std::cout << "Daemon: ERROR - " << e.what() << "\n";
        }
    }

    // Read trust file and collect trusted roots
    static std::set<std::filesystem::path> readTrusted() {
        namespace fs = std::filesystem;
        fs::path cfgHome;
        if (const char* xdg = std::getenv("XDG_CONFIG_HOME"))
            cfgHome = fs::path(xdg);
        else if (const char* home = std::getenv("HOME"))
            cfgHome = fs::path(home) / ".config";
        else
            cfgHome = fs::path("~/.config");
        fs::path trustFile = cfgHome / "yams" / "plugins_trust.txt";
        std::set<fs::path> roots;
        std::ifstream in(trustFile);
        if (!in)
            return roots;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty())
                continue;
            roots.insert(fs::path(line));
        }
        return roots;
    }

    static bool isTrustedPath(const std::filesystem::path& p,
                              const std::set<std::filesystem::path>& roots) {
        namespace fs = std::filesystem;
        std::error_code ec;
        auto canon = fs::weakly_canonical(p, ec);
        for (const auto& r : roots) {
            auto rc = fs::weakly_canonical(r, ec);
            auto cs = canon.string();
            auto rs = rc.string();
            if (!rs.empty() && cs.rfind(rs, 0) == 0)
                return true;
        }
        return false;
    }

    // Resolve name to path by scanning default plugin dirs for a matching descriptor name
    static std::optional<std::filesystem::path> resolveByName(const std::string& name) {
        namespace fs = std::filesystem;
        // 1) Try exact filename/stem match first
        for (const auto& dir : yams::daemon::PluginLoader::getDefaultPluginDirectories()) {
            std::error_code ec;
            if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec))
                continue;
            for (const auto& e : fs::directory_iterator(dir, ec)) {
                if (!e.is_regular_file(ec))
                    continue;
                auto p = e.path();
                auto ext = p.extension().string();
                if (ext != ".so" && ext != ".dylib" && ext != ".dll" && ext != ".wasm")
                    continue;
                if (p.stem().string() == name || p.filename().string() == name)
                    return p;
            }
        }
        // 2) Try ABI descriptor name via dlopen + yams_plugin_get_name()
        for (const auto& dir : yams::daemon::PluginLoader::getDefaultPluginDirectories()) {
            std::error_code ec;
            if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec))
                continue;
            for (const auto& e : fs::directory_iterator(dir, ec)) {
                if (!e.is_regular_file(ec))
                    continue;
                auto p = e.path();
                auto ext = p.extension().string();
                if (ext != ".so" && ext != ".dylib" && ext != ".dll")
                    continue; // ABI path for C++ plugins; WASM handled by manifest separately
                void* h = dlopen(p.c_str(), RTLD_LAZY | RTLD_LOCAL);
                if (!h)
                    continue;
                auto close = [&]() { dlclose(h); };
                auto get_name =
                    reinterpret_cast<const char* (*)()>(dlsym(h, "yams_plugin_get_name"));
                const char* nm = get_name ? get_name() : nullptr;
                bool match = (nm && std::string(nm) == name);
                close();
                if (match)
                    return p;
            }
        }
        // 3) Heuristic: filename contains the token (e.g., libyams_onnx_plugin.so for "onnx")
        for (const auto& dir : yams::daemon::PluginLoader::getDefaultPluginDirectories()) {
            std::error_code ec;
            if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec))
                continue;
            for (const auto& e : fs::directory_iterator(dir, ec)) {
                if (!e.is_regular_file(ec))
                    continue;
                auto p = e.path().filename().string();
                if (p.find(name) != std::string::npos)
                    return e.path();
            }
        }
        return std::nullopt;
    }

    // Perform local dlopen + symbol/iface probes
    void checkPlugin(const std::string& arg) {
        namespace fs = std::filesystem;
        std::cout << "Plugin Doctor: " << arg << "\n";
        fs::path target(arg);
        if (!fs::exists(target)) {
            auto rp = resolveByName(arg);
            if (rp)
                target = *rp;
        }
        if (!fs::exists(target)) {
            std::cout << "  [FAIL] Not found as path or name in default dirs\n";
            return;
        }
        // Read trust
        auto trusted = readTrusted();
        bool isTrusted = isTrustedPath(target, trusted);

        // dlopen and basic ABI checks
        void* handle = dlopen(target.c_str(), RTLD_LAZY | RTLD_LOCAL);
        if (!handle) {
            std::cout << "  [FAIL] dlopen: " << (dlerror() ? dlerror() : "unknown") << "\n";
            return;
        }
        auto close = [&]() { dlclose(handle); };
        auto get_abi = reinterpret_cast<int (*)()>(dlsym(handle, "yams_plugin_get_abi_version"));
        auto get_name = reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_name"));
        auto get_ver =
            reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_version"));
        auto get_manifest =
            reinterpret_cast<const char* (*)()>(dlsym(handle, "yams_plugin_get_manifest_json"));
        bool have_core = (get_abi && get_name && get_ver);
        if (!have_core) {
            std::cout << "  [FAIL] Missing required ABI symbols (get_abi/get_name/get_version)\n";
            close();
            return;
        }
        int abi = get_abi();
        std::string pname = get_name() ? get_name() : "";
        std::string pver = get_ver() ? get_ver() : "";
        std::string manifest = (get_manifest && get_manifest()) ? get_manifest() : std::string();
        std::cout << "  Name: " << pname << "  Version: " << pver << "  ABI: " << abi << "\n";
        std::cout << "  Trusted: " << (isTrusted ? "yes" : "no") << "\n";

        // Probe interface (default: model_provider_v1@1)
        std::string iface = ifaceId_.empty() ? std::string("model_provider_v1") : ifaceId_;
        uint32_t ivers = (ifaceVersion_ == 0 ? 1u : ifaceVersion_);
        using GetIfaceFn = int (*)(const char*, uint32_t, void**);
        dlerror();
        auto get_iface = reinterpret_cast<GetIfaceFn>(dlsym(handle, "yams_plugin_get_interface"));
        const char* dlerr = dlerror();
        if (dlerr || !get_iface) {
            std::cout << "  Interface: [SKIP] get_interface symbol not found\n";
        } else {
            void* out = nullptr;
            int rc = get_iface(iface.c_str(), ivers, &out);
            std::cout << "  Interface: " << iface << " v" << ivers << " -> "
                      << ((rc == 0 && out) ? "AVAILABLE" : "UNAVAILABLE") << "\n";
            // If model_provider_v1 is available, probe batch embedding API
            if (rc == 0 && out && iface == std::string(YAMS_IFACE_MODEL_PROVIDER_V1) &&
                ivers == YAMS_IFACE_MODEL_PROVIDER_V1_VERSION) {
                auto* prov = reinterpret_cast<yams_model_provider_v1*>(out);
                bool has_batch = prov->generate_embedding_batch != nullptr;
                std::cout << "  Batch API: " << (has_batch ? "PRESENT" : "MISSING") << "\n";
                if (has_batch) {
                    // Try a tiny batch with a likely model name
                    const char* model_id = nullptr;
                    std::string chosen;
                    if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL"))
                        chosen = pref;
                    if (chosen.empty()) {
                        // Common defaults
                        const char* candidates[] = {"nomic-embed-text-v1.5", "all-mpnet-base-v2",
                                                    nullptr};
                        for (int i = 0; candidates[i]; ++i) {
                            chosen = candidates[i];
                            break;
                        }
                    }
                    model_id = chosen.c_str();
                    bool loaded = false;
                    if (prov->is_model_loaded) {
                        bool out_loaded = false;
                        if (prov->is_model_loaded(prov->self, model_id, &out_loaded) == YAMS_OK)
                            loaded = out_loaded;
                    }
                    if (!loaded && prov->load_model) {
                        // Best-effort load; ignore errors
                        (void)prov->load_model(prov->self, model_id, nullptr, nullptr);
                    }
                    const char* texts_c[2] = {"hello", "world"};
                    size_t lens[2] = {5, 5};
                    float* vecs = nullptr;
                    size_t out_b = 0, out_d = 0;
                    int brc = prov->generate_embedding_batch(
                        prov->self, model_id, reinterpret_cast<const uint8_t* const*>(texts_c),
                        lens, 2, &vecs, &out_b, &out_d);
                    if (brc == YAMS_OK && out_b == 2 && out_d > 0 && vecs != nullptr) {
                        std::cout << "  Batch probe: OK (batch=" << out_b << ", dim=" << out_d
                                  << ")\n";
                        if (prov->free_embedding_batch)
                            prov->free_embedding_batch(prov->self, vecs, out_b, out_d);
                    } else {
                        std::cout << "  Batch probe: FAIL (status=" << brc << ")\n";
                        if (vecs && prov->free_embedding_batch)
                            prov->free_embedding_batch(prov->self, vecs, out_b, out_d);
                    }
                }
            }
        }
        close();

        // Optional: daemon dry-run load to validate trust + loader behavior
        if (!noDaemonProbe_) {
            try {
                using namespace yams::daemon;
                ClientConfig cfg;
                if (cli_)
                    cfg.dataDir = cli_->getDataPath();
                cfg.singleUseConnections = true;
                cfg.requestTimeout = std::chrono::milliseconds(4000);
                DaemonClient client(cfg);
                PluginLoadRequest req;
                req.pathOrName = target.string();
                req.dryRun = true;
                std::promise<Result<yams::daemon::PluginLoadResponse>> prom2;
                auto fut2 = prom2.get_future();
                boost::asio::co_spawn(
                    boost::asio::system_executor{},
                    [&]() -> boost::asio::awaitable<void> {
                        auto r2 = co_await client.call(req);
                        prom2.set_value(std::move(r2));
                        co_return;
                    },
                    boost::asio::detached);
                auto r = (fut2.wait_for(std::chrono::seconds(4)) == std::future_status::ready)
                             ? fut2.get()
                             : Result<yams::daemon::PluginLoadResponse>(
                                   Error{ErrorCode::Timeout, "load timeout"});
                if (!r) {
                    std::cout << "  Daemon: DRY-RUN LOAD -> FAIL: " << r.error().message << "\n";
                } else {
                    const auto& lr = r.value();
                    std::cout << "  Daemon: DRY-RUN LOAD -> OK (" << lr.record.name << ")\n";
                }
            } catch (const std::exception& e) {
                std::cout << "  Daemon: DRY-RUN LOAD -> ERROR: " << e.what() << "\n";
            }
        }
    }

    // doctor (no args): quick combined
    void runAll() {
        // Keep doctor fast and non-invasive by default:
        // - Daemon status
        // - Installed models
        // - Vector DB dimension check
        checkDaemon();
        checkInstalledModels();
        checkEmbeddingDimMismatch();

        // Knowledge graph quick check: show basic index stats and recommend repair when empty
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

                std::cout << "\nKnowledge Graph\n----------------\n";
                if (entities <= 0 && nodes <= 0) {
                    std::cout << "Graph entities: 0 — run 'yams doctor repair --graph' to build "
                                 "from tags/metadata.\n";
                } else {
                    if (nodes >= 0)
                        std::cout << "Nodes:        " << nodes << "\n";
                    if (edges >= 0)
                        std::cout << "Edges:        " << edges << "\n";
                    if (aliases >= 0)
                        std::cout << "Aliases:      " << aliases << "\n";
                    if (embeddings >= 0)
                        std::cout << "Embeddings:   " << embeddings << "\n";
                    if (entities >= 0)
                        std::cout << "Doc entities: " << entities << "\n";
                }
            }
        } catch (...) {
            // Silent: doctor remains best-effort
        }

        // Show currently loaded plugins (via daemon stats) succinctly
        try {
            using namespace yams::daemon;
            ClientConfig cfg;
            cfg.singleUseConnections = true;
            cfg.requestTimeout = std::chrono::milliseconds(1200);
            DaemonClient client(cfg);
            GetStatsRequest sreq;
            sreq.detailed = false;
            sreq.showFileTypes = false;
            std::promise<Result<GetStatsResponse>> prom;
            auto fut = prom.get_future();
            boost::asio::co_spawn(
                boost::asio::system_executor{},
                [&]() -> boost::asio::awaitable<void> {
                    auto rr = co_await client.getStats(sreq);
                    prom.set_value(std::move(rr));
                    co_return;
                },
                boost::asio::detached);
            auto rr = (fut.wait_for(std::chrono::milliseconds(1300)) == std::future_status::ready)
                          ? fut.get()
                          : Result<GetStatsResponse>(Error{ErrorCode::Timeout, "stats timeout"});
            if (rr) {
                const auto& gs = rr.value();
                auto it = gs.additionalStats.find("plugins_json");
                std::cout << "\nLoaded Plugins\n";
                std::cout << "---------------\n";
                if (it != gs.additionalStats.end() && !it->second.empty()) {
                    try {
                        auto j = nlohmann::json::parse(it->second, nullptr, false);
                        if (!j.is_discarded() && j.is_array() && !j.empty()) {
                            for (const auto& rec : j) {
                                std::string name = rec.contains("name")
                                                       ? rec["name"].get<std::string>()
                                                       : std::string("(unknown)");
                                std::string line = "- " + name;
                                try {
                                    if (rec.contains("provider") && rec["provider"].is_boolean() &&
                                        rec["provider"].get<bool>()) {
                                        line += " [provider]";
                                    }
                                    if (rec.contains("degraded") && rec["degraded"].is_boolean() &&
                                        rec["degraded"].get<bool>()) {
                                        line += " [degraded]";
                                    }
                                    if (rec.contains("models_loaded")) {
                                        try {
                                            int models = rec["models_loaded"].get<int>();
                                            if (models > 0) {
                                                line += " models=" + std::to_string(models);
                                            }
                                        } catch (...) {
                                        }
                                    }
                                    if (rec.contains("error")) {
                                        try {
                                            if (rec["error"].is_string()) {
                                                auto err = rec["error"].get<std::string>();
                                                if (!err.empty()) {
                                                    line += " error=\"" + err + "\"";
                                                }
                                            } else {
                                                line += " error=" + rec["error"].dump();
                                            }
                                        } catch (...) {
                                        }
                                    }
                                } catch (...) {
                                }
                                std::cout << line << "\n";
#ifdef __APPLE__
                                // macOS diagnostic: if this plugin is degraded and we have a path,
                                // run `otool -L` to surface DYLIB linkage issues.
                                try {
                                    bool __degraded = false;
                                    std::string __pluginPath;
                                    try {
                                        if (rec.contains("degraded") &&
                                            rec["degraded"].is_boolean())
                                            __degraded = rec["degraded"].get<bool>();
                                        if (rec.contains("path") && rec["path"].is_string())
                                            __pluginPath = rec["path"].get<std::string>();
                                    } catch (...) {
                                    }
                                    if (__degraded && !__pluginPath.empty()) {
                                        std::cout << "  otool -L: " << __pluginPath << "\n";
                                        int __rc = std::system(
                                            (std::string("otool -L \"") + __pluginPath + "\"")
                                                .c_str());
                                        if (__rc == -1) {
                                            std::cout << "    (failed to execute otool)\n";
                                        }
                                    }
                                } catch (...) {
                                }
#endif
                            }
                        } else {
                            std::cout << "(none loaded)\n";
                        }
                    } catch (...) {
                        std::cout << "(none loaded)\n";
                    }
                } else {
                    std::cout << "(none loaded)\n";
                }
                {
                    auto eit = gs.additionalStats.find("plugins_error");
                    if (eit != gs.additionalStats.end() && !eit->second.empty()) {
                        std::cout << "(plugins error: " << eit->second << ")\n";
                    }
                }
            }
        } catch (...) {
        }

        std::cout << "\nHint: run 'yams doctor plugin <path|name>' for a deep plugin check.\n";

        // Compact live repair progress (short poll). Non-blocking when no repair activity.
        try {
            using namespace yams::daemon;
            ClientConfig cfg;
            cfg.singleUseConnections = true;
            cfg.requestTimeout = std::chrono::milliseconds(1200);
            DaemonClient client(cfg);
            // Poll a few times; stop early if nothing changes or no queue
            uint64_t lastGen = 0, lastFail = 0, lastQ = 0, lastBatches = 0;
            bool printedHeader = false;
            for (int i = 0; i < 8; ++i) {
                GetStatsRequest req;
                req.detailed = false;
                req.showFileTypes = false;
                std::promise<Result<GetStatsResponse>> prom;
                auto fut = prom.get_future();
                boost::asio::co_spawn(
                    boost::asio::system_executor{},
                    [&]() -> boost::asio::awaitable<void> {
                        auto rr = co_await client.getStats(req);
                        prom.set_value(std::move(rr));
                        co_return;
                    },
                    boost::asio::detached);
                auto r =
                    (fut.wait_for(std::chrono::milliseconds(1300)) == std::future_status::ready)
                        ? fut.get()
                        : Result<GetStatsResponse>(Error{ErrorCode::Timeout, "stats timeout"});
                if (!r)
                    break;
                const auto& st = r.value();
                auto getU64 = [&](const char* k) -> uint64_t {
                    auto it = st.additionalStats.find(k);
                    if (it == st.additionalStats.end())
                        return 0;
                    try {
                        return static_cast<uint64_t>(std::stoull(it->second));
                    } catch (...) {
                        return 0;
                    }
                };
                uint64_t gen = getU64("repair_embeddings_generated");
                uint64_t fail = getU64("repair_failed_operations");
                uint64_t q = getU64("repair_queue_depth");
                if (q == 0 && gen == 0 && fail == 0) {
                    // No repair activity; stop polling
                    break;
                }
                uint64_t batches = getU64("repair_batches_attempted");
                if (!printedHeader) {
                    std::cout << "\nEmbeddings Repair (live):\n";
                    printedHeader = true;
                }
                // Single compact line
                std::cout << "  generated=" << gen << " failed=" << fail << " pending=" << q
                          << " batches=" << batches << "\r" << std::flush;
                // Stop if not changing
                if (gen == lastGen && fail == lastFail && q == lastQ && batches == lastBatches) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(250));
                } else {
                    lastGen = gen;
                    lastFail = fail;
                    lastQ = q;
                    lastBatches = batches;
                    std::this_thread::sleep_for(std::chrono::milliseconds(300));
                }
            }
            if (printedHeader)
                std::cout << "\n";
        } catch (...) {
            // Ignore doctor progress errors; keep doctor quick
        }
    }

    static bool promptYesNo(const std::string& prompt, bool defaultYes) {
        std::cout << prompt;
        std::string line;
        std::getline(std::cin, line);

        if (line.empty())
            return defaultYes;
        char c = static_cast<char>(std::tolower(line[0]));
        if (c == 'y')
            return true;
        if (c == 'n')
            return false;
        return defaultYes;
    }

    static void checkInstalledModels() {
        namespace fs = std::filesystem;
        const char* home = std::getenv("HOME");
        if (!home)
            return;
        fs::path base = fs::path(home) / ".yams" / "models";
        std::error_code ec;
        if (!fs::exists(base, ec) || !fs::is_directory(base, ec))
            return;

        std::vector<std::string> warnings;
        for (const auto& entry : fs::directory_iterator(base, ec)) {
            if (!entry.is_directory())
                continue;
            const auto dir = entry.path();
            const auto name = dir.filename().string();

            const bool hasOnnx = fs::exists(dir / "model.onnx", ec);
            const bool hasConfig = fs::exists(dir / "config.json", ec) ||
                                   fs::exists(dir / "sentence_bert_config.json", ec);
            const bool hasTokenizer = fs::exists(dir / "tokenizer.json", ec);

            if (!hasOnnx)
                continue; // ignore non-model dirs

            // General guidance: config helps provider infer dim/seq/pooling
            if (!hasConfig) {
                warnings.emplace_back(
                    "  - " + name +
                    ": missing config.json — run 'yams model download --apply-config " + name +
                    " --force' to refresh model files");
            }

            // Nomic-specific guidance: tokenizer.json is needed for best compatibility
            if (name.find("nomic") != std::string::npos && !hasTokenizer) {
                warnings.emplace_back(
                    "  - " + name +
                    ": missing tokenizer.json — run 'yams model download --apply-config " + name +
                    " --force' to fetch tokenizer");
            }
        }

        if (!warnings.empty()) {
            std::cout << "\nModels (installed)\n-------------------\n";
            std::cout << "Some models are missing companion files required for robust inference.\n";
            for (const auto& w : warnings)
                std::cout << w << "\n";
            std::cout << "\nTip: You can also supply --hf <org/name> when downloading if the name "
                         "isn't recognized.\n";
        }
    }

    void checkEmbeddingDimMismatch() {
        namespace fs = std::filesystem;
        printHeader("Vectors Database");
        std::vector<StepResult> summary;

        // Resolve vectors.db under data path
        fs::path dataDir = cli_ ? cli_->getDataPath() : fs::path{};
        if (dataDir.empty()) {
            std::cout << "Data directory not configured; skipping DB dimension check.\n";
            return;
        }
        fs::path dbPath = dataDir / "vectors.db";
        // We no longer use a maintenance lock; we stop the daemon during writes
        std::error_code ec;
        if (!fs::exists(dbPath, ec)) {
            auto __r = resolveEmbeddingDim();
            size_t __dim = __r.first;
            std::string __src = __r.second;
            printStatusLine("Path", dbPath.string());
            std::cout << "No vectors.db found.\n";
            if (__dim == 0) {
                std::cout << "Could not resolve embedding dimension from config/model.\n";
                return;
            }
            if (promptYesNo(std::string("Create a new vectors.db now using dim=") +
                                std::to_string(__dim) + " (source: " + __src + ")? [y/N]: ",
                            false)) {
                // Ensure parent directory exists
                try {
                    fs::create_directories(dbPath.parent_path());
                } catch (...) {
                }
                // Stop daemon if running (best-effort)
                ensureDaemonStopped();

                // Step 1: Touch the DB file first (separate from SQLite open)
                try {
                    if (!fs::exists(dbPath)) {
                        std::ofstream f(dbPath);
                        f.flush();
                        f.close();
                    }
                    if (!fs::exists(dbPath)) {
                        std::cout << "\n✗ Failed to create file at " << dbPath << "\n";
                        return;
                    }
                    summary.push_back({"Create file", true, dbPath.string()});
                } catch (const std::exception& e) {
                    std::cout << "\n✗ File create error: " << e.what() << "\n";
                    return;
                }

                int timeout_ms = 30000; // default 30s for creation
                if (const char* env = std::getenv("YAMS_DOCTOR_VEC_TIMEOUT_MS")) {
                    try {
                        timeout_ms = std::max(500, std::stoi(env));
                    } catch (...) {
                    }
                }

                // Nudge backend timeouts higher than spinner to avoid double timeouts
                int init_ms = std::max(1000, timeout_ms - 3000);
                setEnvIfUnset("YAMS_SQLITE_VEC_INIT_TIMEOUT_MS", init_ms);
                setEnvIfUnset("YAMS_SQLITE_BUSY_TIMEOUT_MS", 10000);

                // For fast creation: skip vec init during open; load vec explicitly next
                setEnvIfUnset("YAMS_SQLITE_VEC_SKIP_INIT", 1);
                setEnvIfUnset("YAMS_SQLITE_MINIMAL_PRAGMAS", 1);
                yams::vector::SqliteVecBackend be;
                auto openOpt = runWithSpinner(
                    "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); },
                    timeout_ms);
                if (!openOpt) {
                    std::cout << "\n⚠ Timeout opening DB after " << timeout_ms << " ms";
                    if (promptYesNo(" — temporarily stop daemon to proceed? [y/N]: ", false)) {
                        std::cout << "Stopping daemon...\n";
                        ensureDaemonStopped();
                        openOpt = runWithSpinner(
                            "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); },
                            timeout_ms);
                        if (!openOpt) {
                            printError("Timeout opening DB after daemon stop");
                            printSummary("Vector DB Setup", summary);
                            return;
                        }
                    } else {
                        printSummary("Vector DB Setup", summary);
                        return;
                    }
                }
                if (!(*openOpt)) {
                    printError(std::string("Open failed: ") + openOpt->error().message);
                    printSummary("Vector DB Setup", summary);
                    return;
                }
                summary.push_back({"Open database", true, {}});
                auto vecOpt = runWithSpinner(
                    "Initializing sqlite-vec", [&]() { return be.ensureVecLoaded(); }, timeout_ms);
                if (!vecOpt) {
                    be.close();
                    printError(std::string("Timeout initializing sqlite-vec after ") +
                               std::to_string(timeout_ms) + " ms");
                    printSummary("Vector DB Setup", summary);
                    return;
                }
                if (!(*vecOpt)) {
                    printError(std::string("sqlite-vec init failed: ") + vecOpt->error().message);
                    be.close();
                    printSummary("Vector DB Setup", summary);
                    return;
                }
                summary.push_back({"Load sqlite-vec", true, {}});

                auto createOpt = runWithSpinner(
                    "Creating vector tables", [&]() { return be.createTables(__dim); }, timeout_ms);
                if (!createOpt) {
                    be.close();
                    printError(std::string("Timeout creating tables after ") +
                               std::to_string(timeout_ms) + " ms");
                    printSummary("Vector DB Setup", summary);
                    return;
                }
                if (!(*createOpt)) {
                    printError(std::string("Create failed: ") + createOpt->error().message);
                    be.close();
                    printSummary("Vector DB Setup", summary);
                    return;
                }
                summary.push_back(
                    {"Create vec schema", true, std::string("dim=") + std::to_string(__dim)});
                be.close();
                printSuccess(std::string("Created vectors.db (dim=") + std::to_string(__dim) + ")");
                printStatusLine("Next", "yams repair --embeddings");
                printSummary("Vector DB Setup", summary);
            }
            return;
        }

        // Read vec0 table DDL to infer stored dimension
        auto readDbDim = [&](const fs::path& p) -> std::optional<size_t> {
            sqlite3* db = nullptr;
            if (sqlite3_open(p.string().c_str(), &db) != SQLITE_OK) {
                if (db)
                    sqlite3_close(db);
                return std::nullopt;
            }
            const char* sql = "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
            sqlite3_stmt* stmt = nullptr;
            std::optional<size_t> out{};
            if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) == SQLITE_OK) {
                if (sqlite3_step(stmt) == SQLITE_ROW) {
                    const unsigned char* txt = sqlite3_column_text(stmt, 0);
                    if (txt) {
                        std::string ddl(reinterpret_cast<const char*>(txt));
                        auto pos = ddl.find("float[");
                        if (pos != std::string::npos) {
                            auto end = ddl.find(']', pos);
                            if (end != std::string::npos && end > pos + 6) {
                                std::string num = ddl.substr(pos + 6, end - (pos + 6));
                                try {
                                    size_t dim = static_cast<size_t>(std::stoul(num));
                                    out = dim;
                                } catch (...) {
                                }
                            }
                        }
                    }
                }
                sqlite3_finalize(stmt);
            }
            sqlite3_close(db);
            return out;
        };

        auto dbDim = readDbDim(dbPath);
        if (!dbDim) {
            // Existing DB file but no vector tables yet — offer to initialize tables
            auto resolved0 = resolveEmbeddingDim();
            size_t initDim = resolved0.first;
            std::string initSrc = resolved0.second;
            if (initDim == 0) {
                // Fallback to generator if available
                try {
                    auto emb = cli_ ? cli_->getEmbeddingGenerator() : nullptr;
                    if (emb) {
                        initDim = emb->getEmbeddingDimension();
                        initSrc = emb->getConfig().model_name;
                    }
                } catch (...) {
                }
            }
            std::cout << "No vector tables found in existing DB (" << dbPath << ").\n";
            if (initDim == 0) {
                std::cout << "Unable to resolve target dimension to initialize tables.\n";
                return;
            }

            if (promptYesNo(std::string("Initialize vector tables now using dim=") +
                                std::to_string(initDim) + " (source: " + initSrc + ")? [y/N]: ",
                            false)) {
                int timeout_ms = 15000;
                if (const char* env = std::getenv("YAMS_DOCTOR_VEC_TIMEOUT_MS")) {
                    try {
                        timeout_ms = std::max(500, std::stoi(env));
                    } catch (...) {
                    }
                }
                int init_ms = std::max(1000, timeout_ms - 3000);
                setEnvIfUnset("YAMS_SQLITE_VEC_INIT_TIMEOUT_MS", init_ms);
                setEnvIfUnset("YAMS_SQLITE_BUSY_TIMEOUT_MS", 10000);
                setEnvIfUnset("YAMS_SQLITE_VEC_SKIP_INIT", 1);
                setEnvIfUnset("YAMS_SQLITE_MINIMAL_PRAGMAS", 1);
                yams::vector::SqliteVecBackend be;
                auto openOpt = runWithSpinner(
                    "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); },
                    timeout_ms);
                if (!openOpt) {
                    std::cout << "\n⚠ Timeout opening DB after " << timeout_ms << " ms";
                    if (promptYesNo(" — temporarily stop daemon to proceed? [y/N]: ", false)) {
                        ensureDaemonStopped();
                        openOpt = runWithSpinner(
                            "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); },
                            timeout_ms);
                        if (!openOpt) {
                            std::cout << "\n✗ Timeout opening DB after daemon stop\n";
                            return;
                        }
                    } else {
                        return;
                    }
                }
                if (!(*openOpt)) {
                    std::cout << "\n✗ Open failed: " << openOpt->error().message << "\n";
                    return;
                }
                auto vecOpt = runWithSpinner(
                    "Initializing sqlite-vec", [&]() { return be.ensureVecLoaded(); }, timeout_ms);
                if (!vecOpt) {
                    be.close();
                    std::cout << "\n✗ Timeout initializing sqlite-vec after " << timeout_ms
                              << " ms\n";
                    return;
                }
                if (!(*vecOpt)) {
                    std::cout << "\n✗ sqlite-vec init failed: " << vecOpt->error().message << "\n";
                    be.close();
                    return;
                }
                auto createOpt = runWithSpinner(
                    "Creating vector tables", [&]() { return be.createTables(initDim); },
                    timeout_ms);
                if (!createOpt) {
                    be.close();
                    std::cout << "\n✗ Timeout creating tables after " << timeout_ms << " ms\n";
                    return;
                }
                if (!(*createOpt)) {
                    std::cout << "\n✗ Create failed: " << createOpt->error().message << "\n";
                    be.close();
                    return;
                }
                be.close();
                std::cout << "\n✓ Initialized vector tables (dim=" << initDim
                          << ").\nNext: yams repair --embeddings\n";
            }
            return;
        }

        // Determine target dimension for comparison (prefer config/env, fallback to generator)
        // Trigger app context creation to ensure EmbeddingGenerator is provisioned
        (void)(cli_ ? cli_->getAppContext() : nullptr);
        auto resolved = resolveEmbeddingDim();
        size_t targetDim = resolved.first;
        std::string targetSrc = resolved.second;
        if (targetDim == 0) {
            try {
                auto emb = cli_ ? cli_->getEmbeddingGenerator() : nullptr;
                if (emb) {
                    targetDim = emb->getEmbeddingDimension();
                    targetSrc = emb->getConfig().model_name;
                }
            } catch (...) {
            }
        }

        if (targetDim == 0) {
            std::cout
                << "DB dimension: " << *dbDim
                << ". Unable to resolve target dimension — ensure a model/config is available.\n";
            std::cout << "Hint: yams model download --apply-config <name> --hf <org/name>\n";
            return;
        }

        std::cout << "DB dimension: " << *dbDim << ", Target ('"
                  << (targetSrc.empty() ? "auto" : targetSrc) << "') dimension: " << targetDim
                  << "\n";

        // Non-interactive fixes via flags
        if (stopDaemon_) {
            ensureDaemonStopped();
        }
        if (fixConfigDims_) {
            auto cfgPath2 = resolveConfigPath();
            if (writeOrReplaceConfigDims(cfgPath2, targetDim)) {
                printSuccess("Updated config dims");
            } else {
                printError("Failed to update config dims");
            }
        }
        if (recreateVectors_) {
            int timeout_ms2 = 15000;
            if (const char* env = std::getenv("YAMS_DOCTOR_VEC_TIMEOUT_MS")) {
                try {
                    timeout_ms2 = std::max(500, std::stoi(env));
                } catch (...) {
                }
            }
            int init_ms2 = std::max(1000, timeout_ms2 - 3000);
            setEnvIfUnset("YAMS_SQLITE_VEC_INIT_TIMEOUT_MS", init_ms2);
            setEnvIfUnset("YAMS_SQLITE_BUSY_TIMEOUT_MS", 10000);
            setEnvIfUnset("YAMS_SQLITE_VEC_SKIP_INIT", 1);
            setEnvIfUnset("YAMS_SQLITE_MINIMAL_PRAGMAS", 1);
            yams::vector::SqliteVecBackend be2;
            auto openOpt2 = runWithSpinner(
                "Opening vectors.db", [&]() { return be2.initialize(dbPath.string()); },
                timeout_ms2);
            if (openOpt2 && *openOpt2) {
                (void)be2.ensureVecLoaded();
                size_t useDim = recreateDim_ ? *recreateDim_ : targetDim;
                (void)runWithSpinner(
                    "Dropping existing vector tables", [&]() { return be2.dropTables(); },
                    timeout_ms2);
                (void)runWithSpinner(
                    "Creating vector tables", [&]() { return be2.createTables(useDim); },
                    timeout_ms2);
                be2.close();
                // Align config dims to new target
                try {
                    auto cfgPathNI = resolveConfigPath();
                    if (writeOrReplaceConfigDims(cfgPathNI, useDim)) {
                        printSuccess("Updated config dims to match recreated schema");
                    } else {
                        printWarn("Config dims update skipped (write failed)");
                    }
                } catch (...) {
                    printWarn("Config dims update skipped (exception)");
                }
                // Write sentinel for daemon to acknowledge recent realign
                try {
                    if (cli_)
                        writeVectorSentinel(cli_->getDataPath(), useDim);
                } catch (...) {
                }
                printSuccess(std::string("Recreated vector tables (dim=") + std::to_string(useDim) +
                             ")");
                return; // Non-interactive path completes here
            } else {
                printError("Failed to open vectors.db for recreation");
            }
        }
        // Audit config and offer to align dims
        auto cfgPath = resolveConfigPath();
        auto cfgDims = readConfigDims(cfgPath);
        bool cfgMismatch = (cfgDims.embeddings && *cfgDims.embeddings != targetDim) ||
                           (cfgDims.vector_db && *cfgDims.vector_db != targetDim) ||
                           (cfgDims.index && *cfgDims.index != targetDim);
        if (cfgMismatch) {
            printWarn("Config dims differ from target dimension");
            printStatusLine("Config path", cfgPath.string());
            if (cfgDims.embeddings)
                printStatusLine("embeddings.embedding_dim", std::to_string(*cfgDims.embeddings));
            if (cfgDims.vector_db)
                printStatusLine("vector_database.embedding_dim",
                                std::to_string(*cfgDims.vector_db));
            if (cfgDims.index)
                printStatusLine("vector_index.dimension", std::to_string(*cfgDims.index));
            if (promptYesNo(std::string("Align all config dims to target ( ") +
                                std::to_string(targetDim) + ")? [y/N]: ",
                            false)) {
                if (writeOrReplaceConfigDims(cfgPath, targetDim)) {
                    printSuccess("Updated config dims");
                } else {
                    printError("Failed to update config dims");
                }
            }
        }

        if (*dbDim != targetDim) {
            std::cout << "\n⚠ Embedding dimension mismatch detected.\n";
            std::cout << "- Stored vectors are " << *dbDim << "-d but target requires " << targetDim
                      << "-d.\n";

            if (promptYesNo("\nRecreate vectors.db now with the correct dimension? [y/N]: ",
                            false)) {
                int timeout_ms = 15000;
                if (const char* env = std::getenv("YAMS_DOCTOR_VEC_TIMEOUT_MS")) {
                    try {
                        timeout_ms = std::max(500, std::stoi(env));
                    } catch (...) {
                    }
                }
                int init_ms = std::max(1000, timeout_ms - 3000);
                setEnvIfUnset("YAMS_SQLITE_VEC_INIT_TIMEOUT_MS", init_ms);
                setEnvIfUnset("YAMS_SQLITE_BUSY_TIMEOUT_MS", 10000);

                setEnvIfUnset("YAMS_SQLITE_VEC_SKIP_INIT", 1);
                setEnvIfUnset("YAMS_SQLITE_MINIMAL_PRAGMAS", 1);
                yams::vector::SqliteVecBackend be;
                auto openOpt = runWithSpinner(
                    "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); },
                    timeout_ms);
                if (!openOpt) {
                    std::cout << "\n⚠ Timeout opening DB after " << timeout_ms << " ms";
                    if (promptYesNo(" — temporarily stop daemon to proceed? [y/N]: ", false)) {
                        ensureDaemonStopped();
                        openOpt = runWithSpinner(
                            "Opening vectors.db", [&]() { return be.initialize(dbPath.string()); },
                            timeout_ms);
                        if (!openOpt) {
                            std::cout << "\n✗ Timeout opening DB after daemon stop\n";
                            return;
                        }
                    } else {
                        return;
                    }
                }
                if (!(*openOpt)) {
                    std::cout << "\n✗ Open failed: " << openOpt->error().message << "\n";
                    return;
                }
                auto vecOpt = runWithSpinner(
                    "Initializing sqlite-vec", [&]() { return be.ensureVecLoaded(); }, timeout_ms);
                if (!vecOpt) {
                    be.close();
                    std::cout << "\n✗ Timeout initializing sqlite-vec after " << timeout_ms
                              << " ms\n";
                    return;
                }
                if (!(*vecOpt)) {
                    std::cout << "\n✗ sqlite-vec init failed: " << vecOpt->error().message << "\n";
                    be.close();
                    return;
                }
                auto dropOpt = runWithSpinner(
                    "Dropping existing vector tables", [&]() { return be.dropTables(); },
                    timeout_ms);
                if (!dropOpt) {
                    be.close();
                    std::cout << "\n✗ Timeout dropping tables after " << timeout_ms << " ms\n";
                    return;
                }
                if (!(*dropOpt)) {
                    std::cout << "\n✗ Drop failed: " << dropOpt->error().message << "\n";
                    be.close();
                    return;
                }
                auto createOpt = runWithSpinner(
                    "Creating vector tables", [&]() { return be.createTables(targetDim); },
                    timeout_ms);
                if (!createOpt) {
                    be.close();
                    std::cout << "\n✗ Timeout creating tables after " << timeout_ms << " ms\n";
                    return;
                }
                if (!(*createOpt)) {
                    std::cout << "\n✗ Create failed: " << createOpt->error().message << "\n";
                    be.close();
                    return;
                }
                be.close();

                // Align config dims to new target to avoid subsequent mismatch warnings
                try {
                    auto cfgPath3 = resolveConfigPath();
                    if (writeOrReplaceConfigDims(cfgPath3, targetDim)) {
                        printSuccess("Updated config dims to match recreated schema");
                    } else {
                        printWarn("Config dims update skipped (write failed)");
                    }
                } catch (...) {
                    printWarn("Config dims update skipped (exception)");
                }
                // Write sentinel to signal recent realign
                try {
                    if (cli_)
                        writeVectorSentinel(cli_->getDataPath(), targetDim);
                } catch (...) {
                }

                std::cout << "\n✓ Recreated vector tables in existing DB (dim=" << targetDim
                          << ").\nNext: yams repair --embeddings\n";
                // Offer to restart daemon now to avoid stale in-memory dims
                if (promptYesNo("Restart daemon now to apply changes? [y/N]: ", false)) {
                    ensureDaemonStopped();
                    try {
                        using namespace yams::daemon;
                        ClientConfig cfg;
                        if (cli_)
                            cfg.dataDir = cli_->getDataPath();
                        auto sr = DaemonClient::startDaemon(cfg);
                        if (!sr)
                            printWarn(std::string("Daemon restart failed: ") + sr.error().message);
                        else
                            printSuccess("Daemon restarted");
                    } catch (const std::exception& e) {
                        printWarn(std::string("Daemon restart exception: ") + e.what());
                    }
                }
            } else {
                std::cout << "- Then re-generate embeddings:\n"
                          << "    yams doctor repair --embeddings\n";
            }
        } else {
            std::cout << "✓ Embedding dimension matches model.\n";
        }
    }

    YamsCLI* cli_{nullptr};
    bool fixEmbeddings_{false};
    bool fixFts5_{false};
    bool fixGraph_{false};
    bool fixAll_{false};
    bool fixAllTop_{false};
    std::string pluginArg_;
    std::string ifaceId_;
    uint32_t ifaceVersion_{0};
    bool noDaemonProbe_{false};
    // Non-interactive fix flags
    bool fixConfigDims_{false};
    bool recreateVectors_{false};
    std::optional<size_t> recreateDim_{};
    bool stopDaemon_{false};
};

// Factory
std::unique_ptr<ICommand> createDoctorCommand() {
    return std::make_unique<DoctorCommand>();
}

// Build/repair knowledge graph using tags/metadata (non-destructive)
Result<void> DoctorCommand::repairGraph() {
    try {
        if (!cli_)
            return Error{ErrorCode::InvalidState, std::string("CLI not initialized")};
        auto repo = cli_->getMetadataRepository();
        auto kg = cli_->getKnowledgeGraphStore();
        if (!repo)
            return Error{ErrorCode::NotInitialized, "Metadata repository unavailable"};
        if (!kg)
            return Error{ErrorCode::NotInitialized, "Knowledge graph store unavailable"};

        std::cout << "Building knowledge graph from tags/metadata...\n";
        auto docsR = repo->findDocumentsByPath("%");
        if (!docsR)
            return Error{ErrorCode::InternalError,
                         std::string("List docs failed: ") + docsR.error().message};
        const auto& docs = docsR.value();
        size_t processed = 0, updated = 0, skipped = 0;

        auto upsertNode =
            [&](const std::string& key, const std::string& type,
                const std::optional<std::string>& label,
                const std::optional<std::string>& propsJson) -> std::optional<int64_t> {
            yams::metadata::KGNode node;
            node.nodeKey = key;
            node.type = type;
            node.label = label;
            node.properties = propsJson;
            auto r = kg->upsertNode(node);
            if (!r) {
                return std::nullopt;
            }
            return r.value();
        };

        for (const auto& d : docs) {
            ++processed;
            std::unordered_set<int64_t> existing;
            {
                auto er = kg->getDocEntitiesForDocument(d.id, 5000, 0);
                if (er) {
                    for (const auto& e : er.value())
                        if (e.nodeId)
                            existing.insert(*e.nodeId);
                }
            }
            std::vector<yams::metadata::DocEntity> batch;
            batch.reserve(32);

            // Tags
            if (auto tagsR = repo->getDocumentTags(d.id)) {
                for (const auto& tag : tagsR.value()) {
                    std::string key = std::string("tag:") + tag;
                    auto nid = upsertNode(key, "tag", tag, std::nullopt);
                    if (nid && existing.find(*nid) == existing.end()) {
                        yams::metadata::DocEntity de;
                        de.documentId = d.id;
                        de.nodeId = *nid;
                        de.entityText = tag;
                        de.confidence = 1.0f;
                        de.extractor = "meta";
                        batch.push_back(std::move(de));
                    }
                }
            }

            // MIME
            if (!d.mimeType.empty()) {
                std::string key = std::string("mime:") + d.mimeType;
                auto nid = upsertNode(key, "mime", d.mimeType, std::nullopt);
                if (nid && existing.find(*nid) == existing.end()) {
                    yams::metadata::DocEntity de;
                    de.documentId = d.id;
                    de.nodeId = *nid;
                    de.entityText = d.mimeType;
                    de.confidence = 1.0f;
                    de.extractor = "meta";
                    batch.push_back(std::move(de));
                }
            }
            // Extension
            if (!d.fileExtension.empty()) {
                std::string ext =
                    d.fileExtension[0] == '.' ? d.fileExtension.substr(1) : d.fileExtension;
                std::string key = std::string("ext:") + ext;
                auto nid = upsertNode(key, "ext", ext, std::nullopt);
                if (nid && existing.find(*nid) == existing.end()) {
                    yams::metadata::DocEntity de;
                    de.documentId = d.id;
                    de.nodeId = *nid;
                    de.entityText = ext;
                    de.confidence = 1.0f;
                    de.extractor = "meta";
                    batch.push_back(std::move(de));
                }
            }

            // Key/Value metadata
            if (auto mdR = repo->getAllMetadata(d.id)) {
                for (const auto& [k, v] : mdR.value()) {
                    std::string vv = v.asString();
                    if (vv.empty())
                        continue;
                    (void)upsertNode(std::string("meta_key:") + k, "meta_key", k, std::nullopt);
                    std::string nodeKey = std::string("meta_val:") + k + ":" + vv;
                    std::string props = std::string("{\"key\":\"") + k + "\"}";
                    auto nid = upsertNode(nodeKey, "meta_val", vv, props);
                    if (nid && existing.find(*nid) == existing.end()) {
                        yams::metadata::DocEntity de;
                        de.documentId = d.id;
                        de.nodeId = *nid;
                        de.entityText = vv;
                        de.confidence = 1.0f;
                        de.extractor = "meta";
                        batch.push_back(std::move(de));
                    }
                }
            }

            if (!batch.empty()) {
                auto ar = kg->addDocEntities(batch);
                if (ar)
                    ++updated;
                else
                    ++skipped;
            } else {
                ++skipped;
            }

            if (processed % 100 == 0) {
                std::cout << "  processed=" << processed << " updated=" << updated
                          << " skipped=" << skipped << "\n";
            }
        }
        std::cout << "Done. processed=" << processed << " updated=" << updated
                  << " skipped=" << skipped << "\n";
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}
} // namespace yams::cli
