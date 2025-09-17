#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/system_executor.hpp>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/resource/plugin_loader.h>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include "yams/cli/prompt_util.h"
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
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <CLI/CLI.hpp>
#include <yams/plugins/model_provider_v1.h>

namespace yams::cli {

class DoctorCommand : public ICommand {
public:
    std::string getName() const override { return "doctor"; }
    std::string getDescription() const override {
        return "Diagnose daemon connectivity and plugin health";
    }
    void registerCommand(CLI::App& app, YamsCLI* cli) override;

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
        // Prefer existing DB schema when present
        try {
            if (cli_) {
                fs::path dbPath = cli_->getDataPath() / "vectors.db";
                if (fs::exists(dbPath)) {
                    sqlite3* db = nullptr;
                    if (sqlite3_open(dbPath.string().c_str(), &db) == SQLITE_OK && db) {
                        const char* sql =
                            "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
                        sqlite3_stmt* stmt = nullptr;
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
                                                dim = static_cast<size_t>(std::stoul(num));
                                                src = "db";
                                            } catch (...) {
                                            }
                                        }
                                    }
                                }
                            }
                            sqlite3_finalize(stmt);
                        }
                        sqlite3_close(db);
                    }
                }
            }
        } catch (...) {
        }
        if (dim > 0)
            return {dim, src};
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
        // Generator (next best)
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

    // (Removed clearEmbeddingDegraded helper; subcommand placeholder was eliminated. If
    // reintroduced, prefer declaring the method before use or moving implementation
    // out-of-line to avoid ordering issues in inline class definition bodies.)
    void clearEmbeddingDegraded() {
        using namespace yams::daemon;
        try {
            // Connect to daemon
            ClientConfig cfg;
            if (cli_)
                if (cli_->hasExplicitDataDir()) {
                    cfg.dataDir = cli_->getDataPath();
                }
            cfg.requestTimeout = std::chrono::seconds(10);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                std::cout << "Daemon unavailable: " << leaseRes.error().message << "\n";
                return;
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            // Updated: run asynchronous daemon client call via generic run_result helper
            auto status = yams::cli::run_result(client.status(), std::chrono::seconds(3));
            if (!status) {
                std::cout << "Daemon unavailable: " << status.error().message << "\n";
                return;
            }
            bool degraded = false;
            std::string reason;
            try {
                const auto& st = status.value();
                auto it = st.readinessStates.find("embedding_degraded");
                degraded = (it != st.readinessStates.end() && it->second);
                // Reason flags (best-effort)
                for (const auto& kv : st.readinessStates) {
                    if (kv.second && kv.first.rfind("embedding_degraded_reason_", 0) == 0) {
                        reason = kv.first.substr(std::string("embedding_degraded_reason_").size());
                        break;
                    }
                }
            } catch (...) {
            }
            if (!degraded) {
                std::cout << "Embedding subsystem is not degraded. Nothing to clear.\n";
                return;
            }
            std::cout << "Embedding subsystem is degraded";
            if (!reason.empty())
                std::cout << " (reason: " << reason << ")";
            std::cout << "\n";

            // Determine target model to load: prefer loaded model or configured preferred model
            std::string targetModel;
            try {
                const auto& st = status.value();
                for (const auto& m : st.models) {
                    if (m.name != "(provider)") {
                        targetModel = m.name;
                        break;
                    }
                }
            } catch (...) {
            }
            if (targetModel.empty()) {
                // Read from config or env
                if (const char* p = std::getenv("YAMS_PREFERRED_MODEL"))
                    targetModel = p;
                if (targetModel.empty()) {
                    // Fallback: prefer common local models
                    const char* home = std::getenv("HOME");
                    if (home) {
                        namespace fs = std::filesystem;
                        fs::path base = fs::path(home) / ".yams" / "models";
                        std::vector<std::string> prefs{"nomic-embed-text-v1.5",
                                                       "nomic-embed-text-v1", "all-MiniLM-L6-v2",
                                                       "all-mpnet-base-v2"};
                        for (const auto& p : prefs) {
                            if (std::filesystem::exists(base / p / "model.onnx")) {
                                targetModel = p;
                                break;
                            }
                        }
                    }
                }
            }
            if (targetModel.empty()) {
                std::cout
                    << "No target model found (set YAMS_PREFERRED_MODEL or install a model).\n";
                return;
            }

            // Prompt for confirmation
            if (!yams::cli::prompt_yes_no("Load model '" + targetModel +
                                          "' to clear degraded? [Y/n] ")) {
                std::cout << "Cancelled.\n";
                return;
            }

            // Issue LoadModel
            LoadModelRequest lreq;
            lreq.modelName = targetModel;
            lreq.preload = true;
            auto lres = yams::cli::run_result(client.loadModel(lreq), std::chrono::seconds(30));
            if (!lres) {
                std::cout << "Model load failed: " << lres.error().message << "\n";
                return;
            }

            // Verify status again
            auto s2 = yams::cli::run_result(client.status(), std::chrono::seconds(5));
            bool cleared = false;
            if (s2) {
                try {
                    const auto& st2 = s2.value();
                    auto it2 = st2.readinessStates.find("embedding_degraded");
                    cleared = (it2 == st2.readinessStates.end() || !it2->second);
                } catch (...) {
                }
            }
            if (cleared) {
                std::cout << "Degraded cleared.\n";
            } else {
                std::cout << "Degraded still active. Check daemon logs for details.\n";
            }
        } catch (const std::exception& e) {
            std::cout << "Clear degraded error: " << e.what() << "\n";
        }
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
                if (cli_->hasExplicitDataDir()) {
                    ccfg.dataDir = cli_->getDataPath();
                }
            ccfg.requestTimeout = std::chrono::seconds(5);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(ccfg);
            if (!leaseRes)
                return;
            auto leaseHandle = std::move(leaseRes.value());
            auto& shut = **leaseHandle;
            (void)yams::cli::run_result(shut.shutdown(true), std::chrono::seconds(6));
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
                                if (cli_->hasExplicitDataDir()) {
                                    cfg.dataDir = cli_->getDataPath();
                                }
                                // Configurable RPC timeout (default 60s)
                                int rpc_ms = 60000;
                                if (const char* env = std::getenv("YAMS_DOCTOR_RPC_TIMEOUT_MS")) {
                                    try {
                                        rpc_ms = std::max(1000, std::stoi(env));
                                    } catch (...) {
                                    }
                                }
                                cfg.requestTimeout = std::chrono::milliseconds(rpc_ms);
                                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                                if (!leaseRes) {
                                    throw std::runtime_error(leaseRes.error().message);
                                }
                                auto leaseHandle = std::move(leaseRes.value());
                                auto& client = **leaseHandle;
                                yams::daemon::EmbedDocumentsRequest ed;
                                ed.modelName = ""; // let server/provider decide
                                ed.documentHashes = missing.value();
                                ed.batchSize = static_cast<uint32_t>(rcfg.batchSize);
                                ed.skipExisting = rcfg.skipExisting;
                                // Overall wait limit (2x per-request timeout, minimum 30s)
                                int overall_ms = std::max(30000, 2 * rpc_ms);
                                auto er =
                                    yams::cli::run_result<yams::daemon::EmbedDocumentsResponse>(
                                        client.streamingEmbedDocuments(ed),
                                        std::chrono::milliseconds(overall_ms));
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
    // repairGraph declared earlier in the class

    // Minimal daemon check: connect and get status
    void checkDaemon() {
        using namespace yams::daemon;
        try {
            yams::daemon::ClientConfig cfg;
            if (cli_)
                if (cli_->hasExplicitDataDir()) {
                    cfg.dataDir = cli_->getDataPath();
                }
            cfg.requestTimeout = std::chrono::milliseconds(3000);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                std::cout << "Daemon: UNAVAILABLE - " << leaseRes.error().message << "\n";
                return;
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            std::cout << "Daemon: ";
            auto s = yams::cli::run_result<yams::daemon::StatusResponse>(client.status(),
                                                                         std::chrono::seconds(3));
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

            // Show resources (memory/cpu) and worker/mux details. Print conditionally:
            // if metrics are not yet available, show a concise pending line instead.
            try {
                const auto& phase =
                    (st.lifecycleState.empty() ? st.overallStatus : st.lifecycleState);
                double ram = st.memoryUsageMb;
                double cpu = st.cpuUsagePercent;
                auto wt = st.requestCounts.find("worker_threads");
                auto wa = st.requestCounts.find("worker_active");
                auto wq = st.requestCounts.find("worker_queued");
                bool haveWorkers = (wt != st.requestCounts.end()) ||
                                   (wa != st.requestCounts.end()) || (wq != st.requestCounts.end());
                bool haveResources = (ram > 0.0 || cpu > 0.0);
                if (haveResources) {
                    std::cout << "  Resources:    RAM=" << std::fixed << std::setprecision(1) << ram
                              << " MB, CPU=" << std::setprecision(0) << cpu << "%\n";
                } else {
                    if (!phase.empty()) {
                        if (st.retryAfterMs > 0) {
                            std::cout << "  Resources:    pending (daemon " << phase
                                      << ", retry after " << st.retryAfterMs << " ms)\n";
                        } else {
                            std::cout << "  Resources:    pending (daemon " << phase << ")\n";
                        }
                    }
                }
                if (haveWorkers) {
                    size_t threads = wt != st.requestCounts.end() ? wt->second : 0;
                    size_t active = wa != st.requestCounts.end() ? wa->second : 0;
                    size_t queued = wq != st.requestCounts.end() ? wq->second : 0;
                    std::cout << "  Workers:      threads=" << threads << " active=" << active
                              << " queued=" << queued << "\n";
                } else if (!st.ready) {
                    std::cout << "  Workers:      pending\n";
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
                yams::daemon::ClientConfig cfg;
                if (cli_)
                    if (cli_->hasExplicitDataDir()) {
                        cfg.dataDir = cli_->getDataPath();
                    }
                cfg.requestTimeout = std::chrono::milliseconds(4000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (!leaseRes) {
                    throw std::runtime_error(leaseRes.error().message);
                }
                auto leaseHandle = std::move(leaseRes.value());
                auto& client = **leaseHandle;
                PluginLoadRequest req;
                req.pathOrName = target.string();
                req.dryRun = true;
                auto r = yams::cli::run_result<yams::daemon::PluginLoadResponse>(
                    client.call(req), std::chrono::seconds(4));
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
        // Minimal structured recommendations example (will expand in future audits)
        yams::cli::RecommendationBuilder recs;
        // Keep doctor fast and non-invasive by default:
        // - Daemon status
        // - Installed models
        // - Vector DB dimension check
        checkDaemon();
        checkInstalledModels();
        checkEmbeddingDimMismatch();
        // Show embedding runtime from daemon status (best-effort)
        try {
            using namespace yams::daemon;
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(ClientConfig{});
            if (!leaseRes)
                throw std::runtime_error(leaseRes.error().message);
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            auto st = yams::cli::run_result(client.status(), std::chrono::seconds(3));
            if (st) {
                const auto& s = st.value();
                std::cout << "\nEmbedding Runtime\n-----------------\n";
                std::cout << "Available: " << (s.embeddingAvailable ? "yes" : "no") << "\n";
                if (!s.embeddingBackend.empty())
                    std::cout << "Backend : " << s.embeddingBackend << "\n";
                if (!s.embeddingModel.empty())
                    std::cout << "Model   : " << s.embeddingModel << "\n";
                if (!s.embeddingModelPath.empty())
                    std::cout << "Path    : " << s.embeddingModelPath << "\n";
                if (s.embeddingDim > 0)
                    std::cout << "Dim     : " << s.embeddingDim << "\n";
                if (s.embeddingThreadsIntra > 0 || s.embeddingThreadsInter > 0)
                    std::cout << "Threads : " << s.embeddingThreadsIntra << "/"
                              << s.embeddingThreadsInter << "\n";
            }
        } catch (...) {
        }

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
                    std::string msg = "Knowledge graph empty — run 'yams doctor repair --graph' to "
                                      "build from tags/metadata";
                    std::cout << msg << "\n";
                    recs.warning("DOCTOR_KG_EMPTY", msg);
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

        // Show currently loaded plugins, mirroring `yams plugin list` logic
        try {
            using namespace yams::daemon;
            yams::daemon::ClientConfig cfg;
            if (cli_ && cli_->hasExplicitDataDir()) {
                cfg.dataDir = cli_->getDataPath();
            }
            cfg.requestTimeout = std::chrono::milliseconds(3000);
            cfg.enableChunkedResponses = false;
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes)
                throw std::runtime_error(leaseRes.error().message);
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;

            auto sres = yams::cli::run_result<StatusResponse>(client.status(),
                                                              std::chrono::milliseconds(3000));
            GetStatsResponse stats{};
            bool haveStats = false;
            {
                GetStatsRequest req;
                req.detailed = true; // request plugin JSON snapshot too
                req.showFileTypes = false;
                auto gres = yams::cli::run_result<GetStatsResponse>(
                    client.call(req), std::chrono::milliseconds(3000));
                if (gres) {
                    stats = gres.value();
                    haveStats = true;
                }
            }
            // If neither typed providers nor plugins_json are present, ask daemon to scan and wait
            // briefly
            try {
                auto haveTyped = (sres && !sres.value().providers.empty());
                auto haveJson = false;
                if (haveStats) {
                    auto it0 = stats.additionalStats.find("plugins_json");
                    haveJson = (it0 != stats.additionalStats.end() && !it0->second.empty());
                }
                if (!(haveTyped || haveJson)) {
                    // Request a scan of default search paths
                    yams::daemon::PluginScanRequest scanReq;
                    (void)yams::cli::run_result<yams::daemon::PluginScanResponse>(
                        client.call(scanReq), std::chrono::milliseconds(3000));
                    // Poll up to ~3s for providers or json to appear
                    for (int i = 0; i < 6; ++i) {
                        sres = yams::cli::run_result<StatusResponse>(
                            client.status(), std::chrono::milliseconds(1500));
                        if (sres) {
                            const auto& st = sres.value();
                            if (!st.providers.empty())
                                break;
                            auto itp = st.readinessStates.find("plugins");
                            if (itp != st.readinessStates.end() && itp->second)
                                break;
                            auto itmp = st.readinessStates.find("model_provider");
                            if (itmp != st.readinessStates.end() && itmp->second)
                                break;
                        }
                        GetStatsRequest req2;
                        req2.detailed = true;
                        req2.showFileTypes = false;
                        auto gres2 = yams::cli::run_result<GetStatsResponse>(
                            client.call(req2), std::chrono::milliseconds(1500));
                        if (gres2) {
                            stats = gres2.value();
                            auto itj = stats.additionalStats.find("plugins_json");
                            if (itj != stats.additionalStats.end() && !itj->second.empty())
                                break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    }
                }
            } catch (...) {
                // best-effort; continue
            }

            std::cout << "\nLoaded Plugins\n";
            std::cout << "---------------\n";

            // Prefer typed providers list from StatusResponse
            if (sres && !sres.value().providers.empty()) {
                const auto& st = sres.value();
                for (const auto& p : st.providers) {
                    std::cout << "- " << p.name;
                    if (p.isProvider)
                        std::cout << " [provider]";
                    if (!p.ready)
                        std::cout << " [not-ready]";
                    if (p.degraded)
                        std::cout << " [degraded]";
                    if (p.modelsLoaded > 0)
                        std::cout << " models=" << p.modelsLoaded;
                    if (!p.error.empty())
                        std::cout << " error=\"" << p.error << "\"";
                    std::cout << "\n";
                }
            } else if (haveStats) {
                // Fallback to JSON snapshot embedded in stats
                auto it = stats.additionalStats.find("plugins_json");
                if (it != stats.additionalStats.end() && !it->second.empty()) {
                    try {
                        auto j = nlohmann::json::parse(it->second, nullptr, false);
                        if (!j.is_discarded() && j.is_array() && !j.empty()) {
                            for (const auto& rec : j) {
                                std::string name = rec.value("name", std::string("(unknown)"));
                                std::string line = "- " + name;
                                try {
                                    if (rec.value("provider", false))
                                        line += " [provider]";
                                    if (rec.value("degraded", false))
                                        line += " [degraded]";
                                    if (rec.contains("models_loaded")) {
                                        int models = 0;
                                        try {
                                            models = rec["models_loaded"].get<int>();
                                        } catch (...) {
                                        }
                                        if (models > 0)
                                            line += " models=" + std::to_string(models);
                                    }
                                    if (rec.contains("error")) {
                                        if (rec["error"].is_string()) {
                                            auto err = rec["error"].get<std::string>();
                                            if (!err.empty())
                                                line += " error=\"" + err + "\"";
                                        } else {
                                            line += " error=" + rec["error"].dump();
                                        }
                                    }
                                } catch (...) {
                                }
                                std::cout << line << "\n";
#ifdef __APPLE__
                                // macOS diagnostic: surface DYLIB linkage issues for degraded
                                try {
                                    bool __degraded = rec.value("degraded", false);
                                    std::string __pluginPath = rec.value("path", std::string());
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

                // Show any loader errors captured in stats
                auto eit = stats.additionalStats.find("plugins_error");
                if (eit != stats.additionalStats.end() && !eit->second.empty()) {
                    std::cout << "(plugins error: " << eit->second << ")\n";
                }
            } else {
                std::cout << "(none loaded)\n";
            }
        } catch (...) {
            // keep doctor resilient
        }

        // Emit collected recommendations (text only for now)
        if (!recs.empty()) {
            yams::cli::printRecommendationsText(recs, std::cout);
        }

        std::cout << "\nHint: run 'yams doctor plugin <path|name>' for a deep plugin check.\n";

        // Compact live repair progress (short poll). Non-blocking when no repair activity.
        try {
            using namespace yams::daemon;
            yams::daemon::ClientConfig cfg;
            cfg.requestTimeout = std::chrono::milliseconds(1200);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes)
                throw std::runtime_error(leaseRes.error().message);
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            // Poll a few times; stop early if nothing changes or no queue
            uint64_t lastGen = 0, lastFail = 0, lastQ = 0, lastBatches = 0;
            bool printedHeader = false;
            for (int i = 0; i < 8; ++i) {
                GetStatsRequest req;
                req.detailed = false;
                req.showFileTypes = false;
                auto r = yams::cli::run_result<GetStatsResponse>(client.getStats(req),
                                                                 std::chrono::milliseconds(1300));
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

    // Legacy prompt removed; using prompt_yes_no from prompt_util.h

    static void checkInstalledModels() {
        namespace fs = std::filesystem;
        const char* home = std::getenv("HOME");
        if (!home)
            return;
        fs::path base = fs::path(home) / ".yams" / "models";
        std::error_code ec;
        if (!fs::exists(base, ec) || !fs::is_directory(base, ec))
            return;

        std::vector<std::string> warnings; // legacy textual list (kept for existing output)
        struct ModelRec {
            std::string name;
            int dim; // 384/768 or -1 for unknown
        };
        std::vector<ModelRec> models;
        // Also build structured recommendations (printed later if any)
        yams::cli::RecommendationBuilder recBuilder;
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

            // Record model with a heuristic dimension for operator visibility
            int dim = -1;
            if (name == "all-MiniLM-L6-v2" || name.find("MiniLM") != std::string::npos)
                dim = 384;
            else if (name == "all-mpnet-base-v2" || name.find("mpnet") != std::string::npos ||
                     name.find("nomic") != std::string::npos)
                dim = 768;
            models.push_back({name, dim});

            // General guidance: config helps provider infer dim/seq/pooling
            if (!hasConfig) {
                warnings.emplace_back(
                    "  - " + name +
                    ": missing config.json — run 'yams model download --apply-config " + name +
                    " --force'");
                recBuilder.warning(
                    "DOCTOR_MODEL_MISSING_CONFIG",
                    name + ": missing config.json; run 'yams model download --apply-config " +
                        name + " --force'",
                    "Model may have default/incorrect embedding dim inference");
            }

            // Nomic-specific guidance: tokenizer.json is needed for best compatibility
            if (name.find("nomic") != std::string::npos && !hasTokenizer) {
                warnings.emplace_back(
                    "  - " + name +
                    ": missing tokenizer.json — run 'yams model download --apply-config " + name +
                    " --force'");
                recBuilder.info(
                    "DOCTOR_MODEL_MISSING_TOKENIZER",
                    name + ": missing tokenizer.json; download to ensure consistent tokenization",
                    "Tokenizer improves text splitting and pooling accuracy");
            }
        }

        // Print installed models with inferred dimensions
        if (!models.empty()) {
            std::cout << "\nModels (installed)\n-------------------\n";
            for (const auto& m : models) {
                if (m.dim > 0)
                    std::cout << "- " << m.name << ": dim=" << m.dim << " (heuristic)\n";
                else
                    std::cout << "- " << m.name << ": dim=unknown\n";
            }
        }
        if (!warnings.empty()) {
            std::cout
                << "\nSome models are missing companion files required for robust inference.\n";
            for (const auto& w : warnings)
                std::cout << w << "\n";
            std::cout << "\nTip: You can also supply --hf <org/name> when downloading if the name "
                         "isn't recognized.\n";
            if (!recBuilder.empty()) {
                std::cout << "\nRecommendations (models):\n";
                yams::cli::printRecommendationsText(recBuilder, std::cout);
            }
        }
    }

    void checkEmbeddingDimMismatch() {
        namespace fs = std::filesystem;
        printHeader("Vectors Database");

        // Get daemon status
        daemon::StatusResponse status;
        try {
            yams::daemon::ClientConfig cfg;
            if (cli_ && cli_->hasExplicitDataDir()) {
                cfg.dataDir = cli_->getDataPath();
            }
            cfg.requestTimeout = std::chrono::seconds(5);
            auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
            if (!leaseRes) {
                printError("Daemon unavailable: " + leaseRes.error().message);
                return;
            }
            auto leaseHandle = std::move(leaseRes.value());
            auto& client = **leaseHandle;
            auto statusRes = yams::cli::run_result(client.status(), std::chrono::seconds(5));
            if (!statusRes) {
                printError("Failed to get daemon status: " + statusRes.error().message);
                return;
            }
            status = statusRes.value();
        } catch (const std::exception& e) {
            printError(std::string("Failed to get daemon status: ") + e.what());
            return;
        }

        bool dbReady = status.vectorDbReady;
        size_t dbDim = status.vectorDbDim;

        if (!dbReady) {
            std::cout << "Vector database is not ready according to the daemon." << std::endl;
            // The logic to offer DB creation can be adapted here to call a new daemon command.
            return;
        }

        auto resolved = resolveEmbeddingDim();
        size_t targetDim = resolved.first;
        std::string targetSrc = resolved.second;
        if (targetDim == 0 && status.embeddingAvailable && status.embeddingDim > 0) {
            targetDim = status.embeddingDim;
            targetSrc = "daemon_provider";
        }

        if (targetDim == 0) {
            std::cout << "DB dimension: " << dbDim
                      << ". Unable to resolve target embedding dimension from config or models."
                      << std::endl;
            return;
        }

        std::cout << "DB dimension: " << dbDim << ", Target ('" << targetSrc
                  << "') dimension: " << targetDim << std::endl;

        if (dbDim != targetDim) {
            std::cout << "\n⚠ Embedding dimension mismatch detected.\n";
            std::cout << "- Stored vectors are " << dbDim << "-d but target requires " << targetDim
                      << "-d.\n";
            std::cout << "- This will cause vector search to fail or return incorrect results.\n";
            std::cout << "\nTo fix this, you can either:\n";
            std::cout << "  1. Change your model/config to match the database (" << dbDim
                      << "-d).\n";
            std::cout << "  2. Recreate the vector database with the new dimension (" << targetDim
                      << "-d).\n";
            std::cout << "     (This will require re-running 'yams repair --embeddings')\n";
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
    // Dedupe state
    bool dedupeApply_{false};
    std::string dedupeMode_{"path"};
    std::string dedupeStrategy_{"keep-newest"};
    bool dedupeForce_{false};
    bool dedupeVerbose_{false};
    Result<void> repairGraph();
    void runDedupe();
    // Tuning helpers
    Result<void> applyTuningBaseline(bool apply);
    std::map<std::string, std::string> parseSimpleToml(const std::filesystem::path& path) const;
    std::filesystem::path getConfigPath() const;
    Result<void> writeConfigValue(const std::string& key, const std::string& value);
};

// Factory
std::unique_ptr<ICommand> createDoctorCommand() {
    return std::make_unique<DoctorCommand>();
}

void DoctorCommand::registerCommand(CLI::App& app, YamsCLI* cli) {
    cli_ = cli;
    auto* doctor = app.add_subcommand(getName(), getDescription());
    doctor->require_subcommand(0); // allow bare doctor
    doctor->add_flag("--fix", fixAllTop_, "Fix everything (embeddings + FTS5)");
    doctor->add_flag("--fix-config-dims", fixConfigDims_,
                     "Align config embedding dims to target (non-interactive)");
    doctor->add_flag("--recreate-vectors", recreateVectors_,
                     "Drop and recreate vector tables to target dim (non-interactive)");
    doctor->add_option(
        "--dim", recreateDim_,
        "Target dimension to use with --recreate-vectors (defaults to resolved target)");
    doctor->add_flag("--stop-daemon", stopDaemon_, "Attempt to stop daemon before DB operations");

    doctor->callback([this]() { cli_->setPendingCommand(this); });

    auto* dsub = doctor->add_subcommand("daemon", "Check daemon socket and status");
    dsub->callback([this]() { checkDaemon(); });

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

    doctor->add_subcommand("plugins", "Show plugin summary (loaded + scan)")->callback([this]() {
        runAll();
    });

    auto* emb = doctor->add_subcommand("embeddings", "Embeddings diagnostics and actions");
    emb->require_subcommand();
    emb->add_subcommand("clear-degraded",
                        "Attempt to clear embedding degraded state (reloads preferred model)")
        ->callback([this]() { clearEmbeddingDegraded(); });

    auto* rsub = doctor->add_subcommand("repair", "Repair common issues (embeddings, FTS5, graph)");
    rsub->add_flag("--embeddings", fixEmbeddings_, "Generate missing vector embeddings");
    rsub->add_flag("--fts5", fixFts5_, "Rebuild FTS5 text index (best-effort)");
    rsub->add_flag("--graph", fixGraph_, "Construct/repair knowledge graph from tags and metadata");
    rsub->add_flag("--all", fixAll_, "Run all repair operations");
    rsub->callback([this]() { runRepair(); });

    auto* dd = doctor->add_subcommand(
        "dedupe", "Detect (and optionally remove) duplicate documents (metadata)");
    dd->add_flag("--apply", dedupeApply_, "Apply deletions (default: dry-run)");
    dd->add_option("--mode", dedupeMode_, "Grouping mode: path | name | hash")
        ->default_val("path")
        ->check(CLI::IsMember({"path", "name", "hash"}));
    dd->add_option("--strategy", dedupeStrategy_,
                   "Keep strategy: keep-newest | keep-oldest | keep-largest")
        ->default_val("keep-newest")
        ->check(CLI::IsMember({"keep-newest", "keep-oldest", "keep-largest"}));
    dd->add_flag("--force", dedupeForce_,
                 "Allow deletion even when differing hashes (treat as duplicates)");
    dd->add_flag("-v,--verbose", dedupeVerbose_, "Verbose listing of each group");
    dd->callback([this]() { runDedupe(); });

    // Auto-tuning baseline
    auto* tsub =
        doctor->add_subcommand("tuning", "Auto-configure [tuning] based on system baseline");
    bool apply = false;
    tsub->add_flag("--apply", apply, "Write suggestions to config.toml [tuning] section");
    tsub->callback([this, &apply]() {
        auto r = applyTuningBaseline(apply);
        if (!r) {
            spdlog::error("Doctor tuning failed: {}", r.error().message);
            std::exit(1);
        }
    });
}

// --- Minimal TOML helpers (duplicated from config_command for simplicity) ---
std::map<std::string, std::string>
DoctorCommand::parseSimpleToml(const std::filesystem::path& path) const {
    std::map<std::string, std::string> config;
    std::ifstream file(path);
    if (!file)
        return config;
    std::string line;
    std::string currentSection;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#')
            continue;
        if (line[0] == '[') {
            size_t end = line.find(']');
            if (end != std::string::npos) {
                currentSection = line.substr(1, end - 1);
                if (!currentSection.empty())
                    currentSection += ".";
            }
            continue;
        }
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            std::string key = line.substr(0, eq);
            std::string value = line.substr(eq + 1);
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);
            if (value.size() >= 2 && value[0] == '"' && value.back() == '"') {
                value = value.substr(1, value.size() - 2);
            }
            size_t comment = value.find('#');
            if (comment != std::string::npos) {
                value = value.substr(0, comment);
                value.erase(value.find_last_not_of(" \t") + 1);
            }
            config[currentSection + key] = value;
        }
    }
    return config;
}

std::filesystem::path DoctorCommand::getConfigPath() const {
    const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME");
    const char* homeEnv = std::getenv("HOME");
    std::filesystem::path configHome = xdgConfigHome
                                           ? std::filesystem::path(xdgConfigHome)
                                           : (homeEnv ? std::filesystem::path(homeEnv) / ".config"
                                                      : std::filesystem::path("~/.config"));
    return configHome / "yams" / "config.toml";
}

Result<void> DoctorCommand::writeConfigValue(const std::string& key, const std::string& value) {
    try {
        auto configPath = getConfigPath();
        std::filesystem::create_directories(configPath.parent_path());
        auto config = parseSimpleToml(configPath);
        config[key] = value;
        std::ofstream file(configPath);
        if (!file)
            return Error{ErrorCode::WriteError, "Cannot write config: " + configPath.string()};
        std::map<std::string, std::map<std::string, std::string>> sections;
        for (const auto& [fullKey, val] : config) {
            size_t dot = fullKey.find('.');
            if (dot != std::string::npos) {
                std::string section = fullKey.substr(0, dot);
                std::string subkey = fullKey.substr(dot + 1);
                sections[section][subkey] = val;
            } else {
                sections[""][fullKey] = val;
            }
        }
        for (const auto& [section, values] : sections) {
            if (!section.empty())
                file << "\n[" << section << "]\n";
            for (const auto& [k, v] : values) {
                file << k << " = \"" << v << "\"\n";
            }
        }
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

Result<void> DoctorCommand::applyTuningBaseline(bool apply) {
    try {
        // Simple baseline heuristic
        unsigned hc = std::thread::hardware_concurrency();
        if (hc == 0)
            hc = 4;
        uint32_t ipcMax = std::min<unsigned>(32, hc);
        uint32_t ioMax = std::min<unsigned>(32, std::max<unsigned>(1, hc / 2));
        std::map<std::string, std::string> suggestions{
            {"tuning.backpressure_read_pause_ms", "5"},
            {"tuning.worker_poll_ms", "150"},
            {"tuning.idle_cpu_pct", "10.0"},
            {"tuning.idle_mux_low_bytes", "4194304"},
            {"tuning.idle_shrink_hold_ms", "5000"},
            {"tuning.pool_cooldown_ms", "500"},
            {"tuning.pool_scale_step", "1"},
            {"tuning.pool_ipc_min", "1"},
            {"tuning.pool_ipc_max", std::to_string(ipcMax)},
            {"tuning.pool_io_min", "1"},
            {"tuning.pool_io_max", std::to_string(ioMax)},
        };
        std::cout << "Doctor tuning baseline (proposed):\n";
        for (const auto& [k, v] : suggestions) {
            std::cout << "  " << k << " = " << v << "\n";
        }
        if (apply) {
            for (const auto& [k, v] : suggestions) {
                auto r = writeConfigValue(k, v);
                if (!r)
                    return r;
            }
            std::cout << "✓ Applied tuning baseline to [tuning] in config.toml\n";
        } else {
            std::cout << "Use 'yams doctor tuning --apply' to write these values.\n";
        }
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
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

void DoctorCommand::runDedupe() {
    try {
        printHeader("Document Dedupe");
        if (!cli_) {
            printError("CLI context unavailable");
            return;
        }
        namespace fs = std::filesystem;
        fs::path dataDir = cli_->getDataPath();
        std::vector<fs::path> candidates = {dataDir / "metadata" / "metadata.db",
                                            dataDir / "metadata.db"};
        fs::path dbPath;
        for (const auto& c : candidates) {
            if (fs::exists(c)) {
                dbPath = c;
                break;
            }
        }
        if (dbPath.empty()) {
            printError("metadata.db not found under data path");
            return;
        }
        yams::metadata::Database db;
        auto ro = db.open(dbPath.string(), yams::metadata::ConnectionMode::ReadWrite);
        if (!ro) {
            printError(std::string("Open failed: ") + ro.error().message);
            return;
        }
        auto ps = db.prepare(
            "SELECT id,file_path,file_name,file_size,sha256_hash,modified_time,indexed_time FROM "
            "documents");
        if (!ps) {
            printError("Prepare failed: " + ps.error().message);
            return;
        }
        yams::metadata::Statement stmt = std::move(ps).value();
        struct Row {
            int64_t id;
            std::string path;
            std::string name;
            int64_t size;
            std::string hash;
            int64_t mtime;
            int64_t itime;
        };
        std::vector<Row> rows;
        while (true) {
            auto step = stmt.step();
            if (!step) {
                printError("Step error: " + step.error().message);
                return;
            }
            if (!step.value())
                break;
            rows.push_back({stmt.getInt64(0), stmt.getString(1), stmt.getString(2),
                            stmt.getInt64(3), stmt.getString(4), stmt.getInt64(5),
                            stmt.getInt64(6)});
        }
        if (rows.empty()) {
            printSuccess("No documents");
            return;
        }
        struct G {
            Row r;
        };
        std::unordered_map<std::string, std::vector<G>> groups;
        auto keyFn = [&](const Row& r) -> std::string {
            if (dedupeMode_ == "name")
                return r.name;
            if (dedupeMode_ == "hash")
                return r.hash;
            return r.path;
        };
        for (auto& r : rows)
            groups[keyFn(r)].push_back({r});
        size_t dupGroups = 0, candDel = 0, skipped = 0;
        std::vector<int64_t> toDelete;
        for (auto& [k, vec] : groups) {
            if (vec.size() < 2)
                continue;
            bool hashesDiffer = false;
            std::string h0 = vec.front().r.hash;
            for (auto& gi : vec)
                if (gi.r.hash != h0) {
                    hashesDiffer = true;
                    break;
                }
            if (hashesDiffer && !dedupeForce_) {
                skipped++;
                continue;
            }
            dupGroups++;
            auto newest = [](const G& a, const G& b) { return a.r.mtime > b.r.mtime; };
            auto oldest = [](const G& a, const G& b) { return a.r.mtime < b.r.mtime; };
            auto largest = [](const G& a, const G& b) { return a.r.size > b.r.size; };
            if (dedupeStrategy_ == "keep-oldest")
                std::sort(vec.begin(), vec.end(), oldest);
            else if (dedupeStrategy_ == "keep-largest")
                std::sort(vec.begin(), vec.end(), largest);
            else
                std::sort(vec.begin(), vec.end(), newest);
            for (size_t i = 1; i < vec.size(); ++i)
                toDelete.push_back(vec[i].r.id);
            candDel += vec.size() - 1;
            if (dedupeVerbose_) {
                std::cout << "Group: " << k << " keep=" << vec[0].r.id << " total=" << vec.size()
                          << (hashesDiffer ? " (hash-diff)" : "") << "\n";
                for (size_t i = 0; i < vec.size(); ++i) {
                    const auto& rr = vec[i].r;
                    std::cout << (i == 0 ? "  KEEP" : "  DEL ") << " id=" << rr.id
                              << " hash=" << rr.hash.substr(0, 8) << " size=" << rr.size
                              << " mtime=" << rr.mtime << " itime=" << rr.itime << "\n";
                }
            }
        }
        if (!dupGroups) {
            printSuccess("No duplicate groups (mode=" + dedupeMode_ + ")");
            return;
        }
        printStatusLine("Total documents", std::to_string(rows.size()));
        printStatusLine("Duplicate groups", std::to_string(dupGroups));
        printStatusLine("Deletion candidates", std::to_string(candDel));
        if (skipped)
            printStatusLine("Skipped (hash mismatch, use --force)", std::to_string(skipped));
        if (!dedupeApply_) {
            printWarn("Dry-run. Use --apply to delete.");
            return;
        }
        if (toDelete.empty()) {
            printSuccess("Nothing to delete");
            return;
        }
        auto br = db.execute("BEGIN TRANSACTION");
        if (!br) {
            printError("BEGIN failed: " + br.error().message);
            return;
        }
        bool err = false;
        for (auto id : toDelete) {
            auto psd = db.prepare("DELETE FROM documents WHERE id=?");
            if (!psd) {
                printError("Prepare delete failed");
                err = true;
                break;
            }
            auto st2 = std::move(psd).value();
            auto b = st2.bind(1, id);
            if (!b) {
                err = true;
                break;
            }
            auto ex = st2.execute();
            if (!ex) {
                err = true;
                break;
            }
        }
        auto er = db.execute(err ? "ROLLBACK" : "COMMIT");
        if (!er)
            printError("Txn end failed: " + er.error().message);
        if (err) {
            printError("Aborted due to errors");
            return;
        }
        printSuccess("Deleted " + std::to_string(toDelete.size()) + " duplicate rows");
    } catch (const std::exception& e) {
        printError(std::string("Exception: ") + e.what());
    }
}
} // namespace yams::cli
