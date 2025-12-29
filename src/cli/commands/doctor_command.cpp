#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/app/services/services.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include "yams/cli/prompt_util.h"
#include <sqlite3.h>
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);
#include <cstdlib>
#ifdef _WIN32
#include <windows.h>
#define RTLD_LAZY 0
#define RTLD_LOCAL 0

static void* dlopen(const char* filename, int flags) {
    return LoadLibraryA(filename);
}

static void* dlopen(const wchar_t* filename, int flags) {
    return LoadLibraryW(filename);
}

static void* dlsym(void* handle, const char* symbol) {
    return (void*)GetProcAddress((HMODULE)handle, symbol);
}

static int dlclose(void* handle) {
    return FreeLibrary((HMODULE)handle) ? 0 : -1;
}

static const char* dlerror() {
    static char buf[128];
    FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL, GetLastError(),
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), buf, sizeof(buf), NULL);
    return buf;
}

static int setenv(const char* name, const char* value, int overwrite) {
    int errcode = 0;
    if (!overwrite) {
        size_t envsize = 0;
        errcode = getenv_s(&envsize, NULL, 0, name);
        if (errcode || envsize)
            return errcode;
    }
    return _putenv_s(name, value);
}
static int unsetenv(const char* name) {
    return _putenv_s(name, "");
}
#else
#include <dlfcn.h>
#include <unistd.h>
#endif
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <optional>
#include <regex>
#include <set>
#include <string>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
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
        // Only run default doctor if no subcommand was invoked
        // Subcommands set their own flags and handle execution themselves
        if (!fixEmbeddings_ && !fixFts5_ && !fixGraph_ && !validateGraph_ && !fixAll_ &&
            !fixAllTop_ && !dedupeApply_ && !pruneInvoked_ && pluginArg_.empty() &&
            !fixConfigDims_ && !recreateVectors_) {
            // No subcommand flags set, run default doctor summary
            try {
                runAll();
            } catch (const std::exception& e) {
                std::cout << "Doctor error: " << e.what() << "\n";
                co_return Error{ErrorCode::Unknown, e.what()};
            }
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
                if (cli_) {
                    fs::path models = cli_->getDataPath() / "models";
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
                    if (cli_) {
                        namespace fs = std::filesystem;
                        fs::path base = cli_->getDataPath() / "models";
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

    static void printSuccess(const std::string& msg) { std::cout << ui::status_ok(msg) << "\n"; }

    static void printWarn(const std::string& msg) { std::cout << ui::status_warning(msg) << "\n"; }

    static void printError(const std::string& msg) { std::cout << ui::status_error(msg) << "\n"; }

    static void printSummary(const std::string& title, const std::vector<StepResult>& steps) {
        printHeader(title);
        for (const auto& s : steps) {
            std::cout << "  " << (s.ok ? ui::status_ok(s.name) : ui::status_error(s.name));
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
        if (!yams::cli::ui::stdout_is_tty()) {
            auto r = fn();
            return r;
        }
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
                            std::cout << "  " << ui::status_error("Schema dimension mismatch (db=" + std::to_string(*dbDim) + ", target=" + std::to_string(targetDim) + ") — aborting repair.") << "\n";
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
                        std::cout << "  " << ui::status_ok("No documents missing embeddings") << "\n";
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
                                    std::cout << "  " << ui::status_ok("Daemon embeddings: requested=" + std::to_string(er.value().requested) + ", embedded=" + std::to_string(er.value().embedded) + ", skipped=" + std::to_string(er.value().skipped) + ", failed=" + std::to_string(er.value().failed)) << "\n";
                                    attemptedDaemon = true;
                                } else {
                                    std::cout << "  " << ui::status_warning("Daemon embeddings failed (" + er.error().message + ") — falling back to local mode. Use '--no-daemon' to skip RPC.") << "\n";
                                }
                            } catch (const std::exception& ex) {
                                std::cout << "  " << ui::status_warning(std::string("Daemon embeddings exception (") + ex.what() + ") — falling back to local mode. Use '--no-daemon' to skip RPC.") << "\n";
                            }
                        }
                        if (!attemptedDaemon) {
                            auto stats = yams::repair::repairMissingEmbeddings(
                                appCtx->store, appCtx->metadataRepo, emb, rcfg, missing.value(),
                                nullptr, appCtx->contentExtractors);
                            if (!stats) {
                                std::cout << "  " << ui::status_error("Embedding repair failed: " + stats.error().message) << "\n";
                            } else {
                                std::cout << "  " << ui::status_ok("Embeddings generated=" + std::to_string(stats.value().embeddingsGenerated) + ", skipped=" + std::to_string(stats.value().embeddingsSkipped) + ", failed=" + std::to_string(stats.value().failedOperations)) << "\n";
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
                auto docs = metadata::queryDocumentsByPattern(*appCtx->metadataRepo, "%");
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
                std::cout << "  " << ui::status_ok("FTS5 reindex complete: ok=" + std::to_string(ok) + ", fail=" + std::to_string(fail)) << "\n";
            }
        } catch (const std::exception& e) {
            std::cout << "Doctor repair error: " << e.what() << "\n";
        }
    }

    // Build/repair knowledge graph using tags/metadata (non-destructive)
    // repairGraph declared earlier in the class

    // Minimal daemon check: connect and get status
    void checkDaemon(std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        using namespace yams::daemon;
        try {
            // First, perform a lightweight check to see if the daemon is responsive.
            // This avoids triggering auto-start logic in a diagnostic command.
            std::string effectiveSocket =
                daemon::DaemonClient::resolveSocketPathConfigFirst().string();
            if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                std::cout << "\n" << yams::cli::ui::section_header("Daemon Health") << "\n\n";
                std::cout << yams::cli::ui::colorize("✗ UNAVAILABLE", yams::cli::ui::Ansi::RED)
                          << " - Daemon not running on socket: " << effectiveSocket << "\n";
                std::cout << "\n"
                          << yams::cli::ui::colorize(
                                 "Hint: Start the daemon with 'yams daemon start'",
                                 yams::cli::ui::Ansi::DIM)
                          << "\n";
                return;
            }

            // If no cached status, fetch it now
            if (!cachedStatus) {
                yams::daemon::ClientConfig cfg;
                if (cli_)
                    if (cli_->hasExplicitDataDir()) {
                        cfg.dataDir = cli_->getDataPath();
                    }
                cfg.requestTimeout = std::chrono::milliseconds(10000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (!leaseRes) {
                    std::cout << "\n" << yams::cli::ui::section_header("Daemon Health") << "\n\n";
                    std::cout << yams::cli::ui::colorize("✗ UNAVAILABLE", yams::cli::ui::Ansi::RED)
                              << " - " << leaseRes.error().message << "\n";
                    return;
                }
                auto leaseHandle = std::move(leaseRes.value());
                auto& client = **leaseHandle;

                auto sres = yams::cli::run_result<StatusResponse>(client.status(),
                                                                  std::chrono::seconds(10));
                if (!sres) {
                    std::cout << "\n" << yams::cli::ui::section_header("Daemon Health") << "\n\n";
                    std::cout << yams::cli::ui::colorize("✗ Failed to get status",
                                                         yams::cli::ui::Ansi::RED)
                              << " - " << sres.error().message << "\n";
                    return;
                }
                cachedStatus = std::move(sres.value());
            }

            // Database migrations health (local DB), surface failures and guidance
            try {
                namespace fs = std::filesystem;
                fs::path dbPath = cli_->getDataPath() / "yams.db";
                sqlite3* db = nullptr;
                if (sqlite3_open(dbPath.string().c_str(), &db) == SQLITE_OK) {
                    auto queryScalar = [&](const char* sql) -> std::optional<std::string> {
                        sqlite3_stmt* stmt = nullptr;
                        if (sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr) != SQLITE_OK)
                            return std::nullopt;
                        std::optional<std::string> out{};
                        if (sqlite3_step(stmt) == SQLITE_ROW) {
                            const unsigned char* txt = sqlite3_column_text(stmt, 0);
                            if (txt)
                                out = std::string(reinterpret_cast<const char*>(txt));
                        }
                        sqlite3_finalize(stmt);
                        return out;
                    };

                    // Any failed migrations?
                    sqlite3_stmt* st = nullptr;
                    bool haveFail = false;
                    if (sqlite3_prepare_v2(db,
                                           "SELECT version,name,error FROM migration_history "
                                           "WHERE success=0 ORDER BY applied_at DESC LIMIT 3",
                                           -1, &st, nullptr) == SQLITE_OK) {
                        while (sqlite3_step(st) == SQLITE_ROW) {
                            if (!haveFail) {
                                std::cout << "\nDatabase Migrations\n";
                                std::cout << "-------------------\n";
                                haveFail = true;
                            }
                            int ver = sqlite3_column_int(st, 0);
                            const unsigned char* n = sqlite3_column_text(st, 1);
                            const unsigned char* e = sqlite3_column_text(st, 2);
                            std::cout << ui::status_error("version=" + std::to_string(ver) + ", name='" + (n ? (const char*)n : "?") + "'");
                            if (e && *e)
                                std::cout << ", error=\"" << (const char*)e << "\"";
                            std::cout << "\n";
                        }
                        sqlite3_finalize(st);
                    }
                    // Leftover temp FTS table from failed swap?
                    if (auto ftsNew =
                            queryScalar("SELECT name FROM sqlite_master WHERE type='table' AND "
                                        "name='documents_fts_new'")) {
                        if (haveFail == false) {
                            std::cout << "\nDatabase Migrations\n";
                            std::cout << "-------------------\n";
                            haveFail = true;
                        }
                        std::cout
                            << ui::status_warning("Found leftover 'documents_fts_new' — an FTS migration likely failed mid-way.")
                            << "\n";
                        std::cout << "  Run: yams repair --fts5\n";
                    }
                    if (haveFail) {
                        std::cout << "Hint: try 'yams repair --fts5' to rebuild FTS, or rerun your "
                                     "command with '--verbose' for details.\n";
                    }
                    sqlite3_close(db);
                }
            } catch (...) {
            }
            std::cout << "\n" << yams::cli::ui::section_header("Daemon Health") << "\n\n";

            // Use cached status
            if (!cachedStatus) {
                std::cout << yams::cli::ui::colorize("✗ Failed to get status",
                                                     yams::cli::ui::Ansi::RED)
                          << "\n";
                return;
            }
            const auto& st = cachedStatus.value();

            std::vector<yams::cli::ui::Row> daemonRows;
            std::string statusDisplay;
            if (st.ready) {
                statusDisplay = yams::cli::ui::colorize("✓ READY", yams::cli::ui::Ansi::GREEN);
            } else {
                statusDisplay = yams::cli::ui::colorize("◷ NOT READY", yams::cli::ui::Ansi::YELLOW);
            }
            daemonRows.push_back({"Status", statusDisplay, ""});

            std::string state = st.lifecycleState.empty() ? st.overallStatus : st.lifecycleState;
            daemonRows.push_back({"State", state, ""});
            daemonRows.push_back({"Version", st.version.empty() ? "unknown" : st.version, ""});
            daemonRows.push_back({"Connections", std::to_string(st.activeConnections), ""});
            yams::cli::ui::render_rows(std::cout, daemonRows);

            // Vector scoring and resources
            std::cout << "\n";
            std::vector<yams::cli::ui::Row> resourceRows;

            // Show vector scoring availability
            try {
                bool vecAvail = false, vecEnabled = false;
                if (auto it = st.readinessStates.find("vector_embeddings_available");
                    it != st.readinessStates.end())
                    vecAvail = it->second;
                if (auto it = st.readinessStates.find("vector_scoring_enabled");
                    it != st.readinessStates.end())
                    vecEnabled = it->second;

                std::string vecStatus;
                if (vecEnabled) {
                    vecStatus = yams::cli::ui::colorize("✓ enabled", yams::cli::ui::Ansi::GREEN);
                } else {
                    vecStatus = yams::cli::ui::colorize("✗ disabled", yams::cli::ui::Ansi::YELLOW);
                    vecStatus +=
                        " (" +
                        std::string(vecAvail ? "config weight=0" : "embeddings unavailable") + ")";
                }
                resourceRows.push_back({"Vector Scoring", vecStatus, ""});
            } catch (...) {
            }

            // Show resources
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
                    std::ostringstream ramStr;
                    ramStr << std::fixed << std::setprecision(1) << ram << " MB";
                    resourceRows.push_back({"Memory", ramStr.str(), ""});
                    resourceRows.push_back({"CPU", std::to_string((int)cpu) + "%", ""});
                } else if (!phase.empty()) {
                    std::string pending = "pending (" + phase + ")";
                    if (st.retryAfterMs > 0) {
                        pending += " - retry after " + std::to_string(st.retryAfterMs) + "ms";
                    }
                    resourceRows.push_back({"Resources", pending, ""});
                }

                if (haveWorkers) {
                    size_t threads = wt != st.requestCounts.end() ? wt->second : 0;
                    size_t active = wa != st.requestCounts.end() ? wa->second : 0;
                    size_t queued = wq != st.requestCounts.end() ? wq->second : 0;
                    std::ostringstream workerStr;
                    workerStr << threads << " threads, " << active << " active, " << queued
                              << " queued";
                    resourceRows.push_back({"Worker Pool", workerStr.str(), ""});
                } else if (!st.ready) {
                    resourceRows.push_back({"Worker Pool", "pending", ""});
                }

                if (st.muxQueuedBytes > 0) {
                    resourceRows.push_back(
                        {"Mux Queued", std::to_string(st.muxQueuedBytes) + " bytes", ""});
                }
            } catch (...) {
            }

            if (!resourceRows.empty()) {
                yams::cli::ui::render_rows(std::cout, resourceRows);
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
        // 1) Try exact filename/stem match first (ABI default dirs)
        for (const auto& dir : std::vector<std::filesystem::path>{
                 (std::getenv("HOME") ? std::filesystem::path(std::getenv("HOME")) / ".local" /
                                            "lib" / "yams" / "plugins"
                                      : std::filesystem::path()),
#ifdef __APPLE__
                 std::filesystem::path("/opt/homebrew/lib/yams/plugins"),
#endif
                 std::filesystem::path("/usr/local/lib/yams/plugins"),
                 std::filesystem::path("/usr/lib/yams/plugins")
#ifdef YAMS_INSTALL_PREFIX
                     ,
                 std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins"
#endif
             }) {
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
        // 2) Try ABI descriptor name via dlopen + yams_plugin_get_name() in default dirs
        for (const auto& dir : std::vector<std::filesystem::path>{
                 (std::getenv("HOME") ? std::filesystem::path(std::getenv("HOME")) / ".local" /
                                            "lib" / "yams" / "plugins"
                                      : std::filesystem::path()),
#ifdef __APPLE__
                 std::filesystem::path("/opt/homebrew/lib/yams/plugins"),
#endif
                 std::filesystem::path("/usr/local/lib/yams/plugins"),
                 std::filesystem::path("/usr/lib/yams/plugins")
#ifdef YAMS_INSTALL_PREFIX
                     ,
                 std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins"
#endif
             }) {
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
        for (const auto& dir : std::vector<std::filesystem::path>{
                 (std::getenv("HOME") ? std::filesystem::path(std::getenv("HOME")) / ".local" /
                                            "lib" / "yams" / "plugins"
                                      : std::filesystem::path()),
#ifdef __APPLE__
                 std::filesystem::path("/opt/homebrew/lib/yams/plugins"),
#endif
                 std::filesystem::path("/usr/local/lib/yams/plugins"),
                 std::filesystem::path("/usr/lib/yams/plugins")
#ifdef YAMS_INSTALL_PREFIX
                     ,
                 std::filesystem::path(YAMS_INSTALL_PREFIX) / "lib" / "yams" / "plugins"
#endif
             }) {
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
        // JSON output mode: collect data into structured object
        nlohmann::json jsonResult;
        bool useJson = jsonOutput_ || (cli_ && cli_->getJsonOutput());

        // SQLite + FTS status and migration health
        // FTS checks removed to avoid blocking/hangs; use 'yams daemon status -d' for readiness.
        // Minimal structured recommendations example (will expand in future audits)
        yams::cli::RecommendationBuilder recs;
        // Keep doctor fast and non-invasive by default:
        // - Daemon status
        // - Installed models
        // - Vector DB dimension check

        // Show loading indicator immediately (skip for JSON mode)
        if (!useJson) {
            std::cout << yams::cli::ui::colorize("◷ Collecting system information...",
                                                 yams::cli::ui::Ansi::CYAN)
                      << "\n"
                      << std::flush;
        }

        // Daemon light check first; if not running, fast-fail and only run local checks
        bool daemon_up = false;
        std::string effectiveSocket;
        {
            using namespace yams::daemon;
            effectiveSocket = daemon::DaemonClient::resolveSocketPathConfigFirst().string();
            daemon_up = daemon::DaemonClient::isDaemonRunning(effectiveSocket);
        }

        // Record daemon connectivity for JSON
        if (useJson) {
            jsonResult["daemon"]["socket"] = effectiveSocket;
            jsonResult["daemon"]["running"] = daemon_up;
        }

        // Fetch daemon status once if available (reuse throughout)
        std::optional<yams::daemon::StatusResponse> cachedStatus;
        std::optional<yams::daemon::GetStatsResponse> cachedStats;
        if (daemon_up) {
            try {
                using namespace yams::daemon;
                ClientConfig cfg;
                if (cli_ && cli_->hasExplicitDataDir()) {
                    cfg.dataDir = cli_->getDataPath();
                }
                cfg.requestTimeout = std::chrono::milliseconds(10000);
                auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
                if (leaseRes) {
                    auto leaseHandle = std::move(leaseRes.value());
                    auto& client = **leaseHandle;

                    {
                        auto sres = yams::cli::run_result<StatusResponse>(client.status(),
                                                                          std::chrono::seconds(10));
                        if (sres) {
                            cachedStatus = std::move(sres.value());
                        }
                    }

                    {
                        GetStatsRequest req;
                        req.detailed = true;
                        req.showFileTypes = false;
                        auto gres = yams::cli::run_result<GetStatsResponse>(
                            client.getStats(req), std::chrono::milliseconds(10000));
                        if (gres) {
                            cachedStats = std::move(gres.value());
                        }
                    }
                }
            } catch (...) {
            }
        }

        // Clear loading message
        if (!useJson) {
            std::cout << "\r" << std::string(50, ' ') << "\r" << std::flush;
        }

        // For JSON mode, collect all data and output at end
        if (useJson) {
            // Daemon status
            if (cachedStatus) {
                const auto& s = cachedStatus.value();
                jsonResult["daemon"]["ready"] = s.ready;
                jsonResult["daemon"]["running"] = s.running;
                jsonResult["daemon"]["version"] = s.version;
                jsonResult["daemon"]["lifecycle_state"] = s.lifecycleState;
                jsonResult["daemon"]["active_connections"] = s.activeConnections;
                jsonResult["daemon"]["memory_mb"] = s.memoryUsageMb;
                jsonResult["daemon"]["cpu_percent"] = s.cpuUsagePercent;

                // Embedding info
                jsonResult["embedding"]["available"] = s.embeddingAvailable;
                jsonResult["embedding"]["backend"] = s.embeddingBackend;
                jsonResult["embedding"]["model"] = s.embeddingModel;
                jsonResult["embedding"]["path"] = s.embeddingModelPath;
                jsonResult["embedding"]["dimension"] = s.embeddingDim;
                jsonResult["embedding"]["threads_intra"] = s.embeddingThreadsIntra;
                jsonResult["embedding"]["threads_inter"] = s.embeddingThreadsInter;

                // Readiness states
                for (const auto& [k, v] : s.readinessStates) {
                    jsonResult["readiness"][k] = v;
                }

                // Plugins/providers
                nlohmann::json pluginsJson = nlohmann::json::array();
                for (const auto& p : s.providers) {
                    nlohmann::json pj;
                    pj["name"] = p.name;
                    pj["ready"] = p.ready;
                    pj["degraded"] = p.degraded;
                    pj["is_provider"] = p.isProvider;
                    pj["models_loaded"] = p.modelsLoaded;
                    if (!p.error.empty())
                        pj["error"] = p.error;
                    pluginsJson.push_back(std::move(pj));
                }
                jsonResult["plugins"] = std::move(pluginsJson);
            }

            // Models (installed)
            namespace fs = std::filesystem;
            fs::path modelsPath = cli_ ? cli_->getDataPath() / "models" : fs::path();
            nlohmann::json modelsJson = nlohmann::json::array();
            std::error_code ec;
            if (!modelsPath.empty() && fs::exists(modelsPath, ec) &&
                fs::is_directory(modelsPath, ec)) {
                for (const auto& entry : fs::directory_iterator(modelsPath, ec)) {
                    if (!entry.is_directory())
                        continue;
                    fs::path modelOnnx = entry.path() / "model.onnx";
                    if (fs::exists(modelOnnx, ec)) {
                        nlohmann::json mj;
                        mj["name"] = entry.path().filename().string();
                        mj["has_config"] =
                            fs::exists(entry.path() / "config.json", ec) ||
                            fs::exists(entry.path() / "sentence_bert_config.json", ec);
                        mj["has_tokenizer"] = fs::exists(entry.path() / "tokenizer.json", ec);
                        modelsJson.push_back(std::move(mj));
                    }
                }
            }
            jsonResult["models"] = std::move(modelsJson);

            // Vector DB info
            fs::path vecDbPath = cli_ ? cli_->getDataPath() / "vectors.db" : fs::path();
            jsonResult["vector_db"]["path"] = vecDbPath.string();
            jsonResult["vector_db"]["exists"] = !vecDbPath.empty() && fs::exists(vecDbPath, ec);

            // Knowledge graph stats (if db available)
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
                    jsonResult["knowledge_graph"]["nodes"] =
                        countTable("SELECT COUNT(1) FROM kg_nodes");
                    jsonResult["knowledge_graph"]["edges"] =
                        countTable("SELECT COUNT(1) FROM kg_edges");
                    jsonResult["knowledge_graph"]["aliases"] =
                        countTable("SELECT COUNT(1) FROM kg_aliases");
                    jsonResult["knowledge_graph"]["embeddings"] =
                        countTable("SELECT COUNT(1) FROM kg_node_embeddings");
                    jsonResult["knowledge_graph"]["doc_entities"] =
                        countTable("SELECT COUNT(1) FROM doc_entities");
                }
            } catch (...) {
            }

            // Recommendations
            if (!recs.empty()) {
                jsonResult["recommendations"] = yams::cli::recommendationsToJson(recs);
            }

            // Output JSON and return early
            std::cout << jsonResult.dump(2) << "\n";
            return;
        }

        checkDaemon(cachedStatus);
        if (!daemon_up) {
            // Only local checks when daemon is unavailable
            checkInstalledModels(cli_);
            checkVec0Module(); // Check vec0 module even when daemon is down
            checkEmbeddingDimMismatch(cachedStatus);
            return;
        }
        checkInstalledModels(cli_);
        checkVec0Module(); // Check vec0 module availability and schema
        checkEmbeddingDimMismatch(cachedStatus);
        // Show embedding runtime from daemon status (best-effort, use cached data)
        try {
            if (cachedStatus) {
                const auto& s = cachedStatus.value();
                std::cout << "\n" << yams::cli::ui::section_header("Embedding Runtime") << "\n\n";

                std::vector<yams::cli::ui::Row> embRows;
                std::string availStatus =
                    s.embeddingAvailable
                        ? yams::cli::ui::colorize("✓ yes", yams::cli::ui::Ansi::GREEN)
                        : yams::cli::ui::colorize("✗ no", yams::cli::ui::Ansi::YELLOW);
                embRows.push_back({"Available", availStatus, ""});

                if (!s.embeddingBackend.empty())
                    embRows.push_back({"Backend", s.embeddingBackend, ""});
                if (!s.embeddingModel.empty())
                    embRows.push_back({"Model", s.embeddingModel, ""});
                if (!s.embeddingModelPath.empty())
                    embRows.push_back({"Path", s.embeddingModelPath, ""});
                if (s.embeddingDim > 0)
                    embRows.push_back({"Dimension", std::to_string(s.embeddingDim), ""});
                if (s.embeddingThreadsIntra > 0 || s.embeddingThreadsInter > 0) {
                    std::ostringstream thrStr;
                    thrStr << s.embeddingThreadsIntra << " intra / " << s.embeddingThreadsInter
                           << " inter";
                    embRows.push_back({"Threads", thrStr.str(), ""});
                }

                yams::cli::ui::render_rows(std::cout, embRows);
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

                std::cout << "\n" << yams::cli::ui::section_header("Knowledge Graph") << "\n\n";
                if (entities <= 0 && nodes <= 0) {
                    std::string msg = "Knowledge graph empty — run 'yams doctor repair --graph' to "
                                      "build from tags/metadata";
                    std::cout << yams::cli::ui::colorize("⚠ " + msg, yams::cli::ui::Ansi::YELLOW)
                              << "\n";
                    recs.warning("DOCTOR_KG_EMPTY", msg);
                } else {
                    std::vector<yams::cli::ui::Row> kgRows;
                    if (nodes >= 0)
                        kgRows.push_back({"Nodes", std::to_string(nodes), ""});
                    if (edges >= 0)
                        kgRows.push_back({"Edges", std::to_string(edges), ""});
                    if (aliases >= 0)
                        kgRows.push_back({"Aliases", std::to_string(aliases), ""});
                    if (embeddings >= 0)
                        kgRows.push_back({"Embeddings", std::to_string(embeddings), ""});
                    if (entities >= 0)
                        kgRows.push_back({"Doc Entities", std::to_string(entities), ""});
                    yams::cli::ui::render_rows(std::cout, kgRows);
                }
            }
        } catch (...) {
            // Silent: doctor remains best-effort
        }

        // Show currently loaded plugins, mirroring `yams plugin list` logic (use cached data)
        try {
            std::cout << "\n" << yams::cli::ui::section_header("Loaded Plugins") << "\n\n";

            // Prefer typed providers list from StatusResponse
            if (cachedStatus && !cachedStatus.value().providers.empty()) {
                const auto& st = cachedStatus.value();
                for (const auto& p : st.providers) {
                    std::string status;
                    if (p.degraded) {
                        status = yams::cli::ui::colorize("✗", yams::cli::ui::Ansi::RED);
                    } else if (!p.ready) {
                        status = yams::cli::ui::colorize("◷", yams::cli::ui::Ansi::YELLOW);
                    } else {
                        status = yams::cli::ui::colorize("✓", yams::cli::ui::Ansi::GREEN);
                    }

                    std::cout << "  " << status << " "
                              << yams::cli::ui::colorize(p.name, yams::cli::ui::Ansi::CYAN);

                    std::vector<std::string> tags;
                    if (p.isProvider)
                        tags.push_back("provider");
                    if (!p.ready)
                        tags.push_back("not-ready");
                    if (p.degraded)
                        tags.push_back("degraded");

                    if (!tags.empty()) {
                        std::cout << " ["
                                  << yams::cli::ui::colorize(tags[0], yams::cli::ui::Ansi::DIM);
                        for (size_t i = 1; i < tags.size(); ++i) {
                            std::cout << ", "
                                      << yams::cli::ui::colorize(tags[i], yams::cli::ui::Ansi::DIM);
                        }
                        std::cout << "]";
                    }

                    if (p.modelsLoaded > 0)
                        std::cout << " "
                                  << yams::cli::ui::colorize("(" + std::to_string(p.modelsLoaded) +
                                                                 " models)",
                                                             yams::cli::ui::Ansi::DIM);
                    if (!p.error.empty())
                        std::cout << "\n      "
                                  << yams::cli::ui::colorize("Error: " + p.error,
                                                             yams::cli::ui::Ansi::RED);
                    std::cout << "\n";
                }
            } else if (cachedStats) {
                // Fallback to JSON snapshot embedded in stats
                auto it = cachedStats.value().additionalStats.find("plugins_json");
                if (it != cachedStats.value().additionalStats.end() && !it->second.empty()) {
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

                // Show any loader errors captured in stats (use cached stats)
                if (cachedStats) {
                    auto eit = cachedStats.value().additionalStats.find("plugins_error");
                    if (eit != cachedStats.value().additionalStats.end() && !eit->second.empty()) {
                        std::cout << "(plugins error: " << eit->second << ")\n";
                    }
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

    static void checkInstalledModels(YamsCLI* cli) {
        namespace fs = std::filesystem;
        const char* home = std::getenv("HOME");
        if (!home)
            return;

        // Check for models in OLD location (pre-unification)
        fs::path old_base = fs::path(home) / ".yams" / "models";

        // Check for models in NEW unified location
        fs::path new_base = cli ? cli->getDataPath() / "models" : "";
        if (new_base.empty()) {
            const char* xdg_data = std::getenv("XDG_DATA_HOME");
            new_base = xdg_data ? fs::path(xdg_data) / "yams" / "models"
                                : fs::path(home) / ".local" / "share" / "yams" / "models";
        }

        std::error_code ec;

        // Check if old location has models
        size_t old_model_count = 0;
        if (fs::exists(old_base, ec) && fs::is_directory(old_base, ec)) {
            for (const auto& entry : fs::directory_iterator(old_base, ec)) {
                if (entry.is_directory() && fs::exists(entry.path() / "model.onnx", ec)) {
                    old_model_count++;
                }
            }
        }

        // Warn about old location if models found there
        if (old_model_count > 0) {
            std::cout << "\n";
            printWarn("Found " + std::to_string(old_model_count) +
                      " model(s) in OLD location: " + old_base.string());
            printWarn("Models should be in unified storage: " + new_base.string());
            std::cout << "\nMigration command:\n";
            std::cout << "  mkdir -p " << new_base.string() << "\n";
            std::cout << "  mv " << old_base.string() << "/* " << new_base.string() << "/\n";
            std::cout << "  yams daemon restart\n\n";
        }

        // Use the new unified location for checking models
        fs::path base = new_base;

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
            const auto& dir = entry.path();
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

    // Check if vec0 module is available and vector DB schema is valid
    void checkVec0Module() {
        namespace fs = std::filesystem;
        printHeader("Vector DB Schema (vec0 module)");

        fs::path dbPath = cli_ ? cli_->getDataPath() / "vectors.db"
                               : fs::path(std::getenv("HOME") ? std::getenv("HOME") : "/tmp") /
                                     ".local/share/yams/vectors.db";

        if (!fs::exists(dbPath)) {
            printWarn("Vector database does not exist yet: " + dbPath.string());
            std::cout << "  → Database will be created automatically when daemon starts.\n";
            return;
        }

        sqlite3* db = nullptr;
        int rc = sqlite3_open(dbPath.string().c_str(), &db);
        if (rc != SQLITE_OK) {
            printError("Failed to open vector database: " + std::string(sqlite3_errmsg(db)));
            if (db)
                sqlite3_close(db);
            return;
        }

        char* error_msg = nullptr;
        rc = sqlite3_vec_init(db, &error_msg, nullptr);
        if (rc != SQLITE_OK) {
            printError("Failed to initialize vec0 module: " +
                       std::string(error_msg ? error_msg : "unknown"));
            if (error_msg)
                sqlite3_free(error_msg);
            sqlite3_close(db);
            return;
        }

        const char* vec0_check = "SELECT 1 FROM pragma_module_list WHERE name='vec0'";
        sqlite3_stmt* stmt = nullptr;
        bool vec0_available = false;

        if (sqlite3_prepare_v2(db, vec0_check, -1, &stmt, nullptr) == SQLITE_OK) {
            vec0_available = (sqlite3_step(stmt) == SQLITE_ROW);
            sqlite3_finalize(stmt);
        }

        if (!vec0_available) {
            printError("vec0 module failed to load");
            sqlite3_close(db);
            return;
        }

        printSuccess("vec0 module is available");

        // Test 2: Check if doc_embeddings table uses vec0 virtual table
        const char* schema_check =
            "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' AND type='table'";
        std::string schema_ddl;

        if (sqlite3_prepare_v2(db, schema_check, -1, &stmt, nullptr) == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const unsigned char* txt = sqlite3_column_text(stmt, 0);
                if (txt)
                    schema_ddl = reinterpret_cast<const char*>(txt);
            }
            sqlite3_finalize(stmt);
        }

        if (schema_ddl.empty()) {
            printWarn("doc_embeddings table does not exist yet");
            std::cout << "  → Table will be created automatically when daemon starts.\n";
        } else if (schema_ddl.find("USING vec0") == std::string::npos) {
            printError("doc_embeddings table not using vec0 virtual table");
            std::cout << "\nSchema: " << schema_ddl.substr(0, 100) << "...\n\n";
            std::cout << "This table was created without the vec0 module.\n";
            std::cout << "Vector search will not work correctly.\n\n";
            std::cout << "Fix options:\n";
            std::cout << "  1. Recreate the vector tables:\n";
            std::cout << "     yams doctor --recreate-vectors --stop-daemon\n\n";
        } else {
            printSuccess("doc_embeddings table correctly uses vec0 virtual table");

            // Extract dimension from schema
            auto pos = schema_ddl.find("float[");
            if (pos != std::string::npos) {
                auto end = schema_ddl.find(']', pos);
                if (end != std::string::npos) {
                    std::string dim_str = schema_ddl.substr(pos + 6, end - (pos + 6));
                    std::cout << "  Schema dimension: " << dim_str << "\n";
                }
            }
        }

        sqlite3_close(db);
    }

    void checkEmbeddingDimMismatch(std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        printHeader("Vectors Database");

        // Use cached status if available, otherwise fetch
        daemon::StatusResponse status;
        if (cachedStatus) {
            status = cachedStatus.value();
        } else {
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
        }

        bool dbReady = status.vectorDbReady;
        size_t dbDim = status.vectorDbDim;

        // Avoid polling in CLI; rely on current daemon status only

        if (!dbReady) {
            // No direct DB/FS probing here — rely solely on daemon-reported status

            std::cout << "Vector database is not ready according to the daemon." << std::endl;
            // Provide actionable guidance
            std::cout << "\nTo initialize or rebuild vector DB:" << "\n"
                      << "  1. (Optional) Remove corrupt or old DB: rm "
                      << (cli_ ? (cli_->getDataPath() / "vectors.db").string()
                               : "~/.local/share/yams/vectors.db")
                      << " (only if you want a fresh index)\n"
                      << "  2. Ensure desired model is installed: yams model download <model-name>"
                      << "\n"
                      << "  3. Set preferred model (env or config): yams config embeddings model "
                         "<model-name>"
                      << "\n"
                      << "  4. (Optional) Set embedding dim: yams config set "
                         "embeddings.embedding_dim <dim>"
                      << "\n"
                      << "  5. Restart daemon: yams daemon restart" << "\n"
                      << "  6. Trigger rebuild by adding docs or running a search." << "\n";
            std::cout << "\nInstalled models with heuristics (run 'yams doctor' above for details)."
                      << std::endl;
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
            std::cout << ui::status_ok("Embedding dimension matches model.") << "\n";
        }
    }

    YamsCLI* cli_{nullptr};
    bool jsonOutput_{false};
    bool fixEmbeddings_{false};
    bool fixFts5_{false};
    bool fixGraph_{false};
    bool validateGraph_{false};
    bool fixAll_{false};
    bool fixAllTop_{false};
    std::string pluginArg_;
    std::string ifaceId_;
    uint32_t ifaceVersion_{0};
    bool noDaemonProbe_{false};
    // Non-interactive fix flags
    bool fixConfigDims_{false};
    bool recreateVectors_{false};
    std::optional<size_t> recreateDim_;
    bool stopDaemon_{false};
    // Dedupe state
    bool dedupeApply_{false};
    std::string dedupeMode_{"path"};
    std::string dedupeStrategy_{"keep-newest"};
    bool dedupeForce_{false};
    bool dedupeVerbose_{false};
    Result<void> repairGraph();
    Result<void> validateGraph();
    void runDedupe();
    // Prune state
    bool pruneApply_{false};
    std::vector<std::string> pruneCategories_;
    std::vector<std::string> pruneExtensions_;
    std::string pruneOlderThan_;
    std::string pruneLargerThan_;
    std::string pruneSmallerThan_;
    bool pruneVerbose_{false};
    bool pruneInvoked_{false}; // Track if prune subcommand was actually invoked
    void runPrune();
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
    doctor->add_flag("--json", jsonOutput_, "Output results in JSON format");
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
    dsub->callback([this]() {
        std::optional<yams::daemon::StatusResponse> status;
        checkDaemon(status);
    });

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

    auto* vsub = doctor->add_subcommand("validate", "Validate knowledge graph health");
    vsub->add_flag("--graph", validateGraph_, "Validate knowledge graph integrity");
    vsub->callback([this]() {
        if (validateGraph_) {
            auto result = validateGraph();
            if (!result) {
                std::cerr << "Validation failed: " << result.error().message << "\n";
            }
        }
    });

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

    // Prune subcommand
    auto* prune =
        doctor->add_subcommand("prune", "Remove build artifacts, logs, cache, and temporary files");
    prune->add_flag("--apply", pruneApply_, "Apply deletions (default: dry-run)");
    prune
        ->add_option("--category,-c", pruneCategories_,
                     "Categories to prune (comma-separated): build-artifacts, build-system, "
                     "build (both), git-artifacts, logs, cache, temp, coverage, ide, ide-all, "
                     "package-deps, package-cache, packages, all")
        ->delimiter(',');
    prune
        ->add_option("--extension,-e", pruneExtensions_,
                     "File extensions to prune (comma-separated, e.g., o,obj,log)")
        ->delimiter(',');
    prune->add_option("--older-than", pruneOlderThan_,
                      "Only prune files older than duration (e.g., 30d, 2w, 6m)");
    prune->add_option("--larger-than", pruneLargerThan_,
                      "Only prune files larger than size (e.g., 10MB, 1GB)");
    prune->add_option("--smaller-than", pruneSmallerThan_,
                      "Only prune files smaller than size (e.g., 1KB)");
    prune->add_flag("-v,--verbose", pruneVerbose_, "Verbose output");
    prune->callback([this]() {
        pruneInvoked_ = true;
        runPrune();
    });

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
        uint32_t ipcMax = std::min<unsigned>(64, hc * 2);
        uint32_t ioMax = std::min<unsigned>(32, std::max<unsigned>(1, hc / 2));
        std::map<std::string, std::string> suggestions{
            {"tuning.backpressure_read_pause_ms", "10"},
            {"tuning.worker_poll_ms", "75"},
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
            std::cout << ui::status_ok("Applied tuning baseline to [tuning] in config.toml") << "\n";
        } else {
            std::cout << "Use 'yams doctor tuning --apply' to write these values.\n";
        }
        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::Unknown, e.what()};
    }
}

// Build/repair knowledge graph using tags/metadata (daemon-first approach)
Result<void> DoctorCommand::repairGraph() {
    try {
        using namespace yams::daemon;
        using namespace yams::cli::ui;

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::seconds(120);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            return Error{ErrorCode::InternalError,
                         "Daemon unavailable: " + leaseRes.error().message};
        }

        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        GraphRepairRequest req;
        req.dryRun = false;

        std::cout << status_pending("Repairing knowledge graph") << "\n";

        auto result = yams::cli::run_result(client.graphRepair(req), std::chrono::seconds(180));
        if (!result) {
            std::cout << status_error("Graph repair failed") << "\n";
            return Error{ErrorCode::InternalError,
                         "Graph repair failed: " + result.error().message};
        }

        const auto& resp = result.value();

        if (resp.errors == 0) {
            std::cout << status_ok("Graph repair completed") << "\n\n";
        } else {
            std::cout << status_warning("Graph repair completed with errors") << "\n\n";
        }

        std::cout << "  " << colorize("Nodes created:", Ansi::CYAN) << " "
                  << format_number(resp.nodesCreated) << "\n";
        std::cout << "  " << colorize("Nodes updated:", Ansi::CYAN) << " "
                  << format_number(resp.nodesUpdated) << "\n";
        std::cout << "  " << colorize("Edges created:", Ansi::CYAN) << " "
                  << format_number(resp.edgesCreated) << "\n";

        if (resp.errors > 0) {
            std::cout << "  " << colorize("Errors:", Ansi::YELLOW) << " "
                      << format_number(resp.errors) << "\n";
        }

        if (!resp.issues.empty()) {
            std::cout << "\n" << colorize("Issues:", Ansi::YELLOW) << "\n";
            size_t displayCount = std::min(resp.issues.size(), size_t(10));
            for (size_t i = 0; i < displayCount; ++i) {
                std::cout << "  " << bullet(resp.issues[i]) << "\n";
            }
            if (resp.issues.size() > displayCount) {
                std::cout << "  "
                          << colorize("... and " +
                                          std::to_string(resp.issues.size() - displayCount) +
                                          " more",
                                      Ansi::DIM)
                          << "\n";
            }
        }

        return Result<void>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError, e.what()};
    }
}

// Validate knowledge graph health
Result<void> DoctorCommand::validateGraph() {
    try {
        using namespace yams::daemon;
        using namespace yams::cli::ui;

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::seconds(60);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            return Error{ErrorCode::InternalError,
                         "Daemon unavailable: " + leaseRes.error().message};
        }

        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        GraphValidateRequest req;

        std::cout << status_pending("Validating knowledge graph") << "\n";

        auto result = yams::cli::run_result(client.graphValidate(req), std::chrono::seconds(120));
        if (!result) {
            std::cout << status_error("Graph validation failed") << "\n";
            return Error{ErrorCode::InternalError,
                         "Graph validation failed: " + result.error().message};
        }

        const auto& resp = result.value();

        bool hasIssues =
            !resp.issues.empty() || resp.orphanedNodes > 0 || resp.unreachableNodes > 0;
        if (!hasIssues) {
            std::cout << status_ok("Graph validation completed - no issues found") << "\n\n";
        } else {
            std::cout << status_warning("Graph validation completed - issues detected") << "\n\n";
        }

        std::cout << "  " << colorize("Total nodes:", Ansi::CYAN) << " "
                  << format_number(resp.totalNodes) << "\n";
        std::cout << "  " << colorize("Total edges:", Ansi::CYAN) << " "
                  << format_number(resp.totalEdges) << "\n";

        if (resp.orphanedNodes > 0) {
            std::cout << "  " << colorize("Orphaned nodes:", Ansi::YELLOW) << " "
                      << format_number(resp.orphanedNodes) << "\n";
        }

        if (resp.unreachableNodes > 0) {
            std::cout << "  " << colorize("Unreachable nodes:", Ansi::YELLOW) << " "
                      << format_number(resp.unreachableNodes) << "\n";
        }

        if (!resp.issues.empty()) {
            std::cout << "\n" << colorize("Issues:", Ansi::YELLOW) << "\n";
            size_t displayCount = std::min(resp.issues.size(), size_t(10));
            for (size_t i = 0; i < displayCount; ++i) {
                std::cout << "  " << bullet(resp.issues[i]) << "\n";
            }
            if (resp.issues.size() > displayCount) {
                std::cout << "  "
                          << colorize("... and " +
                                          std::to_string(resp.issues.size() - displayCount) +
                                          " more",
                                      Ansi::DIM)
                          << "\n";
            }
        }

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

void DoctorCommand::runPrune() {
    try {
        if (pruneCategories_.empty() && pruneExtensions_.empty()) {
            std::cout << "\n" << yams::cli::ui::section_header("YAMS Doctor Prune") << "\n\n";
            std::cout
                << "Remove build artifacts, logs, cache, and temporary files from YAMS index.\n\n";

            std::cout << yams::cli::ui::subsection_header("Composite Categories") << "\n\n";
            std::cout << "  "
                      << yams::cli::ui::colorize("build-artifacts", yams::cli::ui::Ansi::CYAN)
                      << "  - Compiled objects, libraries, executables, archives\n";
            std::cout << "  " << yams::cli::ui::colorize("build-system", yams::cli::ui::Ansi::CYAN)
                      << "     - CMake, Ninja, Meson, Make, Gradle, Maven, NPM, Cargo, Go, "
                         "Flutter, Dart\n";
            std::cout << "  " << yams::cli::ui::colorize("build", yams::cli::ui::Ansi::CYAN)
                      << "            - Both build-artifacts and build-system\n";
            std::cout << "  " << yams::cli::ui::colorize("git-artifacts", yams::cli::ui::Ansi::CYAN)
                      << "   - Git internal files (.git/objects, .git/logs, .git/refs)\n";
            std::cout << "  " << yams::cli::ui::colorize("logs", yams::cli::ui::Ansi::CYAN)
                      << "             - Build and test logs\n";
            std::cout << "  " << yams::cli::ui::colorize("cache", yams::cli::ui::Ansi::CYAN)
                      << "            - Compiler and package manager cache\n";
            std::cout << "  " << yams::cli::ui::colorize("temp", yams::cli::ui::Ansi::CYAN)
                      << "             - Temporary files, backups, swap files\n";
            std::cout << "  " << yams::cli::ui::colorize("coverage", yams::cli::ui::Ansi::CYAN)
                      << "         - Code coverage data\n";
            std::cout << "  " << yams::cli::ui::colorize("ide", yams::cli::ui::Ansi::CYAN)
                      << "              - IDE project files and caches\n";
            std::cout << "  " << yams::cli::ui::colorize("all", yams::cli::ui::Ansi::CYAN)
                      << "              - Everything except distribution packages\n\n";

            std::cout << yams::cli::ui::subsection_header("Specific Categories") << "\n\n";
            std::cout << "  build-object, build-library, build-executable, build-archive\n";
            std::cout << "  system-cmake, system-ninja, system-meson, system-make\n";
            std::cout << "  system-gradle, system-maven, system-npm, system-cargo, system-go\n";
            std::cout << "  system-flutter, system-dart\n\n";

            std::cout << yams::cli::ui::subsection_header("Usage Examples") << "\n\n";
            std::cout << "  "
                      << yams::cli::ui::colorize("# Preview what would be deleted",
                                                 yams::cli::ui::Ansi::DIM)
                      << "\n";
            std::cout << "  yams doctor prune --category build-artifacts\n\n";
            std::cout << "  "
                      << yams::cli::ui::colorize("# Delete old logs", yams::cli::ui::Ansi::DIM)
                      << "\n";
            std::cout << "  yams doctor prune --category logs --older-than 30d --apply\n\n";
            std::cout << "  "
                      << yams::cli::ui::colorize("# Delete by extension", yams::cli::ui::Ansi::DIM)
                      << "\n";
            std::cout << "  yams doctor prune --extension o,obj,log --apply\n\n";
            std::cout << "  "
                      << yams::cli::ui::colorize("# Delete multiple categories",
                                                 yams::cli::ui::Ansi::DIM)
                      << "\n";
            std::cout << "  yams doctor prune --category build,temp --apply\n\n";

            std::cout << yams::cli::ui::colorize("Note:", yams::cli::ui::Ansi::YELLOW)
                      << " Prune operates on files tracked in YAMS metadata.\n";
            std::cout << "      Use --apply to actually delete files (dry-run by default).\n\n";
            return;
        }

        if (!cli_) {
            printError("CLI context unavailable");
            return;
        }

        // Build prune request
        daemon::PruneRequest req;
        req.categories = pruneCategories_;
        req.extensions = pruneExtensions_;
        req.olderThan = pruneOlderThan_;
        req.largerThan = pruneLargerThan_;
        req.smallerThan = pruneSmallerThan_;
        req.dryRun = !pruneApply_;
        req.verbose = pruneVerbose_;

        daemon::ClientConfig clientCfg;
        clientCfg.socketPath = daemon::DaemonClient::resolveSocketPathConfigFirst();
        clientCfg.requestTimeout = std::chrono::milliseconds(300000);

        if (!daemon::DaemonClient::isDaemonRunning(clientCfg.socketPath)) {
            printError("Daemon not running on socket: " + clientCfg.socketPath.string());
            std::cout << yams::cli::ui::colorize("\nHint:", yams::cli::ui::Ansi::YELLOW)
                      << " Start the daemon with 'yams daemon start'\n\n";
            return;
        }

        std::cout << "\n"
                  << yams::cli::ui::colorize("Sending prune request to daemon...",
                                             yams::cli::ui::Ansi::CYAN)
                  << "\n";

        auto leaseRes = acquire_cli_daemon_client(clientCfg, 1, 4);
        if (!leaseRes) {
            printError("Failed to acquire daemon client: " + leaseRes.error().message);
            return;
        }

        auto lease = std::move(leaseRes.value());

        daemon::PruneResponse resp;
        auto runPruneRequest = [&]() -> Result<void> {
            auto respRes = run_result(lease->call<daemon::PruneRequest>(req),
                                      std::chrono::milliseconds(300000));
            if (!respRes)
                return respRes.error();
            resp = respRes.value();
            return Result<void>();
        };
        auto spinnerRes = runWithSpinner("Pruning", runPruneRequest, 300000);
        if (!spinnerRes) {
            printError("Prune request timed out");
            return;
        }
        if (!spinnerRes.value()) {
            printError("Prune request failed: " + spinnerRes.value().error().message);
            return;
        }

        if (!resp.errorMessage.empty()) {
            printError("Prune operation failed: " + resp.errorMessage);
            return;
        }

        // Display status message if present (e.g., async job queued)
        if (!resp.statusMessage.empty()) {
            std::cout << "\n"
                      << yams::cli::ui::colorize(resp.statusMessage, yams::cli::ui::Ansi::CYAN)
                      << "\n";
            std::cout
                << "Note: Prune is running in the background. Check daemon logs for progress.\n";
            return;
        }

        // Display results
        std::cout << "\n" << yams::cli::ui::section_header("Prune Summary") << "\n\n";

        uint64_t totalFiles = resp.filesDeleted + resp.filesFailed;
        std::cout << "  " << yams::cli::ui::colorize("Total files:", yams::cli::ui::Ansi::BOLD)
                  << " " << totalFiles << "\n";
        std::cout << "  " << yams::cli::ui::colorize("Total size:", yams::cli::ui::Ansi::BOLD)
                  << " " << std::fixed << std::setprecision(2)
                  << (resp.totalBytesFreed / 1024.0 / 1024.0) << " MB\n\n";

        if (!resp.categoryCounts.empty()) {
            std::cout << yams::cli::ui::subsection_header("By Category") << "\n\n";
            for (const auto& [cat, count] : resp.categoryCounts) {
                auto it = resp.categorySizes.find(cat);
                uint64_t size = (it != resp.categorySizes.end()) ? it->second : 0;
                double sizeMB = size / 1024.0 / 1024.0;
                std::cout << "  " << yams::cli::ui::colorize(cat, yams::cli::ui::Ansi::CYAN) << ": "
                          << count << " files, " << std::fixed << std::setprecision(2) << sizeMB
                          << " MB\n";
            }
            std::cout << "\n";
        }

        if (req.dryRun) {
            std::cout << yams::cli::ui::colorize("⚠ Dry-run mode.", yams::cli::ui::Ansi::YELLOW)
                      << " Use " << yams::cli::ui::colorize("--apply", yams::cli::ui::Ansi::BOLD)
                      << " to actually delete files.\n\n";
        } else {
            std::cout << yams::cli::ui::colorize("✓ Deleted " + std::to_string(resp.filesDeleted) +
                                                     " files",
                                                 yams::cli::ui::Ansi::GREEN)
                      << "\n";
            if (resp.filesFailed > 0) {
                std::cout << yams::cli::ui::colorize("⚠ Failed to delete " +
                                                         std::to_string(resp.filesFailed) +
                                                         " files",
                                                     yams::cli::ui::Ansi::YELLOW)
                          << "\n";
            }
            std::cout << "\n";
        }

    } catch (const std::exception& e) {
        printError(std::string("Prune error: ") + e.what());
    }
}
} // namespace yams::cli
