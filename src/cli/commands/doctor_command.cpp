#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <yams/app/services/services.hpp>
#include <yams/vector/vector_utils.h>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/doctor_checks.h>
#include <yams/cli/plugin_util.h>
#include <yams/cli/recommendation_util.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/tune_runner.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/common/fs_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/daemon/resource/abi_plugin_loader.h>
#include <yams/daemon/resource/model_provider.h>
#include <yams/extraction/extraction_util.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/query_helpers.h>
#include <yams/repair/embedding_repair_util.h>
#include <yams/config/config_helpers.h>
#include <yams/search/benchmark_history_store.h>
#include <yams/search/internal_benchmark.h>
#include <yams/search/relevance_label_store.h>
#include <yams/search/search_engine.h>
#include <yams/search/search_tuner.h>
#include <yams/storage/storage_runtime_resolver.h>
#include <yams/vector/sqlite_vec_backend.h>
#include <yams/vector/vector_database.h>

#include <yams/cli/doctor/checks/db_integrity.h>
#include <yams/cli/doctor/checks/dim_consistency.h>
#include <yams/cli/doctor/checks/model_check.h>
#include <yams/cli/doctor/checks/orphan_summary.h>
#include <yams/cli/doctor/checks/vec0_check.h>
#include <yams/cli/doctor/checks/embedding_health.h>
#include <yams/cli/doctor/checks/storage_blob_check.h>
#include <yams/cli/doctor/checks/ref_count_check.h>
#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/doctor/rendering/display.h>
#include <yams/cli/doctor/rendering/render.h>
#include <yams/cli/doctor/repairs/vector_fix.h>
#include <yams/cli/doctor/repairs/dedupe_helpers.h>
#include <yams/cli/doctor/repairs/db_repair.h>
#include <yams/cli/doctor/prune.h>
#include <yams/cli/doctor/benchmark.h>
#include <yams/cli/doctor/plugin_trust.h>
#include <yams/cli/doctor/checks/daemon_check.h>
#include <yams/cli/doctor/checks/plugin_check.h>
#include <yams/cli/doctor/repairs/dedupe.h>

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
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <optional>
#include <regex>
#include <set>
#ifndef _WIN32
#include <signal.h>
#endif
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <CLI/CLI.hpp>
#include <yams/plugins/model_provider_v1.h>

namespace {
std::atomic<bool> g_doctor_cancel_requested{false};
volatile std::sig_atomic_t g_doctor_sigint_seen = 0;

struct DoctorSignalGuard {
    DoctorSignalGuard() {
        g_doctor_cancel_requested.store(false, std::memory_order_relaxed);
        g_doctor_sigint_seen = 0;

#ifdef _WIN32
        prevInt_ = std::signal(SIGINT, &DoctorSignalGuard::handler);
        prevTerm_ = std::signal(SIGTERM, &DoctorSignalGuard::handler);
#else
        struct sigaction sa = {};
        sa.sa_handler = &DoctorSignalGuard::handler;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        haveInt_ = (sigaction(SIGINT, &sa, &oldInt_) == 0);
        haveTerm_ = (sigaction(SIGTERM, &sa, &oldTerm_) == 0);
#endif
    }

    ~DoctorSignalGuard() {
#ifdef _WIN32
        if (prevInt_ != SIG_ERR)
            std::signal(SIGINT, prevInt_);
        if (prevTerm_ != SIG_ERR)
            std::signal(SIGTERM, prevTerm_);
#else
        if (haveInt_)
            (void)sigaction(SIGINT, &oldInt_, nullptr);
        if (haveTerm_)
            (void)sigaction(SIGTERM, &oldTerm_, nullptr);
#endif
    }

    DoctorSignalGuard(const DoctorSignalGuard&) = delete;
    DoctorSignalGuard& operator=(const DoctorSignalGuard&) = delete;

    static void handler(int /*sig*/) {
        g_doctor_cancel_requested.store(true, std::memory_order_relaxed);
        // Second Ctrl-C should terminate immediately even if we're blocked in RPC.
        if (g_doctor_sigint_seen) {
            std::_Exit(130);
        }
        g_doctor_sigint_seen = 1;
    }

private:
#ifdef _WIN32
    using SigFn = void (*)(int);
    SigFn prevInt_{SIG_ERR};
    SigFn prevTerm_{SIG_ERR};
#else
    bool haveInt_{false};
    bool haveTerm_{false};
    struct sigaction oldInt_{};
    struct sigaction oldTerm_{};
#endif
};

} // namespace

namespace yams::cli {

namespace {

using SemanticDedupeMatch = doctor::SemanticDedupeMatch;
using SemanticDedupeGroupPlan = doctor::SemanticDedupeGroupPlan;
using SemanticDedupeAnalysis = doctor::SemanticDedupeAnalysis;

// `cosineSimilarity`, `normalizeTextForTokens`, `tokenSet`, and
// `jaccardOverlap` were copy-pasted into
// `src/cli/commands/doctor/repairs/dedupe.cpp` (lines 53/60/93) and the
// originals here became unused. Removed 2026-05-15 after the fold SAST
// audit flagged them; the canonical definitions remain in dedupe.cpp.

} // namespace

// Note: vecutil (yams::cli::vecutil) and plugin (yams::cli::plugin) namespaces
// are available from vector_db_util.h and plugin_util.h respectively

class DoctorCommand : public ICommand {
public:
    std::string getName() const override { return "doctor"; }
    std::string getDescription() const override {
        return "Diagnose daemon connectivity and plugin health";
    }
    void registerCommand(CLI::App& app, YamsCLI* cli) override;

    Result<void> execute() override { return Result<void>(); }
    boost::asio::awaitable<Result<void>> executeAsync() override {
        // Handle --vectors flag first (detect and fix dimension mismatch)
        if (vectorsFix_) {
            try {
                runVectorsFix();
            } catch (const std::exception& e) {
                std::cout << "Doctor --vectors error: " << e.what() << "\n";
                co_return Error{ErrorCode::Unknown, e.what()};
            }
            co_return Result<void>();
        }
        // Only run default doctor if no subcommand was invoked
        // Subcommands set their own flags and handle execution themselves
        if (!subcommandInvoked_ && !fixEmbeddings_ && !fixFts5_ && !fixGraph_ && !validateGraph_ &&
            !fixAll_ && !fixAllTop_ && !dedupeApply_ && !pruneInvoked_ && !benchmarkInvoked_ &&
            pluginArg_.empty() && !fixConfigDims_ && !recreateVectors_) {
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

    // Resolve embedding dimension for DB creation (delegating to extracted utility)
    std::pair<size_t, std::string> resolveEmbeddingDim() {
        std::filesystem::path dataPath = cli_ ? cli_->getDataPath() : yams::config::get_data_dir();
        auto resolved = vecutil::resolveEmbeddingDimension(cli_, dataPath);
        return {resolved.dimension, resolved.source};
    }

    // (Removed clearEmbeddingDegraded helper; subcommand placeholder was eliminated. If
    // reintroduced, prefer declaring the method before use or moving implementation
    // out-of-line to avoid ordering issues in inline class definition bodies.)
    void clearEmbeddingDegraded() {
        using namespace yams::daemon;
        try {
            // Connect to daemon
            ClientConfig cfg;
            cfg.executor = getExecutor();
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
            auto status =
                yams::cli::run_result(client.status(), std::chrono::seconds(3), getExecutor());
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
            auto lres = yams::cli::run_result(client.loadModel(lreq), std::chrono::seconds(30),
                                              getExecutor());
            if (!lres) {
                std::cout << "Model load failed: " << lres.error().message << "\n";
                return;
            }

            // Verify status again
            auto s2 =
                yams::cli::run_result(client.status(), std::chrono::seconds(5), getExecutor());
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

    static void printSummary(const std::string& title, const std::vector<StepResult>& steps) {
        printHeader(title);
        for (const auto& s : steps) {
            std::cout << "  " << (s.ok ? ui::status_ok(s.name) : ui::status_error(s.name));
            if (!s.message.empty())
                std::cout << " — " << s.message;
            std::cout << "\n";
        }
    }
    // Step helpers to make doctor logic composable (delegating to extracted utilities)
    Result<void> touchDbFile(const std::filesystem::path& dbPath) {
        return vecutil::ensureDbFile(dbPath);
    }

    // Legacy lock helpers kept for reference; no-ops now
    Result<void> acquireMaintenanceLock(const std::filesystem::path&) { return Result<void>(); }
    void releaseMaintenanceLock(const std::filesystem::path&) {}

    // Stop the daemon gracefully if it is running (best-effort)
    void ensureDaemonStopped() {
        try {
            yams::daemon::ClientConfig ccfg;
            ccfg.executor = getExecutor();
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
            (void)yams::cli::run_result(shut.shutdown(true), std::chrono::seconds(6),
                                        getExecutor());
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
        vecutil::writeVectorSentinel(dataDir, dim);
    }
    // ============ Config Helpers (delegating to extracted utilities) ============
    static std::filesystem::path resolveConfigPath() { return yams::config::get_config_path(); }

    // ConfigDims is now yams::config::DimensionConfig
    using ConfigDims = yams::config::DimensionConfig;

    static ConfigDims readConfigDims(const std::filesystem::path& cfg) {
        return yams::config::read_dimension_config(cfg);
    }

    static bool writeOrReplaceConfigDims(const std::filesystem::path& cfg, size_t dim) {
        return yams::config::write_dimension_config(cfg, dim);
    }

    using R2KeychainStatus = doctor::R2ConfigStatus;

    static R2KeychainStatus evaluateR2KeychainStatus() {
        return doctor::DoctorContext::evaluateR2Config();
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
        if (fixAll_) {
            fixEmbeddings_ = true;
            fixFts5_ = true;
            fixGraph_ = true;
        }
        doctor::DbRepairCommand::Config cfg;
        cfg.repairEmbeddings = fixEmbeddings_;
        cfg.repairFts5 = fixFts5_;
        cfg.repairGraph = fixGraph_;
        cfg.noDaemonRepair = noDaemonRepair_;
        doctor::DbRepairCommand cmd;
        cmd.execute(std::cout, cli_, cfg);
    }

    // Build/repair knowledge graph using tags/metadata (non-destructive)
    // repairGraph declared earlier in the class

    // Minimal daemon check: connect and get status
    void checkDaemon(std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        doctor::DaemonCheck check;
        check.execute(std::cout, cli_, cachedStatus);
    }

    static bool parseBoolValue(std::string value, bool fallback) {
        if (value.empty()) {
            return fallback;
        }
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value == "1" || value == "true" || value == "yes" || value == "on";
    }

    using DoctorCachedState = doctor::CachedDaemonState;

    DoctorCachedState collectDoctorCachedState() const {
        return doctor::DoctorContext(cli_).cachedState();
    }

    nlohmann::json buildDoctorJsonResult(const DoctorCachedState& cachedState,
                                         const yams::cli::RecommendationBuilder& recs) {
        return doctor::DoctorRender::buildDoctorJson(cli_, cachedState, &recs);
    }

    void renderDoctorR2Credentials(yams::cli::RecommendationBuilder& recs) {
        auto r2 = doctor::DoctorContext::evaluateR2Config();
        if (!r2.enabled)
            return;
        std::cout << "\n" << yams::cli::ui::section_header("R2 Credentials") << "\n\n";
        std::vector<yams::cli::ui::Row> rows;
        rows.push_back({"Auth Mode", r2.authMode, ""});
        rows.push_back({"Account ID", r2.accountId.empty() ? "(unset)" : r2.accountId, ""});
        std::string kc =
            r2.keychainSupported ? (r2.tokenPresent ? "present" : "missing") : "not supported";
        rows.push_back(
            {"Keychain Token",
             doctor::DoctorContext::evaluateR2Config().keychainSupported
                 ? yams::cli::ui::colorize(kc, r2.tokenPresent ? yams::cli::ui::Ansi::GREEN
                                                               : yams::cli::ui::Ansi::YELLOW)
                 : yams::cli::ui::colorize(kc, yams::cli::ui::Ansi::DIM),
             ""});
        if (!r2.detail.empty())
            rows.push_back({"Detail", r2.detail, ""});
        yams::cli::ui::render_rows(std::cout, rows);
        if (r2.keychainSupported && !r2.tokenPresent)
            recs.warning("DOCTOR_R2_KEYCHAIN_MISSING",
                         "R2 temp-credentials mode is configured but keychain token is missing.");
    }

    void
    renderDoctorEmbeddingRuntime(const std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        try {
            if (!cachedStatus) {
                return;
            }

            const auto& s = *cachedStatus;
            std::cout << "\n" << yams::cli::ui::section_header("Embedding Runtime") << "\n\n";

            std::vector<yams::cli::ui::Row> embRows;
            std::string availStatus =
                s.embeddingAvailable ? yams::cli::ui::colorize("✓ yes", yams::cli::ui::Ansi::GREEN)
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
        } catch (...) {
        }
    }

    void renderDoctorKnowledgeGraph(yams::cli::RecommendationBuilder& recs) {
        doctor::DoctorDisplay::renderKnowledgeGraph(std::cout, cli_, recs);
    }
    void renderDoctorLiveRepairProgress() {
        doctor::DoctorDisplay::renderLiveRepairProgress(std::cout, cli_);
    }

    void renderDoctorLoadedPlugins(std::optional<yams::daemon::StatusResponse>& cachedStatus,
                                   std::optional<yams::daemon::GetStatsResponse>& cachedStats) {
        (void)cachedStatus;
        (void)cachedStats;
    }

    static bool isTrustedPath(const std::filesystem::path& p,
                              const std::set<std::filesystem::path>& roots) {
        return doctor::PluginTrust::isTrustedPath(p, roots);
    }

    static std::optional<std::filesystem::path> resolveByName(const std::string& name) {
        return doctor::PluginTrust::resolveByName(name);
    }

    // Perform local dlopen + symbol/iface probes
    void checkPlugin(const std::string& arg) {
        doctor::PluginCheck::Config cfg;
        cfg.arg = arg;
        cfg.ifaceId = ifaceId_;
        cfg.ifaceVersion = ifaceVersion_;
        cfg.noDaemonProbe = noDaemonProbe_;
        doctor::PluginCheck check;
        check.execute(std::cout, cli_, cfg);
    }

    // doctor (no args): quick combined
    void runAll() {
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

        const auto cachedState = collectDoctorCachedState();
        const bool daemon_up = cachedState.daemonUp;

        auto cachedStatus = cachedState.status;
        auto cachedStats = cachedState.stats;

        // Clear loading message
        if (!useJson) {
            std::cout << "\r" << std::string(50, ' ') << "\r" << std::flush;
        }

        // For JSON mode, collect all data and output at end
        if (useJson) {
            auto jsonResult = buildDoctorJsonResult(cachedState, recs);
            std::cout << jsonResult.dump(2) << "\n";
            return;
        }

        checkDaemon(cachedStatus);
        if (!daemon_up) {
            // Only local checks when daemon is unavailable
            checkInstalledModels(cli_);
            checkVec0Module(); // Check vec0 module even when daemon is down
            checkEmbeddingDimMismatch(cachedStatus);
            checkDbIntegrity(cachedStatus);
            checkOrphanSummary(cachedStatus);
            checkStorageBlob(cachedStatus);
            checkRefCount();
            return;
        }
        checkInstalledModels(cli_);
        checkVec0Module(); // Check vec0 module availability and schema
        checkEmbeddingDimMismatch(cachedStatus);
        checkDbIntegrity(cachedStatus);
        checkOrphanSummary(cachedStatus);
        checkEmbeddingHealth(cachedStatus);
        checkStorageBlob(cachedStatus);
        checkRefCount();
        renderDoctorR2Credentials(recs);
        renderDoctorEmbeddingRuntime(cachedStatus);

        renderDoctorKnowledgeGraph(recs);
        renderDoctorLoadedPlugins(cachedStatus, cachedStats);

        // Emit collected recommendations (text only for now)
        if (!recs.empty()) {
            yams::cli::printRecommendationsText(recs, std::cout);
        }

        std::cout << "\nHint: run 'yams doctor plugin <path|name>' for a deep plugin check.\n";
        renderDoctorLiveRepairProgress();
    }

    static void checkInstalledModels(YamsCLI* cli) {
        doctor::DoctorContext ctx(cli);
        doctor::ModelCheck check;
        auto result = check.execute(ctx);
        doctor::ModelCheck::render(std::cout, result);
    }

    // Check if vec0 module is available and vector DB schema is valid
    void checkVec0Module() {
        doctor::DoctorContext ctx(cli_);
        doctor::Vec0Check check;
        auto result = check.execute(ctx);
        doctor::Vec0Check::render(std::cout, result);
    }

    void checkEmbeddingDimMismatch(std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        doctor::DoctorContext ctx(cli_);
        doctor::DimConsistencyCheck check;
        auto result = check.execute(ctx, cachedStatus ? &cachedStatus.value() : nullptr);
        doctor::DimConsistencyCheck::render(std::cout, result);
    }

    void checkDbIntegrity(std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        doctor::DoctorContext ctx(cli_);
        doctor::DbIntegrityCheck check;
        auto result = check.execute(ctx);
        doctor::DbIntegrityCheck::render(std::cout, result);
    }

    void checkOrphanSummary(std::optional<yams::daemon::StatusResponse>&) {
        doctor::DoctorContext ctx(cli_);
        doctor::OrphanSummaryCheck check;
        auto result = check.execute(ctx);
        doctor::OrphanSummaryCheck::render(std::cout, result);
    }

    void checkEmbeddingHealth(std::optional<yams::daemon::StatusResponse>& cachedStatus) {
        doctor::DoctorContext ctx(cli_);
        doctor::EmbeddingHealthCheck check;
        auto result = check.execute(ctx, cachedStatus ? &cachedStatus.value() : nullptr);
        doctor::EmbeddingHealthCheck::render(std::cout, result);
    }

    void checkStorageBlob(std::optional<yams::daemon::StatusResponse>&) {
        doctor::DoctorContext ctx(cli_);
        doctor::StorageBlobCheck check;
        auto result = check.execute(ctx);
        doctor::StorageBlobCheck::render(std::cout, result);
    }

    void checkRefCount() {
        doctor::DoctorContext ctx(cli_);
        doctor::RefCountCheck check;
        auto result = check.execute(ctx);
        doctor::RefCountCheck::render(std::cout, result);
    }

    YamsCLI* cli_{nullptr};
    bool jsonOutput_{false};
    bool fixEmbeddings_{false};
    bool fixFts5_{false};
    bool fixGraph_{false};
    bool noDaemonRepair_{false};
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
    bool vectorsFix_{false}; // --vectors flag: detect and fix dimension mismatch
    bool subcommandInvoked_{false};
    bool immediateSubcommandHandled_{false};
    // Dedupe state
    bool dedupeApply_{false};
    std::string dedupeMode_{"path"};
    std::string dedupeStrategy_{"keep-newest"};
    bool dedupeForce_{false};
    bool dedupeVerbose_{false};
    bool dedupeList_{false};
    int dedupeListLimit_{25};
    std::string dedupeGroupKey_;
    double dedupeSemanticThreshold_{0.92};
    Result<void> repairGraph();
    Result<void> validateGraph();
    void runDedupe();
    void runVectorsFix(); // Implementation of yams doctor --vectors
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
    // Benchmark state
    bool benchmarkInvoked_{false};
    size_t benchmarkQueries_{100};
    bool benchmarkJson_{false};
    bool benchmarkVerbose_{false};
    std::string benchmarkSaveBaseline_;
    std::string benchmarkCompareBaseline_;
    bool benchmarkHistory_{false};
    std::size_t benchmarkHistoryLimit_{20};
    bool benchmarkHistoryJson_{false};
    void runBenchmark();
    void printBenchmarkHistory();
    // Interactive relevance-feedback tuner (F1)
    bool tuneInvoked_{false};
    std::size_t tuneQueries_{10};
    std::size_t tuneK_{5};
    std::uint64_t tuneSeed_{0};
    bool tuneJson_{false};
    bool tuneNonInteractive_{false};
    void runTune();
    // Tuning helpers
    Result<void> applyTuningBaseline(bool apply);
    std::map<std::string, std::string> parseSimpleToml(const std::filesystem::path& path) const;
    std::filesystem::path getConfigPath() const;
    Result<void> writeConfigValue(const std::string& key, const std::string& value);
};

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
    doctor->add_flag("--vectors", vectorsFix_,
                     "Detect and fix embedding dimension mismatch (updates config to match DB)");

    doctor->callback([this]() {
        if (!immediateSubcommandHandled_) {
            cli_->setPendingCommand(this);
        }
    });

    auto* dsub = doctor->add_subcommand("daemon", "Check daemon socket and status");
    dsub->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        std::optional<yams::daemon::StatusResponse> status;
        checkDaemon(status);
    });

    auto* psub = doctor->add_subcommand("plugin", "Check a plugin (.so/.wasm or by name)");
    psub->add_option("target", pluginArg_, "Plugin path or logical name");
    psub->add_option("--iface", ifaceId_, "Interface ID to probe (default: model_provider_v1)");
    psub->add_option("--iface-version", ifaceVersion_, "Interface version (default: 1)");
    psub->add_flag("--no-daemon", noDaemonProbe_, "Skip daemon dry-run load");
    psub->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
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
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        runAll();
    });

    auto* emb = doctor->add_subcommand("embeddings", "Embeddings diagnostics and actions");
    emb->require_subcommand();
    emb->add_subcommand("clear-degraded",
                        "Attempt to clear embedding degraded state (reloads preferred model)")
        ->callback([this]() {
            subcommandInvoked_ = true;
            immediateSubcommandHandled_ = true;
            clearEmbeddingDegraded();
        });

    auto* rsub = doctor->add_subcommand("repair", "Repair common issues (embeddings, FTS5, graph)");
    rsub->add_flag("--embeddings", fixEmbeddings_, "Generate missing vector embeddings");
    rsub->add_flag("--fts5", fixFts5_, "Rebuild FTS5 text index (best-effort)");
    rsub->add_flag("--graph", fixGraph_, "Construct/repair knowledge graph from tags and metadata");
    rsub->add_flag("--all", fixAll_, "Run all repair operations");
    rsub->add_flag("--no-daemon", noDaemonRepair_,
                   "Skip daemon RPC and run local repair only (best-effort)");
    rsub->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        runRepair();
    });

    auto* vsub = doctor->add_subcommand("validate", "Validate knowledge graph health");
    vsub->add_flag("--graph", validateGraph_, "Validate knowledge graph integrity");
    vsub->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        if (validateGraph_) {
            returnOnError(validateGraph(), "Validation");
            return;
        }
        std::cout << "Nothing to validate. Use --graph.\n";
    });

    auto* dd = doctor->add_subcommand(
        "dedupe", "Detect (and optionally remove) duplicate documents (metadata)");
    auto* ddList = dd->add_subcommand("list", "List persisted semantic duplicate suggestions");
    ddList->add_option("--limit", dedupeListLimit_, "Maximum groups to show")->default_val(25);
    ddList->add_option("--group-key", dedupeGroupKey_, "Show only a specific semantic group key");
    ddList->add_flag("-v,--verbose", dedupeVerbose_, "Show member details for each group");
    ddList->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        dedupeList_ = true;
        dedupeMode_ = "semantic";
        runDedupe();
    });
    dd->add_flag("--apply", dedupeApply_, "Apply deletions (default: dry-run)");
    dd->add_option("--mode", dedupeMode_, "Grouping mode: path | name | hash")
        ->default_val("path")
        ->check(CLI::IsMember({"path", "name", "hash", "semantic"}));
    dd->add_option("--strategy", dedupeStrategy_,
                   "Keep strategy: keep-newest | keep-oldest | keep-largest")
        ->default_val("keep-newest")
        ->check(CLI::IsMember({"keep-newest", "keep-oldest", "keep-largest"}));
    dd->add_flag("--force", dedupeForce_,
                 "Allow deletion even when differing hashes (treat as duplicates)");
    dd->add_option("--semantic-threshold", dedupeSemanticThreshold_,
                   "Cosine threshold for semantic dedupe report mode")
        ->default_val(0.92);
    dd->add_flag("-v,--verbose", dedupeVerbose_, "Verbose listing of each group");
    dd->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        runDedupe();
    });

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
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        pruneInvoked_ = true;
        runPrune();
    });

    // Search quality benchmark
    auto* bench = doctor->add_subcommand("benchmark",
                                         "Run internal search quality benchmark (MRR, Recall@K)");
    bench->add_option("-q,--queries", benchmarkQueries_, "Number of queries to run")
        ->default_val(100);
    bench->add_flag("-j,--json", benchmarkJson_, "Output results as JSON");
    bench->add_flag("-v,--verbose", benchmarkVerbose_, "Verbose per-query output");
    bench->add_option("--save-baseline", benchmarkSaveBaseline_,
                      "Save results to file for future comparison");
    bench->add_option("--compare-baseline", benchmarkCompareBaseline_,
                      "Compare against saved baseline results");
    bench->add_flag("--history", benchmarkHistory_,
                    "Print persisted benchmark history and exit (no new run)");
    bench
        ->add_option("--history-limit", benchmarkHistoryLimit_,
                     "Maximum number of history rows to print")
        ->default_val(20);
    bench->add_flag("--history-json", benchmarkHistoryJson_,
                    "Emit history as JSON array (implies --history)");
    bench->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        benchmarkInvoked_ = true;
        runBenchmark();
    });

    // Interactive relevance-feedback tuner (F1).
    // Layered under doctor rather than a new top-level verb: tuning is diagnostic
    // / non-production tooling, so it belongs next to benchmark and tuning baseline.
    auto* tune =
        doctor->add_subcommand("tune", "Interactive relevance-feedback tuner over your corpus");
    tune->add_option("-q,--queries", tuneQueries_, "Number of synthetic queries to label")
        ->default_val(10);
    tune->add_option("-k,--top-k", tuneK_, "Results to label per query")->default_val(5);
    tune->add_option("--seed", tuneSeed_, "Random seed for query generation (0 = random)")
        ->default_val(0);
    tune->add_flag("--json", tuneJson_, "Emit the persisted session as JSON to stdout");
    tune->add_flag("--non-interactive", tuneNonInteractive_,
                   "Skip prompts; print help and exit 0 (CI-safe)");
    tune->callback([this]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        tuneInvoked_ = true;
        runTune();
    });

    // Auto-tuning baseline
    auto* tsub =
        doctor->add_subcommand("tuning", "Auto-configure [tuning] based on system baseline");
    bool apply = false;
    tsub->add_flag("--apply", apply, "Write suggestions to config.toml [tuning] section");
    tsub->callback([this, &apply]() {
        subcommandInvoked_ = true;
        immediateSubcommandHandled_ = true;
        auto r = applyTuningBaseline(apply);
        if (!r) {
            spdlog::error("Doctor tuning failed: {}", r.error().message);
            std::exit(1);
        }
    });
}

// --- TOML helpers (delegating to shared utilities) ---
std::map<std::string, std::string>
DoctorCommand::parseSimpleToml(const std::filesystem::path& path) const {
    return yams::config::parse_simple_toml(path);
}

std::filesystem::path DoctorCommand::getConfigPath() const {
    return yams::config::get_config_path();
}

Result<void> DoctorCommand::writeConfigValue(const std::string& key, const std::string& value) {
    auto configPath = getConfigPath();
    if (yams::config::write_config_value(configPath, key, value)) {
        return Result<void>();
    }
    return Error{ErrorCode::WriteError, "Failed to write config key: " + key};
}

Result<void> DoctorCommand::applyTuningBaseline(bool apply) {
    doctor::DoctorContext::applyTuningBaseline(std::cout, apply);
    return Result<void>();
}

// Build/repair knowledge graph using tags/metadata (daemon-first approach)
Result<void> DoctorCommand::repairGraph() {
    doctor::DoctorContext::repairGraph(std::cout, cli_);
    return Result<void>();
}

// Validate knowledge graph health
Result<void> DoctorCommand::validateGraph() {
    doctor::DoctorContext::validateGraph(std::cout, cli_);
    return Result<void>();
}

void DoctorCommand::runDedupe() {
    doctor::DedupeCommand::Config cfg;
    cfg.mode = dedupeMode_;
    cfg.strategy = dedupeStrategy_;
    cfg.semanticThreshold = dedupeSemanticThreshold_;
    cfg.apply = dedupeApply_;
    cfg.verbose = dedupeVerbose_;
    cfg.force = dedupeForce_;
    cfg.listOnly = dedupeList_;
    cfg.listLimit = dedupeListLimit_;
    cfg.groupKey = dedupeGroupKey_;
    doctor::DedupeCommand cmd;
    cmd.execute(std::cout, cli_, cfg);
}

void DoctorCommand::runPrune() {
    doctor::PruneCommand::Config cfg;
    cfg.categories = pruneCategories_;
    cfg.extensions = pruneExtensions_;
    cfg.olderThan = pruneOlderThan_;
    cfg.largerThan = pruneLargerThan_;
    cfg.smallerThan = pruneSmallerThan_;
    cfg.apply = pruneApply_;
    cfg.verbose = pruneVerbose_;
    doctor::PruneCommand cmd(cli_, cfg);
    cmd.execute(std::cout);
}

void DoctorCommand::runBenchmark() {
    if (benchmarkHistory_ || benchmarkHistoryJson_) {
        printBenchmarkHistory();
        return;
    }
    doctor::BenchmarkCommand::Config cfg;
    cfg.queryCount = benchmarkQueries_;
    cfg.verbose = benchmarkVerbose_;
    cfg.json = benchmarkJson_;
    cfg.saveBaseline = benchmarkSaveBaseline_;
    cfg.compareBaseline = benchmarkCompareBaseline_;
    doctor::BenchmarkCommand cmd(cli_, cfg);
    cmd.execute(std::cout);
}

void DoctorCommand::runTune() {
    std::cerr << "[deprecated] `yams doctor tune` is moving to `yams tune`. "
                 "This alias will be removed in a future release.\n";

    yams::cli::TuneOptions opts;
    opts.queries = tuneQueries_;
    opts.k = tuneK_;
    opts.seed = tuneSeed_;
    opts.json = tuneJson_;
    opts.nonInteractive = tuneNonInteractive_;
    yams::cli::runInteractiveTune(cli_, opts);
}

void DoctorCommand::printBenchmarkHistory() {
    doctor::BenchmarkCommand cmd(cli_, {});
    cmd.printHistory(std::cout, benchmarkHistoryJson_, benchmarkHistoryLimit_);
}

void DoctorCommand::runVectorsFix() {
    namespace fs = std::filesystem;
    bool useJson = jsonOutput_ || (cli_ && cli_->getJsonOutput());

    if (!useJson) {
        printHeader("Vectors Database Check & Fix");
    }

    if (!cli_) {
        if (useJson) {
            std::cout << R"({"error": "CLI context unavailable"})"
                      << "\n";
        } else {
            std::cout << ui::status_error("CLI context unavailable") << "\n";
        }
        return;
    }

    fs::path vecDbPath = cli_->getDataPath() / "vectors.db";
    fs::path configPath = resolveConfigPath();

    // Step 1: Read DB dimension
    std::optional<size_t> dbDim;
    if (fs::exists(vecDbPath)) {
        sqlite3* db = nullptr;
        if (sqlite3_open(vecDbPath.string().c_str(), &db) == SQLITE_OK && db) {
            // Try vectors table first (new schema with embedding_dim column)
            const char* sql1 = "SELECT DISTINCT embedding_dim FROM vectors LIMIT 1";
            sqlite3_stmt* stmt = nullptr;
            if (sqlite3_prepare_v2(db, sql1, -1, &stmt, nullptr) == SQLITE_OK) {
                if (sqlite3_step(stmt) == SQLITE_ROW) {
                    dbDim = static_cast<size_t>(sqlite3_column_int(stmt, 0));
                }
                sqlite3_finalize(stmt);
            }
            // Fallback: try doc_embeddings (legacy vec0 schema)
            if (!dbDim) {
                const char* sql2 =
                    "SELECT sql FROM sqlite_master WHERE name='doc_embeddings' LIMIT 1";
                if (sqlite3_prepare_v2(db, sql2, -1, &stmt, nullptr) == SQLITE_OK) {
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
                                        dbDim = static_cast<size_t>(std::stoul(num));
                                    } catch (...) {
                                    }
                                }
                            }
                        }
                    }
                    sqlite3_finalize(stmt);
                }
            }
            sqlite3_close(db);
        }
    }

    if (!dbDim) {
        if (useJson) {
            std::cout
                << R"({"status": "no_vectors", "message": "No vectors.db found or no vectors stored yet"})"
                << "\n";
        } else {
            std::cout << ui::status_warning("No vectors.db found or no vectors stored yet.")
                      << "\n";
            std::cout << "Nothing to fix. Index some documents first with 'yams add'.\n";
        }
        return;
    }

    if (!useJson) {
        std::cout << "DB stored dimension: " << *dbDim << "\n";
    }

    // Step 2: Find current model dimension
    std::optional<size_t> modelDim;
    std::string modelName;
    fs::path modelsPath = cli_->getDataPath() / "models";

    // Check preferred model from env first
    if (const char* pref = std::getenv("YAMS_PREFERRED_MODEL")) {
        modelName = pref;
    }

    // Then check config file
    if (modelName.empty()) {
        if (!configPath.empty()) {
            auto config = parseSimpleToml(configPath);
            auto it = config.find("embeddings.preferred_model");
            if (it != config.end() && !it->second.empty()) {
                modelName = it->second;
            }
        }
    }

    // If still no preference, find first installed model
    if (modelName.empty()) {
        std::error_code ec;
        if (fs::exists(modelsPath, ec) && fs::is_directory(modelsPath, ec)) {
            for (const auto& entry : fs::directory_iterator(modelsPath, ec)) {
                if (!entry.is_directory())
                    continue;
                if (fs::exists(entry.path() / "model.onnx", ec)) {
                    modelName = entry.path().filename().string();
                    break;
                }
            }
        }
    }

    // Determine model dimension: metadata first, then name heuristics (A2)
    auto getModelDim = [&](const std::string& name) -> std::optional<size_t> {
        auto dataPath = cli_ ? cli_->getDataPath() : yams::config::get_data_dir();
        if (auto metaDim = vecutil::getModelDimensionFromMetadata(dataPath, name))
            return *metaDim;
        return vecutil::getModelDimensionHeuristic(name);
    };

    if (!modelName.empty()) {
        modelDim = getModelDim(modelName);
        if (!useJson) {
            std::cout << "Current model: " << modelName;
            if (modelDim) {
                std::cout << " (" << *modelDim << "-dim)";
            }
            std::cout << "\n";
        }
    }

    // Step 3: Check for mismatch
    if (modelDim && *modelDim == *dbDim) {
        if (useJson) {
            std::cout << "{\"status\": \"ok\", \"db_dim\": " << *dbDim << ", \"model\": \""
                      << modelName << "\", \"model_dim\": " << *modelDim
                      << ", \"mismatch\": false}\n";
        } else {
            std::cout << "\n"
                      << ui::status_ok("No dimension mismatch. DB and model are aligned.") << "\n";
        }
        return;
    }

    if (!useJson) {
        if (modelDim) {
            std::cout << "\n" << ui::status_warning("Dimension mismatch detected:") << "\n";
            std::cout << "  DB has: " << *dbDim << "-dim vectors\n";
            std::cout << "  Model:  " << *modelDim << "-dim (" << modelName << ")\n\n";
        } else {
            std::cout << "\n"
                      << ui::status_warning(
                             "Could not determine model dimension. Checking installed models...")
                      << "\n\n";
        }
    }

    // Step 4: Find a model that matches DB dimension
    std::string matchingModel;
    std::error_code ec;
    if (fs::exists(modelsPath, ec) && fs::is_directory(modelsPath, ec)) {
        for (const auto& entry : fs::directory_iterator(modelsPath, ec)) {
            if (!entry.is_directory())
                continue;
            if (!fs::exists(entry.path() / "model.onnx", ec))
                continue;
            std::string name = entry.path().filename().string();
            auto dim = getModelDim(name);
            if (dim && *dim == *dbDim) {
                matchingModel = name;
                break;
            }
        }
    }

    if (matchingModel.empty()) {
        if (useJson) {
            std::cout << "{\"status\": \"error\", \"db_dim\": " << *dbDim << ", \"model\": \""
                      << modelName << "\"";
            if (modelDim) {
                std::cout << ", \"model_dim\": " << *modelDim;
            }
            std::cout << ", \"mismatch\": true, \"fixed\": false, \"error\": \"No installed model "
                         "matches "
                      << *dbDim << "-dim\"}\n";
        } else {
            std::cout << ui::status_error("No installed model matches " + std::to_string(*dbDim) +
                                          "-dim")
                      << "\n\n";
            std::cout << "Options:\n";
            if (*dbDim == 384) {
                std::cout << "  1. Download optional matching ONNX model: yams model --download "
                             "all-MiniLM-L6-v2\n";
                std::cout << "  2. Recreate vectors with the default Simeon dimension (1024): "
                             "yams doctor --recreate-vectors --dim 1024\n";
            } else if (*dbDim == 768) {
                std::cout << "  1. Download optional matching ONNX model: yams model --download "
                             "nomic-embed-text-v1.5\n";
                std::cout << "  2. Recreate vectors with the default Simeon dimension (1024): "
                             "yams doctor --recreate-vectors --dim 1024\n";
            } else {
                std::cout << "  1. Download an optional model that produces " << *dbDim
                          << "-dim embeddings\n";
                std::cout << "  2. Recreate vectors with the default Simeon dimension (1024): "
                             "yams doctor --recreate-vectors --dim 1024\n";
            }
            std::cout << "  3. Recreate vectors with current model: yams doctor --recreate-vectors";
            if (modelDim) {
                std::cout << " --dim " << *modelDim;
            }
            std::cout << "\n";
        }
        return;
    }

    // Step 5: Fix config to use matching model
    if (!useJson) {
        std::cout << ui::status_ok("Found matching model: " + matchingModel) << "\n\n";
        std::cout << "Updating config to use " << matchingModel << "...\n";
    }

    // Update config file using proper dimension+model config APIs (A3)
    bool configUpdated = false;
    try {
        auto dataPath = cli_ ? cli_->getDataPath() : yams::config::get_data_dir();

        // Write all three dimension keys consistently
        writeOrReplaceConfigDims(configPath, *dbDim);

        // Write model preference
        if (!matchingModel.empty()) {
            yams::config::write_config_value(
                configPath, "embeddings." + std::string("preferred_model"), matchingModel);
        }

        configUpdated = true;
    } catch (const std::exception& e) {
        if (useJson) {
            std::cout << "{\"status\": \"error\", \"error\": \"Failed to update config: "
                      << e.what() << "\"}\n";
        } else {
            std::cout << ui::status_error("Failed to update config: " + std::string(e.what()))
                      << "\n";
        }
        return;
    }

    if (configUpdated) {
        if (useJson) {
            std::cout << "{\"status\": \"fixed\", \"db_dim\": " << *dbDim
                      << ", \"previous_model\": \"" << modelName << "\"";
            if (modelDim) {
                std::cout << ", \"previous_model_dim\": " << *modelDim;
            }
            std::cout << ", \"new_model\": \"" << matchingModel
                      << "\", \"new_model_dim\": " << *dbDim << ", \"config_path\": \""
                      << configPath.string() << "\"}\n";
        } else {
            std::cout << "\n" << ui::status_ok("Config updated: " + configPath.string()) << "\n";
            std::cout << "  embeddings.preferred_model = \"" << matchingModel << "\"\n";
            std::cout << "  embeddings.embedding_dim = " << *dbDim << "\n";
            std::cout << "  vector_database.embedding_dim = " << *dbDim << "\n";
            std::cout << "  vector_index.dimension = " << *dbDim << "\n\n";
            std::cout << "Restart daemon to apply: yams daemon restart\n";
        }
    }
}
} // namespace yams::cli
