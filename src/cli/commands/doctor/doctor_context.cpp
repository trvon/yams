#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/vector_db_util.h>
#include <yams/cli/yams_cli.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/storage/storage_runtime_resolver.h>

#include <sqlite3.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cstdlib>
#include <thread>

extern "C" int sqlite3_vec_init(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi);

namespace yams::cli::doctor {

// ============================================================================
// DoctorSignalGuard
// ============================================================================

static std::atomic<bool> g_doctor_cancel_requested{false};
static volatile std::sig_atomic_t g_doctor_sigint_seen = 0;

DoctorSignalGuard::DoctorSignalGuard() {
    g_doctor_cancel_requested.store(false, std::memory_order_relaxed);
    g_doctor_sigint_seen = 0;

#if defined(_WIN32)
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

DoctorSignalGuard::~DoctorSignalGuard() {
#if defined(_WIN32)
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

void DoctorSignalGuard::handler(int /*sig*/) {
    g_doctor_cancel_requested.store(true, std::memory_order_relaxed);
    if (g_doctor_sigint_seen) {
        std::_Exit(130);
    }
    g_doctor_sigint_seen = 1;
}

bool DoctorSignalGuard::cancelled() {
    return g_doctor_cancel_requested.load(std::memory_order_relaxed);
}

std::atomic<bool>& DoctorSignalGuard::cancelFlag() {
    return g_doctor_cancel_requested;
}

// ============================================================================
// DoctorContext
// ============================================================================

DoctorContext::DoctorContext(YamsCLI* cli) : cli_(cli) {}

DoctorContext::~DoctorContext() = default;

const std::filesystem::path& DoctorContext::dataDir() const {
    if (!cachedDataDir_) {
        cachedDataDir_ = cli_ ? cli_->getDataPath() : yams::config::get_data_dir();
    }
    return *cachedDataDir_;
}

const std::filesystem::path& DoctorContext::configPath() const {
    if (!cachedConfigPath_) {
        cachedConfigPath_ = yams::config::get_config_path();
    }
    return *cachedConfigPath_;
}

bool DoctorContext::jsonOutput() const {
    if (!cachedJsonOutput_) {
        cachedJsonOutput_ = cli_ && cli_->getJsonOutput();
    }
    return *cachedJsonOutput_;
}

const CachedDaemonState& DoctorContext::cachedState() const {
    if (cachedState_) {
        return *cachedState_;
    }

    CachedDaemonState state;
    state.effectiveSocket = YamsCLI::resolveConfiguredDaemonSocketPath().string();
    state.daemonUp = daemon::DaemonClient::isDaemonRunning(state.effectiveSocket);

    if (state.daemonUp) {
        try {
            daemon::ClientConfig cfg;
            if (cli_ && cli_->hasExplicitDataDir()) {
                cfg.dataDir = cli_->getDataPath();
            }
            cfg.requestTimeout = std::chrono::milliseconds(10000);
            auto leaseRes = acquire_cli_daemon_client_shared(cfg);
            if (leaseRes) {
                auto leaseHandle = std::move(leaseRes.value());
                auto& client = **leaseHandle;

                auto sres =
                    run_result<daemon::StatusResponse>(client.status(), std::chrono::seconds(10));
                if (sres) {
                    state.status = std::move(sres.value());
                }

                daemon::GetStatsRequest req;
                req.detailed = true;
                req.showFileTypes = false;
                auto gres = run_result<daemon::GetStatsResponse>(client.getStats(req),
                                                                 std::chrono::milliseconds(10000));
                if (gres) {
                    state.stats = std::move(gres.value());
                }
            }
        } catch (...) {
        }
    }

    cachedState_ = std::move(state);
    return *cachedState_;
}

void DoctorContext::ensureDaemonStopped() {
    try {
        daemon::ClientConfig cfg;
        cfg.requestTimeout = std::chrono::seconds(5);
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        auto leaseRes = acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes)
            return;
        auto leaseHandle = std::move(leaseRes.value());
        auto& shut = **leaseHandle;
        (void)run_result(shut.shutdown(true), std::chrono::seconds(6));
    } catch (...) {
    }
}

yams::config::DimensionConfig DoctorContext::readConfigDims() const {
    return yams::config::read_dimension_config(configPath());
}

bool DoctorContext::writeConfigDims(size_t dim) const {
    return yams::config::write_dimension_config(configPath(), dim);
}

std::map<std::string, std::string> DoctorContext::parseToml() const {
    return yams::config::parse_simple_toml(configPath());
}

std::pair<size_t, std::string> DoctorContext::resolveEmbeddingDim() {
    auto resolved = vecutil::resolveEmbeddingDimension(cli_, dataDir());
    return {resolved.dimension, resolved.source};
}

uint64_t DoctorContext::walFileSize(const std::filesystem::path& dataDir,
                                    const std::string& dbName) {
    std::error_code ec;
    auto sz = std::filesystem::file_size(dataDir / dbName, ec);
    return ec ? 0 : static_cast<uint64_t>(sz);
}

void DoctorContext::forceWalCheckpointLocal(const std::filesystem::path& dbPath) {
    sqlite3* db = nullptr;
    if (sqlite3_open(dbPath.string().c_str(), &db) != SQLITE_OK) {
        spdlog::warn("[DoctorContext] Failed to open {} for WAL checkpoint: {}", dbPath.string(),
                     sqlite3_errmsg(db));
        if (db)
            sqlite3_close(db);
        return;
    }
    int nLog = 0, nCkpt = 0;
    int rc = sqlite3_wal_checkpoint_v2(db, nullptr, SQLITE_CHECKPOINT_TRUNCATE, &nLog, &nCkpt);
    if (rc == SQLITE_OK) {
        spdlog::info("[DoctorContext] Forced TRUNCATE checkpoint on {}: {}/{} frames",
                     dbPath.string(), nCkpt, nLog);
    } else {
        spdlog::warn("[DoctorContext] WAL checkpoint on {} failed: {}", dbPath.string(),
                     sqlite3_errmsg(db));
    }
    sqlite3_close(db);
}

R2ConfigStatus DoctorContext::evaluateR2Config() {
    R2ConfigStatus status;
    auto cfg = yams::config::parse_simple_toml(yams::config::get_config_path());
    auto getValue = [&cfg](const std::string& key,
                           const std::string& fallback = "") -> std::string {
        auto it = cfg.find(key);
        if (it == cfg.end() || it->second.empty())
            return fallback;
        return it->second;
    };

    std::string engine = getValue("storage.engine", "local");
    std::transform(engine.begin(), engine.end(), engine.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (engine != "s3") {
        status.detail = "storage.engine != s3";
        return status;
    }

    std::string authMode = getValue("storage.s3.r2.auth_mode", "direct");
    std::transform(authMode.begin(), authMode.end(), authMode.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    status.authMode = authMode;
    if (authMode != "temp_credentials") {
        status.detail = "storage.s3.r2.auth_mode != temp_credentials";
        return status;
    }

    status.enabled = true;
    status.accountId = getValue("storage.s3.r2.account_id", "");
    if (status.accountId.empty())
        status.accountId =
            yams::storage::extractCloudflareR2AccountId(getValue("storage.s3.endpoint", ""));
    if (status.accountId.empty()) {
        status.detail = "account id is not configured";
        return status;
    }

    auto token = yams::storage::loadCloudflareApiTokenFromKeychain(status.accountId);
    if (token) {
        status.tokenPresent = true;
        status.detail = "keychain token found";
        return status;
    }
    if (token.error().code == ErrorCode::NotSupported) {
        status.keychainSupported = false;
        status.detail = token.error().message;
        return status;
    }
    status.detail = token.error().message;
    return status;
}

void DoctorContext::repairGraph(std::ostream& os, YamsCLI* cli) {
    using namespace yams::cli::ui;

    yams::daemon::ClientConfig cfg;
    if (cli && cli->hasExplicitDataDir())
        cfg.dataDir = cli->getDataPath();
    cfg.requestTimeout = std::chrono::seconds(120);

    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
    if (!leaseRes) {
        os << status_error("Daemon unavailable") << "\n";
        return;
    }
    auto leaseHandle = std::move(leaseRes.value());
    auto& client = **leaseHandle;

    yams::daemon::GraphRepairRequest req;
    req.dryRun = false;
    os << status_pending("Repairing knowledge graph") << "\n";
    auto r = yams::cli::run_result(client.graphRepair(req), std::chrono::seconds(180));
    if (!r) {
        os << status_error("Graph repair failed") << "\n";
        return;
    }
    const auto& resp = r.value();
    os << (resp.errors == 0 ? status_ok("Graph repair completed")
                            : status_warning("Graph repair completed with errors"))
       << "\n\n";
    os << "  " << colorize("Nodes created:", Ansi::CYAN) << " " << format_number(resp.nodesCreated)
       << "\n";
    os << "  " << colorize("Nodes updated:", Ansi::CYAN) << " " << format_number(resp.nodesUpdated)
       << "\n";
    os << "  " << colorize("Edges created:", Ansi::CYAN) << " " << format_number(resp.edgesCreated)
       << "\n";
    if (resp.errors > 0)
        os << "  " << colorize("Errors:", Ansi::YELLOW) << " " << format_number(resp.errors)
           << "\n";
}

void DoctorContext::validateGraph(std::ostream& os, YamsCLI* cli) {
    using namespace yams::cli::ui;

    yams::daemon::ClientConfig cfg;
    if (cli && cli->hasExplicitDataDir())
        cfg.dataDir = cli->getDataPath();
    cfg.requestTimeout = std::chrono::seconds(60);

    auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
    if (!leaseRes) {
        os << status_error("Daemon unavailable") << "\n";
        return;
    }
    auto leaseHandle = std::move(leaseRes.value());
    auto& client = **leaseHandle;

    yams::daemon::GraphValidateRequest req;
    os << status_pending("Validating knowledge graph") << "\n";
    auto r = yams::cli::run_result(client.graphValidate(req), std::chrono::seconds(120));
    if (!r) {
        os << status_error("Graph validation failed") << "\n";
        return;
    }
    const auto& resp = r.value();
    bool hasIssues = !resp.issues.empty() || resp.orphanedNodes > 0 || resp.unreachableNodes > 0;
    os << (hasIssues ? status_warning("Graph validation completed - issues detected")
                     : status_ok("Graph validation completed - no issues found"))
       << "\n\n";
    os << "  " << colorize("Total nodes:", Ansi::CYAN) << " " << format_number(resp.totalNodes)
       << "\n";
    os << "  " << colorize("Total edges:", Ansi::CYAN) << " " << format_number(resp.totalEdges)
       << "\n";
    if (resp.orphanedNodes > 0)
        os << "  " << colorize("Orphaned nodes:", Ansi::YELLOW) << " "
           << format_number(resp.orphanedNodes) << "\n";
    if (resp.unreachableNodes > 0)
        os << "  " << colorize("Unreachable nodes:", Ansi::YELLOW) << " "
           << format_number(resp.unreachableNodes) << "\n";
    if (!resp.issues.empty()) {
        os << "\n" << colorize("Issues:", Ansi::YELLOW) << "\n";
        size_t n = std::min(resp.issues.size(), size_t(10));
        for (size_t i = 0; i < n; ++i)
            os << "  " << bullet(resp.issues[i]) << "\n";
        if (resp.issues.size() > n)
            os << "  "
               << colorize("... and " + std::to_string(resp.issues.size() - n) + " more", Ansi::DIM)
               << "\n";
    }
}

void DoctorContext::applyTuningBaseline(std::ostream& os, bool apply) {
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
    os << "Doctor tuning baseline (proposed):\n";
    for (const auto& [k, v] : suggestions)
        os << "  " << k << " = " << v << "\n";
    if (apply) {
        auto configPath = yams::config::get_config_path();
        for (const auto& [k, v] : suggestions)
            yams::config::write_config_value(configPath, k, v);
        os << yams::cli::ui::status_ok("Applied tuning baseline to [tuning] in config.toml")
           << "\n";
    } else {
        os << "Use 'yams doctor tuning --apply' to write these values.\n";
    }
}

void DoctorContext::renderEmbeddingProgress(
    std::ostream& os, bool tty, std::chrono::steady_clock::time_point& lastPrint,
    const std::chrono::steady_clock::time_point& progressStarted, size_t current, size_t total,
    const std::string& details) {
    using namespace yams::cli::ui;
    auto now = std::chrono::steady_clock::now();
    if (!tty && (current % 200 != 0))
        return;
    if (tty && (now - lastPrint) < std::chrono::milliseconds(200))
        return;
    uint64_t elapsed_s = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(now - progressStarted).count());
    std::string detail = details;
    if (!detail.empty())
        detail = truncate_to_width(detail, 60);
    std::ostringstream oss;
    oss << status_pending("Embeddings") << " ";
    if (total > 0) {
        double frac = (static_cast<double>(current) / static_cast<double>(total));
        oss << progress_with_stats(frac, 18,
                                   std::make_optional(std::make_pair(static_cast<uint64_t>(current),
                                                                     static_cast<uint64_t>(total))),
                                   "docs");
    } else {
        oss << colorize("working", Ansi::DIM);
    }
    oss << " " << colorize("elapsed " + format_duration(elapsed_s), Ansi::DIM);
    if (!detail.empty())
        oss << " " << colorize(detail, Ansi::DIM);
    if (tty)
        os << "\r\033[K" << oss.str() << std::flush;
    else
        os << "  " << oss.str() << "\n" << std::flush;
    lastPrint = now;
}

void DoctorContext::finishEmbeddingProgress(std::ostream& os, bool tty) {
    if (tty)
        os << "\n";
}

} // namespace yams::cli::doctor
