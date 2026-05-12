#include <yams/cli/doctor/doctor_context.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/result_helpers.h>
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

} // namespace yams::cli::doctor
