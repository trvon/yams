#pragma once

#include <atomic>
#include <cstdint>
#include <csignal>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#endif

namespace yams::cli {
class YamsCLI;
} // namespace yams::cli

namespace yams::config {
struct DimensionConfig;
} // namespace yams::config

namespace yams::cli::doctor {

/// R2 keychain credential status for temp-credentials mode.
struct R2ConfigStatus {
    bool enabled{false};
    std::string authMode{"direct"};
    std::string accountId;
    bool tokenPresent{false};
    bool keychainSupported{true};
    std::string detail;
};

/// RAII signal handler that allows Ctrl-C to cancel long-running doctor operations.
/// On first SIGINT, sets a cancel flag. On second SIGINT, exits immediately.
struct DoctorSignalGuard {
    DoctorSignalGuard();
    ~DoctorSignalGuard();
    DoctorSignalGuard(const DoctorSignalGuard&) = delete;
    DoctorSignalGuard& operator=(const DoctorSignalGuard&) = delete;

    static void handler(int /*sig*/);
    static bool cancelled();
    static std::atomic<bool>& cancelFlag();

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

/// Cached daemon state gathered once at the start of a doctor run.
struct CachedDaemonState {
    bool daemonUp{false};
    std::string effectiveSocket;
    std::optional<daemon::StatusResponse> status;
    std::optional<daemon::GetStatsResponse> stats;
};

/// Shared context passed to every doctor check and repair.
/// Holds the YamsCLI handle, config/data paths, cached daemon state,
/// and helpers for config/vector operations.
class DoctorContext {
public:
    explicit DoctorContext(YamsCLI* cli);
    ~DoctorContext();

    DoctorContext(const DoctorContext&) = delete;
    DoctorContext& operator=(const DoctorContext&) = delete;

    // ── Accessors ───────────────────────────────────────────────────────────
    YamsCLI* cli() const { return cli_; }
    const std::filesystem::path& dataDir() const;
    const std::filesystem::path& configPath() const;
    bool jsonOutput() const;

    // ── Daemon interaction ──────────────────────────────────────────────────

    /// Gather daemon status + stats once. Subsequent calls return the cached copy.
    const CachedDaemonState& cachedState() const;

    /// Best-effort: stop the daemon if it is running.
    void ensureDaemonStopped();

    // ── Config helpers ──────────────────────────────────────────────────────

    yams::config::DimensionConfig readConfigDims() const;
    bool writeConfigDims(size_t dim) const;
    std::map<std::string, std::string> parseToml() const;

    // ── Storage/config diagnostics ──────────────────────────────────────────

    static R2ConfigStatus evaluateR2Config();

    // ── Vector helpers ──────────────────────────────────────────────────────

    std::pair<size_t, std::string> resolveEmbeddingDim();

    // ── WAL helpers ─────────────────────────────────────────────────────────

    static uint64_t walFileSize(const std::filesystem::path& dataDir, const std::string& dbName);
    static void forceWalCheckpointLocal(const std::filesystem::path& dbPath);

    // ── Repair helpers ──────────────────────────────────────────────────────

    static void repairGraph(std::ostream& os, YamsCLI* cli);
    static void validateGraph(std::ostream& os, YamsCLI* cli);
    static void applyTuningBaseline(std::ostream& os, bool apply);

    // ── Progress rendering (shared by runRepair) ────────────────────────────

    static void
    renderEmbeddingProgress(std::ostream& os, bool tty,
                            std::chrono::steady_clock::time_point& lastPrint,
                            const std::chrono::steady_clock::time_point& progressStarted,
                            size_t current, size_t total, const std::string& details);
    static void finishEmbeddingProgress(std::ostream& os, bool tty);

private:
    YamsCLI* cli_{nullptr};
    mutable std::optional<std::filesystem::path> cachedDataDir_;
    mutable std::optional<std::filesystem::path> cachedConfigPath_;
    mutable std::optional<CachedDaemonState> cachedState_;
    mutable std::optional<bool> cachedJsonOutput_;
};

} // namespace yams::cli::doctor
