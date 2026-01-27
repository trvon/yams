#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include "IComponent.h"
#include <yams/core/types.h>

#ifdef _WIN32
#include <process.h>
using pid_t = int;
#else
#include <sys/types.h>
#endif

namespace yams::daemon {

class YamsDaemon; // Forward declaration

/**
 * @class LifecycleComponent
 * @brief Manages the daemon's OS-level lifecycle, including PID file and signal handling.
 */
class LifecycleComponent : public IComponent {
public:
    LifecycleComponent(YamsDaemon* daemon, std::filesystem::path pidFile);
    ~LifecycleComponent() override;

    // IComponent interface
    const char* getName() const override { return "LifecycleManager"; }
    Result<void> initialize() override;
    void shutdown() override;

private:
    struct PidFileInfo {
        pid_t pid = 0;
        std::uint64_t startTimeNs = 0;
        std::string token;
        std::string exePath;
        bool isJson = false;
    };

    // Data-dir lock info read from the lock file
    struct DataDirLockInfo {
        pid_t pid{0};
        std::string socket;
        std::uint64_t timestamp{0};
    };

    enum class PidIdentityStatus { Verified, Mismatch, Unknown };

    void setupSignalHandlers();
    void cleanupSignalHandlers();
    static void signalHandler(int signal);
    void handleSignal(int signal);

    Result<void> createPidFile();
    Result<void> removePidFile() const;
    bool isAnotherInstanceRunning() const;

    // Aggressive single-instance helpers.
    // NOTE: Aggressive mode (terminate other daemons) is currently the default.
    // TODO: Make this configurable via CLI/config and default to safe mode outside tests.
    bool readPidFromFile(pid_t& outPid) const;
    bool readPidFileInfo(PidFileInfo& info) const;
    bool isPidFileLockedByOther() const;
    bool isProcessRunning(pid_t pid) const;
    PidIdentityStatus verifyPidIdentity(const PidFileInfo& info, std::string& detail) const;
    std::uint64_t getProcessStartTimeNs(pid_t pid) const;
    std::string getProcessExecutablePath(pid_t pid) const;
    static std::string generateInstanceToken();
    Result<void> terminateProcess(pid_t pid) const;
    static bool aggressiveModeEnabled();

    bool requestExistingDaemonShutdown(pid_t pid) const;
    bool waitForProcessExit(pid_t pid, std::chrono::milliseconds timeout) const;

    // Data-dir lock methods to prevent multiple daemons from sharing the same data-dir
    Result<void> acquireDataDirLock();
    DataDirLockInfo readDataDirLockInfo() const;
    void releaseDataDirLock();

    YamsDaemon* daemon_; // Non-owning pointer to the main daemon class to signal shutdown
    std::filesystem::path pidFile_;
    int pidFileFd_ = -1; // File descriptor for the locked PID file
    std::string instanceToken_;
    std::uint64_t startTimeNs_ = 0;

    // Data-dir lock state
    int dataDirLockFd_{-1};
    std::filesystem::path dataDirLockFile_;

    static std::atomic<LifecycleComponent*>
        instance_; // Singleton instance for the static signal handler
};

} // namespace yams::daemon
