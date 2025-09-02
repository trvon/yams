#include <spdlog/spdlog.h>
#include <csignal>
#include <unistd.h>
#include <sys/file.h>
#include <sys/wait.h>
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/daemon.h>

namespace yams::daemon {

// Initialize static member for signal handler
LifecycleComponent* LifecycleComponent::instance_ = nullptr;

LifecycleComponent::LifecycleComponent(YamsDaemon* daemon, std::filesystem::path pidFile)
    : daemon_(daemon), pidFile_(std::move(pidFile)) {}

LifecycleComponent::~LifecycleComponent() {
    // Ensure shutdown is called, although the owner should call it explicitly.
    shutdown();
}

Result<void> LifecycleComponent::initialize() {
    if (isAnotherInstanceRunning()) {
        // If aggressive mode is enabled, try to terminate the other process and continue.
        if (aggressiveModeEnabled()) {
            spdlog::warn("Another daemon instance detected. Aggressive mode enabled, attempting to "
                         "terminate it.");
            pid_t pid{};
            if (readPidFromFile(pid) && pid > 0) {
                if (auto term = terminateProcess(pid); !term) {
                    return term; // propagate error
                }
            } else {
                spdlog::warn(
                    "Could not read PID from file '{}', will attempt to remove stale file.",
                    pidFile_.string());
            }

            // Remove PID file if present (stale or after termination)
            if (auto rm = removePidFile(); !rm) {
                return rm;
            }
        } else {
            return Error{ErrorCode::InvalidState,
                         "Another daemon instance is already running, check PID file: " +
                             pidFile_.string()};
        }
    }

    if (auto result = createPidFile(); !result) {
        return result;
    }

    setupSignalHandlers();
    return Result<void>();
}

void LifecycleComponent::shutdown() {
    cleanupSignalHandlers();
    removePidFile();
    if (pidFileFd_ != -1) {
        close(pidFileFd_);
        pidFileFd_ = -1;
    }
}

bool LifecycleComponent::isAnotherInstanceRunning() const {
    if (!std::filesystem::exists(pidFile_)) {
        return false;
    }

    int fd = open(pidFile_.c_str(), O_RDONLY);
    if (fd == -1) {
        return false; // Cannot open, assume not running
    }

    // Try to get an advisory lock without blocking.
    // If we can't get it, another process is holding it.
    if (flock(fd, LOCK_SH | LOCK_NB) == -1 && errno == EWOULDBLOCK) {
        close(fd);
        return true; // It's locked by another process.
    }

    // We got the lock, so no other process is holding it.
    // But let's double-check the PID just in case.
    char pid_buf[16] = {0};
    read(fd, pid_buf, sizeof(pid_buf) - 1);
    pid_t pid = atoi(pid_buf);

    flock(fd, LOCK_UN);
    close(fd);

    if (pid > 0 && kill(pid, 0) == 0) {
        return true; // Process with that PID exists.
    }

    // If we are here, the PID file is stale.
    spdlog::warn("Found stale PID file for a non-existent process. Removing it.");
    removePidFile();
    return false;
}

Result<void> LifecycleComponent::createPidFile() {
    pidFileFd_ = open(pidFile_.c_str(), O_CREAT | O_RDWR, 0644);
    if (pidFileFd_ == -1) {
        return Error{ErrorCode::WriteError,
                     "Failed to open PID file: " + std::string(strerror(errno))};
    }

    if (flock(pidFileFd_, LOCK_EX | LOCK_NB) == -1) {
        close(pidFileFd_);
        pidFileFd_ = -1;
        return Error{ErrorCode::InvalidState, "Daemon already running (PID file is locked)."};
    }

    std::string pid_str = std::to_string(getpid());
    if (ftruncate(pidFileFd_, 0) != 0) {
        // handle error
    }
    if (write(pidFileFd_, pid_str.c_str(), pid_str.length()) !=
        static_cast<ssize_t>(pid_str.length())) {
        close(pidFileFd_);
        pidFileFd_ = -1;
        return Error{ErrorCode::WriteError,
                     "Failed to write to PID file: " + std::string(strerror(errno))};
    }

    return Result<void>();
}

Result<void> LifecycleComponent::removePidFile() const {
    if (std::filesystem::exists(pidFile_)) {
        std::error_code ec;
        if (!std::filesystem::remove(pidFile_, ec)) {
            return Error{ErrorCode::WriteError, "Failed to remove PID file: " + ec.message()};
        }
    }
    return Result<void>();
}

void LifecycleComponent::setupSignalHandlers() {
    instance_ = this;
    std::signal(SIGTERM, &LifecycleComponent::signalHandler);
    std::signal(SIGINT, &LifecycleComponent::signalHandler);
    std::signal(SIGHUP, &LifecycleComponent::signalHandler);
}

void LifecycleComponent::cleanupSignalHandlers() {
    if (instance_ == this) {
        std::signal(SIGTERM, SIG_DFL);
        std::signal(SIGINT, SIG_DFL);
        std::signal(SIGHUP, SIG_DFL);
        instance_ = nullptr;
    }
}

void LifecycleComponent::signalHandler(int signal) {
    if (instance_) {
        instance_->handleSignal(signal);
    }
}

void LifecycleComponent::handleSignal(int signal) {
    switch (signal) {
        case SIGTERM:
        case SIGINT:
            spdlog::info("Received signal {}, initiating shutdown.", signal);
            if (daemon_) {
                // This is a crucial part. The signal handler should not do much work.
                // It should signal the main loop to shut down gracefully.
                // Here, we request a graceful stop and notify lifecycle FSM.
                daemon_->requestStop();
                daemon_->getLifecycle(); // ensure object exists; dispatch below
                // Dispatch shutdown request to FSM (non-blocking)
                // Note: signal handler context is limited; we only set flags and rely on main loop
                // to process tick and transitions.
                // Safe because getLifecycle returns a const&; but we only need to set a flag.
                // We avoid calling non-async-signal-safe operations here beyond logging.
                // Therefore, actual dispatch will be in main loop when it notices stopRequested_.
            }
            break;
        case SIGHUP:
            spdlog::info("Received SIGHUP, configuration reload requested (not yet implemented).");
            break;
        default:
            break;
    }
}

bool LifecycleComponent::readPidFromFile(pid_t& outPid) const {
    outPid = 0;
    if (!std::filesystem::exists(pidFile_)) {
        return false;
    }
    int fd = open(pidFile_.c_str(), O_RDONLY);
    if (fd == -1) {
        return false;
    }
    char pid_buf[32] = {0};
    ssize_t n = read(fd, pid_buf, sizeof(pid_buf) - 1);
    close(fd);
    if (n <= 0) {
        return false;
    }
    outPid = static_cast<pid_t>(atoi(pid_buf));
    return outPid > 0;
}

Result<void> LifecycleComponent::terminateProcess(pid_t pid) const {
    // Try graceful shutdown first
    if (kill(pid, SIGTERM) == -1) {
        if (errno == ESRCH) {
            return Result<void>(); // already gone
        }
        spdlog::warn("Failed to send SIGTERM to PID {}: {}", pid, strerror(errno));
    }
    // Wait up to ~2 seconds for process to exit
    constexpr int max_attempts = 20;
    for (int i = 0; i < max_attempts; ++i) {
        if (kill(pid, 0) == -1 && errno == ESRCH) {
            return Result<void>(); // exited
        }
        usleep(100 * 1000); // 100ms
    }
    spdlog::warn("Process {} did not exit after SIGTERM, sending SIGKILL.", pid);
    if (kill(pid, SIGKILL) == -1) {
        if (errno == ESRCH) {
            return Result<void>();
        }
        return Error{ErrorCode::InvalidState,
                     std::string("Failed to SIGKILL PID: ") + strerror(errno)};
    }
    // Wait briefly to ensure it is gone
    for (int i = 0; i < 10; ++i) {
        if (kill(pid, 0) == -1 && errno == ESRCH) {
            return Result<void>();
        }
        usleep(50 * 1000);
    }
    return Result<void>();
}

bool LifecycleComponent::aggressiveModeEnabled() {
    // TODO: Make this configurable via config file/CLI flag and default to "safe" mode.
    // For now, aggressive mode is the default to ensure tests don't pile up daemons.
    const char* env = std::getenv("YAMS_DAEMON_KILL_OTHERS");
    if (!env || env[0] == '\0') {
        return true; // default ON
    }
    // Explicit opt-out when set to "0" or "false" (case-insensitive)
    if ((env[0] == '0' && env[1] == '\0') || strcasecmp(env, "false") == 0 ||
        strcasecmp(env, "off") == 0 || strcasecmp(env, "no") == 0) {
        return false;
    }
    return true;
}

} // namespace yams::daemon
