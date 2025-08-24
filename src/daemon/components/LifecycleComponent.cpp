#include <spdlog/spdlog.h>
#include <csignal>
#include <fstream>
#include <unistd.h>
#include <sys/file.h>
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
        return Error{ErrorCode::InvalidState,
                     "Another daemon instance is already running, check PID file: " +
                         pidFile_.string()};
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
                // Here, we call the stop method on the main daemon class.
                daemon_->stop();
            }
            break;
        case SIGHUP:
            spdlog::info("Received SIGHUP, configuration reload requested (not yet implemented).");
            break;
        default:
            break;
    }
}

} // namespace yams::daemon
