#include <spdlog/spdlog.h>
#include <chrono>
#include <csignal>
#include <future>
#include <thread>
#include <unistd.h>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <sys/file.h>
#include <sys/wait.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/global_io_context.h>
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/daemon.h>

namespace {

using yams::Error;
using yams::ErrorCode;
using yams::Result;
using yams::daemon::ClientConfig;
using yams::daemon::DaemonClient;
using yams::daemon::GlobalIOContext;

Result<void> sendShutdownRequest(const ClientConfig& cfg, std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<Result<void>>>();
    auto future = promise->get_future();

    try {
        auto& io = GlobalIOContext::instance().get_io_context();
        boost::asio::co_spawn(
            io,
            [cfg, promise]() -> boost::asio::awaitable<void> {
                try {
                    DaemonClient client(cfg);
                    auto connected = co_await client.connect();
                    if (!connected) {
                        promise->set_value(connected.error());
                        co_return;
                    }
                    auto result = co_await client.shutdown(true);
                    promise->set_value(result);
                } catch (const std::exception& e) {
                    promise->set_value(Error{ErrorCode::InternalError,
                                             std::string("Shutdown RPC exception: ") + e.what()});
                } catch (...) {
                    promise->set_value(
                        Error{ErrorCode::InternalError, "Shutdown RPC threw unknown exception"});
                }
                co_return;
            },
            boost::asio::detached);
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Failed to dispatch shutdown RPC: ") + e.what()};
    }

    if (timeout.count() > 0) {
        if (future.wait_for(timeout) != std::future_status::ready) {
            return Error{ErrorCode::Timeout, "Shutdown request timed out"};
        }
    } else {
        future.wait();
    }
    return future.get();
}

} // namespace

namespace yams {
namespace daemon {

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
        pid_t existingPid = 0;
        bool havePid = readPidFromFile(existingPid);
        bool anotherStopped = false;
        if (havePid && existingPid > 0) {
            anotherStopped = requestExistingDaemonShutdown(existingPid);
        } else {
            spdlog::warn("Could not read PID from file '{}' while checking existing daemon.",
                         pidFile_.string());
        }

        if (!anotherStopped) {
            if (aggressiveModeEnabled()) {
                spdlog::warn(
                    "Another daemon instance detected. Aggressive mode enabled, attempting to "
                    "terminate it.");
                if (!havePid || existingPid <= 0) {
                    havePid = readPidFromFile(existingPid);
                }
                if (havePid && existingPid > 0) {
                    if (auto term = terminateProcess(existingPid); !term) {
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
        } else {
            // Ensure stale PID file is cleared before continuing.
            if (auto rm = removePidFile(); !rm) {
                return rm;
            }
        }
    }

    // Proactively remove a stale socket file left by a previous crash before binding.
    try {
        const auto& sock = daemon_->config_.socketPath;
        if (!sock.empty() && std::filesystem::exists(sock)) {
            std::error_code ec;
            std::filesystem::remove(sock, ec);
            if (ec) {
                spdlog::warn("Failed to remove stale socket '{}': {}", sock.string(), ec.message());
            } else {
                spdlog::debug("Removed stale socket file: {}", sock.string());
            }
        }
    } catch (...) {
        // best effort
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

    if (pid > 0) {
        if (kill(pid, 0) == 0) {
            return true; // Process with that PID exists.
        }

        if (errno == EPERM) {
            // Process exists but we lack permission to signal it; treat as running.
            spdlog::warn("PID {} appears to be running but is owned by another user (EPERM)."
                         " Leaving PID file in place.",
                         pid);
            return true;
        }
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
            spdlog::info("Received SIGHUP, configuration reload requested.");
            if (daemon_) {
                daemon_->requestReload();
            }
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
    if (pid <= 0) {
        return Result<void>();
    }

    if (kill(pid, SIGTERM) == -1) {
        if (errno == ESRCH) {
            return Result<void>();
        }
        spdlog::warn("Failed to send SIGTERM to PID {}: {}", pid, strerror(errno));
    } else {
        if (waitForProcessExit(pid, std::chrono::seconds(3))) {
            return Result<void>();
        }
    }

    spdlog::warn("Process {} did not exit after SIGTERM, sending SIGKILL.", pid);
    if (kill(pid, SIGKILL) == -1) {
        if (errno == ESRCH) {
            return Result<void>();
        }
        return Error{ErrorCode::InvalidState,
                     std::string("Failed to SIGKILL PID: ") + strerror(errno)};
    }

    if (waitForProcessExit(pid, std::chrono::seconds(8))) {
        return Result<void>();
    }

    return Error{ErrorCode::Timeout, std::string("Failed to terminate PID ") + std::to_string(pid) +
                                         ": still running after SIGKILL"};
}

bool LifecycleComponent::requestExistingDaemonShutdown(pid_t pid) const {
    if (pid <= 0) {
        return true;
    }

    ClientConfig cfg;
    cfg.socketPath = daemon_->config_.socketPath;
    cfg.autoStart = false;
    cfg.connectTimeout = std::chrono::milliseconds(750);
    cfg.requestTimeout = std::chrono::milliseconds(5000);
    cfg.maxRetries = 1;

    auto res = sendShutdownRequest(cfg, std::chrono::seconds(5));
    if (!res) {
        spdlog::warn("Existing daemon {} did not respond to shutdown request: {}", pid,
                     res.error().message);
        return false;
    }

    if (!waitForProcessExit(pid, std::chrono::seconds(10))) {
        spdlog::warn("Daemon {} remained alive after shutdown RPC", pid);
        return false;
    }

    spdlog::info("Existing daemon {} stopped gracefully via shutdown request", pid);
    return true;
}

bool LifecycleComponent::waitForProcessExit(pid_t pid, std::chrono::milliseconds timeout) const {
    if (pid <= 0) {
        return true;
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (kill(pid, 0) == -1 && errno == ESRCH) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return (kill(pid, 0) == -1 && errno == ESRCH);
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

} // namespace daemon
} // namespace yams
