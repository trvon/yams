#include <spdlog/spdlog.h>
#include <chrono>
#include <csignal>
#include <future>
#include <thread>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/thread_pool.hpp>
#ifndef _WIN32
#include <sys/file.h>
#include <sys/wait.h>
#else
#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <windows.h>

#define strcasecmp _stricmp
#define getpid _getpid
#define ssize_t int
#endif
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/daemon.h>

namespace {

using yams::Error;
using yams::ErrorCode;
using yams::Result;
using yams::daemon::ClientConfig;
using yams::daemon::DaemonClient;

Result<void> sendShutdownRequest(const ClientConfig& cfg, std::chrono::milliseconds timeout) {
    auto promise = std::make_shared<std::promise<Result<void>>>();
    auto future = promise->get_future();

    try {
        boost::asio::thread_pool pool(1);
        boost::asio::co_spawn(
            pool,
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

        auto wait_ready = [&]() -> bool {
            if (timeout.count() > 0) {
                return future.wait_for(timeout) == std::future_status::ready;
            }
            future.wait();
            return true;
        }();

        if (!wait_ready) {
            pool.stop();
            pool.join();
            return Error{ErrorCode::Timeout, "Shutdown request timed out"};
        }

        pool.join();
        return future.get();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InternalError,
                     std::string("Failed to dispatch shutdown RPC: ") + e.what()};
    }
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
        // If the PID file points to our own process, this indicates another
        // YamsDaemon instance is already running within this process. Do NOT
        // treat it as stale; refuse to start to preserve single-instance
        // semantics expected by DaemonTest.SingleInstance.
        if (havePid && existingPid == getpid()) {
            return Error{ErrorCode::InvalidState,
                         "Another daemon instance is already running in this process"};
        } else {
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
                        // Avoid accidentally terminating our own process
                        if (existingPid == getpid()) {
                            spdlog::warn(
                                "Existing PID matches current process ({}); skipping terminate.",
                                existingPid);
                        } else if (auto term = terminateProcess(existingPid); !term) {
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

#ifdef _WIN32
    int fd = _open(pidFile_.string().c_str(), _O_RDONLY);
    if (fd == -1) {
        return false;
    }

    HANDLE hFile = (HANDLE)_get_osfhandle(fd);
    if (hFile != INVALID_HANDLE_VALUE) {
        OVERLAPPED overlapped = {0};
        // Try to lock. If it fails, another process has it.
        if (!LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                        &overlapped)) {
            _close(fd);
            return true;
        }
        UnlockFileEx(hFile, 0, 1, 0, &overlapped);
    }

    char pid_buf[16] = {0};
    _read(fd, pid_buf, sizeof(pid_buf) - 1);
    pid_t pid = atoi(pid_buf);
    _close(fd);

    if (pid > 0) {
        HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
        if (hProcess) {
            DWORD exitCode;
            if (GetExitCodeProcess(hProcess, &exitCode) && exitCode == STILL_ACTIVE) {
                CloseHandle(hProcess);
                return true;
            }
            CloseHandle(hProcess);
        }
    }
#else
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
#endif

    // If we are here, the PID file is stale.
    spdlog::warn("Found stale PID file for a non-existent process. Removing it.");
    removePidFile();
    return false;
}

Result<void> LifecycleComponent::createPidFile() {
#ifdef _WIN32
    pidFileFd_ =
        _open(pidFile_.string().c_str(), _O_CREAT | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
    if (pidFileFd_ == -1) {
        return Error{ErrorCode::WriteError,
                     "Failed to open PID file: " + std::string(strerror(errno))};
    }

    HANDLE hFile = (HANDLE)_get_osfhandle(pidFileFd_);
    if (hFile != INVALID_HANDLE_VALUE) {
        OVERLAPPED overlapped = {0};
        if (!LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                        &overlapped)) {
            _close(pidFileFd_);
            pidFileFd_ = -1;
            return Error{ErrorCode::InvalidState, "Daemon already running (PID file is locked)."};
        }
    }

    std::string pid_str = std::to_string(_getpid());
    _chsize_s(pidFileFd_, 0);
    if (_write(pidFileFd_, pid_str.c_str(), static_cast<unsigned int>(pid_str.length())) !=
        static_cast<int>(pid_str.length())) {
        _close(pidFileFd_);
        pidFileFd_ = -1;
        return Error{ErrorCode::WriteError,
                     "Failed to write to PID file: " + std::string(strerror(errno))};
    }
    // Flush the write buffer to disk on Windows
    _commit(pidFileFd_);
#else
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
#endif
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
#ifndef _WIN32
    std::signal(SIGHUP, &LifecycleComponent::signalHandler);
#endif
}

void LifecycleComponent::cleanupSignalHandlers() {
    if (instance_ == this) {
        std::signal(SIGTERM, SIG_DFL);
        std::signal(SIGINT, SIG_DFL);
#ifndef _WIN32
        std::signal(SIGHUP, SIG_DFL);
#endif
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
                daemon_->requestStop();
            }
            break;
#ifndef _WIN32
        case SIGHUP:
            spdlog::info("Received SIGHUP, reloading configuration.");
            break;
#endif
    }
}

bool LifecycleComponent::readPidFromFile(pid_t& outPid) const {
    outPid = 0;
    if (!std::filesystem::exists(pidFile_)) {
        return false;
    }
#ifdef _WIN32
    int fd = _open(pidFile_.string().c_str(), _O_RDONLY);
#else
    int fd = open(pidFile_.c_str(), O_RDONLY);
#endif
    if (fd == -1) {
        return false;
    }
    char pid_buf[32] = {0};
#ifdef _WIN32
    int n = _read(fd, pid_buf, sizeof(pid_buf) - 1);
    _close(fd);
#else
    ssize_t n = read(fd, pid_buf, sizeof(pid_buf) - 1);
    close(fd);
#endif
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

#ifdef _WIN32
    HANDLE hProcess = OpenProcess(PROCESS_TERMINATE, FALSE, pid);
    if (hProcess == NULL) {
        // Process might not exist
        return Result<void>();
    }

    if (!TerminateProcess(hProcess, 1)) {
        CloseHandle(hProcess);
        return Error{ErrorCode::InvalidState, "Failed to terminate process"};
    }
    CloseHandle(hProcess);

    if (waitForProcessExit(pid, std::chrono::seconds(3))) {
        return Result<void>();
    }
#else
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
#endif

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
#ifdef _WIN32
        HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION | SYNCHRONIZE, FALSE, pid);
        if (hProcess == NULL) {
            return true; // Process gone or inaccessible
        }
        DWORD exitCode;
        if (GetExitCodeProcess(hProcess, &exitCode) && exitCode != STILL_ACTIVE) {
            CloseHandle(hProcess);
            return true;
        }
        CloseHandle(hProcess);
#else
        if (kill(pid, 0) == -1 && errno == ESRCH) {
            return true;
        }
#endif
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
#ifdef _WIN32
    HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
    if (hProcess == NULL)
        return true;
    DWORD exitCode;
    bool exited = (GetExitCodeProcess(hProcess, &exitCode) && exitCode != STILL_ACTIVE);
    CloseHandle(hProcess);
    return exited;
#else
    return (kill(pid, 0) == -1 && errno == ESRCH);
#endif
}

bool LifecycleComponent::aggressiveModeEnabled() {
    // Test guard: allow unit/integration tests to disable kill-others behavior
    // without changing the production default.
    if (const char* test_guard = std::getenv("YAMS_TEST_SAFE_SINGLE_INSTANCE");
        test_guard && *test_guard) {
        if (strcasecmp(test_guard, "1") == 0 || strcasecmp(test_guard, "true") == 0 ||
            strcasecmp(test_guard, "on") == 0 || strcasecmp(test_guard, "yes") == 0) {
            return false; // SAFE mode during tests
        }
    }

    // Production default: aggressive ON unless explicitly opted out.
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
