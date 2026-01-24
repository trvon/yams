#include <spdlog/spdlog.h>
#include <array>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <future>
#include <iomanip>
#include <random>
#include <sstream>
#include <thread>

#include <nlohmann/json.hpp>
using nlohmann::json;

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/thread_pool.hpp>

#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/components/LifecycleComponent.h>
#include <yams/daemon/daemon.h>

#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#include <process.h>
#include <string.h>
#include <windows.h>

#define ssize_t int
#else
#include <unistd.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/wait.h>
#ifdef __APPLE__
#include <libproc.h>
#include <sys/sysctl.h>
#endif
#endif

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

bool equalsIgnoreCase(const char* a, const char* b) {
    if (a == nullptr || b == nullptr) {
        return a == b;
    }
    while (*a && *b) {
        if (std::tolower(static_cast<unsigned char>(*a)) !=
            std::tolower(static_cast<unsigned char>(*b))) {
            return false;
        }
        ++a;
        ++b;
    }
    return *a == '\0' && *b == '\0';
}

} // namespace

namespace yams {
namespace daemon {

// Initialize static member for signal handler
std::atomic<LifecycleComponent*> LifecycleComponent::instance_{nullptr};

LifecycleComponent::LifecycleComponent(YamsDaemon* daemon, std::filesystem::path pidFile)
    : daemon_(daemon), pidFile_(std::move(pidFile)) {}

LifecycleComponent::~LifecycleComponent() {
    // Ensure shutdown is called, although the owner should call it explicitly.
    shutdown();
}

Result<void> LifecycleComponent::initialize() {
    if (isAnotherInstanceRunning()) {
        PidFileInfo info;
        bool haveInfo = readPidFileInfo(info);
        pid_t existingPid = haveInfo ? info.pid : 0;
        std::string identityDetail;
        auto identity =
            haveInfo ? verifyPidIdentity(info, identityDetail) : PidIdentityStatus::Unknown;

        // If the PID file points to our own process, this indicates another
        // YamsDaemon instance is already running within this process. Do NOT
        // treat it as stale; refuse to start to preserve single-instance
        // semantics expected by DaemonTest.SingleInstance.
        if (haveInfo && existingPid == getpid()) {
            return Error{ErrorCode::InvalidState,
                         "Another daemon instance is already running in this process"};
        }

        if (!haveInfo) {
            spdlog::warn("Could not read PID from file '{}' while checking existing daemon.",
                         pidFile_.string());
        } else if (identity == PidIdentityStatus::Unknown) {
            spdlog::warn("Unable to verify daemon identity for PID {} ({}).", existingPid,
                         identityDetail);
        } else if (identity == PidIdentityStatus::Mismatch) {
            spdlog::warn("PID {} does not match expected daemon identity ({}).", existingPid,
                         identityDetail);
        }

        if (!haveInfo || identity != PidIdentityStatus::Verified) {
            if (isProcessRunning(existingPid)) {
                return Error{ErrorCode::InvalidState,
                             "Another process is running with PID from PID file; refusing to "
                             "shutdown without identity verification"};
            }
        }

        bool anotherStopped = false;
        if (haveInfo && existingPid > 0 && identity == PidIdentityStatus::Verified) {
            anotherStopped = requestExistingDaemonShutdown(existingPid);
        }

        if (!anotherStopped) {
            if (aggressiveModeEnabled()) {
                spdlog::warn(
                    "Another daemon instance detected. Aggressive mode enabled, attempting to "
                    "terminate it.");
                if (haveInfo && existingPid > 0 && identity == PidIdentityStatus::Verified) {
                    // Avoid accidentally terminating our own process
                    if (existingPid == getpid()) {
                        spdlog::warn(
                            "Existing PID matches current process ({}); skipping terminate.",
                            existingPid);
                    } else if (auto term = terminateProcess(existingPid); !term) {
                        return term; // propagate error
                    }
                } else {
                    spdlog::warn("Cannot safely terminate daemon; identity not verified for '{}'.",
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

    if (auto result = createPidFile(); !result) {
        return result;
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

    setupSignalHandlers();
    return Result<void>();
}

void LifecycleComponent::shutdown() {
    cleanupSignalHandlers();

    // Close and unlock file BEFORE removal (Windows holds lock while fd is open)
    if (pidFileFd_ != -1) {
#ifdef _WIN32
        // Explicitly unlock on Windows before closing
        HANDLE hFile = (HANDLE)_get_osfhandle(pidFileFd_);
        if (hFile != INVALID_HANDLE_VALUE) {
            OVERLAPPED overlapped = {0};
            UnlockFileEx(hFile, 0, 1, 0, &overlapped);
        }
        _close(pidFileFd_);
#else
        close(pidFileFd_);
#endif
        pidFileFd_ = -1;
    }

    // Now safe to remove the unlocked file
    removePidFile();
}

bool LifecycleComponent::isAnotherInstanceRunning() const {
    if (!std::filesystem::exists(pidFile_)) {
        return false;
    }

    if (isPidFileLockedByOther()) {
        return true;
    }

    PidFileInfo info;
    if (!readPidFileInfo(info) || info.pid <= 0) {
        spdlog::warn("Found unreadable PID file '{}'; leaving it in place.", pidFile_.string());
        return true;
    }

    if (!isProcessRunning(info.pid)) {
        spdlog::warn("Found stale PID file for a non-existent process. Removing it.");
        removePidFile();
        return false;
    }

    std::string detail;
    auto identity = verifyPidIdentity(info, detail);
    if (identity == PidIdentityStatus::Verified) {
        return true;
    }
    if (identity == PidIdentityStatus::Mismatch) {
        spdlog::warn("PID file identity mismatch for PID {} ({}).", info.pid, detail);
        return true;
    }
    spdlog::warn("PID file identity unknown for PID {} ({}).", info.pid, detail);
    return true;
}

Result<void> LifecycleComponent::createPidFile() {
    if (instanceToken_.empty()) {
        instanceToken_ = generateInstanceToken();
    }
    if (startTimeNs_ == 0) {
        startTimeNs_ = getProcessStartTimeNs(getpid());
    }
    if (startTimeNs_ == 0) {
        auto now = std::chrono::system_clock::now().time_since_epoch();
        startTimeNs_ = static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
    }
    auto exePath = getProcessExecutablePath(getpid());
    if (!exePath.empty()) {
        std::error_code ec;
        exePath = std::filesystem::weakly_canonical(exePath, ec).string();
        if (ec) {
            exePath.clear();
        }
    }
    json payload = json::object();
    payload["v"] = 1;
    payload["pid"] = static_cast<std::int64_t>(getpid());
    payload["start_ns"] = startTimeNs_;
    payload["token"] = instanceToken_;
    if (!exePath.empty()) {
        payload["exe"] = exePath;
    }
    const std::string pid_str = payload.dump();
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
    instance_.store(this, std::memory_order_release);
}

void LifecycleComponent::cleanupSignalHandlers() {
    LifecycleComponent* expected = this;
    instance_.compare_exchange_strong(expected, nullptr, std::memory_order_acq_rel);
}

void LifecycleComponent::signalHandler(int signal) {
    auto* inst = instance_.load(std::memory_order_acquire);
    if (inst) {
        inst->handleSignal(signal);
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
    PidFileInfo info;
    if (!readPidFileInfo(info)) {
        outPid = 0;
        return false;
    }
    outPid = info.pid;
    return outPid > 0;
}

bool LifecycleComponent::readPidFileInfo(PidFileInfo& info) const {
    info = PidFileInfo{};
    if (!std::filesystem::exists(pidFile_)) {
        return false;
    }
    std::ifstream pidFile(pidFile_);
    if (!pidFile.is_open()) {
        return false;
    }
    std::string content;
    std::getline(pidFile, content, '\0');
    if (content.empty()) {
        return false;
    }
    // Trim whitespace
    while (!content.empty() && std::isspace(static_cast<unsigned char>(content.back()))) {
        content.pop_back();
    }
    while (!content.empty() && std::isspace(static_cast<unsigned char>(content.front()))) {
        content.erase(content.begin());
    }
    if (content.empty()) {
        return false;
    }
    if (content.front() == '{') {
        auto parsed = json::parse(content, nullptr, false);
        if (!parsed.is_discarded() && parsed.is_object()) {
            info.isJson = true;
            info.pid = static_cast<pid_t>(parsed.value("pid", 0));
            info.startTimeNs = parsed.value("start_ns", 0ull);
            info.token = parsed.value("token", std::string{});
            info.exePath = parsed.value("exe", std::string{});
            return info.pid > 0;
        }
    }
    info.pid = static_cast<pid_t>(std::atoi(content.c_str()));
    return info.pid > 0;
}

bool LifecycleComponent::isPidFileLockedByOther() const {
#ifdef _WIN32
    int fd = _open(pidFile_.string().c_str(), _O_RDONLY);
    if (fd == -1) {
        return false;
    }
    HANDLE hFile = (HANDLE)_get_osfhandle(fd);
    if (hFile != INVALID_HANDLE_VALUE) {
        OVERLAPPED overlapped = {0};
        if (!LockFileEx(hFile, LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0,
                        &overlapped)) {
            _close(fd);
            return true;
        }
        UnlockFileEx(hFile, 0, 1, 0, &overlapped);
    }
    _close(fd);
    return false;
#else
    int fd = open(pidFile_.c_str(), O_RDONLY);
    if (fd == -1) {
        return false;
    }
    if (flock(fd, LOCK_SH | LOCK_NB) == -1 && errno == EWOULDBLOCK) {
        close(fd);
        return true;
    }
    flock(fd, LOCK_UN);
    close(fd);
    return false;
#endif
}

bool LifecycleComponent::isProcessRunning(pid_t pid) const {
    if (pid <= 0) {
        return false;
    }
#ifdef _WIN32
    HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
    if (!hProcess) {
        return false;
    }
    DWORD exitCode = 0;
    bool alive = (GetExitCodeProcess(hProcess, &exitCode) && exitCode == STILL_ACTIVE);
    CloseHandle(hProcess);
    return alive;
#else
    if (kill(pid, 0) == 0) {
        return true;
    }
    return errno == EPERM;
#endif
}

LifecycleComponent::PidIdentityStatus
LifecycleComponent::verifyPidIdentity(const PidFileInfo& info, std::string& detail) const {
    detail.clear();
    if (info.pid <= 0) {
        detail = "missing pid";
        return PidIdentityStatus::Unknown;
    }
    if (!isProcessRunning(info.pid)) {
        detail = "process not running";
        return PidIdentityStatus::Mismatch;
    }
    std::uint64_t liveStart = getProcessStartTimeNs(info.pid);
    if (info.startTimeNs && liveStart) {
        if (info.startTimeNs != liveStart) {
            detail = "start time mismatch";
            return PidIdentityStatus::Mismatch;
        }
    } else if (info.startTimeNs && !liveStart) {
        detail = "start time unavailable";
        return PidIdentityStatus::Unknown;
    }

    auto liveExe = getProcessExecutablePath(info.pid);
    if (!info.exePath.empty()) {
        if (!liveExe.empty()) {
            std::error_code ec;
            auto canonicalInfo = std::filesystem::weakly_canonical(info.exePath, ec).string();
            if (ec) {
                canonicalInfo.clear();
            }
            ec.clear();
            auto canonicalExe = std::filesystem::weakly_canonical(liveExe, ec).string();
            if (ec) {
                canonicalExe.clear();
            }
            if (!canonicalInfo.empty() && !canonicalExe.empty() && canonicalInfo != canonicalExe) {
                detail = "exe mismatch";
                return PidIdentityStatus::Mismatch;
            }
        } else {
            detail = "exe unavailable";
            return PidIdentityStatus::Unknown;
        }
    } else if (!liveExe.empty()) {
        auto currentExe = getProcessExecutablePath(getpid());
        if (!currentExe.empty()) {
            std::error_code ec;
            auto canonicalLive = std::filesystem::weakly_canonical(liveExe, ec).string();
            if (ec) {
                canonicalLive.clear();
            }
            ec.clear();
            auto canonicalCurrent = std::filesystem::weakly_canonical(currentExe, ec).string();
            if (ec) {
                canonicalCurrent.clear();
            }
            if (!canonicalLive.empty() && !canonicalCurrent.empty()) {
                if (canonicalLive != canonicalCurrent) {
                    detail = "exe mismatch";
                    return PidIdentityStatus::Mismatch;
                }
            } else {
                detail = "exe unavailable";
                return PidIdentityStatus::Unknown;
            }
        }
    }

    detail = "verified";
    return PidIdentityStatus::Verified;
}

std::uint64_t LifecycleComponent::getProcessStartTimeNs(pid_t pid) const {
#ifdef _WIN32
    HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
    if (!hProcess) {
        return 0;
    }
    FILETIME createTime{}, exitTime{}, kernelTime{}, userTime{};
    if (!GetProcessTimes(hProcess, &createTime, &exitTime, &kernelTime, &userTime)) {
        CloseHandle(hProcess);
        return 0;
    }
    CloseHandle(hProcess);
    ULARGE_INTEGER uli;
    uli.LowPart = createTime.dwLowDateTime;
    uli.HighPart = createTime.dwHighDateTime;
    // FILETIME is 100ns intervals since 1601
    return static_cast<std::uint64_t>(uli.QuadPart) * 100ull;
#elif __APPLE__
    struct kinfo_proc kp;
    std::memset(&kp, 0, sizeof(kp));
    int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, pid};
    size_t len = sizeof(kp);
    if (sysctl(mib, 4, &kp, &len, nullptr, 0) != 0 || len == 0) {
        return 0;
    }
    auto tv = kp.kp_proc.p_starttime;
    return static_cast<std::uint64_t>(tv.tv_sec) * 1000000000ull +
           static_cast<std::uint64_t>(tv.tv_usec) * 1000ull;
#else
    std::ifstream statFile("/proc/" + std::to_string(pid) + "/stat");
    if (!statFile.is_open()) {
        return 0;
    }
    std::string statLine;
    std::getline(statFile, statLine);
    if (statLine.empty()) {
        return 0;
    }
    auto rparen = statLine.rfind(')');
    if (rparen == std::string::npos) {
        return 0;
    }
    std::istringstream iss(statLine.substr(rparen + 1));
    std::string token;
    for (int i = 0; i < 19; ++i) {
        if (!(iss >> token)) {
            return 0;
        }
    }
    unsigned long long startTicks = 0;
    if (!(iss >> startTicks)) {
        return 0;
    }
    long ticks = sysconf(_SC_CLK_TCK);
    if (ticks <= 0) {
        return 0;
    }
    double seconds = static_cast<double>(startTicks) / static_cast<double>(ticks);
    return static_cast<std::uint64_t>(seconds * 1000000000.0);
#endif
}

std::string LifecycleComponent::getProcessExecutablePath(pid_t pid) const {
#ifdef _WIN32
    HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, pid);
    if (!hProcess) {
        return {};
    }
    char buffer[MAX_PATH];
    DWORD size = static_cast<DWORD>(sizeof(buffer));
    if (!QueryFullProcessImageNameA(hProcess, 0, buffer, &size)) {
        CloseHandle(hProcess);
        return {};
    }
    CloseHandle(hProcess);
    return std::string(buffer, size);
#elif __APPLE__
    char pathbuf[PROC_PIDPATHINFO_MAXSIZE] = {0};
    int ret = proc_pidpath(pid, pathbuf, sizeof(pathbuf));
    if (ret <= 0) {
        return {};
    }
    return std::string(pathbuf);
#else
    std::error_code ec;
    auto exePath = std::filesystem::read_symlink(
        std::filesystem::path("/proc") / std::to_string(pid) / "exe", ec);
    if (ec) {
        return {};
    }
    return exePath.string();
#endif
}

std::string LifecycleComponent::generateInstanceToken() {
    std::array<unsigned char, 16> bytes{};
    std::random_device rd;
    for (auto& b : bytes) {
        b = static_cast<unsigned char>(rd());
    }
    std::ostringstream oss;
    oss << std::hex;
    for (auto b : bytes) {
        oss << std::setw(2) << std::setfill('0') << static_cast<int>(b);
    }
    return oss.str();
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
        if (equalsIgnoreCase(test_guard, "1") || equalsIgnoreCase(test_guard, "true") ||
            equalsIgnoreCase(test_guard, "on") || equalsIgnoreCase(test_guard, "yes")) {
            return false; // SAFE mode during tests
        }
    }

    // Production default: aggressive ON unless explicitly opted out.
    const char* env = std::getenv("YAMS_DAEMON_KILL_OTHERS");
    if (!env || env[0] == '\0') {
        return true; // default ON
    }
    // Explicit opt-out when set to "0" or "false" (case-insensitive)
    if ((env[0] == '0' && env[1] == '\0') || equalsIgnoreCase(env, "false") ||
        equalsIgnoreCase(env, "off") || equalsIgnoreCase(env, "no")) {
        return false;
    }
    return true;
}

} // namespace daemon
} // namespace yams
