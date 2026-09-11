// pi-lens-ignore: fatal error
#include <nlohmann/json.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/daemon_status_render.h>
#include <yams/cli/error_hints.h>
#include <yams/cli/pipeline_stage_render.h>
#include <yams/cli/result_helpers.h>
#include <yams/cli/status_metrics.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/common/fs_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/client/process_discovery.h>
#include <yams/daemon/ipc/socket_utils.h>
#include <yams/daemon/metric_keys.h>
#include <yams/daemon/shutdown_budget.h>
#include <yams/version.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <regex>
#include <set>
#include <signal.h>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <sys/types.h>

#ifdef _WIN32
#include <process.h>
#include <windows.h>

#define pid_t int
#define SIGKILL 9
#define SIGTERM 15
#define execvp _execvp
#define getpid _getpid

inline int kill(pid_t pid, int sig) {
    if (sig == 0) {
        HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, pid);
        if (hProcess) {
            CloseHandle(hProcess);
            return 0;
        }
        return -1;
    }
    HANDLE hProcess = OpenProcess(PROCESS_TERMINATE | SYNCHRONIZE, FALSE, pid);
    if (hProcess) {
        BOOL result = TerminateProcess(hProcess, 1);
        if (result) {
            (void)WaitForSingleObject(hProcess, 5000);
            CloseHandle(hProcess);
            return 0;
        }
        CloseHandle(hProcess);
        return -1;
    }
    return -1;
}

using uid_t = int;
inline uid_t getuid() {
    return 0;
}

static std::string describeProcess(pid_t pid) {
    if (pid <= 0) {
        return "";
    }
    HANDLE hProcess =
        OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, static_cast<DWORD>(pid));
    if (!hProcess) {
        return "";
    }
    char pathBuf[MAX_PATH];
    DWORD size = static_cast<DWORD>(sizeof(pathBuf));
    std::string out;
    if (QueryFullProcessImageNameA(hProcess, 0, pathBuf, &size) != 0) {
        out = std::to_string(pid);
        out.push_back(' ');
        out.append(pathBuf, pathBuf + size);
    }
    CloseHandle(hProcess);
    return out;
}

#endif

using nlohmann::json;

namespace {
// Helper to safely check if a file/socket exists on Windows
// std::filesystem::exists() can throw on Windows for Unix domain sockets
inline bool safe_exists(const std::filesystem::path& p) {
    std::error_code ec;
    return std::filesystem::exists(p, ec);
}

} // namespace

namespace yams::cli {

class DaemonCommand : public ICommand {
public:
    DaemonCommand() = default;

    std::string getName() const override { return "daemon"; }

    std::string getDescription() const override { return "Manage YAMS daemon process"; }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* daemon = app.add_subcommand(getName(), getDescription());
        daemon->require_subcommand();

        // Start command
        auto* start = daemon->add_subcommand("start", "Start the YAMS daemon");
        start->add_option("--socket", socketPath_, "Socket path for daemon communication");
        start->add_option("--data-dir,--storage", dataDir_, "Data directory for daemon storage");
        start->add_option("--pid-file", pidFile_, "PID file path");
        start
            ->add_option("--log-level", startLogLevel_,
                         "Daemon log level (trace, debug, info, warn, error)")
            ->expected(1);
        start->add_option("--config", startConfigPath_, "Path to daemon config file");
        start->add_option("--daemon-binary", startDaemonBinary_,
                          "Path to yams-daemon executable (override)");
        start->add_flag("--foreground", startForeground_, "Run daemon in foreground (don't fork)");
        start->add_flag("-r,--restart", startRestart_, "If already running, restart the daemon");

        start->callback([this]() { startDaemon(); });

        // Stop command
        auto* stop = daemon->add_subcommand("stop", "Stop the YAMS daemon");
        stop->add_option("--socket", socketPath_, "Socket path for daemon communication");
        stop->add_flag("--force", force_, "Force stop (kill -9)");

        stop->callback([this]() {
            if (!stopDaemon()) {
                std::exit(1);
            }
        });

        // Status command
        auto* status = daemon->add_subcommand("status", "Check daemon status");
        status->add_option("--socket", socketPath_, "Socket path for daemon communication");
        status->add_flag("-d,--detailed", detailed_, "Show detailed status");

        status->callback([this]() { showStatus(); });

        // Restart command
        auto* restart = daemon->add_subcommand("restart", "Restart the YAMS daemon");
        restart->add_option("--socket", socketPath_, "Socket path for daemon communication");
        restart->add_flag("--force", force_, "Force stop (kill -9)");

        restart->callback([this]() { restartDaemon(); });

        // Doctor subcommand
        auto* doctor =
            daemon->add_subcommand("doctor", "Diagnose daemon IPC and environment issues");
        doctor->callback([this]() { doctorDaemon(); });

        // Log subcommand
        auto* log = daemon->add_subcommand("log", "View daemon logs");
        log->add_option("-n,--lines", logLines_, "Number of lines to show (default: 50)")
            ->default_val(50);
        log->add_flag("-f,--follow", logFollow_, "Follow log output (like tail -f)");
        log->add_option("--level", logFilterLevel_,
                        "Filter by log level (trace, debug, info, warn, error)");
        log->callback([this]() { showLog(); });

        // Install/uninstall systemd service (user scope by default when not root)
        auto* install =
            daemon->add_subcommand("install", "Install the YAMS daemon as a systemd service");
        install->add_option("--socket", socketPath_, "Socket path for daemon communication");
        install->add_option("--data-dir,--storage", dataDir_, "Data directory for daemon storage");
        install->add_option("--config", startConfigPath_, "Path to daemon config file");
        install->add_option("--daemon-binary", startDaemonBinary_,
                            "Path to yams-daemon executable (override)");
        install->add_flag("--user", installUserScope_,
                          "Install as a user service (default when running as non-root)");
        install->callback([this]() { installDaemonService(); });

        auto* uninstall =
            daemon->add_subcommand("uninstall", "Remove the systemd service and stop it");
        uninstall->add_flag("--user", installUserScope_,
                            "Target the user-scope service (default when non-root)");
        uninstall->callback([this]() { uninstallDaemonService(); });
    }

    Result<void> execute() override {
        // This is called by the base command framework
        // The actual work is done in the callbacks above
        return Result<void>();
    }

private:
    std::filesystem::path expectedDataDir() const {
        if (!dataDir_.empty())
            return std::filesystem::path(dataDir_);
        if (cli_) {
            try {
                auto cliDataDir = cli_->getDataPath();
                if (!cliDataDir.empty())
                    return cliDataDir;
            } catch (...) {
            }
        }
        return yams::config::resolve_data_dir_from_config();
    }

    bool isVersionCompatible(const std::string& runningVersion) {
        std::string currentVersion = YAMS_VERSION_STRING;

        // For now, require exact version match for compatibility
        // TODO: Implement more sophisticated version compatibility checking
        bool compatible = (runningVersion == currentVersion);

        if (!compatible) {
            spdlog::info("Version mismatch detected: running daemon v{}, current binary v{}",
                         runningVersion, currentVersion);
        }

        return compatible;
    }

    pid_t readPidFromFile(const std::string& pidFilePath) {
        std::ifstream pidFile(pidFilePath);
        if (!pidFile.is_open()) {
            return -1;
        }

        std::string content;
        std::getline(pidFile, content, '\0');
        if (content.empty()) {
            return -1;
        }
        while (!content.empty() && std::isspace(static_cast<unsigned char>(content.back()))) {
            content.pop_back();
        }
        while (!content.empty() && std::isspace(static_cast<unsigned char>(content.front()))) {
            content.erase(content.begin());
        }
        if (content.empty()) {
            return -1;
        }
        if (content.front() == '{') {
            auto parsed = json::parse(content, nullptr, false);
            if (parsed.is_discarded() || !parsed.is_object() || !parsed.contains("pid") ||
                !parsed["pid"].is_number_integer()) {
                return -1;
            }
            try {
                const auto pid = parsed["pid"].get<std::int64_t>();
                if (pid > 0 && pid <= std::numeric_limits<pid_t>::max()) {
                    return static_cast<pid_t>(pid);
                }
            } catch (const json::exception&) {
                return -1;
            }
            return -1;
        }

        std::int64_t pid = -1;
        const auto [end, error] =
            std::from_chars(content.data(), content.data() + content.size(), pid);
        if (error != std::errc{} || end != content.data() + content.size() || pid <= 0 ||
            pid > std::numeric_limits<pid_t>::max()) {
            return -1;
        }
        return static_cast<pid_t>(pid);
    }

    bool killDaemonByPid(pid_t pid, bool force = false) {
        if (pid <= 0) {
            return false;
        }

        // Check if process exists
        if (kill(pid, 0) != 0) {
            if (errno == ESRCH) {
                return true; // Already gone
            }
            spdlog::error("Failed to query daemon PID {}: {}", pid, strerror(errno));
            return false; // Process doesn't exist or not permitted
        }

        if (!yams::daemon::client::isLiveDaemonProcess(pid)) {
            spdlog::error("Refusing to signal PID {} because it is not identifiable as yams-daemon",
                          pid);
            return false;
        }

        // Send termination signal
        int sig = force ? SIGKILL : SIGTERM;
        if (kill(pid, sig) != 0) {
            if (errno == ESRCH) {
                return true; // Process exited before signal
            }
            spdlog::error("Failed to send signal to daemon (PID {}): {}", pid, strerror(errno));
            auto desc = describeProcess(pid);
            if (!desc.empty()) {
                spdlog::error("Daemon PID {} state: {}", pid, desc);
            }
            return false;
        }

        spdlog::info("Sent {} to daemon (PID {})", force ? "SIGKILL" : "SIGTERM", pid);

        // Wait for process to terminate (max 5 seconds for SIGTERM)
        const int loops = force ? 20 : 50;
        const auto delay = force ? std::chrono::milliseconds(50) : std::chrono::milliseconds(100);
        for (int i = 0; i < loops; i++) {
            if (kill(pid, 0) != 0) {
                return true; // Process terminated
            }
            std::this_thread::sleep_for(delay);
        }

        if (kill(pid, 0) != 0) {
            return true;
        }
        auto desc = describeProcess(pid);
        if (!desc.empty()) {
            spdlog::debug("Daemon PID {} still alive after signal: {}", pid, desc);
        }
        return false;
    }

    bool waitForDaemonStop(const std::string& socketPath, const std::string& pidFilePath,
                           std::chrono::milliseconds timeout = std::chrono::seconds(10),
                           pid_t fallbackPid = -1) {
        const auto start = std::chrono::steady_clock::now();
        const auto interval = std::chrono::milliseconds(100);
        const std::filesystem::path socketFsPath(socketPath);

        while (std::chrono::steady_clock::now() - start < timeout) {
            pid_t pid = readPidFromFile(pidFilePath);
            if (pid > 0 && !yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFilePath, pid)) {
                pid = -1;
            }
            if (pid <= 0 && fallbackPid > 0) {
                pid = fallbackPid;
            }

            bool processGone = (pid <= 0) || (kill(pid, 0) != 0);
            bool socketFilePresent = !socketPath.empty() && safe_exists(socketFsPath);
            bool proxySocketPresent =
                !socketPath.empty() &&
                safe_exists(yams::daemon::socket_utils::derive_proxy_socket_path(socketFsPath));
            bool socketBoundProcessAlive = false;
#ifndef _WIN32
            socketBoundProcessAlive = isDaemonProcessRunningForSocket(socketPath);
#else
            socketBoundProcessAlive = socketFilePresent || proxySocketPresent;
#endif
            bool socketGone =
                (!socketFilePresent && !proxySocketPresent) || !socketBoundProcessAlive;

            if (processGone && socketGone) {
                spdlog::debug("Daemon fully stopped (PID {}), socket cleared", pid);
                return true;
            }

            spdlog::debug(
                "Waiting for daemon to stop... PID={}, process_gone={}, socket_gone={}, "
                "socket_file_present={}, proxy_socket_present={}, socket_process_alive={}",
                pid, processGone, socketGone, socketFilePresent, proxySocketPresent,
                socketBoundProcessAlive);
            std::this_thread::sleep_for(interval);
        }

        spdlog::debug("Timeout waiting for daemon to stop");
        return false;
    }

    void cleanupDaemonFiles(const std::string& socketPath, const std::string& pidFilePath) {
        auto removeWithRetry = [](const std::filesystem::path& path, const std::string& label) {
            if (path.empty()) {
                return;
            }
            std::error_code ec;
            for (int attempt = 0; attempt < 5; ++attempt) {
                if (!safe_exists(path)) {
                    return;
                }
                ec.clear();
                if (std::filesystem::remove(path, ec)) {
                    spdlog::debug("Removed stale {} file: {}", label, path.string());
                    return;
                }
#ifdef _WIN32
                if (ec.value() == ERROR_SHARING_VIOLATION || ec.value() == ERROR_ACCESS_DENIED) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
#endif
                break;
            }
            if (ec) {
                spdlog::debug("Failed to remove {} file {}: {}", label, path.string(),
                              ec.message());
            }
        };

        // Remove socket file if it exists
        if (!socketPath.empty()) {
            removeWithRetry(std::filesystem::path{socketPath}, "socket");
            removeWithRetry(yams::daemon::socket_utils::derive_proxy_socket_path(
                                std::filesystem::path{socketPath}),
                            "proxy socket");
        }

        // Remove PID file if it exists
        if (!pidFilePath.empty()) {
            removeWithRetry(std::filesystem::path{pidFilePath}, "PID");
        }
    }

#ifndef _WIN32
    static std::string escapeRegexLiteral(const std::string& value) {
        std::string out;
        out.reserve(value.size() * 2);
        for (char ch : value) {
            switch (ch) {
                case '.':
                case '^':
                case '$':
                case '*':
                case '+':
                case '?':
                case '(':
                case ')':
                case '[':
                case ']':
                case '{':
                case '}':
                case '|':
                case '\\':
                    out.push_back('\\');
                    break;
                default:
                    break;
            }
            out.push_back(ch);
        }
        return out;
    }

    static std::string runCommandCapture(const std::string& cmd) {
        std::string output;
        FILE* pipe = popen(cmd.c_str(), "r");
        if (!pipe) {
            return output;
        }
        char buffer[256]{};
        while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
            output.append(buffer);
        }
        pclose(pipe);
        return output;
    }

    static std::optional<std::string> readProcCommandLine(pid_t pid) {
#if defined(__linux__)
        std::ifstream input("/proc/" + std::to_string(pid) + "/cmdline", std::ios::binary);
        if (!input.is_open()) {
            return std::nullopt;
        }

        std::ostringstream buffer;
        buffer << input.rdbuf();
        std::string commandLine = buffer.str();
        if (commandLine.empty()) {
            return std::nullopt;
        }

        for (char& ch : commandLine) {
            if (ch == '\0') {
                ch = ' ';
            }
        }
        while (!commandLine.empty() && commandLine.back() == ' ') {
            commandLine.pop_back();
        }
        if (commandLine.empty()) {
            return std::nullopt;
        }
        return commandLine;
#else
        (void)pid;
        return std::nullopt;
#endif
    }

    static std::string describeProcess(pid_t pid) {
        if (pid <= 0) {
            return "";
        }

        if (auto commandLine = readProcCommandLine(pid); commandLine && !commandLine->empty()) {
            return std::to_string(pid) + " " + *commandLine;
        }

        std::string cmd = "ps -o pid=,ppid=,stat=,command= -p " + std::to_string(pid);
        auto output = runCommandCapture(cmd);
        while (!output.empty() &&
               (output.back() == '\n' || output.back() == '\r' || output.back() == ' ')) {
            output.pop_back();
        }
        return output;
    }

    static std::vector<pid_t> collectDaemonPidsForPattern(const std::string& pattern) {
        std::vector<pid_t> pids;
        std::set<pid_t> seen;
        const std::regex daemonRegex(pattern);

#if defined(__linux__)
        std::error_code procEc;
        for (const auto& entry : std::filesystem::directory_iterator("/proc", procEc)) {
            if (procEc) {
                break;
            }

            std::error_code entryEc;
            if (!entry.is_directory(entryEc) || entryEc) {
                continue;
            }

            const std::string name = entry.path().filename().string();
            if (name.empty() || !std::all_of(name.begin(), name.end(), [](unsigned char ch) {
                    return std::isdigit(ch) != 0;
                })) {
                continue;
            }

            pid_t pid = -1;
            try {
                pid = static_cast<pid_t>(std::stol(name));
            } catch (...) {
                continue;
            }

            auto commandLine = readProcCommandLine(pid);
            if (!commandLine || !std::regex_search(*commandLine, daemonRegex)) {
                continue;
            }

            if (yams::daemon::client::isLiveDaemonProcess(pid) && seen.insert(pid).second) {
                pids.push_back(pid);
            }
        }

        if (!pids.empty()) {
            return pids;
        }
#endif

        std::istringstream lines(runCommandCapture("ps -ax -o pid=,command="));
        std::string line;
        while (std::getline(lines, line)) {
            const auto first = line.find_first_not_of(" \t");
            if (first == std::string::npos) {
                continue;
            }

            const auto pidEnd = line.find_first_of(" \t", first);
            const std::string pidToken = line.substr(first, pidEnd - first);

            pid_t pid = -1;
            try {
                pid = static_cast<pid_t>(std::stol(pidToken));
            } catch (...) {
                continue;
            }

            const std::string command =
                pidEnd == std::string::npos ? std::string{} : line.substr(pidEnd + 1);
            if (command.empty() || !std::regex_search(command, daemonRegex)) {
                continue;
            }

            if (yams::daemon::client::isLiveDaemonProcess(pid) && seen.insert(pid).second) {
                pids.push_back(pid);
            }
        }

        return pids;
    }

    std::string resolveSocketPathForLiveDaemon(const std::string& preferredSocket,
                                               const std::string& pidFilePath,
                                               bool allowAnyDaemonFallback = true) {
        if (auto discovered = yams::daemon::client::discoverLiveDaemonSocket(
                preferredSocket, pidFilePath, allowAnyDaemonFallback);
            discovered && !discovered->empty()) {
            if (discovered->string() != preferredSocket) {
                spdlog::info("Using live daemon socket '{}' instead of configured '{}'",
                             discovered->string(), preferredSocket);
            }
            return discovered->string();
        }

        return preferredSocket;
    }

    static bool isDaemonProcessRunningForSocket(const std::string& socketPath) {
        if (socketPath.empty()) {
            return false;
        }
        std::string pattern = std::string("yams-daemon.*") + escapeRegexLiteral(socketPath);
        return !collectDaemonPidsForPattern(pattern).empty();
    }

    static std::vector<pid_t> collectDaemonPidsForSocket(const std::string& socketPath) {
        if (socketPath.empty()) {
            return {};
        }
        std::string pattern = std::string("yams-daemon.*") + escapeRegexLiteral(socketPath);
        return collectDaemonPidsForPattern(pattern);
    }

    static bool isAnyDaemonProcessRunning() {
        return !collectDaemonPidsForPattern("yams-daemon").empty();
    }
#else
    std::string resolveSocketPathForLiveDaemon(const std::string& preferredSocket,
                                               const std::string& pidFilePath,
                                               bool allowAnyDaemonFallback = true) {
        (void)pidFilePath;
        (void)allowAnyDaemonFallback;
        return preferredSocket;
    }

    static std::vector<pid_t> collectDaemonPidsForSocket(const std::string& socketPath) {
        (void)socketPath;
        return {};
    }
#endif

    bool checkAndHandleVersionMismatch(const std::string& socketPath) {
        if (!daemon::DaemonClient::isDaemonRunning(socketPath)) {
            return false; // No daemon running, no version mismatch
        }

        // Probe status via DaemonClient
        auto statusResult = runDaemonClient(
            {}, [](yams::daemon::DaemonClient& client) { return client.status(); },
            std::chrono::seconds(5));
        if (!statusResult) {
            spdlog::warn("Could not get status from running daemon: {}",
                         statusResult.error().message);
            return false; // Treat as not ready; let startup spinner handle readiness
        }

        const auto& status = statusResult.value();

        if (!isVersionCompatible(status.version)) {
            spdlog::info("Stopping incompatible daemon (version {})...", status.version);

            // Try graceful shutdown via socket first using DaemonClient
            daemon::ShutdownRequest sreq;
            sreq.graceful = true;
            auto shutdownResult = runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.shutdown(true); },
                std::chrono::seconds(10));
            bool stopped = false;

            if (shutdownResult) {
                // Wait for daemon to stop
                for (int i = 0; i < 20; i++) {
                    if (!daemon::DaemonClient::isDaemonRunning(socketPath)) {
                        stopped = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(250));
                }
            }

            // If socket shutdown failed, try PID-based termination
            if (!stopped) {
                spdlog::warn(
                    "Socket shutdown failed for incompatible daemon, trying PID-based termination");

                // Resolve PID file path
                std::string pidFilePath = YamsCLI::resolveConfiguredDaemonPidFilePath().string();

                pid_t pid = readPidFromFile(pidFilePath);
                if (pid > 0 &&
                    yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFilePath, pid)) {
                    // Try SIGTERM first
                    if (killDaemonByPid(pid, false)) {
                        stopped = true;
                        spdlog::info("Incompatible daemon terminated with SIGTERM");
                    } else {
                        // Force kill if SIGTERM failed
                        spdlog::warn("SIGTERM failed, using SIGKILL on incompatible daemon");
                        if (killDaemonByPid(pid, true)) {
                            stopped = true;
                            spdlog::info("Incompatible daemon terminated with SIGKILL");
                        }
                    }
                }

                // Clean up files
                if (stopped) {
                    cleanupDaemonFiles(socketPath, pidFilePath);
                }
            }

            if (stopped) {
                spdlog::info("Successfully stopped incompatible daemon");
                return false; // No daemon running now
            } else {
                spdlog::error("Failed to stop incompatible daemon");
                return true; // Old daemon still running (block new start)
            }
        }

        return true; // Compatible daemon is running (block new start)
    }

    std::string resolveConfiguredSocketPath() const {
        return socketPath_.empty() ? YamsCLI::resolveConfiguredDaemonSocketPath().string()
                                   : socketPath_;
    }

    std::string resolveConfiguredPidFilePath() const {
        return pidFile_.empty() ? YamsCLI::resolveConfiguredDaemonPidFilePath().string() : pidFile_;
    }

    void startDaemon() {
        namespace fs = std::filesystem;

        // Resolve paths if not explicitly provided
        // For start: do NOT persist a resolved socket into socketPath_ unless user passed it.
        // Use a local effective path for pre-checks only; the daemon will resolve from config.
        pidFile_ = resolveConfiguredPidFilePath();
        const std::string configuredSocket = resolveConfiguredSocketPath();
        const std::string effectiveSocket =
            resolveSocketPathForLiveDaemon(configuredSocket, pidFile_, socketPath_.empty());

        spdlog::debug("Using socket (effective): {}", effectiveSocket);
        spdlog::debug("Using PID file: {}", pidFile_);

        // Check if daemon is already running and handle version compatibility
        if (checkAndHandleVersionMismatch(effectiveSocket)) {
            if (startRestart_) {
                std::cout << "[INFO] YAMS daemon is already running - restarting...\n";
                restartDaemon();
                return;
            }

            auto confirm = [](const std::string& q) -> bool {
#ifndef _WIN32
                bool interactive = ::isatty(STDIN_FILENO);
#else
                bool interactive = true;
#endif
                if (!interactive)
                    return false;
                std::cout << q << " [y/N]: " << std::flush;
                std::string ans;
                if (!std::getline(std::cin, ans))
                    return false;
                if (ans.empty())
                    return false;
                char c = static_cast<char>(std::tolower(ans[0]));
                return c == 'y';
            };

            if (!confirm("YAMS daemon is already running. Stop it and start a new one?")) {
                std::cout << "Use '--restart' to restart it, or run 'yams daemon status -d' to "
                             "view daemon status.\n";
                return;
            }

            pid_t pidBeforeStop = readPidFromFile(pidFile_);
            if (!yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFile_, pidBeforeStop)) {
                pidBeforeStop = -1;
            }
            if (!stopDaemon()) {
                std::cerr << "Failed to stop running daemon; not starting a new one.\n";
                std::exit(1);
            }

            if (!waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(5),
                                   pidBeforeStop)) {
                std::cerr << "Failed to stop running daemon; not starting a new one.\n";
                std::exit(1);
            }
        }

        // Determine foreground mode preference: start-level flag overrides global
        const bool runForeground = startForeground_ || foreground_;

        if (runForeground) {
            // Exec yams-daemon with provided flags, do not return on success
            std::cout << "[INFO] Starting YAMS daemon in foreground mode...\n";

            // Derive executable path
            std::string exePath;
            if (!startDaemonBinary_.empty()) {
                exePath = startDaemonBinary_;
            } else if (const auto daemonBin = yams::config::getenv_nonempty("YAMS_DAEMON_BIN")) {
                exePath = *daemonBin;
            } else {
                // Try to auto-detect relative to CLI binary location
                std::error_code ec;
                fs::path selfExe;
#ifdef _WIN32
                wchar_t buf[MAX_PATH];
                DWORD n = GetModuleFileNameW(NULL, buf, MAX_PATH);
                if (n > 0 && n < MAX_PATH) {
                    selfExe = fs::path(buf);
                }
#else
                char buf[4096]{};
                ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
                if (n > 0) {
                    buf[n] = '\0';
                    selfExe = fs::path(buf);
                }
#endif
                if (!selfExe.empty()) {
                    auto cliDir = selfExe.parent_path();
#ifdef _WIN32
                    std::vector<fs::path> candidates = {
                        cliDir / "yams-daemon.exe",
                        cliDir.parent_path() / "yams-daemon.exe",
                        cliDir.parent_path() / "daemon" / "yams-daemon.exe",
                        cliDir.parent_path().parent_path() / "daemon" / "yams-daemon.exe",
                        cliDir.parent_path().parent_path() / "yams-daemon.exe",
                        cliDir.parent_path().parent_path() / "src" / "daemon" / "yams-daemon.exe"};
#else
                    std::vector<fs::path> candidates = {
                        cliDir / "yams-daemon",
                        cliDir.parent_path() / "yams-daemon",
                        cliDir.parent_path() / "daemon" / "yams-daemon",
                        cliDir.parent_path().parent_path() / "daemon" / "yams-daemon",
                        cliDir.parent_path().parent_path() / "yams-daemon",
                        cliDir.parent_path().parent_path() / "src" / "daemon" / "yams-daemon"};
#endif
                    for (const auto& p : candidates) {
                        if (fs::exists(p)) {
                            exePath = p.string();
                            break;
                        }
                    }
                }
                if (exePath.empty()) {
#ifdef _WIN32
                    exePath = "yams-daemon.exe"; // fallback to PATH
#else
                    exePath = "yams-daemon"; // fallback to PATH
#endif
                }
            }

            // Build argv using stable storage first, then create the char* array. Runtime paths and
            // log policy are explicit; the post-fork branch does not mutate process environment.

            std::vector<std::string> args;
            args.push_back(exePath); // argv[0]
            // Only pass --socket if explicitly provided by the user to avoid overriding config
            if (!socketPath_.empty()) {
                args.emplace_back("--socket");
                args.push_back(socketPath_);
            }
            // Optional data-dir (prefer explicit CLI option, then global CLI data path)
            std::string effectiveDataDir;
            if (!dataDir_.empty()) {
                effectiveDataDir = dataDir_;
            } else if (cli_) {
                effectiveDataDir = cli_->getDataPath().string();
            }
            if (!effectiveDataDir.empty()) {
                args.emplace_back("--data-dir");
                args.push_back(effectiveDataDir);
            }
            if (!pidFile_.empty()) {
                args.emplace_back("--pid-file");
                args.push_back(pidFile_);
            }
            if (!startConfigPath_.empty()) {
                args.emplace_back("--config");
                args.push_back(startConfigPath_);
            }
            if (!startLogLevel_.empty()) {
                args.emplace_back("--log-level");
                args.push_back(startLogLevel_);
            }
            // Ensure foreground flag is propagated so the daemon doesn't daemonize
            args.emplace_back("--foreground");

            std::vector<char*> argv;
            argv.reserve(args.size() + 1);
            for (auto& s : args) {
                argv.push_back(const_cast<char*>(s.c_str()));
            }
            argv.push_back(nullptr);

            // Exec and never return on success
            ::execvp(exePath.c_str(), argv.data());
            spdlog::error("Failed to exec yams-daemon ({}): {}", exePath, strerror(errno));
            std::cerr << "[FAIL] Failed to exec yams-daemon: " << exePath << ": " << strerror(errno)
                      << "\n";
            std::exit(1);
        } else {
            // Start daemon in background
            daemon::ClientConfig config;
            config.socketPath = effectiveSocket;
            config.pidFile = pidFile_;
            // Prefer explicit start option; otherwise use global CLI data path
            if (!dataDir_.empty()) {
                config.dataDir = dataDir_;
            } else if (cli_) {
                config.dataDir = cli_->getDataPath();
            }

            config.logLevel = startLogLevel_;
            config.daemonBinary = startDaemonBinary_;
            config.configPath = startConfigPath_;

            // The daemon owns stale lifecycle-artifact recovery while acquiring its
            // process and data-directory locks. Removing files here can orphan a live
            // daemon during a transient IPC failure.
            std::optional<yams::cli::ui::SpinnerRunner> spinner;
            if (yams::cli::ui::stdout_is_tty()) {
                spinner.emplace();
                spinner->start("Starting daemon...");
            }
            auto result = daemon::DaemonClient::startDaemon(config);
            if (spinner) {
                spinner->stop();
            }
            if (!result) {
                spdlog::error("Failed to start daemon: {}", result.error().message);
                std::cerr << formatErrorWithHint(result.error().code, "Failed to start daemon: " +
                                                                          result.error().message)
                          << "\n";
                std::cerr << "  💡 Hint: Check if another daemon is already running\n";
                std::cerr << "  📋 Try: yams daemon stop && yams daemon start\n";
                std::exit(1);
            }

            // No-wait fast exit with a concise tip
            std::cout << "Run 'yams daemon status -d' to monitor readiness.\n";
            return;
        }
    }

    bool stopDaemon() {
        pidFile_ = resolveConfiguredPidFilePath();
        // Resolve paths if not explicitly provided (do not persist into socketPath_)
        const std::string configuredSocket = resolveConfiguredSocketPath();
        const std::string effectiveSocket =
            resolveSocketPathForLiveDaemon(configuredSocket, pidFile_, socketPath_.empty());

        std::optional<yams::cli::ui::SpinnerRunner> spinner;
        if (yams::cli::ui::stdout_is_tty()) {
            spinner.emplace();
            spinner->start("Stopping daemon...");
        }
        auto stopSpinner = [&]() {
            if (spinner) {
                spinner->stop();
            }
        };

        // Capture PID early in case the PID file disappears during shutdown
        const pid_t recordedPid = readPidFromFile(pidFile_);
        const auto liveSocketPids = collectDaemonPidsForSocket(effectiveSocket);
        const auto trustedPid = [&](pid_t pid) -> pid_t {
            if (pid <= 0) {
                return -1;
            }
            if (std::find(liveSocketPids.begin(), liveSocketPids.end(), pid) !=
                liveSocketPids.end()) {
                return pid;
            }
            if (yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFile_, pid)) {
                return pid;
            }
            return -1;
        };
        pid_t initialPid = trustedPid(recordedPid);
        if (recordedPid > 0 && initialPid <= 0 && kill(recordedPid, 0) == 0) {
            spdlog::warn(
                "Ignoring stale or recycled PID {} from '{}' because daemon identity could not "
                "be verified",
                recordedPid, pidFile_);
        }

        // Check if daemon is running
        const bool ipcResponsive = daemon::DaemonClient::isDaemonRunning(effectiveSocket);
        bool daemonRunning = ipcResponsive;
        if (!daemonRunning) {
            if (!liveSocketPids.empty()) {
                if (initialPid <= 0 || kill(initialPid, 0) != 0) {
                    initialPid = liveSocketPids.front();
                }
                spdlog::warn("Found daemon process (PID {}) not responding on socket", initialPid);
                daemonRunning = true;
            }
        }
        if (!daemonRunning) {
            // Check if there's a stale PID file
            pid_t pid = initialPid;
            if (pid > 0 && kill(pid, 0) == 0) {
                spdlog::warn("Found daemon process (PID {}) not responding on socket", pid);
                daemonRunning = true;
            } else {
                // Lock-file payloads are diagnostic only. Without a responsive socket,
                // socket-scoped process match, or verified PID file, there is no authority
                // to signal a process.
                spdlog::info("YAMS daemon is not running");
                cleanupDaemonFiles(effectiveSocket, pidFile_);
                stopSpinner();
                return true;
            }
        }

        bool stopped = false;
        const bool shouldForceOrphanedStop = force_ || !ipcResponsive;
        auto isExpectedShutdownDisconnect = [](const std::string& message) {
            return message.find("Broken pipe") != std::string::npos ||
                   message.find("Connection reset") != std::string::npos ||
                   message.find("Connection closed") != std::string::npos ||
                   message.find("End of file") != std::string::npos ||
                   message.find("EPIPE") != std::string::npos ||
                   message.find("ECONNRESET") != std::string::npos;
        };
        auto pidAlive = [&]() -> bool {
            pid_t pid = trustedPid(readPidFromFile(pidFile_));
            if (pid <= 0) {
                pid = initialPid;
            }
            return pid > 0 && kill(pid, 0) == 0;
        };

        // First try graceful shutdown via socket
        if (daemonRunning) {
            daemon::ShutdownRequest sreq;
            sreq.graceful = !force_;
            yams::daemon::ClientConfig cfg;
            cfg.socketPath = effectiveSocket;
            auto shutdownResult = runDaemonClient(
                cfg,
                [&](yams::daemon::DaemonClient& client) { return client.shutdown(sreq.graceful); },
                std::chrono::seconds(10));
            if (shutdownResult) {
                spdlog::info("Sent shutdown request to daemon");
                const auto waitTimeout =
                    force_ ? std::chrono::seconds(8)
                           : yams::daemon::shutdown_budget::kDefaultGracefulShutdownWaitTimeout;
                stopped = waitForDaemonStop(effectiveSocket, pidFile_, waitTimeout, initialPid);
                if (stopped) {
                    spdlog::info("Daemon stopped successfully after graceful shutdown request");
                } else {
                    spdlog::info("Daemon shutdown requested; waiting for process to exit");
                }
            } else {
                // Treat common peer-closure/transient errors as potentially-successful if the
                // daemon disappears shortly after the request.
                const auto& shutdownMessage = shutdownResult.error().message;
                if (isExpectedShutdownDisconnect(shutdownMessage)) {
                    spdlog::debug("Shutdown request disconnected during daemon exit: {}",
                                  shutdownMessage);
                } else {
                    spdlog::debug("Socket shutdown encountered: {}", shutdownMessage);
                }
                const auto waitTimeout =
                    force_ ? std::chrono::seconds(8)
                           : yams::daemon::shutdown_budget::kDefaultGracefulShutdownWaitTimeout;
                stopped = waitForDaemonStop(effectiveSocket, pidFile_, waitTimeout, initialPid);
                if (stopped) {
                    spdlog::info("Daemon stopped successfully after shutdown disconnect");
                }
            }
        }

        // If socket shutdown failed, try PID-based termination
        if (!stopped) {
            pid_t pid = trustedPid(readPidFromFile(pidFile_));
            if (pid <= 0) {
                pid = initialPid;
            }
            if (pid > 0) {
                spdlog::info("Attempting PID-based termination for daemon (PID {})", pid);

                // Try SIGTERM first
                if (killDaemonByPid(pid, false)) {
                    stopped = true;
                    spdlog::info("Daemon terminated with SIGTERM");
                } else if (shouldForceOrphanedStop) {
                    spdlog::warn("SIGTERM failed, using SIGKILL for unresponsive daemon");
                    if (killDaemonByPid(pid, true)) {
                        stopped = true;
                        spdlog::info("Daemon terminated with SIGKILL");
                    } else {
                        spdlog::error("Failed to kill daemon even with SIGKILL");
                    }
                } else {
                    spdlog::debug("Daemon did not respond to SIGTERM. Use --force to kill it");
                }
            } else {
                spdlog::debug("No PID file found at: {}", pidFile_);
                // If no PID file but socket shutdown was attempted, assume it worked
                if (daemonRunning) {
                    spdlog::debug("Could not verify daemon stopped (no PID file), but shutdown was "
                                  "requested");
                }
            }
        }

        // If still not stopped, try platform-specific last-resort termination for orphaned daemons
        if (!stopped && daemonRunning) {
            spdlog::debug("Daemon not responding to shutdown, attempting to kill orphaned process");

#ifndef _WIN32
            const auto socketPids = collectDaemonPidsForSocket(effectiveSocket);
            if (!socketPids.empty()) {
                spdlog::debug("Found {} daemon PID(s) matching socket", socketPids.size());
                bool socketPidsStopped = true;
                for (auto pid : socketPids) {
                    bool killed = false;
                    if (killDaemonByPid(pid, false)) {
                        killed = true;
                    } else if (shouldForceOrphanedStop) {
                        killed = killDaemonByPid(pid, true);
                    }
                    if (!killed) {
                        socketPidsStopped = false;
                        spdlog::debug("Failed to stop daemon PID {}", pid);
                    }
                }
                if (socketPidsStopped) {
                    stopped = true;
                }
            }
#endif
        }

        // Final verification: poll for daemon exit since slow shutdown
        // (WAL flush, vector index commit, maintenance tasks) can outlast the
        // intermediate timeouts above. Replaces a one-shot check that raced
        // shutdown completion when the daemon needed >~18s to fully exit.
#ifndef _WIN32
        if (!stopped) {
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
            while (std::chrono::steady_clock::now() < deadline) {
                if (!isDaemonProcessRunningForSocket(effectiveSocket) && !pidAlive()) {
                    spdlog::debug("Daemon exited during extended grace; treating as success");
                    stopped = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
#endif

        // Clean up files if daemon was stopped
        if (stopped) {
#ifndef _WIN32
            if (isDaemonProcessRunningForSocket(effectiveSocket)) {
                spdlog::info(
                    "Daemon process still exiting after stop request; allowing extra grace "
                    "period");
                stopped = waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(3),
                                            initialPid);
            }
#endif
        }

        if (stopped) {
            cleanupDaemonFiles(effectiveSocket, pidFile_);
            stopSpinner();
            std::cout << "[OK] YAMS daemon stopped successfully\n";
            return true;
        } else {
            stopSpinner();
            spdlog::error("Failed to stop YAMS daemon");
            std::cerr << "[FAIL] Failed to stop YAMS daemon\n";
            std::cerr << "  💡 Hint: The daemon may be unresponsive or owned by another user\n";
            if (pidAlive()) {
                std::cerr
                    << "  ℹ Daemon may still be shutting down; retry in a few seconds if this "
                       "was a graceful stop\n";
            }
            if (!force_) {
                std::cerr << "  📋 Try: yams daemon stop --force\n";
            }
#ifndef _WIN32
            std::cerr << "  📋 Or manually: pkill yams-daemon\n";
#else
            std::cerr << "  📋 Or manually (Windows): taskkill /IM yams-daemon.exe /T /F\n";
#endif
            return false;
        }
    }

    void doctorDaemon() {
        namespace fs = std::filesystem;
        pidFile_ = resolveConfiguredPidFilePath();
        const std::string configuredSocket = resolveConfiguredSocketPath();
        std::string effectiveSocket =
            resolveSocketPathForLiveDaemon(configuredSocket, pidFile_, socketPath_.empty());
        // Title - more compact
        std::cout << "\n=== YAMS Daemon Doctor ===\n\n";

        // Section: IPC & Files - compact header
        std::cout << "IPC & Files:\n";
        std::cout << "  Socket:    "
                  << (effectiveSocket.empty() ? "<resolve failed>" : effectiveSocket) << "\n";
        if (effectiveSocket != configuredSocket && !configuredSocket.empty()) {
            std::cout << "  Configured Socket: " << configuredSocket << "\n";
        }
        std::cout << "  PID File:  " << pidFile_ << "\n";
        // Helper: interactive confirm when on a TTY
        auto confirm = [](const std::string& q) -> bool {
#ifndef _WIN32
            bool interactive = ::isatty(STDIN_FILENO);
#else
            bool interactive = true;
#endif
            if (!interactive)
                return false;
            std::cout << q << " [y/N]: " << std::flush;
            std::string ans;
            if (!std::getline(std::cin, ans))
                return false;
            if (ans.size() == 0)
                return false;
            char c = static_cast<char>(std::tolower(ans[0]));
            return c == 'y';
        };

        // Check socket path length
        bool ok = true;
        std::vector<std::string> hints; // Collect hints to show at end
#ifndef _WIN32
        if (!effectiveSocket.empty()) {
            size_t maxlen = sizeof(sockaddr_un::sun_path);
            if (effectiveSocket.size() >= maxlen) {
                std::cout << "  [FAIL] Socket path too long (" << effectiveSocket.size() << "/"
                          << maxlen << ")\n";
                ok = false;
                hints.push_back("Socket path too long - use: export "
                                "YAMS_DAEMON_SOCKET=/tmp/yams-daemon-$(id -u).sock");
            } else {
                std::cout << "  [OK] Socket path length OK\n";
            }
        }
#endif
        // Parent directory writable
        bool socket_exists = !effectiveSocket.empty() && safe_exists(effectiveSocket);
        if (!effectiveSocket.empty() && !socket_exists) {
            fs::path parent = fs::path(effectiveSocket).parent_path();
            std::error_code ec;
            if (parent.empty())
                parent = ".";
            yams::common::ensureDirectories(parent); // best effort
            fs::path probe = parent / ".yams-doctor-probe";
            std::ofstream f(probe);
            if (f.good()) {
                f << "ok";
                f.close();
                fs::remove(probe, ec);
                std::cout << "  Socket Dir: " << parent.string() << " "
                          << yams::cli::ui::colorize("[writable]", yams::cli::ui::Ansi::GREEN)
                          << "\n";
            } else {
                std::cout << "  Socket Dir: " << parent.string() << " "
                          << yams::cli::ui::colorize("[not writable]", yams::cli::ui::Ansi::RED)
                          << "\n";
                ok = false;
                hints.push_back("Socket directory not writable - check permissions");
            }
        }
        // Check for stale socket (and show readiness summary if daemon responds)
        if (socket_exists) {
            std::cout << "\nDaemon Probe:\n";
            (void)yams::config::set_environment("YAMS_CLIENT_DEBUG", "1");
            bool alive = daemon::DaemonClient::isDaemonRunning(effectiveSocket);
            if (alive) {
                std::cout << "  Socket: "
                          << yams::cli::ui::colorize("RESPONDING", yams::cli::ui::Ansi::GREEN)
                          << "\n";
                // Fetch detailed readiness and show a short Waiting on: summary
                try {
                    auto sres = runDaemonClient(
                        {}, [](yams::daemon::DaemonClient& client) { return client.status(true); },
                        std::chrono::seconds(2));
                    if (sres) {
                        const auto& s = sres.value();
                        // Daemon Status - compact format
                        std::cout << "\nDaemon Status:\n";
                        std::string lifecycle =
                            !s.lifecycleState.empty()
                                ? s.lifecycleState
                                : (s.overallStatus.empty() ? (s.ready ? std::string("ready")
                                                                      : std::string("initializing"))
                                                           : s.overallStatus);
                        std::cout << "  State:       " << lifecycle << "\n";
                        if (!s.lastError.empty()) {
                            std::cout << "  Last Error:  " << s.lastError << "\n";
                        }
                        if (!s.version.empty())
                            std::cout << "  Version:     " << s.version << "\n";
                        std::cout << "  Uptime:      " << s.uptimeSeconds << "s\n";
                        std::cout << "  Connections: " << s.activeConnections << "\n";
                        // Show degraded search indicator if present
                        try {
                            auto itDeg = s.readinessStates.find("search_engine_degraded");
                            if (itDeg != s.readinessStates.end() && itDeg->second) {
                                std::cout << "  Search:      degraded (repairing)\n";
                            }
                        } catch (...) {
                        }
                        // Show vector scoring availability
                        try {
                            bool vecAvail = false, vecEnabled = false;
                            if (auto it = s.readinessStates.find("vector_embeddings_available");
                                it != s.readinessStates.end())
                                vecAvail = it->second;
                            if (auto it = s.readinessStates.find("vector_scoring_enabled");
                                it != s.readinessStates.end())
                                vecEnabled = it->second;
                            if (!vecEnabled) {
                                std::cout
                                    << "  Vector:      disabled — "
                                    << (vecAvail ? "config weight=0" : "embeddings unavailable")
                                    << "\n";
                            } else {
                                std::cout << "  Vector:      enabled\n";
                            }
                        } catch (...) {
                        }
                        // Worker pool (from requestCounts)
                        try {
                            auto itT = s.requestCounts.find("worker_threads");
                            auto itA = s.requestCounts.find("worker_active");
                            auto itQ = s.requestCounts.find("worker_queued");
                            if (itT != s.requestCounts.end() || itA != s.requestCounts.end() ||
                                itQ != s.requestCounts.end()) {
                                std::size_t threads =
                                    itT != s.requestCounts.end() ? itT->second : 0;
                                std::size_t active = itA != s.requestCounts.end() ? itA->second : 0;
                                std::size_t queued = itQ != s.requestCounts.end() ? itQ->second : 0;
                                std::size_t util =
                                    (threads > 0)
                                        ? static_cast<std::size_t>((100.0 * active) / threads)
                                        : 0;
                                std::cout << "  Worker Pool:  threads=" << threads
                                          << ", active=" << active << ", queued=" << queued
                                          << ", util=" << util << "%\n";
                            }
                        } catch (...) {
                        }

                        // Version mismatch check
                        try {
                            std::string current = YAMS_VERSION_STRING;
                            if (!s.version.empty() && s.version != current) {
                                hints.push_back("Version mismatch - run: yams daemon restart");
                            }
                        } catch (...) {
                        }
                        // Show any components not ready yet
                        bool has_waiting = false;
                        for (const auto& [k, v] : s.readinessStates) {
                            if (!v) {
                                if (!has_waiting) {
                                    std::cout << "\nWaiting:\n";
                                    has_waiting = true;
                                }
                                std::cout << "  - " << k;
                                auto it = s.initProgress.find(k);
                                if (it != s.initProgress.end()) {
                                    std::cout << " (" << (int)it->second << "%)";
                                }
                                std::cout << "\n";

                                // Add specific hints for components
                                if (k == "search_engine") {
                                    hints.push_back(
                                        "Search engine initializing - run: yams session warm");
                                } else if (k == "vector_index") {
                                    hints.push_back(
                                        "Vector index building - check model availability");
                                } else if (k == "content_store") {
                                    hints.push_back("Content store not ready - verify YAMS_STORAGE "
                                                    "is writable");
                                }
                            }
                        }
                        // Show slow components if available
                        try {
                            auto rt = config::get_daemon_status_file();
                            std::ifstream bf(rt);
                            if (bf) {
                                json j;
                                bf >> j;
                                if (j.contains("top_slowest") && j["top_slowest"].is_array() &&
                                    !j["top_slowest"].empty()) {
                                    std::cout << "\nSlow Components:\n";
                                    auto arr = j["top_slowest"];
                                    size_t show = std::min<size_t>(arr.size(), 3);
                                    for (size_t i = 0; i < show; ++i) {
                                        const auto& e = arr[i];
                                        std::string name = e.value("name", std::string{"unknown"});
                                        uint64_t ms = e.value("elapsed_ms", 0ULL);
                                        std::cout << "  - " << name << ": " << ms << "ms\n";
                                    }
                                }
                            }
                        } catch (...) {
                        }

                        // Check resource usage
                        if (s.cpuUsagePercent >= 90.0) {
                            hints.push_back("High CPU usage (" +
                                            std::to_string((int)s.cpuUsagePercent) +
                                            "%) - reduce concurrent tasks");
                        }
                        if (s.memoryUsageMb > 4096.0) {
                            hints.push_back("High memory usage (" +
                                            std::to_string((int)s.memoryUsageMb) +
                                            " MB) - reduce cache/model size");
                        }
                    }
                } catch (...) {
                }
            } else {
                std::cout << "  Socket: "
                          << yams::cli::ui::colorize("STALE", yams::cli::ui::Ansi::YELLOW) << "\n";
                if (confirm("Remove stale socket file?")) {
                    std::error_code ec;
                    fs::remove(effectiveSocket, ec);
                    if (ec) {
                        std::cout << "    [FAIL] " << ec.message() << "\n";
                    } else {
                        std::cout << "    [OK] Removed\n";
                    }
                }
            }
        } else {
            std::cout << "\nSocket: NOT PRESENT\n";
        }
        // PID check
        std::cout << "\nPID Check:\n";
        pid_t pid = readPidFromFile(pidFile_);
        std::string fallbackPidPath;
        pid_t fallbackPid = -1;
        {
            // Compute a deterministic /tmp fallback path: /tmp/yams-daemon-<uid>.pid
            uid_t uid = getuid();
            fallbackPidPath = std::string{"/tmp/yams-daemon-"} + std::to_string(uid) + ".pid";
            if (fallbackPidPath != pidFile_) {
                fallbackPid = readPidFromFile(fallbackPidPath);
            }
        }
#ifndef _WIN32
        if (pid > 0 && yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFile_, pid)) {
            std::cout << "  PID " << pid << ": RUNNING\n";
        } else if (pid > 0) {
            std::cout << "  PID " << pid << ": STALE\n";
            if (confirm("  Remove stale PID file?")) {
                std::error_code ec;
                fs::remove(pidFile_, ec);
                if (ec) {
                    std::cout << "    [FAIL] " << ec.message() << "\n";
                } else {
                    std::cout << "    [OK] Removed\n";
                }
            }
        } else {
            // Try fallback path in /tmp
            if (!fallbackPidPath.empty() && fallbackPid > 0) {
                if (yams::daemon::client::pidFileIdentifiesLiveDaemon(fallbackPidPath,
                                                                      fallbackPid)) {
                    std::cout << "  Fallback PID " << fallbackPid << ": RUNNING\n";
                } else {
                    std::cout << "  Fallback PID " << fallbackPid << ": STALE\n";
                }
            } else {
                std::cout << "  No PID file found\n";
            }
        }

        // If IPC is not yet available and PID is not confirmed running,
        // detect a launching daemon via /proc as an initializing state hint.
        if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
            try {
                bool found = false;
                for (const auto& entry : fs::directory_iterator("/proc")) {
                    if (!entry.is_directory())
                        continue;
                    const auto& p = entry.path();
                    auto name = p.filename().string();
                    if (name.empty() || name.find_first_not_of("0123456789") != std::string::npos)
                        continue; // not a PID directory
                    std::ifstream cmd(p / "cmdline");
                    if (!cmd)
                        continue;
                    std::string cmdline;
                    std::getline(cmd, cmdline, '\0');
                    if (cmdline.find("yams-daemon") != std::string::npos) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    std::cout << "  Process found: INITIALIZING\n";
                }
            } catch (...) {
                // ignore errors from /proc scanning
            }
        }
#else
        if (pid > 0) {
            std::cout << "  PID recorded: " << pid << "\n";
        }
#endif

        // Show collected hints/recommendations
        if (!hints.empty() || !ok) {
            std::cout << "\nRecommendations:\n";
            for (const auto& hint : hints) {
                std::cout << "  • " << hint << "\n";
            }
            if (!ok && hints.empty()) {
                std::cout
                    << "  • Check failed - set YAMS_DAEMON_SOCKET=/tmp/yams-daemon-$(id -u).sock\n";
            }
        } else {
            std::cout << "\n" << ui::status_ok("All checks passed") << "\n";
        }
    }

    // Best-effort memory-sync (P2P) status. The fetch is bounded and failures are silent so a
    // slow sync cycle never stalls daemon status; rendering lives in daemon_status_render.cpp.
    std::optional<yams::daemon::MemorySyncResponse>
    fetchMemorySyncStatus(const yams::daemon::ClientConfig& cfg) const {
        namespace ms = yams::daemon;
        auto res = runDaemonClient(
            cfg,
            [](ms::DaemonClient& client) {
                return client.template call<ms::MemorySyncRequest>(
                    ms::MemorySyncRequest{ms::MemorySyncOperation::Status, {}, {}});
            },
            std::chrono::seconds(3));
        if (!res) {
            return std::nullopt;
        }
        return std::move(res.value());
    }

    void showStatus() {
        pidFile_ = resolveConfiguredPidFilePath();

        const std::string configuredSocket = resolveConfiguredSocketPath();
        const std::string effectiveSocket =
            resolveSocketPathForLiveDaemon(configuredSocket, pidFile_, socketPath_.empty());
        // When the operator explicitly points the CLI at a custom daemon instance (--socket or
        // YAMS_DAEMON_SOCKET), a differing data directory is expected, not an accident.
        const bool explicitCustomSocket =
            !socketPath_.empty() || yams::config::getenv_nonempty("YAMS_DAEMON_SOCKET").has_value();

        if (detailed_) {
            // Enable client debug logging for ping/connect path
            (void)yams::config::set_environment("YAMS_CLIENT_DEBUG", "1");
        }

        // Check if daemon is running (prefer socket; fall back to PID).
        // If the socket preflight is flaky but the daemon process is clearly alive,
        // continue on to the actual StatusRequest rather than bailing out early.
        bool preflightUnavailable = !daemon::DaemonClient::isDaemonRunning(effectiveSocket);
        if (preflightUnavailable) {
            // Try verified PID identity to distinguish "starting" from a recycled PID.
            const pid_t pid = readPidFromFile(pidFile_);
            const bool daemonLikelyAlive =
                yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFile_, pid);
            if (!daemonLikelyAlive) {
                std::cout << "YAMS daemon is not running\n";
                if (effectiveSocket != configuredSocket) {
                    std::cout << "Configured socket: " << configuredSocket << "\n";
                    std::cout << "Last live daemon socket: " << effectiveSocket << "\n";
                }
                return;
            }
        }

        if (!detailed_) {
            // Compact default view - quick health check
            std::optional<yams::cli::ui::SpinnerRunner> spinner;
            if (yams::cli::ui::stdout_is_tty()) {
                spinner.emplace();
                spinner->start("Checking daemon status...");
            }
            yams::daemon::ClientConfig cfg;
            cfg.socketPath = effectiveSocket;
            // Status handler is non-blocking (cached snapshot, no subsystem calls).
            // 5s is generous headroom for transient connect/framing issues.
            auto sres = runDaemonClient(
                cfg, [](yams::daemon::DaemonClient& client) { return client.status(); },
                std::chrono::seconds(5));
            if (spinner) {
                spinner->stop();
            }
            if (!sres) {
                if (sres.error().code == ErrorCode::ResourceBusy) {
                    std::cout << sres.error().message << "\n";
                    return;
                }
                // IPC failed — the daemon is likely still initializing (VectorDB, model
                // loading, etc.) and can't serve requests yet. Fall back to the bootstrap
                // status file that the daemon writes throughout initialization so the
                // user sees useful progress instead of an opaque error.
                try {
                    auto rt = config::get_daemon_status_file();
                    json j;
                    bool haveBootstrap = false;

                    for (int attempt = 0; attempt < 10; ++attempt) {
                        std::ifstream bf(rt);
                        if (bf) {
                            bf >> j;
                            haveBootstrap = true;
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                    }

                    if (haveBootstrap) {
                        std::string overall = j.value("overall", std::string{"initializing"});
                        // Capitalize first letter for display
                        if (!overall.empty())
                            overall[0] = static_cast<char>(
                                std::toupper(static_cast<unsigned char>(overall[0])));

                        std::cout << "YAMS daemon is " << overall << " (IPC not yet responsive)\n";

                        if (j.contains("readiness")) {
                            std::vector<std::string> waiting;
                            for (auto it = j["readiness"].begin(); it != j["readiness"].end();
                                 ++it) {
                                if (!it.value().get<bool>()) {
                                    std::ostringstream w;
                                    w << it.key();
                                    if (j.contains("progress") &&
                                        j["progress"].contains(it.key())) {
                                        try {
                                            w << " (" << j["progress"][it.key()].get<int>() << "%)";
                                        } catch (...) {
                                        }
                                    }
                                    waiting.push_back(w.str());
                                }
                            }
                            if (!waiting.empty()) {
                                std::cout << "  Waiting on: ";
                                for (size_t i = 0; i < waiting.size() && i < 4; ++i) {
                                    if (i)
                                        std::cout << ", ";
                                    std::cout << waiting[i];
                                }
                                if (waiting.size() > 4)
                                    std::cout << ", …";
                                std::cout << "\n";
                            }
                        }
                        auto displayPhase = [](std::string phase) {
                            std::replace(phase.begin(), phase.end(), '_', ' ');
                            if (!phase.empty()) {
                                phase.front() = static_cast<char>(
                                    std::toupper(static_cast<unsigned char>(phase.front())));
                            }
                            return phase;
                        };
                        if (j.contains("database_phase")) {
                            try {
                                auto phase = j["database_phase"].get<std::string>();
                                std::cout << "  Database: " << displayPhase(std::move(phase));
                                if (j.contains("database_phase_elapsed_ms")) {
                                    auto elapsedMs = j["database_phase_elapsed_ms"].get<uint64_t>();
                                    if (elapsedMs > 0) {
                                        std::cout
                                            << " ("
                                            << yams::cli::ui::format_duration(elapsedMs / 1000)
                                            << " elapsed)";
                                    }
                                }
                                std::cout << "\n";
                            } catch (const std::exception& e) {
                                spdlog::debug(
                                    "daemon status bootstrap database phase parse failed: {}",
                                    e.what());
                            } catch (...) {
                                spdlog::debug(
                                    "daemon status bootstrap database phase parse failed");
                            }
                        }
                        if (j.contains("maintenance_phase")) {
                            try {
                                auto phase = j["maintenance_phase"].get<std::string>();
                                if (!phase.empty() && phase != "idle") {
                                    std::cout << "  DB Maintenance: "
                                              << displayPhase(std::move(phase));
                                    if (j.contains("maintenance_phase_elapsed_ms")) {
                                        auto elapsedMs =
                                            j["maintenance_phase_elapsed_ms"].get<uint64_t>();
                                        if (elapsedMs > 0) {
                                            std::cout
                                                << " ("
                                                << yams::cli::ui::format_duration(elapsedMs / 1000)
                                                << " elapsed)";
                                        }
                                    }
                                    std::cout << "\n";
                                }
                            } catch (const std::exception& e) {
                                spdlog::debug(
                                    "daemon status bootstrap maintenance phase parse failed: {}",
                                    e.what());
                            } catch (...) {
                                spdlog::debug(
                                    "daemon status bootstrap maintenance phase parse failed");
                            }
                        }
                        if (j.contains("uptime_seconds")) {
                            try {
                                auto elapsed = j["uptime_seconds"].get<long>();
                                std::cout << "  Uptime: ~" << elapsed << "s\n";
                            } catch (const std::exception& e) {
                                spdlog::debug("daemon status bootstrap uptime parse failed: {}",
                                              e.what());
                            } catch (...) {
                                spdlog::debug("daemon status bootstrap uptime parse failed");
                            }
                        }
                        std::cout << "  Hint: Run 'yams daemon status -d' once ready, "
                                     "or tail the daemon log.\n";
                        return;
                    }
                } catch (...) {
                }
                // No bootstrap file either — the daemon is not running or has not
                // yet written any status. Match the "not running" message emitted
                // by the pre-flight liveness check above.
                std::cout << "YAMS daemon is not running\n";
                return;
            }

            DaemonStatusRenderContext ctx;
            ctx.configuredDataDir = expectedDataDir();
            ctx.explicitCustomSocket = explicitCustomSocket;
            renderDaemonStatusWithOptionalSection(
                sres.value(), ctx, false, std::cout, [&](std::ostream& os) {
                    const auto memorySync = fetchMemorySyncStatus(cfg);
                    renderMemorySyncSection(memorySync ? &*memorySync : nullptr, os);
                });
            return;
        }

        // Detailed status via DaemonClient (synchronous)
        std::optional<yams::cli::ui::SpinnerRunner> spinner;
        if (yams::cli::ui::stdout_is_tty()) {
            spinner.emplace();
            spinner->start("Fetching daemon status...");
        }
        Error lastErr{};
        for (int attempt = 0; attempt < 5; ++attempt) {
            (void)yams::config::set_environment("YAMS_CLIENT_DEBUG", detailed_ ? "1" : "0");
            yams::daemon::ClientConfig cfg;
            cfg.socketPath = effectiveSocket;
            auto statusResult = runDaemonClient(
                cfg, [](yams::daemon::DaemonClient& client) { return client.status(true); },
                std::chrono::seconds(5));
            if (statusResult) {
                if (spinner) {
                    spinner->stop();
                }
                DaemonStatusRenderContext ctx;
                ctx.configuredDataDir = expectedDataDir();
                ctx.explicitCustomSocket = explicitCustomSocket;
                renderDaemonStatusWithOptionalSection(
                    statusResult.value(), ctx, true, std::cout, [&](std::ostream& os) {
                        const auto memorySync = fetchMemorySyncStatus(cfg);
                        renderMemorySyncSection(memorySync ? &*memorySync : nullptr, os);
                    });
                return;
            }
            lastErr = statusResult.error();
            std::this_thread::sleep_for(std::chrono::milliseconds(120 * (attempt + 1)));
        }
        if (spinner) {
            spinner->stop();
        }
        if (lastErr.code == ErrorCode::ResourceBusy) {
            std::cout << lastErr.message << "\n";
            std::exit(2);
        }
        // All retries exhausted — fall back to bootstrap status file before giving up
        try {
            auto rt = config::get_daemon_status_file();
            std::ifstream bf(rt);
            if (bf) {
                json j;
                bf >> j;
                std::string overall = j.value("overall", std::string{"initializing"});
                if (!overall.empty())
                    overall[0] =
                        static_cast<char>(std::toupper(static_cast<unsigned char>(overall[0])));
                std::cout << "YAMS daemon is " << overall << " (IPC not yet responsive after " << 5
                          << " attempts)\n";
                if (j.contains("readiness")) {
                    for (auto it = j["readiness"].begin(); it != j["readiness"].end(); ++it) {
                        std::string state = it.value().get<bool>() ? "ready" : "waiting";
                        std::string pct;
                        if (j.contains("progress") && j["progress"].contains(it.key())) {
                            try {
                                pct = " (" + std::to_string(j["progress"][it.key()].get<int>()) +
                                      "%)";
                            } catch (...) {
                            }
                        }
                        std::cout << "  " << it.key() << ": " << state << pct << "\n";
                    }
                }
                if (j.contains("uptime_seconds")) {
                    try {
                        std::cout << "  Uptime: ~" << j["uptime_seconds"].get<long>() << "s\n";
                    } catch (...) {
                    }
                }
                return;
            }
        } catch (...) {
        }
        spdlog::error("Failed to get daemon status: {}", lastErr.message);
        std::exit(1);
    }

    Result<std::shared_ptr<yams::cli::DaemonClientPool::Lease>>
    acquireDaemonClient(const yams::daemon::ClientConfig& cfg = {}) const {
        auto effectiveCfg = cfg;
        effectiveCfg.executor = getExecutor();
        return yams::cli::acquire_cli_daemon_client_shared(effectiveCfg);
    }

    template <typename AwaitableProvider>
    auto runDaemonClient(const yams::daemon::ClientConfig& cfg, AwaitableProvider&& provider,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) const
        -> decltype(yams::cli::run_result(provider(std::declval<yams::daemon::DaemonClient&>()),
                                          timeout, getExecutor())) {
        using ResultType = decltype(yams::cli::run_result(
            provider(std::declval<yams::daemon::DaemonClient&>()), timeout, getExecutor()));
        auto leaseRes = acquireDaemonClient(cfg);
        if (!leaseRes)
            return ResultType{leaseRes.error()};
        auto leaseHandle = std::move(leaseRes.value());
        return yams::cli::run_result(provider(**leaseHandle), timeout, getExecutor());
    }

    void restartDaemon() {
        pidFile_ = resolveConfiguredPidFilePath();

        const std::string configuredSocket = resolveConfiguredSocketPath();
        const std::string effectiveSocket =
            resolveSocketPathForLiveDaemon(configuredSocket, pidFile_, socketPath_.empty());

        pid_t pidBeforeStop = readPidFromFile(pidFile_);
        if (!yams::daemon::client::pidFileIdentifiesLiveDaemon(pidFile_, pidBeforeStop)) {
            pidBeforeStop = -1;
        }
        if (!stopDaemon()) {
            spdlog::error("Failed to stop daemon for restart");
            std::exit(1);
        }

        if (!waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(5), pidBeforeStop)) {
            spdlog::error("Failed to stop daemon for restart");
            std::exit(1);
        }

        // Start daemon
        std::cout << "[INFO] Starting YAMS daemon...\n";

        daemon::ClientConfig config;
        config.socketPath = effectiveSocket;
        if (!dataDir_.empty())
            config.dataDir = dataDir_;
        else if (cli_)
            config.dataDir = cli_->getDataPath().string();
        config.logLevel = startLogLevel_;
        config.daemonBinary = startDaemonBinary_;
        config.configPath = startConfigPath_;

        auto result = daemon::DaemonClient::startDaemon(config);
        exitOnError(result, "Failed to start daemon");

        bool running = false;
        for (int i = 0; i < 20; ++i) {
            if (daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                running = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        if (running) {
            std::cout << "[OK] YAMS daemon restarted successfully\n";
        } else {
            std::cout << "[WARN] Daemon started but not responding yet. "
                         "Run 'yams daemon status -d' to check readiness.\n";
        }
    }

    void showLog() {
        namespace fs = std::filesystem;

        // The daemon knows its own log file (it may have been started with an explicit
        // --log-file). Ask it first; only fall back to default paths when it is not running or
        // does not report one.
        fs::path logPath;
        try {
            auto statusRes = runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.status(true); },
                std::chrono::seconds(2));
            if (statusRes && !statusRes.value().logFile.empty()) {
                const fs::path reported(statusRes.value().logFile);
                if (fs::exists(reported)) {
                    logPath = reported;
                }
            }
        } catch (...) {
            // Best effort: fall back to the default candidate paths below.
        }

        if (logPath.empty()) {
            // Determine log file path - mirrors YamsDaemon::resolvePath(PathType::LogFile)
            std::vector<fs::path> candidates;

#ifdef _WIN32
            if (const auto localAppData = yams::config::getenv_nonempty("LOCALAPPDATA")) {
                candidates.push_back(fs::path(*localAppData) / "yams" / "daemon.log");
            }
            candidates.push_back(fs::temp_directory_path() / "yams-daemon.log");
#else
            // Root user: /var/log
            if (getuid() == 0) {
                candidates.push_back(fs::path("/var/log/yams-daemon.log"));
            }
            // XDG_STATE_HOME or ~/.local/state
            if (const auto xdgState = yams::config::getenv_nonempty("XDG_STATE_HOME")) {
                candidates.push_back(fs::path(*xdgState) / "yams" / "daemon.log");
            } else if (const auto home = yams::config::getenv_nonempty("HOME")) {
                candidates.push_back(fs::path(*home) / ".local" / "state" / "yams" / "daemon.log");
            }
            // Fallback to /tmp
            candidates.push_back(fs::path("/tmp") /
                                 ("yams-daemon-" + std::to_string(getuid()) + ".log"));
#endif

            // Find the first candidate that exists
            for (const auto& candidate : candidates) {
                if (fs::exists(candidate)) {
                    logPath = candidate;
                    break;
                }
            }

            if (logPath.empty()) {
                std::cerr << "Daemon log file not found. Checked:" << std::endl;
                for (const auto& c : candidates) {
                    std::cerr << "  - " << c.string() << std::endl;
                }
                return;
            }
        }

        // Convert level filter to lowercase for comparison
        std::string levelFilter;
        if (!logFilterLevel_.empty()) {
            levelFilter = logFilterLevel_;
            std::transform(levelFilter.begin(), levelFilter.end(), levelFilter.begin(),
                           [](unsigned char c) { return std::tolower(c); });
        }

        auto matchesLevel = [&](const std::string& line) -> bool {
            if (levelFilter.empty())
                return true;
            // spdlog format: [YYYY-MM-DD HH:MM:SS.mmm] [level] message
            // Look for level in brackets
            auto pos = line.find("] [");
            if (pos == std::string::npos)
                return true; // Can't parse, show anyway
            auto endPos = line.find(']', pos + 3);
            if (endPos == std::string::npos)
                return true;
            std::string lineLevel = line.substr(pos + 3, endPos - pos - 3);
            std::transform(lineLevel.begin(), lineLevel.end(), lineLevel.begin(),
                           [](unsigned char c) { return std::tolower(c); });

            // Level hierarchy: trace < debug < info < warn < error
            static const std::vector<std::string> levels = {"trace", "debug", "info", "warn",
                                                            "error"};
            auto filterIt = std::find(levels.begin(), levels.end(), levelFilter);
            auto lineIt = std::find(levels.begin(), levels.end(), lineLevel);
            if (filterIt == levels.end() || lineIt == levels.end())
                return true;
            return lineIt >= filterIt;
        };

        if (logFollow_) {
            // Follow mode: tail -f style
            std::cout << "Following " << logPath.string() << " (Ctrl+C to stop)..." << std::endl;
            std::ifstream file(logPath, std::ios::ate);
            if (!file) {
                std::cerr << "Failed to open log file" << std::endl;
                return;
            }

            // Show last N lines first
            file.seekg(0, std::ios::end);
            std::streampos fileSize = file.tellg();
            std::vector<std::string> lastLines;

            // Read backwards to find last N lines
            std::string line;
            std::streamoff fileSizeOff = static_cast<std::streamoff>(fileSize);
            std::streamoff pos = fileSizeOff;
            int linesFound = 0;
            while (pos > 0 && linesFound < logLines_) {
                pos--;
                file.seekg(pos);
                char c;
                file.get(c);
                if (c == '\n' && pos < fileSizeOff - 1) {
                    std::getline(file, line);
                    if (matchesLevel(line)) {
                        lastLines.push_back(line);
                        linesFound++;
                    }
                    file.seekg(pos);
                }
            }
            if (pos == 0) {
                file.seekg(0);
                std::getline(file, line);
                if (!line.empty() && matchesLevel(line)) {
                    lastLines.push_back(line);
                }
            }

            // Print in correct order
            for (auto it = lastLines.rbegin(); it != lastLines.rend(); ++it) {
                std::cout << *it << std::endl;
            }

            // Now follow
            file.seekg(0, std::ios::end);
            while (true) {
                while (std::getline(file, line)) {
                    if (matchesLevel(line)) {
                        std::cout << line << std::endl;
                    }
                }
                file.clear();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        } else {
            // Show last N lines
            std::ifstream file(logPath);
            if (!file) {
                std::cerr << "Failed to open log file: " << logPath.string() << std::endl;
                return;
            }

            // Read all matching lines into a deque, keeping last N
            std::deque<std::string> lines;
            std::string line;
            while (std::getline(file, line)) {
                if (matchesLevel(line)) {
                    lines.push_back(line);
                    if (static_cast<int>(lines.size()) > logLines_) {
                        lines.pop_front();
                    }
                }
            }

            for (const auto& l : lines) {
                std::cout << l << std::endl;
            }
        }
    }

    // ---- systemd service install/uninstall ----

    static bool isRootUser() {
#if defined(_WIN32)
        return false;
#else
        return geteuid() == 0;
#endif
    }

    std::string resolveDaemonBinaryForUnit() const {
        namespace fs = std::filesystem;
        if (!startDaemonBinary_.empty()) {
            return startDaemonBinary_;
        }
        // Same candidate resolution as startDaemon: look next to the running CLI binary first.
        std::string selfExe;
#if !defined(_WIN32)
        char buf[PATH_MAX] = {0};
        ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
        if (n > 0) {
            buf[n] = '\0';
            selfExe = buf;
        }
#endif
        if (!selfExe.empty()) {
            auto cliDir = fs::path(selfExe).parent_path();
#ifdef _WIN32
            std::vector<fs::path> candidates = {cliDir / "yams-daemon.exe",
                                                cliDir.parent_path() / "yams-daemon.exe"};
#else
            std::vector<fs::path> candidates = {
                cliDir / "yams-daemon", cliDir.parent_path() / "yams-daemon",
                cliDir.parent_path() / "daemon" / "yams-daemon",
                cliDir.parent_path().parent_path() / "daemon" / "yams-daemon"};
#endif
            for (const auto& p : candidates) {
                std::error_code ec;
                if (fs::exists(p, ec)) {
                    return p.string();
                }
            }
        }
#ifdef _WIN32
        return "yams-daemon.exe";
#else
        return "yams-daemon";
#endif
    }

    static std::string shellQuote(const std::string& value) {
        // Minimal single-quote escaping for the shell command we build below.
        std::string out;
        for (const char ch : value) {
            if (ch == '\'') {
                out += "'\\''";
            } else {
                out.push_back(ch);
            }
        }
        return "'" + out + "'";
    }

    static std::string systemctlPath() {
        return "systemctl"; // resolved through PATH by the shell
    }

    void installDaemonService() {
        namespace fs = std::filesystem;
        const bool userScope = installUserScope_ || !isRootUser();

        const std::string binPath = resolveDaemonBinaryForUnit();
        const std::string socketPath =
            socketPath_.empty() ? resolveConfiguredSocketPath() : socketPath_;
        const std::string dataDir = dataDir_.empty() ? cli_->getDataPath().string() : dataDir_;
        const std::string configPath = startConfigPath_;

        std::string home;
        if (const auto h = yams::config::getenv_nonempty("HOME")) {
            home = *h;
        }
        const fs::path unitDir =
            userScope ? (home.empty() ? fs::path(".") / ".config" / "systemd" / "user"
                                      : fs::path(home) / ".config" / "systemd" / "user")
                      : fs::path("/etc/systemd/system");
        const fs::path unitPath = unitDir / "yams-daemon.service";

        std::ostringstream unit;
        unit << "[Unit]\n"
             << "Description=YAMS daemon\n"
             << "After=network-online.target\n"
             << "Wants=network-online.target\n\n"
             << "[Service]\n"
             << "Type=simple\n";
        if (!configPath.empty()) {
            unit << "Environment=YAMS_CONFIG=" << configPath << "\n"
                 << "Environment=YAMS_CONFIG_PATH=" << configPath << "\n";
        }
        unit << "Environment=YAMS_DAEMON_SOCKET=" << socketPath << "\n"
             << "Environment=YAMS_DATA_DIR=" << dataDir << "\n"
             << "WorkingDirectory=" << dataDir << "\n"
             << "ExecStart=" << binPath << " --foreground --data-dir " << dataDir << " --socket "
             << socketPath;
        if (!configPath.empty()) {
            unit << " --config " << configPath;
        }
        unit << " --log-file " << dataDir << "/daemon.log --log-level info\n"
             << "Restart=on-failure\n"
             << "RestartSec=2\n";
        if (userScope) {
            unit << "NoNewPrivileges=true\n";
        } else {
            unit << "NoNewPrivileges=true\n"
                 << "PrivateTmp=true\n"
                 << "ProtectSystem=full\n"
                 << "ProtectHome=true\n"
                 << "ProtectKernelTunables=true\n"
                 << "ProtectControlGroups=true\n"
                 << "RestrictSUIDSGID=true\n";
        }
        unit << "\n[Install]\n"
             << "WantedBy=" << (userScope ? "default.target" : "multi-user.target") << "\n";

        std::error_code ec;
        fs::create_directories(unitDir, ec);
        if (ec) {
            std::cerr << "[FAIL] Cannot create unit directory " << unitDir.string() << ": "
                      << ec.message() << "\n";
            std::exit(1);
        }
        {
            std::ofstream out(unitPath, std::ios::trunc);
            if (!out) {
                std::cerr << "[FAIL] Cannot write " << unitPath.string() << "\n";
                std::exit(1);
            }
            out << unit.str();
        }
        std::cout << "[OK] Wrote " << unitPath.string() << "\n";

        const std::string ctl = systemctlPath();
        const std::string scope = userScope ? " --user" : "";
        const std::string reloadCmd = ctl + scope + " daemon-reload";
        const std::string enableCmd = ctl + scope + " enable yams-daemon.service";
        const std::string startCmd = ctl + scope + " start yams-daemon.service";
        std::cout << "[INFO] Running: " << reloadCmd << "\n";
        std::cout << "[INFO] Running: " << enableCmd << "\n";
        std::cout << "[INFO] Running: " << startCmd << "\n";
        (void)std::system((reloadCmd + " 2>&1").c_str());
        (void)std::system((enableCmd + " 2>&1").c_str());
        const int startRc = std::system((startCmd + " 2>&1").c_str());
        if (startRc == 0) {
            std::cout << "[OK] yams-daemon.service " << (userScope ? "(user)" : "(system)")
                      << " enabled and started.\n";
        } else {
            std::cout << "[WARN] systemctl start returned " << startRc
                      << "; review the unit with 'systemctl" << scope
                      << " status yams-daemon.service' (log file: " << dataDir << "/daemon.log)\n";
        }
    }

    void uninstallDaemonService() {
        namespace fs = std::filesystem;
        const bool userScope = installUserScope_ || !isRootUser();

        std::string home;
        if (const auto h = yams::config::getenv_nonempty("HOME")) {
            home = *h;
        }
        const fs::path unitDir =
            userScope ? (home.empty() ? fs::path(".") / ".config" / "systemd" / "user"
                                      : fs::path(home) / ".config" / "systemd" / "user")
                      : fs::path("/etc/systemd/system");
        const fs::path unitPath = unitDir / "yams-daemon.service";

        const std::string ctl = systemctlPath();
        const std::string scope = userScope ? " --user" : "";
        (void)std::system((ctl + scope + " stop yams-daemon.service 2>&1").c_str());
        (void)std::system((ctl + scope + " disable yams-daemon.service 2>&1").c_str());
        (void)std::system((ctl + scope + " daemon-reload 2>&1").c_str());
        std::error_code ec;
        if (fs::exists(unitPath, ec)) {
            fs::remove(unitPath, ec);
        }
        std::cout << (fs::exists(unitPath, ec) ? "[WARN] Could not remove " : "[OK] Removed ")
                  << unitPath.string() << "\n";
    }

    // Options (empty = auto-resolve based on environment)
    std::string socketPath_;
    std::string pidFile_;
    bool foreground_ = false;
    bool force_ = false;
    bool detailed_ = false;
    std::string dataDir_;
    // Start-subcommand-only options
    bool startForeground_ = false;
    bool startRestart_ = false;
    bool installUserScope_ = false; // yams daemon install/uninstall --user
    // --wait removed: start command no longer waits for readiness
    std::string startLogLevel_;
    std::string startConfigPath_;
    std::string startDaemonBinary_;
    // Log subcommand options
    int logLines_ = 50;
    bool logFollow_ = false;
    std::string logFilterLevel_;
    YamsCLI* cli_ = nullptr;
};

// Factory function
std::unique_ptr<ICommand> createDaemonCommand() {
    return std::make_unique<DaemonCommand>();
}

} // namespace yams::cli
