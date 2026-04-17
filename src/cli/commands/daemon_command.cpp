#include <nlohmann/json.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
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
#include <yams/daemon/daemon.h>
#include <yams/version.hpp>

#include <spdlog/spdlog.h>

#include <nlohmann/json.hpp>
using nlohmann::json;
#include <algorithm>
#include <cctype>
#include <cerrno>
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
#include <memory>
#include <optional>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <future>
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

// Windows implementation of setenv
inline int setenv(const char* name, const char* value, int overwrite) {
    int errcode = 0;
    if (!overwrite) {
        size_t envsize = 0;
        errcode = getenv_s(&envsize, NULL, 0, name);
        if (errcode || envsize)
            return errcode;
    }
    return _putenv_s(name, value);
}
#endif

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

        stop->callback([this]() { stopDaemon(); });

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
    }

    Result<void> execute() override {
        // This is called by the base command framework
        // The actual work is done in the callbacks above
        return Result<void>();
    }

private:
    // Use shared Severity enum from ui_helpers
    using Severity = yams::cli::ui::Severity;

    struct ReadinessDisplay {
        std::string key;
        std::string label;
        Severity severity{Severity::Good};
        std::string text;
        bool issue{false};
    };

    static std::string humanizeToken(const std::string& token) {
        // Special case mappings for known acronyms and technical terms
        static const std::map<std::string, std::string> specialCases = {
            {"cas", "CAS"}, {"dedup", "Dedup"}, {"ema", "EMA"}, {"ipc", "IPC"},
            {"fsm", "FSM"}, {"cpu", "CPU"},     {"db", "DB"},   {"sec", "sec"},
            {"ms", "ms"},   {"us", "µs"},       {"kb", "KB"},   {"mb", "MB"},
            {"gb", "GB"},   {"io", "I/O"},      {"api", "API"}, {"id", "ID"},
        };

        std::string out;
        out.reserve(token.size() + 10);
        std::string word;

        auto flushWord = [&]() {
            if (word.empty())
                return;
            // Check if this word is a special case
            std::string lower = word;
            std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
            auto it = specialCases.find(lower);
            if (it != specialCases.end()) {
                if (!out.empty() && out.back() != ' ')
                    out.push_back(' ');
                out += it->second;
            } else {
                // Normal word: capitalize first letter, lowercase rest
                if (!out.empty() && out.back() != ' ')
                    out.push_back(' ');
                for (size_t i = 0; i < word.size(); ++i) {
                    auto uch = static_cast<unsigned char>(word[i]);
                    out.push_back(
                        static_cast<char>(i == 0 ? std::toupper(uch) : std::tolower(uch)));
                }
            }
            word.clear();
        };

        for (char ch : token) {
            if (ch == '_' || ch == '-' || ch == '.') {
                flushWord();
                continue;
            }
            word.push_back(ch);
        }
        flushWord();

        return out;
    }

    static ReadinessDisplay classifyReadiness(const std::string& key, bool value) {
        ReadinessDisplay display;
        display.key = key;
        display.label = humanizeToken(key);

        const std::string lowerKey = [&]() {
            std::string s;
            s.reserve(key.size());
            for (char ch : key)
                s.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
            return s;
        }();

        const bool isDegradedFlag = lowerKey.find("degraded") != std::string::npos ||
                                    lowerKey.find("error") != std::string::npos ||
                                    lowerKey.find("failed") != std::string::npos;
        const bool isAvailabilityFlag = lowerKey.find("ready") != std::string::npos ||
                                        lowerKey.find("available") != std::string::npos ||
                                        lowerKey.find("enabled") != std::string::npos ||
                                        lowerKey.find("initialized") != std::string::npos;

        if (isDegradedFlag) {
            if (value) {
                display.severity = Severity::Warn;
                display.text = "Degraded";
                display.issue = true;
            } else {
                display.severity = Severity::Good;
                display.text = "Healthy";
            }
            return display;
        }

        if (value) {
            display.severity = Severity::Good;
            display.text = isAvailabilityFlag ? "Ready" : "Active";
        } else {
            display.severity = Severity::Warn;
            display.text = isAvailabilityFlag ? "Waiting" : "Unavailable";
            display.issue = true;
        }

        return display;
    }

    static std::filesystem::path normalizePath(std::filesystem::path path) {
        std::error_code ec;
        auto canonical = std::filesystem::weakly_canonical(path, ec);
        return ec ? path.lexically_normal() : canonical;
    }

    static bool isEphemeralDataDir(const std::filesystem::path& path) {
        if (path.empty())
            return false;

        const auto normalized = normalizePath(path);
        std::error_code ec;
        const auto tmpRoot = std::filesystem::temp_directory_path(ec);
        if (!ec) {
            const auto normalizedTmp = normalizePath(tmpRoot);
            const auto rel = normalized.lexically_relative(normalizedTmp);
            if (rel.empty() || rel == "." || (!rel.empty() && *rel.begin() != "..")) {
                return true;
            }
        }

        const std::string generic = normalized.generic_string();
        return generic == "/tmp" || generic.rfind("/tmp/", 0) == 0 || generic == "/private/tmp" ||
               generic.rfind("/private/tmp/", 0) == 0;
    }

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

    static std::optional<std::filesystem::path>
    daemonDataDirFromStatus(const yams::daemon::StatusResponse& status) {
        if (status.contentStoreRoot.empty())
            return std::nullopt;

        std::filesystem::path contentRoot(status.contentStoreRoot);
        if (!contentRoot.empty() && contentRoot.filename() == "storage")
            return contentRoot.parent_path();
        return contentRoot;
    }

    // Shared helper: paint a status value with severity icon and color
    // Delegates to ui::severity_text for consistency
    static std::string paintStatus(Severity sev, std::string text) {
        return yams::cli::ui::severity_text(sev, text, true);
    }

    // Shared helper: neutral text (no severity icon)
    static std::string neutralText(const std::string& text) {
        using namespace yams::cli::ui;
        return colorize(text, Ansi::WHITE);
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
            if (!parsed.is_discarded() && parsed.is_object()) {
                return static_cast<pid_t>(parsed.value("pid", -1));
            }
        }

        return static_cast<pid_t>(std::atoi(content.c_str()));
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
            if (pid <= 0 && fallbackPid > 0) {
                pid = fallbackPid;
            }

            bool processGone = (pid <= 0) || (kill(pid, 0) != 0);
            bool socketFilePresent = !socketPath.empty() && safe_exists(socketFsPath);
            bool proxySocketPresent =
                !socketPath.empty() && safe_exists(deriveProxySocketPath(socketFsPath));
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

    std::filesystem::path deriveProxySocketPath(const std::filesystem::path& daemonSocket) {
        if (daemonSocket.empty()) {
            return {};
        }
        auto base = daemonSocket.stem().string();
        if (base.empty()) {
            base = daemonSocket.filename().string();
        }
        if (base.empty()) {
            base = "yams-daemon";
        }
        return daemonSocket.parent_path() / (base + ".proxy.sock");
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
            removeWithRetry(deriveProxySocketPath(std::filesystem::path{socketPath}),
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
        char buffer[256];
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

            if (seen.insert(pid).second) {
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

            if (seen.insert(pid).second) {
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
                if (pid > 0) {
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
            stopDaemon();

            if (!waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(5),
                                   pidBeforeStop)) {
                std::cerr << "Failed to stop running daemon; not starting a new one.\n";
                return;
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
            } else if (const char* daemonBin = std::getenv("YAMS_DAEMON_BIN");
                       daemonBin && *daemonBin) {
                exePath = daemonBin;
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
                char buf[4096];
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

            // Pass storage directory via env for the daemon
            if (!dataDir_.empty()) {
                setenv("YAMS_STORAGE", dataDir_.c_str(), 1);
            } else if (cli_) {
                auto p = cli_->getDataPath();
                if (!p.empty())
                    setenv("YAMS_STORAGE", p.string().c_str(), 1);
            }
            // Pass log level via env too (daemon may read it)
            if (!startLogLevel_.empty()) {
                setenv("YAMS_LOG_LEVEL", startLogLevel_.c_str(), 1);
            }

            // Build argv using stable storage first, then create the char* array
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
            // Only set socketPath if explicitly provided by the user
            if (!socketPath_.empty()) {
                config.socketPath = socketPath_;
            }
            // Prefer explicit start option; otherwise use global CLI data path
            if (!dataDir_.empty()) {
                config.dataDir = dataDir_;
            } else if (cli_) {
                config.dataDir = cli_->getDataPath();
            }

            // Optional: pass debug overrides via environment for the child process
            if (!startLogLevel_.empty()) {
                setenv("YAMS_LOG_LEVEL", startLogLevel_.c_str(), 1);
            }
            if (!startDaemonBinary_.empty()) {
                setenv("YAMS_DAEMON_BIN", startDaemonBinary_.c_str(), 1);
            }
            if (!startConfigPath_.empty()) {
                setenv("YAMS_CONFIG", startConfigPath_.c_str(), 1);
            }

            // Best-effort cleanup if no daemon appears to be running
            if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                cleanupDaemonFiles(effectiveSocket, pidFile_);
            }
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

    void stopDaemon() {
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
        pid_t initialPid = readPidFromFile(pidFile_);
        const auto liveSocketPids = collectDaemonPidsForSocket(effectiveSocket);

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
                // PID file may be missing while data-dir lock still points to an alive daemon.
                pid_t lockPid = -1;
                std::filesystem::path lockFile;
                try {
                    std::filesystem::path dataDir;
                    if (!dataDir_.empty()) {
                        dataDir = std::filesystem::path(dataDir_);
                    } else if (cli_) {
                        auto cliData = cli_->getDataPath();
                        if (!cliData.empty()) {
                            dataDir = cliData;
                        }
                    }
                    if (dataDir.empty()) {
                        dataDir = yams::config::resolve_data_dir_from_config();
                    }
                    lockFile = dataDir / ".yams-lock";
                    if (safe_exists(lockFile)) {
                        std::ifstream in(lockFile);
                        std::string content;
                        std::getline(in, content, '\0');
                        auto parsed = json::parse(content, nullptr, false);
                        if (!parsed.is_discarded() && parsed.is_object()) {
                            lockPid = static_cast<pid_t>(parsed.value("pid", -1));
                        }
                    }
                } catch (...) {
                }

                if (lockPid > 0 && lockPid != getpid() && kill(lockPid, 0) == 0) {
                    auto desc = describeProcess(lockPid);
                    const bool looksLikeDaemon =
                        !desc.empty() && desc.find("yams-daemon") != std::string::npos;
                    if (looksLikeDaemon) {
                        spdlog::debug("Found lock-holder daemon process (PID {}) with missing "
                                      "socket/PID file; attempting termination",
                                      lockPid);
                        if (killDaemonByPid(lockPid, force_)) {
                            cleanupDaemonFiles(effectiveSocket, pidFile_);
                            stopSpinner();
                            std::cout << "[OK] YAMS daemon stopped successfully\n";
                            return;
                        }
                    } else {
                        spdlog::warn(
                            "Lock file {} references PID {} that does not look like yams-daemon; "
                            "not terminating",
                            lockFile.string(), lockPid);
                    }
                }

                spdlog::info("YAMS daemon is not running");
                cleanupDaemonFiles(effectiveSocket, pidFile_);
                stopSpinner();
                return;
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
            pid_t pid = readPidFromFile(pidFile_);
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
                stopped = waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(8),
                                            initialPid);
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
                stopped = waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(8),
                                            initialPid);
                if (stopped) {
                    spdlog::info("Daemon stopped successfully after shutdown disconnect");
                }
            }
        }

        // If socket shutdown failed, try PID-based termination
        if (!stopped) {
            pid_t pid = readPidFromFile(pidFile_);
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

#ifndef _WIN32
            // Unix: use pkill to find and kill yams-daemon processes with our socket
            // Use SIGKILL (-9) when the daemon is unresponsive or --force is set.
            std::string signal = shouldForceOrphanedStop ? "-9 " : "";
            std::string pkillCmd = "pkill " + signal + "-f 'yams-daemon.*" + effectiveSocket + "'";
            int pkillResult = std::system(pkillCmd.c_str());

            if (pkillResult == 0) {
                spdlog::info("Successfully killed orphaned daemon process");
                // Wait a moment for process to die
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                if (!isDaemonProcessRunningForSocket(effectiveSocket)) {
                    stopped = true;
                } else {
                    spdlog::debug("Daemon process still running after pkill (socket match)");
                }
            } else {
                // Try a more general pkill if specific socket match fails
                pkillCmd = "pkill " + signal + "-f 'yams-daemon'";
                pkillResult = std::system(pkillCmd.c_str());
                if (pkillResult == 0) {
                    spdlog::info("Killed yams-daemon process(es)");
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    if (!isDaemonProcessRunningForSocket(effectiveSocket)) {
                        stopped = true;
                    } else {
                        spdlog::debug("Daemon process still running after pkill");
                    }
                }
            }
#else
            // Windows: fall back to taskkill to avoid pkill dependency
            pid_t pid = readPidFromFile(pidFile_);
            std::string taskkillCmd;
            if (pid > 0) {
                taskkillCmd = "taskkill /PID " + std::to_string(pid) + " /T /F";
            } else {
                taskkillCmd = "taskkill /IM yams-daemon.exe /T /F";
            }

            int tkResult = std::system(taskkillCmd.c_str());
            if (tkResult == 0) {
                spdlog::info("Killed daemon process via taskkill");
                stopped = true;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } else {
                spdlog::warn("taskkill failed (exit={}): command='{}'", tkResult, taskkillCmd);
            }
#endif
        }

        // Final verification: check if daemon is actually still running
        // This catches cases where the daemon stopped between our attempts
#ifndef _WIN32
        if (!stopped && !isDaemonProcessRunningForSocket(effectiveSocket)) {
            spdlog::debug("Daemon not running after stop attempts - treating as success");
            stopped = true;
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
            std::exit(1);
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
            setenv("YAMS_CLIENT_DEBUG", "1", 1);
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
                            auto rt =
                                daemon::YamsDaemon::getXDGRuntimeDir() / "yams-daemon.status.json";
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
        if (pid > 0 && kill(pid, 0) == 0) {
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
                if (kill(fallbackPid, 0) == 0) {
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

    void showStatus() {
        pidFile_ = resolveConfiguredPidFilePath();

        const std::string configuredSocket = resolveConfiguredSocketPath();
        const std::string effectiveSocket =
            resolveSocketPathForLiveDaemon(configuredSocket, pidFile_, socketPath_.empty());

        if (detailed_) {
            // Enable client debug logging for ping/connect path
            setenv("YAMS_CLIENT_DEBUG", "1", 1);
        }

        // Check if daemon is running (prefer socket; fall back to PID)
        if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
            // Try PID-based detection to distinguish "starting" vs "not running"
            pid_t pid = readPidFromFile(pidFile_);
#ifndef _WIN32
            if (pid > 0 && kill(pid, 0) == 0) {
                std::cout << "YAMS daemon is starting/initializing (IPC not yet available)\n";
                if (effectiveSocket != configuredSocket) {
                    std::cout << "Configured socket: " << configuredSocket << "\n";
                    std::cout << "Live daemon socket: " << effectiveSocket << "\n";
                }
                if (detailed_) {
                    try {
                        auto logPath = daemon::YamsDaemon::resolveSystemPath(
                                           daemon::YamsDaemon::PathType::LogFile)
                                           .string();
                        std::cout << "[INFO] Likely waiting on search stack (index, models).\n";
                        std::cout << "[INFO] Tail log for details: " << logPath << "\n";
                        // Try bootstrap status file for early readiness
                        try {
                            auto rt =
                                daemon::YamsDaemon::getXDGRuntimeDir() / "yams-daemon.status.json";
                            std::ifstream bf(rt);
                            if (bf) {
                                json j;
                                bf >> j;
                                if (j.contains("readiness")) {
                                    std::vector<std::string> waiting;
                                    for (auto it = j["readiness"].begin();
                                         it != j["readiness"].end(); ++it) {
                                        if (!it.value().get<bool>()) {
                                            std::ostringstream w;
                                            w << it.key();
                                            if (j.contains("progress") &&
                                                j["progress"].contains(it.key())) {
                                                try {
                                                    w << " (" << j["progress"][it.key()].get<int>()
                                                      << "%)";
                                                } catch (...) {
                                                }
                                            }
                                            if (j.contains("uptime_seconds")) {
                                                try {
                                                    w << ", elapsed ~"
                                                      << j["uptime_seconds"].get<long>() << "s";
                                                } catch (...) {
                                                }
                                            }
                                            waiting.push_back(w.str());
                                        }
                                    }
                                    if (!waiting.empty()) {
                                        std::cout << "[INFO] Waiting on: ";
                                        for (size_t i = 0; i < waiting.size() && i < 3; ++i) {
                                            if (i)
                                                std::cout << ", ";
                                            std::cout << waiting[i];
                                        }
                                        if (waiting.size() > 3)
                                            std::cout << ", …";
                                        std::cout << "\n";
                                    }
                                    if (j.contains("top_slowest") && j["top_slowest"].is_array() &&
                                        !j["top_slowest"].empty()) {
                                        std::cout << "[INFO] Top slow components: ";
                                        auto arr = j["top_slowest"];
                                        for (size_t i = 0; i < arr.size() && i < 3; ++i) {
                                            const auto& e = arr[i];
                                            std::string name =
                                                e.value("name", std::string{"unknown"});
                                            uint64_t ms = e.value("elapsed_ms", 0ULL);
                                            if (i)
                                                std::cout << ", ";
                                            std::cout << name << " (" << ms << "ms)";
                                        }
                                        if (arr.size() > 3)
                                            std::cout << ", …";
                                        std::cout << "\n";
                                    }
                                }
                            }
                        } catch (...) {
                        }
                    } catch (...) {
                    }
                }
                return;
            }
            // Do not guess by scanning /proc — if socket is down and no PID file, report not
            // running.
#endif
            std::cout << "YAMS daemon is not running\n";
            if (effectiveSocket != configuredSocket) {
                std::cout << "Configured socket: " << configuredSocket << "\n";
                std::cout << "Last live daemon socket: " << effectiveSocket << "\n";
            }
            return;
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
            auto sres = runDaemonClient(
                cfg, [](yams::daemon::DaemonClient& client) { return client.status(); },
                std::chrono::seconds(5));
            if (spinner) {
                spinner->stop();
            }
            if (!sres) {
                // IPC failed — the daemon is likely still initializing (VectorDB, model
                // loading, etc.) and can't serve requests yet. Fall back to the bootstrap
                // status file that the daemon writes throughout initialization so the
                // user sees useful progress instead of an opaque error.
                try {
                    auto rt = daemon::YamsDaemon::getXDGRuntimeDir() / "yams-daemon.status.json";
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
                        if (j.contains("uptime_seconds")) {
                            try {
                                auto elapsed = j["uptime_seconds"].get<long>();
                                std::cout << "  Uptime: ~" << elapsed << "s\n";
                            } catch (...) {
                            }
                        }
                        std::cout << "  Hint: Run 'yams daemon status -d' once ready, "
                                     "or tail the daemon log.\n";
                        return;
                    }
                } catch (...) {
                }
                // No bootstrap file available either — fall back to the original message
                std::cout << "YAMS daemon status unavailable (IPC error)\n";
                return;
            }

            using namespace yams::cli::ui;
            const auto& s = sres.value();

            // Determine lifecycle state and severity
            std::string lifecycle = !s.lifecycleState.empty()
                                        ? humanizeToken(s.lifecycleState)
                                        : (!s.overallStatus.empty()
                                               ? humanizeToken(s.overallStatus)
                                               : (s.ready ? std::string("Ready")
                                                          : (s.running ? std::string("Initializing")
                                                                       : std::string("Stopped"))));

            Severity stateSeverity = Severity::Good;
            if (!s.running) {
                stateSeverity = Severity::Bad;
            } else if (!s.ready) {
                stateSeverity = Severity::Warn;
            }
            if (!s.lastError.empty())
                stateSeverity = Severity::Bad;

            // Collect issues for quick summary (skip informational flags)
            std::vector<std::string> issues;
            auto suppressDerivedIssue = [&](const std::string& lowerKey) {
                if (lowerKey == "topology_artifacts_fresh" ||
                    lowerKey == "topology artifacts fresh" ||
                    lowerKey == "topology_rebuild_running" ||
                    lowerKey == "topology rebuild running" || lowerKey == "vector_index" ||
                    lowerKey == "vector index") {
                    return true;
                }
                return false;
            };
            for (const auto& [key, ready] : s.readinessStates) {
                if (!ready) {
                    std::string lowerKey = key;
                    std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(), ::tolower);
                    // Skip informational keys: build_reason, *_degraded flags
                    if (lowerKey.find("build_reason") != std::string::npos ||
                        lowerKey.find("build reason") != std::string::npos ||
                        lowerKey.ends_with("_degraded")) {
                        continue;
                    }
                    if ((lowerKey == "vector_db" || lowerKey == "vector db") &&
                        s.vectorDbInitAttempted) {
                        continue;
                    }
                    if (lowerKey == "vector_db_ready" || lowerKey == "vector db ready" ||
                        lowerKey == "vector_db_init_attempted" ||
                        lowerKey == "vector db init attempted") {
                        continue;
                    }
                    if (suppressDerivedIssue(lowerKey)) {
                        continue;
                    }
                    auto rd = classifyReadiness(key, ready);
                    auto pit = s.initProgress.find(key);
                    if (pit != s.initProgress.end() && pit->second < 100) {
                        rd.label += " (" + std::to_string(static_cast<int>(pit->second)) + "%)";
                    }
                    issues.push_back(rd.label);
                }
            }
            std::sort(issues.begin(), issues.end());
            issues.erase(std::unique(issues.begin(), issues.end()), issues.end());

            const auto daemonDataDir = daemonDataDirFromStatus(s);
            const auto configuredDataDir = expectedDataDir();
            const bool hasDaemonDataDir = daemonDataDir.has_value() && !daemonDataDir->empty();
            const bool dataDirMismatch =
                hasDaemonDataDir && !configuredDataDir.empty() &&
                normalizePath(*daemonDataDir) != normalizePath(configuredDataDir);
            const bool daemonUsesEphemeralData =
                hasDaemonDataDir && isEphemeralDataDir(*daemonDataDir);
            const auto searchMetrics = effectiveSearchMetrics(s);

            std::cout << title_banner("YAMS Daemon") << "\n\n";

            // Single overview table with essential info
            std::vector<Row> overview;
            overview.push_back({"State", paintStatus(stateSeverity, lifecycle),
                                s.version.empty() ? "" : "v" + s.version});
            overview.push_back({"Uptime", format_duration(s.uptimeSeconds),
                                std::to_string(s.requestsProcessed) + " requests"});

            // Memory: show governor if available, else basic RSS
            if (s.governorBudgetBytes > 0) {
                const char* levelNames[] = {"Normal", "Warning", "Critical", "Emergency"};
                uint8_t lvl = std::min(s.governorPressureLevel, static_cast<uint8_t>(3));
                Severity pressSev = (lvl == 0)   ? Severity::Good
                                    : (lvl == 1) ? Severity::Warn
                                                 : Severity::Bad;
                std::ostringstream memInfo;
                memInfo << std::fixed << std::setprecision(0)
                        << (static_cast<double>(s.governorRssBytes) / (1024 * 1024)) << " / "
                        << (static_cast<double>(s.governorBudgetBytes) / (1024 * 1024)) << " MB";
                overview.push_back(
                    {"Memory", paintStatus(pressSev, levelNames[lvl]), memInfo.str()});
            } else {
                Severity memSev = s.memoryUsageMb > 4096 ? Severity::Warn : Severity::Good;
                overview.push_back(
                    {"Memory",
                     paintStatus(memSev, std::to_string(static_cast<int>(s.memoryUsageMb)) + " MB"),
                     ""});
            }

            // Search summary
            Severity searchSev = searchMetrics.queued > 50   ? Severity::Bad
                                 : searchMetrics.queued > 10 ? Severity::Warn
                                                             : Severity::Good;
            std::ostringstream searchInfo;
            searchInfo << searchMetrics.active << " active · " << searchMetrics.queued << " queued";
            overview.push_back({"Search", paintStatus(searchSev, searchInfo.str()), ""});

            // Embeddings summary
            Severity embSev = s.embeddingAvailable ? Severity::Good : Severity::Warn;
            std::string embText = s.embeddingAvailable ? "Available" : "Unavailable";
            std::string embExtra = s.embeddingModel.empty() ? "" : s.embeddingModel;
            overview.push_back({"Embeddings", paintStatus(embSev, embText), embExtra});

            // Repair summary
            auto findCompactCount = [&](const char* key) -> uint64_t {
                auto it = s.requestCounts.find(key);
                return it != s.requestCounts.end() ? it->second : 0ULL;
            };
            const bool repairRunning = findCompactCount("repair_running") > 0;
            const bool repairInProgress = findCompactCount("repair_in_progress") > 0;
            const uint64_t repairQueue = findCompactCount("repair_queue_depth");
            const uint64_t repairFailed = findCompactCount("repair_failed_operations");
            Severity repairSev = !repairRunning       ? Severity::Warn
                                 : (repairFailed > 0) ? Severity::Warn
                                 : (repairInProgress) ? Severity::Warn
                                 : (repairQueue > 0)  ? Severity::Warn
                                                      : Severity::Good;
            std::ostringstream repairText;
            repairText << (repairRunning ? "Running" : "Stopped");
            if (repairInProgress) {
                repairText << " · RPC active";
            }
            std::ostringstream repairExtra;
            repairExtra << repairQueue << " pending";
            if (repairFailed > 0) {
                repairExtra << " · " << repairFailed << " failed";
            }
            overview.push_back(
                {"Repair", paintStatus(repairSev, repairText.str()), repairExtra.str()});

            render_rows(std::cout, overview);

            std::vector<std::string> dataDirWarnings;
            if (daemonUsesEphemeralData) {
                dataDirWarnings.push_back("Daemon is serving a temporary data directory");
            }
            if (dataDirMismatch) {
                dataDirWarnings.push_back("Daemon data dir differs from current CLI/config");
            }
            if (!dataDirWarnings.empty()) {
                std::string joined;
                for (std::size_t i = 0; i < dataDirWarnings.size(); ++i) {
                    if (i) {
                        joined += " · ";
                    }
                    joined += dataDirWarnings[i];
                }
                std::cout << "\n"
                          << colorize("• Data dir warning: " + joined, Ansi::YELLOW) << "\n";
            }

            // Show issues if any
            if (!issues.empty()) {
                std::string joined;
                const std::size_t limit = std::min<std::size_t>(issues.size(), 4);
                for (std::size_t i = 0; i < limit; ++i) {
                    if (i)
                        joined += ", ";
                    joined += issues[i];
                }
                if (issues.size() > limit)
                    joined += ", …";
                std::cout << "\n" << colorize("• Waiting on: " + joined, Ansi::YELLOW) << "\n";
            }

            // Show last error if any
            if (!s.lastError.empty()) {
                std::cout << "\n" << colorize("✗ Error: " + s.lastError, Ansi::RED) << "\n";
            }

            std::cout << "\n"
                      << colorize("→ Use 'yams daemon status -d' for detailed diagnostics",
                                  Ansi::DIM)
                      << "\n";
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
            setenv("YAMS_CLIENT_DEBUG", detailed_ ? "1" : "0", 1);
            yams::daemon::ClientConfig cfg;
            cfg.socketPath = effectiveSocket;
            auto statusResult = runDaemonClient(
                cfg, [](yams::daemon::DaemonClient& client) { return client.status(true); },
                std::chrono::seconds(5));
            if (statusResult) {
                if (spinner) {
                    spinner->stop();
                }
                using namespace yams::cli::ui;
                const auto& status = statusResult.value();

                std::vector<ReadinessDisplay> readinessList;
                readinessList.reserve(status.readinessStates.size());
                for (const auto& [key, ready] : status.readinessStates) {
                    readinessList.push_back(classifyReadiness(key, ready));
                }

                std::vector<std::string> waiting;
                waiting.reserve(readinessList.size());
                auto suppressDerivedWaiting = [&](const std::string& lowerKey) {
                    if ((lowerKey == "embedding ready" || lowerKey == "embedding_ready") &&
                        (status.embeddingAvailable ||
                         (status.readinessStates.contains("model_provider") &&
                          status.readinessStates.at("model_provider")))) {
                        return true;
                    }
                    if ((lowerKey == "plugins ready" || lowerKey == "plugins_ready") &&
                        (status.readinessStates.contains("plugins") &&
                         status.readinessStates.at("plugins"))) {
                        return true;
                    }
                    if (lowerKey == "topology artifacts fresh" ||
                        lowerKey == "topology_artifacts_fresh" ||
                        lowerKey == "topology rebuild running" ||
                        lowerKey == "topology_rebuild_running" || lowerKey == "vector index" ||
                        lowerKey == "vector_index") {
                        return true;
                    }
                    return false;
                };
                for (const auto& rd : readinessList) {
                    if (rd.issue) {
                        // Skip "build reason" keys - they're informational, not readiness
                        // indicators
                        std::string lowerLabel = rd.label;
                        std::transform(lowerLabel.begin(), lowerLabel.end(), lowerLabel.begin(),
                                       ::tolower);
                        const bool skipReadinessLabel =
                            lowerLabel.find("build reason") != std::string::npos ||
                            lowerLabel == "vector db ready" ||
                            lowerLabel == "vector db init attempted" ||
                            (rd.key == "vector_db" && status.vectorDbInitAttempted) ||
                            suppressDerivedWaiting(lowerLabel);
                        if (!skipReadinessLabel)
                            waiting.push_back(rd.label);
                    }
                }
                std::sort(waiting.begin(), waiting.end());
                waiting.erase(std::unique(waiting.begin(), waiting.end()), waiting.end());

                const auto daemonDataDir = daemonDataDirFromStatus(status);
                const auto configuredDataDir = expectedDataDir();
                const bool hasDaemonDataDir = daemonDataDir.has_value() && !daemonDataDir->empty();
                const bool dataDirMismatch =
                    hasDaemonDataDir && !configuredDataDir.empty() &&
                    normalizePath(*daemonDataDir) != normalizePath(configuredDataDir);
                const bool daemonUsesEphemeralData =
                    hasDaemonDataDir && isEphemeralDataDir(*daemonDataDir);

                std::string lifecycle = !status.lifecycleState.empty()
                                            ? humanizeToken(status.lifecycleState)
                                            : (!status.overallStatus.empty()
                                                   ? humanizeToken(status.overallStatus)
                                                   : (status.ready ? std::string{"Ready"}
                                                                   : std::string{"Initializing"}));

                Severity stateSeverity = status.running
                                             ? (status.ready ? Severity::Good : Severity::Warn)
                                             : Severity::Bad;
                if (!status.lastError.empty())
                    stateSeverity = Severity::Bad;

                std::cout << title_banner("YAMS Daemon — Detailed") << "\n\n";

                std::vector<Row> overview;
                overview.push_back(
                    {"State", paintStatus(stateSeverity, lifecycle),
                     status.running ? (status.ready ? "ready" : "starting") : "stopped"});
                overview.push_back(
                    {"Version", status.version.empty() ? "unknown" : status.version, ""});
                overview.push_back({"Uptime", format_duration(status.uptimeSeconds), ""});
                overview.push_back(
                    {"Requests", std::to_string(status.requestsProcessed),
                     std::string{"connections: "} + std::to_string(status.activeConnections)});
                if (status.retryAfterMs > 0) {
                    overview.push_back(
                        {"Backpressure",
                         paintStatus(Severity::Warn,
                                     std::to_string(status.retryAfterMs) + " ms cooldown"),
                         ""});
                }
                if (!status.lastError.empty()) {
                    overview.push_back(
                        {"Last error", paintStatus(Severity::Bad, status.lastError), ""});
                }
                render_rows(std::cout, overview);

                std::cout << "\n" << section_header("Resources") << "\n\n";
                std::vector<Row> resourceRows;
                {
                    std::ostringstream cpu;
                    cpu << std::fixed << std::setprecision(1) << status.cpuUsagePercent << "%";
                    Severity cpuSeverity =
                        status.cpuUsagePercent >= 95.0
                            ? Severity::Bad
                            : (status.cpuUsagePercent >= 80.0 ? Severity::Warn : Severity::Good);
                    resourceRows.push_back({"CPU", paintStatus(cpuSeverity, cpu.str()), ""});
                }
                resourceRows.push_back(
                    {"Memory",
                     paintStatus(status.memoryUsageMb > 4096 ? Severity::Warn : Severity::Good,
                                 std::to_string(static_cast<int>(status.memoryUsageMb)) + " MB"),
                     ""});
                resourceRows.push_back({"Pools",
                                        std::to_string(status.ipcPoolSize) + " ipc · " +
                                            std::to_string(status.ioPoolSize) + " io",
                                        ""});

                std::size_t threads = 0, activeJobs = 0, queuedJobs = 0;
                if (auto it = status.requestCounts.find("worker_threads");
                    it != status.requestCounts.end())
                    threads = it->second;
                if (auto it = status.requestCounts.find("worker_active");
                    it != status.requestCounts.end())
                    activeJobs = it->second;
                if (auto it = status.requestCounts.find("worker_queued");
                    it != status.requestCounts.end())
                    queuedJobs = it->second;

                std::size_t workingThreads = activeJobs;
                std::size_t sleepingThreads =
                    (threads > workingThreads) ? (threads - workingThreads) : 0;
                std::size_t util =
                    threads ? static_cast<std::size_t>((100.0 * workingThreads) / threads) : 0;
                double workerFraction =
                    threads > 0 ? static_cast<double>(workingThreads) / threads : 0.0;

                std::ostringstream workerVal;
                workerVal << progress_bar(workerFraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW,
                                          Ansi::RED, true)
                          << " " << threads << " threads · " << workingThreads << " working"
                          << " · " << sleepingThreads << " sleeping · " << activeJobs
                          << " jobs active";
                if (queuedJobs > 0)
                    workerVal << " · " << queuedJobs << " queued";

                Severity workerSeverity =
                    util >= 95 ? Severity::Bad : (util >= 85 ? Severity::Warn : Severity::Good);
                resourceRows.push_back(
                    {"Workers", paintStatus(workerSeverity, workerVal.str()), ""});
                render_rows(std::cout, resourceRows);

                // Resource Governor section (memory pressure management)
                if (status.governorBudgetBytes > 0) {
                    std::cout << "\n" << section_header("Resource Governor") << "\n\n";
                    std::vector<Row> governor;

                    // Memory pressure level with progress bar
                    const char* levelNames[] = {"Normal", "Warning", "Critical", "Emergency"};
                    uint8_t lvl = std::min(status.governorPressureLevel, static_cast<uint8_t>(3));
                    Severity pressSev = (lvl == 0)   ? Severity::Good
                                        : (lvl == 1) ? Severity::Warn
                                                     : Severity::Bad;
                    double memFraction = status.governorBudgetBytes > 0
                                             ? static_cast<double>(status.governorRssBytes) /
                                                   status.governorBudgetBytes
                                             : 0.0;
                    uint64_t memMb = status.governorRssBytes / (1024 * 1024);
                    uint64_t budgetMb = status.governorBudgetBytes / (1024 * 1024);
                    std::ostringstream memBar;
                    memBar << progress_bar(memFraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW,
                                           Ansi::RED, true)
                           << " " << static_cast<int>(memFraction * 100) << "% (" << memMb << "/"
                           << budgetMb << " MB)";
                    std::string statusIndicator = (lvl == 0) ? " ✓ " : (lvl == 1) ? " ⚠ " : " ✗ ";
                    governor.push_back(
                        {"Memory",
                         paintStatus(pressSev, memBar.str() + statusIndicator + levelNames[lvl]),
                         ""});

                    // Scaling headroom
                    Severity headroomSev = (status.governorHeadroomPct >= 50)   ? Severity::Good
                                           : (status.governorHeadroomPct >= 20) ? Severity::Warn
                                                                                : Severity::Bad;
                    governor.push_back(
                        {"Scaling Headroom",
                         paintStatus(headroomSev, std::to_string(status.governorHeadroomPct) + "%"),
                         ""});

                    // ONNX concurrency
                    if (status.onnxTotalSlots > 0) {
                        std::ostringstream onnxInfo;
                        onnxInfo << status.onnxUsedSlots << " / " << status.onnxTotalSlots
                                 << " slots";
                        std::ostringstream onnxBreak;
                        onnxBreak << "gliner " << status.onnxGlinerUsed << " · embed "
                                  << status.onnxEmbedUsed << " · rerank "
                                  << status.onnxRerankerUsed;
                        Severity onnxSev =
                            (status.onnxUsedSlots >= status.onnxTotalSlots)      ? Severity::Warn
                            : (status.onnxUsedSlots > status.onnxTotalSlots / 2) ? Severity::Good
                                                                                 : Severity::Good;
                        governor.push_back({"ONNX Concurrency",
                                            paintStatus(onnxSev, onnxInfo.str()), onnxBreak.str()});
                    }

                    render_rows(std::cout, governor);
                }

                std::cout << "\n" << section_header("Transport") << "\n\n";
                std::vector<Row> transport;
                if (status.muxActiveHandlers || status.muxQueuedBytes ||
                    status.muxWriterBudgetBytes) {
                    double pressure = 0.0;
                    if (status.muxWriterBudgetBytes > 0) {
                        pressure = (100.0 * static_cast<double>(status.muxQueuedBytes)) /
                                   static_cast<double>(status.muxWriterBudgetBytes);
                        if (pressure < 0)
                            pressure = 0;
                    }
                    Severity muxSeverity =
                        pressure >= 75.0 ? Severity::Bad
                                         : (pressure >= 40.0 ? Severity::Warn : Severity::Good);
                    std::ostringstream muxVal;
                    muxVal << status.muxActiveHandlers << " handlers";
                    std::ostringstream muxExtra;
                    muxExtra << "queued " << status.muxQueuedBytes << " B";
                    if (status.muxWriterBudgetBytes > 0)
                        muxExtra << " · budget " << status.muxWriterBudgetBytes << " B";
                    transport.push_back(
                        {"Mux", paintStatus(muxSeverity, muxVal.str()), muxExtra.str()});
                    if (status.muxWriterBudgetBytes > 0) {
                        std::ostringstream pressureStr;
                        pressureStr << std::fixed << std::setprecision(1) << pressure << "%";
                        transport.push_back(
                            {"Mux pressure", paintStatus(muxSeverity, pressureStr.str()), ""});
                    }
                }
                if (status.fsmTransitions || status.fsmHeaderReads || status.fsmPayloadWrites ||
                    status.fsmPayloadReads || status.fsmBytesSent || status.fsmBytesReceived) {
                    std::ostringstream fsm;
                    fsm << status.fsmTransitions << " transitions";
                    std::ostringstream fsmExtra;
                    fsmExtra << "hdr " << status.fsmHeaderReads << " · read "
                             << status.fsmPayloadReads << " · write " << status.fsmPayloadWrites;
                    transport.push_back({"IPC FSM", fsm.str(), fsmExtra.str()});
                    std::ostringstream bytes;
                    bytes << "sent " << status.fsmBytesSent << " · recv "
                          << status.fsmBytesReceived;
                    transport.push_back({"IPC bytes", bytes.str(), ""});
                }
                // Proxy socket info
                if (!status.proxySocketPath.empty()) {
                    std::ostringstream proxyVal;
                    proxyVal << status.proxyActiveConnections << " active";
                    transport.push_back({"Proxy", proxyVal.str(), status.proxySocketPath});
                }
                if (!transport.empty())
                    render_rows(std::cout, transport);

                std::cout << "\n" << section_header("Search") << "\n\n";
                std::vector<Row> searchRows;
                const auto searchMetrics = effectiveSearchMetrics(status);
                std::ostringstream base;
                base << searchMetrics.active << " active · " << searchMetrics.queued << " queued";
                std::ostringstream extra;
                extra << "executed " << searchMetrics.executed << " · cache " << std::fixed
                      << std::setprecision(1) << (searchMetrics.cacheHitRate * 100.0)
                      << "% · latency " << searchMetrics.avgLatencyUs << "µs";
                Severity searchSeverity =
                    searchMetrics.queued > 50
                        ? Severity::Bad
                        : (searchMetrics.queued > 10 ? Severity::Warn : Severity::Good);
                searchRows.push_back(
                    {"Queries", paintStatus(searchSeverity, base.str()), extra.str()});
                if (searchMetrics.concurrencyLimit > 0) {
                    searchRows.push_back(
                        {"Concurrency", std::to_string(searchMetrics.concurrencyLimit), ""});
                }
                render_rows(std::cout, searchRows);

                // Post-Ingest Pipeline section
                std::cout << "\n" << section_header("Post-Ingest Pipeline") << "\n\n";
                std::vector<Row> postIngestRows;
                auto findPostIngestCount = [&](const char* key) -> uint64_t {
                    auto it = status.requestCounts.find(key);
                    return it != status.requestCounts.end() ? it->second : 0ULL;
                };
                {
                    uint64_t queued = findPostIngestCount("post_ingest_queued");
                    uint64_t inflight = findPostIngestCount("post_ingest_inflight");
                    uint64_t cap = findPostIngestCount("post_ingest_capacity");
                    uint64_t rpcQueued = findPostIngestCount("post_ingest_rpc_queued");
                    uint64_t rpcCap = findPostIngestCount("post_ingest_rpc_capacity");
                    uint64_t rpcMaxPerBatch = findPostIngestCount("post_ingest_rpc_max_per_batch");
                    uint64_t processed = findPostIngestCount("post_ingest_processed");
                    uint64_t failed = findPostIngestCount("post_ingest_failed");
                    uint64_t latency = findPostIngestCount("post_ingest_latency_ms_ema");
                    uint64_t rate = findPostIngestCount("post_ingest_rate_sec_ema");

                    // Queue progress bar
                    double queueFraction = cap > 0 ? static_cast<double>(queued) / cap : 0.0;
                    std::ostringstream queueBar;
                    queueBar << progress_bar(queueFraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW,
                                             Ansi::RED, true)
                             << " " << static_cast<int>(queueFraction * 100) << "% (" << queued
                             << "/" << cap << ")";
                    Severity qSev = queued > cap * 0.8
                                        ? Severity::Bad
                                        : (queued > cap * 0.5 ? Severity::Warn : Severity::Good);
                    postIngestRows.push_back({"Queue", paintStatus(qSev, queueBar.str()), ""});
                    if (inflight > 0) {
                        postIngestRows.push_back(
                            {"  Inflight", std::to_string(inflight) + " active", ""});
                    }

                    std::ostringstream throughput;
                    throughput << rate << "/s · " << latency << "ms latency";
                    postIngestRows.push_back({"Throughput", throughput.str(), ""});

                    if (rpcCap > 0) {
                        std::ostringstream rpc;
                        rpc << rpcQueued << "/" << rpcCap;
                        if (rpcMaxPerBatch > 0)
                            rpc << " · max/batch " << rpcMaxPerBatch;
                        postIngestRows.push_back({"RPC Queue", rpc.str(), ""});
                    }

                    std::ostringstream stats;
                    stats << processed << " processed";
                    if (failed > 0)
                        stats << " · " << failed << " failed";
                    Severity statSev = failed > 0 ? Severity::Warn : Severity::Good;
                    postIngestRows.push_back({"Stats", paintStatus(statSev, stats.str()), ""});

                    uint64_t watchEnabled = findPostIngestCount("watch_enabled");
                    uint64_t watchInterval = findPostIngestCount("watch_interval_ms");
                    if (watchEnabled > 0 || watchInterval > 0) {
                        std::ostringstream watchVal;
                        watchVal << (watchEnabled > 0 ? "enabled" : "disabled");
                        if (watchInterval > 0)
                            watchVal << " · " << watchInterval << "ms";
                        postIngestRows.push_back({"Watch", watchVal.str(), ""});
                    }

                    // Per-stage breakdown with progress bars
                    uint64_t extractInFlight = findPostIngestCount("extraction_inflight");
                    uint64_t kgInFlight = findPostIngestCount("kg_inflight");
                    uint64_t kgQueueDepth = findPostIngestCount("kg_queue_depth");
                    uint64_t enrichInFlight = findPostIngestCount("enrich_inflight");
                    uint64_t enrichQueueDepth = findPostIngestCount("enrich_queue_depth");
                    uint64_t enrichLimit =
                        std::max<uint64_t>(1, findPostIngestCount("post_enrich_limit"));
                    // Get dynamic concurrency limits (floor of 1 to prevent div-by-zero)
                    uint64_t extractLimit =
                        std::max<uint64_t>(1, findPostIngestCount("post_extraction_limit"));
                    uint64_t kgLimit = std::max<uint64_t>(1, findPostIngestCount("post_kg_limit"));

                    // Fetch audit metrics for all stages
                    uint64_t kgAuditQueued = findPostIngestCount("kg_queued");
                    uint64_t kgAuditConsumed = findPostIngestCount("kg_consumed");
                    uint64_t kgAuditDropped = findPostIngestCount("kg_dropped");
                    uint64_t enrichAuditQueued = findPostIngestCount("symbol_queued") +
                                                 findPostIngestCount("entity_queued") +
                                                 findPostIngestCount("title_queued");
                    uint64_t enrichAuditConsumed = findPostIngestCount("symbol_consumed") +
                                                   findPostIngestCount("entity_consumed") +
                                                   findPostIngestCount("title_consumed");
                    uint64_t enrichAuditDropped = findPostIngestCount("symbol_dropped") +
                                                  findPostIngestCount("entity_dropped") +
                                                  findPostIngestCount("title_dropped");

                    // Unified Pipeline Stages block
                    using yams::cli::detail::StageInfo;
                    StageInfo stages[] = {
                        {"Extraction", extractInFlight, 0, extractLimit, 0, 0, 0},
                        {"Knowledge Graph", kgInFlight, kgQueueDepth, kgLimit, kgAuditQueued,
                         kgAuditConsumed, kgAuditDropped},
                        {"Enrich", enrichInFlight, enrichQueueDepth, enrichLimit, enrichAuditQueued,
                         enrichAuditConsumed, enrichAuditDropped},
                    };

                    auto stageRows = yams::cli::detail::renderPipelineStages(stages, 3);
                    if (!stageRows.empty()) {
                        postIngestRows.push_back({"", "", ""}); // Separator
                        postIngestRows.push_back({subsection_header("Pipeline Stages"), "", ""});
                        postIngestRows.insert(postIngestRows.end(), stageRows.begin(),
                                              stageRows.end());
                    }
                }
                render_rows(std::cout, postIngestRows);

                // Internal Processing Metrics section (Stream, WorkCoordinator, InternalEventBus)
                std::cout << "\n" << section_header("Internal Processing") << "\n\n";
                std::vector<Row> internalRows;

                // WorkCoordinator metrics
                uint64_t workCoordThreads = findPostIngestCount("worker_threads");
                uint64_t workCoordActive = findPostIngestCount("worker_active");
                uint64_t workCoordRunning = findPostIngestCount("work_coordinator_running");
                uint64_t workCoordQueued = findPostIngestCount("worker_queued");

                if (workCoordRunning > 0 || workCoordThreads > 0) {
                    std::ostringstream workVal;
                    workVal << workCoordActive << "/" << workCoordThreads << " threads active";
                    if (workCoordQueued > 0)
                        workVal << " · " << workCoordQueued << " jobs queued";
                    internalRows.push_back({"WorkCoordinator", workVal.str(), ""});
                }

                // Stream metrics
                uint64_t streamTotal = findPostIngestCount("stream_total");
                uint64_t streamBatches = findPostIngestCount("stream_batches");
                uint64_t streamKeepalives = findPostIngestCount("stream_keepalives");
                if (streamTotal > 0 || streamBatches > 0) {
                    std::ostringstream streamVal;
                    streamVal << streamTotal << " streams";
                    if (streamBatches > 0) {
                        streamVal << " · " << streamBatches << " batches";
                    }
                    if (streamKeepalives > 0) {
                        streamVal << " · " << streamKeepalives << " keepalives";
                    }
                    internalRows.push_back({"Streams", streamVal.str(), ""});
                }

                if (!internalRows.empty()) {
                    render_rows(std::cout, internalRows);
                }

                std::cout << "\n" << section_header("Repair Service") << "\n\n";
                std::vector<Row> repairRows;
                const bool repairRunning = findPostIngestCount("repair_running") > 0;
                const bool repairInProgress = findPostIngestCount("repair_in_progress") > 0;
                const uint64_t repairQueue = findPostIngestCount("repair_queue_depth");
                const uint64_t repairBatches = findPostIngestCount("repair_batches_attempted");
                const uint64_t repairEmbeddings =
                    findPostIngestCount("repair_embeddings_generated");
                const uint64_t repairFailed = findPostIngestCount("repair_failed_operations");
                const uint64_t repairBacklog = findPostIngestCount("repair_total_backlog");
                const uint64_t repairProcessed = findPostIngestCount("repair_processed");

                std::string repairStatus = repairRunning ? "running" : "stopped";
                Severity repairStatusSev = repairRunning ? Severity::Good : Severity::Warn;
                if (repairInProgress) {
                    repairStatus += " · RPC active";
                    repairStatusSev = Severity::Warn;
                }
                repairRows.push_back({"Status", paintStatus(repairStatusSev, repairStatus), ""});

                if (repairBacklog > 0) {
                    const double fraction =
                        std::min(1.0, static_cast<double>(repairProcessed) / repairBacklog);
                    std::ostringstream progress;
                    progress << progress_bar(fraction, 12, "#", "░", Ansi::GREEN, Ansi::YELLOW,
                                             Ansi::RED, true)
                             << " " << repairProcessed << "/" << repairBacklog;
                    repairRows.push_back({"Progress", progress.str(), ""});
                }

                std::ostringstream queue;
                queue << repairQueue << " pending";
                repairRows.push_back({"Queue", queue.str(), ""});

                std::ostringstream repairStats;
                repairStats << repairBatches << " batches · " << repairEmbeddings << " embeddings";
                Severity repairStatsSev = repairFailed > 0 ? Severity::Warn : Severity::Good;
                if (repairFailed > 0) {
                    repairStats << " · " << repairFailed << " failed";
                }
                repairRows.push_back({"Stats", paintStatus(repairStatsSev, repairStats.str()), ""});

                render_rows(std::cout, repairRows);

                std::cout << "\n" << section_header("Storage & Embeddings") << "\n\n";
                std::vector<Row> storageRows;
                auto findCount = [&](const char* key) -> uint64_t {
                    auto it = status.requestCounts.find(key);
                    return it != status.requestCounts.end() ? it->second : 0ULL;
                };
                const uint64_t docs = findCount("documents_total");
                const uint64_t indexed = findCount("documents_indexed");
                if (docs > 0 || indexed > 0) {
                    std::ostringstream docsVal;
                    docsVal << docs << " docs";
                    if (indexed > 0)
                        docsVal << " · indexed " << indexed;
                    storageRows.push_back({"Documents", docsVal.str(), ""});
                }
                const uint64_t casObjects = findCount("storage_documents");
                if (casObjects > 0) {
                    std::ostringstream casVal;
                    casVal << casObjects << " objects";
                    storageRows.push_back({"CAS Objects", casVal.str(), ""});
                }
                const uint64_t logical = findCount("storage_logical_bytes");
                const uint64_t physical = findCount("physical_total_bytes");
                const uint64_t dedup = findCount("cas_dedup_saved_bytes");
                const uint64_t comp = findCount("cas_compress_saved_bytes");
                auto humanBytes = [](uint64_t b) {
                    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
                    auto val = static_cast<double>(b);
                    int idx = 0;
                    while (val >= 1024.0 && idx < 4) {
                        val /= 1024.0;
                        ++idx;
                    }
                    std::ostringstream oss;
                    oss << std::fixed << std::setprecision(val < 10 ? 1 : 0) << val << units[idx];
                    return oss.str();
                };
                if (logical > 0 || physical > 0) {
                    std::ostringstream size;
                    size << "logical " << humanBytes(logical);
                    if (physical > 0)
                        size << " · physical " << humanBytes(physical);
                    storageRows.push_back({"Storage", size.str(), ""});
                }
                if (dedup > 0 || comp > 0) {
                    std::ostringstream savings;
                    savings << "dedup " << humanBytes(dedup);
                    if (comp > 0)
                        savings << " · compress " << humanBytes(comp);
                    storageRows.push_back({"Savings", savings.str(), ""});
                }

                // Storage breakdown with overhead details
                const uint64_t storageObjects = findCount("storage_objects_bytes");
                const uint64_t storageRefsDb = findCount("storage_refs_db_bytes");
                const uint64_t dbBytes = findCount("db_bytes");
                const uint64_t vectorsDbBytes = findCount("vectors_db_bytes");
                const uint64_t vectorIndexBytes = findCount("vector_index_bytes");

                if (storageObjects > 0 || storageRefsDb > 0 || dbBytes > 0 || vectorsDbBytes > 0 ||
                    vectorIndexBytes > 0) {
                    storageRows.push_back({"", "", ""}); // Separator
                    storageRows.push_back({subsection_header("Disk Usage Breakdown"), "", ""});

                    if (storageObjects > 0) {
                        std::ostringstream detail;
                        const uint64_t objFiles = findCount("storage_objects_files");
                        if (objFiles > 0) {
                            detail << objFiles << " files";
                        }
                        storageRows.push_back(
                            {"  CAS Blocks", humanBytes(storageObjects), detail.str()});
                    }

                    if (storageRefsDb > 0) {
                        storageRows.push_back({"  Ref Counter DB", humanBytes(storageRefsDb), ""});
                    }

                    if (dbBytes > 0) {
                        storageRows.push_back({"  Metadata DB", humanBytes(dbBytes), ""});
                    }

                    if (vectorsDbBytes > 0) {
                        storageRows.push_back({"  Vector DB", humanBytes(vectorsDbBytes), ""});
                    }

                    if (vectorIndexBytes > 0) {
                        storageRows.push_back({"  Vector Index", humanBytes(vectorIndexBytes), ""});
                    }

                    // Calculate total overhead (everything except CAS blocks)
                    const uint64_t totalOverhead =
                        storageRefsDb + dbBytes + vectorsDbBytes + vectorIndexBytes;
                    if (totalOverhead > 0 && storageObjects > 0) {
                        double overheadPct =
                            (static_cast<double>(totalOverhead) / storageObjects) * 100.0;
                        std::ostringstream pct;
                        pct << std::fixed << std::setprecision(1) << overheadPct << "% of CAS";
                        storageRows.push_back(
                            {"  Total Overhead", humanBytes(totalOverhead), pct.str()});
                    }
                }

                auto getReadiness = [&](const char* key) -> bool {
                    auto it = status.readinessStates.find(key);
                    return it != status.readinessStates.end() && it->second;
                };

                // Vector DB
                {
                    const bool ready = getReadiness("vector_db");
                    const bool initialized = status.vectorDbInitAttempted;
                    Severity sev = ready ? Severity::Good : Severity::Warn;
                    std::string text =
                        ready ? "Ready" : (initialized ? "Initialized (empty)" : "Not initialized");
                    std::string extra;
                    if (status.vectorDbDim > 0) {
                        extra = "dim=" + std::to_string(status.vectorDbDim);
                    }
                    storageRows.push_back({"Vector DB", paintStatus(sev, text), extra});
                }

                // Embeddings
                {
                    bool available = status.embeddingAvailable;
                    Severity sev = available ? Severity::Good : Severity::Warn;
                    std::string text = available ? "Available" : "Unavailable";
                    std::string extra;
                    if (!status.embeddingModel.empty())
                        extra += status.embeddingModel;
                    if (!status.embeddingBackend.empty()) {
                        if (!extra.empty())
                            extra += " · ";
                        extra += status.embeddingBackend;
                    }
                    if (status.embeddingDim > 0) {
                        if (!extra.empty())
                            extra += " · ";
                        extra += "dim " + std::to_string(status.embeddingDim);
                    }
                    storageRows.push_back({"Embeddings", paintStatus(sev, text), extra});
                }

                if (!status.contentStoreRoot.empty()) {
                    storageRows.push_back(
                        {"Content root", status.contentStoreRoot, status.contentStoreError});
                }
                if (hasDaemonDataDir) {
                    storageRows.push_back({"Daemon data dir", daemonDataDir->string(), ""});
                }
                if (!configuredDataDir.empty()) {
                    std::string extra;
                    if (dataDirMismatch) {
                        extra = paintStatus(Severity::Warn, "daemon differs from CLI/config");
                    }
                    storageRows.push_back({"Expected data dir", configuredDataDir.string(), extra});
                }
                render_rows(std::cout, storageRows);

                std::vector<Row> dataDirWarningRows;
                if (daemonUsesEphemeralData) {
                    dataDirWarningRows.push_back(
                        {"Ephemeral data dir",
                         paintStatus(Severity::Warn,
                                     "Daemon is serving a temporary data directory"),
                         daemonDataDir->string()});
                }
                if (dataDirMismatch) {
                    dataDirWarningRows.push_back(
                        {"Data dir mismatch",
                         paintStatus(Severity::Warn,
                                     "Daemon is not using the current CLI/config data directory"),
                         "Restart daemon if this was accidental"});
                }
                if (!dataDirWarningRows.empty()) {
                    std::cout << "\n" << section_header("Data Directory Warnings") << "\n\n";
                    render_rows(std::cout, dataDirWarningRows);
                }

                // Only show Readiness section if there are issues or non-ready components
                std::vector<Row> issueRows;
                auto suppressDetailedIssue = [&](const std::string& lowerLabel) {
                    if ((lowerLabel == "embedding ready" || lowerLabel == "embedding_ready") &&
                        (status.embeddingAvailable ||
                         (status.readinessStates.contains("model_provider") &&
                          status.readinessStates.at("model_provider")))) {
                        return true;
                    }
                    if ((lowerLabel == "plugins ready" || lowerLabel == "plugins_ready") &&
                        (status.readinessStates.contains("plugins") &&
                         status.readinessStates.at("plugins"))) {
                        return true;
                    }
                    if (lowerLabel == "topology artifacts fresh" ||
                        lowerLabel == "topology_artifacts_fresh" ||
                        lowerLabel == "topology rebuild running" ||
                        lowerLabel == "topology_rebuild_running" || lowerLabel == "vector index" ||
                        lowerLabel == "vector_index") {
                        return true;
                    }
                    return false;
                };
                for (const auto& rd : readinessList) {
                    // Skip "degraded" flags (inverses of ready flags) and items already shown
                    // elsewhere
                    std::string lowerLabel = rd.label;
                    std::transform(lowerLabel.begin(), lowerLabel.end(), lowerLabel.begin(),
                                   ::tolower);
                    bool isDegraded = lowerLabel.find("degraded") != std::string::npos;
                    bool isAlreadyShown = lowerLabel.find("vector db") != std::string::npos ||
                                          lowerLabel.find("embedding") != std::string::npos;
                    // Skip "build reason" keys - they're informational, not readiness indicators.
                    // The search_engine key itself indicates readiness.
                    bool isBuildReason = lowerLabel.find("build reason") != std::string::npos;

                    if (!isDegraded && !isAlreadyShown && !isBuildReason && rd.issue &&
                        !suppressDetailedIssue(lowerLabel)) {
                        issueRows.push_back({rd.label, paintStatus(rd.severity, rd.text), ""});
                    }
                }
                if (!issueRows.empty()) {
                    std::cout << "\n" << section_header("Components Not Ready") << "\n\n";
                    render_rows(std::cout, issueRows);
                }

                if (!status.initProgress.empty()) {
                    std::cout << "\n" << section_header("Initialization progress") << "\n\n";
                    std::vector<Row> initRows;
                    std::vector<std::pair<std::string, uint8_t>> init(status.initProgress.begin(),
                                                                      status.initProgress.end());
                    std::sort(init.begin(), init.end(),
                              [](const auto& a, const auto& b) { return a.second > b.second; });
                    const std::size_t limit = std::min<std::size_t>(init.size(), 10);
                    for (std::size_t i = 0; i < limit; ++i) {
                        initRows.push_back({humanizeToken(init[i].first),
                                            std::to_string(static_cast<int>(init[i].second)) + "%",
                                            ""});
                    }
                    render_rows(std::cout, initRows);
                }

                if (!status.requestCounts.empty()) {
                    auto formatCountValue = [](const std::string& key,
                                               uint64_t value) -> std::string {
                        std::string lowerKey = key;
                        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(),
                                       ::tolower);
                        const bool isBytes = lowerKey.find("bytes") != std::string::npos ||
                                             lowerKey.find("_mb") != std::string::npos ||
                                             lowerKey.find("_kb") != std::string::npos ||
                                             lowerKey.find("_gb") != std::string::npos;

                        if (isBytes && value > 1024) {
                            const char* units[] = {"B", "KB", "MB", "GB", "TB"};
                            auto val = static_cast<double>(value);
                            int idx = 0;
                            while (val >= 1024.0 && idx < 4) {
                                val /= 1024.0;
                                ++idx;
                            }
                            std::ostringstream oss;
                            oss << std::fixed << std::setprecision(val < 10 ? 1 : 0) << val << " "
                                << units[idx];
                            return oss.str();
                        }

                        if (value >= 100000) {
                            std::ostringstream oss;
                            oss << std::fixed << std::setprecision(value < 1000000 ? 0 : 1)
                                << (static_cast<double>(value) / 1000.0) << "k";
                            return oss.str();
                        } else if (value >= 10000) {
                            std::string num = std::to_string(value);
                            std::string formatted;
                            int count = 0;
                            for (auto it = num.rbegin(); it != num.rend(); ++it) {
                                if (count > 0 && count % 3 == 0)
                                    formatted.insert(0, 1, ',');
                                formatted.insert(0, 1, *it);
                                ++count;
                            }
                            return formatted;
                        }

                        return std::to_string(value);
                    };

                    // Filter out internal/tuning counters - show user-facing metrics only
                    std::vector<std::pair<std::string, size_t>> counts;
                    for (const auto& [key, value] : status.requestCounts) {
                        std::string lowerKey = key;
                        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(),
                                       ::tolower);

                        // Skip internal state/tuning counters
                        bool isInternal = lowerKey.find("tuning_") == 0 ||
                                          lowerKey.find("_fsm_state") != std::string::npos ||
                                          lowerKey.find("service_fsm") != std::string::npos ||
                                          lowerKey.find("embedding_state") != std::string::npos ||
                                          lowerKey.find("plugin_host_state") != std::string::npos;

                        if (!isInternal) {
                            counts.emplace_back(key, value);
                        }
                    }

                    if (!counts.empty()) {
                        std::sort(counts.begin(), counts.end(),
                                  [](const auto& a, const auto& b) { return a.second > b.second; });
                        std::cout << "\n" << section_header("Top Metrics") << "\n\n";
                        std::vector<Row> countRows;
                        const std::size_t limit = std::min<std::size_t>(counts.size(), 10);
                        for (std::size_t i = 0; i < limit; ++i) {
                            countRows.push_back(
                                {humanizeToken(counts[i].first),
                                 formatCountValue(counts[i].first, counts[i].second), ""});
                        }
                        render_rows(std::cout, countRows);
                    }
                }

                if (!status.providers.empty()) {
                    std::cout << "\n" << section_header("Providers") << "\n\n";
                    std::vector<Row> providerRows;
                    const std::size_t limit = std::min<std::size_t>(status.providers.size(), 8);
                    for (std::size_t i = 0; i < limit; ++i) {
                        const auto& p = status.providers[i];
                        Severity provSeverity = p.ready ? Severity::Good : Severity::Warn;
                        std::string extra;
                        if (p.modelsLoaded > 0)
                            extra += std::to_string(p.modelsLoaded) + " models";
                        if (!p.error.empty()) {
                            if (!extra.empty())
                                extra += " · ";
                            extra += p.error;
                            provSeverity = Severity::Warn;
                        }
                        if (p.isProvider) {
                            if (!extra.empty())
                                extra += " · ";
                            extra += "active";
                        }
                        providerRows.push_back(
                            {p.name.empty() ? "(unnamed)" : p.name,
                             paintStatus(provSeverity,
                                         p.ready ? "Ready"
                                                 : (p.degraded ? "Degraded" : "Starting")),
                             extra});
                    }
                    render_rows(std::cout, providerRows);
                }

                if (!status.models.empty()) {
                    std::cout << "\n" << section_header("Models") << "\n\n";
                    std::vector<Row> modelRows;
                    const std::size_t limit = std::min<std::size_t>(status.models.size(), 10);
                    for (std::size_t i = 0; i < limit; ++i) {
                        const auto& m = status.models[i];
                        if (m.name == "(provider)")
                            continue;
                        std::ostringstream detail;
                        if (m.memoryMb > 0)
                            detail << m.memoryMb << " MB";
                        if (m.requestCount > 0) {
                            if (detail.tellp() > 0)
                                detail << " · ";
                            detail << m.requestCount << " req";
                        }
                        modelRows.push_back({m.name, m.type, detail.str()});
                    }
                    render_rows(std::cout, modelRows);
                }

                if (!waiting.empty()) {
                    std::string joined;
                    for (std::size_t i = 0; i < waiting.size(); ++i) {
                        if (i)
                            joined += ", ";
                        joined += waiting[i];
                    }
                    std::cout << "\n" << colorize("• Waiting on: " + joined, Ansi::YELLOW) << "\n";
                }

                return;
            }
            lastErr = statusResult.error();
            std::this_thread::sleep_for(std::chrono::milliseconds(120 * (attempt + 1)));
        }
        if (spinner) {
            spinner->stop();
        }
        // All retries exhausted — fall back to bootstrap status file before giving up
        try {
            auto rt = daemon::YamsDaemon::getXDGRuntimeDir() / "yams-daemon.status.json";
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
        stopDaemon();

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

        // Determine log file path - mirrors YamsDaemon::resolvePath(PathType::LogFile)
        fs::path logPath;
        std::vector<fs::path> candidates;

#ifdef _WIN32
        const char* localAppData = std::getenv("LOCALAPPDATA");
        if (localAppData) {
            candidates.push_back(fs::path(localAppData) / "yams" / "daemon.log");
        }
        candidates.push_back(fs::temp_directory_path() / "yams-daemon.log");
#else
        // Root user: /var/log
        if (getuid() == 0) {
            candidates.push_back(fs::path("/var/log/yams-daemon.log"));
        }
        // XDG_STATE_HOME or ~/.local/state
        const char* xdgState = std::getenv("XDG_STATE_HOME");
        if (xdgState) {
            candidates.push_back(fs::path(xdgState) / "yams" / "daemon.log");
        } else {
            const char* home = std::getenv("HOME");
            if (home) {
                candidates.push_back(fs::path(home) / ".local" / "state" / "yams" / "daemon.log");
            }
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
