#include <nlohmann/json.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/error_hints.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/version.hpp>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <future>
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
    enum class Severity { Good, Warn, Bad };

    struct ReadinessDisplay {
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

    // Shared helper: paint a status value with severity icon and color
    static std::string paintStatus(Severity sev, std::string text) {
        using namespace yams::cli::ui;
        const char* color = Ansi::GREEN;
        const char* icon = "✓";
        switch (sev) {
            case Severity::Good:
                break;
            case Severity::Warn:
                color = Ansi::YELLOW;
                icon = "⚠";
                break;
            case Severity::Bad:
                color = Ansi::RED;
                icon = "✗";
                break;
        }
        return colorize(std::string(icon) + " " + std::move(text), color);
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

        pid_t pid;
        pidFile >> pid;
        return pid;
    }

    bool killDaemonByPid(pid_t pid, bool force = false) {
        if (pid <= 0) {
            return false;
        }

        // Check if process exists
        if (kill(pid, 0) != 0) {
            return false; // Process doesn't exist
        }

        // Send termination signal
        int sig = force ? SIGKILL : SIGTERM;
        if (kill(pid, sig) != 0) {
            spdlog::error("Failed to send signal to daemon (PID {}): {}", pid, strerror(errno));
            return false;
        }

        spdlog::info("Sent {} to daemon (PID {})", force ? "SIGKILL" : "SIGTERM", pid);

        // Wait for process to terminate (max 5 seconds for SIGTERM)
        if (!force) {
            for (int i = 0; i < 50; i++) {
                if (kill(pid, 0) != 0) {
                    return true; // Process terminated
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        return kill(pid, 0) != 0; // Check if process is gone
    }

    bool waitForDaemonStop(const std::string& socketPath, const std::string& pidFilePath,
                           std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
        const auto start = std::chrono::steady_clock::now();
        const auto interval = std::chrono::milliseconds(100);

        while (std::chrono::steady_clock::now() - start < timeout) {
            pid_t pid = readPidFromFile(pidFilePath);

            bool processGone = (pid <= 0) || (kill(pid, 0) != 0);
            bool socketGone = !daemon::DaemonClient::isDaemonRunning(socketPath);

            if (processGone && socketGone) {
                spdlog::debug("Daemon fully stopped (PID {}), socket cleared", pid);
                return true;
            }

            spdlog::debug("Waiting for daemon to stop... PID={}, process_gone={}, socket_gone={}",
                          pid, processGone, socketGone);
            std::this_thread::sleep_for(interval);
        }

        spdlog::warn("Timeout waiting for daemon to stop");
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
                spdlog::warn("Failed to remove {} file {}: {}", label, path.string(), ec.message());
            }
        };

        // Remove socket file if it exists
        if (!socketPath.empty()) {
            removeWithRetry(std::filesystem::path{socketPath}, "socket");
        }

        // Remove PID file if it exists
        if (!pidFilePath.empty()) {
            removeWithRetry(std::filesystem::path{pidFilePath}, "PID");
        }
    }

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
                std::string pidFilePath =
                    daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                        .string();

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
    void startDaemon() {
        namespace fs = std::filesystem;

        // Resolve paths if not explicitly provided
        // For start: do NOT persist a resolved socket into socketPath_ unless user passed it.
        // Use a local effective path for pre-checks only; the daemon will resolve from config.
        const std::string effectiveSocket =
            daemon::DaemonClient::resolveSocketPathConfigFirst().string();
        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
        }

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

            stopDaemon();

            if (!waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(5))) {
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
            if (!daemon::DaemonClient::isDaemonRunning(config.socketPath)) {
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
        // Resolve paths if not explicitly provided (do not persist into socketPath_)
        const std::string effectiveSocket =
            socketPath_.empty() ? daemon::DaemonClient::resolveSocketPathConfigFirst().string()
                                : socketPath_;
        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
        }

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

        // Check if daemon is running
        bool daemonRunning = daemon::DaemonClient::isDaemonRunning(effectiveSocket);
        if (!daemonRunning) {
            // Check if there's a stale PID file
            pid_t pid = readPidFromFile(pidFile_);
            if (pid > 0 && kill(pid, 0) == 0) {
                spdlog::warn("Found daemon process (PID {}) not responding on socket", pid);
                daemonRunning = true;
            } else {
                spdlog::info("YAMS daemon is not running");
                cleanupDaemonFiles(effectiveSocket, pidFile_);
                stopSpinner();
                return;
            }
        }

        bool stopped = false;
        auto pidAlive = [&]() -> bool {
            pid_t pid = readPidFromFile(pidFile_);
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

                // Wait for daemon to stop
                for (int i = 0; i < 30; i++) {
                    if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                        if (!pidAlive()) {
                            stopped = true;
                            spdlog::info("Daemon stopped successfully");
                            break;
                        }
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                // If not yet stopped, proceed to PID-based termination fallback
                if (!stopped) {
                    spdlog::info("Daemon shutdown requested; waiting for process to exit");
                }
            } else {
                // Treat common peer-closure/transient errors as potentially-successful if the
                // daemon disappears shortly after the request.
                spdlog::warn("Socket shutdown encountered: {}", shutdownResult.error().message);
                for (int i = 0; i < 30; ++i) {
                    if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                        if (!pidAlive()) {
                            stopped = true;
                            spdlog::info("Daemon stopped successfully");
                            break;
                        }
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        }

        // If socket shutdown failed, try PID-based termination
        if (!stopped) {
            pid_t pid = readPidFromFile(pidFile_);
            if (pid > 0) {
                spdlog::info("Attempting PID-based termination for daemon (PID {})", pid);

                // Try SIGTERM first
                if (killDaemonByPid(pid, false)) {
                    stopped = true;
                    spdlog::info("Daemon terminated with SIGTERM");
                } else if (force_) {
                    // If force flag is set and SIGTERM failed, use SIGKILL
                    spdlog::warn("SIGTERM failed, using SIGKILL");
                    if (killDaemonByPid(pid, true)) {
                        stopped = true;
                        spdlog::info("Daemon terminated with SIGKILL");
                    } else {
                        spdlog::error("Failed to kill daemon even with SIGKILL");
                    }
                } else {
                    spdlog::error("Daemon did not respond to SIGTERM. Use --force to kill it");
                }
            } else {
                spdlog::debug("No PID file found at: {}", pidFile_);
                // If no PID file but socket shutdown was attempted, assume it worked
                if (daemonRunning) {
                    spdlog::warn("Could not verify daemon stopped (no PID file), but shutdown was "
                                 "requested");
                }
            }
        }

        // If still not stopped, try platform-specific last-resort termination for orphaned daemons
        if (!stopped && daemonRunning) {
            spdlog::warn("Daemon not responding to shutdown, attempting to kill orphaned process");

#ifndef _WIN32
            // Unix: use pkill to find and kill yams-daemon processes with our socket
            // Use SIGKILL (-9) when --force is set, otherwise SIGTERM (default)
            std::string signal = force_ ? "-9 " : "";
            std::string pkillCmd = "pkill " + signal + "-f 'yams-daemon.*" + effectiveSocket + "'";
            int pkillResult = std::system(pkillCmd.c_str());

            if (pkillResult == 0) {
                spdlog::info("Successfully killed orphaned daemon process");
                stopped = true;

                // Wait a moment for process to die
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } else {
                // Try a more general pkill if specific socket match fails
                pkillCmd = "pkill " + signal + "-f 'yams-daemon'";
                pkillResult = std::system(pkillCmd.c_str());
                if (pkillResult == 0) {
                    spdlog::info("Killed yams-daemon process(es)");
                    stopped = true;
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
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

        // Clean up files if daemon was stopped
        if (stopped) {
            cleanupDaemonFiles(effectiveSocket, pidFile_);
            stopSpinner();
            std::cout << "[OK] YAMS daemon stopped successfully\n";
        } else {
            stopSpinner();
            spdlog::error("Failed to stop YAMS daemon");
            std::cerr << "[FAIL] Failed to stop YAMS daemon\n";
            std::cerr << "  💡 Hint: The daemon may be unresponsive or owned by another user\n";
            std::cerr << "  📋 Try: yams daemon stop --force\n";
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
        std::string effectiveSocket =
            socketPath_.empty() ? daemon::DaemonClient::resolveSocketPathConfigFirst().string()
                                : socketPath_;
        // Title - more compact
        std::cout << "\n=== YAMS Daemon Doctor ===\n\n";

        // Section: IPC & Files - compact header
        std::cout << "IPC & Files:\n";
        std::cout << "  Socket:    "
                  << (effectiveSocket.empty() ? "<resolve failed>" : effectiveSocket) << "\n";
        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
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
        if (!effectiveSocket.empty()) {
            fs::path parent = fs::path(effectiveSocket).parent_path();
            std::error_code ec;
            if (parent.empty())
                parent = ".";
            fs::create_directories(parent, ec); // best effort
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
        bool socket_exists = !effectiveSocket.empty() && safe_exists(effectiveSocket);
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
                        {}, [](yams::daemon::DaemonClient& client) { return client.status(); },
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
                                nlohmann::json j;
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
        // Resolve socket path if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPathConfigFirst().string();
        }

        if (detailed_) {
            if (pidFile_.empty()) {
                pidFile_ =
                    daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                        .string();
            }
            // Enable client debug logging for ping/connect path
            setenv("YAMS_CLIENT_DEBUG", "1", 1);
        }

        // Check if daemon is running (prefer socket; fall back to PID)
        if (!daemon::DaemonClient::isDaemonRunning(socketPath_)) {
            // Try PID-based detection to distinguish "starting" vs "not running"
            if (pidFile_.empty()) {
                pidFile_ =
                    daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                        .string();
            }
            pid_t pid = readPidFromFile(pidFile_);
#ifndef _WIN32
            if (pid > 0 && kill(pid, 0) == 0) {
                std::cout << "YAMS daemon is starting/initializing (IPC not yet available)\n";
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
                                nlohmann::json j;
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
            return;
        }

        if (!detailed_) {
            // Compact default view - quick health check
            std::optional<yams::cli::ui::SpinnerRunner> spinner;
            if (yams::cli::ui::stdout_is_tty()) {
                spinner.emplace();
                spinner->start("Checking daemon status...");
            }
            auto sres = runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.status(); },
                std::chrono::seconds(5));
            if (spinner) {
                spinner->stop();
            }
            if (!sres) {
                std::cout << "YAMS daemon is running\n";
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
                    auto rd = classifyReadiness(key, ready);
                    issues.push_back(rd.label);
                }
            }
            std::sort(issues.begin(), issues.end());
            issues.erase(std::unique(issues.begin(), issues.end()), issues.end());

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
            Severity searchSev = s.searchMetrics.queued > 50   ? Severity::Bad
                                 : s.searchMetrics.queued > 10 ? Severity::Warn
                                                               : Severity::Good;
            std::ostringstream searchInfo;
            searchInfo << s.searchMetrics.active << " active · " << s.searchMetrics.queued
                       << " queued";
            overview.push_back({"Search", paintStatus(searchSev, searchInfo.str()), ""});

            // Embeddings summary
            Severity embSev = s.embeddingAvailable ? Severity::Good : Severity::Warn;
            std::string embText = s.embeddingAvailable ? "Available" : "Unavailable";
            std::string embExtra = s.embeddingModel.empty() ? "" : s.embeddingModel;
            overview.push_back({"Embeddings", paintStatus(embSev, embText), embExtra});

            render_rows(std::cout, overview);

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
            auto statusResult = runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.status(); },
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
                for (const auto& rd : readinessList) {
                    if (rd.issue) {
                        // Skip "build reason" keys - they're informational, not readiness
                        // indicators
                        std::string lowerLabel = rd.label;
                        std::transform(lowerLabel.begin(), lowerLabel.end(), lowerLabel.begin(),
                                       ::tolower);
                        if (lowerLabel.find("build reason") == std::string::npos)
                            waiting.push_back(rd.label);
                    }
                }
                std::sort(waiting.begin(), waiting.end());
                waiting.erase(std::unique(waiting.begin(), waiting.end()), waiting.end());

                bool degraded = status.overallStatus == "degraded" || !waiting.empty();
                if (!status.lastError.empty())
                    degraded = true;

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

                std::size_t threads = 0, active = 0, queued = 0;
                if (auto it = status.requestCounts.find("worker_threads");
                    it != status.requestCounts.end())
                    threads = it->second;
                if (auto it = status.requestCounts.find("worker_active");
                    it != status.requestCounts.end())
                    active = it->second;
                if (auto it = status.requestCounts.find("worker_queued");
                    it != status.requestCounts.end())
                    queued = it->second;
                std::size_t util =
                    threads ? static_cast<std::size_t>((100.0 * active) / threads) : 0;
                std::ostringstream worker;
                worker << threads << " threads · " << active << " active · " << queued << " queued";
                Severity workerSeverity =
                    util >= 95 ? Severity::Bad : (util >= 85 ? Severity::Warn : Severity::Good);
                resourceRows.push_back({"Worker utilization",
                                        paintStatus(workerSeverity, std::to_string(util) + "%"),
                                        worker.str()});
                render_rows(std::cout, resourceRows);

                // Resource Governor section (memory pressure management)
                if (status.governorBudgetBytes > 0) {
                    std::cout << "\n" << section_header("Resource Governor") << "\n\n";
                    std::vector<Row> governor;

                    // Memory pressure level
                    const char* levelNames[] = {"Normal", "Warning", "Critical", "Emergency"};
                    uint8_t lvl = std::min(status.governorPressureLevel, static_cast<uint8_t>(3));
                    Severity pressSev = (lvl == 0)   ? Severity::Good
                                        : (lvl == 1) ? Severity::Warn
                                                     : Severity::Bad;
                    std::ostringstream memExtra;
                    memExtra << std::fixed << std::setprecision(0)
                             << (static_cast<double>(status.governorRssBytes) / (1024 * 1024))
                             << " MB / "
                             << (static_cast<double>(status.governorBudgetBytes) / (1024 * 1024))
                             << " MB";
                    governor.push_back({"Memory Pressure", paintStatus(pressSev, levelNames[lvl]),
                                        memExtra.str()});

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
                if (!transport.empty())
                    render_rows(std::cout, transport);

                std::cout << "\n" << section_header("Search") << "\n\n";
                std::vector<Row> searchRows;
                std::ostringstream base;
                base << status.searchMetrics.active << " active · " << status.searchMetrics.queued
                     << " queued";
                std::ostringstream extra;
                extra << "executed " << status.searchMetrics.executed << " · cache " << std::fixed
                      << std::setprecision(1) << (status.searchMetrics.cacheHitRate * 100.0)
                      << "% · latency " << status.searchMetrics.avgLatencyUs << "µs";
                Severity searchSeverity =
                    status.searchMetrics.queued > 50
                        ? Severity::Bad
                        : (status.searchMetrics.queued > 10 ? Severity::Warn : Severity::Good);
                searchRows.push_back(
                    {"Queries", paintStatus(searchSeverity, base.str()), extra.str()});
                if (status.searchMetrics.concurrencyLimit > 0) {
                    searchRows.push_back(
                        {"Concurrency", std::to_string(status.searchMetrics.concurrencyLimit), ""});
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
                    uint64_t processed = findPostIngestCount("post_ingest_processed");
                    uint64_t failed = findPostIngestCount("post_ingest_failed");
                    uint64_t latency = findPostIngestCount("post_ingest_latency_ms_ema");
                    uint64_t rate = findPostIngestCount("post_ingest_rate_sec_ema");

                    std::ostringstream queueVal;
                    queueVal << queued << " queued · " << inflight << " inflight";
                    if (cap > 0)
                        queueVal << " · cap " << cap;
                    Severity qSev = queued > cap * 0.8
                                        ? Severity::Bad
                                        : (queued > cap * 0.5 ? Severity::Warn : Severity::Good);
                    postIngestRows.push_back({"Queue", paintStatus(qSev, queueVal.str()), ""});

                    std::ostringstream throughput;
                    throughput << rate << "/s · " << latency << "ms latency";
                    postIngestRows.push_back({"Throughput", throughput.str(), ""});

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

                    // Per-stage breakdown
                    uint64_t extractInFlight = findPostIngestCount("extraction_inflight");
                    uint64_t kgQueuedTotal = findPostIngestCount("kg_queued");
                    uint64_t kgConsumed = findPostIngestCount("kg_consumed");
                    uint64_t kgInFlight = findPostIngestCount("kg_inflight");
                    uint64_t symbolInFlight = findPostIngestCount("symbol_inflight");
                    // Calculate actual pending = queued - consumed - inflight
                    int64_t kgPending = static_cast<int64_t>(kgQueuedTotal) -
                                        static_cast<int64_t>(kgConsumed) -
                                        static_cast<int64_t>(kgInFlight);
                    if (kgPending < 0)
                        kgPending = 0;

                    if (extractInFlight > 0 || kgPending > 0 || kgInFlight > 0 ||
                        symbolInFlight > 0) {
                        postIngestRows.push_back({"", "", ""}); // Separator
                        postIngestRows.push_back({subsection_header("Pipeline Stages"), "", ""});

                        std::ostringstream extractVal;
                        extractVal << extractInFlight << " inflight (max 4)";
                        postIngestRows.push_back({"  Extraction", extractVal.str(), ""});

                        std::ostringstream kgVal;
                        kgVal << kgPending << " queued · " << kgInFlight << " inflight (max 8)";
                        postIngestRows.push_back({"  Knowledge Graph", kgVal.str(), ""});

                        std::ostringstream symbolVal;
                        symbolVal << symbolInFlight << " inflight (max 4)";
                        postIngestRows.push_back({"  Symbol Extraction", symbolVal.str(), ""});
                    }
                }
                render_rows(std::cout, postIngestRows);

                std::cout << "\n" << section_header("Storage & Embeddings") << "\n\n";
                std::vector<Row> storageRows;
                auto findCount = [&](const char* key) -> uint64_t {
                    auto it = status.requestCounts.find(key);
                    return it != status.requestCounts.end() ? it->second : 0ULL;
                };
                const uint64_t docs =
                    std::max(findCount("documents_total"), findCount("storage_documents"));
                const uint64_t indexed = findCount("documents_indexed");
                if (docs > 0 || indexed > 0) {
                    std::ostringstream docsVal;
                    docsVal << docs << " docs";
                    if (indexed > 0)
                        docsVal << " · indexed " << indexed;
                    storageRows.push_back({"Documents", docsVal.str(), ""});
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
                    bool ready = getReadiness("vector_db");
                    Severity sev = ready ? Severity::Good : Severity::Warn;
                    std::string text = ready ? "Ready" : "Not initialized";
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
                render_rows(std::cout, storageRows);

                // Only show Readiness section if there are issues or non-ready components
                std::vector<Row> issueRows;
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

                    if (!isDegraded && !isAlreadyShown && !isBuildReason && rd.issue) {
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
        spdlog::error("Failed to get daemon status: {}", lastErr.message);
        std::exit(1);
    }

    Result<std::shared_ptr<yams::cli::DaemonClientPool::Lease>>
    acquireDaemonClient(const yams::daemon::ClientConfig& cfg = {}) const {
        return yams::cli::acquire_cli_daemon_client_shared(cfg);
    }

    template <typename AwaitableProvider>
    auto runDaemonClient(const yams::daemon::ClientConfig& cfg, AwaitableProvider&& provider,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds{0}) const
        -> decltype(yams::cli::run_result(provider(std::declval<yams::daemon::DaemonClient&>()),
                                          timeout)) {
        using ResultType = decltype(yams::cli::run_result(
            provider(std::declval<yams::daemon::DaemonClient&>()), timeout));
        auto leaseRes = acquireDaemonClient(cfg);
        if (!leaseRes)
            return ResultType{leaseRes.error()};
        auto leaseHandle = std::move(leaseRes.value());
        return yams::cli::run_result(provider(**leaseHandle), timeout);
    }

    void restartDaemon() {
        const std::string effectiveSocket =
            socketPath_.empty() ? daemon::DaemonClient::resolveSocketPathConfigFirst().string()
                                : socketPath_;

        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
        }

        stopDaemon();

        if (!waitForDaemonStop(effectiveSocket, pidFile_, std::chrono::seconds(5))) {
            spdlog::error("Failed to stop daemon for restart");
            std::exit(1);
        }

        // Start daemon
        std::cout << "[INFO] Starting YAMS daemon...\n";

        daemon::ClientConfig config;
        if (!socketPath_.empty())
            config.socketPath = socketPath_;
        if (!dataDir_.empty())
            config.dataDir = dataDir_;
        else if (cli_)
            config.dataDir = cli_->getDataPath().string();

        auto result = daemon::DaemonClient::startDaemon(config);
        if (!result) {
            spdlog::error("Failed to start daemon: {}", result.error().message);
            std::exit(1);
        }

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
