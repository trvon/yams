#include <nlohmann/json.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/use_future.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/progress_indicator.h>
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

        restart->callback([this]() { restartDaemon(); });

        // Doctor subcommand
        auto* doctor =
            daemon->add_subcommand("doctor", "Diagnose daemon IPC and environment issues");
        doctor->callback([this]() { doctorDaemon(); });
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
                    unsigned char uch = static_cast<unsigned char>(word[i]);
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

    void cleanupDaemonFiles(const std::string& socketPath, const std::string& pidFilePath) {
        // Remove socket file if it exists
        if (!socketPath.empty()) {
            std::filesystem::path sp{socketPath};
            if (std::filesystem::exists(sp) && std::filesystem::remove(sp)) {
                spdlog::debug("Removed stale socket file: {}", socketPath);
            }
        }

        // Remove PID file if it exists
        if (!pidFilePath.empty()) {
            std::filesystem::path pp{pidFilePath};
            if (std::filesystem::exists(pp) && std::filesystem::remove(pp)) {
                spdlog::debug("Removed stale PID file: {}", pidFilePath);
            }
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
            } else {
                std::cout << "YAMS daemon is already running.\n";
                std::cout << "Use '--restart' to restart it, or run 'yams daemon status -d' to "
                             "view daemon status.\n";
            }
            return;
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
#ifndef _WIN32
                char buf[4096];
                ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
                if (n > 0) {
                    buf[n] = '\0';
                    selfExe = fs::path(buf);
                }
#endif
                if (!selfExe.empty()) {
                    auto cliDir = selfExe.parent_path();
                    std::vector<fs::path> candidates = {
                        cliDir / "yams-daemon", cliDir.parent_path() / "yams-daemon",
                        cliDir.parent_path() / "daemon" / "yams-daemon",
                        cliDir.parent_path().parent_path() / "daemon" / "yams-daemon",
                        cliDir.parent_path().parent_path() / "yams-daemon"};
                    for (const auto& p : candidates) {
                        if (fs::exists(p)) {
                            exePath = p.string();
                            break;
                        }
                    }
                }
                if (exePath.empty()) {
                    exePath = "yams-daemon"; // fallback to PATH
                }
            }

            // Pass storage directory via env for the daemon
            if (!dataDir_.empty()) {
                setenv("YAMS_STORAGE", dataDir_.c_str(), 1);
            } else if (cli_) {
                auto p = cli_->getDataPath();
                if (!p.empty())
                    setenv("YAMS_STORAGE", p.c_str(), 1);
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
                effectiveDataDir = cli_->getDataPath();
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
            auto result = daemon::DaemonClient::startDaemon(config);
            if (!result) {
                spdlog::error("Failed to start daemon: {}", result.error().message);
                std::cerr << "[FAIL] Failed to start daemon: " << result.error().message << "\n";
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
                return;
            }
        }

        bool stopped = false;

        // First try graceful shutdown via socket
        if (daemonRunning) {
            daemon::ShutdownRequest sreq;
            sreq.graceful = !force_;
            auto shutdownResult = runDaemonClient(
                {},
                [&](yams::daemon::DaemonClient& client) { return client.shutdown(sreq.graceful); },
                std::chrono::seconds(10));
            if (shutdownResult) {
                spdlog::info("Sent shutdown request to daemon");

                // Wait for daemon to stop
                for (int i = 0; i < 30; i++) {
                    if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                        stopped = true;
                        spdlog::info("Daemon stopped successfully");
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                // If we sent the shutdown successfully, consider it stopped
                // even if we can't immediately verify
                if (!stopped) {
                    spdlog::info("Daemon shutdown requested, may take a moment to fully stop");
                    stopped = true;
                }
            } else {
                // Treat common peer-closure/transient errors as potentially-successful if the
                // daemon disappears shortly after the request.
                spdlog::warn("Socket shutdown encountered: {}", shutdownResult.error().message);
                for (int i = 0; i < 30; ++i) {
                    if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                        stopped = true;
                        spdlog::info("Daemon stopped successfully");
                        break;
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

        // If still not stopped, try pkill as last resort for orphaned daemons
        if (!stopped && daemonRunning) {
            spdlog::warn("Daemon not responding to shutdown, attempting to kill orphaned process");

            // Use pkill to find and kill yams-daemon processes with our socket
            std::string pkillCmd = "pkill -f 'yams-daemon.*" + socketPath_ + "'";
            int pkillResult = std::system(pkillCmd.c_str());

            if (pkillResult == 0) {
                spdlog::info("Successfully killed orphaned daemon process");
                stopped = true;

                // Wait a moment for process to die
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            } else {
                // Try a more general pkill if specific socket match fails
                pkillResult = std::system("pkill -f 'yams-daemon'");
                if (pkillResult == 0) {
                    spdlog::info("Killed yams-daemon process(es)");
                    stopped = true;
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }
        }

        // Clean up files if daemon was stopped
        if (stopped) {
            cleanupDaemonFiles(socketPath_, pidFile_);
            std::cout << "[OK] YAMS daemon stopped successfully\n";
        } else {
            spdlog::error("Failed to stop YAMS daemon");
            std::cerr << "[FAIL] Failed to stop YAMS daemon\n";
            std::cerr << "[INFO] You may need to manually kill the process: pkill yams-daemon\n";
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
        bool socket_exists = !effectiveSocket.empty() && fs::exists(effectiveSocket);
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
            std::cout << "\n✓ All checks passed\n";
        }
    }

    void showStatus() {
        // Resolve socket path if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPathConfigFirst().string();
        }

        if (detailed_) {
            std::cout << "Socket: " << socketPath_ << "\n";
            if (pidFile_.empty()) {
                pidFile_ =
                    daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                        .string();
            }
            std::cout << "PID file: " << pidFile_ << "\n";
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
            auto sres = runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.status(); },
                std::chrono::seconds(5));
            if (!sres) {
                std::cout << "YAMS daemon is running\n";
                return;
            }

            using namespace yams::cli::ui;
            const auto& s = sres.value();

            auto formatDuration = [](uint64_t sec) {
                if (sec == 0)
                    return std::string{"0s"};
                std::ostringstream oss;
                if (sec >= 3600) {
                    oss << (sec / 3600) << "h";
                    sec %= 3600;
                    if (sec >= 60)
                        oss << " " << (sec / 60) << "m";
                } else if (sec >= 60) {
                    oss << (sec / 60) << "m";
                }
                if (sec % 60 && sec < 3600) {
                    if (!oss.str().empty())
                        oss << " ";
                    oss << (sec % 60) << "s";
                }
                if (oss.str().empty())
                    return std::to_string(sec) + "s";
                return oss.str();
            };

            auto paint = [&](Severity sev, std::string text) {
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
            };

            std::vector<ReadinessDisplay> readinessList;
            readinessList.reserve(s.readinessStates.size());
            for (const auto& [key, ready] : s.readinessStates) {
                readinessList.push_back(classifyReadiness(key, ready));
            }

            bool anyDegraded = (s.overallStatus == "degraded") || !s.lastError.empty();
            std::vector<std::string> waiting;
            waiting.reserve(readinessList.size());
            for (const auto& rd : readinessList) {
                if (rd.issue) {
                    anyDegraded = true;
                    waiting.push_back(rd.label);
                }
            }
            std::sort(waiting.begin(), waiting.end());
            waiting.erase(std::unique(waiting.begin(), waiting.end()), waiting.end());

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
                stateSeverity = anyDegraded ? Severity::Warn : Severity::Warn;
            }
            if (!s.lastError.empty())
                stateSeverity = Severity::Bad;

            std::cout << title_banner("YAMS Daemon Status") << "\n\n";

            std::vector<Row> overview;
            overview.push_back({"State", paint(stateSeverity, lifecycle),
                                s.running
                                    ? (s.ready ? std::string{"ready"} : std::string{"starting"})
                                    : std::string{"stopped"}});
            overview.push_back({"Version", s.version.empty() ? "unknown" : s.version, ""});
            overview.push_back({"Uptime", formatDuration(s.uptimeSeconds), ""});
            overview.push_back(
                {"Requests", std::to_string(s.requestsProcessed),
                 std::string{"active connections: "} + std::to_string(s.activeConnections)});
            if (s.retryAfterMs > 0) {
                overview.push_back(
                    {"Backpressure",
                     paint(Severity::Warn, std::to_string(s.retryAfterMs) + " ms cooldown"), ""});
            }
            if (!s.lastError.empty()) {
                overview.push_back({"Last error", paint(Severity::Bad, s.lastError), ""});
            }
            render_rows(std::cout, overview);

            std::cout << "\n" << section_header("Performance") << "\n\n";
            std::vector<Row> perf;
            {
                std::ostringstream cpu;
                cpu << std::fixed << std::setprecision(1) << s.cpuUsagePercent << "%";
                Severity cpuSeverity =
                    s.cpuUsagePercent >= 95.0
                        ? Severity::Bad
                        : (s.cpuUsagePercent >= 80.0 ? Severity::Warn : Severity::Good);
                perf.push_back({"CPU", paint(cpuSeverity, cpu.str()), ""});
            }
            perf.push_back({"Memory",
                            paint(s.memoryUsageMb > 4096 ? Severity::Warn : Severity::Good,
                                  std::to_string(static_cast<int>(s.memoryUsageMb)) + " MB"),
                            ""});
            perf.push_back(
                {"Pools",
                 std::to_string(s.ipcPoolSize) + " ipc · " + std::to_string(s.ioPoolSize) + " io",
                 ""});

            std::size_t threads = 0, active = 0, queued = 0;
            if (auto it = s.requestCounts.find("worker_threads"); it != s.requestCounts.end())
                threads = it->second;
            if (auto it = s.requestCounts.find("worker_active"); it != s.requestCounts.end())
                active = it->second;
            if (auto it = s.requestCounts.find("worker_queued"); it != s.requestCounts.end())
                queued = it->second;
            const std::size_t util =
                threads ? static_cast<std::size_t>((100.0 * active) / threads) : 0;
            std::ostringstream worker;
            worker << threads << " threads · " << active << " active · " << queued << " queued";
            Severity workerSeverity =
                util >= 95 ? Severity::Bad : (util >= 85 ? Severity::Warn : Severity::Good);
            perf.push_back(
                {"Worker Util", paint(workerSeverity, std::to_string(util) + "%"), worker.str()});

            render_rows(std::cout, perf);

            std::cout << "\n" << section_header("Search") << "\n\n";
            std::vector<Row> search;
            std::ostringstream searchBase;
            searchBase << s.searchMetrics.active << " active · " << s.searchMetrics.queued
                       << " queued";
            std::ostringstream searchExtra;
            searchExtra << "executed " << s.searchMetrics.executed << " · cache " << std::fixed
                        << std::setprecision(1) << (s.searchMetrics.cacheHitRate * 100.0)
                        << "% · latency " << s.searchMetrics.avgLatencyUs << "µs";
            Severity searchSeverity =
                s.searchMetrics.queued > 50
                    ? Severity::Bad
                    : (s.searchMetrics.queued > 10 ? Severity::Warn : Severity::Good);
            search.push_back(
                {"Queries", paint(searchSeverity, searchBase.str()), searchExtra.str()});
            if (s.searchMetrics.concurrencyLimit > 0) {
                search.push_back(
                    {"Concurrency", std::to_string(s.searchMetrics.concurrencyLimit), ""});
            }
            render_rows(std::cout, search);

            std::cout << "\n" << section_header("Data Plane") << "\n\n";
            std::vector<Row> dataRows;
            const bool vectorReady = s.vectorDbReady;
            Severity vecSeverity =
                vectorReady ? Severity::Good
                            : (s.vectorDbInitAttempted ? Severity::Warn : Severity::Warn);
            std::string vecText =
                vectorReady ? "Ready"
                            : (s.vectorDbInitAttempted ? "Initializing" : "Not initialized");
            dataRows.push_back({"Vector DB", paint(vecSeverity, vecText),
                                s.vectorDbDim > 0 ? "dim " + std::to_string(s.vectorDbDim) : ""});

            Severity embSeverity = s.embeddingAvailable ? Severity::Good : Severity::Warn;
            std::string embText = s.embeddingAvailable ? "Available" : "Unavailable";
            std::string embExtra;
            if (!s.embeddingModel.empty())
                embExtra += s.embeddingModel;
            if (!s.embeddingBackend.empty()) {
                if (!embExtra.empty())
                    embExtra += " · ";
                embExtra += s.embeddingBackend;
            }
            if (s.embeddingDim > 0) {
                if (!embExtra.empty())
                    embExtra += " · ";
                embExtra += "dim " + std::to_string(s.embeddingDim);
            }
            dataRows.push_back({"Embeddings", paint(embSeverity, embText), embExtra});

            if (!s.contentStoreRoot.empty()) {
                dataRows.push_back({"Content Root", s.contentStoreRoot, ""});
            }
            render_rows(std::cout, dataRows);

            std::cout << "\n" << section_header("Services") << "\n\n";
            std::vector<Row> svcRows;
            const std::vector<std::pair<std::string, std::string>> readinessMap = {
                {"content_store", "Content store"},
                {"metadata_repo", "Metadata"},
                {"search_engine", "Search"},
                {"model_provider", "Model provider"},
                {"vector_embeddings_available", "Embeddings"},
                {"vector_index", "Vector index"},
                {"plugins_degraded", "Plugins"},
            };
            for (const auto& [key, label] : readinessMap) {
                auto it = s.readinessStates.find(key);
                if (it == s.readinessStates.end())
                    continue;
                auto rd = classifyReadiness(key, it->second);
                svcRows.push_back(
                    {label.empty() ? rd.label : label, paint(rd.severity, rd.text), ""});
            }
            if (!svcRows.empty())
                render_rows(std::cout, svcRows);

            if (!waiting.empty()) {
                std::string joined;
                const std::size_t limit = std::min<std::size_t>(waiting.size(), 4);
                for (std::size_t i = 0; i < limit; ++i) {
                    if (i)
                        joined += ", ";
                    joined += waiting[i];
                }
                if (waiting.size() > limit)
                    joined += ", …";
                std::cout << "\n" << colorize("• Waiting on: " + joined, Ansi::YELLOW) << "\n";
            }

            if (!s.providers.empty()) {
                std::cout << "\n" << section_header("Model Providers") << "\n\n";
                std::vector<Row> providerRows;
                const std::size_t limit = std::min<std::size_t>(s.providers.size(), 6);
                for (std::size_t i = 0; i < limit; ++i) {
                    const auto& p = s.providers[i];
                    Severity provSeverity =
                        p.ready ? Severity::Good : (p.degraded ? Severity::Warn : Severity::Warn);
                    std::string name = p.name.empty() ? "(unnamed)" : p.name;
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
                        extra += "active provider";
                    }
                    providerRows.push_back(
                        {name,
                         paint(provSeverity,
                               p.ready ? "Ready" : (p.degraded ? "Degraded" : "Starting")),
                         extra});
                }
                render_rows(std::cout, providerRows);
            }

            std::cout << "\n"
                      << colorize("→ Use 'yams daemon status -d' for detailed diagnostics",
                                  Ansi::DIM)
                      << "\n";
            return;
        }

        // Detailed status via DaemonClient (synchronous)
        Error lastErr{};
        for (int attempt = 0; attempt < 5; ++attempt) {
            setenv("YAMS_CLIENT_DEBUG", detailed_ ? "1" : "0", 1);
            auto statusResult = runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.status(); },
                std::chrono::seconds(5));
            if (statusResult) {
                using namespace yams::cli::ui;
                const auto& status = statusResult.value();

                auto formatDuration = [](uint64_t sec) {
                    if (sec == 0)
                        return std::string{"0s"};
                    std::ostringstream oss;
                    if (sec >= 3600) {
                        oss << (sec / 3600) << "h";
                        sec %= 3600;
                        if (sec >= 60)
                            oss << " " << (sec / 60) << "m";
                    } else if (sec >= 60) {
                        oss << (sec / 60) << "m";
                    }
                    if (sec % 60 && sec < 3600) {
                        if (!oss.str().empty())
                            oss << " ";
                        oss << (sec % 60) << "s";
                    }
                    if (oss.str().empty())
                        return std::to_string(sec) + "s";
                    return oss.str();
                };

                auto paint = [&](Severity sev, std::string text) {
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
                };

                auto neutral = [](std::string text) { return text; };

                std::vector<ReadinessDisplay> readinessList;
                readinessList.reserve(status.readinessStates.size());
                for (const auto& [key, ready] : status.readinessStates) {
                    readinessList.push_back(classifyReadiness(key, ready));
                }

                std::vector<std::string> waiting;
                waiting.reserve(readinessList.size());
                for (const auto& rd : readinessList) {
                    if (rd.issue)
                        waiting.push_back(rd.label);
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

                Severity stateSeverity =
                    status.running ? (status.ready ? Severity::Good
                                                   : (degraded ? Severity::Warn : Severity::Warn))
                                   : Severity::Bad;
                if (!status.lastError.empty())
                    stateSeverity = Severity::Bad;

                std::cout << title_banner("YAMS Daemon — Detailed") << "\n\n";

                std::vector<Row> overview;
                overview.push_back(
                    {"State", paint(stateSeverity, lifecycle),
                     status.running ? (status.ready ? "ready" : "starting") : "stopped"});
                overview.push_back(
                    {"Version", status.version.empty() ? "unknown" : status.version, ""});
                overview.push_back({"Uptime", formatDuration(status.uptimeSeconds), ""});
                overview.push_back(
                    {"Requests", std::to_string(status.requestsProcessed),
                     std::string{"connections: "} + std::to_string(status.activeConnections)});
                if (status.retryAfterMs > 0) {
                    overview.push_back({"Backpressure",
                                        paint(Severity::Warn,
                                              std::to_string(status.retryAfterMs) + " ms cooldown"),
                                        ""});
                }
                if (!status.lastError.empty()) {
                    overview.push_back({"Last error", paint(Severity::Bad, status.lastError), ""});
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
                    resourceRows.push_back({"CPU", paint(cpuSeverity, cpu.str()), ""});
                }
                resourceRows.push_back(
                    {"Memory",
                     paint(status.memoryUsageMb > 4096 ? Severity::Warn : Severity::Good,
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
                                        paint(workerSeverity, std::to_string(util) + "%"),
                                        worker.str()});
                render_rows(std::cout, resourceRows);

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
                    transport.push_back({"Mux", paint(muxSeverity, muxVal.str()), muxExtra.str()});
                    if (status.muxWriterBudgetBytes > 0) {
                        std::ostringstream pressureStr;
                        pressureStr << std::fixed << std::setprecision(1) << pressure << "%";
                        transport.push_back(
                            {"Mux pressure", paint(muxSeverity, pressureStr.str()), ""});
                    }
                }
                if (status.fsmTransitions || status.fsmHeaderReads || status.fsmPayloadWrites ||
                    status.fsmPayloadReads || status.fsmBytesSent || status.fsmBytesReceived) {
                    std::ostringstream fsm;
                    fsm << status.fsmTransitions << " transitions";
                    std::ostringstream fsmExtra;
                    fsmExtra << "hdr " << status.fsmHeaderReads << " · read "
                             << status.fsmPayloadReads << " · write " << status.fsmPayloadWrites;
                    transport.push_back({"IPC FSM", neutral(fsm.str()), fsmExtra.str()});
                    std::ostringstream bytes;
                    bytes << "sent " << status.fsmBytesSent << " · recv "
                          << status.fsmBytesReceived;
                    transport.push_back({"IPC bytes", neutral(bytes.str()), ""});
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
                searchRows.push_back({"Queries", paint(searchSeverity, base.str()), extra.str()});
                if (status.searchMetrics.concurrencyLimit > 0) {
                    searchRows.push_back(
                        {"Concurrency", std::to_string(status.searchMetrics.concurrencyLimit), ""});
                }
                render_rows(std::cout, searchRows);

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
                    storageRows.push_back({"Documents", neutral(docsVal.str()), ""});
                }
                const uint64_t logical = findCount("storage_logical_bytes");
                const uint64_t physical = findCount("physical_total_bytes");
                const uint64_t dedup = findCount("cas_dedup_saved_bytes");
                const uint64_t comp = findCount("cas_compress_saved_bytes");
                auto humanBytes = [](uint64_t b) {
                    const char* units[] = {"B", "KB", "MB", "GB", "TB"};
                    double val = static_cast<double>(b);
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
                    storageRows.push_back({"Storage", neutral(size.str()), ""});
                }
                if (dedup > 0 || comp > 0) {
                    std::ostringstream savings;
                    savings << "dedup " << humanBytes(dedup);
                    if (comp > 0)
                        savings << " · compress " << humanBytes(comp);
                    storageRows.push_back({"Savings", neutral(savings.str()), ""});
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

                Severity vecSeverity =
                    status.vectorDbReady
                        ? Severity::Good
                        : (status.vectorDbInitAttempted ? Severity::Warn : Severity::Warn);
                std::string vecText =
                    status.vectorDbReady
                        ? "Ready"
                        : (status.vectorDbInitAttempted ? "Initializing" : "Not initialized");
                std::string vecExtra;
                if (status.vectorDbDim > 0)
                    vecExtra = "dim " + std::to_string(status.vectorDbDim);
                storageRows.push_back({"Vector DB", paint(vecSeverity, vecText), vecExtra});

                Severity embSeverity = status.embeddingAvailable ? Severity::Good : Severity::Warn;
                std::string embText = status.embeddingAvailable ? "Available" : "Unavailable";
                std::string embExtra;
                if (!status.embeddingModel.empty())
                    embExtra += status.embeddingModel;
                if (!status.embeddingBackend.empty()) {
                    if (!embExtra.empty())
                        embExtra += " · ";
                    embExtra += status.embeddingBackend;
                }
                if (status.embeddingDim > 0) {
                    if (!embExtra.empty())
                        embExtra += " · ";
                    embExtra += "dim " + std::to_string(status.embeddingDim);
                }
                storageRows.push_back({"Embeddings", paint(embSeverity, embText), embExtra});

                if (!status.contentStoreRoot.empty()) {
                    storageRows.push_back(
                        {"Content root", status.contentStoreRoot, status.contentStoreError});
                }
                render_rows(std::cout, storageRows);

                std::cout << "\n" << section_header("Readiness") << "\n\n";
                std::vector<Row> readinessRows;
                auto readinessSorted = readinessList;
                std::sort(readinessSorted.begin(), readinessSorted.end(),
                          [](const ReadinessDisplay& a, const ReadinessDisplay& b) {
                              return a.label < b.label;
                          });
                for (const auto& rd : readinessSorted) {
                    readinessRows.push_back({rd.label, paint(rd.severity, rd.text), ""});
                }
                if (!readinessRows.empty())
                    render_rows(std::cout, readinessRows);

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
                    std::cout << "\n" << section_header("Top request counters") << "\n\n";

                    auto formatCountValue = [](const std::string& key,
                                               uint64_t value) -> std::string {
                        // Check if this is a byte counter
                        std::string lowerKey = key;
                        std::transform(lowerKey.begin(), lowerKey.end(), lowerKey.begin(),
                                       ::tolower);
                        const bool isBytes = lowerKey.find("bytes") != std::string::npos ||
                                             lowerKey.find("_mb") != std::string::npos ||
                                             lowerKey.find("_kb") != std::string::npos ||
                                             lowerKey.find("_gb") != std::string::npos;

                        if (isBytes && value > 1024) {
                            const char* units[] = {"B", "KB", "MB", "GB", "TB"};
                            double val = static_cast<double>(value);
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

                        // For non-byte large numbers, use thousand separators or "k" notation
                        if (value >= 100000) {
                            std::ostringstream oss;
                            oss << std::fixed << std::setprecision(value < 1000000 ? 0 : 1)
                                << (static_cast<double>(value) / 1000.0) << "k";
                            return oss.str();
                        } else if (value >= 10000) {
                            // Use comma separator for readability
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

                    std::vector<std::pair<std::string, size_t>> counts(status.requestCounts.begin(),
                                                                       status.requestCounts.end());
                    std::sort(counts.begin(), counts.end(),
                              [](const auto& a, const auto& b) { return a.second > b.second; });
                    std::vector<Row> countRows;
                    const std::size_t limit = std::min<std::size_t>(counts.size(), 10);
                    for (std::size_t i = 0; i < limit; ++i) {
                        countRows.push_back({humanizeToken(counts[i].first),
                                             formatCountValue(counts[i].first, counts[i].second),
                                             counts[i].first});
                    }
                    render_rows(std::cout, countRows);
                }

                if (!status.providers.empty()) {
                    std::cout << "\n" << section_header("Providers") << "\n\n";
                    std::vector<Row> providerRows;
                    const std::size_t limit = std::min<std::size_t>(status.providers.size(), 8);
                    for (std::size_t i = 0; i < limit; ++i) {
                        const auto& p = status.providers[i];
                        Severity provSeverity =
                            p.ready ? Severity::Good
                                    : (p.degraded ? Severity::Warn : Severity::Warn);
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
                             paint(provSeverity,
                                   p.ready ? "Ready" : (p.degraded ? "Degraded" : "Starting")),
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
                        modelRows.push_back({m.name, neutral(m.type), detail.str()});
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
        // Resolve socket path (do not persist unless explicitly provided)
        const std::string effectiveSocket =
            socketPath_.empty() ? daemon::DaemonClient::resolveSocketPathConfigFirst().string()
                                : socketPath_;

        // Stop daemon if running
        if (daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
            spdlog::info("Stopping YAMS daemon...");
            (void)runDaemonClient(
                {}, [](yams::daemon::DaemonClient& client) { return client.shutdown(true); },
                std::chrono::seconds(10));

            for (int i = 0; i < 10; i++) {
                if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket))
                    break;
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }

        // Start daemon
        spdlog::info("Starting YAMS daemon...");

        daemon::ClientConfig config;
        if (!socketPath_.empty())
            config.socketPath = socketPath_;
        if (!dataDir_.empty())
            config.dataDir = dataDir_;
        else if (cli_)
            config.dataDir = cli_->getDataPath();

        auto result = daemon::DaemonClient::startDaemon(config);
        if (!result) {
            spdlog::error("Failed to start daemon: {}", result.error().message);
            std::exit(1);
        }

        spdlog::info("YAMS daemon restarted successfully");
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
    YamsCLI* cli_ = nullptr;
};

// Factory function
std::unique_ptr<ICommand> createDaemonCommand() {
    return std::make_unique<DaemonCommand>();
}

} // namespace yams::cli
