#include <yams/cli/async_bridge.h>
#include <yams/cli/command.h>
#include <yams/cli/progress_indicator.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/version.hpp>

#include <spdlog/spdlog.h>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#ifndef _WIN32
#include <unistd.h>
#endif
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
    }

    Result<void> execute() override {
        // This is called by the base command framework
        // The actual work is done in the callbacks above
        return Result<void>();
    }

private:
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
        yams::daemon::DaemonClient client{};
        auto statusResult = run_sync(client.status(), std::chrono::seconds(5));
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
            yams::daemon::DaemonClient shutClient{};
            auto shutdownResult = run_sync(shutClient.shutdown(true), std::chrono::seconds(10));
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
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPath().string();
        }
        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
        }

        spdlog::debug("Using socket: {}", socketPath_);
        spdlog::debug("Using PID file: {}", pidFile_);

        // Check if daemon is already running and handle version compatibility
        if (checkAndHandleVersionMismatch(socketPath_)) {
            spdlog::info("A YAMS daemon is already running");
            return;
        }

        // Determine foreground mode preference: start-level flag overrides global
        const bool runForeground = startForeground_ || foreground_;

        if (runForeground) {
            // Exec yams-daemon with provided flags, do not return on success
            spdlog::info("Starting YAMS daemon in foreground mode...");

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
            args.emplace_back("--socket");
            args.push_back(socketPath_);
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
            std::exit(1);
        } else {
            // Start daemon in background
            daemon::ClientConfig config;
            config.socketPath = socketPath_;
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

            auto result = daemon::DaemonClient::startDaemon(config);
            if (!result) {
                spdlog::error("Failed to start daemon: {}", result.error().message);
                std::exit(1);
            }

            // Live spinner: poll status until Ready (or timeout)
            using namespace std::chrono_literals;
            const auto timeout = 10s;
            auto t0 = std::chrono::steady_clock::now();

            // Use shared progress indicator for consistent TTY rendering
            ProgressIndicator pi(ProgressIndicator::Style::Spinner, true);
            pi.setUpdateInterval(100);
            pi.setShowCount(false);
            pi.start("Starting daemon…");

            // Brief grace period for socket binding before polling status
            std::this_thread::sleep_for(300ms);

            bool became_ready = false;
            // Poll status (even failures should keep the spinner visible)
            while (std::chrono::steady_clock::now() - t0 < timeout) {
                yams::daemon::DaemonClient probe{};
                auto statusRes = run_sync(probe.status(), std::chrono::seconds(2));
                if (statusRes) {
                    const auto& s = statusRes.value();
                    // TODO(PBI-007-06): Prefer StatusResponse.lifecycle_state/last_error when
                    // available.
                    const auto lifecycle = s.overallStatus.empty()
                                               ? (s.ready ? std::string("Ready")
                                                          : (s.running ? std::string("Starting")
                                                                       : std::string("Stopped")))
                                               : s.overallStatus;
                    bool ready = s.ready || lifecycle == "Ready" || lifecycle == "ready";
                    std::ostringstream msg;
                    msg << lifecycle;
                    size_t shown = 0;
                    for (const auto& kv : s.readinessStates) {
                        if (shown++ >= 4)
                            break;
                        msg << "  " << kv.first << ": " << (kv.second ? "ok" : "…");
                        auto it = s.initProgress.find(kv.first);
                        if (it != s.initProgress.end())
                            msg << " (" << (int)it->second << "%)";
                    }
                    // Render current status and keep spinner animating
                    pi.setMessage(msg.str());
                    pi.tick();
                    if (ready) {
                        pi.stop();
                        spdlog::info("YAMS daemon started successfully");
                        became_ready = true;
                        break;
                    }
                } else {
                    // transient errors right after spawn are expected (ECONNRESET, etc.)
                    pi.tick();
                }
                std::this_thread::sleep_for(200ms);
            }

            if (!became_ready) {
                pi.stop();
                spdlog::warn(
                    "Daemon start initiated. It will finish initializing in the background.");
                spdlog::warn("Tip: run 'yams daemon status -d' or check the log for progress.");
            }
        }
    }

    void stopDaemon() {
        // Resolve paths if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPath().string();
        }
        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
        }

        // Check if daemon is running
        bool daemonRunning = daemon::DaemonClient::isDaemonRunning(socketPath_);
        if (!daemonRunning) {
            // Check if there's a stale PID file
            pid_t pid = readPidFromFile(pidFile_);
            if (pid > 0 && kill(pid, 0) == 0) {
                spdlog::warn("Found daemon process (PID {}) not responding on socket", pid);
                daemonRunning = true;
            } else {
                spdlog::info("YAMS daemon is not running");
                cleanupDaemonFiles(socketPath_, pidFile_);
                return;
            }
        }

        bool stopped = false;

        // First try graceful shutdown via socket
        if (daemonRunning) {
            yams::daemon::DaemonClient shut{};
            daemon::ShutdownRequest sreq;
            sreq.graceful = !force_;
            {
                auto shutdownResult = run_sync(shut.shutdown(sreq.graceful), std::chrono::seconds(10));
                if (shutdownResult) {
                    spdlog::info("Sent shutdown request to daemon");

                    // Wait for daemon to stop
                    for (int i = 0; i < 30; i++) {
                        if (!daemon::DaemonClient::isDaemonRunning(socketPath_)) {
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
                        if (!daemon::DaemonClient::isDaemonRunning(socketPath_)) {
                            stopped = true;
                            spdlog::info("Daemon stopped successfully");
                            break;
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    }
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
            spdlog::info("YAMS daemon stopped successfully");
        } else {
            spdlog::error("Failed to stop YAMS daemon");
            spdlog::error("You may need to manually kill the process: pkill yams-daemon");
            std::exit(1);
        }
    }

    void showStatus() {
        // Resolve socket path if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPath().string();
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
                return;
            }
#endif
            std::cout << "YAMS daemon is not running\n";
            return;
        }

        if (!detailed_) {
            std::cout << "YAMS daemon is running\n";
            return;
        }

        // Detailed status via DaemonClient
        yams::daemon::DaemonClient client{};
        Error lastErr{};
        for (int attempt = 0; attempt < 5; ++attempt) {
            auto statusResult = run_sync(client.status(), std::chrono::seconds(5));
            if (statusResult) {
                const auto& status = statusResult.value();
                std::cout << "YAMS Daemon Status:\n";
                std::cout << "  Running: " << (status.running ? "yes" : "no") << "\n";
                std::cout << "  Version: " << status.version << "\n";
                if (!status.overallStatus.empty()) {
                    std::cout << "  Overall: " << status.overallStatus << "\n";
                }
                std::cout << "  Uptime: " << status.uptimeSeconds << " seconds\n";
                std::cout << "  Requests processed: " << status.requestsProcessed << "\n";
                std::cout << "  Active connections: " << status.activeConnections << "\n";
                std::cout << "  Memory usage: " << status.memoryUsageMb << " MB\n";
                std::cout << "  CPU usage: " << status.cpuUsagePercent << "%\n";
                if (!status.requestCounts.empty()) {
                    std::cout << "  Requests by type:\n";
                    for (const auto& [k, v] : status.requestCounts) {
                        std::cout << "    - " << k << ": " << v << "\n";
                    }
                }
                if (!status.readinessStates.empty()) {
                    std::cout << "  Readiness:\n";
                    for (const auto& [k, v] : status.readinessStates) {
                        std::cout << "    - " << k << ": " << (v ? "ok" : "…") << "\n";
                    }
                }
                if (!status.initProgress.empty()) {
                    std::cout << "  Initialization progress:\n";
                    for (const auto& [k, v] : status.initProgress) {
                        std::cout << "    - " << k << ": " << static_cast<int>(v) << "%\n";
                    }
                }
                return;
            }
            lastErr = statusResult.error();
            std::this_thread::sleep_for(std::chrono::milliseconds(120 * (attempt + 1)));
        }
        spdlog::error("Failed to get daemon status: {}", lastErr.message);
        std::exit(1);
    }

    void restartDaemon() {
        // Resolve socket path if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPath().string();
        }

        // Stop daemon if running
        if (daemon::DaemonClient::isDaemonRunning(socketPath_)) {
            spdlog::info("Stopping YAMS daemon...");
            yams::daemon::DaemonClient client{};
            daemon::ShutdownRequest sreq;
            sreq.graceful = true;
            (void)run_sync(client.shutdown(true), std::chrono::seconds(10));

            // Wait for daemon to stop
            for (int i = 0; i < 10; i++) {
                if (!daemon::DaemonClient::isDaemonRunning(socketPath_)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }

        // Start daemon
        spdlog::info("Starting YAMS daemon...");

        daemon::ClientConfig config;
        config.socketPath = socketPath_;
        if (!dataDir_.empty()) {
            config.dataDir = dataDir_;
        } else if (cli_) {
            config.dataDir = cli_->getDataPath();
        }

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
