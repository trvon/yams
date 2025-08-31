#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>
#include <yams/version.hpp>

#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>

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
        start->add_flag("--foreground", foreground_, "Run daemon in foreground (don't fork)");

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
        if (!socketPath.empty() && std::filesystem::exists(socketPath)) {
            if (std::filesystem::remove(socketPath)) {
                spdlog::debug("Removed stale socket file: {}", socketPath);
            }
        }

        // Remove PID file if it exists
        if (!pidFilePath.empty() && std::filesystem::exists(pidFilePath)) {
            if (std::filesystem::remove(pidFilePath)) {
                spdlog::debug("Removed stale PID file: {}", pidFilePath);
            }
        }
    }

    bool checkAndHandleVersionMismatch(const std::string& socketPath) {
        if (!daemon::DaemonClient::isDaemonRunning(socketPath)) {
            return false; // No daemon running, no version mismatch
        }

        // Connect and check version
        daemon::DaemonClient client;
        auto connectResult = client.connect();
        if (!connectResult) {
            spdlog::warn("Could not connect to running daemon to check version: {}",
                         connectResult.error().message);
            return true; // Assume daemon is running but unknown version
        }

        auto statusResult = client.status();
        if (!statusResult) {
            spdlog::warn("Could not get status from running daemon: {}",
                         statusResult.error().message);
            return true; // Assume daemon is running but unknown version
        }

        const auto& status = statusResult.value();

        if (!isVersionCompatible(status.version)) {
            spdlog::info("Stopping incompatible daemon (version {})...", status.version);

            // Try graceful shutdown via socket first
            auto shutdownResult = client.shutdown(true);
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
                return true; // Old daemon still running
            }
        }

        return true; // Compatible daemon is running
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
            spdlog::info("Compatible YAMS daemon is already running");
            return;
        }

        if (foreground_) {
            // Run daemon in current process (useful for debugging)
            spdlog::info("Starting YAMS daemon in foreground mode...");
            // TODO: Initialize and run daemon directly
            spdlog::error("Foreground mode not yet implemented");
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

            auto result = daemon::DaemonClient::startDaemon(config);
            if (!result) {
                spdlog::error("Failed to start daemon: {}", result.error().message);
                std::exit(1);
            }

            // Live spinner: poll status until Ready (or timeout)
            using namespace std::chrono_literals;
            daemon::DaemonClient client;
            std::string spinner = "|/-\\";
            size_t idx = 0;
            const auto timeout = 45s;
            auto start = std::chrono::steady_clock::now();

            // Try to connect repeatedly while waiting for the socket to appear
            while (std::chrono::steady_clock::now() - start < timeout) {
                auto connectRes = client.connect();
                if (!connectRes) {
                    std::this_thread::sleep_for(200ms);
                    continue;
                }
                break;
            }

            while (std::chrono::steady_clock::now() - start < timeout) {
                auto statusRes = client.status();
                if (statusRes) {
                    const auto& s = statusRes.value();
                    // Determine readiness
                    bool ready = s.ready || (s.overallStatus == "Ready");
                    // Compose a short status line with readiness/progress
                    std::ostringstream oss;
                    oss << "[" << spinner[idx % spinner.size()] << "] "
                        << (s.overallStatus.empty() ? (s.ready ? "Ready" : "Initializing")
                                                    : s.overallStatus)
                        << "  ";
                    size_t shown = 0;
                    for (const auto& kv : s.readinessStates) {
                        if (shown++ >= 4)
                            break; // keep it compact
                        oss << kv.first << ": " << (kv.second ? "ok" : "…");
                        auto it = s.initProgress.find(kv.first);
                        if (it != s.initProgress.end())
                            oss << " (" << (int)it->second << "%)";
                        oss << "  ";
                    }
                    std::cout << "\r" << oss.str() << std::flush;
                    if (ready) {
                        std::cout << "\n";
                        spdlog::info("YAMS daemon started successfully");
                        break;
                    }
                }
                idx++;
                std::this_thread::sleep_for(200ms);
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
            daemon::DaemonClient client;
            auto connectResult = client.connect();
            if (connectResult) {
                auto shutdownResult = client.shutdown(!force_);
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
                    spdlog::warn("Socket shutdown failed: {}", shutdownResult.error().message);
                }
            } else {
                spdlog::warn("Could not connect to daemon: {}", connectResult.error().message);
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

        // Check if daemon is running
        if (!daemon::DaemonClient::isDaemonRunning(socketPath_)) {
            std::cout << "YAMS daemon is not running\n";
            return;
        }

        if (detailed_) {
            // Connect and get detailed status
            daemon::DaemonClient client;

            auto connectResult = client.connect();
            if (!connectResult) {
                spdlog::error("Failed to connect to daemon: {}", connectResult.error().message);
                std::exit(1);
            }

            auto statusResult = client.status();
            if (!statusResult) {
                spdlog::error("Failed to get daemon status: {}", statusResult.error().message);
                std::exit(1);
            }

            const auto& status = statusResult.value();
            std::cout << "YAMS Daemon Status:\n";
            std::cout << "  Running: " << (status.running ? "yes" : "no") << "\n";
            std::cout << "  Version: " << status.version << "\n";
            std::cout << "  Uptime: " << status.uptimeSeconds << " seconds\n";
            std::cout << "  Requests processed: " << status.requestsProcessed << "\n";
            std::cout << "  Active connections: " << status.activeConnections << "\n";
            std::cout << "  Memory usage: " << status.memoryUsageMb << " MB\n";
            std::cout << "  CPU usage: " << status.cpuUsagePercent << "%\n";
        } else {
            std::cout << "YAMS daemon is running\n";
        }
    }

    void restartDaemon() {
        // Resolve socket path if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPath().string();
        }

        // Stop daemon if running
        if (daemon::DaemonClient::isDaemonRunning(socketPath_)) {
            spdlog::info("Stopping YAMS daemon...");

            daemon::DaemonClient client;
            if (auto result = client.connect(); result) {
                client.shutdown(true);
            }

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
    YamsCLI* cli_ = nullptr;
};

// Factory function
std::unique_ptr<ICommand> createDaemonCommand() {
    return std::make_unique<DaemonCommand>();
}

} // namespace yams::cli
