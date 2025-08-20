#include <yams/cli/command.h>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/daemon.h>

#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

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

        // Check if daemon is already running
        if (daemon::DaemonClient::isDaemonRunning(socketPath_)) {
            spdlog::info("YAMS daemon is already running");
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

            spdlog::info("YAMS daemon started successfully");
        }
    }

    void stopDaemon() {
        // Resolve socket path if not explicitly provided
        if (socketPath_.empty()) {
            socketPath_ = daemon::DaemonClient::resolveSocketPath().string();
        }

        // Check if daemon is running
        if (!daemon::DaemonClient::isDaemonRunning(socketPath_)) {
            spdlog::info("YAMS daemon is not running");
            return;
        }

        // Connect to daemon and send shutdown request
        daemon::DaemonClient client;

        auto connectResult = client.connect();
        if (!connectResult) {
            spdlog::error("Failed to connect to daemon: {}", connectResult.error().message);
            std::exit(1);
        }

        auto shutdownResult = client.shutdown(!force_);
        if (!shutdownResult) {
            spdlog::error("Failed to shutdown daemon: {}", shutdownResult.error().message);
            std::exit(1);
        }

        spdlog::info("YAMS daemon stopped successfully");
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
