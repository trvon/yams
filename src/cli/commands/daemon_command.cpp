#include <nlohmann/json.hpp>
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
            std::cout << "[INFO] A YAMS daemon is already running\n";
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
                        std::cout << "[OK] YAMS daemon started successfully\n";
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
                std::cout << "[INFO] Daemon start initiated. It will finish initializing in the "
                             "background.\n";
                std::cout
                    << "[INFO] Tip: run 'yams daemon status -d' or check the log for progress.\n";
            }
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
            yams::daemon::DaemonClient shut{};
            daemon::ShutdownRequest sreq;
            sreq.graceful = !force_;
            {
                auto shutdownResult =
                    run_sync(shut.shutdown(sreq.graceful), std::chrono::seconds(10));
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
        if (effectiveSocket.empty()) {
            std::cout << "Socket: <empty> (resolve failed)\n";
        } else {
            std::cout << "Socket: " << effectiveSocket << "\n";
        }
        if (pidFile_.empty()) {
            pidFile_ = daemon::YamsDaemon::resolveSystemPath(daemon::YamsDaemon::PathType::PidFile)
                           .string();
        }
        std::cout << "PID file: " << pidFile_ << "\n";
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
#ifndef _WIN32
        if (!effectiveSocket.empty()) {
            size_t maxlen = sizeof(sockaddr_un::sun_path);
            if (effectiveSocket.size() >= maxlen) {
                std::cout << "[FAIL] Socket path too long for AF_UNIX (" << effectiveSocket.size()
                          << "/" << maxlen << ")\n";
                ok = false;
                std::cout << "      Suggestion: export YAMS_DAEMON_SOCKET=/tmp/yams-daemon-$(id "
                             "-u).sock\n";
            } else {
                std::cout << "[OK] Socket path length within limit\n";
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
                std::cout << "[OK] Socket directory writable: " << parent.string() << "\n";
            } else {
                std::cout << "[FAIL] Cannot write to socket directory: " << parent.string() << "\n";
                ok = false;
            }
        }
        // Check for stale socket (and show readiness summary if daemon responds)
        bool socket_exists = !effectiveSocket.empty() && fs::exists(effectiveSocket);
        if (socket_exists) {
            std::cout << "[INFO] Socket file exists. Probing server...\n";
            setenv("YAMS_CLIENT_DEBUG", "1", 1);
            bool alive = daemon::DaemonClient::isDaemonRunning(effectiveSocket);
            if (alive) {
                std::cout << "[OK] Daemon responds on socket\n";
                // Fetch detailed readiness and show a short Waiting on: summary
                try {
                    yams::daemon::DaemonClient probe{};
                    auto sres = run_sync(probe.status(), std::chrono::seconds(2));
                    if (sres) {
                        const auto& s = sres.value();
                        // Show any components not ready yet (up to a few for brevity)
                        std::vector<std::string> waiting;
                        for (const auto& [k, v] : s.readinessStates) {
                            if (!v) {
                                std::ostringstream w;
                                w << k;
                                auto it = s.initProgress.find(k);
                                if (it != s.initProgress.end()) {
                                    w << " (" << (int)it->second << "%)";
                                }
                                // Approximate elapsed with daemon uptime (per-component timing TBD)
                                w << ", elapsed ~" << s.uptimeSeconds << "s";
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
                        // Additionally, show top 3 slowest components if bootstrap has timings
                        try {
                            auto rt =
                                daemon::YamsDaemon::getXDGRuntimeDir() / "yams-daemon.status.json";
                            std::ifstream bf(rt);
                            if (bf) {
                                nlohmann::json j;
                                bf >> j;
                                if (j.contains("top_slowest") && j["top_slowest"].is_array() &&
                                    !j["top_slowest"].empty()) {
                                    std::cout << "[INFO] Top slow components: ";
                                    auto arr = j["top_slowest"];
                                    for (size_t i = 0; i < arr.size() && i < 3; ++i) {
                                        const auto& e = arr[i];
                                        std::string name = e.value("name", std::string{"unknown"});
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
                        } catch (...) {
                        }
                    }
                } catch (...) {
                }
            } else {
                std::cout << "[WARN] No response on socket. File may be stale.\n";
                if (confirm("Remove stale socket file now?")) {
                    std::error_code ec;
                    fs::remove(effectiveSocket, ec);
                    if (ec) {
                        std::cout << "[FAIL] Failed to remove socket: " << ec.message() << "\n";
                    } else {
                        std::cout << "[OK] Removed stale socket file\n";
                    }
                }
            }
        } else {
            std::cout << "[INFO] Socket file not present yet\n";
        }
        // PID check
        pid_t pid = readPidFromFile(pidFile_);
#ifndef _WIN32
        if (pid > 0 && kill(pid, 0) == 0) {
            std::cout << "[OK] PID file references running process: " << pid << "\n";
        } else if (pid > 0) {
            std::cout << "[WARN] PID file references non-running process: " << pid << "\n";
            if (confirm("Remove stale PID file now?")) {
                std::error_code ec;
                fs::remove(pidFile_, ec);
                if (ec) {
                    std::cout << "[FAIL] Failed to remove PID file: " << ec.message() << "\n";
                } else {
                    std::cout << "[OK] Removed PID file\n";
                }
            }
        } else {
            std::cout << "[INFO] No PID file or unreadable\n";
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
                    std::cout
                        << "[INFO] Daemon process detected: initializing (IPC not yet available)\n";
                }
            } catch (...) {
                // ignore errors from /proc scanning
            }
        }
#else
        if (pid > 0) {
            std::cout << "[INFO] PID recorded: " << pid << "\n";
        }
#endif
        if (!ok) {
            std::cout << "One or more checks failed. Consider setting YAMS_DAEMON_SOCKET to a "
                         "short path (e.g., /tmp/yams-daemon-$(id -u).sock) and retry.\n";
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
            // Linux: as a best-effort fallback, detect a launching daemon by scanning /proc
            // for a yams-daemon process when socket and PID file are not yet ready.
            try {
                namespace fs = std::filesystem;
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
                        std::cout
                            << "YAMS daemon is starting/initializing (IPC not yet available)\n";
                        if (detailed_) {
                            try {
                                auto logPath = daemon::YamsDaemon::resolveSystemPath(
                                                   daemon::YamsDaemon::PathType::LogFile)
                                                   .string();
                                std::cout
                                    << "[INFO] Likely waiting on search stack (index, models).\n";
                                std::cout << "[INFO] Tail log for details: " << logPath << "\n";
                            } catch (...) {
                            }
                        }
                        return;
                    }
                }
            } catch (...) {
                // Ignore any /proc scanning errors; fall through to not running message
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
            // Keep client debug enabled while polling
            setenv("YAMS_CLIENT_DEBUG", detailed_ ? "1" : "0", 1);
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
        // Resolve socket path (do not persist unless explicitly provided)
        const std::string effectiveSocket =
            socketPath_.empty() ? daemon::DaemonClient::resolveSocketPathConfigFirst().string()
                                : socketPath_;

        // Stop daemon if running
        if (daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
            spdlog::info("Stopping YAMS daemon...");
            yams::daemon::DaemonClient client{};
            daemon::ShutdownRequest sreq;
            sreq.graceful = true;
            (void)run_sync(client.shutdown(true), std::chrono::seconds(10));

            // Wait for daemon to stop
            for (int i = 0; i < 10; i++) {
                if (!daemon::DaemonClient::isDaemonRunning(effectiveSocket)) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }

        // Start daemon
        spdlog::info("Starting YAMS daemon...");

        daemon::ClientConfig config;
        if (!socketPath_.empty()) {
            config.socketPath = socketPath_;
        }
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
static pid_t readPidFromFile(const std::string& path) {
    pid_t pid = 0;
    if (path.empty())
        return pid;
    std::ifstream in(path);
    if (!in)
        return pid;
    in >> pid;
    return pid;
}
