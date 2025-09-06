#include <yams/daemon/daemon.h>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <yams/config/config_migration.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>

// POSIX headers for daemonization
#include <fcntl.h>     // for open(), O_RDONLY, O_RDWR
#include <unistd.h>    // for fork(), setsid(), chdir(), close()
#include <sys/types.h> // for pid_t

// Fatal signal/backtrace support
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <sstream>
#include <string>
#include <thread>
#if !defined(_WIN32)
#include <execinfo.h>
#endif

namespace {
void log_fatal(const char* what) {
    try {
        // Attempt to log via spdlog (if initialized)
        spdlog::critical("FATAL: {}", what);
        spdlog::critical("Aborting after fatal error");
    } catch (...) {
        // Fallback to stderr if logger is not ready
        std::fprintf(stderr, "FATAL: %s\n", what);
    }
}

void signal_handler(int signo) {
    const char* sigstr = (signo == SIGSEGV)   ? "SIGSEGV"
                         : (signo == SIGABRT) ? "SIGABRT"
                                              : "UNKNOWN";
    log_fatal(sigstr);
#if !defined(_WIN32)
    // Capture and log a backtrace
    void* bt[64];
    int n = backtrace(bt, 64);
    char** syms = backtrace_symbols(bt, n);
    if (syms) {
        for (int i = 0; i < n; ++i) {
            spdlog::critical("Backtrace[{}]: {}", i, syms[i]);
        }
        free(syms);
    }
#endif
    // Give logger a moment to flush
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::_Exit(128 + signo);
}

void setup_fatal_handlers() {
    std::signal(SIGSEGV, signal_handler);
    std::signal(SIGABRT, signal_handler);
    std::set_terminate([]() noexcept {
        log_fatal("std::terminate called");
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::_Exit(1);
    });
}
} // namespace

int main(int argc, char* argv[]) {
    // Install fatal handlers as early as possible
    setup_fatal_handlers();

    CLI::App app{"YAMS Daemon - background service"};

    // Configuration options
    yams::daemon::DaemonConfig config;
    std::string configPath;
    // Capture CLI-provided data dir separately to enforce precedence (config > env > CLI)
    std::filesystem::path cliDataDir;

    app.add_option("--config", configPath, "Configuration file path");
    app.add_option("--socket", config.socketPath, "Unix domain socket path");
    auto* dataDirOpt =
        app.add_option("--data-dir,--storage", cliDataDir, "Data directory for storage");

    app.add_option("--pid-file", config.pidFile, "PID file path");

    app.add_option("--log-file", config.logFile, "Log file path");

    app.add_option("--workers", config.workerThreads, "Number of worker threads")->default_val(0);

    app.add_option("--max-memory", config.maxMemoryGb, "Maximum memory usage (GB)")->default_val(8);

    app.add_option("--log-level", config.logLevel, "Log level (trace/debug/info/warn/error)")
        ->default_val("info");

    // Plugin configuration options
    app.add_option("--plugin-dir", config.pluginDir, "Directory containing plugins");
    bool noPlugins = false;
    app.add_flag("--no-plugins", noPlugins, "Disable plugin loading");

    bool foreground = false;
    app.add_flag("-f,--foreground", foreground, "Run in foreground (don't daemonize)");

    CLI11_PARSE(app, argc, argv);

    // Apply the no-plugins flag
    if (noPlugins) {
        config.autoLoadPlugins = false;
    }

    // Load configuration from TOML file
    namespace fs = std::filesystem;
    if (configPath.empty()) {
        // Default to standard config location
        if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            configPath = (fs::path(xdgConfigHome) / "yams" / "config.toml").string();
        } else if (const char* homeEnv = std::getenv("HOME")) {
            configPath = (fs::path(homeEnv) / ".config" / "yams" / "config.toml").string();
        }
    }

    // Load and parse config if it exists
    if (!configPath.empty() && fs::exists(configPath)) {
        yams::config::ConfigMigrator migrator;
        auto parseResult = migrator.parseTomlConfig(configPath);
        if (parseResult) {
            const auto& tomlConfig = parseResult.value();

            // Check if daemon section exists, if not use defaults
            bool hasDaemonSection = (tomlConfig.find("daemon") != tomlConfig.end());

            if (hasDaemonSection) {
                // Load daemon configuration from file
                const auto& daemonSection = tomlConfig.at("daemon");

                // Socket path
                if (config.socketPath.empty() &&
                    daemonSection.find("socket_path") != daemonSection.end()) {
                    config.socketPath = fs::path(daemonSection.at("socket_path"));
                }

                // PID file
                if (config.pidFile.empty() &&
                    daemonSection.find("pid_file") != daemonSection.end()) {
                    config.pidFile = fs::path(daemonSection.at("pid_file"));
                }

                // Data directory (aka storage) — allow multiple key forms for compatibility
                if (config.dataDir.empty()) {
                    if (auto it = daemonSection.find("data_dir"); it != daemonSection.end()) {
                        config.dataDir = fs::path(it->second);
                    } else if (auto it2 = daemonSection.find("storage");
                               it2 != daemonSection.end()) {
                        config.dataDir = fs::path(it2->second);
                    } else if (auto it3 = daemonSection.find("storage_path");
                               it3 != daemonSection.end()) {
                        config.dataDir = fs::path(it3->second);
                    }
                }

                // Worker threads
                if (daemonSection.find("worker_threads") != daemonSection.end()) {
                    try {
                        config.workerThreads = std::stoi(daemonSection.at("worker_threads"));
                    } catch (...) {
                    }
                }

                // Max memory
                if (daemonSection.find("max_memory_gb") != daemonSection.end()) {
                    try {
                        config.maxMemoryGb = std::stoi(daemonSection.at("max_memory_gb"));
                    } catch (...) {
                    }
                }

                // Log level
                if (config.logLevel.empty() &&
                    daemonSection.find("log_level") != daemonSection.end()) {
                    config.logLevel = daemonSection.at("log_level");
                }

                // Enable model provider based on config
                if (daemonSection.find("enable") != daemonSection.end()) {
                    config.enableModelProvider = (daemonSection.at("enable") == "true");
                }

                // Plugin configuration
                if (config.pluginDir.empty() &&
                    daemonSection.find("plugin_dir") != daemonSection.end()) {
                    config.pluginDir = fs::path(daemonSection.at("plugin_dir"));
                }

                if (daemonSection.find("auto_load_plugins") != daemonSection.end()) {
                    config.autoLoadPlugins = (daemonSection.at("auto_load_plugins") == "true");
                }
            } else {
                // Daemon section missing (probably old config) - use safe defaults
                spdlog::info(
                    "Daemon configuration section not found in config file, using defaults");
                // Enable model provider by default if plugins will be loaded
                config.enableModelProvider = config.autoLoadPlugins;
            }

            // Honor [embeddings].enable=false: hard-disable model provider regardless of [daemon]
            // This prevents startup from waiting on embedding services when embeddings are
            // disabled.
            if (tomlConfig.find("embeddings") != tomlConfig.end()) {
                const auto& embSection = tomlConfig.at("embeddings");
                auto it = embSection.find("enable");
                if (it != embSection.end()) {
                    std::string v = it->second;
                    // normalize value to lowercase for comparison
                    for (auto& c : v)
                        c = static_cast<char>(std::tolower(c));
                    if (v == "false" || v == "0" || v == "no" || v == "off") {
                        if (config.enableModelProvider) {
                            spdlog::info("Embeddings disabled in config "
                                         "([embeddings].enable=false); disabling model provider");
                        }
                        config.enableModelProvider = false;
                        // Also short-circuit plugin auto-loading to avoid unnecessary startup work
                        if (config.autoLoadPlugins) {
                            spdlog::info("Embeddings disabled; skipping plugin auto-loading to "
                                         "speed up startup");
                        }
                        config.autoLoadPlugins = false;
                    }
                }
            }

            // Load daemon.models configuration if present
            if (config.enableModelProvider &&
                tomlConfig.find("daemon.models") != tomlConfig.end()) {
                const auto& modelsSection = tomlConfig.at("daemon.models");

                if (modelsSection.find("max_loaded_models") != modelsSection.end()) {
                    try {
                        config.modelPoolConfig.maxLoadedModels =
                            std::stoul(modelsSection.at("max_loaded_models"));
                    } catch (...) {
                    }
                }

                if (modelsSection.find("hot_pool_size") != modelsSection.end()) {
                    try {
                        config.modelPoolConfig.hotPoolSize =
                            std::stoul(modelsSection.at("hot_pool_size"));
                    } catch (...) {
                    }
                }

                if (modelsSection.find("lazy_loading") != modelsSection.end()) {
                    config.modelPoolConfig.lazyLoading =
                        (modelsSection.at("lazy_loading") == "true");
                }
            }

            // Load daemon.download scaffolding (feature disabled by default)
            if (tomlConfig.find("daemon.download") != tomlConfig.end()) {
                const auto& dlSection = tomlConfig.at("daemon.download");

                // Enable flag (default false)
                if (dlSection.find("enable") != dlSection.end()) {
                    config.downloadPolicy.enable = (dlSection.at("enable") == "true");
                }

                // Allowed hosts (comma or array in migrator-normalized string)
                if (dlSection.find("allowed_hosts") != dlSection.end()) {
                    // Expect a comma-separated list; split on ',' and trim
                    std::stringstream ss(dlSection.at("allowed_hosts"));
                    std::string host;
                    while (std::getline(ss, host, ',')) {
                        // trim spaces
                        auto start = host.find_first_not_of(" \t");
                        auto end = host.find_last_not_of(" \t");
                        if (start != std::string::npos) {
                            config.downloadPolicy.allowedHosts.emplace_back(
                                host.substr(start, end - start + 1));
                        }
                    }
                }

                // Allowed schemes
                if (dlSection.find("allowed_schemes") != dlSection.end()) {
                    config.downloadPolicy.allowedSchemes.clear();
                    std::stringstream ss(dlSection.at("allowed_schemes"));
                    std::string scheme;
                    while (std::getline(ss, scheme, ',')) {
                        auto start = scheme.find_first_not_of(" \t");
                        auto end = scheme.find_last_not_of(" \t");
                        if (start != std::string::npos) {
                            config.downloadPolicy.allowedSchemes.emplace_back(
                                scheme.substr(start, end - start + 1));
                        }
                    }
                }

                // Require checksum
                if (dlSection.find("require_checksum") != dlSection.end()) {
                    config.downloadPolicy.requireChecksum =
                        (dlSection.at("require_checksum") == "true");
                }

                // Store only (CAS-only)
                if (dlSection.find("store_only") != dlSection.end()) {
                    config.downloadPolicy.storeOnly = (dlSection.at("store_only") == "true");
                }

                // Timeout
                if (dlSection.find("timeout_ms") != dlSection.end()) {
                    try {
                        config.downloadPolicy.timeout =
                            std::chrono::milliseconds(std::stoll(dlSection.at("timeout_ms")));
                    } catch (...) {
                    }
                }

                // Max file bytes
                if (dlSection.find("max_file_bytes") != dlSection.end()) {
                    try {
                        config.downloadPolicy.maxFileBytes =
                            static_cast<std::uint64_t>(std::stoull(dlSection.at("max_file_bytes")));
                    } catch (...) {
                    }
                }

                // Rate limit RPS
                if (dlSection.find("rate_limit_rps") != dlSection.end()) {
                    try {
                        config.downloadPolicy.rateLimitRps =
                            static_cast<size_t>(std::stoul(dlSection.at("rate_limit_rps")));
                    } catch (...) {
                    }
                }

                // Sandbox strategy
                if (dlSection.find("sandbox") != dlSection.end()) {
                    config.downloadPolicy.sandbox = dlSection.at("sandbox");
                }
            }

            // Continue processing daemon.models only if section still exists
            if (config.enableModelProvider &&
                tomlConfig.find("daemon.models") != tomlConfig.end()) {
                const auto& modelsSection = tomlConfig.at("daemon.models");

                if (modelsSection.find("enable_gpu") != modelsSection.end()) {
                    config.modelPoolConfig.enableGPU = (modelsSection.at("enable_gpu") == "true");
                }

                if (modelsSection.find("num_threads") != modelsSection.end()) {
                    try {
                        config.modelPoolConfig.numThreads =
                            std::stoi(modelsSection.at("num_threads"));
                    } catch (...) {
                    }
                }

                // Parse preload models list
                if (modelsSection.find("preload_models") != modelsSection.end()) {
                    std::string modelsStr = modelsSection.at("preload_models");
                    // Remove brackets and quotes
                    modelsStr.erase(std::remove(modelsStr.begin(), modelsStr.end(), '['),
                                    modelsStr.end());
                    modelsStr.erase(std::remove(modelsStr.begin(), modelsStr.end(), ']'),
                                    modelsStr.end());
                    modelsStr.erase(std::remove(modelsStr.begin(), modelsStr.end(), '"'),
                                    modelsStr.end());
                    modelsStr.erase(std::remove(modelsStr.begin(), modelsStr.end(), ' '),
                                    modelsStr.end());

                    // Split by comma
                    std::stringstream ss(modelsStr);
                    std::string model;
                    while (std::getline(ss, model, ',')) {
                        if (!model.empty()) {
                            config.modelPoolConfig.preloadModels.push_back(model);
                        }
                    }
                }
            }

            // Load core configuration for data directory
            if (config.dataDir.empty() && tomlConfig.find("core") != tomlConfig.end()) {
                const auto& coreSection = tomlConfig.at("core");
                if (coreSection.find("data_dir") != coreSection.end()) {
                    std::string dataDir = coreSection.at("data_dir");
                    // Expand ~ to home directory
                    if (!dataDir.empty() && dataDir[0] == '~') {
                        if (const char* homeEnv = std::getenv("HOME")) {
                            dataDir = fs::path(homeEnv) / dataDir.substr(2);
                        }
                    }
                    config.dataDir = fs::path(dataDir);
                }
            }
        }
    }

    // If dataDir not specified, allow environment to define it (YAMS_STORAGE or YAMS_DATA_DIR)
    if (config.dataDir.empty()) {
        if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
            config.dataDir = std::filesystem::path(storageEnv);
        } else if (const char* dataEnv = std::getenv("YAMS_DATA_DIR")) {
            config.dataDir = std::filesystem::path(dataEnv);
        }
    }

    // Lastly, if still not specified and CLI provided --data-dir, use it as the lowest priority
    if (config.dataDir.empty() && dataDirOpt && dataDirOpt->count() > 0) {
        config.dataDir = cliDataDir;
    }

    // Resolve log file path if not specified
    if (config.logFile.empty()) {
        config.logFile = yams::daemon::YamsDaemon::resolveSystemPath(
            yams::daemon::YamsDaemon::PathType::LogFile);
    }

    // Create log directory if needed
    try {
        std::filesystem::create_directories(config.logFile.parent_path());
    } catch (...) {
        // Best effort
    }

    // Configure logging (default to file to preserve logs after daemonizing)
    try {
        // Use rotating file sink to preserve logs across crashes
        const size_t max_size = 10 * 1024 * 1024; // 10MB per file
        const size_t max_files = 5;               // Keep 5 rotated files
        auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            config.logFile.string(), max_size, max_files);
        auto logger = std::make_shared<spdlog::logger>("yams-daemon", rotating_sink);
        spdlog::set_default_logger(logger);
        spdlog::flush_on(spdlog::level::info);

        // Log rotation info
        spdlog::info("Log rotation enabled: {} (max {}MB x {} files)", config.logFile.string(),
                     max_size / (1024 * 1024), max_files);
    } catch (...) {
        // Fallback silently to default logger
    }

    if (config.logLevel == "trace") {
        spdlog::set_level(spdlog::level::trace);
    } else if (config.logLevel == "debug") {
        spdlog::set_level(spdlog::level::debug);
    } else if (config.logLevel == "info") {
        spdlog::set_level(spdlog::level::info);
    } else if (config.logLevel == "warn") {
        spdlog::set_level(spdlog::level::warn);
    } else if (config.logLevel == "error") {
        spdlog::set_level(spdlog::level::err);
    }

    // Configure FSM metrics based on log level
    // Enable FSM metrics only for debug or trace levels to avoid introducing new tags
    {
        bool enableFsmMetrics = (config.logLevel == "debug" || config.logLevel == "trace");
        yams::daemon::FsmMetricsRegistry::instance().setEnabled(enableFsmMetrics);
        if (enableFsmMetrics) {
            spdlog::debug("FSM metrics collection enabled");
        }
    }

    // Best-effort: notify about config v2 migration before daemonizing (visible to user)
    try {
        namespace fs = std::filesystem;
        fs::path defaultConfigPath;
        if (const char* xdgConfigHome = std::getenv("XDG_CONFIG_HOME")) {
            defaultConfigPath = fs::path(xdgConfigHome) / "yams" / "config.toml";
        } else if (const char* homeEnv = std::getenv("HOME")) {
            defaultConfigPath = fs::path(homeEnv) / ".config" / "yams" / "config.toml";
        }
        if (!defaultConfigPath.empty()) {
            yams::config::ConfigMigrator migrator;
            auto need = migrator.needsMigration(defaultConfigPath);
            if (need && need.value()) {
                spdlog::warn("Configuration migration to v2 is required. Run: yams config migrate");
            }
        }
    } catch (...) {
        // Non-fatal: ignore config check errors in daemon startup
    }

    // Daemonize if not running in foreground
    if (!foreground) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Failed to fork daemon process" << std::endl;
            return 1;
        }
        if (pid > 0) {
            // Parent process exits
            std::cout << "Daemon started with PID: " << pid << std::endl;
            return 0;
        }

        // Child process continues as daemon
        // Create new session
        if (setsid() < 0) {
            std::cerr << "Failed to create new session" << std::endl;
            return 1;
        }

        // Change working directory to root
        if (chdir("/") < 0) {
            std::cerr << "Failed to change working directory" << std::endl;
            return 1;
        }

        // Close standard file descriptors (file logger remains active)
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);

        // Redirect to /dev/null
        open("/dev/null", O_RDONLY); // stdin
        open("/dev/null", O_RDWR);   // stdout
        open("/dev/null", O_RDWR);   // stderr
    }

    try {
        yams::daemon::YamsDaemon daemon(config);

        auto result = daemon.start();
        if (!result) {
            spdlog::error("Failed to start daemon: {}", result.error().message);
            return 1;
        }

        // Keep running until shutdown signal
        while (daemon.isRunning() && !daemon.isStopRequested()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Ensure proper cleanup
        daemon.stop();

        return 0;

    } catch (const std::exception& e) {
        spdlog::error("Daemon error: {}", e.what());
        return 1;
    }
}
