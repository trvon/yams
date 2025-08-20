#include <yams/daemon/daemon.h>

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#include <CLI/CLI.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <yams/config/config_migration.h>

// POSIX headers for daemonization
#include <fcntl.h>     // for open(), O_RDONLY, O_RDWR
#include <unistd.h>    // for fork(), setsid(), chdir(), close()
#include <sys/types.h> // for pid_t

int main(int argc, char* argv[]) {
    CLI::App app{"YAMS Daemon - background service"};

    // Configuration options
    yams::daemon::DaemonConfig config;
    std::string configPath;

    app.add_option("--config", configPath, "Configuration file path");
    app.add_option("--socket", config.socketPath, "Unix domain socket path");
    app.add_option("--data-dir,--storage", config.dataDir, "Data directory for storage");

    app.add_option("--pid-file", config.pidFile, "PID file path");

    app.add_option("--log-file", config.logFile, "Log file path");

    app.add_option("--workers", config.workerThreads, "Number of worker threads")->default_val(4);

    app.add_option("--max-memory", config.maxMemoryGb, "Maximum memory usage (GB)")->default_val(4);

    app.add_option("--log-level", config.logLevel, "Log level (trace/debug/info/warn/error)")
        ->default_val("info");

    bool foreground = false;
    app.add_flag("-f,--foreground", foreground, "Run in foreground (don't daemonize)");

    CLI11_PARSE(app, argc, argv);

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
            } else {
                // Daemon section missing (probably old config) - use safe defaults
                spdlog::info(
                    "Daemon configuration section not found in config file, using defaults");
                // Don't enable model provider by default for backward compatibility
                config.enableModelProvider = false;
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

    // If dataDir not specified, allow YAMS_STORAGE env to define it
    if (config.dataDir.empty()) {
        if (const char* storageEnv = std::getenv("YAMS_STORAGE")) {
            config.dataDir = std::filesystem::path(storageEnv);
        }
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
        auto logger = spdlog::basic_logger_mt("yams-daemon", config.logFile.string(), true);
        spdlog::set_default_logger(logger);
        spdlog::flush_on(spdlog::level::info);
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
        while (daemon.isRunning()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        return 0;

    } catch (const std::exception& e) {
        spdlog::error("Daemon error: {}", e.what());
        return 1;
    }
}
