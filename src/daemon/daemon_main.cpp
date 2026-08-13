#include <yams/common/fs_utils.h>
#include <yams/daemon/daemon.h>
#include <yams/platform/windows_init.h>

#include <nlohmann/json.hpp>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>
#if __has_include(<CLI/CLI.hpp>)
#include <CLI/CLI.hpp>
#define YAMS_HAS_CLI11 1
#else
#define YAMS_HAS_CLI11 0
#endif

#include <filesystem>
#include <fstream>
#include <iostream>
#include <yams/compat/unistd.h>
#include <yams/config/config_helpers.h>
#include <yams/config/config_migration.h>
#include <yams/daemon/components/ConfigResolver.h>
#include <yams/daemon/ipc/fsm_metrics_registry.h>

// POSIX headers for daemonization
#ifndef _WIN32
#include <fcntl.h>     // for open(), O_RDONLY, O_RDWR
#include <unistd.h>    // for fork(), setsid(), chdir(), close()
#include <sys/types.h> // for pid_t
#endif

// Fatal signal/backtrace support
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <sstream>
#include <string>
#include <thread>
#if !defined(_WIN32) && !defined(__ANDROID__)
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
#if !defined(_WIN32) && !defined(__ANDROID__)
    // Capture and log a backtrace
    void* bt[64];
    int n = backtrace(bt, 64);
    char** syms = backtrace_symbols(bt, n);
    if (syms) {
        for (int i = 0; i < n; ++i) {
            spdlog::critical("Backtrace[{}]: {}", i, syms[i]);
        }
        free(static_cast<void*>(syms));
    }
#endif
    // Give logger a moment to flush
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::_Exit(128 + signo);
}

static std::atomic<bool> g_autoload_plugins_now{false};
static std::atomic<bool> g_shutdown_requested{false};

void setup_fatal_handlers() {
    std::signal(SIGSEGV, signal_handler);
    std::signal(SIGABRT, signal_handler);
    // Ensure SIGTERM/SIGINT terminate the daemon promptly if graceful shutdown isn't possible.
#ifdef SIGTERM
    std::signal(SIGTERM, [](int) { g_shutdown_requested.store(true, std::memory_order_relaxed); });
#endif
#ifdef SIGINT
    std::signal(SIGINT, [](int) { g_shutdown_requested.store(true, std::memory_order_relaxed); });
#endif
    // Use SIGUSR1 as on-demand plugin autoload trigger
#ifdef SIGUSR1
    std::signal(SIGUSR1,
                [](int) { g_autoload_plugins_now.store(true, std::memory_order_relaxed); });
#endif
    std::set_terminate([]() noexcept {
        log_fatal("std::terminate called");
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        std::_Exit(1);
    });
}

void clear_session_index_on_start() {
    const auto root = yams::config::get_state_dir();

    auto index = root / "sessions" / "index.json";
    if (!std::filesystem::exists(index)) {
        return;
    }

    try {
        std::ifstream in(index);
        if (!in.good()) {
            return;
        }
        nlohmann::json j;
        in >> j;
        if (!j.contains("current")) {
            return;
        }
        j.erase("current");
        if (j.empty()) {
            std::error_code ec;
            std::filesystem::remove(index, ec);
            if (!ec) {
                spdlog::info("Cleared session index (removed {})", index.string());
            }
            return;
        }
        std::ofstream out(index);
        if (out.good()) {
            out << j.dump(2) << std::endl;
            spdlog::info("Cleared current session in {}", index.string());
        }
    } catch (const std::exception& e) {
        spdlog::warn("Failed to clear session index {}: {}", index.string(), e.what());
    }
}

std::filesystem::path
extractExplicitPathFromArgv(int argc, char* argv[],
                            std::initializer_list<std::string_view> optionNames) {
    for (int i = 1; i < argc; ++i) {
        if (!argv[i]) {
            continue;
        }
        const std::string_view arg{argv[i]};
        if (std::find(optionNames.begin(), optionNames.end(), arg) != optionNames.end() &&
            i + 1 < argc && argv[i + 1]) {
            return std::filesystem::path(argv[i + 1]);
        }
    }
    return {};
}
} // namespace

int main(int argc, char* argv[]) {
    // Install fatal handlers as early as possible
    setup_fatal_handlers();

    // Set YAMS_IN_DAEMON=1 to signal we're running inside the daemon process. This compatibility
    // marker is published through the shared boundary before worker threads start.
    if (!yams::config::set_environment("YAMS_IN_DAEMON", "1")) {
        std::fputs("Failed to publish daemon process identity\n", stderr);
        return 1;
    }

    yams::daemon::DaemonConfig config;
    std::string configPath;
    const auto argvExplicitConfig = extractExplicitPathFromArgv(argc, argv, {"--config"});
    const auto argvExplicitDataDir =
        extractExplicitPathFromArgv(argc, argv, {"--data-dir", "--storage"});
    const auto argvExplicitSocket = extractExplicitPathFromArgv(argc, argv, {"--socket"});
    const auto argvExplicitPid = extractExplicitPathFromArgv(argc, argv, {"--pid-file"});

    // Capture CLI-provided data dir separately so explicit CLI can win over config and env.
    std::filesystem::path cliDataDir;

    bool cliProvidedDataDir = false;
    bool noPlugins = false;
    bool foreground = false;

#if YAMS_HAS_CLI11
    {
        CLI::App app{"YAMS Daemon - background service"};
        app.add_option("--config", configPath, "Configuration file path");
        app.add_option("--socket", config.socketPath, "Unix domain socket path");
        app.add_option("--data-dir,--storage", cliDataDir, "Data directory for storage");
        app.add_option("--pid-file", config.pidFile, "PID file path");
        app.add_option("--log-file", config.logFile, "Log file path");
        app.add_option("--log-level", config.logLevel, "Log level (trace/debug/info/warn/error)")
            ->default_val("info");
        app.add_option("--plugin-dir", config.pluginDir, "Directory containing plugins");
        app.add_flag("--no-plugins", noPlugins, "Disable plugin loading");
        app.add_flag("-f,--foreground", foreground, "Run in foreground (don't daemonize)");
        CLI11_PARSE(app, argc, argv);
        cliProvidedDataDir = !cliDataDir.empty();
    }
#else
    // Minimal fallback parser for environments without CLI11 (e.g., some Linux builds)
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto has_next = [&](int idx) { return idx + 1 < argc; };
        auto next_str = [&](std::string& out) {
            if (has_next(i)) {
                out = argv[++i];
                return true;
            }
            return false;
        };
        if (arg == "--config") {
            (void)next_str(configPath);
        } else if (arg == "--socket") {
            std::string v;
            if (next_str(v))
                config.socketPath = v;
        } else if (arg == "--data-dir" || arg == "--storage") {
            std::string v;
            if (next_str(v))
                cliDataDir = v, cliProvidedDataDir = true;
        } else if (arg == "--pid-file") {
            std::string v;
            if (next_str(v))
                config.pidFile = v;
        } else if (arg == "--log-file") {
            std::string v;
            if (next_str(v))
                config.logFile = v;
        } else if (arg == "--log-level") {
            (void)next_str(config.logLevel);
        } else if (arg == "--plugin-dir") {
            std::string v;
            if (next_str(v))
                config.pluginDir = v;
        } else if (arg == "--no-plugins") {
            noPlugins = true;
        } else if (arg == "-f" || arg == "--foreground") {
            foreground = true;
        }
    }
    if (config.logLevel.empty())
        config.logLevel = "info";
#endif

    // Apply the no-plugins flag
    if (noPlugins) {
        config.autoLoadPlugins = false;
    }

    // Load configuration from TOML file
    namespace fs = std::filesystem;
    if (configPath.empty()) {
        // Use platform-specific config path helper
        configPath = yams::config::get_config_path().string();
    }

    // Load and parse config if it exists
    if (!configPath.empty() && fs::exists(configPath)) {
        // Record for reloads
        config.configFilePath = configPath;
        yams::config::ConfigMigrator migrator;
        auto parseResult = migrator.parseTomlConfig(configPath);
        if (parseResult) {
            const auto& tomlConfig = parseResult.value();

            // Check if daemon section exists, if not use defaults
            bool hasDaemonSection = (tomlConfig.find("daemon") != tomlConfig.end());

            if (hasDaemonSection) {
                // Load daemon configuration from file
                const auto& daemonSection = tomlConfig.at("daemon");

                // Runtime paths are resolved together after TOML parsing so CLI, config,
                // environment aliases, and platform defaults cannot diverge.

                // Log level
                if (config.logLevel.empty() &&
                    daemonSection.find("log_level") != daemonSection.end()) {
                    config.logLevel = daemonSection.at("log_level");
                }

                // Enable model provider based on config
                if (daemonSection.find("enable") != daemonSection.end()) {
                    config.enableModelProvider = (daemonSection.at("enable") == "true");
                }
                if (daemonSection.find("model_provider_required") != daemonSection.end()) {
                    std::string v = daemonSection.at("model_provider_required");
                    for (auto& c : v) {
                        c = static_cast<char>(std::tolower(c));
                    }
                    config.modelProviderRequired =
                        (v == "1" || v == "true" || v == "yes" || v == "on");
                }

                // Plugin configuration
                if (config.pluginDir.empty() &&
                    daemonSection.find("plugin_dir") != daemonSection.end()) {
                    config.pluginDir = fs::path(daemonSection.at("plugin_dir"));
                }

                if (daemonSection.find("auto_load_plugins") != daemonSection.end()) {
                    config.autoLoadPlugins = (daemonSection.at("auto_load_plugins") == "true");
                }

                // Auto repair (RepairService)
                if (daemonSection.find("auto_repair") != daemonSection.end()) {
                    std::string v = daemonSection.at("auto_repair");
                    for (auto& c : v) {
                        c = static_cast<char>(std::tolower(c));
                    }
                    config.enableAutoRepair =
                        !(v == "0" || v == "false" || v == "off" || v == "no");
                }
                if (daemonSection.find("plugin_name_policy") != daemonSection.end()) {
                    config.pluginNamePolicy = daemonSection.at("plugin_name_policy");
                }
                if (daemonSection.find("plugin_dir_strict") != daemonSection.end()) {
                    std::string v = daemonSection.at("plugin_dir_strict");
                    for (auto& c : v) {
                        c = static_cast<char>(std::tolower(c));
                    }
                    config.pluginDirStrict = (v == "1" || v == "true" || v == "yes" || v == "on");
                }
                if (auto it = daemonSection.find("trusted_paths"); it != daemonSection.end()) {
                    auto paths = yams::config::parse_path_list(it->second);
                    for (auto& p : paths) {
                        if (!p.empty())
                            config.trustedPluginPaths.push_back(std::move(p));
                    }
                }
                if (daemonSection.find("auto_repair_batch_size") != daemonSection.end()) {
                    try {
                        config.autoRepairBatchSize = static_cast<size_t>(
                            std::stoul(daemonSection.at("auto_repair_batch_size")));
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.auto_repair_batch_size: {}",
                                     e.what());
                    }
                }
                if (daemonSection.find("auto_rebuild_on_dim_mismatch") != daemonSection.end()) {
                    std::string v = daemonSection.at("auto_rebuild_on_dim_mismatch");
                    for (auto& c : v) {
                        c = static_cast<char>(std::tolower(c));
                    }
                    config.autoRebuildOnDimMismatch =
                        !(v == "0" || v == "false" || v == "off" || v == "no");
                }
            } else {
                // Daemon section missing (probably old config) - use safe defaults
                spdlog::info(
                    "Daemon configuration section not found in config file, using defaults");
                // Enable model provider by default if plugins will be loaded
                config.enableModelProvider = config.autoLoadPlugins;
            }

            // [daemon.graph_prune] section: graph version pruning policy
            if (tomlConfig.find("daemon.graph_prune") != tomlConfig.end()) {
                const auto& pruneSection = tomlConfig.at("daemon.graph_prune");
                if (auto it = pruneSection.find("enabled"); it != pruneSection.end()) {
                    config.graphPrune.enabled = (it->second == "true");
                }
                if (auto it = pruneSection.find("keep_latest"); it != pruneSection.end()) {
                    try {
                        config.graphPrune.keepLatestPerCanonical =
                            static_cast<size_t>(std::stoul(it->second));
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.graph_prune.keep_latest: {}",
                                     e.what());
                    }
                }
                if (auto it = pruneSection.find("interval_minutes"); it != pruneSection.end()) {
                    try {
                        config.graphPrune.interval = std::chrono::minutes{std::stol(it->second)};
                    } catch (const std::exception& e) {
                        spdlog::warn(
                            "Config: failed to parse daemon.graph_prune.interval_minutes: {}",
                            e.what());
                    }
                }
                if (auto it = pruneSection.find("initial_delay_minutes");
                    it != pruneSection.end()) {
                    try {
                        config.graphPrune.initialDelay =
                            std::chrono::minutes{std::stol(it->second)};
                    } catch (const std::exception& e) {
                        spdlog::warn(
                            "Config: failed to parse daemon.graph_prune.initial_delay_minutes: {}",
                            e.what());
                    }
                }
            }

            // [daemon.document_retention] section: opt-in stored document version retention.
            if (tomlConfig.find("daemon.document_retention") != tomlConfig.end()) {
                const auto& retentionSection = tomlConfig.at("daemon.document_retention");
                if (auto it = retentionSection.find("enabled"); it != retentionSection.end()) {
                    config.documentRetention.enabled = (it->second == "true");
                }
                try {
                    if (auto it = retentionSection.find("keep_latest");
                        it != retentionSection.end()) {
                        config.documentRetention.keepLatest =
                            static_cast<size_t>(std::stoul(it->second));
                    }
                    if (auto it = retentionSection.find("interval_minutes");
                        it != retentionSection.end()) {
                        config.documentRetention.interval =
                            std::chrono::minutes{std::stol(it->second)};
                    }
                    if (auto it = retentionSection.find("initial_delay_minutes");
                        it != retentionSection.end()) {
                        config.documentRetention.initialDelay =
                            std::chrono::minutes{std::stol(it->second)};
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Config: invalid daemon.document_retention value: {}", e.what());
                }
                if (config.documentRetention.keepLatest == 0 ||
                    config.documentRetention.interval.count() <= 0 ||
                    config.documentRetention.initialDelay.count() < 0) {
                    spdlog::warn("Config: disabling invalid daemon.document_retention policy");
                    config.documentRetention.enabled = false;
                }
            }

            // [memory_sync] section: opt-in P2P memory sync over a shared store.
            if (tomlConfig.find("memory_sync") != tomlConfig.end()) {
                const auto& msSection = tomlConfig.at("memory_sync");
                if (auto it = msSection.find("enabled"); it != msSection.end()) {
                    config.memorySync.enabled = (it->second == "true");
                }
                if (auto it = msSection.find("node_id"); it != msSection.end()) {
                    config.memorySync.nodeId = it->second;
                }
                if (auto it = msSection.find("backend"); it != msSection.end()) {
                    config.memorySync.backend = it->second;
                }
                if (auto it = msSection.find("path"); it != msSection.end()) {
                    config.memorySync.path = it->second;
                }
                try {
                    if (auto it = msSection.find("sync_interval_ms"); it != msSection.end()) {
                        config.memorySync.syncIntervalMs =
                            static_cast<std::uint32_t>(std::stoul(it->second));
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Config: failed to parse memory_sync.sync_interval_ms: {}",
                                 e.what());
                }
                if (config.memorySync.enabled && config.memorySync.backend != "filesystem" &&
                    config.memorySync.backend != "s3") {
                    spdlog::warn("Config: disabling invalid memory_sync.backend '{}'",
                                 config.memorySync.backend);
                    config.memorySync.enabled = false;
                }
                if (config.memorySync.enabled && config.memorySync.path.empty()) {
                    spdlog::warn("Config: disabling memory_sync with empty path");
                    config.memorySync.enabled = false;
                }
            }

            // [plugins] section: trust list for ABI and external plugins
            if (tomlConfig.find("plugins") != tomlConfig.end()) {
                const auto& pluginsSection = tomlConfig.at("plugins");
                if (auto it = pluginsSection.find("trusted_paths"); it != pluginsSection.end()) {
                    auto paths = yams::config::parse_path_list(it->second);
                    for (auto& p : paths) {
                        if (!p.empty())
                            config.trustedPluginPaths.push_back(std::move(p));
                    }
                }
            }

            // [plugins.<name>] sections: per-plugin configuration as JSON
            // Example: [plugins.glint] with model_path, threshold, etc.
            for (const auto& [sectionName, sectionData] : tomlConfig) {
                if (sectionName.rfind("plugins.", 0) == 0 && sectionName.size() > 8) {
                    // Extract plugin name: "plugins.glint" -> "glint"
                    std::string pluginName = sectionName.substr(8);

                    // Convert section to JSON object
                    nlohmann::json pluginCfg;
                    for (const auto& [key, value] : sectionData) {
                        // Try to parse as bool, number, or leave as string
                        if (value == "true") {
                            pluginCfg[key] = true;
                        } else if (value == "false") {
                            pluginCfg[key] = false;
                        } else if (!value.empty() && value.front() == '[') {
                            // Keep array-like strings intact for plugin parsing.
                            pluginCfg[key] = value;
                        } else {
                            // Try as number first
                            try {
                                if (value.find('.') != std::string::npos) {
                                    pluginCfg[key] = std::stod(value);
                                } else {
                                    pluginCfg[key] = std::stoll(value);
                                }
                            } catch (...) {
                                // Keep as string
                                pluginCfg[key] = value;
                            }
                        }
                    }

                    config.pluginConfigs[pluginName] = pluginCfg.dump();
                    spdlog::info("Parsed plugin config for '{}': {}", pluginName,
                                 config.pluginConfigs[pluginName]);
                }
            }

            config.tuning =
                yams::daemon::ConfigResolver::applyRuntimeTuning(tomlConfig, config.tuning);
            yams::daemon::ConfigResolver::applySearchMaintenance(tomlConfig, config);

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
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.models.max_loaded_models: {}",
                                     e.what());
                    }
                }

                if (modelsSection.find("hot_pool_size") != modelsSection.end()) {
                    try {
                        config.modelPoolConfig.hotPoolSize =
                            std::stoul(modelsSection.at("hot_pool_size"));
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.models.hot_pool_size: {}",
                                     e.what());
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
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.download.timeout_ms: {}",
                                     e.what());
                    }
                }

                // Max file bytes
                if (dlSection.find("max_file_bytes") != dlSection.end()) {
                    try {
                        config.downloadPolicy.maxFileBytes =
                            static_cast<std::uint64_t>(std::stoull(dlSection.at("max_file_bytes")));
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.download.max_file_bytes: {}",
                                     e.what());
                    }
                }

                // Rate limit RPS
                if (dlSection.find("rate_limit_rps") != dlSection.end()) {
                    try {
                        config.downloadPolicy.rateLimitRps =
                            static_cast<size_t>(std::stoul(dlSection.at("rate_limit_rps")));
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.download.rate_limit_rps: {}",
                                     e.what());
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
                    } catch (const std::exception& e) {
                        spdlog::warn("Config: failed to parse daemon.models.num_threads: {}",
                                     e.what());
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
        }
    }

    // Normalize key paths early (before daemonizing) so relative or '~' inputs don't break after
    // chdir("/"). Keep explicit CLI choices intact but canonicalize them.
    try {
        config.socketPath = yams::config::expand_tilde(config.socketPath.string());
        config.pidFile = yams::config::expand_tilde(config.pidFile.string());
        config.logFile = yams::config::expand_tilde(config.logFile.string());
    } catch (const std::exception& e) {
        spdlog::debug("Path normalization error (best-effort): {}", e.what());
    }

    yams::config::RuntimePathOverrides pathOverrides;
    if (!argvExplicitConfig.empty()) {
        pathOverrides.configFile = argvExplicitConfig;
    }
    if (!argvExplicitDataDir.empty()) {
        pathOverrides.dataDir = argvExplicitDataDir;
    } else if (cliProvidedDataDir && !cliDataDir.empty()) {
        pathOverrides.dataDir = cliDataDir;
    }
    if (!argvExplicitSocket.empty()) {
        pathOverrides.socketPath = argvExplicitSocket;
    }
    if (!argvExplicitPid.empty()) {
        pathOverrides.pidFile = argvExplicitPid;
    }

    auto runtimePaths = yams::config::resolve_runtime_paths(pathOverrides);
    if (!runtimePaths) {
        std::cerr << "Runtime path configuration error: " << runtimePaths.error().message << "\n";
        return 1;
    }
    config.configFilePath = runtimePaths.value().configFile.value;
    config.dataDir = runtimePaths.value().dataDir.value;
    config.socketPath = runtimePaths.value().socketPath.value;
    config.pidFile = runtimePaths.value().pidFile.value;
    for (const auto& diagnostic : runtimePaths.value().diagnostics) {
        std::cerr << "Warning: " << diagnostic << "\n";
    }

    // Resolve log file path if not specified
    if (config.logFile.empty()) {
        config.logFile = yams::daemon::YamsDaemon::resolveSystemPath(
            yams::daemon::YamsDaemon::PathType::LogFile);
    }

    {
        const auto policy = yams::daemon::ConfigResolver::resolveInstrumentationPolicy(config);
        config.instrumentation.memoryProfileActive = policy.memoryProfileActive;
        config.instrumentation.suppressAutoRepair = policy.suppressAutoRepair;
        config.instrumentation.suppressSimeonLexicalBuild = policy.suppressSimeonLexicalBuild;
        config.instrumentation.suppressVectorIndexBuild = policy.suppressVectorIndexBuild;
        config.instrumentation.mslStackLogWarnBytes = policy.mslStackLogWarnBytes;
        if (config.instrumentation.suppressAutoRepair) {
            config.enableAutoRepair = false;
        }
    }

    // Create log directory if needed
    try {
        yams::common::ensureDirectories(config.logFile.parent_path());
    } catch (const std::exception& e) {
        // Best effort - log may go to fallback location
        std::cerr << "Warning: could not create log directory: " << e.what() << "\n";
    }

    // Configure logging (default to file to preserve logs after daemonizing)
    const size_t max_size = 10ULL * 1024 * 1024; // 10MB per file
    const size_t max_files = 5;                  // Keep 5 rotated files
    try {
        // Use rotating file sink to preserve logs across crashes
        auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            config.logFile.string(), max_size, max_files);
        auto logger = std::make_shared<spdlog::logger>("yams-daemon", rotating_sink);
        spdlog::set_default_logger(logger);
        spdlog::flush_on(spdlog::level::info);

        // Log rotation info
        spdlog::info("Log rotation enabled: {} (max {}MB x {} files)", config.logFile.string(),
                     max_size / (1024ULL * 1024), max_files);
    } catch (const std::exception& e) {
        std::cerr << "Warning: log file setup failed, using default logger: " << e.what() << "\n";
        try {
            auto fallback = yams::daemon::YamsDaemon::resolveSystemPath(
                yams::daemon::YamsDaemon::PathType::LogFile);
            yams::common::ensureDirectories(fallback.parent_path());
            auto rotating_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
                fallback.string(), max_size, max_files);
            auto logger = std::make_shared<spdlog::logger>("yams-daemon", rotating_sink);
            spdlog::set_default_logger(logger);
            spdlog::flush_on(spdlog::level::info);
            spdlog::warn("Primary log path '{}' unusable ({}); using fallback '{}'",
                         config.logFile.string(), e.what(), fallback.string());
        } catch (const std::exception& e2) {
            std::cerr << "Warning: fallback log file setup failed: " << e2.what() << "\n";
        }
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

    if (config.instrumentation.memoryProfileActive) {
        spdlog::warn("Memory instrumentation profile active: suppress_auto_repair={} "
                     "suppress_simeon_lexical_build={} suppress_vector_index_build={} "
                     "msl_stack_log_warn_mb={}",
                     config.instrumentation.suppressAutoRepair,
                     config.instrumentation.suppressSimeonLexicalBuild,
                     config.instrumentation.suppressVectorIndexBuild,
                     config.instrumentation.mslStackLogWarnBytes / (1024ULL * 1024ULL));
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

    clear_session_index_on_start();

    // Best-effort: notify about config v3 migration before daemonizing (visible to user)
    try {
        namespace fs = std::filesystem;
        fs::path defaultConfigPath = yams::config::get_config_path();
        if (!defaultConfigPath.empty()) {
            yams::config::ConfigMigrator migrator;
            auto need = migrator.needsMigration(defaultConfigPath);
            if (need && need.value()) {
                spdlog::warn("Configuration migration to v3 is required. Run: yams config migrate");
            }
        }
    } catch (const std::exception& e) {
        // Non-fatal: ignore config check errors in daemon startup
        spdlog::debug("Config migration check failed (non-fatal): {}", e.what());
    }

    // Let managed service launches tell the shutdown path not to force an
    // out-of-band process exit from the request-handling thread.
    if (!yams::config::set_environment("YAMS_DAEMON_FOREGROUND", foreground ? "1" : "0")) {
        spdlog::error("Failed to publish daemon foreground lifecycle policy");
        return 1;
    }

    // Daemonize if not running in foreground
    if (!foreground) {
#ifdef _WIN32
        spdlog::warn("Daemon mode not supported on Windows; running in foreground.");
#else
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Failed to fork daemon process\n";
            return 1;
        }
        if (pid > 0) {
            // Parent process exits
            std::cout << "Daemon started with PID: " << pid << '\n';
            return 0;
        }

        // Child process continues as daemon
        // Create new session
        if (setsid() < 0) {
            std::cerr << "Failed to create new session\n";
            return 1;
        }

        // Change working directory to root
        if (chdir("/") < 0) {
            std::cerr << "Failed to change working directory\n";
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
#endif
    }

    try {
        yams::daemon::YamsDaemon daemon(config);

        auto result = daemon.start();
        if (!result) {
            spdlog::error("Failed to start daemon: {}", result.error().message);
            return 1;
        }

        spdlog::info(
            "Entering main loop: isRunning={}, isStopRequested={}, g_shutdown_requested={}",
            daemon.isRunning(), daemon.isStopRequested(), g_shutdown_requested.load());

        // On Windows, avoid creating extra threads due to thread resource limits.
        // Instead, integrate signal checking into the main loop.
        // The daemon's runLoop already has a periodic tick, so we check signals there.

        // Set up a hook for the daemon to check our global signal flags
        auto hookLambda = [&daemon]() {
            if (g_shutdown_requested.load(std::memory_order_relaxed)) {
                spdlog::info("Signal check: shutdown requested, stopping daemon");
                daemon.requestStop();
                return true;
            }
            // Handle on-demand autoload via SIGUSR1
            if (g_autoload_plugins_now.exchange(false, std::memory_order_relaxed)) {
                try {
                    auto r = daemon.autoloadPluginsNow();
                    if (r) {
                        spdlog::info("On-demand plugin autoload completed: {} plugin(s) loaded",
                                     r.value());
                    } else {
                        spdlog::warn("On-demand plugin autoload failed: {}", r.error().message);
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("On-demand plugin autoload exception: {}", e.what());
                }
            }
            return false;
        };
        daemon.setSignalCheckHook(hookLambda);

        // Enable prompt SIGTERM response by binding external shutdown flag to CV predicate
        // (yams-qe6r) This allows the daemon to wake from CV wait immediately when SIGTERM is
        // received, rather than waiting for the full statusTickMs timeout.
        daemon.setExternalShutdownFlag(&g_shutdown_requested);

        // Run the main daemon loop on this thread (handles FSM ticks, metrics refresh, etc.)
        // The loop exits when stopRequested is set (via signal handler or daemon.stop())
        spdlog::info("Calling daemon.runLoop()...");
        daemon.runLoop();
        spdlog::info("daemon.runLoop() returned");

        // Log reason for exit
        spdlog::info("Main loop exiting: isRunning={}, isStopRequested={}, g_shutdown_requested={}",
                     daemon.isRunning(), daemon.isStopRequested(), g_shutdown_requested.load());
        // Ensure proper cleanup
        daemon.stop();

        return 0;

    } catch (const std::exception& e) {
        spdlog::error("Daemon error: {}", e.what());
        return 1;
    }
}
