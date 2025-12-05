// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 Trevon Sides

#include "yams/daemon/resource/external_plugin_host.h"
#include "yams/extraction/jsonrpc_client.hpp"
#include "yams/extraction/plugin_process.hpp"
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <regex>
#include <set>
#include <unordered_map>

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace yams::daemon {

//==============================================================================
// ExternalPluginInstance - tracks a single loaded external plugin
//==============================================================================

struct ExternalPluginInstance {
    PluginDescriptor descriptor;
    std::unique_ptr<extraction::PluginProcess> process;
    std::unique_ptr<extraction::JsonRpcClient> rpc_client;
    std::chrono::steady_clock::time_point load_time;
    std::chrono::steady_clock::time_point last_health_check;
    size_t restart_count{0};
    bool healthy{true};
};

//==============================================================================
// ExternalPluginHost::Impl
//==============================================================================

struct ExternalPluginHost::Impl {
    ServiceManager* service_manager{nullptr};
    std::filesystem::path trust_file;
    ExternalPluginHostConfig config;
    mutable std::mutex mutex;
    std::unordered_map<std::string, std::unique_ptr<ExternalPluginInstance>> loaded;
    std::set<std::filesystem::path> trusted;
    ExternalPluginHost::StateCallback state_callback;

    Impl(ServiceManager* sm, const std::filesystem::path& trustFile, ExternalPluginHostConfig cfg)
        : service_manager(sm), trust_file(trustFile), config(std::move(cfg)) {
        loadTrust();
    }

    ~Impl() {
        // Unload all plugins gracefully
        std::vector<std::string> names;
        {
            std::lock_guard<std::mutex> lock(mutex);
            for (const auto& [name, _] : loaded) {
                names.push_back(name);
            }
        }
        for (const auto& name : names) {
            unload(name);
        }
    }

    //--------------------------------------------------------------------------
    // IPluginHost interface implementation
    //--------------------------------------------------------------------------

    auto scanTarget(const std::filesystem::path& file) -> Result<PluginDescriptor> {
        if (!isExternalPluginFile(file)) {
            return Error{ErrorCode::InvalidArgument,
                         "File is not a valid external plugin: " + file.string()};
        }

        // Launch process temporarily to get manifest
        auto proc_config = buildProcessConfig(file);
        auto process = std::make_unique<extraction::PluginProcess>(std::move(proc_config));

        auto rpc_client = std::make_unique<extraction::JsonRpcClient>(*process);

        // Call handshake to get manifest
        auto result = rpc_client->call("handshake.manifest");
        if (!result) {
            return Error{ErrorCode::IOError,
                         "Failed to get manifest from plugin: " + file.string()};
        }

        auto manifest = result.value();
        PluginDescriptor desc;
        desc.name = manifest.value("name", file.stem().string());
        desc.version = manifest.value("version", "0.0.0");
        desc.abiVersion = manifest.value("abi_version", 0U);
        desc.path = file;
        desc.manifestJson = manifest.dump();

        // Parse interfaces
        if (manifest.contains("capabilities")) {
            auto caps = manifest["capabilities"];
            if (caps.contains("content_extraction")) {
                desc.interfaces.push_back("content_extraction");
            }
            if (caps.contains("symbol_extraction")) {
                desc.interfaces.push_back("symbol_extraction");
            }
            if (caps.contains("graph_store")) {
                desc.interfaces.push_back("graph_store");
            }
        }

        // Shutdown the temporary process
        (void)rpc_client->call("plugin.shutdown");

        return desc;
    }

    auto scanDirectory(const std::filesystem::path& dir) -> Result<std::vector<PluginDescriptor>> {
        std::vector<PluginDescriptor> results;

        if (!fs::exists(dir) || !fs::is_directory(dir)) {
            return Error{ErrorCode::InvalidPath, "Directory does not exist: " + dir.string()};
        }

        for (const auto& entry : fs::directory_iterator(dir)) {
            // Check for plugin directories (contain yams-plugin.json)
            if (entry.is_directory()) {
                if (isPluginDirectory(entry.path())) {
                    auto scan_result = scanTarget(entry.path());
                    if (scan_result) {
                        results.push_back(std::move(scan_result.value()));
                    } else {
                        spdlog::debug("Skipping plugin dir {}: {}", entry.path().string(),
                                      scan_result.error().message);
                    }
                }
                continue;
            }

            // Check for standalone plugin files
            if (!entry.is_regular_file()) {
                continue;
            }
            if (!isExternalPluginFile(entry.path())) {
                continue;
            }

            auto scan_result = scanTarget(entry.path());
            if (scan_result) {
                results.push_back(std::move(scan_result.value()));
            } else {
                spdlog::debug("Skipping {}: {}", entry.path().string(),
                              scan_result.error().message);
            }
        }

        return results;
    }

    auto load(const std::filesystem::path& file, const std::string& configJson)
        -> Result<PluginDescriptor> {
        // Check if file exists
        if (!fs::exists(file)) {
            return Error{ErrorCode::FileNotFound, "Plugin file not found: " + file.string()};
        }

        // Check trust policy
        if (!trust_file.empty() && !isTrusted(file)) {
            return Error{ErrorCode::Unauthorized, "Plugin is not in trust list: " + file.string()};
        }

        // Scan to get descriptor
        auto scan_result = scanTarget(file);
        if (!scan_result) {
            return scan_result;
        }

        auto descriptor = std::move(scan_result.value());
        const auto& name = descriptor.name;

        // Check if already loaded
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (loaded.find(name) != loaded.end()) {
                return Error{ErrorCode::InvalidState, "Plugin already loaded: " + name};
            }
        }

        // Check max plugins limit
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (loaded.size() >= config.maxPlugins) {
                return Error{ErrorCode::ResourceExhausted, "Maximum number of plugins loaded"};
            }
        }

        // Launch the plugin process
        auto proc_config = buildProcessConfig(file);
        auto process = std::make_unique<extraction::PluginProcess>(std::move(proc_config));
        auto rpc_client = std::make_unique<extraction::JsonRpcClient>(*process);

        // Handshake again (real load)
        auto handshake_result = rpc_client->call("handshake.manifest");
        if (!handshake_result) {
            return Error{ErrorCode::IOError, "Handshake failed during load"};
        }

        // Initialize plugin with config
        json init_params;
        if (!configJson.empty()) {
            try {
                init_params = json::parse(configJson);
            } catch (...) {
                init_params = json::object();
            }
        }

        auto init_result = rpc_client->call("plugin.init", init_params);
        if (!init_result) {
            return Error{ErrorCode::IOError, "Plugin init failed"};
        }

        // Store instance
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto instance = std::make_unique<ExternalPluginInstance>();
            instance->descriptor = descriptor;
            instance->process = std::move(process);
            instance->rpc_client = std::move(rpc_client);
            instance->load_time = std::chrono::steady_clock::now();
            instance->last_health_check = instance->load_time;
            loaded[name] = std::move(instance);
        }

        spdlog::info("Loaded external plugin '{}' v{} from {}", name, descriptor.version,
                     file.string());

        if (state_callback) {
            state_callback(name, "loaded");
        }

        return descriptor;
    }

    auto unload(const std::string& name) -> Result<void> {
        std::unique_ptr<ExternalPluginInstance> instance;

        {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = loaded.find(name);
            if (it == loaded.end()) {
                return Error{ErrorCode::NotFound, "Plugin not loaded: " + name};
            }
            instance = std::move(it->second);
            loaded.erase(it);
        }

        // Graceful shutdown via RPC
        if (instance->rpc_client) {
            auto result = instance->rpc_client->call("plugin.shutdown");
            if (!result) {
                spdlog::warn("Plugin '{}' shutdown RPC failed", name);
            }
        }

        // Process destructor handles termination
        instance->rpc_client.reset();
        instance->process.reset();

        spdlog::info("Unloaded external plugin '{}'", name);

        if (state_callback) {
            state_callback(name, "unloaded");
        }

        return {};
    }

    auto listLoaded() const -> std::vector<PluginDescriptor> {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<PluginDescriptor> results;
        results.reserve(loaded.size());
        for (const auto& [_, instance] : loaded) {
            results.push_back(instance->descriptor);
        }
        return results;
    }

    auto trustList() const -> std::vector<std::filesystem::path> {
        std::lock_guard<std::mutex> lock(mutex);
        return std::vector<std::filesystem::path>(trusted.begin(), trusted.end());
    }

    auto trustAdd(const std::filesystem::path& path) -> Result<void> {
        std::lock_guard<std::mutex> lock(mutex);
        auto canonical = fs::weakly_canonical(path);
        trusted.insert(canonical);
        saveTrust();
        return {};
    }

    auto trustRemove(const std::filesystem::path& path) -> Result<void> {
        std::lock_guard<std::mutex> lock(mutex);
        auto canonical = fs::weakly_canonical(path);
        trusted.erase(canonical);
        saveTrust();
        return {};
    }

    auto health(const std::string& name) -> Result<std::string> {
        std::lock_guard<std::mutex> lock(mutex);

        auto it = loaded.find(name);
        if (it == loaded.end()) {
            return Error{ErrorCode::NotFound, "Plugin not loaded: " + name};
        }

        auto& instance = it->second;

        // Check if process is alive
        if (!instance->process || !instance->process->is_alive()) {
            instance->healthy = false;

            // Try to restart if policy allows
            if (instance->restart_count < static_cast<size_t>(config.restartPolicy.maxRetries)) {
                auto path = instance->descriptor.path;
                auto config_json = instance->descriptor.manifestJson;

                spdlog::warn("Plugin '{}' crashed, attempting restart ({}/{})", name,
                             instance->restart_count + 1, config.restartPolicy.maxRetries);

                instance->restart_count++;

                // Try to restart
                auto proc_config = buildProcessConfig(path);
                try {
                    instance->process =
                        std::make_unique<extraction::PluginProcess>(std::move(proc_config));
                    instance->rpc_client =
                        std::make_unique<extraction::JsonRpcClient>(*instance->process);

                    auto handshake = instance->rpc_client->call("handshake.manifest");
                    if (handshake) {
                        auto init = instance->rpc_client->call("plugin.init");
                        if (init) {
                            instance->healthy = true;
                            if (state_callback) {
                                state_callback(name, "restarted");
                            }
                            return std::string(R"({"status":"restarted"})");
                        }
                    }
                } catch (...) {
                    // Restart failed
                }
            }

            if (state_callback) {
                state_callback(name, "crashed");
            }
            return Error{ErrorCode::IOError, "Plugin process is not running"};
        }

        // Call health RPC
        auto result = instance->rpc_client->call("plugin.health");
        if (!result) {
            instance->healthy = false;
            return Error{ErrorCode::IOError, "Health check RPC failed"};
        }

        instance->healthy = true;
        instance->last_health_check = std::chrono::steady_clock::now();
        return result.value().dump();
    }

    //--------------------------------------------------------------------------
    // ExternalPluginHost-specific methods
    //--------------------------------------------------------------------------

    auto callRpc(const std::string& pluginName, const std::string& method, const json& params,
                 std::chrono::milliseconds /*timeout*/) -> Result<json> {
        std::lock_guard<std::mutex> lock(mutex);

        auto it = loaded.find(pluginName);
        if (it == loaded.end()) {
            return Error{ErrorCode::NotFound, "Plugin not loaded: " + pluginName};
        }

        auto& instance = it->second;
        if (!instance->process || !instance->process->is_alive()) {
            return Error{ErrorCode::IOError, "Plugin process not running"};
        }

        auto result = instance->rpc_client->call(method, params);
        if (!result) {
            return Error{ErrorCode::IOError, "RPC call failed"};
        }

        return result.value();
    }

    //--------------------------------------------------------------------------
    // Plugin manifest support
    //--------------------------------------------------------------------------

    static constexpr const char* MANIFEST_FILENAME = "yams-plugin.json";

    /// Check if a path is a plugin directory (contains yams-plugin.json)
    static bool isPluginDirectory(const std::filesystem::path& path) {
        if (!fs::exists(path) || !fs::is_directory(path)) {
            return false;
        }
        return fs::exists(path / MANIFEST_FILENAME);
    }

    /// Read and parse the yams-plugin.json manifest
    static std::optional<json> readManifest(const std::filesystem::path& pluginDir) {
        auto manifestPath = pluginDir / MANIFEST_FILENAME;
        if (!fs::exists(manifestPath)) {
            return std::nullopt;
        }
        try {
            std::ifstream file(manifestPath);
            if (!file.is_open()) {
                return std::nullopt;
            }
            return json::parse(file);
        } catch (const std::exception& e) {
            spdlog::warn("Failed to parse manifest at {}: {}", manifestPath.string(), e.what());
            return std::nullopt;
        }
    }

    /// Substitute variables in a string (e.g., ${plugin_dir})
    static std::string substituteVars(const std::string& input,
                                      const std::filesystem::path& pluginDir) {
        std::string result = input;

        // Replace ${plugin_dir} with the actual plugin directory path
        const std::string pluginDirVar = "${plugin_dir}";
        size_t pos = 0;
        while ((pos = result.find(pluginDirVar, pos)) != std::string::npos) {
            result.replace(pos, pluginDirVar.length(), pluginDir.string());
            pos += pluginDir.string().length();
        }

        return result;
    }

    static auto isExternalPluginFile(const std::filesystem::path& path) -> bool {
        // Check if it's a plugin directory with manifest
        if (isPluginDirectory(path)) {
            return true;
        }

        if (!fs::exists(path) || !fs::is_regular_file(path)) {
            return false;
        }

        auto ext = path.extension().string();
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

        // Python plugins
        if (ext == ".py") {
            return true;
        }

        // Node.js plugins
        if (ext == ".js") {
            return true;
        }

#ifdef _WIN32
        if (ext == ".exe" || ext == ".bat" || ext == ".cmd" || ext == ".ps1") {
            return true;
        }
#else
        // On Unix, check if file is executable
        std::error_code ec;
        auto status = fs::status(path, ec);
        if (!ec && (status.permissions() & fs::perms::owner_exec) != fs::perms::none) {
            return true;
        }
#endif

        return false;
    }

    static auto supportedExtensions() -> std::vector<std::string> { return {".py", ".js"}; }

    auto getStats(const std::string& name) const -> json {
        std::lock_guard<std::mutex> lock(mutex);

        json stats = json::object();

        auto addInstanceStats = [](json& out, const ExternalPluginInstance& inst) {
            auto now = std::chrono::steady_clock::now();
            auto uptime = std::chrono::duration_cast<std::chrono::seconds>(now - inst.load_time);
            out["uptime_seconds"] = uptime.count();
            out["restart_count"] = inst.restart_count;
            out["healthy"] = inst.healthy;
            out["name"] = inst.descriptor.name;
            out["version"] = inst.descriptor.version;
        };

        if (name.empty()) {
            // All plugins
            stats["total_loaded"] = loaded.size();
            stats["plugins"] = json::array();
            for (const auto& [_, instance] : loaded) {
                json plugin_stats;
                addInstanceStats(plugin_stats, *instance);
                stats["plugins"].push_back(plugin_stats);
            }
        } else {
            auto it = loaded.find(name);
            if (it != loaded.end()) {
                addInstanceStats(stats, *it->second);
            }
        }

        return stats;
    }

    void setStateCallback(ExternalPluginHost::StateCallback cb) { state_callback = std::move(cb); }

private:
    extraction::PluginProcessConfig
    buildProcessConfig(const std::filesystem::path& inputPath) const {
        extraction::PluginProcessConfig proc_config;
        proc_config.rpc_timeout = config.defaultRpcTimeout;

        // Canonicalize path to ensure it's absolute
        auto path = fs::weakly_canonical(inputPath);

        // Check if this is a plugin directory with manifest
        if (isPluginDirectory(path)) {
            auto manifest = readManifest(path);

            // Priority 1: Check manifest entry.binary for platform-specific compiled binary
            if (manifest && manifest->contains("entry")) {
                const auto& entry = (*manifest)["entry"];

                if (entry.contains("binary") && entry["binary"].is_object()) {
                    const auto& binary = entry["binary"];
                    std::string platformKey;

#ifdef _WIN32
                    platformKey = "windows";
#elif defined(__APPLE__)
                    platformKey = "darwin";
#else
                    platformKey = "linux";
#endif

                    if (binary.contains(platformKey) && binary[platformKey].is_string()) {
                        std::string binaryPath =
                            substituteVars(binary[platformKey].get<std::string>(), path);
                        auto compiledBinary = fs::path(binaryPath).make_preferred();

                        // Make relative paths absolute (relative to plugin dir)
                        if (compiledBinary.is_relative()) {
                            compiledBinary = path / compiledBinary;
                        }

                        if (fs::exists(compiledBinary)) {
                            proc_config.executable = compiledBinary;
                            proc_config.workdir = path;

                            // Pass any additional args from manifest
                            if (entry.contains("args") && entry["args"].is_array()) {
                                for (const auto& arg : entry["args"]) {
                                    if (arg.is_string()) {
                                        proc_config.args.push_back(
                                            substituteVars(arg.get<std::string>(), path));
                                    }
                                }
                            }
                            // Set environment variables from manifest
                            if (entry.contains("env") && entry["env"].is_object()) {
                                for (auto& [key, value] : entry["env"].items()) {
                                    if (value.is_string()) {
                                        proc_config.env[key] =
                                            substituteVars(value.get<std::string>(), path);
                                    }
                                }
                            }

                            spdlog::info("ExternalPluginHost: Loading compiled plugin from "
                                         "manifest binary: {}",
                                         compiledBinary.string());
                            return proc_config;
                        } else {
                            spdlog::debug("Manifest binary not found: {}", compiledBinary.string());
                        }
                    }
                }
            }

            // Priority 2: Look for default compiled binary (plugin.exe/plugin)
#ifdef _WIN32
            auto defaultBinary = path / "plugin.exe";
#else
            auto defaultBinary = path / "plugin";
#endif
            if (fs::exists(defaultBinary)) {
                proc_config.executable = defaultBinary;
                proc_config.workdir = path;

                // Pass any additional args from manifest
                if (manifest && manifest->contains("entry")) {
                    const auto& entry = (*manifest)["entry"];
                    if (entry.contains("args") && entry["args"].is_array()) {
                        for (const auto& arg : entry["args"]) {
                            if (arg.is_string()) {
                                proc_config.args.push_back(
                                    substituteVars(arg.get<std::string>(), path));
                            }
                        }
                    }
                    // Set environment variables from manifest
                    if (entry.contains("env") && entry["env"].is_object()) {
                        for (auto& [key, value] : entry["env"].items()) {
                            if (value.is_string()) {
                                proc_config.env[key] =
                                    substituteVars(value.get<std::string>(), path);
                            }
                        }
                    }
                }

                spdlog::info(
                    "ExternalPluginHost: Loading compiled plugin from default location: {}",
                    defaultBinary.string());
                return proc_config;
            }

            // Priority 3: Use manifest entry.fallback_cmd or entry.cmd if specified
            if (manifest && manifest->contains("entry")) {
                const auto& entry = (*manifest)["entry"];

                // Check for fallback_cmd first (explicit fallback for when binary isn't built)
                std::string cmdKey = entry.contains("fallback_cmd") ? "fallback_cmd" : "cmd";

                // Get command from manifest
                if (entry.contains(cmdKey) && entry[cmdKey].is_array() && !entry[cmdKey].empty()) {
                    auto cmd = entry[cmdKey];
                    std::string execStr = substituteVars(cmd[0].get<std::string>(), path);

                    // Convert forward slashes to native path separators
                    proc_config.executable = fs::path(execStr).make_preferred();

                    for (size_t i = 1; i < cmd.size(); ++i) {
                        std::string argStr = substituteVars(cmd[i].get<std::string>(), path);
                        // Also normalize path arguments
                        if (argStr.find('/') != std::string::npos ||
                            argStr.find('\\') != std::string::npos) {
                            argStr = fs::path(argStr).make_preferred().string();
                        }
                        proc_config.args.push_back(argStr);
                    }
                }

                // Set environment variables from manifest
                if (entry.contains("env") && entry["env"].is_object()) {
                    for (auto& [key, value] : entry["env"].items()) {
                        if (value.is_string()) {
                            proc_config.env[key] = substituteVars(value.get<std::string>(), path);
                        }
                    }
                }

                // Set working directory to plugin directory
                proc_config.workdir = path;

                spdlog::info("ExternalPluginHost: Loading plugin from manifest at {}",
                             path.string());
                spdlog::info("  executable: '{}'", proc_config.executable.string());
                spdlog::info("  workdir: '{}'", path.string());
                for (const auto& arg : proc_config.args) {
                    spdlog::info("  arg: '{}'", arg);
                }

                return proc_config;
            }

            // Priority 3: Fallback - look for plugin.py (least secure, requires interpreter)
            auto pluginPy = path / "plugin.py";
            if (fs::exists(pluginPy)) {
                spdlog::warn("ExternalPluginHost: Loading uncompiled Python plugin {} "
                             "(consider compiling for security)",
                             pluginPy.string());
                proc_config.executable = config.pythonExecutable.empty()
                                             ? std::filesystem::path("python")
                                             : config.pythonExecutable;
                proc_config.args = {"-u", pluginPy.string()};
                proc_config.workdir = path;
                return proc_config;
            }
        }

        // Fall back to file-based detection (direct file path provided)
        auto ext = path.extension().string();
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

        if (ext == ".py") {
            spdlog::warn("ExternalPluginHost: Loading uncompiled Python plugin {} "
                         "(consider compiling for security)",
                         path.string());
            proc_config.executable = config.pythonExecutable.empty()
                                         ? std::filesystem::path("python")
                                         : config.pythonExecutable;
            proc_config.args = {"-u", path.string()}; // -u for unbuffered
        } else if (ext == ".js") {
            spdlog::warn("ExternalPluginHost: Loading uncompiled Node.js plugin {} "
                         "(consider compiling for security)",
                         path.string());
            proc_config.executable = config.nodeExecutable.empty() ? std::filesystem::path("node")
                                                                   : config.nodeExecutable;
            proc_config.args = {path.string()};
        } else {
            // Direct executable (already compiled)
            proc_config.executable = path;
        }

        return proc_config;
    }

    void loadTrust() {
        if (trust_file.empty()) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex);
        trusted.clear();

        if (!fs::exists(trust_file)) {
            return;
        }

        std::ifstream file(trust_file);
        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty() && line[0] != '#') {
                trusted.insert(fs::weakly_canonical(line));
            }
        }
    }

    void saveTrust() const {
        if (trust_file.empty()) {
            return;
        }

        // Create parent directories if needed
        auto parent = trust_file.parent_path();
        if (!parent.empty()) {
            std::error_code ec;
            fs::create_directories(parent, ec);
        }

        std::ofstream file(trust_file);
        file << "# YAMS External Plugin Trust List\n";
        file << "# One plugin path per line\n";
        for (const auto& path : trusted) {
            file << path.string() << "\n";
        }
    }

    bool isTrusted(const std::filesystem::path& path) const {
        std::error_code ec;
        auto candidate = fs::weakly_canonical(path, ec);
        if (ec)
            candidate = path;

        for (const auto& entry : trusted) {
            std::error_code tec;
            auto base = fs::weakly_canonical(entry, tec);
            if (tec)
                base = entry;

            auto baseStr = base.string();
            auto candStr = candidate.string();
            if (!baseStr.empty() && candStr.rfind(baseStr, 0) == 0) {
                return true;
            }
        }
        return false;
    }
};

//==============================================================================
// ExternalPluginHost implementation (delegates to Impl)
//==============================================================================

ExternalPluginHost::ExternalPluginHost(ServiceManager* sm, const std::filesystem::path& trustFile,
                                       ExternalPluginHostConfig config)
    : pImpl(std::make_unique<Impl>(sm, trustFile, std::move(config))) {}

ExternalPluginHost::~ExternalPluginHost() = default;

auto ExternalPluginHost::scanTarget(const std::filesystem::path& file) -> Result<PluginDescriptor> {
    return pImpl->scanTarget(file);
}

auto ExternalPluginHost::scanDirectory(const std::filesystem::path& dir)
    -> Result<std::vector<PluginDescriptor>> {
    return pImpl->scanDirectory(dir);
}

auto ExternalPluginHost::load(const std::filesystem::path& file, const std::string& configJson)
    -> Result<PluginDescriptor> {
    return pImpl->load(file, configJson);
}

auto ExternalPluginHost::unload(const std::string& name) -> Result<void> {
    return pImpl->unload(name);
}

auto ExternalPluginHost::listLoaded() const -> std::vector<PluginDescriptor> {
    return pImpl->listLoaded();
}

auto ExternalPluginHost::trustList() const -> std::vector<std::filesystem::path> {
    return pImpl->trustList();
}

auto ExternalPluginHost::trustAdd(const std::filesystem::path& path) -> Result<void> {
    return pImpl->trustAdd(path);
}

auto ExternalPluginHost::trustRemove(const std::filesystem::path& path) -> Result<void> {
    return pImpl->trustRemove(path);
}

auto ExternalPluginHost::health(const std::string& name) -> Result<std::string> {
    return pImpl->health(name);
}

auto ExternalPluginHost::callRpc(const std::string& pluginName, const std::string& method,
                                 const json& params, std::chrono::milliseconds timeout)
    -> Result<json> {
    return pImpl->callRpc(pluginName, method, params, timeout);
}

bool ExternalPluginHost::isExternalPluginFile(const std::filesystem::path& path) {
    return Impl::isExternalPluginFile(path);
}

std::vector<std::string> ExternalPluginHost::supportedExtensions() {
    return Impl::supportedExtensions();
}

json ExternalPluginHost::getStats(const std::string& name) const {
    return pImpl->getStats(name);
}

void ExternalPluginHost::setStateCallback(StateCallback callback) {
    pImpl->setStateCallback(std::move(callback));
}

} // namespace yams::daemon
