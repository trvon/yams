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

    Impl(ServiceManager* sm, const std::filesystem::path& trustFile,
         ExternalPluginHostConfig cfg)
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

    auto scanDirectory(const std::filesystem::path& dir)
        -> Result<std::vector<PluginDescriptor>> {
        std::vector<PluginDescriptor> results;

        if (!fs::exists(dir) || !fs::is_directory(dir)) {
            return Error{ErrorCode::InvalidPath, "Directory does not exist: " + dir.string()};
        }

        for (const auto& entry : fs::directory_iterator(dir)) {
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

    auto load(const std::filesystem::path& file,
              const std::string& configJson) -> Result<PluginDescriptor> {
        // Check if file exists
        if (!fs::exists(file)) {
            return Error{ErrorCode::FileNotFound, "Plugin file not found: " + file.string()};
        }

        // Check trust policy
        if (!trust_file.empty() && !isTrusted(file)) {
            return Error{ErrorCode::Unauthorized,
                         "Plugin is not in trust list: " + file.string()};
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
                return Error{ErrorCode::InvalidState,
                             "Plugin already loaded: " + name};
            }
        }

        // Check max plugins limit
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (loaded.size() >= config.maxPlugins) {
                return Error{ErrorCode::ResourceExhausted,
                             "Maximum number of plugins loaded"};
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

    auto callRpc(const std::string& pluginName, const std::string& method,
                 const json& params, std::chrono::milliseconds /*timeout*/)
        -> Result<json> {
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

    static auto isExternalPluginFile(const std::filesystem::path& path) -> bool {
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

    static auto supportedExtensions() -> std::vector<std::string> {
        return {".py", ".js"};
    }

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

    void setStateCallback(ExternalPluginHost::StateCallback cb) {
        state_callback = std::move(cb);
    }

private:
    extraction::PluginProcessConfig buildProcessConfig(const std::filesystem::path& file) const {
        extraction::PluginProcessConfig proc_config;

        auto ext = file.extension().string();
        std::transform(ext.begin(), ext.end(), ext.begin(), ::tolower);

        if (ext == ".py") {
            // Python plugin
            proc_config.executable = config.pythonExecutable.empty()
                                         ? std::string("python3")
                                         : config.pythonExecutable.string();
            proc_config.args = {"-u", file.string()}; // -u for unbuffered
        } else if (ext == ".js") {
            // Node.js plugin
            proc_config.executable = config.nodeExecutable.empty()
                                         ? std::string("node")
                                         : config.nodeExecutable.string();
            proc_config.args = {file.string()};
        } else {
            // Direct executable
            proc_config.executable = file.string();
        }

        proc_config.rpc_timeout = config.defaultRpcTimeout;

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
        auto canonical = fs::weakly_canonical(path);
        return trusted.find(canonical) != trusted.end();
    }
};

//==============================================================================
// ExternalPluginHost implementation (delegates to Impl)
//==============================================================================

ExternalPluginHost::ExternalPluginHost(ServiceManager* sm,
                                       const std::filesystem::path& trustFile,
                                       ExternalPluginHostConfig config)
    : pImpl(std::make_unique<Impl>(sm, trustFile, std::move(config))) {}

ExternalPluginHost::~ExternalPluginHost() = default;

auto ExternalPluginHost::scanTarget(const std::filesystem::path& file)
    -> Result<PluginDescriptor> {
    return pImpl->scanTarget(file);
}

auto ExternalPluginHost::scanDirectory(const std::filesystem::path& dir)
    -> Result<std::vector<PluginDescriptor>> {
    return pImpl->scanDirectory(dir);
}

auto ExternalPluginHost::load(const std::filesystem::path& file,
                              const std::string& configJson) -> Result<PluginDescriptor> {
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
