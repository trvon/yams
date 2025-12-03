/**
 * @file external_plugin_host.h
 * @brief ExternalPluginHost for Python/process-based plugins
 *
 * Implements IPluginHost for external (non-native) plugins that run as
 * separate processes and communicate via JSON-RPC over stdio.
 *
 * Supported plugin types:
 * - Python scripts (.py)
 * - Node.js scripts (.js)
 * - Any executable with JSON-RPC 2.0 support
 *
 * @see RFC-EPH-001: RFC-external-plugin-host.md
 * @see docs/spec/external_plugin_jsonrpc_protocol.md
 */

#pragma once

#include <yams/core/types.h>
#include <yams/daemon/resource/plugin_host.h>

#include <nlohmann/json_fwd.hpp>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace yams::daemon {

class ServiceManager;

/**
 * @brief Configuration for ExternalPluginHost
 */
struct ExternalPluginHostConfig {
    /// Python interpreter path (default: "python3" or "python" on Windows)
    std::filesystem::path pythonExecutable;

    /// Node.js interpreter path (default: "node")
    std::filesystem::path nodeExecutable;

    /// Default RPC timeout for plugin calls
    std::chrono::milliseconds defaultRpcTimeout{30'000};

    /// Health check interval (0 to disable)
    std::chrono::milliseconds healthCheckInterval{30'000};

    /// Maximum concurrent external plugins
    size_t maxPlugins{10};

    /// Restart policy for crashed plugins
    struct RestartPolicy {
        int maxRetries{3};
        std::chrono::milliseconds initialDelay{1'000};
        std::chrono::milliseconds maxDelay{30'000};
        double backoffMultiplier{2.0};
    } restartPolicy;
};

/**
 * @brief Plugin host for external (Python/process-based) plugins
 *
 * Implements IPluginHost interface for plugins that run as separate processes
 * and communicate via JSON-RPC 2.0 over stdio.
 *
 * **Features:**
 * - Process lifecycle management (spawn, monitor, restart, terminate)
 * - Health monitoring with configurable intervals
 * - Automatic crash recovery with exponential backoff
 * - Trust-based security model
 * - RPC gateway for arbitrary method calls
 *
 * **Plugin Discovery:**
 * Plugins are identified by:
 * - File extension (.py, .js)
 * - Optional manifest file (yams-plugin.json)
 * - Handshake protocol (handshake.manifest RPC)
 *
 * **Example:**
 * @code
 * ExternalPluginHost host{serviceManager, trustFile, config};
 *
 * // Load a Python plugin
 * auto result = host.load("/path/to/plugin.py", "{}");
 * if (result) {
 *     // Call a custom RPC method
 *     auto response = host.callRpc(result->name, "analyze", {{"path", "/bin/ls"}});
 * }
 * @endcode
 *
 * @see IPluginHost
 * @see AbiPluginHost
 */
class ExternalPluginHost : public IPluginHost {
public:
    /**
     * @brief Construct ExternalPluginHost
     *
     * @param sm ServiceManager for daemon integration
     * @param trustFile Path to trust list file (empty for default)
     * @param config Configuration options
     */
    explicit ExternalPluginHost(ServiceManager* sm,
                                const std::filesystem::path& trustFile = {},
                                ExternalPluginHostConfig config = {});

    /**
     * @brief Destructor - terminates all plugin processes
     */
    ~ExternalPluginHost();

    // Non-copyable, non-movable (owns processes)
    ExternalPluginHost(const ExternalPluginHost&) = delete;
    ExternalPluginHost& operator=(const ExternalPluginHost&) = delete;
    ExternalPluginHost(ExternalPluginHost&&) = delete;
    ExternalPluginHost& operator=(ExternalPluginHost&&) = delete;

    // =========================================================================
    // IPluginHost interface
    // =========================================================================

    /**
     * @brief Scan a file to detect if it's a loadable external plugin
     *
     * Spawns the plugin temporarily to perform handshake and retrieve manifest.
     *
     * @param file Path to plugin file (.py, .js, etc.)
     * @return Plugin descriptor on success, error otherwise
     */
    Result<PluginDescriptor> scanTarget(const std::filesystem::path& file) override;

    /**
     * @brief Scan directory for external plugins
     *
     * @param dir Directory to scan
     * @return Vector of plugin descriptors found
     */
    Result<std::vector<PluginDescriptor>>
    scanDirectory(const std::filesystem::path& dir) override;

    /**
     * @brief Load an external plugin
     *
     * Spawns the plugin process, performs handshake, and initializes it
     * with the provided configuration.
     *
     * @param file Path to plugin file
     * @param configJson JSON configuration to pass to plugin.init
     * @return Plugin descriptor on success, error otherwise
     */
    Result<PluginDescriptor> load(const std::filesystem::path& file,
                                  const std::string& configJson) override;

    /**
     * @brief Unload an external plugin
     *
     * Sends shutdown RPC, waits for graceful exit, then terminates if needed.
     *
     * @param name Plugin name (from manifest)
     * @return Success or error
     */
    Result<void> unload(const std::string& name) override;

    /**
     * @brief List all loaded external plugins
     *
     * @return Vector of plugin descriptors
     */
    std::vector<PluginDescriptor> listLoaded() const override;

    /**
     * @brief Get list of trusted paths
     *
     * @return Vector of trusted filesystem paths
     */
    std::vector<std::filesystem::path> trustList() const override;

    /**
     * @brief Add path to trust list
     *
     * @param p Path to trust (file or directory)
     * @return Success or error
     */
    Result<void> trustAdd(const std::filesystem::path& p) override;

    /**
     * @brief Remove path from trust list
     *
     * @param p Path to remove
     * @return Success or error
     */
    Result<void> trustRemove(const std::filesystem::path& p) override;

    /**
     * @brief Get health status of a plugin
     *
     * Calls plugin.health RPC and returns the response.
     *
     * @param name Plugin name
     * @return JSON health status string, or error
     */
    Result<std::string> health(const std::string& name) override;

    // =========================================================================
    // External plugin specific
    // =========================================================================

    /**
     * @brief Call an arbitrary RPC method on a loaded plugin
     *
     * Provides a gateway for calling custom plugin methods that don't map
     * to standard daemon interfaces.
     *
     * @param pluginName Name of loaded plugin
     * @param method RPC method name (e.g., "ghidra.analyze")
     * @param params JSON parameters
     * @param timeout RPC timeout (0 for default)
     * @return JSON result, or error
     */
    Result<nlohmann::json> callRpc(const std::string& pluginName, const std::string& method,
                                   const nlohmann::json& params,
                                   std::chrono::milliseconds timeout = {});

    /**
     * @brief Check if a file is a supported external plugin type
     *
     * @param file Path to check
     * @return true if supported extension (.py, .js, etc.)
     */
    static bool isExternalPluginFile(const std::filesystem::path& file);

    /**
     * @brief Get supported plugin file extensions
     *
     * @return Vector of extensions (e.g., {".py", ".js"})
     */
    static std::vector<std::string> supportedExtensions();

    /**
     * @brief Get plugin statistics
     *
     * @param name Plugin name (empty for all plugins)
     * @return JSON with stats (uptime, crash count, last health, etc.)
     */
    nlohmann::json getStats(const std::string& name = {}) const;

    /**
     * @brief Set callback for plugin state changes
     *
     * @param callback Function called when plugin state changes
     */
    using StateCallback = std::function<void(const std::string& name, const std::string& event)>;
    void setStateCallback(StateCallback callback);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;
};

} // namespace yams::daemon
