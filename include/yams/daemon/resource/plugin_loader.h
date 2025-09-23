#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <yams/core/types.h>

namespace yams::daemon {

// Forward declarations
class IModelProvider;

/**
 * Plugin information structure
 */
struct PluginInfo {
    std::string name;
    std::filesystem::path path;
    void* handle = nullptr; // dlopen handle
    bool loaded = false;
    std::string error;
    // Optional: interfaces/types this plugin provides (e.g., "model_provider", "graph_adapter_v1")
    std::vector<std::string> interfaces;
};

/**
 * Dynamic plugin loader for model providers
 *
 * This class handles loading shared libraries (plugins) at runtime,
 * extracting their factory functions, and registering them with the
 * model provider registry.
 */
class PluginLoader {
public:
    PluginLoader();
    ~PluginLoader();

    // Prevent copying
    PluginLoader(const PluginLoader&) = delete;
    PluginLoader& operator=(const PluginLoader&) = delete;

    /**
     * Load a single plugin from the specified path
     * @param pluginPath Path to the shared library
     * @return Success or error with details
     */
    Result<void> loadPlugin(const std::filesystem::path& pluginPath);

    /**
     * Load all plugins from a directory
     * @param directory Directory containing plugin shared libraries
     * @param pattern File pattern to match (default: "*.so" on Linux, "*.dylib" on macOS)
     * @return Number of successfully loaded plugins
     */
    Result<size_t> loadPluginsFromDirectory(const std::filesystem::path& directory,
                                            const std::string& pattern = "");

    /**
     * Unload a specific plugin
     * @param pluginName Name of the plugin to unload
     * @return Success or error
     */
    Result<void> unloadPlugin(const std::string& pluginName);

    /**
     * Unload all loaded plugins
     */
    void unloadAllPlugins();

    /**
     * Get information about loaded plugins
     * @return Vector of plugin information
     */
    std::vector<PluginInfo> getLoadedPlugins() const;

    /**
     * Check if a plugin is loaded
     * @param pluginName Name of the plugin
     * @return True if loaded
     */
    bool isPluginLoaded(const std::string& pluginName) const;

    /**
     * Get default plugin directories to search
     * @return List of directories in search order
     */
    static std::vector<std::filesystem::path> getDefaultPluginDirectories();

    /**
     * Provide configured plugin directories (from config / CLI).
     * These take precedence over auto-discovered defaults but do NOT exclude them.
     * Passing an empty vector clears previously configured directories.
     */
    static void setConfiguredPluginDirectories(const std::vector<std::filesystem::path>& dirs);

    /**
     * Find and load plugins from default directories
     * @return Total number of plugins loaded
     */
    Result<size_t> autoLoadPlugins();

    // Configured directories (shared across loader instances)
    static std::vector<std::filesystem::path> configuredDirs_;

private:
    // Map of plugin name to plugin info
    std::unordered_map<std::string, PluginInfo> plugins_;

    // Helper to get platform-specific library extension
    static std::string getPlatformLibraryExtension();

    // Helper to construct library filename from plugin name
    static std::string getPluginLibraryName(const std::string& pluginName);
};

} // namespace yams::daemon
