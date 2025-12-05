#pragma once
/**
 * @file plugin_installer.hpp
 * @brief Plugin installation manager for downloading, verifying, and installing plugins
 *
 * The PluginInstaller orchestrates the full plugin installation workflow:
 * 1. Query repository for plugin metadata
 * 2. Download plugin bundle (.tar.gz)
 * 3. Verify integrity (SHA-256 checksum)
 * 4. Extract to plugin directory
 * 5. Add to trust list
 * 6. Optionally load the plugin
 */

#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/core/types.h>
#include <yams/plugins/plugin_repo_client.hpp>

namespace yams::plugins {

/**
 * Progress callback for plugin installation.
 */
struct InstallProgress {
    enum class Stage {
        Querying,      // Fetching metadata from repository
        Downloading,   // Downloading plugin bundle
        Verifying,     // Verifying checksum
        Extracting,    // Extracting archive
        Installing,    // Moving to plugin directory
        Trusting,      // Adding to trust list
        Loading,       // Loading plugin
        Complete       // Installation complete
    };

    Stage stage{Stage::Querying};
    std::string message;
    float progress{0.0f};       // 0.0 - 1.0
    uint64_t bytesDownloaded{0};
    uint64_t totalBytes{0};
};

using InstallProgressCallback = std::function<void(const InstallProgress&)>;

/**
 * Installation options.
 */
struct InstallOptions {
    std::optional<std::string> version;           // Specific version (default: latest)
    std::filesystem::path installDir;             // Target directory (default: ~/.local/lib/yams/plugins)
    bool autoTrust{true};                         // Add to trust list after install
    bool autoLoad{true};                          // Load plugin after install (requires daemon)
    bool force{false};                            // Overwrite if already installed
    bool dryRun{false};                           // Preview only, don't install
    std::optional<std::string> checksum;          // Override checksum verification
    InstallProgressCallback onProgress{nullptr};  // Progress callback
};

/**
 * Installation result.
 */
struct InstallResult {
    std::string pluginName;
    std::string version;
    std::filesystem::path installedPath;
    std::string checksum;
    uint64_t sizeBytes{0};
    bool wasUpgrade{false};     // true if replaced existing version
    std::string previousVersion;
    std::chrono::milliseconds elapsed{0};
};

/**
 * Plugin installer interface.
 */
class IPluginInstaller {
public:
    virtual ~IPluginInstaller() = default;

    /**
     * Install a plugin from the repository.
     * @param nameOrUrl Plugin name or direct URL
     * @param options Installation options
     * @return Installation result
     */
    virtual Result<InstallResult> install(
        const std::string& nameOrUrl,
        const InstallOptions& options = {}) = 0;

    /**
     * Uninstall a plugin.
     * @param name Plugin name
     * @param removeFromTrust Also remove from trust list
     * @return Success or error
     */
    virtual Result<void> uninstall(
        const std::string& name,
        bool removeFromTrust = true) = 0;

    /**
     * List installed plugins.
     * @return Vector of installed plugin names
     */
    virtual Result<std::vector<std::string>> listInstalled() = 0;

    /**
     * Check if a plugin is installed.
     * @param name Plugin name
     * @return Installed version or nullopt
     */
    virtual Result<std::optional<std::string>> installedVersion(
        const std::string& name) = 0;

    /**
     * Check for available updates.
     * @param name Plugin name (empty = check all)
     * @return Map of plugin name -> available version
     */
    virtual Result<std::map<std::string, std::string>> checkUpdates(
        const std::string& name = "") = 0;
};

/**
 * Create a plugin installer.
 * @param repoClient Plugin repository client
 * @param installDir Default installation directory
 * @param trustFile Trust file path
 */
std::unique_ptr<IPluginInstaller> makePluginInstaller(
    std::shared_ptr<IPluginRepoClient> repoClient,
    const std::filesystem::path& installDir = {},
    const std::filesystem::path& trustFile = {});

/**
 * Get the default plugin installation directory.
 * Order of precedence:
 * 1. $YAMS_PLUGIN_DIR environment variable
 * 2. ~/.local/lib/yams/plugins (Linux/macOS)
 * 3. %LOCALAPPDATA%/yams/plugins (Windows)
 */
std::filesystem::path getDefaultPluginInstallDir();

/**
 * Get the default plugin trust file path.
 */
std::filesystem::path getDefaultPluginTrustFile();

} // namespace yams::plugins
