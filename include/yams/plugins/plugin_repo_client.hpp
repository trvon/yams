#pragma once
/**
 * @file plugin_repo_client.hpp
 * @brief Client interface for querying the YAMS Plugin Repository API
 *
 * The Plugin Repository is a remote service (default: https://repo.yams.dev) that provides:
 * - Plugin discovery and search
 * - Plugin metadata (manifest, versions, checksums)
 * - Plugin download URLs
 *
 * Environment variables:
 *   YAMS_PLUGIN_REPO_URL - Override default repository URL
 */

#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <yams/core/types.h>

namespace yams::plugins {

// Default plugin repository URL
constexpr const char* DEFAULT_PLUGIN_REPO_URL = "https://repo.yams.dev";

/**
 * Plugin metadata from the repository.
 */
struct RemotePluginInfo {
    std::string name;
    std::string version;
    std::string description;
    std::vector<std::string> interfaces;
    std::string author;
    std::string license;
    std::string minYamsVersion;

    // Download info
    std::string downloadUrl; // Full URL to .tar.gz bundle
    std::string checksum;    // sha256:<hex>
    uint64_t sizeBytes{0};

    // Platform compatibility
    std::string platform; // linux-x86_64, darwin-arm64, windows-x86_64, etc.
    std::string arch;     // x86_64, arm64, etc.
    int abiVersion{1};

    // Timestamps
    std::string publishedAt;
    std::string updatedAt;

    // Usage metrics (optional)
    uint64_t downloads{0};
};

/**
 * Brief plugin listing (for search results).
 */
struct RemotePluginSummary {
    std::string name;
    std::string latestVersion;
    std::string description;
    std::vector<std::string> interfaces;
    uint64_t downloads{0};
};

/**
 * Plugin repository client configuration.
 */
struct PluginRepoConfig {
    std::string repoUrl{DEFAULT_PLUGIN_REPO_URL};
    std::chrono::milliseconds timeout{30000};
    bool verifyTls{true};
    std::optional<std::string> proxy;
    std::string userAgent{"yams-cli"};
};

/**
 * Interface for querying the plugin repository.
 */
class IPluginRepoClient {
public:
    virtual ~IPluginRepoClient() = default;

    /**
     * List all available plugins.
     * @param filter Optional search filter (name, interface, etc.)
     * @return Vector of plugin summaries
     */
    virtual Result<std::vector<RemotePluginSummary>> list(const std::string& filter = "") = 0;

    /**
     * Get detailed metadata for a specific plugin.
     * @param name Plugin name
     * @param version Optional version (default: latest)
     * @return Plugin metadata including download URL
     */
    virtual Result<RemotePluginInfo>
    get(const std::string& name, const std::optional<std::string>& version = std::nullopt) = 0;

    /**
     * List all versions of a plugin.
     * @param name Plugin name
     * @return Vector of version strings (newest first)
     */
    virtual Result<std::vector<std::string>> versions(const std::string& name) = 0;

    /**
     * Check if a plugin exists in the repository.
     * @param name Plugin name
     * @return true if plugin exists
     */
    virtual Result<bool> exists(const std::string& name) = 0;
};

/**
 * Create a plugin repository client.
 */
std::unique_ptr<IPluginRepoClient> makePluginRepoClient(const PluginRepoConfig& config = {});

} // namespace yams::plugins
