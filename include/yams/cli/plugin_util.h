#pragma once

#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::cli::plugin {

// ============================================================================
// Plugin Trust Management
// ============================================================================

/// Read trusted plugin roots from plugins_trust.txt
/// @return Set of trusted root paths
std::set<std::filesystem::path> readTrustedRoots();

/// Check if a plugin path is within a trusted root
/// @param pluginPath Path to the plugin
/// @param trustedRoots Set of trusted root paths
/// @return true if plugin is trusted
bool isPathTrusted(const std::filesystem::path& pluginPath,
                   const std::set<std::filesystem::path>& trustedRoots);

// ============================================================================
// Plugin Resolution
// ============================================================================

/// Get default plugin search directories for the current platform
/// @return Vector of plugin directory paths
std::vector<std::filesystem::path> getPluginSearchDirs();

/// Resolve a plugin by name or path
/// Searches: exact path -> filename match -> ABI name match -> heuristic match
/// @param nameOrPath Plugin name or path
/// @return Resolved path if found
std::optional<std::filesystem::path> resolvePlugin(const std::string& nameOrPath);

// ============================================================================
// Plugin ABI Probing
// ============================================================================

/// Information extracted from a plugin's ABI symbols
struct PluginInfo {
    std::string name;
    std::string version;
    int abiVersion{0};
    bool trusted{false};
    std::string manifestJson; // Raw manifest if available
};

/// Probe a plugin for basic ABI information (dlopen + symbol lookup)
/// @param pluginPath Path to the plugin .so/.dylib/.dll
/// @return PluginInfo if successful, error otherwise
Result<PluginInfo> probePluginAbi(const std::filesystem::path& pluginPath);

/// Result of interface availability check
struct InterfaceProbe {
    std::string interfaceId;
    uint32_t version{0};
    bool available{false};
    bool hasBatchApi{false}; // For model_provider_v1: has generate_embedding_batch
};

/// Check if a specific interface is available in an already-loaded plugin
/// @param handle dlopen handle
/// @param interfaceId Interface identifier (e.g., "model_provider_v1")
/// @param version Interface version
/// @return InterfaceProbe result
InterfaceProbe probeInterface(void* handle, const std::string& interfaceId, uint32_t version);

// ============================================================================
// Platform Compatibility
// ============================================================================

/// Get platform-appropriate plugin file extension
/// @return ".so" on Linux, ".dylib" on macOS, ".dll" on Windows
std::string getPluginExtension();

/// Check if a file has a valid plugin extension
/// @param path File path
/// @return true if extension matches platform plugin extension
bool hasPluginExtension(const std::filesystem::path& path);

} // namespace yams::cli::plugin
