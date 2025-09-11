#pragma once

#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>
#include <yams/core/types.h>

namespace yams::daemon {

class AbiPluginLoader {
public:
    struct ScanResult {
        std::string name;
        std::string version;
        uint32_t abiVersion{0};
        std::filesystem::path path;
        std::string manifestJson;
        std::vector<std::string> interfaces; // parsed from manifest if present
    };

    AbiPluginLoader() = default;

    // Trust policy (config-backed)
    std::vector<std::filesystem::path> trustList() const;
    Result<void> trustAdd(const std::filesystem::path& p);
    Result<void> trustRemove(const std::filesystem::path& p);

    // Scanning APIs (no init)
    Result<ScanResult> scanTarget(const std::filesystem::path& file) const;
    Result<std::vector<ScanResult>> scanDirectory(const std::filesystem::path& dir) const;

    // Loading APIs (init/shutdown)
    Result<ScanResult> load(const std::filesystem::path& file, const std::string& configJson = "");
    Result<void> unload(const std::string& name);
    std::vector<ScanResult> loaded() const;

    // Query plugin health JSON via ABI, if the plugin exposes it
    Result<std::string> health(const std::string& name) const;

    // Configuration
    void setTrustFile(const std::filesystem::path& f) {
        trustFile_ = f;
        loadTrust();
    }

private:
    struct HandleInfo {
        void* handle{nullptr};
        void* host_context{nullptr};
        ScanResult info;
    };

    mutable std::map<std::string, HandleInfo> loaded_;
    std::filesystem::path trustFile_;
    std::set<std::filesystem::path> trusted_;

    void loadTrust();
    void saveTrust() const;
    bool isTrusted(const std::filesystem::path& p) const;

public:
    // Retrieve a typed interface from a loaded plugin via C-ABI
    // Returns pointer to interface table/struct as provided by the plugin.
    Result<void*> getInterface(const std::string& name, const std::string& ifaceId,
                               uint32_t version) const;
};

} // namespace yams::daemon
