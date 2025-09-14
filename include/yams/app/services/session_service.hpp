#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace yams::app::services {

struct AppContext; // forward decl

struct PrepareBudget {
    int maxCores{-1};
    int maxMemoryGb{-1};
    long maxTimeMs{-1};
    bool aggressive{false};
};

struct MaterializedItem {
    std::string name;
    std::string path;
    std::string hash;
    std::string mime;
    std::uint64_t size{0};
    std::string snippet; // may be empty
};

class ISessionService {
public:
    virtual ~ISessionService() = default;

    // Lifecycle
    virtual std::optional<std::string> current() const = 0;
    virtual std::vector<std::string> listSessions() const = 0;
    virtual bool exists(const std::string& name) const = 0;
    virtual void init(const std::string& name, const std::string& desc) = 0;
    virtual void use(const std::string& name) = 0;
    virtual void remove(const std::string& name) = 0;

    // Selectors (Phase 1: path-only)
    virtual std::vector<std::string> listPathSelectors(const std::string& name) const = 0;
    virtual void addPathSelector(const std::string& path, const std::vector<std::string>& tags,
                                 const std::vector<std::pair<std::string, std::string>>& meta) = 0;
    virtual void removePathSelector(const std::string& path) = 0;

    // Materialization & Scoping
    virtual std::vector<std::string>
    activeIncludePatterns(const std::optional<std::string>& name = std::nullopt) const = 0;
    virtual std::size_t warm(std::size_t limit, std::size_t snippetLen) = 0;
    virtual std::size_t prepare(const PrepareBudget& budget, std::size_t limit,
                                std::size_t snippetLen) = 0;
    virtual void clearMaterialized() = 0;

    // Enumerate materialized items for the current or specified session
    virtual std::vector<MaterializedItem>
    listMaterialized(const std::optional<std::string>& name = std::nullopt) const = 0;

    // Save/Load
    virtual void save(const std::optional<std::filesystem::path>& outFile) const = 0;
    virtual void load(const std::filesystem::path& inFile,
                      const std::optional<std::string>& name) = 0;

    // Session watch (auto-ingest) configuration — Phase 1 (config only)
    // Enable or disable monitoring for a session (current when name not provided)
    virtual void enableWatch(bool on, const std::optional<std::string>& name = std::nullopt) = 0;
    // Query whether monitoring is enabled
    virtual bool watchEnabled(const std::optional<std::string>& name = std::nullopt) const = 0;
    // Set or get polling interval in milliseconds
    virtual void setWatchIntervalMs(uint32_t intervalMs,
                                    const std::optional<std::string>& name = std::nullopt) = 0;
    virtual uint32_t
    watchIntervalMs(const std::optional<std::string>& name = std::nullopt) const = 0;
    // Convenience: get pinned patterns for a session (current when name not provided)
    virtual std::vector<std::string>
    getPinnedPatterns(const std::optional<std::string>& name = std::nullopt) const = 0;
};

std::shared_ptr<ISessionService> makeSessionService(const AppContext* ctx = nullptr);

} // namespace yams::app::services
