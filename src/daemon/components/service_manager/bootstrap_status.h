#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

namespace yams::daemon {
struct DaemonConfig;

namespace service_manager {

struct BootstrapReadinessSnapshot {
    bool ipcServerReady{false};
    bool contentStoreReady{false};
    bool databaseReady{false};
    bool metadataRepoReady{false};
    bool searchEngineReady{false};
    bool modelProviderReady{false};
    bool vectorIndexReady{false};
    bool pluginsReady{false};
    bool vectorDbInitAttempted{false};
    bool vectorDbReady{false};
    std::uint32_t vectorDbDim{0};
    int searchProgress{0};
    int vectorIndexProgress{0};
    int modelLoadProgress{0};

    bool bootstrapReady() const noexcept;
};

struct BootstrapFreshnessSnapshot {
    bool simeonLexicalConfigured{false};
    bool simeonLexicalReady{false};
    bool simeonLexicalBuilding{false};
    bool simeonFragmentGeometryReady{false};
};

struct BootstrapStatusData {
    BootstrapReadinessSnapshot readiness;
    std::optional<BootstrapFreshnessSnapshot> freshness;
    std::string databaseRecoveredAt;
    std::string databaseRecoveredFrom;
    std::string databasePhase;
    std::chrono::steady_clock::time_point databasePhaseSince;
    std::string maintenancePhase;
    std::chrono::steady_clock::time_point maintenancePhaseSince;
    std::string storageWarning;
    std::map<std::string, std::uint64_t> initDurationsMs;
    std::chrono::steady_clock::time_point startTime;
    std::filesystem::path dataDir;
};

nlohmann::json buildBootstrapStatusSnapshot(
    const BootstrapStatusData& data,
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());

bool shouldPublishBootstrapStatus(bool ready, std::chrono::steady_clock::time_point lastWriteAt,
                                  std::chrono::steady_clock::time_point now) noexcept;

// Best-effort publication for CLI progress before IPC is ready. Pre-ready writes are throttled;
// ready snapshots always publish immediately.
void writeBootstrapStatusFile(const DaemonConfig& config, const BootstrapStatusData& data);

} // namespace service_manager
} // namespace yams::daemon
