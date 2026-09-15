#include "bootstrap_status.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <map>
#include <mutex>
#include <vector>

#include <spdlog/spdlog.h>
#include <yams/common/fs_utils.h>
#include <yams/config/config_helpers.h>
#include <yams/daemon/daemon.h>
#include <yams/daemon/metric_keys.h>

namespace yams::daemon::service_manager {
namespace {

constexpr auto kPreReadyWriteInterval = std::chrono::milliseconds{250};

int computeEtaRemaining(int expectedSeconds, int progressPercent) {
    if (expectedSeconds <= 0) {
        return 0;
    }
    const int completed = (expectedSeconds * progressPercent + 99) / 100;
    return std::max(0, expectedSeconds - completed);
}

Result<config::ResolvedRuntimePaths> resolveRuntimePaths(const DaemonConfig& daemonConfig) {
    config::RuntimePathOverrides overrides;
    if (!daemonConfig.configFilePath.empty()) {
        overrides.configFile = daemonConfig.configFilePath;
    }
    if (!daemonConfig.dataDir.empty()) {
        overrides.dataDir = daemonConfig.dataDir;
    }
    if (!daemonConfig.socketPath.empty()) {
        overrides.socketPath = daemonConfig.socketPath;
    }
    if (!daemonConfig.pidFile.empty()) {
        overrides.pidFile = daemonConfig.pidFile;
    }
    return config::resolve_runtime_paths(overrides);
}

} // namespace

bool BootstrapReadinessSnapshot::bootstrapReady() const noexcept {
    return ipcServerReady && contentStoreReady && databaseReady && metadataRepoReady &&
           searchEngineReady && modelProviderReady && vectorIndexReady && pluginsReady;
}

nlohmann::json buildBootstrapStatusSnapshot(const BootstrapStatusData& data,
                                            std::chrono::steady_clock::time_point now) {
    const auto& readinessSnapshot = data.readiness;
    nlohmann::json status;
    status["ready"] = readinessSnapshot.bootstrapReady();
    std::string overall = readinessSnapshot.bootstrapReady()
                              ? "Ready"
                              : (readinessSnapshot.ipcServerReady ? "Initializing" : "Starting");
    for (auto& character : overall) {
        character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
    }
    status["overall"] = std::move(overall);

    nlohmann::json readinessJson;
    readinessJson[std::string(readiness::kIpcServer)] = readinessSnapshot.ipcServerReady;
    readinessJson[std::string(readiness::kContentStore)] = readinessSnapshot.contentStoreReady;
    readinessJson[std::string(readiness::kDatabase)] = readinessSnapshot.databaseReady;
    readinessJson[std::string(readiness::kMetadataRepo)] = readinessSnapshot.metadataRepoReady;
    readinessJson[std::string(readiness::kSearchEngine)] = readinessSnapshot.searchEngineReady;
    readinessJson[std::string(readiness::kModelProvider)] = readinessSnapshot.modelProviderReady;
    readinessJson[std::string(readiness::kVectorIndex)] = readinessSnapshot.vectorIndexReady;
    readinessJson[std::string(readiness::kPlugins)] = readinessSnapshot.pluginsReady;
    readinessJson[std::string(readiness::kVectorDbInitAttempted)] =
        readinessSnapshot.vectorDbInitAttempted;
    readinessJson[std::string(readiness::kVectorDbReady)] = readinessSnapshot.vectorDbReady;
    readinessJson[std::string(readiness::kVectorDbDim)] = readinessSnapshot.vectorDbDim;
    if (data.freshness) {
        const auto& freshness = *data.freshness;
        readinessJson[std::string(readiness::kSearchEngineLexicalEnhancementConfigured)] =
            freshness.simeonLexicalConfigured;
        readinessJson[std::string(readiness::kSearchEngineLexicalEnhancementReady)] =
            freshness.simeonLexicalReady;
        readinessJson[std::string(readiness::kSearchEngineLexicalEnhancementBuilding)] =
            freshness.simeonLexicalBuilding;
        readinessJson[std::string(readiness::kSearchEngineFragmentGeometryReady)] =
            freshness.simeonFragmentGeometryReady;
        if (!freshness.simeonLexicalConfigured) {
            status["search_engine_lexical_enhancement_state"] = "disabled";
        } else if (freshness.simeonLexicalBuilding) {
            status["search_engine_lexical_enhancement_state"] = "building";
        } else if (freshness.simeonLexicalReady) {
            status["search_engine_lexical_enhancement_state"] = "ready";
        } else {
            status["search_engine_lexical_enhancement_state"] = "skipped";
        }
    }
    status["readiness"] = std::move(readinessJson);

    if (!data.databaseRecoveredAt.empty()) {
        status[std::string(status_keys::kDatabaseRecoveredAt)] = data.databaseRecoveredAt;
        status[std::string(status_keys::kDatabaseRecoveredFrom)] = data.databaseRecoveredFrom;
    }
    if (!data.databasePhase.empty()) {
        status[std::string(status_keys::kDatabasePhase)] = data.databasePhase;
        if (data.databasePhaseSince.time_since_epoch().count() != 0) {
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - data.databasePhaseSince)
                    .count();
            status[std::string(status_keys::kDatabasePhaseElapsedMs)] =
                static_cast<std::uint64_t>(elapsed);
        }
    }
    if (!data.maintenancePhase.empty()) {
        status[std::string(status_keys::kMaintenancePhase)] = data.maintenancePhase;
        if (data.maintenancePhaseSince.time_since_epoch().count() != 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                     now - data.maintenancePhaseSince)
                                     .count();
            status[std::string(status_keys::kMaintenancePhaseElapsedMs)] =
                static_cast<std::uint64_t>(elapsed);
        }
    }
    if (!data.storageWarning.empty()) {
        status[std::string(status_keys::kStorageWarning)] = data.storageWarning;
    }

    nlohmann::json progress;
    progress[std::string(readiness::kSearchEngine)] = readinessSnapshot.searchProgress;
    progress[std::string(readiness::kVectorIndex)] = readinessSnapshot.vectorIndexProgress;
    progress[std::string(readiness::kModelProvider)] = readinessSnapshot.modelLoadProgress;
    status["progress"] = std::move(progress);

    const auto secondsSinceStart =
        std::chrono::duration_cast<std::chrono::seconds>(now - data.startTime).count();
    const std::map<std::string, int> expectedSeconds{
        {std::string(readiness::kPlugins), 1},       {std::string(readiness::kContentStore), 2},
        {std::string(readiness::kDatabase), 2},      {std::string(readiness::kMetadataRepo), 2},
        {std::string(readiness::kVectorIndex), 3},   {std::string(readiness::kSearchEngine), 4},
        {std::string(readiness::kModelProvider), 20}};
    nlohmann::json eta;
    const auto addEta = [&](const std::string& key, bool ready, int progressPercent) {
        if (ready) {
            return;
        }
        const auto expected = expectedSeconds.find(key);
        int expectedForComponent = expected != expectedSeconds.end() ? expected->second : 5;
        const auto historical = data.initDurationsMs.find(key);
        if (historical != data.initDurationsMs.end()) {
            const int historicalSeconds = static_cast<int>((historical->second + 999) / 1000);
            if (historicalSeconds > 0) {
                expectedForComponent = historicalSeconds;
            }
        }
        const int remainingByProgress = computeEtaRemaining(expectedForComponent, progressPercent);
        const int remainingByElapsed =
            std::max(0, expectedForComponent - static_cast<int>(secondsSinceStart));
        eta[key] = std::max(remainingByProgress, remainingByElapsed);
    };
    addEta(std::string(readiness::kPlugins), readinessSnapshot.pluginsReady, 100);
    addEta(std::string(readiness::kContentStore), readinessSnapshot.contentStoreReady, 100);
    addEta(std::string(readiness::kDatabase), readinessSnapshot.databaseReady, 100);
    addEta(std::string(readiness::kMetadataRepo), readinessSnapshot.metadataRepoReady, 100);
    addEta(std::string(readiness::kVectorIndex), readinessSnapshot.vectorIndexReady,
           readinessSnapshot.vectorIndexProgress);
    addEta(std::string(readiness::kSearchEngine), readinessSnapshot.searchEngineReady,
           readinessSnapshot.searchProgress);
    addEta(std::string(readiness::kModelProvider), readinessSnapshot.modelProviderReady,
           readinessSnapshot.modelLoadProgress);
    status["eta_seconds"] = std::move(eta);

    if (!data.initDurationsMs.empty()) {
        status["durations_ms"] = data.initDurationsMs;
        std::vector<std::pair<std::string, std::uint64_t>> items(data.initDurationsMs.begin(),
                                                                 data.initDurationsMs.end());
        std::sort(items.begin(), items.end(),
                  [](const auto& lhs, const auto& rhs) { return lhs.second > rhs.second; });
        nlohmann::json slowest;
        const std::size_t count = std::min<std::size_t>(3, items.size());
        for (std::size_t index = 0; index < count; ++index) {
            slowest.push_back({{"name", items[index].first}, {"elapsed_ms", items[index].second}});
        }
        if (!slowest.empty()) {
            status["top_slowest"] = std::move(slowest);
        }
    }

    status["uptime_seconds"] = secondsSinceStart;
    status["data_dir"] = data.dataDir.string();
    return status;
}

bool shouldPublishBootstrapStatus(bool ready, std::chrono::steady_clock::time_point lastWriteAt,
                                  std::chrono::steady_clock::time_point now) noexcept {
    return ready || lastWriteAt.time_since_epoch().count() == 0 ||
           now - lastWriteAt >= kPreReadyWriteInterval;
}

void writeBootstrapStatusFile(const DaemonConfig& config, const BootstrapStatusData& data) {
    static std::mutex lastWriteMutex;
    static std::chrono::steady_clock::time_point lastWriteAt{};
    {
        const auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(lastWriteMutex);
        if (!shouldPublishBootstrapStatus(data.readiness.bootstrapReady(), lastWriteAt, now)) {
            return;
        }
        lastWriteAt = now;
    }

    try {
        const auto runtimePaths = resolveRuntimePaths(config);
        if (!runtimePaths) {
            spdlog::debug("[ServiceManager] Bootstrap path resolution failed: {}",
                          runtimePaths.error().message);
            return;
        }
        const std::filesystem::path& directory = runtimePaths.value().runtimeDir.value;
        if (directory.empty()) {
            return;
        }
        common::ensureDirectories(directory);
        const std::filesystem::path path = directory / "yams-daemon.status.json";
        std::ofstream output(path);
        if (output) {
            output << buildBootstrapStatusSnapshot(data).dump(2);
        }
    } catch (...) {
        spdlog::debug("[ServiceManager] Failed to write bootstrap status file");
    }
}

} // namespace yams::daemon::service_manager
