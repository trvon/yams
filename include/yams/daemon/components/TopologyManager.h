#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
#include <yams/core/types.h>

namespace yams::metadata {
class MetadataRepository;
class IMetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::vector {
class VectorDatabase;
} // namespace yams::vector

namespace yams::daemon {

class TopologyManager {
public:
    struct RebuildStats {
        bool skipped{false};
        bool dryRun{false};
        bool fullRebuild{true};
        bool stored{false};
        std::string reason;
        std::string snapshotId;
        std::string algorithm;
        std::uint64_t documentsRequested{0};
        std::uint64_t documentsProcessed{0};
        std::uint64_t documentsMissingEmbeddings{0};
        std::uint64_t documentsMissingGraphNodes{0};
        std::uint64_t neighborEdgesScanned{0};
        std::uint64_t neighborsReturned{0};
        std::uint64_t clustersBuilt{0};
        std::uint64_t membershipsBuilt{0};
        std::uint64_t dirtySeedCount{0};
        std::uint64_t dirtyRegionDocs{0};
        std::uint64_t coalescedDirtySets{0};
        std::uint64_t fallbackFullRebuilds{0};
        std::vector<std::string> issues;
    };

    struct TelemetrySnapshot {
        bool rebuildRunning{false};
        bool artifactsFresh{false};
        bool lastRunSucceeded{false};
        bool lastRunSkipped{false};
        bool lastRunFullRebuild{true};
        bool lastRunStored{false};
        std::uint64_t dirtyDocumentCount{0};
        std::uint64_t dirtySinceUnixMillis{0};
        std::uint64_t lastStartedUnixSeconds{0};
        std::uint64_t lastCompletedUnixSeconds{0};
        std::uint64_t lastSuccessAgeMs{0};
        std::uint64_t rebuildLagMs{0};
        std::uint64_t rebuildRunningAgeMs{0};
        std::uint64_t lastDurationMs{0};
        std::uint64_t rebuildsTotal{0};
        std::uint64_t rebuildFailuresTotal{0};
        std::uint64_t lastDocumentsRequested{0};
        std::uint64_t lastDocumentsProcessed{0};
        std::uint64_t lastDocumentsMissingEmbeddings{0};
        std::uint64_t lastDocumentsMissingGraphNodes{0};
        std::uint64_t lastClustersBuilt{0};
        std::uint64_t lastMembershipsBuilt{0};
        std::uint64_t lastDirtySeedCount{0};
        std::uint64_t lastDirtyRegionDocs{0};
        std::uint64_t lastCoalescedDirtySets{0};
        std::uint64_t lastFallbackFullRebuilds{0};
        std::string lastReason;
        std::string lastSnapshotId;
        std::string lastAlgorithm;
    };

    struct Dependencies {
        std::function<std::shared_ptr<metadata::MetadataRepository>()> getMetadataRepo;
        std::function<std::shared_ptr<metadata::KnowledgeGraphStore>()> getKgStore;
        std::function<std::shared_ptr<vector::VectorDatabase>()> getVectorDatabase;
    };

    explicit TopologyManager(Dependencies deps);

    void markDirty(const std::string& hash);
    void markDirtyBatch(const std::vector<std::string>& hashes);

    [[nodiscard]] std::vector<std::string> getOverlayHashes(std::size_t limit = 64) const;

    [[nodiscard]] std::vector<std::string> drainDirtyHashes();
    void restoreDirtyHashes(const std::vector<std::string>& hashes);

    [[nodiscard]] bool hasDirtyHashes() const;

    [[nodiscard]] bool isRebuildInProgress() const {
        return rebuildRunning_.load(std::memory_order_acquire);
    }

    [[nodiscard]] std::uint64_t publishedEpoch() const {
        return publishedEpoch_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool tryScheduleRebuild();
    void clearScheduled();

    [[nodiscard]] TelemetrySnapshot getTelemetrySnapshot() const;

    Result<RebuildStats> rebuildArtifacts(const std::string& reason, bool dryRun,
                                          const std::vector<std::string>& documentHashes,
                                          const std::string& topologyAlgorithm);

private:
    Result<RebuildStats> runRebuild(const std::string& reason, bool dryRun,
                                    const std::vector<std::string>& documentHashes,
                                    const std::string& topologyAlgorithm);

    static std::uint64_t nowUnixSeconds();
    static std::uint64_t nowUnixMillis();

    Dependencies deps_;

    mutable std::mutex dirtyMutex_;
    std::unordered_set<std::string> dirtyHashes_;

    mutable std::mutex telemetryMutex_;
    TelemetrySnapshot telemetry_;

    std::atomic<bool> rebuildRunning_{false};
    std::atomic<bool> rebuildScheduled_{false};
    std::atomic<std::uint64_t> publishedEpoch_{0};
};

} // namespace yams::daemon
