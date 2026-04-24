#pragma once

#include <atomic>
#include <chrono>
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

class TopologyTuner;

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
        std::uint64_t clusterSizeMax{0};
        std::uint64_t clusterSizeP50{0};
        std::uint64_t clusterSizeP90{0};
        std::uint64_t singletonCount{0};
        std::uint64_t orphanDocCount{0};
        double singletonRatio{0.0};
        double giantClusterRatio{0.0};
        double clusterSizeGini{0.0};
        double avgIntraEdgeWeight{0.0};
        // Phase H-TDA wiring fix: H_0 persistence computed on the cluster
        // centroids produced by this rebuild (not on raw doc embeddings).
        // Varies per arm because each clustering produces a different
        // centroid cloud. 0 means no valid centroids (e.g. one giant cluster
        // or all singletons → no MST edges between centroids).
        double clusterCentroidPersistence{0.0};
        std::size_t clusterCentroidCount{0};
        std::string cellIdentity;
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
        std::uint64_t lastClusterSizeMax{0};
        std::uint64_t lastClusterSizeP50{0};
        std::uint64_t lastClusterSizeP90{0};
        std::uint64_t lastSingletonCount{0};
        std::uint64_t lastOrphanDocCount{0};
        double lastSingletonRatio{0.0};
        double lastGiantClusterRatio{0.0};
        double lastClusterSizeGini{0.0};
        double lastAvgIntraEdgeWeight{0.0};
        std::string lastReason;
        std::string lastSnapshotId;
        std::string lastAlgorithm;
        std::string lastCellIdentity;
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

    void setPublishedEpoch(std::uint64_t epoch) {
        publishedEpoch_.store(epoch, std::memory_order_release);
    }

    [[nodiscard]] bool tryScheduleRebuild();
    void clearScheduled();

    void setAutoRebuildEnabled(bool enabled) {
        autoRebuildEnabled_.store(enabled, std::memory_order_release);
    }

    [[nodiscard]] bool autoRebuildEnabled() const {
        return autoRebuildEnabled_.load(std::memory_order_acquire);
    }

    void setHdbscanMinPoints(std::size_t n) {
        hdbscanMinPoints_.store(n, std::memory_order_release);
    }

    [[nodiscard]] std::size_t hdbscanMinPoints() const {
        return hdbscanMinPoints_.load(std::memory_order_acquire);
    }

    void setHdbscanMinClusterSize(std::size_t n) {
        hdbscanMinClusterSize_.store(n, std::memory_order_release);
    }

    [[nodiscard]] std::size_t hdbscanMinClusterSize() const {
        return hdbscanMinClusterSize_.load(std::memory_order_acquire);
    }

    void setFeatureSmoothingHops(std::size_t n) {
        featureSmoothingHops_.store(n, std::memory_order_release);
    }

    [[nodiscard]] std::size_t featureSmoothingHops() const {
        return featureSmoothingHops_.load(std::memory_order_acquire);
    }

    // Phase G: optional adaptive tuner. When set and enabled, each
    // rebuildArtifacts() call may pull a new arm (within cooldown) and
    // overwrite the HDBSCAN parameters + algorithm before clustering runs.
    void setTopologyTuner(std::shared_ptr<TopologyTuner> tuner);

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
    std::atomic<bool> autoRebuildEnabled_{true};
    std::atomic<std::uint64_t> publishedEpoch_{0};
    std::atomic<std::size_t> hdbscanMinPoints_{0};
    std::atomic<std::size_t> hdbscanMinClusterSize_{0};
    std::atomic<std::size_t> featureSmoothingHops_{0};

    // Tuner state (Phase G). Protected by tunerMutex_ since multiple
    // fields move together on each pull / observation.
    mutable std::mutex tunerMutex_;
    std::shared_ptr<TopologyTuner> tuner_;
    std::chrono::steady_clock::time_point tunerLastPullTime_{};
    std::chrono::milliseconds tunerLastDuration_{0};
    std::size_t tunerLastPullDocCount_{0};
    std::string tunerCurrentArmId_;
};

} // namespace yams::daemon
