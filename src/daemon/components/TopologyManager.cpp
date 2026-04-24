#include <yams/daemon/components/TopologyManager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <string_view>

#include <yams/daemon/components/TopologyTuner.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/search/topological_quality.h>
#include <yams/vector/vector_database.h>

#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_factory.h>
#include <yams/topology/topology_input_extractor.h>
#include <yams/topology/topology_metadata_store.h>

namespace yams::daemon {

using yams::Error;
using yams::ErrorCode;
using yams::Result;

namespace {

std::string envOr(const char* name, std::string_view fallback) {
    const char* raw = std::getenv(name);
    if (raw != nullptr && *raw != '\0') {
        return raw;
    }
    return std::string{fallback};
}

std::string computeCellIdentity() {
    std::string identity;
    identity.reserve(128);
    identity.append("topk=");
    identity.append(envOr("YAMS_GRAPH_SEMANTIC_TOPK", "default"));
    identity.append(";thr=");
    identity.append(envOr("YAMS_GRAPH_SEMANTIC_THRESHOLD", "default"));
    identity.append(";engine=");
    identity.append(envOr("YAMS_TOPOLOGY_ENGINE", "default"));
    identity.append(";reciprocal=");
    identity.append(envOr("YAMS_TOPOLOGY_RECIPROCAL_ONLY", "default"));
    identity.append(";min_edge=");
    identity.append(envOr("YAMS_TOPOLOGY_MIN_EDGE_SCORE", "default"));
    identity.append(";max_neighbors=");
    identity.append(envOr("YAMS_TOPOLOGY_MAX_NEIGHBORS", "default"));
    return identity;
}

} // namespace

TopologyManager::TopologyManager(Dependencies deps) : deps_(std::move(deps)) {}

void TopologyManager::markDirty(const std::string& hash) {
    if (hash.empty())
        return;
    std::lock_guard<std::mutex> lock(dirtyMutex_);
    const bool wasEmpty = dirtyHashes_.empty();
    dirtyHashes_.insert(hash);
    if (wasEmpty) {
        std::lock_guard<std::mutex> telemetryLock(telemetryMutex_);
        telemetry_.dirtySinceUnixMillis = nowUnixMillis();
    }
}

void TopologyManager::markDirtyBatch(const std::vector<std::string>& hashes) {
    if (hashes.empty())
        return;
    std::lock_guard<std::mutex> lock(dirtyMutex_);
    const bool wasEmpty = dirtyHashes_.empty();
    for (const auto& hash : hashes) {
        if (!hash.empty())
            dirtyHashes_.insert(hash);
    }
    if (wasEmpty && !dirtyHashes_.empty()) {
        std::lock_guard<std::mutex> telemetryLock(telemetryMutex_);
        telemetry_.dirtySinceUnixMillis = nowUnixMillis();
    }
}

std::vector<std::string> TopologyManager::getOverlayHashes(std::size_t limit) const {
    std::vector<std::string> hashes;
    std::lock_guard<std::mutex> lock(dirtyMutex_);
    hashes.reserve(std::min(limit, dirtyHashes_.size()));
    for (const auto& hash : dirtyHashes_) {
        if (hashes.size() >= limit)
            break;
        hashes.push_back(hash);
    }
    return hashes;
}

std::vector<std::string> TopologyManager::drainDirtyHashes() {
    std::lock_guard<std::mutex> lock(dirtyMutex_);
    std::vector<std::string> hashes(dirtyHashes_.begin(), dirtyHashes_.end());
    dirtyHashes_.clear();
    return hashes;
}

void TopologyManager::restoreDirtyHashes(const std::vector<std::string>& hashes) {
    if (hashes.empty())
        return;
    std::lock_guard<std::mutex> lock(dirtyMutex_);
    const bool wasEmpty = dirtyHashes_.empty();
    for (const auto& hash : hashes) {
        dirtyHashes_.insert(hash);
    }
    if (wasEmpty && !dirtyHashes_.empty()) {
        std::lock_guard<std::mutex> telemetryLock(telemetryMutex_);
        telemetry_.dirtySinceUnixMillis = nowUnixMillis();
    }
}

bool TopologyManager::hasDirtyHashes() const {
    std::lock_guard<std::mutex> lock(dirtyMutex_);
    return !dirtyHashes_.empty();
}

bool TopologyManager::tryScheduleRebuild() {
    if (!autoRebuildEnabled_.load(std::memory_order_acquire)) {
        return false;
    }
    bool expected = false;
    return rebuildScheduled_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}

void TopologyManager::clearScheduled() {
    rebuildScheduled_.store(false, std::memory_order_release);
}

TopologyManager::TelemetrySnapshot TopologyManager::getTelemetrySnapshot() const {
    TelemetrySnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(telemetryMutex_);
        snapshot = telemetry_;
    }
    {
        std::lock_guard<std::mutex> lock(dirtyMutex_);
        snapshot.dirtyDocumentCount = dirtyHashes_.size();
    }
    snapshot.rebuildRunning = rebuildRunning_.load(std::memory_order_acquire);
    const auto nowMs = nowUnixMillis();
    if (snapshot.lastCompletedUnixSeconds > 0) {
        const auto completedMs = snapshot.lastCompletedUnixSeconds * 1000ULL;
        snapshot.lastSuccessAgeMs = nowMs > completedMs ? nowMs - completedMs : 0;
    }
    if (snapshot.dirtyDocumentCount > 0 && snapshot.dirtySinceUnixMillis > 0) {
        snapshot.rebuildLagMs =
            nowMs > snapshot.dirtySinceUnixMillis ? nowMs - snapshot.dirtySinceUnixMillis : 0;
    } else {
        snapshot.rebuildLagMs = 0;
    }
    if (snapshot.rebuildRunning && snapshot.lastStartedUnixSeconds > 0) {
        const auto startedMs = snapshot.lastStartedUnixSeconds * 1000ULL;
        snapshot.rebuildRunningAgeMs = nowMs > startedMs ? nowMs - startedMs : 0;
    } else {
        snapshot.rebuildRunningAgeMs = 0;
    }
    snapshot.artifactsFresh = !snapshot.rebuildRunning && snapshot.dirtyDocumentCount == 0 &&
                              snapshot.lastRunStored && snapshot.lastCompletedUnixSeconds > 0;
    return snapshot;
}

void TopologyManager::setTopologyTuner(std::shared_ptr<TopologyTuner> tuner) {
    std::lock_guard<std::mutex> lock(tunerMutex_);
    tuner_ = std::move(tuner);
    tunerCurrentArmId_.clear();
    tunerLastPullTime_ = std::chrono::steady_clock::time_point{};
    tunerLastDuration_ = std::chrono::milliseconds{0};
    tunerLastPullDocCount_ = 0;
}

Result<TopologyManager::RebuildStats>
TopologyManager::rebuildArtifacts(const std::string& reason, bool dryRun,
                                  const std::vector<std::string>& documentHashes,
                                  const std::string& topologyAlgorithm) {
    bool expected = false;
    if (!rebuildRunning_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        RebuildStats skipped;
        skipped.skipped = true;
        skipped.dryRun = dryRun;
        skipped.fullRebuild = documentHashes.empty();
        skipped.reason = reason;
        skipped.issues.push_back("topology rebuild already in progress");
        return skipped;
    }

    struct Guard {
        std::atomic<bool>& running;
        ~Guard() { running.store(false, std::memory_order_release); }
    } guard{rebuildRunning_};

    const auto startedAt = std::chrono::steady_clock::now();
    const std::string cellIdentity = computeCellIdentity();
    {
        std::lock_guard<std::mutex> lock(telemetryMutex_);
        ++telemetry_.rebuildsTotal;
        telemetry_.lastReason = reason;
        telemetry_.lastRunSucceeded = false;
        telemetry_.lastRunSkipped = false;
        telemetry_.lastRunStored = false;
        telemetry_.lastRunFullRebuild = documentHashes.empty();
        telemetry_.lastStartedUnixSeconds = nowUnixSeconds();
        telemetry_.lastDocumentsRequested = documentHashes.size();
        telemetry_.lastCellIdentity = cellIdentity;
    }

    std::string effectiveAlgorithm = topologyAlgorithm;
    std::string pulledArmId;
    {
        std::lock_guard<std::mutex> lock(tunerMutex_);
        if (tuner_ && tuner_->config().enabled) {
            const auto now = std::chrono::steady_clock::now();
            const std::size_t curDocCount = documentHashes.size();
            // Rebuild arm grid against current corpus size so cluster-size
            // candidates (log2(n), sqrt(n), 0.05·n) stay meaningful as the
            // corpus grows. No-op when the new grid's arm ids match the
            // current set — preserves MAB state across non-trivial rebuilds.
            if (curDocCount > 0 && tuner_->rebuildArmGridForCorpusSize(curDocCount)) {
                spdlog::info("[TopologyManager] tuner arm grid resized for "
                             "corpus={} → {} arms",
                             curDocCount, tuner_->arms().size());
            }
            if (tuner_->canPullArm(now, tunerLastPullTime_, tunerLastDuration_, curDocCount,
                                   tunerLastPullDocCount_)) {
                if (auto arm = tuner_->selectArm()) {
                    pulledArmId = arm->id;
                    tunerCurrentArmId_ = arm->id;
                    tunerLastPullTime_ = now;
                    tunerLastPullDocCount_ = curDocCount;
                    setHdbscanMinClusterSize(arm->hdbscanMinClusterSize);
                    setHdbscanMinPoints(arm->hdbscanMinPoints);
                    setFeatureSmoothingHops(arm->featureSmoothingHops);
                    if (!arm->engine.empty()) {
                        effectiveAlgorithm = arm->engine;
                    }
                    spdlog::info("[TopologyManager] tuner pulled arm '{}' (engine={} minc={} "
                                 "minp={} hops={})",
                                 arm->id, arm->engine, arm->hdbscanMinClusterSize,
                                 arm->hdbscanMinPoints, arm->featureSmoothingHops);
                }
            }
        }
    }

    auto result = runRebuild(reason, dryRun, documentHashes, effectiveAlgorithm);
    if (result) {
        result.value().cellIdentity = cellIdentity;
    }
    if (!result) {
        spdlog::warn("[TopologyManager] rebuild failed (reason={}): {}", reason,
                     result.error().message);
        std::lock_guard<std::mutex> lock(telemetryMutex_);
        ++telemetry_.rebuildFailuresTotal;
        telemetry_.lastDurationMs =
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - startedAt)
                                           .count());
        return Result<RebuildStats>(result.error());
    }

    const auto& stats = result.value();
    {
        std::lock_guard<std::mutex> lock(telemetryMutex_);
        telemetry_.lastRunSucceeded = !stats.skipped;
        telemetry_.lastRunSkipped = stats.skipped;
        telemetry_.lastRunStored = stats.stored;
        telemetry_.lastRunFullRebuild = stats.fullRebuild;
        telemetry_.lastCompletedUnixSeconds = nowUnixSeconds();
        telemetry_.lastDurationMs =
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                           std::chrono::steady_clock::now() - startedAt)
                                           .count());
        telemetry_.lastSnapshotId = stats.snapshotId;
        telemetry_.lastAlgorithm = stats.algorithm;
        telemetry_.lastDocumentsRequested = stats.documentsRequested;
        telemetry_.lastDocumentsProcessed = stats.documentsProcessed;
        telemetry_.lastDocumentsMissingEmbeddings = stats.documentsMissingEmbeddings;
        telemetry_.lastDocumentsMissingGraphNodes = stats.documentsMissingGraphNodes;
        telemetry_.lastClustersBuilt = stats.clustersBuilt;
        telemetry_.lastMembershipsBuilt = stats.membershipsBuilt;
        telemetry_.lastDirtySeedCount = stats.dirtySeedCount;
        telemetry_.lastDirtyRegionDocs = stats.dirtyRegionDocs;
        telemetry_.lastCoalescedDirtySets = stats.coalescedDirtySets;
        telemetry_.lastFallbackFullRebuilds = stats.fallbackFullRebuilds;
        telemetry_.lastClusterSizeMax = stats.clusterSizeMax;
        telemetry_.lastClusterSizeP50 = stats.clusterSizeP50;
        telemetry_.lastClusterSizeP90 = stats.clusterSizeP90;
        telemetry_.lastSingletonCount = stats.singletonCount;
        telemetry_.lastOrphanDocCount = stats.orphanDocCount;
        telemetry_.lastSingletonRatio = stats.singletonRatio;
        telemetry_.lastGiantClusterRatio = stats.giantClusterRatio;
        telemetry_.lastClusterSizeGini = stats.clusterSizeGini;
        telemetry_.lastAvgIntraEdgeWeight = stats.avgIntraEdgeWeight;
    }
    if (stats.skipped) {
        spdlog::info(
            "[TopologyManager] rebuild skipped (reason={}, docs={}, missing_embeddings={}, "
            "missing_graph_nodes={}, issues={})",
            reason, stats.documentsProcessed, stats.documentsMissingEmbeddings,
            stats.documentsMissingGraphNodes, stats.issues.empty() ? 0 : stats.issues.size());
    } else {
        spdlog::info("[TopologyManager] rebuild complete (reason={}, docs={}, clusters={}, "
                     "memberships={}, stored={}, snapshot={})",
                     reason, stats.documentsProcessed, stats.clustersBuilt, stats.membershipsBuilt,
                     stats.stored ? 1 : 0, stats.snapshotId);
    }

    {
        std::lock_guard<std::mutex> lock(tunerMutex_);
        if (tuner_ && !tunerCurrentArmId_.empty()) {
            tunerLastDuration_ =
                std::chrono::milliseconds{static_cast<std::chrono::milliseconds::rep>(
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - startedAt)
                        .count())};
            const auto rewardMode = tuner_->config().rewardMode;
            if (rewardMode == TunerRewardMode::Geometric) {
                tuner_->observeRebuildStats(tunerCurrentArmId_, stats);
                const double reward = computeIntrinsicReward(stats, tuner_->config().weights);
                spdlog::info("[TopologyManager] tuner observed arm '{}' reward={:.4f} "
                             "(singleton={:.3f} giant={:.3f} gini={:.3f} intra={:.3f})",
                             tunerCurrentArmId_, reward, stats.singletonRatio,
                             stats.giantClusterRatio, stats.clusterSizeGini,
                             stats.avgIntraEdgeWeight);
            } else {
                // Phase H-TDA wiring fix: persistence now comes from the per-arm
                // cluster centroids computed inside runRebuild (not from a
                // sample of raw doc embeddings, which was corpus-constant).
                const double persistence = stats.clusterCentroidPersistence;
                const double scale = stats.clusterCentroidCount > 1
                                         ? static_cast<double>(stats.clusterCentroidCount - 1)
                                         : 1.0;
                tuner_->observeRebuildStatsWithPersistence(tunerCurrentArmId_, stats, persistence,
                                                           scale);
                const double geometric = computeIntrinsicReward(stats, tuner_->config().weights);
                const char* modeStr =
                    (rewardMode == TunerRewardMode::Persistence) ? "persistence" : "hybrid";
                spdlog::info("[TopologyManager] tuner observed arm '{}' mode={} "
                             "geometric={:.4f} persistence={:.4f} scale={:.1f} centroids={} "
                             "(singleton={:.3f} giant={:.3f})",
                             tunerCurrentArmId_, modeStr, geometric, persistence, scale,
                             stats.clusterCentroidCount, stats.singletonRatio,
                             stats.giantClusterRatio);
            }
            tunerCurrentArmId_.clear();
        }
    }

    return result;
}

Result<TopologyManager::RebuildStats>
TopologyManager::runRebuild(const std::string& reason, bool dryRun,
                            const std::vector<std::string>& documentHashes,
                            const std::string& topologyAlgorithm) {
    auto metadataRepo = deps_.getMetadataRepo();
    auto kgStore = deps_.getKgStore();
    auto vectorDb = deps_.getVectorDatabase();
    if (!metadataRepo || !kgStore || !vectorDb) {
        return Result<RebuildStats>(
            Error{ErrorCode::InvalidState,
                  "topology rebuild requires metadata, KG store, and vector database"});
    }

    auto metadataIface = std::static_pointer_cast<metadata::IMetadataRepository>(metadataRepo);
    topology::MetadataKgTopologyArtifactStore store(metadataIface, kgStore);
    auto extractor =
        std::make_shared<topology::TopologyInputExtractor>(metadataIface, kgStore, vectorDb);

    const std::string_view algorithmKey = topology::resolveFactoryKey(topologyAlgorithm);
    auto engine = topology::makeEngine(algorithmKey);
    if (algorithmKey != topologyAlgorithm) {
        spdlog::info("[topology] requested algorithm '{}' not registered; using '{}'",
                     topologyAlgorithm, algorithmKey);
    }

    std::optional<topology::TopologyArtifactBatch> latestBatch;
    {
        auto latestResult = store.loadLatest();
        if (!latestResult) {
            return Result<RebuildStats>(latestResult.error());
        }
        latestBatch = std::move(latestResult.value());
    }

    std::vector<std::string> rebuildHashes = documentHashes;
    topology::TopologyDirtyRegion dirtyRegion;
    topology::TopologyExtractionStats seedExtractionStats;
    topology::TopologyUpdateStats updateStats;
    if (!documentHashes.empty()) {
        topology::TopologyExtractionConfig seedConfig;
        seedConfig.documentHashes = documentHashes;
        seedConfig.limit = static_cast<int>(documentHashes.size());
        seedConfig.maxNeighborsPerDocument = 32;
        seedConfig.includeEmbeddings = true;
        seedConfig.includeMetadata = false;
        seedConfig.requireEmbeddings = false;
        seedConfig.requireGraphNode = false;

        topology::TopologyBuildConfig regionConfig;
        regionConfig.mode = topology::TopologyBuildMode::Incremental;
        regionConfig.inputKind = topology::TopologyInputKind::Hybrid;
        regionConfig.reciprocalOnly = true;
        regionConfig.maxNeighborsPerDocument = seedConfig.maxNeighborsPerDocument;

        auto seedExtracted = extractor->extract(seedConfig, &seedExtractionStats);
        if (!seedExtracted) {
            return Result<RebuildStats>(seedExtracted.error());
        }

        if (latestBatch.has_value()) {
            auto dirtyRegionResult =
                engine->defineDirtyRegion(*latestBatch, seedExtracted.value(), regionConfig);
            if (!dirtyRegionResult) {
                return Result<RebuildStats>(dirtyRegionResult.error());
            }
            dirtyRegion = std::move(dirtyRegionResult.value());
            if (!dirtyRegion.expandedDocumentHashes.empty()) {
                rebuildHashes = dirtyRegion.expandedDocumentHashes;
            }
        }
    }

    topology::TopologyExtractionConfig extractionConfig;
    extractionConfig.documentHashes = rebuildHashes;
    extractionConfig.limit = rebuildHashes.empty() ? 0 : static_cast<int>(rebuildHashes.size());
    extractionConfig.maxNeighborsPerDocument = 32;
    extractionConfig.includeEmbeddings = true;
    extractionConfig.includeMetadata = true;
    extractionConfig.requireEmbeddings = true;
    extractionConfig.requireGraphNode = true;

    topology::TopologyBuildConfig buildConfig;
    buildConfig.mode = topology::TopologyBuildMode::Approximate;
    buildConfig.inputKind = topology::TopologyInputKind::Hybrid;
    buildConfig.reciprocalOnly = true;
    buildConfig.maxNeighborsPerDocument = extractionConfig.maxNeighborsPerDocument;
    buildConfig.hdbscanMinPoints = hdbscanMinPoints_.load(std::memory_order_acquire);
    buildConfig.hdbscanMinClusterSize = hdbscanMinClusterSize_.load(std::memory_order_acquire);
    buildConfig.featureSmoothingHops = featureSmoothingHops_.load(std::memory_order_acquire);

    topology::TopologyExtractionStats extractionStats;
    auto extracted = extractor->extract(extractionConfig, &extractionStats);
    if (!extracted) {
        return Result<RebuildStats>(extracted.error());
    }

    if (!documentHashes.empty() && extracted.value().empty()) {
        RebuildStats skipped;
        skipped.skipped = true;
        skipped.dryRun = dryRun;
        skipped.fullRebuild = false;
        skipped.reason = reason;
        skipped.documentsRequested = extractionStats.documentsRequested;
        skipped.documentsProcessed = 0;
        skipped.documentsMissingEmbeddings = extractionStats.documentsMissingEmbeddings;
        skipped.documentsMissingGraphNodes = extractionStats.documentsMissingGraphNodes;
        skipped.neighborEdgesScanned = extractionStats.neighborEdgesScanned;
        skipped.neighborsReturned = extractionStats.neighborsReturned;
        skipped.dirtySeedCount = seedExtractionStats.seedDocuments;
        skipped.dirtyRegionDocs = extractionStats.regionDocuments;
        skipped.issues.push_back(
            "dirty-region extraction returned no ready docs; deferring topology rebuild");
        return skipped;
    }

    Result<topology::TopologyArtifactBatch> artifactResult = [&]() {
        if (latestBatch.has_value() && !rebuildHashes.empty()) {
            return engine->updateArtifacts(*latestBatch, extracted.value(), buildConfig,
                                           &updateStats);
        }
        return engine->buildArtifacts(extracted.value(), buildConfig);
    }();
    if (!artifactResult) {
        return Result<RebuildStats>(artifactResult.error());
    }

    auto& artifacts = artifactResult.value();
    if (artifacts.topologyEpoch == 0) {
        artifacts.topologyEpoch = latestBatch.has_value() ? latestBatch->topologyEpoch + 1 : 1;
    }

    RebuildStats stats;
    stats.reason = reason;
    stats.dryRun = dryRun;
    stats.fullRebuild = documentHashes.empty();
    stats.snapshotId = artifacts.snapshotId;
    stats.algorithm = artifacts.algorithm;
    stats.documentsRequested = extractionStats.documentsRequested;
    stats.documentsProcessed = extractionStats.documentsReturned;
    stats.documentsMissingEmbeddings = extractionStats.documentsMissingEmbeddings;
    stats.documentsMissingGraphNodes = extractionStats.documentsMissingGraphNodes;
    stats.neighborEdgesScanned = extractionStats.neighborEdgesScanned;
    stats.neighborsReturned = extractionStats.neighborsReturned;
    stats.clustersBuilt = artifacts.clusters.size();
    stats.membershipsBuilt = artifacts.memberships.size();
    stats.dirtySeedCount = updateStats.dirtySeedCount > 0 ? updateStats.dirtySeedCount
                                                          : seedExtractionStats.seedDocuments;
    stats.dirtyRegionDocs = updateStats.dirtyRegionDocs > 0 ? updateStats.dirtyRegionDocs
                                                            : extractionStats.regionDocuments;
    stats.coalescedDirtySets = updateStats.coalescedDirtySets;
    stats.fallbackFullRebuilds = updateStats.fallbackFullRebuilds;

    if (!artifacts.clusters.empty()) {
        std::vector<std::size_t> sizes;
        sizes.reserve(artifacts.clusters.size());
        double cohesionSum = 0.0;
        std::size_t cohesionCount = 0;
        std::size_t totalMembers = 0;
        std::size_t singletons = 0;
        std::size_t maxSize = 0;
        for (const auto& cluster : artifacts.clusters) {
            const std::size_t sz =
                cluster.memberCount > 0 ? cluster.memberCount : cluster.memberDocumentHashes.size();
            sizes.push_back(sz);
            totalMembers += sz;
            if (sz == 1)
                ++singletons;
            if (sz > maxSize)
                maxSize = sz;
            if (cluster.cohesionScore > 0.0) {
                cohesionSum += cluster.cohesionScore;
                ++cohesionCount;
            }
        }
        std::sort(sizes.begin(), sizes.end());
        const auto pct = [&](double p) -> std::size_t {
            if (sizes.empty())
                return 0;
            const auto idx = std::min<std::size_t>(
                sizes.size() - 1, static_cast<std::size_t>(p * (sizes.size() - 1)));
            return sizes[idx];
        };
        stats.clusterSizeMax = static_cast<std::uint64_t>(maxSize);
        stats.clusterSizeP50 = static_cast<std::uint64_t>(pct(0.5));
        stats.clusterSizeP90 = static_cast<std::uint64_t>(pct(0.9));
        stats.singletonCount = static_cast<std::uint64_t>(singletons);
        stats.singletonRatio =
            sizes.empty() ? 0.0
                          : static_cast<double>(singletons) / static_cast<double>(sizes.size());
        stats.giantClusterRatio =
            totalMembers > 0 ? static_cast<double>(maxSize) / static_cast<double>(totalMembers)
                             : 0.0;

        if (!sizes.empty()) {
            double cumulative = 0.0;
            double weightedSum = 0.0;
            for (std::size_t i = 0; i < sizes.size(); ++i) {
                cumulative += static_cast<double>(sizes[i]);
                weightedSum += static_cast<double>(i + 1) * static_cast<double>(sizes[i]);
            }
            if (cumulative > 0.0) {
                const double n = static_cast<double>(sizes.size());
                stats.clusterSizeGini =
                    std::clamp((2.0 * weightedSum) / (n * cumulative) - (n + 1.0) / n, 0.0, 1.0);
            }
        }
        stats.avgIntraEdgeWeight =
            cohesionCount > 0 ? cohesionSum / static_cast<double>(cohesionCount) : 0.0;

        // Phase H-TDA wiring fix: compute H_0 persistence on cluster
        // centroids. Each cluster's centroid is the mean of its members'
        // embeddings; the resulting centroid cloud varies per arm because
        // the clustering does. Skip clusters with < 2 members (singletons
        // would give a centroid == the doc itself, biasing toward dispersed
        // shapes regardless of arm).
        try {
            std::vector<std::vector<float>> centroids;
            centroids.reserve(artifacts.clusters.size());
            for (const auto& cluster : artifacts.clusters) {
                if (cluster.memberDocumentHashes.size() < 2) {
                    continue;
                }
                std::vector<float> centroid;
                std::size_t accumCount = 0;
                for (const auto& hash : cluster.memberDocumentHashes) {
                    auto recs = vectorDb->getVectorsByDocument(hash);
                    if (recs.empty()) {
                        continue;
                    }
                    const auto& emb = recs.front().embedding;
                    if (emb.empty()) {
                        continue;
                    }
                    if (centroid.empty()) {
                        centroid.assign(emb.size(), 0.0F);
                    } else if (centroid.size() != emb.size()) {
                        continue;
                    }
                    for (std::size_t i = 0; i < emb.size(); ++i) {
                        centroid[i] += emb[i];
                    }
                    ++accumCount;
                }
                if (accumCount > 0 && !centroid.empty()) {
                    for (auto& v : centroid) {
                        v /= static_cast<float>(accumCount);
                    }
                    centroids.push_back(std::move(centroid));
                }
            }
            stats.clusterCentroidCount = centroids.size();
            if (centroids.size() >= 2) {
                stats.clusterCentroidPersistence = yams::search::computePersistenceH0(centroids);
            }
        } catch (const std::exception& e) {
            spdlog::debug("[TopologyManager] centroid persistence failed: {}", e.what());
        }
    }

    const auto processed = static_cast<std::uint64_t>(extractionStats.documentsReturned);
    const auto membershipCount = static_cast<std::uint64_t>(artifacts.memberships.size());
    stats.orphanDocCount = processed > membershipCount ? processed - membershipCount : 0;

    stats.issues.push_back(std::string{"topology_algorithm="} + std::string{algorithmKey});

    if (!documentHashes.empty()) {
        stats.issues.push_back("incremental topology rebuild expanded " +
                               std::to_string(documentHashes.size()) + " seed docs to " +
                               std::to_string(rebuildHashes.size()) + " docs");
        stats.issues.push_back(
            std::string{"dirty-region expansion="} +
            (dirtyRegion.includedPriorClusterMembers ? "prior_cluster+" : "") +
            (dirtyRegion.includedSemanticNeighbors ? "neighbors" : "seeds_only"));
        if (dirtyRegion.requiresWiderRebuild) {
            stats.issues.push_back("dirty-region requested wider rebuild fallback");
        }
    }

    if (artifacts.memberships.empty()) {
        stats.skipped = true;
        stats.issues.push_back(
            "topology rebuild produced no memberships; graph nodes or embeddings may be missing");
        return stats;
    }

    if (!dryRun) {
        auto storeResult = store.storeBatch(artifacts);
        if (!storeResult) {
            return Result<RebuildStats>(storeResult.error());
        }
        stats.stored = true;
        publishedEpoch_.store(artifacts.topologyEpoch, std::memory_order_release);
    }

    if (stats.stored && stats.fullRebuild) {
        std::lock_guard<std::mutex> lock(dirtyMutex_);
        dirtyHashes_.clear();
        std::lock_guard<std::mutex> telemetryLock(telemetryMutex_);
        telemetry_.dirtySinceUnixMillis = 0;
    }

    return stats;
}

std::uint64_t TopologyManager::nowUnixSeconds() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count());
}

std::uint64_t TopologyManager::nowUnixMillis() {
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                          std::chrono::system_clock::now().time_since_epoch())
                                          .count());
}

} // namespace yams::daemon
