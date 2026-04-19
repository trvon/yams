#include <yams/daemon/components/TopologyManager.h>

#include <spdlog/spdlog.h>
#include <algorithm>
#include <chrono>

#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/vector/vector_database.h>

#include <yams/topology/topology_baseline.h>
#include <yams/topology/topology_factory.h>
#include <yams/topology/topology_input_extractor.h>
#include <yams/topology/topology_metadata_store.h>

namespace yams::daemon {

using yams::Error;
using yams::ErrorCode;
using yams::Result;

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
    }

    auto result = runRebuild(reason, dryRun, documentHashes, topologyAlgorithm);
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
