#pragma once

#include <yams/search/search_engine_config.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace yams::metadata

namespace yams::search {

struct TopologyRoutingSnapshot {
    std::shared_ptr<const yams::topology::TopologyArtifactBatch> artifacts;
    std::unordered_map<std::string, std::size_t> clustersById;
    yams::topology::SparseRouteIndex sparseRouteIndex;
};

struct TopologyRoutingSnapshotLookup {
    std::shared_ptr<const TopologyRoutingSnapshot> snapshot;
    bool cacheHit = false;
};

using TopologyRoutingSnapshotLoader =
    std::function<Result<std::optional<yams::topology::TopologyArtifactBatch>>()>;

/// Thread-safe, epoch-aware cache for an immutable, prevalidated routing snapshot.
class TopologyRoutingSnapshotCache {
public:
    explicit TopologyRoutingSnapshotCache(TopologyRoutingSnapshotLoader loader);

    [[nodiscard]] Result<TopologyRoutingSnapshotLookup> get(std::uint64_t expectedEpoch = 0);

private:
    TopologyRoutingSnapshotLoader loader_;
    std::mutex mutex_;
    std::shared_ptr<const TopologyRoutingSnapshot> cached_;
};

struct TopologyRoutingTimings {
    std::int64_t totalMicros = 0;
    std::int64_t loadMicros = 0;
    std::int64_t validateMicros = 0;
    std::int64_t requestPrepMicros = 0;
    std::int64_t routeMicros = 0;
    std::int64_t clusterLookupMicros = 0;
    std::int64_t docLookupMicros = 0;
    std::int64_t candidateInsertMicros = 0;
};

struct TopologyRoutingSessionRequest {
    std::string query;
    std::vector<std::string> seedDocumentHashes;
    std::vector<yams::topology::WeightedDocumentSeed> weightedSeedDocuments;
    std::unordered_set<std::string> existingCandidateHashes;
    std::optional<std::vector<float>> queryEmbedding;
    SearchEngineConfig::TopologyRoutingMode routingMode =
        SearchEngineConfig::TopologyRoutingMode::Disabled;
    SearchEngineConfig::TopologyRouteScoringMode routeScoringMode =
        SearchEngineConfig::TopologyRouteScoringMode::Current;
    SearchEngineConfig::TopologyExpansionSource expansionSource =
        SearchEngineConfig::TopologyExpansionSource::Clusters;
    bool weakTier1Query = false;
    std::size_t minClusters = 1;
    std::size_t maxClusters = 0;
    float adaptiveProbeScoreGap = 0.0F;
    float narrowMinBoundaryMargin = 0.0F;
    std::size_t maxDocs = 0;
    std::size_t perClusterLimit = 0;
    float sparseDenseAlpha = 0.5F;
    float minRouteScore = 0.0F;
    bool medoidOnlyExpansion = false;
    /// Materialize full membership for confidently selected clusters so callers can gate an
    /// existing candidate stream without query-scoring every member.
    bool collectRouteMembership = false;
    float graphNeighborMinScore = 0.25F;
    bool graphNeighborReciprocalOnly = true;
    std::uint64_t expectedTopologyEpoch = 0;
    std::shared_ptr<TopologyRoutingSnapshotCache> snapshotCache;
};

struct TopologyRoutingSessionResult {
    bool enabled = false;
    bool loadAttempted = false;
    bool loadSucceeded = false;
    bool artifactAdmitted = false;
    bool applied = false;
    bool narrowApplied = false;
    bool artifactsFresh = false;
    bool snapshotCacheHit = false;
    std::uint64_t topologyEpoch = 0;
    std::string skipReason;
    std::size_t routedClusters = 0;
    std::size_t availableRoutes = 0;
    std::size_t routedDocs = 0;
    std::size_t routesRejected = 0;
    float bestRouteScore = 0.0F;
    float meanAcceptedRouteScore = 0.0F;
    float routeBoundaryScoreMargin = 0.0F;
    bool confidenceAbstained = false;
    std::size_t acceptedRoutes = 0;
    std::size_t seedCount = 0;
    std::size_t seedsInRoutedClusters = 0;
    std::size_t addedCandidates = 0;
    std::size_t duplicateCandidates = 0;
    std::size_t staleCandidates = 0;
    std::size_t memberRerankCandidates = 0;
    std::size_t memberRerankSelected = 0;
    std::vector<std::string> addedCandidateHashes;
    std::unordered_set<std::string> routedCandidateHashes;
    std::unordered_set<std::string> routeAllowedDocumentHashes;
    std::unordered_set<std::string> medoidHashes;
    TopologyRoutingTimings timings;
};

struct TopologyRouteSelection {
    std::vector<yams::topology::ClusterRoute> routes;
    std::size_t availableRoutes{0};
    float boundaryScoreMargin{0.0F};
    bool abstained{false};
};

/// Choose an adaptive prefix from score-sorted routes. A positive score gap
/// grows the prefix while routes remain close to the best score. A positive
/// boundary margin turns an ambiguous selected/excluded boundary into abstention.
[[nodiscard]] TopologyRouteSelection
selectTopologyRoutesForNarrowing(const std::vector<yams::topology::ClusterRoute>& routes,
                                 std::size_t minClusters, std::size_t maxClusters,
                                 float adaptiveScoreGap, float minBoundaryMargin);

SearchEngineConfig::TopologyRoutingMode
resolveTopologyRoutingMode(const SearchEngineConfig& config) noexcept;

bool topologyRoutingMayLoad(SearchEngineConfig::TopologyRoutingMode mode,
                            bool weakTier1Query) noexcept;

bool topologyRoutingMayExpand(SearchEngineConfig::TopologyRoutingMode mode,
                              bool weakTier1Query) noexcept;

yams::topology::RouteScoringMode
topologyRouteScoringMode(SearchEngineConfig::TopologyRouteScoringMode mode) noexcept;

std::optional<std::string>
validateTopologyArtifactBatchForRouting(const yams::topology::TopologyArtifactBatch& batch);

using TopologyMemberReranker = std::function<std::vector<std::string>(
    const std::vector<std::string>& members, std::size_t limit)>;

/// Rank pure graph-neighbor candidates from seed adjacency (testable without KG).
/// `seedNeighbors[seedHash]` is a list of (neighborHash, score, reciprocal).
/// Returns up to maxDocs unique neighbor hashes ordered by score (then hash).
[[nodiscard]] std::vector<std::string> rankGraphNeighborCandidates(
    const std::unordered_map<std::string, std::vector<std::tuple<std::string, float, bool>>>&
        seedNeighbors,
    const std::vector<std::string>& seedDocumentHashes, std::size_t maxDocs, float minScore,
    bool reciprocalOnly);

TopologyRoutingSessionResult
runTopologyRoutingSession(const TopologyRoutingSessionRequest& request,
                          const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                          const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore,
                          const TopologyMemberReranker& memberReranker = {});

} // namespace yams::search
