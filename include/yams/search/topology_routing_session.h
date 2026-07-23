#pragma once

#include <yams/search/search_engine_config.h>
#include <yams/topology/protected_relation_cover.h>
#include <yams/topology/topology_artifacts.h>
#include <yams/topology/topology_baseline.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::metadata {
class KnowledgeGraphStore;
class MetadataRepository;
} // namespace yams::metadata

namespace yams::search {

/// Bounded structural evidence for one candidate within the query-selected route cover.
/// Values are normalized to [0,1] while the immutable snapshot is traversed; consumers do not
/// need to re-read topology artifacts or inspect cluster membership at ranking time.
struct TopologyCandidateStructureEvidence {
    float scaleAgreement{0.0F};
    float overlapSupport{0.0F};
    float persistenceSupport{0.0F};
    float cohesionSupport{0.0F};
    float bridgeSupport{0.0F};
    float densitySupport{0.0F};
};

struct TopologyRoutingSnapshot {
    std::shared_ptr<const yams::topology::TopologyArtifactBatch> artifacts;
    std::string constructionFingerprint;
    std::unordered_map<std::string, std::size_t> clustersById;
    std::unordered_map<std::string, std::size_t> membershipsByDocumentHash;
    yams::topology::ProtectedRelationCoverIndex protectedRelationCover;
    yams::topology::SparseRouteIndex sparseRouteIndex;
    bool denseAnnBuildAttempted = false;
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

    [[nodiscard]] Result<TopologyRoutingSnapshotLookup> get(std::uint64_t expectedEpoch = 0,
                                                            bool requireDenseAnnIndex = true);

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

/// Typed routing policy shared by the product assist stage and the routing session.
/// Execution inputs (query, seeds, stores, snapshot epoch/cache) remain on their owning request.
struct TopologyRoutingOptions {
    SearchEngineConfig::TopologyRoutingMode routingMode =
        SearchEngineConfig::TopologyRoutingMode::Disabled;
    SearchEngineConfig::TopologyRouteScoringMode routeScoringMode =
        SearchEngineConfig::TopologyRouteScoringMode::Current;
    SearchEngineConfig::TopologyExpansionSource expansionSource =
        SearchEngineConfig::TopologyExpansionSource::Clusters;
    bool weakTier1Query = false;
    std::size_t minClusters = 1;
    std::size_t maxClusters = 0;
    std::size_t representativeLimit = 0;
    std::size_t denseAnnCandidateLimit = 0;
    float adaptiveProbeScoreGap = 0.0F;
    float narrowMinBoundaryMargin = 0.0F;
    /// Maximum documents in a materialized route allowed set or expansion result.
    /// Zero explicitly means unbounded full membership.
    std::size_t maxDocs = 0;
    float sparseDenseAlpha = 0.5F;
    float minRouteScore = 0.0F;
    SearchEngineConfig::TopologyRouteRiskCalibration routeRiskCalibration;
    SearchEngineConfig::TopologyRouteWorkBudget routeWorkBudget;
    /// Materialize full membership for confidently selected clusters so callers can gate an
    /// existing candidate stream without query-scoring every member.
    bool collectRouteMembership = false;
    float graphNeighborMinScore = 0.25F;
    bool graphNeighborReciprocalOnly = true;
    /// Collect a trace-only ledger of relation, fetch, filter, and selection boundaries.
    /// This may perform additional graph and metadata reads but must not alter routing output.
    bool collectGraphDiagnostics = false;
};

/// Snapshot the topology fields from SearchEngineConfig at the stage/session boundary.
[[nodiscard]] TopologyRoutingOptions
makeTopologyRoutingOptions(const SearchEngineConfig& config,
                           SearchEngineConfig::TopologyRoutingMode routingMode, bool weakTier1Query,
                           bool collectRouteMembership = false);

struct TopologyRoutingSessionRequest {
    std::string query;
    std::vector<std::string> seedDocumentHashes;
    std::vector<yams::topology::WeightedDocumentSeed> weightedSeedDocuments;
    std::unordered_set<std::string> existingCandidateHashes;
    std::optional<std::vector<float>> queryEmbedding;
    std::string queryEmbeddingSpaceIdentity;
    TopologyRoutingOptions options;
    std::uint64_t expectedTopologyEpoch = 0;
    std::shared_ptr<TopologyRoutingSnapshotCache> snapshotCache;
};

/// Query-to-cover evidence retained before the router scalarizes it into routeScore.
struct TopologyRouteEvidence {
    std::string clusterId;
    std::optional<float> semanticCost;
    std::optional<float> sparseCost;
    float persistencePenalty{1.0F};
    float cohesionPenalty{1.0F};
    float sizePenalty{0.0F};
    float routeScore{0.0F};
    bool scoreEligible{false};
    bool inSelectedPrefix{false};
};

/// State of one independently produced proof obligation for hard topology narrowing.
/// Unavailable is distinct from Satisfied so default-valued measurements cannot admit a route.
enum class TopologyProofObligationStatus {
    Unavailable,
    Satisfied,
    Violated,
};

[[nodiscard]] constexpr std::string_view
topologyProofObligationStatusToString(TopologyProofObligationStatus status) noexcept {
    switch (status) {
        case TopologyProofObligationStatus::Unavailable:
            return "unavailable";
        case TopologyProofObligationStatus::Satisfied:
            return "satisfied";
        case TopologyProofObligationStatus::Violated:
            return "violated";
    }
    return "unavailable";
}

/// Runtime counterpart of the independent protected-relation obligations in
/// RetrievalCoordinates.lean.
/// Producers satisfy these fields separately; route scoring alone never changes admission.
struct TopologyRouteAdmission {
    TopologyProofObligationStatus coordinateSpaceAlignment{
        TopologyProofObligationStatus::Unavailable};
    TopologyProofObligationStatus protectedRelationCoverage{
        TopologyProofObligationStatus::Unavailable};
    TopologyProofObligationStatus protectedFibersRepresented{
        TopologyProofObligationStatus::Unavailable};
    TopologyProofObligationStatus certificateSaturatesProtectedFibers{
        TopologyProofObligationStatus::Unavailable};
    TopologyProofObligationStatus routeRisk{TopologyProofObligationStatus::Unavailable};
    TopologyProofObligationStatus work{TopologyProofObligationStatus::Unavailable};
    TopologyProofObligationStatus coverMaterialization{TopologyProofObligationStatus::Unavailable};
    std::size_t selectedCoverDocuments{0};
    std::size_t materializedCoverDocuments{0};

    [[nodiscard]] std::string_view denialReason() const noexcept;
    [[nodiscard]] bool eligibleForTrialNarrowing() const noexcept;
    [[nodiscard]] bool eligibleForNarrowing() const noexcept { return denialReason().empty(); }
};

/// Counterfactual or committed routed-vector work. The observation bit is explicit because
/// zero work is meaningful only when every backend counter was available.
struct TopologyRouteWorkObservation {
    bool observed{false};
    std::size_t rowsVisited{0};
    std::size_t exactDistanceEvaluations{0};
    std::size_t annCandidateBudget{0};
};

enum class TopologyRouteAction {
    Global,
    Augment,
    Narrow,
};

[[nodiscard]] constexpr std::string_view
topologyRouteActionToString(TopologyRouteAction action) noexcept {
    switch (action) {
        case TopologyRouteAction::Global:
            return "global";
        case TopologyRouteAction::Augment:
            return "augment";
        case TopologyRouteAction::Narrow:
            return "narrow";
    }
    return "global";
}

/// Construction-bound runtime counterpart of `SelectiveRouteCertificate`. Topology owns cover
/// selection and materialization; SearchEngine consumes this object without reconstructing theorem
/// premises from raw routing state.
struct TopologyRouteCertificate {
    std::string constructionFingerprint;
    std::string coordinateSpaceIdentity;
    std::vector<std::string> selectedCoverIds;
    std::vector<std::string> selectedProtectedRelationFiberIds;
    std::unordered_set<std::string> allowedDocumentHashes;
    /// Full selected membership by accepted route, in route-score order. The union above remains
    /// the backend filter; these groups let callers allocate balanced result quotas.
    std::vector<std::unordered_set<std::string>> allowedDocumentHashGroups;
    TopologyRouteAdmission admission;
    TopologyRouteWorkObservation work;

    [[nodiscard]] bool hasUsefulRoute() const noexcept { return !allowedDocumentHashes.empty(); }
    [[nodiscard]] bool isConstructionBound() const noexcept {
        return !constructionFingerprint.empty() && !selectedCoverIds.empty() &&
               !selectedProtectedRelationFiberIds.empty();
    }
    [[nodiscard]] bool eligibleForTrialNarrowing() const noexcept {
        return hasUsefulRoute() && isConstructionBound() && admission.eligibleForTrialNarrowing();
    }
    [[nodiscard]] TopologyRouteAction action() const noexcept {
        if (hasUsefulRoute() && isConstructionBound() && admission.eligibleForNarrowing()) {
            return TopologyRouteAction::Narrow;
        }
        return hasUsefulRoute() ? TopologyRouteAction::Augment : TopologyRouteAction::Global;
    }
};

/// Trace-only attribution for the graph-neighbor producer. Candidate document IDs preserve
/// seed-hit/score order except for seed IDs, which are sorted for stable comparison.
struct GraphNeighborStageTrace {
    bool collected{false};
    std::size_t edgeFetchLimit{0};
    std::size_t fetchTruncatedSeedCount{0};
    std::vector<std::string> seedDocumentIds;
    std::size_t seedUnresolvedCount{0};
    std::size_t relationCandidateCount{0};
    std::vector<std::string> relationCandidateDocumentIds;
    std::size_t relationUnresolvedCount{0};
    std::size_t fetchedCandidateCount{0};
    std::vector<std::string> fetchedCandidateDocumentIds;
    std::size_t fetchedUnresolvedCount{0};
    std::size_t eligibleCandidateCount{0};
    std::vector<std::string> eligibleCandidateDocumentIds;
    std::size_t eligibleUnresolvedCount{0};
};

struct TopologyRoutingSessionResult {
    bool enabled = false;
    bool loadAttempted = false;
    bool loadSucceeded = false;
    bool artifactAdmitted = false;
    bool applied = false;
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
    std::size_t routeRepresentativeDistanceEvaluations = 0;
    std::size_t routeRepresentativeCountMax = 0;
    bool routeAnnUsed = false;
    std::size_t routeAnnCandidates = 0;
    std::size_t routeAnnDistanceEvaluations = 0;
    std::size_t routeExactRepresentativeDistanceEvaluations = 0;
    TopologyRouteCertificate certificate;
    std::vector<TopologyRouteEvidence> routeEvidence;
    std::vector<std::string> addedCandidateHashes;
    /// Query-trace identifiers for materialized relation candidates. These are
    /// populated at the same document-lookup boundary as routedCandidateHashes
    /// so benchmark qrels can distinguish reachability from later vector scoring.
    std::vector<std::string> routedCandidateDocIds;
    std::unordered_set<std::string> routedCandidateHashes;
    std::unordered_set<std::string> medoidHashes;
    std::unordered_map<std::string, TopologyCandidateStructureEvidence> candidateStructureEvidence;
    GraphNeighborStageTrace graphNeighborTrace;
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
resolveTopologyRoutingMode(const SearchEngineConfig& config,
                           std::optional<bool> legacyWeakQueryRouting = std::nullopt) noexcept;

bool topologyRoutingMayLoad(SearchEngineConfig::TopologyRoutingMode mode,
                            bool weakTier1Query) noexcept;

bool topologyRoutingMayExpand(SearchEngineConfig::TopologyRoutingMode mode,
                              bool weakTier1Query) noexcept;

yams::topology::RouteScoringMode
topologyRouteScoringMode(SearchEngineConfig::TopologyRouteScoringMode mode) noexcept;

std::optional<std::string>
validateTopologyArtifactBatchForRouting(const yams::topology::TopologyArtifactBatch& batch);

/// Stable identity of topology construction inputs/outputs, excluding publication time, epoch,
/// and the experimental routing representative set.
[[nodiscard]] std::string
topologyRoutingConstructionFingerprint(const yams::topology::TopologyArtifactBatch& batch);

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
                          const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore);

} // namespace yams::search
