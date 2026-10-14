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
#include <span>
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

/// One undirected road step: the neighbouring cluster (index into `artifacts->clusters`) and the
/// road weight shared by both endpoints.
struct TopologyRoadNeighbor {
    std::size_t clusterIndex{0};
    double weight{0.0};

    friend bool operator==(const TopologyRoadNeighbor&, const TopologyRoadNeighbor&) = default;
};

struct TopologyRoutingSnapshot {
    std::shared_ptr<const yams::topology::TopologyArtifactBatch> artifacts;
    std::string constructionFingerprint;
    std::string representationFingerprint;
    /// Identity of the loaded road set (`topologyRoadFingerprint`). Provenance and cache
    /// identity for road consumers only; not part of the construction or policy fingerprints.
    std::string roadFingerprint;
    /// Road adjacency by cluster index (`buildTopologyRoadAdjacency`): the union of both stored
    /// orientations, each source sorted by weight desc then target cluster id asc. Empty when the
    /// batch carries no roads (never built, or dropped with `roadUnavailableReason`).
    std::vector<std::vector<TopologyRoadNeighbor>> roadAdjacency;
    /// Walk order per cluster index (`buildTopologyOrderedMemberHashes`): the cluster's
    /// `orderedMembers` resolved to member hashes, in walk order;
    /// `artifacts->clusters[i].orderKind` names the kind. Empty for a cluster without an order or
    /// with an invalid permutation. Read by the road walk only; never part of any fingerprint.
    std::vector<std::vector<std::string_view>> orderedMemberHashes;
    std::unordered_map<std::string_view, std::size_t> clustersById;
    std::unordered_map<std::string_view, std::size_t> membershipsByDocumentHash;
    yams::topology::ProtectedRelationCoverIndex protectedRelationCover;
    yams::topology::SparseRouteIndex sparseRouteIndex;
    bool denseAnnBuildAttempted = false;
    /// Coordinate prefix the centroid BQ index was built over (0 = full dimension).
    std::size_t bqPrefixDimension = 0;
    /// Rotation and seed the centroid BQ index was built with; queries reuse the index's own
    /// rotation, so both sides always share this seed. Part of the cache key.
    yams::vector::BinaryRotation bqRotation = yams::vector::BinaryRotation::None;
    std::uint64_t bqRotationSeed = 0;
};

struct TopologyRoutingSnapshotLookup {
    std::shared_ptr<const TopologyRoutingSnapshot> snapshot;
    bool cacheHit = false;
};

/// Walk orders of the clusters a routed query draws from, built by the session when
/// `TopologyRoutingOptions::roadWalkWindow` > 0: the selected routes first, then the road-expanded
/// clusters, keeping only the clusters that carry an order. The vector stage expands every routed
/// hit along these orders (`walkTopologyRoadOrders`). Immutable once built; the member views
/// borrow from `snapshot`, which the index keeps alive.
struct TopologyRoadWalkIndex {
    std::shared_ptr<const TopologyRoutingSnapshot> snapshot;
    /// Indices into `snapshot->artifacts->clusters` of the walkable clusters, routes first.
    std::vector<std::size_t> clusterIndices;
    /// Member hash -> every (slot into `clusterIndices`, position along that cluster's order).
    std::unordered_map<std::string_view, std::vector<std::pair<std::size_t, std::size_t>>>
        positions;

    /// The walk order of `clusterIndices[slot]`.
    [[nodiscard]] const std::vector<std::string_view>& order(std::size_t slot) const {
        return snapshot->orderedMemberHashes[clusterIndices[slot]];
    }
};

/// Expand every anchor by `window` positions on each side along each order it belongs to, bounded
/// to that order. Returns the walked hashes that are not anchors and not in `exclude`,
/// deduplicated. `anchorsExpanded`, when given, receives the number of anchors found in at least
/// one order. A window of 0 walks nothing.
[[nodiscard]] std::unordered_set<std::string>
walkTopologyRoadOrders(const TopologyRoadWalkIndex& index, std::span<const std::string> anchors,
                       std::size_t window, const std::unordered_set<std::string>& exclude,
                       std::size_t* anchorsExpanded = nullptr);

using TopologyRoutingSnapshotLoader =
    std::function<Result<std::optional<yams::topology::TopologyArtifactBatch>>()>;
/// Loader that hands out the store's resident snapshot instead of a copy (null when none).
using TopologyRoutingSharedSnapshotLoader =
    std::function<Result<std::shared_ptr<const yams::topology::TopologyArtifactBatch>>()>;

/// Thread-safe, epoch-aware cache for an immutable, prevalidated routing snapshot.
class TopologyRoutingSnapshotCache {
public:
    explicit TopologyRoutingSnapshotCache(TopologyRoutingSnapshotLoader loader);
    explicit TopologyRoutingSnapshotCache(TopologyRoutingSharedSnapshotLoader loader);

    [[nodiscard]] Result<TopologyRoutingSnapshotLookup>
    get(std::uint64_t expectedEpoch = 0, bool requireDenseAnnIndex = true,
        std::size_t bqPrefixDimension = 0,
        yams::vector::BinaryRotation bqRotation = yams::vector::BinaryRotation::None);

private:
    TopologyRoutingSharedSnapshotLoader loader_;
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
    std::size_t maxSeedDocuments = 32;
    std::size_t representativeLimit = 0;
    std::size_t denseAnnCandidateLimit = 0;
    /// Centroid shortlist size for 1-bit BQ pre-filtering; zero disables explicit BQ routing.
    std::size_t bqCandidateLimit = 0;
    /// Leading coordinate prefix for the BQ index; only meaningful for Matryoshka-trained
    /// embeddings. Zero uses the full dimension.
    std::size_t bqPrefixDimension = 0;
    SearchEngineConfig::TopologyBqRotation bqRotation =
        SearchEngineConfig::TopologyBqRotation::None;
    float adaptiveProbeScoreGap = 0.0F;
    float narrowMinBoundaryMargin = 0.0F;
    /// Maximum documents in a materialized route allowed set or expansion result.
    /// Zero explicitly means unbounded full membership.
    std::size_t maxDocs = 0;
    /// Road beam: hops along cluster roads from the selected routes and the clusters kept per
    /// hop (`search.topology.road_hops`, `road_beam_width`). Zero hops is off. The expansion is
    /// reported beside the certificate (`TopologyRoutingSessionResult::roadExpanded*`), so both
    /// knobs are excluded from `topologyRoutingPolicyFingerprint` and never touch admission.
    std::size_t roadHops = 0;
    std::size_t roadBeamWidth = 4;
    /// Road walk (`search.topology.road_walk_window`): positions taken on each side of a routed
    /// hit along its cluster's walk order. Zero is off. The session only indexes the walkable
    /// orders (`TopologyRoutingSessionResult::roadWalkIndex`); the vector stage walks around the
    /// hits it observed. Excluded from `topologyRoutingPolicyFingerprint` like the beam knobs.
    std::size_t roadWalkWindow = 0;
    float sparseDenseAlpha = 0.5F;
    float minRouteScore = 0.0F;
    SearchEngineConfig::TopologyRouteRiskCalibration routeRiskCalibration;
    SearchEngineConfig::TopologyRouteWorkBudget routeWorkBudget;
    /// Materialize full membership for confidently selected clusters so callers can gate an
    /// existing candidate stream without query-scoring every member.
    bool collectRouteMembership = false;
    /// Augment policy: report materialized route members absent from the request's existing
    /// candidates as added candidates (present ones as duplicates). Accounting only; the allowed
    /// set and the routing policy fingerprint are unchanged. Narrow and Shadow leave this off.
    bool countRouteMemberAdditions = false;
    float graphNeighborMinScore = 0.25F;
    bool graphNeighborReciprocalOnly = true;
    bool graphWeightedSeedRanking = false;
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
    std::optional<float> semanticCost{};
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
    std::string routingPolicyFingerprint;
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
    /// Representative rule recorded in the loaded snapshot (Degree when nothing loaded).
    yams::topology::RepresentativeRule representativeRule =
        yams::topology::RepresentativeRule::Degree;
    std::string skipReason;
    /// Why the loaded snapshot carries no roads although a sidecar existed (store diagnostic,
    /// `TopologyArtifactBatch::roadUnavailableReason`). Empty when roads loaded or were never
    /// written. Informational: a set reason never changes routing.
    std::string roadUnavailableReason;
    /// Road beam (`TopologyRoutingOptions::roadHops`). The clusters reached along roads and their
    /// members are kept apart from the certificate: they never widen `allowedDocumentHashes`,
    /// admission or the policy fingerprint. RouteOnly and Augment query the groups as extra routes.
    /// `roadHopsRequested` echoes the option so consumers can tell "off" from "nothing reached";
    /// `roadSkipReason` says why hops > 0 expanded nothing (roads_absent, no_selected_routes).
    std::size_t roadHopsRequested = 0;
    std::size_t roadHopsApplied = 0;
    std::vector<std::string> roadExpandedClusterIds;
    /// One member group per expanded cluster, in beam order, excluding certificate members and
    /// members already listed by an earlier road group.
    std::vector<std::unordered_set<std::string>> roadExpandedDocumentHashGroups;
    /// Centroid evaluations the beam performed; also counted in the representative distance
    /// evaluations below so routed work stays comparable with the no-road arm.
    std::size_t roadWorkCentroidEvaluations = 0;
    std::string roadSkipReason;
    /// Why one or more walk orders were dropped when the snapshot loaded (store diagnostic,
    /// `TopologyArtifactBatch::orderUnavailableReason`). Empty when every order loaded or none
    /// was written. Informational: a set reason never changes routing.
    std::string orderUnavailableReason;
    /// Road walk (`TopologyRoutingOptions::roadWalkWindow`). `roadWalkWindowRequested` echoes the
    /// option; `roadWalkIndex` holds the walk orders of the selected routes and road-expanded
    /// clusters (null when the window is 0 or no walkable cluster has an order);
    /// `roadWalkClustersUnordered` counts the walkable clusters without one. The stage reports the
    /// anchors and the appended documents; nothing here touches the certificate.
    std::size_t roadWalkWindowRequested = 0;
    std::size_t roadWalkClustersUnordered = 0;
    std::shared_ptr<const TopologyRoadWalkIndex> roadWalkIndex;
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
    bool routeBqUsed = false;
    std::size_t routeBqCandidates = 0;
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

/// Cached per snapshot; includes ordered representative vectors as well as construction.
[[nodiscard]] std::string
topologyRoutingRepresentationFingerprint(const yams::topology::TopologyArtifactBatch& batch);

/// Identity of `batch.roads` alone (order-independent). Separate from the construction
/// fingerprint, which stays road-free so road changes never invalidate construction-bound
/// calibrations; an empty road set has a fixed, non-empty fingerprint.
[[nodiscard]] std::string
topologyRoadFingerprint(const yams::topology::TopologyArtifactBatch& batch);

/// Undirected road adjacency by cluster index from `batch.roads`: both orientations of every road
/// are listed once each, a pair stored twice collapses to its heavier weight, and each source is
/// sorted by weight desc then target cluster id asc. Roads with an unknown endpoint, a self loop or
/// a non-finite/negative weight are skipped. Empty when the batch has no roads.
[[nodiscard]] std::vector<std::vector<TopologyRoadNeighbor>>
buildTopologyRoadAdjacency(const yams::topology::TopologyArtifactBatch& batch);

/// Walk order per cluster index: `ClusterArtifact::orderedMembers` resolved to views of the
/// cluster's `memberDocumentHashes`, in walk order. A cluster with `orderKind` None, an empty order
/// or a permutation that fails `validateMemberPermutation` resolves to an empty list. The views
/// borrow from `batch`.
[[nodiscard]] std::vector<std::vector<std::string_view>>
buildTopologyOrderedMemberHashes(const yams::topology::TopologyArtifactBatch& batch);

/// Identity of the allowed-set selection policy; excludes query values and diagnostics. The road
/// beam knobs (`roadHops`, `roadBeamWidth`) and the road walk window (`roadWalkWindow`) are
/// deliberately left out: roads and the walk expand beside the certificate, so calibrations bound
/// to this fingerprint stay valid with roads on or off.
[[nodiscard]] std::string
topologyRoutingPolicyFingerprint(std::string_view representationFingerprint,
                                 const TopologyRoutingOptions& options);

/// Rank pure graph-neighbor candidates from seed adjacency (testable without KG).
/// `seedNeighbors[seedHash]` is a list of (neighborHash, score, reciprocal).
/// Returns up to maxDocs unique neighbor hashes ordered by score (then hash).
[[nodiscard]] std::vector<std::string> rankGraphNeighborCandidates(
    const std::unordered_map<std::string, std::vector<std::tuple<std::string, float, bool>>>&
        seedNeighbors,
    const std::vector<std::string>& seedDocumentHashes, std::size_t maxDocs, float minScore,
    bool reciprocalOnly,
    std::span<const yams::topology::WeightedDocumentSeed> weightedSeedDocuments = {});

/// Candidate community support from topology co-membership: candidates that share a snapshot
/// cluster form a community, scored (size - 1) / (referenceSize - 1) (or / (candidates - 1)
/// when referenceSize <= 1), clamped to [0, 1]. Candidates outside the snapshot get 0.
struct TopologyCommunityStats {
    std::size_t supportedDocs = 0;
    std::size_t largestCommunity = 0;
    std::size_t communities = 0;
};
[[nodiscard]] std::vector<float>
topologyCommunitySupport(const TopologyRoutingSnapshot& snapshot,
                         const std::vector<std::string>& candidateHashes, float referenceSize,
                         TopologyCommunityStats* stats = nullptr);

TopologyRoutingSessionResult
runTopologyRoutingSession(const TopologyRoutingSessionRequest& request,
                          const std::shared_ptr<yams::metadata::MetadataRepository>& metadataRepo,
                          const std::shared_ptr<yams::metadata::KnowledgeGraphStore>& kgStore);

} // namespace yams::search
