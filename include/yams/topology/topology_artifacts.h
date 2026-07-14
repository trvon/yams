#pragma once

#include <yams/core/types.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace yams::topology {

enum class TopologyBuildMode : uint8_t {
    Exact,
    Incremental,
    Approximate,
};

enum class TopologyInputKind : uint8_t {
    SemanticNeighborGraph,
    EmbeddingNeighborhood,
    Hybrid,
};

enum class DirtyRegionExpansionMode : uint8_t {
    NeighborsOnly,
    PriorClusterAndNeighbors,
    Adaptive,
};

enum class DocumentTopologyRole : uint8_t {
    Core,
    Bridge,
    Medoid,
    Outlier,
};

struct TopologyNeighbor {
    std::string documentHash;
    float score{0.0F};
    bool reciprocal{false};
};

struct TopologyDocumentInput {
    std::string documentHash;
    std::string filePath;
    std::vector<float> embedding;
    std::vector<TopologyNeighbor> neighbors;
    std::unordered_map<std::string, std::string> metadata;
};

struct TopologyBuildConfig {
    TopologyBuildMode mode{TopologyBuildMode::Incremental};
    TopologyInputKind inputKind{TopologyInputKind::Hybrid};
    std::size_t maxDocuments{0};
    std::size_t maxNeighborsPerDocument{32};
    std::size_t maxDirtyRegionDocs{256};
    std::size_t maxDirtyRegionDepth{2};
    std::size_t maxDirtySeedCount{64};
    std::size_t maxLevels{3};
    /// Maximum secondary cluster assignments per boundary document.
    std::size_t overlapLimit{1};
    /// Admit a secondary assignment only when its residual distance is within this multiple of
    /// the primary residual distance.
    double overlapBoundaryDistanceRatio{1.05};
    /// SOAR lambda: penalty for the candidate residual component parallel to the primary residual.
    /// Zero reduces to nearest-centroid spilling.
    double overlapResidualPenalty{1.0};
    std::size_t rollingWindowDocuments{0};
    std::size_t fullRebuildDocThreshold{4096};
    Duration coalesceWindow{Duration{250}};
    // Floor for undirected edge admission when building pairWeights / CC / SGC.
    // 0.0 keeps every reciprocal edge (legacy; often creates giant components).
    double minEdgeScore{0.0};
    double minClusterPersistence{0.0};
    // Lean ConstructionGates anti-giant cap. 0 = unlimited (legacy). Production
    // TopologyManager defaults this to 64 so oversized CC components are split.
    std::size_t maxComponentDocs{0};
    bool reciprocalOnly{true};
    bool allowOverlap{false};
    bool emitBridgeAnnotations{true};
    bool emitOutliers{true};
    DirtyRegionExpansionMode dirtyRegionExpansion{
        DirtyRegionExpansionMode::PriorClusterAndNeighbors};
    Duration budget{Duration{250}};
    // Phase S: KMeans engine knobs.
    // kmeansK: target cluster count for KMeans engines (0 = auto: max(64, min(300, sqrt(n)))).
    // kmeansMaxIterations: Lloyd iteration cap during buildArtifacts.
    // minSimilarityToJoin: cosine threshold below which an incremental update spawns a
    //   new cluster instead of assigning to nearest existing centroid (default 0.45).
    std::size_t kmeansK{0};
    std::size_t kmeansMaxIterations{10};
    float minSimilarityToJoin{0.45F};
    // Total dense route representatives per cluster, including the centroid. Values above one
    // add deterministic diverse member embeddings. The default preserves centroid-only routing.
    std::size_t routingRepresentativeCount{1};
};

struct TopologyDirtyRegion {
    std::vector<std::string> seedDocumentHashes;
    std::vector<std::string> expandedDocumentHashes;
    std::size_t bfsDepthReached{0};
    bool includedPriorClusterMembers{false};
    bool includedSemanticNeighbors{false};
    bool coalesced{false};
    bool exceededRegionBudget{false};
    bool requiresWiderRebuild{false};
};

struct DocumentClusterMembership {
    std::string documentHash;
    std::string clusterId;
    std::optional<std::string> parentClusterId;
    std::size_t clusterLevel{0};
    double persistenceScore{0.0};
    double cohesionScore{0.0};
    double bridgeScore{0.0};
    DocumentTopologyRole role{DocumentTopologyRole::Core};
    std::vector<std::string> overlapClusterIds;
};

struct ClusterRepresentative {
    std::string clusterId;
    std::string documentHash;
    std::string filePath;
    double representativeScore{0.0};
};

struct ClusterRoutingRepresentative {
    std::string documentHash;
    std::vector<float> embedding;
};

struct ClusterArtifact {
    std::string clusterId;
    std::optional<std::string> parentClusterId;
    std::size_t level{0};
    std::size_t memberCount{0};
    double persistenceScore{0.0};
    double cohesionScore{0.0};
    /// Undirected density of the primary construction core in [0,1]. Boundary-spill replicas are
    /// retrieval postings and do not alter this structural statistic.
    double densityScore{0.0};
    double bridgeMass{0.0};
    std::optional<ClusterRepresentative> medoid;
    std::vector<std::string> memberDocumentHashes;
    std::vector<std::string> overlapClusterIds;
    // Phase S: optional running-mean centroid for online KMeans engine.
    // Empty for engines that don't compute it (Connected/Louvain).
    std::vector<float> centroidEmbedding;
    // Additional bounded representatives; centroidEmbedding is always the implicit first one.
    std::vector<ClusterRoutingRepresentative> routingRepresentatives;
};

struct TopologyArtifactBatch {
    std::string snapshotId;
    std::string algorithm;
    TopologyInputKind inputKind{TopologyInputKind::Hybrid};
    uint64_t generatedAtUnixSeconds{0};
    // Monotonically increasing epoch stamped by the builder on every published batch.
    // Distinct from snapshotId (which is a timestamp) so query-side code can detect
    // topology drift against the artifacts that seeded the route.
    uint64_t topologyEpoch{0};
    std::vector<ClusterArtifact> clusters;
    std::vector<DocumentClusterMembership> memberships;
};

struct TopologyUpdateStats {
    std::size_t documentsProcessed{0};
    std::size_t clustersCreated{0};
    std::size_t clustersUpdated{0};
    std::size_t membershipsUpdated{0};
    std::size_t bridgeDocsTagged{0};
    std::size_t medoidsSelected{0};
    std::size_t dirtySeedCount{0};
    std::size_t dirtyRegionDocs{0};
    std::size_t coalescedDirtySets{0};
    std::size_t fallbackFullRebuilds{0};
    Duration elapsed{Duration{0}};
};

enum class RouteScoringMode : uint8_t {
    Current,
    SizeWeighted,
    SeedCoverage,
};

/// Query-time evidence for routing a document into a cluster. The weight is
/// already normalized/rank-discounted by the caller; routers aggregate it
/// without needing to know which lexical backend produced it.
struct WeightedDocumentSeed {
    std::string documentHash;
    float weight{0.0F};
};

struct TopologyRouteRequest {
    std::string queryText;
    std::vector<std::string> seedDocumentHashes;
    std::vector<WeightedDocumentSeed> weightedSeedDocuments;
    std::size_t limit{8};
    RouteScoringMode scoringMode{RouteScoringMode::Current};
    // Phase S: optional dense signal for sparse-guided routing.
    // Empty queryEmbedding falls back to seed-only scoring (current behaviour).
    std::vector<float> queryEmbedding;
    // Blend factor for SparseGuidedClusterRouter: score = alpha · bm25_mass +
    // (1-alpha) · centroid_cosine. 0.0 = pure dense; 1.0 = pure sparse.
    float sparseDenseAlpha{0.5F};
    /// Maximum total dense representatives evaluated per cluster, including the centroid.
    /// Zero evaluates the complete prebuilt cover.
    std::size_t maxRoutingRepresentatives{0};
};

struct ClusterRoute {
    std::string clusterId;
    std::optional<std::string> medoidDocumentHash;
    double routeScore{0.0};
    double stabilityScore{0.0};
    std::size_t memberCount{0};
};

} // namespace yams::topology
