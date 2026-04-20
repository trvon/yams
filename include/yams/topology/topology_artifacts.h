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
    std::size_t overlapLimit{2};
    std::size_t rollingWindowDocuments{0};
    std::size_t fullRebuildDocThreshold{4096};
    Duration coalesceWindow{Duration{250}};
    double minEdgeScore{0.0};
    double minClusterPersistence{0.0};
    bool reciprocalOnly{true};
    bool allowOverlap{true};
    bool emitBridgeAnnotations{true};
    bool emitOutliers{true};
    DirtyRegionExpansionMode dirtyRegionExpansion{
        DirtyRegionExpansionMode::PriorClusterAndNeighbors};
    Duration budget{Duration{250}};
    // KMeansEmbedding engine: requested cluster count (0 = auto via sqrt_n heuristic);
    // ignored by other engines.
    std::size_t kmeansK{0};
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

struct ClusterArtifact {
    std::string clusterId;
    std::optional<std::string> parentClusterId;
    std::size_t level{0};
    std::size_t memberCount{0};
    double persistenceScore{0.0};
    double cohesionScore{0.0};
    double bridgeMass{0.0};
    std::optional<ClusterRepresentative> medoid;
    std::vector<std::string> memberDocumentHashes;
    std::vector<std::string> overlapClusterIds;
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

struct TopologyRouteRequest {
    std::string queryText;
    std::vector<std::string> seedDocumentHashes;
    std::size_t limit{8};
    bool preferStableClusters{true};
    bool weakQueryOnly{true};
    RouteScoringMode scoringMode{RouteScoringMode::Current};
};

struct ClusterRoute {
    std::string clusterId;
    std::optional<std::string> medoidDocumentHash;
    double routeScore{0.0};
    double stabilityScore{0.0};
    std::size_t memberCount{0};
};

} // namespace yams::topology
