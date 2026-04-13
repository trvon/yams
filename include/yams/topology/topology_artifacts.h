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
    std::size_t maxLevels{3};
    std::size_t overlapLimit{2};
    std::size_t rollingWindowDocuments{0};
    double minEdgeScore{0.0};
    double minClusterPersistence{0.0};
    bool reciprocalOnly{true};
    bool allowOverlap{true};
    bool emitBridgeAnnotations{true};
    bool emitOutliers{true};
    Duration budget{Duration{250}};
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
    Duration elapsed{Duration{0}};
};

struct TopologyRouteRequest {
    std::string queryText;
    std::vector<std::string> seedDocumentHashes;
    std::size_t limit{8};
    bool preferStableClusters{true};
    bool weakQueryOnly{true};
};

struct ClusterRoute {
    std::string clusterId;
    std::optional<std::string> medoidDocumentHash;
    double routeScore{0.0};
    double stabilityScore{0.0};
    std::size_t memberCount{0};
};

} // namespace yams::topology
