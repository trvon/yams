#pragma once

#include <yams/core/types.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/topology/topology_engine.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace yams::cli {

class YamsCLI;

std::string topologyInputKindLabel(yams::topology::TopologyInputKind kind);
std::string topologyRoleLabel(yams::topology::DocumentTopologyRole role);
std::string
formatTopologyRoleCounts(const std::unordered_map<std::string, std::size_t>& roleCounts);

struct TopologyClusterStats {
    std::size_t scopedMemberCount{0};
    std::unordered_map<std::string, std::size_t> roleCounts;
};

struct TopologyClusterMembershipView {
    const yams::topology::DocumentClusterMembership* membership{nullptr};
    std::string resolvedPath;
    bool inScope{true};
};

class GraphTopologySupport {
public:
    GraphTopologySupport(YamsCLI* cli, std::string snapshotId);
    GraphTopologySupport(YamsCLI* cli, std::string snapshotId,
                         std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
                         std::shared_ptr<metadata::KnowledgeGraphStore> kgStore = nullptr);

    Result<std::optional<yams::topology::TopologyArtifactBatch>> loadTopologySnapshot() const;
    std::string resolveDocumentPathByHash(const std::string& hash) const;
    Result<std::unordered_set<std::string>>
    buildCurrentScopePathSet(const std::filesystem::path& cwd) const;
    // Perf guard: cluster-list/detail renderers should batch topology membership work once per
    // snapshot and reuse these precomputed views/maps inside cluster loops. Regressing to
    // per-cluster recomputation reintroduces O(clusters x memberships) scans and extra metadata
    // lookups on the human-output hot path.
    std::unordered_map<std::string, TopologyClusterStats>
    buildClusterStatsById(const yams::topology::TopologyArtifactBatch& snapshot) const;
    std::unordered_map<std::string, TopologyClusterStats>
    buildClusterStatsById(const yams::topology::TopologyArtifactBatch& snapshot,
                          const std::unordered_set<std::string>& scopedPaths,
                          const std::filesystem::path& cwd) const;
    std::vector<TopologyClusterMembershipView>
    buildClusterMembershipViews(const yams::topology::TopologyArtifactBatch& snapshot,
                                const yams::topology::ClusterArtifact& cluster) const;
    std::vector<TopologyClusterMembershipView>
    buildClusterMembershipViews(const yams::topology::TopologyArtifactBatch& snapshot,
                                const yams::topology::ClusterArtifact& cluster,
                                const std::unordered_set<std::string>& scopedPaths,
                                const std::filesystem::path& cwd) const;
    // Compatibility helper for one-off callers. Avoid invoking this inside per-cluster loops;
    // prefer the batched helpers above so document-path memoization and membership precomputation
    // stay effective.
    TopologyClusterStats
    collectTopologyClusterStats(const yams::topology::TopologyArtifactBatch& snapshot,
                                const yams::topology::ClusterArtifact& cluster,
                                const std::unordered_set<std::string>* scopedPaths = nullptr,
                                const std::filesystem::path* cwd = nullptr) const;

private:
    Result<void> ensureReadContext() const;

    YamsCLI* cli_{nullptr};
    std::string snapshotId_;
    mutable bool readContextInitialized_{false};
    mutable std::shared_ptr<metadata::ConnectionPool> connectionPoolCache_;
    mutable std::shared_ptr<metadata::IMetadataRepository> metadataRepoCache_;
    mutable std::shared_ptr<metadata::KnowledgeGraphStore> kgStoreCache_;
    mutable std::unordered_map<std::string, std::string> documentPathByHashCache_;
};

} // namespace yams::cli
