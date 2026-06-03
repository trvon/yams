#include <yams/cli/graph_topology_support.h>

#include <yams/cli/graph_helpers.h>
#include <yams/cli/graph_scope_support.h>
#include <yams/cli/yams_cli.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_metadata_store.h>

#include <chrono>

namespace yams::cli {

namespace {} // namespace

std::string topologyInputKindLabel(yams::topology::TopologyInputKind kind) {
    using yams::topology::TopologyInputKind;
    switch (kind) {
        case TopologyInputKind::SemanticNeighborGraph:
            return "semantic_neighbor_graph";
        case TopologyInputKind::EmbeddingNeighborhood:
            return "embedding_neighborhood";
        case TopologyInputKind::Hybrid:
            return "hybrid";
    }
    return "hybrid";
}

std::string topologyRoleLabel(yams::topology::DocumentTopologyRole role) {
    using yams::topology::DocumentTopologyRole;
    switch (role) {
        case DocumentTopologyRole::Core:
            return "core";
        case DocumentTopologyRole::Bridge:
            return "bridge";
        case DocumentTopologyRole::Medoid:
            return "medoid";
        case DocumentTopologyRole::Outlier:
            return "outlier";
    }
    return "core";
}

std::string
formatTopologyRoleCounts(const std::unordered_map<std::string, std::size_t>& roleCounts) {
    return formatRelationCounts(roleCounts, 4);
}

GraphTopologySupport::GraphTopologySupport(YamsCLI* cli, std::string snapshotId)
    : cli_(cli), snapshotId_(std::move(snapshotId)) {}

GraphTopologySupport::GraphTopologySupport(
    YamsCLI* cli, std::string snapshotId,
    std::shared_ptr<metadata::IMetadataRepository> metadataRepo,
    std::shared_ptr<metadata::KnowledgeGraphStore> kgStore)
    : cli_(cli), snapshotId_(std::move(snapshotId)),
      readContextInitialized_(metadataRepo != nullptr), metadataRepoCache_(std::move(metadataRepo)),
      kgStoreCache_(std::move(kgStore)) {}

Result<void> GraphTopologySupport::ensureReadContext() const {
    if (readContextInitialized_) {
        return Result<void>();
    }

    if (cli_ == nullptr) {
        readContextInitialized_ = true;
        return Result<void>();
    }

    if (auto repo = cli_->getMetadataRepository()) {
        metadataRepoCache_ = std::move(repo);
        kgStoreCache_ = cli_->getKnowledgeGraphStore();
        readContextInitialized_ = true;
        return Result<void>();
    }

    const auto dbPath = cli_->getDataPath() / "yams.db";
    if (!std::filesystem::exists(dbPath)) {
        readContextInitialized_ = true;
        return Result<void>();
    }

    metadata::ConnectionPoolConfig poolConfig;
    poolConfig.maxConnections = 4;
    poolConfig.minConnections = 1;
    poolConfig.connectTimeout = std::chrono::seconds(10);

    auto pool = std::make_shared<metadata::ConnectionPool>(dbPath.string(), poolConfig);
    auto poolInit = pool->initialize();
    if (!poolInit) {
        return poolInit.error();
    }

    connectionPoolCache_ = std::move(pool);
    metadataRepoCache_ = std::make_shared<metadata::MetadataRepository>(*connectionPoolCache_);

    metadata::KnowledgeGraphStoreConfig kgCfg;
    kgCfg.enable_alias_fts = true;
    kgCfg.enable_wal = true;
    auto kgStoreRes = metadata::makeSqliteKnowledgeGraphStore(dbPath.string(), kgCfg);
    if (kgStoreRes) {
        kgStoreCache_ =
            std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(kgStoreRes.value()));
    }

    readContextInitialized_ = true;
    return Result<void>();
}

Result<std::optional<yams::topology::TopologyArtifactBatch>>
GraphTopologySupport::loadTopologySnapshot() const {
    auto ctxRes = ensureReadContext();
    if (!ctxRes) {
        return ctxRes.error();
    }
    if (!metadataRepoCache_) {
        return std::optional<yams::topology::TopologyArtifactBatch>{std::nullopt};
    }

    yams::topology::MetadataKgTopologyArtifactStore store(metadataRepoCache_, kgStoreCache_);
    return store.loadLatest(snapshotId_);
}

std::string GraphTopologySupport::resolveDocumentPathByHash(const std::string& hash) const {
    if (hash.empty()) {
        return {};
    }
    if (cli_ == nullptr && metadataRepoCache_ == nullptr) {
        return {};
    }
    if (auto it = documentPathByHashCache_.find(hash); it != documentPathByHashCache_.end()) {
        return it->second;
    }

    auto ctxRes = ensureReadContext();
    if (!ctxRes) {
        return {};
    }
    if (!metadataRepoCache_) {
        documentPathByHashCache_.emplace(hash, std::string{});
        return {};
    }

    auto docRes = metadataRepoCache_->getDocumentByHash(hash);
    if (!docRes) {
        return {};
    }
    if (!docRes.value().has_value()) {
        documentPathByHashCache_.emplace(hash, std::string{});
        return {};
    }

    auto filePath = docRes.value()->filePath;
    documentPathByHashCache_.emplace(hash, filePath);
    return filePath;
}

Result<std::unordered_set<std::string>>
GraphTopologySupport::buildCurrentScopePathSet(const std::filesystem::path& cwd) const {
    return buildGraphCurrentScopePathSet(cli_, cwd);
}

std::unordered_map<std::string, TopologyClusterStats> GraphTopologySupport::buildClusterStatsById(
    const yams::topology::TopologyArtifactBatch& snapshot) const {
    std::unordered_map<std::string, TopologyClusterStats> statsByClusterId;
    for (const auto& membership : snapshot.memberships) {
        auto& stats = statsByClusterId[membership.clusterId];
        stats.roleCounts[topologyRoleLabel(membership.role)]++;
        ++stats.scopedMemberCount;
    }
    return statsByClusterId;
}

std::unordered_map<std::string, TopologyClusterStats>
GraphTopologySupport::buildClusterStatsById(const yams::topology::TopologyArtifactBatch& snapshot,
                                            const std::unordered_set<std::string>& scopedPaths,
                                            const std::filesystem::path& cwd) const {
    std::unordered_map<std::string, TopologyClusterStats> statsByClusterId;
    for (const auto& membership : snapshot.memberships) {
        auto& stats = statsByClusterId[membership.clusterId];
        stats.roleCounts[topologyRoleLabel(membership.role)]++;

        std::string path = resolveDocumentPathByHash(membership.documentHash);
        if (!path.empty()) {
            auto normalized = normalizeGraphScopePath(std::filesystem::path(path), cwd);
            if (scopedPaths.count(normalized) > 0) {
                ++stats.scopedMemberCount;
            }
        }
    }
    return statsByClusterId;
}

std::vector<TopologyClusterMembershipView> GraphTopologySupport::buildClusterMembershipViews(
    const yams::topology::TopologyArtifactBatch& snapshot,
    const yams::topology::ClusterArtifact& cluster) const {
    std::vector<TopologyClusterMembershipView> views;
    views.reserve(cluster.memberCount);

    for (const auto& membership : snapshot.memberships) {
        if (membership.clusterId != cluster.clusterId) {
            continue;
        }

        TopologyClusterMembershipView view;
        view.membership = &membership;
        view.resolvedPath = resolveDocumentPathByHash(membership.documentHash);
        view.inScope = true;
        views.push_back(std::move(view));
    }

    return views;
}

std::vector<TopologyClusterMembershipView> GraphTopologySupport::buildClusterMembershipViews(
    const yams::topology::TopologyArtifactBatch& snapshot,
    const yams::topology::ClusterArtifact& cluster,
    const std::unordered_set<std::string>& scopedPaths, const std::filesystem::path& cwd) const {
    std::vector<TopologyClusterMembershipView> views;
    views.reserve(cluster.memberCount);

    for (const auto& membership : snapshot.memberships) {
        if (membership.clusterId != cluster.clusterId) {
            continue;
        }

        TopologyClusterMembershipView view;
        view.membership = &membership;
        view.resolvedPath = resolveDocumentPathByHash(membership.documentHash);
        view.inScope = false;
        if (!view.resolvedPath.empty()) {
            auto normalized =
                normalizeGraphScopePath(std::filesystem::path(view.resolvedPath), cwd);
            view.inScope = scopedPaths.count(normalized) > 0;
        }
        views.push_back(std::move(view));
    }

    return views;
}

TopologyClusterStats GraphTopologySupport::collectTopologyClusterStats(
    const yams::topology::TopologyArtifactBatch& snapshot,
    const yams::topology::ClusterArtifact& cluster,
    const std::unordered_set<std::string>* scopedPaths, const std::filesystem::path* cwd) const {
    std::unordered_map<std::string, TopologyClusterStats> statsByClusterId;
    if (scopedPaths != nullptr && cwd != nullptr) {
        statsByClusterId = buildClusterStatsById(snapshot, *scopedPaths, *cwd);
    } else {
        statsByClusterId = buildClusterStatsById(snapshot);
    }
    if (auto it = statsByClusterId.find(cluster.clusterId); it != statsByClusterId.end()) {
        return it->second;
    }
    return {};
}

} // namespace yams::cli
