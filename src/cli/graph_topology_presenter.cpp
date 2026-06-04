#include <yams/cli/graph_topology_presenter.h>

#include <yams/cli/graph_helpers.h>
#include <yams/cli/graph_scope_support.h>
#include <yams/cli/graph_topology_support.h>
#include <yams/cli/ui_helpers.hpp>

#include <nlohmann/json.hpp>

#include <algorithm>
#include <iostream>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace yams::cli {

using json = nlohmann::json;

namespace {

std::vector<const yams::topology::ClusterArtifact*>
sortedClusters(const yams::topology::TopologyArtifactBatch& snapshot) {
    std::vector<const yams::topology::ClusterArtifact*> clusters;
    clusters.reserve(snapshot.clusters.size());
    for (const auto& cluster : snapshot.clusters) {
        clusters.push_back(&cluster);
    }
    std::sort(clusters.begin(), clusters.end(), [](const auto* lhs, const auto* rhs) {
        if (lhs->bridgeMass != rhs->bridgeMass) {
            return lhs->bridgeMass > rhs->bridgeMass;
        }
        if (lhs->persistenceScore != rhs->persistenceScore) {
            return lhs->persistenceScore > rhs->persistenceScore;
        }
        if (lhs->memberCount != rhs->memberCount) {
            return lhs->memberCount > rhs->memberCount;
        }
        return lhs->clusterId < rhs->clusterId;
    });
    return clusters;
}

TopologyClusterStats buildClusterStatsFromViews(
    const std::vector<yams::cli::TopologyClusterMembershipView>& membershipViews) {
    TopologyClusterStats stats;
    for (const auto& view : membershipViews) {
        if (view.membership == nullptr) {
            continue;
        }
        stats.roleCounts[topologyRoleLabel(view.membership->role)]++;
        if (view.inScope) {
            ++stats.scopedMemberCount;
        }
    }
    return stats;
}

} // namespace

Result<void>
renderTopologySnapshots(std::ostream& out,
                        const std::optional<yams::topology::TopologyArtifactBatch>& snapshot,
                        const TopologyCommandRenderOptions& options) {
    if (!snapshot.has_value()) {
        if (options.jsonOutput) {
            json payload;
            payload["snapshot"] = nullptr;
            payload["requested_snapshot_id"] = options.requestedSnapshotId.empty()
                                                   ? json(nullptr)
                                                   : json(options.requestedSnapshotId);
            out << payload.dump(2) << "\n";
        } else {
            out << yams::cli::ui::section_header("Topology Snapshots") << "\n\n";
            out << yams::cli::ui::status_info("No topology snapshot available") << "\n";
        }
        return Result<void>();
    }

    const auto& value = *snapshot;
    if (options.jsonOutput) {
        json payload;
        payload["snapshot"] = {{"snapshot_id", value.snapshotId},
                               {"algorithm", value.algorithm},
                               {"input_kind", topologyInputKindLabel(value.inputKind)},
                               {"generated_at_unix_seconds", value.generatedAtUnixSeconds},
                               {"topology_epoch", value.topologyEpoch},
                               {"cluster_count", value.clusters.size()},
                               {"membership_count", value.memberships.size()}};
        out << payload.dump(2) << "\n";
        return Result<void>();
    }

    out << yams::cli::ui::section_header("Topology Snapshots") << "\n\n";
    yams::cli::ui::Table table;
    table.headers = {"SNAPSHOT", "ALGORITHM", "INPUT", "CLUSTERS", "MEMBERSHIPS", "EPOCH"};
    table.has_header = true;
    table.add_row({value.snapshotId, value.algorithm.empty() ? "-" : value.algorithm,
                   topologyInputKindLabel(value.inputKind),
                   yams::cli::ui::format_number(value.clusters.size()),
                   yams::cli::ui::format_number(value.memberships.size()),
                   yams::cli::ui::format_number(value.topologyEpoch)});
    yams::cli::ui::render_table(out, table);

    if (options.verbose) {
        out << "\n"
            << yams::cli::ui::key_value("Generated At (unix seconds)",
                                        std::to_string(value.generatedAtUnixSeconds))
            << "\n";
        if (!options.requestedSnapshotId.empty()) {
            out << yams::cli::ui::key_value("Requested Snapshot", options.requestedSnapshotId)
                << "\n";
        }
    }
    return Result<void>();
}

Result<void> renderTopologyClusters(std::ostream& out,
                                    const yams::topology::TopologyArtifactBatch& snapshot,
                                    const GraphTopologySupport& support,
                                    const TopologyCommandRenderOptions& options) {
    std::unordered_set<std::string> scopedPaths;
    if (options.scopeToCwd) {
        auto scopeRes = support.buildCurrentScopePathSet(options.cwd);
        if (!scopeRes) {
            return scopeRes.error();
        }
        scopedPaths = std::move(scopeRes.value());
    }
    const auto clusters = sortedClusters(snapshot);
    // Perf guard: keep cluster stats precomputed once per snapshot. Moving scoped-stat hydration
    // into the cluster loop regresses topology rendering back toward repeated membership scans and
    // repeated document-hash resolution on large snapshots.
    const auto statsByClusterId =
        options.scopeToCwd ? support.buildClusterStatsById(snapshot, scopedPaths, options.cwd)
                           : support.buildClusterStatsById(snapshot);

    if (options.jsonOutput) {
        json payload;
        payload["snapshot_id"] = snapshot.snapshotId;
        payload["cluster_count"] = snapshot.clusters.size();
        json clustersJson = json::array();
        for (const auto* cluster : clusters) {
            const auto statsIt = statsByClusterId.find(cluster->clusterId);
            const TopologyClusterStats emptyStats;
            const auto& stats = statsIt != statsByClusterId.end() ? statsIt->second : emptyStats;
            std::string medoidPath;
            if (cluster->medoid.has_value()) {
                medoidPath = !cluster->medoid->filePath.empty()
                                 ? cluster->medoid->filePath
                                 : support.resolveDocumentPathByHash(cluster->medoid->documentHash);
                medoidPath = projectPathForCli(medoidPath, options.cwd);
            }
            json row;
            row["cluster_id"] = cluster->clusterId;
            row["level"] = cluster->level;
            row["member_count"] = cluster->memberCount;
            row["scoped_member_count"] = stats.scopedMemberCount;
            row["persistence_score"] = cluster->persistenceScore;
            row["cohesion_score"] = cluster->cohesionScore;
            row["bridge_mass"] = cluster->bridgeMass;
            row["role_summary"] = formatTopologyRoleCounts(stats.roleCounts);
            row["parent_cluster_id"] = cluster->parentClusterId.has_value()
                                           ? json(*cluster->parentClusterId)
                                           : json(nullptr);
            row["overlap_cluster_ids"] = cluster->overlapClusterIds;
            if (cluster->medoid.has_value()) {
                row["medoid"] = {{"document_hash", cluster->medoid->documentHash},
                                 {"file_path", medoidPath},
                                 {"representative_score", cluster->medoid->representativeScore}};
            }
            clustersJson.push_back(std::move(row));
        }
        payload["clusters"] = std::move(clustersJson);
        out << payload.dump(2) << "\n";
        return Result<void>();
    }

    out << yams::cli::ui::section_header("Topology Clusters") << "\n\n";
    out << yams::cli::ui::status_info("Snapshot: " + snapshot.snapshotId) << "\n\n";

    yams::cli::ui::Table table;
    table.headers = {"CLUSTER", "LEVEL",    "MEMBERS", "SCOPED", "ROLES",
                     "PERSIST", "COHESION", "BRIDGE",  "MEDOID"};
    table.has_header = true;
    for (const auto* cluster : clusters) {
        const auto statsIt = statsByClusterId.find(cluster->clusterId);
        const TopologyClusterStats emptyStats;
        const auto& stats = statsIt != statsByClusterId.end() ? statsIt->second : emptyStats;
        std::string medoid = "-";
        if (cluster->medoid.has_value()) {
            medoid = !cluster->medoid->filePath.empty()
                         ? cluster->medoid->filePath
                         : support.resolveDocumentPathByHash(cluster->medoid->documentHash);
            medoid = projectPathForCli(medoid, options.cwd);
            if (medoid.empty()) {
                medoid = cluster->medoid->documentHash;
            }
        }
        table.add_row(
            {yams::cli::ui::truncate_to_width(cluster->clusterId, 28),
             std::to_string(cluster->level), yams::cli::ui::format_number(cluster->memberCount),
             yams::cli::ui::format_number(stats.scopedMemberCount),
             yams::cli::ui::truncate_to_width(formatTopologyRoleCounts(stats.roleCounts), 26),
             yams::cli::ui::truncate_to_width(std::to_string(cluster->persistenceScore), 6),
             yams::cli::ui::truncate_to_width(std::to_string(cluster->cohesionScore), 6),
             yams::cli::ui::truncate_to_width(std::to_string(cluster->bridgeMass), 6),
             yams::cli::ui::truncate_to_width(medoid, 38)});
    }
    yams::cli::ui::render_table(out, table);

    if (options.verbose) {
        out << "\n"
            << yams::cli::ui::status_info(
                   "Sorted by bridge mass, then persistence, then member count")
            << "\n";
    }
    return Result<void>();
}

Result<void> renderTopologyClusterDetail(std::ostream& out,
                                         const yams::topology::TopologyArtifactBatch& snapshot,
                                         const GraphTopologySupport& support,
                                         const TopologyCommandRenderOptions& options) {
    auto clusterIt =
        std::find_if(snapshot.clusters.begin(), snapshot.clusters.end(),
                     [&](const auto& cluster) { return cluster.clusterId == options.clusterId; });
    if (clusterIt == snapshot.clusters.end()) {
        return Error{ErrorCode::NotFound, "Topology cluster not found: " + options.clusterId};
    }

    const auto& cluster = *clusterIt;
    std::unordered_set<std::string> scopedPaths;
    if (options.scopeToCwd) {
        auto scopeRes = support.buildCurrentScopePathSet(options.cwd);
        if (!scopeRes) {
            return scopeRes.error();
        }
        scopedPaths = std::move(scopeRes.value());
    }
    // Perf guard: build membership views once for the requested cluster and derive scoped counts
    // from that cached view set instead of re-running membership/path lookups per output section.
    const auto membershipViews =
        options.scopeToCwd
            ? support.buildClusterMembershipViews(snapshot, cluster, scopedPaths, options.cwd)
            : support.buildClusterMembershipViews(snapshot, cluster);
    const auto clusterStats = buildClusterStatsFromViews(membershipViews);

    json membersJson = json::array();
    yams::cli::ui::Table membersTable;
    membersTable.headers = {"ROLE", "PATH", "HASH", "BRIDGE"};
    membersTable.has_header = true;
    std::size_t scopedMemberCount = 0;

    for (const auto& view : membershipViews) {
        if (view.membership == nullptr || !view.inScope) {
            continue;
        }

        const auto& membership = *view.membership;
        std::string path = projectPathForCli(view.resolvedPath, options.cwd);
        ++scopedMemberCount;
        json member{{"document_hash", membership.documentHash},
                    {"path", path.empty() ? json(nullptr) : json(path)},
                    {"role", topologyRoleLabel(membership.role)},
                    {"bridge_score", membership.bridgeScore},
                    {"cluster_level", membership.clusterLevel},
                    {"persistence_score", membership.persistenceScore},
                    {"cohesion_score", membership.cohesionScore},
                    {"overlap_cluster_ids", membership.overlapClusterIds}};
        membersJson.push_back(std::move(member));

        membersTable.add_row(
            {topologyRoleLabel(membership.role),
             yams::cli::ui::truncate_to_width(path.empty() ? "-" : path, 48),
             yams::cli::ui::truncate_to_width(membership.documentHash, 18),
             yams::cli::ui::truncate_to_width(std::to_string(membership.bridgeScore), 6)});
    }

    if (options.jsonOutput) {
        json payload;
        payload["snapshot_id"] = snapshot.snapshotId;
        payload["cluster"] = {{"cluster_id", cluster.clusterId},
                              {"parent_cluster_id", cluster.parentClusterId.has_value()
                                                        ? json(*cluster.parentClusterId)
                                                        : json(nullptr)},
                              {"level", cluster.level},
                              {"member_count", cluster.memberCount},
                              {"scoped_member_count", scopedMemberCount},
                              {"role_summary", formatTopologyRoleCounts(clusterStats.roleCounts)},
                              {"role_counts", clusterStats.roleCounts},
                              {"persistence_score", cluster.persistenceScore},
                              {"cohesion_score", cluster.cohesionScore},
                              {"bridge_mass", cluster.bridgeMass},
                              {"overlap_cluster_ids", cluster.overlapClusterIds}};
        if (cluster.medoid.has_value()) {
            std::string medoidPath =
                !cluster.medoid->filePath.empty()
                    ? cluster.medoid->filePath
                    : support.resolveDocumentPathByHash(cluster.medoid->documentHash);
            medoidPath = projectPathForCli(medoidPath, options.cwd);
            payload["cluster"]["medoid"] = {
                {"document_hash", cluster.medoid->documentHash},
                {"file_path", medoidPath},
                {"representative_score", cluster.medoid->representativeScore}};
        }
        payload["scope_cwd"] = options.scopeToCwd;
        payload["members"] = std::move(membersJson);
        out << payload.dump(2) << "\n";
        return Result<void>();
    }

    out << yams::cli::ui::section_header("Topology Cluster") << "\n\n";
    out << yams::cli::ui::key_value("Snapshot", snapshot.snapshotId) << "\n";
    out << yams::cli::ui::key_value("Cluster", cluster.clusterId) << "\n";
    if (cluster.parentClusterId.has_value()) {
        out << yams::cli::ui::key_value("Parent", *cluster.parentClusterId) << "\n";
    }
    out << yams::cli::ui::key_value("Level", std::to_string(cluster.level)) << "\n";
    out << yams::cli::ui::key_value("Members", std::to_string(cluster.memberCount)) << "\n";
    if (options.scopeToCwd) {
        out << yams::cli::ui::key_value("Scoped Members", std::to_string(scopedMemberCount))
            << "\n";
    }
    out << yams::cli::ui::key_value("Roles", formatTopologyRoleCounts(clusterStats.roleCounts))
        << "\n";
    out << yams::cli::ui::key_value("Persistence", std::to_string(cluster.persistenceScore))
        << "\n";
    out << yams::cli::ui::key_value("Cohesion", std::to_string(cluster.cohesionScore)) << "\n";
    out << yams::cli::ui::key_value("Bridge", std::to_string(cluster.bridgeMass)) << "\n";
    if (!cluster.overlapClusterIds.empty()) {
        std::ostringstream os;
        for (std::size_t i = 0; i < cluster.overlapClusterIds.size(); ++i) {
            if (i) {
                os << ", ";
            }
            os << cluster.overlapClusterIds[i];
        }
        out << yams::cli::ui::key_value("Overlaps", os.str()) << "\n";
    }
    if (cluster.medoid.has_value()) {
        std::string medoidPath =
            !cluster.medoid->filePath.empty()
                ? cluster.medoid->filePath
                : support.resolveDocumentPathByHash(cluster.medoid->documentHash);
        medoidPath = projectPathForCli(medoidPath, options.cwd);
        out << yams::cli::ui::key_value("Medoid Hash", cluster.medoid->documentHash) << "\n";
        if (!medoidPath.empty()) {
            out << yams::cli::ui::key_value("Medoid Path", medoidPath) << "\n";
        }
    }

    out << "\n" << yams::cli::ui::subsection_header("Members") << "\n";
    if (scopedMemberCount == 0) {
        out << yams::cli::ui::status_info(options.scopeToCwd ? "No members in current scope"
                                                             : "No members found")
            << "\n";
    } else {
        yams::cli::ui::render_table(out, membersTable);
    }
    if (options.scopeToCwd) {
        out << "\n" << yams::cli::ui::status_info(std::string{kGraphScopeToCwdDescription}) << "\n";
    }
    return Result<void>();
}

} // namespace yams::cli
