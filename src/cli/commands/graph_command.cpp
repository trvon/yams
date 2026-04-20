#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/graph_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/core/magic_numbers.hpp>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>
#include <yams/metadata/connection_pool.h>
#include <yams/metadata/knowledge_graph_store.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>
#include <yams/topology/topology_metadata_store.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>

namespace yams::cli {

using json = nlohmann::json;

class GraphCommand : public ICommand {
public:
    std::string getName() const override { return "graph"; }
    std::string getDescription() const override {
        return "Inspect knowledge graph relationships and entities";
    }

    void registerCommand(CLI::App& app, YamsCLI* cli) override {
        cli_ = cli;
        auto* cmd = app.add_subcommand(getName(), getDescription());

        // Target selection - multiple options available
        auto* group = cmd->add_option_group("target");
        group->add_option("hash", hash_, "SHA-256 of the target document");
        group->add_option("--name", name_, "Path/name of the target document");
        group->add_option("--node-key", nodeKey_, "Direct KG node key (e.g., fn:abc123:0x1000)");
        group->add_option("--node-id", nodeId_, "Direct KG node ID (integer)");
        // For listing nodes by type without traversal
        group->add_option("--list-type", listNodeType_,
                          "List KG nodes by type (e.g., binary.function, binary.import)");

        cmd->add_option("--depth", depth_, "Graph traversal depth (1-5)")
            ->default_val(1)
            ->check(CLI::Range(1, 5));

        // Relation filtering for traversal
        cmd->add_option("--relation,-r", relationFilter_,
                        "Filter edges by relation (e.g., calls, contains, imports)");

        // Output options
        cmd->add_option("--limit,-l", limit_, "Maximum results to return")->default_val(100);
        cmd->add_option("--offset", offset_, "Pagination offset")->default_val(0);
        cmd->add_flag("-v,--verbose", verbose_, "Verbose output with properties");
        cmd->add_flag("--json", jsonOutput_, "Output as JSON");
        cmd->add_option("--format", outputFormat_, "Output format: table, json, dot (for graphviz)")
            ->default_val("table");

        // Search within node properties (for binary analysis)
        cmd->add_option("--prop-filter", propFilter_,
                        "Filter nodes by JSON property (e.g., 'decompiled:malloc')");

        cmd->add_flag("--scope-cwd", scopeToCwd_,
                      "Scope list results to src/** and include/** under CWD");

        // yams-66h: List available node types with counts
        cmd->add_flag("--list-types", listTypes_, "List available node types with counts");

        // yams-kt5t: List relation types with counts
        cmd->add_flag("--relations", listRelations_, "List available relation types with counts");

        // yams-kt5t: Search nodes by label pattern
        cmd->add_option("--search", searchPattern_,
                        "Search nodes by label pattern (supports * and ? wildcards)");

        cmd->add_flag("--topology-snapshots", topologySnapshots_,
                      "Show topology snapshot summary from the artifact store");
        cmd->add_flag("--topology-clusters", topologyClusters_,
                      "List topology clusters from the artifact store");
        cmd->add_option("--cluster", topologyClusterId_, "Show detail for a topology cluster id");
        cmd->add_option("--snapshot", topologySnapshotId_,
                        "Specific topology snapshot id (defaults to latest)");

        cmd->callback([this]() { cli_->setPendingCommand(this); });
    }

    Result<void> execute() override { return Result<void>(); }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        try {
            using namespace yams::daemon;

            // yams-66h: Handle --list-types mode (show available node types)
            if (listTypes_) {
                co_return co_await executeListTypes();
            }

            // yams-kt5t: Handle --relations mode (show relation types with counts)
            if (listRelations_) {
                co_return co_await executeListRelations();
            }

            // yams-kt5t: Handle --search mode (search nodes by label pattern)
            if (!searchPattern_.empty()) {
                co_return co_await executeSearch();
            }

            if (topologySnapshots_) {
                co_return executeTopologySnapshots();
            }

            if (topologyClusters_) {
                co_return executeTopologyClusters();
            }

            if (!topologyClusterId_.empty()) {
                co_return executeTopologyClusterDetail();
            }

            // Handle --list-type mode (list nodes by type without traversal)
            if (!listNodeType_.empty()) {
                co_return co_await executeListByType();
            }

            // Handle standard graph traversal
            co_return co_await executeGraphTraversal();

        } catch (const std::exception& e) {
            co_return Error{ErrorCode::Unknown, e.what()};
        }
    }

private:
    Result<yams::cli::CliDaemonClientLease> acquireGraphClientLease() const {
        yams::daemon::ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);
        return yams::cli::acquire_cli_daemon_client_shared_with_policy(
            cfg, yams::cli::CliDaemonAccessPolicy::AllowInProcessFallback, 1, 12,
            std::chrono::milliseconds(10000));
    }

    bool wantsJsonOutput() const { return jsonOutput_ || outputFormat_ == "json"; }

    void printFallbackNoticeIfNeeded(const yams::cli::CliDaemonClientPlan& plan) const {
        if (!plan.usedInProcessFallback) {
            return;
        }
        if (!plan.fallbackReason.empty()) {
            spdlog::info("graph: socket transport unavailable; using in-process transport: {}",
                         plan.fallbackReason);
        } else {
            spdlog::info("graph: socket transport unavailable; using in-process transport");
        }
        if (wantsJsonOutput()) {
            return;
        }
        const bool verboseMode = verbose_ || (cli_ != nullptr && cli_->getVerbose());
        if (!verboseMode) {
            return;
        }
        std::cout << "Using in-process transport (socket daemon not ready)" << "\n";
        if (!plan.fallbackReason.empty()) {
            std::cout << "  Reason: " << plan.fallbackReason << "\n";
        }
    }

    static std::string canonicalizeRelationName(std::string value) {
        auto trimLeft = std::find_if_not(value.begin(), value.end(),
                                         [](unsigned char c) { return std::isspace(c) != 0; });
        auto trimRight = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char c) {
                             return std::isspace(c) != 0;
                         }).base();
        if (trimLeft >= trimRight) {
            return {};
        }

        std::string normalized(trimLeft, trimRight);
        std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        std::replace(normalized.begin(), normalized.end(), '-', '_');
        std::replace(normalized.begin(), normalized.end(), ' ', '_');

        if (normalized == "call")
            return "calls";
        if (normalized == "include")
            return "includes";
        if (normalized == "inherit")
            return "inherits";
        if (normalized == "implement")
            return "implements";
        if (normalized == "reference")
            return "references";
        if (normalized == "rename_to")
            return "renamed_to";
        if (normalized == "rename_from")
            return "renamed_from";

        return normalized;
    }

    static std::string buildPathFileNodeKey(const std::string& path) {
        try {
            auto derived = yams::metadata::computePathDerivedValues(path);
            if (!derived.normalizedPath.empty()) {
                return "path:file:" + derived.normalizedPath;
            }
        } catch (...) {
            // Fall through to raw path fallback.
        }
        return "path:file:" + path;
    }

    static std::string displayNodePath(const yams::daemon::GraphNode& node) {
        if (!node.documentPath.empty()) {
            return node.documentPath;
        }
        auto extracted = extractNodePath(node);
        if (extracted.has_value()) {
            return extracted->generic_string();
        }
        return {};
    }

    static json parsePropertiesField(const std::string& properties) {
        try {
            return json::parse(properties);
        } catch (...) {
            return json(properties);
        }
    }

    json makeGraphNodeJson(
        const yams::daemon::GraphNode& node, bool includeDistance = false,
        const std::unordered_map<int64_t, std::string>* traversalHints = nullptr) const {
        json out;
        out["nodeId"] = node.nodeId;
        out["nodeKey"] = node.nodeKey;
        out["label"] = node.label;
        out["type"] = node.type;
        if (includeDistance) {
            out["distance"] = node.distance;
        }
        if (!node.documentHash.empty()) {
            out["documentHash"] = node.documentHash;
        }
        if (auto nodePath = displayNodePath(node); !nodePath.empty()) {
            out["path"] = nodePath;
        }
        if (traversalHints != nullptr) {
            if (auto hintIt = traversalHints->find(node.nodeId); hintIt != traversalHints->end()) {
                out["via"] = hintIt->second;
            }
        }
        if (!node.properties.empty()) {
            out["properties"] = parsePropertiesField(node.properties);
        }
        return out;
    }

    void renderNodePropertiesSection(const std::vector<yams::daemon::GraphNode>& nodes) const {
        bool hasProps = false;
        for (const auto& node : nodes) {
            if (!node.properties.empty()) {
                hasProps = true;
                break;
            }
        }
        if (!hasProps) {
            return;
        }

        std::cout << "\n" << yams::cli::ui::subsection_header("Node Properties") << "\n";
        for (const auto& node : nodes) {
            if (!node.properties.empty()) {
                std::cout << yams::cli::ui::bullet(node.label, 2) << "\n";
                std::cout << yams::cli::ui::indent(node.properties, 6) << "\n";
            }
        }
    }

    static std::string topologyInputKindLabel(yams::topology::TopologyInputKind kind) {
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

    static std::string topologyRoleLabel(yams::topology::DocumentTopologyRole role) {
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

    struct TopologyClusterStats {
        std::size_t scopedMemberCount{0};
        std::unordered_map<std::string, std::size_t> roleCounts;
    };

    struct TopologyReadContext {
        std::shared_ptr<metadata::ConnectionPool> connectionPool;
        std::shared_ptr<metadata::IMetadataRepository> metadataRepo;
        std::shared_ptr<metadata::KnowledgeGraphStore> kgStore;
    };

    Result<std::optional<TopologyReadContext>> buildTopologyReadContext() const {
        if (cli_ == nullptr) {
            return std::optional<TopologyReadContext>{std::nullopt};
        }

        if (auto repo = cli_->getMetadataRepository()) {
            TopologyReadContext ctx;
            ctx.metadataRepo = repo;
            ctx.kgStore = cli_->getKnowledgeGraphStore();
            return std::optional<TopologyReadContext>{std::move(ctx)};
        }

        const auto dbPath = cli_->getDataPath() / "yams.db";
        if (!std::filesystem::exists(dbPath)) {
            return std::optional<TopologyReadContext>{std::nullopt};
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

        TopologyReadContext ctx;
        ctx.connectionPool = pool;
        ctx.metadataRepo = std::make_shared<metadata::MetadataRepository>(*pool);

        metadata::KnowledgeGraphStoreConfig kgCfg;
        kgCfg.enable_alias_fts = true;
        kgCfg.enable_wal = true;
        auto kgStoreRes = metadata::makeSqliteKnowledgeGraphStore(dbPath.string(), kgCfg);
        if (kgStoreRes) {
            ctx.kgStore =
                std::shared_ptr<metadata::KnowledgeGraphStore>(std::move(kgStoreRes.value()));
        }
        return std::optional<TopologyReadContext>{std::move(ctx)};
    }

    Result<std::optional<yams::topology::TopologyArtifactBatch>> loadTopologySnapshot() const {
        auto ctxRes = buildTopologyReadContext();
        if (!ctxRes) {
            return ctxRes.error();
        }
        const auto& ctxOpt = ctxRes.value();
        if (!ctxOpt.has_value() || !ctxOpt->metadataRepo) {
            return std::optional<yams::topology::TopologyArtifactBatch>{std::nullopt};
        }

        yams::topology::MetadataKgTopologyArtifactStore store(ctxOpt->metadataRepo,
                                                              ctxOpt->kgStore);
        return store.loadLatest(topologySnapshotId_);
    }

    std::string resolveDocumentPathByHash(const std::string& hash) const {
        if (hash.empty() || cli_ == nullptr) {
            return {};
        }
        auto ctxRes = buildTopologyReadContext();
        if (!ctxRes || !ctxRes.value().has_value() || !ctxRes.value()->metadataRepo) {
            return {};
        }
        auto docRes = ctxRes.value()->metadataRepo->getDocumentByHash(hash);
        if (!docRes || !docRes.value().has_value()) {
            return {};
        }
        return docRes.value()->filePath;
    }

    static std::vector<std::string> splitPathSegments(const std::filesystem::path& path) {
        std::vector<std::string> segments;
        for (const auto& part : path) {
            auto s = part.generic_string();
            if (!s.empty() && s != "/") {
                segments.push_back(std::move(s));
            }
        }
        return segments;
    }

    static std::string projectPathForDisplay(const std::string& rawPath,
                                             const std::filesystem::path& cwd) {
        if (rawPath.empty()) {
            return {};
        }

        const auto normalized = yams::metadata::computePathDerivedValues(rawPath).normalizedPath;
        std::filesystem::path normalizedPath(normalized);
        std::error_code ec;
        auto rel = normalizedPath.lexically_relative(cwd);
        if (!rel.empty() && rel.native().find("..") != 0) {
            return rel.generic_string();
        }

        const auto cwdSegs = splitPathSegments(cwd.lexically_normal());
        const auto pathSegs = splitPathSegments(normalizedPath.lexically_normal());
        if (cwdSegs.empty() || pathSegs.empty()) {
            return normalized;
        }

        std::size_t bestLen = 0;
        std::size_t bestPathStart = 0;
        for (std::size_t len = std::min(cwdSegs.size(), pathSegs.size()); len >= 2; --len) {
            const std::size_t cwdStart = cwdSegs.size() - len;
            for (std::size_t pathStart = 0; pathStart + len <= pathSegs.size(); ++pathStart) {
                bool match = true;
                for (std::size_t i = 0; i < len; ++i) {
                    if (cwdSegs[cwdStart + i] != pathSegs[pathStart + i]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    bestLen = len;
                    bestPathStart = pathStart;
                    break;
                }
            }
            if (bestLen > 0) {
                break;
            }
            if (len == 2) {
                break;
            }
        }

        if (bestLen > 0) {
            std::filesystem::path projected;
            for (std::size_t i = bestPathStart + bestLen; i < pathSegs.size(); ++i) {
                projected /= pathSegs[i];
            }
            if (!projected.empty()) {
                return projected.generic_string();
            }
        }

        return normalized;
    }

    TopologyClusterStats
    collectTopologyClusterStats(const yams::topology::TopologyArtifactBatch& snapshot,
                                const yams::topology::ClusterArtifact& cluster,
                                const std::unordered_set<std::string>* scopedPaths = nullptr,
                                const std::filesystem::path* cwd = nullptr) const {
        TopologyClusterStats stats;
        for (const auto& membership : snapshot.memberships) {
            if (membership.clusterId != cluster.clusterId) {
                continue;
            }
            stats.roleCounts[topologyRoleLabel(membership.role)]++;

            bool include = true;
            if (scopedPaths != nullptr && cwd != nullptr) {
                std::string path = resolveDocumentPathByHash(membership.documentHash);
                if (path.empty()) {
                    include = false;
                } else {
                    auto normalized = normalizePath(std::filesystem::path(path), *cwd);
                    include = scopedPaths->count(normalized) > 0;
                }
            }
            if (include) {
                ++stats.scopedMemberCount;
            }
        }
        return stats;
    }

    static std::string
    formatRoleCounts(const std::unordered_map<std::string, std::size_t>& roleCounts) {
        return formatRelationCounts(roleCounts, 4);
    }

    Result<void> executeTopologySnapshots() {
        auto snapshotRes = loadTopologySnapshot();
        if (!snapshotRes) {
            return snapshotRes.error();
        }

        const auto& snapshotOpt = snapshotRes.value();
        if (!snapshotOpt.has_value()) {
            if (wantsJsonOutput()) {
                json out;
                out["snapshot"] = nullptr;
                out["requested_snapshot_id"] =
                    topologySnapshotId_.empty() ? json(nullptr) : json(topologySnapshotId_);
                std::cout << out.dump(2) << "\n";
            } else {
                std::cout << yams::cli::ui::section_header("Topology Snapshots") << "\n\n";
                std::cout << yams::cli::ui::status_info("No topology snapshot available") << "\n";
            }
            return Result<void>();
        }

        const auto& snapshot = *snapshotOpt;
        if (wantsJsonOutput()) {
            json out;
            out["snapshot"] = {{"snapshot_id", snapshot.snapshotId},
                               {"algorithm", snapshot.algorithm},
                               {"input_kind", topologyInputKindLabel(snapshot.inputKind)},
                               {"generated_at_unix_seconds", snapshot.generatedAtUnixSeconds},
                               {"topology_epoch", snapshot.topologyEpoch},
                               {"cluster_count", snapshot.clusters.size()},
                               {"membership_count", snapshot.memberships.size()}};
            std::cout << out.dump(2) << "\n";
            return Result<void>();
        }

        std::cout << yams::cli::ui::section_header("Topology Snapshots") << "\n\n";
        yams::cli::ui::Table table;
        table.headers = {"SNAPSHOT", "ALGORITHM", "INPUT", "CLUSTERS", "MEMBERSHIPS", "EPOCH"};
        table.has_header = true;
        table.add_row({snapshot.snapshotId, snapshot.algorithm.empty() ? "-" : snapshot.algorithm,
                       topologyInputKindLabel(snapshot.inputKind),
                       yams::cli::ui::format_number(snapshot.clusters.size()),
                       yams::cli::ui::format_number(snapshot.memberships.size()),
                       yams::cli::ui::format_number(snapshot.topologyEpoch)});
        yams::cli::ui::render_table(std::cout, table);

        if (verbose_) {
            std::cout << "\n"
                      << yams::cli::ui::key_value("Generated At (unix seconds)",
                                                  std::to_string(snapshot.generatedAtUnixSeconds))
                      << "\n";
            if (!topologySnapshotId_.empty()) {
                std::cout << yams::cli::ui::key_value("Requested Snapshot", topologySnapshotId_)
                          << "\n";
            }
        }
        return Result<void>();
    }

    Result<void> executeTopologyClusters() {
        auto snapshotRes = loadTopologySnapshot();
        if (!snapshotRes) {
            return snapshotRes.error();
        }

        const auto& snapshotOpt = snapshotRes.value();
        if (!snapshotOpt.has_value()) {
            if (wantsJsonOutput()) {
                json out;
                out["snapshot_id"] =
                    topologySnapshotId_.empty() ? json(nullptr) : json(topologySnapshotId_);
                out["clusters"] = json::array();
                std::cout << out.dump(2) << "\n";
            } else {
                std::cout << yams::cli::ui::section_header("Topology Clusters") << "\n\n";
                std::cout << yams::cli::ui::status_info("No topology snapshot available") << "\n";
            }
            return Result<void>();
        }

        const auto& snapshot = *snapshotOpt;
        std::unordered_set<std::string> scopedPaths;
        const auto cwd = std::filesystem::current_path();
        if (scopeToCwd_) {
            auto scopeRes = buildCurrentScopePathSet(cwd);
            if (!scopeRes) {
                return scopeRes.error();
            }
            scopedPaths = std::move(scopeRes.value());
        }
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

        if (wantsJsonOutput()) {
            json out;
            out["snapshot_id"] = snapshot.snapshotId;
            out["cluster_count"] = snapshot.clusters.size();
            json clustersJson = json::array();
            for (const auto* cluster : clusters) {
                auto stats = collectTopologyClusterStats(snapshot, *cluster,
                                                         scopeToCwd_ ? &scopedPaths : nullptr,
                                                         scopeToCwd_ ? &cwd : nullptr);
                std::string medoidPath;
                if (cluster->medoid.has_value()) {
                    medoidPath = !cluster->medoid->filePath.empty()
                                     ? cluster->medoid->filePath
                                     : resolveDocumentPathByHash(cluster->medoid->documentHash);
                    medoidPath = projectPathForDisplay(medoidPath, cwd);
                }
                json row;
                row["cluster_id"] = cluster->clusterId;
                row["level"] = cluster->level;
                row["member_count"] = cluster->memberCount;
                row["scoped_member_count"] = stats.scopedMemberCount;
                row["persistence_score"] = cluster->persistenceScore;
                row["cohesion_score"] = cluster->cohesionScore;
                row["bridge_mass"] = cluster->bridgeMass;
                row["role_summary"] = formatRoleCounts(stats.roleCounts);
                row["parent_cluster_id"] = cluster->parentClusterId.has_value()
                                               ? json(*cluster->parentClusterId)
                                               : json(nullptr);
                row["overlap_cluster_ids"] = cluster->overlapClusterIds;
                if (cluster->medoid.has_value()) {
                    row["medoid"] = {
                        {"document_hash", cluster->medoid->documentHash},
                        {"file_path", medoidPath},
                        {"representative_score", cluster->medoid->representativeScore}};
                }
                clustersJson.push_back(std::move(row));
            }
            out["clusters"] = std::move(clustersJson);
            std::cout << out.dump(2) << "\n";
            return Result<void>();
        }

        std::cout << yams::cli::ui::section_header("Topology Clusters") << "\n\n";
        std::cout << yams::cli::ui::status_info("Snapshot: " + snapshot.snapshotId) << "\n\n";

        yams::cli::ui::Table table;
        table.headers = {"CLUSTER", "LEVEL",    "MEMBERS", "SCOPED", "ROLES",
                         "PERSIST", "COHESION", "BRIDGE",  "MEDOID"};
        table.has_header = true;
        for (const auto* cluster : clusters) {
            auto stats = collectTopologyClusterStats(snapshot, *cluster,
                                                     scopeToCwd_ ? &scopedPaths : nullptr,
                                                     scopeToCwd_ ? &cwd : nullptr);
            std::string medoid = "-";
            if (cluster->medoid.has_value()) {
                medoid = !cluster->medoid->filePath.empty()
                             ? cluster->medoid->filePath
                             : resolveDocumentPathByHash(cluster->medoid->documentHash);
                medoid = projectPathForDisplay(medoid, cwd);
                if (medoid.empty()) {
                    medoid = cluster->medoid->documentHash;
                }
            }
            table.add_row(
                {yams::cli::ui::truncate_to_width(cluster->clusterId, 28),
                 std::to_string(cluster->level), yams::cli::ui::format_number(cluster->memberCount),
                 yams::cli::ui::format_number(stats.scopedMemberCount),
                 yams::cli::ui::truncate_to_width(formatRoleCounts(stats.roleCounts), 26),
                 yams::cli::ui::truncate_to_width(std::to_string(cluster->persistenceScore), 6),
                 yams::cli::ui::truncate_to_width(std::to_string(cluster->cohesionScore), 6),
                 yams::cli::ui::truncate_to_width(std::to_string(cluster->bridgeMass), 6),
                 yams::cli::ui::truncate_to_width(medoid, 38)});
        }
        yams::cli::ui::render_table(std::cout, table);

        if (verbose_) {
            std::cout << "\n"
                      << yams::cli::ui::status_info(
                             "Sorted by bridge mass, then persistence, then member count")
                      << "\n";
        }
        return Result<void>();
    }

    Result<void> executeTopologyClusterDetail() {
        auto snapshotRes = loadTopologySnapshot();
        if (!snapshotRes) {
            return snapshotRes.error();
        }

        const auto& snapshotOpt = snapshotRes.value();
        if (!snapshotOpt.has_value()) {
            return Error{ErrorCode::NotFound, "No topology snapshot available"};
        }

        const auto& snapshot = *snapshotOpt;
        auto clusterIt = std::find_if(
            snapshot.clusters.begin(), snapshot.clusters.end(),
            [&](const auto& cluster) { return cluster.clusterId == topologyClusterId_; });
        if (clusterIt == snapshot.clusters.end()) {
            return Error{ErrorCode::NotFound, "Topology cluster not found: " + topologyClusterId_};
        }

        const auto& cluster = *clusterIt;
        std::unordered_set<std::string> scopedPaths;
        const auto cwd = std::filesystem::current_path();
        if (scopeToCwd_) {
            auto scopeRes = buildCurrentScopePathSet(cwd);
            if (!scopeRes) {
                return scopeRes.error();
            }
            scopedPaths = std::move(scopeRes.value());
        }
        auto clusterStats = collectTopologyClusterStats(
            snapshot, cluster, scopeToCwd_ ? &scopedPaths : nullptr, scopeToCwd_ ? &cwd : nullptr);

        json membersJson = json::array();
        yams::cli::ui::Table membersTable;
        membersTable.headers = {"ROLE", "PATH", "HASH", "BRIDGE"};
        membersTable.has_header = true;
        std::size_t scopedMemberCount = 0;

        for (const auto& membership : snapshot.memberships) {
            if (membership.clusterId != cluster.clusterId) {
                continue;
            }

            std::string path = resolveDocumentPathByHash(membership.documentHash);
            path = projectPathForDisplay(path, cwd);
            bool include = true;
            if (scopeToCwd_) {
                if (path.empty()) {
                    include = false;
                } else {
                    auto normalized = normalizePath(
                        std::filesystem::path(resolveDocumentPathByHash(membership.documentHash)),
                        cwd);
                    include = scopedPaths.count(normalized) > 0;
                }
            }
            if (!include) {
                continue;
            }

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

        if (wantsJsonOutput()) {
            json out;
            out["snapshot_id"] = snapshot.snapshotId;
            out["cluster"] = {{"cluster_id", cluster.clusterId},
                              {"parent_cluster_id", cluster.parentClusterId.has_value()
                                                        ? json(*cluster.parentClusterId)
                                                        : json(nullptr)},
                              {"level", cluster.level},
                              {"member_count", cluster.memberCount},
                              {"scoped_member_count", scopedMemberCount},
                              {"role_summary", formatRoleCounts(clusterStats.roleCounts)},
                              {"role_counts", clusterStats.roleCounts},
                              {"persistence_score", cluster.persistenceScore},
                              {"cohesion_score", cluster.cohesionScore},
                              {"bridge_mass", cluster.bridgeMass},
                              {"overlap_cluster_ids", cluster.overlapClusterIds}};
            if (cluster.medoid.has_value()) {
                std::string medoidPath =
                    !cluster.medoid->filePath.empty()
                        ? cluster.medoid->filePath
                        : resolveDocumentPathByHash(cluster.medoid->documentHash);
                medoidPath = projectPathForDisplay(medoidPath, cwd);
                out["cluster"]["medoid"] = {
                    {"document_hash", cluster.medoid->documentHash},
                    {"file_path", medoidPath},
                    {"representative_score", cluster.medoid->representativeScore}};
            }
            out["scope_cwd"] = scopeToCwd_;
            out["members"] = std::move(membersJson);
            std::cout << out.dump(2) << "\n";
            return Result<void>();
        }

        std::cout << yams::cli::ui::section_header("Topology Cluster") << "\n\n";
        std::cout << yams::cli::ui::key_value("Snapshot", snapshot.snapshotId) << "\n";
        std::cout << yams::cli::ui::key_value("Cluster", cluster.clusterId) << "\n";
        if (cluster.parentClusterId.has_value()) {
            std::cout << yams::cli::ui::key_value("Parent", *cluster.parentClusterId) << "\n";
        }
        std::cout << yams::cli::ui::key_value("Level", std::to_string(cluster.level)) << "\n";
        std::cout << yams::cli::ui::key_value("Members", std::to_string(cluster.memberCount))
                  << "\n";
        if (scopeToCwd_) {
            std::cout << yams::cli::ui::key_value("Scoped Members",
                                                  std::to_string(scopedMemberCount))
                      << "\n";
        }
        std::cout << yams::cli::ui::key_value("Roles", formatRoleCounts(clusterStats.roleCounts))
                  << "\n";
        std::cout << yams::cli::ui::key_value("Persistence",
                                              std::to_string(cluster.persistenceScore))
                  << "\n";
        std::cout << yams::cli::ui::key_value("Cohesion", std::to_string(cluster.cohesionScore))
                  << "\n";
        std::cout << yams::cli::ui::key_value("Bridge", std::to_string(cluster.bridgeMass)) << "\n";
        if (!cluster.overlapClusterIds.empty()) {
            std::ostringstream os;
            for (std::size_t i = 0; i < cluster.overlapClusterIds.size(); ++i) {
                if (i)
                    os << ", ";
                os << cluster.overlapClusterIds[i];
            }
            std::cout << yams::cli::ui::key_value("Overlaps", os.str()) << "\n";
        }
        if (cluster.medoid.has_value()) {
            std::string medoidPath = !cluster.medoid->filePath.empty()
                                         ? cluster.medoid->filePath
                                         : resolveDocumentPathByHash(cluster.medoid->documentHash);
            medoidPath = projectPathForDisplay(medoidPath, cwd);
            std::cout << yams::cli::ui::key_value("Medoid Hash", cluster.medoid->documentHash)
                      << "\n";
            if (!medoidPath.empty()) {
                std::cout << yams::cli::ui::key_value("Medoid Path", medoidPath) << "\n";
            }
        }

        std::cout << "\n" << yams::cli::ui::subsection_header("Members") << "\n";
        if (scopedMemberCount == 0) {
            std::cout << yams::cli::ui::status_info(scopeToCwd_ ? "No members in current scope"
                                                                : "No members found")
                      << "\n";
        } else {
            yams::cli::ui::render_table(std::cout, membersTable);
        }
        if (scopeToCwd_) {
            std::cout << "\n"
                      << yams::cli::ui::status_info(std::string{kScopeToCwdDescription}) << "\n";
        }
        return Result<void>();
    }

    static std::string
    formatRelationCounts(const std::unordered_map<std::string, std::size_t>& counts,
                         std::size_t topLimit = 3) {
        if (counts.empty()) {
            return "-";
        }

        std::vector<std::pair<std::string, std::size_t>> sorted(counts.begin(), counts.end());
        std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
            if (a.second != b.second) {
                return a.second > b.second;
            }
            return a.first < b.first;
        });

        const std::size_t shown = std::min(topLimit, sorted.size());
        std::string out;
        for (std::size_t i = 0; i < shown; ++i) {
            if (!out.empty()) {
                out += ", ";
            }
            out += sorted[i].first + "(" + std::to_string(sorted[i].second) + ")";
        }
        if (sorted.size() > shown) {
            out += ", +" + std::to_string(sorted.size() - shown) + " more";
        }
        return out;
    }

    static std::unordered_map<int64_t, std::string>
    buildTraversalRelationHints(const yams::daemon::GraphQueryResponse& resp) {
        std::unordered_map<int64_t, std::string> hints;
        if (resp.connectedNodes.empty() || resp.edges.empty()) {
            return hints;
        }

        std::unordered_set<int64_t> connectedIds;
        connectedIds.reserve(resp.connectedNodes.size());

        std::unordered_map<int64_t, int32_t> distanceByNode;
        distanceByNode.reserve(resp.connectedNodes.size() + 1);
        for (const auto& node : resp.connectedNodes) {
            connectedIds.insert(node.nodeId);
            distanceByNode[node.nodeId] = node.distance;
        }
        distanceByNode[resp.originNode.nodeId] = 0;

        std::unordered_map<int64_t, std::unordered_map<std::string, std::size_t>> preferred;
        std::unordered_map<int64_t, std::unordered_map<std::string, std::size_t>> fallback;

        auto accumulate = [&](int64_t nodeId, int64_t otherNodeId,
                              const std::string& relationLabel) {
            if (connectedIds.find(nodeId) == connectedIds.end()) {
                return;
            }

            fallback[nodeId][relationLabel] += 1;

            auto distIt = distanceByNode.find(nodeId);
            auto otherDistIt = distanceByNode.find(otherNodeId);
            if (distIt == distanceByNode.end() || otherDistIt == distanceByNode.end()) {
                return;
            }
            if (distIt->second > 0 && otherDistIt->second == distIt->second - 1) {
                preferred[nodeId][relationLabel] += 1;
            }
        };

        for (const auto& edge : resp.edges) {
            const std::string relationLabel = edge.relation.empty() ? "edge" : edge.relation;
            accumulate(edge.srcNodeId, edge.dstNodeId, relationLabel);
            accumulate(edge.dstNodeId, edge.srcNodeId, relationLabel);
        }

        hints.reserve(resp.connectedNodes.size());
        for (const auto& node : resp.connectedNodes) {
            if (auto it = preferred.find(node.nodeId);
                it != preferred.end() && !it->second.empty()) {
                hints[node.nodeId] = formatRelationCounts(it->second);
                continue;
            }
            if (auto it = fallback.find(node.nodeId); it != fallback.end() && !it->second.empty()) {
                hints[node.nodeId] = formatRelationCounts(it->second);
                continue;
            }
            hints[node.nodeId] = "-";
        }

        return hints;
    }

    // yams-66h: List available node types with counts
    boost::asio::awaitable<Result<void>> executeListTypes() {
        using namespace yams::daemon;

        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        // Build GraphQueryRequest with listTypes mode
        GraphQueryRequest req;
        req.listTypes = true;

        auto r = co_await client.call(req);
        if (!r) {
            std::cerr << "Graph query error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();

        if (!resp.kgAvailable) {
            std::cout << yams::cli::ui::status_error("Knowledge graph not available") << "\n";
            co_return Result<void>();
        }

        if (wantsJsonOutput()) {
            json out;
            out["totalTypes"] = resp.nodeTypeCounts.size();
            json types = json::array();
            for (const auto& [type, count] : resp.nodeTypeCounts) {
                types.push_back({{"type", type}, {"count", count}});
            }
            out["nodeTypes"] = types;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Available Node Types") << "\n\n";

            if (resp.nodeTypeCounts.empty()) {
                std::cout << yams::cli::ui::status_info("No node types found in knowledge graph")
                          << "\n";
                std::cout << "\nHint: Add files with 'yams add <path>' to populate the graph.\n";
            } else {
                // Build table
                yams::cli::ui::Table table;
                table.headers = {"TYPE", "COUNT"};
                table.has_header = true;

                uint64_t totalNodes = 0;
                for (const auto& [type, count] : resp.nodeTypeCounts) {
                    table.add_row({type, yams::cli::ui::format_number(count)});
                    totalNodes += count;
                }

                yams::cli::ui::render_table(std::cout, table);

                std::cout << "\n"
                          << yams::cli::ui::status_info(
                                 "Total: " + yams::cli::ui::format_number(totalNodes) +
                                 " nodes across " + std::to_string(resp.nodeTypeCounts.size()) +
                                 " types")
                          << "\n";
                std::cout << "\nUsage: yams graph --list-type <type> to view nodes of a type\n";
            }
        }

        co_return Result<void>();
    }

    // yams-kt5t: List relation types with counts
    boost::asio::awaitable<Result<void>> executeListRelations() {
        using namespace yams::daemon;

        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        // Build GraphQueryRequest with listRelations mode
        GraphQueryRequest req;
        req.listRelations = true;

        auto r = co_await client.call(req);
        if (!r) {
            std::cerr << "Graph query error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();

        if (!resp.kgAvailable) {
            std::cout << yams::cli::ui::status_error("Knowledge graph not available") << "\n";
            co_return Result<void>();
        }

        if (wantsJsonOutput()) {
            json out;
            out["totalRelations"] = resp.relationTypeCounts.size();
            json relations = json::array();
            for (const auto& [relation, count] : resp.relationTypeCounts) {
                relations.push_back({{"relation", relation}, {"count", count}});
            }
            out["relationTypes"] = relations;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Available Relation Types") << "\n\n";

            if (resp.relationTypeCounts.empty()) {
                std::cout << yams::cli::ui::status_info("No relations found in knowledge graph")
                          << "\n";
                std::cout << "\nHint: Add files with 'yams add <path>' to populate the graph.\n";
            } else {
                // Build table
                yams::cli::ui::Table table;
                table.headers = {"RELATION", "COUNT"};
                table.has_header = true;

                uint64_t totalEdges = 0;
                for (const auto& [relation, count] : resp.relationTypeCounts) {
                    table.add_row({relation, yams::cli::ui::format_number(count)});
                    totalEdges += count;
                }

                yams::cli::ui::render_table(std::cout, table);

                std::cout << "\n"
                          << yams::cli::ui::status_info(
                                 "Total: " + yams::cli::ui::format_number(totalEdges) +
                                 " edges across " + std::to_string(resp.relationTypeCounts.size()) +
                                 " relation types")
                          << "\n";
                std::cout
                    << "\nUsage: yams graph --node-key <key> -r <relation> to filter by relation\n";
            }
        }

        co_return Result<void>();
    }

    // yams-kt5t: Search nodes by label pattern
    boost::asio::awaitable<Result<void>> executeSearch() {
        using namespace yams::daemon;

        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        // Build GraphQueryRequest with search mode
        GraphQueryRequest req;
        req.searchMode = true;
        req.searchPattern = searchPattern_;
        req.limit = static_cast<uint32_t>(limit_);
        req.offset = static_cast<uint32_t>(offset_);
        req.includeNodeProperties = verbose_;

        auto r = co_await client.call(req);
        if (!r) {
            std::cerr << "Graph search error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();

        if (!resp.kgAvailable) {
            std::cout << yams::cli::ui::status_error("Knowledge graph not available") << "\n";
            co_return Result<void>();
        }

        if (wantsJsonOutput()) {
            json out;
            out["pattern"] = searchPattern_;
            out["limit"] = limit_;
            out["offset"] = offset_;
            out["totalNodesFound"] = resp.totalNodesFound;
            out["truncated"] = resp.truncated;

            json jsonNodes = json::array();
            for (const auto& node : resp.connectedNodes) {
                jsonNodes.push_back(makeGraphNodeJson(node));
            }
            out["nodes"] = jsonNodes;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Search: " + searchPattern_) << "\n\n";

            std::string summary = "Found " + yams::cli::ui::format_number(resp.totalNodesFound) +
                                  " matching node" + (resp.totalNodesFound != 1 ? "s" : "");
            std::cout << yams::cli::ui::status_info(summary) << "\n";
            std::cout << "Showing: " << resp.connectedNodes.size() << " (offset " << offset_
                      << ", limit " << limit_ << ")\n\n";

            if (!resp.connectedNodes.empty()) {
                yams::cli::ui::Table table;
                table.headers = {"ID", "TYPE", "LABEL", "KEY"};
                table.has_header = true;

                for (const auto& node : resp.connectedNodes) {
                    table.add_row({std::to_string(node.nodeId), node.type.empty() ? "-" : node.type,
                                   yams::cli::ui::truncate_to_width(node.label, 35),
                                   yams::cli::ui::truncate_to_width(node.nodeKey, 40)});
                }
                yams::cli::ui::render_table(std::cout, table);
            }

            if (resp.truncated) {
                std::cout << "\n"
                          << yams::cli::ui::status_warning(
                                 "Results truncated. Use --limit and --offset for pagination.")
                          << "\n";
            }

            std::cout << "\nHint: Use wildcards in pattern: * (any chars), ? (single char)\n";
        }

        co_return Result<void>();
    }

    boost::asio::awaitable<Result<void>> executeListByType() {
        using namespace yams::daemon;

        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        // Build GraphQueryRequest with listByType mode
        GraphQueryRequest req;
        req.listByType = true;
        req.nodeType = listNodeType_;
        req.limit = static_cast<uint32_t>(limit_);
        req.offset = static_cast<uint32_t>(offset_);
        req.includeNodeProperties = verbose_;

        auto r = co_await client.call(req);
        if (!r) {
            std::cerr << "Graph query error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();
        std::vector<yams::daemon::GraphNode> nodes = resp.connectedNodes;
        const auto cwd = std::filesystem::current_path();
        if (scopeToCwd_) {
            auto res = buildCurrentScopePathSet(cwd);
            if (!res) {
                co_return res.error();
            }
            nodes = filterNodesToScopedPaths(std::move(nodes), res.value(), cwd);
        }

        if (wantsJsonOutput()) {
            json out;
            out["type"] = listNodeType_;
            out["limit"] = limit_;
            out["offset"] = offset_;
            out["totalNodesFound"] = resp.totalNodesFound;
            out["truncated"] = resp.truncated;
            out["scoped"] = scopeToCwd_;
            if (scopeToCwd_) {
                out["filteredCount"] = nodes.size();
                out["cwd"] = cwd.generic_string();
            }

            json jsonNodes = json::array();
            for (const auto& node : nodes) {
                jsonNodes.push_back(makeGraphNodeJson(node));
            }
            out["nodes"] = jsonNodes;
            std::cout << out.dump(2) << "\n";
        } else {
            // Table format using ui_helpers for uniform CLI output
            std::cout << yams::cli::ui::section_header("KG Nodes: " + listNodeType_) << "\n\n";

            // Summary line
            std::string summary = "Found " + yams::cli::ui::format_number(resp.totalNodesFound) +
                                  " node" + (resp.totalNodesFound != 1 ? "s" : "");
            std::cout << yams::cli::ui::status_info(summary) << "\n";
            if (scopeToCwd_) {
                std::cout << yams::cli::ui::status_info(std::string{kScopeToCwdDescription})
                          << "\n";
            }
            std::cout << "Showing: " << nodes.size() << " (offset " << offset_ << ", limit "
                      << limit_ << ")\n\n";

            // Build table
            yams::cli::ui::Table table;
            table.headers = {"ID", "LABEL", "KEY"};
            if (verbose_) {
                table.headers.push_back("HASH");
            }
            table.has_header = true;

            for (const auto& node : nodes) {
                std::vector<std::string> row;
                row.push_back(std::to_string(node.nodeId));
                row.push_back(yams::cli::ui::truncate_to_width(node.label, 40));
                row.push_back(yams::cli::ui::truncate_to_width(node.nodeKey, 30));
                if (verbose_) {
                    std::string hashDisplay =
                        node.documentHash.empty() ? "-" : node.documentHash.substr(0, 12) + "...";
                    row.push_back(hashDisplay);
                }
                table.add_row(row);
            }

            yams::cli::ui::render_table(std::cout, table);

            // Properties in verbose mode (after table)
            if (verbose_) {
                renderNodePropertiesSection(nodes);
            }

            if (resp.truncated) {
                std::cout << "\n"
                          << yams::cli::ui::status_warning(
                                 "Results truncated. Use --limit and --offset for pagination.")
                          << "\n";
            }
        }

        co_return Result<void>();
    }

    static std::string normalizePath(const std::filesystem::path& path,
                                     const std::filesystem::path& cwd) {
        std::filesystem::path normalized = path;
        if (normalized.is_relative()) {
            normalized = cwd / normalized;
        }
        normalized = normalized.lexically_normal();
        return normalized.generic_string();
    }

    static Result<std::unordered_set<std::string>>
    buildScopedPathSet(const std::filesystem::path& cwd,
                       const std::shared_ptr<metadata::IMetadataRepository>& repo) {
        if (!repo) {
            return Error{ErrorCode::NotInitialized, "Metadata repository not available"};
        }
        std::unordered_set<std::string> paths;
        auto addPrefix = [&](std::string_view prefix) -> Result<void> {
            auto res = repo->findDocumentsByPathTreePrefix(prefix, true, 0);
            if (!res) {
                return res.error();
            }
            for (const auto& doc : res.value()) {
                paths.insert(normalizePath(std::filesystem::path(doc.filePath), cwd));
            }
            return Result<void>();
        };

        for (const auto& prefix : {"src", "include"}) {
            auto r = addPrefix(prefix);
            if (!r) {
                return r.error();
            }
        }
        for (auto it = paths.begin(); it != paths.end();) {
            const auto& pathStr = *it;
            std::string_view ext;
            if (auto dot = pathStr.find_last_of('.'); dot != std::string::npos) {
                ext = std::string_view(pathStr).substr(dot + 1);
            }
            auto cat = yams::magic::getPruneCategory(pathStr, ext);
            if (cat == yams::magic::PruneCategory::GitArtifacts ||
                yams::magic::matchesPruneGroup(cat, "build") ||
                yams::magic::matchesPruneGroup(cat, "packages") ||
                yams::magic::matchesPruneGroup(cat, "ide-all")) {
                it = paths.erase(it);
                continue;
            }
            ++it;
        }
        return paths;
    }

    Result<std::unordered_set<std::string>>
    buildCurrentScopePathSet(const std::filesystem::path& cwd) const {
        auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
        if (!appCtx || !appCtx->metadataRepo) {
            return Error{ErrorCode::NotInitialized,
                         "Path tree scoping unavailable (metadata repo not ready)"};
        }
        return buildScopedPathSet(cwd, appCtx->metadataRepo);
    }

    static std::vector<yams::daemon::GraphNode>
    filterNodesToScopedPaths(std::vector<yams::daemon::GraphNode> nodes,
                             const std::unordered_set<std::string>& scopedPaths,
                             const std::filesystem::path& cwd) {
        std::vector<yams::daemon::GraphNode> filtered;
        filtered.reserve(nodes.size());
        for (const auto& node : nodes) {
            auto nodePathOpt = extractNodePath(node);
            if (!nodePathOpt.has_value()) {
                continue;
            }
            auto normalized = normalizePath(nodePathOpt.value(), cwd);
            if (scopedPaths.count(normalized) > 0) {
                filtered.push_back(node);
            }
        }
        return filtered;
    }

    static constexpr std::string_view kScopeToCwdDescription =
        "Scoped to src/** and include/** via path tree (excluding tests/, benchmarks/, "
        "third_party/, node_modules/, build*)";

    static std::optional<std::filesystem::path>
    extractNodePath(const yams::daemon::GraphNode& node) {
        if (node.nodeKey.rfind("path:file:", 0) == 0) {
            return std::filesystem::path(node.nodeKey.substr(10));
        }
        if (node.nodeKey.rfind("path:dir:", 0) == 0) {
            return std::filesystem::path(node.nodeKey.substr(9));
        }
        if (node.nodeKey.rfind("path:logical:", 0) == 0) {
            return std::filesystem::path(node.nodeKey.substr(13));
        }
        if (node.nodeKey.rfind("path:", 0) == 0) {
            auto secondColon = node.nodeKey.find(':', 5);
            if (secondColon != std::string::npos && secondColon + 1 < node.nodeKey.size()) {
                return std::filesystem::path(node.nodeKey.substr(secondColon + 1));
            }
        }
        auto at = node.nodeKey.rfind('@');
        if (at != std::string::npos && at + 1 < node.nodeKey.size()) {
            return std::filesystem::path(node.nodeKey.substr(at + 1));
        }
        if (!node.properties.empty()) {
            try {
                auto props = json::parse(node.properties);
                if (props.contains("file_path")) {
                    return std::filesystem::path(props["file_path"].get<std::string>());
                }
                if (props.contains("path")) {
                    return std::filesystem::path(props["path"].get<std::string>());
                }
            } catch (...) {
                return std::nullopt;
            }
        }
        return std::nullopt;
    }

    boost::asio::awaitable<Result<void>> executeGraphTraversal() {
        using namespace yams::daemon;

        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        // If using node-key or node-id, use GraphQueryRequest directly
        if (!nodeKey_.empty() || nodeId_ >= 0) {
            GraphQueryRequest gReq;
            gReq.nodeId = nodeId_;
            gReq.nodeKey = nodeKey_; // Daemon will resolve nodeKey to nodeId if needed
            gReq.maxDepth = depth_;
            gReq.maxResults = static_cast<uint32_t>(limit_);
            gReq.maxResultsPerDepth = 100;
            gReq.offset = static_cast<uint32_t>(offset_);
            gReq.limit = static_cast<uint32_t>(limit_);
            gReq.includeNodeProperties = verbose_;
            gReq.includeEdgeProperties = verbose_;

            if (!relationFilter_.empty()) {
                auto canonicalRelation = canonicalizeRelationName(relationFilter_);
                if (!canonicalRelation.empty()) {
                    gReq.relationFilters.push_back(std::move(canonicalRelation));
                }
            }

            auto r = co_await client.call(gReq);
            if (!r) {
                std::cerr << "Graph query error: " << r.error().message << "\n";
                co_return r.error();
            }

            // client.call<GraphQueryRequest> returns Result<GraphQueryResponse> directly
            co_return printGraphQueryResponse(r.value());
        }

        // If --name is provided, try to resolve it to a file node in the KG first
        if (!name_.empty()) {
            auto candidates = build_graph_file_node_candidates(name_);
            for (const auto& candidate : candidates) {
                std::string fileNodeKey = buildPathFileNodeKey(candidate);

                GraphQueryRequest gReq;
                gReq.nodeKey = fileNodeKey;
                gReq.maxDepth = depth_;
                gReq.maxResults = static_cast<uint32_t>(limit_);
                gReq.maxResultsPerDepth = 100;
                gReq.offset = static_cast<uint32_t>(offset_);
                gReq.limit = static_cast<uint32_t>(limit_);
                gReq.includeNodeProperties = verbose_;
                gReq.includeEdgeProperties = verbose_;

                if (!relationFilter_.empty()) {
                    auto canonicalRelation = canonicalizeRelationName(relationFilter_);
                    if (!canonicalRelation.empty()) {
                        gReq.relationFilters.push_back(std::move(canonicalRelation));
                    }
                }

                auto r = co_await client.call(gReq);
                if (r && r.value().kgAvailable && r.value().originNode.nodeId > 0) {
                    // Found the file node, show the KG graph response
                    co_return printGraphQueryResponse(r.value());
                }
            }
            // Fall through to document-based lookup if file node not found
        }

        // Fall back to document-based lookup via GetRequest
        GetRequest req;
        req.hash = hash_;
        req.name = name_;
        req.byName = !name_.empty();
        req.metadataOnly = true;
        req.showGraph = true;
        req.graphDepth = depth_;
        req.verbose = verbose_;

        auto r = co_await client.get(req);
        if (!r) {
            std::cerr << "Graph error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();
        co_return printDocumentGraphResponse(resp);
    }

    Result<void> printGraphQueryResponse(const yams::daemon::GraphQueryResponse& resp) {
        if (!resp.kgAvailable) {
            std::cout << yams::cli::ui::status_error(
                             "Knowledge graph not available" +
                             (resp.warning.empty() ? "" : ": " + resp.warning))
                      << "\n";
            return Result<void>();
        }

        const auto traversalHints = buildTraversalRelationHints(resp);

        if (wantsJsonOutput()) {
            json out;
            out["origin"] = {{"nodeId", resp.originNode.nodeId},
                             {"nodeKey", resp.originNode.nodeKey},
                             {"label", resp.originNode.label},
                             {"type", resp.originNode.type}};
            out["totalNodesFound"] = resp.totalNodesFound;
            out["totalEdgesTraversed"] = resp.totalEdgesTraversed;
            out["maxDepthReached"] = resp.maxDepthReached;
            out["truncated"] = resp.truncated;

            json nodes = json::array();
            for (const auto& node : resp.connectedNodes) {
                nodes.push_back(makeGraphNodeJson(node, true, &traversalHints));
            }
            out["connectedNodes"] = nodes;
            if (!resp.edges.empty()) {
                json edges = json::array();
                for (const auto& edge : resp.edges) {
                    json e;
                    e["edgeId"] = edge.edgeId;
                    e["srcNodeId"] = edge.srcNodeId;
                    e["dstNodeId"] = edge.dstNodeId;
                    e["relation"] = edge.relation;
                    e["weight"] = edge.weight;
                    if (!edge.properties.empty()) {
                        try {
                            e["properties"] = json::parse(edge.properties);
                        } catch (...) {
                            e["properties"] = edge.properties;
                        }
                    }
                    edges.push_back(e);
                }
                out["edges"] = edges;
            }
            std::cout << out.dump(2) << "\n";
        } else if (outputFormat_ == "dot") {
            // DOT format for graphviz visualization
            std::cout << "digraph G {\n";
            std::cout << "  rankdir=LR;\n";
            std::cout << "  node [shape=box];\n";

            std::unordered_map<int64_t, std::string> nodeKeyById;
            if (!resp.originNode.nodeKey.empty()) {
                nodeKeyById[resp.originNode.nodeId] = resp.originNode.nodeKey;
            }

            // Origin node
            std::cout << "  \"" << resp.originNode.nodeKey << "\" [label=\""
                      << resp.originNode.label << "\\n(" << resp.originNode.type
                      << ")\", style=filled, fillcolor=lightblue];\n";

            // Connected nodes and edges
            for (const auto& node : resp.connectedNodes) {
                std::cout << "  \"" << node.nodeKey << "\" [label=\"" << node.label << "\\n("
                          << node.type << ")\"];\n";
                if (!node.nodeKey.empty()) {
                    nodeKeyById[node.nodeId] = node.nodeKey;
                }
            }
            if (!resp.edges.empty()) {
                for (const auto& edge : resp.edges) {
                    auto srcIt = nodeKeyById.find(edge.srcNodeId);
                    auto dstIt = nodeKeyById.find(edge.dstNodeId);
                    std::string src =
                        srcIt != nodeKeyById.end() ? srcIt->second : std::to_string(edge.srcNodeId);
                    std::string dst =
                        dstIt != nodeKeyById.end() ? dstIt->second : std::to_string(edge.dstNodeId);
                    std::string label =
                        edge.relation.empty() ? "" : " [label=\"" + edge.relation + "\"]";
                    std::cout << "  \"" << src << "\" -> \"" << dst << "\"" << label << ";\n";
                }
            } else {
                for (const auto& node : resp.connectedNodes) {
                    std::cout << "  \"" << resp.originNode.nodeKey << "\" -> \"" << node.nodeKey
                              << "\";\n";
                }
            }
            std::cout << "}\n";
        } else {
            // Table format using ui_helpers
            std::cout << yams::cli::ui::section_header("Knowledge Graph Query") << "\n\n";

            // Origin node info
            std::cout << yams::cli::ui::colorize("Origin: ", yams::cli::ui::Ansi::BOLD)
                      << resp.originNode.label << " (" << resp.originNode.type << ")\n";
            std::cout << yams::cli::ui::key_value("  Node Key", resp.originNode.nodeKey) << "\n";
            std::cout << yams::cli::ui::key_value("  Node ID",
                                                  std::to_string(resp.originNode.nodeId))
                      << "\n\n";

            // Summary
            std::string summary =
                "Connected: " + yams::cli::ui::format_number(resp.connectedNodes.size()) + " of " +
                yams::cli::ui::format_number(resp.totalNodesFound) + " nodes, " +
                yams::cli::ui::format_number(resp.totalEdgesTraversed) + " edges";
            std::cout << yams::cli::ui::status_info(summary) << "\n\n";

            // Build table
            yams::cli::ui::Table table;
            table.headers = {"LABEL", "TYPE", "DIST", "VIA"};
            if (verbose_) {
                table.headers.push_back("PATH");
                table.headers.push_back("KEY");
                table.headers.push_back("HASH");
            }
            table.has_header = true;

            for (const auto& node : resp.connectedNodes) {
                std::vector<std::string> row;
                row.push_back(yams::cli::ui::truncate_to_width(node.label, 35));
                row.push_back(node.type.empty() ? "-" : node.type);
                row.push_back(std::to_string(node.distance));
                std::string viaDisplay = "-";
                if (auto it = traversalHints.find(node.nodeId); it != traversalHints.end()) {
                    viaDisplay = it->second;
                }
                row.push_back(yams::cli::ui::truncate_to_width(viaDisplay, 34));
                if (verbose_) {
                    auto nodePath = displayNodePath(node);
                    row.push_back(
                        nodePath.empty() ? "-" : yams::cli::ui::truncate_to_width(nodePath, 42));
                    row.push_back(yams::cli::ui::truncate_to_width(node.nodeKey, 25));
                    std::string hashDisplay =
                        node.documentHash.empty() ? "-" : node.documentHash.substr(0, 12) + "...";
                    row.push_back(hashDisplay);
                }
                table.add_row(row);
            }

            yams::cli::ui::render_table(std::cout, table);

            // Properties in verbose mode
            if (verbose_) {
                renderNodePropertiesSection(resp.connectedNodes);
            }

            if (resp.truncated) {
                std::cout << "\n"
                          << yams::cli::ui::status_warning(
                                 "Results truncated. Use --limit to see more.")
                          << "\n";
            }
        }

        return Result<void>();
    }

    Result<void> printDocumentGraphResponse(const yams::daemon::GetResponse& resp) {
        if (wantsJsonOutput()) {
            json out;
            out["document"] = resp.fileName;
            out["hash"] = resp.hash;
            out["graphEnabled"] = resp.graphEnabled;

            json related = json::array();
            for (const auto& rel : resp.related) {
                related.push_back({{"name", rel.name},
                                   {"hash", rel.hash},
                                   {"relationship", rel.relationship},
                                   {"distance", rel.distance}});
            }
            out["related"] = related;
            std::cout << out.dump(2) << "\n";
            return Result<void>();
        }

        // Table format using ui_helpers
        std::cout << yams::cli::ui::section_header("Knowledge Graph") << "\n\n";

        // Document info
        if (!resp.fileName.empty())
            std::cout << yams::cli::ui::key_value("Document", resp.fileName) << "\n";
        if (!resp.hash.empty()) {
            std::string hashDisplay =
                resp.hash.size() > 12 ? resp.hash.substr(0, 12) + "..." : resp.hash;
            std::cout << yams::cli::ui::key_value("Hash", hashDisplay) << "\n";
        }
        if (!resp.path.empty())
            std::cout << yams::cli::ui::key_value("Path", resp.path) << "\n";

        if (!resp.graphEnabled) {
            std::cout << "\n"
                      << yams::cli::ui::status_warning(
                             "Graph data unavailable (graph disabled or empty)")
                      << "\n";
            return Result<void>();
        }

        if (resp.related.empty()) {
            std::cout << "\n"
                      << yams::cli::ui::status_info("No related documents found at depth " +
                                                    std::to_string(depth_))
                      << "\n";
            return Result<void>();
        }

        std::cout << "\n";
        std::string relSummary = "Related Documents (depth " + std::to_string(depth_) + ")";
        std::cout << yams::cli::ui::colorize(relSummary, yams::cli::ui::Ansi::BOLD) << "\n\n";

        // Build table for related docs
        yams::cli::ui::Table table;
        table.headers = {"NAME", "RELATIONSHIP", "DIST", "HASH"};
        table.has_header = true;

        for (const auto& rel : resp.related) {
            std::string hashDisplay =
                rel.hash.size() > 8 ? rel.hash.substr(0, 8) + "..." : rel.hash;
            table.add_row({yams::cli::ui::truncate_to_width(rel.name, 40), rel.relationship,
                           std::to_string(rel.distance), hashDisplay});
        }

        yams::cli::ui::render_table(std::cout, table);

        return Result<void>();
    }

    YamsCLI* cli_{nullptr};
    std::string hash_;
    std::string name_;
    std::string nodeKey_;
    int64_t nodeId_{-1};
    std::string listNodeType_;
    std::string relationFilter_;
    int depth_{1};
    size_t limit_{100};
    size_t offset_{0};
    bool verbose_{false};
    bool jsonOutput_{false};
    std::string outputFormat_{"table"};
    std::string propFilter_;
    bool listTypes_{false};
    bool listRelations_{false};
    std::string searchPattern_;
    bool topologySnapshots_{false};
    bool topologyClusters_{false};
    std::string topologyClusterId_;
    std::string topologySnapshotId_;
    bool scopeToCwd_{false};
};

std::unique_ptr<ICommand> createGraphCommand() {
    return std::make_unique<GraphCommand>();
}

} // namespace yams::cli
