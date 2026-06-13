#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <yams/app/services/graph_context_service.hpp>
#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/graph_explore_presenter.h>
#include <yams/cli/graph_helpers.h>
#include <yams/cli/graph_query_execution.h>
#include <yams/cli/graph_query_presenter.h>
#include <yams/cli/graph_scope_support.h>
#include <yams/cli/graph_topology_presenter.h>
#include <yams/cli/graph_topology_support.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
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
        cmd->add_option(
            "--explore", exploreQuery_,
            "Build agent-oriented graph context for a symbol, file, or natural-language query");
        cmd->add_option("--max-files", exploreMaxFiles_, "Maximum files to include for --explore")
            ->default_val(8);

        // Agent-oriented navigation ops (share --depth where relevant)
        cmd->add_option("--lookup", lookupSymbol_,
                        "Resolve a symbol definition (go-to-definition)");
        cmd->add_option("--at-file", lookupAtFile_,
                        "Disambiguate --lookup by file path substring");
        cmd->add_option("--impact", impactSymbol_,
                        "Show reverse dependents (blast radius) of a symbol");
        cmd->add_option("--trace", traceFrom_, "Trace a path from this symbol (use with --to)");
        cmd->add_option("--to", traceTo_, "Target symbol for --trace");
        cmd->add_option("--affected-tests", affectedTestsFiles_,
                        "Find tests affected by the given changed file(s)")
            ->expected(-1);
        cmd->add_option("--test-pattern", testPathPattern_,
                        "Path substring identifying test files for --affected-tests");

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
            invocationCwd_ = std::filesystem::current_path();

            if (!exploreQuery_.empty()) {
                co_return co_await executeGraphExplore();
            }

            if (!lookupSymbol_.empty()) {
                co_return co_await executeGraphLookup();
            }
            if (!impactSymbol_.empty()) {
                co_return co_await executeGraphImpact();
            }
            if (!traceFrom_.empty()) {
                co_return co_await executeGraphTrace();
            }
            if (!affectedTestsFiles_.empty()) {
                co_return co_await executeGraphAffectedTests();
            }

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
        std::cout << "Using in-process transport (socket daemon not ready)"
                  << "\n";
        if (!plan.fallbackReason.empty()) {
            std::cout << "  Reason: " << plan.fallbackReason << "\n";
        }
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
        } catch (const std::exception& e) {
            spdlog::trace("graph: treating node properties as raw JSON string: {}", e.what());
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

    struct GraphSearchGroup {
        std::string key;
        std::string label;
        std::string path;
        std::vector<const yams::daemon::GraphNode*> nodes;
        std::set<std::string> types;
        bool generatedOrCache{false};
    };

    static bool isGeneratedOrCachePath(std::string_view path) {
        return path.find("/.cache/") != std::string_view::npos ||
               path.find("/build/") != std::string_view::npos ||
               path.find("/public/search/") != std::string_view::npos ||
               path.find("/node_modules/") != std::string_view::npos ||
               path.find("/third_party/") != std::string_view::npos ||
               path.find("/.git/") != std::string_view::npos;
    }

    static std::string canonicalGraphSearchPath(const yams::daemon::GraphNode& node) {
        auto path = displayNodePath(node);
        if (path.empty()) {
            path = node.label;
        }
        if (path.empty()) {
            return {};
        }
        try {
            auto derived = yams::metadata::computePathDerivedValues(path);
            if (!derived.normalizedPath.empty()) {
                return derived.normalizedPath;
            }
        } catch (const std::exception& e) {
            spdlog::trace("graph: canonical path fallback for '{}': {}", path, e.what());
        } catch (...) {
            spdlog::trace("graph: canonical path fallback for '{}'", path);
        }
        return std::filesystem::path(path).lexically_normal().generic_string();
    }

    static std::vector<GraphSearchGroup>
    buildCanonicalGraphSearchGroups(const std::vector<yams::daemon::GraphNode>& nodes) {
        std::map<std::string, GraphSearchGroup> byKey;
        for (const auto& node : nodes) {
            auto key = canonicalGraphSearchPath(node);
            if (key.empty()) {
                key = node.nodeKey.empty() ? std::to_string(node.nodeId) : node.nodeKey;
            }
            auto& group = byKey[key];
            if (group.key.empty()) {
                group.key = key;
                group.path = displayNodePath(node);
                if (group.path.empty() && key.find('/') != std::string::npos) {
                    group.path = key;
                }
                group.label = group.path.empty() ? node.label : group.path;
                group.generatedOrCache = isGeneratedOrCachePath(key) ||
                                         isGeneratedOrCachePath(group.path) ||
                                         isGeneratedOrCachePath(node.label);
            }
            group.nodes.push_back(&node);
            if (!node.type.empty()) {
                group.types.insert(node.type);
            }
        }

        std::vector<GraphSearchGroup> groups;
        groups.reserve(byKey.size());
        for (auto& [_, group] : byKey) {
            groups.push_back(std::move(group));
        }
        return groups;
    }

    json makeGraphSearchGroupJson(const GraphSearchGroup& group,
                                  const std::filesystem::path& cwd) const {
        json out;
        out["key"] = group.key;
        out["label"] = group.label.find('/') == std::string::npos
                           ? group.label
                           : projectPathForCli(group.label, cwd);
        if (!group.path.empty()) {
            out["path"] = group.path;
        }
        out["nodeCount"] = group.nodes.size();
        out["generatedOrCache"] = group.generatedOrCache;
        out["types"] = json::array();
        for (const auto& type : group.types) {
            out["types"].push_back(type);
        }
        out["nodeIds"] = json::array();
        for (const auto* node : group.nodes) {
            out["nodeIds"].push_back(node->nodeId);
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

    GraphTopologySupport topologySupport() const {
        return GraphTopologySupport(cli_, topologySnapshotId_);
    }

    Result<void> executeTopologySnapshots() {
        auto support = topologySupport();
        auto snapshotRes = support.loadTopologySnapshot();
        if (!snapshotRes) {
            return snapshotRes.error();
        }

        TopologyCommandRenderOptions options;
        options.jsonOutput = wantsJsonOutput();
        options.verbose = verbose_;
        options.requestedSnapshotId = topologySnapshotId_;
        return renderTopologySnapshots(std::cout, snapshotRes.value(), options);
    }

    Result<void> executeTopologyClusters() {
        auto support = topologySupport();
        auto snapshotRes = support.loadTopologySnapshot();
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

        TopologyCommandRenderOptions options;
        options.jsonOutput = wantsJsonOutput();
        options.verbose = verbose_;
        options.scopeToCwd = scopeToCwd_;
        options.cwd = std::filesystem::current_path();
        return renderTopologyClusters(std::cout, *snapshotOpt, support, options);
    }

    Result<void> executeTopologyClusterDetail() {
        auto support = topologySupport();
        auto snapshotRes = support.loadTopologySnapshot();
        if (!snapshotRes) {
            return snapshotRes.error();
        }

        const auto& snapshotOpt = snapshotRes.value();
        if (!snapshotOpt.has_value()) {
            return Error{ErrorCode::NotFound, "No topology snapshot available"};
        }

        TopologyCommandRenderOptions options;
        options.jsonOutput = wantsJsonOutput();
        options.scopeToCwd = scopeToCwd_;
        options.cwd = std::filesystem::current_path();
        options.clusterId = topologyClusterId_;
        return renderTopologyClusterDetail(std::cout, *snapshotOpt, support, options);
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

        auto r = co_await executeGraphListTypesQuery(client);
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

        auto r = co_await executeGraphListRelationsQuery(client);
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

        auto r = co_await executeGraphSearchQuery(client,
                                                  GraphSearchQueryOptions{.pattern = searchPattern_,
                                                                          .limit = limit_,
                                                                          .offset = offset_,
                                                                          .verbose = verbose_});
        if (!r) {
            std::cerr << "Graph search error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();

        if (!resp.kgAvailable) {
            std::cout << yams::cli::ui::status_error("Knowledge graph not available") << "\n";
            co_return Result<void>();
        }

        const auto cwd = std::filesystem::current_path();
        const auto canonicalGroups = buildCanonicalGraphSearchGroups(resp.connectedNodes);

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
            json groups = json::array();
            for (const auto& group : canonicalGroups) {
                groups.push_back(makeGraphSearchGroupJson(group, cwd));
            }
            out["canonicalGroups"] = groups;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Search: " + searchPattern_) << "\n\n";

            std::string summary = "Found " + yams::cli::ui::format_number(resp.totalNodesFound) +
                                  " matching node" + (resp.totalNodesFound != 1 ? "s" : "");
            std::cout << yams::cli::ui::status_info(summary) << "\n";
            std::vector<const GraphSearchGroup*> visibleGroups;
            visibleGroups.reserve(canonicalGroups.size());
            std::size_t hiddenGroups = 0;
            for (const auto& group : canonicalGroups) {
                if (group.generatedOrCache) {
                    ++hiddenGroups;
                    continue;
                }
                visibleGroups.push_back(&group);
            }
            if (visibleGroups.empty()) {
                for (const auto& group : canonicalGroups) {
                    visibleGroups.push_back(&group);
                }
                hiddenGroups = 0;
            }

            std::cout << "Showing: " << visibleGroups.size() << " canonical group"
                      << (visibleGroups.size() != 1 ? "s" : "") << " from "
                      << resp.connectedNodes.size() << " node"
                      << (resp.connectedNodes.size() != 1 ? "s" : "") << " (offset " << offset_
                      << ", limit " << limit_ << ")";
            if (hiddenGroups > 0) {
                std::cout << " · hidden generated/cache groups: " << hiddenGroups;
            }
            std::cout << "\n\n";

            if (!resp.connectedNodes.empty()) {
                yams::cli::ui::Table table;
                table.headers = {"NODES", "TYPES", "LABEL", "KEY"};
                table.has_header = true;

                for (const auto* group : visibleGroups) {
                    std::string types;
                    for (const auto& type : group->types) {
                        if (!types.empty()) {
                            types += ",";
                        }
                        types += type;
                    }
                    if (types.empty()) {
                        types = "-";
                    }
                    table.add_row({std::to_string(group->nodes.size()),
                                   yams::cli::ui::truncate_to_width(types, 22),
                                   yams::cli::ui::truncate_to_width(
                                       group->label.find('/') == std::string::npos
                                           ? group->label
                                           : projectPathForCli(group->label, cwd),
                                       55),
                                   yams::cli::ui::truncate_to_width(group->key, 48)});
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

        auto r = co_await executeGraphListByTypeQuery(
            client, GraphListByTypeQueryOptions{.nodeType = listNodeType_,
                                                .limit = limit_,
                                                .offset = offset_,
                                                .verbose = verbose_});
        if (!r) {
            std::cerr << "Graph query error: " << r.error().message << "\n";
            co_return r.error();
        }

        const auto& resp = r.value();
        std::vector<yams::daemon::GraphNode> nodes = resp.connectedNodes;
        const auto cwd = std::filesystem::current_path();
        if (scopeToCwd_) {
            auto res = buildGraphCurrentScopePathSet(cli_, cwd);
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
                std::cout << yams::cli::ui::status_info(std::string{kGraphScopeToCwdDescription})
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
            auto normalized = normalizeGraphScopePath(nodePathOpt.value(), cwd);
            if (scopedPaths.count(normalized) > 0) {
                filtered.push_back(node);
            }
        }
        return filtered;
    }

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
            // Historical path nodes use path:<timestamp>Z:<absolute-path>. Prefer the delimiter
            // after the timestamp; a simple second-colon split truncates paths at HH:MM.
            auto timestampEnd = node.nodeKey.find("Z:", 5);
            if (timestampEnd != std::string::npos && timestampEnd + 2 < node.nodeKey.size()) {
                return std::filesystem::path(node.nodeKey.substr(timestampEnd + 2));
            }
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

        const GraphTraversalQueryOptions traversalOptions{.depth = depth_,
                                                          .limit = limit_,
                                                          .offset = offset_,
                                                          .verbose = verbose_,
                                                          .relationFilter = relationFilter_};

        if (!nodeKey_.empty() || nodeId_ >= 0) {
            auto r = co_await executeGraphTraversalByNode(
                client, traversalOptions, nodeKey_,
                nodeId_ >= 0 ? std::optional<int64_t>{nodeId_} : std::nullopt);
            if (!r) {
                std::cerr << "Graph query error: " << r.error().message << "\n";
                co_return r.error();
            }

            // client.call<GraphQueryRequest> returns Result<GraphQueryResponse> directly
            co_return printGraphQueryResponse(r.value());
        }

        if (!name_.empty()) {
            auto graphResp = co_await executeGraphTraversalByNameCandidates(
                client, traversalOptions, name_, invocationCwd_);
            if (!graphResp) {
                std::cerr << "Graph query error: " << graphResp.error().message << "\n";
                co_return graphResp.error();
            }
            if (graphResp.value().has_value()) {
                co_return printGraphQueryResponse(*graphResp.value());
            }
        }

        auto r = co_await executeDocumentGraphLookup(
            client, DocumentGraphLookupOptions{
                        .hash = hash_, .name = name_, .depth = depth_, .verbose = verbose_});
        if (!r) {
            std::cerr << "Graph error: " << r.error().message << "\n";
            if (!name_.empty() &&
                r.error().message.find("Document not found with name") != std::string::npos) {
                const auto displayName = projectPathForCli(name_, invocationCwd_);
                if (!displayName.empty()) {
                    std::cout << yams::cli::ui::status_info(
                                     "If this file is new, run: yams add \"" + displayName +
                                     "\" --sync")
                              << "\n";
                    std::cout << yams::cli::ui::status_info("Then retry: yams graph --name \"" +
                                                            displayName + "\" --depth " +
                                                            std::to_string(depth_))
                              << "\n";
                }
                if (auto searchHint = buildGraphSearchHint(name_, invocationCwd_);
                    !searchHint.empty()) {
                    std::cout << yams::cli::ui::status_info("Or explore graph labels with: " +
                                                            searchHint)
                              << "\n";
                }
            }
            co_return r.error();
        }

        const auto& resp = r.value();
        co_return printDocumentGraphResponse(resp);
    }

    Result<void> printGraphQueryResponse(const yams::daemon::GraphQueryResponse& resp) {
        return yams::cli::renderGraphQueryResponse(
            std::cout, resp,
            GraphQueryRenderOptions{.jsonOutput = wantsJsonOutput(),
                                    .verbose = verbose_,
                                    .outputFormat = outputFormat_,
                                    .cwd = invocationCwd_});
    }

    Result<void> printDocumentGraphResponse(const yams::daemon::GetResponse& resp) {
        return yams::cli::renderDocumentGraphResponse(
            std::cout, resp,
            DocumentGraphRenderOptions{
                .jsonOutput = wantsJsonOutput(), .depth = depth_, .cwd = invocationCwd_});
    }

    boost::asio::awaitable<Result<void>> executeGraphExplore() {
        const auto renderLocal = [&]() -> Result<void> {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (appCtx == nullptr) {
                return Error{ErrorCode::InvalidState, "CLI app context unavailable"};
            }
            auto service =
                app::services::makeGraphContextService(appCtx->kgStore, appCtx->metadataRepo);
            if (!service) {
                return Error{ErrorCode::InvalidState, "Graph context service unavailable"};
            }
            app::services::GraphExploreRequest localReq;
            localReq.query = exploreQuery_;
            localReq.budget.maxFiles = exploreMaxFiles_;
            auto localResult = service->explore(localReq);
            if (!localResult) {
                return localResult.error();
            }
            if (wantsJsonOutput()) {
                std::cout << yams::cli::makeGraphExploreJson(localResult.value()).dump(2) << "\n";
            } else {
                yams::cli::renderGraphExploreMarkdown(std::cout, localResult.value(),
                                                      invocationCwd_);
            }
            return Result<void>();
        };

        if (cli_ != nullptr && cli_->hasExplicitDataDir()) {
            co_return renderLocal();
        }

        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        daemon::GraphExploreRequest req;
        req.query = exploreQuery_;
        req.maxFiles = static_cast<uint64_t>(exploreMaxFiles_);

        auto result = co_await client.call(req);
        if (!result) {
            const auto& err = result.error();
            const bool daemonCompatibilityFailure =
                err.code == ErrorCode::InvalidData && err.message == "Unexpected response type";
            if (!yams::cli::is_transport_failure(err) && !daemonCompatibilityFailure) {
                co_return err;
            }
            spdlog::debug("graph explore daemon request failed; falling back to local service: {}",
                          err.message);
            co_return renderLocal();
        }

        auto appResponse = yams::cli::mapGraphExploreResponseFromDaemon(result.value());
        if (wantsJsonOutput()) {
            std::cout << yams::cli::makeGraphExploreJson(appResponse).dump(2) << "\n";
        } else {
            yams::cli::renderGraphExploreMarkdown(std::cout, appResponse, invocationCwd_);
        }
        co_return Result<void>();
    }

    template <typename Sym> static void appendSymbolJson(nlohmann::json& arr, const Sym& s) {
        nlohmann::json sj;
        sj["label"] = s.label;
        sj["qualifiedName"] = s.qualifiedName;
        sj["kind"] = s.kind;
        sj["filePath"] = s.filePath;
        if (s.startLine.has_value())
            sj["startLine"] = *s.startLine;
        if (s.endLine.has_value())
            sj["endLine"] = *s.endLine;
        arr.push_back(std::move(sj));
    }

    template <typename Sym> static void printSymbolLine(const Sym& s) {
        std::cout << "  " << s.qualifiedName << " [" << s.kind << "] " << s.filePath;
        if (s.startLine.has_value())
            std::cout << ":" << *s.startLine;
        std::cout << "\n";
    }

    template <typename Resp> void renderLookup(const Resp& resp) const {
        if (wantsJsonOutput()) {
            nlohmann::json j;
            j["symbol"] = resp.symbol;
            j["ambiguous"] = resp.ambiguous;
            j["matches"] = nlohmann::json::array();
            for (const auto& m : resp.matches)
                appendSymbolJson(j["matches"], m);
            j["trail"] = nlohmann::json::array();
            for (const auto& r : resp.trail)
                j["trail"].push_back({{"relation", r.relation},
                                      {"source", r.sourceLabel},
                                      {"target", r.targetLabel}});
            j["warnings"] = resp.warnings;
            std::cout << j.dump(2) << "\n";
            return;
        }
        if (resp.matches.empty()) {
            std::cout << "No symbol matching '" << resp.symbol << "' found\n";
        } else {
            if (resp.ambiguous)
                std::cout << resp.matches.size() << " matches (ambiguous):\n";
            for (const auto& m : resp.matches)
                printSymbolLine(m);
            if (!resp.trail.empty()) {
                std::cout << "Relationships:\n";
                for (const auto& r : resp.trail)
                    std::cout << "  " << r.sourceLabel << " --" << r.relation << "--> "
                              << r.targetLabel << "\n";
            }
        }
        for (const auto& w : resp.warnings)
            std::cout << "warning: " << w << "\n";
    }

    template <typename Resp> void renderImpact(const Resp& resp) const {
        if (wantsJsonOutput()) {
            nlohmann::json j;
            j["symbol"] = resp.symbol;
            j["truncated"] = resp.truncated;
            j["affectedSymbols"] = nlohmann::json::array();
            for (const auto& s : resp.affectedSymbols)
                appendSymbolJson(j["affectedSymbols"], s);
            j["warnings"] = resp.warnings;
            std::cout << j.dump(2) << "\n";
            return;
        }
        std::cout << "Impact of '" << resp.symbol << "': " << resp.affectedSymbols.size()
                  << " dependent symbol(s)\n";
        for (const auto& s : resp.affectedSymbols)
            printSymbolLine(s);
        if (resp.truncated)
            std::cout << "  (truncated)\n";
        for (const auto& w : resp.warnings)
            std::cout << "warning: " << w << "\n";
    }

    template <typename Resp> void renderTrace(const Resp& resp) const {
        if (wantsJsonOutput()) {
            nlohmann::json j;
            j["from"] = resp.from;
            j["to"] = resp.to;
            j["found"] = resp.found;
            j["path"] = nlohmann::json::array();
            for (const auto& r : resp.path)
                j["path"].push_back({{"relation", r.relation},
                                     {"source", r.sourceLabel},
                                     {"target", r.targetLabel}});
            j["warnings"] = resp.warnings;
            std::cout << j.dump(2) << "\n";
            return;
        }
        if (!resp.found) {
            std::cout << "No path found from '" << resp.from << "' to '" << resp.to << "'\n";
        } else {
            std::cout << "Path from '" << resp.from << "' to '" << resp.to << "' ("
                      << resp.path.size() << " hop(s)):\n";
            for (const auto& r : resp.path)
                std::cout << "  " << r.sourceLabel << " --" << r.relation << "--> "
                          << r.targetLabel << "\n";
        }
        for (const auto& w : resp.warnings)
            std::cout << "warning: " << w << "\n";
    }

    template <typename Resp> void renderAffectedTests(const Resp& resp) const {
        if (wantsJsonOutput()) {
            nlohmann::json j;
            j["changedFiles"] = resp.changedFiles;
            j["affectedTests"] = resp.affectedTests;
            j["warnings"] = resp.warnings;
            std::cout << j.dump(2) << "\n";
            return;
        }
        std::cout << resp.affectedTests.size() << " affected test file(s):\n";
        for (const auto& t : resp.affectedTests)
            std::cout << "  " << t << "\n";
        for (const auto& w : resp.warnings)
            std::cout << "warning: " << w << "\n";
    }

    boost::asio::awaitable<Result<void>> executeGraphLookup() {
        const auto renderLocal = [&]() -> Result<void> {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (appCtx == nullptr) {
                return Error{ErrorCode::InvalidState, "CLI app context unavailable"};
            }
            auto service =
                app::services::makeGraphContextService(appCtx->kgStore, appCtx->metadataRepo);
            if (!service) {
                return Error{ErrorCode::InvalidState, "Graph context service unavailable"};
            }
            app::services::GraphSymbolLookupRequest req;
            req.symbol = lookupSymbol_;
            if (!lookupAtFile_.empty()) {
                req.file = lookupAtFile_;
            }
            req.includeCode = verbose_;
            auto result = service->lookupSymbol(req);
            if (!result) {
                return result.error();
            }
            renderLookup(result.value());
            return Result<void>();
        };

        if (cli_ != nullptr && cli_->hasExplicitDataDir()) {
            co_return renderLocal();
        }
        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        daemon::GraphSymbolLookupRequest req;
        req.symbol = lookupSymbol_;
        if (!lookupAtFile_.empty()) {
            req.hasFile = true;
            req.file = lookupAtFile_;
        }
        req.includeCode = verbose_;
        auto result = co_await client.call(req);
        if (!result) {
            const auto& err = result.error();
            const bool daemonCompatibilityFailure =
                err.code == ErrorCode::InvalidData && err.message == "Unexpected response type";
            if (!yams::cli::is_transport_failure(err) && !daemonCompatibilityFailure) {
                co_return err;
            }
            co_return renderLocal();
        }
        renderLookup(result.value());
        co_return Result<void>();
    }

    boost::asio::awaitable<Result<void>> executeGraphImpact() {
        const auto renderLocal = [&]() -> Result<void> {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (appCtx == nullptr) {
                return Error{ErrorCode::InvalidState, "CLI app context unavailable"};
            }
            auto service =
                app::services::makeGraphContextService(appCtx->kgStore, appCtx->metadataRepo);
            if (!service) {
                return Error{ErrorCode::InvalidState, "Graph context service unavailable"};
            }
            app::services::GraphImpactRequest req;
            req.symbol = impactSymbol_;
            req.depth = static_cast<std::size_t>(depth_);
            auto result = service->impact(req);
            if (!result) {
                return result.error();
            }
            renderImpact(result.value());
            return Result<void>();
        };

        if (cli_ != nullptr && cli_->hasExplicitDataDir()) {
            co_return renderLocal();
        }
        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        daemon::GraphImpactRequest req;
        req.symbol = impactSymbol_;
        req.depth = static_cast<uint64_t>(depth_);
        auto result = co_await client.call(req);
        if (!result) {
            const auto& err = result.error();
            const bool daemonCompatibilityFailure =
                err.code == ErrorCode::InvalidData && err.message == "Unexpected response type";
            if (!yams::cli::is_transport_failure(err) && !daemonCompatibilityFailure) {
                co_return err;
            }
            co_return renderLocal();
        }
        renderImpact(result.value());
        co_return Result<void>();
    }

    boost::asio::awaitable<Result<void>> executeGraphTrace() {
        if (traceTo_.empty()) {
            co_return Error{ErrorCode::InvalidArgument, "--trace requires --to <symbol>"};
        }
        const auto maxDepth = static_cast<uint64_t>(depth_ > 1 ? depth_ : 6);
        const auto renderLocal = [&]() -> Result<void> {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (appCtx == nullptr) {
                return Error{ErrorCode::InvalidState, "CLI app context unavailable"};
            }
            auto service =
                app::services::makeGraphContextService(appCtx->kgStore, appCtx->metadataRepo);
            if (!service) {
                return Error{ErrorCode::InvalidState, "Graph context service unavailable"};
            }
            app::services::GraphTraceRequest req;
            req.from = traceFrom_;
            req.to = traceTo_;
            req.maxDepth = static_cast<std::size_t>(maxDepth);
            auto result = service->trace(req);
            if (!result) {
                return result.error();
            }
            renderTrace(result.value());
            return Result<void>();
        };

        if (cli_ != nullptr && cli_->hasExplicitDataDir()) {
            co_return renderLocal();
        }
        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        daemon::GraphTraceRequest req;
        req.from = traceFrom_;
        req.to = traceTo_;
        req.maxDepth = maxDepth;
        auto result = co_await client.call(req);
        if (!result) {
            const auto& err = result.error();
            const bool daemonCompatibilityFailure =
                err.code == ErrorCode::InvalidData && err.message == "Unexpected response type";
            if (!yams::cli::is_transport_failure(err) && !daemonCompatibilityFailure) {
                co_return err;
            }
            co_return renderLocal();
        }
        renderTrace(result.value());
        co_return Result<void>();
    }

    boost::asio::awaitable<Result<void>> executeGraphAffectedTests() {
        const auto depth = static_cast<uint64_t>(depth_ > 1 ? depth_ : 5);
        const auto renderLocal = [&]() -> Result<void> {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (appCtx == nullptr) {
                return Error{ErrorCode::InvalidState, "CLI app context unavailable"};
            }
            auto service =
                app::services::makeGraphContextService(appCtx->kgStore, appCtx->metadataRepo);
            if (!service) {
                return Error{ErrorCode::InvalidState, "Graph context service unavailable"};
            }
            app::services::GraphAffectedTestsRequest req;
            req.changedFiles = affectedTestsFiles_;
            req.depth = static_cast<std::size_t>(depth);
            req.testPathPattern = testPathPattern_;
            auto result = service->affectedTests(req);
            if (!result) {
                return result.error();
            }
            renderAffectedTests(result.value());
            return Result<void>();
        };

        if (cli_ != nullptr && cli_->hasExplicitDataDir()) {
            co_return renderLocal();
        }
        auto leaseRes = acquireGraphClientLease();
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        printFallbackNoticeIfNeeded(leaseHandle.plan);
        auto& client = **leaseHandle.lease;

        daemon::GraphAffectedTestsRequest req;
        req.changedFiles = affectedTestsFiles_;
        req.depth = depth;
        req.testPathPattern = testPathPattern_;
        auto result = co_await client.call(req);
        if (!result) {
            const auto& err = result.error();
            const bool daemonCompatibilityFailure =
                err.code == ErrorCode::InvalidData && err.message == "Unexpected response type";
            if (!yams::cli::is_transport_failure(err) && !daemonCompatibilityFailure) {
                co_return err;
            }
            co_return renderLocal();
        }
        renderAffectedTests(result.value());
        co_return Result<void>();
    }

    YamsCLI* cli_{nullptr};
    std::filesystem::path invocationCwd_ = std::filesystem::current_path();
    std::string hash_;
    std::string name_;
    std::string nodeKey_;
    int64_t nodeId_{-1};
    std::string listNodeType_;
    std::string relationFilter_;
    std::string exploreQuery_;
    std::size_t exploreMaxFiles_{8};
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
    std::string lookupSymbol_;
    std::string lookupAtFile_;
    std::string impactSymbol_;
    std::string traceFrom_;
    std::string traceTo_;
    std::vector<std::string> affectedTestsFiles_;
    std::string testPathPattern_;
};

std::unique_ptr<ICommand> createGraphCommand() {
    return std::make_unique<GraphCommand>();
}

} // namespace yams::cli
