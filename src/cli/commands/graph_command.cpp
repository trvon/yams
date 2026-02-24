#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
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
#include <yams/metadata/metadata_repository.h>
#include <yams/metadata/path_utils.h>

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

        // Isolated node detection (nodes with no incoming edges of specified relation)
        cmd->add_flag("--isolated", showIsolated_,
                      "Find isolated nodes (no incoming edges for --relation type)");

        // Dead-code report (scoped isolated nodes with allowlist)
        cmd->add_flag("--dead-code-report", deadCodeReport_,
                      "Generate dead-code report for src/** (scoped isolated nodes)");
        cmd->add_flag("--scope-cwd", scopeToCwd_,
                      "Scope list/isolated results to src/** and include/** under CWD");

        // yams-66h: List available node types with counts
        cmd->add_flag("--list-types", listTypes_, "List available node types with counts");

        // yams-kt5t: List relation types with counts
        cmd->add_flag("--relations", listRelations_, "List available relation types with counts");

        // yams-kt5t: Search nodes by label pattern
        cmd->add_option("--search", searchPattern_,
                        "Search nodes by label pattern (supports * and ? wildcards)");

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

            if (deadCodeReport_) {
                co_return co_await executeDeadCodeReport();
            }

            // Handle --list-type mode (list nodes by type without traversal)
            if (!listNodeType_.empty()) {
                if (showIsolated_) {
                    co_return co_await executeIsolatedNodes();
                }
                co_return co_await executeListByType();
            }

            // Handle standard graph traversal
            co_return co_await executeGraphTraversal();

        } catch (const std::exception& e) {
            co_return Error{ErrorCode::Unknown, e.what()};
        }
    }

private:
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

    // yams-66h: List available node types with counts
    boost::asio::awaitable<Result<void>> executeListTypes() {
        using namespace yams::daemon;

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

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

        if (jsonOutput_ || outputFormat_ == "json") {
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

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

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

        if (jsonOutput_ || outputFormat_ == "json") {
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

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

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

        if (jsonOutput_ || outputFormat_ == "json") {
            json out;
            out["pattern"] = searchPattern_;
            out["limit"] = limit_;
            out["offset"] = offset_;
            out["totalNodesFound"] = resp.totalNodesFound;
            out["truncated"] = resp.truncated;

            json jsonNodes = json::array();
            for (const auto& node : resp.connectedNodes) {
                json n;
                n["nodeId"] = node.nodeId;
                n["nodeKey"] = node.nodeKey;
                n["label"] = node.label;
                n["type"] = node.type;
                if (!node.documentHash.empty()) {
                    n["documentHash"] = node.documentHash;
                }
                if (!node.properties.empty()) {
                    try {
                        n["properties"] = json::parse(node.properties);
                    } catch (...) {
                        n["properties"] = node.properties;
                    }
                }
                jsonNodes.push_back(n);
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

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

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
        std::optional<std::unordered_set<std::string>> scopedPaths;
        if (scopeToCwd_) {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (!appCtx || !appCtx->metadataRepo) {
                co_return Error{ErrorCode::NotInitialized,
                                "Path tree scoping unavailable (metadata repo not ready)"};
            }
            auto res = buildScopedPathSet(cwd, appCtx->metadataRepo);
            if (!res) {
                co_return res.error();
            }
            scopedPaths.emplace(std::move(res.value()));
        }
        if (scopeToCwd_ && scopedPaths.has_value()) {
            std::vector<yams::daemon::GraphNode> filtered;
            filtered.reserve(nodes.size());
            for (const auto& node : nodes) {
                auto nodePathOpt = extractNodePath(node);
                if (!nodePathOpt.has_value()) {
                    continue;
                }
                auto normalized = normalizePath(nodePathOpt.value(), cwd);
                if (scopedPaths->count(normalized) > 0) {
                    filtered.push_back(node);
                }
            }
            nodes.swap(filtered);
        }

        if (jsonOutput_ || outputFormat_ == "json") {
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
                json n;
                n["nodeId"] = node.nodeId;
                n["nodeKey"] = node.nodeKey;
                n["label"] = node.label;
                n["type"] = node.type;
                if (!node.documentHash.empty()) {
                    n["documentHash"] = node.documentHash;
                }
                if (!node.properties.empty()) {
                    // Parse properties JSON string to include as object
                    try {
                        n["properties"] = json::parse(node.properties);
                    } catch (...) {
                        n["properties"] = node.properties; // fallback to raw string
                    }
                }
                jsonNodes.push_back(n);
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
                std::cout << yams::cli::ui::status_info(
                                 "Scoped to src/** and include/** via path tree (excluding tests/, "
                                 "benchmarks/, third_party/, node_modules/, build*)")
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
                bool hasProps = false;
                for (const auto& node : nodes) {
                    if (!node.properties.empty()) {
                        hasProps = true;
                        break;
                    }
                }
                if (hasProps) {
                    std::cout << "\n"
                              << yams::cli::ui::subsection_header("Node Properties") << "\n";
                    for (const auto& node : nodes) {
                        if (!node.properties.empty()) {
                            std::cout << yams::cli::ui::bullet(node.label, 2) << "\n";
                            std::cout << yams::cli::ui::indent(node.properties, 6) << "\n";
                        }
                    }
                }
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

    boost::asio::awaitable<Result<void>> executeIsolatedNodes() {
        using namespace yams::daemon;

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        std::string relation =
            relationFilter_.empty() ? "calls" : canonicalizeRelationName(relationFilter_);
        if (relation.empty()) {
            relation = "calls";
        }

        // Use optimized isolated mode query (single SQL query instead of N+1)
        GraphQueryRequest req;
        req.isolatedMode = true;
        req.nodeType = listNodeType_;
        req.isolatedRelation = relation;
        req.limit = static_cast<uint32_t>(limit_ > 0 ? limit_ : 1000);

        auto result = co_await client.call(req);
        if (!result) {
            std::cerr << "Error querying isolated nodes: " << result.error().message << "\n";
            co_return result.error();
        }

        std::vector<yams::daemon::GraphNode> isolatedNodes = result.value().connectedNodes;
        const auto cwd = std::filesystem::current_path();
        std::optional<std::unordered_set<std::string>> scopedPaths;
        if (scopeToCwd_) {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (appCtx && appCtx->metadataRepo) {
                auto res = buildScopedPathSet(cwd, appCtx->metadataRepo);
                if (res) {
                    scopedPaths.emplace(std::move(res.value()));
                } else {
                    std::cerr << yams::cli::ui::status_warning("Path tree scoping unavailable: " +
                                                               res.error().message)
                              << "\n";
                }
            } else {
                std::cerr << yams::cli::ui::status_warning(
                                 "Path tree scoping unavailable (metadata repo not ready)")
                          << "\n";
            }
        }
        if (scopeToCwd_ && scopedPaths.has_value()) {
            std::vector<yams::daemon::GraphNode> filtered;
            filtered.reserve(isolatedNodes.size());
            for (const auto& node : isolatedNodes) {
                auto nodePathOpt = extractNodePath(node);
                if (!nodePathOpt.has_value()) {
                    continue;
                }
                auto normalized = normalizePath(nodePathOpt.value(), cwd);
                if (scopedPaths->count(normalized) > 0) {
                    filtered.push_back(node);
                }
            }
            isolatedNodes.swap(filtered);
        }

        if (jsonOutput_ || outputFormat_ == "json") {
            json out;
            out["type"] = listNodeType_;
            out["relation"] = relation;
            out["isolatedCount"] = isolatedNodes.size();
            out["scoped"] = scopeToCwd_;
            if (scopeToCwd_) {
                out["cwd"] = cwd.generic_string();
            }

            json jsonNodes = json::array();
            for (const auto& node : isolatedNodes) {
                json n;
                n["nodeId"] = node.nodeId;
                n["nodeKey"] = node.nodeKey;
                n["label"] = node.label;
                n["type"] = node.type;
                jsonNodes.push_back(n);
            }
            out["isolatedNodes"] = jsonNodes;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Isolated " + listNodeType_ + " nodes")
                      << "\n\n";
            std::cout << yams::cli::ui::status_info(
                             "Found " + std::to_string(isolatedNodes.size()) + " isolated " +
                             listNodeType_ + " nodes (no incoming " + relation + " edges)")
                      << "\n\n";
            if (scopeToCwd_) {
                std::cout << yams::cli::ui::status_info(
                                 "Scoped to src/** and include/** via path tree (excluding tests/, "
                                 "benchmarks/, third_party/, node_modules/, build*)")
                          << "\n\n";
            }

            if (!isolatedNodes.empty()) {
                yams::cli::ui::Table table;
                table.headers = {"ID", "LABEL", "KEY"};
                table.has_header = true;

                for (const auto& node : isolatedNodes) {
                    table.add_row({std::to_string(node.nodeId),
                                   yams::cli::ui::truncate_to_width(node.label, 40),
                                   yams::cli::ui::truncate_to_width(node.nodeKey, 50)});
                }
                yams::cli::ui::render_table(std::cout, table);
            }
        }

        co_return Result<void>();
    }

    struct DeadCodeTarget {
        std::string type;
        std::string relation;
        std::string label;
    };

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

    boost::asio::awaitable<Result<void>> executeDeadCodeReport() {
        using namespace yams::daemon;

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

        std::vector<DeadCodeTarget> targets = {
            {"function", "calls", "Isolated functions (no incoming calls)"},
            {"class", "calls", "Isolated classes (no incoming calls)"},
            {"struct", "calls", "Isolated structs (no incoming calls)"},
            {"file", "includes", "Isolated files (no incoming includes)"},
        };

        const auto cwd = std::filesystem::current_path();

        struct DeadCodeRow {
            std::string type;
            std::string label;
            std::string nodeKey;
            std::string path;
        };

        std::vector<DeadCodeRow> allRows;
        std::unordered_set<std::string> scopedPaths;
        {
            auto appCtx = cli_ ? cli_->getAppContext() : nullptr;
            if (!appCtx || !appCtx->metadataRepo) {
                co_return Error{ErrorCode::NotInitialized,
                                "Path tree scoping unavailable (metadata repo not ready)"};
            }
            auto res = buildScopedPathSet(cwd, appCtx->metadataRepo);
            if (!res) {
                co_return res.error();
            }
            scopedPaths = std::move(res.value());
        }

        for (const auto& target : targets) {
            GraphQueryRequest req;
            req.isolatedMode = true;
            req.nodeType = target.type;
            req.isolatedRelation = target.relation;
            req.limit = static_cast<uint32_t>(limit_ > 0 ? limit_ : 1000);
            req.includeNodeProperties = true;

            auto result = co_await client.call(req);
            if (!result) {
                std::cerr << "Error querying isolated nodes: " << result.error().message << "\n";
                co_return result.error();
            }

            for (const auto& node : result.value().connectedNodes) {
                auto nodePathOpt = extractNodePath(node);
                if (!nodePathOpt.has_value()) {
                    continue;
                }
                auto normalized = normalizePath(nodePathOpt.value(), cwd);
                if (scopedPaths.find(normalized) == scopedPaths.end()) {
                    continue;
                }
                std::error_code ec;
                auto rel = std::filesystem::relative(nodePathOpt.value(), cwd, ec);
                if (ec) {
                    rel = std::filesystem::path(normalized).lexically_relative(cwd);
                }
                DeadCodeRow row;
                row.type = target.type;
                row.label = node.label;
                row.nodeKey = node.nodeKey;
                row.path = rel.generic_string();
                allRows.push_back(std::move(row));
            }
        }

        if (jsonOutput_ || outputFormat_ == "json") {
            json out;
            out["cwd"] = cwd.generic_string();
            out["total"] = allRows.size();
            json rows = json::array();
            for (const auto& row : allRows) {
                rows.push_back({{"type", row.type},
                                {"label", row.label},
                                {"path", row.path},
                                {"nodeKey", row.nodeKey}});
            }
            out["nodes"] = rows;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Dead-code report") << "\n\n";
            std::cout << yams::cli::ui::status_info(
                             "Scoped to src/** and include/** via path tree (excluding tests/, "
                             "benchmarks/, third_party/, node_modules/, build*)")
                      << "\n\n";

            if (allRows.empty()) {
                std::cout << yams::cli::ui::status_info("No isolated nodes found in scope") << "\n";
                co_return Result<void>();
            }

            yams::cli::ui::Table table;
            table.headers = {"TYPE", "LABEL", "PATH"};
            table.has_header = true;
            for (const auto& row : allRows) {
                table.add_row({row.type, yams::cli::ui::truncate_to_width(row.label, 40),
                               yams::cli::ui::truncate_to_width(row.path, 50)});
            }
            yams::cli::ui::render_table(std::cout, table);
            std::cout << "\n"
                      << yams::cli::ui::status_info("Total: " +
                                                    yams::cli::ui::format_number(allRows.size()))
                      << "\n";
        }

        co_return Result<void>();
    }

    boost::asio::awaitable<Result<void>> executeGraphTraversal() {
        using namespace yams::daemon;

        ClientConfig cfg;
        if (cli_ && cli_->hasExplicitDataDir()) {
            cfg.dataDir = cli_->getDataPath();
        }
        cfg.requestTimeout = std::chrono::milliseconds(60000);

        auto leaseRes = yams::cli::acquire_cli_daemon_client_shared(cfg);
        if (!leaseRes) {
            co_return leaseRes.error();
        }
        auto leaseHandle = std::move(leaseRes.value());
        auto& client = **leaseHandle;

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

        if (jsonOutput_ || outputFormat_ == "json") {
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
                json n;
                n["nodeId"] = node.nodeId;
                n["nodeKey"] = node.nodeKey;
                n["label"] = node.label;
                n["type"] = node.type;
                n["distance"] = node.distance;
                if (!node.documentHash.empty()) {
                    n["documentHash"] = node.documentHash;
                }
                if (!node.properties.empty()) {
                    try {
                        n["properties"] = json::parse(node.properties);
                    } catch (...) {
                        n["properties"] = node.properties;
                    }
                }
                nodes.push_back(n);
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
                yams::cli::ui::format_number(resp.totalNodesFound) + " nodes";
            std::cout << yams::cli::ui::status_info(summary) << "\n\n";

            // Build table
            yams::cli::ui::Table table;
            table.headers = {"LABEL", "TYPE", "DIST"};
            if (verbose_) {
                table.headers.push_back("KEY");
                table.headers.push_back("HASH");
            }
            table.has_header = true;

            for (const auto& node : resp.connectedNodes) {
                std::vector<std::string> row;
                row.push_back(yams::cli::ui::truncate_to_width(node.label, 35));
                row.push_back(node.type.empty() ? "-" : node.type);
                row.push_back(std::to_string(node.distance));
                if (verbose_) {
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
                bool hasProps = false;
                for (const auto& node : resp.connectedNodes) {
                    if (!node.properties.empty()) {
                        hasProps = true;
                        break;
                    }
                }
                if (hasProps) {
                    std::cout << "\n"
                              << yams::cli::ui::subsection_header("Node Properties") << "\n";
                    for (const auto& node : resp.connectedNodes) {
                        if (!node.properties.empty()) {
                            std::cout << yams::cli::ui::bullet(node.label, 2) << "\n";
                            std::cout << yams::cli::ui::indent(node.properties, 6) << "\n";
                        }
                    }
                }
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
        if (jsonOutput_ || outputFormat_ == "json") {
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
    bool showIsolated_{false};
    bool deadCodeReport_{false};
    bool listTypes_{false};
    bool listRelations_{false};
    std::string searchPattern_;
    bool scopeToCwd_{false};
};

std::unique_ptr<ICommand> createGraphCommand() {
    return std::make_unique<GraphCommand>();
}

} // namespace yams::cli
