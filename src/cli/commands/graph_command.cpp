#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <yams/cli/command.h>
#include <yams/cli/daemon_helpers.h>
#include <yams/cli/ui_helpers.hpp>
#include <yams/cli/yams_cli.h>
#include <yams/daemon/client/daemon_client.h>
#include <yams/daemon/ipc/ipc_protocol.h>

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
                        "Filter edges by relation (e.g., CALLS, CONTAINS, IMPORTS)");

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

        cmd->callback([this]() { cli_->setPendingCommand(this); });
    }

    Result<void> execute() override { return Result<void>(); }

    boost::asio::awaitable<Result<void>> executeAsync() override {
        try {
            using namespace yams::daemon;

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

        if (jsonOutput_ || outputFormat_ == "json") {
            json out;
            out["type"] = listNodeType_;
            out["limit"] = limit_;
            out["offset"] = offset_;
            out["totalNodesFound"] = resp.totalNodesFound;
            out["truncated"] = resp.truncated;

            json nodes = json::array();
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
                    // Parse properties JSON string to include as object
                    try {
                        n["properties"] = json::parse(node.properties);
                    } catch (...) {
                        n["properties"] = node.properties; // fallback to raw string
                    }
                }
                nodes.push_back(n);
            }
            out["nodes"] = nodes;
            std::cout << out.dump(2) << "\n";
        } else {
            // Table format using ui_helpers for uniform CLI output
            std::cout << yams::cli::ui::section_header("KG Nodes: " + listNodeType_) << "\n\n";

            // Summary line
            std::string summary = "Found " + yams::cli::ui::format_number(resp.totalNodesFound) +
                                  " node" + (resp.totalNodesFound != 1 ? "s" : "");
            std::cout << yams::cli::ui::status_info(summary) << "\n";
            std::cout << "Showing: " << resp.connectedNodes.size() << " (offset " << offset_
                      << ", limit " << limit_ << ")\n\n";

            // Build table
            yams::cli::ui::Table table;
            table.headers = {"ID", "LABEL", "KEY"};
            if (verbose_) {
                table.headers.push_back("HASH");
            }
            table.has_header = true;

            for (const auto& node : resp.connectedNodes) {
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

        std::string relation = relationFilter_.empty() ? "calls" : relationFilter_;

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

        const auto& isolatedNodes = result.value().connectedNodes;

        if (jsonOutput_ || outputFormat_ == "json") {
            json out;
            out["type"] = listNodeType_;
            out["relation"] = relation;
            out["isolatedCount"] = isolatedNodes.size();

            json nodes = json::array();
            for (const auto& node : isolatedNodes) {
                json n;
                n["nodeId"] = node.nodeId;
                n["nodeKey"] = node.nodeKey;
                n["label"] = node.label;
                n["type"] = node.type;
                nodes.push_back(n);
            }
            out["isolatedNodes"] = nodes;
            std::cout << out.dump(2) << "\n";
        } else {
            std::cout << yams::cli::ui::section_header("Isolated " + listNodeType_ + " nodes") << "\n\n";
            std::cout << yams::cli::ui::status_info(
                "Found " + std::to_string(isolatedNodes.size()) + " isolated " + listNodeType_ +
                " nodes (no incoming " + relation + " edges)") << "\n\n";

            if (!isolatedNodes.empty()) {
                yams::cli::ui::Table table;
                table.headers = {"ID", "LABEL", "KEY"};
                table.has_header = true;

                for (const auto& node : isolatedNodes) {
                    table.add_row({
                        std::to_string(node.nodeId),
                        yams::cli::ui::truncate_to_width(node.label, 40),
                        yams::cli::ui::truncate_to_width(node.nodeKey, 50)
                    });
                }
                yams::cli::ui::render_table(std::cout, table);
            }
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
                gReq.relationFilters.push_back(relationFilter_);
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
            // Extract just the filename from the path for the node key
            std::filesystem::path namePath(name_);
            std::string fileName = namePath.filename().string();
            std::string fileNodeKey = "file:" + fileName;

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
                gReq.relationFilters.push_back(relationFilter_);
            }

            auto r = co_await client.call(gReq);
            if (r && r.value().kgAvailable && r.value().originNode.nodeId > 0) {
                // Found the file node, show the KG graph response
                co_return printGraphQueryResponse(r.value());
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
            std::cout << out.dump(2) << "\n";
        } else if (outputFormat_ == "dot") {
            // DOT format for graphviz visualization
            std::cout << "digraph G {\n";
            std::cout << "  rankdir=LR;\n";
            std::cout << "  node [shape=box];\n";

            // Origin node
            std::cout << "  \"" << resp.originNode.nodeKey << "\" [label=\""
                      << resp.originNode.label << "\\n(" << resp.originNode.type
                      << ")\", style=filled, fillcolor=lightblue];\n";

            // Connected nodes and edges
            for (const auto& node : resp.connectedNodes) {
                std::cout << "  \"" << node.nodeKey << "\" [label=\"" << node.label << "\\n("
                          << node.type << ")\"];\n";
                std::cout << "  \"" << resp.originNode.nodeKey << "\" -> \"" << node.nodeKey
                          << "\";\n";
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
};

std::unique_ptr<ICommand> createGraphCommand() {
    return std::make_unique<GraphCommand>();
}

} // namespace yams::cli
