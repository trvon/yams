#include <yams/cli/graph_query_presenter.h>

#include <yams/cli/graph_helpers.h>
#include <yams/cli/ui_helpers.hpp>

#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <iostream>
#include <unordered_set>
#include <vector>

namespace yams::cli {

using json = nlohmann::json;

namespace {

json parsePropertiesField(const std::string& properties) {
    try {
        return json::parse(properties);
    } catch (const std::exception& e) {
        spdlog::trace("graph: treating node properties as raw JSON string: {}", e.what());
        return json(properties);
    }
}

struct VisibleNodeRow {
    const yams::daemon::GraphNode* node{nullptr};
    yams::cli::GraphNodeCliPresentation presentation;
};

} // namespace

std::optional<std::filesystem::path> extractGraphNodePath(const yams::daemon::GraphNode& node) {
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

std::string displayGraphNodePath(const yams::daemon::GraphNode& node) {
    if (!node.documentPath.empty()) {
        return node.documentPath;
    }
    auto extracted = extractGraphNodePath(node);
    if (extracted.has_value()) {
        return extracted->generic_string();
    }
    return {};
}

json makeGraphNodeJson(const yams::daemon::GraphNode& node, bool includeDistance,
                       const std::unordered_map<std::int64_t, std::string>* traversalHints) {
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
    if (auto nodePath = displayGraphNodePath(node); !nodePath.empty()) {
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

void renderGraphNodePropertiesSection(std::ostream& out,
                                      const std::vector<yams::daemon::GraphNode>& nodes) {
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

    out << "\n" << yams::cli::ui::subsection_header("Node Properties") << "\n";
    for (const auto& node : nodes) {
        if (!node.properties.empty()) {
            out << yams::cli::ui::bullet(node.label, 2) << "\n";
            out << yams::cli::ui::indent(node.properties, 6) << "\n";
        }
    }
}

std::unordered_map<std::int64_t, std::string>
buildTraversalRelationHints(const yams::daemon::GraphQueryResponse& resp) {
    std::unordered_map<std::int64_t, std::string> hints;
    if (resp.connectedNodes.empty() || resp.edges.empty()) {
        return hints;
    }

    std::unordered_set<std::int64_t> connectedIds;
    connectedIds.reserve(resp.connectedNodes.size());

    std::unordered_map<std::int64_t, std::int32_t> distanceByNode;
    distanceByNode.reserve(resp.connectedNodes.size() + 1);
    for (const auto& node : resp.connectedNodes) {
        connectedIds.insert(node.nodeId);
        distanceByNode[node.nodeId] = node.distance;
    }
    distanceByNode[resp.originNode.nodeId] = 0;

    std::unordered_map<std::int64_t, std::unordered_map<std::string, std::size_t>> preferred;
    std::unordered_map<std::int64_t, std::unordered_map<std::string, std::size_t>> fallback;

    auto accumulate = [&](std::int64_t nodeId, std::int64_t otherNodeId,
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
        if (auto it = preferred.find(node.nodeId); it != preferred.end() && !it->second.empty()) {
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

Result<void> renderGraphQueryResponse(std::ostream& out,
                                      const yams::daemon::GraphQueryResponse& resp,
                                      const GraphQueryRenderOptions& options) {
    if (!resp.kgAvailable) {
        out << yams::cli::ui::status_error("Knowledge graph not available" +
                                           (resp.warning.empty() ? "" : ": " + resp.warning))
            << "\n";
        return Result<void>();
    }

    const auto traversalHints = buildTraversalRelationHints(resp);

    if (options.jsonOutput) {
        json payload;
        payload["origin"] = {{"nodeId", resp.originNode.nodeId},
                             {"nodeKey", resp.originNode.nodeKey},
                             {"label", resp.originNode.label},
                             {"type", resp.originNode.type}};
        payload["totalNodesFound"] = resp.totalNodesFound;
        payload["totalEdgesTraversed"] = resp.totalEdgesTraversed;
        payload["maxDepthReached"] = resp.maxDepthReached;
        payload["truncated"] = resp.truncated;

        json nodes = json::array();
        for (const auto& node : resp.connectedNodes) {
            nodes.push_back(makeGraphNodeJson(node, true, &traversalHints));
        }
        payload["connectedNodes"] = std::move(nodes);
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
                edges.push_back(std::move(e));
            }
            payload["edges"] = std::move(edges);
        }
        out << payload.dump(2) << "\n";
        return Result<void>();
    }

    if (options.outputFormat == "dot") {
        out << "digraph G {\n";
        out << "  rankdir=LR;\n";
        out << "  node [shape=box];\n";

        std::unordered_map<std::int64_t, std::string> nodeKeyById;
        if (!resp.originNode.nodeKey.empty()) {
            nodeKeyById[resp.originNode.nodeId] = resp.originNode.nodeKey;
        }

        out << "  \"" << resp.originNode.nodeKey << "\" [label=\"" << resp.originNode.label
            << "\\n(" << resp.originNode.type << ")\", style=filled, fillcolor=lightblue];\n";

        for (const auto& node : resp.connectedNodes) {
            out << "  \"" << node.nodeKey << "\" [label=\"" << node.label << "\\n(" << node.type
                << ")\"];\n";
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
                out << "  \"" << src << "\" -> \"" << dst << "\"" << label << ";\n";
            }
        } else {
            for (const auto& node : resp.connectedNodes) {
                out << "  \"" << resp.originNode.nodeKey << "\" -> \"" << node.nodeKey << "\";\n";
            }
        }
        out << "}\n";
        return Result<void>();
    }

    out << yams::cli::ui::section_header("Knowledge Graph Query") << "\n\n";

    std::vector<VisibleNodeRow> visibleNodes;
    visibleNodes.reserve(resp.connectedNodes.size());
    const bool hasHighSignalNodes =
        std::any_of(resp.connectedNodes.begin(), resp.connectedNodes.end(), [&](const auto& node) {
            return !describeGraphNodeForCli(node, options.cwd).hideByDefault;
        });
    for (const auto& node : resp.connectedNodes) {
        auto presentation = describeGraphNodeForCli(node, options.cwd);
        if (!options.verbose && hasHighSignalNodes && presentation.hideByDefault) {
            continue;
        }
        visibleNodes.push_back(
            VisibleNodeRow{.node = &node, .presentation = std::move(presentation)});
    }
    std::stable_sort(visibleNodes.begin(), visibleNodes.end(), [](const auto& a, const auto& b) {
        if (a.presentation.sortRank != b.presentation.sortRank) {
            return a.presentation.sortRank < b.presentation.sortRank;
        }
        if (a.node->distance != b.node->distance) {
            return a.node->distance < b.node->distance;
        }
        return a.presentation.displayLabel < b.presentation.displayLabel;
    });

    out << yams::cli::ui::colorize("Origin: ", yams::cli::ui::Ansi::BOLD)
        << projectPathForCli(resp.originNode.label, options.cwd) << " (" << resp.originNode.type
        << ")\n";
    out << yams::cli::ui::key_value("  Node Key", resp.originNode.nodeKey) << "\n";
    out << yams::cli::ui::key_value("  Node ID", std::to_string(resp.originNode.nodeId)) << "\n\n";

    std::string summary = "Connected: " + yams::cli::ui::format_number(visibleNodes.size()) +
                          " of " + yams::cli::ui::format_number(resp.totalNodesFound) + " nodes, " +
                          yams::cli::ui::format_number(resp.totalEdgesTraversed) + " edges";
    out << yams::cli::ui::status_info(summary) << "\n";
    if (!options.verbose && hasHighSignalNodes &&
        visibleNodes.size() != resp.connectedNodes.size()) {
        out << yams::cli::ui::status_info("Showing high-signal relationships; rerun with --verbose "
                                          "for storage and structural nodes")
            << "\n";
    }
    out << "\n";

    yams::cli::ui::Table table;
    table.headers = {"LABEL", "TYPE", "DIST", "VIA"};
    if (options.verbose) {
        table.headers.push_back("PATH");
        table.headers.push_back("KEY");
        table.headers.push_back("HASH");
    }
    table.has_header = true;

    for (const auto& visible : visibleNodes) {
        const auto* node = visible.node;
        std::vector<std::string> row;
        row.push_back(yams::cli::ui::truncate_to_width(visible.presentation.displayLabel, 35));
        row.push_back(visible.presentation.displayType.empty() ? "-"
                                                               : visible.presentation.displayType);
        row.push_back(std::to_string(node->distance));
        std::string viaDisplay = "-";
        if (auto it = traversalHints.find(node->nodeId); it != traversalHints.end()) {
            viaDisplay = it->second;
        }
        row.push_back(yams::cli::ui::truncate_to_width(viaDisplay, 34));
        if (options.verbose) {
            auto nodePath = displayGraphNodePath(*node);
            row.push_back(nodePath.empty() ? "-"
                                           : yams::cli::ui::truncate_to_width(
                                                 projectPathForCli(nodePath, options.cwd), 42));
            row.push_back(yams::cli::ui::truncate_to_width(node->nodeKey, 25));
            std::string hashDisplay =
                node->documentHash.empty() ? "-" : node->documentHash.substr(0, 12) + "...";
            row.push_back(hashDisplay);
        }
        table.add_row(std::move(row));
    }

    if (visibleNodes.empty()) {
        out << yams::cli::ui::status_info("No high-signal relationships found. Rerun with "
                                          "--verbose to inspect storage and structural nodes")
            << "\n";
    } else {
        yams::cli::ui::render_table(out, table);
    }

    if (options.verbose) {
        renderGraphNodePropertiesSection(out, resp.connectedNodes);
    }

    if (resp.truncated) {
        out << "\n"
            << yams::cli::ui::status_warning("Results truncated. Use --limit to see more.") << "\n";
    }

    return Result<void>();
}

Result<void> renderDocumentGraphResponse(std::ostream& out, const yams::daemon::GetResponse& resp,
                                         const DocumentGraphRenderOptions& options) {
    if (options.jsonOutput) {
        json payload;
        payload["document"] = resp.fileName;
        payload["hash"] = resp.hash;
        payload["graphEnabled"] = resp.graphEnabled;

        json related = json::array();
        for (const auto& rel : resp.related) {
            related.push_back({{"name", rel.name},
                               {"hash", rel.hash},
                               {"relationship", rel.relationship},
                               {"distance", rel.distance}});
        }
        payload["related"] = std::move(related);
        out << payload.dump(2) << "\n";
        return Result<void>();
    }

    out << yams::cli::ui::section_header("Knowledge Graph") << "\n\n";

    if (!resp.fileName.empty()) {
        out << yams::cli::ui::key_value("Document", resp.fileName) << "\n";
    }
    if (!resp.hash.empty()) {
        std::string hashDisplay =
            resp.hash.size() > 12 ? resp.hash.substr(0, 12) + "..." : resp.hash;
        out << yams::cli::ui::key_value("Hash", hashDisplay) << "\n";
    }
    if (!resp.path.empty()) {
        out << yams::cli::ui::key_value("Path", projectPathForCli(resp.path, options.cwd)) << "\n";
    }

    if (!resp.graphEnabled) {
        out << "\n"
            << yams::cli::ui::status_warning("Graph data unavailable (graph disabled or not "
                                             "indexed yet). Retry after indexing completes.")
            << "\n";
        const auto displayPath = projectPathForCli(resp.path, options.cwd);
        if (!displayPath.empty()) {
            out << yams::cli::ui::status_info("If this file is new, run: yams add \"" +
                                              displayPath + "\" --sync")
                << "\n";
            out << yams::cli::ui::status_info("Then retry: yams graph --name \"" + displayPath +
                                              "\" --depth " + std::to_string(options.depth))
                << "\n";
        }
        if (auto searchHint =
                buildGraphSearchHint(resp.path.empty() ? resp.fileName : resp.path, options.cwd);
            !searchHint.empty()) {
            out << yams::cli::ui::status_info("Or explore graph labels with: " + searchHint)
                << "\n";
        }
        return Result<void>();
    }

    if (resp.related.empty()) {
        out << "\n"
            << yams::cli::ui::status_info("No related documents found at depth " +
                                          std::to_string(options.depth))
            << "\n";
        return Result<void>();
    }

    out << "\n";
    std::string relSummary = "Related Documents (depth " + std::to_string(options.depth) + ")";
    out << yams::cli::ui::colorize(relSummary, yams::cli::ui::Ansi::BOLD) << "\n\n";

    yams::cli::ui::Table table;
    table.headers = {"NAME", "RELATIONSHIP", "DIST", "HASH"};
    table.has_header = true;

    for (const auto& rel : resp.related) {
        std::string hashDisplay = rel.hash.size() > 8 ? rel.hash.substr(0, 8) + "..." : rel.hash;
        table.add_row({yams::cli::ui::truncate_to_width(rel.name, 40), rel.relationship,
                       std::to_string(rel.distance), hashDisplay});
    }

    yams::cli::ui::render_table(out, table);
    return Result<void>();
}

} // namespace yams::cli
