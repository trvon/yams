// Copyright (c) 2025 YAMS Contributors
// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_query_presenter.h>

#include <sstream>

TEST_CASE("Graph query presenter renders graph responses across formats", "[cli][graph]") {
    yams::daemon::GraphQueryResponse response;
    response.kgAvailable = true;
    response.originNode.nodeId = 1;
    response.originNode.nodeKey = "function:demo::caller@/repo/src/caller.cpp";
    response.originNode.label = "/repo/src/caller.cpp";
    response.originNode.type = "function";
    response.totalNodesFound = 2;
    response.totalEdgesTraversed = 1;

    yams::daemon::GraphNode node;
    node.nodeId = 2;
    node.nodeKey = "function:demo::callee@/repo/src/callee.cpp";
    node.label = "callee";
    node.type = "function";
    node.distance = 1;
    node.documentHash = "abcdef1234567890";
    node.documentPath = "/repo/src/callee.cpp";
    node.properties = R"({"language":"cpp"})";
    response.connectedNodes.push_back(node);

    yams::daemon::GraphEdge edge;
    edge.edgeId = 42;
    edge.srcNodeId = 1;
    edge.dstNodeId = 2;
    edge.relation = "calls";
    edge.weight = 1.0f;
    response.edges.push_back(edge);

    std::ostringstream jsonOut;
    auto jsonResult = yams::cli::renderGraphQueryResponse(
        jsonOut, response, yams::cli::GraphQueryRenderOptions{.jsonOutput = true, .cwd = "/repo"});
    REQUIRE(jsonResult.has_value());
    CHECK(jsonOut.str().find("\"connectedNodes\"") != std::string::npos);
    CHECK(jsonOut.str().find("\"via\": \"calls(1)\"") != std::string::npos);

    std::ostringstream tableOut;
    auto tableResult = yams::cli::renderGraphQueryResponse(
        tableOut, response, yams::cli::GraphQueryRenderOptions{.verbose = true, .cwd = "/repo"});
    REQUIRE(tableResult.has_value());
    CHECK(tableOut.str().find("Knowledge Graph Query") != std::string::npos);
    CHECK(tableOut.str().find("src/callee.cpp") != std::string::npos);
    CHECK(tableOut.str().find("Node Properties") != std::string::npos);

    std::ostringstream dotOut;
    auto dotResult = yams::cli::renderGraphQueryResponse(
        dotOut, response,
        yams::cli::GraphQueryRenderOptions{.outputFormat = "dot", .cwd = "/repo"});
    REQUIRE(dotResult.has_value());
    CHECK(dotOut.str().find("digraph G") != std::string::npos);
    CHECK(dotOut.str().find("calls") != std::string::npos);
}

TEST_CASE("Graph query presenter renders document graph fallbacks and related docs",
          "[cli][graph]") {
    yams::daemon::GetResponse disabled;
    disabled.fileName = "src/new_file.cpp";
    disabled.path = "/repo/src/new_file.cpp";
    disabled.hash = "1234567890abcdef";
    disabled.graphEnabled = false;

    std::ostringstream disabledOut;
    auto disabledResult = yams::cli::renderDocumentGraphResponse(
        disabledOut, disabled, yams::cli::DocumentGraphRenderOptions{.depth = 2, .cwd = "/repo"});
    REQUIRE(disabledResult.has_value());
    CHECK(disabledOut.str().find("yams add \"src/new_file.cpp\" --sync") != std::string::npos);
    CHECK(disabledOut.str().find("yams graph --search \"*new_file*\"") != std::string::npos);

    yams::daemon::GetResponse related;
    related.fileName = "src/main.cpp";
    related.path = "/repo/src/main.cpp";
    related.hash = "fedcba0987654321";
    related.graphEnabled = true;
    yams::daemon::RelatedDocumentEntry rel;
    rel.hash = "abcd1234efgh5678";
    rel.name = "src/helper.cpp";
    rel.relationship = "calls";
    rel.distance = 1;
    related.related.push_back(rel);

    std::ostringstream relatedOut;
    auto relatedResult = yams::cli::renderDocumentGraphResponse(
        relatedOut, related, yams::cli::DocumentGraphRenderOptions{.depth = 2, .cwd = "/repo"});
    REQUIRE(relatedResult.has_value());
    CHECK(relatedOut.str().find("Related Documents (depth 2)") != std::string::npos);
    CHECK(relatedOut.str().find("src/helper.cpp") != std::string::npos);
}
