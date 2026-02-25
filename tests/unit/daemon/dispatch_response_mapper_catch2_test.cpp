// SPDX-License-Identifier: GPL-3.0-or-later

#include <catch2/catch_test_macros.hpp>

#include <yams/app/services/graph_query_service.hpp>
#include <yams/app/services/services.hpp>
#include <yams/daemon/components/dispatch_response.hpp>

#include <string>
#include <unordered_set>

using namespace yams::app::services;

TEST_CASE("SearchResultMapper preserves service metadata", "[unit][daemon][mapper]") {
    SearchItem item;
    item.id = 42;
    item.title = "main.cpp";
    item.path = "/repo/main.cpp";
    item.hash = "abc123";
    item.score = 0.91;
    item.snippet = "snippet";
    item.metadata["relation_count"] = "3";
    item.metadata["relation_summary"] = "defines(2), has_version(1)";

    auto mapped = yams::daemon::dispatch::SearchResultMapper::fromServiceItem(item, false);

    REQUIRE(mapped.metadata.contains("relation_count"));
    CHECK(mapped.metadata.at("relation_count") == "3");
    REQUIRE(mapped.metadata.contains("relation_summary"));
    CHECK(mapped.metadata.at("relation_summary") == "defines(2), has_version(1)");

    REQUIRE(mapped.metadata.contains("hash"));
    CHECK(mapped.metadata.at("hash") == "abc123");
    REQUIRE(mapped.metadata.contains("path"));
    CHECK(mapped.metadata.at("path") == "/repo/main.cpp");
}

TEST_CASE("SearchResultMapper paths-only mode omits metadata", "[unit][daemon][mapper]") {
    SearchItem item;
    item.id = 7;
    item.title = "util.cpp";
    item.path = "/repo/util.cpp";
    item.hash = "def456";
    item.score = 0.77;
    item.metadata["relation_summary"] = "calls(1)";

    auto mapped = yams::daemon::dispatch::SearchResultMapper::fromServiceItem(item, true);
    CHECK(mapped.metadata.empty());
}

TEST_CASE("GraphQueryResponseMapper maps traversal node context", "[unit][daemon][mapper]") {
    yams::app::services::GraphConnectedNode connected;
    connected.nodeMetadata.node.nodeId = 7;
    connected.nodeMetadata.node.nodeKey = "symbol:main";
    connected.nodeMetadata.node.label = "main";
    connected.nodeMetadata.node.type = "function";
    connected.nodeMetadata.documentHash = "abc123";
    connected.nodeMetadata.documentPath = "/repo/src/main.cpp";
    connected.nodeMetadata.snapshotId = "snap-001";
    connected.distance = 2;

    yams::app::services::GraphEdgeDescriptor edge;
    edge.edgeId = 99;
    edge.srcNodeId = 4;
    edge.dstNodeId = 7;
    edge.relation = "calls";
    edge.weight = 1.0f;
    edge.properties =
        R"({"source":"treesitter","confidence":0.88,"provenance":{"source":"treesitter","confidence":0.88}})";
    connected.connectingEdges.push_back(edge);

    std::vector<yams::daemon::GraphNode> nodes;
    std::vector<yams::daemon::GraphEdge> edges;
    std::unordered_set<int64_t> seenEdges;

    yams::daemon::dispatch::GraphQueryResponseMapper::mapConnectedNodesAndEdges(connected, nodes,
                                                                                edges, seenEdges);

    REQUIRE(nodes.size() == 1);
    CHECK(nodes.front().documentHash == "abc123");
    CHECK(nodes.front().documentPath == "/repo/src/main.cpp");
    CHECK(nodes.front().snapshotId == "snap-001");
    CHECK(nodes.front().distance == 2);

    REQUIRE(edges.size() == 1);
    CHECK(edges.front().edgeId == 99);
    CHECK(edges.front().relation == "calls");
    CHECK(edges.front().properties.find("\"source\":\"treesitter\"") != std::string::npos);
}
