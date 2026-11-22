#include <catch2/catch_test_macros.hpp>

#include <yams/cli/graph_ingest_support.h>

using nlohmann::json;
using namespace yams;
using namespace yams::cli;

TEST_CASE("graph ingest payload accepts the MCP ingest shape", "[cli][graph][ingest]") {
    const auto parsed = parseGraphIngestPayloadText(R"({
        "action": "ingest",
        "nodes": [{"node_key": "paper:a", "type": "paper", "label": "A",
                   "properties": {"ns": "paperbridge"}},
                  {"node_key": "paper:b", "properties": "{\"ns\":\"paperbridge\"}"}],
        "edges": [{"src_node_key": "paper:a", "dst_node_key": "paper:b", "relation": "cites"}],
        "aliases": [{"node_key": "paper:a", "alias": "10.1/a", "source": "doi"}],
        "skip_existing_edges": true
    })");
    REQUIRE(parsed.has_value());
    const auto& req = parsed.value();
    REQUIRE(req.nodes.size() == 2);
    CHECK(req.nodes[0].properties == R"({"ns":"paperbridge"})");
    CHECK(req.nodes[1].properties == R"({"ns":"paperbridge"})");
    CHECK(req.nodes[1].type.empty());
    REQUIRE(req.edges.size() == 1);
    CHECK(req.edges[0].weight == 1.0f);
    REQUIRE(req.aliases.size() == 1);
    CHECK(req.aliases[0].confidence == 1.0f);
    CHECK_FALSE(req.skipExistingNodes);
    CHECK(req.skipExistingEdges);
}

TEST_CASE("graph ingest payload rejects malformed input with a located message",
          "[cli][graph][ingest]") {
    const auto reject = [](std::string_view text) {
        auto parsed = parseGraphIngestPayloadText(text);
        REQUIRE_FALSE(parsed.has_value());
        CHECK(parsed.error().code == ErrorCode::InvalidArgument);
        return parsed.error().message;
    };
    CHECK(reject("not json").find("not valid JSON") != std::string::npos);
    CHECK(reject("[]").find("JSON object") != std::string::npos);
    CHECK(reject("{}").find("at least one of") != std::string::npos);
    CHECK(reject(R"({"action":"remove","nodes":[]})").find("action") != std::string::npos);
    CHECK(reject(R"({"nodes":{}})").find("'nodes' must be an array") != std::string::npos);
    CHECK(reject(R"({"nodes":[{"type":"x"}]})").find("nodes[0]") != std::string::npos);
    CHECK(reject(R"({"edges":[{"src_node_key":"a","dst_node_key":"b"}]})").find("relation") !=
          std::string::npos);
    CHECK(reject(R"({"nodes":[{"node_key":"a","properties":[1]}]})").find("properties") !=
          std::string::npos);
    CHECK(reject(R"({"nodes":[{"node_key":"a","properties":"[1]"}]})").find("JSON object") !=
          std::string::npos);
    CHECK(reject(R"({"nodes":[],"skip_existing_nodes":"yes"})").find("boolean") !=
          std::string::npos);
}

TEST_CASE("graph ingest chunks keep nodes ahead of edges and aliases", "[cli][graph][ingest]") {
    daemon::KgIngestRequest req;
    for (int i = 0; i < 5; ++i) {
        req.nodes.push_back(daemon::KgIngestNode{"n" + std::to_string(i), "", "", ""});
    }
    for (int i = 0; i < 3; ++i) {
        req.edges.push_back(daemon::KgIngestEdge{"n0", "n1", "r" + std::to_string(i), 1.0f, ""});
    }
    req.aliases.push_back(daemon::KgIngestAlias{"n0", "alias", "", 1.0f});
    req.skipExistingNodes = true;

    const auto chunks = chunkGraphIngestRequest(req, 2);
    REQUIRE(chunks.size() == 6); // 3 node chunks, 2 edge chunks, 1 alias chunk
    for (std::size_t i = 0; i < 3; ++i) {
        CHECK_FALSE(chunks[i].nodes.empty());
        CHECK(chunks[i].edges.empty());
        CHECK(chunks[i].skipExistingNodes);
    }
    CHECK(chunks[2].nodes.size() == 1);
    CHECK(chunks[3].edges.size() == 2);
    CHECK(chunks[4].edges.size() == 1);
    CHECK(chunks[5].aliases.size() == 1);

    CHECK(chunkGraphIngestRequest(daemon::KgIngestRequest{}, 10).size() == 1);
}

TEST_CASE("graph ingest merges chunk responses and renders the MCP result keys",
          "[cli][graph][ingest]") {
    daemon::KgIngestResponse a;
    a.nodesInserted = 2;
    a.nodesUpdated = 1;
    daemon::KgIngestResponse b;
    b.edgesInserted = 3;
    b.edgesSkipped = 1;
    b.errors = {"Edge destination node not found: x"};
    b.unresolvedEdges.push_back(daemon::KgIngestUnresolvedEdge{"a", "x", "cites", false, true});
    b.success = true;

    const auto merged = mergeGraphIngestResponses({a, b});
    CHECK(merged.success);
    CHECK(merged.nodesInserted == 2);
    CHECK(merged.nodesUpdated == 1);
    CHECK(merged.edgesInserted == 3);
    REQUIRE(merged.unresolvedEdges.size() == 1);

    const auto j = graphIngestResponseToJson(merged);
    for (const auto* key :
         {"nodes_inserted", "nodes_updated", "nodes_skipped", "edges_inserted", "edges_updated",
          "edges_skipped", "aliases_inserted", "aliases_skipped", "unresolved_edges", "success"}) {
        CHECK(j.contains(key));
    }
    CHECK(j["unresolved_edges"][0]["dst_missing"] == true);

    b.success = false;
    CHECK_FALSE(mergeGraphIngestResponses({a, b}).success);

    const auto removed =
        graphRemoveResponseToJson(daemon::KgRemoveResponse{1, 2, true}, "paperbridge", "cites");
    CHECK(removed["nodes_removed"] == 1);
    CHECK(removed["edges_removed"] == 2);
    CHECK(removed["dry_run"] == true);
    CHECK(removed["relation"] == "cites");
}
