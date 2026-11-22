// End-to-end `yams graph ingest` / `yams graph remove` against a private embedded corpus.
#include "embedded_cli_fixture.h"

#include <catch2/catch_test_macros.hpp>

using nlohmann::json;
using yams::test::EmbeddedCorpus;

namespace {

json parseJson(const yams::test::CliRun& r) {
    auto parsed = json::parse(r.out, nullptr, false);
    INFO("stdout: " << r.out << "\nstderr: " << r.err);
    REQUIRE_FALSE(parsed.is_discarded());
    return parsed;
}

constexpr const char* kPayload = R"({
  "nodes": [
    {"node_key": "paper:a", "type": "paper", "label": "A", "properties": {"ns": "pb"}},
    {"node_key": "paper:b", "type": "paper", "label": "B", "properties": {"ns": "pb"}},
    {"node_key": "note:z", "type": "note", "label": "Z"}
  ],
  "edges": [
    {"src_node_key": "paper:a", "dst_node_key": "paper:b", "relation": "cites",
     "properties": {"ns": "pb"}},
    {"src_node_key": "paper:a", "dst_node_key": "paper:missing", "relation": "cites",
     "properties": {"ns": "pb"}},
    {"src_node_key": "note:z", "dst_node_key": "paper:a", "relation": "about"}
  ]
})";

} // namespace

TEST_CASE("graph ingest and remove round-trip through the CLI", "[cli][graph][ingest][e2e]") {
    EmbeddedCorpus corpus("yams_graph_ingest_cli_");

    auto first = corpus.run({"graph", "ingest", "--file", "-", "--json"}, kPayload);
    REQUIRE(first.rc == 0);
    auto j = parseJson(first);
    CHECK(j["success"] == true);
    CHECK(j["nodes_inserted"] == 3);
    CHECK(j["edges_inserted"] == 2);
    CHECK(j["chunks_committed"] == 2);
    CHECK(j["chunks_total"] == 2);
    REQUIRE(j["unresolved_edges"].size() == 1);
    CHECK(j["unresolved_edges"][0]["dst_node_key"] == "paper:missing");
    CHECK(j["ns_conflicts"].empty());

    auto again = corpus.run({"graph", "ingest", "-f", "-", "--json"}, kPayload);
    REQUIRE(again.rc == 0);
    j = parseJson(again);
    CHECK(j["nodes_updated"] == 3);
    CHECK(j["edges_updated"] == 2);

    // Taking over a row owned by nobody is refused and reported, not applied.
    auto takeover =
        corpus.run({"graph", "ingest", "-f", "-", "--json"},
                   R"({"nodes":[{"node_key":"note:z","type":"note","properties":{"ns":"pb"}}]})");
    REQUIRE(takeover.rc == 0);
    j = parseJson(takeover);
    CHECK(j["nodes_skipped"] == 1);
    REQUIRE(j["ns_conflicts"].size() == 1);
    CHECK(j["ns_conflicts"][0]["reason"] == "ns_change");

    auto dry = corpus.run({"graph", "remove", "--ns", "pb", "--dry-run", "--json"});
    REQUIRE(dry.rc == 0);
    j = parseJson(dry);
    CHECK(j["nodes_removed"] == 2);
    CHECK(j["foreign_edges_removed"] == 1);

    auto refused = corpus.run({"graph", "remove", "--ns", "pb", "--json"});
    CHECK(refused.rc != 0);
    j = parseJson(refused);
    CHECK(j["refused"] == true);
    CHECK(j["success"] == false);
    CHECK((refused.out + refused.err).find("--include-foreign-edges") != std::string::npos);

    auto removed =
        corpus.run({"graph", "remove", "--ns", "pb", "--include-foreign-edges", "--json"});
    REQUIRE(removed.rc == 0);
    j = parseJson(removed);
    CHECK(j["refused"] == false);
    CHECK(j["nodes_removed"] == 2);
    CHECK(j["edges_removed"] == 2);

    auto gone = corpus.run({"graph", "--node-key", "paper:a"});
    CHECK(gone.rc != 0);
    auto kept = corpus.run({"graph", "--node-key", "note:z"});
    CHECK(kept.rc == 0);
}

TEST_CASE("graph ingest rejects malformed namespaces and document_hash before writing",
          "[cli][graph][ingest][e2e]") {
    EmbeddedCorpus corpus("yams_graph_ingest_cli_bad_");
    for (const auto* payload : {R"({"nodes":[{"node_key":"a","properties":{"ns":""}}]})",
                                R"({"nodes":[{"node_key":"a","properties":{"ns":5}}]})",
                                R"({"nodes":[{"node_key":"a","properties":{"ns":{}}}]})",
                                R"({"nodes":[{"node_key":"a"}],"document_hash":"abc"})"}) {
        auto r = corpus.run({"graph", "ingest", "-f", "-"}, payload);
        INFO(payload << " -> " << r.out << r.err);
        CHECK(r.rc != 0);
    }
    auto none = corpus.run({"graph", "--node-key", "a"});
    CHECK(none.rc != 0);
}

TEST_CASE("graph ingest --strict fails on refused rows and unresolved edges",
          "[cli][graph][ingest][e2e]") {
    EmbeddedCorpus corpus("yams_graph_ingest_cli_strict_");
    constexpr const char* kUnresolved = R"({"ns":"pb","nodes":[{"node_key":"paper:s",
        "properties":{"ns":"pb"}}],"edges":[{"src_node_key":"paper:s",
        "dst_node_key":"paper:none","relation":"cites","properties":{"ns":"pb"}}]})";

    auto lenient = corpus.run({"graph", "ingest", "-f", "-"}, kUnresolved);
    CHECK(lenient.rc == 0);
    auto strict = corpus.run({"graph", "ingest", "-f", "-", "--strict", "--json"}, kUnresolved);
    CHECK(strict.rc != 0);
    auto j = parseJson(strict);
    CHECK(j["nodes_updated"] == 1); // the written rows stay written
    CHECK((strict.out + strict.err).find("strict") != std::string::npos);

    // An alias on a node another namespace owns is refused under the request's ns.
    auto alias = corpus.run({"graph", "ingest", "-f", "-", "--strict"},
                            R"({"ns":"other","aliases":[{"node_key":"paper:s","alias":"s"}]})");
    CHECK(alias.rc != 0);
}
