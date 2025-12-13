// MCP Search DTO round-trip tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>

#include <yams/mcp/tool_registry.h>

using nlohmann::json;

TEST_CASE("MCPSearchDto - Request includeDiff roundtrip", "[mcp][dto][search][catch2]") {
    yams::mcp::MCPSearchRequest req;
    req.query = "foo";
    req.limit = 5;
    req.pathPattern = "src/main.cpp";
    req.includeDiff = true;
    auto j = req.toJson();
    REQUIRE(j.contains("include_diff"));
    CHECK(j["include_diff"].get<bool>() == true);

    auto req2 = yams::mcp::MCPSearchRequest::fromJson(j);
    CHECK(req2.query == "foo");
    CHECK(req2.includeDiff == true);
    CHECK(req2.pathPattern == "src/main.cpp");
}

TEST_CASE("MCPSearchDto - Response diff optional fields", "[mcp][dto][search][catch2]") {
    json j = {{"total", 1},
              {"type", "daemon"},
              {"execution_time_ms", 12},
              {"results", json::array({json{{"id", "doc1"},
                                            {"path", "src/main.cpp"},
                                            {"score", 0.9},
                                            {"diff", json{{"added", json::array()},
                                                          {"removed", json::array()},
                                                          {"truncated", false}}},
                                            {"local_input_file", "/abs/src/main.cpp"}}})}};
    auto resp = yams::mcp::MCPSearchResponse::fromJson(j);
    REQUIRE(resp.total == 1u);
    REQUIRE(resp.results.size() == 1u);
    const auto& r = resp.results[0];
    REQUIRE(r.diff.has_value());
    REQUIRE(r.localInputFile.has_value());
    auto out = resp.toJson();
    CHECK(out["results"][0].contains("diff"));
    CHECK(out["results"][0].contains("local_input_file"));
}
