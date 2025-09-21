#include <nlohmann/json.hpp>
#include <gtest/gtest.h>

#include <yams/mcp/tool_registry.h>

using nlohmann::json;

TEST(MCPSearchDtoTest, RequestIncludeDiffRoundtrip) {
    yams::mcp::MCPSearchRequest req;
    req.query = "foo";
    req.limit = 5;
    req.pathPattern = "src/main.cpp";
    req.includeDiff = true;
    auto j = req.toJson();
    ASSERT_TRUE(j.contains("include_diff"));
    EXPECT_TRUE(j["include_diff"].get<bool>());

    auto req2 = yams::mcp::MCPSearchRequest::fromJson(j);
    EXPECT_EQ(req2.query, "foo");
    EXPECT_TRUE(req2.includeDiff);
    EXPECT_EQ(req2.pathPattern, "src/main.cpp");
}

TEST(MCPSearchDtoTest, ResponseDiffOptionalFields) {
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
    ASSERT_EQ(resp.total, 1u);
    ASSERT_EQ(resp.results.size(), 1u);
    const auto& r = resp.results[0];
    ASSERT_TRUE(r.diff.has_value());
    ASSERT_TRUE(r.localInputFile.has_value());
    auto out = resp.toJson();
    ASSERT_TRUE(out["results"][0].contains("diff"));
    ASSERT_TRUE(out["results"][0].contains("local_input_file"));
}
