// MCP DTO roundtrip tests for search/grep/get response ergonomics

#include <catch2/catch_test_macros.hpp>

#include <yams/mcp/tool_registry.h>

using yams::mcp::json;

TEST_CASE("MCP DTO - SearchResponse includes anchors and truncation markers",
          "[mcp][dto][search][anchors][catch2]") {
    yams::mcp::MCPSearchResponse resp;
    resp.total = 1;
    resp.type = "daemon";

    yams::mcp::MCPSearchResponse::Result r;
    r.id = "doc-1";
    r.path = "src/mcp/mcp_server.cpp";
    r.score = 0.91F;
    r.snippet = "registerTool<MCPGraphRequest, MCPGraphResponse>(...)";
    r.lineStart = 4241;
    r.lineEnd = 4245;
    r.charStart = 8;
    r.charEnd = 64;
    r.snippetTruncated = true;
    resp.results.push_back(r);

    json j = resp.toJson();
    REQUIRE(j.contains("results"));
    REQUIRE(j["results"].is_array());
    REQUIRE(j["results"].size() == 1);

    const auto& out = j["results"][0];
    CHECK(out["line_start"] == 4241);
    CHECK(out["line_end"] == 4245);
    CHECK(out["char_start"] == 8);
    CHECK(out["char_end"] == 64);
    CHECK(out["snippet_truncated"].get<bool>());

    auto back = yams::mcp::MCPSearchResponse::fromJson(j);
    REQUIRE(back.results.size() == 1);
    REQUIRE(back.results[0].lineStart.has_value());
    CHECK(back.results[0].lineStart.value() == 4241);
    CHECK(back.results[0].snippetTruncated);
}

TEST_CASE("MCP DTO - GrepResponse preserves structured matches",
          "[mcp][dto][grep][matches][catch2]") {
    yams::mcp::MCPGrepResponse resp;
    resp.output = "src/mcp/mcp_server.cpp:4241: registerTool<...>";
    resp.matchCount = 1;
    resp.fileCount = 1;
    resp.outputTruncated = true;
    resp.outputMaxBytes = 16384;

    yams::mcp::MCPGrepResponse::Match m;
    m.file = "src/mcp/mcp_server.cpp";
    m.lineNumber = 4241;
    m.lineText = "toolRegistry_->registerTool<MCPGraphRequest, MCPGraphResponse>(...)";
    m.contextBefore = {"...", "// Graph tool"};
    m.contextAfter = {"..."};
    m.matchType = "regex";
    m.confidence = 1.0;
    m.matchId = "src/mcp/mcp_server.cpp:4241:1";
    m.fileMatches = 16;
    resp.matches.push_back(m);

    json j = resp.toJson();
    REQUIRE(j.contains("matches"));
    REQUIRE(j["matches"].is_array());
    REQUIRE(j["matches"].size() == 1);
    CHECK(j["output_truncated"].get<bool>());
    CHECK(j["output_max_bytes"] == 16384);

    auto back = yams::mcp::MCPGrepResponse::fromJson(j);
    REQUIRE(back.matches.size() == 1);
    CHECK(back.matches[0].file == "src/mcp/mcp_server.cpp");
    CHECK(back.matches[0].lineNumber == 4241);
    CHECK(back.matches[0].fileMatches == 16);
    CHECK(back.outputTruncated);
}

TEST_CASE("MCP DTO - RetrieveDocumentResponse includes content truncation metadata",
          "[mcp][dto][get][truncation][catch2]") {
    yams::mcp::MCPRetrieveDocumentResponse resp;
    resp.hash = "deadbeef";
    resp.path = "src/mcp/mcp_server.cpp";
    resp.name = "mcp_server.cpp";
    resp.size = 123456;
    resp.mimeType = "text/x-c++src";
    resp.content = std::string("abc");
    resp.contentTruncated = true;
    resp.contentBytes = 3;
    resp.contentMaxBytes = 32768;
    resp.metadata["snapshot_id"] = "snap-1";

    json j = resp.toJson();
    CHECK(j["content_truncated"].get<bool>());
    CHECK(j["content_bytes"] == 3);
    CHECK(j["content_max_bytes"] == 32768);
    REQUIRE(j.contains("metadata"));
    CHECK(j["metadata"]["snapshot_id"] == "snap-1");

    auto back = yams::mcp::MCPRetrieveDocumentResponse::fromJson(j);
    CHECK(back.contentTruncated);
    CHECK(back.contentBytes == 3);
    CHECK(back.contentMaxBytes == 32768);
    REQUIRE(back.metadata.contains("snapshot_id"));
    CHECK(back.metadata["snapshot_id"] == "snap-1");
}

TEST_CASE("MCP DTO - RetrieveDocumentRequest defaults include_content=true",
          "[mcp][dto][get][defaults][catch2]") {
    auto req = yams::mcp::MCPRetrieveDocumentRequest::fromJson(json::object());
    CHECK(req.includeContent);
}
