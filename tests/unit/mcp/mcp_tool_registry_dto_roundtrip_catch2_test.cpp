// MCP DTO roundtrip tests for search/grep/get response ergonomics

#include <catch2/catch_test_macros.hpp>

#include <yams/mcp/tool_registry.h>

using yams::mcp::json;

TEST_CASE("MCP search references survive missing hashes and paths-only output",
          "[mcp][dto][search][retrieval-reference]") {
    yams::mcp::MCPSearchResponse response;
    response.results.emplace_back();
    auto missing = response.toJson();
    REQUIRE(missing.at("results").at(0).contains("hash"));
    CHECK(missing.at("results").at(0).at("hash").is_null());
    CHECK(missing.at("results").at(0).at("hydration").is_null());
    CHECK(yams::mcp::MCPSearchResponse::fromJson(missing).results.at(0).hash.empty());
    response.results[0].hash = std::string(64, 'a');
    response.pathsOnly = true;
    response.paths = {"/source"};
    auto paths = response.toJson();
    REQUIRE(paths.at("paths") == response.paths);
    REQUIRE(paths.contains("results"));
    CHECK(paths.at("results").at(0).at("hash") == response.results[0].hash);
    CHECK(paths.at("results").at(0).at("hydration").at("method") == "get");
    CHECK(yams::mcp::MCPSearchResponse::fromJson(paths).results.at(0).hash ==
          response.results[0].hash);
}

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
    m.hash = std::string(64, 'a');
    resp.matches.push_back(m);

    json j = resp.toJson();
    REQUIRE(j.contains("matches"));
    REQUIRE(j["matches"].is_array());
    REQUIRE(j["matches"].size() == 1);
    CHECK(j["output_truncated"].get<bool>());
    CHECK(j["output_max_bytes"] == 16384);
    CHECK(j["matches"][0]["hash"] == m.hash);
    CHECK(j["matches"][0]["hydration"]["hash"] == m.hash);
    CHECK(j["matches"][0]["hydration"]["method"] == "get");

    auto back = yams::mcp::MCPGrepResponse::fromJson(j);
    REQUIRE(back.matches.size() == 1);
    CHECK(back.matches[0].file == "src/mcp/mcp_server.cpp");
    CHECK(back.matches[0].lineNumber == 4241);
    CHECK(back.matches[0].fileMatches == 16);
    CHECK(back.matches[0].hash == m.hash);
    CHECK(back.outputTruncated);
}

TEST_CASE("MCP grep unknown identity stays explicitly unhydratable", "[mcp][dto][grep]") {
    const auto legacy = yams::mcp::MCPGrepResponse::fromJson(
        json{{"matches", json::array({json{{"file", "unknown.txt"}}})}});
    const auto output = legacy.toJson();
    CHECK(output.at("matches").at(0).at("hash").is_null());
    CHECK(output.at("matches").at(0).at("hydration").is_null());
    const auto roundtrip = yams::mcp::MCPGrepResponse::fromJson(output);
    REQUIRE(roundtrip.matches.size() == 1);
    CHECK(roundtrip.matches[0].hash.empty());
}

TEST_CASE("MCP DTO - GrepRequest preserves session and selector fields",
          "[mcp][dto][grep][request][catch2]") {
    yams::mcp::MCPGrepRequest req;
    req.pattern = "TODO";
    req.name = "src";
    req.subpath = false;
    req.paths = {"src/**/*.cpp"};
    req.includePatterns = {"src/**", "include/**"};
    req.ignoreCase = true;
    req.lineNumbers = true;
    req.useSession = true;
    req.sessionName = "bench-session";
    req.tags = {"code"};
    req.matchAllTags = true;
    req.cwd = "/tmp/worktree";

    json j = req.toJson();
    CHECK(j["name"] == "src");
    CHECK(j["subpath"] == false);
    REQUIRE(j.contains("include_patterns"));
    CHECK(j["include_patterns"] == json::array({"src/**", "include/**"}));
    CHECK(j["use_session"].get<bool>());
    CHECK(j["session"] == "bench-session");

    auto back = yams::mcp::MCPGrepRequest::fromJson(j);
    CHECK(back.pattern == "TODO");
    CHECK(back.name == "src");
    CHECK(back.subpath == false);
    CHECK(back.paths == std::vector<std::string>{"src/**/*.cpp"});
    CHECK(back.includePatterns == std::vector<std::string>{"src/**", "include/**"});
    CHECK(back.ignoreCase);
    CHECK(back.lineNumbers);
    CHECK(back.useSession);
    CHECK(back.sessionName == "bench-session");
    CHECK(back.tags == std::vector<std::string>{"code"});
    CHECK(back.matchAllTags);
    CHECK(back.cwd == "/tmp/worktree");
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
