#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <yams/mcp/tool_registry.h>

using nlohmann::json;
using namespace yams::mcp;

namespace {

TEST(MCPDownloaderIntegrationTest, ToolWrapperReturnsContentArrayAndParsesResponse) {
    // Arrange: create a ToolRegistry and register a fake downloader tool handler
    ToolRegistry registry;

    // Register the downloader tool with a handler that returns a fabricated response
    registry.registerTool<MCPDownloadRequest, MCPDownloadResponse>(
        "downloader.download",
        [](const MCPDownloadRequest& req) -> yams::Result<MCPDownloadResponse> {
            // Validate the request fields minimally
            EXPECT_FALSE(req.url.empty());

            // Return a synthetic successful response
            MCPDownloadResponse resp;
            resp.url = req.url;
            resp.hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
            resp.storedPath = "/tmp/objects/sha256/01/23/012345...";
            resp.sizeBytes = 123456;
            resp.success = true;
            resp.httpStatus = 200;
            resp.etag = "W/\"etag-123\"";
            resp.lastModified = "Mon, 01 Jan 2024 00:00:00 GMT";
            resp.checksumOk = true;
            return resp;
        },
        json{{"type", "object"},
             {"properties", {{"url", {{"type", "string"}}}}},
             {"required", json::array({"url"})}},
        "Downloader tool integration test");

    // Build the arguments that would be sent by an MCP client
    json args{{"url", "https://example.com/resource.bin"}};

    // Act: invoke the tool via the registry
    json result = registry.callTool("downloader.download", args);

    // Assert: result has a "content" array (MCP requires array of content blocks)
    ASSERT_TRUE(result.contains("content")) << result.dump(2);
    ASSERT_TRUE(result["content"].is_array()) << result.dump(2);
    ASSERT_FALSE(result["content"].empty()) << result.dump(2);

    const json& block = result["content"].at(0);
    ASSERT_TRUE(block.contains("type")) << result.dump(2);
    ASSERT_TRUE(block.contains("text")) << result.dump(2);
    EXPECT_EQ(block["type"], "text");

    // The "text" contains the JSON-serialized MCPDownloadResponse
    ASSERT_TRUE(block["text"].is_string());
    const std::string textPayload = block["text"].get<std::string>();

    // Parse the embedded JSON string
    json embedded = json::parse(textPayload);

    // Use the provided typed DTO parser to ensure schema compatibility
    MCPDownloadResponse parsed = MCPDownloadResponse::fromJson(embedded);

    // Validate the parsed response
    EXPECT_EQ(parsed.url, "https://example.com/resource.bin");
    EXPECT_EQ(parsed.hash,
              "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    EXPECT_EQ(parsed.storedPath, "/tmp/objects/sha256/01/23/012345...");
    EXPECT_EQ(parsed.sizeBytes, 123456u);
    EXPECT_TRUE(parsed.success);
    ASSERT_TRUE(parsed.httpStatus.has_value());
    EXPECT_EQ(*parsed.httpStatus, 200);
    ASSERT_TRUE(parsed.etag.has_value());
    EXPECT_EQ(*parsed.etag, "W/\"etag-123\"");
    ASSERT_TRUE(parsed.lastModified.has_value());
    EXPECT_EQ(*parsed.lastModified, "Mon, 01 Jan 2024 00:00:00 GMT");
    ASSERT_TRUE(parsed.checksumOk.has_value());
    EXPECT_TRUE(*parsed.checksumOk);
}

} // namespace