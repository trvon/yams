// MCP Tools schema and smoke tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <yams/mcp/mcp_server.h>

#include <nlohmann/json.hpp>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <yams/api/content_store_builder.h>
#include <yams/metadata/connection_pool.h>

using nlohmann::json;

namespace {

// Minimal no-op transport for constructing MCPServer in tests without starting loops.
class NullTransport : public yams::mcp::ITransport {
public:
    void send(const json&) override {}
    yams::mcp::MessageResult receive() override {
        return yams::Error{yams::ErrorCode::NotImplemented, "Null transport"};
    }
    bool isConnected() const override { return false; }
    void close() override {}
    yams::mcp::TransportState getState() const override {
        return yams::mcp::TransportState::Disconnected;
    }
};

struct ServerUnderTest {
    // Construct MCPServer with null dependencies that aren't needed for listTools schema checks
    static std::unique_ptr<yams::mcp::MCPServer> make() {
        auto transport = std::make_unique<NullTransport>();
        return std::make_unique<yams::mcp::MCPServer>(std::move(transport));
    }
};

// Helper: find a tool by name within listTools() response
std::optional<json> findTool(const json& listToolsResult, const std::string& toolName) {
    if (!listToolsResult.is_object() || !listToolsResult.contains("tools") ||
        !listToolsResult["tools"].is_array()) {
        return std::nullopt;
    }
    for (const auto& t : listToolsResult["tools"]) {
        if (t.is_object() && t.value("name", "") == toolName) {
            return t;
        }
    }
    return std::nullopt;
}

// Helper: fetch inputSchema.properties object for a tool
std::optional<json> toolProps(const json& tool) {
    if (!tool.is_object())
        return std::nullopt;
    if (!tool.contains("inputSchema"))
        return std::nullopt;
    const auto& schema = tool["inputSchema"];
    if (!schema.is_object())
        return std::nullopt;
    if (!schema.contains("properties") || !schema["properties"].is_object())
        return std::nullopt;
    return schema["properties"];
}

// Helper: check if a property exists within properties
bool hasProp(const json& props, const std::string& name) {
    return props.is_object() && props.contains(name);
}

} // namespace

// ============================================================================
// ListTools schema validation
// ============================================================================

TEST_CASE("MCP Schema - ListTools contains all expected tools", "[mcp][schema][tools][catch2]") {
    auto server = ServerUnderTest::make();
    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());

    // Expected set of core tools per CHANGELOG and implementation
    std::vector<std::string> expected = {
        "search",        "grep",  "download", "session_start",    "session_stop",  "session_pin",
        "session_unpin", "graph", "get",      "status",           "update",        "delete_by_name",
        "list",          "add",   "restore",  "list_collections", "list_snapshots"};

    // Gather actual names
    std::vector<std::string> actual;
    for (const auto& t : result["tools"]) {
        if (t.is_object() && t.contains("name")) {
            actual.push_back(t.value("name", ""));
        }
    }

    for (const auto& name : expected) {
        bool found = std::find(actual.begin(), actual.end(), name) != actual.end();
        CHECK(found);
    }
}

TEST_CASE("MCP Schema - SearchDocuments has ergonomic and context params",
          "[mcp][schema][search][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "search");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Required fields and core search ergonomics
    CHECK(hasProp(*props, "query"));
    CHECK(hasProp(*props, "limit"));
    CHECK(hasProp(*props, "fuzzy"));
    CHECK(hasProp(*props, "similarity"));
    CHECK(hasProp(*props, "type"));

    // Output shaping
    CHECK(hasProp(*props, "paths_only"));

    // Path filtering
    CHECK(hasProp(*props, "path_pattern"));
    CHECK(hasProp(*props, "include_patterns"));

    // Tag filtering (YAMS extension)
    CHECK(hasProp(*props, "tags"));
}

TEST_CASE("MCP Schema - GrepDocuments has expected grep options", "[mcp][schema][grep][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "grep");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Required pattern field
    CHECK(hasProp(*props, "pattern"));

    // Path targeting
    CHECK(hasProp(*props, "name"));
    CHECK(hasProp(*props, "paths"));
    CHECK(hasProp(*props, "include_patterns"));
    CHECK(hasProp(*props, "subpath"));

    // Pattern options
    CHECK(hasProp(*props, "ignore_case"));

    // Output options
    CHECK(hasProp(*props, "line_numbers"));
    CHECK(hasProp(*props, "context"));
}

TEST_CASE("MCP Schema - RetrieveDocument has expected params", "[mcp][schema][retrieve][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "get");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Target identification
    CHECK(hasProp(*props, "hash"));
    CHECK(hasProp(*props, "name"));

    // Output options
    CHECK(hasProp(*props, "output_path"));
    CHECK(hasProp(*props, "include_content"));

    // Session scoping
    CHECK(hasProp(*props, "use_session"));
    CHECK(hasProp(*props, "session"));
}

TEST_CASE("MCP Schema - Graph tool has CLI parity params", "[mcp][schema][graph][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "graph");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Target selection
    CHECK(hasProp(*props, "hash"));
    CHECK(hasProp(*props, "name"));
    CHECK(hasProp(*props, "node_key"));
    CHECK(hasProp(*props, "node_id"));

    // Mode selection
    CHECK(hasProp(*props, "list_types"));
    CHECK(hasProp(*props, "list_type"));
    CHECK(hasProp(*props, "isolated"));

    // Traversal options
    CHECK(hasProp(*props, "relation"));
    CHECK(hasProp(*props, "depth"));
    CHECK(hasProp(*props, "limit"));
    CHECK(hasProp(*props, "offset"));
    CHECK(hasProp(*props, "reverse"));

    // Output control
    CHECK(hasProp(*props, "include_properties"));
    CHECK(hasProp(*props, "scope_snapshot"));
}

TEST_CASE("MCP Schema - UpdateMetadata supports name or hash and multiple pairs",
          "[mcp][schema][update][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "update");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Target selection
    CHECK(hasProp(*props, "hash"));
    CHECK(hasProp(*props, "name"));
    CHECK(hasProp(*props, "path"));
    CHECK(hasProp(*props, "names"));
    CHECK(hasProp(*props, "pattern"));

    // Disambiguation
    CHECK(hasProp(*props, "latest"));
    CHECK(hasProp(*props, "oldest"));

    // Update data
    CHECK(hasProp(*props, "metadata"));
    CHECK(hasProp(*props, "tags"));
}

TEST_CASE("MCP Schema - ListDocuments supports filters and sorting",
          "[mcp][schema][list][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "list");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Filtering and targeting
    CHECK(hasProp(*props, "pattern"));
    CHECK(hasProp(*props, "name"));
    CHECK(hasProp(*props, "tags"));

    // Recency
    CHECK(hasProp(*props, "recent"));

    // Output control
    CHECK(hasProp(*props, "paths_only"));
    CHECK(hasProp(*props, "include_diff"));

    // Pagination
    CHECK(hasProp(*props, "limit"));
    CHECK(hasProp(*props, "offset"));
}

TEST_CASE("MCP Schema - GetStats supports file types breakdown", "[mcp][schema][stats][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "status");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "detailed"));
}

TEST_CASE("MCP Schema - Restore has expected properties", "[mcp][schema][restore][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "restore");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "collection"));
    CHECK(hasProp(*props, "snapshot_id"));
    CHECK(hasProp(*props, "snapshot_label"));
    CHECK(hasProp(*props, "output_directory"));
    CHECK(hasProp(*props, "layout_template"));
    CHECK(hasProp(*props, "include_patterns"));
    CHECK(hasProp(*props, "exclude_patterns"));
    CHECK(hasProp(*props, "overwrite"));
    CHECK(hasProp(*props, "create_dirs"));
    CHECK(hasProp(*props, "dry_run"));
}

TEST_CASE("MCP Schema - ListSnapshots has withLabels", "[mcp][schema][snapshots][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "list_snapshots");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "with_labels"));
}
