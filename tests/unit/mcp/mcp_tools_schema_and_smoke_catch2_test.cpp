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
    std::vector<std::string> expected = {"search",
                                         "grep",
                                         "download",
                                         "session_start",
                                         "session_stop",
                                         "get",
                                         "stats",
                                         "update",
                                         "delete_by_name",
                                         "cat",
                                         "list",
                                         "add_directory",
                                         "restore_collection",
                                         "restore_snapshot",
                                         "restore",
                                         "list_collections",
                                         "list_snapshots",
                                         "get_by_name"};

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

    // Required fields and ergonomics
    CHECK(hasProp(*props, "query"));
    CHECK(hasProp(*props, "limit"));
    CHECK(hasProp(*props, "fuzzy"));
    CHECK(hasProp(*props, "similarity"));
    CHECK(hasProp(*props, "hash"));
    CHECK(hasProp(*props, "type"));

    // LLM ergonomics / output shaping + session scoping
    CHECK(hasProp(*props, "paths_only"));
    CHECK(hasProp(*props, "use_session"));
    CHECK(hasProp(*props, "session"));

    // Contextual display options
    CHECK(hasProp(*props, "line_numbers"));
    CHECK(hasProp(*props, "after_context"));
    CHECK(hasProp(*props, "before_context"));
    CHECK(hasProp(*props, "context"));

    // Color control
    CHECK(hasProp(*props, "color"));
}

TEST_CASE("MCP Schema - GrepDocuments has expected grep options", "[mcp][schema][grep][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "grep");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "pattern"));
    CHECK(hasProp(*props, "paths"));

    // Context
    CHECK(hasProp(*props, "after_context"));
    CHECK(hasProp(*props, "before_context"));
    CHECK(hasProp(*props, "context"));

    // Pattern options
    CHECK(hasProp(*props, "ignore_case"));
    CHECK(hasProp(*props, "word"));
    CHECK(hasProp(*props, "invert"));

    // Output modes
    CHECK(hasProp(*props, "line_numbers"));
    CHECK(hasProp(*props, "with_filename"));
    CHECK(hasProp(*props, "count"));
    CHECK(hasProp(*props, "files_with_matches"));
    CHECK(hasProp(*props, "files_without_match"));

    // Color and max count + session scoping
    CHECK(hasProp(*props, "color"));
    CHECK(hasProp(*props, "max_count"));
    CHECK(hasProp(*props, "use_session"));
    CHECK(hasProp(*props, "session"));
}

TEST_CASE("MCP Schema - RetrieveDocument has graph params", "[mcp][schema][retrieve][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "get");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "hash"));
    CHECK(hasProp(*props, "outputPath"));
    CHECK(hasProp(*props, "graph"));
    CHECK(hasProp(*props, "depth"));
    CHECK(hasProp(*props, "include_content"));
}

TEST_CASE("MCP Schema - UpdateMetadata supports name or hash and multiple pairs",
          "[mcp][schema][update][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "update");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "hash"));
    CHECK(hasProp(*props, "type"));
}

TEST_CASE("MCP Schema - ListDocuments supports filters and sorting",
          "[mcp][schema][list][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "list");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Filtering
    CHECK(hasProp(*props, "limit"));
    CHECK(hasProp(*props, "pattern"));
    CHECK(hasProp(*props, "tags"));
    CHECK(hasProp(*props, "type"));
    CHECK(hasProp(*props, "mime"));
    CHECK(hasProp(*props, "extension"));
    CHECK(hasProp(*props, "binary"));
    CHECK(hasProp(*props, "text"));

    // Time filters
    CHECK(hasProp(*props, "created_after"));
    CHECK(hasProp(*props, "created_before"));
    CHECK(hasProp(*props, "modified_after"));
    CHECK(hasProp(*props, "modified_before"));
    CHECK(hasProp(*props, "indexed_after"));
    CHECK(hasProp(*props, "indexed_before"));

    // Recency and sorting
    CHECK(hasProp(*props, "name"));
    CHECK(hasProp(*props, "metadata"));
}

TEST_CASE("MCP Schema - GetStats supports file types breakdown", "[mcp][schema][stats][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "stats");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "detailed"));
    CHECK(hasProp(*props, "file_types"));
}

TEST_CASE("MCP Schema - AddDirectory has expected properties",
          "[mcp][schema][directory][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "add_directory");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "directory_path"));
    CHECK(hasProp(*props, "collection"));
    CHECK(hasProp(*props, "snapshot_id"));
    CHECK(hasProp(*props, "snapshot_label"));
    CHECK(hasProp(*props, "recursive"));
    CHECK(hasProp(*props, "include_patterns"));
    CHECK(hasProp(*props, "exclude_patterns"));
}

TEST_CASE("MCP Schema - RestoreCollection has expected properties",
          "[mcp][schema][restore][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "restore");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "collection"));
    CHECK(hasProp(*props, "output_directory"));
    CHECK(hasProp(*props, "layout_template"));
    CHECK(hasProp(*props, "include_patterns"));
    CHECK(hasProp(*props, "exclude_patterns"));
    CHECK(hasProp(*props, "overwrite"));
    CHECK(hasProp(*props, "create_dirs"));
    CHECK(hasProp(*props, "dry_run"));
}

TEST_CASE("MCP Schema - RestoreSnapshot has expected properties",
          "[mcp][schema][snapshot][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "restore_snapshot");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

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
    auto t = findTool(tools, "list");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "with_labels"));
}
