// MCP Tools schema and smoke tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <yams/mcp/mcp_server.h>

#include <nlohmann/json.hpp>
#include <chrono>
#include <cstdlib>
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
    static std::shared_ptr<yams::mcp::MCPServer> make() {
        auto transport = std::make_unique<NullTransport>();
        auto server = std::make_shared<yams::mcp::MCPServer>(std::move(transport));
        return server;
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

class ScopedEnvVar {
public:
    ScopedEnvVar(const char* name, const std::string& value) : name_(name) {
        if (const char* old = std::getenv(name_)) {
            hadOld_ = true;
            oldValue_ = old;
        }
#if defined(_WIN32)
        _putenv_s(name_, value.c_str());
#else
        setenv(name_, value.c_str(), 1);
#endif
    }

    ~ScopedEnvVar() {
        if (hadOld_) {
#if defined(_WIN32)
            _putenv_s(name_, oldValue_.c_str());
#else
            setenv(name_, oldValue_.c_str(), 1);
#endif
        } else {
#if defined(_WIN32)
            _putenv_s(name_, "");
#else
            unsetenv(name_);
#endif
        }
    }

private:
    const char* name_;
    bool hadOld_ = false;
    std::string oldValue_;
};

} // namespace

// ============================================================================
// ListTools schema validation
// ============================================================================

TEST_CASE("MCP Schema - ListTools contains composite tools", "[mcp][schema][tools][catch2]") {
    auto server = ServerUnderTest::make();
    json result = server->testListTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());

    // Composite tools exposed to clients
    std::vector<std::string> expected = {"query", "execute", "mcp.echo"};

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

    // Individual tools should NOT be exposed
    CHECK(std::find(actual.begin(), actual.end(), "search") == actual.end());
    CHECK(std::find(actual.begin(), actual.end(), "grep") == actual.end());
}

TEST_CASE("MCP Schema - Internal tools contain all individual ops",
          "[mcp][schema][tools][catch2]") {
    auto server = ServerUnderTest::make();
    json result = server->testListInternalTools();

    REQUIRE(result.is_object());
    REQUIRE(result.contains("tools"));
    REQUIRE(result["tools"].is_array());

    // All individual tools are in the internal registry for dispatch
    std::vector<std::string> expected = {
        "search",        "grep",  "download", "session_start",    "session_stop",  "session_pin",
        "session_unpin", "graph", "get",      "status",           "update",        "delete_by_name",
        "list",          "add",   "restore",  "list_collections", "list_snapshots"};

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
    json tools = server->testListInternalTools();
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
    CHECK(hasProp(*props, "match_all_tags"));
}

TEST_CASE("MCP Schema - GrepDocuments has expected grep options", "[mcp][schema][grep][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListInternalTools();
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
    json tools = server->testListInternalTools();
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
    json tools = server->testListInternalTools();
    auto t = findTool(tools, "graph");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Unified mode selector
    CHECK(hasProp(*props, "action"));

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

    // Ingest mode inputs
    CHECK(hasProp(*props, "nodes"));
    CHECK(hasProp(*props, "edges"));
    CHECK(hasProp(*props, "aliases"));
    CHECK(hasProp(*props, "document_hash"));
    CHECK(hasProp(*props, "skip_existing_nodes"));
    CHECK(hasProp(*props, "skip_existing_edges"));
}

TEST_CASE("MCP Schema - UpdateMetadata supports name or hash and multiple pairs",
          "[mcp][schema][update][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListInternalTools();
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
    json tools = server->testListInternalTools();
    auto t = findTool(tools, "list");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    // Filtering and targeting
    CHECK(hasProp(*props, "pattern"));
    CHECK(hasProp(*props, "name"));
    CHECK(hasProp(*props, "tags"));
    CHECK(hasProp(*props, "match_all_tags"));

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
    json tools = server->testListInternalTools();
    auto t = findTool(tools, "status");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "detailed"));
}

TEST_CASE("MCP Schema - Restore has expected properties", "[mcp][schema][restore][catch2]") {
    auto server = ServerUnderTest::make();
    json tools = server->testListInternalTools();
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
    json tools = server->testListInternalTools();
    auto t = findTool(tools, "list_snapshots");
    REQUIRE(t.has_value());

    auto props = toolProps(*t);
    REQUIRE(props.has_value());

    CHECK(hasProp(*props, "with_labels"));
}

TEST_CASE("MCP Download - relative staging config does not hard-fail in unwritable cwd",
          "[mcp][download][hardening][catch2]") {
    namespace fs = std::filesystem;

    auto server = ServerUnderTest::make();

    const auto unique = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const fs::path root = fs::temp_directory_path() / ("yams_mcp_dl_hardening_" + unique);
    const fs::path xdgCfg = root / "xdg_config";
    const fs::path cfgDir = xdgCfg / "yams";
    std::error_code ec;
    fs::create_directories(cfgDir, ec);
    REQUIRE_FALSE(ec);

    // Deliberately relative path to mirror user-reported failure mode.
    {
        std::ofstream out(cfgDir / "config.toml", std::ios::binary | std::ios::trunc);
        REQUIRE(out.good());
        out << "[storage]\n";
        out << "staging_dir = \"staging\"\n";
    }

    ScopedEnvVar cfgHome("XDG_CONFIG_HOME", xdgCfg.string());
    ScopedEnvVar verbose("YAMS_POOL_VERBOSE", "1");

    const fs::path oldCwd = fs::current_path();
    bool changed = false;
    try {
        fs::current_path(fs::path("/"));
        changed = true;
    } catch (...) {
        // If this platform disallows changing cwd to '/', skip this edge-case assertion.
        SKIP("Could not switch cwd to '/' for unwritable-cwd staging hardening test");
    }

    auto restoreCwd = [&]() {
        if (changed) {
            std::error_code ignored;
            fs::current_path(oldCwd, ignored);
        }
    };

    auto res =
        server->callToolPublic("download", json{{"url", "http://127.0.0.1:1/yams-mcp-hardening"},
                                                {"post_index", false},
                                                {"store_only", true},
                                                {"timeout_ms", 1000},
                                                {"follow_redirects", false}});

    restoreCwd();

    REQUIRE(res.is_object());
    REQUIRE(res.contains("error"));
    const auto msg = res["error"].value("message", std::string{});
    const bool hasStagingFailure = msg.find("Failed to create staging dir") != std::string::npos;
    CHECK_FALSE(hasStagingFailure);

    std::error_code cleanup_ec;
    fs::remove_all(root, cleanup_ec);
}
