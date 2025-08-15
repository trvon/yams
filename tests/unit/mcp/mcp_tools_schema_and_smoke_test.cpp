#include <gmock/gmock.h>
#include <gtest/gtest.h>

// Access MCPServer internals for white-box schema validation in tests via friend declarations
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
using ::testing::Contains;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsSupersetOf;
using ::testing::Key;
using ::testing::Pair;
using ::testing::UnorderedElementsAreArray;

namespace {

// Minimal no-op transport for constructing MCPServer in tests without starting loops.
class NullTransport : public yams::mcp::ITransport {
public:
    void send(const json&) override {}
    json receive() override { return json(); }
    bool isConnected() const override { return false; }
    void close() override {}
};

struct ServerUnderTest {
    // Construct MCPServer with null dependencies that aren't needed for listTools schema checks
    static std::unique_ptr<yams::mcp::MCPServer> make() {
        auto transport = std::make_unique<NullTransport>();
        std::shared_ptr<yams::api::IContentStore> store;          // nullptr ok for schema tests
        std::shared_ptr<yams::search::SearchExecutor> searchExec; // nullptr ok for schema tests
        std::shared_ptr<yams::metadata::MetadataRepository> repo; // nullptr ok for schema tests
        std::shared_ptr<yams::search::HybridSearchEngine> hybrid; // nullptr ok for schema tests
        return std::make_unique<yams::mcp::MCPServer>(store, searchExec, repo, hybrid,
                                                      std::move(transport));
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

// Helper: assert a property exists within properties
::testing::AssertionResult hasProp(const json& props, const std::string& name) {
    if (!props.is_object()) {
        return ::testing::AssertionFailure() << "properties is not an object";
    }
    if (!props.contains(name)) {
        return ::testing::AssertionFailure() << "missing property: " << name;
    }
    return ::testing::AssertionSuccess();
}

} // namespace

// ============================================================================
// ListTools schema validation
// ============================================================================

TEST(MCPSchemaTest, ListTools_ContainsAllExpectedTools) {
    auto server = ServerUnderTest::make();
    json result = server->testListTools();

    ASSERT_TRUE(result.is_object());
    ASSERT_TRUE(result.contains("tools"));
    ASSERT_TRUE(result["tools"].is_array());

    // Expected set of core tools per CHANGELOG and implementation
    std::vector<std::string> expected = {
        "search_documents", "grep_documents",   "store_document", "retrieve_document",
        "get_stats",        "update_metadata",  "delete_by_name", "get_by_name",
        "cat_document",     "list_documents",   "add_directory",  "restore_collection",
        "restore_snapshot", "list_collections", "list_snapshots"};

    // Gather actual names
    std::vector<std::string> actual;
    for (const auto& t : result["tools"]) {
        if (t.is_object() && t.contains("name")) {
            actual.push_back(t.value("name", ""));
        }
    }

    for (const auto& name : expected) {
        EXPECT_THAT(actual, Contains(name)) << "Missing tool in listTools(): " << name;
    }
}

TEST(MCPSchemaTest, SearchDocuments_SchemaHasErgonomicAndContextParams) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "search_documents");
    ASSERT_TRUE(t.has_value()) << "search_documents not found in tools/list";

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    // Required fields and ergonomics
    EXPECT_TRUE(hasProp(*props, "query"));
    EXPECT_TRUE(hasProp(*props, "limit"));
    EXPECT_TRUE(hasProp(*props, "fuzzy"));
    EXPECT_TRUE(hasProp(*props, "similarity"));
    EXPECT_TRUE(hasProp(*props, "hash"));
    EXPECT_TRUE(hasProp(*props, "verbose"));
    EXPECT_TRUE(hasProp(*props, "type"));

    // LLM ergonomics / output shaping
    EXPECT_TRUE(hasProp(*props, "paths_only"));

    // Contextual display options
    EXPECT_TRUE(hasProp(*props, "line_numbers"));
    EXPECT_TRUE(hasProp(*props, "after_context"));
    EXPECT_TRUE(hasProp(*props, "before_context"));
    EXPECT_TRUE(hasProp(*props, "context"));

    // Color control
    EXPECT_TRUE(hasProp(*props, "color"));
}

TEST(MCPSchemaTest, GrepDocuments_SchemaHasExpectedGrepOptions) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "grep_documents");
    ASSERT_TRUE(t.has_value()) << "grep_documents not found in tools/list";

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "pattern"));
    EXPECT_TRUE(hasProp(*props, "paths"));

    // Context
    EXPECT_TRUE(hasProp(*props, "after_context"));
    EXPECT_TRUE(hasProp(*props, "before_context"));
    EXPECT_TRUE(hasProp(*props, "context"));

    // Pattern options
    EXPECT_TRUE(hasProp(*props, "ignore_case"));
    EXPECT_TRUE(hasProp(*props, "word"));
    EXPECT_TRUE(hasProp(*props, "invert"));

    // Output modes
    EXPECT_TRUE(hasProp(*props, "line_numbers"));
    EXPECT_TRUE(hasProp(*props, "with_filename"));
    EXPECT_TRUE(hasProp(*props, "count"));
    EXPECT_TRUE(hasProp(*props, "files_with_matches"));
    EXPECT_TRUE(hasProp(*props, "files_without_match"));

    // Color and max count
    EXPECT_TRUE(hasProp(*props, "color"));
    EXPECT_TRUE(hasProp(*props, "max_count"));
}

TEST(MCPSchemaTest, RetrieveDocument_SchemaHasGraphParams) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "retrieve_document");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "hash"));
    EXPECT_TRUE(hasProp(*props, "outputPath"));
    EXPECT_TRUE(hasProp(*props, "graph"));
    EXPECT_TRUE(hasProp(*props, "depth"));
    EXPECT_TRUE(hasProp(*props, "include_content"));
}

TEST(MCPSchemaTest, UpdateMetadata_SchemaSupportsNameOrHashAndMultiplePairs) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "update_metadata");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "hash"));
    EXPECT_TRUE(hasProp(*props, "name"));
    EXPECT_TRUE(hasProp(*props, "metadata"));
    EXPECT_TRUE(hasProp(*props, "verbose"));
}

TEST(MCPSchemaTest, ListDocuments_SchemaSupportsFiltersAndSorting) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "list_documents");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    // Filtering
    EXPECT_TRUE(hasProp(*props, "limit"));
    EXPECT_TRUE(hasProp(*props, "pattern"));
    EXPECT_TRUE(hasProp(*props, "tags"));
    EXPECT_TRUE(hasProp(*props, "type"));
    EXPECT_TRUE(hasProp(*props, "mime"));
    EXPECT_TRUE(hasProp(*props, "extension"));
    EXPECT_TRUE(hasProp(*props, "binary"));
    EXPECT_TRUE(hasProp(*props, "text"));

    // Time filters
    EXPECT_TRUE(hasProp(*props, "created_after"));
    EXPECT_TRUE(hasProp(*props, "created_before"));
    EXPECT_TRUE(hasProp(*props, "modified_after"));
    EXPECT_TRUE(hasProp(*props, "modified_before"));
    EXPECT_TRUE(hasProp(*props, "indexed_after"));
    EXPECT_TRUE(hasProp(*props, "indexed_before"));

    // Recency and sorting
    EXPECT_TRUE(hasProp(*props, "recent"));
    EXPECT_TRUE(hasProp(*props, "sort_by"));
    EXPECT_TRUE(hasProp(*props, "sort_order"));
}

TEST(MCPSchemaTest, GetStats_SchemaSupportsFileTypesBreakdown) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "get_stats");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "detailed"));
    EXPECT_TRUE(hasProp(*props, "file_types"));
}

// ============================================================================
// Smoke tests (incremental)
// Note: These are minimal "does not crash and returns a structured object"
//       tests. Full functional verification with a temporary repository will
//       be added incrementally.
// ============================================================================

class MCPSmokeTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Construct server with a no-op transport.
        auto transport = std::make_unique<NullTransport>();
        std::shared_ptr<yams::api::IContentStore> store;
        std::shared_ptr<yams::search::SearchExecutor> searchExec;
        std::shared_ptr<yams::metadata::MetadataRepository> repo;
        std::shared_ptr<yams::search::HybridSearchEngine> hybrid;
        server = std::make_unique<yams::mcp::MCPServer>(store, searchExec, repo, hybrid,
                                                        std::move(transport));
    }

    std::unique_ptr<yams::mcp::MCPServer> server;
};

TEST_F(MCPSmokeTest, SearchDocuments_AcceptsErgonomicFlags_NoCrash) {
    // NOTE: Backend dependencies are not wired here; this is an API-level smoke test.
    // It verifies the method accepts the documented parameters and returns a JSON object.
    json args = {{"query", "MCP"}, {"paths_only", true}, {"line_numbers", true},
                 {"context", 2},   {"color", "never"},   {"limit", 5}};

    // Using white-box access to private method via macro above
    json response = server->testSearchDocuments(args);

    // Should respond structurally (either results or a structured error)
    ASSERT_TRUE(response.is_object());
    // Accept either shape: {"results":[...]} or {"error": "..."} depending on backend availability
    EXPECT_TRUE(response.contains("results") || response.contains("error"));
}

TEST_F(MCPSmokeTest, GrepDocuments_AcceptsKeyFlags_NoCrash) {
    json args = {{"pattern", "TODO|MCP"},
                 {"line_numbers", true},
                 {"context", 1},
                 {"ignore_case", true},
                 {"word", false},
                 {"invert", false},
                 {"count", false},
                 {"files_with_matches", false},
                 {"files_without_match", false},
                 {"color", "never"},
                 {"max_count", 1}};

    json response = server->testGrepDocuments(args);

    ASSERT_TRUE(response.is_object());
    EXPECT_TRUE(response.contains("matches") || response.contains("error") ||
                response.contains("results"));
}

// ============================================================================
// Stubs for additional tools (graph/update/list/stats) to be filled incrementally
// ============================================================================

TEST_F(MCPSmokeTest, DISABLED_RetrieveDocument_WithGraph_Stub) {
    GTEST_SKIP() << "TODO: Wire a temporary metadata repository and content to validate graph, "
                    "depth, include_content.";
    json args = {{"hash", "deadbeef"}, {"graph", true}, {"depth", 2}, {"include_content", false}};
    json response = server->testRetrieveDocument(args);
    (void)response;
}

TEST_F(MCPSmokeTest, DISABLED_UpdateMetadata_Stub) {
    GTEST_SKIP() << "TODO: Wire a temporary metadata repository and validate multi key=value "
                    "updates by name/hash.";
    json args = {{"name", "CHANGELOG.md"},
                 {"metadata", json::array({"pbi=123", "status=active"})},
                 {"verbose", true}};
    json response = server->testUpdateDocumentMetadata(args);
    (void)response;
}

TEST_F(MCPSmokeTest, DISABLED_ListDocuments_Stub) {
    GTEST_SKIP() << "TODO: Populate temp store with files and validate type/mime/extension/time "
                    "filters and sorting.";
    json args = {{"limit", 10}, {"pattern", "*.md"},    {"text", true},
                 {"recent", 5}, {"sort_by", "indexed"}, {"sort_order", "desc"}};
    json response = server->testListDocuments(args);
    (void)response;
}

TEST_F(MCPSmokeTest, DISABLED_GetStats_FileTypesBreakdown_Stub) {
    GTEST_SKIP() << "TODO: Populate temp store with several types and validate file_types "
                    "breakdown and top mime/extensions.";
    json args = {{"file_types", true}};
    json response = server->testGetStats(args);
    (void)response;
}

// ============================================================================
// Additional schema tests for directory/collection tools
// ============================================================================

TEST(MCPSchemaTest, AddDirectory_SchemaHasExpectedProperties) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "add_directory");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "directory_path"));
    EXPECT_TRUE(hasProp(*props, "collection"));
    EXPECT_TRUE(hasProp(*props, "snapshot_id"));
    EXPECT_TRUE(hasProp(*props, "snapshot_label"));
    EXPECT_TRUE(hasProp(*props, "recursive"));
    EXPECT_TRUE(hasProp(*props, "include_patterns"));
    EXPECT_TRUE(hasProp(*props, "exclude_patterns"));
    EXPECT_TRUE(hasProp(*props, "tags"));
    EXPECT_TRUE(hasProp(*props, "metadata"));
}

TEST(MCPSchemaTest, RestoreCollection_SchemaHasExpectedProperties) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "restore_collection");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "collection"));
    EXPECT_TRUE(hasProp(*props, "output_directory"));
    EXPECT_TRUE(hasProp(*props, "layout_template"));
    EXPECT_TRUE(hasProp(*props, "include_patterns"));
    EXPECT_TRUE(hasProp(*props, "exclude_patterns"));
    EXPECT_TRUE(hasProp(*props, "overwrite"));
    EXPECT_TRUE(hasProp(*props, "create_dirs"));
    EXPECT_TRUE(hasProp(*props, "dry_run"));
}

TEST(MCPSchemaTest, RestoreSnapshot_SchemaHasExpectedProperties) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "restore_snapshot");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "snapshot_id"));
    EXPECT_TRUE(hasProp(*props, "snapshot_label"));
    EXPECT_TRUE(hasProp(*props, "output_directory"));
    EXPECT_TRUE(hasProp(*props, "layout_template"));
    EXPECT_TRUE(hasProp(*props, "include_patterns"));
    EXPECT_TRUE(hasProp(*props, "exclude_patterns"));
    EXPECT_TRUE(hasProp(*props, "overwrite"));
    EXPECT_TRUE(hasProp(*props, "create_dirs"));
    EXPECT_TRUE(hasProp(*props, "dry_run"));
}

TEST(MCPSchemaTest, ListCollections_SchemaMinimal) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "list_collections");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());
    // No required properties for list_collections; properties may be empty object
}

TEST(MCPSchemaTest, ListSnapshots_SchemaHasWithLabels) {
    auto server = ServerUnderTest::make();
    json tools = server->testListTools();
    auto t = findTool(tools, "list_snapshots");
    ASSERT_TRUE(t.has_value());

    auto props = toolProps(*t);
    ASSERT_TRUE(props.has_value());

    EXPECT_TRUE(hasProp(*props, "with_labels"));
}

// ============================================================================
// Functional tests with temporary content store + metadata repository
// ============================================================================

class MCPE2ETest : public ::testing::Test {
protected:
    struct CreatedDoc {
        std::string hash;
        std::filesystem::path path;
        int64_t id;
    };

    void SetUp() override {
        tmpDir = std::filesystem::temp_directory_path() /
                 ("yams_mcp_e2e_" +
                  std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        filesDir = tmpDir / "files";
        storageDir = tmpDir / "storage";
        std::filesystem::create_directories(filesDir);
        std::filesystem::create_directories(storageDir);

        // Initialize metadata repository (SQLite via connection pool)
        pool = std::make_unique<yams::metadata::ConnectionPool>((tmpDir / "metadata.db").string());
        auto init = pool->initialize();
        ASSERT_TRUE(init.has_value());

        repo = std::make_shared<yams::metadata::MetadataRepository>(*pool);

        // Build content store
        yams::api::ContentStoreConfig cfg;
        cfg.storagePath = storageDir;
        auto built = yams::api::ContentStoreBuilder().withConfig(cfg).build();
        ASSERT_TRUE(built.has_value());
        auto storeUnique = std::move(built).value();
        store = std::shared_ptr<yams::api::IContentStore>(storeUnique.release());

        // Construct server with working store + repo
        auto transport = std::make_unique<NullTransport>();
        std::shared_ptr<yams::search::SearchExecutor> searchExec; // not required for these tests
        std::shared_ptr<yams::search::HybridSearchEngine>
            hybrid; // null -> fallback to repo fuzzy/fts
        server = std::make_unique<yams::mcp::MCPServer>(store, searchExec, repo, hybrid,
                                                        std::move(transport));
    }

    void TearDown() override {
        server.reset();
        store.reset();
        repo.reset();
        pool.reset();
        std::error_code ec;
        std::filesystem::remove_all(tmpDir, ec);
    }

    CreatedDoc addTextDoc(const std::string& name, const std::string& content,
                          const std::string& mime) {
        auto p = filesDir / name;
        {
            std::ofstream f(p, std::ios::binary);
            f << content;
        }

        // Store into content store
        yams::api::ContentMetadata meta;
        meta.name = name;
        meta.mimeType = mime;
        auto storeRes = store->store(p, meta);
        EXPECT_TRUE(storeRes.has_value());
        auto hash = storeRes.value().contentHash;

        // Insert into metadata repository so MCP can discover it
        yams::metadata::DocumentInfo info;
        info.filePath = p.string();
        info.fileName = name;
        info.fileExtension = std::filesystem::path(name).extension().string();
        info.fileSize = static_cast<int64_t>(content.size());
        info.sha256Hash = hash;
        info.mimeType = mime;
        auto now = std::chrono::system_clock::now();
        info.createdTime = now;
        info.modifiedTime = now;
        info.indexedTime = now;
        info.contentExtracted = true;
        info.extractionStatus = yams::metadata::ExtractionStatus::Success;

        auto idRes = repo->insertDocument(info);
        EXPECT_TRUE(idRes.has_value());
        int64_t id = idRes.value();

        // Index content (FTS5 if available; otherwise no-op)
        (void)repo->indexDocumentContent(id, name, content, "text/plain");

        return CreatedDoc{hash, p, id};
    }

    // Helper to create a binary doc
    CreatedDoc addBinaryDoc(const std::string& name, const std::vector<std::byte>& data,
                            const std::string& mime) {
        auto p = filesDir / name;
        {
            std::ofstream f(p, std::ios::binary);
            f.write(reinterpret_cast<const char*>(data.data()),
                    static_cast<std::streamsize>(data.size()));
        }

        yams::api::ContentMetadata meta;
        meta.name = name;
        meta.mimeType = mime;
        auto storeRes = store->store(p, meta);
        EXPECT_TRUE(storeRes.has_value());
        auto hash = storeRes.value().contentHash;

        yams::metadata::DocumentInfo info;
        info.filePath = p.string();
        info.fileName = name;
        info.fileExtension = std::filesystem::path(name).extension().string();
        info.fileSize = static_cast<int64_t>(data.size());
        info.sha256Hash = hash;
        info.mimeType = mime;
        auto now = std::chrono::system_clock::now();
        info.createdTime = now;
        info.modifiedTime = now;
        info.indexedTime = now;
        info.contentExtracted = false;
        info.extractionStatus = yams::metadata::ExtractionStatus::Skipped;

        auto idRes = repo->insertDocument(info);
        EXPECT_TRUE(idRes.has_value());
        int64_t id = idRes.value();
        return CreatedDoc{hash, p, id};
    }

    std::unique_ptr<yams::mcp::MCPServer> server;
    std::shared_ptr<yams::api::IContentStore> store;
    std::shared_ptr<yams::metadata::MetadataRepository> repo;
    std::unique_ptr<yams::metadata::ConnectionPool> pool;
    std::filesystem::path tmpDir, filesDir, storageDir;
};

TEST_F(MCPE2ETest, SearchDocuments_PathsOnly_ReturnsStoredPath) {
    auto doc = addTextDoc("alpha.md", "# Title\nalpha beta gamma\n", "text/markdown");

    json args = {{"query", "alpha"}, {"paths_only", true}, {"limit", 10}};

    auto resp = server->testSearchDocuments(args);
    ASSERT_TRUE(resp.is_object());
    ASSERT_TRUE(resp.contains("paths"));
    ASSERT_TRUE(resp["paths"].is_array());

    bool found = false;
    for (const auto& p : resp["paths"]) {
        if (p.is_string() && p.get<std::string>() == doc.path.string()) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(MCPE2ETest, GrepDocuments_FindsTODO_WithContextAndLineNumbers) {
    auto doc = addTextDoc("notes.md", "line1\nTODO: something\nline3\n", "text/markdown");

    json args = {{"pattern", "TODO"}, {"context", 1}, {"line_numbers", true}};

    auto resp = server->testGrepDocuments(args);
    ASSERT_TRUE(resp.is_object());
    EXPECT_TRUE(resp.contains("results"));
    const auto& results = resp["results"];
    ASSERT_TRUE(results.is_array());
    bool anyMatch = false;
    for (const auto& fileRes : results) {
        if (fileRes.contains("file") && fileRes.contains("matches")) {
            const auto& matches = fileRes["matches"];
            if (matches.is_array() && !matches.empty()) {
                anyMatch = true;
                // Validate line number presence
                EXPECT_TRUE(matches[0].contains("line_number"));
                break;
            }
        }
    }
    EXPECT_TRUE(anyMatch);
}

TEST_F(MCPE2ETest, ListDocuments_FilterByExtensionTypeAndSorting) {
    auto d1 = addTextDoc("readme.md", "# Readme\n", "text/markdown");
    auto d2 = addTextDoc("data.json", "{\"k\":\"v\"}\n", "application/json");
    std::vector<std::byte> bin(16, std::byte{0x01});
    auto d3 = addBinaryDoc("blob.bin", bin, "application/octet-stream");

    json args = {{"limit", 10},
                 {"extension", "md"},
                 {"type", "text"},
                 {"sort_by", "name"},
                 {"sort_order", "asc"}};

    auto resp = server->testListDocuments(args);
    ASSERT_TRUE(resp.is_object());
    ASSERT_TRUE(resp.contains("documents"));
    const auto& docs = resp["documents"];
    ASSERT_TRUE(docs.is_array());
    ASSERT_FALSE(docs.empty());

    for (const auto& item : docs) {
        ASSERT_TRUE(item.is_object());
        ASSERT_TRUE(item.contains("extension"));
        auto ext = item["extension"].get<std::string>();
        EXPECT_TRUE(ext == ".md" || ext == "md");
        ASSERT_TRUE(item.contains("file_type"));
        EXPECT_EQ(item["file_type"].get<std::string>(), "text");
    }

    ASSERT_TRUE(resp.contains("sort_by"));
    EXPECT_EQ(resp["sort_by"].get<std::string>(), "name");
}

TEST_F(MCPE2ETest, RetrieveDocument_GraphTraversal_DepthBounded) {
    auto a = addTextDoc("docA.txt", "A content", "text/plain");
    auto b = addTextDoc("docB.txt", "B content", "text/plain"); // same dir -> related

    json args = {{"hash", a.hash}, {"graph", true}, {"depth", 2}, {"include_content", false}};

    auto resp = server->testRetrieveDocument(args);
    ASSERT_TRUE(resp.is_object());
    ASSERT_TRUE(resp.contains("graph_enabled"));
    EXPECT_TRUE(resp["graph_enabled"].get<bool>());

    ASSERT_TRUE(resp.contains("document"));
    EXPECT_EQ(resp["document"]["hash"].get<std::string>(), a.hash);

    ASSERT_TRUE(resp.contains("related_documents"));
    const auto& rel = resp["related_documents"];
    ASSERT_TRUE(rel.is_array());
    ASSERT_FALSE(rel.empty());
    for (const auto& rd : rel) {
        ASSERT_TRUE(rd.is_object());
        ASSERT_TRUE(rd.contains("distance"));
        int dist = rd["distance"].get<int>();
        EXPECT_LE(dist, 2);
    }
}

TEST_F(MCPE2ETest, UpdateMetadata_ByHash_MultiplePairs) {
    auto a = addTextDoc("meta.md", "metadata test", "text/markdown");

    json args = {{"hash", a.hash},
                 {"metadata", json::array({"status=active", "owner=tester"})},
                 {"verbose", true}};

    auto resp = server->testUpdateDocumentMetadata(args);
    ASSERT_TRUE(resp.is_object());
    ASSERT_TRUE(resp.contains("updates_applied"));
    EXPECT_EQ(resp["updates_applied"].get<size_t>(), 2u);
    ASSERT_TRUE(resp.contains("document_id"));
    int64_t docId = resp["document_id"].get<int64_t>();

    auto status = repo->getMetadata(docId, "status");
    ASSERT_TRUE(status.has_value());
    ASSERT_TRUE(status.value().has_value());
    EXPECT_EQ(status.value()->asString(), "active");
}

TEST_F(MCPE2ETest, GetStats_FileTypesBreakdown_ReturnsData) {
    // Ensure at least one text and one binary file exist
    (void)addTextDoc("stats.md", "stats", "text/markdown");
    std::vector<std::byte> bin(8, std::byte{0x02});
    (void)addBinaryDoc("stats.bin", bin, "application/octet-stream");

    json args = {{"file_types", true}, {"detailed", true}};

    auto resp = server->testGetStats(args);
    ASSERT_TRUE(resp.is_object());
    ASSERT_TRUE(resp.contains("total_objects"));
    ASSERT_TRUE(resp.contains("file_type_breakdown"));
    const auto& fbd = resp["file_type_breakdown"];
    ASSERT_TRUE(fbd.is_object());
    ASSERT_TRUE(fbd.contains("file_type_distribution"));
    ASSERT_TRUE(fbd["file_type_distribution"].is_array());
}
