// Additional MCP daemon DTO parsing tests
// Focus: exercise more tool_registry.cpp branches and optionals.

#include <catch2/catch_test_macros.hpp>

#include <catch2/catch_approx.hpp>

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <yams/mcp/tool_registry.h>

using nlohmann::json;

TEST_CASE("MCP DTO parsing - SearchRequest tolerant parsing for null/ints and before/after when no "
          "context",
          "[mcp][dto][search][catch2]") {
    using yams::mcp::MCPSearchRequest;

    // null values should fall back to defaults
    {
        const auto r = MCPSearchRequest::fromJson(
            json{{"query", "q"}, {"limit", nullptr}, {"similarity", nullptr}});
        CHECK(r.limit == 10u);
        CHECK(r.similarity == Catch::Approx(0.7));
    }

    // integer values should be accepted
    {
        const auto r = MCPSearchRequest::fromJson(json{{"query", "q"},
                                                       {"limit", 12},
                                                       {"similarity", 1},
                                                       {"before_context", "2"},
                                                       {"after_context", "4"}});
        CHECK(r.limit == 12u);
        CHECK(r.similarity == 1.0);
        CHECK(r.context == 0);
        CHECK(r.beforeContext == 2);
        CHECK(r.afterContext == 4);
    }

    // unsupported types should be ignored (default)
    {
        const auto r = MCPSearchRequest::fromJson(json{{"query", "q"}, {"context", true}});
        CHECK(r.context == 0);
        CHECK(r.beforeContext == 0);
        CHECK(r.afterContext == 0);
    }
}

TEST_CASE(
    "MCP DTO parsing - SearchResponse toJson supports paths_only and score_breakdown optionals",
    "[mcp][dto][search][catch2]") {
    using yams::mcp::MCPSearchResponse;

    {
        MCPSearchResponse r;
        r.total = 3;
        r.type = "hybrid";
        r.executionTimeMs = 7;
        r.paths = {"a.md", "b.md"};
        MCPSearchResponse::Result ignored;
        ignored.id = "ignored";
        r.results.push_back(std::move(ignored));

        const auto j = r.toJson();
        CHECK(j["total"] == 3);
        CHECK(j["type"] == "hybrid");
        CHECK(j["execution_time_ms"] == 7);
        CHECK(j["paths"] == json::array({"a.md", "b.md"}));

        // paths_only mode should return early without emitting results
        CHECK_FALSE(j.contains("results"));
    }

    {
        MCPSearchResponse r;
        r.total = 1;
        r.type = "hybrid";
        r.executionTimeMs = 1;

        MCPSearchResponse::Result item;
        item.id = "id1";
        item.hash = "h";
        item.title = "t";
        item.path = "p";
        item.score = 0.42f;
        item.snippet = "s";
        item.diff = json{{"added", 1}};
        item.localInputFile = "/tmp/in.txt";
        item.vectorScore = 0.1f;
        item.structuralScore = 0.2f;
        r.results.push_back(item);

        const auto j = r.toJson();
        REQUIRE(j.contains("results"));
        REQUIRE(j["results"].is_array());
        REQUIRE(j["results"].size() == 1);
        const auto& out = j["results"][0];
        CHECK(out["id"] == "id1");
        CHECK(out["hash"] == "h");
        CHECK(out["title"] == "t");
        CHECK(out["path"] == "p");
        CHECK(out["score"] == Catch::Approx(0.42));
        CHECK(out["snippet"] == "s");
        CHECK(out["diff"] == json{{"added", 1}});
        CHECK(out["local_input_file"] == "/tmp/in.txt");
        REQUIRE(out.contains("score_breakdown"));
        CHECK(out["score_breakdown"].contains("vector_score"));
        CHECK(out["score_breakdown"].contains("structural_score"));
        CHECK_FALSE(out["score_breakdown"].contains("keyword_score"));
        CHECK_FALSE(out["score_breakdown"].contains("kg_entity_score"));
    }
}

TEST_CASE("MCP DTO parsing - SearchResponse fromJson parses optionals and breakdown",
          "[mcp][dto][search][catch2]") {
    using yams::mcp::MCPSearchResponse;

    const json fixture = {
        {"total", 1},
        {"type", "hybrid"},
        {"execution_time_ms", 9},
        {"results", json::array({json{{"id", "id1"},
                                      {"hash", "h"},
                                      {"title", "t"},
                                      {"path", "p"},
                                      {"score", 0.5},
                                      {"snippet", "sn"},
                                      {"diff", json{{"removed", 1}}},
                                      {"local_input_file", "/tmp/in"},
                                      {"score_breakdown", json{{"vector_score", 0.1},
                                                               {"keyword_score", 0.2},
                                                               {"structural_score", 0.3}}}}})},
    };

    const auto r = MCPSearchResponse::fromJson(fixture);
    CHECK(r.total == 1u);
    CHECK(r.results.size() == 1);
    CHECK(r.results[0].diff.has_value());
    CHECK(r.results[0].localInputFile.has_value());
    REQUIRE(r.results[0].vectorScore.has_value());
    CHECK(*r.results[0].vectorScore == Catch::Approx(0.1));
    REQUIRE(r.results[0].keywordScore.has_value());
    CHECK(*r.results[0].keywordScore == Catch::Approx(0.2));
    REQUIRE(r.results[0].structuralScore.has_value());
    CHECK(*r.results[0].structuralScore == Catch::Approx(0.3));
    CHECK_FALSE(r.results[0].kgEntityScore.has_value());
}

TEST_CASE("MCP DTO parsing - SessionPinRequest ignores non-object metadata",
          "[mcp][dto][session][catch2]") {
    using yams::mcp::MCPSessionPinRequest;

    const auto r = MCPSessionPinRequest::fromJson(
        json{{"path", "src"}, {"tags", json::array({"pinned"})}, {"metadata", "not-object"}});
    CHECK(r.path == "src");
    CHECK(r.tags == std::vector<std::string>{"pinned"});
    CHECK(r.metadata.is_object());
    CHECK(r.metadata.empty());
}

TEST_CASE("MCP DTO parsing - AddDirectoryRequest reads defaults and patterns",
          "[mcp][dto][add][catch2]") {
    using yams::mcp::MCPAddDirectoryRequest;

    {
        const auto r = MCPAddDirectoryRequest::fromJson(json{{"directory_path", "/tmp/x"}});
        CHECK(r.directoryPath == "/tmp/x");
        CHECK(r.collection.empty());
        CHECK(r.includePatterns.empty());
        CHECK(r.excludePatterns.empty());
        CHECK(r.tags.empty());
        CHECK(r.metadata.is_null());
        CHECK(r.recursive == true);
        CHECK(r.followSymlinks == false);
        CHECK(r.snapshotId.empty());
        CHECK(r.snapshotLabel.empty());
    }

    {
        const auto r = MCPAddDirectoryRequest::fromJson(
            json{{"directory_path", "d"},
                 {"include_patterns", "*.cpp"},
                 {"exclude_patterns", json::array({"a", 1, true, "b", nullptr})},
                 {"tags", json::array({"t1", 2, "t2", false})},
                 {"metadata", json{{"k", "v"}, {"n", 1}}},
                 {"recursive", false},
                 {"follow_symlinks", true},
                 {"snapshot_id", "sid"},
                 {"snapshot_label", "lab"}});
        CHECK(r.includePatterns == std::vector<std::string>{"*.cpp"});
        CHECK(r.excludePatterns == std::vector<std::string>{"a", "b"});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
        REQUIRE(r.metadata.is_object());
        CHECK(r.metadata["k"] == "v");
        CHECK(r.metadata["n"] == 1);
        CHECK(r.recursive == false);
        CHECK(r.followSymlinks == true);
        CHECK(r.snapshotId == "sid");
        CHECK(r.snapshotLabel == "lab");
    }
}

TEST_CASE("MCP DTO parsing - AddDirectoryResponse results only when array",
          "[mcp][dto][add][catch2]") {
    using yams::mcp::MCPAddDirectoryResponse;

    {
        const auto r = MCPAddDirectoryResponse::fromJson(
            json{{"files_processed", 3},
                 {"results", json::array({json{{"path", "a"}}, json{{"path", "b"}}})}});
        CHECK(r.filesProcessed == 3u);
        CHECK(r.results.size() == 2);
        CHECK(r.results[0]["path"] == "a");
    }

    {
        const auto r = MCPAddDirectoryResponse::fromJson(
            json{{"files_processed", 3}, {"results", json::object()}});
        CHECK(r.filesProcessed == 3u);
        CHECK(r.results.empty());
    }
}

TEST_CASE("MCP DTO parsing - UpdateMetadataResponse updated_hashes parsing and omission",
          "[mcp][dto][update][catch2]") {
    using yams::mcp::MCPUpdateMetadataResponse;

    {
        const auto r = MCPUpdateMetadataResponse::fromJson(
            json{{"success", true},
                 {"matched", 2},
                 {"updated", 1},
                 {"updated_hashes", json::array({"h1", 2, "h2", false})}});
        CHECK(r.success == true);
        CHECK(r.matched == 2u);
        CHECK(r.updated == 1u);
        CHECK(r.updatedHashes == std::vector<std::string>{"h1", "h2"});
    }

    {
        MCPUpdateMetadataResponse r;
        r.success = true;
        const auto j = r.toJson();
        CHECK(j["success"].get<bool>() == true);
        CHECK_FALSE(j.contains("updated_hashes"));
    }

    {
        MCPUpdateMetadataResponse r;
        r.success = true;
        r.updatedHashes = {"h1"};
        const auto j = r.toJson();
        REQUIRE(j.contains("updated_hashes"));
        CHECK(j["updated_hashes"] == json::array({"h1"}));
    }
}

TEST_CASE("MCP DTO parsing - DeleteByNameResponse deleted accepts string/array/mixed",
          "[mcp][dto][delete][catch2]") {
    using yams::mcp::MCPDeleteByNameResponse;

    {
        const auto r = MCPDeleteByNameResponse::fromJson(
            json{{"deleted", "one"}, {"count", 1}, {"dry_run", true}});
        CHECK(r.deleted == std::vector<std::string>{"one"});
        CHECK(r.count == 1u);
        CHECK(r.dryRun == true);
    }

    {
        const auto r = MCPDeleteByNameResponse::fromJson(
            json{{"deleted", json::array({"a", 1, "b", false})}, {"count", 2}});
        CHECK(r.deleted == std::vector<std::string>{"a", "b"});
        CHECK(r.count == 2u);
        CHECK(r.dryRun == false);
    }
}

TEST_CASE("MCP DTO parsing - Restore requests support include/exclude patterns",
          "[mcp][dto][restore][catch2]") {
    using yams::mcp::MCPRestoreCollectionRequest;
    using yams::mcp::MCPRestoreRequest;
    using yams::mcp::MCPRestoreSnapshotRequest;

    {
        const auto r = MCPRestoreCollectionRequest::fromJson(
            json{{"collection", "c"}, {"output_directory", "out"}});
        CHECK(r.layoutTemplate == "{path}");
        CHECK(r.overwrite == false);
        CHECK(r.createDirs == true);
        CHECK(r.dryRun == false);
        CHECK(r.includePatterns.empty());
        CHECK(r.excludePatterns.empty());
    }

    {
        const auto r = MCPRestoreSnapshotRequest::fromJson(
            json{{"snapshot_id", "sid"},
                 {"output_directory", "out"},
                 {"include_patterns", "*.md"},
                 {"exclude_patterns", json::array({"a", 7, "b"})}});
        CHECK(r.layoutTemplate == "{path}");
        CHECK(r.includePatterns == std::vector<std::string>{"*.md"});
        CHECK(r.excludePatterns == std::vector<std::string>{"a", "b"});
    }

    {
        const auto r =
            MCPRestoreRequest::fromJson(json{{"collection", "c"},
                                             {"snapshot_id", "sid"},
                                             {"snapshot_label", "lab"},
                                             {"output_directory", "out"},
                                             {"dry_run", true},
                                             {"include_patterns", json::array({"a", 1, "b"})}});
        CHECK(r.collection == "c");
        CHECK(r.snapshotId == "sid");
        CHECK(r.snapshotLabel == "lab");
        CHECK(r.outputDirectory == "out");
        CHECK(r.dryRun == true);
        CHECK(r.includePatterns == std::vector<std::string>{"a", "b"});
    }
}

TEST_CASE("MCP DTO parsing - Restore responses parse restored_paths",
          "[mcp][dto][restore][catch2]") {
    using yams::mcp::MCPRestoreCollectionResponse;
    using yams::mcp::MCPRestoreResponse;
    using yams::mcp::MCPRestoreSnapshotResponse;

    {
        const auto r = MCPRestoreCollectionResponse::fromJson(
            json{{"files_restored", 2},
                 {"restored_paths", json::array({"p1", 1, "p2"})},
                 {"dry_run", false}});
        CHECK(r.filesRestored == 2u);
        CHECK(r.restoredPaths == std::vector<std::string>{"p1", "p2"});
        CHECK(r.dryRun == false);
    }

    {
        const auto r = MCPRestoreSnapshotResponse::fromJson(
            json{{"files_restored", 1}, {"restored_paths", "only"}});
        CHECK(r.filesRestored == 1u);
        CHECK(r.restoredPaths == std::vector<std::string>{"only"});
        CHECK(r.dryRun == false);
    }

    {
        const auto r =
            MCPRestoreResponse::fromJson(json{{"files_restored", 0},
                                              {"restored_paths", json::array({nullptr, "p"})},
                                              {"dry_run", true}});
        CHECK(r.filesRestored == 0u);
        CHECK(r.restoredPaths == std::vector<std::string>{"p"});
        CHECK(r.dryRun == true);
    }
}

TEST_CASE("MCP DTO parsing - ListCollectionsResponse ignores non-strings",
          "[mcp][dto][list_collections][catch2]") {
    using yams::mcp::MCPListCollectionsResponse;

    const auto r = MCPListCollectionsResponse::fromJson(
        json{{"collections", json::array({"a", 123, "b", false})}});
    CHECK(r.collections == std::vector<std::string>{"a", "b"});
}

TEST_CASE("MCP DTO parsing - GetByName and CatDocument roundtrip defaults",
          "[mcp][dto][get][catch2]") {
    using yams::mcp::MCPCatDocumentRequest;
    using yams::mcp::MCPCatDocumentResponse;
    using yams::mcp::MCPGetByNameRequest;
    using yams::mcp::MCPGetByNameResponse;

    {
        const auto req = MCPGetByNameRequest::fromJson(json{{"name", "README.md"}});
        const auto j = req.toJson();
        CHECK(j["name"] == "README.md");
        CHECK(j["subpath"].get<bool>() == true);
        CHECK(j["extract_text"].get<bool>() == true);
        CHECK(MCPGetByNameRequest::fromJson(j).toJson() == j);
    }

    {
        const json fixture = {{"hash", "sha256:deadbeef"},    {"name", "README.md"},
                              {"path", "docs/README.md"},     {"size", 123},
                              {"mime_type", "text/markdown"}, {"content", "# hi\n"}};
        const auto resp = MCPGetByNameResponse::fromJson(fixture);
        CHECK(resp.toJson() == fixture);
        CHECK(MCPGetByNameResponse::fromJson(resp.toJson()).toJson() == fixture);
    }

    {
        const auto req = MCPCatDocumentRequest::fromJson(json{{"hash", "sha256:abc123"}});
        const auto j = req.toJson();
        CHECK(j["hash"] == "sha256:abc123");
        CHECK(j["extract_text"].get<bool>() == true);
        CHECK(MCPCatDocumentRequest::fromJson(j).toJson() == j);
    }

    {
        const json fixture = {
            {"content", "hello"}, {"hash", "sha256:abc123"}, {"name", "README.md"}, {"size", 5}};
        const auto resp = MCPCatDocumentResponse::fromJson(fixture);
        CHECK(resp.toJson() == fixture);
        CHECK(MCPCatDocumentResponse::fromJson(resp.toJson()).toJson() == fixture);
    }
}

TEST_CASE("MCP DTO parsing - RetrieveDocumentResponse optionals omitted unless set",
          "[mcp][dto][retrieve][catch2]") {
    using yams::mcp::MCPRetrieveDocumentResponse;

    {
        const json fixture = {{"hash", "h"},
                              {"path", "out/file.bin"},
                              {"name", "file.bin"},
                              {"size", 10},
                              {"mime_type", "application/octet-stream"},
                              {"compressed", true},
                              {"compression_algorithm", nullptr},
                              {"compression_level", nullptr},
                              {"uncompressed_size", nullptr},
                              {"compressed_crc32", nullptr},
                              {"uncompressed_crc32", nullptr},
                              {"compression_header", nullptr},
                              {"graph_enabled", false},
                              {"related", json::array()}};
        const auto r = MCPRetrieveDocumentResponse::fromJson(fixture);
        CHECK_FALSE(r.content.has_value());
        CHECK_FALSE(r.compressionAlgorithm.has_value());
        const auto j = r.toJson();
        CHECK(j["compressed"].get<bool>() == true);
        CHECK_FALSE(j.contains("related"));
        CHECK_FALSE(j.contains("compression_algorithm"));
        CHECK(MCPRetrieveDocumentResponse::fromJson(j).toJson() == j);
    }

    {
        const json fixture = {{"hash", "h"},
                              {"path", "out/file.bin"},
                              {"name", "file.bin"},
                              {"size", 10},
                              {"mime_type", "application/octet-stream"},
                              {"compressed", true},
                              {"compression_algorithm", 3},
                              {"compression_level", 7},
                              {"uncompressed_size", 100},
                              {"compressed_crc32", 123456},
                              {"uncompressed_crc32", 654321},
                              {"compression_header", "hdr"},
                              {"content", "payload"},
                              {"graph_enabled", true},
                              {"related", json::array({json{{"hash", "r1"}}})}};
        const auto r = MCPRetrieveDocumentResponse::fromJson(fixture);
        REQUIRE(r.compressionAlgorithm.has_value());
        CHECK(*r.compressionAlgorithm == 3);
        REQUIRE(r.content.has_value());
        CHECK(*r.content == "payload");
        CHECK(r.graphEnabled == true);
        CHECK(r.related.size() == 1);
        CHECK(r.toJson() == fixture);
    }
}

TEST_CASE("MCP DTO parsing - StatsResponse optionals only emitted when non-empty",
          "[mcp][dto][stats][catch2]") {
    using yams::mcp::MCPStatsResponse;

    {
        const auto r = MCPStatsResponse::fromJson(json{{"total_objects", 1},
                                                       {"total_bytes", 2},
                                                       {"unique_hashes", 3},
                                                       {"deduplication_savings", 4},
                                                       {"file_types", json::array()},
                                                       {"additional_stats", json::object()}});
        CHECK(r.fileTypes.empty());
        CHECK(r.additionalStats.empty());
        const auto j = r.toJson();
        CHECK_FALSE(j.contains("file_types"));
        CHECK_FALSE(j.contains("additional_stats"));
        CHECK(MCPStatsResponse::fromJson(j).toJson() == j);
    }

    {
        const json fixture = {{"total_objects", 1},
                              {"total_bytes", 2},
                              {"unique_hashes", 3},
                              {"deduplication_savings", 4},
                              {"file_types", json::array({json{{"ext", ".md"}, {"count", 2}}})},
                              {"additional_stats", json{{"note", "ok"}}}};
        const auto r = MCPStatsResponse::fromJson(fixture);
        CHECK(r.fileTypes.size() == 1);
        CHECK(r.additionalStats.at("note") == "ok");
        CHECK(r.toJson() == fixture);
    }
}

TEST_CASE("MCP DTO parsing - GraphRequest relation_filters and tolerant numeric parsing",
          "[mcp][dto][graph][catch2]") {
    using yams::mcp::MCPGraphRequest;

    {
        const auto r = MCPGraphRequest::fromJson(json::object());
        CHECK(r.nodeId == -1);
        CHECK(r.depth == 1);
        CHECK(r.limit == 100u);
        CHECK(r.offset == 0u);
        CHECK(r.hydrateFully == true);
        CHECK(r.relationFilters.empty());
    }

    {
        const auto r = MCPGraphRequest::fromJson(
            json{{"node_id", "42"},
                 {"depth", "3"},
                 {"limit", "10"},
                 {"offset", "2"},
                 {"relation_filters", json::array({"imports", 1, "calls", false})}});
        CHECK(r.nodeId == 42);
        CHECK(r.depth == 3);
        CHECK(r.limit == 10u);
        CHECK(r.offset == 2u);
        CHECK(r.relationFilters == std::vector<std::string>{"imports", "calls"});
    }
}

TEST_CASE("MCP DTO parsing - GraphResponse optional emission", "[mcp][dto][graph][catch2]") {
    using yams::mcp::MCPGraphResponse;

    {
        MCPGraphResponse r;
        const auto j = r.toJson();
        REQUIRE(j.contains("origin"));
        REQUIRE(j.contains("connected_nodes"));
        CHECK_FALSE(j.contains("node_type_counts"));
        CHECK_FALSE(j.contains("warning"));
        CHECK(MCPGraphResponse::fromJson(j).toJson() == j);
    }

    {
        MCPGraphResponse r;
        r.nodeTypeCounts = json{{"binary.function", 2}};
        r.warning = "kg disabled";
        const auto j = r.toJson();
        CHECK(j["node_type_counts"] == json{{"binary.function", 2}});
        CHECK(j["warning"] == "kg disabled");
    }
}
