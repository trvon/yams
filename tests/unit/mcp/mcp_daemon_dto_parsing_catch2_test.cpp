// MCP daemon DTO parsing tests
// Focus: exercise readStringArray() via real DTO fromJson() implementations.

#include <catch2/catch_test_macros.hpp>

#include <catch2/catch_approx.hpp>

#include <string>

#include <nlohmann/json.hpp>

#include <yams/mcp/tool_registry.h>

using nlohmann::json;

TEST_CASE("MCP DTO parsing - SearchRequest accepts string/array/null for include_patterns/tags",
          "[mcp][dto][search][catch2]") {
    using yams::mcp::MCPSearchRequest;

    {
        const auto r = MCPSearchRequest::fromJson(json{{"query", "q"}});
        CHECK(r.includePatterns.empty());
        CHECK(r.tags.empty());
    }

    {
        const auto r = MCPSearchRequest::fromJson(
            json{{"query", "q"}, {"include_patterns", nullptr}, {"tags", nullptr}});
        CHECK(r.includePatterns.empty());
        CHECK(r.tags.empty());
    }

    {
        const auto r = MCPSearchRequest::fromJson(
            json{{"query", "q"}, {"include_patterns", "*.cpp"}, {"tags", "pinned"}});
        CHECK(r.includePatterns == std::vector<std::string>{"*.cpp"});
        CHECK(r.tags == std::vector<std::string>{"pinned"});
    }

    {
        const auto r = MCPSearchRequest::fromJson(
            json{{"query", "q"},
                 {"include_patterns", json::array({"a.*", 1, true, "b.*", nullptr})},
                 {"tags", json::array({"t1", 2, false, "t2"})}});
        CHECK(r.includePatterns == std::vector<std::string>{"a.*", "b.*"});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
    }
}

TEST_CASE("MCP DTO parsing - SearchRequest tolerant numeric parsing and context override",
          "[mcp][dto][search][catch2]") {
    using yams::mcp::MCPSearchRequest;

    // normalize_query + tolerant parsing for limit/similarity and context override semantics.
    json j = {
        {"query", "  hi\nthere  "}, {"limit", "5"},         {"similarity", "0.5"}, {"context", "3"},
        {"before_context", 111},    {"after_context", 222}, {"path", "src"},
    };

    const auto r = MCPSearchRequest::fromJson(j);
    CHECK(r.query == "hi there");
    CHECK(r.limit == 5u);
    CHECK(r.similarity == Catch::Approx(0.5));

    // When context > 0, it overrides before/after.
    CHECK(r.context == 3);
    CHECK(r.beforeContext == 3);
    CHECK(r.afterContext == 3);

    // path_pattern falls back to "path".
    CHECK(r.pathPattern == "src");
}

TEST_CASE("MCP DTO parsing - GrepRequest accepts string/array for paths/include_patterns/tags",
          "[mcp][dto][grep][catch2]") {
    using yams::mcp::MCPGrepRequest;

    {
        const auto r = MCPGrepRequest::fromJson(json{{"pattern", "re"}});
        CHECK(r.paths.empty());
        CHECK(r.includePatterns.empty());
        CHECK(r.tags.empty());
    }

    {
        const auto r = MCPGrepRequest::fromJson(json{{"pattern", "re"},
                                                     {"paths", "src/main.cpp"},
                                                     {"include_patterns", "*.h"},
                                                     {"tags", "pinned"}});
        CHECK(r.paths == std::vector<std::string>{"src/main.cpp"});
        CHECK(r.includePatterns == std::vector<std::string>{"*.h"});
        CHECK(r.tags == std::vector<std::string>{"pinned"});
    }

    {
        const auto r = MCPGrepRequest::fromJson(
            json{{"pattern", "re"},
                 {"paths", json::array({"a", 1, "b", false})},
                 {"include_patterns", json::array({"*.cpp", {"x", 1}, "*.hpp"})},
                 {"tags", json::array({"t1", 0, "t2"})}});
        CHECK(r.paths == std::vector<std::string>{"a", "b"});
        CHECK(r.includePatterns == std::vector<std::string>{"*.cpp", "*.hpp"});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
    }
}

TEST_CASE("MCP DTO parsing - GrepRequest max_count tolerates numeric strings",
          "[mcp][dto][grep][catch2]") {
    using yams::mcp::MCPGrepRequest;

    {
        const auto r = MCPGrepRequest::fromJson(json{{"pattern", "re"}, {"max_count", "7"}});
        REQUIRE(r.maxCount.has_value());
        CHECK(r.maxCount.value() == 7);
    }

    {
        // Empty or invalid values should not enable maxCount.
        const auto r = MCPGrepRequest::fromJson(json{{"pattern", "re"}, {"max_count", ""}});
        CHECK_FALSE(r.maxCount.has_value());
    }

    {
        const auto r = MCPGrepRequest::fromJson(json{{"pattern", "re"}, {"max_count", "nope"}});
        CHECK_FALSE(r.maxCount.has_value());
    }
}

TEST_CASE("MCP DTO parsing - DownloadRequest accepts string/array for headers/tags",
          "[mcp][dto][download][catch2]") {
    using yams::mcp::MCPDownloadRequest;

    {
        const auto r = MCPDownloadRequest::fromJson(json{{"url", "https://e"}});
        CHECK(r.headers.empty());
        CHECK(r.tags.empty());
    }

    {
        const auto r = MCPDownloadRequest::fromJson(
            json{{"url", "https://e"}, {"headers", "Accept: */*"}, {"tags", "docs"}});
        CHECK(r.headers == std::vector<std::string>{"Accept: */*"});
        CHECK(r.tags == std::vector<std::string>{"docs"});
    }

    {
        const auto r = MCPDownloadRequest::fromJson(
            json{{"url", "https://e"},
                 {"headers", json::array({"A: 1", 2, false, "B: 2"})},
                 {"tags", json::array({"t1", json::object(), nullptr, "t2"})}});
        CHECK(r.headers == std::vector<std::string>{"A: 1", "B: 2"});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
    }
}

TEST_CASE("MCP DTO parsing - DownloadRequest metadata stringifies values",
          "[mcp][dto][download][catch2]") {
    using yams::mcp::MCPDownloadRequest;

    json j = {
        {"url", "https://e"},
        {"metadata", json{{"s", "x"},
                          {"i", -12},
                          {"u", 12},
                          {"b", true},
                          {"f", 1.5},
                          {"arr", json::array({1, 2})},
                          {"obj", json{{"k", 1}}}}},
    };

    const auto r = MCPDownloadRequest::fromJson(j);
    REQUIRE(r.metadata.contains("s"));
    CHECK(r.metadata.at("s") == "x");
    CHECK(r.metadata.at("i") == "-12");
    CHECK(r.metadata.at("u") == "12");
    CHECK(r.metadata.at("b") == "true");
    CHECK(r.metadata.at("arr") == json::array({1, 2}).dump());
    CHECK(r.metadata.at("obj") == json{{"k", 1}}.dump());
}

TEST_CASE("MCP DTO parsing - DownloadResponse preserves indexed flag",
          "[mcp][dto][download][catch2]") {
    using yams::mcp::MCPDownloadResponse;

    {
        const auto r = MCPDownloadResponse::fromJson(
            json{{"url", "https://e"}, {"success", true}, {"indexed", true}});
        CHECK(r.success);
        CHECK(r.indexed);
    }

    {
        MCPDownloadResponse resp;
        resp.url = "https://e";
        resp.success = true;
        resp.indexed = false;

        const auto j = resp.toJson();
        REQUIRE(j.contains("indexed"));
        CHECK(j["indexed"].is_boolean());
        CHECK_FALSE(j["indexed"].get<bool>());
    }
}

TEST_CASE("MCP DTO parsing - ListDocumentsRequest accepts string/array for tags",
          "[mcp][dto][list][catch2]") {
    using yams::mcp::MCPListDocumentsRequest;

    {
        const auto r = MCPListDocumentsRequest::fromJson(json{{"pattern", "*.md"}});
        CHECK(r.tags.empty());
        CHECK_FALSE(r.matchAllTags);
    }

    {
        const auto r =
            MCPListDocumentsRequest::fromJson(json{{"pattern", "*.md"}, {"tags", "pinned"}});
        CHECK(r.tags == std::vector<std::string>{"pinned"});
    }

    {
        const auto r = MCPListDocumentsRequest::fromJson(
            json{{"pattern", "*.md"}, {"tags", json::array({"t1", 1, "t2", false})}});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
    }

    {
        const auto r = MCPListDocumentsRequest::fromJson(json{
            {"pattern", "*.md"}, {"tags", json::array({"t1", "t2"})}, {"match_all_tags", true}});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
        CHECK(r.matchAllTags);

        const auto j = r.toJson();
        REQUIRE(j.contains("match_all_tags"));
        CHECK(j["match_all_tags"].is_boolean());
        CHECK(j["match_all_tags"].get<bool>());
    }
}

TEST_CASE("MCP DTO parsing - UpdateMetadataRequest accepts string/array for names/tags/remove_tags",
          "[mcp][dto][update][catch2]") {
    using yams::mcp::MCPUpdateMetadataRequest;

    {
        const auto r = MCPUpdateMetadataRequest::fromJson(json{{"hash", "h"}});
        CHECK(r.names.empty());
        CHECK(r.tags.empty());
        CHECK(r.removeTags.empty());
    }

    {
        const auto r = MCPUpdateMetadataRequest::fromJson(
            json{{"hash", "h"}, {"names", "n1"}, {"tags", "t1"}, {"remove_tags", "t2"}});
        CHECK(r.names == std::vector<std::string>{"n1"});
        CHECK(r.tags == std::vector<std::string>{"t1"});
        CHECK(r.removeTags == std::vector<std::string>{"t2"});
    }

    {
        const auto r = MCPUpdateMetadataRequest::fromJson(
            json{{"hash", "h"},
                 {"names", json::array({"n1", 2, "n2"})},
                 {"tags", json::array({"t1", false, "t2"})},
                 {"remove_tags", json::array({nullptr, "rt1", json::object(), "rt2"})}});
        CHECK(r.names == std::vector<std::string>{"n1", "n2"});
        CHECK(r.tags == std::vector<std::string>{"t1", "t2"});
        CHECK(r.removeTags == std::vector<std::string>{"rt1", "rt2"});
    }
}

TEST_CASE("MCP DTO parsing - DeleteByNameRequest accepts string/array for names",
          "[mcp][dto][delete][catch2]") {
    using yams::mcp::MCPDeleteByNameRequest;

    {
        const auto r = MCPDeleteByNameRequest::fromJson(json{{"names", "a"}});
        CHECK(r.names == std::vector<std::string>{"a"});
    }

    {
        const auto r =
            MCPDeleteByNameRequest::fromJson(json{{"names", json::array({"a", 1, "b", false})}});
        CHECK(r.names == std::vector<std::string>{"a", "b"});
    }
}
