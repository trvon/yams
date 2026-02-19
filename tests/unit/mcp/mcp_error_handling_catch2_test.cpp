// MCP error handling helpers tests
// Focused coverage for include/yams/mcp/error_handling.h

#include <catch2/catch_test_macros.hpp>

#include <string>

#include <nlohmann/json.hpp>

#include <yams/core/types.h>
#include <yams/mcp/error_handling.h>

namespace {
using yams::ErrorCode;
using yams::mcp::json;
} // namespace

TEST_CASE("MCP JsonUtils - parse_json handles empty input", "[mcp][json][error_handling][catch2]") {
    const auto r = yams::mcp::json_utils::parse_json("");
    REQUIRE_FALSE(r);
    CHECK(r.error().code == ErrorCode::InvalidData);
    CHECK(r.error().message.find("Empty input") != std::string::npos);
}

TEST_CASE("MCP JsonUtils - parse_json reports parse errors",
          "[mcp][json][error_handling][catch2]") {
    const auto r = yams::mcp::json_utils::parse_json("{");
    REQUIRE_FALSE(r);
    CHECK(r.error().code == ErrorCode::InvalidData);
    CHECK(r.error().message.find("JSON parse error:") != std::string::npos);
    CHECK(r.error().message.find("at position") != std::string::npos);
}

TEST_CASE("MCP JsonUtils - validate_jsonrpc_message rejects invalid envelopes",
          "[mcp][jsonrpc][error_handling][catch2]") {
    {
        const auto r = yams::mcp::json_utils::validate_jsonrpc_message(json::array());
        REQUIRE_FALSE(r);
        CHECK(r.error().code == ErrorCode::InvalidData);
        CHECK(r.error().message.find("JSON object") != std::string::npos);
    }

    {
        const auto r = yams::mcp::json_utils::validate_jsonrpc_message(json::object());
        REQUIRE_FALSE(r);
        CHECK(r.error().code == ErrorCode::InvalidData);
        CHECK(r.error().message.find("jsonrpc") != std::string::npos);
    }

    {
        json msg = {{"jsonrpc", 2}};
        const auto r = yams::mcp::json_utils::validate_jsonrpc_message(msg);
        REQUIRE_FALSE(r);
        CHECK(r.error().code == ErrorCode::InvalidData);
        CHECK(r.error().message.find("version") != std::string::npos);
    }

    {
        json msg = {{"jsonrpc", "1.0"}};
        const auto r = yams::mcp::json_utils::validate_jsonrpc_message(msg);
        REQUIRE_FALSE(r);
        CHECK(r.error().code == ErrorCode::InvalidData);
        CHECK(r.error().message.find("version") != std::string::npos);
    }

    {
        json msg = {{"jsonrpc", "2.0"}};
        const auto r = yams::mcp::json_utils::validate_jsonrpc_message(msg);
        REQUIRE(r);
        CHECK(r.value().at("jsonrpc").get<std::string>() == "2.0");
    }
}

TEST_CASE("MCP JsonUtils - get_field reports missing/wrong type",
          "[mcp][json][error_handling][catch2]") {
    {
        json obj = {{"x", 5}};
        const auto r = yams::mcp::json_utils::get_field<int>(obj, "x");
        REQUIRE(r);
        CHECK(r.value() == 5);
    }

    {
        json obj = json::object();
        const auto r = yams::mcp::json_utils::get_field<int>(obj, "x");
        REQUIRE_FALSE(r);
        CHECK(r.error().code == ErrorCode::InvalidData);
        CHECK(r.error().message.find("Missing required field") != std::string::npos);
    }

    {
        json obj = {{"x", "not-an-int"}};
        const auto r = yams::mcp::json_utils::get_field<int>(obj, "x");
        REQUIRE_FALSE(r);
        CHECK(r.error().code == ErrorCode::InvalidData);
        CHECK(r.error().message.find("Invalid field") != std::string::npos);
    }
}
