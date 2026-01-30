// MCP Content-Length Framing Tests
// Tests for LSP-style Content-Length framing compatibility
// These verify the transport can receive Content-Length framed messages
// for backward compatibility with clients that use LSP-style framing.

#include <catch2/catch_test_macros.hpp>

#include <nlohmann/json.hpp>
#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

#include <yams/mcp/error_handling.h>
#include <yams/mcp/mcp_server.h>

using namespace yams::mcp;
using test_json = nlohmann::json;

namespace {

// MCP stdio spec: newline-delimited JSON (NDJSON)
std::string frameMessage(const test_json& j) {
    return j.dump() + "\n";
}

// Frame a message using Content-Length header (LSP-style)
std::string frameContentLength(const test_json& j) {
    std::string payload = j.dump();
    return "Content-Length: " + std::to_string(payload.size()) + "\r\n\r\n" + payload;
}

std::string frameContentLengthRaw(const std::string& raw) {
    return "Content-Length: " + std::to_string(raw.size()) + "\r\n\r\n" + raw;
}

class StdioTransportFixture {
public:
    std::unique_ptr<StdioTransport> transport;
    std::stringstream inputStream;
    std::stringstream outputStream;
    std::streambuf* originalCout;
    std::streambuf* originalCin;

    StdioTransportFixture() {
        // Save original cout/cin
        originalCout = std::cout.rdbuf();
        originalCin = std::cin.rdbuf();

        // Redirect cout to our stream
        std::cout.rdbuf(outputStream.rdbuf());

        transport = std::make_unique<StdioTransport>();
    }

    ~StdioTransportFixture() {
        // Restore original cout/cin
        std::cout.rdbuf(originalCout);
        std::cin.rdbuf(originalCin);
    }

    void setInput(const std::string& input) {
        inputStream.str(input);
        inputStream.clear();
        std::cin.rdbuf(inputStream.rdbuf());
    }

    std::string getOutput() { return outputStream.str(); }

    void clearOutput() {
        outputStream.str("");
        outputStream.clear();
    }
};

} // namespace

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Receive Content-Length framed message",
                 "[mcp][transport][stdio][framing][content-length][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "initialize"}, {"id", 1}};

    setInput(frameContentLength(testMessage));

    auto result = transport->receive();

    REQUIRE(result.has_value());
    const test_json& received = result.value();
    CHECK(received["jsonrpc"] == "2.0");
    CHECK(received["method"] == "initialize");
    CHECK(received["id"] == 1);
}

TEST_CASE_METHOD(StdioTransportFixture,
                 "ContentLength - Content-Length framing is case-insensitive",
                 "[mcp][transport][stdio][framing][content-length][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};
    std::string payload = testMessage.dump();

    // Test lowercase header name
    std::string input = "content-length: " + std::to_string(payload.size()) + "\r\n\r\n" + payload;
    setInput(input);

    auto result = transport->receive();

    REQUIRE(result.has_value());
    CHECK(result.value()["method"] == "test");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Content-Length with extra headers",
                 "[mcp][transport][stdio][framing][content-length][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};
    std::string payload = testMessage.dump();

    // Content-Length with additional headers (should be ignored)
    std::string input = "Content-Length: " + std::to_string(payload.size()) + "\r\n" +
                        "Content-Type: application/json\r\n" + "\r\n" + payload;
    setInput(input);

    auto result = transport->receive();

    REQUIRE(result.has_value());
    CHECK(result.value()["method"] == "test");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Content-Length with wrong length",
                 "[mcp][transport][stdio][framing][content-length][error][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};
    std::string payload = testMessage.dump();

    // Wrong Content-Length (too short)
    std::string input = "Content-Length: 5\r\n\r\n" + payload;
    setInput(input);

    auto result = transport->receive();

    // Should fail because we can't read the full payload
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Missing Content-Length value",
                 "[mcp][transport][stdio][framing][content-length][error][catch2]") {
    std::string input = "Content-Length: \r\n\r\n{\"test\": true}";
    setInput(input);

    auto result = transport->receive();

    // Should handle gracefully (likely treat as error or skip)
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Zero Content-Length",
                 "[mcp][transport][stdio][framing][content-length][error][catch2]") {
    std::string input = "Content-Length: 0\r\n\r\n";
    setInput(input);

    auto result = transport->receive();

    // Zero-length content should be handled
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Content-Length with whitespace",
                 "[mcp][transport][stdio][framing][content-length][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};
    std::string payload = testMessage.dump();

    // Content-Length with leading/trailing whitespace
    std::string input =
        "Content-Length:   " + std::to_string(payload.size()) + "  \r\n\r\n" + payload;
    setInput(input);

    auto result = transport->receive();

    REQUIRE(result.has_value());
    CHECK(result.value()["method"] == "test");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Receive multiple Content-Length messages",
                 "[mcp][transport][stdio][framing][content-length][catch2]") {
    test_json message1 = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "test1"}};
    test_json message2 = {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "test2"}};

    setInput(frameContentLength(message1) + frameContentLength(message2));

    auto result1 = transport->receive();
    REQUIRE(result1.has_value());
    CHECK(result1.value()["id"] == 1);
    CHECK(result1.value()["method"] == "test1");

    auto result2 = transport->receive();
    REQUIRE(result2.has_value());
    CHECK(result2.value()["id"] == 2);
    CHECK(result2.value()["method"] == "test2");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Mixed NDJSON and Content-Length input",
                 "[mcp][transport][stdio][framing][mixed][catch2]") {
    test_json message1 = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "ndjson"}};
    test_json message2 = {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "content-length"}};

    // First message: NDJSON, Second: Content-Length
    setInput(frameMessage(message1) + frameContentLength(message2));

    auto result1 = transport->receive();
    REQUIRE(result1.has_value());
    CHECK(result1.value()["method"] == "ndjson");

    auto result2 = transport->receive();
    REQUIRE(result2.has_value());
    CHECK(result2.value()["method"] == "content-length");
}

TEST_CASE_METHOD(StdioTransportFixture,
                 "ContentLength - Output is always NDJSON regardless of input framing",
                 "[mcp][transport][stdio][framing][output][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};

    // Receive a Content-Length framed message
    setInput(frameContentLength(testMessage));
    auto result = transport->receive();
    REQUIRE(result.has_value());

    clearOutput();

    // Send a response
    test_json response = {{"jsonrpc", "2.0"}, {"id", 1}, {"result", {{"success", true}}}};
    transport->send(response);
    std::cout.flush();

    std::string output = getOutput();

    // Output should be NDJSON (newline-terminated, no Content-Length)
    CHECK(output.back() == '\n');
    CHECK(output.find("Content-Length:") == std::string::npos);
    CHECK(output.find("\r\n\r\n") == std::string::npos);

    // Should be valid JSON
    auto parsed = test_json::parse(output.substr(0, output.size() - 1));
    CHECK(parsed["result"]["success"] == true);
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Large Content-Length message",
                 "[mcp][transport][stdio][framing][content-length][large][catch2]") {
    // Create a large payload
    std::string largeData(10000, 'x');
    test_json testMessage = {
        {"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}, {"data", largeData}};

    setInput(frameContentLength(testMessage));

    auto result = transport->receive();

    REQUIRE(result.has_value());
    CHECK(result.value()["method"] == "test");
    CHECK(result.value()["data"].get<std::string>().size() == 10000);
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Unicode in Content-Length message",
                 "[mcp][transport][stdio][framing][content-length][unicode][catch2]") {
    test_json testMessage = {
        {"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}, {"message", "Hello 世界 🌍"}};

    setInput(frameContentLength(testMessage));

    auto result = transport->receive();

    REQUIRE(result.has_value());
    CHECK(result.value()["message"] == "Hello 世界 🌍");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Content-Length framing mode persists",
                 "[mcp][transport][stdio][framing][persistence][catch2]") {
    test_json message1 = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "first"}};
    test_json message2 = {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "second"}};

    // First message: Content-Length
    setInput(frameContentLength(message1));
    auto result1 = transport->receive();
    REQUIRE(result1.has_value());

    // Second message: NDJSON (after Content-Length, transport should still handle both)
    setInput(frameMessage(message2));
    auto result2 = transport->receive();

    // This tests that the transport doesn't get stuck in Content-Length mode
    // and can still handle NDJSON after receiving Content-Length
    REQUIRE(result2.has_value());
    CHECK(result2.value()["method"] == "second");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Empty lines before Content-Length",
                 "[mcp][transport][stdio][framing][content-length][edge][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};
    std::string payload = testMessage.dump();

    // Empty lines before the actual message
    std::string input =
        "\n\n\nContent-Length: " + std::to_string(payload.size()) + "\r\n\r\n" + payload;
    setInput(input);

    auto result = transport->receive();

    // Should skip empty lines and parse the Content-Length message
    REQUIRE(result.has_value());
    CHECK(result.value()["method"] == "test");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Malformed Content-Length header",
                 "[mcp][transport][stdio][framing][content-length][error][catch2]") {
    // Missing colon
    setInput("Content-Length 10\r\n\r\n{\"test\": 1}");
    auto result = transport->receive();
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Content-Length with negative value",
                 "[mcp][transport][stdio][framing][content-length][error][catch2]") {
    // Test that negative Content-Length values are rejected
    // Note: std::stoull("-1") does NOT throw - it returns a very large unsigned value
    // due to unsigned integer overflow. The transport now explicitly checks for
    // negative values and treats them as invalid (contentLength = 0).

    // Verify std::stoull behavior with negative values
    std::size_t rawStoull = static_cast<std::size_t>(std::stoull("-1"));
    CHECK(rawStoull == std::numeric_limits<std::size_t>::max()); // Demonstrates the issue

    // Now test the actual transport - it should reject negative values
    setInput("Content-Length: -1\r\n\r\n{\"test\": 1}");
    auto result = transport->receive();
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::InvalidData);
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Content-Length with non-numeric value",
                 "[mcp][transport][stdio][framing][content-length][error][catch2]") {
    setInput("Content-Length: abc\r\n\r\n{\"test\": 1}");
    auto result = transport->receive();
    REQUIRE_FALSE(result.has_value());
}

// ============================================================================
// Real Client Simulation Tests
// ============================================================================

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Simulate OpenCode handshake",
                 "[mcp][transport][stdio][framing][client][opencode][catch2]") {
    // OpenCode typically sends NDJSON, but let's test it can handle both

    // Step 1: Initialize request (NDJSON)
    test_json initRequest = {{"jsonrpc", "2.0"},
                             {"id", 1},
                             {"method", "initialize"},
                             {"params",
                              {{"protocolVersion", "2025-11-25"},
                               {"clientInfo", {{"name", "opencode"}, {"version", "1.0.0"}}},
                               {"capabilities", test_json::object()}}}};

    setInput(frameMessage(initRequest));
    auto result1 = transport->receive();
    REQUIRE(result1.has_value());
    CHECK(result1.value()["method"] == "initialize");

    // Step 2: Send response (should be NDJSON)
    clearOutput();
    test_json initResponse = {{"jsonrpc", "2.0"},
                              {"id", 1},
                              {"result",
                               {{"protocolVersion", "2025-11-25"},
                                {"serverInfo", {{"name", "yams-mcp"}, {"version", "1.0.0"}}},
                                {"capabilities", test_json::object()}}}};
    transport->send(initResponse);
    std::cout.flush();

    std::string output = getOutput();
    CHECK(output.find("Content-Length:") == std::string::npos);
    CHECK(output.back() == '\n');

    // Step 3: tools/list request (NDJSON)
    test_json toolsRequest = {
        {"jsonrpc", "2.0"}, {"id", 2}, {"method", "tools/list"}, {"params", test_json::object()}};

    setInput(frameMessage(toolsRequest));
    auto result2 = transport->receive();
    REQUIRE(result2.has_value());
    CHECK(result2.value()["method"] == "tools/list");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Simulate client switching framing modes",
                 "[mcp][transport][stdio][framing][client][switch][catch2]") {
    // Test a hypothetical client that starts with Content-Length then switches to NDJSON

    test_json message1 = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "first"}};
    test_json message2 = {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "second"}};
    test_json message3 = {{"jsonrpc", "2.0"}, {"id", 3}, {"method", "third"}};

    // Content-Length, NDJSON, Content-Length
    setInput(frameContentLength(message1) + frameMessage(message2) + frameContentLength(message3));

    auto result1 = transport->receive();
    REQUIRE(result1.has_value());
    CHECK(result1.value()["method"] == "first");

    auto result2 = transport->receive();
    REQUIRE(result2.has_value());
    CHECK(result2.value()["method"] == "second");

    auto result3 = transport->receive();
    REQUIRE(result3.has_value());
    CHECK(result3.value()["method"] == "third");
}

TEST_CASE_METHOD(StdioTransportFixture, "ContentLength - Rapid alternating framing",
                 "[mcp][transport][stdio][framing][rapid][catch2]") {
    // Rapidly alternate between NDJSON and Content-Length
    std::string input;
    for (int i = 0; i < 10; ++i) {
        test_json msg = {{"jsonrpc", "2.0"}, {"id", i}, {"method", "test"}};
        if (i % 2 == 0) {
            input += frameMessage(msg);
        } else {
            input += frameContentLength(msg);
        }
    }

    setInput(input);

    for (int i = 0; i < 10; ++i) {
        auto result = transport->receive();
        REQUIRE(result.has_value());
        CHECK(result.value()["id"] == i);
    }
}
