// MCP Stdio Transport tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

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

std::string frameRaw(const std::string& raw) {
    return raw + "\n";
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

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Initial state",
                 "[mcp][transport][stdio][catch2]") {
    CHECK(transport->isConnected());
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Send message",
                 "[mcp][transport][stdio][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};

    transport->send(testMessage);

    // Flush to ensure output is written to our stringstream
    std::cout.flush();

    std::string output = getOutput();
    REQUIRE_FALSE(output.empty());

    // MCP stdio spec: output should be NDJSON (JSON + newline)
    CHECK(output.back() == '\n');
    CHECK(output.find("Content-Length:") == std::string::npos); // No LSP headers
    CHECK(output.find("Content-Type:") == std::string::npos);   // No LSP headers

    // Should contain the JSON fields
    CHECK(output.find("\"jsonrpc\"") != std::string::npos);
    CHECK(output.find("\"method\"") != std::string::npos);
    CHECK(output.find("\"test\"") != std::string::npos);
    CHECK(output.find("\"id\"") != std::string::npos);

    // Should be parseable as JSON (without the newline)
    auto parsed = test_json::parse(output.substr(0, output.size() - 1));
    CHECK(parsed["method"] == "test");
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Receive valid JSON",
                 "[mcp][transport][stdio][catch2]") {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "initialize"}, {"id", 1}};

    setInput(frameMessage(testMessage));

    auto result = transport->receive();

    REQUIRE(result.has_value());
    const test_json& received = result.value();
    CHECK(received["jsonrpc"] == "2.0");
    CHECK(received["method"] == "initialize");
    CHECK(received["id"] == 1);
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Receive invalid JSON",
                 "[mcp][transport][stdio][catch2]") {
    setInput(frameRaw("invalid json"));

    auto result = transport->receive();

    // Should return error for invalid JSON
    REQUIRE_FALSE(result.has_value());
    const auto& error = result.error();
    CHECK(error.code == yams::ErrorCode::InvalidData);
    CHECK_FALSE(error.message.empty());
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Receive empty line",
                 "[mcp][transport][stdio][catch2]") {
    setInput("\n"); // Just a newline, no framed message

    auto result = transport->receive();

    // Should skip empty lines and eventually timeout/close
    REQUIRE_FALSE(result.has_value());
    // Could be transport closed or timeout
    CHECK(result.error().code == yams::ErrorCode::NetworkError);
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Receive multiple messages",
                 "[mcp][transport][stdio][catch2]") {
    test_json message1 = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "test1"}};
    test_json message2 = {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "test2"}};

    setInput(frameMessage(message1) + frameMessage(message2));

    auto result1 = transport->receive();
    REQUIRE(result1.has_value());
    CHECK(result1.value()["id"] == 1);
    CHECK(result1.value()["method"] == "test1");

    auto result2 = transport->receive();
    REQUIRE(result2.has_value());
    CHECK(result2.value()["id"] == 2);
    CHECK(result2.value()["method"] == "test2");
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Send multiple messages",
                 "[mcp][transport][stdio][catch2]") {
    test_json message1 = {{"id", 1}, {"method", "test1"}};
    test_json message2 = {{"id", 2}, {"method", "test2"}};

    transport->send(message1);
    transport->send(message2);

    std::string output = getOutput();

    // Both messages should be in output, each with newline (NDJSON)
    CHECK(output.find("test1") != std::string::npos);
    CHECK(output.find("test2") != std::string::npos);

    // Count newlines - should be 2 (one per message)
    size_t newlineCount = std::count(output.begin(), output.end(), '\n');
    CHECK(newlineCount == 2);

    // Should NOT have LSP headers
    CHECK(output.find("Content-Length:") == std::string::npos);
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Close transport",
                 "[mcp][transport][stdio][catch2]") {
    CHECK(transport->isConnected());

    transport->close();

    CHECK_FALSE(transport->isConnected());
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Send after close",
                 "[mcp][transport][stdio][catch2]") {
    transport->close();

    test_json testMessage = {{"test", "message"}};

    // Should not crash when sending after close
    REQUIRE_NOTHROW(transport->send(testMessage));

    // Should not produce output
    std::string output = getOutput();
    CHECK(output.empty());
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Receive after close",
                 "[mcp][transport][stdio][catch2]") {
    transport->close();

    setInput("{\"test\": \"message\"}\n");

    auto result = transport->receive();

    // Should return transport closed error
    REQUIRE_FALSE(result.has_value());
    CHECK(result.error().code == yams::ErrorCode::NetworkError);
}

TEST_CASE_METHOD(StdioTransportFixture, "StdioTransport - Complex JSON message",
                 "[mcp][transport][stdio][catch2]") {
    test_json complexMessage = {
        {"jsonrpc", "2.0"},
        {"id", 42},
        {"method", "tools/call"},
        {"params",
         {{"name", "search"},
          {"arguments",
           {{"query", "complex search with spaces"},
            {"limit", 10},
            {"filters",
             {{"type", "document"},
              {"tags", {"important", "urgent"}},
              {"date_range", {{"start", "2024-01-01"}, {"end", "2024-12-31"}}}}}}}}}};

    // Test sending complex message
    transport->send(complexMessage);
    std::string output = getOutput();
    CHECK_FALSE(output.empty());

    clearOutput();

    // Test receiving complex message
    setInput(frameMessage(complexMessage));
    auto result = transport->receive();

    REQUIRE(result.has_value());
    const test_json& received = result.value();
    CHECK(received["jsonrpc"] == "2.0");
    CHECK(received["id"] == 42);
    CHECK(received["method"] == "tools/call");
    REQUIRE(received.contains("params"));

    auto params = received["params"];
    CHECK(params["name"] == "search");
    CHECK(params.contains("arguments"));
}
