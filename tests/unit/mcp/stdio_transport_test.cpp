#include <nlohmann/json.hpp>
#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>
#include <gtest/gtest.h>
#include <yams/mcp/error_handling.h>
#include <yams/mcp/mcp_server.h>

using namespace yams::mcp;
using test_json = nlohmann::json;

// MCP stdio spec: newline-delimited JSON (NDJSON)
static std::string frameMessage(const test_json& j) {
    return j.dump() + "\n";
}

static std::string frameRaw(const std::string& raw) {
    return raw + "\n";
}

class StdioTransportTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Save original cout/cin
        originalCout = std::cout.rdbuf();
        originalCin = std::cin.rdbuf();

        // Redirect cout to our stream
        std::cout.rdbuf(outputStream.rdbuf());

        transport = std::make_unique<StdioTransport>();
    }

    void TearDown() override {
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

    std::unique_ptr<StdioTransport> transport;

private:
    std::stringstream inputStream;
    std::stringstream outputStream;
    std::streambuf* originalCout;
    std::streambuf* originalCin;
};

TEST_F(StdioTransportTest, InitialState) {
    EXPECT_TRUE(transport->isConnected());
}

TEST_F(StdioTransportTest, SendMessage) {
    test_json testMessage = {{"jsonrpc", "2.0"}, {"method", "test"}, {"id", 1}};

    transport->send(testMessage);

    // Flush to ensure output is written to our stringstream
    std::cout.flush();

    std::string output = getOutput();
    ASSERT_FALSE(output.empty()) << "Output buffer is empty after send()";

    // MCP stdio spec: output should be NDJSON (JSON + newline)
    EXPECT_EQ(output.back(), '\n');
    EXPECT_EQ(output.find("Content-Length:"), std::string::npos); // No LSP headers
    EXPECT_EQ(output.find("Content-Type:"), std::string::npos);   // No LSP headers

    // Should contain the JSON fields
    EXPECT_NE(output.find("\"jsonrpc\""), std::string::npos);
    EXPECT_NE(output.find("\"method\""), std::string::npos);
    EXPECT_NE(output.find("\"test\""), std::string::npos);
    EXPECT_NE(output.find("\"id\""), std::string::npos);

    // Should be parseable as JSON (without the newline)
    auto parsed = test_json::parse(output.substr(0, output.size() - 1));
    EXPECT_EQ(parsed["method"], "test");
}

TEST_F(StdioTransportTest, ReceiveValidJson) {
    json testMessage = {{"jsonrpc", "2.0"}, {"method", "initialize"}, {"id", 1}};

    setInput(frameMessage(testMessage));

    auto result = transport->receive();

    EXPECT_TRUE(result);
    if (result) {
        const json& received = result.value();
        EXPECT_EQ(received["jsonrpc"], "2.0");
        EXPECT_EQ(received["method"], "initialize");
        EXPECT_EQ(received["id"], 1);
    }
}

TEST_F(StdioTransportTest, ReceiveInvalidJson) {
    setInput(frameRaw("invalid json"));

    auto result = transport->receive();

    // Should return error for invalid JSON
    EXPECT_FALSE(result);
    if (!result) {
        const auto& error = result.error();
        EXPECT_EQ(error.code, yams::ErrorCode::InvalidData);
        EXPECT_FALSE(error.message.empty());
    }
}

TEST_F(StdioTransportTest, ReceiveEmptyLine) {
    setInput("\n"); // Just a newline, no framed message

    auto result = transport->receive();

    // Should skip empty lines and eventually timeout/close
    EXPECT_FALSE(result);
    if (!result) {
        // Could be transport closed or timeout
        EXPECT_EQ(result.error().code, yams::ErrorCode::NetworkError);
    }
}

TEST_F(StdioTransportTest, ReceiveMultipleMessages) {
    json message1 = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "test1"}};
    json message2 = {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "test2"}};

    setInput(frameMessage(message1) + frameMessage(message2));

    auto result1 = transport->receive();
    EXPECT_TRUE(result1.has_value());
    if (result1) {
        EXPECT_EQ(result1.value()["id"], 1);
        EXPECT_EQ(result1.value()["method"], "test1");
    }

    auto result2 = transport->receive();
    EXPECT_TRUE(result2.has_value());
    if (result2) {
        EXPECT_EQ(result2.value()["id"], 2);
        EXPECT_EQ(result2.value()["method"], "test2");
    }
}

TEST_F(StdioTransportTest, SendMultipleMessages) {
    json message1 = {{"id", 1}, {"method", "test1"}};
    json message2 = {{"id", 2}, {"method", "test2"}};

    transport->send(message1);
    transport->send(message2);

    std::string output = getOutput();

    // Both messages should be in output, each with newline (NDJSON)
    EXPECT_NE(output.find("test1"), std::string::npos);
    EXPECT_NE(output.find("test2"), std::string::npos);

    // Count newlines - should be 2 (one per message)
    size_t newlineCount = std::count(output.begin(), output.end(), '\n');
    EXPECT_EQ(newlineCount, 2);

    // Should NOT have LSP headers
    EXPECT_EQ(output.find("Content-Length:"), std::string::npos);
}

TEST_F(StdioTransportTest, CloseTransport) {
    EXPECT_TRUE(transport->isConnected());

    transport->close();

    EXPECT_FALSE(transport->isConnected());
}

TEST_F(StdioTransportTest, SendAfterClose) {
    transport->close();

    json testMessage = {{"test", "message"}};

    // Should not crash when sending after close
    EXPECT_NO_THROW(transport->send(testMessage));

    // Should not produce output
    std::string output = getOutput();
    EXPECT_TRUE(output.empty());
}

TEST_F(StdioTransportTest, ReceiveAfterClose) {
    transport->close();

    setInput("{\"test\": \"message\"}\n");

    auto result = transport->receive();

    // Should return transport closed error
    EXPECT_FALSE(result);
    if (!result) {
        EXPECT_EQ(result.error().code, yams::ErrorCode::NetworkError);
    }
}

TEST_F(StdioTransportTest, ComplexJsonMessage) {
    json complexMessage = {
        {"jsonrpc", "2.0"},
        {"id", 42},
        {"method", "tools/call"},
        {"params",
         {{"name", "search_documents"},
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
    EXPECT_FALSE(output.empty());

    clearOutput();

    // Test receiving complex message
    setInput(frameMessage(complexMessage));
    auto result = transport->receive();

    EXPECT_TRUE(result);
    if (result) {
        const json& received = result.value();
        EXPECT_EQ(received["jsonrpc"], "2.0");
        EXPECT_EQ(received["id"], 42);
        EXPECT_EQ(received["method"], "tools/call");
        EXPECT_TRUE(received.contains("params"));

        auto params = received["params"];
        EXPECT_EQ(params["name"], "search_documents");
        EXPECT_TRUE(params.contains("arguments"));

        auto arguments = params["arguments"];
        EXPECT_EQ(arguments["query"], "complex search with spaces");
        EXPECT_EQ(arguments["limit"], 10);
    }
}

TEST_F(StdioTransportTest, JsonWithSpecialCharacters) {
    json messageWithSpecialChars = {{"message", "Test with special chars: \n\t\r\"\\"},
                                    {"unicode", "Unicode: 🚀 测试 🎉"},
                                    {"escaped", "Escaped: \\\"quoted\\\" and \\n newline"}};

    // Test round-trip
    transport->send(messageWithSpecialChars);
    std::string output = getOutput();
    EXPECT_FALSE(output.empty());

    setInput(frameMessage(messageWithSpecialChars));
    auto result = transport->receive();

    EXPECT_TRUE(result);
    if (result) {
        const json& received = result.value();
        EXPECT_EQ(received["message"], "Test with special chars: \n\t\r\"\\");
        EXPECT_EQ(received["unicode"], "Unicode: 🚀 测试 🎉");
        EXPECT_EQ(received["escaped"], "Escaped: \\\"quoted\\\" and \\n newline");
    }
}

TEST_F(StdioTransportTest, ConnectionStateConsistency) {
    // Should be connected initially
    EXPECT_TRUE(transport->isConnected());

    // After close, should not be connected
    transport->close();
    EXPECT_FALSE(transport->isConnected());

    // Multiple calls to isConnected should be consistent
    EXPECT_FALSE(transport->isConnected());
    EXPECT_FALSE(transport->isConnected());
    EXPECT_FALSE(transport->isConnected());
}

TEST_F(StdioTransportTest, LargeMessage) {
    // Create a large JSON message
    json largeMessage = {{"jsonrpc", "2.0"},
                         {"id", 1},
                         {"method", "test"},
                         {"params",
                          {
                              {"large_data", std::string(10000, 'A')} // 10KB of 'A' characters
                          }}};

    // Should handle large messages
    EXPECT_NO_THROW(transport->send(largeMessage));

    std::string output = getOutput();
    EXPECT_GT(output.length(), 10000);

    // Test receiving large message
    setInput(frameMessage(largeMessage));
    auto result = transport->receive();

    EXPECT_TRUE(result);
    if (result) {
        const json& received = result.value();
        EXPECT_EQ(received["method"], "test");
        EXPECT_EQ(received["params"]["large_data"].get<std::string>().length(), 10000);
    }
}

TEST_F(StdioTransportTest, NDJSONInputAndOutput) {
    // Test standard NDJSON input/output (MCP spec compliant)
    json testMessage = {{"jsonrpc", "2.0"}, {"id", 1}, {"method", "test"}};

    // Send NDJSON input
    setInput(frameMessage(testMessage));
    auto result = transport->receive();

    EXPECT_TRUE(result);
    if (result) {
        EXPECT_EQ(result.value()["method"], "test");
    }

    // Output should be NDJSON
    clearOutput();
    transport->send(testMessage);
    std::string output = getOutput();

    EXPECT_EQ(output.back(), '\n');
    EXPECT_EQ(output.find("Content-Length:"), std::string::npos);

    // Should be parseable
    auto parsed = json::parse(output.substr(0, output.size() - 1));
    EXPECT_EQ(parsed["method"], "test");
}

TEST_F(StdioTransportTest, ReceiveJsonRpcBatch) {
    json batch = json::array({{{"jsonrpc", "2.0"}, {"id", 1}, {"method", "batch.one"}},
                              {{"jsonrpc", "2.0"}, {"id", 2}, {"method", "batch.two"}}});
    setInput(frameRaw(batch.dump()));

    auto result = transport->receive();
    ASSERT_TRUE(result);
    const json& received = result.value();
    ASSERT_TRUE(received.is_array());
    EXPECT_EQ(received.size(), 2);
    EXPECT_EQ(received[0].at("id"), 1);
    EXPECT_EQ(received[1].at("method"), "batch.two");
}

TEST_F(StdioTransportTest, MalformedHeadersExhaustRetryBudget) {
    // Five malformed header attempts should push the transport into an error state.
    const std::string malformed = "Content-Bad: nope\n\n";
    for (int attempt = 0; attempt < 5; ++attempt) {
        setInput(malformed);
        auto result = transport->receive();
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, yams::ErrorCode::InvalidData);
    }

    // Once the retry budget is exhausted, transport should refuse further reads.
    json validMessage = {{"jsonrpc", "2.0"}, {"id", 99}, {"method", "still-running"}};
    setInput(frameMessage(validMessage));

    auto finalResult = transport->receive();
    ASSERT_FALSE(finalResult);
    EXPECT_EQ(finalResult.error().code, yams::ErrorCode::NetworkError);
}
