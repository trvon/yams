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

// MCP stdio protocol uses newline-delimited JSON, not HTTP framing
static std::string formatMessage(const test_json& j) {
    return j.dump() + "\n";
}

static std::string formatRaw(const std::string& raw) {
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

    std::string output = getOutput();
    EXPECT_FALSE(output.empty());

    // Should contain the JSON message
    EXPECT_NE(output.find("\"jsonrpc\""), std::string::npos);
    EXPECT_NE(output.find("\"method\""), std::string::npos);
    EXPECT_NE(output.find("\"test\""), std::string::npos);
    EXPECT_NE(output.find("\"id\""), std::string::npos);
}

TEST_F(StdioTransportTest, ReceiveValidJson) {
    json testMessage = {{"jsonrpc", "2.0"}, {"method", "initialize"}, {"id", 1}};

    setInput(formatMessage(testMessage));

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
    setInput(formatRaw("invalid json"));

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
    setInput("\n"); // Just a newline, no JSON

    auto result = transport->receive();

    // Should skip empty lines and eventually timeout/close
    EXPECT_FALSE(result);
    if (!result) {
        // Could be transport closed or timeout
        EXPECT_EQ(result.error().code, yams::ErrorCode::NetworkError);
    }
}

TEST_F(StdioTransportTest, ReceiveMultipleMessages) {
    json message1 = {{"id", 1}, {"method", "test1"}};
    json message2 = {{"id", 2}, {"method", "test2"}};

    setInput(formatMessage(message1) + formatMessage(message2));

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

    // Both messages should be in output
    EXPECT_NE(output.find("test1"), std::string::npos);
    EXPECT_NE(output.find("test2"), std::string::npos);

    // Should have two separate lines
    size_t firstNewline = output.find('\n');
    size_t secondNewline = output.find('\n', firstNewline + 1);
    EXPECT_NE(firstNewline, std::string::npos);
    EXPECT_NE(secondNewline, std::string::npos);
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
    setInput(formatMessage(complexMessage));
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

    setInput(formatMessage(messageWithSpecialChars));
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
    setInput(formatMessage(largeMessage));
    auto result = transport->receive();

    EXPECT_TRUE(result);
    if (result) {
        const json& received = result.value();
        EXPECT_EQ(received["method"], "test");
        EXPECT_EQ(received["params"]["large_data"].get<std::string>().length(), 10000);
    }
}