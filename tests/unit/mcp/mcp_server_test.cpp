#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <yams/mcp/mcp_server.h>
#include <memory>
#include <string>

using namespace yams::mcp;

// Basic transport tests - focus on basic functionality only
class TransportTest : public ::testing::Test {
public:
    void SetUp() override {
        // Simple test setup
    }
};

TEST_F(TransportTest, StdioTransportBasics) {
    // Test basic StdioTransport functionality
    StdioTransport transport;
    
    // Test that transport starts as connected
    EXPECT_TRUE(transport.isConnected());
    
    // Test close functionality
    transport.close();
    EXPECT_FALSE(transport.isConnected());
}

TEST_F(TransportTest, WebSocketTransportConfig) {
    // Test WebSocket transport configuration
    WebSocketTransport::Config config;
    config.host = "test.example.com";
    config.port = 9999;
    config.path = "/test";
    
    // Verify config values
    EXPECT_EQ(config.host, "test.example.com");
    EXPECT_EQ(config.port, 9999);
    EXPECT_EQ(config.path, "/test");
}

// Basic test to ensure headers include correctly and types are available
TEST(MCPTypesTest, BasicTypeAvailability) {
    // Test that we can create transport config
    WebSocketTransport::Config wsConfig;
    EXPECT_EQ(wsConfig.host, "localhost");
    EXPECT_EQ(wsConfig.port, 8080);
    EXPECT_EQ(wsConfig.path, "/mcp");
    EXPECT_FALSE(wsConfig.useSSL);
    
    // Test basic types are available
    EXPECT_TRUE(true); // Basic compilation test
}