#include <memory>
#include <string>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <yams/mcp/mcp_server.h>

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

// Basic test to ensure headers include correctly and types are available
TEST(MCPTypesTest, BasicTypeAvailability) {
    // Test basic types are available
    EXPECT_TRUE(true); // Basic compilation test
}