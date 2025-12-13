// MCP Server basics tests
// Catch2 migration from GTest (yams-3s4 / yams-84g)

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>

#include <yams/mcp/mcp_server.h>

using namespace yams::mcp;

TEST_CASE("MCP Transport - StdioTransport basics", "[mcp][transport][catch2]") {
    StdioTransport transport;

    // Test that transport starts as connected
    CHECK(transport.isConnected());

    // Test close functionality
    transport.close();
    CHECK_FALSE(transport.isConnected());
}

TEST_CASE("MCP Types - Basic type availability", "[mcp][types][catch2]") {
    // Test basic types are available - compilation test
    CHECK(true);
}
