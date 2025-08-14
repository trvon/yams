# MCP server unit tests

add_executable(mcp_tests
    mcp_server_test.cpp
    websocket_transport_test.cpp
    stdio_transport_test.cpp
    mcp_tools_schema_and_smoke_test.cpp
)

target_link_libraries(mcp_tests
    PRIVATE
        GTest::gtest_main
        GTest::gmock
        yams_mcp
        yams::api
        yams::search
        yams::metadata
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        OpenSSL::SSL
        OpenSSL::Crypto
        Threads::Threads
)

# Define YAMS_TESTING macro for testing interface
target_compile_definitions(mcp_tests PRIVATE YAMS_TESTING)

# Boost.Beast is header-only and included with Boost
# No additional include directories needed

gtest_discover_tests(mcp_tests)

# Add to CTest
add_test(NAME mcp_unit_tests COMMAND mcp_tests)
