# MCP server unit tests

add_executable(mcp_tests
    mcp_server_test.cpp
    stdio_transport_test.cpp
    mcp_tools_schema_and_smoke_test.cpp
    mcp_jsonrpc_roundtrip_test.cpp
    mcp_server_exec_test.cpp
)

target_link_libraries(mcp_tests
    PRIVATE
        test_utils
        gtest_main
        yams_mcp
        yams_app_services
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

gtest_discover_tests(mcp_tests
    DISCOVERY_TIMEOUT 30
    PROPERTIES LABELS mcp
)

# Add to CTest
add_test(NAME mcp_unit_tests COMMAND mcp_tests)
set_tests_properties(mcp_unit_tests PROPERTIES LABELS mcp)
