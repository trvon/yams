# Toggle legacy integration tests (pre-tinyfsm IPC, older flows)
option(YAMS_BUILD_LEGACY_INTEGRATION_TESTS "Build legacy integration tests (pre-tinyfsm IPC)" OFF)

# Integration test subdirectories (legacy)
if(YAMS_BUILD_LEGACY_INTEGRATION_TESTS)
    add_subdirectory(manifest)
    add_subdirectory(search)
    add_subdirectory(daemon)
endif()

# Add-then-query pooled request test (always build)
# Include Boost.Test shim to avoid accidental prg/exec monitor undefined refs
include(${CMAKE_SOURCE_DIR}/cmake/boost_test_shim.cmake)
if(NOT TARGET pool_add_then_query_test)
    add_executable(pool_add_then_query_test
        daemon/pool_add_then_query_test.cpp
    )
    target_link_libraries(pool_add_then_query_test PRIVATE
        yams_mcp
        yams_cli
    yams_asio_client
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
        yams::boost_test_shim
    )
    add_test(NAME pool_add_then_query_test COMMAND pool_add_then_query_test)
    set_tests_properties(pool_add_then_query_test PROPERTIES LABELS "daemon;integration")
endif()

# Always build the pooled request integration test for PBI-007-01, even if legacy daemon tests are disabled
if(NOT TARGET pooled_request_integration_test)
    add_executable(pooled_request_integration_test
        daemon/pooled_request_integration_test.cpp
    )
    target_link_libraries(pooled_request_integration_test PRIVATE
        yams_mcp
        yams_cli
    yams_asio_client
        yams_api
        yams_search
        yams_metadata
        yams_downloader
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
        yams::boost_test_shim
    )
    # Enable testing-only MCPServer interface for this target
    target_compile_definitions(pooled_request_integration_test PRIVATE YAMS_TESTING)
    add_test(NAME pooled_request_integration_test COMMAND pooled_request_integration_test)
    set_tests_properties(pooled_request_integration_test PROPERTIES LABELS "daemon;integration")
endif()

# Boundary IPC tests for pool/daemon/client interactions
if(NOT TARGET boundary_ipc_pool_tests)
    add_executable(boundary_ipc_pool_tests
        daemon/boundary_ipc_pool_tests.cpp
    )
    target_link_libraries(boundary_ipc_pool_tests PRIVATE
        yams_cli
    yams_asio_client
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
        yams::boost_test_shim
    )
    add_test(NAME boundary_ipc_pool_tests COMMAND boundary_ipc_pool_tests)
    set_tests_properties(boundary_ipc_pool_tests PROPERTIES LABELS "daemon;integration")
endif()
# MCP downloader integration test (header-only MCP tool registry)
add_executable(mcp_downloader_integration_test
    mcp/mcp_downloader_integration_test.cpp
)
target_link_libraries(mcp_downloader_integration_test PRIVATE
    GTest::gtest
    GTest::gtest_main
    nlohmann_json::nlohmann_json
    yams_mcp
    yams::boost_test_shim
)
add_test(NAME mcp_downloader_integration_test COMMAND mcp_downloader_integration_test)





# New comprehensive integration tests: removed for stabilization (will be reintroduced after porting)

# Streaming concurrency test: ensure stream returns while data is being added (always build)
if(NOT TARGET stream_return_while_adding_test)
    add_executable(stream_return_while_adding_test
        daemon/stream_return_while_adding_test.cpp
    )
    target_link_libraries(stream_return_while_adding_test PRIVATE
        yams_mcp
        yams_cli
    yams_asio_client
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
        yams::boost_test_shim
    )
    add_test(NAME stream_return_while_adding_test COMMAND stream_return_while_adding_test)
    set_tests_properties(stream_return_while_adding_test PROPERTIES LABELS "daemon;integration")
endif()

# Discover legacy tests only if enabled and target exists
if(YAMS_BUILD_LEGACY_INTEGRATION_TESTS)
    if(TARGET integration_tests)
        gtest_discover_tests(integration_tests
            PROPERTIES
                LABELS "integration"
                TIMEOUT 60
        )
    endif()
endif()

# Discover new integration tests
if(YAMS_ENABLE_NEW_INTEGRATION_TESTS)
endif()
