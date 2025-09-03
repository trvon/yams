# Toggle legacy integration tests (pre-tinyfsm IPC, older flows)
option(YAMS_BUILD_LEGACY_INTEGRATION_TESTS "Build legacy integration tests (pre-tinyfsm IPC)" OFF)

# Integration test subdirectories (legacy)
if(YAMS_BUILD_LEGACY_INTEGRATION_TESTS)
    add_subdirectory(manifest)
    add_subdirectory(search)
    add_subdirectory(daemon)
endif()

# pool_add_then_query_test removed - API changed with Boost.ASIO migration
# Include Boost.Test shim to avoid accidental prg/exec monitor undefined refs
include(GoogleTest)
set(CMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE POST_BUILD)
include(${CMAKE_SOURCE_DIR}/cmake/boost_test_shim.cmake)

# pooled_request_integration_test removed - API changed with Boost.ASIO migration

# Boundary IPC tests for pool/daemon/client interactions
if(NOT TARGET boundary_ipc_pool_tests)
    add_executable(boundary_ipc_pool_tests
        daemon/boundary_ipc_pool_tests.cpp
    )
    target_link_libraries(boundary_ipc_pool_tests PRIVATE
        test_utils
        gtest_main
        yams_cli
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        Threads::Threads
        yams::boost_test_shim
    )
    add_test(NAME boundary_ipc_pool_tests COMMAND boundary_ipc_pool_tests)
    set_tests_properties(boundary_ipc_pool_tests PROPERTIES LABELS "daemon;integration")
endif()
# mcp_downloader_integration_test removed - API changed with Boost.ASIO migration





# New comprehensive integration tests: removed for stabilization (will be reintroduced after porting)

# Streaming concurrency test: ensure stream returns while data is being added (always build)
if(NOT TARGET stream_return_while_adding_test)
    add_executable(stream_return_while_adding_test
        daemon/stream_return_while_adding_test.cpp
    )
    target_link_libraries(stream_return_while_adding_test PRIVATE
        test_utils
        gtest_main
        yams_mcp
        yams_cli
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        Threads::Threads
        yams::boost_test_shim
    )
    gtest_discover_tests(stream_return_while_adding_test
        DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
        PROPERTIES
            LABELS "daemon;integration"
            TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
    )
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
