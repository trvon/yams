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

add_executable(daemon_server_multiplex_integration_test daemon/daemon_server_multiplex_integration_test.cpp)
target_link_libraries(daemon_server_multiplex_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_server_multiplex_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

add_executable(daemon_search_multiplex_integration_test daemon/daemon_search_multiplex_integration_test.cpp)
target_link_libraries(daemon_search_multiplex_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_search_multiplex_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})



# pooled_request_integration_test removed - API changed with Boost.ASIO migration

# Boundary IPC tests for pool/daemon/client interactions
## Removed legacy pool tests
# mcp_downloader_integration_test removed - API changed with Boost.ASIO migration





# New comprehensive integration tests: removed for stabilization (will be reintroduced after porting)

# Streaming concurrency test: ensure stream returns while data is being added (always build)
add_executable(daemon_interleaving_integration_test
    daemon/daemon_interleaving_integration_test.cpp)
target_link_libraries(daemon_interleaving_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_interleaving_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

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

add_executable(daemon_mux_metrics_integration_test daemon/daemon_mux_metrics_integration_test.cpp)
target_link_libraries(daemon_mux_metrics_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_mux_metrics_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

add_executable(daemon_writer_budget_integration_test daemon/daemon_writer_budget_integration_test.cpp)
target_link_libraries(daemon_writer_budget_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_writer_budget_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

add_executable(daemon_mux_active_handlers_integration_test daemon/daemon_mux_active_handlers_integration_test.cpp)
target_link_libraries(daemon_mux_active_handlers_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_mux_active_handlers_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})
