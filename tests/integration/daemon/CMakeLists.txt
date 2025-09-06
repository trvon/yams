# Daemon Integration Tests

# Daemon lifecycle integration test
add_executable(daemon_lifecycle_integration_test
    daemon_lifecycle_integration_test.cpp
)
target_link_libraries(daemon_lifecycle_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
)
add_test(NAME daemon_lifecycle_integration_test COMMAND daemon_lifecycle_integration_test)
set_tests_properties(daemon_lifecycle_integration_test PROPERTIES LABELS "daemon;integration")

# Daemon search integration test
add_executable(daemon_search_integration_test
    daemon_search_integration_test.cpp
)
target_link_libraries(daemon_search_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::search
    yams::metadata
    yams::indexing
)
add_test(NAME daemon_search_integration_test COMMAND daemon_search_integration_test)
set_tests_properties(daemon_search_integration_test PROPERTIES LABELS "daemon;integration")

# Daemon streaming test
add_executable(daemon_streaming_test
    daemon_streaming_test.cpp
)
target_link_libraries(daemon_streaming_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
)
add_test(NAME daemon_streaming_test COMMAND daemon_streaming_test)
set_tests_properties(daemon_streaming_test PROPERTIES LABELS "daemon;integration")

# Daemon resilience test
add_executable(daemon_resilience_test
    daemon_resilience_test.cpp
)
target_link_libraries(daemon_resilience_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
)
add_test(NAME daemon_resilience_test COMMAND daemon_resilience_test)
set_tests_properties(daemon_resilience_test PROPERTIES LABELS "daemon;integration")

# Daemon download integration test
add_executable(daemon_download_integration_test
    daemon_download_integration_test.cpp
)
target_link_libraries(daemon_download_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
    yams::metadata
)
add_test(NAME daemon_download_integration_test COMMAND daemon_download_integration_test)
set_tests_properties(daemon_download_integration_test PROPERTIES LABELS "daemon;integration")

# Split tests: daemon add normalization and daemon delete parity
add_executable(daemon_add_normalization_integration_test
    daemon_delete_add_integration_test.cpp
)
target_link_libraries(daemon_add_normalization_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
    yams::metadata
)
add_test(NAME daemon_add_normalization_integration_test COMMAND daemon_add_normalization_integration_test)
set_tests_properties(daemon_add_normalization_integration_test PROPERTIES LABELS "daemon;integration")

add_executable(daemon_delete_parity_integration_test
    daemon_delete_add_integration_test.cpp
)
target_link_libraries(daemon_delete_parity_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
    yams::metadata
)
add_test(NAME daemon_delete_parity_integration_test COMMAND daemon_delete_parity_integration_test)
set_tests_properties(daemon_delete_parity_integration_test PROPERTIES LABELS "daemon;integration")

# Pooled request integration test (uses real daemon)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/pooled_request_integration_test.cpp")
    add_executable(pooled_request_integration_test
        pooled_request_integration_test.cpp
    )
    target_link_libraries(pooled_request_integration_test PRIVATE
        test_utils
        gtest_main
        yams_mcp
        yams_cli
        yams_api
        yams_search
        yams_metadata
        yams_downloader
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        Threads::Threads
    )
    add_test(NAME pooled_request_integration_test COMMAND pooled_request_integration_test)
    set_tests_properties(pooled_request_integration_test PROPERTIES LABELS "daemon;integration")
else()
    message(STATUS "Skipping pooled_request_integration_test (source not present)")
endif()

# Protocol streaming/non-streaming behavioral tests
add_executable(daemon_search_streaming_protocol_test
    daemon_search_streaming_protocol_test.cpp
)
target_link_libraries(daemon_search_streaming_protocol_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams_cli
    spdlog::spdlog
)
add_test(NAME daemon_search_streaming_protocol_test COMMAND daemon_search_streaming_protocol_test)
set_tests_properties(daemon_search_streaming_protocol_test PROPERTIES LABELS "daemon;integration")

# Status/Stats proto fields smoke test
add_executable(daemon_status_stats_integration_test
    daemon_status_stats_integration_test.cpp
)
target_link_libraries(daemon_status_stats_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams_cli
)
add_test(NAME daemon_status_stats_integration_test COMMAND daemon_status_stats_integration_test)
set_tests_properties(daemon_status_stats_integration_test PROPERTIES LABELS "daemon;integration")
