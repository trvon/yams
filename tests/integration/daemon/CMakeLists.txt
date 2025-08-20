# Daemon Integration Tests

# Daemon lifecycle integration test
add_executable(daemon_lifecycle_integration_test
    daemon_lifecycle_integration_test.cpp
)
target_link_libraries(daemon_lifecycle_integration_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME daemon_lifecycle_integration_test COMMAND daemon_lifecycle_integration_test)

# Daemon search integration test
add_executable(daemon_search_integration_test
    daemon_search_integration_test.cpp
)
target_link_libraries(daemon_search_integration_test PRIVATE
    yams::daemon
    yams::search
    yams::metadata
    yams::indexing
    test_utils
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME daemon_search_integration_test COMMAND daemon_search_integration_test)

# Daemon streaming test
add_executable(daemon_streaming_test
    daemon_streaming_test.cpp
)
target_link_libraries(daemon_streaming_test PRIVATE
    yams::daemon
    yams::api
    test_utils
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME daemon_streaming_test COMMAND daemon_streaming_test)

# Daemon resilience test
add_executable(daemon_resilience_test
    daemon_resilience_test.cpp
)
target_link_libraries(daemon_resilience_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME daemon_resilience_test COMMAND daemon_resilience_test)

# Daemon download integration test
add_executable(daemon_download_integration_test
    daemon_download_integration_test.cpp
)
target_link_libraries(daemon_download_integration_test PRIVATE
    yams::daemon
    yams::api
    yams::metadata
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME daemon_download_integration_test COMMAND daemon_download_integration_test)
