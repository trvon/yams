# CLI Tests

# TUI Services Smoke Tests - only build if TUI is enabled
if(YAMS_ENABLE_TUI)
    add_executable(test_tui_services
        test_tui_services.cpp
    )

    # Require C++20 to match the project configuration
    target_compile_features(test_tui_services PRIVATE cxx_std_20)

    # Link against the CLI library to inherit include paths and dependencies
    target_link_libraries(test_tui_services
        PRIVATE
            yams_cli
    )

    # Register test with CTest
    add_test(NAME tui_services COMMAND test_tui_services)
endif()

# Update Command Tests - only build if yams_cli is available
if(TARGET yams_cli)
    add_executable(update_command_tests
        update_command_test.cpp
    )

    # Configure update command tests
    target_compile_features(update_command_tests PRIVATE cxx_std_20)
    target_link_libraries(update_command_tests
        PRIVATE
            GTest::gtest_main
            GTest::gmock
            yams_cli
            yams::metadata
            yams::api
    )

    # Register tests with CTest
    add_test(NAME update_command_tests COMMAND update_command_tests)

    # Discover tests for better integration with timeout
    gtest_discover_tests(update_command_tests
        DISCOVERY_TIMEOUT 30
    )

    # Daemon command CLI parsing tests
    add_executable(daemon_command_tests
        daemon_command_test.cpp
    )

    target_compile_features(daemon_command_tests PRIVATE cxx_std_20)
    target_link_libraries(daemon_command_tests
        PRIVATE
            GTest::gtest_main
            yams_cli
    )

    add_test(NAME daemon_command_tests COMMAND daemon_command_tests)
    gtest_discover_tests(daemon_command_tests
        DISCOVERY_TIMEOUT 30
    )
else()
    message(WARNING "yams_cli target not found - skipping update_command_tests")
endif()

# PooledRequestManager unit tests removed in favor of real-daemon integration tests.
# The integration suite exercises the pooled request path end-to-end.
