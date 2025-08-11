# TUI Services Smoke Tests

# Create the executable for TUI services tests
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

# Register the test with CTest
add_test(NAME tui_services COMMAND test_tui_services)
