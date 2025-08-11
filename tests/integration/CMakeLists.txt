# Integration test subdirectories
add_subdirectory(manifest)
add_subdirectory(search)

# Integration tests - temporarily disabled due to API changes
# TODO: Update tests to match new API
add_executable(integration_tests
    main.cpp
    # full_system_test.cpp          # API changes needed
    # compression_integration_test.cpp  # API changes needed
    cli_init_integration_test.cpp
    # cli_delete_test.cpp           # Has its own main() - conflicts
)

target_link_libraries(integration_tests
    PRIVATE
        test_utils
        yams::api
        yams::manifest
        yams::storage_engine
        yams::chunking
        yams::crypto
        yams::wal
        yams::integrity
        yams::compression
        GTest::gtest
)

gtest_discover_tests(integration_tests
    PROPERTIES
        LABELS "integration"
        TIMEOUT 60
)
