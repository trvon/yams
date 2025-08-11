# Integration test subdirectories
add_subdirectory(manifest)

# Integration tests will be added as components are implemented
add_executable(integration_tests
    main.cpp
    full_system_test.cpp
    compression_integration_test.cpp
    cli_init_integration_test.cpp
    cli_delete_test.cpp
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
