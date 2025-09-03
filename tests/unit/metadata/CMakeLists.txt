# Metadata unit tests
add_executable(metadata_tests
    database_test.cpp
    metadata_schema_test.cpp
    fts5_test.cpp
    metadata_repository_test.cpp
)

target_link_libraries(metadata_tests
    PRIVATE
        test_utils
        gtest_main
        yams::metadata
        yams::core
        yams_search
        spdlog::spdlog
)

gtest_discover_tests(metadata_tests)

# Add to CTest
add_test(NAME metadata_unit_tests COMMAND metadata_tests)
