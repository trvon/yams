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

file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/testdata)
gtest_discover_tests(metadata_tests
    DISCOVERY_MODE PRE_TEST
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES
        ENVIRONMENT "XDG_DATA_HOME=${CMAKE_BINARY_DIR}/testdata"
        LABELS "metadata;unit"
        RUN_SERIAL TRUE
        TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)

# Add to CTest
add_test(NAME metadata_unit_tests COMMAND metadata_tests)
