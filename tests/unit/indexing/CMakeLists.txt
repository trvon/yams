# Indexing unit tests
add_executable(indexing_tests
    indexing_test.cpp
)

target_link_libraries(indexing_tests
    PRIVATE
        yams_indexing
        yams_metadata
        yams_extraction
        yams_search
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
)

# Add test
add_test(NAME indexing_tests COMMAND indexing_tests)

# Set test properties
set_tests_properties(indexing_tests PROPERTIES
    TIMEOUT 30
    LABELS "unit;indexing"
)