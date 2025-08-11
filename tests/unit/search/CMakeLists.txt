# Search unit tests
add_executable(search_tests
    query_parser_test.cpp
    search_executor_test.cpp
    search_cache_test.cpp
    bk_tree_test.cpp
)

target_link_libraries(search_tests
    PRIVATE
        yams_search
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
)

# Add test
add_test(NAME search_tests COMMAND search_tests)

# Set test properties
set_tests_properties(search_tests PROPERTIES
    TIMEOUT 30
    LABELS "unit;search"
)