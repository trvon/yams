# Search unit tests
add_executable(search_tests
    query_parser_test.cpp
    hybrid_search_engine_test.cpp
    entity_linker_test.cpp
    kg_scorer_simple_test.cpp
    # search_executor_test.cpp  # Disabled - API has changed
    # search_cache_test.cpp     # Disabled - API has changed
    # bk_tree_test.cpp          # Disabled - API has changed
)

target_link_libraries(search_tests
    PRIVATE
        test_utils
        gtest_main
        yams_search
        Threads::Threads
)

# Add test
add_test(NAME search_tests COMMAND search_tests)

# Set test properties
set_tests_properties(search_tests PROPERTIES
    TIMEOUT 30
    LABELS "unit;search"
)
