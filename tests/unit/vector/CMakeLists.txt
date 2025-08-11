# Vector database unit tests

add_executable(vector_database_tests
    vector_database_test.cpp
    embedding_generator_test.cpp
    document_chunker_test.cpp
    vector_index_manager_test.cpp
    model_management_test.cpp
)

target_link_libraries(vector_database_tests
    PRIVATE
        yams::vector
        yams::core
        gtest
        gtest_main
)

# Set properties
set_target_properties(vector_database_tests PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

# Discover tests
gtest_discover_tests(vector_database_tests
    PROPERTIES
        LABELS "vector;unit"
        TIMEOUT 60
)