# Vector database unit tests

add_executable(vector_database_tests
    vector_database_test.cpp
    embedding_generator_test.cpp
    document_chunker_test.cpp
    vector_index_manager_test.cpp
    model_management_test.cpp
    test_onnx_runtime.cpp
)

target_link_libraries(vector_database_tests
    PRIVATE
        test_utils
        gtest_main
        yams::vector
        yams::core
)

# Link Tracy if profiling is enabled
if(YAMS_ENABLE_PROFILING AND TARGET Tracy::TracyClient)
    target_link_libraries(vector_database_tests PRIVATE Tracy::TracyClient)
endif()

# Set properties
set_target_properties(vector_database_tests PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

# Discover tests with increased timeout and environment for discovery mode
gtest_discover_tests(vector_database_tests
    PROPERTIES
        LABELS "vector;unit"
        TIMEOUT 60
        ENVIRONMENT "GTEST_DISCOVERY_MODE=1"
    DISCOVERY_TIMEOUT 30  # Increase from default 5 to 30 seconds
)
