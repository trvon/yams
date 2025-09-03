add_executable(chunking_tests
    rabin_chunker_test.cpp
)

target_link_libraries(chunking_tests
    PRIVATE
        test_utils
        yams::chunking
        yams::core
        gtest_main
        spdlog::spdlog
)

add_test(NAME chunking_tests COMMAND chunking_tests)

set_tests_properties(chunking_tests PROPERTIES
    TIMEOUT 30
    LABELS "unit;chunking"
)
