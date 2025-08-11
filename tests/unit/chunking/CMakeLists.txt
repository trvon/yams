add_executable(chunking_tests
    rabin_chunker_test.cpp
    streaming_chunker_test.cpp
)

target_link_libraries(chunking_tests
    PRIVATE
        test_utils
        yams::chunking
        GTest::gtest_main
)

gtest_discover_tests(chunking_tests
    PROPERTIES
        LABELS "unit;chunking"
)