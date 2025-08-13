# Text extraction unit tests
add_executable(extraction_tests
    text_extraction_test.cpp
    pdf_extractor_test.cpp
)

target_link_libraries(extraction_tests
    PRIVATE
        GTest::gtest_main
        yams::extraction
        yams::core
)

gtest_discover_tests(extraction_tests)

# Add to CTest
add_test(NAME extraction_unit_tests COMMAND extraction_tests)