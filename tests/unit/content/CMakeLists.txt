# Content handler tests
add_executable(pdf_content_handler_test
    pdf_content_handler_test.cpp
)

target_link_libraries(pdf_content_handler_test
    PRIVATE
        test_utils
        gtest_main
        yams::content
        yams::detection
)

gtest_discover_tests(pdf_content_handler_test
    PROPERTIES
        LABELS "unit;content"
        TIMEOUT 60
)

# Test if existing content handler tests should be included
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/text_handler_test.cpp")
    add_executable(text_handler_test
        text_handler_test.cpp
    )

    target_link_libraries(text_handler_test
        PRIVATE
            test_utils
            gtest_main
            yams::content
            yams::detection
    )

    gtest_discover_tests(text_handler_test
        PROPERTIES
            LABELS "unit;content"
            TIMEOUT 60
    )
endif()

# Test if modern content handler tests exist
# NOTE: Temporarily disabled due to C++20 compatibility issues
# if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test_content_handlers_modern.cpp")
#     add_executable(content_handlers_modern_test
#         test_content_handlers_modern.cpp
#     )
#
#     target_link_libraries(content_handlers_modern_test
#         PRIVATE
#             yams::content
#             yams::detection
#             GTest::gtest
#             GTest::gmock
#             GTest::gtest_main
#     )
#
#     gtest_discover_tests(content_handlers_modern_test
#         PROPERTIES
#             LABELS "unit;content;modern"
#             TIMEOUT 120
#     )
# endif()
