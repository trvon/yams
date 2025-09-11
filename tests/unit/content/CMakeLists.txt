# Content handler tests
# Legacy PDF tests moved to plugin; do not build pdf_handler_test or pdf_content_handler_test here.
# PDF content handler tests moved to plugin; core no longer builds them.

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

# EncodingDetector tests (extraction utilities)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/encoding_detector_test.cpp")
    add_executable(encoding_detector_test
        encoding_detector_test.cpp
    )

    target_link_libraries(encoding_detector_test
        PRIVATE
            test_utils
            gtest_main
            yams::extraction
    )

    gtest_discover_tests(encoding_detector_test
        PROPERTIES
            LABELS "unit;content;extraction"
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
