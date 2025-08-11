# Content API unit tests
add_executable(test_content_api
    content_store_test.cpp
)

target_link_libraries(test_content_api
    PRIVATE
        yams::api
        GTest::gtest
        GTest::gtest_main
        GTest::gmock
)

# Add test to CTest
add_test(
    NAME test_content_api
    COMMAND test_content_api
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

# Set test properties
set_tests_properties(test_content_api PROPERTIES
    LABELS "unit;api"
    TIMEOUT 60
)

# Metadata API unit tests disabled - API has changed significantly
# TODO: Update tests to match new API
# add_executable(test_metadata_api
#     metadata_api_test.cpp
# )
# 
# target_link_libraries(test_metadata_api
#     PRIVATE
#         yams::api
#         yams::metadata
#         yams::indexing
#         test_utils
#         GTest::gtest
#         GTest::gtest_main
#         nlohmann_json::nlohmann_json
# )
# 
# # Add test to CTest
# add_test(
#     NAME test_metadata_api
#     COMMAND test_metadata_api
#     WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
# )
# 
# # Set test properties
# set_tests_properties(test_metadata_api PROPERTIES
#     LABELS "unit;api;metadata"
#     TIMEOUT 60
# )