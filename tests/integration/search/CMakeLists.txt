# Search integration tests - disabled due to API changes
# TODO: Update tests to match new API
# add_executable(search_integration_test
#     search_integration_test.cpp
# )
# 
# target_link_libraries(search_integration_test
#     PRIVATE
#         yams::search
#         yams::metadata
#         yams::indexing
#         yams::extraction
#         GTest::gtest
#         GTest::gtest_main
# )
# 
# set_target_properties(search_integration_test PROPERTIES
#     CXX_STANDARD 20
#     CXX_STANDARD_REQUIRED ON
# )
# 
# # Add test to CTest
# add_test(NAME search_integration_test COMMAND search_integration_test)
# 
# set_tests_properties(search_integration_test PROPERTIES
#     TIMEOUT 120  # 2 minutes for integration tests
#     LABELS "integration;search"
# )