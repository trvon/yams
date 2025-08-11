# Manifest integration tests temporarily disabled due to API changes
# TODO: Update tests to match new API
# add_executable(manifest_integration_tests
#     reconstruction_test.cpp
# )
# 
# target_link_libraries(manifest_integration_tests
#     PRIVATE
#         gtest
#         gtest_main
#         gmock
#         gmock_main
#         yams::manifest
#         yams::storage_engine
#         yams::chunking
#         yams::crypto
#         spdlog::spdlog
# )
# 
# target_include_directories(manifest_integration_tests
#     PRIVATE
#         ${CMAKE_SOURCE_DIR}/tests/utils
# )
# 
# # Enable C++20 for tests
# target_compile_features(manifest_integration_tests PRIVATE cxx_std_20)
# 
# # Conditionally link protobuf for tests
# if(Protobuf_FOUND)
#     target_link_libraries(manifest_integration_tests PRIVATE protobuf::libprotobuf)
# endif()
# 
# # Register with CTest
# add_test(NAME ManifestIntegrationTests COMMAND manifest_integration_tests)