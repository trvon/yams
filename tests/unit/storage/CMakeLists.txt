# Storage engine tests - TODO: Fix compilation issues
# add_executable(storage_engine_unit_tests
#     storage_engine_test.cpp
# )
# 
# target_link_libraries(storage_engine_unit_tests
#     PRIVATE
#         gtest
#         gtest_main
#         gmock
#         yams::storage_engine
#         yams::crypto
#         test_utils
# )
# 
# target_include_directories(storage_engine_unit_tests
#     PRIVATE
#         ${CMAKE_SOURCE_DIR}/tests/utils
# )
# 
# # Enable C++20 for tests
# target_compile_features(storage_engine_unit_tests PRIVATE cxx_std_20)
# 
# # Register with CTest
# add_test(NAME StorageEngineUnitTests COMMAND storage_engine_unit_tests)

# Reference counter tests
add_executable(reference_counter_unit_tests
    reference_counter_test.cpp
)

target_link_libraries(reference_counter_unit_tests
    PRIVATE
        gtest
        gtest_main
        yams::reference_counter
        yams::storage_engine
        test_utils
)

target_include_directories(reference_counter_unit_tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/utils
)

# Enable C++20 for tests
target_compile_features(reference_counter_unit_tests PRIVATE cxx_std_20)

# Register with CTest
add_test(NAME ReferenceCounterUnitTests COMMAND reference_counter_unit_tests)

# Compressed storage engine tests - TODO: Fix API mismatch issues
# add_executable(compressed_storage_engine_unit_tests
#     compressed_storage_engine_test.cpp
# )
# 
# target_link_libraries(compressed_storage_engine_unit_tests
#     PRIVATE
#         gtest
#         gtest_main
#         yams::storage_engine
#         yams::compression
#         test_utils
# )
# 
# target_include_directories(compressed_storage_engine_unit_tests
#     PRIVATE
#         ${CMAKE_SOURCE_DIR}/tests/utils
# )
# 
# # Enable C++20 for tests
# target_compile_features(compressed_storage_engine_unit_tests PRIVATE cxx_std_20)
# 
# # Register with CTest
# add_test(NAME CompressedStorageEngineUnitTests COMMAND compressed_storage_engine_unit_tests)