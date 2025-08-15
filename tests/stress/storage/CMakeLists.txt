add_executable(storage_concurrent_tests
    concurrent_test.cpp
)

target_link_libraries(storage_concurrent_tests
    PRIVATE
        test_utils
        $<IF:$<TARGET_EXISTS:gtest::gtest_main>,gtest::gtest_main,GTest::gtest_main>
        yams::storage_engine
        yams::crypto
        Threads::Threads
)

target_include_directories(storage_concurrent_tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/utils
)

# Enable C++20 for tests
target_compile_features(storage_concurrent_tests PRIVATE cxx_std_20)

# Register with CTest
add_test(NAME StorageConcurrentTests COMMAND storage_concurrent_tests)