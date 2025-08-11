# Stress test subdirectories
add_subdirectory(storage)

# Combined stress test executable (optional)
add_executable(all_stress_tests
    main.cpp
    compression_stress_test.cpp
)

target_link_libraries(all_stress_tests
    PRIVATE
        test_utils
        yams::storage_engine
        yams::compression
        GTest::gtest
)

# Discover stress tests
gtest_discover_tests(all_stress_tests
    PROPERTIES
        LABELS "stress"
        TIMEOUT 300  # 5 minutes for stress tests
)