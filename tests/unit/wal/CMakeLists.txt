add_executable(wal_manager_unit_tests
    wal_manager_test.cpp
    wal_manager_comprehensive_test.cpp
)

target_link_libraries(wal_manager_unit_tests
    PRIVATE
        test_utils
        gtest_main
        yams::wal
        yams::crypto
)

target_include_directories(wal_manager_unit_tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/utils
)

# Enable C++20 for tests
target_compile_features(wal_manager_unit_tests PRIVATE cxx_std_20)

# Register with CTest
add_test(NAME WALManagerUnitTests COMMAND wal_manager_unit_tests)
