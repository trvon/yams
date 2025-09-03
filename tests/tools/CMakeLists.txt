# Tools tests

add_executable(maintenance_tools_test
    maintenance_tools_test.cpp
)

target_link_libraries(maintenance_tools_test
    PRIVATE
        test_utils
        yams::storage_engine
        yams::reference_counter
        yams::crypto
        yams::core
        gtest_main
        spdlog::spdlog
)

target_include_directories(maintenance_tools_test
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include
)

target_compile_features(maintenance_tools_test PRIVATE cxx_std_20)

# Add tests to CTest
gtest_discover_tests(maintenance_tools_test
    DISCOVERY_MODE PRE_TEST
)
