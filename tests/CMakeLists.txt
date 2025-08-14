include(GoogleTest)

# Test utilities library
add_library(test_utils INTERFACE)
target_include_directories(test_utils INTERFACE 
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/mocks
)
target_link_libraries(test_utils INTERFACE
    $<IF:$<TARGET_EXISTS:gtest::gtest>,gtest::gtest,GTest::gtest>
    $<IF:$<TARGET_EXISTS:gtest::gmock>,gtest::gmock,GTest::gmock>
    yams::core
)

# Enable better test discovery
set(CMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE PRE_TEST)

# Unit tests
add_subdirectory(unit)

# Integration tests
add_subdirectory(integration)

# Tools tests
add_subdirectory(tools)

# Stress tests
if(YAMS_BUILD_STRESS_TESTS)
    add_subdirectory(stress)
endif()

# Performance benchmarks
if(YAMS_BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# Test data directory
set(YAMS_TEST_DATA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/data")
target_compile_definitions(test_utils INTERFACE
    YAMS_TEST_DATA_DIR="${YAMS_TEST_DATA_DIR}"
)

# Coverage settings
if(YAMS_ENABLE_COVERAGE)
    target_compile_options(test_utils INTERFACE --coverage)
    target_link_options(test_utils INTERFACE --coverage)
endif()