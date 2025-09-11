include(GoogleTest)

# Global test options for stabilization
option(YAMS_BUILD_LEGACY_INTEGRATION_TESTS "Build legacy integration tests" OFF)
option(YAMS_ENABLE_BENCHMARKS "Enable benchmarks in CTest" OFF)
option(YAMS_ENABLE_NEW_INTEGRATION_TESTS "Enable new integration tests" ON)

# Reasonable default discovery and test timeouts
set(YAMS_CTEST_DISCOVERY_TIMEOUT "30" CACHE STRING "Default GTest discovery timeout (s)")
set(YAMS_CTEST_DEFAULT_TEST_TIMEOUT "60" CACHE STRING "Default per-test timeout (s)")

# Test utilities library
add_library(test_utils INTERFACE)
target_include_directories(test_utils INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/mocks
)
# Prefer in-tree/fetched googletest targets to avoid picking up sanitized system libs
target_link_libraries(test_utils INTERFACE
    $<IF:$<TARGET_EXISTS:gtest>,gtest,$<IF:$<TARGET_EXISTS:GTest::gtest>,GTest::gtest,$<IF:$<TARGET_EXISTS:gtest::gtest>,gtest::gtest,>>> 
    $<IF:$<TARGET_EXISTS:gmock>,gmock,$<IF:$<TARGET_EXISTS:GTest::gmock>,GTest::gmock,$<IF:$<TARGET_EXISTS:gtest::gmock>,gtest::gmock,>>> 
    yams::core
)

# Define YAMS_TESTING for all tests to enable test-safe code paths
target_compile_definitions(test_utils INTERFACE YAMS_TESTING=1)

# Enable better test discovery
set(CMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE POST_BUILD)
set_property(GLOBAL PROPERTY CTEST_TARGETS_ADDED 1)
set(ENV{CTEST_OUTPUT_ON_FAILURE} 1)

# Unit tests
add_subdirectory(unit)

# Build test plugins/fixtures
add_subdirectory(plugins)


# SDK tests (Python-based)
add_subdirectory(sdk)


# Integration tests
add_subdirectory(integration)


# Tools tests
add_subdirectory(tools)

# Stress tests
if(YAMS_BUILD_STRESS_TESTS)
    add_subdirectory(stress)
endif()

# Performance benchmarks
if(YAMS_BUILD_BENCHMARKS AND YAMS_ENABLE_BENCHMARKS)
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
