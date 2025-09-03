# Resolver unit tests

add_executable(resolver_tests
    resolver_tests.cpp
)

target_compile_features(resolver_tests PRIVATE cxx_std_20)

target_link_libraries(resolver_tests
    PRIVATE
        test_utils
        gtest_main
        yams::metadata
)

# Ensure GoogleTest include directories are available (best-effort)
# test_utils already wires gtest/gmock includes

gtest_discover_tests(resolver_tests
    PROPERTIES
        LABELS "unit"
        TIMEOUT 60
)
