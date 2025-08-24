# Resolver unit tests

add_executable(resolver_tests
    resolver_tests.cpp
)

target_compile_features(resolver_tests PRIVATE cxx_std_20)

target_link_libraries(resolver_tests
    PRIVATE
        # Prefer modern imported targets if available; fall back to legacy names
        $<IF:$<TARGET_EXISTS:gtest::gtest>,gtest::gtest,
            $<IF:$<TARGET_EXISTS:GTest::gtest>,GTest::gtest,gtest>>
        $<IF:$<TARGET_EXISTS:gtest::gtest_main>,gtest::gtest_main,
            $<IF:$<TARGET_EXISTS:GTest::gtest_main>,GTest::gtest_main,gtest_main>>
        $<IF:$<TARGET_EXISTS:gtest::gmock>,gtest::gmock,
            $<IF:$<TARGET_EXISTS:GTest::gmock>,GTest::gmock,gmock>>
        yams::metadata
)

# Ensure GoogleTest include directories are available (best-effort)
if(TARGET gtest::gtest)
    target_include_directories(resolver_tests PRIVATE
        $<TARGET_PROPERTY:gtest::gtest,INTERFACE_INCLUDE_DIRECTORIES>)
elseif(TARGET GTest::gtest)
    target_include_directories(resolver_tests PRIVATE
        $<TARGET_PROPERTY:GTest::gtest,INTERFACE_INCLUDE_DIRECTORIES>)
elseif(TARGET gtest)
    target_include_directories(resolver_tests PRIVATE
        $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
endif()

gtest_discover_tests(resolver_tests
    PROPERTIES
        LABELS "unit"
        TIMEOUT 60
)
