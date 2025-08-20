# CMakeLists for downloader unit tests

# Create test executable
add_executable(downloader_basic_tests
    test_downloader_basic.cpp
)

# Require C++20
target_compile_features(downloader_basic_tests PRIVATE cxx_std_20)

# Link against shared test utilities and required libs
target_link_libraries(downloader_basic_tests
    PRIVATE
        test_utils
        yams_downloader
        nlohmann_json::nlohmann_json
        spdlog::spdlog
        $<IF:$<TARGET_EXISTS:gtest::gtest_main>,gtest::gtest_main,GTest::gtest_main>
)

# Discover and register tests
gtest_discover_tests(downloader_basic_tests
    DISCOVERY_MODE PRE_TEST
)
