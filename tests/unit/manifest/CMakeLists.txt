add_executable(manifest_unit_tests
    manifest_test.cpp
)

target_link_libraries(manifest_unit_tests
    PRIVATE
        test_utils
        gtest_main
        yams::manifest
        yams::chunking
        yams::crypto
)

target_include_directories(manifest_unit_tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/utils
)

# Enable C++20 for tests
target_compile_features(manifest_unit_tests PRIVATE cxx_std_20)

# Conditionally link protobuf for tests
if(Protobuf_FOUND)
    target_link_libraries(manifest_unit_tests PRIVATE protobuf::libprotobuf)
endif()

# Register with CTest
add_test(NAME ManifestUnitTests COMMAND manifest_unit_tests)
