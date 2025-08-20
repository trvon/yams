# Detection unit tests
add_executable(detection_tests
    file_type_detector_test.cpp
    file_type_graceful_fallback_test.cpp
    command_integration_test.cpp
)

target_link_libraries(detection_tests
    PRIVATE
        GTest::gtest_main
        GTest::gmock
        yams::detection
        yams_cli
        yams::metadata
        yams::api
        yams::core
)

# Add test data directory
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test_data/valid_magic_numbers.json 
               ${CMAKE_CURRENT_BINARY_DIR}/test_data/valid_magic_numbers.json COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/test_data/invalid_magic_numbers.json 
               ${CMAKE_CURRENT_BINARY_DIR}/test_data/invalid_magic_numbers.json COPYONLY)

gtest_discover_tests(detection_tests
    PROPERTIES
        LABELS "unit"
        TIMEOUT 30
    DISCOVERY_MODE PRE_TEST
)