# Integrity unit tests

# Check if required targets exist
if(TARGET yams::integrity AND TARGET yams::storage_engine AND TARGET yams::crypto)

    add_executable(integrity_unit_tests
        integrity_verifier_simple_test.cpp
        verification_scheduler_test.cpp
        verification_monitor_test.cpp
    )

    target_link_libraries(integrity_unit_tests
        PRIVATE
            yams::integrity
            yams::storage_engine
            yams::reference_counter
            yams::crypto
            yams::core
            GTest::gtest
            GTest::gtest_main
            GTest::gmock
            spdlog::spdlog
    )

    target_include_directories(integrity_unit_tests
        PRIVATE
            ${CMAKE_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/tests/utils
    )

    # Add tests to CTest
    gtest_discover_tests(integrity_unit_tests
        DISCOVERY_MODE PRE_TEST
    )

else()
    message(STATUS "Skipping integrity unit tests - required targets not available")
endif()