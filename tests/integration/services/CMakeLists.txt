set(TEST_NAME services_retrieval_ingestion_integration_test)

add_executable(${TEST_NAME}
    retrieval_and_ingestion_service_test.cpp
)

target_link_libraries(${TEST_NAME}
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        yams::app_services
        yams::daemon
        spdlog::spdlog
        Threads::Threads
        yams::boost_test_shim
)

yams_gtest_discover_tests(${TEST_NAME}
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "integration;services"
    PROPERTIES TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)
