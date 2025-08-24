# Service layer unit tests

# IndexingService test
add_executable(indexing_service_test
    indexing_service_test.cpp
)

target_link_libraries(indexing_service_test
    PRIVATE
        yams_app_services
        yams::metadata
        yams::api
        yams::content
        yams::detection
        yams::search
        GTest::gtest
        GTest::gmock
        GTest::gtest_main
)

gtest_discover_tests(indexing_service_test
    PROPERTIES
        LABELS "unit;services;indexing"
        TIMEOUT 120
)

# DocumentService test
add_executable(document_service_test
    document_service_test.cpp
)

target_link_libraries(document_service_test
    PRIVATE
        yams_app_services
        yams::metadata
        yams::api
        yams::search
        GTest::gtest
        GTest::gmock
        GTest::gtest_main
)

gtest_discover_tests(document_service_test
    PROPERTIES
        LABELS "unit;services;document"
        TIMEOUT 120
)

# SearchService test
add_executable(search_service_test
    search_service_test.cpp
)

target_link_libraries(search_service_test
    PRIVATE
        yams_app_services
        yams::metadata
        yams::api
        yams::search
        GTest::gtest
        GTest::gmock
        GTest::gtest_main
)

gtest_discover_tests(search_service_test
    PROPERTIES
        LABELS "unit;services;search"
        TIMEOUT 120
)

# GrepService test
add_executable(grep_service_test
    grep_service_test.cpp
)

target_link_libraries(grep_service_test
    PRIVATE
        yams_app_services
        yams::metadata
        yams::api
        yams::search
        GTest::gtest
        GTest::gmock
        GTest::gtest_main
)

gtest_discover_tests(grep_service_test
    PROPERTIES
        LABELS "unit;services;grep"
        TIMEOUT 120
)
