# Service layer unit tests

# IndexingService test
add_executable(indexing_service_test
    indexing_service_test.cpp
)

target_link_libraries(indexing_service_test
    PRIVATE
        test_utils
        gtest_main
        yams_app_services
        yams::metadata
        yams::api
        yams::content
        yams::detection
        yams::search
)

gtest_discover_tests(indexing_service_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 90
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
        test_utils
        gtest_main
        yams_app_services
        yams::metadata
        yams::api
        yams::search
)

gtest_discover_tests(document_service_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 90
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
        test_utils
        gtest_main
        yams_app_services
        yams::daemon
        yams::metadata
        yams::api
        yams::search
)

gtest_discover_tests(search_service_test
    # Use PRE_TEST (default) discovery to avoid running --gtest_list_tests during
    # the parallel build phase, which was causing intermittent "Subprocess killed"
    # failures under heavy link/load (likely resource pressure). Discovery will
    # occur at ctest execution time instead.
    DISCOVERY_MODE PRE_TEST
    DISCOVERY_TIMEOUT 90
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
        test_utils
        gtest_main
        yams_app_services
        yams::metadata
        yams::api
        yams::search
)

gtest_discover_tests(grep_service_test
    # Switch to PRE_TEST discovery for stability (see note above).
    DISCOVERY_MODE PRE_TEST
    DISCOVERY_TIMEOUT 90
    PROPERTIES
        LABELS "unit;services;grep"
        TIMEOUT 120
)

# SessionService tests
add_executable(session_service_test
    session_service_test.cpp
)

target_link_libraries(session_service_test
    PRIVATE
        test_utils
        gtest_main
        yams_app_services
        yams::metadata
        yams::api
)

gtest_discover_tests(session_service_test
    DISCOVERY_MODE POST_BUILD
    DISCOVERY_TIMEOUT 90
    PROPERTIES
        LABELS "unit;services;session"
        TIMEOUT 120
)
