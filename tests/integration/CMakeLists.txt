# Integration test subdirectories
add_subdirectory(manifest)
add_subdirectory(search)

# Integration tests - temporarily disabled due to API changes
# TODO: Update tests to match new API
add_executable(integration_tests
    main.cpp
    # full_system_test.cpp          # API changes needed
    # compression_integration_test.cpp  # API changes needed
    cli_init_integration_test.cpp
    # cli_delete_test.cpp           # Has its own main() - conflicts
)

# New comprehensive integration tests
add_executable(document_lifecycle_test
    document_lifecycle_test.cpp
)

add_executable(multi_command_test
    multi_command_test.cpp
)

add_executable(concurrency_test
    concurrency_test.cpp
)

add_executable(pdf_integration_test
    pdf_integration_test.cpp
)

add_executable(task_management_test
    task_management_test.cpp
)

target_link_libraries(integration_tests
    PRIVATE
        test_utils
        yams::api
        yams::manifest
        yams::storage_engine
        yams::chunking
        yams::crypto
        yams::wal
        yams::integrity
        yams::compression
        GTest::gtest
)

# Link new integration tests
foreach(_test_target IN ITEMS
    document_lifecycle_test
    multi_command_test
    concurrency_test
    pdf_integration_test
    task_management_test
)
    target_link_libraries(${_test_target}
        PRIVATE
            test_utils
            yams::api
            yams::metadata
            yams::extraction
            yams::search
            GTest::gtest
            GTest::gtest_main
    )
    
    target_compile_features(${_test_target} PRIVATE cxx_std_20)
    target_include_directories(${_test_target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)
endforeach()

gtest_discover_tests(integration_tests
    PROPERTIES
        LABELS "integration"
        TIMEOUT 60
)

# Discover new integration tests
foreach(_test_target IN ITEMS
    document_lifecycle_test
    multi_command_test
    concurrency_test
    pdf_integration_test
    task_management_test
)
    gtest_discover_tests(${_test_target}
        PROPERTIES
            LABELS "integration"
            TIMEOUT 120
    )
endforeach()
