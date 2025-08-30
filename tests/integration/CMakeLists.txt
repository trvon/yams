# Toggle legacy integration tests (pre-tinyfsm IPC, older flows)
option(YAMS_BUILD_LEGACY_INTEGRATION_TESTS "Build legacy integration tests (pre-tinyfsm IPC)" OFF)

# Integration test subdirectories (legacy)
if(YAMS_BUILD_LEGACY_INTEGRATION_TESTS)
    add_subdirectory(manifest)
    add_subdirectory(search)
    add_subdirectory(daemon)
endif()

# Add-then-query pooled request test (always build)
if(NOT TARGET pool_add_then_query_test)
    add_executable(pool_add_then_query_test
        daemon/pool_add_then_query_test.cpp
    )
    target_link_libraries(pool_add_then_query_test PRIVATE
        yams_mcp
        yams_cli
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
    )
    add_test(NAME pool_add_then_query_test COMMAND pool_add_then_query_test)
    set_tests_properties(pool_add_then_query_test PROPERTIES LABELS "daemon;integration")
endif()

# Always build the pooled request integration test for PBI-007-01, even if legacy daemon tests are disabled
if(NOT TARGET pooled_request_integration_test)
    add_executable(pooled_request_integration_test
        daemon/pooled_request_integration_test.cpp
    )
    target_link_libraries(pooled_request_integration_test PRIVATE
        yams_mcp
        yams_cli
        yams_api
        yams_search
        yams_metadata
        yams_downloader
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
    )
    # Enable testing-only MCPServer interface for this target
    target_compile_definitions(pooled_request_integration_test PRIVATE YAMS_TESTING)
    add_test(NAME pooled_request_integration_test COMMAND pooled_request_integration_test)
    set_tests_properties(pooled_request_integration_test PROPERTIES LABELS "daemon;integration")
endif()

# Boundary IPC tests for pool/daemon/client interactions
if(NOT TARGET boundary_ipc_pool_tests)
    add_executable(boundary_ipc_pool_tests
        daemon/boundary_ipc_pool_tests.cpp
    )
    target_link_libraries(boundary_ipc_pool_tests PRIVATE
        yams_cli
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
    )
    add_test(NAME boundary_ipc_pool_tests COMMAND boundary_ipc_pool_tests)
    set_tests_properties(boundary_ipc_pool_tests PROPERTIES LABELS "daemon;integration")
endif()
# MCP downloader integration test (header-only MCP tool registry)
add_executable(mcp_downloader_integration_test
    mcp/mcp_downloader_integration_test.cpp
)
target_link_libraries(mcp_downloader_integration_test PRIVATE
    GTest::gtest
    GTest::gtest_main
    nlohmann_json::nlohmann_json
    yams_mcp
)
add_test(NAME mcp_downloader_integration_test COMMAND mcp_downloader_integration_test)





# New comprehensive integration tests
if(YAMS_ENABLE_NEW_INTEGRATION_TESTS)
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

    # Legacy target linking moved to legacy guard above

    # Debug: Check which GoogleTest targets exist (disabled for placeholder)
    # TODO: Re-enable debugging when fixing GoogleTest issues
    # message(STATUS "Integration tests: Checking GoogleTest targets")
    # if(TARGET gtest::gtest)
    #     message(STATUS "Integration tests: Found gtest::gtest")
    # endif()
    # if(TARGET GTest::gtest)
    #     message(STATUS "Integration tests: Found GTest::gtest")
    # endif()
    # if(TARGET gtest)
    #     message(STATUS "Integration tests: Found gtest")
    # endif()
    # if(TARGET gtest_main)
    #     message(STATUS "Integration tests: Found gtest_main")
    # endif()
    # if(TARGET gtest::gtest_main)
    #     message(STATUS "Integration tests: Found gtest::gtest_main")
    # endif()
    # if(TARGET GTest::gtest_main)
    #     message(STATUS "Integration tests: Found GTest::gtest_main")
    # endif()
    # if(TARGET gmock)
    #     message(STATUS "Integration tests: Found gmock")
    # endif()
    # if(TARGET gmock_main)
    #     message(STATUS "Integration tests: Found gmock_main")
    # endif()

    # GoogleTest include directory logic disabled for placeholder
    # TODO: Re-enable when actual integration tests are restored
    # if(TARGET gtest::gtest)
    #     message(STATUS "Integration tests: Using gtest::gtest for includes")
    #     target_include_directories(integration_tests PRIVATE
    #         $<TARGET_PROPERTY:gtest::gtest,INTERFACE_INCLUDE_DIRECTORIES>)
    # elseif(TARGET GTest::gtest)
    #     message(STATUS "Integration tests: Using GTest::gtest for includes")
    #     target_include_directories(integration_tests PRIVATE
    #         $<TARGET_PROPERTY:GTest::gtest,INTERFACE_INCLUDE_DIRECTORIES>)
    # elseif(TARGET gtest)
    #     message(STATUS "Integration tests: Using gtest for includes")
    #     target_include_directories(integration_tests PRIVATE
    #         $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
    # else()
    #     message(WARNING "Integration tests: No GoogleTest target found for include directories")
    # endif()

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
            $<IF:$<TARGET_EXISTS:gtest::gtest>,gtest::gtest,$<IF:$<TARGET_EXISTS:GTest::gtest>,GTest::gtest,gtest>>
            $<IF:$<TARGET_EXISTS:gtest::gtest_main>,gtest::gtest_main,$<IF:$<TARGET_EXISTS:GTest::gtest_main>,GTest::gtest_main,gtest_main>>
    )

        target_compile_features(${_test_target} PRIVATE cxx_std_20)
        target_include_directories(${_test_target} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../)

        # Ensure GoogleTest include directories are available for new test targets
        if(TARGET gtest::gtest)
            target_include_directories(${_test_target} PRIVATE
            $<TARGET_PROPERTY:gtest::gtest,INTERFACE_INCLUDE_DIRECTORIES>)
        elseif(TARGET GTest::gtest)
            target_include_directories(${_test_target} PRIVATE
            $<TARGET_PROPERTY:GTest::gtest,INTERFACE_INCLUDE_DIRECTORIES>)
        elseif(TARGET gtest)
            target_include_directories(${_test_target} PRIVATE
            $<TARGET_PROPERTY:gtest,INTERFACE_INCLUDE_DIRECTORIES>)
        endif()
    endforeach()
endif()

# Streaming concurrency test: ensure stream returns while data is being added (always build)
if(NOT TARGET stream_return_while_adding_test)
    add_executable(stream_return_while_adding_test
        daemon/stream_return_while_adding_test.cpp
    )
    target_link_libraries(stream_return_while_adding_test PRIVATE
        yams_mcp
        yams_cli
        yams_api
        yams_search
        yams_metadata
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        GTest::gtest
        GTest::gtest_main
        Threads::Threads
    )
    add_test(NAME stream_return_while_adding_test COMMAND stream_return_while_adding_test)
    set_tests_properties(stream_return_while_adding_test PROPERTIES LABELS "daemon;integration")
endif()

# Discover legacy tests only if enabled and target exists
if(YAMS_BUILD_LEGACY_INTEGRATION_TESTS)
    if(TARGET integration_tests)
        gtest_discover_tests(integration_tests
            PROPERTIES
                LABELS "integration"
                TIMEOUT 60
        )
    endif()
endif()

# Discover new integration tests
if(YAMS_ENABLE_NEW_INTEGRATION_TESTS)
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
endif()
