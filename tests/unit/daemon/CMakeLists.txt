# Daemon Unit Tests

# Helper function to create daemon unit tests
function(yams_add_daemon_test TEST_NAME)
    cmake_parse_arguments(ARG "" "TIMEOUT" "SOURCES;LIBRARIES" ${ARGN})

    # Default timeout if not specified
    if(NOT ARG_TIMEOUT)
        set(ARG_TIMEOUT 30)
    endif()

    add_executable(${TEST_NAME} ${ARG_SOURCES})

    target_link_libraries(${TEST_NAME} PRIVATE
        test_utils
        gtest_main
        ${ARG_LIBRARIES}
    )

    # test_utils already wires gtest includes via link targets

    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    set_tests_properties(${TEST_NAME} PROPERTIES LABELS "daemon;unit" TIMEOUT ${ARG_TIMEOUT})
endfunction()

# Main daemon test
add_executable(daemon_test
    daemon_test.cpp
)
target_link_libraries(daemon_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
)
# Define YAMS_TESTING to enable test API
target_compile_definitions(daemon_test PRIVATE YAMS_TESTING)
add_test(NAME daemon_test COMMAND daemon_test)
set_tests_properties(daemon_test PROPERTIES LABELS "daemon;unit" TIMEOUT 45)

## Removed legacy tests during Boost.Asio migration:
## - async_io_socketpair_test (AsyncSocket tests)
## - async_ipc_server tests

# Connection FSM tests
yams_add_daemon_test(connection_fsm_test
    SOURCES connection_fsm_test.cpp
    LIBRARIES yams::daemon
)

# IPC client tests removed - API changed with Boost.ASIO migration

# Repair scheduling adapter tests
yams_add_daemon_test(repair_scheduling_adapter_test
    SOURCES repair_scheduling_adapter_test.cpp
    LIBRARIES yams::daemon
)

# RepairCoordinator basic behavior test (gating/metrics)
yams_add_daemon_test(repair_coordinator_test
    SOURCES repair_coordinator_test.cpp
    LIBRARIES yams::daemon
)

# Message framing tests
yams_add_daemon_test(message_framing_test
    SOURCES message_framing_test.cpp
    LIBRARIES yams::daemon ZLIB::ZLIB  # For CRC32
)

# Resource pool tests
yams_add_daemon_test(resource_pool_test
    SOURCES resource_pool_test.cpp
    LIBRARIES yams::daemon
)

# Plugin loader tests
yams_add_daemon_test(plugin_loader_test
    SOURCES plugin_loader_test.cpp
    LIBRARIES yams::daemon ${CMAKE_DL_LIBS}  # For dlopen/dlsym
)

# ONNX Model pool tests - only if ONNX plugin is available
find_package(onnxruntime QUIET)

if(onnxruntime_FOUND AND NOT YAMS_DISABLE_ONNX)
    add_executable(onnx_model_pool_test
        onnx_model_pool_test.cpp
    )
    target_link_libraries(onnx_model_pool_test PRIVATE
        gtest_main
        test_utils
        yams::daemon
        yams::vector
        yams::onnx_plugin
    )
    add_test(NAME onnx_model_pool_test COMMAND onnx_model_pool_test)
    set_tests_properties(onnx_model_pool_test PROPERTIES LABELS "daemon;unit" TIMEOUT 60)

    # ONNX Model Provider tests
    add_executable(onnx_model_provider_test
        onnx_model_provider_test.cpp
    )
    target_link_libraries(onnx_model_provider_test PRIVATE
        gtest_main
        test_utils
        yams::daemon
        yams::onnx_plugin
    )
    add_test(NAME onnx_model_provider_test COMMAND onnx_model_provider_test)
    set_tests_properties(onnx_model_provider_test PROPERTIES LABELS "daemon;unit" TIMEOUT 60)
else()
    message(STATUS "Skipping ONNX tests - ONNX Runtime not available")
endif()

# ResponseOf trait tests
yams_add_daemon_test(response_of_test
    SOURCES response_of_test.cpp
    LIBRARIES yams::daemon
)

# Download protocol tests
yams_add_daemon_test(download_protocol_test
    SOURCES download_protocol_test.cpp
    LIBRARIES yams::daemon ZLIB::ZLIB  # For CRC32
)
