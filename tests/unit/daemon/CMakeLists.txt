# Daemon Unit Tests

# Daemon core tests
add_executable(daemon_test
    daemon_test.cpp
)
target_link_libraries(daemon_test PRIVATE
    yams::daemon
    yams::daemon_client
    GTest::gtest
    GTest::gtest_main
)
# Define YAMS_TESTING to enable test API
target_compile_definitions(daemon_test PRIVATE YAMS_TESTING)
add_test(NAME daemon_test COMMAND daemon_test)
set_tests_properties(daemon_test PROPERTIES LABELS "daemon;unit")

# IPC server tests
add_executable(ipc_server_test
    ipc_server_test.cpp
)
target_link_libraries(ipc_server_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME ipc_server_test COMMAND ipc_server_test)
set_tests_properties(ipc_server_test PROPERTIES LABELS "daemon;unit")

# Connection FSM tests
add_executable(connection_fsm_test
    connection_fsm_test.cpp
)
target_link_libraries(connection_fsm_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME connection_fsm_test COMMAND connection_fsm_test)
set_tests_properties(connection_fsm_test PROPERTIES LABELS "daemon;unit")

# IPC client tests
add_executable(ipc_client_test
    ipc_client_test.cpp
)
target_link_libraries(ipc_client_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME ipc_client_test COMMAND ipc_client_test)
set_tests_properties(ipc_client_test PROPERTIES LABELS "daemon;unit")

# Repair scheduling adapter tests
add_executable(repair_scheduling_adapter_test
    repair_scheduling_adapter_test.cpp
)
target_link_libraries(repair_scheduling_adapter_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME repair_scheduling_adapter_test COMMAND repair_scheduling_adapter_test)
set_tests_properties(repair_scheduling_adapter_test PROPERTIES LABELS "daemon;unit")

# RepairCoordinator basic behavior test (gating/metrics)
add_executable(repair_coordinator_test
    repair_coordinator_test.cpp
)
target_link_libraries(repair_coordinator_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME repair_coordinator_test COMMAND repair_coordinator_test)
set_tests_properties(repair_coordinator_test PROPERTIES LABELS "daemon;unit")

# Message framing tests
add_executable(message_framing_test
    message_framing_test.cpp
)
target_link_libraries(message_framing_test PRIVATE
    yams::daemon
    ZLIB::ZLIB  # For CRC32
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME message_framing_test COMMAND message_framing_test)
set_tests_properties(message_framing_test PROPERTIES LABELS "daemon;unit")

# Resource pool tests
add_executable(resource_pool_test
    resource_pool_test.cpp
)
target_link_libraries(resource_pool_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME resource_pool_test COMMAND resource_pool_test)
set_tests_properties(resource_pool_test PROPERTIES LABELS "daemon;unit")

# Plugin loader tests
add_executable(plugin_loader_test
    plugin_loader_test.cpp
)
target_link_libraries(plugin_loader_test PRIVATE
    yams::daemon
    ${CMAKE_DL_LIBS}  # For dlopen/dlsym
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME plugin_loader_test COMMAND plugin_loader_test)
set_tests_properties(plugin_loader_test PROPERTIES LABELS "daemon;unit")

# ONNX Model pool tests - only if ONNX plugin is available
find_package(onnxruntime QUIET)

if(onnxruntime_FOUND AND NOT YAMS_DISABLE_ONNX)
    add_executable(onnx_model_pool_test
        onnx_model_pool_test.cpp
    )
    target_link_libraries(onnx_model_pool_test PRIVATE
        yams::daemon
        yams::vector
        yams::onnx_plugin
        GTest::gtest
        GTest::gtest_main
    )
    add_test(NAME onnx_model_pool_test COMMAND onnx_model_pool_test)
    set_tests_properties(onnx_model_pool_test PROPERTIES LABELS "daemon;unit")

    # ONNX Model Provider tests
    add_executable(onnx_model_provider_test
        onnx_model_provider_test.cpp
    )
    target_link_libraries(onnx_model_provider_test PRIVATE
        yams::daemon
        yams::onnx_plugin
        GTest::gtest
        GTest::gtest_main
    )
    add_test(NAME onnx_model_provider_test COMMAND onnx_model_provider_test)
    set_tests_properties(onnx_model_provider_test PROPERTIES LABELS "daemon;unit")
else()
    message(STATUS "Skipping ONNX tests - ONNX Runtime not available")
endif()

# ResponseOf trait tests
add_executable(response_of_test
    response_of_test.cpp
)
target_link_libraries(response_of_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME response_of_test COMMAND response_of_test)
set_tests_properties(response_of_test PROPERTIES LABELS "daemon;unit")

# Download protocol tests
add_executable(download_protocol_test
    download_protocol_test.cpp
)
target_link_libraries(download_protocol_test PRIVATE
    yams::daemon
    ZLIB::ZLIB  # For CRC32
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME download_protocol_test COMMAND download_protocol_test)
set_tests_properties(download_protocol_test PROPERTIES LABELS "daemon;unit")
