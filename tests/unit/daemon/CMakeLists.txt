add_executable(daemon_stats_unit_tests
    get_stats_wal_metrics_test.cpp
)

target_link_libraries(daemon_stats_unit_tests
    PRIVATE
        gtest_main
        yams_daemon
        yams::core
)

target_include_directories(daemon_stats_unit_tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/utils
)

target_compile_features(daemon_stats_unit_tests PRIVATE cxx_std_20)

add_test(NAME DaemonStatsUnitTests COMMAND daemon_stats_unit_tests)

# Streaming processor unit tests
add_executable(streaming_processor_test
    streaming_processor_test.cpp
)

target_link_libraries(streaming_processor_test
    PRIVATE
        test_utils
        gtest_main
        yams::daemon
        yams::core
        spdlog::spdlog
        ZLIB::ZLIB
)

# Link IPC object files directly for ABI parity in unit tests
if(TARGET yams_daemon_ipc_obj)
    target_sources(streaming_processor_test PRIVATE $<TARGET_OBJECTS:yams_daemon_ipc_obj>)
endif()

target_compile_features(streaming_processor_test PRIVATE cxx_std_20)

add_test(NAME streaming_processor_test COMMAND streaming_processor_test)
set_tests_properties(streaming_processor_test PROPERTIES LABELS "daemon;unit" TIMEOUT 30)

# Connection FSM legality and behavior tests
add_executable(connection_fsm_legality_test
    connection_fsm_legality_test.cpp
)
target_link_libraries(connection_fsm_legality_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(connection_fsm_legality_test PRIVATE cxx_std_20)
add_test(NAME connection_fsm_legality_test COMMAND connection_fsm_legality_test)
set_tests_properties(connection_fsm_legality_test PROPERTIES LABELS "daemon;unit" TIMEOUT 15)

add_executable(connection_fsm_backpressure_test
    connection_fsm_backpressure_test.cpp
)
target_link_libraries(connection_fsm_backpressure_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(connection_fsm_backpressure_test PRIVATE cxx_std_20)
add_test(NAME connection_fsm_backpressure_test COMMAND connection_fsm_backpressure_test)
set_tests_properties(connection_fsm_backpressure_test PROPERTIES LABELS "daemon;unit" TIMEOUT 15)

add_executable(connection_fsm_timeout_retry_test
    connection_fsm_timeout_retry_test.cpp
)
target_link_libraries(connection_fsm_timeout_retry_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(connection_fsm_timeout_retry_test PRIVATE cxx_std_20)
add_test(NAME connection_fsm_timeout_retry_test COMMAND connection_fsm_timeout_retry_test)
set_tests_properties(connection_fsm_timeout_retry_test PROPERTIES LABELS "daemon;unit" TIMEOUT 15)

add_executable(connection_fsm_shutdown_test
    connection_fsm_shutdown_test.cpp
)
target_link_libraries(connection_fsm_shutdown_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(connection_fsm_shutdown_test PRIVATE cxx_std_20)
add_test(NAME connection_fsm_shutdown_test COMMAND connection_fsm_shutdown_test)
set_tests_properties(connection_fsm_shutdown_test PROPERTIES LABELS "daemon;unit" TIMEOUT 15)

add_executable(connection_fsm_error_flow_test
    connection_fsm_error_flow_test.cpp
)
target_link_libraries(connection_fsm_error_flow_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(connection_fsm_error_flow_test PRIVATE cxx_std_20)
add_test(NAME connection_fsm_error_flow_test COMMAND connection_fsm_error_flow_test)
set_tests_properties(connection_fsm_error_flow_test PROPERTIES LABELS "daemon;unit" TIMEOUT 15)

add_executable(connection_fsm_streaming_sequence_test
    connection_fsm_streaming_sequence_test.cpp
)
target_link_libraries(connection_fsm_streaming_sequence_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(connection_fsm_streaming_sequence_test PRIVATE cxx_std_20)
add_test(NAME connection_fsm_streaming_sequence_test COMMAND connection_fsm_streaming_sequence_test)
set_tests_properties(connection_fsm_streaming_sequence_test PROPERTIES LABELS "daemon;unit" TIMEOUT 15)

add_executable(request_handler_io_edge_test
    request_handler_io_edge_test.cpp
)
target_link_libraries(request_handler_io_edge_test PRIVATE gtest_main yams::daemon_ipc)
target_compile_features(request_handler_io_edge_test PRIVATE cxx_std_20)
add_test(NAME request_handler_io_edge_test COMMAND request_handler_io_edge_test)
set_tests_properties(request_handler_io_edge_test PROPERTIES LABELS "daemon;unit" TIMEOUT 30)

# Plugin host unit tests (ABI + WASM host behaviors without requiring real plugins)
add_executable(plugin_host_test
    plugin_host_test.cpp
)

target_link_libraries(plugin_host_test
    PRIVATE
        gtest_main
        yams::daemon
        yams::core
)

target_compile_features(plugin_host_test PRIVATE cxx_std_20)

add_test(NAME plugin_host_test COMMAND plugin_host_test)
set_tests_properties(plugin_host_test PROPERTIES LABELS "daemon;unit" TIMEOUT 30)

# Ensure test can reference the built mock plugin file path
add_dependencies(plugin_host_test yams_mock_model_plugin)
target_compile_definitions(plugin_host_test PRIVATE TEST_ABI_PLUGIN_FILE="$<TARGET_FILE:yams_mock_model_plugin>")

# Conditional ONNX plugin harness: if target exists, pass its path; otherwise
# test can use env TEST_ONNX_PLUGIN_FILE
add_executable(onnx_plugin_harness_test onnx_plugin_harness_test.cpp)
target_link_libraries(onnx_plugin_harness_test PRIVATE gtest_main yams::daemon yams::core)
target_compile_features(onnx_plugin_harness_test PRIVATE cxx_std_20)
if(TARGET yams_onnx_plugin)
    add_dependencies(onnx_plugin_harness_test yams_onnx_plugin)
    target_compile_definitions(onnx_plugin_harness_test PRIVATE yams_onnx_plugin_BUILT="$<TARGET_FILE:yams_onnx_plugin>")
endif()
add_test(NAME onnx_plugin_harness_test COMMAND onnx_plugin_harness_test)
set_tests_properties(onnx_plugin_harness_test PROPERTIES LABELS "daemon;unit" TIMEOUT 45)

# Conditional WASM plugin harness: relies on TEST_WASM_PLUGIN_FILE
add_executable(wasm_plugin_harness_test wasm_plugin_harness_test.cpp)
target_link_libraries(wasm_plugin_harness_test PRIVATE gtest_main yams::daemon yams::core)
target_compile_features(wasm_plugin_harness_test PRIVATE cxx_std_20)
add_test(NAME wasm_plugin_harness_test COMMAND wasm_plugin_harness_test)
set_tests_properties(wasm_plugin_harness_test PROPERTIES LABELS "daemon;unit" TIMEOUT 45)

# StatusResponse proto roundtrip tests
add_executable(status_response_proto_test
    status_response_proto_test.cpp
)

target_link_libraries(status_response_proto_test
    PRIVATE
        gtest_main
        yams::daemon
        yams::daemon_ipc
)

target_compile_features(status_response_proto_test PRIVATE cxx_std_20)

add_test(NAME status_response_proto_test COMMAND status_response_proto_test)
set_tests_properties(status_response_proto_test PROPERTIES LABELS "daemon;unit" TIMEOUT 30)
