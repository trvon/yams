# Daemon Unit Tests

# Daemon core tests
add_executable(daemon_test
    daemon_test.cpp
)
target_link_libraries(daemon_test PRIVATE
    yams::daemon
    GTest::gtest
    GTest::gtest_main
)
add_test(NAME daemon_test COMMAND daemon_test)

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