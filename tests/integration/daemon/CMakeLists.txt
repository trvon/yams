# Daemon Integration Tests

# Daemon lifecycle integration test
add_executable(daemon_lifecycle_integration_test
    daemon_lifecycle_integration_test.cpp
)
target_link_libraries(daemon_lifecycle_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
)
add_test(NAME daemon_lifecycle_integration_test COMMAND daemon_lifecycle_integration_test)
set_tests_properties(daemon_lifecycle_integration_test PROPERTIES LABELS "daemon;integration")

# Daemon search integration test
add_executable(daemon_search_integration_test
    daemon_search_integration_test.cpp
)
target_link_libraries(daemon_search_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::search
    yams::metadata
    yams::indexing
)
add_test(NAME daemon_search_integration_test COMMAND daemon_search_integration_test)
set_tests_properties(daemon_search_integration_test PROPERTIES LABELS "daemon;integration")

# Daemon streaming test
add_executable(daemon_streaming_test
    daemon_streaming_test.cpp
)
target_link_libraries(daemon_streaming_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
)
add_test(NAME daemon_streaming_test COMMAND daemon_streaming_test)
set_tests_properties(daemon_streaming_test PROPERTIES LABELS "daemon;integration")

# Daemon resilience test
add_executable(daemon_resilience_test
    daemon_resilience_test.cpp
)
target_link_libraries(daemon_resilience_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
)
add_test(NAME daemon_resilience_test COMMAND daemon_resilience_test)
set_tests_properties(daemon_resilience_test PROPERTIES LABELS "daemon;integration")

# Daemon download integration test
add_executable(daemon_download_integration_test
    daemon_download_integration_test.cpp
)
target_link_libraries(daemon_download_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
    yams::metadata
)
add_test(NAME daemon_download_integration_test COMMAND daemon_download_integration_test)
set_tests_properties(daemon_download_integration_test PROPERTIES LABELS "daemon;integration")

# Split tests: daemon add normalization and daemon delete parity
add_executable(daemon_add_normalization_integration_test
    daemon_delete_add_integration_test.cpp
)
target_link_libraries(daemon_add_normalization_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
    yams::metadata
)
add_test(NAME daemon_add_normalization_integration_test COMMAND daemon_add_normalization_integration_test)
set_tests_properties(daemon_add_normalization_integration_test PROPERTIES LABELS "daemon;integration")

add_executable(daemon_delete_parity_integration_test
    daemon_delete_add_integration_test.cpp
)
target_link_libraries(daemon_delete_parity_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::api
    yams::metadata
)
add_test(NAME daemon_delete_parity_integration_test COMMAND daemon_delete_parity_integration_test)
set_tests_properties(daemon_delete_parity_integration_test PROPERTIES LABELS "daemon;integration")

# Pooled request integration test (uses real daemon)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/pooled_request_integration_test.cpp")
    add_executable(pooled_request_integration_test
        pooled_request_integration_test.cpp
    )
    target_link_libraries(pooled_request_integration_test PRIVATE
        test_utils
        gtest_main
        yams_mcp
        yams_cli
        yams_api
        yams_search
        yams_metadata
        yams_downloader
        spdlog::spdlog
        nlohmann_json::nlohmann_json
        Threads::Threads
    )
    add_test(NAME pooled_request_integration_test COMMAND pooled_request_integration_test)
    set_tests_properties(pooled_request_integration_test PROPERTIES LABELS "daemon;integration")
else()
    message(STATUS "Skipping pooled_request_integration_test (source not present)")
endif()

# Protocol streaming/non-streaming behavioral tests
add_executable(daemon_search_streaming_protocol_test
    daemon_search_streaming_protocol_test.cpp
)
target_link_libraries(daemon_search_streaming_protocol_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams_cli
    spdlog::spdlog
)
add_test(NAME daemon_search_streaming_protocol_test COMMAND daemon_search_streaming_protocol_test)
set_tests_properties(daemon_search_streaming_protocol_test PROPERTIES LABELS "daemon;integration")

# Unary vs streaming semantics tests
add_executable(daemon_unary_streaming_semantics_test
    daemon_unary_streaming_semantics_test.cpp
)
target_link_libraries(daemon_unary_streaming_semantics_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams_cli
    spdlog::spdlog
)
add_test(NAME daemon_unary_streaming_semantics_test COMMAND daemon_unary_streaming_semantics_test)
set_tests_properties(daemon_unary_streaming_semantics_test PROPERTIES LABELS "daemon;integration")

# Status/Stats proto fields smoke test
add_executable(daemon_status_stats_integration_test
    daemon_status_stats_integration_test.cpp
)
target_link_libraries(daemon_status_stats_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams_cli
)
add_test(NAME daemon_status_stats_integration_test COMMAND daemon_status_stats_integration_test)
set_tests_properties(daemon_status_stats_integration_test PROPERTIES LABELS "daemon;integration")

# Embeddings end-to-end (non-streaming) exists in daemon_embeddings_integration_test.cpp
add_executable(daemon_embeddings_integration_test
    daemon_embeddings_integration_test.cpp
)
target_link_libraries(daemon_embeddings_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
)
add_test(NAME daemon_embeddings_integration_test COMMAND daemon_embeddings_integration_test)
set_tests_properties(daemon_embeddings_integration_test PROPERTIES LABELS "daemon;integration")

# Embedding progress streaming over IPC
add_executable(daemon_embedding_streaming_progress_test
    daemon_embedding_streaming_progress_test.cpp
)
target_link_libraries(daemon_embedding_streaming_progress_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
)
add_test(NAME daemon_embedding_streaming_progress_test COMMAND daemon_embedding_streaming_progress_test)
set_tests_properties(daemon_embedding_streaming_progress_test PROPERTIES LABELS "daemon;integration")

# ONNX plugin integration (ABI host)
add_executable(onnx_plugin_integration_test
    onnx_plugin_integration_test.cpp
)
target_link_libraries(onnx_plugin_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
)
# If the ONNX plugin target exists, pass its path so the test can find it
if(TARGET yams_onnx_plugin)
    add_dependencies(onnx_plugin_integration_test yams_onnx_plugin)
    target_compile_definitions(onnx_plugin_integration_test PRIVATE yams_onnx_plugin_BUILT="$<TARGET_FILE:yams_onnx_plugin>")
endif()
add_test(NAME onnx_plugin_integration_test COMMAND onnx_plugin_integration_test)
set_tests_properties(onnx_plugin_integration_test PROPERTIES LABELS "daemon;integration" TIMEOUT 120)

# FTS5 end-to-end
add_executable(fts5_end_to_end_integration_test
    fts5_end_to_end_integration_test.cpp
)
target_link_libraries(fts5_end_to_end_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
    yams::metadata
)
add_test(NAME fts5_end_to_end_integration_test COMMAND fts5_end_to_end_integration_test)
set_tests_properties(fts5_end_to_end_integration_test PROPERTIES LABELS "daemon;integration" TIMEOUT 60)

# Embeddings vector persistence across restart
add_executable(embeddings_vector_persist_integration_test
    embeddings_vector_persist_integration_test.cpp
)
target_link_libraries(embeddings_vector_persist_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
)
if(TARGET yams_onnx_plugin)
    add_dependencies(embeddings_vector_persist_integration_test yams_onnx_plugin)
    target_compile_definitions(embeddings_vector_persist_integration_test PRIVATE yams_onnx_plugin_BUILT="$<TARGET_FILE:yams_onnx_plugin>")
endif()
add_test(NAME embeddings_vector_persist_integration_test COMMAND embeddings_vector_persist_integration_test)
set_tests_properties(embeddings_vector_persist_integration_test PROPERTIES LABELS "daemon;integration" TIMEOUT 120)
