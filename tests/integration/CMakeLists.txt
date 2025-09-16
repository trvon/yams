

# pool_add_then_query_test removed - API changed with Boost.ASIO migration
# Include Boost.Test shim to avoid accidental prg/exec monitor undefined refs
include(GoogleTest)
set(CMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE ${YAMS_GTEST_DISCOVERY_MODE})
include(${CMAKE_SOURCE_DIR}/cmake/boost_test_shim.cmake)

# Services integration tests (DocumentIngestionService, RetrievalService)
add_subdirectory(services)

add_executable(daemon_server_multiplex_integration_test daemon/daemon_server_multiplex_integration_test.cpp)
target_link_libraries(daemon_server_multiplex_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
# Ensure compression objects aren’t GC’d by lld; also support macOS force_load in tests
target_link_libraries(daemon_server_multiplex_integration_test PRIVATE
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--start-group>
        $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
        yams::compression
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--end-group>
)
if(NOT APPLE)
    target_link_options(daemon_server_multiplex_integration_test PRIVATE
        "-Wl,--no-as-needed" "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create" "-Wl,-u,Lzma2EncProps_Init" "-Wl,-u,Lzma2Enc_SetProps" "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties" "-Wl,-u,Lzma2Enc_Encode2" "-Wl,-u,LzmaEncProps_Init" "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate" "-Wl,-u,LzmaDec_Init" "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate" "-Wl,-u,Lzma2Dec_Init" "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()
yams_gtest_discover_tests(daemon_server_multiplex_integration_test PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

add_executable(daemon_search_multiplex_integration_test daemon/daemon_search_multiplex_integration_test.cpp)
target_link_libraries(daemon_search_multiplex_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
target_link_libraries(daemon_search_multiplex_integration_test PRIVATE
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--start-group>
        $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
        yams::compression
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--end-group>
)
if(NOT APPLE)
    target_link_options(daemon_search_multiplex_integration_test PRIVATE
        "-Wl,--no-as-needed" "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create" "-Wl,-u,Lzma2EncProps_Init" "-Wl,-u,Lzma2Enc_SetProps" "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties" "-Wl,-u,Lzma2Enc_Encode2" "-Wl,-u,LzmaEncProps_Init" "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate" "-Wl,-u,LzmaDec_Init" "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate" "-Wl,-u,Lzma2Dec_Init" "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()
yams_gtest_discover_tests(daemon_search_multiplex_integration_test PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})



# pooled_request_integration_test removed - API changed with Boost.ASIO migration

# Boundary IPC tests for pool/daemon/client interactions
## Removed legacy pool tests
# mcp_downloader_integration_test removed - API changed with Boost.ASIO migration





# New comprehensive integration tests: removed for stabilization (will be reintroduced after porting)

# Streaming concurrency test: ensure stream returns while data is being added (always build)
add_executable(daemon_interleaving_integration_test
    daemon/daemon_interleaving_integration_test.cpp)
target_link_libraries(daemon_interleaving_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
target_link_libraries(daemon_interleaving_integration_test PRIVATE
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--start-group>
        $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
        yams::compression
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--end-group>
)
if(NOT APPLE)
    target_link_options(daemon_interleaving_integration_test PRIVATE
        "-Wl,--no-as-needed" "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create" "-Wl,-u,Lzma2EncProps_Init" "-Wl,-u,Lzma2Enc_SetProps" "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties" "-Wl,-u,Lzma2Enc_Encode2" "-Wl,-u,LzmaEncProps_Init" "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate" "-Wl,-u,LzmaDec_Init" "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate" "-Wl,-u,Lzma2Dec_Init" "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()
gtest_discover_tests(daemon_interleaving_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

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
endif()

add_executable(daemon_mux_metrics_integration_test daemon/daemon_mux_metrics_integration_test.cpp)
target_link_libraries(daemon_mux_metrics_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
target_link_libraries(daemon_mux_metrics_integration_test PRIVATE
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--start-group>
        $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
        yams::compression
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--end-group>
)
if(NOT APPLE)
    target_link_options(daemon_mux_metrics_integration_test PRIVATE
        "-Wl,--no-as-needed" "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create" "-Wl,-u,Lzma2EncProps_Init" "-Wl,-u,Lzma2Enc_SetProps" "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties" "-Wl,-u,Lzma2Enc_Encode2" "-Wl,-u,LzmaEncProps_Init" "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate" "-Wl,-u,LzmaDec_Init" "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate" "-Wl,-u,Lzma2Dec_Init" "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()
gtest_discover_tests(daemon_mux_metrics_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

add_executable(daemon_writer_budget_integration_test daemon/daemon_writer_budget_integration_test.cpp)
target_link_libraries(daemon_writer_budget_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
target_link_libraries(daemon_writer_budget_integration_test PRIVATE
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--start-group>
        $<$<PLATFORM_ID:Darwin>:-Wl,-force_load,$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--whole-archive>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:$<TARGET_FILE:yams_compression>>
        $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-whole-archive>
        yams::compression
    $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--end-group>
)
if(NOT APPLE)
    target_link_options(daemon_writer_budget_integration_test PRIVATE
        "-Wl,--no-as-needed" "-Wl,--no-gc-sections"
        "-Wl,-u,Lzma2Enc_Create" "-Wl,-u,Lzma2EncProps_Init" "-Wl,-u,Lzma2Enc_SetProps" "-Wl,-u,Lzma2Enc_Destroy"
        "-Wl,-u,Lzma2Enc_WriteProperties" "-Wl,-u,Lzma2Enc_Encode2" "-Wl,-u,LzmaEncProps_Init" "-Wl,-u,LzmaEncode"
        "-Wl,-u,LzmaDec_Allocate" "-Wl,-u,LzmaDec_Init" "-Wl,-u,LzmaDec_DecodeToBuf"
        "-Wl,-u,Lzma2Dec_Allocate" "-Wl,-u,Lzma2Dec_Init" "-Wl,-u,Lzma2Dec_DecodeToBuf"
    )
endif()
gtest_discover_tests(daemon_writer_budget_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

add_executable(daemon_mux_active_handlers_integration_test daemon/daemon_mux_active_handlers_integration_test.cpp)
target_link_libraries(daemon_mux_active_handlers_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_mux_active_handlers_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

# Prepare session IPC round-trip
add_executable(daemon_prepare_session_integration_test daemon/daemon_prepare_session_integration_test.cpp)
target_link_libraries(daemon_prepare_session_integration_test PRIVATE test_utils gtest_main yams_cli yams_api spdlog::spdlog Threads::Threads yams::boost_test_shim)
gtest_discover_tests(daemon_prepare_session_integration_test DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT} PROPERTIES LABELS "daemon;integration;prepare" TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT})

# Basic daemon communication roundtrip (status/ping/list)
add_executable(daemon_basic_comm_integration_test daemon/daemon_basic_comm_integration_test.cpp)
target_link_libraries(daemon_basic_comm_integration_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
)
yams_gtest_discover_tests(daemon_basic_comm_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)

# Embeddings pipeline integration test
add_executable(daemon_embeddings_integration_test daemon/daemon_embeddings_integration_test.cpp)

# Link the usual integration test deps
target_link_libraries(daemon_embeddings_integration_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
# Preflight helper implementation
target_sources(daemon_embeddings_integration_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/utils/daemon_preflight.cpp)

gtest_discover_tests(daemon_embeddings_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)

# Unary vs streaming semantics protocol behavior
add_executable(daemon_unary_streaming_semantics_test daemon/daemon_unary_streaming_semantics_test.cpp)
target_link_libraries(daemon_unary_streaming_semantics_test PRIVATE
    test_utils
    gtest_main
    yams::daemon
    yams::daemon_client
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
gtest_discover_tests(daemon_unary_streaming_semantics_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)

## Removed external_plugin_integration_test (CLI-driven business logic) after service centralization

add_executable(onnx_plugin_integration_test daemon/onnx_plugin_integration_test.cpp)
target_link_libraries(onnx_plugin_integration_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
# If the ONNX plugin target exists in this build, pass its path to the test
if(TARGET yams_onnx_plugin)
    add_dependencies(onnx_plugin_integration_test yams_onnx_plugin)
    target_compile_definitions(onnx_plugin_integration_test PRIVATE yams_onnx_plugin_BUILT="$<TARGET_FILE:yams_onnx_plugin>")
endif()
gtest_discover_tests(onnx_plugin_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TEST_PREFIX "onnx_plugin_integration_test."
    TIMEOUT 90)

# FTS5 end-to-end — ingest and search
add_executable(fts5_end_to_end_integration_test daemon/fts5_end_to_end_integration_test.cpp)
target_link_libraries(fts5_end_to_end_integration_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    yams_metadata
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
gtest_discover_tests(fts5_end_to_end_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TEST_PREFIX "fts5_end_to_end_integration_test."
    TIMEOUT 90)

# Embeddings vector persistence across restart
add_executable(embeddings_vector_persist_integration_test daemon/embeddings_vector_persist_integration_test.cpp)
target_link_libraries(embeddings_vector_persist_integration_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
# Preflight helper implementation
target_sources(embeddings_vector_persist_integration_test PRIVATE ${CMAKE_SOURCE_DIR}/tests/utils/daemon_preflight.cpp)
# If the ONNX plugin target exists, pass its path so the test can find it
if(TARGET yams_onnx_plugin)
    add_dependencies(embeddings_vector_persist_integration_test yams_onnx_plugin)
    target_compile_definitions(embeddings_vector_persist_integration_test PRIVATE yams_onnx_plugin_BUILT="$<TARGET_FILE:yams_onnx_plugin>")
endif()
gtest_discover_tests(embeddings_vector_persist_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TEST_PREFIX "embeddings_vector_persist_integration_test."
    TIMEOUT 90)

# Semantic search end-to-end: add -> embed -> search
add_executable(semantic_search_e2e_test daemon/semantic_search_e2e_test.cpp)
target_link_libraries(semantic_search_e2e_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)

# Mock content extractor integration test
add_executable(mock_content_extractor_integration_test daemon/mock_content_extractor_integration_test.cpp)
target_link_libraries(mock_content_extractor_integration_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
gtest_discover_tests(mock_content_extractor_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration;plugins"
    TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)

# Auto embed on add integration test
add_executable(auto_embed_on_add_integration_test daemon/auto_embed_on_add_integration_test.cpp)
target_link_libraries(auto_embed_on_add_integration_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
gtest_discover_tests(auto_embed_on_add_integration_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration;embeddings"
    TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)
gtest_discover_tests(semantic_search_e2e_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration"
    TEST_PREFIX "semantic_search_e2e_test."
    TIMEOUT 90)

# Plugins JSON fail-safe integration test
add_executable(daemon_plugins_json_fail_safe_test daemon/daemon_plugins_json_fail_safe_test.cpp)
target_link_libraries(daemon_plugins_json_fail_safe_test PRIVATE
    test_utils
    gtest_main
    yams_cli
    yams_api
    spdlog::spdlog
    Threads::Threads
    yams::boost_test_shim
)
gtest_discover_tests(daemon_plugins_json_fail_safe_test
    DISCOVERY_TIMEOUT ${YAMS_CTEST_DISCOVERY_TIMEOUT}
    PROPERTIES LABELS "daemon;integration;plugins"
    TIMEOUT ${YAMS_CTEST_DEFAULT_TEST_TIMEOUT}
)

# Removed cli_plugin_degraded_output_test after service centralization
