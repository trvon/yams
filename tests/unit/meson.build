# Unit tests object library with explicit sources (no run_command)

# Preserve prior exclusions:
# - Exclude: api/metadata_api_test.cpp
# - Exclude: compression/{error_handler_test_simple.cpp,error_handler_test.cpp,integrity_validator_test_simple.cpp,integrity_validator_test.cpp}
# - Exclude: content/*, crypto/*, cli/*, detection/* (entire dirs)
# - Exclude: daemon/{multiplex_client_test.cpp,request_dispatcher_plugin_test.cpp,onnx_*}
# - Exclude: manifest/manifest_test.cpp
# - Exclude: integrity/integrity_verifier_test.cpp
# - Exclude: storage/{storage_engine_test.cpp,compressed_storage_engine_test.cpp}

unit_root = files(
  'versioning_document_indexer_test.cpp',
)

unit_api = files(
  'api/content_store_test.cpp',
  # 'api/metadata_api_test.cpp' excluded
)

unit_app_services = files(
  'app/services/document_service_test.cpp',
  'app/services/document_service_errors_test.cpp',
  'app/services/grep_service_test.cpp',
  'app/services/grep_service_edge_cases_test.cpp',
  'app/services/grep_service_unicode_test.cpp',
  'app/services/indexing_service_test.cpp',
  'app/services/search_service_test.cpp',
  'app/services/search_hash_edge_cases_test.cpp',
  'app/services/session_service_test.cpp',
  'app/services/utils_test.cpp',
)

unit_chunking = files(
  'chunking/rabin_chunker_test.cpp',
  'chunking/streaming_chunker_test.cpp',
)

unit_cli = []
if tests_has_cli
  unit_cli += files(
    'cli/daemon_command_test.cpp',
    'cli/model_command_test.cpp',
    'cli/recommendation_util_test.cpp',
    'cli/session_command_test.cpp',
    'cli/stream_heuristics_test.cpp',
    'cli/update_command_test.cpp',
  )

  if get_option('enable-tui')
    unit_cli += files(
      'cli/highlight_utils_test.cpp',
      # Excluded: test_tui_services.cpp has its own main() - it's a standalone test
    )
  endif
endif

unit_compression = files(
  'compression/basic_error_handler_test.cpp',
  'compression/basic_integrity_validator_test.cpp',
  'compression/compression_comprehensive_test.cpp',
  'compression/compression_monitor_test.cpp',
  'compression/compression_policy_test.cpp',
  'compression/recovery_manager_test.cpp',
  'compression/transaction_manager_test.cpp',
  'compression/zstandard_compressor_test.cpp',
  # excluded: error_handler_test_simple.cpp, error_handler_test.cpp, integrity_validator*_test.cpp
)

# LZMA test optional (as before)
lzma_opt = get_option('enable-lzma')
lzma_dep = dependency('lzma', required: false)
unit_compression_opt = []
if lzma_dep.found() and lzma_opt.allowed()
  unit_compression_opt += files('compression/lzma_compressor_test.cpp')
endif

unit_daemon_base = files(
  'daemon/connection_fsm_backpressure_test.cpp',
  'daemon/connection_fsm_error_flow_test.cpp',
  'daemon/connection_fsm_legality_test.cpp',
  'daemon/connection_fsm_shutdown_test.cpp',
  'daemon/connection_fsm_streaming_sequence_test.cpp',
  'daemon/connection_fsm_test.cpp',
  'daemon/connection_fsm_timeout_retry_test.cpp',
  'daemon/daemon_lifecycle_fsm_test.cpp',
  'daemon/service_manager_fsm_test.cpp',
  'daemon/daemon_test.cpp',
  'daemon/daemon_fsm_status_test.cpp',
  'daemon/download_protocol_test.cpp',
  'daemon/get_stats_wal_metrics_test.cpp',
  'daemon/ipc_server_test.cpp',
  'daemon/message_framing_test.cpp',
  'daemon/plugin_host_test.cpp',
  'daemon/plugin_host_fsm_failure_test.cpp',
  'daemon/plugin_host_fsm_recovery_test.cpp',
  'daemon/daemon_plugins_degraded_status_test.cpp',
  'daemon/daemon_plugins_recovery_status_test.cpp',
  'daemon/plugin_loader_test.cpp',
  'daemon/post_ingest_queue_test.cpp',
  'daemon/grep_response_proto_test.cpp',
  'daemon/internal_event_bus_concurrency_test.cpp',
  'daemon/post_ingest_queue_bus_stress_test.cpp',
  'daemon/repair_coordinator_test.cpp',
  'daemon/repair_scheduling_adapter_test.cpp',
  'daemon/request_handler_io_edge_test.cpp',
  'daemon/resource_pool_test.cpp',
  'daemon/response_of_test.cpp',
  'daemon/service_manager_test.cpp',
  'daemon/service_manager_fsm_status_test.cpp',
  'daemon/service_manager_shutdown_ownership_test.cpp',
  'daemon/socket_server_lifecycle_test.cpp',
  'daemon/status_response_proto_test.cpp',
  'daemon/service_manager_provider_degraded_test.cpp',
  'daemon/ipc_message_type_test.cpp',
  'daemon/proto_roundtrip_test.cpp',
  'daemon/streaming_processor_test.cpp',
  'daemon/wasm_plugin_harness_test.cpp',
  # excluded: multiplex_client_test.cpp, request_dispatcher_plugin_test.cpp, onnx_*
)

# Daemon embedding tests - requires ONNX/model support
unit_daemon_embedding = []
if get_option('enable-vector-tests')
  unit_daemon_embedding += files(
    'daemon/embedding_provider_fsm_failure_test.cpp',
    'daemon/embedding_provider_recovery_test.cpp',
    'daemon/status_embedding_test.cpp',
  )
endif

unit_daemon = unit_daemon_base + unit_daemon_embedding

unit_downloader = files(
  'downloader/test_downloader_basic.cpp',
)

unit_integrity = files(
  'integrity/chunk_validator_test.cpp',
  'integrity/integrity_verifier_simple_test.cpp',
  'integrity/repair_manager_test.cpp',
  'integrity/verification_monitor_test.cpp',
  'integrity/verification_scheduler_test.cpp',
  # excluded: integrity_verifier_test.cpp
)

unit_repair_base = []

# Repair embedding tests - requires vector support
unit_repair_embedding = []
if get_option('enable-vector-tests')
  unit_repair_embedding += files(
    'repair/embedding_repair_scan_test.cpp',
  )
endif

unit_repair = unit_repair_base + unit_repair_embedding

unit_manifest = files(
  # excluded: manifest/manifest_test.cpp
)

unit_mcp = files(
  'mcp/initialize_response_test.cpp',
  'mcp/mcp_jsonrpc_roundtrip_test.cpp',
  'mcp/mcp_search_dto_test.cpp',
  'mcp/mcp_server_exec_test.cpp',
  'mcp/mcp_server_test.cpp',
  'mcp/mcp_tools_schema_and_smoke_test.cpp',
  'mcp/mcp_tool_registry_wrappers_test.cpp',
  'mcp/stdio_transport_test.cpp',
  'mcp/websocket_transport_test.cpp',
  'mcp/mcp_socket_resolution_test.cpp',
  'mcp/mcp_doctor_flags_test.cpp',
  'mcp/mcp_add_disconnect_test.cpp',
  'mcp/mcp_disconnect_uniform_errors_test.cpp',
)

unit_metadata = files(
  'metadata/database_test.cpp',
  'metadata/fts5_test.cpp',
  'metadata/kg_store_alias_and_entities_test.cpp',
  'metadata/kg_store_test.cpp',
  'metadata/metadata_repository_test.cpp',
  'metadata/metadata_schema_test.cpp',
  'metadata/query_helpers_test.cpp',
  'metadata/sql_build_select_test.cpp',
  'metadata/concepts_compile_test.cpp',
  'metadata/tags_queryspec_integration_test.cpp',
  'metadata/fts_queryspec_integration_test.cpp',
  'metadata/query_helpers_integration_test.cpp',
  'metadata/database_statement_time_test.cpp',
  'metadata/metadata_repository_cache_test.cpp',
  'metadata/connection_pool_maintenance_smoke_test.cpp',
  'metadata/connection_pool_waiting_guard_test.cpp',
  'metadata/tree_builder_test.cpp',
  'metadata/tree_differ_test.cpp',
)

unit_resolve = files(
  'resolve/resolver_tests.cpp',
)

unit_search = files(
  'search/bk_tree_smoke_test.cpp',
  'search/bk_tree_test.cpp',
  'search/chunk_coverage_test.cpp',
  'search/entity_linker_test.cpp',
  'search/hybrid_grouping_smoke_test.cpp',
  'search/hybrid_search_engine_test.cpp',
  'search/kg_scorer_simple_test.cpp',
  'search/learned_fusion_smoke_test.cpp',
  'search/query_parser_test.cpp',
  'search/query_qualifiers_test.cpp',
  'search/search_cache_test.cpp',
  'search/search_executor_test.cpp',
  'search/search_smoke_test.cpp',
)

unit_storage = files(
  'storage/compressed_storage_stats_test.cpp',
  'storage/object_storage_adapter_test.cpp',
  'storage/reference_counter_test.cpp',
  'storage/s3_signer_test.cpp',
  'storage/test_storage_backend.cpp',
  # excluded: storage_engine_test.cpp, compressed_storage_engine_test.cpp
)

# Vector/embedding tests - excluded from base unit tests (requires ONNX/models)
# These are heavy tests that require model downloads and ONNX runtime
unit_vector = []
if get_option('enable-vector-tests')
  unit_vector += files(
    'vector/document_chunker_test.cpp',
    'vector/embedding_genai_selection_test.cpp',
  )
endif

unit_wal = files(
  # legacy WAL tests removed
)

unit_srcs = []
unit_srcs += unit_root
unit_srcs += unit_api
unit_srcs += unit_app_services
unit_srcs += unit_chunking
unit_srcs += unit_cli
unit_srcs += unit_compression
unit_srcs += unit_compression_opt
unit_srcs += unit_daemon
unit_srcs += unit_downloader
unit_srcs += unit_integrity
unit_srcs += unit_repair
unit_srcs += unit_manifest
unit_srcs += unit_mcp
unit_srcs += unit_metadata
unit_srcs += unit_resolve
unit_srcs += unit_search
unit_srcs += unit_storage
unit_srcs += unit_vector
unit_srcs += unit_wal

yams_unit_objs = static_library('yams_unit_objs', unit_srcs,
  include_directories: incs,
  dependencies: [gtest_compile_dep] + gmock_compile_deps + test_deps,
  cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
  install: false,
  pic: true,
)
