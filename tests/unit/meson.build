# Unit tests object library with explicit sources (no run_command)

# If GTest isn't available, skip compiling the legacy GTest unit object library.
# Catch2 test executables are defined in tests/meson.build and can still be
# enabled independently when Catch2 is present.
yams_unit_objs = []

# Preserve prior exclusions:
# - Exclude: api/metadata_api_test.cpp
# - Exclude: compression/{error_handler_test_simple.cpp,error_handler_test.cpp,integrity_validator_test_simple.cpp,integrity_validator_test.cpp}
# - Exclude: content/*, crypto/*, cli/*, detection/* (entire dirs)
# - Exclude: daemon/{multiplex_client_test.cpp,request_dispatcher_plugin_test.cpp,onnx_*}
# - Exclude: manifest/manifest_test.cpp
# - Exclude: integrity/integrity_verifier_test.cpp
# - Exclude: storage/{storage_engine_test.cpp,compressed_storage_engine_test.cpp}

# Migrated to Catch2 - see tests/meson.build (catch2_versioning_submodule)
unit_root = files(
  # 'versioning_document_indexer_test.cpp', # Migrated to Catch2
)

unit_api = files(
  # All API tests migrated to Catch2 - see tests/meson.build catch2_api_submodule_exe
)

unit_app_services = files(
  # Migrated to Catch2 - see tests/meson.build
  # 'app/services/document_service_test.cpp',
  # 'app/services/document_service_errors_test.cpp',
  # Migrated to Catch2 - see tests/meson.build (search_service_catch2_test.cpp)
  # 'app/services/search_service_test.cpp',
  # Migrated to Catch2 - see tests/meson.build (hybrid_search_comprehensive_test.cpp)
  # 'app/services/search_service_comprehensive_test.cpp',
  # Migrated to Catch2 - see tests/meson.build (search_hash_edge_cases_catch2_test.cpp)
  # 'app/services/search_hash_edge_cases_test.cpp',
  # Migrated to Catch2 - see tests/meson.build (service_utilities_test.cpp)
  # 'app/services/session_service_test.cpp',
  # 'app/services/utils_test.cpp',
  # 'app/services/download_service_smoke_test.cpp',
)

unit_chunking = files(
  # Migrated to Catch2 - see tests/meson.build
  # 'chunking/rabin_chunker_test.cpp',
  # 'chunking/streaming_chunker_test.cpp',
)

unit_core = files(
  # Migrated to Catch2 - see tests/meson.build
  # 'core/cpp23_features_test.cpp',
  # 'core/magic_numbers_test.cpp',
)

unit_cli = []
if tests_has_cli
  # Migrated to Catch2: see tests/meson.build catch2_cli_submodule_exe (yams-cli)
  # All CLI tests moved to catch2_cli_submodule_exe
  # unit_cli += files(
  #   'cli/config_command_test.cpp',
  #   'cli/daemon_command_test.cpp',
  #   'cli/doctor_command_test.cpp',
  #   # init_command_test.cpp moved to Catch2 (init_command_catch2_test.cpp)
  #   'cli/model_command_test.cpp',
  #   'cli/recommendation_util_test.cpp',
  #   'cli/session_command_test.cpp',
  #   'cli/stream_heuristics_test.cpp',
  #   'cli/update_command_test.cpp',
  #   'cli/add_command_stdin_test.cpp',
  #   'cli/grep_command_path_tree_test.cpp',
  #   # Error hints and prompt utilities
  #   'cli/error_hints_test.cpp',
  #   'cli/prompt_util_test.cpp',
  #   # CLI11 option isolation (descriptions not leaking into values)
  #   'cli/cli_option_isolation_test.cpp',
  #   # Data dir resolution precedence (config > env > CLI)
  #   'cli/data_dir_resolution_test.cpp',
  # )
endif

unit_compression = files(
  # Migrated to Catch2: see tests/meson.build catch2_compression_submodule_exe (yams-pmf)
  # 'compression/basic_error_handler_test.cpp',
  # 'compression/basic_integrity_validator_test.cpp',
  # 'compression/compression_comprehensive_test.cpp',
  # 'compression/compression_monitor_test.cpp',
  # 'compression/compression_policy_test.cpp',
  # 'compression/recovery_manager_test.cpp',
  # 'compression/transaction_manager_test.cpp',
  # 'compression/zstandard_compressor_test.cpp',
  # excluded: error_handler_test_simple.cpp, error_handler_test.cpp, integrity_validator*_test.cpp
)

# LZMA test optional (as before)
lzma_opt = get_option('enable-lzma')
lzma_dep = dependency('lzma', required: false)
unit_compression_opt = []
if lzma_dep.found() and lzma_opt.allowed()
  # Migrated to Catch2: lzma_compressor_catch2_test.cpp (yams-pmf)
  # unit_compression_opt += files('compression/lzma_compressor_test.cpp')
endif

unit_daemon_base = files(
  # Migrated to Catch2 - see tests/meson.build (metrics_consistency_catch2_test.cpp)
  # 'daemon/metrics_consistency_test.cpp',
  # Daemon FSM tests migrated to Catch2 (see tests/meson.build catch2_test_sources)
  # Previously: 14 FSM test files now consolidated in daemon_fsm_suite_test.cpp

  # Daemon metrics/status tests migrated to Catch2 (daemon_metrics_status_test.cpp)
  # Removed: get_stats_wal_metrics_test.cpp, daemon_plugins_degraded_status_test.cpp,
  #          daemon_plugins_recovery_status_test.cpp, status_response_proto_test.cpp

  # Daemon component tests migrated to Catch2 (daemon_components_test.cpp)
  # Removed: ipc_server_test.cpp (empty), socket_server_lifecycle_test.cpp,
  #          socket_utils_test.cpp, resource_pool_test.cpp

  # Daemon protocol tests migrated to Catch2 (daemon_protocol_test.cpp)
  # Removed: download_protocol_test.cpp, message_framing_test.cpp, grep_response_proto_test.cpp,
  #          response_of_test.cpp, ipc_message_type_test.cpp, proto_roundtrip_test.cpp

  # Daemon background processing tests migrated to Catch2 (daemon_background_processing_test.cpp)
  # Removed: post_ingest_queue_test.cpp, post_ingest_queue_bus_stress_test.cpp,
  #          internal_event_bus_concurrency_test.cpp

  # Daemon plugin harness migrated to Catch2 (onnx_plugin_harness_test.cpp)
  # Now uses Catch2 with timeout handling and enhanced isolation

  # Extracted component tests (PBI-088) - MIGRATED TO CATCH2 (yams-3s4 / yams-zns)
  # Now in tests/meson.build as catch2_daemon_component_submodule_exe
  # 'daemon/components/config_resolver_test.cpp',
  # 'daemon/components/vector_system_manager_test.cpp',
  # 'daemon/components/plugin_manager_test.cpp',
  # 'daemon/components/database_manager_test.cpp',
  # 'daemon/components/request_queue_test.cpp',
  # 'daemon/components/checkpoint_manager_test.cpp',

  # Legacy GTest tests (to be migrated)
  # 'daemon/daemon_test.cpp',  # Migrated to Catch2 (daemon_catch2_test.cpp)
  # Migrated to Catch2 - see tests/meson.build (plugin_infrastructure_test.cpp)
  # 'daemon/plugin_host_test.cpp',
  # 'daemon/repair_coordinator_test.cpp',  # Migrated to Catch2
  # 'daemon/repair_scheduling_adapter_test.cpp',  # Migrated
  # 'daemon/request_handler_io_edge_test.cpp',  # Migrated
  # 'daemon/service_manager_test.cpp',  # Migrated
  # 'daemon/service_manager_shutdown_ownership_test.cpp',  # Migrated
  # 'daemon/service_manager_provider_degraded_test.cpp',  # Migrated
  # 'daemon/streaming_processor_test.cpp',  # Migrated
  # 'daemon/entity_graph_service_test.cpp',  # Migrated
  # Migrated to Catch2 - see tests/meson.build (graph_component_catch2_test.cpp)
  # 'daemon/graph_component_test.cpp',
  # Migrated to Catch2: plugin_host_test.cpp, request_dispatcher_plugin_test.cpp → plugin_infrastructure_test.cpp
  # Excluded: multiplex_client_test.cpp, onnx_*
)

# Daemon embedding tests - requires ONNX/model support
# NOTE: All embedding tests migrated to Catch2 - see tests/meson.build
# catch2_embedding_recovery_exe and catch2_abi_model_provider_dim_exe
unit_daemon_embedding = []

unit_daemon = unit_daemon_base + unit_daemon_embedding

unit_downloader = files(
  # All downloader tests migrated to Catch2 - see tests/meson.build catch2_downloader_submodule_exe
)

# Migrated to Catch2 - see tests/meson.build integrity_catch2_exe
# unit_integrity = files(
#   'integrity/chunk_validator_test.cpp',
#   'integrity/integrity_verifier_simple_test.cpp',
#   'integrity/repair_manager_test.cpp',
#   'integrity/verification_monitor_test.cpp',
#   'integrity/verification_scheduler_test.cpp',
#   # excluded: integrity_verifier_test.cpp
# )
unit_integrity = []

unit_repair_base = []

# Repair embedding tests - migrated to Catch2 (catch2_repair_submodule)
unit_repair_embedding = []
# if get_option('enable-vector-tests')
#   unit_repair_embedding += files(
#     'repair/embedding_repair_scan_test.cpp',  # Migrated to Catch2
#   )
# endif

unit_repair = unit_repair_base + unit_repair_embedding

unit_manifest = files(
  # excluded: manifest/manifest_test.cpp
)

# MCP tests migrated to Catch2 - see tests/meson.build (catch2_mcp_submodule)
# Migration: yams-3s4 / yams-84g
unit_mcp = files(
  # 'mcp/initialize_response_test.cpp',              # Migrated to Catch2
  # 'mcp/mcp_jsonrpc_roundtrip_test.cpp',            # Migrated to Catch2
  # 'mcp/mcp_search_dto_test.cpp',                   # Migrated to Catch2
  # 'mcp/mcp_server_exec_test.cpp',                  # Migrated to Catch2
  # 'mcp/mcp_server_test.cpp',                       # Migrated to Catch2
  # 'mcp/mcp_tools_schema_and_smoke_test.cpp',       # Migrated to Catch2
  # 'mcp/mcp_tool_registry_wrappers_test.cpp',       # Migrated to Catch2
  # 'mcp/stdio_transport_test.cpp',                  # Migrated to Catch2
  # 'mcp/websocket_transport_test.cpp',              # Migrated to Catch2
  # 'mcp/mcp_socket_resolution_test.cpp',            # Migrated to Catch2
  # 'mcp/mcp_doctor_flags_test.cpp',                 # Migrated to Catch2
  # 'mcp/mcp_add_disconnect_test.cpp',               # Migrated to Catch2
  # 'mcp/mcp_disconnect_uniform_errors_test.cpp',    # Migrated to Catch2
  # 'mcp/mcp_status_response_test.cpp',              # Placeholder - not migrated (empty)
)

unit_metadata = files(
  # All metadata tests migrated to Catch2 - see tests/meson.build
  # Migrated to Catch2 - see tests/meson.build (fts5_performance_stress_catch2_test.cpp)
  # 'metadata/fts5_performance_stress_test.cpp',  # Stress test (opt-in via YAMS_RUN_STRESS_TESTS=1)
)

unit_resolve = files(
  # All resolve tests migrated to Catch2 - see tests/meson.build catch2_resolve_submodule_exe
)

# Search tests migrated to Catch2 - see tests/meson.build (catch2_search_submodule)
# Migration: yams-3s4 / yams-1yq
unit_search = files(
  # 'search/bk_tree_smoke_test.cpp',      # Migrated to Catch2 (bk_tree_smoke_catch2_test.cpp)
  # 'search/bk_tree_test.cpp',            # Migrated to Catch2 (bk_tree_catch2_test.cpp)
  # 'search/chunk_coverage_test.cpp',     # Migrated to Catch2 (chunk_coverage_catch2_test.cpp)
  # 'search/entity_linker_test.cpp',      # Migrated to Catch2 (entity_linker_catch2_test.cpp)
  # 'search/kg_scorer_simple_test.cpp',   # Migrated to Catch2 (kg_scorer_simple_catch2_test.cpp)
  # 'search/query_parser_test.cpp',       # Migrated to Catch2 (query_parser_catch2_test.cpp)
  # 'search/query_qualifiers_test.cpp',   # Migrated to Catch2 (query_qualifiers_catch2_test.cpp)
  # 'search/search_executor_test.cpp',    # Migrated to Catch2 (search_executor_catch2_test.cpp)
  # 'search/search_smoke_test.cpp',       # Migrated to Catch2 (search_smoke_catch2_test.cpp)
)

# Plugin tests migrated to Catch2 - see tests/meson.build (plugins_catch2)
unit_plugins = files(
  # 'plugins/plugin_installer_test.cpp',        # Migrated to Catch2
  # 'plugins/plugin_repo_client_test.cpp',      # Migrated to Catch2
  # 'plugins/symbol_extractor_query_test.cpp',  # Migrated to Catch2
)

# Storage tests migrated to Catch2 - see tests/meson.build (catch2_storage_submodule)
# GTest files commented out as part of yams-3s4 / yams-8b9
unit_storage = files(
  # 'storage/compressed_storage_stats_test.cpp',   # Migrated to Catch2
  # 'storage/object_storage_adapter_test.cpp',     # Migrated to Catch2
  # 'storage/reference_counter_test.cpp',          # Migrated to Catch2
  # 'storage/reference_counter_stress_test.cpp',   # Migrated to Catch2
  # 'storage/s3_signer_test.cpp',                  # Migrated to Catch2
  # test_storage_backend.cpp migrated to Catch2 - see tests/meson.build (storage_backend_catch2_test.cpp)
)

# Vector tests migrated to Catch2 - see tests/meson.build (catch2_vector_submodule)
# GTest files commented out as part of yams-3s4 / yams-1d4
unit_vector = []
if get_option('enable-vector-tests')
  unit_vector += files(
    # 'vector/document_chunker_test.cpp',
    # 'vector/embedding_genai_selection_test.cpp',
    # 'vector/sqlite_vec_c_api_smoke_test.cpp',
    # 'vector/vector_dimension_validation_test.cpp',
  )
endif

unit_ingest = []
# Ingest tests migrated to Catch2 or empty - see tests/meson.build when ready
# if get_option('enable-vector-tests')
#   unit_ingest += files(
#     'ingest/ingest_dimension_validation_catch2_test.cpp',  # TODO: Empty, pending implementation
#   )
# endif

unit_wal = files(
  # legacy WAL tests removed
)

unit_srcs = []
unit_srcs += unit_root
unit_srcs += unit_api
unit_srcs += unit_app_services
unit_srcs += unit_chunking
unit_srcs += unit_core
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
unit_srcs += unit_ingest
unit_srcs += unit_wal
unit_srcs += unit_plugins

if gtest_dep.found() and unit_srcs.length() > 0
  yams_unit_objs = static_library('yams_unit_objs', unit_srcs,
    include_directories: incs,
    dependencies: [gtest_compile_dep] + gmock_compile_deps + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    install: false,
    pic: true,
  )
elif unit_srcs.length() == 0
  message('All GTest sources migrated to Catch2; skipping yams_unit_objs.')
else
  message('GTest not found; skipping compilation of yams_unit_objs.')
endif
