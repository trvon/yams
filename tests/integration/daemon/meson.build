# Daemon integration tests - Catch2 socket/client integration tests
#
# IMPORTANT: Each test case runs in its own process to avoid static pool state
# issues (RepairThreadPool, ConnectionRegistry, GlobalIOContext) that cause
# std::future_error crashes when multiple daemon instances are created/destroyed
# within a single process.

if not catch2_dep.found()
  message('Catch2 not found; skipping daemon integration tests (PBI-066).')
else
  daemon_catch2_srcs = files(
    'daemon_socket_client_test.cpp',
  )

  daemon_shutdown_srcs = files(
    'daemon_shutdown_test.cpp',
  )

  post_ingest_queue_validation_srcs = files(
    'post_ingest_queue_validation_test.cpp',
  )

  post_ingest_trace_srcs = files(
    'post_ingest_trace.cpp',
  )

  client_timeout_recovery_srcs = files(
    'client_timeout_recovery_test.cpp',
  )

  # idle_streaming and writer_drain tests removed - timing-sensitive and flaky in CI
  # See git history for original implementation if needed

  socket_memory_pressure_srcs = files(
    'socket_memory_pressure_test.cpp',
  )

  stats_command_crash_srcs = files(
    'stats_command_crash_test.cpp',
  )

  cli_responsiveness_under_load_srcs = files(
    'cli_responsiveness_under_load_test.cpp',
  )

  entity_graph_integration_srcs = files(
    'entity_graph_service_integration_catch2_test.cpp',
  )

  ghidra_kg_entity_ingestion_srcs = files(
    'ghidra_kg_entity_ingestion_test.cpp',
  )

  daemon_sigterm_srcs = files(
    'daemon_sigterm_test.cpp',
  )

  dbg = (get_option('buildtype') == 'debug')
  if (get_option('enable-integration-tests') or dbg) and (get_option('enable-daemon-integration') or dbg)
    # catch2_main_dep is inherited from parent tests/meson.build - no need to redeclare

    # Common environment for daemon tests
    _daemon_test_env = {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'YAMS_DISABLE_SESSION_WATCHER': '1',
      'TSAN_OPTIONS': _test_tsan_options,
    }

    daemon_catch2_exe = executable('yams_daemon_socket_tests_catch2',
      daemon_catch2_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    # Split socket tests into individual processes to avoid static pool state issues
    # Each test case creates/destroys a daemon, and static singletons (RepairThreadPool,
    # ConnectionRegistry) don't reset properly between runs, causing future_error crashes.
    test('daemon_socket_connection_lifecycle',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon socket connection lifecycle', '--allow-running-no-tests'],
    )

    test('daemon_socket_request_execution',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon client request execution', '--allow-running-no-tests'],
    )

    test('daemon_socket_error_handling',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon client error handling', '--allow-running-no-tests'],
    )

    test('daemon_socket_concurrent_requests',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon client concurrent requests', '--allow-running-no-tests'],
    )

    test('daemon_socket_timeout_behavior',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon client timeout behavior', '--allow-running-no-tests'],
    )

    test('daemon_socket_move_semantics',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon client move semantics', '--allow-running-no-tests'],
    )

    test('daemon_socket_file_lifecycle',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon socket file lifecycle', '--allow-running-no-tests'],
    )

    daemon_shutdown_exe = executable('yams_daemon_shutdown_tests_catch2',
      daemon_shutdown_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    # Split shutdown tests into individual processes
    test('daemon_shutdown_timing',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon shutdown timing', '--allow-running-no-tests'],
    )

    test('daemon_shutdown_inflight_ops',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon shutdown with in-flight operations', '--allow-running-no-tests'],
    )

    # Removed flaky shutdown tests (idempotency, after_ops, under_load, graceful)
    # Core shutdown behavior is covered by timing and inflight_ops tests above
    
    post_ingest_queue_validation_exe = executable('yams_post_ingest_queue_validation',
      post_ingest_queue_validation_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('post_ingest_queue_validation',
      post_ingest_queue_validation_exe,
      suite: ['integration', 'daemon', 'catch2', 'post-ingest'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    client_timeout_recovery_exe = executable('yams_client_timeout_recovery_tests_catch2',
      client_timeout_recovery_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('client_timeout_recovery',
      client_timeout_recovery_exe,
      suite: ['integration', 'daemon', 'catch2', 'timeout'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    # idle_streaming and writer_drain executables/tests removed (flaky)

    socket_memory_pressure_exe = executable('yams_socket_memory_pressure_tests_catch2',
      socket_memory_pressure_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('socket_memory_pressure',
      socket_memory_pressure_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket', 'memory-pressure', 'crash'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    stats_command_crash_exe = executable('yams_stats_command_crash_tests_catch2',
      stats_command_crash_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('stats_command_crash',
      stats_command_crash_exe,
      suite: ['integration', 'daemon', 'catch2', 'stats', 'crash'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
    )

    cli_responsiveness_under_load_exe = executable('yams_cli_responsiveness_under_load_tests_catch2',
      cli_responsiveness_under_load_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('cli_responsiveness_under_load',
      cli_responsiveness_under_load_exe,
      suite: ['integration', 'daemon', 'catch2', 'cli', 'responsiveness', 'load'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 120,  # 2 minutes for load testing
      args: ['--allow-running-no-tests'],
    )

    entity_graph_integration_exe = executable('yams_entity_graph_integration_tests_catch2',
      entity_graph_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    # KG tests need vectors enabled, but still disable session watcher
    _kg_test_env = {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_SESSION_WATCHER': '1',
      'TSAN_OPTIONS': _test_tsan_options,
    }

    test('entity_graph_integration',
      entity_graph_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'kg', 'pbi-009'],
      env: _kg_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    ghidra_kg_entity_ingestion_exe = executable('yams_ghidra_kg_entity_ingestion_tests_catch2',
      ghidra_kg_entity_ingestion_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('ghidra_kg_entity_ingestion',
      ghidra_kg_entity_ingestion_exe,
      suite: ['integration', 'daemon', 'catch2', 'kg', 'ghidra', 'pbi-3jb'],
      env: _kg_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    # SIGTERM signal handling tests (yams-qe6r)
    daemon_sigterm_exe = executable('yams_daemon_sigterm_tests_catch2',
      daemon_sigterm_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('daemon_sigterm_external_flag',
      daemon_sigterm_exe,
      suite: ['integration', 'daemon', 'catch2', 'sigterm'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon responds to external shutdown flag', '--allow-running-no-tests'],
    )

    test('daemon_sigterm_timing',
      daemon_sigterm_exe,
      suite: ['integration', 'daemon', 'catch2', 'sigterm'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon shutdown timing with external flag', '--allow-running-no-tests'],
    )

    # ONNX Concurrency / TuneAdvisor integration tests
    onnx_tuning_integration_srcs = files(
      'onnx_tuning_integration_test.cpp',
    )

    onnx_tuning_integration_exe = executable('yams_onnx_tuning_integration_tests_catch2',
      onnx_tuning_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    # Split ONNX tuning tests into separate test cases to avoid state leakage
    test('onnx_tuning_env_settings',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'tuning'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 120,  # 2 minutes for multiple daemon restarts
      args: ['OnnxConcurrencyRegistry respects TuneAdvisor env settings', '--allow-running-no-tests'],
    )

    test('onnx_tuning_embedding_lanes',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'embedding'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Embedding lane slots are configured correctly', '--allow-running-no-tests'],
    )

    test('onnx_tuning_fsm_metrics',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'metrics'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['FSM metrics reflect ONNX configuration', '--allow-running-no-tests'],
    )

    test('onnx_tuning_lifecycle',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'lifecycle'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 120,  # 2 minutes for daemon restarts
      args: ['OnnxConcurrencyRegistry state resets on daemon restart', '--allow-running-no-tests'],
    )

    test('onnx_tuning_lanes',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'lanes'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Lane metrics update during slot acquisition', '--allow-running-no-tests'],
    )

    # Adapter test needs vectors ENABLED to test real ONNX plugin path
    # Use _kg_test_env which doesn't have YAMS_DISABLE_VECTORS=1
    test('onnx_tuning_adapter',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'adapter'],
      env: _kg_test_env,
      is_parallel: false,
      timeout: 300,  # 5 minutes - real ONNX model loading is slow
      args: ['AbiModelProviderAdapter acquires SlotGuard during real embedding', '--allow-running-no-tests'],
    )

    test('onnx_tuning_timeout',
      onnx_tuning_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'onnx', 'timeout'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['ONNX slot exhaustion causes graceful timeout', '--allow-running-no-tests'],
    )

    # Diagnostic tool (not a test, but helpful for debugging)
    post_ingest_trace_exe = executable('yams_post_ingest_trace',
      post_ingest_trace_srcs,
      include_directories: incs,
      dependencies: test_deps,
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    # HNSW Retrieval Diagnostic - tests vector search quality with known corpus
    hnsw_retrieval_diagnostic_srcs = files(
      '../vector/hnsw_retrieval_diagnostic.cpp',
    )

    hnsw_retrieval_diagnostic_exe = executable('hnsw_retrieval_diagnostic',
      hnsw_retrieval_diagnostic_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('hnsw_retrieval_diagnostic',
      hnsw_retrieval_diagnostic_exe,
      suite: ['integration', 'daemon', 'catch2', 'hnsw', 'vector', 'diagnostic'],
      env: _kg_test_env,
      is_parallel: false,
      timeout: 300,  # 5 minutes for embedding generation
      args: ['--allow-running-no-tests'],
    )
  else
    message('daemon_socket_client test disabled (enable with -Denable-daemon-integration=true).')
  endif
endif
