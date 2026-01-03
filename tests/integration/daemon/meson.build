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

  idle_streaming_srcs = files(
    'idle_streaming_test.cpp',
  )

  writer_drain_srcs = files(
    'writer_drain_test.cpp',
  )

  socket_memory_pressure_srcs = files(
    'socket_memory_pressure_test.cpp',
  )

  stats_command_crash_srcs = files(
    'stats_command_crash_test.cpp',
  )

  entity_graph_integration_srcs = files(
    'entity_graph_service_integration_catch2_test.cpp',
  )

  ghidra_kg_entity_ingestion_srcs = files(
    'ghidra_kg_entity_ingestion_test.cpp',
  )

  dbg = (get_option('buildtype') == 'debug')
  if (get_option('enable-integration-tests') or dbg) and (get_option('enable-daemon-integration') or dbg)
    # Catch2 needs its main library
    catch2_main_dep = dependency('catch2-with-main', required: false)
    if not catch2_main_dep.found()
      catch2_main_dep = dependency('Catch2WithMain', required: false, method: 'cmake')
    endif

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
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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

    test('daemon_shutdown_idempotency',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon shutdown idempotency', '--allow-running-no-tests'],
    )

    test('daemon_shutdown_after_ops',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon shutdown after operations', '--allow-running-no-tests'],
    )

    test('daemon_shutdown_under_load',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon shutdown under load', '--allow-running-no-tests'],
    )

    test('daemon_shutdown_graceful',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['Daemon graceful shutdown behavior', '--allow-running-no-tests'],
    )
    
    post_ingest_queue_validation_exe = executable('yams_post_ingest_queue_validation',
      post_ingest_queue_validation_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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

    idle_streaming_exe = executable('yams_idle_streaming_tests_catch2',
      idle_streaming_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('idle_streaming',
      idle_streaming_exe,
      suite: ['integration', 'daemon', 'catch2', 'idle', 'streaming'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    writer_drain_exe = executable('yams_writer_drain_tests_catch2',
      writer_drain_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('writer_drain',
      writer_drain_exe,
      suite: ['integration', 'daemon', 'catch2', 'writer-drain', 'streaming'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    socket_memory_pressure_exe = executable('yams_socket_memory_pressure_tests_catch2',
      socket_memory_pressure_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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

    entity_graph_integration_exe = executable('yams_entity_graph_integration_tests_catch2',
      entity_graph_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
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

    # Diagnostic tool (not a test, but helpful for debugging)
    post_ingest_trace_exe = executable('yams_post_ingest_trace',
      post_ingest_trace_srcs,
      include_directories: incs,
      dependencies: test_deps,
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )
  else
    message('daemon_socket_client test disabled (enable with -Denable-daemon-integration=true).')
  endif
endif
