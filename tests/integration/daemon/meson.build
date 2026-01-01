# Daemon integration tests - Catch2 socket/client integration tests

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

  connection_pool_stale_srcs = files(
    'connection_pool_stale_test.cpp',
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
    
    daemon_catch2_exe = executable('yams_daemon_socket_tests_catch2',
      daemon_catch2_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('daemon_socket_client',
      daemon_catch2_exe,
      suite: ['integration', 'daemon', 'catch2', 'socket'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    daemon_shutdown_exe = executable('yams_daemon_shutdown_tests_catch2',
      daemon_shutdown_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('daemon_shutdown',
      daemon_shutdown_exe,
      suite: ['integration', 'daemon', 'catch2', 'shutdown'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
      args: ['--allow-running-no-tests'],
    )

    connection_pool_stale_exe = executable('yams_connection_pool_stale_tests_catch2',
      connection_pool_stale_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('connection_pool_stale',
      connection_pool_stale_exe,
      suite: ['integration', 'daemon', 'catch2', 'connection-pool'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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

    test('entity_graph_integration',
      entity_graph_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'kg', 'pbi-009'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'TSAN_OPTIONS': _test_tsan_options,
      },
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
