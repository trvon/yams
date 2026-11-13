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

  daemon_harness_srcs = files(
    'daemon_harness_catch2_test.cpp',
  )

  post_ingest_queue_validation_srcs = files(
    'post_ingest_queue_validation_test.cpp',
  )

  post_ingest_queue_benchmark_srcs = files(
    'post_ingest_queue_benchmark_test.cpp',
  )

  post_ingest_trace_srcs = files(
    'post_ingest_trace.cpp',
  )

  client_timeout_recovery_srcs = files(
    'client_timeout_recovery_test.cpp',
  )

  codex_sandbox_embedding_regression_srcs = files(
    'codex_sandbox_embedding_regression_test.cpp',
  )

  # idle_streaming and writer_drain tests removed - timing-sensitive and flaky in CI
  # See git history for original implementation if needed

  socket_memory_pressure_srcs = files(
    'socket_memory_pressure_test.cpp',
  )

  daemon_db_corruption_recovery_srcs = files(
    'daemon_db_corruption_recovery_test.cpp',
  )

  cli_responsiveness_under_load_srcs = files(
    'cli_responsiveness_under_load_test.cpp',
  )

  ghidra_kg_entity_ingestion_srcs = files(
    'ghidra_kg_entity_ingestion_test.cpp',
  )

  daemon_sigterm_srcs = files(
    'daemon_sigterm_test.cpp',
  )

  proxy_slot_starvation_srcs = files(
    'proxy_slot_starvation_test.cpp',
  )

  lifecycle_single_instance_srcs = files(
    'lifecycle_single_instance_test.cpp',
  )

  memory_sync_service_integration_srcs = files(
    'memory_sync_service_integration_test.cpp',
  )

  dbg = (get_option('buildtype') == 'debug')
  daemon_integration_timeout = 1200
  if (get_option('enable-integration-tests') or dbg)
    # catch2_main_dep is inherited from parent tests/meson.build - no need to redeclare

    # Common environment for daemon tests
    _daemon_test_env = {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'YAMS_DISABLE_SESSION_WATCHER': '1',
      'TSAN_OPTIONS': _test_tsan_options,
    }

    # Registrations carry no --allow-running-no-tests: in Catch2 v3 that flag also turns
    # "every selected case skipped" into exit 0, so a daemon that never started (or a filter
    # that matches nothing) passed silently. Most daemon IPC cases SKIP on Windows
    # (SKIP_DAEMON_TEST_ON_WINDOWS), and an all-skipped run exits 4, so those registrations are
    # declared under daemon_ipc_supported (tests/meson.build) only.
    # tests/scripts/check_test_filters.py (catch2_filter_policy) fails any registration whose
    # filter selects zero test cases.

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
    if daemon_ipc_supported
      test('daemon_socket_connection_lifecycle',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon socket connection lifecycle'],
      )

      test('daemon_socket_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client request execution'],
      )

      test('daemon_socket_download_job_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket', 'download-jobs'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client download job request execution'],
      )

      test('daemon_socket_model_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client model request execution'],
      )

      test('daemon_socket_plugin_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client plugin request execution'],
      )

      test('daemon_socket_prune_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client prune request execution'],
      )

      test('daemon_socket_graph_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client graph maintenance request execution'],
      )

      test('daemon_socket_collection_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client collection request execution'],
      )

      test('daemon_socket_session_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client session request execution'],
      )

      test('daemon_socket_repair_request_execution',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client repair request execution'],
      )

      test('daemon_socket_error_handling',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client error handling'],
      )

      test('daemon_socket_concurrent_requests',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client concurrent requests'],
      )

      # Independent clients issue overlapping status() calls on one live daemon.
      test('daemon_socket_parallel_status',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client parallel status from independent clients'],
      )

      test('daemon_socket_timeout_behavior',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client timeout behavior'],
      )

      test('daemon_socket_move_semantics',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon client move semantics'],
      )

      test('daemon_socket_file_lifecycle',
        daemon_catch2_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon socket file lifecycle'],
      )
    endif

    daemon_shutdown_exe = executable('yams_daemon_shutdown_tests_catch2',
      daemon_shutdown_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    daemon_harness_exe = executable('yams_daemon_harness_tests_catch2',
      daemon_harness_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('daemon_harness_startup_rollback',
        daemon_harness_exe,
        suite: ['integration', 'daemon', 'catch2', 'harness'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['DaemonHarness rolls back owned endpoints when startup callback fails'],
      )
    endif

    test('daemon_harness_endpoint_ownership',
      daemon_harness_exe,
      suite: ['integration', 'daemon', 'catch2', 'harness'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: daemon_integration_timeout,
      args: ['DaemonHarness preserves endpoints claimed after fixture construction'],
    )

    test('daemon_harness_config_exception',
      daemon_harness_exe,
      suite: ['integration', 'daemon', 'catch2', 'harness'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: daemon_integration_timeout,
      args: ['DaemonHarness restores process state when configuration throws'],
    )

    if daemon_ipc_supported
      test('daemon_harness_config_lifecycle',
        daemon_harness_exe,
        suite: ['integration', 'daemon', 'catch2', 'harness'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['DaemonHarness isolates config and observes lifecycle cleanup'],
      )

      test('daemon_harness_full_isolation',
        daemon_harness_exe,
        suite: ['integration', 'daemon', 'catch2', 'harness'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['DaemonHarness full isolation ignores ambient developer config and data'],
      )

      # Split shutdown tests into individual processes
      test('daemon_shutdown_timing',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon shutdown timing'],
      )

      test('daemon_shutdown_inflight_ops',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon shutdown with in-flight operations'],
      )

      test('daemon_shutdown_draining_connections',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon stop outlives connections still draining on the IO threads'],
      )

      # Restored: idempotency, after-operations, under-load and graceful shutdown. The two that
      # raced (an immediate list after async adds, a fixed 200 ms before shutdown) now wait for the
      # condition they check.
      test('daemon_shutdown_graceful',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon graceful shutdown behavior'],
      )
      test('daemon_shutdown_after_operations',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon shutdown after operations'],
      )
      test('daemon_shutdown_idempotency',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon shutdown idempotency'],
      )
      test('daemon_shutdown_under_load',
        daemon_shutdown_exe,
        suite: ['integration', 'daemon', 'catch2', 'shutdown'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon shutdown under load'],
      )
    endif

    # Core shutdown behavior is covered by timing and inflight_ops tests above
    
    post_ingest_queue_validation_exe = executable('yams_post_ingest_queue_validation',
      post_ingest_queue_validation_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('post_ingest_queue_validation',
        post_ingest_queue_validation_exe,
        suite: ['integration', 'daemon', 'catch2', 'post-ingest', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )
    endif

    # Both benchmark cases SKIP on Windows; the first also SKIPs without a source corpus, the
    # second always runs, so the registration never reports all-skipped on POSIX.
    if get_option('enable-bench-tests') and daemon_ipc_supported
      post_ingest_queue_benchmark_exe = executable('yams_post_ingest_queue_benchmark',
        post_ingest_queue_benchmark_srcs,
        include_directories: incs,
        dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
        cpp_args: ['-DYAMS_TESTING=1'],
        install: false,
      )

      test('post_ingest_queue_benchmark',
        post_ingest_queue_benchmark_exe,
        suite: ['integration', 'daemon', 'catch2', 'post-ingest', 'benchmark'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 600, # Longer timeout for benchmark
      )
    endif

    client_timeout_recovery_exe = executable('yams_client_timeout_recovery_tests_catch2',
      client_timeout_recovery_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('client_timeout_recovery',
        client_timeout_recovery_exe,
        suite: ['integration', 'daemon', 'catch2', 'timeout'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )
    endif

    # idle_streaming and writer_drain executables/tests removed (flaky)

    socket_memory_pressure_exe = executable('yams_socket_memory_pressure_tests_catch2',
      socket_memory_pressure_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('socket_memory_pressure',
        socket_memory_pressure_exe,
        suite: ['integration', 'daemon', 'catch2', 'socket', 'memory-pressure', 'crash'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )
    endif

    daemon_db_corruption_recovery_exe = executable('yams_daemon_db_corruption_recovery_tests_catch2',
      daemon_db_corruption_recovery_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('daemon_db_corruption_recovery',
        daemon_db_corruption_recovery_exe,
        suite: ['integration', 'daemon', 'catch2', 'db-recovery'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )
    endif

    cli_responsiveness_under_load_exe = executable('yams_cli_responsiveness_under_load_tests_catch2',
      cli_responsiveness_under_load_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('cli_responsiveness_under_load',
        cli_responsiveness_under_load_exe,
        suite: ['integration', 'daemon', 'catch2', 'cli', 'responsiveness', 'load', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 120,  # 2 minutes for load testing
      )
    endif

    # KG tests need vectors enabled, but still disable session watcher
    _kg_test_env = {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_SESSION_WATCHER': '1',
      'TSAN_OPTIONS': _test_tsan_options,
    }

    ghidra_kg_entity_ingestion_exe = executable('yams_ghidra_kg_entity_ingestion_tests_catch2',
      ghidra_kg_entity_ingestion_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('ghidra_kg_entity_ingestion',
        ghidra_kg_entity_ingestion_exe,
        suite: ['integration', 'daemon', 'catch2', 'kg', 'ghidra'],
        env: _kg_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )
    endif

    codex_sandbox_embedding_regression_exe = executable('yams_codex_sandbox_embedding_regression_tests_catch2',
      codex_sandbox_embedding_regression_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps,
                     dependency('yams_daemon_client_embedded'), catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('codex_sandbox_embedding_regression',
        codex_sandbox_embedding_regression_exe,
        suite: ['integration', 'daemon', 'catch2', 'sandbox', 'embeddings'],
        env: _kg_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )
    endif

    # SIGTERM signal handling tests (yams-qe6r)
    daemon_sigterm_exe = executable('yams_daemon_sigterm_tests_catch2',
      daemon_sigterm_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    proxy_slot_starvation_exe = executable('yams_proxy_slot_starvation_tests_catch2',
      proxy_slot_starvation_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('proxy_slot_starvation',
        proxy_slot_starvation_exe,
        suite: ['integration', 'daemon', 'catch2', 'proxy', 'slots'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
      )

      test('daemon_sigterm_external_flag',
        daemon_sigterm_exe,
        suite: ['integration', 'daemon', 'catch2', 'sigterm'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon responds to external shutdown flag'],
      )

      test('daemon_sigterm_timing',
        daemon_sigterm_exe,
        suite: ['integration', 'daemon', 'catch2', 'sigterm'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon shutdown timing with external flag'],
      )

      test('daemon_sigterm_signal',
        daemon_sigterm_exe,
        suite: ['integration', 'daemon', 'catch2', 'sigterm'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Daemon handles actual SIGTERM signal'],
      )
    endif

    # Data-dir single-instance enforcement tests
    lifecycle_single_instance_exe = executable('yams_lifecycle_single_instance_tests_catch2',
      lifecycle_single_instance_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    if daemon_ipc_supported
      test('lifecycle_single_instance_lock',
        lifecycle_single_instance_exe,
        suite: ['integration', 'daemon', 'catch2', 'lifecycle', 'single-instance'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 120,  # 2 minutes for takeover scenarios
        args: ['Data-dir lock prevents concurrent access'],
      )

      test('lifecycle_single_instance_release',
        lifecycle_single_instance_exe,
        suite: ['integration', 'daemon', 'catch2', 'lifecycle', 'single-instance'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 60,  # 1 minute for sequential start/stop
        args: ['Data-dir lock released on shutdown'],
      )
    endif

    memory_sync_service_integration_exe = executable('yams_memory_sync_service_integration',
      memory_sync_service_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1', _test_timeout_cpp_arg],
      install: false,
    )

    test('daemon_memory_sync_auth_startup_failure',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 30,
      args: ['Daemon refuses enabled memory sync when writer authentication cannot initialize'],
    )
    test('daemon_memory_sync_periodic_convergence',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync periodically converges and stops cleanly'],
    )
    test('daemon_memory_sync_delete_integrity',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Production document deletion also replicates content-byte deletion'],
    )
    test('daemon_memory_sync_apply_stop',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync stop during production apply prevents later adapters'],
    )
    test('daemon_memory_sync_apply_serialization',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync', 'concurrency'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon serializes concurrent memory sync apply callbacks'],
    )
    test('daemon_memory_sync_offresident_read',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync IPC reads off-resident authenticated winners'],
    )
    test('daemon_memory_sync_mcp_cold_read',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync', 'mcp'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 90,
      args: ['MCP get serves authenticated replicated cold documents after content prerequisites'],
    )
    test('daemon_memory_sync_pressure_health',
        memory_sync_service_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 60,
        args: ['Daemon memory sync pressure defers topology without reporting a hard failure'],
    )
    test('daemon_memory_sync_deferred_total',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon topology status keeps exact deferred totals and retries local prerequisites'],
    )
    test('daemon_memory_sync_prerequisites',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync stage failures neither skip later stages nor block retry'],
    )
    test('daemon_memory_sync_content_prerequisite',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync requires content before metadata visibility'],
    )
    test('daemon_memory_sync_backfill_stop',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync stop during production backfill bounds published documents'],
    )
    test('daemon_memory_sync_backfill_budget',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync backfill budget resumes document cursor'],
    )
    test('daemon_memory_sync_poisoned_blob',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync backfill skips a poisoned blob and continues'],
    )
    test('daemon_memory_sync_oversized_blob',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon memory sync backfill skips an oversized blob and continues'],
    )
    test('daemon_memory_sync_p2p_recovery_readiness',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync', 'p2p'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon readiness and search do not wait for direct P2P recovery'],
    )
    test('daemon_memory_sync_p2p_recovery_throws',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync', 'p2p'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Direct P2P recovery that throws degrades P2P instead of terminating'],
    )
    test('daemon_memory_sync_direct_delta_hydration',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon direct deltas immediately hydrate content and metadata'],
    )
    test('daemon_memory_sync_temporary',
      memory_sync_service_integration_exe,
      suite: ['integration', 'daemon', 'catch2', 'memory-sync'],
      env: _daemon_test_env,
      is_parallel: false,
      timeout: 60,
      args: ['Daemon temporary memory sync converges inside one session and cleans only its namespace'],
    )

    # One registration for both hysteresis cases. They carry [slow] (two auto-repair daemon
    # starts with 400 ms holds), but there is no separate slow lane in CI: the former
    # '[repair-hysteresis]~[slow]' registration selected zero cases and only the '[slow]' one ran.
    # The idle-connection case SKIPs on hosts that do not count silent sockets (macOS); the
    # cross-instance case always runs, so the registration never reports all-skipped.
    if daemon_ipc_supported
      test('lifecycle_repair_hysteresis',
        lifecycle_single_instance_exe,
        suite: ['integration', 'daemon', 'catch2', 'lifecycle', 'repair', 'slow'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 300,
        args: ['[repair-hysteresis]'],
      )
    endif

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
    if daemon_ipc_supported
      test('onnx_tuning_env_settings',
        onnx_tuning_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'onnx', 'tuning'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 300,  # 5 sections, ~32s/section (EmbeddingService quiesce dominates)
        args: ['OnnxConcurrencyRegistry respects TuneAdvisor env settings'],
      )

      test('onnx_tuning_embedding_lanes',
        onnx_tuning_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'onnx', 'embedding'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Embedding lane slots are configured correctly'],
      )

      test('onnx_tuning_fsm_metrics',
        onnx_tuning_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'onnx', 'metrics'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['FSM metrics reflect ONNX configuration'],
      )

      test('onnx_tuning_lifecycle',
        onnx_tuning_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'onnx', 'lifecycle'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 120,  # 2 minutes for daemon restarts
        args: ['OnnxConcurrencyRegistry state resets on daemon restart'],
      )

      test('onnx_tuning_lanes',
        onnx_tuning_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'onnx', 'lanes'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Lane metrics update during slot acquisition'],
      )

      # adapter/config_backend load the real ONNX plugin, so they are declared only when the
      # plugin target exists (plugins/onnx/meson.build; CI configures -Denable-onnx=disabled).
      # Each selects one case, and a build without the plugin could only skip it; gating here
      # replaces --allow-running-no-tests, which also passed a daemon that never started.
      # The plugin directory comes from the build root (harness-only YAMS_TEST_PLUGIN_DIR): the
      # test used to derive <cwd>/build/debug/plugins, which never exists under `meson test`.
      # They need vectors ENABLED, hence _kg_test_env (no YAMS_DISABLE_VECTORS=1). A lane that
      # builds the plugin but exports YAMS_DISABLE_VECTORS / YAMS_SQLITE_VEC_SKIP_INIT, or has
      # no all-MiniLM-L6-v2 model installed, skips the case and fails with exit 4.
      if is_variable('yams_onnx_plugin')
        _onnx_real_plugin_env = _kg_test_env + {
          'YAMS_TEST_PLUGIN_DIR': meson.project_build_root() / 'plugins' / 'onnx',
        }

        test('onnx_tuning_adapter',
          onnx_tuning_integration_exe,
          suite: ['integration', 'daemon', 'catch2', 'onnx', 'adapter'],
          env: _onnx_real_plugin_env,
          depends: [yams_onnx_plugin],
          is_parallel: false,
          timeout: 300,  # 5 minutes - real ONNX model loading is slow
          args: ['AbiModelProviderAdapter acquires SlotGuard during real embedding'],
        )

        test('onnx_tuning_config_backend',
          onnx_tuning_integration_exe,
          suite: ['integration', 'daemon', 'catch2', 'onnx', 'config'],
          env: _onnx_real_plugin_env,
          depends: [yams_onnx_plugin],
          is_parallel: false,
          timeout: 300,
          args: ['Config selecting onnx backend uses ONNX provider for embeddings'],
        )
      endif

      test('onnx_tuning_timeout',
        onnx_tuning_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'onnx', 'timeout'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['ONNX slot exhaustion causes graceful timeout'],
      )
    endif

    # ResourceGovernor integration tests - verify throttling behavior during real operations
    resource_governor_integration_srcs = files(
      'resource_governor_integration_test.cpp',
    )

    resource_governor_integration_exe = executable('yams_resource_governor_integration_tests_catch2',
      resource_governor_integration_srcs,
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, test_deps, catch2_trailing_link_deps],
      cpp_args: ['-DYAMS_TESTING=1'],
      install: false,
    )

    # Split governor tests into separate test cases to avoid state leakage
    if daemon_ipc_supported
      test('resource_governor_status_metrics',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'status'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['ResourceGovernor metrics exposed in daemon status'],
      )

      test('resource_governor_singleton',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'singleton'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['ResourceGovernor singleton is accessible during daemon operation'],
      )

    endif

    # The pressure case SKIPs under UBSan (known Boost.Asio kqueue teardown race), so it is not
    # registered in UBSan builds rather than reporting an all-skipped run.
    if daemon_ipc_supported and not yams_sanitize_undefined
      test('resource_governor_pressure_detection',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'pressure'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 120,  # 2 minutes for ingest + processing
        args: ['Governor detects pressure with low CPU threshold'],
      )
    endif

    if daemon_ipc_supported
      test('resource_governor_scaling_caps',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'scaling'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Governor scaling caps adjust based on pressure level'],
      )

      test('resource_governor_ingest_metrics',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'ingest'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: 120,  # 2 minutes for ingest + processing
        args: ['Status response contains governor metrics during ingest'],
      )

      test('resource_governor_snapshot',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'snapshot'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['ResourceGovernor snapshot contains valid metrics'],
      )

      test('resource_governor_concurrent',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor', 'concurrency'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['Concurrent status queries return consistent governor data'],
      )

      test('resource_governor_policy_surfaces',
        resource_governor_integration_exe,
        suite: ['integration', 'daemon', 'catch2', 'governor'],
        env: _daemon_test_env,
        is_parallel: false,
        timeout: daemon_integration_timeout,
        args: ['ResourceGovernor phase-4 policy surfaces respond during daemon operation'],
      )
    endif

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

    # Declared only when the ONNX plugin is built: without it the single case can only skip.
    # Keeps --allow-running-no-tests (allowlisted in tests/scripts/check_test_filters.py): the
    # case still SKIPs when the host has no ONNX models_root / embedding model installed, which
    # cannot be decided at configure time. Daemon startup failure is a FAIL.
    if is_variable('yams_onnx_plugin')
      test('hnsw_retrieval_diagnostic',
        hnsw_retrieval_diagnostic_exe,
        suite: ['integration', 'daemon', 'catch2', 'hnsw', 'vector', 'diagnostic'],
        env: _kg_test_env,
        depends: [yams_onnx_plugin],
        is_parallel: false,
        timeout: 300,  # 5 minutes for embedding generation
        args: ['--allow-running-no-tests'],
      )
    endif
  else
    message('daemon_socket_client test disabled (enable with -Denable-integration-tests=true).')
  endif
endif
