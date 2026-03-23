# Meson-native registration for services integration tests (no shell globs)

# Explicit file list keeps configuration deterministic and fast
dbg = (get_option('buildtype') == 'debug')
services_stress_iters = get_option('stress-iters').to_string()
if get_option('enable-tsan')
  services_stress_iters = '20'
endif

# integration_services is now Catch2-only; legacy GTest suite removed.

# Catch2 migration: Retrieval and Ingestion service tests (uses DaemonHarness)
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and catch2_dep.found()
  catch2_retrieval_ingestion_exe = executable('catch2_retrieval_ingestion',
    files('retrieval_and_ingestion_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('retrieval_ingestion_catch2', catch2_retrieval_ingestion_exe,
    suite: ['catch2', 'integration', 'services', 'isolated'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_STRESS_ITERS': services_stress_iters,
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-timeout'),
    # Allow exit 0 when all tests skip (Windows daemon shutdown not yet implemented)
    args: ['--allow-running-no-tests'],
  )
endif

# Catch2 migration: Sync Indexing tests
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and catch2_dep.found()
  catch2_sync_indexing_exe = executable('catch2_sync_indexing',
    files('sync_indexing_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('sync_indexing_catch2', catch2_sync_indexing_exe,
    suite: ['catch2', 'integration', 'sync', 'indexing'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: 120,
    args: ['--allow-running-no-tests'],
  )
endif

# Catch2 migration: UI/CLI expectations service tests
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and catch2_dep.found()
  catch2_ui_cli_expectations_exe = executable('catch2_ui_cli_expectations',
    files('ui_cli_expectations_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  foreach ui_batch : ['batch1', 'batch2', 'batch3a', 'batch3b', 'batch3c', 'batch3d']
    test('ui_cli_expectations_catch2_' + ui_batch, catch2_ui_cli_expectations_exe,
      suite: ['catch2', 'integration', 'services', 'ui'],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_STRESS_ITERS': services_stress_iters,
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
        'TSAN_OPTIONS': _test_tsan_options,
      },
      is_parallel: false,
      timeout: get_option('integration-ui-timeout'),
      args: ['[ui-cli][' + ui_batch + ']', '--allow-running-no-tests'],
    )
  endforeach
endif

# Catch2 migration: grep service expectations
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and catch2_dep.found()
  catch2_grep_service_expectations_exe = executable('catch2_grep_service_expectations',
    files('grep_service_expectations_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('grep_service_expectations_catch2', catch2_grep_service_expectations_exe,
    suite: ['catch2', 'integration', 'services', 'grep'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_STRESS_ITERS': services_stress_iters,
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-ui-timeout'),
    args: ['--allow-running-no-tests'],
  )
endif

# Catch2 migration: content extraction service tests
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and catch2_dep.found()
  catch2_content_extraction_exe = executable('catch2_content_extraction',
    files('content_extraction_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('content_extraction_catch2', catch2_content_extraction_exe,
    suite: ['catch2', 'integration', 'services', 'content'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_STRESS_ITERS': services_stress_iters,
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-ui-timeout'),
    args: ['--allow-running-no-tests'],
  )
endif

# Catch2 migration: IPC conformance service tests
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and catch2_dep.found()
  catch2_ipc_conformance_exe = executable('catch2_ipc_conformance',
    files('ipc_conformance_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('ipc_conformance_catch2', catch2_ipc_conformance_exe,
    suite: ['catch2', 'integration', 'services', 'ipc'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_STRESS_ITERS': services_stress_iters,
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-ui-timeout'),
    args: ['--allow-running-no-tests'],
  )
endif
