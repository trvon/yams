# Meson-native registration for services integration tests (no shell globs)

# Explicit file list keeps configuration deterministic and fast
dbg = (get_option('buildtype') == 'debug')
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-services') or dbg) and tests_has_cli and gtest_dep.found()
  # GTest-based services tests (excluding retrieval_and_ingestion which moved to Catch2)
  services_integration_srcs = files(
    'ipc_conformance_test.cpp',
    'ui_cli_expectations_test.cpp',
    'grep_service_expectations_test.cpp',
    'sync_indexing_test.cpp',
    'content_extraction_test.cpp',
  )

  services_integ_exe = executable('yams_integration_services_tests', services_integration_srcs + files('../../test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('integration_services', services_integ_exe,
    suite: ['integration', 'isolated'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_STRESS_ITERS': get_option('stress-iters').to_string(),
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-timeout'),
  )

  # Custom shard for UI/CLI expectations at the services layer
  test('integration_ui', services_integ_exe,
    suite: ['integration', 'ui'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_STRESS_ITERS': get_option('stress-iters').to_string(),
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-ui-timeout'),
    args: ['--gtest_filter=UiCliExpectationsIT.*:GrepServiceExpectationsIT.*']
  )

  # Catch2 migration: Retrieval and Ingestion service tests (uses DaemonHarness)
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
      'YAMS_STRESS_ITERS': get_option('stress-iters').to_string(),
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-timeout'),
    # Allow exit 0 when all tests skip (Windows daemon shutdown not yet implemented)
    args: ['--allow-running-no-tests'],
  )
elif get_option('enable-integration-tests')
  if not gtest_dep.found()
    message('integration_services suite disabled (GTest not found).')
  else
    message('integration_services suite disabled (enable with -Denable-integration-services=true).')
  endif
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
