# Integration smoke tests built as a small, isolated binary (Catch2-based)

integration_smoke_srcs = []

if tests_has_cli11
  integration_smoke_srcs += files('cli_config_smoke_catch2_test.cpp')
else
  message('CLI11 not detected; skipping cli_config_smoke from integration smoke suite.')
endif

integration_smoke_srcs += files(
  'add_command_sandbox_smoke_catch2_test.cpp',
  'cli_transport_fallback_smoke_catch2_test.cpp',
  'daemon_embedded_streaming_smoke_catch2_test.cpp',
  'graph_command_sandbox_smoke_catch2_test.cpp',
  'integrity_auto_repair_smoke_catch2_test.cpp',
  'smoke_link_catch2_test.cpp',
  'storage_compression_policy_builder_smoke_catch2_test.cpp',
  'storage_compression_smoke_catch2_test.cpp',
)

if tests_has_cli
  integration_smoke_srcs += files(
    'cli_subprocess_segfault_regression_smoke_catch2_test.cpp',
    'daemon_cli_start_status_regression_smoke_catch2_test.cpp',
    'daemon_embeddings_regression_smoke_catch2_test.cpp',
    'daemon_ingestion_reliability_smoke_catch2_test.cpp',
    'mcp_doctor_positive_smoke_catch2_test.cpp',
    'pdf_search_smoke_catch2_test.cpp',
  )
else
  message('yams_cli dependency unavailable; skipping CLI-driven smoke tests.')
endif

dbg = (get_option('buildtype') == 'debug')
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-integration-smoke') or dbg) and integration_smoke_srcs.length() > 0 and catch2_dep.found()
  smoke_test_args = []
  if get_option('enable-tsan')
    smoke_test_args += ['~[mcpdocopsfixture]']
  endif

  integ_exe = executable('yams_integration_smoke_tests',
    integration_smoke_srcs + files('test_main_smoke_catch2.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('integration_smoke', integ_exe,
    suite: ['integration', 'isolated'],
    args: smoke_test_args,
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'TSAN_OPTIONS': _test_tsan_options,
    },
    is_parallel: false,
    timeout: get_option('integration-smoke-timeout'),
  )
else
  if not catch2_dep.found()
    message('integration_smoke suite disabled (Catch2 not found).')
  else
    message('integration_smoke suite disabled (enable with -Denable-integration-smoke=true).')
  endif
endif
