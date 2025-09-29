# Integration smoke tests built as a small, isolated binary

integration_smoke_srcs = files(
  # keep the subset small; exclude daemon_client_smoke_test.cpp per prior filter
  'cli_config_smoke_test.cpp',
  'daemon_embeddings_regression_smoke_test.cpp',
  'daemon_ingestion_reliability_smoke_test.cpp',
  'mcp_doctor_positive_smoke_test.cpp',
  'integrity_auto_repair_smoke_test.cpp',
  'smoke_link_test.cpp',
  'storage_compression_policy_builder_smoke_test.cpp',
  'storage_compression_smoke_test.cpp',
  'pdf_search_smoke_test.cpp',
)

integ_exe = executable('yams_integration_smoke_tests', integration_smoke_srcs + files('test_main_smoke.cpp'),
  include_directories: incs,
  dependencies: [gtest_dep, test_deps],
  cpp_args: ['-DYAMS_TESTING=1'],
  install: false,
)

test('integration_smoke', integ_exe,
  suite: ['integration', 'isolated'],
  env: {'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1'},
  is_parallel: false,
  timeout: get_option('integration-smoke-timeout'),
)
