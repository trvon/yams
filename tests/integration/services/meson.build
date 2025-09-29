# Meson-native registration for services integration tests (no shell globs)

# Explicit file list keeps configuration deterministic and fast
services_integration_srcs = files(
  'retrieval_and_ingestion_service_test.cpp',
  'ipc_conformance_test.cpp',
  'ui_cli_expectations_test.cpp',
  'grep_service_expectations_test.cpp',
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
  },
  is_parallel: false,
  timeout: get_option('integration-ui-timeout'),
  args: ['--gtest_filter=UiCliExpectationsIT.*:GrepServiceExpectationsIT.*']
)
