# Integration tests for CLI commands
# Phase 3, Step 4: Path-tree grep integration tests

dbg = (get_option('buildtype') == 'debug')
if (get_option('enable-integration-tests') or dbg) and tests_has_cli
  cli_integration_srcs = files(
    'grep_path_tree_test.cpp',
  )

  cli_integ_exe = executable('yams_integration_cli_tests', cli_integration_srcs + files('../../test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('integration_cli', cli_integ_exe,
    suite: ['integration', 'cli'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_VECTORS': '1',
    },
    is_parallel: false,
    timeout: get_option('integration-timeout'),
  )
elif get_option('enable-integration-tests')
  message('integration_cli suite disabled (cli not built).')
endif
