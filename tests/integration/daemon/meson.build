# Daemon integration tests - e2e tests with running daemon

# Always include shutdown/drain tests (does not require CLI)
daemon_integration_srcs = files(
  'daemon_shutdown_drain_test.cpp',
)

# Add CLI-dependent daemon tests when CLI is available
if tests_has_cli
  daemon_integration_srcs += files(
    'semantic_search_e2e_test.cpp',
    'tree_based_list_test.cpp',
  )
else
  message('yams_cli dependency unavailable; skipping CLI-dependent daemon integration tests.')
endif

dbg = (get_option('buildtype') == 'debug')
if (get_option('enable-integration-tests') or dbg) and (get_option('enable-daemon-integration') or dbg) and daemon_integration_srcs.length() > 0
  daemon_integ_exe = executable('yams_integration_daemon_tests', daemon_integration_srcs + files('../../test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('integration_daemon', daemon_integ_exe,
    suite: ['integration', 'daemon', 'isolated'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_DISABLE_VECTORS': '1',
    },
    is_parallel: false,
    timeout: get_option('integration-daemon-timeout'),
  )
else
  message('integration_daemon suite disabled (enable with -Denable-daemon-integration=true).')
endif
