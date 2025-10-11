# Daemon integration tests - e2e tests with running daemon

daemon_integration_srcs = []

if tests_has_cli
  daemon_integration_srcs += files(
    'semantic_search_e2e_test.cpp',
    'tree_based_list_test.cpp',
  )
else
  message('yams_cli dependency unavailable; skipping daemon integration tests.')
endif

if get_option('enable-integration-tests') and get_option('enable-daemon-integration') and daemon_integration_srcs.length() > 0
  daemon_integ_exe = executable('yams_integration_daemon_tests', daemon_integration_srcs + files('../../test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('integration_daemon', daemon_integ_exe,
    suite: ['integration', 'daemon', 'isolated'],
    env: {'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1'},
    is_parallel: false,
    timeout: get_option('integration-daemon-timeout'),
  )
else
  message('integration_daemon suite disabled (enable with -Denable-daemon-integration=true).')
endif
