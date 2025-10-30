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
      },
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
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
      },
      is_parallel: false,
      timeout: get_option('integration-daemon-timeout'),
    )
  else
    message('daemon_socket_client test disabled (enable with -Denable-daemon-integration=true).')
  endif
endif
