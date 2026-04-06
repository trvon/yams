mobile_test_srcs = files(
  'mobile_abi_smoke_test.cpp',
)

mobile_dep = dependency('yams_mobile', required: get_option('enable-mobile-bindings'))

if mobile_dep.found() and catch2_dep.found() and catch2_main_dep.found()
  mobile_test_deps = [catch2_dep, catch2_main_dep, mobile_dep, dependency('nlohmann_json'),
                      dependency('yams_daemon'), dependency('spdlog'), dependency('boost'),
                      dependency('tl-expected')]

  mobile_test = executable('catch2_mobile_abi', mobile_test_srcs,
    include_directories: incs,
    dependencies: mobile_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    install: false,
  )

  test('mobile_abi_smoke', mobile_test,
    suite: ['mobile', 'abi'],
    timeout: get_option('unit-timeout'),
  )
else
  message('Catch2 or yams_mobile dependency not found; skipping mobile ABI smoke tests.')
endif
