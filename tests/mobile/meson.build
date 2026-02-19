mobile_test_srcs = files(
  'mobile_abi_smoke_test.cpp',
  'main.cpp',
)

mobile_dep = dependency('yams_mobile', required: get_option('enable-mobile-bindings'))

if mobile_dep.found()
  if not gtest_dep.found()
    message('GTest not found; skipping mobile ABI smoke tests.')
  else
    mobile_test_deps = [gtest_dep, mobile_dep, dependency('nlohmann_json')]

    mobile_test = executable('yams_mobile_abi_tests', mobile_test_srcs,
      include_directories: incs,
      dependencies: mobile_test_deps,
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      install: false,
    )

    test('mobile_abi_smoke', mobile_test,
      suite: ['mobile', 'abi'],
      timeout: get_option('unit-timeout'),
    )
  endif
endif
