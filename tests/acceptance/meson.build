# PBI-043 End-to-End Acceptance Tests

acceptance_srcs = files(
  'pbi_043_e2e_validation.cpp',
)

acceptance_exe = executable('pbi_043_e2e_validation',
  acceptance_srcs,
  include_directories: incs,
  dependencies: [gtest_dep, test_deps],
  cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
  install: false,
)

# Run acceptance tests in 'acceptance' suite
test('pbi_043_e2e', acceptance_exe,
  suite: 'acceptance',
  timeout: 600,  # 10 minutes for full E2E validation
  env: {
    'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
    'GTEST_BRIEF': '1',
  }
)
