# Dedicated plugin test shard to avoid symbol conflicts
plugin_tests = files(
  'symbol_extractor_all_langs_test.cpp',
  '../test_main.cpp',
)

plugin_exe = executable('yams_plugin_tests', plugin_tests,
  include_directories: incs,
  dependencies: [gtest_dep] + test_deps,
  install: false,
)

test('plugins', plugin_exe)
