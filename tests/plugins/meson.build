# Dedicated plugin test shard to avoid symbol conflicts
plugin_tests = files(
  'symbol_extractor_all_langs_test.cpp',
  'symbol_extractor_edge_cases_test.cpp',
  'symbol_extractor_solidity_test.cpp',
  'onnx_request_plugin_test.cpp',
  'onnx_provider_plugin_test.cpp',
  '../test_main.cpp',
)

# Define plugin path based on build directory and platform
plugin_build_dir = meson.project_build_root()
if host_machine.system() == 'windows'
  plugin_ext = '.dll'
elif host_machine.system() == 'darwin'
  plugin_ext = '.dylib'
else
  plugin_ext = '.so'
endif
plugin_path = join_paths(plugin_build_dir, 'plugins/symbol_extractor_treesitter/yams_symbol_extractor' + plugin_ext)

plugin_exe = executable('yams_plugin_tests', plugin_tests,
  include_directories: incs,
  dependencies: [gtest_dep] + test_deps,
  cpp_args: ['-DPLUGIN_PATH="' + plugin_path + '"'],
  install: false,
)

test('plugins', plugin_exe, env: {'YAMS_AUTO_DOWNLOAD_GRAMMARS':'1','YAMS_SKIP_MODEL_LOADING':'1'})
