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

plugin_runtime_deps = get_variable('symbol_extractor_plugin_target', [])

plugin_exe = executable('yams_plugin_tests', plugin_tests,
  include_directories: incs,
  dependencies: [gtest_dep] + test_deps,
  cpp_args: ['-DPLUGIN_PATH="' + plugin_path + '"'],
  install: false,
)

test('plugins', plugin_exe,
  env: {'YAMS_AUTO_DOWNLOAD_GRAMMARS':'1','YAMS_SKIP_MODEL_LOADING':'1'},
  depends: plugin_runtime_deps,
)

# zyp (Zig YAMS PDF) plugin tests
if get_option('plugin-zyp')
  zyp_plugin_path = join_paths(plugin_build_dir, 'plugins/zyp/yams_zyp' + plugin_ext)

  zyp_test_exe = executable('yams_zyp_tests',
    files('zyp/zyp_plugin_test.cpp', '../test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep] + test_deps,
    cpp_args: ['-DZYP_PLUGIN_PATH="' + zyp_plugin_path + '"'],
    install: false,
  )

  test('zyp_plugin', zyp_test_exe,
    env: {'YAMS_SKIP_MODEL_LOADING': '1'},
    timeout: 60,
  )
endif

# Glint (GLiNER NL entity extractor) plugin tests
if get_option('plugin-glint')
  glint_plugin_path = join_paths(plugin_build_dir, 'plugins/glint/yams_glint' + plugin_ext)

  glint_test_exe = executable('yams_glint_tests',
    files('glint/glint_plugin_test.cpp', '../test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep, nlohmann_json_dep] + test_deps,
    cpp_args: ['-DGLINT_PLUGIN_PATH="' + glint_plugin_path + '"'],
    install: false,
  )

  test('glint_plugin', glint_test_exe,
    env: {'YAMS_SKIP_MODEL_LOADING': '1'},
    timeout: 60,
  )
endif
