# Dedicated plugin test shard to avoid symbol conflicts.
# These tests require the tree-sitter symbol extractor plugin binary to exist.
if get_option('plugin-symbols')
  plugin_tests = files(
    'symbol_extractor_all_langs_catch2_test.cpp',
    'symbol_extractor_edge_cases_catch2_test.cpp',
    'symbol_extractor_solidity_catch2_test.cpp',
    'onnx_request_plugin_catch2_test.cpp',
    'onnx_provider_plugin_catch2_test.cpp',
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
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DPLUGIN_PATH="' + plugin_path + '"'],
    install: false,
  )

  test('plugins', plugin_exe,
    env: {'YAMS_AUTO_DOWNLOAD_GRAMMARS':'1','YAMS_SKIP_MODEL_LOADING':'1'},
    depends: plugin_runtime_deps,
  )
endif

# S3 object storage plugin smoke test (networked; skips when env is unset)
if get_option('build-plugins') and get_option('plugin-s3')
  s3_plugin_dir = join_paths(plugin_build_dir, 'plugins/object_storage_s3')
  s3_smoke_exe = executable('yams_s3_plugin_smoke',
    files('s3/s3_plugin_smoke_test.cpp'),
    include_directories: incs,
    dependencies: test_deps,
    install: false,
  )

  test('s3_plugin_smoke', s3_smoke_exe,
    env: {
      'YAMS_SKIP_MODEL_LOADING': '1',
      'YAMS_PLUGIN_DIR': s3_plugin_dir,
    },
    depends: [yams_object_storage_s3],
    timeout: 180,
  )
endif

# zyp (Zig YAMS PDF) plugin tests
if get_option('plugin-zyp')
  zyp_plugin_path = join_paths(plugin_build_dir, 'plugins/zyp/yams_zyp' + plugin_ext)

  zyp_test_exe = executable('yams_zyp_tests',
    files('zyp/zyp_plugin_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
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
    files('glint/glint_plugin_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep, nlohmann_json_dep] + test_deps,
    cpp_args: ['-DGLINT_PLUGIN_PATH="' + glint_plugin_path + '"'],
    install: false,
  )

  test('glint_plugin', glint_test_exe,
    env: {'YAMS_SKIP_MODEL_LOADING': '1'},
    timeout: 60,
  )
endif
