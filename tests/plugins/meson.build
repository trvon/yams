plugin_build_dir = meson.project_build_root()
if host_machine.system() == 'windows'
  plugin_ext = '.dll'
elif host_machine.system() == 'darwin'
  plugin_ext = '.dylib'
else
  plugin_ext = '.so'
endif

plugin_test_ld_library_path = ':'.join([
  join_paths(plugin_build_dir, 'src/daemon/resource'),
  plugin_build_dir,
  '/opt/yams/usr/local/lib',
])

# Dedicated symbol plugin shard to avoid ABI symbol conflicts.
if get_option('plugin-symbols')
  symbol_plugin_tests = files(
    'symbol_extractor_all_langs_catch2_test.cpp',
    'symbol_extractor_edge_cases_catch2_test.cpp',
    'symbol_extractor_solidity_catch2_test.cpp',
  )

  plugin_path = join_paths(plugin_build_dir, 'plugins/symbol_extractor_treesitter/yams_symbol_extractor' + plugin_ext)

  plugin_runtime_deps = get_variable('symbol_extractor_plugin_target', [])

  symbol_plugin_exe = executable('yams_symbol_plugin_tests', symbol_plugin_tests,
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DPLUGIN_PATH="' + plugin_path + '"'],
    install: false,
  )

  test('symbol_plugins', symbol_plugin_exe,
    suite: ['plugins', 'symbols', 'catch2'],
    env: {
      'YAMS_AUTO_DOWNLOAD_GRAMMARS': '1',
      'YAMS_SKIP_MODEL_LOADING': '1',
      'LD_LIBRARY_PATH': plugin_test_ld_library_path,
    },
    depends: plugin_runtime_deps,
  )
endif

# ONNX plugin ABI tests are registered separately from symbol plugin tests so
# plugin-symbols=false does not hide ONNX coverage when ONNX is available.
onnx_plugin_runtime_deps = get_variable('yams_onnx_plugin_target', [])
if get_option('plugin-onnx') and onnx_plugin_runtime_deps.length() > 0
  if host_machine.system() == 'windows'
    onnx_plugin_filename = 'yams_onnx_plugin' + plugin_ext
  else
    onnx_plugin_filename = 'libyams_onnx_plugin' + plugin_ext
  endif
  onnx_plugin_path = join_paths(plugin_build_dir, 'plugins/onnx', onnx_plugin_filename)
  onnx_plugin_exe = executable('yams_onnx_plugin_tests',
    files(
      'onnx_request_plugin_catch2_test.cpp',
      'onnx_provider_plugin_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DONNX_PLUGIN_PATH="' + onnx_plugin_path + '"'],
    install: false,
  )

  foreach onnx_case : [
    ['onnx_request_no_inputs', 'OnnxRequestPluginTest.NoInputsError'],
    ['onnx_request_mock_batch_shape', 'OnnxRequestPluginTest.EmbeddingMockBatchShape'],
    ['onnx_provider_runtime_info', 'OnnxProviderPluginTest.RuntimeInfoIncludesHints'],
    ['onnx_provider_missing_runtime', 'OnnxProviderPluginTest.MissingRuntimeReportedViaHealth'],
    ['onnx_provider_config_preload', 'OnnxProviderPluginTest.ConfigPreloadParsingAndReady'],
    ['onnx_provider_batch_errors', 'OnnxProviderPluginTest.GenerateEmbeddingBatchErrors'],
  ]
    test(onnx_case[0], onnx_plugin_exe,
      suite: ['plugins', 'onnx', 'catch2'],
      args: [onnx_case[1], '--allow-running-no-tests'],
      env: {
        'YAMS_SKIP_MODEL_LOADING': '1',
        'LD_LIBRARY_PATH': plugin_test_ld_library_path,
      },
      depends: onnx_plugin_runtime_deps,
      timeout: 120,
    )
  endforeach
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
    suite: ['plugins', 's3', 'smoke'],
    env: {
      'YAMS_SKIP_MODEL_LOADING': '1',
      'YAMS_PLUGIN_DIR': s3_plugin_dir,
      'LD_LIBRARY_PATH': plugin_test_ld_library_path,
    },
    depends: [yams_object_storage_s3],
    timeout: 180,
  )
endif

# zyp (Zig YAMS PDF) plugin tests
zyp_plugin_runtime_deps = []
if is_variable('zyp_plugin')
  zyp_plugin_runtime_deps = [zyp_plugin]
endif
if get_option('plugin-zyp') and zyp_plugin_runtime_deps.length() > 0
  zyp_plugin_path = join_paths(plugin_build_dir, 'plugins/zyp/yams_zyp' + plugin_ext)

  zyp_test_exe = executable('yams_zyp_tests',
    files('zyp/zyp_plugin_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: ['-DZYP_PLUGIN_PATH="' + zyp_plugin_path + '"'],
    install: false,
  )

  test('zyp_plugin', zyp_test_exe,
    suite: ['plugins', 'zyp', 'catch2'],
    env: {
      'YAMS_SKIP_MODEL_LOADING': '1',
      'LD_LIBRARY_PATH': plugin_test_ld_library_path,
    },
    depends: zyp_plugin_runtime_deps,
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

  catch2_allow_all_skipped = files('../scripts/catch2_allow_all_skipped.py')
  test('glint_plugin', python_exe,
    args: [catch2_allow_all_skipped, glint_test_exe],
    suite: ['plugins', 'glint', 'catch2'],
    env: {
      'YAMS_SKIP_MODEL_LOADING': '1',
      'LD_LIBRARY_PATH': plugin_test_ld_library_path,
    },
    depends: glint_test_exe,
    timeout: 60,
  )
endif
