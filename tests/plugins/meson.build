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

zyp_pdf_metadata_test_exe = executable('yams_zyp_pdf_metadata_tests',
  files(
    'zyp/pdf_metadata_catch2_test.cpp',
    '../../plugins/zyp/pdf_metadata.cpp',
  ),
  include_directories: incs,
  dependencies: [catch2_dep, catch2_main_dep] + test_deps,
  install: false,
)

test('zyp_pdf_metadata', zyp_pdf_metadata_test_exe,
  suite: ['plugins', 'zyp', 'catch2'],
  timeout: 30,
)

# ONNX plugin ABI tests
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
