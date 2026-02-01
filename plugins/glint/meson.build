if not get_option('plugin-glint')
  subdir_done()
endif

cpp = meson.get_compiler('cpp')

spdlog_dep = dependency('spdlog', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
yams_config_dep = dependency('yams_config', required: true)

# ONNX Runtime is optional - Glint works in mock mode without it
# Try 'onnxruntime' first (pkg-config name), then 'libonnxruntime' as fallback
onnxruntime_dep = dependency('onnxruntime', required: false)
if not onnxruntime_dep.found()
  onnxruntime_dep = dependency('libonnxruntime', required: false)
endif

yams_onnx_resource_dep = dependency('yams_onnx_resource', required: true)
glint_deps = [spdlog_dep, nlohmann_json_dep, yams_config_dep, yams_onnx_resource_dep]
glint_cpp_args = []

if onnxruntime_dep.found()
  glint_deps += [onnxruntime_dep]
  glint_cpp_args += ['-DYAMS_USE_ONNX_RUNTIME']
  message('Glint plugin: ONNX Runtime enabled')
else
  message('Glint plugin: ONNX Runtime not found, running in mock mode')
endif

glint_sources = files(
  'plugin.cpp',
  'gliner_session.cpp',
)

shared_module(
  'yams_glint',
  glint_sources,
  include_directories: [include_directories('../../include')],
  dependencies: glint_deps,
  cpp_args: glint_cpp_args,
  override_options: ['cpp_std=c++23'],
  build_by_default: true,
  install: true,
  install_dir: plugins_dir,
  name_prefix: '',
)
