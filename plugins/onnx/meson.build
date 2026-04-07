fs = import('fs')

onnx_dep = onnxruntime_resolved_dep
spdlog_dep = dependency('spdlog', required: false)
json_dep = dependency('nlohmann_json', required: false)
yams_core = dependency('yams_core', required: false)
yams_onnx_resource = dependency('yams_onnx_resource', required: false)
boost_dep = dependency('boost', modules: ['asio'], required: false)
if not boost_dep.found()
  boost_dep = dependency('boost', required: false)
endif

win_extra_deps = []
if host_machine.system() == 'windows'
  cpp = meson.get_compiler('cpp')
  win_extra_deps += [cpp.find_library('dxgi', required: true)]
endif

onnx_opt = get_option('enable-onnx')
onnx_feature_requested = not onnx_opt.disabled()

if not get_option('plugin-onnx')
  subdir_done()
endif

if not onnx_feature_requested
  message('enable-onnx disabled; skipping yams_onnx_plugin build')
  subdir_done()
endif

# Gracefully skip when ONNX Runtime is not discoverable in this environment.
if not onnx_dep.found()
  message('ONNX Runtime not found in search paths; skipping yams_onnx_plugin')
  subdir_done()
endif

yams_onnx_plugin_srcs = [
  'onnx_plugin.cpp',
  'model_provider.cpp',
  'onnx_text_processing.cpp',
  'ort_runtime_loader.cpp',
  'onnx_model_pool.cpp',
  'onnx_reranker_session.cpp',
  'onnx_colbert_session.cpp',
  '../../src/vector/tokenizer.cpp',
]

plugin_cppargs = []

# GPU provider defines - these MUST be passed to the plugin for GPU acceleration to work.
# Conan sets these in pkg-config Cflags, but CMake method may not propagate them.
# Check if already present via onnx_dep, otherwise use platform detection.
onnx_cflags = onnx_dep.get_variable(pkgconfig: 'Cflags', default_value: '')
if onnx_cflags.contains('YAMS_ONNX_COREML_ENABLED')
  plugin_cppargs += ['-DYAMS_ONNX_COREML_ENABLED=1']
  message('ONNX CoreML GPU support enabled (from pkg-config)')
elif onnx_cflags.contains('YAMS_ONNX_CUDA_ENABLED')
  plugin_cppargs += ['-DYAMS_ONNX_CUDA_ENABLED=1']
  message('ONNX CUDA GPU support enabled (from pkg-config)')
elif onnx_cflags.contains('YAMS_ONNX_DIRECTML_ENABLED')
  plugin_cppargs += ['-DYAMS_ONNX_DIRECTML_ENABLED=1']
  message('ONNX DirectML GPU support enabled (from pkg-config)')
elif onnx_cflags.contains('YAMS_ONNX_MIGRAPHX_ENABLED')
  plugin_cppargs += ['-DYAMS_ONNX_MIGRAPHX_ENABLED=1']
  message('ONNX MIGraphX GPU support enabled (from pkg-config)')
elif host_machine.system() == 'darwin'
  # Fallback: macOS always has CoreML support in prebuilt ONNX Runtime
  plugin_cppargs += ['-DYAMS_ONNX_COREML_ENABLED=1']
  message('ONNX CoreML GPU support enabled (macOS platform detection)')
endif

onnx_plugin_deps = [
  spdlog_dep,
  json_dep,
  yams_core,
  yams_onnx_resource,
  boost_dep,
] + win_extra_deps

onnx_compile_dep = onnx_dep.partial_dependency(compile_args: true, includes: true)

onnx_plugin_deps += [onnx_compile_dep]

onnx_plugin_link_args = []
if host_machine.system() == 'linux'
  # Fail link on unresolved symbols to avoid runtime dlopen failures.
  onnx_plugin_link_args += ['-Wl,-z,defs']
endif

onnx_plugin_install_rpath = ''
if host_machine.system() == 'darwin'
  onnx_plugin_install_rpath = '@loader_path/../..'
elif host_machine.system() == 'linux'
  onnx_plugin_install_rpath = '$ORIGIN/../..'
endif

yams_onnx_plugin = shared_module('yams_onnx_plugin',
  yams_onnx_plugin_srcs,
  include_directories: [ include_directories('../../include') ],
  dependencies: onnx_plugin_deps,
  link_args: onnx_plugin_link_args,
  cpp_args: plugin_cppargs,
  install: true,
  install_dir: plugins_dir,
  install_rpath: onnx_plugin_install_rpath,
)

# --- Windows: Copy runtime DLLs next to the ONNX plugin ---
if host_machine.system() == 'windows' and windows_runtime_dlls.length() > 0
  foreach dll_info : windows_runtime_dlls
    dll_name = dll_info[0]
    dll_src = dll_info[1]
    fs.copyfile(dll_src, dll_name, install: false)
  endforeach
endif
