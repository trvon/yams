fs = import('fs')

if not get_option('plugin-glint')
  subdir_done()
endif

cpp = meson.get_compiler('cpp')

spdlog_dep = dependency('spdlog', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
yams_config_dep = dependency('yams_config', required: true)

# ONNX Runtime is optional - Glint works in mock mode without it
onnxruntime_dep = onnxruntime_resolved_dep

yams_onnx_resource_dep = dependency('yams_onnx_resource', required: true)
glint_deps = [spdlog_dep, nlohmann_json_dep, yams_config_dep, yams_onnx_resource_dep]
glint_cpp_args = []
glint_link_deps = []

if onnxruntime_dep.found()
  onnxruntime_compile_dep = onnxruntime_dep.partial_dependency(compile_args: true, includes: true)
  onnxruntime_libdirs = []

  onnxruntime_pkg_libdir = onnxruntime_dep.get_variable(pkgconfig: 'libdir', default_value: '')
  if onnxruntime_pkg_libdir != ''
    onnxruntime_libdirs += [onnxruntime_pkg_libdir]
  endif

  onnxruntime_cmake_prefix = onnxruntime_dep.get_variable(cmake: 'PACKAGE_PREFIX_DIR', default_value: '')
  if onnxruntime_cmake_prefix != ''
    onnxruntime_libdirs += [join_paths(onnxruntime_cmake_prefix, 'lib')]
  endif

  if host_machine.system() == 'darwin'
    brew_onnx_libdir = '/opt/homebrew/opt/onnxruntime/lib'
    if fs.exists(brew_onnx_libdir)
      onnxruntime_libdirs += [brew_onnx_libdir]
    endif
  endif

  onnxruntime_link_dep = cpp.find_library('onnxruntime', dirs: onnxruntime_libdirs, required: false)
  if not onnxruntime_link_dep.found()
    onnxruntime_link_dep = cpp.find_library('onnxruntime', required: false)
  endif

  if onnxruntime_link_dep.found()
    glint_deps += [onnxruntime_compile_dep]
    glint_link_deps += [onnxruntime_link_dep]
    glint_cpp_args += ['-DYAMS_USE_ONNX_RUNTIME']
    message('Glint plugin: ONNX Runtime enabled')
  else
    message('Glint plugin: ONNX Runtime headers found but linker library missing, running in mock mode')
  endif
else
  message('Glint plugin: ONNX Runtime not found, running in mock mode')
endif

glint_sources = files(
  'plugin.cpp',
  'gliner_session.cpp',
)

if cpp.get_id() == 'msvc' or cpp.get_id() == 'clang-cl'
  _plugin_std = 'vc++latest'
else
  _plugin_std = 'c++23'
endif

glint_plugin_install_rpath = ''
if host_machine.system() == 'darwin'
  glint_plugin_install_rpath = '@loader_path/../..'
elif host_machine.system() == 'linux'
  glint_plugin_install_rpath = '$ORIGIN/../..'
endif

shared_module(
  'yams_glint',
  glint_sources,
  include_directories: [include_directories('../../include')],
  dependencies: glint_deps + glint_link_deps,
  cpp_args: glint_cpp_args,
  override_options: ['cpp_std=' + _plugin_std],
  build_by_default: true,
  install: true,
  install_dir: plugins_dir,
  install_rpath: glint_plugin_install_rpath,
  name_prefix: '',
)
