# yams_onnx_resource: Shared library for ONNX resource management
# This library is shared between the daemon and ONNX plugin to ensure
# proper symbol resolution on Windows where DLLs require explicit linking.

spdlog_dep = dependency('spdlog', required: true)

onnx_resource_deps = [spdlog_dep]

# Ensure TSAN runtime is linked for this shared library when sanitizer is enabled
if get_option('enable-tsan') and host_machine.system() == 'linux'
  cpp_compiler = meson.get_compiler('cpp')
  tsan_dep = cpp_compiler.find_library('tsan', required: false)
  if tsan_dep.found()
    onnx_resource_deps += tsan_dep
  endif
endif

onnx_resource_sources = [
  'OnnxConcurrencyRegistry.cpp',
]

# Define export macro when building the library
onnx_resource_args = []
if host_machine.system() == 'windows'
  onnx_resource_args += ['-DYAMS_ONNX_RESOURCE_BUILDING']
endif

yams_onnx_resource_lib = shared_library('yams_onnx_resource',
  onnx_resource_sources,
  include_directories: include_directories('../../../include'),
  dependencies: onnx_resource_deps,
  cpp_args: onnx_resource_args,
  install: true,
  install_dir: get_option('libdir'),
  # Version for soname on Linux
  version: meson.project_version(),
)

yams_onnx_resource_dep = declare_dependency(
  link_with: yams_onnx_resource_lib,
  include_directories: include_directories('../../../include'),
)

meson.override_dependency('yams_onnx_resource', yams_onnx_resource_dep)

# Daemon-neutral ABI plugin runtime used by both daemon and local CLI model loading.
# Keep this out of libyams_daemon.a so socket clients can link plugin loading without
# pulling the whole daemon service stack.
yams_plugin_runtime_lib = static_library('yams_plugin_runtime',
  [
    'abi_plugin_loader.cpp',
    'abi_model_provider_adapter.cpp',
    'plugin_host_services_shim.cpp',
  ],
  include_directories: include_directories('../../../include'),
  dependencies: [
    dependency('yams_core'),
    dependency('yams_vector'),
    dependency('yams_app_services'),
    dependency('yams_config'),
    yams_onnx_resource_dep,
    dependency('nlohmann_json'),
    spdlog_dep,
  ],
  install: false,
)

yams_plugin_runtime_dep = declare_dependency(
  link_with: yams_plugin_runtime_lib,
  include_directories: include_directories('../../../include'),
  dependencies: [
    dependency('yams_core'),
    dependency('yams_vector'),
    dependency('yams_app_services'),
    yams_onnx_resource_dep,
    dependency('nlohmann_json'),
    spdlog_dep,
  ],
)

meson.override_dependency('yams_plugin_runtime', yams_plugin_runtime_dep)
