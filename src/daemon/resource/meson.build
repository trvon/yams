# yams_onnx_resource: Shared library for ONNX resource management
# This library is shared between the daemon and ONNX plugin to ensure
# proper symbol resolution on Windows where DLLs require explicit linking.

spdlog_dep = dependency('spdlog', required: true)

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
  dependencies: [spdlog_dep],
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
