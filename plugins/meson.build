plugins_dir = get_option('libdir') / 'yams' / 'plugins'

# Every plugin builds with hidden visibility and links with an export list that
# keeps only the yams_plugin_* C ABI (nothing from statically linked deps).
plugin_export_link_args = []
plugin_export_link_depends = []
if host_machine.system() == 'darwin'
  plugin_export_link_depends = files('exports/yams_plugin.exp')
  plugin_export_link_args = ['-Wl,-exported_symbols_list,' + meson.current_source_dir() / 'exports' / 'yams_plugin.exp']
elif host_machine.system() != 'windows'
  plugin_export_link_depends = files('exports/yams_plugin.map')
  plugin_export_link_args = meson.get_compiler('cpp').get_supported_link_arguments(
    '-Wl,--version-script=' + meson.current_source_dir() / 'exports' / 'yams_plugin.map',
    '-Wl,--exclude-libs,ALL',
  )
endif

# Shared ONNX Runtime loader for the ONNX and Glint plugins. Neither plugin links
# ONNX Runtime: the loader dlopen()s a compatible copy at runtime (override,
# configured path, system copy, then the private copy in <libdir>/yams/onnxruntime).
# Only the ONNX Runtime headers are needed at build time.
yams_ort_loader_dep = dependency('', required: false)
if onnxruntime_resolved_dep.found() and (get_option('plugin-onnx') or get_option('plugin-glint'))
  _ort_headers_dep = onnxruntime_resolved_dep.partial_dependency(compile_args: true, includes: true)
  _yams_ort_loader_lib = static_library('yams_ort_loader',
    ['ort_runtime/ort_runtime_loader.cpp', 'ort_runtime/ort_runtime_resolution.cpp'],
    include_directories: include_directories('../include'),
    dependencies: [_ort_headers_dep, dependency('spdlog'), dependency('yams_onnx_resource')],
    gnu_symbol_visibility: 'hidden',
    pic: true,
    install: false,
  )
  yams_ort_loader_dep = declare_dependency(
    link_with: _yams_ort_loader_lib,
    include_directories: include_directories('ort_runtime'),
    dependencies: [_ort_headers_dep, dependency('yams_onnx_resource')],
  )
endif

if get_option('plugin-s3')
  subdir('object_storage_s3')
endif

if get_option('plugin-onnx')
  subdir('onnx')
endif

if get_option('plugin-zyp')
  subdir('zyp')
endif

if get_option('plugin-glint')
  subdir('glint')
endif

