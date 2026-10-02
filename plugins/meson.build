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

