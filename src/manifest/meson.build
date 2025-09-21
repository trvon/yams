spdlog_dep = dependency('spdlog', required: true)
yams_core = dependency('yams_core', required: false)
yams_crypto = dependency('yams_crypto', required: false)
yams_chunking = dependency('yams_chunking', required: false)

yams_manifest = static_library('yams_manifest',
  [ 'manifest_manager.cpp' ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, yams_core, yams_crypto, yams_chunking],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_manifest,
  name: 'yams-manifest',
  description: 'YAMS manifest management',
  subdirs: ['yams'],
)

yams_manifest_dep = declare_dependency(
  link_with: yams_manifest,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_manifest', yams_manifest_dep)

