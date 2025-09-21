spdlog_dep = dependency('spdlog', required: true)
yams_core = dependency('yams_core', required: false)

chunking_src = files(
  'rabin_chunker.cpp',
  'streaming_chunker.cpp',
)

crypto_dep = dependency('yams_crypto', required: true)

yams_chunking = static_library('yams_chunking',
  chunking_src,
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, yams_core, crypto_dep],
  install: true,
)

# Provide a Meson dependency for other subdirs
yams_chunking_dep = declare_dependency(
  link_with: yams_chunking,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_chunking', yams_chunking_dep)

# pkg-config export for downstream (optional)
pkg = import('pkgconfig')
pkg.generate(yams_chunking,
  name: 'yams-chunking',
  description: 'YAMS content chunking',
  subdirs: ['yams'],
)
