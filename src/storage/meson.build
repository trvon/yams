spdlog_dep = dependency('spdlog', required: true)
threads_dep = dependency('threads')
curl_dep = dependency('libcurl', required: true)
sqlite_dep = dependency('sqlite3', required: true)
yams_core = dependency('yams_core', required: false)
yams_crypto = dependency('yams_crypto', required: false)
yams_compression = dependency('yams_compression', required: false)

cc = meson.get_compiler('cpp')
dl_dep = cc.find_library('dl', required: host_machine.system() == 'linux')

# Platform sources
platform_src = []
if host_machine.system() != 'windows'
  platform_src += files('platform/posix_storage.cpp')
else
  platform_src += files('platform/windows_storage.cpp')
endif

yams_storage_engine = static_library('yams_storage_engine',
  [
    'storage_engine.cpp',
    'compressed_storage_engine.cpp',
    'storage_backend_factory.cpp',
    'url_backend.cpp',
    's3_signer.cpp',
    'object_storage_plugin_loader.cpp',
    '../storage/object_storage_adapter.cpp',
  ] + platform_src,
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, threads_dep, curl_dep, yams_core, yams_crypto, yams_compression, dl_dep],
  install: true,
)

yams_reference_counter = static_library('yams_reference_counter',
  [
    'reference_counter.cpp',
    'garbage_collector.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, threads_dep, yams_core, sqlite_dep, declare_dependency(link_with: yams_storage_engine)],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_storage_engine,
  name: 'yams-storage-engine',
  description: 'YAMS storage engine',
  subdirs: ['yams'],
)
pkg.generate(yams_reference_counter,
  name: 'yams-reference-counter',
  description: 'YAMS reference counter',
  subdirs: ['yams'],
)

yams_storage_engine_dep = declare_dependency(
  link_with: yams_storage_engine,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_storage_engine', yams_storage_engine_dep)

yams_reference_counter_dep = declare_dependency(
  link_with: yams_reference_counter,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_reference_counter', yams_reference_counter_dep)

