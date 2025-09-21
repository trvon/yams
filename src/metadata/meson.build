spdlog_dep = dependency('spdlog', required: true)
sqlite_dep = dependency('sqlite3', required: true)
yams_core = dependency('yams_core', required: false)

yams_metadata = static_library('yams_metadata',
  [
    'database.cpp',
    'connection_pool.cpp',
    'migration.cpp',
    'metadata_repository.cpp',
    'knowledge_graph_store_sqlite.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, sqlite_dep, yams_core],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_metadata,
  name: 'yams-metadata',
  description: 'YAMS metadata store',
  subdirs: ['yams'],
)

yams_metadata_dep = declare_dependency(
  link_with: yams_metadata,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_metadata', yams_metadata_dep)

