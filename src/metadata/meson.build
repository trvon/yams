spdlog_dep = dependency('spdlog', required: true)
sqlite_dep = dependency('sqlite3', required: true)
openssl_dep = dependency('openssl', modules: ['crypto'], required: true)
yams_core = dependency('yams_core', required: false)
yams_storage_engine_dep = dependency('yams_storage_engine', required: false)

yams_metadata = static_library('yams_metadata',
  [
    'database.cpp',
    'connection_pool.cpp',
    'migration.cpp',
    'metadata_repository.cpp',
    'query_helpers.cpp',
    'knowledge_graph_store_sqlite.cpp',
    'path_utils.cpp',
    'tree_builder.cpp',
    'tree_differ.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, sqlite_dep, openssl_dep, yams_core, yams_storage_engine_dep],
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
