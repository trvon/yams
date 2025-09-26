mobile_sources = [
  'mobile_bindings.cpp',
]

yams_mobile_lib = shared_library('yams_mobile',
  mobile_sources,
  dependencies: [
    dependency('yams_app_services'),
    dependency('yams_metadata'),
    dependency('yams_api'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_indexing'),
    dependency('yams_storage_engine'),
    dependency('yams_manifest'),
    dependency('yams_chunking'),
    dependency('yams_crypto'),
    dependency('yams_daemon'),
    dependency('boost'),
    dependency('nlohmann_json'),
    dependency('sqlite3'),
  ],
  include_directories: include_directories('../../include'),
  install: true,
)

yams_mobile_dep = declare_dependency(
  link_with: yams_mobile_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_mobile', yams_mobile_dep)
