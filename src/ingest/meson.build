yams_ingest_lib = static_library('yams_ingest',
  'ingest_helpers.cpp',
  dependencies: [
    dependency('yams_core'),
    dependency('yams_metadata'),
    dependency('yams_vector'),
    dependency('yams_extraction'),
    dependency('spdlog'),
  ],
  include_directories: include_directories('../../include'),
  install: true,
)

yams_ingest_dep = declare_dependency(
  link_with: yams_ingest_lib,
  include_directories: include_directories('../../include'),
  dependencies: [
    dependency('yams_core'),
    dependency('yams_metadata'),
    dependency('yams_vector'),
    dependency('yams_extraction'),
    dependency('spdlog'),
  ],
)

meson.override_dependency('yams_ingest', yams_ingest_dep)

