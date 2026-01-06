yams_api_lib = static_library('yams_api',
  [
    'content_store_impl.cpp',
    'async_content_store.cpp',
    'content_metadata.cpp',
    'content_store_builder.cpp',
    'progress_reporter.cpp',
    'metadata_api.cpp',
    'semantic_search_api.cpp',
    'raw_text_storage.cpp',
  ],
  dependencies: [
    dependency('yams_core'),
    dependency('yams_storage_engine'),
    dependency('yams_reference_counter'),
    dependency('yams_chunking'),
    dependency('yams_crypto'),
    dependency('yams_manifest'),
    dependency('yams_metadata'),
    dependency('yams_indexing'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('nlohmann_json'),
    dependency('libcurl', required: false),
    dependency('OpenSSL'),
    dependency('zlib'),
    dependency('spdlog'),
    dependency('boost'),
  ],
  include_directories: include_directories('../../include'),
  install: true,
)

yams_api_dep = declare_dependency(
  link_with: yams_api_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_api', yams_api_dep)