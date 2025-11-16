tl_expected_dep = dependency('tl-expected', required: true)

yams_app_services_lib = static_library('yams_app_services',
  [
    'document_service.cpp',
    'document_ingestion_service.cpp',
    'list_input_resolver.cpp',
    'retrieval_service.cpp',
    'search_service.cpp',
    'enhanced_search_executor.cpp',
    'grep_service.cpp',
    'literal_extractor.cpp',
    'simd_newline_scanner.cpp',
    'grep_mode_tls.cpp',
    'download_service.cpp',
    'indexing_service.cpp',
    'stats_service.cpp',
    'utils.cpp',
    'factory.cpp',
    'session_service.cpp',
    'graph_query_service.cpp',
  ],
  dependencies: [
    dependency('yams_api'),
    dependency('yams_daemon_client'),
    dependency('yams_metadata'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_downloader'),
    dependency('boost'),
    dependency('nlohmann_json'),
    dependency('spdlog'),
    tl_expected_dep,
  ],
  include_directories: include_directories('../../../include'),
  install: true,
)

yams_app_services_dep = declare_dependency(
  link_with: yams_app_services_lib,
  include_directories: include_directories('../../../include'),
)

meson.override_dependency('yams_app_services', yams_app_services_dep)
