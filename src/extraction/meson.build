spdlog_dep = dependency('spdlog', required: true)
yams_core = dependency('yams_core', required: false)

nlohmann_json_dep = dependency('nlohmann_json', required: true)

yams_extraction = static_library('yams_extraction',
  [
    'text_extractor_factory.cpp',
    'text_extractor_utils.cpp',
    'title_util.cpp',
    'plain_text_extractor.cpp',
    'html_text_extractor.cpp',
    'format_handlers/format_handler.cpp',
    '../extraction/extraction_util.cpp',
    'plugin_process.cpp',
    'jsonrpc_client.cpp',
    'external_plugin_extractor.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, yams_core, nlohmann_json_dep, yams_metadata_dep],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_extraction,
  name: 'yams-extraction',
  description: 'YAMS text extraction',
  subdirs: ['yams'],
)

yams_extraction_dep = declare_dependency(
  link_with: yams_extraction,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_extraction', yams_extraction_dep)

