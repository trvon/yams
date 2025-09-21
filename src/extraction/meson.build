spdlog_dep = dependency('spdlog', required: true)
yams_core = dependency('yams_core', required: false)

yams_extraction = static_library('yams_extraction',
  [
    'text_extractor_factory.cpp',
    'text_extractor_utils.cpp',
    'plain_text_extractor.cpp',
    'html_text_extractor.cpp',
    'format_handlers/format_handler.cpp',
    '../extraction/extraction_util.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, yams_core],
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

