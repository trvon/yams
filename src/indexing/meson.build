spdlog_dep = dependency('spdlog', required: true)
threads_dep = dependency('threads')
boost_dep = dependency('boost', modules: ['system'], required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
yams_extraction = dependency('yams_extraction', required: false)
yams_content = dependency('yams_content', required: false)
yams_detection = dependency('yams_detection', required: false)

yams_indexing = static_library('yams_indexing',
  [ 'document_indexer.cpp', 'indexing_pipeline.cpp' ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, threads_dep, boost_dep, yams_core, yams_metadata, yams_extraction, yams_content, yams_detection],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_indexing,
  name: 'yams-indexing',
  description: 'YAMS indexing pipeline',
  subdirs: ['yams'],
)

yams_indexing_dep = declare_dependency(
  link_with: yams_indexing,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_indexing', yams_indexing_dep)

