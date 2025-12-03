spdlog_dep = dependency('spdlog', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
yams_vector = dependency('yams_vector', required: false)

yams_search = static_library('yams_search',
  [
    'query_tokenizer.cpp',
    'query_parser.cpp',
    'query_literal_extractor.cpp',
    'search_results.cpp',
    'search_filters.cpp',
    'result_ranker.cpp',
    'chunk_coverage.cpp',
    'search_executor.cpp',
    'parallel_post_processor.cpp',
    'search_engine.cpp',
    'entity_linker.cpp',
    'kg_scorer_simple.cpp',
    'search_engine_builder.cpp',
    'bk_tree.cpp',
    'hotzone_manager.cpp',
    'symbol_enrichment.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, nlohmann_json_dep, yams_core, yams_metadata, yams_vector],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_search,
  name: 'yams-search',
  description: 'YAMS search library',
  subdirs: ['yams'],
)

yams_search_dep = declare_dependency(
  link_with: yams_search,
  include_directories: include_directories('../../include'),
  dependencies: [yams_core, yams_metadata, yams_vector, nlohmann_json_dep, spdlog_dep],
)
meson.override_dependency('yams_search', yams_search_dep)
