spdlog_dep = dependency('spdlog', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
boost_dep = dependency('boost', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
yams_vector = dependency('yams_vector', required: false)

# SymSpell dependency (defined in third_party/symspell/meson.build)
# Note: symspell_dep must be declared before this file is processed

yams_search = static_library('yams_search',
  [
    'query_tokenizer.cpp',
    'query_parser.cpp',
    'query_literal_extractor.cpp',
    'search_results.cpp',
    'search_filters.cpp',
    'result_ranker.cpp',
    'chunk_coverage.cpp',
    'parallel_post_processor.cpp',
    'search_engine.cpp',
    'search_tuner.cpp',
    'entity_linker.cpp',
    'kg_scorer_simple.cpp',
    'search_engine_builder.cpp',
    'hotzone_manager.cpp',
    'symbol_enrichment.cpp',
    'internal_benchmark.cpp',
    'symspell_search.cpp',
  ],
  include_directories: [
    include_directories('../../include'),
    include_directories('../../third_party/symspell/include'),
  ],
  dependencies: [spdlog_dep, nlohmann_json_dep, boost_dep, yams_core, yams_metadata, yams_vector, symspell_dep],
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
  include_directories: [
    include_directories('../../include'),
    include_directories('../../third_party/symspell/include'),
  ],
  dependencies: [yams_core, yams_metadata, yams_vector, nlohmann_json_dep, spdlog_dep, boost_dep, symspell_dep],
)
meson.override_dependency('yams_search', yams_search_dep)
