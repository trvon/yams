spdlog_dep = dependency('spdlog', required: true)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
boost_dep = dependency('boost', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
yams_topology = dependency('yams_topology', required: false)
yams_vector = dependency('yams_vector', required: false)

#SymSpell dependency(defined in third_party / symspell / meson.build)
#Note : symspell_dep must be declared before this file is processed

yams_search = static_library('yams_search',
  [
    'query_tokenizer.cpp',
    'query_router.cpp',
    'query_text_utils.cpp',
    'graph_expansion.cpp',
    'search_tracing.cpp',
    'query_expansion.cpp',
    'query_parser.cpp',
    'query_literal_extractor.cpp',
    'search_results.cpp',
    'search_filters.cpp',
    'result_ranker.cpp',
    'chunk_coverage.cpp',
    'parallel_post_processor.cpp',
    'search_execution_context.cpp',
    'lexical_scoring.cpp',
    'search_lexical_pipeline.cpp',
    'search_engine.cpp',
    'search_result_fusion.cpp',
    'search_vector_pipeline.cpp',
    'search_tuner.cpp',
    'tuning_pipeline.cpp',
    'tuning_features.cpp',
    'tuning_reward.cpp',
    'tuner_mab.cpp',
    'topological_quality.cpp',
    'anchor_fusion.cpp',
    'kg_scorer_simple.cpp',
    'search_engine_builder.cpp',
    'hotzone_manager.cpp',
    'symbol_enrichment.cpp',
    'internal_benchmark.cpp',
    'benchmark_history_store.cpp',
    'relevance_label_store.cpp',
    'symspell_search.cpp',
    'query_concept_extractor.cpp',
    'turboquant_packed_reranker.cpp',
    'simeon_lexical_backend.cpp',
  ],
  include_directories: [
    include_directories('../../include'),
    include_directories('../../third_party/symspell/include'),
  ],
  dependencies: [spdlog_dep, nlohmann_json_dep, boost_dep, yams_core, yams_metadata, yams_topology, yams_vector, symspell_dep],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_search,
  name: 'yams-search',
  description: 'YAMS search library',
  subdirs: ['yams'],
)

yams_search_dep = declare_dependency(
#yams_search owns simeon_lexical_backend.cpp, so consumers also need the
#vendored simeon archive that provides QueryRouter / recipe_name.
  link_with: [yams_search, simeon_lib],
  include_directories: [
    include_directories('../../include'),
    include_directories('../../third_party/symspell/include'),
  ],
  dependencies: [yams_core, yams_metadata, yams_topology, yams_vector, nlohmann_json_dep, spdlog_dep, boost_dep, symspell_dep],
)
meson.override_dependency('yams_search', yams_search_dep)
