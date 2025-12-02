# Build standalone benchmark executables. These are not registered as Meson
# tests; CI invokes them directly to generate JSON/MD reports.

# Tracy profiling support (optional)
tracy_dep = dependency('tracy', required: false, static: true)
tracy_args = []
tracy_deps = []
if tracy_dep.found()
  tracy_args += ['-DTRACY_ENABLE']
  tracy_deps += tracy_dep
  message('Tracy profiling enabled for benchmarks')
endif

# Google Benchmark dependency
benchmark_dep = dependency('benchmark', required: false)
if not benchmark_dep.found()
  benchmark_dep = dependency('google-benchmark', required: false)
endif

api_bench = executable('yams_api_benchmarks', files('api_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [test_deps, dependency('yams_benchmarks')] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

search_bench = executable('yams_search_benchmarks', files('search_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [test_deps, dependency('yams_benchmarks')] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

grep_bench = executable('yams_grep_benchmarks', files('grep_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [test_deps, dependency('yams_benchmarks')] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

search_tree_bench = executable('search_tree_bench', files('search_tree_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_benchmarks'),
    dependency('yams_app_services'),
    dependency('yams_metadata'),
    dependency('yams_search'),
    dependency('yams_daemon'),
    dependency('spdlog'),
    dependency('nlohmann_json'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

if benchmark_dep.found()
  retrieval_bench_deps = [test_deps, dependency('yams_benchmarks')] + tracy_deps + [benchmark_dep]

  retrieval_bench = executable('yams_retrieval_service_benchmarks',
    files('retrieval_service_benchmarks.cpp'),
    include_directories: incs,
    dependencies: retrieval_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  metadata_query_bench_deps = [dependency('yams_metadata'), dependency('yams_search'), dependency('yams_daemon'), dependency('spdlog')] + tracy_deps + [benchmark_dep]

  metadata_query_bench = executable('metadata_path_query_bench', files('metadata_path_query_bench.cpp'),
    include_directories: incs,
    dependencies: metadata_query_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  # Tree-based list with filters benchmark (v0.7.5)
  tree_list_filter_bench = executable('tree_list_filter_bench', files('tree_list_filter_bench.cpp'),
    include_directories: incs,
    dependencies: metadata_query_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_tree_list_filter', tree_list_filter_bench)

  # Common dependencies for search-related benchmarks
  search_bench_deps = [
    dependency('yams_metadata'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_daemon'),
    dependency('spdlog')
  ] + tracy_deps + [benchmark_dep]

  # RAG retrieval quality benchmark
  retrieval_quality_bench_deps = search_bench_deps + [test_deps]

  retrieval_quality_bench = executable('retrieval_quality_bench',
    files('../../src/search/benchmarks/retrieval_quality_bench.cpp'),
    include_directories: incs,
    dependencies: retrieval_quality_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_retrieval_quality', retrieval_quality_bench)

  # PBI-004: End-to-end ingestion pipeline benchmark
  ingestion_e2e_bench_deps = search_bench_deps + [test_deps]

  ingestion_e2e_bench = executable('ingestion_e2e_bench',
    files('../../src/search/benchmarks/ingestion_e2e_bench.cpp'),
    include_directories: incs,
    dependencies: ingestion_e2e_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_ingestion_e2e', ingestion_e2e_bench)
else
  message('Google Benchmark not found; skipping service and metadata benchmark executables')
endif

ingestion_bench = executable('ingestion_throughput_bench', files('ingestion_throughput_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('boost', modules: ['system']),
    dependency('yams_benchmarks'),
    dependency('yams_api'),
    dependency('yams_app_services'),
    dependency('yams_metadata'),
    dependency('yams_daemon'),
    dependency('yams_storage_engine'),
    dependency('nlohmann_json'),
    dependency('spdlog'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_ingest', ingestion_bench)

# PBI-043 Tree-diff benchmarks
tree_diff_bench = executable('tree_diff_benchmarks', files('tree_diff_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_benchmarks'),
    dependency('yams_metadata'),
    dependency('yams_storage_engine'),
    dependency('nlohmann_json'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_tree_diff', tree_diff_bench)

ipc_stream_bench = executable('ipc_stream_bench', files('ipc_streaming_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_benchmarks'),
    dependency('yams_daemon'),
    dependency('nlohmann_json'),
    dependency('tl-expected'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_ipc_stream', ipc_stream_bench)

# Socket accept and metrics performance benchmark
daemon_socket_accept_bench = executable('daemon_socket_accept_bench', files('daemon_socket_accept_bench.cpp'),
  include_directories: incs,
  dependencies: [
    gtest_dep,
    test_deps,
    dependency('yams_benchmarks'),
    dependency('yams_daemon'),
    dependency('nlohmann_json'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_socket_accept', daemon_socket_accept_bench)

# PBI-073: Symbol extraction benchmark (Catch2-based)
# Validates symbol extraction quality (Recall/Precision/F1) and performance
symbol_extraction_bench = executable('symbol_extraction_bench',
  files('symbol_extraction_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('catch2-with-main'),
    dependency('nlohmann_json'),
    dependency('spdlog'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

# Register as Catch2 test (not a standard test, only run manually or in nightly CI)
test('symbol_extraction_bench', symbol_extraction_bench,
     suite: ['bench', 'symbol_extraction'],
     is_parallel: false,
     timeout: 300,
     env: {
       'YAMS_AUTO_DOWNLOAD_GRAMMARS': '1',
       'YAMS_SKIP_MODEL_LOADING': '1',
       'LD_LIBRARY_PATH': _test_env_ld_library_path,
     })

alias_target('bench_symbol_extraction', symbol_extraction_bench)

