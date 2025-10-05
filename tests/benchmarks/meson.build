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

retrieval_bench_deps = [test_deps, dependency('yams_benchmarks')] + tracy_deps
if benchmark_dep.found()
  retrieval_bench_deps += [benchmark_dep]
endif

retrieval_bench = executable('yams_retrieval_service_benchmarks', 
  files('retrieval_service_benchmarks.cpp'),
  include_directories: incs,
  dependencies: retrieval_bench_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

metadata_query_bench_deps = [dependency('yams_metadata'), dependency('yams_search')] + tracy_deps
if benchmark_dep.found()
  metadata_query_bench_deps += [benchmark_dep]
endif

metadata_query_bench = executable('metadata_path_query_bench', files('metadata_path_query_bench.cpp'),
  include_directories: incs,
  dependencies: metadata_query_bench_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

ingestion_bench = executable('ingestion_throughput_bench', files('ingestion_throughput_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_benchmarks'),
    dependency('yams_api'),
    dependency('yams_app_services'),
    dependency('yams_metadata'),
    dependency('yams_storage_engine'),
    dependency('nlohmann_json'),
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
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_tree_diff', tree_diff_bench)
