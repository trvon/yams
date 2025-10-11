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

core_bench = executable('yams_core_benchmarks', files('core_benchmarks.cpp'),
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

  metadata_query_bench_deps = [dependency('yams_metadata'), dependency('yams_search'), dependency('spdlog')] + tracy_deps + [benchmark_dep]

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
else
  message('Google Benchmark not found; skipping service and metadata benchmark executables')
endif

ingestion_bench = executable('ingestion_throughput_bench', files('ingestion_throughput_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_benchmarks'),
    dependency('yams_api'),
    dependency('yams_app_services'),
    dependency('yams_metadata'),
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
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_ipc_stream', ipc_stream_bench)

# Socket accept and metrics performance benchmark
daemon_socket_accept_bench = executable('daemon_socket_accept_bench', files('daemon_socket_accept_bench.cpp'),
  include_directories: incs,
  dependencies: [
    test_deps,  # Includes gtest
    dependency('yams_benchmarks'),
    dependency('yams_daemon'),
    dependency('nlohmann_json'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_socket_accept', daemon_socket_accept_bench)
