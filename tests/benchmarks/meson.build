# Build standalone benchmark executables. These are not registered as Meson
# tests; CI invokes them directly to generate JSON/MD reports.

fs = import('fs')
# Tracy profiling support (optional, off unless enable-profiling is set)
enable_profiling = get_option('enable-profiling')
enable_rocprofiler_tracy = get_option('enable-rocprofiler-tracy')
tracy_dep = dependency('tracy', required: enable_profiling, static: true)
tracy_args = []
tracy_deps = []
if enable_profiling and tracy_dep.found()
  tracy_args += ['-DTRACY_ENABLE']
  tracy_deps += tracy_dep
  rocm_hint = '/opt/rocm'
  rocm_present = fs.exists(rocm_hint)
  want_rocprofiler = (not enable_rocprofiler_tracy.disabled()) and (enable_rocprofiler_tracy.enabled() or (enable_rocprofiler_tracy.auto() and rocm_present))
  if want_rocprofiler
    rocprofiler_dep = dependency(
      'rocprofiler-sdk',
      method: 'cmake',
      cmake_module_path: [join_paths(rocm_hint, 'lib', 'cmake'), join_paths(rocm_hint, 'lib64', 'cmake')],
      required: enable_rocprofiler_tracy.enabled(),
    )
    if rocprofiler_dep.found()
      tracy_deps += rocprofiler_dep
    elif enable_rocprofiler_tracy.enabled()
      error('enable-rocprofiler-tracy=enabled but rocprofiler-sdk was not found')
    else
      warning('Tracy present but rocprofiler-sdk missing; disabling Tracy ROCm hooks for benchmarks')
      tracy_deps = []
      tracy_args = []
    endif
  endif
  if tracy_deps.length() > 0
    cpp = meson.get_compiler('cpp')
    tracy_link_ok = cpp.links('int main(){ return 0; }', dependencies: tracy_deps, name: 'tracy-bench-link-check', required: false)
    if tracy_link_ok
      message('Tracy profiling enabled for benchmarks')
    else
      warning('Tracy dependency failed to link; disabling Tracy for benchmarks')
      tracy_deps = []
      tracy_args = []
    endif
  endif
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

grep_bench = executable('yams_grep_benchmarks', files('grep_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [test_deps, dependency('yams_benchmarks')] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

# Large-scale ingestion benchmark (requires google-benchmark)
if benchmark_dep.found()
  large_scale_ingestion_bench_deps = [
    test_deps,
    dependency('yams_benchmarks'),
    benchmark_dep,
  ] + tracy_deps

  large_scale_ingestion_bench = executable('yams_large_scale_ingestion_bench',
    files('large_scale_ingestion_bench.cpp'),
    include_directories: incs,
    dependencies: large_scale_ingestion_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_large_scale_ingestion', large_scale_ingestion_bench)
else
  message('Google Benchmark not found; skipping large_scale_ingestion_bench')
endif

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
  retrieval_quality_bench_deps = search_bench_deps + [test_deps, dependency('libcurl'), dependency('nlohmann_json')]

  retrieval_quality_bench = executable('retrieval_quality_bench',
    files('../../src/search/benchmarks/retrieval_quality_bench.cpp'),
    include_directories: incs,
    dependencies: retrieval_quality_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_retrieval_quality', retrieval_quality_bench)

  # Fusion strategy experiment - compares different fusion approaches
  fusion_experiment = executable('fusion_strategy_experiment',
    files('../../src/search/benchmarks/fusion_strategy_experiment.cpp'),
    include_directories: incs,
    dependencies: retrieval_quality_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_fusion_experiment', fusion_experiment)

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
if gtest_dep.found()
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
else
  message('GTest not found; skipping daemon_socket_accept_bench.')
endif

# PBI-073: Symbol extraction benchmark (Catch2-based)
# Validates symbol extraction quality (Recall/Precision/F1) and performance
# Only build if Catch2 is available (requires build_tests=true in conan)
catch2_with_main_dep = dependency('catch2-with-main', required: false)
if catch2_with_main_dep.found()
  symbol_extraction_bench = executable('symbol_extraction_bench',
    files('symbol_extraction_bench.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_with_main_dep,
      dependency('nlohmann_json'),
      dependency('spdlog'),
    ] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  # Register as Catch2 test (not a standard test, only run manually or in nightly CI)
  # Tests are tagged [!benchmark] so won't run by default - use --allow-running-no-tests
  test('symbol_extraction_bench', symbol_extraction_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'symbol_extraction'],
       is_parallel: false,
       timeout: 300,
       env: {
         'YAMS_AUTO_DOWNLOAD_GRAMMARS': '1',
         'YAMS_SKIP_MODEL_LOADING': '1',
         'LD_LIBRARY_PATH': _test_env_ld_library_path,
       })

  alias_target('bench_symbol_extraction', symbol_extraction_bench)

  # HNSW Entity Search benchmark (Catch2-based)
  # Benchmarks HNSW search performance with various corpus sizes, dimensions, and k values
  # Also tests symbol extractor integration for entity vector search
  hnsw_entity_search_bench = executable('hnsw_entity_search_bench',
    files('hnsw_entity_search_bench.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_with_main_dep,
      dependency('nlohmann_json'),
      dependency('yams_vector'),
      dependency('spdlog'),
    ] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  # Register as Catch2 test (not a standard test, only run manually or in nightly CI)
  # Tests are tagged [!benchmark] so won't run by default
  test('hnsw_entity_search_bench', hnsw_entity_search_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'hnsw'],
       is_parallel: false,
       timeout: 300,
       env: {
         'YAMS_SKIP_MODEL_LOADING': '1',
         'LD_LIBRARY_PATH': _test_env_ld_library_path,
       })

  alias_target('bench_hnsw', hnsw_entity_search_bench)

  # Multi-client ingestion & concurrent access benchmark (Catch2-based)
  # Measures ingestion throughput, latency, and fairness under multi-client load.
  # Tagged [!benchmark] so won't run by default - invoke manually or in nightly CI.
  multi_client_ingestion_bench = executable('multi_client_ingestion_bench',
    files('multi_client_ingestion_bench.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_with_main_dep,
      test_deps,
      dependency('nlohmann_json'),
      dependency('spdlog'),
    ] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  test('multi_client_ingestion_bench', multi_client_ingestion_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'multi-client'],
       is_parallel: false,
       timeout: 600,
       env: {
         'YAMS_TESTING': '1',
         'YAMS_DISABLE_VECTORS': '1',
         'YAMS_DISABLE_SESSION_WATCHER': '1',
         'YAMS_SKIP_MODEL_LOADING': '1',
         'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
         'LD_LIBRARY_PATH': _test_env_ld_library_path,
       })

  alias_target('bench_multi_client', multi_client_ingestion_bench)
else
  message('Catch2 not found, skipping symbol_extraction_bench, hnsw_entity_search_bench, and multi_client_ingestion_bench')
endif
