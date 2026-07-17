# Build standalone benchmark executables. These are not registered as Meson
# tests; CI invokes them directly to generate JSON/MD reports.

fs = import('fs')
# Tracy profiling support (optional, off unless enable-profiling is set)
enable_profiling = get_option('enable-profiling')
enable_rocprofiler_tracy = get_option('enable-rocprofiler-tracy')
tracy_args = []
tracy_deps = []
if enable_profiling
  tracy_dep = dependency('tracy', modules: ['Tracy::TracyClient'], required: true, static: true)
  tracy_args += ['-DTRACY_ENABLE', '-DTRACY_ON_DEMAND']
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

# Google Benchmark dependency (library + main entry point)
benchmark_dep = dependency('benchmark', required: false)
benchmark_main_dep = dependency('benchmark', modules: ['benchmark::benchmark_main'], required: false)
if not benchmark_dep.found()
  benchmark_dep = dependency('google-benchmark', required: false)
  benchmark_main_dep = dependency('google-benchmark', modules: ['benchmark::benchmark_main'], required: false)
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

vector_backend_engine_compare = executable('vector_backend_engine_compare',
  files('vector_backend_engine_compare.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_vector'),
    dependency('spdlog'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_vector_backend_engine_compare', vector_backend_engine_compare)

vector_memory_scaling_bench = executable('vector_memory_scaling_bench',
  files('vector_memory_scaling_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_vector'),
    dependency('spdlog'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_vector_memory_scaling', vector_memory_scaling_bench)

# Large-scale ingestion benchmark (requires google-benchmark and provides its own main)
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

if benchmark_dep.found() and benchmark_main_dep.found()
  storage_bench_deps = [test_deps, dependency('yams_benchmarks'), benchmark_main_dep] + tracy_deps + [benchmark_dep]

  storage_bench = executable('yams_storage_throughput_benchmarks',
    files('storage_throughput_bench.cpp'),
    include_directories: incs,
    dependencies: storage_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )
else
  message('Google Benchmark benchmark_main not found; skipping storage throughput benchmark')
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
else
  message('Google Benchmark not found; skipping retrieval service benchmark')
endif

if benchmark_dep.found()
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
    dependency('yams_app_services'),
    dependency('spdlog')
  ] + tracy_deps + [benchmark_dep]

  # RAG retrieval quality benchmark
  retrieval_quality_bench_deps = search_bench_deps + [test_deps, dependency('libcurl'), dependency('nlohmann_json')]
  retrieval_quality_bench_rpath = []
  if host_machine.system() == 'darwin'
    retrieval_quality_bench_rpath += ['/opt/homebrew/lib']
  endif

  retrieval_quality_bench = executable('retrieval_quality_bench',
    files(
      'search/retrieval_quality_bench.cpp',
      'search/retrieval_benchmark_support.cpp',
    ),
    include_directories: incs,
    dependencies: retrieval_quality_bench_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    build_rpath: ':'.join(retrieval_quality_bench_rpath),
    install: false,
  )

  alias_target('bench_retrieval_quality', retrieval_quality_bench)

  # Per-arm retrieval-quality bench: scores each SimeonLexicalBackend bandit arm
  # directly over a judged corpus (nDCG/Recall/MRR), deterministic (no bandit
  # exploration). Drives the strategy-router keep/enable/remove decision.
  simeon_arm_quality_bench = executable('simeon_arm_quality_bench',
    files(
      'search/simeon_arm_quality_bench.cpp',
    ),
    include_directories: incs,
    dependencies: search_bench_deps + [test_deps, dependency('nlohmann_json')],
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_simeon_arm_quality', simeon_arm_quality_bench)

  entity_extraction_bench = executable('entity_extraction_bench',
    files('entity_extraction_bench.cpp'),
    include_directories: incs,
    dependencies: [benchmark_main_dep, benchmark_dep, dependency('spdlog')] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_entity_extraction', entity_extraction_bench)

  fts5_indexing_bench = executable('fts5_indexing_bench',
    files('fts5_indexing_bench.cpp'),
    include_directories: incs,
    # Keep project/Conan dependencies before benchmark. On macOS runners,
    # benchmark may resolve from Homebrew and add /opt/homebrew/include ahead
    # of Conan's fmt; spdlog then sees the wrong fmt headers and fails to
    # compile fmt::basic_format_string references.
    dependencies: [
      dependency('yams_metadata'),
      dependency('yams_search'),
      dependency('yams_daemon'),
      dependency('spdlog'),
      benchmark_main_dep,
      benchmark_dep,
    ] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_fts5_indexing', fts5_indexing_bench)

  # KG edge/entity insert microbenchmark. Covers addEdgesUnique, addEdges,
  # upsertNodes, and WriteBatch throughput with varying batch sizes.
  # Used as a baseline for the KGWriteBuffer optimization (#2).
  kg_edge_insert_bench = executable('kg_edge_insert_bench',
    files('kg_edge_insert_bench.cpp'),
    include_directories: incs,
    dependencies: [
      dependency('yams_metadata'),
      dependency('spdlog'),
      benchmark_main_dep,
      benchmark_dep,
    ] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  alias_target('bench_kg_edge_insert', kg_edge_insert_bench)

else
  message('Google Benchmark not found; skipping service and metadata benchmark executables')
endif

# PBI-004: End-to-end ingestion pipeline benchmark. It provides its own main
# and must remain buildable when the optional Google Benchmark dependency is absent.
ingestion_e2e_bench = executable('ingestion_e2e_bench',
  files('search/ingestion_e2e_bench.cpp'),
  include_directories: incs,
  dependencies: [
    test_deps,
    dependency('yams_metadata'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_daemon'),
    dependency('yams_app_services'),
    dependency('spdlog'),
    dependency('nlohmann_json'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_ingestion_e2e', ingestion_e2e_bench)

# KPI 4: repair ability (standalone main, no Google Benchmark dependency)
repair_ability_bench = executable('repair_ability_bench',
  files('repair_ability_bench.cpp'),
  include_directories: incs,
  dependencies: [
    test_deps,
    dependency('nlohmann_json'),
    dependency('spdlog'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)
alias_target('bench_repair_ability', repair_ability_bench)

ingestion_bench = executable('ingestion_throughput_bench', files('ingestion_throughput_bench.cpp'),
  include_directories: incs,
  dependencies: [
    boost_dep,
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

tree_build_bench = executable('tree_build_bench', files('tree_build_bench.cpp'),
  include_directories: incs,
  dependencies: [
    dependency('yams_benchmarks'),
    dependency('yams_metadata'),
    dependency('yams_storage_engine'),
    dependency('nlohmann_json'),
    dependency('spdlog'),
  ] + tracy_deps,
  cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
  install: false,
)

alias_target('bench_tree_build', tree_build_bench)

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

# Socket accept and metrics performance benchmark (Catch2-based)
_catch2_with_main_dep_bench = dependency('catch2-with-main', required: false)
if _catch2_with_main_dep_bench.found()
  daemon_socket_accept_bench = executable('daemon_socket_accept_bench', files('daemon_socket_accept_bench.cpp'),
    include_directories: incs,
    dependencies: [
      _catch2_with_main_dep_bench,
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
  write_coordinator_bench = executable('write_coordinator_bench',
    files('write_coordinator_bench.cpp'),
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

  write_coordinator_bench_env = {
    'YAMS_TESTING': '1',
    'YAMS_DISABLE_VECTORS': '1',
    'YAMS_DISABLE_SESSION_WATCHER': '1',
    'YAMS_SKIP_MODEL_LOADING': '1',
    'LD_LIBRARY_PATH': _test_env_ld_library_path,
  }

  test('write_coordinator_bench', write_coordinator_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'write-coordinator'],
       is_parallel: false,
       timeout: 600,
       env: write_coordinator_bench_env)

  simeon_lexical_score_bench = executable('simeon_lexical_score_bench',
    files('simeon_lexical_score_bench.cpp'),
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
  alias_target('bench_simeon_lexical_score', simeon_lexical_score_bench)

  test('simeon_lexical_score_bench', simeon_lexical_score_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'simeon'],
       is_parallel: false,
       timeout: 600,
       env: write_coordinator_bench_env)

  alias_target('bench_write_coordinator', write_coordinator_bench)

  metadata_contention_bench = executable('metadata_contention_bench',
    files('metadata_contention_bench.cpp'),
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

  metadata_contention_bench_env = {
    'YAMS_TESTING': '1',
    'YAMS_DISABLE_VECTORS': '1',
    'YAMS_DISABLE_SESSION_WATCHER': '1',
    'YAMS_SKIP_MODEL_LOADING': '1',
    'LD_LIBRARY_PATH': _test_env_ld_library_path,
  }

  test('metadata_contention_bench', metadata_contention_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'metadata-contention'],
       is_parallel: false,
       timeout: 600,
       env: metadata_contention_bench_env)

  alias_target('bench_metadata_contention', metadata_contention_bench)

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

  # Single-client pipelining benchmark: in-flight window scaling on one
  # client process. Quantifies the solo-vs-concurrent per-client throughput
  # gap found in the 2026-06 scaling curve. Tagged [!benchmark].
  single_client_pipeline_bench = executable('single_client_pipeline_bench',
    files('single_client_pipeline_bench.cpp'),
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
  alias_target('bench_single_client_pipeline', single_client_pipeline_bench)

  multi_client_bench_env = {
    'YAMS_TESTING': '1',
    'YAMS_DISABLE_VECTORS': '1',
    'YAMS_DISABLE_SESSION_WATCHER': '1',
    'YAMS_SKIP_MODEL_LOADING': '1',
    'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
    'LD_LIBRARY_PATH': _test_env_ld_library_path,
  }

  if get_option('build-cli') and tests_has_cli
    multi_client_bench_env += { 'YAMS_BENCH_CLI_BIN': yams_cli_exe.full_path() }
  endif

  test('multi_client_ingestion_bench', multi_client_ingestion_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'multi-client'],
       is_parallel: false,
       timeout: 600,
       env: multi_client_bench_env)

  alias_target('bench_multi_client', multi_client_ingestion_bench)

  # Topology ablation bench (Catch2-based, component-level)
  # Sweeps router limit and role-score boosts on a synthetic topology batch.
  # See docs/benchmarks/topology_ablation_matrix.md.
  topology_ablation_bench = executable('topology_ablation_bench',
    files('topology_ablation_bench.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_with_main_dep,
      dependency('yams_topology'),
      dependency('nlohmann_json'),
    ] + tracy_deps,
    cpp_args: ['-DYAMS_TESTING=1'] + tracy_args,
    install: false,
  )

  test('topology_ablation_bench', topology_ablation_bench,
       args: ['--allow-running-no-tests'],
       suite: ['bench', 'topology-ablation'],
       is_parallel: false,
       timeout: 300,
       env: {
         'YAMS_TESTING': '1',
         'LD_LIBRARY_PATH': _test_env_ld_library_path,
       })

  alias_target('bench_topology_ablation', topology_ablation_bench)
else
  message('Catch2 not found, skipping symbol_extraction_bench, hnsw_entity_search_bench, and multi_client_ingestion_bench')
endif
