#! Meson configure-time test discovery without fs.glob/subdir_glob

gtest_dep = dependency('gtest', required: true)
gmock_dep = dependency('gmock', required: false)

gtest_compile_dep = gtest_dep.partial_dependency(compile_args: true, includes: true)
gmock_compile_deps = []
gmock_link_deps = []
if gmock_dep.found()
  gmock_compile_deps += [gmock_dep.partial_dependency(compile_args: true, includes: true)]
  gmock_link_deps += [gmock_dep]
endif

# Normalize CLI11 discovery across environments (pkg-config -> Conan/CMake)
# (touch) reconfigure to drop/accept test file changes [2]
fs = import('fs')
cli11_dep = dependency('cli11', required: false)
if not cli11_dep.found()
  # Some Conan/PkgConfigDeps setups expose CLI11 as 'CLI11' (uppercase)
  cli11_dep = dependency('CLI11', required: false, method: 'pkg-config')
endif
if not cli11_dep.found()
  conan_gen_opt = get_option('conan-generators-dir')
  conan_candidate_dirs = []
  if conan_gen_opt != ''
    conan_candidate_dirs += [join_paths(meson.project_source_root(), conan_gen_opt)]
  endif

  base_build = meson.project_build_root()
  default_candidates = [
    base_build,
    join_paths(base_build, 'conan'),
    join_paths(base_build, 'conan', 'generators'),
    join_paths(base_build, 'generators'),
  ]
  foreach variant : ['build-debug', 'build-release', 'debug', 'release']
    variant_path = join_paths(base_build, variant)
    if fs.exists(variant_path)
      default_candidates += [variant_path]
      default_candidates += [join_paths(variant_path, 'conan')]
      default_candidates += [join_paths(variant_path, 'conan', 'generators')]
      default_candidates += [join_paths(variant_path, 'generators')]
    endif
  endforeach

  foreach cand : default_candidates
    if fs.exists(cand)
      conan_candidate_dirs += [cand]
    endif
  endforeach

  cli11_paths = []
  foreach base : conan_candidate_dirs
    if fs.exists(base)
      cli11_paths += base
      foreach sub : ['CLI11', 'cli11']
        cand = join_paths(base, sub)
        if fs.exists(cand)
          cli11_paths += cand
        endif
      endforeach
    endif
  endforeach

  # Allow explicit override from option
  cli11_opt = get_option('cli11-cmake-path')
  if cli11_opt != ''
    cli11_paths += cli11_opt
  endif

  if cli11_paths.length() > 0
    cli11_dep = dependency('cli11', method: 'cmake', cmake_module_path: cli11_paths, required: false)
  endif
endif
if not cli11_dep.found()
  cli11_dep = dependency('CLI11', method: 'cmake', required: false)
endif
if not cli11_dep.found()
  cli11_dep = declare_dependency()
endif

# Common dependencies for tests; mirror CLI breadth to cover includes
test_deps = [
  dependency('spdlog'),
  dependency('nlohmann_json'),
  dependency('sqlite3'),
  dependency('OpenSSL'),

  cli11_dep,
  dependency('boost', required: true),
  dependency('libcurl', required: false),
  dependency('zlib'),
  dependency('fmt', required: false),
  dependency('yams_core'),
  dependency('yams_config'),
  dependency('yams_crypto'),
  dependency('yams_chunking'),
  dependency('yams_compression'),
  dependency('yams_storage_engine'),
  dependency('yams_reference_counter'),
  dependency('yams_manifest'),
  dependency('yams_integrity'),
  dependency('yams_metadata'),
  dependency('yams_extraction'),
  dependency('yams_detection'),
  dependency('yams_content'),
  dependency('yams_downloader'),
  dependency('yams_indexing'),
  dependency('yams_vector'),
  dependency('yams_search'),
  dependency('yams_api'),
  dependency('yams_repair'),
  dependency('yams_app_services'),
  dependency('yams_daemon_client'),
  dependency('yams_daemon'),
  dependency('yams_mcp'),
]

tests_has_cli = false
tests_has_cli11 = cli11_dep.found()

if get_option('build-cli')
  yams_cli_dep = dependency('yams_cli', required: false)
  if yams_cli_dep.found()
    test_deps += yams_cli_dep
    tests_has_cli = true
  else
    message('build-cli enabled but yams_cli dependency not found; skipping CLI-dependent tests.')
  endif
else
  # If build-cli is disabled, still try to find yams_cli but don't error if missing
  yams_cli_dep = dependency('yams_cli', required: false)
  if yams_cli_dep.found()
    test_deps += yams_cli_dep
    tests_has_cli = true
  endif
endif

incs = [
  include_directories('..'),             # project root
  include_directories('.'),              # tests/
  include_directories('common'),         # shared helpers
  include_directories('../include'),     # headers
  include_directories('../tools/yams-tools/include'),
]

# Keep project strict, but don’t fail unit tests on common test-only warnings.
# We downgrade selective warnings to non-errors for test targets only.
cxx = meson.get_compiler('cpp')
test_cpp_noerror_args = cxx.get_supported_arguments([
  '-Wno-error=sign-compare',
  '-Wno-error=unused-variable',
  '-Wno-error=unused-but-set-variable',
  '-Wno-error=unused-result',
  '-Wno-error=unused-lambda-capture',
  '-Wno-error=missing-field-initializers',
])

## Meson-native test discovery via structured subdirs (no shell globs)
# Build unit test object library in subdir and link here for the aggregate runner
subdir('unit')



if get_option('enable-integration-tests')
  subdir('integration/smoke')
  subdir('integration/daemon')
endif
if get_option('enable-mobile-tests')
  subdir('mobile')
endif

stress_srcs = files(
  'stress/main.cpp',
  'stress/storage/concurrent_test.cpp',
)

common_srcs = []  # currently no .cpp files under tests/common
sdk_srcs = []     # currently no .cpp files under tests/sdk

# NOTE (PBI 028): Temporarily disable plugin tests to avoid duplicate plugin
# symbol definitions in a single aggregated integration binary.
plugin_test_srcs = []

all_unit = []  # unit sources now provided by 'yams_unit_objs' from tests/unit/meson.build
all_stress = stress_srcs

# Always rely on tests/unit_main.cpp; do not include tests/test_main.cpp here
test_main = []

heavy_tests = [
  'ResourcePoolTest.MaxSizeEnforcement',
  'StreamingProcessorTest.*',
  'ZstandardCompressorTest.DecompressionPerformance',
  'DatabaseTest.Migrations',
  'ResultRankerTest.RankingResults',
  'CompressedStorageStatsTest.*',
  'IntegrityVerifierTest.*',
  'DaemonTest.*',
  'StreamingChunkerTest.*',
  'ChunkValidatorParallel.*',
  'DownloadManagerExport.*',
  'StorageBackendTest.*',
  'RepairCoordinatorTest.*',
  # Temporary skips to stabilize shards (Option A): vector DB / WAL heavy
  'VectorDatabaseTest.*',
  'VectorDatabasePersistenceTest.*',
  'VectorIndexManagerTest.*',
  'OnnxRuntimeTest.*',
  'EmbeddingGeneratorTest.*',
  'ModelManagementTest.*',
  'WALDualReadTest.*',
  'WALManagerComprehensiveTest.*',
  'WALManagerTest.*',
]

unit_exclude = '*-' + ':'.join(heavy_tests)


# Compute build_rpath for external deps (so tests run without env vars)
onnx_dep = dependency('onnxruntime', required: false, method: 'pkg-config')
if not onnx_dep.found()
  onnx_dep = dependency('onnxruntime', required: false)
endif

tbb_dep = dependency('tbb', required: false, method: 'pkg-config')
if not tbb_dep.found()
  tbb_dep = dependency('onetbb', required: false, method: 'pkg-config')
endif
if not tbb_dep.found()
  tbb_dep = dependency('tbb', required: false)
endif
if not tbb_dep.found()
  tbb_dep = dependency('onetbb', required: false)
endif

# Collect library directories from pkg-config generated by Conan
_test_rpaths = []
foreach d : [onnx_dep, tbb_dep]
  if d.found()
    libdir = d.get_variable(pkgconfig: 'libdir', default_value: '')
    if libdir != ''
      _test_rpaths += libdir
    endif
  endif
endforeach

unit_exe = executable('yams_unit_tests', files('unit/main.cpp'),
  include_directories: incs,
  dependencies: [gtest_dep] + gmock_link_deps + test_deps,
  cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
  link_whole: yams_unit_objs,
  build_rpath: ':'.join(_test_rpaths),
  install: false,
)
  unit_shards = get_option('unit-shards')
  unit_timeout = get_option('unit-timeout')
  bench_timeout = get_option('bench-timeout')
  # CI-friendly timeout for the unit_safe runner: 2x unit_timeout, floor at 600s
  safe_timeout = unit_timeout * 2
  if safe_timeout < 600
    safe_timeout = 600
  endif

  if get_option('enable-full-unit') and unit_shards > 1
    # Derive per-shard timeout (ceil division) with a small floor of 120s
    shard_timeout = (unit_timeout + unit_shards - 1) / unit_shards
    if shard_timeout < 120
      shard_timeout = 120
    endif

    foreach i : range(unit_shards)
      test('unit_shard@0@'.format(i), unit_exe,
        suite: 'unit',
        args: ['--gtest_filter=' + unit_exclude],
        env: {
          'GTEST_TOTAL_SHARDS': unit_shards.to_string(),
          'GTEST_SHARD_INDEX': i.to_string(),
          # Be brief to reduce log size
          'GTEST_BRIEF': '1',
          'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
          # Stabilize SQLite/vec for unit runs (PBI-040)
          'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
          'YAMS_SQLITE_VEC_INIT_TIMEOUT_MS': '1500',
          'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
          'YAMS_VDB_IN_MEMORY': '1',
          'YAMS_SQLITE_VEC_SKIP_INIT': '1',
          'YAMS_DISABLE_VECTORS': '1',
          'YAMS_TESTING': '1',
        },
        timeout: shard_timeout
      )
    endforeach
  elif get_option('enable-full-unit')
    test('unit', unit_exe,
      suite: 'unit',
      args: ['--gtest_filter=' + unit_exclude],
      env: {
        'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
        'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
        'YAMS_SQLITE_VEC_INIT_TIMEOUT_MS': '1500',
        'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
        'YAMS_VDB_IN_MEMORY': '1',
        'YAMS_SQLITE_VEC_SKIP_INIT': '1',
        'YAMS_DISABLE_VECTORS': '1',
        'YAMS_TESTING': '1',
      },
      timeout: unit_timeout)
  endif

  if get_option('enable-bench-tests')
  test('unit_isolated_integrity_verifier', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=IntegrityVerifierTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  # Daemon lifecycle smoke coverage remains opt-in because it performs full daemon start/stop sequences.
  endif

  if get_option('enable-daemon-long-tests')
  test('unit_isolated_daemon', unit_exe,
    suite: ['daemon-long'],
    args: ['--gtest_filter=DaemonTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )
  endif

  if get_option('enable-bench-tests')
  test('unit_isolated_streaming_chunker', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=StreamingChunkerTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_chunk_validator', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=ChunkValidatorParallel.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_download_manager', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=DownloadManagerExport.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_storage_backend', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=StorageBackendTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_repair_coordinator', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=RepairCoordinatorTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_service_manager', unit_exe,
    suite: ['bench', 'isolated'],
    args: ['--gtest_filter=ServiceManagerTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_shard_vectordb_metrics', unit_exe,
    suite: ['bench'],
    args: ['--gtest_filter=DaemonMetrics.VectorDbReadyHealsFromHandle'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  test('unit_shard_post_ingest_queue', unit_exe,
    suite: ['bench'],
    args: ['--gtest_filter=PostIngestQueueStandardTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  # Fast warm-latency bench (replaces unit WarmLatencyBenchmark)
  test('unit_bench_daemon_warm_latency', unit_exe,
    suite: ['bench'],
    args: ['--gtest_filter=DaemonBench.WarmLatencyFast'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      # Require an explicit opt-in to actually run the daemon warm bench
      # in CI or local: YAMS_ENABLE_DAEMON_BENCH=1 must be set in addition
      # to this fast flag; otherwise the test self-skips.
      'YAMS_BENCH_FAST': '1',
    },
    timeout: bench_timeout,
  )

  endif

  if get_option('enable-reliability-tests')
  test('unit_reliability_grep_search_startup', unit_exe,
    suite: ['bench', 'reliability'],
    args: ['--gtest_filter=GrepServiceTest.RetriesTransientMetadataErrors:GrepServiceTest.PropagatesMetadataErrorsWhenTagsUnavailable:SearchServiceTest.LightIndexRetriesTransientMetadataErrors'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )
  endif

  # Define a fast "smoke" subset using gtest filter for developer iteration
  smoke_filter = 'ContentStoreTest.*:ProgressReporterTest.*:ContentMetadataTest.*:ContentStoreBuilderTest.*:SearchSmoke.*'
  test('unit_smoke', unit_exe,
    suite: ['unit', 'smoke'],
    args: ['--gtest_filter=' + smoke_filter],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_VEC_INIT_TIMEOUT_MS': '1500',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_VDB_IN_MEMORY': '1',
      'YAMS_SQLITE_VEC_SKIP_INIT': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'YAMS_TESTING': '1',
    },
    timeout: 120,
  )

  # Targeted fast subset for PBI-045 metadata modernization acceptance
  targeted_filter = 'DatabaseStatementChronoTest.*:QueryHelpersIntegration.*:MetadataRepositoryCache.*:ConnectionPoolMaintenance.*:SqlBuildSelectTest.*:TagsQuerySpecIntegration.*:MetadataRepositoryTest.CountsAndModifiedSince:MetadataValueVariantTest.*'
  test('unit_targeted_metadata', unit_exe,
    suite: ['unit', 'pbi045'],
    args: ['--gtest_filter=' + targeted_filter],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_VEC_INIT_TIMEOUT_MS': '1500',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_VDB_IN_MEMORY': '1',
      'YAMS_SQLITE_VEC_SKIP_INIT': '1',
      'YAMS_DISABLE_VECTORS': '1',
    },
    timeout: 180,
  )

  # PBI-040: unit_safe runner — stable daily gate excluding known heavy/fragile suites
  # This provides a fast, reliable unit test subset for CI/developer iteration
  # CI-stable subset: drop FTS-heavy, repo search, reference counter stats and vector smoke
  safe_include_filter = 'ContentStoreTest.*:ProgressReporterTest.*:QueryHelpersIntegration.*:ConnectionPoolMaintenance.*:BKTreeTest.*:SearchCacheTest.*:QueryParserTest.*:MessageFramingTest.*:ResponseOfTest.*:IPCMessageTypeTest.*'
  test('unit_safe', unit_exe,
    suite: ['unit', 'safe', 'pbi040'],
    args: ['--gtest_filter=' + safe_include_filter],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_VEC_INIT_TIMEOUT_MS': '1500',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_VDB_IN_MEMORY': '1',
      'YAMS_SQLITE_VEC_SKIP_INIT': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'YAMS_TESTING': '1',
    },
    timeout: safe_timeout,
  )

  # PBI-040: Conditional vector smoke test (runs only when sqlite-vec is available)
  test('unit_vector_smoke', unit_exe,
    suite: ['unit', 'vector', 'pbi040'],
    args: ['--gtest_filter=VectorSmokeTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_VEC_INIT_TIMEOUT_MS': '3000',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_VDB_IN_MEMORY': '1',
      'YAMS_TESTING': '1',
      # Note: sqlite-vec env toggles NOT set, allowing initialization if available
    },
    timeout: 120,
  )


## integration_smoke now defined in subdir('integration/smoke')

# Next integration group: Services (daemon ingestion/retrieval)
# Compile this group separately to avoid duplicate plugin symbols and to allow isolated serial runs.
# Prefer Meson-native subdirs over shell discovery for services integration tests
subdir('integration/services')

# Benchmarks (standalone executables). Register bench test target only when enabled.
subdir('benchmarks')

# Standalone, minimal daemon warm-latency bench (does not depend on unit object lib)
daemon_warm_bench = executable('yams_bench_daemon_warm',
  files('benchmarks/daemon_warm_latency_main.cpp'),
  include_directories: incs,
  dependencies: [test_deps],
  install: false,
)

if get_option('enable-bench-tests')
  test('bench_daemon_warm_latency', daemon_warm_bench,
    suite: ['bench'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
    },
    timeout: bench_timeout,
  )
endif

# Acceptance tests (E2E validation)
subdir('acceptance')

# Stress tests can be heavy; register but mark as long-running via suite name
if get_option('enable-stress-tests') and all_stress.length() > 0
  stress_exe = executable('yams_stress_tests', all_stress + test_main,
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )
  test('stress', stress_exe, suite: 'stress', timeout: 1800)
endif
