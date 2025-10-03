#! Meson configure-time test discovery without fs.glob/subdir_glob

gtest_dep = dependency('gtest', required: true)
gmock_dep = dependency('gmock', required: false)

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
  dependency('yams_cli'),
]

if gmock_dep.found()
  test_deps += gmock_dep
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



subdir('integration/smoke')

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
]

unit_exclude = '*-' + ':'.join(heavy_tests)

unit_exe = executable('yams_unit_tests', files('unit/main.cpp'),
  include_directories: incs,
  dependencies: [gtest_dep, test_deps],
  cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
  link_whole: yams_unit_objs,
  install: false,
)
  unit_shards = get_option('unit-shards')
  unit_timeout = get_option('unit-timeout')
  bench_timeout = get_option('bench-timeout')

  if unit_shards > 1
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
        },
        timeout: shard_timeout
      )
    endforeach
  else
    test('unit', unit_exe,
      suite: 'unit',
      args: ['--gtest_filter=' + unit_exclude],
      env: {'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1'},
      timeout: unit_timeout)
  endif

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
  test('unit_isolated_daemon', unit_exe,
    suite: ['daemon-long'],
    args: ['--gtest_filter=DaemonTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

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

  test('unit_reliability_grep_search_startup', unit_exe,
    suite: ['bench', 'reliability'],
    args: ['--gtest_filter=GrepServiceTest.RetriesTransientMetadataErrors:GrepServiceTest.PropagatesMetadataErrorsWhenTagsUnavailable:SearchServiceTest.LightIndexRetriesTransientMetadataErrors'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
    },
    timeout: bench_timeout,
  )

  # Define a fast "smoke" subset using gtest filter for developer iteration
  smoke_filter = 'ContentStoreTest.*:ProgressReporterTest.*:ContentMetadataTest.*:ContentStoreBuilderTest.*:SearchSmoke.*'
  test('unit_smoke', unit_exe,
    suite: ['unit', 'smoke'],
    args: ['--gtest_filter=' + smoke_filter],
    env: {'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1'},
    timeout: 120,
  )


## integration_smoke now defined in subdir('integration/smoke')

# Next integration group: Services (daemon ingestion/retrieval)
# Compile this group separately to avoid duplicate plugin symbols and to allow isolated serial runs.
# Prefer Meson-native subdirs over shell discovery for services integration tests
subdir('integration/services')

# Benchmarks (standalone executables, not part of "meson test")
subdir('benchmarks')

# Stress tests can be heavy; register but mark as long-running via suite name
if all_stress.length() > 0
  stress_exe = executable('yams_stress_tests', all_stress + test_main,
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )
  test('stress', stress_exe, suite: 'stress', timeout: 1800)
endif
