#! Meson configure-time test discovery without fs.glob/subdir_glob

gtest_dep = dependency('gtest', required: false)
gmock_dep = dependency('gmock', required: false)

gtest_compile_dep = []
if gtest_dep.found()
  gtest_compile_dep = gtest_dep.partial_dependency(compile_args: true, includes: true)
endif
gmock_compile_deps = []
gmock_link_deps = []
if gmock_dep.found()
  gmock_compile_deps += [gmock_dep.partial_dependency(compile_args: true, includes: true)]
  gmock_link_deps += [gmock_dep]
endif

# Catch2 dependency for modern test framework (PBI-050)
# Prefer system installs, but fall back to Meson wrap subproject when available.
catch2_dep = dependency('catch2', required: false, fallback: ['catch2', 'catch2_dep'])
catch2_main_dep = dependency('catch2-with-main', required: false, fallback: ['catch2', 'catch2_with_main_dep'])

# Get Catch2 partial dependency with includes for test builds
catch2_compile_dep = []
if catch2_dep.found()
  catch2_compile_dep = [catch2_dep.partial_dependency(compile_args: true, includes: true)]
endif

# Trailing link deps for Catch2 tests - ensure spdlog/fmt symbols are available
# after Catch2 libs (static link order matters: users before providers)
catch2_trailing_link_deps = [
  dependency('spdlog'),
  dependency('fmt', required: false),
]

catch2_incs = []

# TSAN suppressions file for tests (if TSAN is enabled)
# Must be defined early before subdirs are included
_tsan_suppressions_file = meson.project_source_root() / 'tsan.suppressions'
_test_tsan_options = 'suppressions=' + _tsan_suppressions_file

# Normalize CLI11 discovery across environments (pkg-config -> Conan/CMake)
# (touch) reconfigure to drop/accept test file changes [2]
fs = import('fs')
cli11_dep = dependency('cli11', required: false)
if not cli11_dep.found()
  cli11_dep = dependency('CLI11', required: false, method: 'pkg-config')
endif
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
    join_paths(base_build, 'build-release', 'conan', 'generators'),
    join_paths(base_build, 'build-debug', 'conan', 'generators'),
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
    if not cli11_dep.found()
      cli11_dep = dependency('CLI11', method: 'cmake', cmake_module_path: cli11_paths, required: false)
    endif
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
  dependency('yams_plugins_mgmt'),
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

onnxruntime_dep_opt = dependency('onnxruntime', required: false)
if onnxruntime_dep_opt.found()
  test_deps += onnxruntime_dep_opt
endif

# Windows: stage runtime DLLs next to every test executable (mirrors Windows install).
# Placing DLLs alongside the test binaries prevents the loader from falling back to
# older System32 copies (e.g., onnxruntime.dll API 17) and keeps tests aligned with
# the Conan-shipped runtime used for installation.
if host_machine.system() == 'windows' and windows_runtime_dlls.length() > 0
  py_mod = import('python')
  python = py_mod.find_installation('python3')

  # All test binary directories (relative to builddir/tests)
  test_runtime_dirs = [
    '.',
    'integration',
    'integration/smoke',
    'integration/daemon',
    'integration/cli',
    'integration/search',
    'integration/services',
  ]

  foreach dll_info : windows_runtime_dlls
    dll_name = dll_info[0]
    dll_src = dll_info[1]

    foreach dest_rel : test_runtime_dirs
      dest_path = join_paths(meson.current_build_dir(), dest_rel, dll_name)
      stamp = 'dllcopy_@0@_@1@.stamp'.format(dll_name, dest_rel.replace('/', '_'))
      custom_target(stamp,
        input: dll_src,
        output: stamp,
        command: [python, '-c',
          'import pathlib, shutil, sys; src=sys.argv[1]; dest=pathlib.Path(sys.argv[2]); stamp=pathlib.Path(sys.argv[3]); dest.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(src, dest); stamp.parent.mkdir(parents=True, exist_ok=True); stamp.write_text("ok")',
          dll_src, dest_path, '@OUTPUT@'],
        build_by_default: true,
      )
    endforeach
  endforeach
endif

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
] + catch2_incs

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
# Build legacy GTest unit tests only when GTest is available.
if gtest_dep.found()
  # Build unit test object library in subdir and link here for the aggregate runner
  subdir('unit')
  if get_option('build-plugins')
    subdir('plugins')
  endif
else
  message('GTest not found; skipping GTest unit/plugin test build (Catch2 tests may still be enabled).')
  yams_unit_objs = []
endif



if get_option('enable-integration-tests') or get_option('buildtype') == 'debug'
  subdir('integration/smoke')
  subdir('integration/daemon')
  if gtest_dep.found()
    subdir('integration/cli')
  else
    message('GTest not found; skipping GTest-based CLI integration tests.')
  endif
  subdir('integration/search')
endif
if get_option('enable-mobile-tests')
  subdir('mobile')
endif

stress_srcs = files(
  'stress/main.cpp',
  'stress/storage/concurrent_test.cpp',
  'stress/mcp_socket_stress_test.cpp',
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
onnx_cmake_paths = []
onnx_candidate_dirs = [base_build, join_paths(base_build, 'conan'), join_paths(base_build, 'conan', 'generators'), join_paths(base_build, 'generators')]

foreach variant : ['build-debug', 'build-release', 'debug', 'release']
  variant_path = join_paths(base_build, variant)
  if fs.exists(variant_path)
    onnx_candidate_dirs += [variant_path]
    onnx_candidate_dirs += [join_paths(variant_path, 'conan')]
    onnx_candidate_dirs += [join_paths(variant_path, 'conan', 'generators')]
    onnx_candidate_dirs += [join_paths(variant_path, 'generators')]
  endif
endforeach

foreach cand : onnx_candidate_dirs
  if fs.exists(cand)
    onnx_cmake_paths += cand
    foreach sub : ['onnxruntime', 'onnxruntime/Release', 'onnxruntime/Debug']
      subdir = join_paths(cand, sub)
      if fs.exists(subdir)
        onnx_cmake_paths += subdir
      endif
    endforeach
  endif
endforeach

# Prefer Conan CMake config before falling back to system discovery
onnx_dep = dependency('onnxruntime', method: 'cmake', cmake_module_path: onnx_cmake_paths, required: false)
if not onnx_dep.found()
  onnx_dep = dependency('onnxruntime', required: false, method: 'pkg-config')
endif
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

# Add TBB to test_deps if found (needed by yams_vector via ONNX runtime)
if tbb_dep.found()
  test_deps += tbb_dep
endif

# Collect library directories from pkg-config generated by Conan
_test_rpaths = []
_test_ld_library_path = []
foreach d : [onnx_dep, tbb_dep]
  if d.found()
    libdir = d.get_variable(pkgconfig: 'libdir', default_value: '')
    if libdir != ''
      _test_rpaths += libdir
      _test_ld_library_path += libdir
    endif
  endif
endforeach

# Prepare LD_LIBRARY_PATH for test environment (needed for transitive TBB dependencies)
_test_env_ld_library_path = ':'.join(_test_ld_library_path)

 unit_exe = []
 if gtest_dep.found()
   unit_exe = executable('yams_unit_tests', files('unit/main.cpp'),
     include_directories: incs,
     dependencies: [gtest_dep] + gmock_link_deps + test_deps,
     cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
     link_whole: yams_unit_objs,
     build_rpath: ':'.join(_test_rpaths),
     install: false,
   )
 else
   message('GTest not found; skipping GTest-based unit test registration (Catch2 tests may still be enabled).')
 endif
  unit_shards = get_option('unit-shards')
  unit_timeout = get_option('unit-timeout')
  bench_timeout = get_option('bench-timeout')
  # CI-friendly timeout for the unit_safe runner: 2x unit_timeout, floor at 600s
  safe_timeout = unit_timeout * 2
  if safe_timeout < 600
    safe_timeout = 600
  endif

  if gtest_dep.found() and get_option('enable-full-unit') and unit_shards > 1
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
          # Add TBB library path for transitive dependencies
          'LD_LIBRARY_PATH': _test_env_ld_library_path,
        },
        timeout: shard_timeout
      )
    endforeach
  elif gtest_dep.found() and get_option('enable-full-unit')
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
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
      },
      timeout: unit_timeout)
  endif

  if gtest_dep.found() and get_option('enable-bench-tests')
  # Note: All bench tests use suite 'bench' or ['bench', 'isolated']
  # Run unified benchmarks with: meson test -C builddir --suite bench

  test('unit_isolated_integrity_verifier', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=IntegrityVerifierTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
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

  if gtest_dep.found() and get_option('enable-bench-tests')
  test('unit_isolated_streaming_chunker', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=StreamingChunkerTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_chunk_validator', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=ChunkValidatorParallel.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_download_manager', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=DownloadManagerExport.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_storage_backend', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=StorageBackendTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_repair_coordinator', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=RepairCoordinatorTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_isolated_service_manager', unit_exe,
    suite: ['bench', 'isolated', 'perf'],
    args: ['--gtest_filter=ServiceManagerTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_shard_vectordb_metrics', unit_exe,
    suite: ['bench', 'perf'],
    args: ['--gtest_filter=DaemonMetrics.VectorDbReadyHealsFromHandle'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  test('unit_shard_post_ingest_queue', unit_exe,
    suite: ['bench', 'perf'],
    args: ['--gtest_filter=PostIngestQueueStandardTest.*'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  # Fast warm-latency bench (replaces unit WarmLatencyBenchmark)
  test('unit_bench_daemon_warm_latency', unit_exe,
    suite: ['bench', 'perf'],
    args: ['--gtest_filter=DaemonBench.WarmLatencyFast'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'GTEST_BRIEF': '1',
      # Require an explicit opt-in to actually run the daemon warm bench
      # in CI or local: YAMS_ENABLE_DAEMON_BENCH=1 must be set in addition
      # to this fast flag; otherwise the test self-skips.
      'YAMS_BENCH_FAST': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )

  endif

  if gtest_dep.found() and get_option('enable-reliability-tests')
  test('unit_reliability_grep_search_startup', unit_exe,
    suite: ['bench', 'reliability'],
    args: ['--gtest_filter=GrepServiceTest.RetriesTransientMetadataErrors:GrepServiceTest.PropagatesMetadataErrorsWhenTagsUnavailable:SearchServiceTest.LightIndexRetriesTransientMetadataErrors'],
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
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )
  endif

  if gtest_dep.found()
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
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
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
        'YAMS_TESTING': '1',
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
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
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
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
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
      },
      timeout: 120,
    )
  endif


## integration_smoke now defined in subdir('integration/smoke')

# Next integration group: Services (daemon ingestion/retrieval)
# Compile this group separately to avoid duplicate plugin symbols and to allow isolated serial runs.
# Prefer Meson-native subdirs over shell discovery for services integration tests
if gtest_dep.found()
  subdir('integration/services')
else
  message('GTest not found; skipping GTest-based services integration tests (Catch2-based suites may still build).')
endif

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
    suite: ['bench', 'perf'],
    env: {
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
    },
    timeout: bench_timeout,
  )
endif

# Acceptance tests (E2E validation)
subdir('acceptance')

# Stress tests can be heavy; register but mark as long-running via suite name
if gtest_dep.found() and get_option('enable-stress-tests') and all_stress.length() > 0
  stress_exe = executable('yams_stress_tests', all_stress + test_main,
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )
  test('stress', stress_exe,
    suite: 'stress',
    args: ['--gtest_filter=MCPSocketStressTest.*'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
    },
    timeout: 300)
elif get_option('enable-stress-tests')
  message('GTest not found; skipping stress test registration.')
endif

# ============================================================================
# Catch2 Test Executables (PBI-068: Separated for isolation)
# Each test suite gets its own executable to prevent resource conflicts
# and enable parallel execution. This resolves SIGABRT crashes from shared
# filesystem state and temp directory conflicts.
# ============================================================================

if catch2_dep.found()

  # Daemon Test Suites (6 consolidated executables)
  # --------------------------------------------------------------------------

  catch2_daemon_fsm_exe = executable('catch2_daemon_fsm',
    files('unit/daemon/daemon_fsm_suite_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_metrics_exe = executable('catch2_daemon_metrics',
    files('unit/daemon/daemon_metrics_status_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_components_exe = executable('catch2_daemon_components',
    files('unit/daemon/daemon_components_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_protocol_exe = executable('catch2_daemon_protocol',
    files('unit/daemon/daemon_protocol_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_background_exe = executable('catch2_daemon_background',
    files('unit/daemon/daemon_background_processing_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_graph_component_exe = executable('catch2_daemon_graph_component',
    files(
      'unit/daemon/graph_component_catch2_test.cpp',
      'unit/app/services/graph_query_service_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Daemon component submodule tests (CheckpointManager, ConfigResolver, etc.)
  # Migrated from GTest as part of yams-3s4 / yams-zns
  catch2_daemon_component_submodule_exe = executable('catch2_daemon_component_submodule',
    files(
      'unit/daemon/components/checkpoint_manager_test.cpp',
      'unit/daemon/components/config_resolver_test.cpp',
      'unit/daemon/components/database_manager_test.cpp',
      'unit/daemon/components/plugin_manager_test.cpp',
      'unit/daemon/plugin_manager_config_test.cpp',
      'unit/daemon/components/request_queue_test.cpp',
      'unit/daemon/components/vector_system_manager_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Gradient limiter tests (Netflix Gradient2 adaptive concurrency)
  catch2_gradient_limiter_exe = executable('catch2_gradient_limiter',
    files('unit/daemon/components/gradient_limiter_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # WorkCoordinator tests (async work coordination)
  catch2_work_coordinator_exe = executable('catch2_work_coordinator',
    files('unit/daemon/work_coordinator_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # PostIngestQueue unit tests (LruCache template + stage constants)
  catch2_post_ingest_queue_unit_exe = executable('catch2_post_ingest_queue_unit',
    files('unit/daemon/components/post_ingest_queue_unit_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Add plugin path for plugin harness tests
  plugin_test_cpp_args = test_cpp_noerror_args + ['-DYAMS_TESTING=1']
  if get_option('plugin-onnx')
    # Pass the expected plugin location to the test
    onnx_plugin_path = join_paths(meson.project_build_root(), 'plugins', 'onnx', 'libyams_onnx_plugin.so')
    plugin_test_cpp_args += ['-Dyams_onnx_plugin_BUILT="' + onnx_plugin_path + '"']
  endif

  catch2_daemon_plugin_exe = executable('catch2_daemon_plugin',
    files('unit/daemon/onnx_plugin_harness_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: plugin_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Reranker session test (uses mock mode by default, real model optional)
  # Compile the reranker session source directly into the test to avoid plugin linking issues
  reranker_test_deps = [catch2_dep, catch2_main_dep] + test_deps
  if onnx_dep.found()
    reranker_test_deps += onnx_dep
  endif

  catch2_daemon_reranker_exe = executable('catch2_daemon_reranker',
    files(
      'unit/daemon/onnx_reranker_session_test.cpp',
      '../plugins/onnx/onnx_reranker_session.cpp',
    ),
    include_directories: incs,
    dependencies: reranker_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  if onnx_dep.found()
    onnx_provider_test_deps = [catch2_dep, catch2_main_dep] + test_deps + onnx_dep
    catch2_daemon_onnx_provider_exe = executable('catch2_daemon_onnx_provider',
      files(
        'unit/daemon/onnx_model_provider_config_test.cpp',
        '../plugins/onnx/model_provider.cpp',
        '../plugins/onnx/onnx_model_pool.cpp',
        '../plugins/onnx/onnx_reranker_session.cpp',
        '../plugins/onnx/onnx_colbert_session.cpp',
      ),
      include_directories: incs,
      dependencies: onnx_provider_test_deps,
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )

    catch2_gpu_provider_exe = executable('catch2_gpu_provider',
      files('unit/daemon/onnx_gpu_provider_catch2_test.cpp'),
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep] + test_deps + [onnx_dep],
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )
  endif

  catch2_plugin_infrastructure_exe = executable('catch2_plugin_infrastructure',
    files('unit/daemon/plugin_infrastructure_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: plugin_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_external_plugin_host_exe = executable('catch2_external_plugin_host',
    files('unit/daemon/external_plugin_host_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: plugin_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_extractor_adapter_exe = executable('catch2_extractor_adapter',
    files('unit/daemon/plugin_content_extractor_adapter_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Daemon repair and service manager tests (Catch2)
  # Migrated from GTest as part of yams-3s4
  # RepairCoordinator tests disabled — replaced by RepairService (port pending)
  # catch2_repair_coordinator_exe = executable('catch2_repair_coordinator',
  #   files('unit/daemon/repair_coordinator_catch2_test.cpp'),
  #   include_directories: incs,
  #   dependencies: [catch2_dep, catch2_main_dep] + test_deps,
  #   cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
  #   build_rpath: ':'.join(_test_rpaths),
  #   install: false,
  # )

  catch2_repair_scheduling_exe = executable('catch2_repair_scheduling',
    files('unit/daemon/repair_scheduling_adapter_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_repair_service_exe = executable('catch2_repair_service',
    files('unit/daemon/repair_service_post_ingest_channel_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_service_manager_exe = executable('catch2_service_manager',
    files('unit/daemon/service_manager_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_streaming_processor_exe = executable('catch2_streaming_processor',
    files('unit/daemon/streaming_processor_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_multiplex_client_exe = executable('catch2_multiplex_client',
    files('unit/daemon/multiplex_client_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_entity_graph_service_exe = executable('catch2_entity_graph_service',
    files('unit/daemon/entity_graph_service_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_service_manager_shutdown_exe = executable('catch2_service_manager_shutdown',
    files('unit/daemon/service_manager_shutdown_ownership_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_service_manager_degraded_exe = executable('catch2_service_manager_degraded',
    files('unit/daemon/service_manager_provider_degraded_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_repair_binary_filter_exe = executable('catch2_repair_binary_filter',
    files('unit/daemon/repair_binary_filter_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_abi_model_provider_dim_exe = executable('catch2_abi_model_provider_dim',
    files('unit/daemon/abi_model_provider_dimension_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_embedding_recovery_exe = executable('catch2_embedding_recovery',
    files('unit/daemon/embedding_provider_recovery_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_request_io_edge_exe = executable('catch2_request_io_edge',
    files('unit/daemon/request_handler_io_edge_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_warm_bench_exe = executable('catch2_daemon_warm_bench',
    files('unit/daemon/daemon_warm_bench_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_request_lifetime_exe = executable('catch2_request_lifetime',
    files('unit/daemon/request_handler_lifetime_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_writer_drain_cleanup_exe = executable('catch2_writer_drain_cleanup',
    files('unit/daemon/request_handler_writer_drain_cleanup_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_asio_connection_pool_exe = executable('catch2_asio_connection_pool',
    files('unit/daemon/asio_connection_pool_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_exe = executable('catch2_daemon',
    files('unit/daemon/daemon_catch2_test.cpp'),
    files('unit/daemon/streaming_handlers_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_tune_advisor_threshold_exe = executable('catch2_tune_advisor_threshold',
    files('unit/daemon/tune_advisor_threshold_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_tuning_allocation_exe = executable('catch2_tuning_allocation',
    files('unit/daemon/tuning_allocation_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_tuning_reconciliation_exe = executable('catch2_tuning_reconciliation',
    files('unit/daemon/tuning_reconciliation_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_post_ingest_dynamic_cap_recovery_exe = executable('catch2_post_ingest_dynamic_cap_recovery',
    files('unit/daemon/post_ingest_dynamic_cap_recovery_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_resource_governance_exe = executable('catch2_resource_governance',
    files('unit/daemon/resource_governance_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_timing_audit_exe = executable('catch2_timing_audit',
    files('unit/daemon/timing_audit_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # NOTE: onnx_model_pool tests require linking with the ONNX plugin library,
  # which is not directly available as a dependency. The original GTest version
  # was also never properly linked. Skipping Catch2 migration until plugin
  # test harness infrastructure is added.
  # catch2_onnx_model_pool_exe = executable('catch2_onnx_model_pool',
  #   files('unit/daemon/onnx_model_pool_catch2_test.cpp'),
  #   include_directories: incs,
  #   dependencies: [catch2_dep, catch2_main_dep, onnx_dep] + test_deps,
  #   cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
  #   build_rpath: ':'.join(_test_rpaths),
  #   install: false,
  # )

  # CLI Test Suites (Catch2)
  # --------------------------------------------------------------------------

  catch2_cli_init_command_exe = executable('catch2_cli_init_command',
    files('unit/cli/init_command_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Storage Test Suites (Catch2)
  # --------------------------------------------------------------------------

  catch2_storage_backend_exe = executable('catch2_storage_backend',
    files('unit/storage/storage_backend_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Storage submodule tests (StorageEngine, CompressedStorageEngine, ReferenceCounter, etc.)
  # Migrated from GTest as part of yams-3s4 / yams-8b9
  catch2_storage_submodule_exe = executable('catch2_storage_submodule',
    files(
      'unit/storage/compressed_storage_engine_catch2_test.cpp',
      'unit/storage/compressed_storage_stats_catch2_test.cpp',
      'unit/storage/object_storage_adapter_catch2_test.cpp',
      'unit/storage/reference_counter_catch2_test.cpp',
      'unit/storage/reference_counter_stress_catch2_test.cpp',
      'unit/storage/s3_signer_catch2_test.cpp',
      'unit/storage/storage_engine_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # WAL Entry tests - serialization, checksums, encode/decode round-trips
  catch2_wal_entry_exe = executable('catch2_wal_entry',
    files('unit/wal/wal_entry_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Search submodule tests (QueryParser, SearchExecutor, etc.)
  # Migrated from GTest as part of yams-3s4 / yams-1yq
  catch2_search_submodule_exe = executable('catch2_search_submodule',
    files(
      'unit/search/chunk_coverage_catch2_test.cpp',
      'unit/search/entity_linker_catch2_test.cpp',
      'unit/search/kg_scorer_simple_catch2_test.cpp',
      'unit/search/query_parser_catch2_test.cpp',
      'unit/search/query_qualifiers_catch2_test.cpp',
      'unit/search/search_smoke_catch2_test.cpp',
      'unit/search/query_literal_extractor_catch2_test.cpp',
      'unit/search/search_filters_catch2_test.cpp',
      'unit/search/search_results_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # SearchTuner tests (Phase 2 of Epic yams-7ez4: Adaptive Search Engine Tuning via Corpus-Aware FSM)
  # Tests FSM state transitions, TunedParams matrix, and SearchEngineConfig generation
  catch2_search_tuner_exe = executable('catch2_search_tuner',
    files('unit/search/search_tuner_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # InternalBenchmark tests (Phase 3 of Epic yams-7ez4: Internal Benchmark Suite)
  # Tests LatencyStats, BenchmarkResults, BenchmarkComparison, query generation configs
  catch2_internal_benchmark_exe = executable('catch2_internal_benchmark',
    files('unit/search/internal_benchmark_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Compression submodule tests (CompressionPolicy, CompressionMonitor, Zstandard, LZMA, ErrorHandler, etc.)
  # Migrated from GTest as part of yams-3s4 / yams-pmf
  catch2_compression_submodule_exe = executable('catch2_compression_submodule',
    files(
      'unit/compression/basic_error_handler_catch2_test.cpp',
      # TODO(yams-3s4): Fix API mismatch - uses non-existent ValidationConfig fields
      # 'unit/compression/basic_integrity_validator_catch2_test.cpp',
      'unit/compression/compression_comprehensive_catch2_test.cpp',
      'unit/compression/compression_monitor_catch2_test.cpp',
      'unit/compression/compression_policy_catch2_test.cpp',
      'unit/compression/error_handler_catch2_test.cpp',
      # TODO(yams-3s4): Fix API mismatch - uses non-existent IntegrityValidator methods
      # 'unit/compression/integrity_validator_catch2_test.cpp',
      'unit/compression/lzma_compressor_catch2_test.cpp',
      'unit/compression/recovery_manager_catch2_test.cpp',
      'unit/compression/transaction_manager_catch2_test.cpp',
      'unit/compression/zstandard_compressor_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # MCP submodule tests (MCPServer, Transport, JSON-RPC, Tools Schema, etc.)
  # Migrated from GTest as part of yams-3s4 / yams-84g
  catch2_mcp_submodule_exe = executable('catch2_mcp_submodule',
    files(
      'unit/mcp/initialize_response_catch2_test.cpp',
      'unit/mcp/mcp_add_disconnect_catch2_test.cpp',
      'unit/mcp/mcp_apps/capability_negotiation_test.cpp',
      'unit/mcp/mcp_apps/tool_metadata_test.cpp',
      'unit/mcp/mcp_apps/ui_resource_handling_test.cpp',
      'unit/mcp/mcp_daemon_dto_parsing_catch2_test.cpp',
      'unit/mcp/mcp_daemon_dto_parsing_more_catch2_test.cpp',
      'unit/mcp/mcp_error_handling_catch2_test.cpp',
      'unit/mcp/mcp_disconnect_uniform_errors_catch2_test.cpp',
      'unit/mcp/mcp_doctor_flags_catch2_test.cpp',
      'unit/mcp/mcp_jsonrpc_roundtrip_catch2_test.cpp',
      'unit/mcp/mcp_protocol_version_features_test.cpp',
      'unit/mcp/mcp_search_dto_catch2_test.cpp',
      'unit/mcp/mcp_server_catch2_test.cpp',
      'unit/mcp/mcp_server_exec_catch2_test.cpp',
      'unit/mcp/mcp_socket_resolution_catch2_test.cpp',
      'unit/mcp/mcp_tool_registry_wrappers_catch2_test.cpp',
      'unit/mcp/mcp_tools_schema_and_smoke_catch2_test.cpp',
      'unit/mcp/stdio_transport_catch2_test.cpp',
      'unit/mcp/websocket_transport_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # CLI submodule tests (Commands, Data Resolution, Error Hints, Prompt Utils, etc.)
  # Migrated from GTest as part of yams-3s4 / yams-cli
  catch2_cli_submodule_exe = executable('catch2_cli_submodule',
    files(
      'unit/common/format_catch2_test.cpp',
      'unit/common/pattern_utils_catch2_test.cpp',
      'unit/common/test_utils_catch2_test.cpp',
      'unit/common/utf8_utils_catch2_test.cpp',
      'unit/cli/add_command_stdin_catch2_test.cpp',
      'unit/cli/cli_option_isolation_catch2_test.cpp',
      'unit/cli/config_command_catch2_test.cpp',
      'unit/cli/daemon_command_catch2_test.cpp',
      'unit/cli/data_dir_resolution_catch2_test.cpp',
      'unit/cli/doctor_command_catch2_test.cpp',
      'unit/cli/error_hints_catch2_test.cpp',
      'unit/cli/graph_helpers_catch2_test.cpp',
      'unit/cli/grep_command_path_tree_catch2_test.cpp',
      'unit/cli/init_command_catch2_test.cpp',
      'unit/cli/model_command_catch2_test.cpp',
      'unit/cli/pipeline_stage_render_catch2_test.cpp',
      'unit/cli/prompt_util_catch2_test.cpp',
      'unit/cli/recommendation_util_catch2_test.cpp',
      'unit/cli/repair_command_catch2_test.cpp',
      'unit/cli/session_command_catch2_test.cpp',
      'unit/cli/stream_heuristics_catch2_test.cpp',
      'unit/cli/update_command_catch2_test.cpp',
      'unit/cli/time_parser_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Vector Test Suites (Catch2) - requires enable-vector-tests
  # Migrated from GTest as part of yams-3s4 / yams-1d4
  # --------------------------------------------------------------------------
  if get_option('enable-vector-tests')
    catch2_vector_submodule_exe = executable('catch2_vector_submodule',
      files(
        'unit/vector/document_chunker_catch2_test.cpp',
        'unit/vector/embedding_genai_selection_catch2_test.cpp',
        'unit/vector/hnsw_index_catch2_test.cpp',
        'unit/vector/multi_dimension_hnsw_catch2_test.cpp',
        'unit/vector/sqlite_vec_backend_comprehensive_catch2_test.cpp',
        'unit/vector/sqlite_vec_c_api_smoke_catch2_test.cpp',
        'unit/vector/vector_dimension_validation_catch2_test.cpp',
        'unit/vector/vector_smoke_catch2_test.cpp',
      ),
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep] + test_deps,
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )
  endif

  # API Test Suites (Catch2) - content store API tests
  # Migrated from GTest as part of yams-3s4 / api
  # --------------------------------------------------------------------------
  catch2_api_submodule_exe = executable('catch2_api_submodule',
    files(
      'unit/api/content_store_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Downloader Test Suites (Catch2) - download manager, resume store, CAS helpers
  # Migrated from GTest as part of yams-3s4 / downloader
  # --------------------------------------------------------------------------
  catch2_downloader_submodule_exe = executable('catch2_downloader_submodule',
    files(
      'unit/downloader/downloader_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Resolve Test Suites (Catch2) - resolver, backend implementations
  # Migrated from GTest as part of yams-3s4 / resolve
  # --------------------------------------------------------------------------
  catch2_resolve_submodule_exe = executable('catch2_resolve_submodule',
    files(
      'unit/resolve/resolver_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Manifest Test Suites (Catch2) - manifest manager, serialization, reconstruction
  # Migrated from GTest as part of yams-3s4 / yams-1jm
  # --------------------------------------------------------------------------
  catch2_manifest_submodule_exe = executable('catch2_manifest_submodule',
    files(
      'unit/manifest/manifest_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Repair Test Suites (Catch2) - embedding repair scan
  # Migrated from GTest as part of yams-3s4 / yams-1jm
  # --------------------------------------------------------------------------
  catch2_repair_submodule_exe = executable('catch2_repair_submodule',
    files(
      'unit/repair/embedding_repair_scan_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Versioning Test Suites (Catch2) - document versioning, indexer
  # Migrated from GTest as part of yams-3s4 / yams-1jm
  # --------------------------------------------------------------------------
  catch2_versioning_submodule_exe = executable('catch2_versioning_submodule',
    files(
      'unit/versioning_document_indexer_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Metadata Test Suites (4 executables)
  # --------------------------------------------------------------------------

  catch2_metadata_retrieval_exe = executable('catch2_metadata_retrieval',
    files('unit/app/services/retrieval_schema_validation_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Removed: catch2_metadata_search (search_metadata_interface_test.cpp) - HybridSearchEngine specific

  catch2_metadata_symbol_exe = executable('catch2_metadata_symbol',
    files('unit/metadata/symbol_metadata_schema_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_metadata_migration_exe = executable('catch2_metadata_migration',
    files('unit/metadata/migration_validation_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Metadata Unit Tests (Catch2) - concepts, database, query_helpers, sql, value_variant
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_metadata_unit_exe = executable('catch2_metadata_unit',
    files('unit/metadata/metadata_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Connection Pool Tests (Catch2) - pool maintenance, waiting guard
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_connection_pool_exe = executable('catch2_connection_pool',
    files('unit/metadata/connection_pool_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # KG Store Tests (Catch2) - knowledge graph store operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_kg_store_exe = executable('catch2_kg_store',
    files('unit/metadata/kg_store_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Database Tests (Catch2) - database operations, connection pool, migrations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_database_exe = executable('catch2_database',
    files('unit/metadata/database_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # FTS5 Tests (Catch2) - FTS5 support detection and fallback
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_fts5_exe = executable('catch2_fts5',
    files(
      'unit/metadata/fts5_catch2_test.cpp',
      'unit/metadata/fts5_query_sanitizer_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Tree Builder Tests (Catch2) - tree node operations and directory tree building
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_tree_builder_exe = executable('catch2_tree_builder',
    files('unit/metadata/tree_builder_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Tree Differ Tests (Catch2) - tree diffing operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_tree_differ_exe = executable('catch2_tree_differ',
    files('unit/metadata/tree_differ_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Path Tree Delete Tests (Catch2) - path tree removal operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_path_tree_delete_exe = executable('catch2_path_tree_delete',
    files('unit/metadata/path_tree_delete_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Session Isolation Tests (Catch2) - session-isolated memory operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_session_isolation_exe = executable('catch2_session_isolation',
    files('unit/metadata/session_isolation_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Metadata Repository Tests (Catch2) - repository operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_metadata_repository_exe = executable('catch2_metadata_repository',
    files('unit/metadata/metadata_repository_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Metadata Repository Cache Tests (Catch2) - cache operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_metadata_repository_cache_exe = executable('catch2_metadata_repository_cache',
    files('unit/metadata/metadata_repository_cache_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Fuzzy Index Prioritization Tests (Catch2) - fuzzy index scoring
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_fuzzy_index_prioritization_exe = executable('catch2_fuzzy_index_prioritization',
    files('unit/metadata/fuzzy_index_prioritization_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # KG Store Alias and Entities Tests (Catch2) - KG alias and entity operations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_kg_store_alias_entities_exe = executable('catch2_kg_store_alias_entities',
    files('unit/metadata/kg_store_alias_and_entities_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # FTS Query Spec Integration Tests (Catch2) - FTS search integration
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_fts_queryspec_integration_exe = executable('catch2_fts_queryspec_integration',
    files('unit/metadata/fts_queryspec_integration_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Tags Query Spec Integration Tests (Catch2) - tags query integration
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_tags_queryspec_integration_exe = executable('catch2_tags_queryspec_integration',
    files('unit/metadata/tags_queryspec_integration_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Query Helpers Integration Tests (Catch2) - query helper integration
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_query_helpers_integration_exe = executable('catch2_query_helpers_integration',
    files('unit/metadata/query_helpers_integration_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Metadata Schema Tests (Catch2) - comprehensive schema, CRUD, FTS, migrations
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_metadata_schema_exe = executable('catch2_metadata_schema',
    files('unit/metadata/metadata_schema_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # FTS5 Failure Scenarios Tests (Catch2) - FTS5 edge cases and failure modes
  # Migrated from GTest as part of yams-3s4 / yams-aqc
  # --------------------------------------------------------------------------
  catch2_fts5_failure_scenarios_exe = executable('catch2_fts5_failure_scenarios',
    files('unit/metadata/fts5_failure_scenarios_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Corpus Stats Tests (Catch2) - corpus statistics for adaptive search tuning
  # Phase 1 of Epic yams-7ez4: Adaptive Search Engine Tuning via Corpus-Aware FSM
  # --------------------------------------------------------------------------
  catch2_corpus_stats_exe = executable('catch2_corpus_stats',
    files('unit/metadata/corpus_stats_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Repository Foundation Tests (Catch2) - entity_traits, result_helpers, crud_ops
  # Phase 1 of ADR-0004: MetadataRepository Refactor with Templates and Metaprogramming
  # --------------------------------------------------------------------------
  catch2_repository_foundation_exe = executable('catch2_repository_foundation',
    files('unit/metadata/repository_foundation_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Core tests (minimal dependencies - no daemon)
  core_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
  ]

  core_cpp23_features_exe = executable('core_cpp23_features',
    files('unit/core/cpp23_features_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + core_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  core_magic_numbers_exe = executable('core_magic_numbers',
    files('unit/core/magic_numbers_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + core_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  core_uuid_exe = executable('core_uuid',
    files('unit/core/uuid_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + core_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  detection_exe = executable('detection',
    files(
      'unit/detection/command_integration_test.cpp',
      'unit/detection/detection_test_helpers.h',
      'unit/detection/file_type_detector_test.cpp',
      'unit/detection/file_type_graceful_fallback_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Plugins tests (Catch2 - migrated from GTest)
  plugins_catch2_exe = executable('plugins_catch2',
    files(
      'unit/plugins/plugin_installer_test.cpp',
      'unit/plugins/plugin_repo_client_test.cpp',
      'unit/plugins/onnx_request_test.cpp',
      'unit/plugins/symbol_extractor_query_test.cpp',
      'unit/plugins/symbol_extractor_validation_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Integrity tests (Catch2 - migrated from GTest)
  integrity_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_crypto'),
    dependency('yams_integrity'),
    dependency('yams_storage_engine'),
    dependency('yams_reference_counter'),
  ]

  integrity_catch2_exe = executable('integrity_catch2',
    files(
      'unit/integrity/chunk_validator_test.cpp',
      'unit/integrity/integrity_verifier_simple_test.cpp',
      'unit/integrity/repair_manager_test.cpp',
      'unit/integrity/verification_monitor_test.cpp',
      'unit/integrity/verification_scheduler_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + integrity_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Content tests (depends on content, extraction, detection modules)
  content_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_content'),
    dependency('yams_extraction'),
    dependency('yams_detection'),
    dependency('sqlite3'),
  ]

  content_handlers_exe = executable('content_handlers',
    files('unit/content/content_handlers_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + content_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  html_extractor_robust_exe = executable('html_extractor_robust',
    files('unit/extraction/html_extractor_robust_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + content_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Text extraction to FTS5 pipeline test (detection -> extraction -> indexing -> search)
  text_extraction_fts5_pipeline_exe = executable('text_extraction_fts5_pipeline',
    files('unit/extraction/text_extraction_fts5_pipeline_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + content_test_deps + [dependency('yams_metadata')],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Plugin process tests (external plugin infrastructure)
  plugin_process_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_extraction'),
    dependency('nlohmann_json'),
  ]

  plugin_process_exe = executable('plugin_process',
    files('unit/extraction/plugin_process_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + plugin_process_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  plugin_process_minimal_exe = executable('plugin_process_minimal',
    files('unit/extraction/plugin_process_minimal_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + plugin_process_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  plugin_process_launcher_exe = executable('plugin_process_launcher',
    files('unit/extraction/plugin_process_launcher_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + plugin_process_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  jsonrpc_client_exe = executable('jsonrpc_client',
    files('unit/extraction/jsonrpc_client_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + plugin_process_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  external_plugin_extractor_exe = executable('external_plugin_extractor',
    files('unit/extraction/external_plugin_extractor_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + plugin_process_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Chunking tests (depends on chunking module)
  chunking_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_chunking'),
    dependency('yams_crypto'),
  ]

  chunking_exe = executable('chunking',
    files('unit/chunking/chunking_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + chunking_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Crypto tests (depends on crypto module)
  crypto_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_crypto'),
  ]

  crypto_exe = executable('crypto',
    files('unit/crypto/crypto_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + crypto_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  grep_service_exe = executable('grep_service',
    files('unit/app/services/grep_service_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  indexing_service_exe = executable('indexing_service',
    files('unit/app/services/indexing_service_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  document_service_exe = executable('document_service',
    files('unit/app/services/document_service_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  service_utilities_exe = executable('service_utilities',
    files('unit/app/services/service_utilities_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  search_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_metadata'),
    dependency('yams_app_services'),
    dependency('yams_api'),
    boost_dep,
  ]

  # Re-enabled with mock-based tests disabled, placeholder integration test added
  # TODO PBI-071: Implement full integration tests with real components
  hybrid_search_exe = executable('hybrid_search',
    files('unit/search/hybrid_search_comprehensive_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + search_test_deps + [
      dependency('yams_daemon'),
      dependency('yams_daemon_client'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Corpus Stats Search Integration Tests (Catch2)
  # Phase 1 of Epic yams-7ez4: Validates corpus stats collection from real document store
  # --------------------------------------------------------------------------
  corpus_stats_search_integration_exe = executable('corpus_stats_search_integration',
    files('unit/search/corpus_stats_search_integration_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + search_test_deps + [
      dependency('yams_daemon'),
      dependency('yams_daemon_client'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Reranker Integration Tests (Catch2)
  # Tests IReranker interface, ModelProviderRerankerAdapter, and reranker config
  reranker_integration_exe = executable('reranker_integration',
    files('unit/search/reranker_integration_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + search_test_deps + [
      dependency('yams_daemon'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Ghidra plugin integration test (PBI-075)
  # Tests plugin spawning and JSON-RPC communication without requiring Ghidra installed
  if host_machine.system() != 'windows'
    ghidra_plugin_exe = executable('ghidra_plugin_communication',
      files('integration/ghidra_plugin_communication_test.cpp'),
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep, dependency('nlohmann_json'), dependency('spdlog')],
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )
  endif

  # Test Registrations
  # --------------------------------------------------------------------------

  # Metadata tests
  test('metadata_retrieval_schema', catch2_metadata_retrieval_exe,
    suite: ['catch2', 'unit', 'metadata', 'retrieval'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # Removed: metadata_search test - HybridSearchEngine specific

  test('metadata_symbol_schema', catch2_metadata_symbol_exe,
    suite: ['catch2', 'unit', 'metadata', 'symbol'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('metadata_migration_validation', catch2_metadata_migration_exe,
    suite: ['catch2', 'unit', 'metadata', 'migration'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('metadata_unit', catch2_metadata_unit_exe,
    suite: ['catch2', 'unit', 'metadata', 'yams-metadata'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('connection_pool', catch2_connection_pool_exe,
    suite: ['catch2', 'unit', 'metadata', 'connection_pool'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('kg_store', catch2_kg_store_exe,
    suite: ['catch2', 'unit', 'metadata', 'kg'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('database', catch2_database_exe,
    suite: ['catch2', 'unit', 'metadata', 'database'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('fts5', catch2_fts5_exe,
    suite: ['catch2', 'unit', 'metadata', 'fts5'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('tree_builder', catch2_tree_builder_exe,
    suite: ['catch2', 'unit', 'metadata', 'tree_builder'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('tree_differ', catch2_tree_differ_exe,
    suite: ['catch2', 'unit', 'metadata', 'tree_differ'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('path_tree_delete', catch2_path_tree_delete_exe,
    suite: ['catch2', 'unit', 'metadata', 'path_tree'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('session_isolation', catch2_session_isolation_exe,
    suite: ['catch2', 'unit', 'metadata', 'session'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('metadata_repository', catch2_metadata_repository_exe,
    suite: ['catch2', 'unit', 'metadata', 'repository'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  test('metadata_repository_cache', catch2_metadata_repository_cache_exe,
    suite: ['catch2', 'unit', 'metadata', 'cache'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('fuzzy_index_prioritization', catch2_fuzzy_index_prioritization_exe,
    suite: ['catch2', 'unit', 'metadata', 'fuzzy'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('kg_store_alias_entities', catch2_kg_store_alias_entities_exe,
    suite: ['catch2', 'unit', 'metadata', 'kg'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('fts_queryspec_integration', catch2_fts_queryspec_integration_exe,
    suite: ['catch2', 'unit', 'metadata', 'fts'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('tags_queryspec_integration', catch2_tags_queryspec_integration_exe,
    suite: ['catch2', 'unit', 'metadata', 'tags'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('query_helpers_integration', catch2_query_helpers_integration_exe,
    suite: ['catch2', 'unit', 'metadata', 'query_helpers'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('metadata_schema', catch2_metadata_schema_exe,
    suite: ['catch2', 'unit', 'metadata', 'schema'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300  # Comprehensive tests including performance metrics
  )

  test('fts5_failure_scenarios', catch2_fts5_failure_scenarios_exe,
    suite: ['catch2', 'unit', 'metadata', 'fts5', 'failure'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180  # Includes concurrent and large content tests
  )

  test('corpus_stats', catch2_corpus_stats_exe,
    suite: ['catch2', 'unit', 'metadata', 'corpus_stats'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('repository_foundation', catch2_repository_foundation_exe,
    suite: ['catch2', 'unit', 'metadata', 'repository', 'foundation'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # Core tests
  test('core_cpp23_features', core_cpp23_features_exe,
    suite: ['catch2', 'unit', 'core', 'cpp23'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('core_magic_numbers', core_magic_numbers_exe,
    suite: ['catch2', 'unit', 'core', 'magic'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('core_uuid', core_uuid_exe,
    suite: ['catch2', 'unit', 'core', 'uuid'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('detection', detection_exe,
    suite: ['catch2', 'unit', 'detection'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('plugins_catch2', plugins_catch2_exe,
    suite: ['catch2', 'unit', 'plugins'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('integrity_catch2', integrity_catch2_exe,
    suite: ['catch2', 'unit', 'integrity'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  # Content tests
  test('content_handlers', content_handlers_exe,
    suite: ['catch2', 'unit', 'content', 'handlers'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('html_extractor_robust', html_extractor_robust_exe,
    suite: ['catch2', 'unit', 'extraction', 'html'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120  # Allow longer timeout for large file tests
  )

  test('text_extraction_fts5_pipeline', text_extraction_fts5_pipeline_exe,
    suite: ['catch2', 'unit', 'extraction', 'pipeline', 'fts5'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('plugin_process', plugin_process_exe,
    suite: ['catch2', 'unit', 'extraction', 'plugin'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('plugin_process_minimal', plugin_process_minimal_exe,
    suite: ['catch2', 'unit', 'extraction', 'plugin', 'debug'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('plugin_process_launcher', plugin_process_launcher_exe,
    suite: ['catch2', 'unit', 'extraction', 'plugin', 'launcher'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path, 'YAMS_SOURCE_DIR': meson.project_source_root() },
    timeout: host_machine.system() == 'windows' ? 180 : 120,
    # PyInstaller process tests have resource contention when run in parallel
    is_parallel: false
  )

  test('jsonrpc_client', jsonrpc_client_exe,
    suite: ['catch2', 'unit', 'extraction', 'jsonrpc'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('external_plugin_extractor', external_plugin_extractor_exe,
    suite: ['catch2', 'unit', 'extraction', 'plugin'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # Chunking tests
  test('chunking', chunking_exe,
    suite: ['catch2', 'unit', 'chunking'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # Crypto tests
  test('crypto', crypto_exe,
    suite: ['catch2', 'unit', 'crypto'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # Service tests
  test('grep_service', grep_service_exe,
    suite: ['catch2', 'unit', 'services', 'grep'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('indexing_service', indexing_service_exe,
    suite: ['catch2', 'unit', 'services', 'indexing'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  test('document_service', document_service_exe,
    suite: ['catch2', 'unit', 'services', 'document'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  test('service_utilities', service_utilities_exe,
    suite: ['catch2', 'unit', 'services', 'utilities'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  # Search tests
  test('hybrid_search', hybrid_search_exe,
    suite: ['catch2', 'unit', 'search', 'hybrid'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_DISABLE_VECTORS': '1',
    },
    timeout: 120
  )

  test('corpus_stats_search_integration', corpus_stats_search_integration_exe,
    suite: ['catch2', 'integration', 'search', 'corpus_stats'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_DISABLE_VECTORS': '1',
    },
    timeout: 180
  )

  test('reranker_integration', reranker_integration_exe,
    suite: ['catch2', 'unit', 'search', 'reranker'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # Plugin integration tests
  if host_machine.system() != 'windows'
    test('ghidra_plugin_communication', ghidra_plugin_exe,
      suite: ['catch2', 'integration', 'plugin', 'ghidra'],
      env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path, 'YAMS_SOURCE_DIR': meson.project_source_root() },
      timeout: 30,
      is_parallel: false
    )
  endif

  # Daemon tests
  test('daemon_fsm_suite', catch2_daemon_fsm_exe,
    suite: ['catch2', 'unit', 'daemon', 'fsm'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('daemon_metrics_status', catch2_daemon_metrics_exe,
    suite: ['catch2', 'unit', 'daemon', 'metrics', 'status'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('daemon_components', catch2_daemon_components_exe,
    suite: ['catch2', 'unit', 'daemon', 'components'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('repair_service', catch2_repair_service_exe,
    suite: ['catch2', 'unit', 'daemon', 'repair', 'stuck-docs'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_SQLITE_VEC_SKIP_INIT': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'YAMS_DISABLE_VECTOR_DB': '1',
      'YAMS_SKIP_MODEL_LOADING': '1',
      'YAMS_TESTING': '1',
    },
    timeout: 120,
    is_parallel: false
  )

  test('daemon_protocol', catch2_daemon_protocol_exe,
    suite: ['catch2', 'unit', 'daemon', 'protocol'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('daemon_background_processing', catch2_daemon_background_exe,
    suite: ['catch2', 'unit', 'daemon', 'background'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('daemon_graph_component', catch2_daemon_graph_component_exe,
    suite: ['catch2', 'unit', 'daemon', 'graph', 'pbi-009'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('daemon_component_submodule', catch2_daemon_component_submodule_exe,
    suite: ['catch2', 'unit', 'daemon', 'components', 'yams-zns'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('gradient_limiter', catch2_gradient_limiter_exe,
    suite: ['catch2', 'unit', 'daemon', 'gradient'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('work_coordinator', catch2_work_coordinator_exe,
    suite: ['catch2', 'unit', 'daemon', 'work-coordinator'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('post_ingest_queue_unit', catch2_post_ingest_queue_unit_exe,
    suite: ['catch2', 'unit', 'daemon', 'post-ingest'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('daemon_plugin_harness', catch2_daemon_plugin_exe,
    suite: ['catch2', 'unit', 'daemon', 'plugin'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  # Reranker session test - uses mock mode by default, enabling quick CI runs
  test('daemon_reranker_session', catch2_daemon_reranker_exe,
    suite: ['catch2', 'unit', 'daemon', 'reranker'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_TEST_MODE': '1',  # Enable mock mode for CI
    },
    timeout: 60,
    is_parallel: true
  )

  if onnx_dep.found()
    test('daemon_onnx_provider_config', catch2_daemon_onnx_provider_exe,
      suite: ['catch2', 'unit', 'daemon', 'onnx'],
      env: {
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
        'YAMS_TEST_MODE': '1',
      },
      timeout: 60,
      is_parallel: true
    )

    test('gpu_provider_coreml', catch2_gpu_provider_exe,
      suite: ['catch2', 'unit', 'daemon', 'gpu'],
      env: {
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
      },
      timeout: 30,
      is_parallel: true
    )
  endif

  test('plugin_infrastructure', catch2_plugin_infrastructure_exe,
    suite: ['catch2', 'unit', 'daemon', 'plugin', 'infrastructure'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('external_plugin_host', catch2_external_plugin_host_exe,
    suite: ['catch2', 'unit', 'daemon', 'plugin', 'external'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('extractor_adapter', catch2_extractor_adapter_exe,
    suite: ['catch2', 'unit', 'daemon', 'extractor'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # test('repair_coordinator', catch2_repair_coordinator_exe,
  #   suite: ['catch2', 'unit', 'daemon', 'repair', 'yams-3s4'],
  #   env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
  #   timeout: 60,
  #   is_parallel: false
  # )

  test('repair_scheduling_adapter', catch2_repair_scheduling_exe,
    suite: ['catch2', 'unit', 'daemon', 'repair', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('service_manager', catch2_service_manager_exe,
    suite: ['catch2', 'unit', 'daemon', 'service_manager', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('streaming_processor', catch2_streaming_processor_exe,
    suite: ['catch2', 'unit', 'daemon', 'streaming', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('multiplex_client', catch2_multiplex_client_exe,
    suite: ['catch2', 'unit', 'daemon', 'multiplex', 'yams-3s4'],
    args: ['--allow-running-no-tests'],  # All tests have [.requires_daemon] tag
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('entity_graph_service', catch2_entity_graph_service_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  # These tests use ServiceManager/WorkCoordinator which have resource contention
  # when run in parallel with other daemon tests - run sequentially
  test('service_manager_shutdown', catch2_service_manager_shutdown_exe,
    suite: ['catch2', 'unit', 'daemon', 'service_manager', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('service_manager_degraded', catch2_service_manager_degraded_exe,
    suite: ['catch2', 'unit', 'daemon', 'service_manager', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('repair_binary_filter', catch2_repair_binary_filter_exe,
    suite: ['catch2', 'unit', 'daemon', 'repair', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('abi_model_provider_dim', catch2_abi_model_provider_dim_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('embedding_recovery', catch2_embedding_recovery_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('request_io_edge', catch2_request_io_edge_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    args: ['--allow-running-no-tests'],  # Placeholder tests - all skipped
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('daemon_warm_bench', catch2_daemon_warm_bench_exe,
    suite: ['catch2', 'unit', 'daemon', 'bench', 'yams-3s4'],
    args: ['--allow-running-no-tests'],  # Bench only runs when YAMS_BENCH_FAST=1
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('request_lifetime', catch2_request_lifetime_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    args: ['--allow-running-no-tests'],  # Skips when streaming disabled
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('writer_drain_cleanup', catch2_writer_drain_cleanup_exe,
    suite: ['catch2', 'unit', 'daemon', 'ipc', 'mux'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('asio_connection_pool', catch2_asio_connection_pool_exe,
    suite: ['catch2', 'unit', 'daemon', 'connection-pool'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false,
    # Test skips on Windows (Unix domain sockets not supported)
    should_fail: host_machine.system() == 'windows'
  )

  test('daemon', catch2_daemon_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      # Disable ABI plugins to avoid TSAN+ONNX memory pressure causing SIGKILL
      'YAMS_DISABLE_ABI_PLUGINS': '1',
    },
    timeout: 120,
    # Test skips on Windows (Unix domain sockets not supported)
    should_fail: host_machine.system() == 'windows'
  )

  test('tune_advisor_threshold', catch2_tune_advisor_threshold_exe,
    suite: ['catch2', 'unit', 'daemon', 'tune', 'advisor'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('tuning_allocation', catch2_tuning_allocation_exe,
    suite: ['catch2', 'unit', 'daemon', 'tune', 'allocation'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('tuning_reconciliation', catch2_tuning_reconciliation_exe,
    suite: ['catch2', 'unit', 'daemon', 'tune', 'reconciliation'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('post_ingest_dynamic_cap_recovery', catch2_post_ingest_dynamic_cap_recovery_exe,
    suite: ['catch2', 'unit', 'daemon', 'tune', 'post-ingest'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('resource_governance', catch2_resource_governance_exe,
    suite: ['catch2', 'unit', 'daemon', 'governance'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('timing_audit', catch2_timing_audit_exe,
    suite: ['catch2', 'unit', 'daemon', 'timing-audit'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  # NOTE: onnx_model_pool test registration skipped - see comment above
  # if onnx_dep.found()
  #   test('onnx_model_pool', catch2_onnx_model_pool_exe,
  #     suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
  #     env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
  #     timeout: 60
  #   )
  # endif

  test('cli_init_command', catch2_cli_init_command_exe,
    suite: ['catch2', 'unit', 'cli', 'init'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false
  )

  test('storage_backend', catch2_storage_backend_exe,
    suite: ['catch2', 'unit', 'storage', 'backend'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('storage_submodule', catch2_storage_submodule_exe,
    suite: ['catch2', 'unit', 'storage', 'yams-8b9'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300
  )

  test('wal_entry', catch2_wal_entry_exe,
    suite: ['catch2', 'unit', 'wal'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('search_submodule', catch2_search_submodule_exe,
    suite: ['catch2', 'unit', 'search', 'yams-1yq'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # SearchTuner FSM tests (Phase 2 of Epic yams-7ez4)
  test('search_tuner', catch2_search_tuner_exe,
    suite: ['catch2', 'unit', 'search', 'tuner', 'yams-7ez4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  # InternalBenchmark tests (Phase 3 of Epic yams-7ez4)
  test('internal_benchmark', catch2_internal_benchmark_exe,
    suite: ['catch2', 'unit', 'search', 'benchmark', 'yams-7ez4'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('compression_submodule', catch2_compression_submodule_exe,
    suite: ['catch2', 'unit', 'compression', 'yams-pmf'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300
  )

  test('mcp_submodule', catch2_mcp_submodule_exe,
    suite: ['catch2', 'unit', 'mcp', 'yams-84g'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # CLI submodule tests - all tests pass but Windows has a cleanup crash (0xC0000005)
  # during process exit due to global destructor issues with daemon client state.
  # Using should_fail: true until the cleanup issue is fixed.
  test('cli_submodule', catch2_cli_submodule_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300,  # Tests take ~200s
    should_fail: host_machine.system() == 'windows'
  )

  # Vector test suite (requires enable-vector-tests)
  # Migrated from GTest as part of yams-3s4 / yams-1d4
  if get_option('enable-vector-tests')
    test('vector_submodule', catch2_vector_submodule_exe,
      suite: ['catch2', 'unit', 'vector', 'yams-1d4'],
      env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
      timeout: 300
    )
  endif

  # API submodule tests (content store, metadata API)
  # Migrated from GTest as part of yams-3s4 / api
  test('api_submodule', catch2_api_submodule_exe,
    suite: ['catch2', 'unit', 'api', 'yams-api'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # Downloader submodule tests (download manager, resume, CAS)
  # Migrated from GTest as part of yams-3s4 / downloader
  test('downloader_submodule', catch2_downloader_submodule_exe,
    suite: ['catch2', 'unit', 'downloader', 'yams-downloader'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # Resolve submodule tests (resolver, backends)
  # Migrated from GTest as part of yams-3s4 / resolve
  test('resolve_submodule', catch2_resolve_submodule_exe,
    suite: ['catch2', 'unit', 'resolve', 'yams-resolve'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # Manifest submodule tests (manifest manager, serialization, reconstruction)
  # Migrated from GTest as part of yams-3s4 / yams-1jm
  test('manifest_submodule', catch2_manifest_submodule_exe,
    suite: ['catch2', 'unit', 'manifest', 'yams-manifest'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # Repair submodule tests (embedding repair scan)
  # Migrated from GTest as part of yams-3s4 / yams-1jm
  # Note: --allow-running-no-tests needed because tests skip when YAMS_DISABLE_VECTORS=1
  test('repair_submodule', catch2_repair_submodule_exe,
    suite: ['catch2', 'unit', 'repair', 'yams-repair'],
    args: ['--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  # Versioning submodule tests (document versioning, indexer)
  # Migrated from GTest as part of yams-3s4 / yams-1jm
  test('versioning_submodule', catch2_versioning_submodule_exe,
    suite: ['catch2', 'unit', 'versioning', 'yams-versioning'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )
endif
