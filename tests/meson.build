#!Meson configure - time test discovery without fs.glob / subdir_glob

#GTest / GMock migration complete(PBI - 068) : all tests now use Catch2.

#Catch2 dependency for modern test framework(PBI - 050)
#Prefer system installs, but fall back to Meson wrap subproject when available.
catch2_dep = dependency('catch2', required: false, fallback: ['catch2', 'catch2_dep'])

#Get Catch2 partial dependency with includes for test builds
catch2_compile_dep = []
if catch2_dep.found()
  catch2_compile_dep = [catch2_dep.partial_dependency(compile_args: true, includes: true)]
endif

#Trailing link deps for Catch2 tests - ensure spdlog / fmt symbols are available
#after Catch2 libs(static link order matters : users before providers)
catch2_trailing_link_deps = [
  dependency('spdlog'),
  dependency('fmt', required: false),
]

catch2_incs = []

#TSAN suppressions file for tests(if TSAN is enabled)
#Must be defined early before subdirs are included
_tsan_suppressions_file = meson.project_source_root() / 'tsan.suppressions'
_test_tsan_options = 'suppressions=' + _tsan_suppressions_file
_test_thread_sanitizer = yams_sanitize_thread
_test_address_sanitizer = yams_sanitize_address
_test_debug_integrations = get_option('buildtype') == 'debug' and not _test_address_sanitizer
_test_timeout_scale = _test_thread_sanitizer ? 10 : get_option('test-timeout-scale')
_test_timeout_cpp_arg = '-DYAMS_TEST_TIMEOUT_SCALE=' + _test_timeout_scale.to_string()

fs = import('fs')

# Sanitized shared libraries must not introduce a different compiler's runtime.
# Inspect the built artifact without loading it, so mixed-runtime startup crashes
# are reported as a precise linkage assertion rather than hiding the regression.
if yams_sanitize_thread and host_machine.system() == 'linux' and is_variable('yams_onnx_resource_lib')
    sanitizer_compiler = meson.get_compiler('cpp').get_id()
    if sanitizer_compiler in ['clang', 'gcc']
        sanitizer_readelf = find_program('readelf', required: true)
        test('onnx_resource_tsan_linkage',
            python_exe,
            args: [
                files('scripts/check_sanitizer_runtime_linkage.py'),
                '--library', yams_onnx_resource_lib.full_path(),
                '--compiler', sanitizer_compiler,
                '--readelf', sanitizer_readelf.full_path(),
            ],
            depends: [yams_onnx_resource_lib],
            suite: ['unit', 'build', 'sanitizer'],
            timeout: 30,
        )
    endif
endif

raw_environment_policy_script = files('scripts/check_raw_environment_calls.py')
raw_environment_policy_tests = files('scripts/test_check_raw_environment_calls.py')
raw_environment_allowlist = files('scripts/raw_environment_allowlist.txt')
benchmark_environment_policy_script = files('scripts/check_benchmark_environment_leaks.py')
benchmark_environment_policy_tests = files('scripts/test_check_benchmark_environment_leaks.py')
configuration_authority_policy_script = files('scripts/check_configuration_authority.py')
configuration_authority_policy_tests = files('scripts/test_check_configuration_authority.py')
production_environment_keys = files('scripts/production_environment_keys.txt')
configuration_reader_allowlist = files('scripts/configuration_reader_allowlist.txt')
retrieval_contract_tests = files('benchmarks/test_retrieval_contract.py')
config_key_usage_report_tests = files('scripts/test_report_config_key_usage.py')
portability_policy_script = files('scripts/check_portability.py')
portability_policy_tests = files('scripts/test_check_portability.py')
portability_allowlist = files('scripts/portability_allowlist.txt')
tracy_zone_policy_script = files('scripts/check_tracy_zones.py')
tracy_zone_policy_tests = files('scripts/test_check_tracy_zones.py')
repository_metadata_policy_script = files('scripts/check_repository_metadata.py')
forgejo_mirror_policy_script = files('scripts/check_forgejo_mirror_workflow.py')
experimental_release_source_policy_script = files('scripts/check_experimental_release_source.py')
release_manifest_unit_tests = files('scripts/test_generate_release_manifest.py')
repository_publication_unit_tests = files('scripts/test_publish_repository.py')
test('release_manifest_unit',
  python_exe,
  args: [release_manifest_unit_tests],
  suite: ['unit', 'release'],
  timeout: 30,
)
test('repository_publication_unit',
  python_exe,
  args: [repository_publication_unit_tests],
  suite: ['unit', 'policy', 'release', 'repository'],
  timeout: 30,
)
test('experimental_release_source_policy',
  python_exe,
  args: [experimental_release_source_policy_script, '--root', meson.project_source_root()],
  suite: ['unit', 'policy', 'release'],
  timeout: 30,
)
test('forgejo_mirror_policy',
  python_exe,
  args: [forgejo_mirror_policy_script, '--root', meson.project_source_root()],
  suite: ['unit', 'policy', 'repository'],
  timeout: 30,
)
test('serve_runtime_environment_unit',
  python_exe,
  args: [files('scripts/test_serve_runtime_environment.py')],
  suite: ['unit', 'cli', 'mcp'],
  timeout: 30,
)
if is_variable('yams_cli_exe') and get_option('build-mcp-server') and host_machine.system() != 'windows'
  serve_delegation_args = [files('scripts/test_serve_delegation.py'), '--cli', yams_cli_exe.full_path()]
  serve_delegation_deps = [yams_cli_exe]
  if is_variable('yams_onnx_resource_lib')
    # The fixture relocates the executable, not its build-tree shared libraries.
    serve_delegation_args += ['--runtime-library-dir', fs.parent(yams_onnx_resource_lib.full_path())]
    serve_delegation_deps += [yams_onnx_resource_lib]
  endif
  test('serve_delegation',
    python_exe,
    args: serve_delegation_args,
    depends: serve_delegation_deps,
    suite: ['integration', 'cli', 'mcp'],
    timeout: 90,
  )
endif

test('repository_metadata_scan_unit',
  python_exe,
  args: [files('scripts/test_repository_metadata_scan.py')],
  suite: ['unit', 'policy', 'repository'],
  timeout: 30,
)
test('repository_metadata_policy',
  python_exe,
  args: [repository_metadata_policy_script, '--root', meson.project_source_root()],
  suite: ['unit', 'policy', 'repository'],
  timeout: 30,
)
test('raw_environment_policy_unit',
  python_exe,
  args: [raw_environment_policy_tests],
  suite: ['unit', 'policy', 'environment'],
  timeout: 30,
)
test('raw_environment_policy',
  python_exe,
  args: [
    raw_environment_policy_script,
    '--root', meson.project_source_root(),
    '--allowlist', raw_environment_allowlist,
  ],
  suite: ['unit', 'policy', 'environment'],
  timeout: 30,
)
test('benchmark_environment_policy_unit',
  python_exe,
  args: [benchmark_environment_policy_tests],
  suite: ['unit', 'policy', 'environment'],
  timeout: 30,
)
test('benchmark_environment_policy',
  python_exe,
  args: [benchmark_environment_policy_script, '--root', meson.project_source_root()],
  suite: ['unit', 'policy', 'environment'],
  timeout: 30,
)
test('configuration_authority_policy_unit',
  python_exe,
  args: [configuration_authority_policy_tests],
  suite: ['unit', 'policy', 'configuration'],
  timeout: 30,
)
test('configuration_authority_policy',
  python_exe,
  args: [
    configuration_authority_policy_script,
    '--root', meson.project_source_root(),
    '--environment-allowlist', production_environment_keys,
    '--reader-allowlist', configuration_reader_allowlist,
  ],
  suite: ['unit', 'policy', 'configuration'],
  timeout: 30,
)
test('config_key_usage_report_unit',
  python_exe,
  args: [config_key_usage_report_tests],
  suite: ['unit', 'policy', 'configuration'],
  timeout: 30,
)
test('retrieval_contract_unit',
  python_exe,
  args: [retrieval_contract_tests],
  workdir: meson.project_source_root() / 'tests' / 'benchmarks',
  suite: ['unit', 'policy', 'benchmark'],
  timeout: 30,
)
test('meson_failure_reporter_unit',
  python_exe,
  args: files('scripts/test_report_meson_failures.py'),
  suite: ['unit', 'ci'],
  timeout: 30,
)
test('portability_policy_unit',
  python_exe,
  args: [portability_policy_tests],
  suite: ['unit', 'policy', 'portability'],
  timeout: 30,
)
test('portability_policy',
  python_exe,
  args: [
    portability_policy_script,
    '--root', meson.project_source_root(),
    '--allowlist', portability_allowlist,
  ],
  suite: ['unit', 'policy', 'portability'],
  timeout: 30,
)
test('tracy_zone_policy_unit',
  python_exe,
  args: [tracy_zone_policy_tests],
  suite: ['unit', 'policy', 'profiling'],
  timeout: 30,
)
test('tracy_zone_policy',
  python_exe,
  args: [tracy_zone_policy_script, '--root', meson.project_source_root()],
  suite: ['unit', 'policy', 'profiling'],
  timeout: 30,
)

release_candidate_unit_tests = files('scripts/test_validate_release_candidate.py')
test('release_candidate_unit',
  python_exe,
  args: [release_candidate_unit_tests],
  suite: ['unit', 'policy', 'release'],
  timeout: 30,
)

git_environment_isolation_tests = files('scripts/test_git_environment_isolation.py')
test('git_environment_isolation_unit',
  python_exe,
  args: [git_environment_isolation_tests],
  suite: ['unit', 'policy', 'release'],
  timeout: 60,
)

promotion_reconciliation_tests = files('scripts/test_reconcile_promotion_pr.py')
test('promotion_reconciliation',
  python_exe,
  args: [promotion_reconciliation_tests],
  suite: ['unit', 'policy', 'release'],
  timeout: 30,
)

cli11_dep = cli11_resolved_dep
if not cli11_dep.found()
  cli11_dep = declare_dependency()
endif

#Common dependencies for tests; mirror CLI breadth to cover includes
test_deps = [
  dependency('spdlog'),
  dependency('nlohmann_json'),
  dependency('sqlite3'),
  dependency('OpenSSL'),

  cli11_dep,
  boost_dep,
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
  dependency('yams_topology'),
  dependency('yams_extraction'),
  dependency('yams_detection'),
  dependency('yams_content'),
  dependency('yams_downloader'),
  dependency('yams_vector'),
  dependency('yams_search'),
  dependency('yams_api'),
  dependency('yams_repair'),
  dependency('yams_app_services'),
  dependency('yams_daemon_client'),
  dependency('yams_daemon'),
]

# YAMS_TESTING build of TuneAdvisor's bodies must precede the production archive on every test
# link line (see src/daemon/tune_advisor/meson.build). Order is load-bearing: meson emits
# dependencies in list order, and the first archive that defines a symbol wins. Any executable
# compiled with -DYAMS_TESTING that does not use test_deps must list
# yams_tune_advisor_testing first itself.
test_deps = [dependency('yams_tune_advisor_testing')] + test_deps

yams_plugins_mgmt_dep = dependency('yams_plugins_mgmt', required: false)
if yams_plugins_mgmt_dep.found()
  test_deps += yams_plugins_mgmt_dep
endif

yams_mcp_dep = dependency('yams_mcp', required: false)
if yams_mcp_dep.found()
  test_deps += yams_mcp_dep
endif

onnxruntime_dep_opt = onnxruntime_resolved_dep
if onnxruntime_dep_opt.found()
  test_deps += onnxruntime_dep_opt
endif

#Windows : stage runtime DLLs next to every test executable(mirrors Windows install).
#Placing DLLs alongside the test binaries prevents the loader from falling back to
#older System32 copies(e.g., onnxruntime.dll API 17) and keeps tests aligned with
#the Conan - shipped runtime used for installation.
if host_machine.system() == 'windows' and windows_runtime_dlls.length() > 0
  py_mod = import('python')
  python = py_mod.find_installation('python3', required: false)
  if not python.found()
    python = py_mod.find_installation('python', required: false)
  endif

  if python.found()
#All test binary directories(relative to builddir / tests)
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
  else
    message('Python not found (python3/python); skipping Windows test runtime DLL staging')
  endif
endif

# Snapshot non-CLI dependencies for component-only tests before the broad CLI lane.
embedding_service_test_deps = test_deps

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
#If build - cli is disabled, still try to find yams_cli but don't error if missing
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
  generated_inc,                         # builddir/version_generated.h
] + catch2_incs

#Keep project strict, but don’t fail unit tests on common test - only warnings.
#We downgrade selective warnings to non - errors for test targets only.
cxx = meson.get_compiler('cpp')
test_cpp_noerror_args = cxx.get_supported_arguments([
  '-Wno-error=sign-compare',
  '-Wno-error=unused-variable',
  '-Wno-error=unused-but-set-variable',
  '-Wno-error=unused-result',
  '-Wno-error=unused-lambda-capture',
  '-Wno-error=missing-field-initializers',
])

catch2_main_dep = disabler()
if catch2_dep.found()
  catch2_yams_main_lib = static_library('catch2_yams_main',
    files('support/catch2_main.cpp'),
    dependencies: [catch2_dep],
    cpp_args: test_cpp_noerror_args,
    install: false,
  )
  catch2_main_dep = declare_dependency(
    link_with: catch2_yams_main_lib,
    dependencies: [catch2_dep],
  )
endif

#Meson - native test registration via structured subdirs(no shell globs).
#Unit tests are now individual Catch2 executables; tests / unit keeps only a
#compatibility stub for older yams_unit_objs references.
subdir('unit')

if get_option('build-plugins') and catch2_dep.found()
  subdir('plugins')
endif

if get_option('enable-integration-tests') or _test_debug_integrations
  subdir('integration/smoke')
  subdir('integration/daemon')
  subdir('integration/cli')
  subdir('integration/search')
endif
if get_option('enable-mobile-tests')
  subdir('mobile')
endif

stress_srcs = files(
  'stress/storage/concurrent_catch2_test.cpp',
  'stress/mcp_socket_stress_catch2_test.cpp',
)

#Prefer Conan CMake config before falling back to system discovery
onnx_dep = onnxruntime_resolved_dep
tbb_dep = tbb_resolved_dep

#Add TBB to test_deps if found(needed by yams_vector via ONNX runtime)
if tbb_dep.found()
  test_deps += tbb_dep
endif

#Collect library directories for test runtime.
#
#Docker note : our Linux Docker build uses a BuildKit cache mount for / root /.conan2,
#which is not committed into the image layers.We therefore rely on Conan's
#runtime_deploy output in the build directory for running tests in later stages.
#Always include the build root in RPATH / LD_LIBRARY_PATH so tests can find
#deployed runtime libraries(e.g.ONNX, TBB) even when the Conan cache is absent.
_test_rpaths = [meson.project_build_root()]
_test_ld_library_path = [meson.project_build_root()]
if host_machine.system() == 'linux'
#Docker test stages install Conan - provided runtimes under / opt / yams / usr / local / lib
#before invoking meson test, so include that path in the test environment.
  _test_ld_library_path += ['/opt/yams/usr/local/lib']
endif
foreach d : [onnx_dep, tbb_dep]
  if d.found()
    libdir = d.get_variable(pkgconfig: 'libdir', default_value: '')
    if libdir != ''
      _test_rpaths += libdir
      _test_ld_library_path += libdir
    endif
  endif
endforeach

#Prepare LD_LIBRARY_PATH for test environment(needed for transitive TBB dependencies)
_test_env_ld_library_path = ':'.join(_test_ld_library_path)

#Bench timeout is referenced by standalone Catch2 benchmark targets below.
bench_timeout = get_option('bench-timeout')



## integration_smoke now defined in subdir('integration/smoke')

#Next integration group : Services(daemon ingestion / retrieval)
#Compile this group separately to avoid duplicate plugin symbols and to allow isolated serial runs.
#Prefer Meson - native subdirs over shell discovery for services integration tests.
#
#IMPORTANT : services integration includes both GTest - and Catch2 - based suites.
#Do not gate the entire subdir on GTest.
if get_option('enable-integration-tests') or _test_debug_integrations
  subdir('integration/services')
endif

#Benchmarks(standalone executables).Register bench test target only when enabled.
if get_option('build-benchmarks')
  subdir('benchmarks')

#Standalone, minimal daemon warm - latency bench(does not depend on unit object lib)
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
endif

#Stress tests can be heavy; register but mark as long - running via suite name
if catch2_dep.found() and get_option('enable-stress-tests') and stress_srcs.length() > 0
  stress_exe = executable('yams_stress_tests', stress_srcs,
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep, test_deps],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )
  test('stress', stress_exe,
    suite: 'stress',
    args: ['[mcpsocketstresstest]'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
    },
    timeout: 300)
elif get_option('enable-stress-tests')
  message('Catch2 not found; skipping stress test registration.')
endif

# Standard Catch2 tests: one row per test in catch2_test_table, built by the loop at the
# end of this file. Each row needs 'name', 'sources', 'deps' (added after the Catch2
# deps), 'suite' and 'timeout'. Optional: 'target' (default 'catch2_' + name), 'args'
# and 'is_parallel'. Tests with extra env, cpp_args or several registrations stay
# hand-written.
catch2_test_table = []

# ============================================================================
#Catch2 Test Executables(PBI - 068 : Separated for isolation)
#Each test suite gets its own executable to prevent resource conflicts
#and enable parallel execution.This resolves SIGABRT crashes from shared
#filesystem state and temp directory conflicts.
# ============================================================================

if catch2_dep.found()

  catch2_retrieval_json_exe = executable('catch2_retrieval_json',
    files('unit/cli/retrieval_json_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep, dependency('nlohmann_json')],
    cpp_args: test_cpp_noerror_args,
    install: false,
  )
  test('retrieval_json', catch2_retrieval_json_exe,
    suite: ['catch2', 'unit', 'cli'], timeout: 30)

  catch2_extraction_contracts_exe = executable('catch2_extraction_contracts',
    files('unit/daemon/extraction_response_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep, dependency('nlohmann_json')],
    cpp_args: test_cpp_noerror_args,
    install: false,
  )
  test('extraction_contracts', catch2_extraction_contracts_exe,
    suite: ['catch2', 'unit', 'daemon', 'extraction'], timeout: 30)

#Daemon Test Suites(6 consolidated executables)
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

#Daemon tests
  catch2_test_table += [{
    'name': 'daemon_fsm_suite',
    'target': 'catch2_daemon_fsm',
    'sources': files('unit/daemon/daemon_fsm_suite_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'fsm'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_daemon_metrics_status_exe = executable('catch2_daemon_metrics_status',
    files('unit/daemon/daemon_metrics_status_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'daemon_metrics_core',
    'sources': files('unit/daemon/daemon_metrics_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'metrics', 'core'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'daemon_components',
    'sources': files(
      'unit/daemon/daemon_components_test.cpp',
      'unit/daemon/systemd_user_unit_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'components'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'p2p_transport',
    'sources': files('unit/daemon/p2p_transport_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'p2p', 'network'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_p2p_protocol_exe = executable('catch2_p2p_protocol',
    files('unit/daemon/p2p_protocol_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_p2p_delta_exe = executable('catch2_p2p_delta',
    files('unit/daemon/p2p_delta_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'peer_registry',
    'sources': files('unit/daemon/peer_registry_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'p2p', 'sqlite'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'p2p_manager',
    'sources': files('unit/daemon/p2p_manager_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'p2p', 'network', 'sqlite'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'admission_policy',
    'sources': files('unit/daemon/admission_policy_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'admission-policy'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'grep_result_window',
    'sources': files(
      'unit/daemon/grep_result_window_catch2_test.cpp',
      '../src/daemon/components/grep_result_window.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'grep'],
    'timeout': 60,
  }]

  catch2_daemon_client_link_exe = executable('catch2_daemon_client_link',
    files('unit/daemon/daemon_client_link_catch2_test.cpp'),
    dependencies: [catch2_dep, catch2_main_dep, dependency('yams_daemon_client')],
    cpp_args: test_cpp_noerror_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )
  test('daemon_client_link', catch2_daemon_client_link_exe,
    suite: ['unit', 'daemon', 'sdk'],
    timeout: 30,
  )

  catch2_test_table += [{
    'name': 'search_admission_controller',
    'sources': files('unit/daemon/search_admission_controller_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'search-admission'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'ingest_metrics_publisher',
    'sources': files('unit/daemon/ingest_metrics_publisher_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'ingest-metrics'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'topology_manager',
    'sources': files('unit/daemon/topology_manager_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'topology-manager'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'embedding_lifecycle',
    'sources': files('unit/daemon/embedding_lifecycle_manager_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'embedding-lifecycle'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'daemon_protocol',
    'sources': files(
      'unit/daemon/daemon_protocol_test.cpp',
      'unit/daemon/frame_reader_catch2_test.cpp',
      'unit/daemon/dispatch_response_mapper_catch2_test.cpp',
      'unit/daemon/request_handler_admission_reuse_catch2_test.cpp',
      'unit/daemon/request_handler_repair_cancel_catch2_test.cpp',
      'unit/daemon/request_handler_status_unary_catch2_test.cpp',
      'unit/daemon/proto_serializer_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'protocol'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_daemon_background_exe = executable('catch2_daemon_background',
    files('unit/daemon/daemon_background_processing_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'daemon_graph_component',
    'sources': files('unit/daemon/graph_component_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'graph', 'pbi-009'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'graph_services',
    'sources': files(
      'unit/app/services/graph_query_service_catch2_test.cpp',
      'unit/app/services/graph_context_service_catch2_test.cpp',
      'unit/metadata/delete_cascade_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'graph'],
    'timeout': 60,
  }]

#Daemon component submodule tests(CheckpointManager, ConfigResolver, etc.)
#Migrated from GTest as part of yams - 3s4 / yams - zns
  catch2_test_table += [{
    'name': 'daemon_component_submodule',
    'sources': files(
      'unit/daemon/components/checkpoint_manager_test.cpp',
      'unit/daemon/components/config_resolver_test.cpp',
      'unit/daemon/components/database_manager_test.cpp',
      'unit/daemon/components/plugin_manager_test.cpp',
      'unit/daemon/plugin_manager_config_test.cpp',
      'unit/daemon/components/topology_tuner_catch2_test.cpp',
      'unit/daemon/components/vector_system_manager_test.cpp',
      'unit/search/topological_quality_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'components', 'yams-zns'],
    'timeout': 120,
  }]

# Keep embedding/bootstrap regressions independent of unrelated component bootstrap.
  catch2_test_table += [{
    'name': 'embedding_service_timing',
    'sources': files('unit/daemon/components/embedding_service_timing_catch2_test.cpp'),
    'deps': embedding_service_test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'embedding', 'startup'],
    'timeout': 60,
  }]

# Virtual-time startup contracts: no database, model, socket, CLI or live governor needed.
  catch2_test_table += [{
    'name': 'startup_replay',
    'sources': files(
      'unit/daemon/components/startup_replay_catch2_test.cpp',
      '../tools/fuzzing/fuzz_startup.cpp',
    ),
    'deps': [dependency('yams_tune_advisor_testing'), dependency('yams_daemon'), dependency('yams_core'), dependency('spdlog')],
    'suite': ['catch2', 'unit', 'daemon', 'startup', 'fuzz-smoke'],
    'timeout': 30,
  }]

#Gradient limiter tests(Netflix Gradient2 adaptive concurrency)
  catch2_test_table += [{
    'name': 'gradient_limiter',
    'sources': files('unit/daemon/components/gradient_limiter_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'gradient'],
    'timeout': 60,
  }]

#WorkCoordinator tests(async work coordination)
  catch2_work_coordinator_exe = executable('catch2_work_coordinator',
    files('unit/daemon/work_coordinator_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#PostIngestQueue unit tests(LruCache template + stage constants)
  catch2_test_table += [{
    'name': 'post_ingest_queue_unit',
    'sources': files('unit/daemon/components/post_ingest_queue_unit_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'post-ingest'],
    'timeout': 60,
  }]

#PressureLimitedPoller shutdown unit tests(capability - sleep / stop paths)
  catch2_test_table += [{
    'name': 'pressure_limited_poller',
    'sources': files('unit/daemon/components/pressure_limited_poller_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'poller'],
    'timeout': 30,
  }]

#Assertion validation tests(fork - based death tests for YAMS_ASSERT / YAMS_PRECONDITION)
  catch2_assert_val_exe = executable('catch2_assert_val',
    files('unit/core/assert_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#Metrics consistency tests(migrated from GTest)
  catch2_test_table += [{
    'name': 'metrics_consistency',
    'sources': files('unit/daemon/metrics_consistency_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'metrics'],
    'timeout': 60,
  }]

#SearchService integration tests(migrated from GTest)
  catch2_search_service_exe = executable('catch2_search_service',
    files(
      'unit/app/services/search_service_catch2_test.cpp',
      'unit/app/services/search_hash_edge_cases_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#FTS5 performance / stress tests(migrated from GTest, opt - in via YAMS_RUN_STRESS_TESTS = 1)
  catch2_test_table += [{
    'name': 'fts5_stress',
    'sources': files('unit/metadata/fts5_performance_stress_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'stress', 'metadata', 'fts5'],
    'args': ['--allow-running-no-tests'],
    'timeout': 300,
    'is_parallel': false,
  }]

#Add plugin path for plugin harness tests
  plugin_test_cpp_args = test_cpp_noerror_args + ['-DYAMS_TESTING=1']
  if get_option('plugin-onnx')
#Pass the expected plugin location to the test
    onnx_plugin_name = 'libyams_onnx_plugin.so'
    if host_machine.system() == 'darwin'
      onnx_plugin_name = 'libyams_onnx_plugin.dylib'
    elif host_machine.system() == 'windows'
      onnx_plugin_name = 'yams_onnx_plugin.dll'
    endif
    onnx_plugin_path = join_paths(meson.project_build_root(), 'plugins', 'onnx', onnx_plugin_name)
    plugin_test_cpp_args += ['-Dyams_onnx_plugin_BUILT="' + onnx_plugin_path + '"']
  endif

  # ONNX Runtime resolution policy (pure; no runtime needed): override >
  # configured > system > bundled > legacy, skipping incompatible API versions.
  # Fixtures use ELF/Mach-O library names; Windows resolution is the DLL search.
  if host_machine.system() != 'windows'
    catch2_ort_runtime_resolution_exe = executable('catch2_ort_runtime_resolution',
      files(
        'unit/plugins/ort_runtime_resolution_catch2_test.cpp',
        '../plugins/ort_runtime/ort_runtime_resolution.cpp',
      ),
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep],
      install: false,
    )
    test('ort_runtime_resolution', catch2_ort_runtime_resolution_exe,
      suite: ['catch2', 'unit', 'plugins', 'onnx'],
      timeout: 30,
    )
  endif

  catch2_daemon_plugin_exe = executable('catch2_daemon_plugin',
    files('unit/daemon/onnx_plugin_harness_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: plugin_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  if onnx_dep.found()
    onnx_provider_test_deps = [catch2_dep, catch2_main_dep] + test_deps + onnx_dep
    catch2_daemon_onnx_provider_exe = executable('catch2_daemon_onnx_provider',
      files(
        'unit/daemon/onnx_model_provider_config_test.cpp',
        '../plugins/onnx/model_provider.cpp',
        '../plugins/onnx/onnx_text_processing.cpp',
        '../plugins/ort_runtime/ort_runtime_loader.cpp',
        '../plugins/ort_runtime/ort_runtime_resolution.cpp',
        '../plugins/onnx/onnx_model_pool.cpp',
        '../plugins/onnx/onnx_colbert_session.cpp',
        '../src/vector/tokenizer.cpp',
      ),
      include_directories: incs,
      dependencies: onnx_provider_test_deps,
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )

    catch2_gpu_provider_exe = executable('catch2_gpu_provider',
      files(
        'unit/daemon/onnx_gpu_provider_catch2_test.cpp',
        '../plugins/ort_runtime/ort_runtime_loader.cpp',
        '../plugins/ort_runtime/ort_runtime_resolution.cpp',
      ),
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep] + test_deps + [onnx_dep],
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )

    catch2_onnx_embedding_diag_exe = executable('catch2_onnx_embedding_diag',
      files(
        'unit/daemon/onnx_embedding_diagnostic_test.cpp',
        '../plugins/onnx/model_provider.cpp',
        '../plugins/onnx/onnx_text_processing.cpp',
        '../plugins/ort_runtime/ort_runtime_loader.cpp',
        '../plugins/ort_runtime/ort_runtime_resolution.cpp',
        '../plugins/onnx/onnx_model_pool.cpp',
        '../plugins/onnx/onnx_colbert_session.cpp',
        '../src/vector/tokenizer.cpp',
      ),
      include_directories: incs,
      dependencies: onnx_provider_test_deps,
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )

    test('onnx_embedding_diagnostic', catch2_onnx_embedding_diag_exe,
      suite: ['catch2', 'unit', 'daemon', 'onnx', 'diagnostic'],
      env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
      args: ['--allow-running-no-tests'],
      timeout: 120,
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

  catch2_test_table += [{
    'name': 'extractor_adapter',
    'sources': files('unit/daemon/plugin_content_extractor_adapter_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'extractor'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'repair_scheduling_adapter',
    'target': 'catch2_repair_scheduling',
    'sources': files('unit/daemon/repair_scheduling_adapter_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'repair', 'yams-3s4'],
    'timeout': 120,
  }]

  catch2_repair_service_exe = executable('catch2_repair_service',
    files('unit/daemon/repair_service_post_ingest_channel_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'repair_maintenance',
    'sources': files('unit/daemon/repair_maintenance_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'repair'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'write_coordinator',
    'sources': files('unit/daemon/write_coordinator_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'write-coordinator'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'service_manager',
    'sources': files('unit/daemon/service_manager_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'service_manager', 'yams-3s4'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'memory_sync_apply',
    'sources': files('unit/daemon/memory_sync_apply_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'memory-sync'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'db_recovery',
    'sources': files('unit/daemon/db_recovery_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'db_recovery'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'db_salvage',
    'sources': files('unit/daemon/db_salvage_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'db_salvage'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'db_recovery_vacuum',
    'sources': files('unit/daemon/db_recovery_vacuum_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'db_recovery'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'storage_preflight',
    'sources': files('unit/daemon/storage_preflight_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'storage_preflight'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'tune_advisor_snapshot',
    'sources': files('unit/daemon/tune_advisor_snapshot_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune_advisor'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'repair_hold_hints',
    'sources': files('unit/daemon/repair_hold_hints_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tuning'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'physical_walk_ttl',
    'sources': files('unit/daemon/physical_walk_ttl_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tuning'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'streaming_processor',
    'sources': files('unit/daemon/streaming_processor_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'streaming', 'yams-3s4'],
    'args': ['--allow-running-no-tests'],  # Skips entirely on Windows
    'timeout': 60,
  }]

  catch2_in_process_transport_exe = executable('catch2_in_process_transport',
    files('unit/daemon/in_process_transport_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep, dependency('yams_daemon_client_embedded')]
      + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'sandbox_detection',
    'sources': files('unit/daemon/sandbox_detection_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'sandbox'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'multiplex_client',
    'sources': files('unit/daemon/multiplex_client_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'multiplex', 'yams-3s4'],
    'args': ['--allow-running-no-tests'],  # All tests have [.requires_daemon] tag
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'entity_graph_service',
    'sources': files('unit/daemon/entity_graph_service_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'yams-3s4'],
    'timeout': 120,
  }]

#These tests use ServiceManager / WorkCoordinator which have resource contention
#when run in parallel with other daemon tests - run sequentially
  catch2_test_table += [{
    'name': 'service_manager_shutdown',
    'sources': files('unit/daemon/service_manager_shutdown_ownership_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'service_manager', 'yams-3s4'],
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'service_manager_degraded',
    'sources': files('unit/daemon/service_manager_provider_degraded_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'service_manager', 'yams-3s4'],
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'repair_binary_filter',
    'sources': files('unit/daemon/repair_binary_filter_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'repair', 'yams-3s4'],
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'abi_model_provider_dim',
    'sources': files('unit/daemon/abi_model_provider_dimension_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'yams-3s4'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'status_readiness_vector_usable',
    'sources': files('unit/daemon/status_readiness_vector_usable_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'readiness'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'model_provider_registry',
    'sources': files('unit/daemon/model_provider_registry_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'model_provider'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'simeon_model_provider',
    'sources': files('unit/daemon/simeon_model_provider_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'model_provider', 'simeon'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'embedding_recovery',
    'sources': files('unit/daemon/embedding_provider_recovery_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'yams-3s4'],
    'args': ['--allow-running-no-tests'],  # Skips entirely on Windows
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'request_io_edge',
    'sources': files('unit/daemon/request_handler_io_edge_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'yams-3s4'],
    'args': ['--allow-running-no-tests'],  # Placeholder tests - all skipped
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'daemon_warm_bench',
    'sources': files('unit/daemon/daemon_warm_bench_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'bench', 'yams-3s4'],
    'args': ['--allow-running-no-tests'],  # Bench only runs when YAMS_BENCH_FAST=1
    'timeout': 120,
  }]

  catch2_daemon_idle_control_bench_exe = executable('catch2_daemon_idle_control_bench',
    files('unit/daemon/daemon_idle_control_bench_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'request_lifetime',
    'sources': files('unit/daemon/request_handler_lifetime_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'yams-3s4'],
    'args': ['--allow-running-no-tests'],  # Skips when streaming disabled
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'writer_drain_cleanup',
    'sources': files('unit/daemon/request_handler_writer_drain_cleanup_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'ipc', 'mux'],
    'args': ['--allow-running-no-tests'],  # Skips entirely on Windows
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'asio_connection_pool',
    'sources': files('unit/daemon/asio_connection_pool_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'connection-pool'],
    'timeout': 120,
    'is_parallel': false,
  }]

  catch2_asio_transport_exhaustion_exe = executable('catch2_asio_transport_exhaustion',
    files('unit/daemon/asio_transport_exhaustion_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_asio_transport_inflight_close_ipc_prefix_exe = executable('catch2_asio_transport_inflight_close_ipc_prefix',
    files('unit/daemon/asio_transport_inflight_close_ipc_prefix_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_daemon_exe = executable('catch2_daemon',
    files('unit/daemon/daemon_catch2_test.cpp'),
    files('unit/daemon/streaming_handlers_catch2_test.cpp'),
    files('unit/daemon/daemon_client_list_status_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'tune_advisor_threshold',
    'sources': files('unit/daemon/tune_advisor_threshold_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'advisor'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'tuning_allocation',
    'sources': files('unit/daemon/tuning_allocation_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'allocation'],
    'timeout': 120,
  }]

  # Real-service registry controls have process-global publication and bootstrap costs.
  catch2_tuning_registry_activity_exe = executable('catch2_tuning_registry_activity',
    files('unit/daemon/tuning_registry_activity_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  # Real proxy request wiring owns service executors and a separate socket I/O context.
  catch2_socket_proxy_request_activity_exe = executable('catch2_socket_proxy_request_activity',
    files('unit/daemon/socket_proxy_request_activity_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'tuning_reconciliation',
    'sources': files('unit/daemon/tuning_reconciliation_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'reconciliation'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'post_ingest_dynamic_cap_recovery',
    'sources': files('unit/daemon/post_ingest_dynamic_cap_recovery_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'post-ingest'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'resource_governance',
    'sources': files('unit/daemon/resource_governance_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'governance'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'host_pressure',
    'sources': files('unit/daemon/host_pressure_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'governance'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'timing_audit',
    'sources': files('unit/daemon/timing_audit_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'timing-audit'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'dynamic_cap_sentinel',
    'sources': files('unit/daemon/dynamic_cap_sentinel_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'dynamic-cap'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'idle_zero_concurrency',
    'sources': files('unit/daemon/idle_zero_concurrency_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'idle'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'health_check_isolation',
    'sources': files('unit/daemon/health_check_isolation_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'state', 'health-check'],
    'timeout': 60,
  }]

  catch2_socket_server_teardown_exe = executable('catch2_socket_server_teardown',
    files('unit/daemon/socket_server_teardown_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'vector_index_coordinator_bulk_scope',
    'sources': files('unit/daemon/vector_index_coordinator_bulk_scope_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'vector', 'coordinator'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'vector_index_coordinator_coalesce',
    'sources': files('unit/daemon/vector_index_coordinator_coalesce_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'vector', 'coordinator'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'vector_index_coordinator_telemetry',
    'sources': files('unit/daemon/vector_index_coordinator_telemetry_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'vector', 'coordinator'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'tuning_loop_wakeup',
    'sources': files('unit/daemon/tuning_loop_wakeup_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'daemon', 'tune', 'wakeup'],
    'timeout': 60,
  }]

  if onnx_dep.found()
    catch2_onnx_model_pool_exe = executable('catch2_onnx_model_pool',
      files(
        'unit/daemon/onnx_model_pool_catch2_test.cpp',
        '../plugins/onnx/model_provider.cpp',
        '../plugins/onnx/onnx_text_processing.cpp',
        '../plugins/ort_runtime/ort_runtime_loader.cpp',
        '../plugins/ort_runtime/ort_runtime_resolution.cpp',
        '../plugins/onnx/onnx_model_pool.cpp',
        '../plugins/onnx/onnx_colbert_session.cpp',
        '../src/vector/tokenizer.cpp',
      ),
      include_directories: incs,
      dependencies: onnx_provider_test_deps,
      cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )
  endif

#CLI Test Suites(Catch2)
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

  catch2_test_table += [{
    'name': 'cli_init_command',
    'sources': files('unit/cli/init_command_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'cli', 'init'],
    'timeout': 120,
    'is_parallel': false,
  }]

#Storage Test Suites(Catch2)
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

  catch2_test_table += [{
    'name': 'storage_backend',
    'sources': files(
      'unit/storage/storage_backend_catch2_test.cpp',
      'unit/storage/windows_path_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'storage', 'backend'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'version_vector',
    'sources': files('unit/memory_sync/version_vector_catch2_test.cpp'),
    'deps': [dependency('nlohmann_json')],
    'suite': ['catch2', 'unit', 'memory-sync'],
    'timeout': 60,
  }]

  catch2_memory_sync_apply_health_exe = executable('catch2_memory_sync_apply_health',
    files('unit/memory_sync/apply_health_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'memory_sync_records',
    'sources': files('unit/memory_sync/records_catch2_test.cpp'),
    'deps': [dependency('nlohmann_json')],
    'suite': ['catch2', 'unit', 'memory-sync'],
    'timeout': 60,
  }]

  catch2_memory_sync_exe = executable('catch2_memory_sync',
    files('unit/memory_sync/memory_sync_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('nlohmann_json'),
      dependency('OpenSSL'),
      dependency('yams_storage_engine'),
      dependency('yams_crypto'),
      dependency('yams_memory_sync'),
      dependency('yams_core'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_content_blob_sync_exe = executable('catch2_content_blob_sync',
    files('unit/memory_sync/content_blob_sync_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('nlohmann_json'),
      dependency('yams_storage_engine'),
      dependency('yams_crypto'),
      dependency('yams_memory_sync'),
      dependency('yams_core'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_memory_sync_service_exe = executable('catch2_memory_sync_service',
    files('unit/memory_sync/memory_sync_service_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('nlohmann_json'),
      dependency('yams_storage_engine'),
      dependency('yams_crypto'),
      dependency('yams_memory_sync'),
      dependency('yams_core'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_memory_sync_config_exe = executable('catch2_memory_sync_config',
    files('unit/memory_sync/memory_sync_config_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('nlohmann_json'),
      dependency('yams_storage_engine'),
      dependency('yams_crypto'),
      dependency('yams_memory_sync'),
      dependency('yams_core'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_memory_sync_cli_exe = executable('catch2_memory_sync_cli',
    files('unit/memory_sync/memory_sync_cli_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('nlohmann_json'),
      dependency('yams_storage_engine'),
      dependency('yams_crypto'),
      dependency('yams_memory_sync'),
      dependency('yams_core'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#Storage submodule tests(StorageEngine, CompressedStorageEngine, ReferenceCounter, etc.)
#Migrated from GTest as part of yams - 3s4 / yams - 8b9
  catch2_storage_submodule_exe = executable('catch2_storage_submodule',
    files(
      'unit/storage/compressed_storage_engine_catch2_test.cpp',
      'unit/storage/compressed_storage_stats_catch2_test.cpp',
      'unit/storage/storage_backend_engine_adapter_catch2_test.cpp',
      'unit/storage/object_storage_adapter_catch2_test.cpp',
      'unit/storage/reference_counter_catch2_test.cpp',
      'unit/storage/reference_counter_stress_catch2_test.cpp',
      'unit/storage/garbage_collector_catch2_test.cpp',
      'unit/storage/sqlite_retry_catch2_test.cpp',
      'unit/storage/s3_signer_catch2_test.cpp',
      'unit/storage/storage_backend_factory_catch2_test.cpp',
      'unit/storage/storage_runtime_resolver_catch2_test.cpp',
      'unit/storage/storage_engine_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#WAL Entry tests - serialization, checksums, encode / decode round - trips
  catch2_wal_entry_exe = executable('catch2_wal_entry',
    files('unit/wal/wal_entry_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + [
      '-DYAMS_TESTING=1',
      '-DYAMS_WAL_FIXTURE_DIR="@0@"'.format(meson.current_source_dir() / 'fixtures' / 'wal'),
    ],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'wal_submodule',
    'sources': files(
      'unit/wal/wal_file_catch2_test.cpp',
      'unit/wal/wal_manager_catch2_test.cpp',
      'unit/wal/wal_metrics_provider_catch2_test.cpp',
      'unit/wal/wal_recovery_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'wal'],
    'timeout': 120,
  }]

#Search submodule tests
#Migrated from GTest as part of yams - 3s4 / yams - 1yq
  catch2_test_table += [{
    'name': 'search_submodule',
    'sources': files(
      'unit/search/chunk_coverage_catch2_test.cpp',
      'unit/search/graph_expansion_catch2_test.cpp',
      'unit/search/hotzone_manager_catch2_test.cpp',
      'unit/search/kg_scorer_simple_catch2_test.cpp',
      'unit/search/lexical_scoring_catch2_test.cpp',
      'unit/search/simeon_lexical_backend_catch2_test.cpp',
      'unit/search/search_engine_simeon_lexical_catch2_test.cpp',
      'unit/search/search_engine_topology_catch2_test.cpp',
      'unit/search/simeon_bandit_catch2_test.cpp',
      'unit/search/simeon_bandit_backend_catch2_test.cpp',
      'unit/search/query_helpers_catch2_test.cpp',
      'unit/search/query_router_catch2_test.cpp',
      'unit/search/query_concept_extractor_catch2_test.cpp',
      'unit/search/query_qualifiers_catch2_test.cpp',
      'unit/search/search_engine_config_catch2_test.cpp',
      'unit/search/search_config_environment_catch2_test.cpp',
      'unit/search/search_tracing_catch2_test.cpp',
      'unit/search/component_fanout_catch2_test.cpp',
      'unit/search/concept_boost_catch2_test.cpp',
      'unit/search/cross_rerank_catch2_test.cpp',
      'unit/search/final_result_order_catch2_test.cpp',
      'unit/search/search_vector_pipeline_catch2_test.cpp',
      'unit/search/search_facets_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'yams-1yq'],
    'timeout': 180,
  }]

  catch2_test_table += [{
    'name': 'search_vector_stage',
    'sources': files('unit/search/search_vector_stage_characterization_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'vector_stage'],
    'timeout': 180,
  }]

  catch2_test_table += [{
    'name': 'search_vector_stage_route_only',
    'sources': files('unit/search/search_vector_stage_route_only_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'vector_stage'],
    'timeout': 180,
  }]

  # Road beam (search.topology.road_hops) in RouteOnly / Augment; kept apart from the RouteOnly
  # cells so their goldens stay byte-identical.
  catch2_test_table += [{
    'name': 'search_vector_stage_road_beam',
    'sources': files('unit/search/search_vector_stage_road_beam_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'vector_stage'],
    'timeout': 180,
  }]

  # Road walk (search.topology.road_walk_window) along ordered cluster members in RouteOnly /
  # Augment; kept apart from the RouteOnly and road beam cells so their goldens stay byte-identical.
  catch2_test_table += [{
    'name': 'search_vector_stage_road_walk',
    'sources': files('unit/search/search_vector_stage_road_walk_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'vector_stage'],
    'timeout': 180,
  }]

  catch2_test_table += [{
    'name': 'symspell_search',
    'sources': files('unit/search/symspell_search_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'symspell'],
    'timeout': 30,
  }]

  # Executor-backed vector stage under a component deadline (lifetime of late completions).
  catch2_test_table += [{
    'name': 'search_vector_stage_timeout',
    'sources': files('unit/search/search_vector_stage_timeout_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'vector_stage'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'evidence_search_pipeline',
    'sources': files('unit/search/evidence_search_pipeline_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'evidence_pipeline'],
    'timeout': 30,
  }]

#TuningSlot tests(provenance - tracking primitives for restructured tuning pipeline)
#TuningSlot provenance tests
  catch2_test_table += [{
    'name': 'tuning_slot',
    'sources': files('unit/search/tuning_slot_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'tuner'],
    'timeout': 30,
  }]

#R3 Tuning features + reward(featurize / bucketize / combineReward)
#R3 tuning features + reward tests
  catch2_test_table += [{
    'name': 'tuning_features',
    'sources': files('unit/search/tuning_features_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'tuner'],
    'timeout': 30,
  }]

# SearchTuner tests: adaptive search engine tuning via corpus-aware FSM.
#Tests FSM state transitions, TunedParams matrix, and SearchEngineConfig generation
#SearchTuner FSM tests(Phase 2 of Epic yams - 7ez4)
  catch2_test_table += [{
    'name': 'search_tuner',
    'sources': files('unit/search/search_tuner_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'tuner', 'yams-7ez4'],
    'timeout': 60,
  }]

#InternalBenchmark tests(Phase 3 of Epic yams - 7ez4 : Internal Benchmark Suite)
#Tests LatencyStats, BenchmarkResults, BenchmarkComparison, query generation configs
#InternalBenchmark tests(Phase 3 of Epic yams - 7ez4)
  catch2_test_table += [{
    'name': 'internal_benchmark',
    'sources': files('unit/search/internal_benchmark_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'benchmark', 'yams-7ez4'],
    'timeout': 60,
  }]

  catch2_benchmark_history_store_exe = executable('catch2_benchmark_history_store',
    files('unit/search/benchmark_history_store_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_benchmark_invocation_exe = executable('catch2_benchmark_invocation',
    files('unit/benchmarks/benchmark_invocation_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep],
    cpp_args: test_cpp_noerror_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'retrieval_benchmark_support',
    'sources': files(
      'unit/search/retrieval_benchmark_support_catch2_test.cpp',
      'benchmarks/search/retrieval_benchmark_support.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'benchmark'],
    'timeout': 30,
  }]

  catch2_search_bandit_bench_exe = executable('catch2_search_bandit_bench',
    files('benchmarks/search_bandit_bench.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'relevance_label_store',
    'sources': files('unit/search/relevance_label_store_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search', 'relevance'],
    'timeout': 30,
  }]

# Compression submodule tests.
#Migrated from GTest as part of yams - 3s4 / yams - pmf
  catch2_compression_submodule_exe = executable('catch2_compression_submodule',
    files(
#basic_error_handler_catch2_test.cpp removed : fully covered by error_handler_catch2_test.cpp
# basic_integrity_validator_catch2_test.cpp removed: covered by the comprehensive test.
      'unit/compression/compression_comprehensive_catch2_test.cpp',
      'unit/compression/compression_monitor_catch2_test.cpp',
      'unit/compression/compression_policy_catch2_test.cpp',
      'unit/compression/error_handler_catch2_test.cpp',
      'unit/compression/framed_payload_catch2_test.cpp',
#TODO(yams - 3s4) : Fix API mismatch - uses non - existent IntegrityValidator methods
#'unit/compression/integrity_validator_catch2_test.cpp',
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

#MCP submodule tests(MCPServer, Transport, JSON - RPC, Tools Schema, etc.)
#Migrated from GTest as part of yams - 3s4 / yams - 84g
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
      'unit/mcp/mcp_context_suggestion_catch2_test.cpp',
      'unit/mcp/mcp_semantic_dedupe_catch2_test.cpp',
      'unit/mcp/mcp_disconnect_uniform_errors_catch2_test.cpp',
      'unit/mcp/mcp_doctor_flags_catch2_test.cpp',
      'unit/mcp/mcp_jsonrpc_roundtrip_catch2_test.cpp',
      'unit/mcp/mcp_protocol_version_features_test.cpp',
      'unit/mcp/mcp_prompts_catch2_test.cpp',
      'unit/mcp/mcp_resource_read_catch2_test.cpp',
      'unit/mcp/mcp_request_routing_catch2_test.cpp',
      'unit/mcp/mcp_search_dto_catch2_test.cpp',
      'unit/mcp/mcp_tool_registry_dto_roundtrip_catch2_test.cpp',
      'unit/mcp/mcp_server_catch2_test.cpp',
      'unit/mcp/mcp_server_exec_catch2_test.cpp',
      'unit/mcp/mcp_socket_resolution_catch2_test.cpp',
      'unit/mcp/mcp_timeout_catch2_test.cpp',
      'unit/mcp/mcp_code_mode_catch2_test.cpp',
      'unit/mcp/mcp_composite_dispatch_catch2_test.cpp',
      'unit/mcp/mcp_mode_router_catch2_test.cpp',
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

#CLI submodule tests split by behavior cluster to improve Meson parallelism
#and keep failures localized without dropping coverage.
#Migrated from GTest as part of yams - 3s4 / yams - cli
  cli_test_deps = [version_generated_dep, catch2_dep, catch2_main_dep] + test_deps
  cli_test_cpp_args = test_cpp_noerror_args + ['-DYAMS_TESTING=1']

  catch2_cli_common_exe = executable('catch2_cli_common',
    version_header,
    files(
      'unit/common/format_catch2_test.cpp',
      'unit/common/crc32_catch2_test.cpp',
      'unit/common/hash_predicates_catch2_test.cpp',
      'unit/common/log_rate_limiter_catch2_test.cpp',
      'unit/common/path_alias_catch2_test.cpp',
      'unit/common/pattern_utils_catch2_test.cpp',
      'unit/common/string_utils_catch2_test.cpp',
      'unit/common/test_rusage_catch2_test.cpp',
      'unit/common/test_state_isolation_catch2_test.cpp',
      'unit/common/test_utils_catch2_test.cpp',
      'unit/common/time_utils_catch2_test.cpp',
      'unit/common/utf8_utils_catch2_test.cpp',
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/error_hints_catch2_test.cpp',
      'unit/cli/homebrew_service_formula_catch2_test.cpp',
      'unit/cli/pipeline_stage_render_catch2_test.cpp',
      'unit/cli/prompt_util_catch2_test.cpp',
      'unit/cli/recommendation_util_catch2_test.cpp',
      'unit/cli/status_metrics_catch2_test.cpp',
      'unit/cli/time_filters_catch2_test.cpp',
      'unit/cli/stream_heuristics_catch2_test.cpp',
      'unit/cli/time_parser_catch2_test.cpp',
      'unit/cli/version_generated_header_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_cli_config_exe = executable('catch2_cli_config',
    version_header,
    files(
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/cli_option_isolation_catch2_test.cpp',
      'unit/cli/cli_utility_coverage_catch2_test.cpp',
      'unit/cli/config_command_catch2_test.cpp',
      'unit/cli/data_dir_resolution_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_cli_commands_exe = executable('catch2_cli_commands',
    version_header,
    files(
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/add_command_stdin_catch2_test.cpp',
      'unit/cli/command_registry_catch2_test.cpp',
      'unit/cli/content_roundtrip_catch2_test.cpp',
      'unit/cli/completion_command_catch2_test.cpp',
      'unit/cli/delete_command_catch2_test.cpp',
      'unit/cli/download_command_catch2_test.cpp',
      'unit/cli/init_command_catch2_test.cpp',
      'unit/cli/list_command_catch2_test.cpp',
      'unit/cli/update_command_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_cli_model_plugin_exe = executable('catch2_cli_model_plugin',
    version_header,
    files(
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/model_command_catch2_test.cpp',
      'unit/cli/plugin_command_catch2_test.cpp',
      'unit/cli/repair_command_catch2_test.cpp',
      'unit/cli/session_command_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_cli_doctor_exe = executable('catch2_cli_doctor',
    version_header,
    files(
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/doctor_command_catch2_test.cpp',
      'unit/cli/doctor_repair_renderers_catch2_test.cpp',
      'unit/cli/doctor/db_integrity_catch2_test.cpp',
      'unit/cli/doctor/dim_consistency_catch2_test.cpp',
      'unit/cli/doctor/storage_blob_check_catch2_test.cpp',
      'unit/cli/doctor/ref_count_check_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_cli_graph_exe = executable('catch2_cli_graph',
    version_header,
    files(
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/graph_explore_presenter_catch2_test.cpp',
      'unit/cli/graph_helpers_catch2_test.cpp',
      'unit/cli/graph_query_presenter_catch2_test.cpp',
      'unit/cli/graph_topology_support_catch2_test.cpp',
      'unit/cli/grep_command_path_tree_catch2_test.cpp',
      'unit/cli/vector_db_util_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_cli_daemon_exe = executable('catch2_cli_daemon',
    version_header,
    files(
      'unit/cli/cli_test_cleanup_listener.cpp',
      'unit/cli/daemon_helpers_pool_catch2_test.cpp',
      'unit/cli/daemon_command_catch2_test.cpp',
      'unit/cli/daemon_status_render_catch2_test.cpp',
      'unit/cli/p2p_status_format_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: cli_test_deps,
    cpp_args: cli_test_cpp_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#Vector Test Suites(Catch2) - requires enable - vector - tests
#Migrated from GTest as part of yams - 3s4 / yams - 1d4
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  if get_option('enable-vector-tests')
    vector_test_cpp_args = test_cpp_noerror_args + ['-DYAMS_TESTING=1']

    catch2_vector_submodule_exe = executable('catch2_vector_submodule',
      files(
        'unit/vector/document_chunker_catch2_test.cpp',
        'unit/vector/dim_resolver_catch2_test.cpp',
        'unit/vector/embedding_service_synthetic_catch2_test.cpp',
        'unit/vector/sqlite_vec_backend_comprehensive_catch2_test.cpp',
        'unit/vector/sqlite_vec_backend_persistence_catch2_test.cpp',
        'unit/vector/static_cosine_ann_index_catch2_test.cpp',
        'unit/vector/sqlite_vec_c_api_smoke_catch2_test.cpp',
        'unit/vector/vector_schema_migration_vec0_catch2_test.cpp',
        'unit/vector/vector_dimension_validation_catch2_test.cpp',
        'unit/vector/vector_smoke_catch2_test.cpp',
        'unit/vector/simeon_backend_catch2_test.cpp',
      ),
      include_directories: incs,
      dependencies: [catch2_dep, catch2_main_dep] + test_deps,
      cpp_args: vector_test_cpp_args,
      build_rpath: ':'.join(_test_rpaths),
      install: false,
    )
  endif

  catch2_test_table += [{
    'name': 'vector_vec0_cosine_order',
    'sources': files('unit/vector/vec0_cosine_order_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'vector'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'vector_sync_adapter',
    'sources': files('unit/vector/vector_sync_adapter_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'vector', 'memory-sync'],
    'timeout': 120,
  }]

#Tokenizer Tests(Catch2) - HuggingFaceTokenizer WordPiece, Unigram, special token injection
#Always - on : uses synthetic tokenizer.json files, no model downloads or sqlite - vec required.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Tokenizer tests(WordPiece, Unigram, special token injection)
  catch2_test_table += [{
    'name': 'tokenizer',
    'sources': files('unit/vector/tokenizer_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'vector', 'yams-tokenizer'],
    'timeout': 60,
  }]

#API Test Suites(Catch2) - content store API tests
#Migrated from GTest as part of yams - 3s4 / api
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#API submodule tests(content store, metadata API)
#Migrated from GTest as part of yams - 3s4 / api
  catch2_test_table += [{
    'name': 'api_submodule',
    'sources': files(
      'unit/api/content_store_catch2_test.cpp',
      'unit/api/content_store_builder_compression_catch2_test.cpp',
      'unit/api/metadata_api_catch2_test.cpp',
      'unit/api/semantic_search_api_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'api', 'yams-api'],
    'timeout': 180,
  }]

#Downloader Test Suites(Catch2) - download manager, resume store, CAS helpers
#Migrated from GTest as part of yams - 3s4 / downloader
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Downloader submodule tests(download manager, resume, CAS)
#Migrated from GTest as part of yams - 3s4 / downloader
  catch2_test_table += [{
    'name': 'downloader_submodule',
    'sources': files('unit/downloader/downloader_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'downloader', 'yams-downloader'],
    'timeout': 180,
  }]

#Resolve Test Suites(Catch2) - resolver, backend implementations
#Migrated from GTest as part of yams - 3s4 / resolve
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Resolve submodule tests(resolver, backends)
#Migrated from GTest as part of yams - 3s4 / resolve
  catch2_test_table += [{
    'name': 'resolve_submodule',
    'sources': files('unit/resolve/resolver_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'resolve', 'yams-resolve'],
    'timeout': 180,
  }]

#Manifest Test Suites(Catch2) - manifest manager, serialization, reconstruction
#Migrated from GTest as part of yams - 3s4 / yams - 1jm
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Manifest submodule tests(manifest manager, serialization, reconstruction)
#Migrated from GTest as part of yams - 3s4 / yams - 1jm
  catch2_test_table += [{
    'name': 'manifest_submodule',
    'sources': files('unit/manifest/manifest_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'manifest', 'yams-manifest'],
    'timeout': 180,
  }]

#Repair Test Suites(Catch2) - embedding repair scan
#Migrated from GTest as part of yams - 3s4 / yams - 1jm
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Repair submodule tests(embedding repair scan)
#Migrated from GTest as part of yams - 3s4 / yams - 1jm
#Note : --allow - running - no - tests needed because tests skip when YAMS_DISABLE_VECTORS = 1
  catch2_test_table += [{
    'name': 'repair_submodule',
    'sources': files('unit/repair/embedding_repair_scan_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'repair', 'yams-repair'],
    'args': ['--allow-running-no-tests'],
    'timeout': 180,
  }]

#Versioning Test Suites(Catch2) - versioning utility
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Versioning submodule tests(document versioning, indexer)
#Migrated from GTest as part of yams - 3s4 / yams - 1jm
  catch2_test_table += [{
    'name': 'versioning_submodule',
    'sources': files('unit/metadata/versioning_util_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'versioning', 'yams-versioning'],
    'timeout': 180,
  }]

#Metadata Test Suites(4 executables)
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

#Metadata tests
  catch2_test_table += [{
    'name': 'metadata_retrieval_schema',
    'target': 'catch2_metadata_retrieval',
    'sources': files('unit/app/services/retrieval_schema_validation_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'retrieval'],
    'timeout': 60,
  }]

#Removed : catch2_metadata_search(search_metadata_interface_test.cpp) - HybridSearchEngine specific

  catch2_test_table += [{
    'name': 'metadata_migration_validation',
    'target': 'catch2_metadata_migration',
    'sources': files('unit/metadata/migration_validation_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'migration'],
    'timeout': 120,
  }]

#Metadata Unit Tests(Catch2) - concepts, database, query_helpers, sql, value_variant
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'metadata_unit',
    'sources': files('unit/metadata/metadata_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'yams-metadata'],
    'timeout': 120,
  }]

#Connection Pool Tests(Catch2) - pool maintenance, waiting guard
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'connection_pool',
    'sources': files('unit/metadata/connection_pool_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'connection_pool'],
    'timeout': 120,
  }]

# Document snapshot membership and the legacy per-snapshot key backfill
  catch2_test_table += [{
    'name': 'document_snapshots',
    'sources': files('unit/metadata/document_snapshots_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'snapshots'],
    'timeout': 120,
  }]

#KG Store Tests(Catch2) - knowledge graph store operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'kg_store',
    'sources': files(
      'unit/metadata/kg_store_catch2_test.cpp',
      'unit/metadata/kg_relation_summary_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'kg'],
    'timeout': 120,
  }]

#KG Write Buffer Tests(Catch2) - in - memory edge / entity coalescing
  catch2_test_table += [{
    'name': 'kg_write_buffer',
    'sources': files('unit/metadata/kg_write_buffer_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'kg'],
    'timeout': 60,
  }]

#Database Tests(Catch2) - database operations, connection pool, migrations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'database',
    'sources': files('unit/metadata/database_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'database'],
    'timeout': 120,
  }]

#Metadata Corruption Tests(Catch2) - corruption injection, detection, recovery
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'metadata_corruption',
    'sources': files(
      'unit/metadata/database_corruption_catch2_test.cpp',
      'unit/metadata/migration_corruption_catch2_test.cpp',
      'unit/metadata/kg_store_corruption_catch2_test.cpp',
      'unit/metadata/connection_pool_corruption_catch2_test.cpp',
      'unit/metadata/writer_corruption_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'corruption'],
    'timeout': 120,
  }]

#FTS5 Tests(Catch2) - FTS5 support detection and fallback
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'fts5',
    'sources': files(
      'unit/metadata/fts5_catch2_test.cpp',
      'unit/metadata/fts5_query_sanitizer_catch2_test.cpp',
      'unit/metadata/search_query_helpers_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'fts5'],
    'timeout': 60,
  }]

#Tree Builder Tests(Catch2) - tree node operations and directory tree building
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'tree_builder',
    'sources': files('unit/metadata/tree_builder_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'tree_builder'],
    'timeout': 120,
  }]

#Tree Differ Tests(Catch2) - tree diffing operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'tree_differ',
    'sources': files('unit/metadata/tree_differ_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'tree_differ'],
    'timeout': 120,
  }]

#Path Tree Delete Tests(Catch2) - path tree removal operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'path_tree_delete',
    'sources': files('unit/metadata/path_tree_delete_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'path_tree'],
    'timeout': 120,
  }]

#Session Isolation Tests(Catch2) - session - isolated memory operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'session_isolation',
    'sources': files('unit/metadata/session_isolation_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'session'],
    'timeout': 120,
  }]

#Metadata Repository Tests(Catch2) - repository operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'metadata_repository',
    'sources': files(
      'unit/metadata/metadata_repository_catch2_test.cpp',
      'unit/metadata/metadata_repository_tag_stats_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'repository'],
    'timeout': 180,
  }]

  catch2_test_table += [{
    'name': 'metadata_content_transaction',
    'sources': files('unit/metadata/metadata_content_transaction_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'repository', 'transaction'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'metadata_sync_adapter',
    'sources': files('unit/metadata/metadata_sync_adapter_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'memory-sync'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'topology_sync_adapter',
    'sources': files('unit/metadata/topology_sync_adapter_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'topology', 'memory-sync'],
    'timeout': 120,
  }]

#Metadata Repository Cache Tests(Catch2) - cache operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'metadata_repository_cache',
    'sources': files('unit/metadata/metadata_repository_cache_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'cache'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'topology_baseline',
    'sources': files('unit/topology/topology_baseline_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'topology_identity',
    'sources': files('unit/topology/protected_relation_identity_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'topology_factory',
    'sources': files('unit/topology/topology_factory_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'topology_kmeans',
    'sources': files('unit/topology/kmeans_engine_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'topology_representative_rule',
    'sources': files('unit/topology/topology_representative_rule_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'topology_roads',
    'sources': files('unit/topology/topology_roads_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'topology_road_sidecar',
    'sources': files('unit/topology/topology_road_sidecar_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'topology_roads_incremental',
    'sources': files('unit/topology/topology_roads_incremental_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'topology_input_event_time',
    'sources': files('unit/topology/topology_input_event_time_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'topology_ordered_members',
    'sources': files('unit/topology/topology_ordered_members_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'topology_codec',
    'sources': files('unit/topology/topology_codec_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'topology_metadata_store',
    'sources': files('unit/topology/topology_metadata_store_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'topology'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'string_interner',
    'sources': files('unit/search/string_interner_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'search'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'binary_quantization',
    'sources': files('unit/vector/binary_quantization_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'vector'],
    'timeout': 30,
  }]

#Fuzzy Index Prioritization Tests(Catch2) - fuzzy index scoring
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'fuzzy_index_prioritization',
    'sources': files('unit/metadata/fuzzy_index_prioritization_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'fuzzy'],
    'timeout': 60,
  }]

#KG Store Alias and Entities Tests(Catch2) - KG alias and entity operations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'kg_store_alias_entities',
    'sources': files('unit/metadata/kg_store_alias_and_entities_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'kg'],
    'timeout': 120,
  }]

#FTS Query Spec Integration Tests(Catch2) - FTS search integration
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'fts_queryspec_integration',
    'sources': files('unit/metadata/fts_queryspec_integration_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'fts'],
    'timeout': 60,
  }]

#Tags Query Spec Integration Tests(Catch2) - tags query integration
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'tags_queryspec_integration',
    'sources': files('unit/metadata/tags_queryspec_integration_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'tags'],
    'timeout': 60,
  }]

#Query Helpers Integration Tests(Catch2) - query helper integration
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'query_helpers_integration',
    'sources': files('unit/metadata/query_helpers_integration_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'query_helpers'],
    'timeout': 60,
  }]

#Metadata Schema Tests(Catch2) - comprehensive schema, CRUD, FTS, migrations
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_metadata_schema_exe = executable('catch2_metadata_schema',
    files('unit/metadata/metadata_schema_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

#FTS5 Failure Scenarios Tests(Catch2) - FTS5 edge cases and failure modes
#Migrated from GTest as part of yams - 3s4 / yams - aqc
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'fts5_failure_scenarios',
    'sources': files('unit/metadata/fts5_failure_scenarios_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'fts5', 'failure'],
    'timeout': 180,
  }]

#Corpus Stats Tests(Catch2) - corpus statistics for adaptive search tuning
#Phase 1 of Epic yams - 7ez4 : Adaptive Search Engine Tuning via Corpus - Aware FSM
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'corpus_stats',
    'sources': files('unit/metadata/corpus_stats_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'corpus_stats'],
    'timeout': 120,
  }]

#Repository Foundation Tests(Catch2) - entity_traits, result_helpers, crud_ops
#Phase 1 of ADR - 0004 : MetadataRepository Refactor with Templates and Metaprogramming
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  catch2_test_table += [{
    'name': 'repository_foundation',
    'sources': files('unit/metadata/repository_foundation_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'metadata', 'repository', 'foundation'],
    'timeout': 180,
  }]

#Core tests(minimal dependencies - no daemon)
  core_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
  ]

#Core tests
  catch2_test_table += [{
    'name': 'core_cpp23_features',
    'target': 'core_cpp23_features',
    'sources': files('unit/core/cpp23_features_test.cpp'),
    'deps': core_test_deps,
    'suite': ['catch2', 'unit', 'core', 'cpp23'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'core_cpp23_features_fallback',
    'target': 'core_cpp23_features_fallback',
    'sources': files('unit/core/cpp23_features_fallback_test.cpp'),
    'deps': core_test_deps,
    'suite': ['catch2', 'unit', 'core', 'cpp23'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'core_magic_numbers',
    'target': 'core_magic_numbers',
    'sources': files('unit/core/magic_numbers_test.cpp'),
    'deps': core_test_deps,
    'suite': ['catch2', 'unit', 'core', 'magic'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'core_uuid',
    'target': 'core_uuid',
    'sources': files('unit/core/uuid_test.cpp'),
    'deps': core_test_deps,
    'suite': ['catch2', 'unit', 'core', 'uuid'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'core_checked_arithmetic',
    'target': 'core_checked_arithmetic',
    'sources': files('unit/core/checked_arithmetic_catch2_test.cpp'),
    'deps': core_test_deps,
    'suite': ['catch2', 'unit', 'core', 'arithmetic'],
    'timeout': 60,
  }]

  config_parse_utils_exe = executable('config_parse_utils',
    files(
      'unit/config/config_parse_utils_catch2_test.cpp',
      'unit/config/environment_catch2_test.cpp',
      'unit/config/flat_toml_reader_catch2_test.cpp',
    ),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('threads'),
      dependency('yams_config'),
      dependency('spdlog'),
    ],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  tune_advisor_environment_snapshot_exe = executable(
    'tune_advisor_environment_snapshot',
    files('unit/daemon/tune_advisor_environment_snapshot_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [
      catch2_dep,
      catch2_main_dep,
      dependency('threads'),
      dependency('yams_config'),
      dependency('yams_tune_advisor'),
      dependency('spdlog'),
    ],
    cpp_args: test_cpp_noerror_args,
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )
  test('tune_advisor_environment_snapshot', tune_advisor_environment_snapshot_exe,
    timeout: 30,
  )

  config_reader_link_smoke_exe = executable('config_reader_link_smoke',
    files('unit/config/config_reader_link_smoke.cpp'),
    include_directories: incs,
    dependencies: [dependency('yams_api')],
    install: false,
  )

  catch2_test_table += [{
    'name': 'detection',
    'target': 'detection',
    'sources': files(
      'unit/detection/command_integration_test.cpp',
      'unit/detection/detection_test_helpers.h',
      'unit/detection/file_type_detector_test.cpp',
      'unit/detection/file_type_graceful_fallback_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'detection'],
    'timeout': 120,
  }]

#Plugins tests(Catch2 - migrated from GTest)
  catch2_test_table += [{
    'name': 'plugins_catch2',
    'target': 'plugins_catch2',
    'sources': files(
      'unit/plugins/plugin_installer_test.cpp',
      'unit/plugins/plugin_repo_client_test.cpp',
      'unit/plugins/onnx_request_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'plugins'],
    'timeout': 120,
  }]

#Integrity tests(Catch2 - migrated from GTest)
  integrity_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_crypto'),
    dependency('yams_integrity'),
    dependency('yams_storage_engine'),
    dependency('yams_reference_counter'),
    dependency('yams_metadata'),
    dependency('yams_search'),
    dependency('yams_daemon'),
  ]

  integrity_catch2_exe = executable('integrity_catch2',
    files(
      'unit/integrity/chunk_validator_test.cpp',
      'unit/integrity/integrity_verifier_simple_test.cpp',
      'unit/integrity/integrity_verifier_catch2_test.cpp',
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

#Content tests(depends on content, extraction, detection modules)
  content_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_content'),
    dependency('yams_extraction'),
    dependency('yams_detection'),
    dependency('sqlite3'),
  ]

#Content tests
  catch2_test_table += [{
    'name': 'content_handlers',
    'target': 'content_handlers',
    'sources': files('unit/content/content_handlers_test.cpp'),
    'deps': content_test_deps,
    'suite': ['catch2', 'unit', 'content', 'handlers'],
    'timeout': 60,
  }]

#Archive content handler tests(formatToString, ProcessingStats, helper functions, canHandle)
  catch2_test_table += [{
    'name': 'archive_content_handler',
    'target': 'archive_content_handler',
    'sources': files('unit/content/archive_content_handler_catch2_test.cpp'),
    'deps': content_test_deps,
    'suite': ['catch2', 'unit', 'content', 'archive'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'html_extractor_robust',
    'target': 'html_extractor_robust',
    'sources': files('unit/extraction/html_extractor_robust_test.cpp'),
    'deps': content_test_deps,
    'suite': ['catch2', 'unit', 'extraction', 'html'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'plain_text_extractor',
    'target': 'plain_text_extractor',
    'sources': files('unit/extraction/plain_text_extractor_catch2_test.cpp'),
    'deps': content_test_deps,
    'suite': ['catch2', 'unit', 'extraction'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'text_basic_handler',
    'target': 'text_basic_handler',
    'sources': files('unit/extraction/text_basic_handler_catch2_test.cpp'),
    'deps': content_test_deps,
    'suite': ['catch2', 'unit', 'extraction'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'builtin_content_extractor',
    'target': 'builtin_content_extractor',
    'sources': files('unit/extraction/builtin_content_extractor_catch2_test.cpp'),
    'deps': content_test_deps,
    'suite': ['catch2', 'unit', 'extraction', 'content_extractor'],
    'timeout': 60,
  }]

#Text extraction to FTS5 pipeline test(detection->extraction->indexing->search)
  catch2_test_table += [{
    'name': 'text_extraction_fts5_pipeline',
    'target': 'text_extraction_fts5_pipeline',
    'sources': files('unit/extraction/text_extraction_fts5_pipeline_test.cpp'),
    'deps': content_test_deps + [dependency('yams_metadata')],
    'suite': ['catch2', 'unit', 'extraction', 'pipeline', 'fts5'],
    'timeout': 60,
  }]

#Plugin process tests(external plugin infrastructure)
  plugin_process_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_extraction'),
    dependency('nlohmann_json'),
  ]

  catch2_test_table += [{
    'name': 'plugin_process',
    'target': 'plugin_process',
    'sources': files('unit/extraction/plugin_process_test.cpp'),
    'deps': plugin_process_test_deps,
    'suite': ['catch2', 'unit', 'extraction', 'plugin'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'plugin_process_minimal',
    'target': 'plugin_process_minimal',
    'sources': files('unit/extraction/plugin_process_minimal_test.cpp'),
    'deps': plugin_process_test_deps,
    'suite': ['catch2', 'unit', 'extraction', 'plugin', 'debug'],
    'timeout': 120,
    'is_parallel': false,
  }]

  plugin_process_launcher_exe = executable('plugin_process_launcher',
    files('unit/extraction/plugin_process_launcher_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + plugin_process_test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'jsonrpc_client',
    'target': 'jsonrpc_client',
    'sources': files('unit/extraction/jsonrpc_client_test.cpp'),
    'deps': plugin_process_test_deps,
    'suite': ['catch2', 'unit', 'extraction', 'jsonrpc'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'external_plugin_extractor',
    'target': 'external_plugin_extractor',
    'sources': files('unit/extraction/external_plugin_extractor_test.cpp'),
    'deps': plugin_process_test_deps,
    'suite': ['catch2', 'unit', 'extraction', 'plugin'],
    'timeout': 60,
  }]

#Chunking tests(depends on chunking module)
  chunking_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_chunking'),
    dependency('yams_crypto'),
  ]

#Chunking tests
  catch2_test_table += [{
    'name': 'chunking',
    'target': 'chunking',
    'sources': files('unit/chunking/chunking_test.cpp'),
    'deps': chunking_test_deps,
    'suite': ['catch2', 'unit', 'chunking'],
    'timeout': 60,
  }]

#Crypto tests(depends on crypto module)
  crypto_test_deps = [
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_crypto'),
  ]

#Crypto tests
  catch2_test_table += [{
    'name': 'crypto',
    'target': 'crypto',
    'sources': files('unit/crypto/crypto_test.cpp'),
    'deps': crypto_test_deps,
    'suite': ['catch2', 'unit', 'crypto'],
    'timeout': 60,
  }]

#Service tests
  catch2_test_table += [{
    'name': 'grep_service',
    'target': 'grep_service',
    'sources': files('unit/app/services/grep_service_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'grep'],
    'timeout': 120,
  }]

  catch2_test_table += [{
    'name': 'indexing_service',
    'target': 'indexing_service',
    'sources': files('unit/app/services/indexing_service_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'indexing'],
    'timeout': 180,
  }]

  catch2_test_table += [{
    'name': 'document_service',
    'target': 'document_service',
    'sources': files('unit/app/services/document_service_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'document'],
    'timeout': 180,
  }]

  document_service_update_metadata_exe = executable('document_service_update_metadata',
    files('unit/app/services/document_service_update_metadata_catch2_test.cpp'),
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + test_deps,
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )

  catch2_test_table += [{
    'name': 'document_ingestion_batch',
    'target': 'document_ingestion_batch',
    'sources': files('unit/app/services/document_ingestion_batch_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'ingestion', 'batch'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'service_utilities',
    'target': 'service_utilities',
    'sources': files(
      'unit/app/services/service_utilities_test.cpp',
      'unit/app/services/download_metadata_entries_test.cpp',
      'unit/app/services/enhanced_search_config_catch2_test.cpp',
      'unit/app/services/retrieval_service_mapping_catch2_test.cpp',
    ),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'utilities'],
    'timeout': 60,
    'is_parallel': false,
  }]

  catch2_test_table += [{
    'name': 'service_deadline',
    'target': 'service_deadline',
    'sources': files('unit/app/services/service_deadline_catch2_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'deadline'],
    'timeout': 30,
  }]

  catch2_test_table += [{
    'name': 'simd_memmem',
    'target': 'simd_memmem',
    'sources': files('unit/app/services/simd_memmem_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'simd'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'grep_regex',
    'target': 'grep_regex',
    'sources': files('unit/app/services/grep_regex_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'grep'],
    'timeout': 60,
  }]

  catch2_test_table += [{
    'name': 'literal_extractor',
    'target': 'literal_extractor',
    'sources': files('unit/app/services/literal_extractor_test.cpp'),
    'deps': test_deps,
    'suite': ['catch2', 'unit', 'services', 'literal'],
    'timeout': 60,
  }]

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

#Re - enabled with mock - based tests disabled, placeholder integration test added
#TODO PBI - 071 : Implement full integration tests with real components
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

#Corpus Stats Search Integration Tests(Catch2)
#Phase 1 of Epic yams - 7ez4 : Validates corpus stats collection from real document store
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  if not _test_address_sanitizer
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

  endif

#Test Registrations
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

#Removed : metadata_search test - HybridSearchEngine specific


  test('metadata_schema', catch2_metadata_schema_exe,
    suite: ['catch2', 'unit', 'metadata', 'schema'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300
  )

  test('metadata_schema_slow', catch2_metadata_schema_exe,
    suite: ['catch2', 'slow', 'metadata', 'schema'],
    args: ['[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300,
    is_parallel: false
  )

  test('config_parse_utils', config_parse_utils_exe,
    suite: ['catch2', 'unit', 'config', 'parse'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('config_environment_random_order', config_parse_utils_exe,
    suite: ['catch2', 'unit', 'config', 'environment', 'order'],
    args: ['[config][environment]', '--order', 'rand', '--rng-seed', '66'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('config_reader_link_smoke', config_reader_link_smoke_exe,
    suite: ['unit', 'config', 'link'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 30
  )

  test('integrity_catch2', integrity_catch2_exe,
    suite: ['catch2', 'unit', 'integrity'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: host_machine.system() == 'windows' ? 600 : 120
  )

  test('plugin_process_launcher', plugin_process_launcher_exe,
    suite: ['catch2', 'unit', 'extraction', 'plugin', 'launcher'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path, 'YAMS_SOURCE_DIR': meson.project_source_root() },
    timeout: host_machine.system() == 'windows' ? 180 : 120,
#PyInstaller process tests have resource contention when run in parallel
    is_parallel: false
  )

#Search tests
  test('hybrid_search', hybrid_search_exe,
    suite: ['catch2', 'unit', 'search', 'hybrid'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_DISABLE_VECTORS': '1',
    },
    timeout: 120
  )

  test('hybrid_search_slow', hybrid_search_exe,
    suite: ['catch2', 'slow', 'search', 'hybrid'],
    args: ['[slow]', '--allow-running-no-tests'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_DISABLE_VECTORS': '1',
    },
    timeout: 120
  )

  if not _test_address_sanitizer
    test('corpus_stats_search_integration', corpus_stats_search_integration_exe,
      suite: ['catch2', 'integration', 'search', 'corpus_stats'],
      env: {
        'LD_LIBRARY_PATH': _test_env_ld_library_path,
        'YAMS_DISABLE_VECTORS': '1',
      },
      timeout: 180
    )

  endif

  daemon_metrics_timeout = (yams_sanitize_thread or yams_sanitize_address) ? 240 : 180

  test('daemon_metrics_status', catch2_daemon_metrics_status_exe,
    suite: ['catch2', 'unit', 'daemon', 'metrics', 'status'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: daemon_metrics_timeout,
    is_parallel: false
  )

  test('p2p_protocol', catch2_p2p_protocol_exe,
    args: ['~[fuzz]'],
    suite: ['catch2', 'unit', 'daemon', 'p2p', 'network'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('p2p_protocol_fuzz_controls', catch2_p2p_protocol_exe,
    args: ['[daemon][p2p][protocol][fuzz]'],
    suite: ['catch2', 'unit', 'daemon', 'p2p', 'fuzz-smoke'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 30
  )

  test('p2p_delta', catch2_p2p_delta_exe,
    args: ['~[fuzz]'],
    suite: ['catch2', 'unit', 'daemon', 'p2p', 'network'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('p2p_delta_fuzz_controls', catch2_p2p_delta_exe,
    args: ['[daemon][p2p][delta][fuzz]'],
    suite: ['catch2', 'unit', 'daemon', 'p2p', 'fuzz-smoke'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 30
  )

  test('p2p_delta_large_history', catch2_p2p_delta_exe,
    args: ['direct P2P catch-up advances beyond 1024 retained operations'],
    suite: ['catch2', 'slow', 'daemon', 'p2p', 'network'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 1200,
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
      'YAMS_DISABLE_SESSION_WATCHER': '1',
      'YAMS_SKIP_MODEL_LOADING': '1',
      'YAMS_TESTING': '1',
    },
    timeout: 120,
    is_parallel: false
  )

  test('daemon_background_processing', catch2_daemon_background_exe,
    suite: ['catch2', 'unit', 'daemon', 'background'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('daemon_background_processing_slow', catch2_daemon_background_exe,
    suite: ['catch2', 'slow', 'daemon', 'background'],
    args: ['[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120
  )

  test('work_coordinator', catch2_work_coordinator_exe,
    suite: ['catch2', 'unit', 'daemon', 'work-coordinator'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('work_coordinator_slow', catch2_work_coordinator_exe,
    suite: ['catch2', 'slow', 'daemon', 'work-coordinator'],
    args: ['[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('assert_val', catch2_assert_val_exe,
    suite: ['catch2', 'unit', 'core', 'assert'],
    args: ['--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: host_machine.system() == 'windows' ? 60 : 30,
    is_parallel: false
  )

  test('search_service', catch2_search_service_exe,
    suite: ['catch2', 'unit', 'services', 'search'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_TEST_SAFE_SINGLE_INSTANCE': '1',
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_SQLITE_VEC_SKIP_INIT': '1',
      'YAMS_DISABLE_VECTORS': '1',
      'YAMS_TESTING': '1',
    },
    timeout: 180,
    is_parallel: false
  )

  test('daemon_plugin_harness', catch2_daemon_plugin_exe,
    suite: ['catch2', 'unit', 'daemon', 'plugin'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
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

    if host_machine.system() == 'darwin'
      test('gpu_provider_coreml', catch2_gpu_provider_exe,
        suite: ['catch2', 'unit', 'daemon', 'gpu'],
        env: {
          'LD_LIBRARY_PATH': _test_env_ld_library_path,
        },
        timeout: 30,
        is_parallel: true
      )
    endif
  endif

  test('plugin_infrastructure', catch2_plugin_infrastructure_exe,
    suite: ['catch2', 'unit', 'daemon', 'plugin', 'infrastructure'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60,
    is_parallel: false
  )

  test('plugin_infrastructure_slow', catch2_plugin_infrastructure_exe,
    suite: ['catch2', 'slow', 'daemon', 'plugin', 'infrastructure'],
    args: ['[slow]', '--allow-running-no-tests'],
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

  test('in_process_transport', catch2_in_process_transport_exe,
    suite: ['catch2', 'unit', 'daemon', 'streaming', 'inprocess'],
    args: ['--allow-running-no-tests'],  # Skips entirely on Windows
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('asio_transport_exhaustion', catch2_asio_transport_exhaustion_exe,
    suite: ['catch2', 'unit', 'daemon', 'transport', 'max-inflight'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false,
#Test skips on Windows(Unix domain sockets not supported)
    should_fail: host_machine.system() == 'windows'
  )

  test('asio_transport_inflight_close_ipc_prefix', catch2_asio_transport_inflight_close_ipc_prefix_exe,
    suite: ['catch2', 'unit', 'daemon', 'transport', 'ipc'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 120,
    is_parallel: false,
#Test skips on Windows(Unix domain sockets not supported)
    should_fail: host_machine.system() == 'windows'
  )

  test('daemon', catch2_daemon_exe,
    suite: ['catch2', 'unit', 'daemon', 'yams-3s4'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
#Disable ABI plugins to avoid TSAN + ONNX memory pressure causing SIGKILL
      'YAMS_DISABLE_ABI_PLUGINS': '1',
    },
    timeout: 120
  )

  test('tuning_registry_activity', catch2_tuning_registry_activity_exe,
    suite: ['catch2', 'slow', 'daemon', 'tune', 'registry', 'activity'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_TESTING': '1',
    },
    timeout: 120,
    is_parallel: false
  )

  test('socket_proxy_request_activity', catch2_socket_proxy_request_activity_exe,
    suite: ['catch2', 'slow', 'daemon', 'socket', 'proxy', 'activity'],
    env: {
      'LD_LIBRARY_PATH': _test_env_ld_library_path,
      'YAMS_SQLITE_MINIMAL_PRAGMAS': '1',
      'YAMS_SQLITE_BUSY_TIMEOUT_MS': '1000',
      'YAMS_TESTING': '1',
    },
    timeout: 120,
    is_parallel: false
  )

  if host_machine.system() != 'windows'
    test('socket_server_teardown', catch2_socket_server_teardown_exe,
      suite: ['catch2', 'unit', 'daemon', 'socket', 'shutdown'],
      env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
      timeout: 60
    )
  endif

  if onnx_dep.found()
    test('onnx_model_pool', catch2_onnx_model_pool_exe,
      suite: ['catch2', 'unit', 'daemon', 'onnx', 'pool'],
      env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
      timeout: 120,
    )
  endif

  test('memory_sync_apply_health', catch2_memory_sync_apply_health_exe,
    suite: ['catch2', 'unit', 'memory-sync'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('memory_sync', catch2_memory_sync_exe,
    suite: ['catch2', 'unit', 'memory-sync'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('content_blob_sync', catch2_content_blob_sync_exe,
    suite: ['catch2', 'unit', 'memory-sync'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('memory_sync_service', catch2_memory_sync_service_exe,
    suite: ['catch2', 'unit', 'memory-sync'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('memory_sync_config', catch2_memory_sync_config_exe,
    suite: ['catch2', 'unit', 'memory-sync'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('memory_sync_cli', catch2_memory_sync_cli_exe,
    suite: ['catch2', 'unit', 'memory-sync'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('storage_submodule', catch2_storage_submodule_exe,
    suite: ['catch2', 'unit', 'storage', 'yams-8b9'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300
  )

  test('storage_submodule_slow', catch2_storage_submodule_exe,
    suite: ['catch2', 'slow', 'storage', 'yams-8b9'],
    args: ['[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 300
  )

  test('wal_entry', catch2_wal_entry_exe,
    suite: ['catch2', 'unit', 'wal'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 60
  )

  test('benchmark_invocation', catch2_benchmark_invocation_exe,
    suite: ['catch2', 'unit', 'benchmark', 'harness'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    workdir: meson.project_source_root(),
    timeout: 30
  )

  heavy_submodule_timeout = (yams_sanitize_thread or yams_sanitize_address) ? 600 : 300

  test('compression_submodule', catch2_compression_submodule_exe,
    suite: ['catch2', 'unit', 'compression', 'yams-pmf'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: heavy_submodule_timeout
  )

  test('mcp_submodule', catch2_mcp_submodule_exe,
    suite: ['catch2', 'unit', 'mcp', 'yams-84g'],
    args: ['~[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  test('mcp_submodule_slow', catch2_mcp_submodule_exe,
    suite: ['catch2', 'slow', 'mcp', 'yams-84g'],
    args: ['[slow]', '--allow-running-no-tests'],
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: 180
  )

  cli_test_env = {
    'LD_LIBRARY_PATH': _test_env_ld_library_path,
    'TSAN_OPTIONS': _test_tsan_options,
    'YAMS_SOURCE_DIR': meson.project_source_root(),
    'MESON_BUILD_ROOT': meson.project_build_root(),
    'YAMS_DAEMON_SOCKET': meson.project_build_root() / 'cli-tests-no-daemon.sock',
    'YAMS_DAEMON_SOCKET_PATH': meson.project_build_root() / 'cli-tests-no-daemon.sock',
    'YAMS_CLI_DISABLE_DAEMON_AUTOSTART': '1',
  }

  test('cli_common', catch2_cli_common_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'common'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 120
  )

  test('cli_config', catch2_cli_config_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'config'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 180
  )

  test('cli_commands', catch2_cli_commands_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'commands'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 180
  )

  test('cli_model_plugin', catch2_cli_model_plugin_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'model-plugin'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 120
  )

  test('cli_doctor', catch2_cli_doctor_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'doctor'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 180
  )

  test('cli_graph', catch2_cli_graph_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'graph'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 120
  )

  test('cli_daemon', catch2_cli_daemon_exe,
    suite: ['catch2', 'unit', 'cli', 'yams-cli', 'daemon'],
    env: cli_test_env,
    depends: [yams_cli_exe, version_header],
    timeout: 120
  )

#Vector test suite(requires enable - vector - tests)
#Migrated from GTest as part of yams - 3s4 / yams - 1d4
  if get_option('enable-vector-tests')
    test('vector_submodule', catch2_vector_submodule_exe,
      suite: ['catch2', 'unit', 'vector', 'yams-1d4'],
      env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
      timeout: heavy_submodule_timeout
    )
  endif

endif

foreach t : catch2_test_table
  t_exe = executable(t.get('target', 'catch2_' + t['name']), t['sources'],
    include_directories: incs,
    dependencies: [catch2_dep, catch2_main_dep] + t['deps'],
    cpp_args: test_cpp_noerror_args + ['-DYAMS_TESTING=1'],
    build_rpath: ':'.join(_test_rpaths),
    install: false,
  )
  test(t['name'], t_exe,
    suite: t['suite'],
    args: t.get('args', []),
    env: { 'LD_LIBRARY_PATH': _test_env_ld_library_path },
    timeout: t['timeout'],
    is_parallel: t.get('is_parallel', true),
  )
endforeach
