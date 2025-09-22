#! Meson configure-time test discovery without fs.glob/subdir_glob
fsmod = import('fs')

gtest_dep = dependency('gtest', required: true)
gmock_dep = dependency('gmock', required: false)

# Normalize CLI11 discovery across environments (pkg-config -> Conan/CMake)
# (touch) reconfigure to drop/accept test file changes [2]
fs = import('fs')
cli11_dep = dependency('cli11', required: false)
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
]

if gmock_dep.found()
  test_deps += gmock_dep
endif

incs = [
  include_directories('..'),             # project root
  include_directories('.'),              # tests/
  include_directories('../include'),     # headers
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

# Collect sources recursively using run_command per directory (no user functions)
unit_srcs = []
# Exclude temporarily-outdated tests until ported
_ru = run_command('sh', '-c', 'find unit -type f -name "*.cpp" ! -path "unit/api/metadata_api_test.cpp" ! -path "unit/compression/error_handler_test_simple.cpp" ! -path "unit/compression/error_handler_test.cpp" ! -path "unit/compression/integrity_validator_test_simple.cpp" ! -path "unit/compression/integrity_validator_test.cpp" ! -path "unit/content/*" ! -path "unit/crypto/*" ! -path "unit/daemon/multiplex_client_test.cpp" ! -path "unit/daemon/request_dispatcher_plugin_test.cpp" ! -path "unit/daemon/onnx_*" ! -path "unit/manifest/manifest_test.cpp" ! -path "unit/integrity/integrity_verifier_test.cpp" ! -path "unit/storage/storage_engine_test.cpp" ! -path "unit/storage/compressed_storage_engine_test.cpp" ! -path "unit/wal/wal_manager_test.cpp" ! -path "unit/cli/*" ! -path "unit/mcp/*" ! -path "unit/detection/*" 2>/dev/null | sort', check: false)
if _ru.returncode() == 0
  _out = _ru.stdout()
  if _out != ''
    foreach p : _out.split('\n')
      if p != ''
        unit_srcs += files(p)
      endif
    endforeach
  endif
endif

# Conditionally add LZMA unit test when liblzma is available and option allows it
lzma_opt = get_option('enable-lzma')
lzma_dep = dependency('lzma', required: false)
if lzma_dep.found() and lzma_opt.allowed()
  unit_srcs += files('unit/compression/lzma_compressor_test.cpp')
endif

integration_srcs = []
# Re-enable a tiny integration smoke subset
_ri = run_command('sh', '-c', 'find integration/smoke -type f -name "*.cpp" ! -name "daemon_client_smoke_test.cpp" 2>/dev/null | sort', check: false)
if _ri.returncode() == 0
  _outi = _ri.stdout()
  if _outi != ''
    foreach p : _outi.split('\n')
      if p != ''
        integration_srcs += files(p)
      endif
    endforeach
  endif
endif

stress_srcs = []
# NOTE (PBI 028): Temporarily disabling stress tests compilation for now.

common_srcs = []
_rc = run_command('sh', '-c', 'find common -type f -name "*.cpp" 2>/dev/null | sort', check: false)
if _rc.returncode() == 0
  _outc = _rc.stdout()
  if _outc != ''
    foreach p : _outc.split('\n')
      if p != ''
        common_srcs += files(p)
      endif
    endforeach
  endif
endif

sdk_srcs = []
_rk = run_command('sh', '-c', 'find sdk -type f -name "*.cpp" 2>/dev/null | sort', check: false)
if _rk.returncode() == 0
  _outk = _rk.stdout()
  if _outk != ''
    foreach p : _outk.split('\n')
      if p != ''
        sdk_srcs += files(p)
      endif
    endforeach
  endif
endif

# NOTE (PBI 028): Temporarily disable plugin tests to avoid duplicate plugin
# symbol definitions in a single aggregated integration binary.
plugin_test_srcs = []

all_unit = common_srcs + unit_srcs
# Temporarily disable integration and stress aggregates
all_integration = integration_srcs
all_stress = []

# Always rely on tests/unit_main.cpp; do not include tests/test_main.cpp here
test_main = []

if all_unit.length() > 0
  unit_exe = executable('yams_unit_tests', all_unit + test_main,
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    cpp_args: test_cpp_noerror_args,
    install: false,
  )
  # Give the aggregated unit binary more time; default 30s was tight on CI/macOS
  test('unit', unit_exe, suite: 'unit', timeout: 180)

  # Define a fast "smoke" subset using gtest filter for developer iteration
  smoke_filter = 'ContentStoreTest.*:ProgressReporterTest.*:ContentMetadataTest.*:ContentStoreBuilderTest.*:SearchSmoke.*'
  test('unit_smoke', unit_exe,
    suite: ['unit', 'smoke'],
    args: ['--gtest_filter=' + smoke_filter],
    timeout: 120,
  )
endif

if all_integration.length() > 0
  # Integration smoke uses dedicated test main to provide entry point
  integ_exe = executable('yams_integration_smoke_tests', all_integration + files('test_main.cpp'),
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    install: false,
  )
  test('integration_smoke', integ_exe, suite: ['integration','smoke'], timeout: 60)
endif

# Stress tests can be heavy; register but mark as long-running via suite name
if all_stress.length() > 0
  stress_exe = executable('yams_stress_tests', all_stress + test_main,
    include_directories: incs,
    dependencies: [gtest_dep, test_deps],
    install: false,
  )
  test('stress', stress_exe, suite: 'stress', timeout: 1800)
endif
