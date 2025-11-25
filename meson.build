project('yams', ['cpp', 'c'],
  version: '0.0.0',
  default_options: [
    'buildtype=release',
    'warning_level=2',
    'cpp_std=c++20',
    'c_std=c11',
  ],
)

fs = import('fs')
pkg = import('pkgconfig')

if host_machine.system() == 'darwin'
  add_global_link_arguments('-Wl,-no_warn_duplicate_libraries', language: 'cpp')
endif

if host_machine.system() == 'windows'
  add_project_arguments('-D_WIN32_WINNT=0x0A00', language: 'cpp')
  add_project_arguments('-D_CRT_SECURE_NO_WARNINGS', language: 'cpp')
  add_project_arguments('-D_SILENCE_CXX20_OLD_SHARED_PTR_ATOMIC_SUPPORT_DEPRECATION_WARNING', language: 'cpp')
  add_project_arguments('-DNOMINMAX', language: 'cpp')
  add_project_arguments('-DWIN32_LEAN_AND_MEAN', language: 'cpp')
endif

# --- libc++ hardening mode ---
# Apply hardening defines when using libc++ (Darwin always uses libc++, Linux when explicitly set)
cpp = meson.get_compiler('cpp')
libcxx_hardening = get_option('libcxx-hardening')

if libcxx_hardening != 'none'
  # Check if we're actually using libc++
  using_libcxx = false
  if host_machine.system() == 'darwin'
    # macOS always uses libc++
    using_libcxx = true
  elif cpp.get_id() == 'clang' or cpp.get_id() == 'clang-cl'
    # On non-Darwin, check if -stdlib=libc++ is in use
    # This is a heuristic; proper detection would check link flags
    has_libcxx = cpp.has_header('__config', prefix: '#include <ciso646>')
    if has_libcxx
      using_libcxx = true
    endif
  endif
  
  if using_libcxx
    hardening_mode_map = {
      'fast': '_LIBCPP_HARDENING_MODE_FAST',
      'extensive': '_LIBCPP_HARDENING_MODE_EXTENSIVE',
      'debug': '_LIBCPP_HARDENING_MODE_DEBUG',
    }
    hardening_define = hardening_mode_map.get(libcxx_hardening)
    if hardening_define != ''
      add_project_arguments('-D' + hardening_define, language: 'cpp')
      message('Enabling libc++ hardening mode: ' + libcxx_hardening + ' (' + hardening_define + ')')
    endif
  else
    warning('libc++ hardening requested but not using libc++ (detected: ' + cpp.get_id() + '). Hardening mode ignored.')
  endif
endif

# --- ThreadSanitizer for race detection ---
enable_tsan = get_option('enable-tsan')
if enable_tsan
  buildtype = get_option('buildtype')
  # TSan is most useful in debug builds; warn if enabled in release
  if buildtype == 'release' or buildtype == 'plain'
    warning('ThreadSanitizer enabled in non-debug build. This will significantly impact performance.')
  endif
  
  tsan_flags = [
    '-fsanitize=thread',
    '-g',  # Debug symbols for better stack traces
    '-fno-omit-frame-pointer',  # Better stack traces
  ]
  
  # Check if compiler supports TSan
  if cpp.has_argument('-fsanitize=thread')
    add_project_arguments(tsan_flags, language: 'cpp')
    add_project_arguments(tsan_flags, language: 'c')
    add_project_link_arguments(['-fsanitize=thread'], language: 'cpp')
    add_project_link_arguments(['-fsanitize=thread'], language: 'c')
    message('ThreadSanitizer enabled - expect significant performance impact')
    message('Note: TSan requires all code (including dependencies) to be compiled with -fsanitize=thread')
  else
    error('ThreadSanitizer requested but compiler does not support -fsanitize=thread')
  endif
endif

# --- Install prefix for plugin auto-trust ---
# Pass the install prefix to C++ code so the daemon knows where system plugins are installed
install_prefix = get_option('prefix')
# Escape backslashes for Windows paths to avoid compiler warnings about unrecognized escape sequences
install_prefix_escaped = install_prefix.replace('\\', '\\\\')
add_project_arguments('-DYAMS_INSTALL_PREFIX="' + install_prefix_escaped + '"', language: 'cpp')
message('YAMS install prefix: ' + install_prefix)

# --- Windows DLL deployment ---
# On Windows, DLLs must be in the same directory as the executable or in PATH.
# We extract the DLL paths from the dependency's pkg-config `bindir` variable,
# then use fs.copyfile() to deploy them to the build directory.
#
# This is the Meson-idiomatic approach that doesn't rely on Conan's runtime_deploy.
windows_runtime_dlls = []  # List of [dll_name, dll_full_path] tuples
if host_machine.system() == 'windows'
  # Get TBB dependency and extract bindir
  tbb_dep = dependency('tbb', required: false)
  if tbb_dep.found()
    tbb_prefix = tbb_dep.get_variable(pkgconfig: 'prefix', default_value: '')
    tbb_bindir = tbb_dep.get_variable(pkgconfig: 'bindir', default_value: '')
    if tbb_bindir != ''
      # Resolve ${prefix} placeholder if present
      tbb_bindir = tbb_bindir.replace('${prefix}', tbb_prefix)
      foreach dll_name : ['tbb12.dll', 'tbbbind_2_5.dll', 'tbbmalloc.dll', 'tbbmalloc_proxy.dll']
        dll_path = tbb_bindir / dll_name
        if fs.exists(dll_path)
          windows_runtime_dlls += [[dll_name, dll_path]]
        endif
      endforeach
    endif
  endif

  # Get ONNX Runtime dependency and extract bindir
  onnx_dep_check = dependency('onnxruntime', required: false)
  if onnx_dep_check.found()
    onnx_prefix = onnx_dep_check.get_variable(pkgconfig: 'prefix', default_value: '')
    onnx_bindir = onnx_dep_check.get_variable(pkgconfig: 'bindir', default_value: '')
    if onnx_bindir != ''
      # Resolve ${prefix} placeholder if present
      onnx_bindir = onnx_bindir.replace('${prefix}', onnx_prefix)
      foreach dll_name : ['onnxruntime.dll', 'onnxruntime_providers_shared.dll']
        dll_path = onnx_bindir / dll_name
        if fs.exists(dll_path)
          windows_runtime_dlls += [[dll_name, dll_path]]
        endif
      endforeach
    endif
  endif

  # Log discovered DLLs
  foreach dll_info : windows_runtime_dlls
    message('Found runtime DLL: ' + dll_info[0] + ' at ' + dll_info[1])
  endforeach
endif

onnx_opt = get_option('enable-onnx')

# Enforce sqlite3 FTS5 presence at configure time (configurable)
require_fts5 = get_option('require-fts5')
sqlite_dep = dependency('sqlite3', required: true)
# Skip runtime test when cross-compiling
if meson.is_cross_build()
  warning('Cross-compiling: skipping SQLite FTS5 runtime check. Ensure FTS5 is enabled in your target SQLite build.')
else
  fts5_prog = cpp.run(files('meson/fts5_check.cpp'), dependencies: sqlite_dep, name: 'sqlite3 FTS5 check')
  if fts5_prog.returncode() != 0
    msg = 'SQLite3 built without FTS5 support. Enable FTS5 in your package manager or Conan (e.g., -o "sqlite3/*:fts5=True").'
    if require_fts5
      error(msg)
    else
      warning(msg)
    endif
  endif
endif

# Boost is a core requirement (e.g., Boost.Asio for daemon communication).
# Conan's PkgConfigDeps often doesn't emit boost.pc, so prefer CMake config.
boost_dep = dependency('boost', modules: ['system', 'thread'], method: 'cmake', required: true)
meson.override_dependency('boost', boost_dep)

# --- Version header generation ---
_override_version = get_option('yams-version')
_effective_version = _override_version != '' ? _override_version : meson.project_version()

python_exe = find_program('python3', 'python')
_build_timestamp = run_command(python_exe, '-c', 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))', check: true).stdout().strip()

_git_desc = ''
_git_cmd = find_program('git', required: false)
if _git_cmd.found()
  # Capture short git describe (best-effort; ignore errors quietly)
  _r = run_command(_git_cmd, ['describe', '--tags', '--always', '--dirty'], check: false)
  if _r.returncode() == 0
    _git_desc = _r.stdout().strip()
  endif
endif

version_config = configuration_data()
version_config.set('YAMS_EFFECTIVE_VERSION', _effective_version)
version_config.set('YAMS_PROJECT_VERSION', meson.project_version())
version_config.set('YAMS_OVERRIDE_VERSION', _override_version)
version_config.set('YAMS_BUILD_TIMESTAMP', _build_timestamp)
version_config.set('YAMS_GIT_DESCRIBE', _git_desc)

configure_file(
  output: 'version_generated.h',
  install: true,
  install_dir: get_option('includedir') / 'yams',
  configuration: version_config,
  format: 'cmake', # Simple @VAR@ substitution; header template below.
  input: files('include/yams/version_generated.h.in')
)

yams_build_cli = get_option('build-cli')
yams_build_mcp = get_option('build-mcp-server')

# Install public headers under the compiler's include prefix without duplicating
# the top-level 'include' directory (was producing prefix/include/include/...).
install_subdir('include', install_dir: get_option('includedir'), strip_directory: true)

# Core libraries (ordered by dependency)
subdir('src/core')
subdir('src/crypto')
subdir('src/chunking')
subdir('src/compression')
subdir('src/storage')
subdir('src/wal')
subdir('src/manifest')
subdir('src/metadata')
subdir('src/integrity')
subdir('src/config')
subdir('src/extraction')
subdir('src/detection')
subdir('src/content')
subdir('src/downloader')
subdir('src/indexing')
subdir('src/genai')
subdir('src/vector')
subdir('src/search')
subdir('src/api')
subdir('src/ingest')
subdir('src/repair')
subdir('src/daemon/client')
subdir('src/app/services')
subdir('src/daemon')
subdir('src/mobile')
subdir('src/benchmarks')

if get_option('build-mcp-server')
  subdir('src/mcp')
endif

if get_option('build-plugins')
  subdir('plugins')
endif

if get_option('build-cli')
  subdir('src/cli')
  subdir('tools/yams-cli')
  if get_option('build-mcp-server')
    subdir('tools/yams-mcp')
  endif
endif

# Tests (enabled by default in Debug builds)
_tests_opt = get_option('build-tests')
_buildtype = get_option('buildtype')
_tests_enabled = _tests_opt or (_buildtype == 'debug')
if _tests_enabled
  subdir('tests')
endif

# Fuzzing harnesses (AFL++/libFuzzer)
if get_option('build-fuzzers')
  subdir('tools/fuzzing')
endif

summary({
  'project': meson.project_name(),
  'version': meson.project_version(),
  'cpp_std': get_option('cpp_std'),
  'build_cli': yams_build_cli,
  'build_mcp_server': yams_build_mcp,
}, section: 'YAMS Meson configuration', bool_yn: true)

if get_option('b_coverage')
  gcovr = find_program('gcovr', required: false)
  if gcovr.found()
    coverage_report_target = custom_target('coverage-report',
      output: 'coverage-report.stamp',
      command: [
        gcovr,
        '--root', meson.project_source_root(),
        '--filter', join_paths(meson.project_source_root(), 'src'),
        '--filter', join_paths(meson.project_source_root(), 'include'),
        '--exclude', join_paths(meson.project_source_root(), 'src', 'benchmarks'),
        '--html', '--html-details',
        '--output', join_paths(meson.project_build_root(), 'coverage', 'index.html'),
        meson.project_build_root(),
      ],
      build_by_default: false,
      build_always_stale: true,
    )

    coverage_txt_target = custom_target('coverage-summary',
      output: 'coverage.txt',
      command: [
        gcovr,
        '--root', meson.project_source_root(),
        '--filter', join_paths(meson.project_source_root(), 'src'),
        '--filter', join_paths(meson.project_source_root(), 'include'),
        '--exclude', join_paths(meson.project_source_root(), 'src', 'benchmarks'),
        '--print-summary',
        '--output', '@OUTPUT@',
        meson.project_build_root(),
      ],
      build_by_default: false,
      build_always_stale: true,
    )

    message('Coverage reporting configured. Run tests then: ninja -C builddir coverage-report')
  else
    warning('gcovr not found. Install gcovr to generate coverage reports.')
  endif
endif

# Note: Runtime DLLs are now deployed via Conan's runtime_deploy deployer during conan install
# No custom meson install script needed

