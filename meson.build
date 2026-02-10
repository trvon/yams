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

cpp = meson.get_compiler('cpp')

wasi_enabled = get_option('wasi')
if wasi_enabled
  add_project_arguments('-DYAMS_WASI=1', language: 'cpp')
endif

_yams_include_dir = join_paths(meson.current_source_dir(), 'include')
if cpp.get_id() == 'msvc' or cpp.get_id() == 'clang-cl'
  add_project_arguments('/I' + _yams_include_dir, language: 'cpp')
else
  add_project_arguments('-I' + _yams_include_dir, language: 'cpp')
endif

if host_machine.system() == 'darwin'
  add_global_link_arguments('-Wl,-no_warn_duplicate_libraries', language: 'cpp')
endif

if host_machine.system() == 'windows'
  add_project_arguments('-D_WIN32_WINNT=0x0A00', language: 'cpp')
  add_project_arguments('-D_CRT_SECURE_NO_WARNINGS', language: 'cpp')
  add_project_arguments('-D_SILENCE_CXX20_OLD_SHARED_PTR_ATOMIC_SUPPORT_DEPRECATION_WARNING', language: 'cpp')
  add_project_arguments('-DNOMINMAX', language: 'cpp')
  add_project_arguments('-DWIN32_LEAN_AND_MEAN', language: 'cpp')
  # Large source files like mcp_server.cpp exceed MSVC's default section limit
  add_project_arguments('/bigobj', language: 'cpp')
endif

# --- libc++ hardening mode ---
# Apply hardening defines when using libc++ (Darwin always uses libc++, Linux when explicitly set)
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

  # Check if compiler supports TSan
  if cpp.has_argument('-fsanitize=thread')
    # Use additional flags for better stack traces (sanitizer is set via b_sanitize option)
    tsan_extra_flags = ['-fno-omit-frame-pointer']
    add_project_arguments(tsan_extra_flags, language: 'cpp')
    add_project_arguments(tsan_extra_flags, language: 'c')
    message('ThreadSanitizer enabled - expect significant performance impact')
    message('Note: TSan requires all code (including dependencies) to be compiled with -fsanitize=thread')
  else
    error('ThreadSanitizer requested but compiler does not support -fsanitize=thread')
  endif
endif

# --- C++20 Modules Support (Experimental) ---
# Detect and configure C++20 module compilation when enabled
yams_modules_enabled = get_option('enable-modules')
yams_modules_supported = false
yams_module_args = []
yams_module_flags = []
yams_use_modules_def = []
yams_module_cpp_args = []

if yams_modules_enabled
  # Include module detection and configuration
  subdir('meson')
endif

# --- Install prefix for plugin auto-trust ---
# Pass the install prefix to C++ code so the daemon knows where system plugins are installed
install_prefix = get_option('prefix')
# Escape backslashes for Windows paths to avoid compiler warnings about unrecognized escape sequences
install_prefix_escaped = install_prefix.replace('\\', '\\\\')
add_project_arguments('-DYAMS_INSTALL_PREFIX="' + install_prefix_escaped + '"', language: 'cpp')
message('YAMS install prefix: ' + install_prefix)

base_build = meson.project_build_root()
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

  # Get ONNX Runtime dependency and extract bindir (prefer Conan CMake config)
  onnx_dep_check = dependency('onnxruntime', method: 'cmake', cmake_module_path: onnx_cmake_paths, required: false)
  if not onnx_dep_check.found()
    onnx_dep_check = dependency('onnxruntime', required: false)
  endif

  onnx_bindir_candidates = []
  if onnx_dep_check.found()
    onnx_prefix = onnx_dep_check.get_variable(pkgconfig: 'prefix', default_value: '')
    onnx_bindir = onnx_dep_check.get_variable(pkgconfig: 'bindir', default_value: '')
    if onnx_bindir != ''
      onnx_bindir = onnx_bindir.replace('${prefix}', onnx_prefix)
      onnx_bindir_candidates += [onnx_bindir]
    endif

    # CMake configs from Conan often expose PACKAGE_PREFIX_DIR; fall back to <prefix>/bin
    cmake_prefix = onnx_dep_check.get_variable(cmake: 'PACKAGE_PREFIX_DIR', default_value: '')
    if cmake_prefix != ''
      onnx_bindir_candidates += [join_paths(cmake_prefix, 'bin')]
    elif onnx_prefix != ''
      onnx_bindir_candidates += [join_paths(onnx_prefix, 'bin')]
    endif
  endif

  # As a last resort, probe the Conan cache directly (~/.conan2/p/**/p/bin) via a tiny Python helper
  py_mod = import('python')
  python3 = py_mod.find_installation('python3')
  conan_probe = run_command(python3, ['-c', '''import pathlib, os
root = pathlib.Path(os.path.expanduser("~/.conan2/p"))
dirs = []
if root.exists():
    for child in root.iterdir():
        bin_dir = child / "p" / "bin"
        if bin_dir.exists():
            dll = bin_dir / "onnxruntime.dll"
            if dll.exists():
                dirs.append(str(bin_dir))
print(";".join(dirs))'''], check: false)
  if conan_probe.returncode() == 0
    conan_dirs = conan_probe.stdout().strip().split(';')
    foreach d : conan_dirs
      if d != ''
        onnx_bindir_candidates += [d]
      endif
    endforeach
  endif

  foreach bindir : onnx_bindir_candidates
    foreach dll_name : ['onnxruntime.dll', 'onnxruntime_providers_shared.dll']
      dll_path = bindir / dll_name
      if fs.exists(dll_path)
        windows_runtime_dlls += [[dll_name, dll_path]]
      endif
    endforeach
  endforeach

  # Deduplicate DLL entries by name to avoid duplicate targets
  dedup_map = {}
  deduped = []
  foreach dll_info : windows_runtime_dlls
    name = dll_info[0]
    if not dedup_map.has_key(name)
      dedup_map += {name: true}
      deduped += [dll_info]
    endif
  endforeach
  windows_runtime_dlls = deduped

  # Log discovered DLLs
  foreach dll_info : windows_runtime_dlls
    message('Found runtime DLL: ' + dll_info[0] + ' at ' + dll_info[1])
  endforeach
endif

onnx_opt = get_option('enable-onnx')

if not wasi_enabled
  # Database backend selection (libsql or sqlite)
  db_backend = get_option('database-backend')
  require_fts5 = get_option('require-fts5')

  # Try to find the selected database backend
  using_libsql = false
  if db_backend == 'libsql'
    # Try to find libsql (provides sqlite3-compatible API)
    # 1. First try system-installed libsql via pkg-config
    libsql_dep = dependency('libsql', required: false)
    if not libsql_dep.found()
      libsql_dep = dependency('libsql-sqlite3', required: false)
    endif

    # 2. If not found, try building from subproject (requires Rust toolchain)
    if not libsql_dep.found()
      cargo_prog = find_program('cargo', required: false)
      if cargo_prog.found()
        message('libSQL not found on system, building from subproject (requires Rust)...')
        libsql_subproj = subproject('libsql', required: false)
        if libsql_subproj.found()
          libsql_dep = libsql_subproj.get_variable('libsql_dep')
        endif
      endif
    endif

    if libsql_dep.found()
      sqlite_dep = libsql_dep
      using_libsql = true
      message('Using libSQL database backend (MVCC concurrent writes enabled)')
    else
      warning('libSQL not found and subproject build failed. Falling back to standard SQLite3.')
      warning('To use libSQL: install Rust toolchain (rustup) or system libsql package.')
      sqlite_dep = dependency('sqlite3', required: true)
    endif
  else
    sqlite_dep = dependency('sqlite3', required: true)
    message('Using standard SQLite3 database backend')
  endif

  # Add compile definition for backend detection
  if using_libsql
    add_project_arguments('-DYAMS_LIBSQL_BACKEND=1', language: 'cpp')
  else
    add_project_arguments('-DYAMS_LIBSQL_BACKEND=0', language: 'cpp')
  endif

  # Skip runtime test when cross-compiling or using subproject sqlite (internal dep can't be used with cpp.run in newer meson)
  if meson.is_cross_build()
    warning('Cross-compiling: skipping SQLite FTS5 runtime check. Ensure FTS5 is enabled in your target SQLite build.')
  elif sqlite_dep.type_name() == 'internal'
    warning('Using subproject sqlite3: skipping FTS5 runtime check. Subproject sqlite3 has FTS5 enabled by default.')
  elif using_libsql
    message('Using libSQL: skipping FTS5 runtime check. libSQL includes FTS5 support.')
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
endif

# Boost is a core requirement (e.g., Boost.Asio for daemon communication).
# Conan's PkgConfigDeps often doesn't emit boost.pc, so prefer CMake config.
boost_dep = dependency('boost', modules: ['system', 'thread'], method: 'cmake', required: true)
meson.override_dependency('boost', boost_dep)

# --- Version header generation ---
_override_version = get_option('yams-version')

# Detect version from git tags if not overridden
_git_version = ''
_git_cmd = find_program('git', required: false)
if _git_cmd.found() and _override_version == ''
  # Try to get the most recent semver tag (v*)
  _r = run_command(_git_cmd, ['describe', '--tags', '--match', 'v*', '--abbrev=0'], check: false)
  if _r.returncode() == 0
    _tag = _r.stdout().strip()
    # Strip leading 'v' if present
    if _tag.startswith('v')
      _git_version = _tag.substring(1)
    else
      _git_version = _tag
    endif
  endif
endif

# Priority: command-line override > git tag > meson.project_version()
if _override_version != ''
  _effective_version = _override_version
elif _git_version != ''
  _effective_version = _git_version
else
  _effective_version = meson.project_version()
endif

python_exe = find_program('python3', 'python')
_build_timestamp = run_command(python_exe, '-c', 'import datetime; print(datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))', check: true).stdout().strip()

_git_desc = ''
_git_commit = ''
_git_cmd = find_program('git', required: false)
if _git_cmd.found()
  # Capture short git describe (best-effort; ignore errors quietly)
  _r = run_command(_git_cmd, ['describe', '--tags', '--always', '--dirty'], check: false)
  if _r.returncode() == 0
    _git_desc = _r.stdout().strip()
  endif
  # Capture short commit hash for dev builds
  _r = run_command(_git_cmd, ['rev-parse', '--short', 'HEAD'], check: false)
  if _r.returncode() == 0
    _git_commit = _r.stdout().strip()
  endif
endif

version_config = configuration_data()
version_config.set('YAMS_EFFECTIVE_VERSION', _effective_version)
version_config.set('YAMS_PROJECT_VERSION', meson.project_version())
version_config.set('YAMS_OVERRIDE_VERSION', _override_version)
version_config.set('YAMS_BUILD_TIMESTAMP', _build_timestamp)
version_config.set('YAMS_GIT_DESCRIBE', _git_desc)
version_config.set('YAMS_GIT_COMMIT', _git_commit)

# Generate version header - will be at builddir/version_generated.h
# CLI includes it via builddir in include path
version_header = configure_file(
  output: 'version_generated.h',
  install: true,
  install_dir: get_option('includedir') / 'yams',
  configuration: version_config,
  # Header template uses @VAR@ tokens.
  format: 'meson',
  input: files('include/yams/version_generated.h.in')
)

# Make builddir available as include path for generated headers
# CLI will include as "version_generated.h" directly
generated_inc = include_directories('.')

yams_build_cli = get_option('build-cli') and not wasi_enabled
yams_build_mcp = get_option('build-mcp-server')

# Install public headers under the compiler's include prefix without duplicating
# the top-level 'include' directory (was producing prefix/include/include/...).
install_subdir('include', install_dir: get_option('includedir'), strip_directory: true)

# --- tl-expected global resolution ---
# tl-expected is a header-only library. Homebrew installs it without pkgconfig.
# Resolve it once here and override for all subdirs.
_tl_expected_dep = dependency('tl-expected', method: 'cmake', required: false)
if not _tl_expected_dep.found()
  _tl_expected_dep = dependency('tl-expected', method: 'pkg-config', required: false)
endif
if not _tl_expected_dep.found()
  # Manual fallback for homebrew installation
  _tl_expected_inc = include_directories('/opt/homebrew/include', is_system: true)
  _tl_expected_dep = declare_dependency(include_directories: _tl_expected_inc)
endif
meson.override_dependency('tl-expected', _tl_expected_dep)

if wasi_enabled
  # WASI profile: compile a minimal subset (no daemon IPC, no plugins, no downloads).
  # Keep this intentionally small so cross-compilers don't need system libs like
  # OpenSSL/libcurl/sqlite/onnx.
  subdir('src/core')

  if get_option('build-mcp-server')
    subdir('src/mcp')
  endif
else
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
  if get_option('build-plugins')
    subdir('src/plugins')
  endif
  subdir('src/indexing')
  subdir('src/genai')
  subdir('src/daemon/client')
  subdir('src/vector')

  # Third-party libraries (must come before src/search which depends on symspell)
  subdir('third_party/symspell')

  subdir('src/search')
  subdir('src/api')
  subdir('src/ingest')
  subdir('src/repair')
  subdir('src/app/services')
  subdir('src/daemon')
  if get_option('enable-mobile-bindings')
    subdir('src/mobile')
  endif
  if get_option('build-benchmarks')
    subdir('src/benchmarks')
  endif

  if get_option('build-mcp-server')
    subdir('src/mcp')
  endif
endif

if get_option('build-plugins') and not wasi_enabled
  subdir('plugins')
endif

if get_option('build-cli') and not wasi_enabled
  subdir('src/cli')
  subdir('tools/yams-cli')
  if get_option('build-mcp-server')
    subdir('tools/yams-mcp')
  endif
endif

# Tests (enabled by default in Debug builds)
_tests_opt = get_option('build-tests')
_buildtype = get_option('buildtype')
_tests_enabled = (not wasi_enabled) and (_tests_opt or (_buildtype == 'debug' and not get_option('build-fuzzers')))
if _tests_enabled
  subdir('tests')
endif

# Fuzzing harnesses (AFL++/libFuzzer)
if get_option('build-fuzzers') and not wasi_enabled
  subdir('tools/fuzzing')
endif

sql_install_dir = join_paths(get_option('datadir'), 'yams', 'sql')
install_data('sql/reference_schema.sql', install_dir: sql_install_dir)

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
    _cpp_compiler = meson.get_compiler('cpp')
    _gcov_executable = 'gcov'
    if _cpp_compiler.get_id() == 'clang'
      _llvm_cov = find_program('llvm-cov', required: false)
      if _llvm_cov.found()
        _gcov_executable = 'llvm-cov gcov -p'
      endif
    endif

    coverage_report_target = custom_target('coverage-report',
      output: 'coverage-report.stamp',
      command: [
        gcovr,
        '--root', meson.project_source_root(),
        '--gcov-executable', _gcov_executable,
        '--object-directory', meson.project_build_root(),
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
        '--gcov-executable', _gcov_executable,
        '--object-directory', meson.project_build_root(),
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

    message('Coverage reporting configured. Run tests then: ninja -C builddir coverage.txt && ninja -C builddir coverage-report.stamp')
  else
    warning('gcovr not found. Install gcovr to generate coverage reports.')
  endif
endif

# Note: Runtime DLLs are now deployed via Conan's runtime_deploy deployer during conan install
# No custom meson install script needed
