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

# --- Install prefix for plugin auto-trust ---
# Pass the install prefix to C++ code so the daemon knows where system plugins are installed
install_prefix = get_option('prefix')
add_project_arguments('-DYAMS_INSTALL_PREFIX="' + install_prefix + '"', language: 'cpp')
message('YAMS install prefix: ' + install_prefix)

onnx_opt = get_option('enable-onnx')

# Enforce sqlite3 FTS5 presence at configure time (configurable)
require_fts5 = get_option('require-fts5')
sqlite_dep = dependency('sqlite3', required: true)
fts5_prog = cpp.run(files('meson/fts5_check.cpp'), dependencies: sqlite_dep, name: 'sqlite3 FTS5 check')
if fts5_prog.returncode() != 0
  msg = 'SQLite3 built without FTS5 support. Enable FTS5 in your package manager or Conan (e.g., -o "sqlite3/*:fts5=True").'
  if require_fts5
    error(msg)
  else
    warning(msg)
  endif
endif

# Boost is a core requirement (e.g., Boost.Asio for daemon communication).
# Conan's PkgConfigDeps often doesn't emit boost.pc, so prefer CMake config.
boost_dep = dependency('boost', modules: ['system', 'thread'], method: 'cmake', required: true)
meson.override_dependency('boost', boost_dep)

# --- Version header generation ---
_override_version = get_option('yams-version')
_effective_version = _override_version != '' ? _override_version : meson.project_version()
_build_timestamp = run_command('date', '-u', '+%Y-%m-%dT%H:%M:%SZ', check: true).stdout().strip()
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
yams_enable_tui = get_option('enable-tui')

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
subdir('src/integrity')
subdir('src/config')
subdir('src/metadata')
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

summary({
  'project': meson.project_name(),
  'version': meson.project_version(),
  'cpp_std': get_option('cpp_std'),
  'build_cli': yams_build_cli,
  'build_mcp_server': yams_build_mcp,
  'enable_tui': yams_enable_tui,
}, section: 'YAMS Meson configuration', bool_yn: true)
