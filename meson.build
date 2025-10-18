project('yams', 'cpp', 'c',
  version: '0.8.0',
  default_options: [
    'buildtype=release',
    'warning_level=3',
    'cpp_std=c++23',  # Request C++23 (fallback to C++20 via feature detection)
    'c_std=c11',
  ],
)

fs = import('fs')
pkg = import('pkgconfig')

# Define install prefix for runtime plugin discovery
prefix = get_option('prefix')
add_project_arguments('-DYAMS_INSTALL_PREFIX="' + prefix + '"', language: 'cpp')

if host_machine.system() == 'darwin'
  add_global_link_arguments('-Wl,-no_warn_duplicate_libraries', language: 'cpp')
endif

onnx_opt = get_option('enable-onnx')

# Enforce sqlite3 FTS5 presence at configure time (configurable)
require_fts5 = get_option('require-fts5')
sqlite_dep = dependency('sqlite3', required: true)
cpp = meson.get_compiler('cpp')

# ============================================================================
# C++23 Feature Detection
# ============================================================================
# Detect C++23 constexpr container support
# This allows us to use constexpr std::vector, std::string in C++23 while
# maintaining fallback compatibility with C++20 compilers
has_constexpr_vector = cpp.compiles('''
    #include <vector>
    constexpr int test_constexpr_vector() {
        std::vector<int> v = {1, 2, 3, 4, 5};
        v.push_back(6);
        return v.size();
    }
    int main() { 
        constexpr int size = test_constexpr_vector();
        return size == 6 ? 0 : 1;
    }
    ''',
    name: 'C++23 constexpr std::vector support',
    args: cpp.get_id() == 'msvc' ? ['/std:c++latest'] : []
)

has_constexpr_string = cpp.compiles('''
    #include <string>
    constexpr bool test_constexpr_string() {
        std::string s = "hello";
        s += " world";
        return s.size() == 11;
    }
    int main() { 
        constexpr bool result = test_constexpr_string();
        return result ? 0 : 1;
    }
    ''',
    name: 'C++23 constexpr std::string support',
    args: cpp.get_id() == 'msvc' ? ['/std:c++latest'] : []
)

# Check actual C++ standard being used
cpp_std_version = cpp.get_define('__cplusplus')
message('C++ standard macro __cplusplus: @0@'.format(cpp_std_version))

# Report feature detection results
if has_constexpr_vector
    message('✓ C++23 constexpr std::vector detected')
    add_project_arguments('-DYAMS_HAS_CONSTEXPR_VECTOR=1', language: 'cpp')
else
    message('✗ C++23 constexpr std::vector not available (using C++20 fallback)')
    add_project_arguments('-DYAMS_HAS_CONSTEXPR_VECTOR=0', language: 'cpp')
endif

if has_constexpr_string
    message('✓ C++23 constexpr std::string detected')
    add_project_arguments('-DYAMS_HAS_CONSTEXPR_STRING=1', language: 'cpp')
else
    message('✗ C++23 constexpr std::string not available (using C++20 fallback)')
    add_project_arguments('-DYAMS_HAS_CONSTEXPR_STRING=0', language: 'cpp')
endif

# Add unified flag for all C++23 constexpr container features
if has_constexpr_vector and has_constexpr_string
    message('✓ Full C++23 constexpr containers support enabled')
    add_project_arguments('-DYAMS_HAS_CONSTEXPR_CONTAINERS=1', language: 'cpp')
else
    message('ℹ Using C++20 mode (constexpr containers unavailable)')
    add_project_arguments('-DYAMS_HAS_CONSTEXPR_CONTAINERS=0', language: 'cpp')
endif

# Detect compiler version for documentation
cpp_id = cpp.get_id()
cpp_version = cpp.version()
message('Compiler: @0@ @1@'.format(cpp_id, cpp_version))

# Warn if using older compiler that doesn't support C++23
if not has_constexpr_vector
    if cpp_id == 'gcc' and cpp_version.version_compare('<13.0')
        warning('GCC 13+ recommended for full C++23 support (detected: @0@)'.format(cpp_version))
    elif cpp_id == 'clang' and cpp_version.version_compare('<16.0')
        warning('Clang 16+ recommended for full C++23 support (detected: @0@)'.format(cpp_version))
    elif cpp_id == 'msvc' and cpp_version.version_compare('<19.33')
        warning('MSVC 19.33+ (VS 2022 17.3+) recommended for full C++23 support (detected: @0@)'.format(cpp_version))
    endif
endif
# ============================================================================

# Optional LLD linker support (preferred with Clang, optional with GCC)
use_lld = get_option('use-lld')
lld_available = cpp.has_link_argument('-fuse-ld=lld')

if use_lld.enabled() and not lld_available
  error('LLD linker was requested but is not available')
elif use_lld.auto() and cpp.get_id() == 'clang' and lld_available
  # Auto-enable LLD with Clang
  add_project_link_arguments('-fuse-ld=lld', language: 'cpp')
  add_project_link_arguments('-fuse-ld=lld', language: 'c')
  message('Using LLD linker (auto-enabled with Clang)')
elif use_lld.enabled() and lld_available
  # Explicitly enabled
  add_project_link_arguments('-fuse-ld=lld', language: 'cpp')
  add_project_link_arguments('-fuse-ld=lld', language: 'c')
  message('Using LLD linker (explicitly enabled)')
elif use_lld.auto() and not lld_available
  message('LLD linker not available; using default linker')
endif
# Skip FTS5 runtime check when cross-compiling (can't execute target binaries on host)
if not meson.is_cross_build()
  fts5_prog = cpp.run(files('meson/fts5_check.cpp'), dependencies: sqlite_dep, name: 'sqlite3 FTS5 check')
  if fts5_prog.returncode() != 0
    msg = 'SQLite3 built without FTS5 support. Enable FTS5 in your package manager or Conan (e.g., -o "sqlite3/*:fts5=True").'
    if require_fts5
      error(msg)
    else
      warning(msg)
    endif
  endif
else
  message('Cross-compiling: skipping SQLite3 FTS5 runtime check (assuming Conan configured correctly)')
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

# Determine linker info for summary
_linker_name = 'default (ld)'
if use_lld.enabled() and lld_available
  _linker_name = 'lld (forced)'
elif use_lld.auto() and cpp.get_id() == 'clang' and lld_available
  _linker_name = 'lld (auto with Clang)'
elif lld_available and cpp.has_link_argument('-fuse-ld=lld')
  # Check if LLD arguments were actually added
  _linker_name = 'lld (available but not used)'
endif

summary({
  'project': meson.project_name(),
  'version': meson.project_version(),
  'cpp_std': get_option('cpp_std'),
  'compiler': cpp.get_id() + ' ' + cpp.version(),
  'linker': _linker_name,
  'build_cli': yams_build_cli,
  'build_mcp_server': yams_build_mcp,
  'enable_tui': yams_enable_tui,
}, section: 'YAMS Meson configuration', bool_yn: true)
