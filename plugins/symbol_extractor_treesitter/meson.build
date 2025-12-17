if not get_option('plugin-symbols')
  subdir_done()
endif

cpp = meson.get_compiler('cpp')

# Conan-provided runtime (PkgConfigDeps)
ts_dep = dependency('tree-sitter', required: true)
if not ts_dep.found()
  warning('tree-sitter dependency not found, skipping symbol_extractor_treesitter plugin.')
  subdir_done()
endif

# dlopen for loading grammar at runtime
libdl_dep = cpp.find_library('dl', required: host_machine.system() != 'windows')
# filesystem for C++20
filesystem_dep = dependency('threads', required: false) # Most compilers have filesystem built-in

# C++20/23 symbol extractor sources - modular architecture
extractor_sources = files(
  'plugin_api.cpp',
  'symbol_extractor.cpp',
  'grammar_loader.cpp',
)

extractor_headers = files(
  'grammar_loader.h',
  'symbol_extractor.h',
)

tl_expected_dep = dependency('tl-expected', required: true)
spdlog_dep = dependency('spdlog', required: true)

shared_module(
  'yams_symbol_extractor',
  extractor_sources,
  include_directories: [ include_directories('../../include'), include_directories('.') ],
  dependencies: [ts_dep, libdl_dep, tl_expected_dep, spdlog_dep],
  cpp_args: ['-std=c++23', '-fexceptions'],  # C++23 with exceptions for error handling
  build_by_default: true,
  install: true,
  install_dir: plugins_dir,
  name_prefix: '',  # Don't add 'lib' prefix
)
