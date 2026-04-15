yams_cli_sources = [
  'yams_cli.cpp',
  'command_registry.cpp',
  'session_store.cpp',
  'time_parser.cpp',
  'progress_indicator.cpp',
  'graph_helpers.cpp',
  'vector_db_util.cpp',
  'plugin_util.cpp',
  'doctor_checks.cpp',
  'init_assets.cpp',
  'tune_runner.cpp',
  'commands/add_command.cpp',
  'commands/get_command.cpp',
  'commands/restore_command.cpp',
  'commands/cat_command.cpp',
  'commands/init_command.cpp',
  'commands/search_command.cpp',
  'search_runner.cpp',
  'commands/grep_command.cpp',
  'commands/config_command.cpp',
  'commands/auth_command.cpp',
  'commands/uninstall_command.cpp',
  'commands/migrate_command.cpp',
  'commands/delete_command.cpp',
  'commands/list_command.cpp',
  'commands/tree_command.cpp',
  'commands/status_command.cpp',
  'commands/update_command.cpp',
  'commands/session_command.cpp',
  'commands/completion_command.cpp',
  'commands/repair_command.cpp',
  'commands/model_command.cpp',
  'commands/daemon_command.cpp',
  'commands/pipeline_stage_render.cpp',
  'commands/download_command.cpp',
  'commands/watch_command.cpp',
  'commands/plugin_command.cpp',
  'commands/doctor_command.cpp',
  'commands/dr_command.cpp',
  'commands/graph_command.cpp',
  'commands/diff_command.cpp',
  'commands/tune_command.cpp',
]

if get_option('build-mcp-server')
  yams_cli_sources += 'commands/serve_command.cpp'
endif

cli11_dep = cli11_resolved_dep

if not cli11_dep.found()
  if get_option('build-cli')
    message('CLI11 not found; skipping CLI build. Provide -Dcli11-cmake-path=<...>/lib/cmake/CLI11 to enable, or pass -Dbuild-cli=false to silence this message.')
    subdir_done()
  else
    cli11_dep = declare_dependency()
  endif
endif

cli_deps = [
  dependency('yams_api'),
  dependency('yams_vector'),
  dependency('yams_search'),
  dependency('yams_metadata'),
  dependency('yams_detection'),
  dependency('yams_config'),
  dependency('yams_daemon'),
  dependency('yams_daemon_client'),
  dependency('yams_downloader'),
  dependency('yams_plugins_mgmt'),
  dependency('nlohmann_json'),
  dependency('spdlog'),
  dependency('OpenSSL'),
  dependency('sqlite3'),
  dependency('boost'),
  cli11_dep,
]

# TUI framework dependencies removed - migrating to Flutter UI

if get_option('build-mcp-server')
  cli_deps += dependency('yams_mcp')
endif

cli_cppargs = []
if get_option('build-mcp-server')
  cli_cppargs += ['-DYAMS_BUILD_MCP_SERVER=1']
endif

# Include both source include dir and builddir for generated headers (version_generated.h)
cli_inc_dirs = [include_directories('../../include'), generated_inc]

yams_cli_lib = static_library('yams_cli',
  yams_cli_sources,
  include_directories: cli_inc_dirs,
  dependencies: cli_deps,
  cpp_args: cli_cppargs,
  install: true,
)

yams_cli_dep = declare_dependency(
  link_with: yams_cli_lib,
  include_directories: cli_inc_dirs,
  dependencies: cli_deps,
)

meson.override_dependency('yams_cli', yams_cli_dep)

# ============================================================================
# C++20 Modules Support (Experimental)
# ============================================================================
# When enable-modules=true and compiler supports modules, build module interfaces
# alongside traditional headers for gradual migration.

if yams_modules_enabled and yams_modules_supported
  message('Building yams.cli.utils module')

  # Utility sources that will be compiled as module implementation units
  cli_utils_sources = [
    'time_parser.cpp',
    'progress_indicator.cpp',
    'session_store.cpp',
  ]

  # Compiler-specific module interface compilation
  cpp_id = meson.get_compiler('cpp').get_id()

  if cpp_id == 'msvc' or cpp_id == 'clang-cl'
    # MSVC: Use /interface flag for module interface units
    # Note: MSVC expects .ixx extension by default, but we use .cppm with explicit flag
    yams_cli_utils_module = custom_target('yams_cli_utils_module',
      input: 'yams_cli_utils.cppm',
      output: 'yams.cli.utils.ifc',
      command: [
        meson.get_compiler('cpp').cmd_array(),
        '/std:c++20',
        '/interface',
        '/TP',  # Treat as C++
        '/c',
        '@INPUT@',
        '/Fo' + '@OUTPUT@'.replace('.ifc', '.obj'),
        '/ifcOutput', '@OUTPUT@',
      ],
      install: false,
    )

  elif cpp_id == 'clang'
    # Clang: Use --precompile for module interface
    yams_cli_utils_module = custom_target('yams_cli_utils_module',
      input: 'yams_cli_utils.cppm',
      output: 'yams.cli.utils.pcm',
      command: [
        meson.get_compiler('cpp').cmd_array(),
        '-std=c++20',
        '--precompile',
        '-x', 'c++-module',
        '-I', meson.project_source_root() / 'include',
        '-c',
        '@INPUT@',
        '-o', '@OUTPUT@',
      ],
      install: false,
    )

  elif cpp_id == 'gcc'
    # GCC: Use -fmodules-ts
    yams_cli_utils_module = custom_target('yams_cli_utils_module',
      input: 'yams_cli_utils.cppm',
      output: 'yams.cli.utils.gcm',
      command: [
        meson.get_compiler('cpp').cmd_array(),
        '-std=c++20',
        '-fmodules-ts',
        '-x', 'c++',
        '-I', meson.project_source_root() / 'include',
        '-c',
        '@INPUT@',
        '-o', '@OUTPUT@',
      ],
      install: false,
    )
  endif

  # Build module implementation library
  yams_cli_utils_module_impl = static_library('yams_cli_utils_module',
    cli_utils_sources,
    include_directories: include_directories('../../include'),
    dependencies: [
      dependency('yams_core'),
      dependency('nlohmann_json'),
      dependency('spdlog'),
    ],
    cpp_args: yams_module_cpp_args + cli_cppargs,
    install: false,
  )

  # Create module dependency for consumers
  yams_cli_utils_module_dep = declare_dependency(
    link_with: yams_cli_utils_module_impl,
    compile_args: yams_module_cpp_args,
    sources: [yams_cli_utils_module],
  )

  meson.override_dependency('yams_cli_utils_module', yams_cli_utils_module_dep)

  message('  Module interface: yams_cli_utils.cppm')
  message('  Module name: yams.cli.utils')
  message('  Consumers can use: import yams.cli.utils;')
endif

# CLI executable is defined in tools/yams-cli (builds yams-cli and installs a 'yams' symlink)
