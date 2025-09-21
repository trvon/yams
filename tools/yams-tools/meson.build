srcs = files(
  'src/main.cpp',
  'src/commands/gc_command.cpp',
  'src/commands/stats_command.cpp',
  'src/commands/verify_command.cpp',
)

deps = [
  dependency('yams_core'),
  dependency('yams_cli'),            # for CLI helpers (daemon)
  dependency('yams_daemon_client'),
  dependency('yams_storage_engine'),
  dependency('yams_reference_counter'),
  dependency('yams_manifest'),
  dependency('yams_api'),
  dependency('yams_metadata'),
  dependency('spdlog'),
  dependency('nlohmann_json'),
  dependency('sqlite3'),
  dependency('CLI11'),
]

inc = [
  include_directories('include'),
  include_directories('../../include'),
]

executable('yams-tools',
  srcs,
  include_directories: inc,
  dependencies: deps,
  install: true,
  install_dir: get_option('bindir'),
)
