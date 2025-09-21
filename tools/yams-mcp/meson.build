deps = [
  dependency('yams_mcp'),
  dependency('yams_api'),
  dependency('yams_search'),
  dependency('yams_app_services'),
  dependency('yams_downloader'),
  dependency('yams_config'),
  dependency('yams_daemon'),
  dependency('yams_daemon_client'),
  dependency('nlohmann_json'),
  dependency('spdlog'),
  dependency('boost'),
  dependency('OpenSSL'),
  dependency('threads'),
  dependency('CLI11'),
]

executable('yams-mcp-server',
  'main.cpp',
  include_directories: [
    include_directories('../../include'),
  ],
  dependencies: deps,
  install: true,
  install_dir: get_option('bindir'),
)
