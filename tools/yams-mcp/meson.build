fs = import('fs')

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

yams_mcp_exe = executable('yams-mcp-server',
  'main.cpp',
  include_directories: [
    include_directories('../../include'),
  ],
  dependencies: deps,
  install: true,
  install_dir: get_option('bindir'),
)

# --- Windows: Copy runtime DLLs next to the MCP server executable ---
if host_machine.system() == 'windows' and windows_runtime_dlls.length() > 0
  foreach dll_info : windows_runtime_dlls
    dll_name = dll_info[0]
    dll_src = dll_info[1]
    fs.copyfile(dll_src, dll_name, install: false)
  endforeach
endif

