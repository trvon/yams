# MCP server static library (disabled via option when not needed)

yams_mcp_sources = [
  'mcp_server.cpp',
  'tool_registry.cpp',
]

yams_mcp_lib = static_library('yams_mcp',
  yams_mcp_sources,
  include_directories: include_directories('../../include'),
  dependencies: [
    dependency('yams_api'),
    dependency('yams_search'),
    dependency('yams_app_services'),
    dependency('yams_downloader'),
    dependency('yams_config'),
    dependency('yams_daemon_client'),
    dependency('nlohmann_json'),
    dependency('spdlog'),
    dependency('boost'),
    dependency('OpenSSL'),
    dependency('threads'),
  ],
  install: true,
)

yams_mcp_dep = declare_dependency(
  link_with: yams_mcp_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_mcp', yams_mcp_dep)
