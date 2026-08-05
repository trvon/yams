# MCP server static library (disabled via option when not needed)

yams_mcp_sources = [
  'mcp_server.cpp',
  'mcp_server_transport.cpp',
  'mcp_server_tool_registry.cpp',
  'mcp_prompts.cpp',
  'mcp_request_routing.cpp',
  'mcp_handshake.cpp',
  'mcp_ui_resources.cpp',
  'mcp_cancellation.cpp',
  'mcp_resources.cpp',
  'mcp_tool_calls.cpp',
  'stdio_transport.cpp',
  'tool_registry.cpp',
]

if get_option('wasi')
  yams_mcp_sources += 'mcp_wasi_handlers.cpp'
else
  yams_mcp_sources += [
    'mcp_server_handlers.cpp',
    'mcp_code_mode.cpp',
    'mode_router.cpp',
  ]
endif

yams_mcp_deps = [
  dependency('yams_core'),
  dependency('nlohmann_json'),
  dependency('spdlog'),
  version_generated_dep,
  boost_dep,
  dependency('threads'),
]

if not get_option('wasi')
  yams_mcp_deps += dependency('yams_api')
  yams_mcp_deps += dependency('yams_search')
  yams_mcp_deps += dependency('yams_app_services')
  yams_mcp_deps += dependency('yams_downloader')
  yams_mcp_deps += dependency('yams_config')
  yams_mcp_deps += dependency('yams_daemon_client')
  yams_mcp_deps += dependency('OpenSSL')
endif

yams_mcp_inc_dirs = [include_directories('../../include'), generated_inc]

yams_mcp_lib = static_library('yams_mcp',
  yams_mcp_sources,
  include_directories: yams_mcp_inc_dirs,
  dependencies: yams_mcp_deps,
  cpp_args: wasi_cpp_args,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_mcp_lib,
  name: 'yams-mcp',
  description: 'YAMS MCP server library',
  subdirs: ['yams'],
  extra_cflags: wasi_cpp_args,
)

yams_mcp_dep = declare_dependency(
  link_with: yams_mcp_lib,
  include_directories: yams_mcp_inc_dirs,
  dependencies: yams_mcp_deps,
  compile_args: wasi_cpp_args,
)

meson.override_dependency('yams_mcp', yams_mcp_dep)
