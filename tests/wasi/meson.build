wasi_mcp_final_link_smoke = executable(
  'wasi_mcp_final_link_smoke',
  'mcp_final_link_smoke.cpp',
  dependencies: dependency('yams_mcp'),
  install: false,
)

if meson.can_run_host_binaries()
  test('wasi_mcp_final_link_smoke', wasi_mcp_final_link_smoke)
endif
