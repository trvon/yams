tl_expected_dep = dependency('tl-expected', required: true)

yams_daemon_client_lib = static_library('yams_daemon_client',
  [
    'asio_connection.cpp',
    'asio_connection_pool.cpp',
    'daemon_client.cpp',
    'asio_transport.cpp',
    'global_io_context.cpp',
  ],
  dependencies: [
    dependency('yams_core'),
    dependency('spdlog'),
    # Boost.Asio relies on these Boost components at link time
    dependency('Boost', modules: ['system', 'thread'], method: 'cmake'),
    tl_expected_dep,
  ],
  include_directories: include_directories('../../../include'),
  install: true,
)

yams_daemon_client_dep = declare_dependency(
  link_with: yams_daemon_client_lib,
  include_directories: include_directories('../../../include'),
  dependencies: [
    tl_expected_dep,
    dependency('yams_core'),
  ],
)

meson.override_dependency('yams_daemon_client', yams_daemon_client_dep)
