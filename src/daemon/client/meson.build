tl_expected_dep = dependency('tl-expected', required: true)
protobuf_dep = dependency('protobuf')
nlohmann_json_dep = dependency('nlohmann_json')

# Protobuf generation for client
pb_includedir = protobuf_dep.get_variable(pkgconfig: 'includedir', default_value: '')
proto_prefix = protobuf_dep.get_variable(pkgconfig: 'prefix', default_value: '')
protoc_candidate = proto_prefix != '' ? join_paths(proto_prefix, 'bin', 'protoc') : 'protoc'
protoc_prog = find_program(protoc_candidate, required: protobuf_dep.found())

proto_source_root = join_paths(meson.project_source_root(), 'include')
proto_input = join_paths(proto_source_root, 'yams', 'daemon', 'ipc', 'proto', 'ipc_envelope.proto')

# Use custom_target to control output location precisely
# We output directly to the build directory, flattening the structure
proto_generated = custom_target('ipc_envelope_proto',
  input: proto_input,
  output: ['ipc_envelope.pb.cc', 'ipc_envelope.pb.h'],
  command: [
    protoc_prog,
    '--cpp_out=lite:@OUTDIR@',
    '--proto_path=' + join_paths(proto_source_root, 'yams', 'daemon', 'ipc', 'proto'),
    '@INPUT@'
  ]
)

yams_daemon_client_lib = static_library('yams_daemon_client',
  [
    'asio_connection.cpp',
    'asio_connection_pool.cpp',
    'daemon_client.cpp',
    'asio_transport.cpp',
    'global_io_context.cpp',
    '../ipc/connection_fsm.cpp',
    '../ipc/message_framing.cpp',
    '../ipc/ipc_protocol.cpp',
    '../ipc/proto_serializer.cpp',
    '../ipc/socket_utils.cpp',
    proto_generated,
  ],
  dependencies: [
    dependency('yams_core'),
    dependency('spdlog'),
    protobuf_dep,
    # Boost.Asio relies on these Boost components at link time
    dependency('Boost', modules: 'system'),
    dependency('Boost', modules: 'thread'),
    tl_expected_dep,
    nlohmann_json_dep,
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
