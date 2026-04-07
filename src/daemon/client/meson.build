tl_expected_dep = dependency('tl-expected', required: true)
protobuf_dep = dependency('protobuf')
nlohmann_json_dep = dependency('nlohmann_json')

# Protobuf generation for client
protoc_override = get_option('protoc-path')
proto_prefix = protobuf_dep.get_variable(pkgconfig: 'prefix', default_value: '')
dep_protoc_prog = disabler()
if proto_prefix != ''
  dep_protoc_prog = find_program(join_paths(proto_prefix, 'bin', 'protoc'), required: false)
endif

if protoc_override != ''
  protoc_prog = find_program(protoc_override, native: meson.is_cross_build(), required: true)
elif meson.is_cross_build()
  protoc_prog = find_program('protoc', native: true, required: true)
else
  protoc_candidate = proto_prefix != '' ? join_paths(proto_prefix, 'bin', 'protoc') : 'protoc'
  protoc_prog = find_program(protoc_candidate, required: protobuf_dep.found())
endif

if protobuf_dep.found()
  protobuf_version = protobuf_dep.version()
  protoc_version_cmd = run_command(protoc_prog, '--version', check: false)
  protoc_version = ''
  if protoc_version_cmd.returncode() == 0
    protoc_version_text = protoc_version_cmd.stdout().strip().split(' ')
    if protoc_version_text.length() >= 2
      protoc_version = protoc_version_text[1]
    endif
  endif

  # Normalize versions: protoc may report "34.1" while protobuf pkg-config
  # reports "34.1.0". Treat them as matching if one is a prefix of the other.
  _versions_match = protoc_version == protobuf_version or protobuf_version.startswith(protoc_version + '.') or protoc_version.startswith(protobuf_version + '.')

  if protoc_version != '' and not _versions_match
    fallback_ok = false
    fallback_version = ''
    if dep_protoc_prog.found() and dep_protoc_prog.full_path() != protoc_prog.full_path()
      fallback_version_cmd = run_command(dep_protoc_prog, '--version', check: false)
      if fallback_version_cmd.returncode() == 0
        fallback_version_text = fallback_version_cmd.stdout().strip().split(' ')
        if fallback_version_text.length() >= 2
          fallback_version = fallback_version_text[1]
        endif
      endif
      fallback_ok = fallback_version == protobuf_version or protobuf_version.startswith(fallback_version + '.') or fallback_version.startswith(protobuf_version + '.')
    endif

    if fallback_ok
      warning('Configured protoc @0@ (version @1@) does not match protobuf dependency version @2@; falling back to @3@.'.format(
        protoc_prog.full_path(), protoc_version, protobuf_version, dep_protoc_prog.full_path()))
      protoc_prog = dep_protoc_prog
    else
      error('Configured protoc version @0@ does not match protobuf dependency version @1@. Use a matching protoc, or leave -Dprotoc-path empty to use the dependency-provided generator.'.format(
        protoc_version == '' ? 'unknown' : protoc_version, protobuf_version))
    endif
  endif
endif

proto_source_root = join_paths(meson.project_source_root(), 'include')
proto_input = join_paths(proto_source_root, 'yams', 'daemon', 'ipc', 'proto', 'ipc_envelope.proto')

# Use generator to preserve path structure matching the include hierarchy
proto_generator = generator(protoc_prog,
  output: ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments: [
    '--cpp_out=lite:@BUILD_DIR@',
    '--proto_path=' + proto_source_root,
    '@INPUT@'
  ]
)

proto_generated = proto_generator.process(
  [proto_input],
  preserve_path_from: proto_source_root
)

yams_daemon_client_lib = static_library('yams_daemon_client',
  [
    'asio_connection.cpp',
    'asio_connection_pool.cpp',
    'daemon_client.cpp',
    'asio_transport.cpp',
    'in_process_transport.cpp',
    'sandbox_detection.cpp',
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
    dependency('yams_config'),
    dependency('spdlog'),
    protobuf_dep,
    # Boost.Asio relies on these Boost components at link time
    dependency('Boost', modules: 'system'),
    dependency('Boost', modules: 'thread'),
    tl_expected_dep,
    nlohmann_json_dep,
  ],
  cpp_args: cpp.get_id() == 'msvc' ? ['/wd4100'] : ['-Wno-unused-parameter'],
  include_directories: include_directories('../../../include'),
  install: true,
)

yams_daemon_client_dep = declare_dependency(
  link_with: yams_daemon_client_lib,
  include_directories: include_directories('../../../include'),
  dependencies: [
    tl_expected_dep,
    dependency('yams_core'),
    dependency('yams_config'),
  ],
)

meson.override_dependency('yams_daemon_client', yams_daemon_client_dep)
