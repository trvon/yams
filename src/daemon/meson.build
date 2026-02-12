fs = import('fs')
yams_daemon_client_dep = dependency('yams_daemon_client')
protobuf_dep = dependency('protobuf')

# Build yams_onnx_resource shared library first (contains OnnxConcurrencyRegistry)
subdir('resource')
yams_onnx_resource_dep = dependency('yams_onnx_resource')

# Tracy profiling support (optional, controlled by -Denable-profiling=true)
enable_profiling = get_option('enable-profiling')
enable_rocprofiler_tracy = get_option('enable-rocprofiler-tracy')
tracy_dep = dependency('tracy', required: enable_profiling, static: true)
tracy_args = []
tracy_deps = []
tracy_enabled = tracy_dep.found() and enable_profiling
if tracy_enabled
  tracy_args += ['-DTRACY_ENABLE']
  tracy_deps += tracy_dep
  rocm_hint = '/opt/rocm'
  rocm_present = fs.exists(rocm_hint)
  want_rocprofiler = (not enable_rocprofiler_tracy.disabled()) and (enable_rocprofiler_tracy.enabled() or (enable_rocprofiler_tracy.auto() and rocm_present))
  if want_rocprofiler
    rocprofiler_dep = dependency(
      'rocprofiler-sdk',
      method: 'cmake',
      cmake_module_path: [join_paths(rocm_hint, 'lib', 'cmake'), join_paths(rocm_hint, 'lib64', 'cmake')],
      required: enable_rocprofiler_tracy.enabled(),
    )
    if rocprofiler_dep.found()
      tracy_deps += rocprofiler_dep
    elif enable_rocprofiler_tracy.enabled()
      error('enable-rocprofiler-tracy=enabled but rocprofiler-sdk was not found')
    else
      warning('Tracy present but rocprofiler-sdk missing; disabling Tracy ROCm hooks to avoid link errors')
      tracy_enabled = false
      tracy_deps = []
      tracy_args = []
    endif
  endif
endif
if tracy_enabled and tracy_deps.length() > 0
  cpp = meson.get_compiler('cpp')
  tracy_link_ok = cpp.links('int main(){ return 0; }', dependencies: tracy_deps, name: 'tracy-link-check', required: false)
  if not tracy_link_ok
    warning('Tracy dependency failed to link (likely missing rocprofiler-sdk); disabling Tracy for this build')
    tracy_enabled = false
    tracy_deps = []
    tracy_args = []
  endif
endif
if tracy_enabled
  message('Tracy profiling enabled for daemon')
endif
# pb_includedir = protobuf_dep.get_variable(pkgconfig: 'includedir', default_value: '')
# pb_system_inc = pb_includedir != '' ? include_directories(pb_includedir, is_system: true) : []
pb_system_inc = []
# Prefer protoc from the protobuf dependency (Conan) to avoid header/runtime mismatch.
proto_prefix = protobuf_dep.get_variable(pkgconfig: 'prefix', default_value: '')
protoc_candidate = proto_prefix != '' ? join_paths(proto_prefix, 'bin', 'protoc') : 'protoc'
protoc_prog = find_program(protoc_candidate, required: protobuf_dep.found())
proto_source_root = join_paths(meson.project_source_root(), 'include')
proto_out_dir = join_paths(meson.current_build_dir(), 'libyams_daemon.a.p')
proto_generator = generator(protoc_prog,
  output: ['@BASENAME@.pb.cc', '@BASENAME@.pb.h'],
  arguments: [
    '--cpp_out=lite:' + proto_out_dir,
    '--proto_path=' + proto_source_root,
    '@INPUT@'
  ]
)
proto_input = join_paths(proto_source_root, 'yams', 'daemon', 'ipc', 'proto', 'ipc_envelope.proto')
proto_generated = proto_generator.process(
  [proto_input],
  preserve_path_from: proto_source_root
)

nlohmann_json_dep = dependency('nlohmann_json')

# Resolve CLI11 similarly to src/cli/meson.build (pkg-config first, then CMake
# via Conan generator paths when available), to avoid environment-specific
# inconsistencies.
cli11_dep = dependency('cli11', required: false)
if not cli11_dep.found()
  conan_gen_opt = get_option('conan-generators-dir')
  conan_candidate_dirs = []
  if conan_gen_opt != ''
    conan_candidate_dirs += [join_paths(meson.project_source_root(), conan_gen_opt)]
  endif

  base_build = meson.project_build_root()
  default_candidates = [
    base_build,
    join_paths(base_build, 'conan'),
    join_paths(base_build, 'conan', 'generators'),
    join_paths(base_build, 'generators'),
  ]
  foreach variant : ['build-debug', 'build-release', 'debug', 'release']
    variant_path = join_paths(base_build, variant)
    if fs.exists(variant_path)
      default_candidates += [variant_path]
      default_candidates += [join_paths(variant_path, 'conan')]
      default_candidates += [join_paths(variant_path, 'conan', 'generators')]
      default_candidates += [join_paths(variant_path, 'generators')]
    endif
  endforeach

  foreach cand : default_candidates
    if fs.exists(cand)
      conan_candidate_dirs += [cand]
    endif
  endforeach

  cli11_paths = []
  foreach base : conan_candidate_dirs
    if fs.exists(base)
      cli11_paths += base
      foreach sub : ['CLI11', 'cli11']
        cand = join_paths(base, sub)
        if fs.exists(cand)
          cli11_paths += cand
        endif
      endforeach
    endif
  endforeach

  # Allow explicit override from option
  cli11_opt = get_option('cli11-cmake-path')
  if cli11_opt != ''
    cli11_paths += cli11_opt
  endif

  if cli11_paths.length() > 0
    cli11_dep = dependency('cli11', method: 'cmake', cmake_module_path: cli11_paths, required: false)
  endif
endif
if not cli11_dep.found()
  cli11_dep = dependency('CLI11', method: 'cmake', required: false)
endif

yams_daemon_lib = static_library('yams_daemon',
  [
    'daemon.cpp',
    'tracy_memory_hooks.cpp',
    'components/BackgroundTaskManager.cpp',
    'components/CheckpointManager.cpp',
    'components/DaemonMetrics.cpp',
    'components/RequestDispatcher.cpp',
    'components/dispatcher/request_dispatcher_plugins.cpp',
    'components/dispatcher/request_dispatcher_models.cpp',
    'components/dispatcher/request_dispatcher_embeddings.cpp',
    'components/dispatcher/request_dispatcher_documents.cpp',
    'components/dispatcher/request_dispatcher_status.cpp',
    'components/dispatcher/request_dispatcher_search.cpp',
    'components/dispatcher/request_dispatcher_misc.cpp',
    'components/dispatcher/request_dispatcher_session.cpp',
    'components/dispatcher/request_dispatcher_tree_diff.cpp',
    'components/dispatcher/request_dispatcher_prune.cpp',
    'components/dispatcher/request_dispatcher_collections.cpp',
    'components/dispatcher/request_dispatcher_graph.cpp',
    'components/dispatcher/request_dispatcher_graph_maintenance.cpp',
    'components/dispatcher/request_dispatcher_repair.cpp',
    'components/LifecycleComponent.cpp',
    'components/WorkCoordinator.cpp',
    'components/ServiceManager.cpp',
    'components/SearchEngineManager.cpp',
    'components/SearchComponent.cpp',
    'components/GraphComponent.cpp',
    'components/PostIngestQueue.cpp',
    'components/embed_preparer.cpp',
    'components/RequestQueue.cpp',
    'components/IOCoordinator.cpp',
    'components/PoolManager.cpp',
    'components/EmbeddingService.cpp',
    'components/KGWriteQueue.cpp',
    'components/TuningManager.cpp',
    'components/TuneAdvisor.cpp',
    'components/GradientLimiter.cpp',
    'components/ResourceGovernor.cpp',
    'components/RepairService.cpp',
    'components/EntityGraphService.cpp',
    'components/gliner_query_extractor.cpp',
    'components/SocketServer.cpp',
    'components/WalMetricsProvider.cpp',
    'resource/mock_model_provider.cpp',
    'components/dispatch_utils.cpp',
    'resource/abi_model_provider_adapter.cpp',
    'resource/abi_entity_extractor_adapter.cpp',
    'resource/abi_symbol_extractor_adapter.cpp',
    'resource/abi_plugin_loader.cpp',
    'resource/plugin_trust.cpp',
    'resource/plugin_host.cpp',
    'resource/plugin_host_services_shim.cpp',
    'resource/external_plugin_host.cpp',
    'resource/plugin_content_extractor_adapter.cpp',
    'resource/external_entity_provider_adapter.cpp',
    # IPC objects are now part of the library
    'ipc/request_handler.cpp',
    'ipc/streaming_processor.cpp',
    'ipc/connection_fsm.cpp',
    'ipc/retrieval_session.cpp',

    'ipc/socket_utils.cpp',
    'ipc/message_framing.cpp',
    'ipc/proto_serializer.cpp',
    'ipc/ipc_protocol.cpp',
    'components/DaemonLifecycleFsm.cpp',
    'components/IngestService.cpp',
    'components/ConfigResolver.cpp',
    'components/VectorSystemManager.cpp',
    'components/DatabaseManager.cpp',
    'components/PluginManager.cpp',
    proto_generated,
  ],
  dependencies: [
    dependency('yams_core'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_ingest'),
    dependency('yams_extraction'),
    dependency('yams_api'),
    dependency('yams_wal'),
    dependency('yams_downloader'),
    dependency('yams_repair'),
    dependency('yams_integrity'),
    dependency('yams_app_services'),
    dependency('yams_config'),
    yams_daemon_client_dep,
    yams_onnx_resource_dep,
    # Direct Boost dependency required because daemon sources (e.g. RepairCoordinator,
    # various IPC components) include boost/asio headers directly. Relying on the
    # daemon_client's transitive usage isn't sufficient for include paths at compile time.
    dependency('boost'),
    dependency('spdlog'),
    dependency('threads'),
    dependency('libcurl', required: false),
    dependency('OpenSSL'),
    dependency('zlib'),
    nlohmann_json_dep,
    protobuf_dep,
  ] + tracy_deps,
  cpp_args: tracy_args + (cpp.get_id() == 'msvc' ? ['/wd4100'] : ['-Wno-unused-parameter']),
  include_directories: [
    include_directories('../../include'),
    include_directories('.'),
    pb_system_inc,
  ],
  install: true,
)

yams_daemon_dep = declare_dependency(
  link_with: yams_daemon_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_daemon', yams_daemon_dep)

# Set platform-specific rpath for daemon
# macOS uses @executable_path, Linux uses $ORIGIN
if host_machine.system() == 'darwin'
  daemon_install_rpath = '@executable_path/../lib'
else
  daemon_install_rpath = '$ORIGIN/../lib'
endif

yams_daemon_exe = executable('yams-daemon',
  'daemon_main.cpp',
  dependencies: [
    yams_daemon_dep,
    cli11_dep,
    dependency('spdlog'),
    dependency('boost'),
    dependency('OpenSSL'),
    dependency('threads'),
    dependency('zlib'),
    dependency('nlohmann_json'),
    dependency('yams_config'),
    protobuf_dep,
    dependency('libcurl', required: false),
  ] + tracy_deps,
  cpp_args: tracy_args,
  install: true,
  install_dir: get_option('bindir'),
  install_rpath: daemon_install_rpath,
)

# --- Windows: Copy runtime DLLs next to the daemon executable ---
# Use install: true so DLLs are included in meson install output for MSI packaging
if host_machine.system() == 'windows' and windows_runtime_dlls.length() > 0
  daemon_build_dir = meson.current_build_dir()
  
  foreach dll_info : windows_runtime_dlls
    dll_name = dll_info[0]
    dll_src = dll_info[1]
    fs.copyfile(dll_src, dll_name, install: true, install_dir: get_option('bindir'))
  endforeach
endif
