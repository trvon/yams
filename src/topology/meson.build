nlohmann_json_dep = dependency('nlohmann_json', required: true)
spdlog_dep = dependency('spdlog', required: true)
yams_core = dependency('yams_core', required: true)
yams_metadata = dependency('yams_metadata', required: true)
yams_vector = dependency('yams_vector', required: true)
yams_compression = dependency('yams_compression', required: true)

# Tracy profiling (optional, -Denable-profiling=true), mirroring src/search so the YAMS_* zones
# in this library emit. Zero-cost when off: profiling.h compiles to nothing without TRACY_ENABLE.
topology_tracy_args = []
topology_tracy_deps = []
if get_option('enable-profiling')
  topology_tracy_dep = dependency('tracy', modules: ['Tracy::TracyClient'], required: false, static: true)
  if topology_tracy_dep.found()
    topology_tracy_args += ['-DTRACY_ENABLE']
    topology_tracy_deps += topology_tracy_dep
  endif
endif

yams_topology = static_library('yams_topology',
  [
    'topology_alternate_engines.cpp',
    'topology_baseline.cpp',
    'topology_codec.cpp',
    'protected_relation_cover.cpp',
    'topology_factory.cpp',
    'topology_input_extractor.cpp',
    'topology_metadata_store.cpp',
    'topology_offline_analyzer.cpp',
    'topology_representatives.cpp',
    'topology_sgc.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [nlohmann_json_dep, spdlog_dep, yams_core, yams_metadata, yams_vector, yams_compression, simeon_retrieval_dep] + topology_tracy_deps,
  cpp_args: topology_tracy_args,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_topology,
  name: 'yams-topology',
  description: 'YAMS topology artifact and routing helpers',
  subdirs: ['yams'],
)

yams_topology_dep = declare_dependency(
  link_with: yams_topology,
  include_directories: include_directories('../../include'),
  dependencies: [nlohmann_json_dep, spdlog_dep, yams_core, yams_metadata, yams_vector, yams_compression, simeon_retrieval_dep],
)

meson.override_dependency('yams_topology', yams_topology_dep)
