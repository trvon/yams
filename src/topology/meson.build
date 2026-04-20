nlohmann_json_dep = dependency('nlohmann_json', required: true)
spdlog_dep = dependency('spdlog', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
yams_vector = dependency('yams_vector', required: false)
yams_hdbscan_dep = dependency('yams_hdbscan_cpp', required: true)

yams_topology = static_library('yams_topology',
  [
    'topology_alternate_engines.cpp',
    'topology_baseline.cpp',
    'topology_factory.cpp',
    'topology_input_extractor.cpp',
    'topology_metadata_store.cpp',
    'topology_offline_analyzer.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [nlohmann_json_dep, spdlog_dep, yams_core, yams_metadata, yams_vector,
                 yams_hdbscan_dep],
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
  dependencies: [nlohmann_json_dep, spdlog_dep, yams_core, yams_metadata, yams_vector,
                 yams_hdbscan_dep],
)

meson.override_dependency('yams_topology', yams_topology_dep)
