nlohmann_json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)

yams_topology = static_library('yams_topology',
  [
    'topology_baseline.cpp',
    'topology_metadata_store.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [nlohmann_json_dep, yams_core, yams_metadata],
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
  dependencies: [nlohmann_json_dep, yams_core, yams_metadata],
)

meson.override_dependency('yams_topology', yams_topology_dep)
