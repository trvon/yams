spdlog_dep = dependency('spdlog', required: true)
json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)

yams_config = static_library('yams_config',
  [ 'config_migration.cpp' ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, json_dep, yams_core],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_config,
  name: 'yams-config',
  description: 'YAMS configuration helpers',
  subdirs: ['yams'],
)

yams_config_dep = declare_dependency(
  link_with: yams_config,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_config', yams_config_dep)

