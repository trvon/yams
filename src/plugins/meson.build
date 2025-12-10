# src/plugins/meson.build
# Plugin management library for downloading and installing plugins from a remote repository

spdlog_dep = dependency('spdlog', required: true)
json_dep = dependency('nlohmann_json', required: true)
threads_dep = dependency('threads')
curl_dep = dependency('libcurl', required: true)
openssl_dep = dependency('openssl', modules: ['ssl', 'crypto'], required: true)
libarchive_dep = dependency('libarchive', required: true)
yams_core = dependency('yams_core', required: true)

yams_plugins_mgmt = static_library('yams_plugins_mgmt',
  [
    'plugin_repo_client.cpp',
    'plugin_installer.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, json_dep, threads_dep, curl_dep, openssl_dep, libarchive_dep, yams_core],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_plugins_mgmt,
  name: 'yams-plugins-mgmt',
  description: 'YAMS plugin management library',
  subdirs: ['yams'],
)

yams_plugins_mgmt_dep = declare_dependency(
  link_with: yams_plugins_mgmt,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_plugins_mgmt', yams_plugins_mgmt_dep)
