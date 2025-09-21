spdlog_dep = dependency('spdlog', required: true)
json_dep = dependency('nlohmann_json', required: true)
threads_dep = dependency('threads')
curl_dep = dependency('libcurl', required: true)
openssl_dep = dependency('openssl', modules: ['ssl', 'crypto'], required: true)
yams_core = dependency('yams_core', required: false)

yams_downloader = static_library('yams_downloader',
  [
    'http_adapter_curl.cpp',
    'disk_writer.cpp',
    'integrity_verifier.cpp',
    'resume_store.cpp',
    'rate_limiter.cpp',
    'download_manager.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, json_dep, threads_dep, curl_dep, openssl_dep, yams_core],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_downloader,
  name: 'yams-downloader',
  description: 'YAMS downloader',
  subdirs: ['yams'],
)

yams_downloader_dep = declare_dependency(
  link_with: yams_downloader,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_downloader', yams_downloader_dep)

