curl_dep = dependency('libcurl', required: true)
openssl_dep = dependency('openssl', modules: ['crypto'], required: true)
zlib_dep = dependency('zlib', required: true)
spdlog_dep = dependency('spdlog', required: false)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)
yams_storage_engine = dependency('yams_storage_engine', required: false)

yams_object_storage_s3 = shared_module('yams_object_storage_s3',
  [ 's3_plugin.cpp' ],
  include_directories: [ include_directories('../../include') ],
  dependencies: [curl_dep, openssl_dep, zlib_dep, spdlog_dep, nlohmann_json_dep, yams_core, yams_storage_engine],
  cpp_args: ['-DCURL_STATICLIB=1'],
  install: true,
  install_dir: plugins_dir,
)

