curl_dep = dependency('libcurl', required: true)
openssl_dep = dependency('openssl', modules: ['crypto'], required: true)
zlib_dep = dependency('zlib', required: true)
spdlog_dep = dependency('spdlog', required: false)
nlohmann_json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)
yams_storage_engine = dependency('yams_storage_engine', required: false)

# The plugin links its own static OpenSSL, which registers atexit(OPENSSL_cleanup). glibc runs
# that handler at dlclose, but TSan's atexit interceptor keeps it until exit and then calls into
# the unmapped plugin (SEGV after all tests pass). Keep the plugin mapped in Linux TSan builds.
s3_plugin_link_args = plugin_export_link_args
if yams_sanitize_thread and host_machine.system() == 'linux'
  s3_plugin_link_args += ['-Wl,-z,nodelete']
endif

yams_object_storage_s3 = shared_module('yams_object_storage_s3',
  [ 's3_plugin.cpp' ],
  include_directories: [ include_directories('../../include') ],
  dependencies: [curl_dep, openssl_dep, zlib_dep, spdlog_dep, nlohmann_json_dep, yams_core, yams_storage_engine],
  cpp_args: ['-DCURL_STATICLIB=1'],
  gnu_symbol_visibility: 'hidden',
  link_args: s3_plugin_link_args,
  link_depends: plugin_export_link_depends,
  install: true,
  install_dir: plugins_dir,
)

