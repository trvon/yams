spdlog_dep = dependency('spdlog', required: true)
openssl_dep = dependency('openssl', modules: ['crypto'], required: true)
json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)

crypto_src = files(
  'sha256_hasher.cpp',
  'writer_auth.cpp',
)

yams_crypto = static_library('yams_crypto',
  crypto_src,
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, openssl_dep, json_dep, yams_core],
  install: false,
)

# Provide a Meson dependency for other subdirs
yams_crypto_dep = declare_dependency(
  link_with: yams_crypto,
  include_directories: include_directories('../../include'),
  dependencies: [json_dep],
)
meson.override_dependency('yams_crypto', yams_crypto_dep)
