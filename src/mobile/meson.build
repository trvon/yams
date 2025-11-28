mobile_sources = [
  'mobile_bindings.cpp',
]

# Mobile shared library must be built without TSAN because:
# 1. TSAN requires ALL code (including dependencies) to be compiled with -fsanitize=thread
# 2. Conan dependencies (sqlite3, spdlog, boost, etc.) are NOT built with TSAN
# 3. This causes undefined references to __tsan_* symbols at link time
mobile_cpp_args = []
mobile_link_args = []
if get_option('enable-tsan')
  message('Disabling TSAN for mobile shared library (incompatible with non-TSAN Conan dependencies)')
  # Override TSAN flags by adding -fno-sanitize=thread
  mobile_cpp_args += ['-fno-sanitize=thread']
  mobile_link_args += ['-fno-sanitize=thread']
endif

yams_mobile_lib = shared_library('yams_mobile',
  mobile_sources,
  dependencies: [
    dependency('yams_app_services'),
    dependency('yams_metadata'),
    dependency('yams_api'),
    dependency('yams_search'),
    dependency('yams_vector'),
    dependency('yams_indexing'),
    dependency('yams_storage_engine'),
    dependency('yams_manifest'),
    dependency('yams_chunking'),
    dependency('yams_crypto'),
    dependency('yams_daemon'),
    dependency('boost'),
    dependency('nlohmann_json'),
    dependency('sqlite3'),
    dependency('spdlog'),
  ],
  include_directories: include_directories('../../include'),
  cpp_args: mobile_cpp_args,
  link_args: mobile_link_args,
  install: true,
)

yams_mobile_dep = declare_dependency(
  link_with: yams_mobile_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_mobile', yams_mobile_dep)

pkg.generate(yams_mobile_lib,
  name: 'yams_mobile',
  description: 'YAMS mobile C ABI',
  subdirs: ['yams/api'],
)
