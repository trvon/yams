# Memory-sync reconciliation loop. The public contract stays in
# include/yams/memory_sync/; this library holds MemorySyncLoop's bodies so
# daemon translation units no longer compile ~3k lines of implementation.
# Match the storage engine's test-hook build policy. Hooks are static/thread-local;
# no production object layout depends on YAMS_TESTING.
memory_sync_testing_cpp_args = []
if get_option('build-tests') or (get_option('buildtype') == 'debug' and not get_option('build-fuzzers'))
  memory_sync_testing_cpp_args += ['-DYAMS_TESTING=1']
endif

yams_memory_sync = static_library('yams_memory_sync',
  [
    'memory_sync.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [
    dependency('nlohmann_json'),
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_crypto'),
    dependency('yams_storage_engine'),
    dependency('sqlite3'),
  ] + yams_tracy_deps,
  cpp_args: memory_sync_testing_cpp_args + yams_tracy_args,
  install: false,
)

yams_memory_sync_dep = declare_dependency(
  link_with: yams_memory_sync,
  include_directories: include_directories('../../include'),
  dependencies: [
    dependency('nlohmann_json'),
    dependency('spdlog'),
    dependency('yams_core'),
    dependency('yams_crypto'),
    dependency('yams_storage_engine'),
    dependency('sqlite3'),
  ],
)
meson.override_dependency('yams_memory_sync', yams_memory_sync_dep)
