# Memory-sync reconciliation loop. The public contract stays in
# include/yams/memory_sync/; this library holds MemorySyncLoop's bodies so
# daemon translation units no longer compile ~3k lines of implementation.
yams_memory_sync = static_library('yams_memory_sync',
  [
    'memory_sync.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [
    dependency('nlohmann_json'),
    dependency('yams_core'),
    dependency('yams_crypto'),
    dependency('yams_storage_engine'),
  ],
  install: true,
)

yams_memory_sync_dep = declare_dependency(
  link_with: yams_memory_sync,
  include_directories: include_directories('../../include'),
  dependencies: [
    dependency('nlohmann_json'),
    dependency('yams_core'),
    dependency('yams_crypto'),
    dependency('yams_storage_engine'),
  ],
)
meson.override_dependency('yams_memory_sync', yams_memory_sync_dep)
