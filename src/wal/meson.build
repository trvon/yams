spdlog_dep = dependency('spdlog', required: true)
threads_dep = dependency('threads')
yams_core = dependency('yams_core', required: false)

yams_wal = static_library('yams_wal',
  [
    'wal_entry.cpp',
    'wal_manager.cpp',
    'wal_file.cpp',
    'wal_recovery.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, threads_dep, yams_core],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_wal,
  name: 'yams-wal',
  description: 'YAMS write-ahead log',
  subdirs: ['yams'],
)

yams_wal_dep = declare_dependency(
  link_with: yams_wal,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_wal', yams_wal_dep)

