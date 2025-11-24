spdlog_dep = dependency('spdlog', required: true)
threads_dep = dependency('threads')
yams_core = dependency('yams_core', required: false)
yams_storage_engine = dependency('yams_storage_engine', required: false)
yams_crypto = dependency('yams_crypto', required: false)
yams_manifest = dependency('yams_manifest', required: false)
yams_chunking = dependency('yams_chunking', required: false)

yams_integrity = static_library('yams_integrity',
  [
    'integrity_verifier.cpp',
    'verification_scheduler.cpp',
    'verification_monitor.cpp',
    'repair_manager.cpp',
    'chunk_validator.cpp',
  ],
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, threads_dep, yams_core, yams_storage_engine, yams_crypto, yams_manifest, yams_chunking, yams_metadata_dep],
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_integrity,
  name: 'yams-integrity',
  description: 'YAMS integrity verification',
  subdirs: ['yams'],
)

yams_integrity_dep = declare_dependency(
  link_with: yams_integrity,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_integrity', yams_integrity_dep)

