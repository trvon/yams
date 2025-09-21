spdlog_dep = dependency('spdlog', required: true)
openssl_dep = dependency('openssl', modules: ['crypto'], required: true)
zstd_dep = dependency('libzstd', required: true)
threads_dep = dependency('threads')
yams_core = dependency('yams_core', required: false)

lzma_opt = get_option('enable-lzma')
lzma_dep = dependency('lzma', required: lzma_opt.enabled())
have_lzma = lzma_dep.found() and lzma_opt.allowed()

compression_src = files(
  'zstandard_compressor.cpp',
  'compression_registry.cpp',
  'compression_utils.cpp',
  'compression_header.cpp',
  'compression_policy.cpp',
  'compression_scheduler.cpp',
  'compression_monitor.cpp',
  'error_handler.cpp',
  'integrity_validator.cpp',
  'recovery_manager.cpp',
  'transaction_manager.cpp',
)

compression_cppargs = []
if have_lzma
  compression_src += files('lzma_compressor.cpp')
  compression_cppargs += ['-DYAMS_ENABLE_LZMA=1']
else
  compression_cppargs += ['-DYAMS_ENABLE_LZMA=0']
endif

yams_compression = static_library('yams_compression',
  compression_src,
  include_directories: include_directories('../../include'),
  cpp_args: compression_cppargs,
  dependencies: [spdlog_dep, openssl_dep, zstd_dep, threads_dep, yams_core] + (have_lzma ? [lzma_dep] : []),
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_compression,
  name: 'yams-compression',
  description: 'YAMS compression (zstd + optional lzma)',
  subdirs: ['yams'],
)

yams_compression_dep = declare_dependency(
  link_with: yams_compression,
  include_directories: include_directories('../../include'),
  dependencies: [zstd_dep, openssl_dep]
)
meson.override_dependency('yams_compression', yams_compression_dep)
