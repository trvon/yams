spdlog_dep = dependency('spdlog', required: true)
openssl_dep = dependency('openssl', modules: ['crypto'], required: true)
zstd_dep = dependency('libzstd', required: true)
threads_dep = dependency('threads')
yams_core = dependency('yams_core', required: false)

lzma_opt = get_option('enable-lzma')
lzma_dep = dependency('lzma', required: false, allow_fallback: false)
if not lzma_dep.found()
  lzma_dep = dependency('liblzma', required: false, allow_fallback: false)
endif
if not lzma_dep.found()
  lzma_dep = dependency('LZMA', method: 'cmake', required: false, allow_fallback: false)
endif

if lzma_opt.enabled() and not lzma_dep.found()
  error('LZMA support was requested but liblzma could not be located. Install the xz development package or disable it via -Denable-lzma=disabled.')
endif

cpp = meson.get_compiler('cpp')
have_lzma_headers = false
if lzma_dep.found()
  have_lzma_headers = (
    cpp.has_header('LzmaEnc.h', dependencies: [lzma_dep]) or
    cpp.has_header('lzma/LzmaEnc.h', dependencies: [lzma_dep])
  ) and (
    cpp.has_header('Alloc.h', dependencies: [lzma_dep]) or
    cpp.has_header('7z/Alloc.h', dependencies: [lzma_dep])
  )
endif

have_lzma = lzma_dep.found() and have_lzma_headers and lzma_opt.allowed()
if lzma_dep.found() and not have_lzma_headers
  message('LZMA library was located but LZMA SDK headers (LzmaEnc.h) were missing; disabling LZMA support.')
endif
if lzma_opt.auto() and not have_lzma
  message('LZMA support disabled: liblzma not found or headers unavailable (install liblzma-dev/xz-devel to enable).')
endif

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
