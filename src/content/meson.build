spdlog_dep = dependency('spdlog', required: true)
threads_dep = dependency('threads')
yams_core = dependency('yams_core', required: false)
yams_detection = dependency('yams_detection', required: false)
yams_extraction = dependency('yams_extraction', required: false)
yams_crypto = dependency('yams_crypto', required: false)

taglib_dep = dependency('taglib', required: false)
libarchive_dep = dependency('libarchive', required: false)
liblzma_dep = dependency('liblzma', required: false)

ffprobe = find_program('ffprobe', required: false)
mediainfo = find_program('mediainfo', required: false)

content_src = files(
  'content_handler_registry.cpp',
  'text_content_handler.cpp',
  'binary_content_handler.cpp',
  'image_content_handler.cpp',
  'audio_content_handler.cpp',
  'archive_content_handler.cpp',
)

if ffprobe.found() or mediainfo.found()
  content_src += files('video_content_handler.cpp')
endif

yams_content_cppargs = []
if taglib_dep.found()
  yams_content_cppargs += ['-DYAMS_HAVE_TAGLIB=1']
endif
if libarchive_dep.found()
  yams_content_cppargs += ['-DYAMS_HAVE_LIBARCHIVE=1']
endif
if ffprobe.found()
  yams_content_cppargs += ['-DYAMS_HAVE_FFPROBE=1']
endif
if mediainfo.found()
  yams_content_cppargs += ['-DYAMS_HAVE_MEDIAINFO=1']
endif

yams_content = static_library('yams_content',
  content_src,
  include_directories: include_directories('../../include'),
  dependencies: [spdlog_dep, threads_dep, yams_core, yams_detection, yams_extraction, yams_crypto, taglib_dep, libarchive_dep, yams_metadata_dep],
  cpp_args: yams_content_cppargs,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_content,
  name: 'yams-content',
  description: 'YAMS content handlers',
  subdirs: ['yams'],
)

yams_content_dep = declare_dependency(
  link_with: yams_content,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_content', yams_content_dep)
