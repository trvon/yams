spdlog_dep = dependency('spdlog', required: true)
json_dep = dependency('nlohmann_json', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)

# Optional libmagic
libmagic_dep = dependency('libmagic', required: false)

yams_detection_cppargs = []
if libmagic_dep.found()
  yams_detection_cppargs += ['-DYAMS_HAS_LIBMAGIC=1']
endif

yams_detection = static_library('yams_detection',
  [ 'file_type_detector.cpp' ],
  include_directories: [include_directories('../../include'), include_directories('.')],
  dependencies: [spdlog_dep, json_dep, yams_core, yams_metadata, libmagic_dep],
  cpp_args: yams_detection_cppargs,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_detection,
  name: 'yams-detection',
  description: 'YAMS file type detection',
  subdirs: ['yams'],
)

yams_detection_dep = declare_dependency(
  link_with: yams_detection,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_detection', yams_detection_dep)
