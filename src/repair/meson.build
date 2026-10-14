yams_repair_lib = static_library('yams_repair',
  'embedding_repair_util.cpp',
  dependencies: [
    dependency('yams_core'),
    dependency('yams_api'),
    dependency('yams_metadata'),
    dependency('yams_vector'),
    dependency('yams_tune_advisor'),
    boost_dep,
    dependency('spdlog'),
  ] + yams_tracy_deps,
  include_directories: include_directories('../../include'),
  cpp_args: yams_tracy_args,
  install: false,
)

yams_repair_dep = declare_dependency(
  link_with: yams_repair_lib,
  include_directories: include_directories('../../include'),
  dependencies: [dependency('yams_tune_advisor')],
)

meson.override_dependency('yams_repair', yams_repair_dep)
