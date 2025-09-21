# Header-only core dependency (provides include paths)

core_inc = include_directories('../../include')

yams_core_dep = declare_dependency(
  include_directories: core_inc,
)

# Make available as dependency('yams_core') to other subdirs
meson.override_dependency('yams_core', yams_core_dep)

