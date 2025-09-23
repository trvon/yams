core_inc = include_directories('../../include')

# Bring in transitive headers used by public includes (e.g., <tl/expected.hpp>)
tl_expected_dep = dependency('tl-expected', required: true)

yams_core_dep = declare_dependency(
  include_directories: core_inc,
  dependencies: [tl_expected_dep],
)

# Make available as dependency('yams_core') to other subdirs
meson.override_dependency('yams_core', yams_core_dep)
