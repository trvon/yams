# Benchmark utility library (non-Google benchmark helpers)

yams_benchmarks_lib = static_library('yams_benchmarks',
  'vector_benchmark.cpp',
  dependencies: [
    dependency('yams_core'),
    dependency('spdlog'),
  ] + yams_tracy_deps,
  include_directories: include_directories('../../include'),
  cpp_args: yams_tracy_args,
  install: false,
)

yams_benchmarks_dep = declare_dependency(
  link_with: yams_benchmarks_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_benchmarks', yams_benchmarks_dep)

# Internal event bus micro-benchmark
executable('yams_bus_bench',
  'internal_event_bus_benchmark.cpp',
  dependencies: [dependency('spdlog'), dependency('yams_core')] + yams_tracy_deps,
  cpp_args: yams_tracy_args,
  install: false,
)
