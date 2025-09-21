# Benchmark utility library (non-Google benchmark helpers)

yams_benchmarks_lib = static_library('yams_benchmarks',
  'vector_benchmark.cpp',
  dependencies: [
    dependency('yams_core'),
    dependency('spdlog'),
  ],
  include_directories: include_directories('../../include'),
  install: true,
)

yams_benchmarks_dep = declare_dependency(
  link_with: yams_benchmarks_lib,
  include_directories: include_directories('../../include'),
)

meson.override_dependency('yams_benchmarks', yams_benchmarks_dep)
