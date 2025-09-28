# Build standalone benchmark executables. These are not registered as Meson
# tests; CI invokes them directly to generate JSON/MD reports.

api_bench = executable('yams_api_benchmarks', files('api_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [test_deps, dependency('yams_benchmarks')],
  cpp_args: ['-DYAMS_TESTING=1'],
  install: false,
)

search_bench = executable('yams_search_benchmarks', files('search_benchmarks.cpp'),
  include_directories: incs,
  dependencies: [test_deps, dependency('yams_benchmarks')],
  cpp_args: ['-DYAMS_TESTING=1'],
  install: false,
)

