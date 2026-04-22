spdlog_dep = dependency('spdlog', required: true)
sqlite_dep = dependency('sqlite3', required: true)
json_dep = dependency('nlohmann_json', required: true)
fmt_dep = dependency('fmt', required: false)
boost_dep = dependency('boost', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
tl_expected_dep = dependency('tl-expected', required: true)
fs = import('fs')

# Optional include dirs for third-party headers
# Note: HNSW implementation now uses sqlite-vec-cpp, hnswlib no longer needed

# Generate sqlite-vec.h (as in CMake)
sqlite_vec_hdr = configure_file(
  input: 'sqlite-vec.h.in',
  output: 'sqlite-vec.h',
  copy: true,
)

# sqlite-vec-cpp (C++ implementation)
sqlite_vec_cpp_dir = join_paths(meson.project_source_root(), 'third_party', 'sqlite-vec-cpp')
sqlite_vec_cpp_inc = []
sqlite_vec_cpp_src = []

if fs.exists(join_paths(sqlite_vec_cpp_dir, 'include', 'sqlite-vec-cpp', 'sqlite_vec.hpp'))
  message('Found sqlite-vec-cpp implementation')
  sqlite_vec_cpp_inc = include_directories(join_paths('../../third_party', 'sqlite-vec-cpp', 'include'), is_system: true)
  sqlite_vec_cpp_src = [
    join_paths(sqlite_vec_cpp_dir, 'src', 'sqlite_vec_c_api.cpp'),
  ]
endif

# Enforce static sqlite-vec-cpp by default for clean, consistent builds.
if sqlite_vec_cpp_src.length() == 0
  if get_option('require-sqlite-vec')
    error('sqlite-vec-cpp not found. Initialize the submodule: git submodule update --init third_party/sqlite-vec-cpp. ' +
          'To bypass (NOT recommended), configure with -Drequire-sqlite-vec=false and provide a runtime module via YAMS_SQLITE_VEC_MODULE.')
  else
    message('sqlite-vec-cpp not found and require-sqlite-vec=false; building without static sqlite-vec (runtime module required).')
  endif
endif

vector_cppargs = [
  '-DSQLITE_CORE=1',
  '-DSQLITE_ENABLE_LOAD_EXTENSION=1',
]

# Enable SIMD for sqlite-vec-cpp when supported
cpp_comp = meson.get_compiler('cpp')
host_cpu = host_machine.cpu_family()
if host_cpu == 'x86_64' or host_cpu == 'x86'
  if cpp_comp.has_argument('-mavx') and cpp_comp.has_argument('-mfma')
    vector_cppargs += ['-DSQLITE_VEC_ENABLE_AVX', '-mavx', '-mfma']
    if cpp_comp.has_argument('-mavx2')
      vector_cppargs += ['-DSQLITE_VEC_ENABLE_AVX2', '-mavx2']
    endif
  endif
elif host_cpu == 'aarch64' or host_cpu == 'arm'
  # NEON is baseline on aarch64; on 32-bit arm it may depend on sub-arch
  vector_cppargs += ['-DSQLITE_VEC_ENABLE_NEON']
  # DotProd (ARMv8.2+): compile-test vdotq_s32; all Apple Silicon supports this
  dotprod_test = '''
  #include <arm_neon.h>
  int main() {
      int8x16_t a = vdupq_n_s8(1);
      int8x16_t b = vdupq_n_s8(2);
      int32x4_t c = vdupq_n_s32(0);
      c = vdotq_s32(c, a, b);
      return vgetq_lane_s32(c, 0);
  }
  '''
  if cpp_comp.compiles(dotprod_test, args: ['-march=armv8.2-a+dotprod'], name: 'ARM DotProd (vdotq_s32)')
    vector_cppargs += ['-march=armv8.2-a+dotprod']
    message('ARM DotProd: enabled for sqlite-vec-cpp (int8 cosine/IP ~33x faster)')
  endif
endif

if fmt_dep.found()
  # Force fallback to {fmt} implementation in our format shim
  vector_cppargs += ['-DYAMS_STD_FORMAT_AVAILABLE=0']
endif

srcs = [
  'vector_database.cpp',
  'embedding_generator.cpp',
  'embedding_service.cpp',
  'batch_metrics.cpp',
  'document_chunker.cpp',
  'vector_utils.cpp',
  'model_registry.cpp',
  'model_cache.cpp',
  'model_loader.cpp',
  'sqlite_vec_backend.cpp',
  'vector_schema_migration.cpp',
  'mock_embedding_provider.cpp',
  'dim_resolver.cpp',
  'tokenizer.cpp',
  'turboquant.cpp',
] + sqlite_vec_cpp_src

# ------------------------------------------------------------------
# Vendored simeon (model-free SIMD embedding) — third_party/simeon
# Built into yams_vector as an opt-in backend; standalone builds also
# live in third_party/simeon/build for library-level development.
# ------------------------------------------------------------------
simeon_dir = join_paths(meson.project_source_root(), 'third_party', 'simeon')
simeon_inc = include_directories('../../third_party/simeon/include', is_system: true)

simeon_core_srcs = [
  join_paths(simeon_dir, 'src', 'simeon.cpp'),
  join_paths(simeon_dir, 'src', 'tokenizer.cpp'),
  join_paths(simeon_dir, 'src', 'tokenizer_bpe.cpp'),
  join_paths(simeon_dir, 'src', 'hasher.cpp'),
  join_paths(simeon_dir, 'src', 'hash_xxh64.cpp'),
  join_paths(simeon_dir, 'src', 'hash_crc32c.cpp'),
  join_paths(simeon_dir, 'src', 'hash_tabulation.cpp'),
  join_paths(simeon_dir, 'src', 'projection.cpp'),
  join_paths(simeon_dir, 'src', 'pq.cpp'),
  join_paths(simeon_dir, 'src', 'pmi.cpp'),
  join_paths(simeon_dir, 'src', 'bm25.cpp'),
  join_paths(simeon_dir, 'src', 'fusion.cpp'),
  join_paths(simeon_dir, 'src', 'query_router.cpp'),
  join_paths(simeon_dir, 'src', 'arch', 'scalar.cpp'),
]

simeon_extra_libs = []
if host_cpu == 'aarch64' or host_cpu == 'arm'
  simeon_neon_lib = static_library('yams_simeon_neon',
    [join_paths(simeon_dir, 'src', 'arch', 'neon.cpp')],
    include_directories: simeon_inc,
    install: false,
  )
  simeon_extra_libs += simeon_neon_lib
elif host_cpu == 'x86_64' or host_cpu == 'x86'
  if cpp_comp.has_argument('-mavx2') and cpp_comp.has_argument('-mfma')
    simeon_avx_args = ['-mavx2', '-mfma']
    simeon_avx_lib = static_library('yams_simeon_avx2',
      [join_paths(simeon_dir, 'src', 'arch', 'avx2.cpp')],
      include_directories: simeon_inc,
      cpp_args: simeon_avx_args,
      install: false,
    )
    simeon_extra_libs += simeon_avx_lib
  endif
endif

simeon_lib = static_library('yams_simeon',
  simeon_core_srcs + ['../embedding_simeon/simeon_embedding_backend.cpp'],
  include_directories: [simeon_inc, include_directories('../../include')],
  dependencies: [spdlog_dep, tl_expected_dep, yams_core],
  link_with: simeon_extra_libs,
  install: false,
)

# Define SQLITE_VEC_STATIC based on whether we have sqlite-vec-cpp sources
if sqlite_vec_cpp_src.length() > 0
  vector_cppargs += ['-DSQLITE_VEC_STATIC=1', '-DSQLITE_VEC_CPP=1']
  message('Building with C++20/23 sqlite-vec-cpp implementation')
else
  vector_cppargs += ['-DSQLITE_VEC_STATIC=0']
endif

yams_daemon_client = dependency('yams_daemon_client', required: true)

deps_vec = [spdlog_dep, sqlite_dep, json_dep, fmt_dep, boost_dep, yams_core, yams_metadata, tl_expected_dep, yams_daemon_client]

vector_platform_deps = []
if host_machine.system() == 'windows'
  vector_platform_deps += [cpp_comp.find_library('dxgi', required: true)]
endif

yams_vector = static_library('yams_vector',
  srcs,
  include_directories: [include_directories('../../include'), include_directories('.'), sqlite_vec_cpp_inc, simeon_inc],
  dependencies: deps_vec + vector_platform_deps,
  link_with: [simeon_lib],
  cpp_args: vector_cppargs,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_vector,
  name: 'yams-vector',
  description: 'YAMS vector database',
  subdirs: ['yams'],
)

yams_vector_dep = declare_dependency(
  link_with: [yams_vector, simeon_lib],
  include_directories: [include_directories('../../include'), simeon_inc],
  dependencies: vector_platform_deps,
)
meson.override_dependency('yams_vector', yams_vector_dep)
