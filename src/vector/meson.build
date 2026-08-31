spdlog_dep = dependency('spdlog', required: true)
sqlite_dep = dependency('sqlite3', required: true)
json_dep = dependency('nlohmann_json', required: true)
fmt_dep = dependency('fmt', required: false)
#boost_dep inherited from top - level meson.build(per - library pkg - config probes).
#Re - invoking dependency('boost', ...) here would bypass the override on the
#modules : form(meson #14146).
yams_core = dependency('yams_core', required: false)
yams_config = dependency('yams_config', required: true)
yams_metadata = dependency('yams_metadata', required: false)
tl_expected_dep = dependency('tl-expected', required: true)
fs = import('fs')
cpp_comp = meson.get_compiler('cpp')

#Optional include dirs for third - party headers
#Note : HNSW implementation now uses sqlite - vec - cpp, hnswlib no longer needed

#Generate sqlite - vec.h(as in CMake)
sqlite_vec_hdr = configure_file(
  input: 'sqlite-vec.h.in',
  output: 'sqlite-vec.h',
  copy: true,
)

#sqlite - vec - cpp(C++ implementation)
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

#Enforce static sqlite - vec - cpp by default for clean, consistent builds.
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
if get_option('build-tests') or get_option('build-benchmarks')
  vector_cppargs += ['-DYAMS_TEST_PROVIDER_CONTROLS=1']
endif

sqlite_vec_feature_cppargs = []

#SIMD codegen flags for sqlite - vec - cpp only. These are applied per-TU to
#the sqlite_vec_cpp sources and isolated backend TU so the rest of yams_vector
#(tokenizer, chunker, etc.) is not locked into requiring AVX2 / dotprod at runtime.
vec_simd_cppargs = []

#Enable SIMD for sqlite - vec - cpp when supported
host_cpu = host_machine.cpu_family()
target_is_x86 = cpp_comp.compiles('''
#if !defined(__i386__) && !defined(__x86_64__) && !defined(_M_IX86) && !defined(_M_X64)
#error not x86
#endif
int main() {
    return 0; }
''', name: 'compiler target is x86/x64')
target_is_arm = cpp_comp.compiles('''
#if !defined(__arm__) && !defined(__aarch64__) && !defined(_M_ARM) && !defined(_M_ARM64)
#error not arm
#endif
int main() {
    return 0; }
''', name: 'compiler target is ARM/ARM64')

if target_is_x86
  # Compiler support does not prove the deployment CPU supports AVX/FMA. Keep generic release
  # binaries at the baseline ISA until sqlite-vec and all callers provide runtime dispatch.
  message('x86 sqlite-vec SIMD: disabled pending runtime CPU dispatch')
elif target_is_arm and host_cpu == 'aarch64'
  # NEON is baseline on AArch64. DotProd is guaranteed on Apple Silicon but not generic Linux or
  # Windows AArch64, so do not raise their deployment ISA from a compiler-only probe.
  sqlite_vec_feature_cppargs += ['-DSQLITE_VEC_ENABLE_NEON']
  if host_machine.system() == 'darwin'
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
    if cpp_comp.compiles(dotprod_test, args: ['-march=armv8.2-a+dotprod'], name: 'Apple Silicon DotProd (vdotq_s32)')
      vec_simd_cppargs += ['-march=armv8.2-a+dotprod']
      message('Apple Silicon DotProd: enabled for sqlite-vec-cpp')
    endif
  endif
endif

if fmt_dep.found()
#Force fallback to{fmt } implementation in our format shim
  vector_cppargs += ['-DYAMS_STD_FORMAT_AVAILABLE=0']
endif

srcs = [
  'vector_database.cpp',
  'embedding_generator.cpp',
  'embedding_daemon_backend.cpp',
  'embedding_service.cpp',
  'batch_metrics.cpp',
  'document_chunker.cpp',
  'vector_utils.cpp',
  'static_cosine_ann_index.cpp',
  'vector_schema_migration.cpp',
  'mock_embedding_provider.cpp',
  'embedding_provider_registry.cpp',
  'dim_resolver.cpp',
  'tokenizer.cpp',
]
srcs += yams_embedding_simeon_srcs

#Isolate SIMD-heavy sqlite-vec-cpp sources into their own static library so
#-mavx / -mavx2 / -mfma / -march=armv8.2-a+dotprod do not leak into the rest
#of yams_vector. This keeps generic TUs buildable on older CPUs while still
#enabling the fast sqlite-vec kernels.
sqlite_vec_cpp_lib = []
if sqlite_vec_cpp_src.length() > 0
  sqlite_vec_cpp_lib = static_library('yams_sqlite_vec_cpp',
    sqlite_vec_cpp_src,
    include_directories: [sqlite_vec_cpp_inc, include_directories('../../include'), include_directories('.')],
    dependencies: [sqlite_dep, spdlog_dep],
    cpp_args: vector_cppargs + sqlite_vec_feature_cppargs + vec_simd_cppargs,
    install: false,
  )
endif

#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
#Vendored simeon(model - free SIMD embedding) — third_party / simeon
#Built into yams_vector as an opt - in backend; standalone builds also
#live in third_party / simeon / build for library - level development.
#-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
simeon_dir = join_paths(meson.project_source_root(), 'third_party', 'simeon')
simeon_inc = include_directories('../../third_party/simeon/include', is_system: true)

simeon_common_srcs = [
  join_paths(simeon_dir, 'src', 'tokenizer.cpp'),
  join_paths(simeon_dir, 'src', 'tokenizer_bpe.cpp'),
  join_paths(simeon_dir, 'src', 'hasher.cpp'),
  join_paths(simeon_dir, 'src', 'hash_xxh64.cpp'),
  join_paths(simeon_dir, 'src', 'hash_crc32c.cpp'),
  join_paths(simeon_dir, 'src', 'hash_tabulation.cpp'),
  join_paths(simeon_dir, 'src', 'projection.cpp'),
  join_paths(simeon_dir, 'src', 'pmi.cpp'),
  join_paths(simeon_dir, 'src', 'hashed_idf.cpp'),
  join_paths(simeon_dir, 'src', 'arch', 'scalar.cpp'),
]

simeon_vector_srcs = [
  join_paths(simeon_dir, 'src', 'simeon.cpp'),
  join_paths(simeon_dir, 'src', 'pq.cpp'),
  join_paths(simeon_dir, 'src', 'matryoshka.cpp'),
]

simeon_retrieval_srcs = [
  join_paths(simeon_dir, 'src', 'bm25.cpp'),
  join_paths(simeon_dir, 'src', 'fusion.cpp'),
  join_paths(simeon_dir, 'src', 'minhash.cpp'),
  join_paths(simeon_dir, 'src', 'prf.cpp'),
  join_paths(simeon_dir, 'src', 'concept_mining.cpp'),
  join_paths(simeon_dir, 'src', 'aho_corasick.cpp'),
  join_paths(simeon_dir, 'src', 'text_rank.cpp'),
  join_paths(simeon_dir, 'src', 'fragment_geometry.cpp'),
  join_paths(simeon_dir, 'src', 'persistent_homology.cpp'),
  join_paths(simeon_dir, 'src', 'query_router.cpp'),
  join_paths(simeon_dir, 'src', 'retrieval_strategy.cpp'),
  join_paths(simeon_dir, 'src', 'corpus_adapter.cpp'),
]

#SIMD tier defines — must be visible to every TU that includes
#<simeon / simd.hpp>, since the dispatcher(l2_normalize / dot / …) is
#header - inline and guards the NEON / AVX2 call paths with
#SIMEON_HAS_NEON / SIMEON_HAS_AVX2.Without these, active_simd_tier()
#always returns Scalar even though arch / neon.cpp or arch / avx2.cpp is
#compiled in.
simeon_simd_defines = []
simeon_extra_libs = []
if target_is_arm and host_cpu == 'aarch64'
  simeon_simd_defines += '-DSIMEON_HAS_NEON=1'
  simeon_neon_lib = static_library('yams_simeon_neon',
    [join_paths(simeon_dir, 'src', 'arch', 'neon.cpp')],
    include_directories: simeon_inc,
    cpp_args: simeon_simd_defines,
    install: false,
  )
  simeon_extra_libs += simeon_neon_lib
elif target_is_x86
  # pq.cpp selects SIMEON_HAS_AVX2 at compile time instead of consulting active_simd_tier().
  # Keep x86 scalar until every call site uses runtime CPUID dispatch.
  message('x86 Simeon AVX2: disabled pending runtime CPU dispatch')
endif

simeon_common_lib = static_library('yams_simeon_common',
  simeon_common_srcs,
  include_directories: [simeon_inc, include_directories('../../include')],
  dependencies: [spdlog_dep, tl_expected_dep, yams_core],
  cpp_args: simeon_simd_defines,
  link_with: simeon_extra_libs,
  install: false,
)

simeon_vector_lib = static_library('yams_simeon_vector',
  simeon_vector_srcs,
  include_directories: [simeon_inc, include_directories('../../include')],
  dependencies: [spdlog_dep, tl_expected_dep, yams_core],
  cpp_args: simeon_simd_defines,
  link_with: [simeon_common_lib] + simeon_extra_libs,
  install: false,
)

simeon_retrieval_lib = static_library('yams_simeon_retrieval',
  simeon_retrieval_srcs,
  include_directories: [simeon_inc, include_directories('../../include')],
  dependencies: [spdlog_dep, tl_expected_dep, yams_core],
  cpp_args: simeon_simd_defines,
  link_with: [simeon_vector_lib, simeon_common_lib] + simeon_extra_libs,
  install: false,
)

simeon_retrieval_dep = declare_dependency(
  link_with: [simeon_retrieval_lib, simeon_vector_lib, simeon_common_lib] + simeon_extra_libs,
  include_directories: simeon_inc,
  compile_args: simeon_simd_defines,
)

#Define SQLITE_VEC_STATIC based on whether we have sqlite - vec - cpp sources
if sqlite_vec_cpp_src.length() > 0
  vector_cppargs += ['-DSQLITE_VEC_STATIC=1', '-DSQLITE_VEC_CPP=1']
  message('Building with C++20/23 sqlite-vec-cpp implementation')
else
  vector_cppargs += ['-DSQLITE_VEC_STATIC=0']
endif

yams_daemon_client = dependency('yams_daemon_client', required: true)

deps_vec = [spdlog_dep, sqlite_dep, json_dep, fmt_dep, boost_dep, yams_core, yams_config, yams_metadata, tl_expected_dep, yams_daemon_client]

vector_platform_deps = []
if host_machine.system() == 'windows'
  vector_platform_deps += [cpp_comp.find_library('dxgi', required: true)]
endif

sqlite_vec_backend_lib = static_library('yams_sqlite_vec_backend',
  ['sqlite_vec_backend.cpp'],
  include_directories: [include_directories('../../include'), include_directories('.'), sqlite_vec_cpp_inc, simeon_inc],
  dependencies: deps_vec + vector_platform_deps,
  # This TU instantiates sqlite-vec's header-only HNSW distance templates, so it needs the same
  # feature defines and codegen flags as the sqlite-vec implementation library.
  cpp_args: vector_cppargs + sqlite_vec_feature_cppargs + vec_simd_cppargs + simeon_simd_defines,
  install: false,
)

yams_vector = static_library('yams_vector',
  srcs,
  include_directories: [include_directories('../../include'), include_directories('.'), sqlite_vec_cpp_inc, simeon_inc],
  dependencies: deps_vec + vector_platform_deps,
  link_with: [simeon_vector_lib, simeon_common_lib, sqlite_vec_backend_lib] + simeon_extra_libs + sqlite_vec_cpp_lib,
  cpp_args: vector_cppargs + simeon_simd_defines,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_vector,
  name: 'yams-vector',
  description: 'YAMS vector database',
  subdirs: ['yams'],
)

yams_vector_dep = declare_dependency(
  link_with: [yams_vector, simeon_vector_lib, simeon_common_lib] + simeon_extra_libs,
  include_directories: include_directories('../../include'),
  dependencies: [yams_config] + vector_platform_deps,
)
meson.override_dependency('yams_vector', yams_vector_dep)
