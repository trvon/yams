spdlog_dep = dependency('spdlog', required: true)
sqlite_dep = dependency('sqlite3', required: true)
json_dep = dependency('nlohmann_json', required: true)
fmt_dep = dependency('fmt', required: false)
boost_dep = dependency('boost', required: true)
yams_core = dependency('yams_core', required: false)
yams_metadata = dependency('yams_metadata', required: false)
tl_expected_dep = dependency('tl-expected', required: true)
fs = import('fs')

onnx_opt = get_option('enable-onnx')

# Try to locate onnxruntime via Conan generator directories
conan_gen_opt = get_option('conan-generators-dir')
conan_candidate_dirs = []
if conan_gen_opt != ''
  conan_candidate_dirs += [join_paths(meson.project_source_root(), conan_gen_opt)]
endif

base_build = meson.project_build_root()
default_candidates = [
  base_build,
  join_paths(base_build, 'conan'),
  join_paths(base_build, 'conan', 'generators'),
  join_paths(base_build, 'generators'),
]

variant_dirs = ['build-debug', 'build-release', 'debug', 'release']
foreach variant : variant_dirs
  variant_path = join_paths(base_build, variant)
  if fs.exists(variant_path)
    default_candidates += [variant_path]
    default_candidates += [join_paths(variant_path, 'conan')]
    default_candidates += [join_paths(variant_path, 'conan', 'generators')]
    default_candidates += [join_paths(variant_path, 'generators')]
  endif
endforeach

foreach cand : default_candidates
  if fs.exists(cand)
    conan_candidate_dirs += [cand]
  endif
endforeach

onnx_cmake_paths = []
foreach base : conan_candidate_dirs
  if fs.exists(base)
    onnx_cmake_paths += base
    foreach sub : ['onnxruntime', 'onnxruntime/Release', 'onnxruntime/Debug']
      cand = join_paths(base, sub)
      if fs.exists(cand)
        onnx_cmake_paths += cand
      endif
    endforeach
  endif
endforeach

# Allow explicit override (Homebrew/custom installs)
onnx_opt_path = get_option('onnxruntime-cmake-path')
if onnx_opt_path != ''
  onnx_cmake_paths += onnx_opt_path
endif

# Prefer pkg-config (Conan ships onnxruntime.pc); fall back to CMake if available
onnx_dep = dependency('onnxruntime', required: false)
if (not onnx_dep.found()) and (onnx_cmake_paths.length() > 0)
  onnx_dep = dependency('onnxruntime', method: 'cmake', cmake_module_path: onnx_cmake_paths, required: false)
endif

onnx_feature_requested = not onnx_opt.disabled()
onnx_runtime_link = false
onnx_genai_headers_present = false
onnx_include_dirs = []
if onnx_dep.found()
  prefix = onnx_dep.get_variable(pkgconfig: 'prefix', default_value: '')
  if prefix != ''
    foreach key : ['includedir', 'includedir1', 'includedir2']
      dirval = onnx_dep.get_variable(pkgconfig: key, default_value: '')
      if dirval != ''
        dirval = dirval.replace('${prefix}', prefix)
        onnx_include_dirs += dirval
      endif
    endforeach
  endif
endif

if onnx_feature_requested and onnx_dep.found()
  onnx_runtime_link = true

  genai_header_candidates = [
    'ort_genai.h',
    'onnxruntime/genai/ort_genai.h',
    'onnxruntime/genai/embedding.h',
    'onnxruntime/genai/genai_api.h',
    'onnxruntime/genai/pipeline.h',
  ]

  foreach incdir : onnx_include_dirs
    if incdir == ''
      continue
    endif
    foreach hdr : genai_header_candidates
      if fs.exists(join_paths(incdir, hdr))
        onnx_genai_headers_present = true
        break
      endif
    endforeach
    if onnx_genai_headers_present
      break
    endif
  endforeach
endif

# Optional include dirs for third-party headers
# Treat third-party headers as system to avoid external warnings being promoted to errors
hnsw_inc = include_directories('../../third_party/hnswlib', is_system: true)

# Generate sqlite-vec.h (as in CMake)
sqlite_vec_hdr = configure_file(
  input: 'sqlite-vec.h.in',
  output: 'sqlite-vec.h',
  copy: true,
)

# Check for C++ implementation first, fallback to C
sqlite_vec_cpp_dir = join_paths(meson.project_source_root(), 'third_party', 'sqlite-vec-cpp')
sqlite_vec_cpp_inc = []
sqlite_vec_cpp_src = []
use_cpp_impl = false

if fs.exists(join_paths(sqlite_vec_cpp_dir, 'include', 'sqlite-vec-cpp', 'sqlite_vec.hpp'))
  message('Found sqlite-vec-cpp implementation')
  use_cpp_impl = true
  sqlite_vec_cpp_inc = include_directories(join_paths('../../third_party', 'sqlite-vec-cpp', 'include'), is_system: true)
  # Add C++ sources from sqlite-vec-cpp
  sqlite_vec_cpp_src = [
    join_paths(sqlite_vec_cpp_dir, 'src', 'sqlite_vec_c_api.cpp'),
  ]
endif

sqlite_vec_src_dir = get_option('sqlite-vec-source-dir')
sqlite_vec_src = []
if not use_cpp_impl
  if sqlite_vec_src_dir == ''
    candidate = join_paths(meson.project_source_root(), 'third_party', 'sqlite-vec')
    if fs.exists(join_paths(candidate, 'sqlite-vec.c'))
      sqlite_vec_src_dir = candidate
    endif
  endif
  if sqlite_vec_src_dir != ''
    sqlite_vec_src = [join_paths(sqlite_vec_src_dir, 'sqlite-vec.c')]
  endif
endif

# Enforce static sqlite-vec by default for clean, consistent builds.
if sqlite_vec_src.length() == 0 and sqlite_vec_cpp_src.length() == 0
  if get_option('require-sqlite-vec')
    error('Neither sqlite-vec.c nor sqlite-vec-cpp found. Vendor the sources under third_party/sqlite-vec/ or third_party/sqlite-vec-cpp/. ' +
          'Alternatively, pass -Dsqlite-vec-source-dir=/absolute/path/to/sqlite-vec. ' +
          'To bypass (NOT recommended), configure with -Drequire-sqlite-vec=false and provide a runtime module via YAMS_SQLITE_VEC_MODULE.')
  else
    message('sqlite-vec sources not found and require-sqlite-vec=false; building without static sqlite-vec (runtime module required).')
  endif
endif

vector_cppargs = [
  '-DSQLITE_CORE=1',
  '-DSQLITE_ENABLE_LOAD_EXTENSION=1',
]

# C sources (sqlite-vec.c) need the same defines
vector_cargs = [
  '-DSQLITE_CORE=1',
  '-DSQLITE_ENABLE_LOAD_EXTENSION=1',
]

# Suppress strict warnings from vendored C source sqlite-vec.c without weakening
# warnings for the rest of the project. These flags apply only to C sources in
# this target (not C++).
cc = meson.get_compiler('c')
vector_cargs += cc.get_supported_arguments([
  '-Wno-c23-extensions',      # allow binary integer literals like 0b11111111
  '-Wno-unused-parameter',    # sqlite-vec entry points have unused sqlite3_api params
])

if fmt_dep.found()
  # Force fallback to {fmt} implementation in our format shim
  vector_cppargs += ['-DYAMS_STD_FORMAT_AVAILABLE=0']
endif

if onnx_feature_requested
  vector_cppargs += ['-DYAMS_USE_ONNX_GENAI_HEADER_PRESENT']
  if onnx_genai_headers_present
    vector_cppargs += ['-DYAMS_GENAI_RUNTIME_PRESENT']
  endif
  if onnx_runtime_link
    vector_cppargs += ['-DYAMS_USE_ONNX_RUNTIME']
  elif onnx_opt.enabled()
    message('enable-onnx is enabled but onnxruntime dependency not found; building stubbed vector backend')
  endif
else
  message('enable-onnx disabled by configuration; building without ONNX support')
endif

srcs = [
  'vector_database.cpp',
  'embedding_generator.cpp',
  'embedding_service.cpp',
  'batch_metrics.cpp',
  'document_chunker.cpp',
  'vector_index_manager.cpp',
  'vector_index_optimizer.cpp',
  'model_registry.cpp',
  'model_cache.cpp',
  'model_loader.cpp',
  'sqlite_vec_backend.cpp',
  'mock_embedding_provider.cpp',
  'dim_resolver.cpp',
  '../genai/onnx_genai_adapter.cpp',
] + (use_cpp_impl ? sqlite_vec_cpp_src : sqlite_vec_src)

# Define SQLITE_VEC_STATIC based on whether we have sqlite-vec sources
if sqlite_vec_src.length() > 0 or sqlite_vec_cpp_src.length() > 0
  vector_cppargs += ['-DSQLITE_VEC_STATIC=1']
else
  vector_cppargs += ['-DSQLITE_VEC_STATIC=0']
endif

if use_cpp_impl
  vector_cppargs += ['-DSQLITE_VEC_CPP=1']
  message('Building with C++20/23 sqlite-vec implementation')
endif

deps_vec = [spdlog_dep, sqlite_dep, json_dep, fmt_dep, boost_dep, yams_core, yams_metadata, tl_expected_dep]
if onnx_runtime_link
  deps_vec += onnx_dep
endif

yams_vector = static_library('yams_vector',
  srcs,
  include_directories: [include_directories('../../include'), include_directories('.'), hnsw_inc] + (use_cpp_impl ? [sqlite_vec_cpp_inc] : []),
  dependencies: deps_vec,
  cpp_args: vector_cppargs,
  c_args: vector_cargs,
  install: true,
)

pkg = import('pkgconfig')
pkg.generate(yams_vector,
  name: 'yams-vector',
  description: 'YAMS vector database',
  subdirs: ['yams'],
)

yams_vector_dep = declare_dependency(
  link_with: yams_vector,
  include_directories: include_directories('../../include'),
)
meson.override_dependency('yams_vector', yams_vector_dep)
