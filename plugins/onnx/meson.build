fs = import('fs')
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

foreach variant : ['build-debug', 'build-release', 'debug', 'release']
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

# Allow explicit override from project option
onnx_opt_path = get_option('onnxruntime-cmake-path')
if onnx_opt_path != ''
  onnx_cmake_paths += onnx_opt_path
endif

onnx_dep = dependency('onnxruntime', required: false)
if (not onnx_dep.found()) and (onnx_cmake_paths.length() > 0)
  onnx_dep = dependency('onnxruntime', method: 'cmake', cmake_module_path: onnx_cmake_paths, required: false)
endif
spdlog_dep = dependency('spdlog', required: false)
json_dep = dependency('nlohmann_json', required: false)
yams_core = dependency('yams_core', required: false)
yams_vector = dependency('yams_vector', required: false)

onnx_opt = get_option('enable-onnx')
onnx_feature_requested = not onnx_opt.disabled()
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

if not get_option('plugin-onnx')
  subdir_done()
endif

if not onnx_feature_requested
  message('enable-onnx disabled; skipping yams_onnx_plugin build')
  subdir_done()
endif

# Gracefully skip when ONNX Runtime is not discoverable in this environment.
if not onnx_dep.found()
  message('ONNX Runtime not found in search paths; skipping yams_onnx_plugin')
  subdir_done()
endif

yams_onnx_plugin_srcs = [
  'onnx_plugin.cpp',
  'model_provider.cpp',
  'onnx_model_pool.cpp',
  '../../src/genai/onnx_genai_adapter.cpp',
]

plugin_cppargs = ['-DYAMS_ENABLE_ONNX_GENAI=1']
if onnx_genai_headers_present
  plugin_cppargs += ['-DYAMS_GENAI_RUNTIME_PRESENT']
endif

yams_onnx_plugin = shared_module('yams_onnx_plugin',
  yams_onnx_plugin_srcs,
  include_directories: [ include_directories('../../include') ],
  dependencies: [onnx_dep, spdlog_dep, json_dep, yams_core, yams_vector],
  cpp_args: plugin_cppargs,
  install: true,
  install_dir: plugins_dir,
)
