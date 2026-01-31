mobile_sources = [
  'mobile_bindings.cpp',
]

# TSAN handling for mobile shared library:
# When building a shared library with TSAN-instrumented static dependencies,
# we need to ensure the TSAN runtime is explicitly linked.
mobile_deps = [
  dependency('yams_app_services'),
  dependency('yams_metadata'),
  dependency('yams_api'),
  dependency('yams_search'),
  dependency('yams_vector'),
  dependency('yams_indexing'),
  dependency('yams_storage_engine'),
  dependency('yams_manifest'),
  dependency('yams_chunking'),
  dependency('yams_crypto'),
  dependency('yams_daemon'),
  dependency('boost'),
  dependency('nlohmann_json'),
  dependency('sqlite3'),
  dependency('spdlog'),
]

mobile_cpp_args = []
mobile_link_args = []

if get_option('enable-tsan')
  # When TSAN is enabled, we must ensure libtsan is linked
  # The -fsanitize=thread flag should do this, but for shared libraries
  # we need to be explicit to avoid undefined references
  cpp_compiler = meson.get_compiler('cpp')
  
  # On Linux, find the TSAN runtime library
  if host_machine.system() == 'linux'
    # First, try to find libtsan as a dependency
    tsan_dep = cpp_compiler.find_library('tsan', required: false)
    if tsan_dep.found()
      mobile_deps += tsan_dep
      message('Mobile library: found and linked TSAN runtime library')
    else
      # Fallback: the -fsanitize=thread linker flag should handle it
      # This is already set globally via add_project_link_arguments
      message('Mobile library: relying on -fsanitize=thread to link TSAN runtime')
    endif
  endif
endif

if get_option('enable-mobile-bindings')
  yams_mobile_lib = shared_library('yams_mobile',
    mobile_sources,
    dependencies: mobile_deps,
    include_directories: include_directories('../../include'),
    cpp_args: mobile_cpp_args,
    link_args: mobile_link_args,
    install: true,
  )

  yams_mobile_dep = declare_dependency(
    link_with: yams_mobile_lib,
    include_directories: include_directories('../../include'),
  )

  meson.override_dependency('yams_mobile', yams_mobile_dep)

  pkg.generate(yams_mobile_lib,
    name: 'yams_mobile',
    description: 'YAMS mobile C ABI',
    subdirs: ['yams/api'],
  )
else
  yams_mobile_dep = disabler()
endif
