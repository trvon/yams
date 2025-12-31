# zyp - Zig YAMS PDF Extractor
# High-performance PDF text extraction using zpdf (https://github.com/Lulzx/zpdf)

if not get_option('plugin-zyp')
  subdir_done()
endif

# Find Zig compiler (requires 0.15.2+)
zig = find_program('zig', required: false)
if not zig.found()
  warning('Zig compiler not found, skipping zyp plugin. Install Zig 0.15.2+ to enable.')
  subdir_done()
endif

# Platform-specific library name and rpath
if host_machine.system() == 'darwin'
  zpdf_lib_name = 'libzpdf.dylib'
  # @loader_path is used for shared libraries/modules to find libs in same directory
  zyp_install_rpath = '@loader_path'
  zyp_link_args = ['-fno-sanitize=thread']
elif host_machine.system() == 'windows'
  zpdf_lib_name = 'zpdf.dll'
  zyp_install_rpath = ''  # Windows doesn't use rpath
  zyp_link_args = ['/NODEFAULTLIB:libcmt']  # Avoid linker conflicts
else
  zpdf_lib_name = 'libzpdf.so'
  # $ORIGIN refers to directory containing the loading module
  zyp_install_rpath = '$ORIGIN'
  zyp_link_args = ['-fno-sanitize=thread']
endif

# Build zpdf shared library via Zig and copy to build directory
# Zig outputs to zpdf/zig-out/lib/, we copy it to make it accessible for linking
zpdf_src_dir = meson.current_source_dir() / 'zpdf'
zpdf_lib_src = zpdf_src_dir / 'zig-out' / 'lib' / zpdf_lib_name
zpdf_build_dir = meson.current_build_dir()

# Platform-specific build command
if host_machine.system() == 'windows'
  # Windows: Use PowerShell for build
  zpdf_lib = custom_target('zpdf_shared',
    command: [
      'powershell', '-NoProfile', '-Command',
      'Push-Location "@0@"; & "@1@" build -Doptimize=ReleaseFast; Copy-Item "@2@" "@3@\\@4@"; Pop-Location'.format(
        zpdf_src_dir,
        zig.full_path(),
        zpdf_lib_src,
        zpdf_build_dir,
        zpdf_lib_name
      )
    ],
    output: [zpdf_lib_name],
    build_by_default: true,
    console: true,
  )
else
  # Unix (Linux/macOS): Use shell
  zpdf_lib = custom_target('zpdf_shared',
    command: [
      'sh', '-c',
      'cd "@0@" && "@1@" build -Doptimize=ReleaseFast && cp "@2@" "@3@/@4@"'.format(
        zpdf_src_dir,
        zig.full_path(),
        zpdf_lib_src,
        zpdf_build_dir,
        zpdf_lib_name
      )
    ],
    output: [zpdf_lib_name],
    build_by_default: true,
    console: true,
  )
endif

# Create a dependency object that includes the zpdf library
# Use build_rpath for build-time, install_rpath handles installed location
zpdf_dep = declare_dependency(
  link_args: ['-L' + zpdf_build_dir, '-lzpdf'],
  sources: [zpdf_lib],  # This creates the build dependency
)

# Plugin sources
zyp_sources = files(
  'zyp_plugin.cpp',
  'zpdf_wrapper.cpp',
  'pdf_metadata.cpp',
)

# Build the plugin (minimal dependencies - no spdlog to avoid TSan conflicts)
zyp_plugin = shared_module('yams_zyp',
  zyp_sources,
  include_directories: [
    include_directories('../../include'),
    include_directories('.'),
  ],
  dependencies: [zpdf_dep],
  cpp_args: ['-std=c++20', '-fno-sanitize=thread'],
  link_args: zyp_link_args,
  # build_rpath ensures the plugin can find libzpdf during build/test
  build_rpath: zpdf_build_dir,
  # install_rpath ensures the plugin can find libzpdf after installation
  install_rpath: zyp_install_rpath,
  build_by_default: true,
  install: true,
  install_dir: plugins_dir,
  name_prefix: '',
)

# Install zpdf library alongside the plugin
# Use platform-specific install script
if host_machine.system() == 'windows'
  meson.add_install_script('powershell', '-NoProfile', '-Command',
    'Copy-Item -Path "@0@" -Destination "$env:DESTDIR@1@"'.format(
      zpdf_build_dir / zpdf_lib_name,
      get_option('prefix') / plugins_dir
    )
  )
else
  meson.add_install_script('sh', '-c',
    'mkdir -p "$DESTDIR@1@" && cp "@0@" "$DESTDIR@1@/"'.format(
      zpdf_build_dir / zpdf_lib_name,
      get_option('prefix') / plugins_dir
    )
  )
endif
