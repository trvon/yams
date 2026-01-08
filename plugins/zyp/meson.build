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
  zpdf_import_lib_name = 'zpdf.lib'
  zyp_install_rpath = ''  # Windows doesn't use rpath
  # Link against dynamic CRT libraries; avoid static CRT conflict
  zyp_link_args = [
    '/NODEFAULTLIB:libcmt',
    '/NODEFAULTLIB:libcmtd',
    'msvcrt.lib',
    'vcruntime.lib',
    'ucrt.lib',
  ]
else
  zpdf_lib_name = 'libzpdf.so'
  # $ORIGIN refers to directory containing the loading module
  zyp_install_rpath = '$ORIGIN'
  zyp_link_args = ['-fno-sanitize=thread']
endif

# Build zpdf shared library via Zig and copy to build directory
# Zig outputs DLLs to zpdf/zig-out/bin/ on Windows, shared libs to zig-out/lib/ on Unix
zpdf_src_dir = meson.current_source_dir() / 'zpdf'
if host_machine.system() == 'windows'
  # Windows: DLL goes to bin/, import lib goes to lib/
  zpdf_lib_src = zpdf_src_dir / 'zig-out' / 'bin' / zpdf_lib_name
  zpdf_import_lib_src = zpdf_src_dir / 'zig-out' / 'lib' / zpdf_import_lib_name
else
  zpdf_lib_src = zpdf_src_dir / 'zig-out' / 'lib' / zpdf_lib_name
endif
zpdf_build_dir = meson.current_build_dir()
fs = import('fs')
if not fs.exists(zpdf_src_dir / 'build.zig')
  warning('zpdf source not found (missing build.zig), skipping zyp plugin build.')
  subdir_done()
endif

# Platform-specific build command
if host_machine.system() == 'windows'
  # Windows: Use PowerShell for build
  # Build zpdf and copy both DLL and import lib to build directory
  zpdf_lib = custom_target('zpdf_shared',
    command: [
      'powershell', '-NoProfile', '-Command',
      'Push-Location "@0@"; & "@1@" build -Doptimize=ReleaseFast; Copy-Item "@2@" "@3@\\@4@"; Copy-Item "@5@" "@3@\\@6@"; Pop-Location'.format(
        zpdf_src_dir,
        zig.full_path(),
        zpdf_lib_src,
        zpdf_build_dir,
        zpdf_lib_name,
        zpdf_import_lib_src,
        zpdf_import_lib_name
      )
    ],
    output: [zpdf_lib_name, zpdf_import_lib_name],
    build_by_default: true,
    console: true,
  )
  # Extract just the import library for linking
  zpdf_import_lib = zpdf_lib[1]
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
if host_machine.system() == 'windows'
  # On Windows, link against the import library only (not the DLL)
  # Use sources to establish build dependency, but link via explicit path
  zpdf_dep = declare_dependency(
    link_args: [zpdf_build_dir / zpdf_import_lib_name],
    sources: [zpdf_import_lib],  # Build dependency on just the import lib
  )
else
  zpdf_dep = declare_dependency(
    link_args: ['-L' + zpdf_build_dir, '-lzpdf'],
    sources: [zpdf_lib],  # This creates the build dependency
  )
endif

# Compiler-specific flags for this plugin.
zyp_cpp_args = []
if meson.get_compiler('cpp').get_id() != 'msvc'
  zyp_cpp_args = ['-std=c++20', '-fno-sanitize=thread']
endif

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
  cpp_args: zyp_cpp_args,
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
    '$dest = if ($env:DESTDIR) { Join-Path $env:DESTDIR "@1@" } else { "@1@" }; New-Item -ItemType Directory -Force -Path $dest | Out-Null; Copy-Item -Path "@0@" -Destination $dest'.format(
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
