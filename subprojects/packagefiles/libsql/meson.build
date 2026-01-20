project('libsql', 'c', version: '0.3.4')

cargo = find_program('cargo', required: true)
python = find_program('python3', 'python', required: true)

buildtype = get_option('buildtype')
use_debug = buildtype == 'debug'

if host_machine.system() == 'windows'
  libname = 'libsql.lib'
else
  libname = 'liblibsql.a'
endif

profile = use_debug ? 'debug' : 'release'

libsql_lib = custom_target(
  'libsql_build',
  output: libname,
  command: [
    python,
    '-c',
    'import os, shutil, subprocess, sys\n' +
    'src_dir, profile, libname, output_path, cargo = sys.argv[1:6]\n' +
    'cmd = [cargo, "build", "--locked"]\n' +
    'if profile == "release":\n' +
    '    cmd.append("--release")\n' +
    'subprocess.check_call(cmd, cwd=src_dir)\n' +
    'lib_path = os.path.join(src_dir, "target", profile, libname)\n' +
    'if not os.path.exists(lib_path):\n' +
    '    raise SystemExit(f"libsql output not found: {lib_path}")\n' +
    'os.makedirs(os.path.dirname(output_path), exist_ok=True)\n' +
    'shutil.copy2(lib_path, output_path)\n',
    meson.current_source_dir(),
    profile,
    libname,
    '@OUTPUT@',
    cargo
  ],
  build_by_default: true
)

libsql_include = include_directories(meson.current_source_dir())
libsql_dep = declare_dependency(
  include_directories: libsql_include,
  link_args: [join_paths(meson.current_build_dir(), libname)],
  sources: libsql_lib
)
