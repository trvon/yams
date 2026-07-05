project('libsql', 'c', version: '0.3.4')

cargo = find_program('cargo', required: true)
python = find_program('python', 'python3', required: true)
cc = meson.get_compiler('c')

buildtype = get_option('buildtype')
use_debug = buildtype == 'debug'

cargo_target = ''
git_usr_bin = ''
if host_machine.system() == 'windows'
  libname = 'libsql.lib'
  target_is_x64 = cc.compiles('''
#if !defined(_M_X64) && !defined(__x86_64__)
#error not x64
#endif
int main(void) { return 0; }
''', name: 'libsql cargo target is x64')
  target_is_arm64 = cc.compiles('''
#if !defined(_M_ARM64) && !defined(__aarch64__)
#error not arm64
#endif
int main(void) { return 0; }
''', name: 'libsql cargo target is arm64')
  if target_is_x64
    cargo_target = 'x86_64-pc-windows-msvc'
  elif target_is_arm64
    cargo_target = 'aarch64-pc-windows-msvc'
  endif
  git_program = find_program('git', required: false)
  if git_program.found()
    git_usr_bin = git_program.full_path()
  endif
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
    'src_dir, profile, libname, output_path, cargo, cargo_target, git_usr_bin = sys.argv[1:8]\n' +
    'env = os.environ.copy()\n' +
    'if git_usr_bin:\n' +
    '    git_dir = os.path.dirname(git_usr_bin)\n' +
    '    candidates = [os.path.normpath(os.path.join(git_dir, "..", "usr", "bin"))]\n' +
    '    candidates += [r"C:\\Program Files\\Git\\usr\\bin", r"C:\\Program Files (x86)\\Git\\usr\\bin"]\n' +
    '    for candidate in candidates:\n' +
    '        if os.path.exists(os.path.join(candidate, "cp.exe")):\n' +
    '            env["PATH"] = env.get("PATH", "") + os.pathsep + candidate\n' +
    '            break\n' +
    'if cargo_target and sys.platform == "win32":\n' +
    '    scoop_dir = os.path.dirname(os.path.dirname(cargo)) if os.path.basename(os.path.dirname(cargo)).lower() == "shims" else ""\n' +
    '    rustup_home = os.path.join(scoop_dir, "persist", "rustup", ".rustup") if scoop_dir else ""\n' +
    '    cargo_home = os.path.join(scoop_dir, "persist", "rustup", ".cargo") if scoop_dir else ""\n' +
    '    toolchains_dir = os.path.join(rustup_home, "toolchains")\n' +
    '    if os.path.isdir(toolchains_dir):\n' +
    '        for toolchain in os.listdir(toolchains_dir):\n' +
    '            candidate = os.path.join(toolchains_dir, toolchain)\n' +
    '            target_lib = os.path.join(candidate, "lib", "rustlib", cargo_target)\n' +
    '            cargo_exe = os.path.join(candidate, "bin", "cargo.exe")\n' +
    '            if os.path.isdir(target_lib) and os.path.exists(cargo_exe):\n' +
    '                cargo = cargo_exe\n' +
    '                env["RUSTUP_HOME"] = rustup_home\n' +
    '                env["CARGO_HOME"] = cargo_home\n' +
    '                env["RUSTUP_TOOLCHAIN"] = toolchain\n' +
    '                env["PATH"] = os.path.join(candidate, "bin") + os.pathsep + env.get("PATH", "")\n' +
    '                break\n' +
    '    target_env = cargo_target.replace("-", "_")\n' +
    '    env.setdefault("CMAKE_GENERATOR", "Ninja")\n' +
    '    env.setdefault("TARGET_CMAKE_GENERATOR", "Ninja")\n' +
    '    env.setdefault(f"CMAKE_GENERATOR_{target_env}", "Ninja")\n' +
    '    ninja = shutil.which("ninja", path=env.get("PATH", "")) or shutil.which("ninja.exe", path=env.get("PATH", ""))\n' +
    '    if ninja:\n' +
    '        env.setdefault("CMAKE_MAKE_PROGRAM", ninja)\n' +
    '        env.setdefault("TARGET_CMAKE_MAKE_PROGRAM", ninja)\n' +
    '        env.setdefault(f"CMAKE_MAKE_PROGRAM_{target_env}", ninja)\n' +
    'cmd = [cargo, "build", "--locked"]\n' +
    'if cargo_target:\n' +
    '    cmd += ["--target", cargo_target]\n' +
    'if profile == "release":\n' +
    '    cmd.append("--release")\n' +
    'subprocess.check_call(cmd, cwd=src_dir, env=env)\n' +
    'target_dir = os.path.join(src_dir, "target")\n' +
    'if cargo_target:\n' +
    '    target_dir = os.path.join(target_dir, cargo_target)\n' +
    'lib_path = os.path.join(target_dir, profile, libname)\n' +
    'if not os.path.exists(lib_path):\n' +
    '    raise SystemExit(f"libsql output not found: {lib_path}")\n' +
    'os.makedirs(os.path.dirname(output_path), exist_ok=True)\n' +
    'shutil.copy2(lib_path, output_path)\n',
    meson.current_source_dir(),
    profile,
    libname,
    '@OUTPUT@',
    cargo,
    cargo_target,
    git_usr_bin
  ],
  build_by_default: true
)

libsql_include = include_directories('.')
libsql_dep = declare_dependency(
  include_directories: libsql_include,
  link_args: [join_paths(meson.current_build_dir(), libname)],
  sources: libsql_lib
)
