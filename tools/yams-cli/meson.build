# Respect build flag and presence of CLI library; skip if unavailable
if not get_option('build-cli')
  message('build-cli=false; skipping yams-cli target')
  subdir_done()
endif

fs = import('fs')

yams_cli_dep = dependency('yams_cli', required: false)
if not yams_cli_dep.found()
  message('yams_cli library not found; skipping yams-cli target (build of CLI lib may have been skipped due to missing CLI11).')
  subdir_done()
endif

yams_cli_exe_deps = [
  yams_cli_dep,
  dependency('yams_daemon_client'),
  dependency('yams_app_services'),
  dependency('yams_repair'),
  dependency('yams_api'),
  dependency('yams_search'),
  dependency('yams_vector'),
  dependency('yams_content'),
  dependency('yams_downloader'),
  dependency('yams_metadata'),
  dependency('yams_wal'),
  dependency('yams_storage_engine'),
  dependency('yams_reference_counter'),
  dependency('yams_manifest'),
  dependency('yams_integrity'),
  dependency('yams_chunking'),
  dependency('yams_compression'),
  dependency('yams_crypto'),
  dependency('yams_detection'),
  dependency('yams_extraction'),
  dependency('yams_config'),
  dependency('yams_core'),
  boost_dep,
  dependency('spdlog'),
  dependency('fmt'),
]

if cli11_resolved_dep.found()
  yams_cli_exe_deps += cli11_resolved_dep
endif

# Set platform-specific rpath
# macOS uses @executable_path, Linux uses $ORIGIN
if host_machine.system() == 'darwin'
  cli_install_rpath = '@executable_path/../' + get_option('libdir') / 'yams'
else
  cli_install_rpath = '$ORIGIN/../' + get_option('libdir') / 'yams'
endif

# On Windows, use delay-load for onnxruntime.dll to allow SetDllDirectory before loading
cli_link_args = []
if host_machine.system() == 'windows'
  cli_link_args += ['/DELAYLOAD:onnxruntime.dll', 'delayimp.lib']
endif

yams_cli_exe = executable('yams-cli',
  'main.cpp',
  dependencies: yams_cli_exe_deps,
  link_args: cli_link_args,
  install: true,
  install_dir: get_option('bindir'),
  install_rpath: cli_install_rpath,
)

# --- Windows: Copy runtime DLLs next to the CLI executable ---
# Use install: true so DLLs are included in meson install output for MSI packaging
if host_machine.system() == 'windows' and windows_runtime_dlls.length() > 0
  foreach dll_info : windows_runtime_dlls
    dll_name = dll_info[0]
    dll_src = dll_info[1]
    fs.copyfile(dll_src, dll_name, install: true, install_dir: get_option('bindir'), install_tag: 'runtime')
  endforeach
endif

# Bundle the ONNX Runtime shared library this build was made with as a private
# fallback copy in <libdir>/yams/onnxruntime. The ONNX and Glint plugins prefer a
# compatible system ONNX Runtime and fall back to this copy (see
# plugins/ort_runtime/ort_runtime_resolution.h). Real files are installed once and
# versioned symlinks are recreated, so archives keep the soname chain intact.
# Windows already ships onnxruntime.dll next to the executables.
if onnxruntime_resolved_dep.found() and host_machine.system() != 'windows' and onnxruntime_bundle_dirs.length() > 0
  _ort_list = run_command(python_exe, files('../../scripts/list-onnxruntime-libs.py'),
    onnxruntime_bundle_dirs, check: true)
  _ort_dir = ''
  _ort_install_dir = get_option('libdir') / 'yams' / 'onnxruntime'
  foreach _line : _ort_list.stdout().strip().split('\n')
    _parts = _line.strip().split(' ')
    if _parts.length() >= 2 and _parts[0] == 'dir'
      _ort_dir = _line.strip().substring(4)
    elif _parts.length() == 2 and _parts[0] == 'file'
      install_data(_ort_dir / _parts[1], install_dir: _ort_install_dir, install_tag: 'runtime')
    elif _parts.length() == 3 and _parts[0] == 'link'
      install_symlink(_parts[1], pointing_to: _parts[2], install_dir: _ort_install_dir,
        install_tag: 'runtime')
    endif
  endforeach
  if _ort_dir == ''
    warning('ONNX Runtime found but no shared library to bundle in: ' + ', '.join(onnxruntime_bundle_dirs))
  else
    message('Bundling ONNX Runtime from ' + _ort_dir + ' into ' + _ort_install_dir)
  endif
endif

# Also install a friendly `yams` alias as a symlink next to the binary.
# This avoids duplicate builds while giving users the expected command name.
symlink_name = 'yams'
symlink_target = 'yams-cli'

if host_machine.system() == 'windows'
  symlink_name = 'yams.exe'
  symlink_target = 'yams-cli.exe'
endif

install_symlink(symlink_name,
  install_dir: get_option('bindir'),
  pointing_to: symlink_target,
  install_tag: 'runtime',
)
