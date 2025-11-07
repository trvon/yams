# Respect build flag and presence of CLI library; skip if unavailable
if not get_option('build-cli')
  message('build-cli=false; skipping yams-cli target')
  subdir_done()
endif

yams_cli_dep = dependency('yams_cli', required: false)
if not yams_cli_dep.found()
  message('yams_cli library not found; skipping yams-cli target (build of CLI lib may have been skipped due to missing CLI11).')
  subdir_done()
endif

yams_cli_exe_deps = [
  yams_cli_dep,
  # Ensure correct static link order: client first, then daemon providers
  dependency('yams_daemon_client'),
  dependency('yams_daemon'),
  dependency('yams_app_services'),
  dependency('yams_repair'),
  dependency('yams_api'),
  dependency('yams_search'),
  dependency('yams_vector'),
  dependency('yams_indexing'),
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
  dependency('boost'),
  dependency('spdlog'),
  dependency('fmt'),
  # CLI11 may be brought in via yams_cli; keep optional here to avoid hard failure
  dependency('cli11', required: false),
]

if get_option('build-mcp-server')
  yams_cli_exe_deps += dependency('yams_mcp')
endif

# Set platform-specific rpath
# macOS uses @executable_path, Linux uses $ORIGIN
if host_machine.system() == 'darwin'
  cli_install_rpath = '@executable_path/../lib'
else
  cli_install_rpath = '$ORIGIN/../lib'
endif

yams_cli_exe = executable('yams-cli',
  'main.cpp',
  dependencies: yams_cli_exe_deps,
  install: true,
  install_dir: get_option('bindir'),
  install_rpath: cli_install_rpath,
)

# Install shared libraries from conan dependencies
# This ensures runtime dependencies like libonnxruntime.so are available
onnx_dep = dependency('onnxruntime', required: false)
if onnx_dep.found()
  # Get the library directory from onnxruntime dependency
  onnx_libdir = onnx_dep.get_variable('libdir', default_value: '')
  if onnx_libdir != ''
    # Install onnxruntime shared libraries to lib directory
    install_subdir(onnx_libdir,
      install_dir: get_option('libdir'),
      strip_directory: true,
      exclude_files: ['pkgconfig'],
      exclude_directories: ['pkgconfig'],
    )
  endif
endif

# Also install a friendly `yams` alias as a symlink next to the binary.
# This avoids duplicate builds while giving users the expected command name.
install_symlink('yams',
  install_dir: get_option('bindir'),
  pointing_to: 'yams-cli',
)
