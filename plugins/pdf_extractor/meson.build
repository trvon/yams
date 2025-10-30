if not get_option('plugin-pdf')
  subdir_done()
endif

# QPDF library via pkg-config
qpdf_dep = dependency('libqpdf', method: 'pkg-config', required: false)
spdlog_dep = dependency('spdlog', required: false)
yams_core = dependency('yams_core', required: false)
yams_app_services = dependency('yams_app_services', required: false)
yams_daemon = dependency('yams_daemon', required: false)

if not (qpdf_dep.found() and yams_app_services.found() and yams_daemon.found())
  # Defer building until app/services and qpdf are available in Meson
  subdir_done()
endif

yams_pdf_extractor = shared_module('yams_pdf_extractor',
  [ 'pdf_extractor_plugin.cpp', 'pdf_extractor.cpp', 'pdf_content_handler.cpp' ],
  include_directories: [ include_directories('../../include'), include_directories('.') ],
  dependencies: [qpdf_dep, spdlog_dep, yams_core, yams_app_services, yams_daemon],
  install: true,
  install_dir: plugins_dir,
)

