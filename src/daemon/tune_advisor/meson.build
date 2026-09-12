# TuneAdvisor's method bodies live in one translation unit. Everything that reads the
# knobs (vector, repair, app services, MCP, the ONNX plugin, the daemon) links this
# library instead of re-instantiating the bodies from the header. The override slots
# stay `static inline` in the header, so the plugin and the daemon share them exactly as
# they did when the bodies were inline.
yams_tune_advisor_lib = static_library('yams_tune_advisor',
  files('../components/TuneAdvisor.cpp'),
  include_directories: include_directories('../../../include'),
  dependencies: [dependency('yams_config'), dependency('yams_metadata')],
  install: true,
)

yams_tune_advisor_dep = declare_dependency(
  link_with: yams_tune_advisor_lib,
  include_directories: include_directories('../../../include'),
  dependencies: [dependency('yams_config'), dependency('yams_metadata')],
)

meson.override_dependency('yams_tune_advisor', yams_tune_advisor_dep)

# Test executables need the YAMS_TESTING build of the same bodies: under that define
# compatibilityEnvironment() reads the process environment live instead of the startup
# snapshot, which is what every env-driven tuning test relies on. Before the split each test
# TU instantiated the inline bodies itself with the define; now the bodies are in one TU, so
# tests link this variant ahead of the production archive (first definition wins, the
# production member is never pulled).
# Same predicate that enters tests/ (root meson.build) plus benchmarks: a plain debug
# configure builds tests, so it must build this archive too.
if get_option('build-tests') or get_option('build-benchmarks') or (get_option('buildtype') == 'debug' and not get_option('build-fuzzers'))
  yams_tune_advisor_testing_lib = static_library('yams_tune_advisor_testing',
    files('../components/TuneAdvisor.cpp'),
    include_directories: include_directories('../../../include'),
    dependencies: [dependency('yams_config'), dependency('yams_metadata')],
    cpp_args: ['-DYAMS_TESTING=1'],
    install: false,
  )
  yams_tune_advisor_testing_dep = declare_dependency(
    link_with: yams_tune_advisor_testing_lib,
    include_directories: include_directories('../../../include'),
    dependencies: [dependency('yams_config'), dependency('yams_metadata')],
  )
  meson.override_dependency('yams_tune_advisor_testing', yams_tune_advisor_testing_dep)
endif
