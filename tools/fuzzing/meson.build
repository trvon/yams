# YAMS Fuzzing Harnesses
#
# Builds fuzzers for AFL++ using libFuzzer compatibility mode.
# AFL++ docs: "afl-clang-fast++ -fsanitize=fuzzer ... provides the fastest speed possible"
#
# Build with:
#   ./tools/fuzzing/fuzz-docker.sh build

cpp = meson.get_compiler('cpp')

# Check if we're using AFL++ compiler wrappers
# AFL++ automatically defines __AFL_COMPILER
afl_check = cpp.compiles('''
  #ifndef __AFL_COMPILER
  #error "Not AFL++"
  #endif
  int main() { return 0; }
''', name: 'AFL++ compiler check')

if not afl_check
  warning('AFL++ compiler not detected. Fuzzers require afl-clang-fast++')
  warning('Run: ./tools/fuzzing/fuzz-docker.sh build')
  subdir_done()
endif

message('AFL++ compiler detected - building fuzzers')

# AFL++ libFuzzer compatibility mode flags
# Per docs: "-fsanitize=fuzzer" provides automatic persistent mode
fuzzer_args = ['-fsanitize=fuzzer']

# Get daemon IPC library dependencies
yams_core_dep = dependency('yams_core', required: true)
yams_daemon_dep = dependency('yams_daemon', required: false)

# If daemon dependency doesn't exist as pkg-config, link directly
daemon_deps = [yams_core_dep]
if not yams_daemon_dep.found()
  # Link against daemon libraries built in this tree
  daemon_deps += dependency('threads')
  daemon_deps += dependency('boost', modules: ['system'], method: 'cmake')
endif

# IPC Protocol Fuzzer
fuzz_ipc_protocol = executable('fuzz_ipc_protocol',
  'fuzz_ipc_protocol.cpp',
  include_directories: include_directories('../../include'),
  dependencies: daemon_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# AddDocumentRequest Fuzzer
fuzz_add_document = executable('fuzz_add_document',
  'fuzz_add_document.cpp',
  include_directories: include_directories('../../include'),
  dependencies: daemon_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

message('Fuzzing harnesses configured with AFL++ libFuzzer mode')
message('ASAN enabled via AFL_USE_ASAN=1 environment variable')
