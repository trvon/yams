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

# Link against the daemon library built in this tree
# The daemon library contains MessageFramer and IPC protocol implementation
# Note: yams_daemon_lib is defined in src/daemon/meson.build and available in this scope

# IPC Protocol Fuzzer
fuzz_ipc_protocol = executable('fuzz_ipc_protocol',
  'fuzz_ipc_protocol.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# AddDocumentRequest Fuzzer
fuzz_add_document = executable('fuzz_add_document',
  'fuzz_add_document.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# ProtoSerializer Fuzzer
fuzz_proto_serializer = executable('fuzz_proto_serializer',
  'fuzz_proto_serializer.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# RequestHandler Fuzzer
fuzz_request_handler = executable('fuzz_request_handler',
  'fuzz_request_handler.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# StreamingRequestProcessor Fuzzer
fuzz_streaming_processor = executable('fuzz_streaming_processor',
  'fuzz_streaming_processor.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

message('Fuzzing harnesses configured with AFL++ libFuzzer mode')
message('ASAN enabled via AFL_USE_ASAN=1 environment variable')
