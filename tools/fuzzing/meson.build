# YAMS Fuzzing Harnesses
#
# Builds fuzzers for AFL++ using libFuzzer compatibility mode.
# AFL++ docs: "afl-clang-fast++ -fsanitize=fuzzer ... provides the fastest speed possible"
#
# Build with:
#   ./tools/fuzzing/fuzz.sh build

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
  warning('Run: ./tools/fuzzing/fuzz.sh build')
  subdir_done()
endif

message('AFL++ compiler detected - building fuzzers')

# Fuzzers include Boost.Asio headers directly; ensure Boost include/link flags
# are present when compiling the harnesses (static libraries don't propagate
# include dirs to consumers).
fuzz_deps = [
  dependency('boost'),
  dependency('threads'),
  dependency('zlib'),
  dependency('OpenSSL'),
  dependency('protobuf'),
  dependency('nlohmann_json'),
]

# Seed generator (non-fuzzer utility)
seedgen = executable('seedgen',
  'seedgen.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  build_by_default: true,
)

# AFL++ libFuzzer compatibility mode flags
# Per docs: "-fsanitize=fuzzer" provides automatic persistent mode
fuzzer_args = ['-fsanitize=fuzzer']

# Controlled ABI plugin used by fuzz_plugin_abi_mount.
# This keeps dlopen() deterministic and avoids loading random fuzz bytes as shared libs.
fuzz_abi_test_plugin = shared_module('fuzz_abi_test_plugin',
  'fuzz_abi_test_plugin.c',
  include_directories: include_directories('../../include'),
  c_args: ['-fPIC'],
  install: false,
  build_by_default: true,
)

fuzz_abi_negotiation_plugin = shared_module('fuzz_abi_negotiation_plugin',
  'fuzz_abi_negotiation_plugin.c',
  include_directories: include_directories('../../include'),
  c_args: ['-fPIC'],
  install: false,
  build_by_default: true,
)

# Link against the daemon library built in this tree
# The daemon library contains MessageFramer and IPC protocol implementation
# Note: yams_daemon_lib is defined in src/daemon/meson.build and available in this scope

# IPC Protocol Fuzzer
fuzz_ipc_protocol = executable('fuzz_ipc_protocol',
  'fuzz_ipc_protocol.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# AddDocumentRequest Fuzzer
fuzz_add_document = executable('fuzz_add_document',
  'fuzz_add_document.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# ProtoSerializer Fuzzer
fuzz_proto_serializer = executable('fuzz_proto_serializer',
  'fuzz_proto_serializer.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# RequestHandler Fuzzer
fuzz_request_handler = executable('fuzz_request_handler',
  'fuzz_request_handler.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# StreamingRequestProcessor Fuzzer
fuzz_streaming_processor = executable('fuzz_streaming_processor',
  'fuzz_streaming_processor.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# IPC Roundtrip Fuzzer (CLI<->daemon framing + payload)
fuzz_ipc_roundtrip = executable('fuzz_ipc_roundtrip',
  'fuzz_ipc_roundtrip.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# QueryParser Fuzzer (user-facing search query parsing)
fuzz_query_parser = executable('fuzz_query_parser',
  'fuzz_query_parser.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# Plugin Trust/Path Fuzzer (trust file parsing + safe path containment)
fuzz_plugin_trust = executable('fuzz_plugin_trust',
  'fuzz_plugin_trust.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# ABI plugin mount fuzzer (loads controlled test plugin .so through AbiPluginLoader)
fuzz_plugin_abi_mount = executable('fuzz_plugin_abi_mount',
  'fuzz_plugin_abi_mount.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# ABI interface/manifest negotiation fuzzer
fuzz_plugin_abi_negotiation = executable('fuzz_plugin_abi_negotiation',
  'fuzz_plugin_abi_negotiation.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

message('Fuzzing harnesses configured with AFL++ libFuzzer mode')
message('ASAN enabled via AFL_USE_ASAN=1 environment variable')
