# YAMS Fuzzing Harnesses
#
# Two engines share these targets (selected in the top-level meson.build):
#   afl       - AFL++ in libFuzzer compatibility mode (afl-clang-fast++, Docker):
#                 ./tools/fuzzing/fuzz.sh build
#   libfuzzer - plain clang with the libFuzzer runtime; every project object is built with
#               -fsanitize=fuzzer-no-link plus ASan/UBSan, harnesses link -fsanitize=fuzzer:
#                 CC=clang CXX=clang++ meson setup build/fuzz -Dbuild-fuzzers=true \
#                   -Dbuild-tests=false -Dbuild-plugins=false -Dbuild-cli=false
#                 meson compile -C build/fuzz
#               then ./tools/fuzzing/fuzz.sh local <target> (see fuzz.sh usage).

cpp = meson.get_compiler('cpp')

if yams_fuzz_engine == 'none'
  warning('No fuzzing engine detected. Use afl-clang-fast++ (./tools/fuzzing/fuzz.sh build) '
    + 'or a clang that ships the libFuzzer runtime (e.g. Homebrew/apt LLVM).')
  subdir_done()
endif

message('Fuzzing engine ' + yams_fuzz_engine + ' detected - building fuzzers')

# Fuzzers include Boost.Asio headers directly; ensure Boost include/link flags
# are present when compiling the harnesses (static libraries don't propagate
# include dirs to consumers).
fuzz_deps = [
  boost_dep,
  dependency('threads'),
  dependency('zlib'),
  dependency('OpenSSL'),
  dependency('protobuf'),
  dependency('nlohmann_json'),
]
if host_machine.system() == 'windows'
  fuzz_deps += cpp.find_library('advapi32', required: true)
endif

# Seed generator (non-fuzzer utility)
seedgen = executable('seedgen',
  'seedgen.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  build_by_default: true,
)

# AFL++ libFuzzer compatibility mode flags
# Per docs: "-fsanitize=fuzzer" provides automatic persistent mode. AFL applies ASan through
# AFL_USE_ASAN; plain libFuzzer builds link the sanitizer runtimes explicitly.
if yams_fuzz_engine == 'afl'
  fuzzer_args = ['-fsanitize=fuzzer']
else
  fuzzer_args = ['-fsanitize=fuzzer,address,undefined']
endif

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

# Direct P2P protocol-v4 handshake/control fuzzer.
# The corpus retains protocol-v3 seeds to continuously exercise exact-version rejection.
fuzz_p2p_protocol = executable('fuzz_p2p_protocol',
  'fuzz_p2p_protocol.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# Direct P2P delta/bootstrap control-message fuzzer.
fuzz_p2p_delta = executable('fuzz_p2p_delta',
  'fuzz_p2p_delta.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

# Direct P2P lane over the FrameSource seam: frame JSON, connection strings and pins, the
# handshake validators and acceptor, the delta receive paths with fuzzed session bounds, and the
# full manager inbound session. Each harness traps on an oracle violation.
foreach p2p_target : ['p2p_json', 'p2p_connstr', 'p2p_handshake', 'p2p_delta_stream',
                      'p2p_inbound']
  executable('fuzz_' + p2p_target,
    'fuzz_' + p2p_target + '.cpp',
    include_directories: include_directories('../../include'),
    link_with: yams_daemon_lib,
    dependencies: fuzz_deps,
    cpp_args: fuzzer_args,
    link_args: fuzzer_args,
    build_by_default: true,
  )
endforeach

# Topology snapshot decoder (untrusted persisted payloads).
fuzz_topology_codec = executable('fuzz_topology_codec',
  'fuzz_topology_codec.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps + [dependency('yams_topology')],
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

# Daemon download job IPC fuzzer
fuzz_download_jobs = executable('fuzz_download_jobs',
  'fuzz_download_jobs.cpp',
  include_directories: include_directories('../../include'),
  link_with: yams_daemon_lib,
  dependencies: fuzz_deps,
  cpp_args: fuzzer_args,
  link_args: fuzzer_args,
  build_by_default: true,
)

if yams_fuzz_engine == 'afl'
  message('Fuzzing harnesses configured with AFL++ libFuzzer mode')
  message('ASAN enabled via AFL_USE_ASAN=1 environment variable')
else
  message('Fuzzing harnesses configured with libFuzzer + ASan/UBSan')
endif
