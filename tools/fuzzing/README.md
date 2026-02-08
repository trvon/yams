# YAMS Fuzzing Infrastructure

AFL++ fuzzing harnesses for CLI/daemon IPC protocol testing per issue #8.

## Targets

### Protocol Layer
- `fuzz_ipc_protocol` - MessageFramer, CRC32 validation, frame header parsing
- `fuzz_add_document` - AddDocumentRequest/UpdateDocumentRequest deserialization
- `fuzz_ipc_roundtrip` - CLI<->daemon framing + payload decode/encode roundtrip

### Daemon Services & IPC Bus
- `fuzz_proto_serializer` - Protobuf encoding/decoding of IPC message payloads
- `fuzz_request_handler` - Request processing, message framing, and handler configuration
- `fuzz_streaming_processor` - Chunked streaming response handling and processor delegation

Fuzzers run in `aflplusplus/aflplusplus` Docker container for reproducibility.

## Usage

Build fuzzers:
```bash
./tools/fuzzing/fuzz.sh build
```

Run fuzzer:
```bash
./tools/fuzzing/fuzz.sh fuzz ipc_protocol
./tools/fuzzing/fuzz.sh fuzz add_document
./tools/fuzzing/fuzz.sh fuzz ipc_roundtrip
./tools/fuzzing/fuzz.sh fuzz proto_serializer
./tools/fuzzing/fuzz.sh fuzz request_handler
./tools/fuzzing/fuzz.sh fuzz streaming_processor
```

Monitor:
```bash
./tools/fuzzing/fuzz.sh exec afl-whatsup /fuzz/findings/ipc_protocol
./tools/fuzzing/fuzz.sh exec afl-whatsup /fuzz/findings/proto_serializer
```

Reproduce crash:
```bash
./tools/fuzzing/fuzz.sh exec \
  /src/build/fuzzing/tools/fuzzing/fuzz_ipc_protocol /fuzz/findings/ipc_protocol/crashes/id:000000*
./tools/fuzzing/fuzz.sh exec \
  /src/build/fuzzing/tools/fuzzing/fuzz_proto_serializer /fuzz/findings/proto_serializer/crashes/id:000000*
```

## Architecture

### Directory Structure

```
tools/fuzzing/
├── README.md                       # This file
├── fuzz.sh                         # Docker wrapper script
├── meson.build                    # Build definitions
├── fuzz_ipc_protocol.cpp          # IPC protocol fuzzer harness
├── fuzz_add_document.cpp          # Document fuzzer harness
├── fuzz_proto_serializer.cpp      # Protobuf serializer fuzzer
├── fuzz_request_handler.cpp       # Request handler fuzzer
├── fuzz_streaming_processor.cpp   # Streaming processor fuzzer
└── generate_corpus.sh             # Seed corpus generator

data/fuzz/
├── corpus/                        # Input seeds (read-only for fuzzer)
│   ├── ipc_protocol/             # IPC protocol seeds
│   ├── ipc_roundtrip/             # CLI/daemon roundtrip seeds
│   ├── add_document/             # Document request seeds
│   ├── proto_serializer/         # Protobuf message seeds
│   ├── request_handler/          # Request handler seeds
│   └── streaming_processor/      # Streaming processor seeds
└── findings/                      # AFL++ output (crashes, hangs, queue)
    ├── ipc_protocol/
    ├── ipc_roundtrip/
    ├── add_document/
    ├── proto_serializer/
    ├── request_handler/
    └── streaming_processor/
```

### Instrumentation Flow

```
1. Conan Profile       → conan/profiles/aflplusplus-docker
                         Sets: AFL_USE_ASAN=1, AFL_HARDEN=1
                         Compiler: afl-clang-fast++

2. Setup Script        → ./setup.sh Fuzzing
                         Build type: Debug (for symbols)
                         Enables: -Dbuild-fuzzers=true

3. Meson Build         → tools/fuzzing/meson.build
                         Flags: -fsanitize=fuzzer (AFL++ libFuzzer mode)
                         Links: Static daemon IPC libraries

4. Fuzzer Binary       → build/fuzzing/tools/fuzzing/fuzz_*
                         Entry: LLVMFuzzerTestOneInput()
                         ASAN: Memory safety checks
                         AFL++: Coverage-guided mutation
```

## Configuration

Conan profile (`conan/profiles/aflplusplus-docker`):
- `AFL_USE_ASAN=1` - Address sanitizer
- `AFL_LLVM_INSTRUMENT=AFL` - AFL instrumentation mode
- Compiler: `afl-clang-fast++`
- libcxx: `libstdc++11`

Compiler flags:
- `-fsanitize=fuzzer` (compile and link)
- Enables AFL++ libFuzzer compatibility mode with automatic persistent mode

## Advanced

Parallel fuzzing (main + secondary workers):
```bash
# Terminal 1
./tools/fuzzing/fuzz.sh fuzz ipc_protocol

# Terminal 2-N
docker run -ti --rm -v "$(pwd)/data/fuzz:/fuzz" yams-fuzz \
  afl-fuzz -i /fuzz/corpus/ipc_protocol -o /fuzz/findings/ipc_protocol \
  -S worker1 -m none /src/build/fuzzing/tools/fuzzing/fuzz_ipc_protocol
```

Corpus minimization:
```bash
./tools/fuzzing/fuzz.sh exec bash -c \
  "afl-cmin -i /fuzz/findings/ipc_protocol/queue -o /fuzz/corpus/ipc_min -m none \
  -- /src/build/fuzzing/tools/fuzzing/fuzz_ipc_protocol"
```

Crash minimization:
```bash
./tools/fuzzing/fuzz.sh exec bash -c \
  "afl-tmin -i /fuzz/findings/ipc_protocol/crashes/id:000000 -o crash_min -m none \
  -- /src/build/fuzzing/tools/fuzzing/fuzz_ipc_protocol"
```

## Seed Corpus

Location: `data/fuzz/corpus/<target>/`

Generate minimal seeds:
```bash
./tools/fuzzing/generate_corpus.sh
```

Add custom seeds from:
- Integration test traffic capture
- Sanitized CLI→daemon messages
- Manual construction

Corpus should cover request types, edge cases, and real-world patterns. Do not include secrets.

## CI Integration

See `.github/workflows/fuzzing.yml`:
- PR smoke tests: 5 min timeout
- Nightly: 1+ hour for deeper coverage

## Crash Triage

Reproduce:
```bash
./tools/fuzzing/fuzz.sh exec \
  /src/build/fuzzing/tools/fuzzing/fuzz_ipc_protocol /fuzz/findings/ipc_protocol/crashes/id:000000*
```

ASAN provides automatic stack traces. After fixing:
1. Rebuild fuzzers
2. Verify crash input no longer crashes
3. Add crash input to corpus for regression prevention

## Troubleshooting

AFL++ compiler not detected: Use `./tools/fuzzing/fuzz.sh build`

No instrumentation: Verify Conan profile `conan/profiles/aflplusplus-docker` sets `afl-clang-fast++`

Out of memory: Increase Docker limit or disable ASAN for throughput fuzzing

Corpus too large: Run `afl-cmin` for minimization

## References

- Issue #8: Crash Fuzzing & Stability Hardening
- AFL++ docs: https://github.com/AFLplusplus/AFLplusplus/tree/stable/docs
- AFL++ container: https://hub.docker.com/r/aflplusplus/aflplusplus
