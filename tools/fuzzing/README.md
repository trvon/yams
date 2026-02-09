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
- `fuzz_query_parser` - User-facing search query parsing + FTS5 translation
- `fuzz_plugin_trust` - Plugin trust list parsing + safe path containment checks

Fuzzers run in `aflplusplus/aflplusplus` Docker container for reproducibility.

## Usage

Build fuzzers:
```bash
./tools/fuzzing/fuzz.sh build
```

Run a fuzzer (interactive / live AFL++ UI):
```bash
./tools/fuzzing/fuzz.sh fuzz ipc_protocol
./tools/fuzzing/fuzz.sh fuzz add_document
./tools/fuzzing/fuzz.sh fuzz ipc_roundtrip
./tools/fuzzing/fuzz.sh fuzz proto_serializer
./tools/fuzzing/fuzz.sh fuzz request_handler
./tools/fuzzing/fuzz.sh fuzz streaming_processor
./tools/fuzzing/fuzz.sh fuzz query_parser
./tools/fuzzing/fuzz.sh fuzz plugin_trust
```

Notes:
- Run each fuzzer in its own terminal tab/pane if you want multiple live dashboards.
- If you copy/paste commands from VS Code, paste the raw command text (not a Markdown link like `[fuzz.sh](...)`).

Monitor:
```bash
./tools/fuzzing/fuzz.sh exec afl-whatsup /fuzz/findings/ipc_protocol
./tools/fuzzing/fuzz.sh exec afl-whatsup /fuzz/findings/proto_serializer
```

Lightweight live stats (host-side):
```bash
watch -n 1 "sed -n '1,80p' data/fuzz/findings/ipc_roundtrip/default/fuzzer_stats"
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
│   └── plugin_trust/             # Plugin trust/path seeds
└── findings/                      # AFL++ output (crashes, hangs, queue)
    ├── ipc_protocol/
    ├── ipc_roundtrip/
    ├── add_document/
    ├── proto_serializer/
    ├── request_handler/
    └── streaming_processor/
    └── plugin_trust/
```

### Instrumentation Flow

```
1. Conan Profile       → conan/profiles/aflplusplus-docker
                         Sets: AFL_USE_ASAN=1
                         Compiler: afl-clang-fast++ (via afl-clang-fast wrappers)

2. Meson Build         → tools/fuzzing/meson.build
                         Flags: -fsanitize=fuzzer (AFL++ libFuzzer-compat mode)
                         Links: yams_daemon_lib (IPC/framing/protocol code)

3. Fuzzer Binary       → build/fuzzing/tools/fuzzing/fuzz_*
                         Entry: LLVMFuzzerTestOneInput()
                         ASAN: Memory safety checks (AFL_USE_ASAN)
                         AFL++: Coverage-guided mutation + queue management
```

## Configuration

Conan profile (`conan/profiles/aflplusplus-docker`):
- `AFL_USE_ASAN=1` - Address sanitizer
- `AFL_LLVM_INSTRUMENT=AFL` - AFL instrumentation mode
- Compiler: `afl-clang-fast++`
- libcxx: `libstdc++11`

Container build flags (`tools/fuzzing/Dockerfile`):
- Sets `CXXFLAGS` to relax a couple of warning-to-error cases seen in dependencies.

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

Generate HTML plots:
```bash
./tools/fuzzing/fuzz.sh exec \
  afl-plot /fuzz/findings/ipc_protocol/default /fuzz/findings/ipc_protocol/plot
```
Then open: `data/fuzz/findings/ipc_protocol/plot/index.html`

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

If the `yams-fuzz` Docker image is already built, `generate_corpus.sh` also runs a small
structured seed generator (`seedgen`) that emits framed protobuf messages for edge-case
`SearchRequest`, `GrepRequest`, and `DeleteRequest` variants (roughly 10–50 inputs).

Note: this script creates directories matching `fuzz.sh fuzz <target>` (e.g. `ipc_protocol`, `ipc_roundtrip`).

Add custom seeds from:
- Integration test traffic capture
- Sanitized CLI→daemon messages
- Manual construction

Corpus should cover request types, edge cases, and real-world patterns. Do not include secrets.

## CI Integration

Not wired up yet in this repo. If you want CI fuzz smoke tests, a common pattern is:
- Build `yams-fuzz` image in CI
- Run each fuzzer for a short, fixed time budget (e.g. 60–300s)
- Upload `data/fuzz/findings/**` as artifacts when crashes are found

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

## LLM-assisted workflow (optional)

LLMs can complement AFL++ by speeding up the human parts (seed quality, triage, and variant hunting):

1) Seed generation (coverage boost)
- Ask the LLM to propose edge-case inputs for a specific request type (e.g. `GrepRequest`, `SearchRequest`).
- Turn those into a small seed set (even 10–50 good seeds often helps AFL explore faster).

2) Crash triage (faster root-cause)
- Provide: minimized crashing input + ASAN stack trace + the immediate code around the top frames.
- Ask for: likely root cause, invariants violated, and a minimal patch + regression test idea.

3) Dedup + prioritize
- Feed multiple ASAN traces; ask the LLM to cluster by top frame / signature and identify the highest-risk classes (OOB write, UAF, etc.).

You can also do this locally with the helper:
```bash
python3 ./tools/fuzzing/asan_cluster.py ./asan_logs/*.txt
```

4) Variant analysis
- Once you fix one bug, ask the LLM to search for similar patterns across the codebase (same API misuse, same unchecked length assumptions, etc.).

Safety note: don’t paste secrets from real documents into prompts; use minimized test inputs and scrubbed logs.

## Troubleshooting

AFL++ compiler not detected: Use `./tools/fuzzing/fuzz.sh build`

No instrumentation: Verify Conan profile `conan/profiles/aflplusplus-docker` sets `afl-clang-fast++`

Out of memory: Increase Docker limit or disable ASAN for throughput fuzzing

Corpus too large: Run `afl-cmin` for minimization

Existing findings directory: `fuzz.sh fuzz <target>` sets `AFL_AUTORESUME=1` so it resumes safely.
To start a fresh run while keeping old results, rename `data/fuzz/findings/<target>/` first.

Directory is in use: if you see ".../default is in use", another afl-fuzz is already running.
By default `fuzz.sh` now runs with a unique `-S` fuzzer id per terminal. To resume the original
instance explicitly, run:
```bash
AFL_FUZZER_ID=default ./tools/fuzzing/fuzz.sh fuzz <target>
```

## References

- Issue #8: Crash Fuzzing & Stability Hardening
- AFL++ docs: https://github.com/AFLplusplus/AFLplusplus/tree/stable/docs
- AFL++ container: https://hub.docker.com/r/aflplusplus/aflplusplus
