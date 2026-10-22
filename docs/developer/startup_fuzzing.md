# Startup contract fuzzing

`catch2_startup_replay` and `fuzz_startup` use the same replay support in
`tools/fuzzing/startup_fuzz_support.h`. The harness drives the real
`ServiceManagerFsm`, the real `DeferralGate` with virtual time, and the production
semantic-score helpers. It never starts a daemon, opens a corpus, loads a model,
changes host pressure, or sleeps.

## Fast regression and seed replay

After configuring the usual Debug test build:

```sh
meson compile -C build/debug -j2 catch2_startup_replay catch2_embedding_service_timing
build/debug/tests/catch2_startup_replay --rng-seed 42
build/debug/tests/catch2_embedding_service_timing '[semantic-startup],[startup-eligibility]'
```

The seeded case tests 1,024 traces of 256 actions and float bit patterns per run.
Failures report the Catch2 root seed, round and action offset. Re-running with
that root seed reproduces the entire deterministic campaign. Named cases cover
pressure deadlines before readiness, optional vector initialization, failure,
shutdown, cancellation and explicit restart/reset. A separate real vector/KG
fixture covers both streamed backfill and corpus-rebuild edge weights.

## Coverage-guided campaign

Use the existing fuzz build configuration. `./setup.sh Fuzzing` sets
`-Dbuild-fuzzers=true -Db_lundef=false` and selects AFL++ or libFuzzer
(`tools/fuzzing/meson.build`). It defaults TSan **off**: the harnesses link
`-fsanitize=fuzzer,address,undefined`, so combining fuzzing with `--tsan` is
rejected rather than producing a binary that cannot start.

A plain Meson configure also works and avoids Conan re-resolution:

```sh
CC=clang CXX=clang++ meson setup build/fuzzing \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --wrap-mode=nofallback --buildtype=debug -Db_lundef=false \
  -Dbuild-fuzzers=true -Dbuild-tests=false -Dbuild-cli=false \
  -Dbuild-plugins=false -Dbuild-benchmarks=false
```

Every project object is then rebuilt with `-fsanitize=fuzzer-no-link` plus
ASan/UBSan, so the first build of this lane is expensive. `meson compile`
accepts either the executable name or its output path; raw `ninja` needs the path:

```sh
meson compile -C build/fuzzing -j4 fuzz_startup        # same as tools/fuzzing/fuzz_startup
python3 tools/fuzzing/startup_seeds.py /tmp/yams-startup-corpus
build/fuzzing/tools/fuzzing/fuzz_startup /tmp/yams-startup-corpus -seed=42 -max_total_time=60
build/fuzzing/tools/fuzzing/fuzz_startup /tmp/yams-startup-corpus/pressure-before-ready
```

Keep failing raw input files as regression corpus artifacts. The low seven bits
of each byte select `StartupAction` modulo its count; the high bit selects the
optional-vector branch for an advance action. Processing is bounded to 512
actions and 32 float-component pairs. A deadline action advances virtual time
past the pressure starvation deadline; elapsed time cannot supply missing
startup readiness. Seed generation refuses to overwrite different existing files.

## Scope

This is a **contract-valid, single-owner state-machine/data fuzzer**, not a full
startup or concurrency proof. `tinyfsm` state is process-global, so do not run
multiple replay instances concurrently in one process. Independent fuzz worker
processes are fine. The Catch2 smoke target also compiles and invokes the actual
coverage-fuzzer entry point without claiming coverage-guided instrumentation.

Known coverage limit: the replay asserts that ineligible work leaves the real
`DeferralGate` untouched, but that ordering lives in the harness. A regression
that moved `ResourceGovernor::admitDeferrable()` ahead of the readiness check
inside `EmbeddingService::channelPoller` would not fail the replay. The
service-level `[startup-eligibility]` test covers the production predicate
(`semanticBackfillEligible`) and its stop/idle behavior, not the governor call
order; covering that needs an injectable admission seam.

Arbitrary late callbacks, real executor interleavings, concurrent FSM instances,
filesystem/model/plugin faults, migrations and cold-start resource KPIs need
additional production seams and separate integration campaigns. Do not infer
live startup speed, sanitizer-engine coverage or end-to-end recovery from a
passing replay test.
