# Mobile Bindings Overview

_Status: ready for SDK consumers_

## Goals
- Provide a minimal C ABI that exposes search and grep capabilities without leaking internal C++ types.
- Enable embedders to build thin Swift/Kotlin wrappers on top of a shared library (`libyams_mobile`).
- Maintain binary compatibility across minor releases via versioned structs and feature flags.

## Target Platforms
- iOS 15+/macOS 13+ (Swift Package Manager integration)
- Android API 26+ (AAR with JNI bindings)
- Additional consumers via C-compatible dynamic libraries (e.g., React Native, Flutter)

## Architectural Shape
1. **Core ABI Layer** — versioned header `include/yams/api/mobile_bindings.h` compiled into `libyams_mobile`.
2. **Platform Glue** — Swift/Kotlin wrappers (future milestone) translate native types and may layer async semantics over the blocking C entrypoints.
3. **Service Facade** — long-lived `yams_mobile_context_t` mapping to `AppContext` with configurable storage paths, caches, and worker executors.
4. **Lifecycle Hooks** — initialization, shutdown, and JSON-based diagnostics helpers.

```
[Swift/Combine]      [Kotlin/Coroutines]
        \                 /
         \ yams_mobile C ABI /
          \             /
          Core services (grep/search + fixture-driven corpora)
```

## Data Model Principles
- All structs are POD with explicit sizes; optional fields are represented via nullable pointers or sentinel flags.
- Strings use UTF-8 `char*` with caller-managed allocation; response containers return opaque handles with dispose functions.
- Errors propagate through `yams_mobile_status` enums plus optional extended payloads.

## Threading & Concurrency
- Embedders may call APIs from any thread; the library owns a background `boost::asio::thread_pool` per context.
- The current C ABI exposes synchronous entrypoints only (`yams_mobile_*_execute`). Async helpers will ship with platform wrappers.
- Swift/Kotlin adapters should wrap the blocking calls using dispatch queues/coroutines to surface async behaviour.

## Feature Skeleton
| Area                  | Available entrypoints                          | Notes |
|-----------------------|------------------------------------------------|-------|
| Context lifecycle     | `yams_mobile_context_create`, `yams_mobile_context_destroy` | Config accepts working/cache dirs and a `telemetry_sink` string (`console`, `stderr`, `noop`, or `file:/path`). |
| Grep/Search           | `yams_mobile_grep_execute`, `yams_mobile_search_execute`, destroy helpers, `yams_mobile_{grep,search}_result_stats_json` | Blocking calls returning opaque handles plus JSON stats (latency, retry counters). |
| Document ingest       | `yams_mobile_store_document`, `yams_mobile_remove_document` | Update/list flows are deferred; wrappers should enforce doc hygiene. |
| Metadata inspection   | `yams_mobile_get_metadata`, `yams_mobile_metadata_result_json` | JSON payload mirrors DocumentService metadata map. |
| Vector status         | `yams_mobile_get_vector_status`, `yams_mobile_vector_status_result_json` | Returns document counts/storage stats; warmup/model control APIs remain TODO. |

## Compatibility Strategy
- ABI version encoded in `yams_mobile_version_info` (major/minor/patch) and mirrored in
  the header macros `YAMS_MOBILE_API_VERSION_{MAJOR,MINOR,PATCH}`.
- Every request struct now embeds a `struct_size`/`version` header so older clients continue to
  function when fields are appended. Callers should initialize structs via the
  `yams_mobile_context_config_default()` / `yams_mobile_request_header_default()` helpers before
  setting additional fields.
- Breaking changes must bump `YAMS_MOBILE_API_VERSION_MAJOR` and ship a parallel symbol surface
  (`yams_mobile_v2_*`) while preserving the previous version for at least one minor release.
- CI enforces symbol compatibility with `scripts/ci/check_mobile_abi.sh`, comparing the exported
  surface against `public/mobile/abi/yams_mobile_v1.symbols` on every PR.

## Fixtures & Test Corpora
- `tests/common/search_corpus_presets.h` exposes `mobileSearchCorpusSpec()` which captures the
  curated mobile dataset (sync deltas, path sensitivity, semantic warm-up, case toggles). Use this in
  unit/integration tests to align with the demo apps and smoke suites.
- `tests/mobile/mobile_abi_smoke_test.cpp` validates the round-trip ingest/list/search flow against
  the new corpus and guards the struct header defaults.

## Swift Package Demo
- The Swift reference package lives under `examples/mobile/swift/YamsMobileDemo`.
- It provides a `CYamsMobile` system library target (via the generated `yams_mobile.pc`) and a
  minimal executable harness that calls `yams_mobile_get_version()` and dispatches grep/search flows.
- To build locally:
  1. Install YAMS (or build `libyams_mobile`) and expose it via `PKG_CONFIG_PATH`.
  2. Run `swift build` inside the package; the manifest references `CYamsMobile` through pkg-config.
  3. Run `swift test` to execute the smoke test that mirrors the C++ round-trip harness.

## Android Demo
- The Android reference lives under `examples/mobile/android/YamsMobileDemo` with a Gradle library
  module that wraps `libyams_mobile` via JNI.
- `src/main/cpp/yams_mobile_jni.cpp` translates Kotlin calls to the C ABI; instrumentation tests in
  `src/androidTest` ensure `yams_mobile_get_version()` and a basic search flow succeed against the
  mobile corpus fixtures.
- Build steps:
  1. Place the compiled `libyams_mobile.so` for each ABI under `app/src/main/jniLibs/<abi>/`.
  2. Run `./gradlew connectedAndroidTest` to execute the smoke tests on a device/emulator.

## Developer Workflow
1. Initialize the fixtures using `FixtureManager` (see smoke test for a template) or reuse the
   packaged corpora in CI.
2. Link against `yams_mobile` using pkg-config (Swift) or JNI (Android).
3. Run the platform demo harnesses to validate bindings before shipping downstream SDK updates.
