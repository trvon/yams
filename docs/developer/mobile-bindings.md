# Mobile Bindings Overview

_Status: in progress_

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
- ABI version encoded in `yams_mobile_version_info` (major/minor/patch).
- Breaking changes create new symbol set (`yams_mobile_v2_*`) while keeping v1 exported.
- Headers guarded via `YAMS_MOBILE_API_VERSION` macros.

## Next Steps
1. Add vector warmup/model management entrypoints before the public SDK release.
2. Implement Swift Package / Android AAR wrappers that offer async APIs and lifecycle helpers.
3. Document fixture selection and mobile smoke workflow once dedicated corpora land in FixtureManager.
