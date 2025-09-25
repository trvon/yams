# Mobile Bindings Overview

_Status: draft_

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
2. **Platform Glue** — Swift/Kotlin wrappers translate native types, manage threading, and expose async APIs.
3. **Service Facade** — long-lived `yams_mobile_context_t` mapping to `AppContext` with configurable storage paths and worker executors.
4. **Lifecycle Hooks** — initialization, warmup, shutdown, diagnostics.

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
- Embedders may call APIs from any thread; library serializes access via internal dispatch contexts.
- Async operations expose both blocking (`*_execute`) and callback-based variants (`*_execute_async`).
- Swift/Kotlin wrappers translate callbacks into platform futures (Combine publishers / Kotlin flows).

## Feature Skeleton
| Area                  | Functions (draft)                              | Notes |
|-----------------------|-----------------------------------------------|-------|
| Context lifecycle     | `yams_mobile_context_create`, `*_destroy`, `*_reload_config` | Takes config struct (paths, cache size, feature toggles). |
| Grep/Search           | `yams_mobile_grep_execute`, `yams_mobile_search_execute`, `*_dispose_result` | Mirrors CLI flags (pattern, literal, word, context windows, filters, hybrid toggles). |
| Document ingest/update| `yams_mobile_store_document`, `yams_mobile_update_document`, `yams_mobile_delete_document` | Wraps DocumentService flows; supports fixture-backed paths. |
| Metadata & fixtures   | `yams_mobile_list_documents`, `yams_mobile_get_metadata`, `yams_mobile_list_fixtures` | Enables mobile clients to inspect corpus state and pull sample data. |
| Vector/embedding      | `yams_mobile_vector_status`, `yams_mobile_warm_embeddings`, `yams_mobile_list_models` | Provides ONNX/model readiness checks and warmup controls. |
| Warmup/telemetry      | `yams_mobile_get_stats`, `yams_mobile_set_logger` | Warmup primes metadata/embeddings; stats include retry counters; optional logging hook supplied by embedder. |

## Compatibility Strategy
- ABI version encoded in `yams_mobile_version_info` (major/minor/patch).
- Breaking changes create new symbol set (`yams_mobile_v2_*`) while keeping v1 exported.
- Headers guarded via `YAMS_MOBILE_API_VERSION` macros.

## Next Steps
1. Finalize header layout (`yams_mobile_context_config`, request/response structs, status codes).
2. Identify deterministic fixture sets for mobile smoke tests (reuse `FixtureManager`).
3. Implement thin Swift demo harness calling grep/search flows.
4. Extend CI to build & run new reliability suite + mobile demos under `unit_reliability_grep_search_startup`.
