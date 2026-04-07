# Mobile Bindings Overview

_Status: in progress; corpus bindings are the primary scope_

## Goals
- Provide a minimal C ABI for corpus access without leaking internal C++ types.
- Enable embedders to build thin Swift/Kotlin/Flutter wrappers on top of a shared library (`libyams_mobile`).
- Maintain binary compatibility across minor releases via versioned structs and feature flags.

## Product Boundary

The current intended split is:

- `libyams_mobile` owns corpus access: context lifecycle, ingest, update/delete, list/search/get, metadata,
  and graph retrieval where supported.
- The host app owns the LLM runtime and chat UX: model download/load, prompt assembly, generation,
  streaming responses, tool orchestration, and conversation presentation.

This means mobile bindings should be evaluated as a corpus SDK, not as an inference SDK.

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
          Core corpus services (ingest/search/get + fixture-driven corpora)
```

## Data Model Principles
- All structs are POD with explicit sizes; optional fields are represented via nullable pointers or sentinel flags.
- Strings use UTF-8 `char*` with caller-managed allocation; response containers return opaque handles with dispose functions.
- Errors propagate through `yams_mobile_status` enums plus optional extended payloads.

## Threading & Concurrency
- Embedders may call APIs from any thread; the library owns a background `boost::asio::thread_pool` per context.
- The current C ABI exposes synchronous entrypoints only (`yams_mobile_*_execute`). Async helpers will ship with platform wrappers.
- Swift/Kotlin adapters should wrap the blocking calls using dispatch queues/coroutines to surface async behaviour.

## Responsibility Split

### In Scope For Bindings

- Open and manage a local YAMS corpus context.
- Store and update documents, tags, and metadata.
- Retrieve context for prompt assembly through list/search/get APIs.
- Expose graph and metadata inspection where the backend supports it.
- Return stable JSON payloads suitable for thin platform wrappers.

### Out Of Scope For Bindings

- Hosting Gemma or any other local model.
- Chat session state, streaming token output, or response rendering.
- Model lifecycle UX such as download progress, preload controls, and provider settings.
- App-side ranking, prompt composition, or summarization policy beyond raw corpus retrieval.

## Feature Skeleton
| Area                  | Available entrypoints                          | Notes |
|-----------------------|------------------------------------------------|-------|
| Context lifecycle     | `yams_mobile_context_create`, `yams_mobile_context_destroy` | Config accepts working/cache dirs and a `telemetry_sink` string (`console`, `stderr`, `noop`, or `file:/path`). |
| Grep/Search           | `yams_mobile_grep_execute`, `yams_mobile_search_execute`, destroy helpers, `yams_mobile_{grep,search}_result_stats_json` | Blocking calls returning opaque handles plus JSON stats (latency, retry counters). |
| Document lifecycle    | `yams_mobile_store_document`, `yams_mobile_download`, `yams_mobile_update_document`, `yams_mobile_delete_by_name`, `yams_mobile_remove_document` | CRUD exists; delete APIs now align on the common status/payload contract across embedded and daemon mode. Naming cleanup is still in progress. |
| Document retrieval    | `yams_mobile_list_documents`, `yams_mobile_get_document` | Core surface for app-side prompt assembly and memory recall; `list_documents` and `get_document` now return stable JSON payloads across embedded and daemon mode. |
| Metadata inspection   | `yams_mobile_get_metadata`, `yams_mobile_metadata_result_json` | JSON payload mirrors DocumentService metadata map. |
| Graph retrieval       | `yams_mobile_graph_query`, `yams_mobile_graph_query_result_json` | Supported in both daemon and embedded mode for corpus-backed documents; follow-up parity review is still needed for edge cases and cleanup behavior. |
| Corpus status         | `yams_mobile_get_vector_status`, `yams_mobile_vector_status_result_json` | JSON now reports corpus/index readiness, embedding coverage, and corpus feature signals. The request-side `warmup` flag is deprecated and ignored for ABI compatibility. |

## Current Gaps

- Embedded and daemon backends do not yet expose identical behavior for all corpus APIs.
- `get_document.include_content` parity is fixed and covered by daemon/mobile tests, but other corpus APIs still need backend-by-backend review.
- `list_documents` now returns a stable top-level/document JSON shape in both embedded and daemon mode for the common non-`paths_only` corpus flow.
- `search_execute` now returns a stable top-level/result JSON shape in both embedded and daemon mode, while preserving the effective search type after runtime fallback.
- `delete_by_name` now returns the same empty-result payload across backends when nothing matches, while `remove_document` preserves `NOT_FOUND` semantics for strict hash deletes.
- `update_document` now returns a stable JSON shape in both embedded and daemon mode, including
  real `updatesApplied`, `tagsAdded`, and `tagsRemoved` counters from the daemon path.
- Embedded graph query now works for corpus-backed mobile stores, but backend parity still needs review for edge cases and deletion/cleanup behavior.
- `yams_mobile_get_vector_status` still carries legacy naming and a deprecated `warmup` request field, but the payload is now corpus-shaped and `warmup` is explicitly ignored.
- The documentation previously implied demo apps and ABI automation that are not present in this checkout.

## Compatibility Strategy
- ABI version encoded in `yams_mobile_version_info` (major/minor/patch) and mirrored in
  the header macros `YAMS_MOBILE_API_VERSION_{MAJOR,MINOR,PATCH}`.
- Every request struct now embeds a `struct_size`/`version` header so older clients continue to
  function when fields are appended. Callers should initialize structs via the
  `yams_mobile_context_config_default()` / `yams_mobile_request_header_default()` helpers before
  setting additional fields.
- Breaking changes must bump `YAMS_MOBILE_API_VERSION_MAJOR` and ship a parallel symbol surface
  (`yams_mobile_v2_*`) while preserving the previous version for at least one minor release.
- The current source of truth for compatibility is the versioned C header plus mobile ABI smoke
  coverage. CI now verifies that the built shared library exports every `YAMS_MOBILE_API` symbol
  declared in the versioned header.

## Build & Distribution

- CI packaging and ABI verification live in `mobile-builds.md` (in the [yams-app](https://github.com/trvon/yams-app) repository).
- The current CI artifact is a host-native shared library bundle for validation and downstream
  integration work, not yet a shipped `.xcframework` or `.aar`.
- The intended layering is:
  - this repo publishes the native corpus library plus headers
  - platform wrappers package that library for iOS/Android
  - the Flutter app/plugin consumes those packaged native artifacts

## Fixtures & Test Corpora
- `tests/common/search_corpus_presets.h` exposes `mobileSearchCorpusSpec()` which captures the
  curated mobile dataset (sync deltas, path sensitivity, semantic warm-up, case toggles). Use this in
  unit/integration tests to align with the demo apps and smoke suites.
- `tests/mobile/mobile_abi_smoke_test.cpp` now runs on Catch2, validates the round-trip
  ingest/list/search flow against the new corpus, covers daemon `get_document.include_content`
  parity, verifies embedded graph query on a corpus-backed store, checks the corpus-status
  payload in both embedded and daemon modes, and verifies stable `search_execute`,
  `list_documents`, `delete_by_name`, and `update_document` payload shapes across embedded and
  daemon backends, plus `remove_document` status parity for missing hashes. `update_document`
  coverage now also asserts real tag counter parity instead of shape-only presence checks. The
  current suite also retries the first daemon store call in smoke coverage to absorb
  daemon-startup transport flakiness seen only in full-suite execution.

## Platform Wrappers

Reference Swift/Android/Flutter wrappers are planned, but they are not yet checked into this repo.
Until those wrappers land, `libyams_mobile` should be treated as the stable integration seam and the
wrapper layer should stay intentionally thin.

## Developer Workflow
1. Initialize the fixtures using `FixtureManager` (see smoke test for a template) or reuse the
   packaged corpora in CI.
2. Link against `yams_mobile` from the host platform wrapper.
3. Validate corpus flows first: store, list, search, get, update, delete, metadata, and graph.
4. Keep app-owned LLM concerns outside the bindings layer.
