# Infer Static Analysis — Work Matrix

**Last updated**: 2026-04-11  
**Tool**: Infer dev build with `OS_ACTIVITY_MODE=disable`  
**Primary report**: `infer-out-full-src-oslogoff-metadata-track-20260412/report.txt`

## Current Snapshot

- Repo-wide source-only run: `471` issues
- Focused daemon-core slice: `0` issues
- Focused daemon verification target: `builddir/tests/catch2_daemon_protocol`

### Repo-wide Category Totals

| Issue Type | Count |
|---|---:|
| `PULSE_UNNECESSARY_COPY_ASSIGNMENT` | 152 |
| `PULSE_UNNECESSARY_COPY_INTERMEDIATE` | 126 |
| `PULSE_UNNECESSARY_COPY` | 108 |
| `DEAD_STORE` | 39 |
| `PULSE_CONST_REFABLE` | 35 |
| `LOCK_CONSISTENCY_VIOLATION` | 15 |
| `PULSE_UNNECESSARY_COPY_OPTIONAL` | 5 |
| `PULSE_UNNECESSARY_COPY_ASSIGNMENT_CONST` | 5 |

## Done

| Track | Scope | Outcome | Verification |
|---|---|---|---|
| Daemon core reliability | `src/daemon/daemon.cpp`, `src/daemon/ipc/request_handler.cpp`, `src/daemon/components/{RepairService,BackgroundTaskManager,DaemonMetrics}.cpp` | Fixed starvation/deadlock paths, shutdown ownership, metrics race, and streaming cancel deadlock | Focused Infer slice clean; `catch2_daemon_protocol` green |
| CLI daemon helper hygiene | `include/yams/cli/daemon_helpers.h` | Removed the top repo-wide Infer findings in daemon helper plumbing; repo-wide total reduced `654 -> 649` | `catch2_cli_submodule` focused cases green; full repo-wide Infer rerun complete |
| Compression concurrency | `src/compression/{compression_monitor,recovery_manager,transaction_manager,error_handler}.cpp` | Fixed config races, stabilized transaction cleanup locking, and removed recovery callback churn; repo-wide total reduced `649 -> 637` | Focused compression Infer clean; `catch2_compression_submodule` green |
| ONNX/model pool concurrency | `plugins/onnx/{onnx_model_pool,onnx_reranker_session,model_provider}.cpp` plus `include/yams/daemon/resource/resource_pool.h` and `plugins/onnx/onnx_gpu_provider.h` | Fixed ONNX session/pool races, promoted pool readiness to atomic, hardened handle dereference sites, and cleared focused ONNX Infer slice; repo-wide total reduced `637 -> 594` | Focused ONNX Infer clean; `catch2_onnx_model_pool` and `catch2_daemon_onnx_provider` green |
| Vector/index concurrency | `src/vector/{sqlite_vec_backend,vector_database,vector_index_manager,model_cache}.cpp` plus adjacent vector hygiene files and targeted vendored HNSW fixes | Fixed manager/backend races, hardened HNSW search/build interactions, and reduced the focused vector slice to vendored-only residuals before cleaning those too; repo-wide total reduced `594 -> 521` | Focused vector Infer clean; `catch2_vector_submodule` and `catch2_tokenizer` green |
| Wider daemon component hygiene | `src/daemon/client/*`, `src/daemon/components/{PostIngestQueue,EmbeddingService,PluginManager,RequestDispatcher,RequestQueue}.cpp` | Cleared the focused daemon hygiene slice and reduced repo-wide total `521 -> 485` with transport/client, queue, plugin, and post-ingest cleanup | Focused daemon hygiene Infer clean; daemon and CLI tests green |
| Storage/reference counter concurrency | `src/storage/reference_counter.cpp` | Serialized transaction mutable state and removed the remaining reference counter race warnings; repo-wide total reduced `485 -> 477` | Focused Infer slice clean; `catch2_storage_submodule` ReferenceCounter tests green |
| Metadata/concurrency cleanup | `src/metadata/metadata_repository.cpp` plus nearby metadata helper headers | Fixed SymSpell teardown/init serialization and cleared the remaining focused metadata repository issues; repo-wide total reduced `477 -> 471` | Focused Infer slice clean; `catch2_metadata_repository` and `catch2_metadata_repository_cache` green |

## Work Matrix

| Priority | Track | Representative Files | Dominant Issue Types | Risk | Recommended Approach | Verification | Status |
|---|---|---|---|---|---|---|---|
| 1 | CLI daemon helper hygiene | `include/yams/cli/daemon_helpers.h`, `include/yams/cli/ui_helpers.hpp` | copy, const-ref, dead store | Low | Batch low-risk signature and move/reference cleanups first | Repo-wide Infer diff; CLI tests/build | In progress |
| 2 | Compression concurrency | `src/compression/compression_monitor.cpp`, `src/compression/recovery_manager.cpp`, `src/compression/transaction_manager.cpp`, `src/compression/error_handler.cpp` | lock consistency, const-ref | High | Audit config/state access patterns before touching copy noise | Focused Infer slice for `src/compression/**`; targeted unit tests | Complete |
| 3 | ONNX/model pool concurrency | `plugins/onnx/onnx_model_pool.cpp`, `plugins/onnx/onnx_reranker_session.cpp`, `plugins/onnx/model_provider.cpp` | lock consistency, null deref | High | Treat as correctness work, not hygiene; trace ownership and session state transitions | Focused Infer slice for `plugins/onnx/**`; ONNX tests | Complete |
| 4 | Vector/index concurrency | `src/vector/sqlite_vec_backend.cpp`, `src/vector/vector_database.cpp`, `src/vector/vector_index_manager.cpp`, `src/vector/model_cache.cpp` | lock consistency, readonly shared ptr, copy | High | Triage races first, then cleanup copies in same files | Focused Infer slice for `src/vector/**`; vector tests | Complete |
| 5 | Wider daemon component hygiene | `src/daemon/client/*`, `src/daemon/components/PostIngestQueue.cpp`, `src/daemon/components/EmbeddingService.cpp`, `src/daemon/components/PluginManager.cpp` | copy, dead store, readonly shared ptr | Medium | Batch by file after correctness-heavy tracks above | Focused Infer slices per subsystem; daemon tests | Complete |
| 6 | Storage/reference counter concurrency | `src/storage/reference_counter.cpp` | lock consistency | High | Audit transaction object lifetime and state mutation ordering | Focused Infer slice for `src/storage/reference_counter.cpp`; storage tests | Complete |
| 7 | Metadata/concurrency cleanup | `src/metadata/metadata_repository.cpp` | lock consistency | Medium | Single-file race audit and narrow synchronization cleanup | Focused Infer slice for metadata repo; metadata tests | Complete |
| 8 | Header/util hygiene | `include/yams/daemon/components/dispatch_utils.hpp`, `include/yams/daemon/resource/plugin_trust.h`, `include/yams/daemon/resource/resource_pool.h` | copy, null deref | Medium | Small surgical fixes; keep behavior unchanged | Repo-wide Infer diff; rebuild | Pending |
| 9 | Third-party vendored triage | `third_party/sqlite-vec-cpp/include/sqlite-vec-cpp/index/hnsw.hpp`, `third_party/sqlite-vec-cpp/include/sqlite-vec-cpp/utils/error.hpp` | lock consistency, use-after-delete | High | Decide: patch locally, upstream, or suppress with justification | Focused Infer slice for third-party paths; benchmark/smoke test | Pending |

## Suggested Execution Order

| Order | Track | Why |
|---|---|---|
| 1 | Header/util hygiene | Low-risk cleanup sweep |
| 2 | Third-party vendored triage | Best handled after first-party code is clean |

## Focused Verification Pattern

For each track:

1. Create a focused Infer slice for the affected files.
2. Fix correctness issues before copy/dead-store hygiene.
3. Run the nearest subsystem tests.
4. Re-run the focused Infer slice.
5. Only then return to the full repo-wide report.

## Notes

- The daemon-core subset is clean and should stay that way. Re-run the focused daemon slice before and after any daemon-adjacent changes.
- The first CLI hygiene pass is complete for `daemon_helpers.h`; the next low-risk header candidates are `include/yams/cli/ui_helpers.hpp`, `include/yams/daemon/components/dispatch_utils.hpp`, and `include/yams/daemon/resource/{plugin_trust.h,resource_pool.h}`.
- The compression track is complete for the focused subsystem slice. Remaining repo-wide compression issues should now be hygiene-only spillover outside the core four files.
- The ONNX/model-pool track is complete for the focused slice. Remaining repo-wide ONNX findings are now folded into general hygiene counts rather than concurrency risk.
- The vector/index track is complete for first-party code. The focused vector slice is now clean after small vendored `sqlite-vec-cpp` fixes; the repo-wide residual `LOCK_CONSISTENCY_VIOLATION` count is now small enough that remaining work is better treated as targeted daemon/storage/metadata cleanup plus later vendor triage.
- The wider daemon hygiene track is complete; the focused daemon client/component slice is clean and the remaining repo-wide daemon work is now mostly outside this batch in header/util cleanup.
- The `reference_counter` track is complete; the remaining repo-wide `LOCK_CONSISTENCY_VIOLATION` set is now small enough to treat `metadata_repository.cpp` as the last major first-party concurrency cleanup before a broad header/util sweep.
- The metadata repository track is complete; the remaining first-party work is now dominated by low-risk header/util cleanup, with only a small residual `LOCK_CONSISTENCY_VIOLATION` tail left for later vendor/edge-case triage.
- The repo-wide report is now dominated by hygiene findings, but the `LOCK_CONSISTENCY_VIOLATION` cluster still deserves subsystem-by-subsystem triage before broad cleanup sweeps.
- Keep using `OS_ACTIVITY_MODE=disable` for Infer on this machine; it avoids the macOS fork-time crash in the Infer child process.
