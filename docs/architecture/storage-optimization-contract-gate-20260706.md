# Storage Optimization Contract Gate — 2026-07-06

## Purpose

Future ingestion/storage hot-path optimizations must name and preserve the Lean storage contract before C++ changes land.

Primary proof surface:

```text
formal/topology/Yams/Topology/Storage.lean
```

Key contract:

```lean
StoreOptimizationContract before after docs
```

## Required invariants

Every storage/content optimization must preserve:

1. **Durable-before-visible**
   - content and manifest durability must happen before metadata publication.
2. **Refs-before-visible**
   - reference-counter commits must happen before metadata publication.
3. **Visibility equivalence**
   - optimized publication exposes the same document hashes as the baseline semantics.
4. **Rollback safety boundary**
   - if a later phase fails, metadata must not expose a hash lacking a manifest or committed refs.
5. **Search boundary preservation**
   - metadata-visible hashes remain the handoff point for keyword/semantic/graph searchability contracts.

## Mandatory implementation checklist

Before landing a storage optimization:

- Identify the C++ phase being optimized, for example:
  - chunk object write
  - manifest write
  - reference-counter commit
  - metadata insert/publication
- State whether the change is:
  - physical encoding only
  - batching/coalescing
  - scheduling/reordering
  - durability or transaction-boundary changing
- If it changes only physical encoding, prove or argue that `contentHash`, manifest contents, committed refs, and metadata visibility are unchanged.
- If it changes batching or ordering, map it to `batchedCommit_contract` or add a narrower Lean theorem first.
- Add focused C++ tests for the observable boundary.
- Run:

```bash
source ~/.zshenv 2>/dev/null || true
meson compile -C build/debug -j4 <focused-target>
meson test -C build/debug <focused-test> --print-errorlogs
cd formal/topology && lake build
```

- Run the ingestion ablation harness and compare against the previous artifact.
- If the change can affect query behavior, run search-impact probes and check the Lean search observation/equivalence contracts.

## Current example

The sub-KiB compression-threshold optimization is a physical encoding change. It preserves the proof boundary because it does not change:

- content hash publication
- manifest durability requirements
- reference-counter commit requirements
- metadata publication ordering
- downstream searchability preconditions

Its focused test verifies that default small objects skip compression while larger objects still use the compression path.
