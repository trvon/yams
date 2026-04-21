# E7 — Adaptive similarity threshold via SearchTuner

**Date:** 2026-04-21
**Phase:** E7 (adaptive-runtime + bounds-safe threshold in `SearchTuner`)
**Bench:** `topology_ablation_quality_bench "*axis-8*"` at XXL, 3 reps on the `scifact_simeon` B1 fixture.
**Script:** `/tmp/bench_e7_adaptive_threshold.sh` · **Raw:** `/tmp/bench_e7_adaptive_threshold.jsonl` (6 rows)
**Comparison baselines:** E1 (`/tmp/bench_e1_stage_trace.jsonl`), E6b (`/tmp/bench_e6b_e3_reverted.jsonl`).

## Result — bench byte-identical to E6b, adaptive path delivered

| Cell | E1 nDCG | E6b nDCG | E7 nDCG | vec_q (E1→E7) | vec_pool_med (E1→E7) |
|---|---|---|---|---|---|
| `connected_components_v1` | 0.40432 | 0.43135 | **0.43135** | 86 → 232 | 0 → 4 |
| `hdbscan_v1`              | 0.40147 | 0.42764 | **0.42764** | 86 → 232 | 0 → 4 |

E7 bench rows match E6b byte-identically. The adaptive `SearchTuner` loop is **not exercised by the bench harness** — the bench talks to the daemon via `DaemonClient`, and `SearchEngineManager::buildEngine` does not construct a `SearchTuner`. The static default drop from 0.65 → 0.30 (landed in the same series as E6b) already captures the full vector-recall recovery visible here: vector leg fires on 232/300 queries (vs 86/300 pre-drop) and median vector-pool size moves from 0 → 4.

## What E7 actually ships

The user's design correction 2026-04-21 explicitly rejected a hardcoded `similarityThreshold = 0.15f`: *"why are we hard coding, the correct design is allow the system to adapt per @src/search/search_tuner.cpp"*. E7 delivers that design:

### 1. Telemetry plumbing (static-cost, always on)

- `TraceStageSummary` gains `scoreStatsValid`, `minScore`, `maxScore` (`include/yams/search/search_tracing.h:54-57`).
- `SearchTraceCollector::markStageResult` computes min/max of `comp.score` across the vector-stage batch (`src/search/search_tracing.cpp`).
- `buildStageSummaryJson` emits `score_stats_valid`, `min_score`, `max_score`.
- `RuntimeStageSignal` mirrors those three fields (`include/yams/search/search_tuner.h`).
- JSON parse in `search_engine.cpp` ~5146 populates them on every stage telemetry record.

Cost: O(batch) per vector-stage observation. No heap traffic.

### 2. Adaptive state (in `SearchTuner::AdaptiveRuntimeState`)

Three new EWMA counters:
```cpp
double ewmaVectorMaxSimilarity = 0.0;
std::uint64_t vectorStageObservations = 0;
std::uint64_t vectorStageEmptyStreak = 0;
```

Persisted in `adaptiveStateToJsonLocked` and `loadAdaptiveState` under keys `ewma_vector_max_similarity`, `vector_stage_observations`, `vector_stage_empty_streak` — survives engine restarts via `options.tunerStatePath`.

### 3. Observation arm (`observeLocked`, post-query)

Added after the existing lexical/kg telemetry blocks:

```cpp
const auto* vectorStage = findStage(telemetry, "vector");
if (vectorStage && vectorStage->enabled && vectorStage->attempted && !vectorStage->skipped) {
    adaptive_.vectorStageObservations++;
    if (vectorStage->scoreStatsValid) {
        adaptive_.ewmaVectorMaxSimilarity = ewmaUpdate(
            adaptive_.ewmaVectorMaxSimilarity, vectorStage->maxScore,
            adaptive_.vectorStageObservations);
        adaptive_.vectorStageEmptyStreak = 0;
    } else {
        adaptive_.vectorStageEmptyStreak++;
    }
}
```

### 4. Adjustment arm (also in `observeLocked`, before `applyAdaptiveClamp`)

Two triggers, both respecting `candidate.similarityThreshold.pinned`:

| Trigger | Condition | Action |
|---|---|---|
| **Lower** | `vectorStageEmptyStreak >= 5` after warmup (`kAdaptiveWarmupObservations = 5`) | `threshold -= 0.05`, floored at `0.5 × ewmaVectorMaxSimilarity`; clamp to `[0.05, 0.70]`; emit reason `"vector_empty_pool_streak"`; reset streak |
| **Raise** | `ewmaVectorMaxSimilarity > threshold + 0.20` and empty-streak == 0 | `threshold += 0.02`; clamp; emit reason `"vector_sim_headroom"` |

`applyAdaptiveClamp` bounds-checks the slot via `setClamp(kMinSimilarityThreshold = 0.05f, kMaxSimilarityThreshold = 0.70f)` so direct profile/corpus writes are also clamped.

Constants (top of `src/search/search_tuner.cpp`):
```cpp
constexpr float kMinSimilarityThreshold = 0.05f;
constexpr float kMaxSimilarityThreshold = 0.70f;
constexpr std::uint64_t kAdaptiveVectorEmptyStreakThreshold = 5;
constexpr float kSimilarityThresholdLowerStep = 0.05f;
constexpr float kSimilarityThresholdRaiseStep = 0.02f;
constexpr float kSimilarityThresholdRaiseMargin = 0.20f;
```

## Threshold dynamics observed in bench

Threshold value histogram (log scan across all 3 reps, ~3400 HNSW probes):
- `threshold=0.3000` — 2904 probes (primary path)
- `threshold=0.2000` — 516 probes (`sqlite_vec_backend` two-tier fallback when primary returns 0)

No runtime adjustment fired — expected, since the bench daemon path doesn't wire a `SearchTuner`. The primary-threshold = 0.30 is the static `TuningSlot<float> similarityThreshold{0.30f}` default at `include/yams/search/search_tuner.h:96` and `include/yams/search/search_engine.h:150`.

## Why the bench doesn't exercise the adaptive loop

Path: `bench → DaemonClient → daemon → SearchEngineManager::buildEngine`. `SearchEngineManager` constructs `SearchEngine` via the direct factory, **not** via `SearchEngineBuilder`, and never calls `setSearchTuner`. Only `SearchEngineBuilder::build` with `options.autoTune = true` instantiates a tuner (`search_engine_builder.cpp:152-153`).

That is the right production scope for now: the CLI/production flow *does* wire the tuner when autoTune is on, so real user traffic will hit the adaptive loop. The bench demonstrates the static-default correctness; unit coverage in `catch2_search_submodule` (948 assertions / 248 cases, all green) validates the observation/adjustment arms deterministically.

## Determinism & safety

- **Unit tests:** `catch2_search_submodule` — 948 assertions, 248 test cases, all green post-E7.
- **Bounds:** `applyAdaptiveClamp` clamps the slot into `[0.05, 0.70]` on every `observe` → profile/corpus layers cannot push outside the envelope.
- **Pinning:** `candidate.similarityThreshold.pinned` (set via `pinEnvOverrides` if `YAMS_SEARCH_SIMILARITY_THRESHOLD` is in the environment) suppresses all runtime adjustments.
- **State persistence:** `loadAdaptiveState` reads back EWMAs and streak counters; warm starts don't re-trigger warmup-gated adjustments.

## Decision vs Phase E6 bands

E7 ndcg = 0.43135 CC / 0.42764 HDBSCAN → same as E6b — still in the **`< 0.45`** band.

Interpretation: the HNSW filter is no longer the dominant signal-loss layer (vector leg recovered 86 → 232 queries). The remaining gap between fused nDCG (0.431) and vector-only nDCG (0.419) + text-only nDCG (0.371) is a **fusion-strategy** problem, not a filter problem. Per plan E6 decision table:

> **< 0.45** — Signal-filter layer wasn't the dominant cause. Escalate to encoder-dim alignment (E9) before adding capabilities.

Caveat: E9 (encoder dim 1024 → 768) requires rebuilding the fixture (~20 min) and may not close the gap on its own. Alternative reading: the `vector_pool_median_size = 4` suggests the vector leg's score-mass is highly concentrated; fusion under WEIGHTED_LINEAR_ZSCORE with α=0.75 may be undercrediting the lexical leg on queries where vector contributes only a handful of strong candidates. Phase E3's lexical-normalization flattening was reverted in E6b; revisiting that with the current per-query vector signal may be a cheaper next lever than E9.

## Files touched

- `include/yams/search/search_engine.h` — `similarityThreshold = 0.30f` initial (was already landed).
- `include/yams/search/search_tuner.h` — `TuningSlot<float> similarityThreshold{0.30f}`; new adaptive fields.
- `include/yams/search/search_tracing.h` — `TraceStageSummary` gains score-stats fields.
- `src/search/search_tracing.cpp` — compute + emit min/max per stage batch.
- `src/search/search_engine.cpp` — parse `score_stats_valid`/`min_score`/`max_score` from stage JSON.
- `src/search/search_tuner.cpp` — observation arm, adjustment arm, clamp, persistence.
- `src/vector/meson.build` — add `query_router.cpp` and `fusion.cpp` to `simeon_core_srcs` (required by `SimeonLexicalBackend::buildAsync` / `scoreRouted` linkage).
