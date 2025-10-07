# Tuning Profiles — Efficient, Balanced, Aggressive

This guide explains the YAMS tuning profiles and how they shape dynamic scaling. Profiles provide a simple, high-level switch that bundles multiple tuning heuristics.

## Profiles

- efficient: prioritizes resource efficiency. Slower pool growth, higher IO connections per thread threshold, fewer post-ingest workers.
- balanced (default): moderate ramp-up and thresholds; good general purpose choice.
- aggressive: prioritizes throughput/latency under load. Faster pool growth, lower IO thresholds, more post-ingest workers.

## What profiles affect

Profiles modulate these behaviors (multipliers shown where applicable):
- IPC worker pool grow/shrink step: efficient ×0.75, balanced ×1.0, aggressive ×2.0
- IO pool "connections per thread" threshold: efficient ÷0.75 (higher threshold), balanced ×1.0, aggressive ÷2.0 (lower threshold)
- Post-ingest worker target: efficient ×0.75, balanced ×1.0, aggressive ×2.0 (before min/max clamps)
- Multiplexed writer budget per request turn: tuned continuously; aggressive profile now keeps ≥1.5× the balanced byte budget floor, while efficient trims sooner to reduce contention
- Backpressure read pause: default 10ms (aggressive profile forces a 5ms pause unless overridden)
- Max connections: derived dynamically; can be overridden via env (see below)

The tuning manager now pushes writer-budget updates straight into the socket server, so active IPC streams pick up profile changes immediately—no restart required. This applies to profile switches and to any live adjustments you make through CLI or env overrides.

## How to set a profile

- Config (persisted):
  `yams config set tuning.profile aggressive`
  `yams daemon restart`

- Environment (ephemeral):
  `export YAMS_TUNING_PROFILE=aggressive`
  `yams daemon restart`

Allowed values: `efficient | balanced | aggressive`

## Related environment variables

- `YAMS_MAX_ACTIVE_CONN`: hard cap on accepted connections. By default, the daemon computes a cap: `max(256, recommendedThreads × ioConnPerThread × 4)`. Set this when running many agents to avoid accept backoff.
- `YAMS_BACKPRESSURE_READ_PAUSE_MS`: default 10. Small sleep applied when the server is backpressured; increasing can smooth heavy load.

## Observability

- `yams status --json`: check `additionalStats.pool_ipc_size`, `post_ingest.threads`, and `activeConnections`.
- Logs: watch for "Accept error (EINVAL)" rebuilds and "Broken pipe" (client closed) under extreme churn. Aggressive profile typically reduces timeouts.

## Tips

- Start with balanced. If many agents push concurrent adds/search, try aggressive.
- For single-box, resource-constrained setups, efficient reduces CPU churn.
- Profiles are safe overlays; fine-grained knobs (e.g., `tuning.pool_scale_step`) still apply when set.
