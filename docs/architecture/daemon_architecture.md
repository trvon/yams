# Architecture: Daemon and Services

This document summarizes the YAMS daemon, its lifecycle, metrics, and how it hosts plugins and services.

## Responsibilities

- Manage long-running services for indexing, search, and plugin execution.
- Provide IPC endpoints for CLI and tools; maintain a robust connection FSM.
- Expose health and metrics for observability.

## Lifecycle and metrics

- The daemon initializes core services then enters a supervised run loop.
- Metrics collection guards against partial initialization and null lifecycles.

## Plugin hosting

- Hosts plugins via ABI and WASM runtimes.
- Enforces a trust list; paths are canonicalized when adding/removing trust.
- Loads plugin manifests and applies a name policy (e.g., from spec metadata).

## Code references

- Lifecycle/metrics: `src/daemon/components/DaemonMetrics.cpp`
- Plugin host and trust list: `src/daemon/resource/plugin_host.cpp`
- ABI loader and manifest handling: `src/daemon/resource/abi_plugin_loader.cpp`

See also
- Research note: Daemon connection FSM (`docs/research/fsm-for-daemon-ipc.md`)
- Spec: Plugin metadata schema (`docs/spec/plugin_metadata.schema.json`)
- Spec: Plugin interfaces (`docs/spec/plugin_spec.md`, `docs/spec/wit/*`)

