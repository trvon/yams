# YAMS Plugins: Overview and Spec

This document is an entry point for YAMS plugin development. The authoritative
specification lives at `docs/spec/plugin_spec.md`.

- Spec: docs/spec/plugin_spec.md
- C‑ABI core: include/yams/plugins/abi.h
  - `yams_plugin_init` receives an optional host context to call back into YAMS services.
- Interfaces:
  - Plugin-provided (called by host):
    - Model provider (v1): include/yams/plugins/model_provider_v1.h
    - Object storage (v1): include/yams/plugins/object_storage_v1.h (if present)
  - Host-provided (called by plugin):
    - Host Services (v1): include/yams/plugins/host_services_v1.h
      - Exposes core application services like Document and Search to plugins.

Quick start
- Discover plugins: `yams plugin scan [dir|file]`
- Trust a plugin: `yams plugin trust add /path/to/plugin.so`
- List trusted: `yams plugin trust list`
- Load on daemon start: place plugins in a trusted directory or set `plugin_dir` in config.

Runtime notes
- The daemon prefers a host‑backed `model_provider_v1` plugin for embeddings; otherwise it
  falls back to the built‑in registry or mock/null providers.
- Progress events (ModelLoadEvent) may be emitted during model preload or warmup.

Model Provider (model_provider_v1)
- Interface version: v1.2 (pre‑stable). Header: `include/yams/plugins/model_provider_v1.h`.
- Base API: load/unload, single/batch embeddings.
- New in 1.2 (pre‑stable additions):
  - `get_embedding_dim(self, model_id, out_dim)` — fast dimension lookup.
  - `get_runtime_info_json(self, model_id, out_json)` — JSON with runtime details; free via `free_string`:
    `{ "backend": "onnxruntime|genai", "pipeline": "raw_ort|genai", "model": "all-mpnet-base-v2", "dim": 384, "intra_threads": 8, "inter_threads": 1, "runtime_version": "x.y.z" }`
  - `set_threading(self, model_id, intra_threads, inter_threads)` — optional; may return UNSUPPORTED.

Adoption and telemetry
- Plugins should advertise `{ id: "model_provider_v1", version: 2 }` in their manifest.
- The daemon can probe alt versions for backward compatibility, but v2 (v1.2) is recommended.
- Daemon status surfaces embedding runtime info (backend/model/dim/threads) when available.

SDK
- In‑repo SDK: external/yams-sdk (Python‑first; JSON‑RPC helpers, templates, conformance runner)
- Mirror: https://git.sr.ht/~trvon/yams-sdk
- Templates: external/yams-sdk/python/templates (e.g., external-dr)
- SDK‑driven tests: yams/tests/sdk (exercise handshake/call/conformance without the daemon)

For detailed vtables, error mapping, lifecycle, and security/trust policy, see docs/spec/plugin_spec.md.
