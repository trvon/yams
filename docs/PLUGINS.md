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

SDK
- In‑repo SDK: external/yams-sdk (Python‑first; JSON‑RPC helpers, templates, conformance runner)
- Mirror: https://git.sr.ht/~trvon/yams-sdk
- Templates: external/yams-sdk/python/templates (e.g., external-dr)
- SDK‑driven tests: yams/tests/sdk (exercise handshake/call/conformance without the daemon)

For detailed vtables, error mapping, lifecycle, and security/trust policy, see docs/spec/plugin_spec.md.
