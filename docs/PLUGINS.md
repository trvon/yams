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

Loading custom plugins at daemon start
- Search order (first match wins per provider):
  1) Configured directories (daemon config `plugin_dir` or multiple `plugin_dirs` when supported)
  2) Env override (deprecated): `YAMS_PLUGIN_DIR`
  3) User: `$HOME/.local/lib/yams/plugins`
  4) System: `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`
  5) Install prefix: `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`

- Trust policy (default deny):
  - Trust file: `~/.config/yams/plugins_trust.txt` (one absolute path per line, directories or files)
  - CLI helpers:
    - `yams plugin trust add /path/to/dir-or-file`
    - `yams plugin trust remove /path/to/dir-or-file`
  - Dev override (use with caution): `YAMS_PLUGIN_TRUST_ALL=1` trusts paths found via search order.

- De-duplication and naming (Linux/macOS):
  - The daemon prefers non-`lib` prefixed filenames when both `libyams_foo_plugin.*` and
    `yams_foo_plugin.*` exist in the same directory.
  - Providers are de-duplicated by logical name: only one implementation is adopted by priority
    (user dir > system dir; non-`lib` filename > `lib` filename).

Name policy (display identity and selection)
- The daemon accepts a name policy for ABI plugins that influences how plugin names are presented
  and deduplicated:
  - `Relaxed` (default): derive a canonical name from filename (e.g., strip leading `lib` from
    `libyams_foo_plugin`) when manifest does not specify a name. Deduplicate variants by canonical
    name.
  - `Spec`: respect the plugin’s manifest `name` field verbatim for display and selection.
    De-duplication still merges file variants that implement the same provider.

- Configure the policy:
  - Env: `YAMS_PLUGIN_NAME_POLICY=spec|relaxed`
  - Daemon config: `plugin_name_policy: spec|relaxed` (when available)

Examples
```
# Build or download your plugin to a user dir, trust it, and restart daemon
mkdir -p ~/.local/lib/yams/plugins
cp ./build/plugins/my_plugin/yams_my_plugin.dylib ~/.local/lib/yams/plugins/   # macOS
cp ./build/plugins/my_plugin/libyams_my_plugin.so ~/.local/lib/yams/plugins/    # Linux
yams plugin trust add ~/.local/lib/yams/plugins
yams daemon restart

# Enforce spec naming (use manifest name in listings)
export YAMS_PLUGIN_NAME_POLICY=spec
yams plugins list
```

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

For detailed vtables, error mapping, lifecycle, and security/trust policy, see:
- docs/spec/plugin_spec.md
- spec/plugin_naming_and_loading.md (naming, discovery, trust, dedupe; Linux/macOS)
