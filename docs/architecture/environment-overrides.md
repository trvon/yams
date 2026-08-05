# Environment override policy

Production code reads process environment values through `yams::config`:

- `getenv_optional()` / `getenv_nonempty()` copy string values under the shared environment lock.
- `read_env_bool()`, `read_env_int()`, `read_env_size()`, `read_env_u32()`, `read_env_float()`, `read_env_double()`, and `read_env_milliseconds()` provide strict typed parsing.
- `set_environment()` serializes compatibility writes and unsets through the same boundary.
- `set_environment_owned()` and `restore_environment_if_owned()` provide generation-checked
  compatibility leases. Restoration fails closed when any newer boundary writer has touched the
  key, including same-value ABA writes.

Boolean overrides accept `1/0`, `true/false`, `yes/no`, and `on/off`, case-insensitively. Unset or empty values preserve the typed default. Unknown values emit a bounded warning and also preserve the typed default; a typo must not silently enable a feature.

## Vector compatibility aliases

`resolve_vector_environment()` is the single compatibility boundary for:

- `YAMS_DISABLE_VECTORS`
- `YAMS_DISABLE_VECTOR`
- `YAMS_DISABLE_VECTOR_DB`

A valid true value from any alias disables vectors for initialization, search, repair, and health probes. Valid false values do not override an already-disabled typed default. If true and false aliases conflict, disable wins conservatively and the policy reports a diagnostic. Invalid values are ignored with a warning.

Typed configuration remains the product authority. Compatibility environment overlays are retained for existing deployments and isolated test fixtures; new tuning surfaces belong in typed configuration rather than new `YAMS_*` variables.

## Environment categories and retention decisions

`tests/scripts/check_configuration_authority.py` inventories tracked `include/`, `src/`,
`plugins/`, and `tools/` C/C++ sources by extracting environment reads and writes, then sorting by
key, access kind, file, and line. New keys and readers require explicit review through its tracked
allowlists.

| Category | Decision |
| --- | --- |
| Deployment identity and paths | Retain compatibility aliases only at the runtime-path boundary. In-process daemon alias writes use generation-checked leases and cannot overwrite a newer owner during teardown. |
| Credentials and platform/provider selection | Retain as deployment inputs; never copy values into status or evidence. |
| Diagnostics and tracing | Retain explicit opt-in environment controls. They are not product tuning. |
| Compatibility and test overlays | Retain narrowly, parse through `yams::config`, snapshot at lifecycle construction or first production use, and report their source where they affect effective policy. |
| Product tuning | TOML/typed configuration is authoritative. Existing `YAMS_*` names are deprecated compatibility inputs; no new product environment names may be added. |

Named decisions from the tuning migration:

- `[search].automatic_rebuilds` is the single product authority for automatic search rebuilds.
  `YAMS_DISABLE_SEARCH_REBUILDS` remains a deprecated compatibility input only when the typed key
  is absent. The effective value and source are present in daemon status.
- Search ranking/topology compatibility inputs are copied into immutable builder/manager snapshots;
  long-lived search components do not re-read ambient process state.
- TuneAdvisor environment reads are routed through the shared boundary. Production resolves each
  legacy input once; explicit TOML setters outrank the compatibility snapshot. Test builds retain
  fresh reads so isolated compatibility tests can characterize aliases without cross-process setup.
- New typed product sections are `[tuning.ipc]` (`timeout_ms`,
  `stream_chunk_timeout_ms`), `[tuning.resource]` (`enabled`, `admission_control`, memory thresholds,
  budgets, and hysteresis), and `[tuning.post_ingest]` (stage concurrency, batch, RPC queue, and RPC
  drain controls). Their effective values and per-key source are exposed as `runtime_tuning` in
  detailed/JSON status.
- CLI search header/body timeouts are passed in `ClientConfig`; the dead process-wide
  `YAMS_HEADER_TIMEOUT` / `YAMS_BODY_TIMEOUT` writes were removed.
- Daemon compatibility aliases (`YAMS_IN_DAEMON`, data/config aliases, and socket path) remain for
  consumers that cannot yet accept explicit injection. Their leases use a monotonic boundary
  generation, so teardown restores only keys the daemon still owns.
