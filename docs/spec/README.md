# YAMS Interfaces & Versions

This directory contains interface specifications and schemas for YAMS plugins and public surfaces.

- `wit/` – WIT definitions for WASM components (e.g., `object_storage_v1.wit`, `dr_provider_v1.wit`).
- New: `graph_adapter_v1.wit` defines a read-focused property-graph interface with cursor-based
  iteration and optional import/export and delta-apply.
- `schemas/` – JSON Schemas describing manifests and interface payloads.
- `interface_versions.json` – authoritative version registry for public interfaces:
  - Keys: `plugin_abi`, `object_storage_v1`, `dr_provider_v1`, `model_provider_v1`, etc.
  - New: `graph_adapter_v1`, `graphjson_v1`, `graph_delta_v1`, `provenance_v1`, `card_anchor_v1`.
  - Bump the corresponding version when changing WIT/schemas/headers for that interface.

Policy
- Any change to public surfaces requires either:
  - A version bump in `docs/spec/interface_versions.json` for the affected interfaces, or
  - A CHANGELOG/stability PBI update explicitly acknowledging the change.
- CI enforces this via `scripts/ci/check_yams_semver.py` during build.

Plugin Naming Policy (ABI)
- ABI plugins have a configurable naming policy that controls how the daemon chooses a display
  identity and deduplicates multiple file variants (e.g., `libyams_foo_plugin.*` and
  `yams_foo_plugin.*`) across Linux and macOS:
  - `relaxed` (default):
    - Canonicalize derived names from filenames when a manifest `name` is not present.
    - Prefer non-`lib` prefixes when both variants exist in the same directory.
    - De-duplicate providers by logical name with a path priority of user > system locations.
  - `spec`:
    - Respect the plugin’s manifest `name` verbatim for display and selection.
    - Still de-duplicate file variants that provide the same provider.

- Configuration:
  - Env: `YAMS_PLUGIN_NAME_POLICY=spec|relaxed`
  - Daemon config: `plugin_name_policy: spec|relaxed` (if supported by your deployment)

Security and Trust
- Plugins are only loaded from trusted paths. The canonical trust file is
  `<data_dir>/plugins.trust` (default `~/.local/share/yams/plugins.trust`, one absolute path per line). CLI helpers:
  `yams plugin trust add|remove ...`.
- For development, `YAMS_PLUGIN_TRUST_ALL=1` can be set to trust directories discovered in the
  standard search order; do not use this in production.

Notes
- Keep WIT and schema filenames aligned with the version keys (e.g., `object_storage_v1.wit`, `object_storage_v1.schema.json`) so automated mapping is accurate.
- For new interfaces, add an entry to `interface_versions.json` when introducing the first version.
 - GraphAdapter v1 provides read-first APIs. Write operations are intentionally scoped to import and
   delta-apply to keep implementations simple and deterministic.
