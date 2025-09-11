# YAMS Interfaces & Versions

This directory contains interface specifications and schemas for YAMS plugins and public surfaces.

- `wit/` – WIT definitions for WASM components (e.g., `object_storage_v1.wit`, `dr_provider_v1.wit`).
- `schemas/` – JSON Schemas describing manifests and interface payloads.
- `interface_versions.json` – authoritative version registry for public interfaces:
  - Keys: `plugin_abi`, `object_storage_v1`, `dr_provider_v1`, `model_provider_v1`, etc.
  - Bump the corresponding version when changing WIT/schemas/headers for that interface.

Policy
- Any change to public surfaces requires either:
  - A version bump in `docs/spec/interface_versions.json` for the affected interfaces, or
  - A CHANGELOG/stability PBI update explicitly acknowledging the change.
- CI enforces this via `scripts/ci/check_yams_semver.py` during build.

Notes
- Keep WIT and schema filenames aligned with the version keys (e.g., `object_storage_v1.wit`, `object_storage_v1.schema.json`) so automated mapping is accurate.
- For new interfaces, add an entry to `interface_versions.json` when introducing the first version.
