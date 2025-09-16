# Architecture: Plugin and Content Extraction

This document describes the plugin system and content extraction architecture, consolidating the trust model, manifest schema, and runtimes.

## Goals

- Safely extend YAMS with storage, model, and extraction capabilities.
- Support both native ABI and sandboxed WASM plugins.
- Provide clear metadata and interface contracts for discovery and loading.

## Manifests and interfaces

- Each plugin provides a manifest describing name, version, capabilities, and interface versions.
- Manifests conform to `docs/spec/plugin_metadata.schema.json`.
- Interfaces are versioned; see `docs/spec/plugin_spec.md` and `docs/spec/wit/*`.

## Runtimes

- ABI loader: loads native libraries, fetches manifest via exported symbol, and enforces name policy from manifest when configured.
- WASM host: loads WIT-defined interfaces and executes in a sandboxed runtime.

## Trust model

- Plugins must be explicitly trusted; paths are stored and compared by canonical path.
- Trust add/remove updates are reflected immediately in the host’s trust list.

## Code references

- Host logic: `src/daemon/resource/plugin_host.cpp`
- ABI loader: `src/daemon/resource/abi_plugin_loader.cpp`
- Manifest schema: `docs/spec/plugin_metadata.schema.json`

See also
- API examples: `docs/api/examples/storage_plugin/*.manifest.json`
- Design: Content Handler Architecture (`docs/design/content-handler-architecture.md`)

