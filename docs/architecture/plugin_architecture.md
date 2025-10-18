# Architecture: Plugin and Content Extraction

This overview reflects the current implementation of the plugin stack. Every item below maps directly to production code.

## Manifests and Capability Negotiation (`src/daemon/resource/plugin_loader.cpp`)

- `PluginLoader::loadPlugin` opens a shared object, probes exported functions such as `getProviderName`, `createOnnxProvider`, and `createGraphAdapterV1`, and records which interfaces the plugin implements.
- Manifests declared in `docs/spec/plugin_metadata.schema.json` describe versioning and required capabilities; loader logic validates the manifest against this schema before registering providers.
- Graph adapter factories are tracked in an in-process registry via `registerGraphAdapter` / `createGraphAdapter` so knowledge-graph integrations can be selected at runtime.

## Trust and Admission (`src/daemon/resource/plugin_host.cpp`)

- `PluginHost::trust`/`untrust` maintain a canonicalized allow-list persisted alongside the daemon data directory.
- `PluginHost::enumerateTrusted` filters discovered libraries against that list, which `ServiceManager::autoloadPluginsNow` uses to decide which plugins can be materialized.

## Native ABI Loader (`src/daemon/resource/abi_plugin_loader.cpp`)

- Resolves ABI entry points, maps them to C++ adapter classes, and converts plugin callbacks into daemon-facing interfaces (storage, model providers, etc.).
- Handles symbol version mismatches and throws structured errors that bubble back to the caller.
