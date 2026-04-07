# Architecture: Plugin and Content Extraction

This overview reflects the current implementation of the plugin stack and content extraction pipeline. Every item below maps directly to production code.

For the user-facing plugin guide (writing and configuring plugins), see [Plugins](../PLUGINS.md).

## Manifests and Capability Negotiation

**Source**: `src/daemon/resource/plugin_loader.cpp`

- `PluginLoader::loadPlugin` opens a shared object, probes exported functions such as `getProviderName`, `createOnnxProvider`, and `createGraphAdapterV1`, and records which interfaces the plugin implements.
- Manifests declared in `docs/spec/plugin_metadata.schema.json` describe versioning and required capabilities; loader logic validates the manifest against this schema before registering providers.
- Graph adapter factories are tracked in an in-process registry via `registerGraphAdapter` / `createGraphAdapter` so knowledge-graph integrations can be selected at runtime.

## Trust and Admission

**Source**: `src/daemon/resource/plugin_host.cpp`

- `PluginHost::trust`/`untrust` maintain a canonicalized allow-list persisted alongside the daemon data directory.
- `PluginHost::enumerateTrusted` filters discovered libraries against that list, which `ServiceManager::autoloadPluginsNow` uses to decide which plugins can be materialized.

## Native ABI Loader

**Source**: `src/daemon/resource/abi_plugin_loader.cpp`

- Resolves ABI entry points, maps them to C++ adapter classes, and converts plugin callbacks into daemon-facing interfaces (storage, model providers, etc.).
- Handles symbol version mismatches and throws structured errors that bubble back to the caller.

## Content Extraction Pipeline

Content extraction is driven by plugins implementing the `content_extractor_v1` interface. The daemon coordinates extraction during ingestion:

1. **Plugin discovery**: The daemon scans for extractors via `PluginLoader` at startup, matching file types to capable plugins.
2. **Extraction dispatch**: During `add`/ingest, the `PostIngestQueue` dispatches documents to registered extractors based on MIME type (e.g., PDF extractor, tree-sitter symbol extractor).
3. **Symbol extraction**: The tree-sitter plugin (`plugin-symbols`) parses source files and emits function/class/import definitions into the knowledge graph, enabling symbol-aware search.
4. **PDF extraction**: The PDF extractor plugin uses qpdf/poppler to extract text content, which is then indexed for full-text and vector search.

## Plugin Interfaces

| Interface | Purpose | Entry points |
|-----------|---------|-------------|
| Model provider | Embedding generation (ONNX) | `createOnnxProvider`, `getProviderName` |
| Graph adapter | Knowledge graph backend | `createGraphAdapterV1` |
| Content extractor | Text/symbol extraction | `content_extractor_v1` ABI |
| Search provider | Plugin-contributed search results | `search_provider_v1` ABI |
| Storage plugin | Object storage backends (S3/R2) | See [Storage Plugin v1](../api/storage_plugin_v1.md) |
