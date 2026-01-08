# YAMS External Plugins (Examples)

This directory contains example scaffolding you can copy into an out‑of‑tree repository (e.g., `git@git.sr.ht:~trvon/yams-plugins`).

- See `docs/guide/external_dr_plugins.md` for usage and load instructions.
- Keep this out of the default build; these are examples only.

## Available Plugin Interfaces

| Interface | Header | Description |
|-----------|--------|-------------|
| `entity_extractor_v2` | `include/yams/plugins/entity_extractor_v2.h` | NL entity extraction (GLiNER, etc.) |
| `symbol_extractor_v1` | `include/yams/plugins/symbol_extractor_v1.h` | Code symbol extraction (tree-sitter) |
| `model_provider_v1` | `include/yams/plugins/model_provider_v1.h` | Embedding model provider (ONNX) |
| `object_storage_v1` | `include/yams/plugins/object_storage_v1.h` | Object storage backend (S3) |

## Tree

```
s3/
  c_abi/
    CMakeLists.txt
    src/s3_stub_plugin.c
    plugin_manifest.json
LICENSE-GPL-3.0.txt   # Placeholder; use full GPLv3 text in your plugin repo
```
