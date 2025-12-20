# Embeddings & FTS5: Auto Mode and Repair

## Overview
- YAMS can automatically generate embeddings and index document content on add.
- Embeddings are stored in the vector database; text content is indexed in SQLite FTS5.
- Both paths are best-effort and non-blocking to keep ingestion fast.

## Auto Embeddings on Add

Toggle via config in `~/.config/yams/config.toml`:

```toml
[embeddings]
auto_on_add = true           # Queue background embedding on add
preferred_model = "all-MiniLM-L6-v2"  # Model name
embedding_dim = 384          # Must match model output dimensions
```

**Behavior:**
- `yams add` and daemon directory indexing call embeddings asynchronously when enabled.
- Extraction + FTS5 indexing still run immediately during add (for supported formats).

## FTS5 Indexing
- When available in the SQLite build, YAMS indexes extracted text into `documents_fts`.
- DocumentService indexes FTS5 during add; directory add uses the same storage path.
- Searching uses hybrid keyword (FTS5) + semantic (vector) ranking when configured.

## Repair and Rebuild

CLI supports targeted repair flows:

```bash
# Generate missing embeddings for stored documents
yams repair --embeddings

# Rebuild FTS5 entries (delete/insert) using robust extraction
yams repair --fts5

# Build/repair knowledge graph from tags/metadata
yams doctor repair --graph

# Run all repair operations
yams repair --all
```

The daemon RepairCoordinator also performs best-effort FTS5 reindex for documents it fixes embeddings for.

## Model Management

```bash
# List available models
yams model list

# Download a model
yams model download all-MiniLM-L6-v2

# Check ONNX runtime and plugin status
yams model check

# Set preferred model in config
yams config set embeddings.preferred_model all-MiniLM-L6-v2
```

**Model resolution order:**
1. `embeddings.model_path` in config
2. `~/.yams/models/<name>/model.onnx`
3. `models/` in current directory
4. `/usr/local/share/yams/models`

## Embedding Dimension Source of Truth

**Single key:** Set `embeddings.embedding_dim` in `~/.config/yams/config.toml`.

**Runtime precedence:** config > env (`YAMS_EMBED_DIM`) > generator > heuristic.

The daemon derives vector DB schema and in-memory index dimensions from this single value to prevent drift.

## Notes
- Embedding and FTS5 operations degrade gracefully: failures are logged and skipped.
- Batch sizes and retries are conservative to avoid blocking foreground operations.

## Troubleshooting

**Plugin not loaded:**
```bash
yams plugin list                              # Check loaded plugins
yams plugin trust add ~/.local/lib/yams/plugins  # Trust plugin directory
yams doctor plugin onnx                       # Diagnose ONNX plugin
```

**Dimension mismatch:**
```bash
yams doctor                                   # Shows vector DB dim vs model target
yams doctor --recreate-vectors --dim 384      # Recreate with correct dimension
```

**Missing embeddings:**
```bash
yams repair --embeddings                      # Generate missing embeddings
yams stats -v                                 # Check embedding coverage
```
