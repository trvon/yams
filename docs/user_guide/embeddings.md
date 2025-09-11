Embeddings & FTS5: Auto Mode and Repair

Overview
- YAMS can automatically generate embeddings and index document content on add.
- Embeddings are stored in the vector database; text content is indexed in SQLite FTS5.
- Both paths are best‑effort and non‑blocking to keep ingestion fast.

Auto Embeddings on Add
- Toggle via config: set in `~/.config/yams/config.toml`.
- Section `[embeddings]` keys:
  - `auto_on_add = "true|false"` — when true, new adds queue background embedding.
  - `preferred_model` and `model_path` optionally control model selection/loading.
- Behavior:
  - `yams add` and daemon directory indexing call embeddings asynchronously when enabled.
  - Extraction + FTS5 indexing still run immediately during add (for supported formats).

FTS5 Indexing
- When available in the SQLite build, YAMS indexes extracted text into `documents_fts`.
- DocumentService indexes FTS5 during add; directory add uses the same storage path.
- Searching uses hybrid keyword (FTS5) + semantic (vector) ranking when configured.

Repair and Rebuild
- CLI supports targeted repair flows:
  - `yams repair --embeddings` — generates missing embeddings for stored documents.
  - `yams repair --fts5` — rebuilds FTS5 entries (delete/insert) using robust extraction.
- Daemon RepairCoordinator also performs a best‑effort FTS5 reindex for documents it fixes embeddings for.

Notes
- Embedding and FTS5 operations degrade gracefully: failures are logged and skipped.
- Batch sizes and retries are conservative to avoid blocking foreground operations.

Embedding Dimension Source of Truth
- Single key: set `embeddings.embedding_dim` in `~/.config/yams/config.toml`.
- Runtime precedence: config > env (`YAMS_EMBED_DIM`) > generator > heuristic.
- The daemon derives vector DB schema and in-memory index dimensions from this single value to prevent drift.
