# Architecture Overview (Index)

A concise landing page for YAMS architecture. Read this to understand the moving parts, then jump into the focused docs.

## TL;DR

- Core model: content‑addressed storage + rich metadata + fast search (FTS and optional vector/semantic)
- Runtime entry points:
  - CLI: `yams` (tools for add/get/delete/list/search/etc.)
  - MCP server: `yams serve` (stdio/websocket) for AI/agent integrations
- Datastores:
  - Content store on disk (chunked, deduped, compressed)
  - SQLite for metadata and FTS5 text indexing
  - Optional vector/semantic search layer (see vector search doc)
- Design goals: correctness, durability, speed, simple ops

## System Map (High Level)

- Storage
  - Chunking, hashing (SHA-256), deduplication
  - Compression (Zstd, LZMA for archival paths as configured)
- Metadata
  - SQLite schema for documents, metadata, tags, and search indices
  - Migrations for schema evolution
- Search
  - FTS5 for full‑text (names, content, tags)
  - Optional fuzzy search (BK‑tree style indexing) and snippets
  - Optional vector/semantic search (similarity / hybrid)
- API Surfaces
  - CLI (primary for local/hacker workflows)
  - MCP (agent/runtime integration; stdio or websocket)
  - HTTP API (in progress; see OpenAPI and topical guides)
- Configuration
  - XDG paths by default; env overrides (e.g., `YAMS_STORAGE`)
  - TOML config; keys under `~/.config/yams/keys/`

## Code Orientation (Where to Look)

- Core libraries: `src/{core,storage,compression,chunking,integrity,manifest,wal}`
- Metadata and DB: `src/metadata`
- Search and indexing: `src/search`, `src/indexing`, `src/extraction`
- CLI: `src/cli`
- MCP server and transports: `src/mcp`
- Public headers: `include/yams/...`

Tip: Start with CLI flows (commands call into storage/metadata/search), then drill into storage and search internals as needed.

## Key Architecture Docs

- Full‑text and keyword search (FTS, fuzzy, snippets)  
  - ../../architecture/search_system.md
- Vector/semantic search (similarity & hybrid)  
  - ../../architecture/vector_search_architecture.md

## Operations & Usage (Jump Points)

- CLI Reference (authoritative, verbose‑help source):  
  - ../../user_guide/cli.md
- Installation (fast path):  
  - ../../user_guide/installation.md
- MCP WebSocket transport (agent integrations):  
  - ../../mcp_websocket_transport.md
- Admin configuration (paths, DB, indexing, performance knobs):  
  - ../../admin/configuration.md

## Principles (Design North Star)

- Content is immutable; addresses derive from content
- Metadata is explicit, indexable, and query‑friendly
- Durable by default; performance without sacrificing integrity
- Clear boundaries: storage, metadata, search, surfaces (CLI/MCP/HTTP)

## Contributing Architecture Notes

- Keep pages short, link‑rich, and diagram‑friendly
- Put deep dives into dedicated files under `docs/architecture/`
- Cross‑link to CLI and admin docs to avoid duplication
- Prefer “example first” sections for features and flows

---
This index intentionally stays brief. For exhaustive behavior/flags, see the CLI reference and per‑topic architecture docs linked above.