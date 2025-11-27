# YAMS Roadmap

> Pre-1.0 releases (v0.x) are not stable. Expect breaking changes until v1.0 GA.

---

## Principles

| Principle | Description |
|-----------|-------------|
| **OSS-first** | CLI, storage engine, search, and MCP (stdio) are the open-source core |
| **Data portability** | Export/import with full fidelity; no vendor lock-in |
| **Deterministic builds** | Reproducible artifacts across platforms |
| **Local-first** | OSS version runs entirely offline; managed hosting is optional |

---

## Current Release: v0.8.x

### Storage Engine
- Content-addressed blobs (SHA-256), Rabin chunking, zstd/LZMA compression
- WAL-backed SQLite metadata with FTS5 full-text index
- Path tree indexing with `path_prefix`, `reverse_path`, `path_hash` indexes
- Snapshot versioning with automatic git metadata detection (commit, branch, remote)
- Tree-based diff with Merkle comparison and rename detection (≥99% accuracy)

### Search
- **Hybrid search engine**: FTS5 keyword + vector similarity + Knowledge Graph fusion
  - Reciprocal Rank Fusion (RRF) as default fusion strategy
  - Configurable weights: keyword, vector, KG, tag, metadata, symbol
- **Hierarchical embeddings**: document-level → chunk-level two-stage refinement
  - `twoStageVectorSearch` with configurable `doc_stage_limit`, `chunk_stage_limit`, `hierarchy_boost`
- **Symbol-aware search**: tree-sitter code analysis (15+ languages including Solidity)
  - `SymbolEnricher` extracts definitions, references, call graphs from KG
  - Symbol metadata boosts ranking (`symbol_weight` default: 0.15)
- **Query processing**: literal extraction, qualifier parsing (lines:, pages:, section:, name:, ext:, mime:)
- **Parallel post-processing**: `ParallelPostProcessor` for result sets ≥100 items (3.4x speedup at 1000 results)
- **Fuzzy search**: BK-tree index with intelligent document prioritization

### Grep
- **Literal extraction**: fast literal pre-filter from regex patterns
- **Boyer-Moore-Horspool**: optimized string search for patterns ≥3 characters
- **SIMD newline scanning**: AVX2/SSE2/NEON with scalar fallback (4-8x speedup)
- **Parallel candidate filtering**: chunk-based processing for large corpora (2-4x speedup)
- **FTS-first optimization**: uses FTS5 index for literal patterns before full scan

### CLI & MCP
- CLI-first design; MCP server via stdio only (no HTTP/WebSocket in OSS)
- `diff`: tree-based comparison with rename detection (default); `--flat-diff` for legacy
- `session`: pinned paths with `pin`, `unpin`, `list`, `warm` commands
- `doctor`: dedupe, prune (9 build systems, 10+ languages), embeddings repair, plugin diagnostics
- `graph`: read-only Knowledge Graph viewer with depth control
- Streaming IPC with protobuf serialization, multiplexing, and backpressure

### Plugin System (ABI-stable C interface)
- **ONNX Runtime provider**: all-MiniLM-L6-v2 (default), all-mpnet-base-v2, nomic-embed-text-v1.5
- **Tree-sitter symbol extractor**: auto-downloads grammars (v13-15), 15+ languages
- **PDF extractor**: content_extractor_v1 + search_provider_v1 interfaces
- Plugin discovery: `YAMS_PLUGIN_DIR`, standard directories, trust policies
- Lifecycle: scan, load, unload; daemon autoload on startup

### Daemon Architecture
- **WorkCoordinator**: centralized thread pool with Boost.Asio strands
  - Hardware-aware sizing (8-32 threads based on CPU cores)
  - Replaced 3 separate pools (IngestService, PostIngestQueue, EmbeddingService)
- **Tuning profiles**: efficient/balanced/aggressive via `TuningManager`
- **Connection state machine**: tinyfsm-based `ConnectionFsm` with clean transitions
- **Async-first**: C++20 coroutines (`asio::awaitable`), `as_tuple` error handling
- **Streaming**: header-first chunked transfer, persistent sockets, TTFB metrics

### Packaging & Distribution
- **Build**: Meson + Conan 2.x; Release/Debug/Profiling configurations
- **Linux**: deb, rpm, AppImage, Docker (amd64/arm64)
- **macOS**: pkg, zip (Apple Silicon + Intel with `-mcpu=apple-m1` optimizations)
- **Windows**: MSI installer (WiX v4), winget manifest
- **Homebrew**: tap at `yams-sh/homebrew-yams`

---

## Planned: v0.9 → v1.0

Focus: **Stability, polish, and production readiness**

### CLI & Developer Experience
- [ ] Path tree repair automation (background daemon task)
- [ ] Improved error messages with actionable hints
- [ ] Consistent `--json` output across all commands
- [ ] Incremental `yams add` with predictable include/exclude

### Search Quality
- [ ] FTS5 hygiene: index only queried fields
- [ ] Optional post-ingest `VACUUM`/`optimize`
- [ ] Content-type-aware defaults (code vs. prose vs. docs)

### Stability
- [ ] Thread safety hardening (TSan enabled in Debug CI)
- [ ] Connection pool reliability fixes
- [ ] Streaming response timeout handling

### Documentation
- [ ] Managed hosting early access guide
- [ ] Vector search tuning recommendations

---

## Future: Post-1.0

### Content Lifecycle
- [ ] Export/import snapshots preserving tags and edges
- [ ] Space reclamation and integrity verification CLI

### Platform Expansion
- [ ] Flutter bindings for iOS/Android
- [ ] Cross-repository search federation

### Advanced Features
- [ ] Custom embedding model support
- [ ] Prometheus metrics endpoint
- [ ] Structured logging with trace correlation

---

## Release History

| Version | Highlights | Changelog |
|---------|------------|-----------|
| **v0.7** | WorkCoordinator thread pool, hierarchical embeddings, KG-boosted search, tree-sitter symbol extraction, grep SIMD optimizations, parallel post-processing | [v0.7](changelogs/v0.7.md) |
| **v0.6** | Protobuf IPC, connection FSM, tuning profiles, RRF fusion, post-ingest pipeline, plugin system v0.1 | [v0.6](changelogs/v0.6.md) |
| **v0.5** | Service architecture, HNSW index, daemon pooling, repair coordinator, PDF extraction | [v0.5](changelogs/v0.5.md) |
| **v0.4** | Daemon architecture, universal content handlers, snapshot versioning, tree diff | [v0.4](changelogs/v0.4.md) |
| **v0.3** | Hybrid search, vector database (sqlite-vec), MCP server, Apple Silicon optimizations | [v0.3](changelogs/v0.3.md) |
| **v0.2** | FTS5 search, content chunking, basic CLI | [v0.2](changelogs/v0.2.md) |
| **v0.1** | Initial storage engine, add/get/delete | [v0.1](changelogs/v0.1.md) |

---

## Contributing

1. **Open an issue** with a short proposal: problem, approach, scope, risks
2. **For larger items**: include evaluation plan and acceptance criteria
3. **Keep changes incremental** and testable

See [CONTRIBUTING.md](../CONTRIBUTING.md) for development setup.
