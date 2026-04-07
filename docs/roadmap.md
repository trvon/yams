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

## Current Release: v0.12.x

### Storage Engine
- Content-addressed blobs (SHA-256), Rabin chunking, zstd/LZMA compression
- WAL-backed SQLite metadata with FTS5 full-text index
- Path tree indexing with `path_prefix`, `reverse_path`, `path_hash` indexes
- Snapshot versioning with automatic git metadata detection (commit, branch, remote)
- Tree-based diff with Merkle comparison and rename detection (≥99% accuracy)
- Download manager with stop/start/resume controls via daemon

### Search
- **Hybrid search engine**: FTS5 keyword + vector similarity + Knowledge Graph fusion
  - Reciprocal Rank Fusion (RRF) as default fusion strategy
  - WEIGHTED_MAX fusion strategy for benchmark corpora
  - Configurable weights: keyword, vector, KG, tag, metadata, symbol
- **TurboQuant vector compression**: End-to-end packed-vector storage, reranking, and direct compressed ANN traversal with persisted per-coordinate calibration
- **Hierarchical embeddings**: document-level → chunk-level two-stage refinement
  - `twoStageVectorSearch` with configurable `doc_stage_limit`, `chunk_stage_limit`, `hierarchy_boost`
- **Symbol-aware search**: tree-sitter code analysis (15+ languages including Solidity, Zig)
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
- MCP code mode (composite tools: query, execute, session)
- `diff`: tree-based comparison with rename detection (default); `--flat-diff` for legacy
- `session`: pinned paths with `pin`, `unpin`, `list`, `warm` commands
- `doctor`: dedupe, prune (9 build systems, 10+ languages), embeddings repair, plugin diagnostics
- `graph`: read-only Knowledge Graph viewer with depth control
- Streaming IPC with protobuf serialization, multiplexing, and backpressure
- Consistent `--json` output across all commands
- Sandbox detection for AI coding environments (Codex, etc.)

### Daemon Architecture
- **WorkCoordinator**: centralized thread pool with Boost.Asio strands
  - Hardware-aware sizing (8-32 threads based on CPU cores)
- **Gradient2 adaptive concurrency limiters** (Netflix-style)
- **IOCoordinator**: dedicated read/write coordinator for socket server
- **Tuning profiles**: efficient/balanced/aggressive via `TuningManager`
- **Connection state machine**: tinyfsm-based `ConnectionFsm` with clean transitions
- **Async-first**: C++20 coroutines (`asio::awaitable`), `as_tuple` error handling
- **Streaming**: header-first chunked transfer, persistent sockets, TTFB metrics
- **Multi-client stability**: tested up to 16 concurrent clients

### Plugin System (ABI-stable C interface)
- **ONNX Runtime provider**: all-MiniLM-L6-v2 (default), multi-qa-MiniLM-L6-cos-v1, nomic-embed-text-v1.5
  - Dynamic tensor padding (~70-85x throughput for short inputs)
  - Session reuse with 25x warm-cycle latency reduction
  - Hardware-adaptive pool sizing
  - GPU acceleration: CoreML (macOS), CUDA (Linux/Windows), DirectML (Windows), MIGraphX (ROCm)
- **Tree-sitter symbol extractor**: auto-downloads grammars (v13-15), 15+ languages
- **PDF extractor**: content_extractor_v1 + search_provider_v1 interfaces
- Plugin discovery: `YAMS_PLUGIN_DIR`, standard directories, trust policies
- Lifecycle: scan, load, unload; daemon autoload on startup

### Packaging & Distribution
- **Build**: Meson + Conan 2.x; Release/Debug/Profiling configurations
- **Linux**: deb (APT repo at repo.yamsmemory.ai), rpm (YUM repo), Docker (amd64/arm64)
- **macOS**: Homebrew tap (`trvon/yams/yams`), pkg, zip (Apple Silicon + Intel)
- **Windows**: MSI installer (WiX v4), winget manifest
- **Package signing**: signed releases via CI

---

## Planned

### Mobile App
- [ ] Flutter-based mobile app with local corpus access
- [ ] Memory picker and context injection UX
- [ ] Conversation artifact write-back to YAMS
- [ ] Local LLM hosting (Gemma) for on-device inference

### P2P Corpus Sync
- [ ] Peer-to-peer corpus synchronization protocol
- [ ] Cross-device corpus replication
- [ ] Conflict resolution for concurrent edits
- [ ] Selective sync (collection/tag-based filtering)

### Mobile Bindings
- [ ] Finalize C ABI contract for corpus access (`libyams_mobile`)
- [ ] Backend parity: embedded and daemon behavior fully aligned
- [ ] Ship `.xcframework` (iOS) and `.aar` (Android) packages
- [ ] Flutter plugin consuming native corpus library

### Search & Retrieval
- [ ] Vector search tuning recommendations (documentation)
- [ ] Custom embedding model support

### Stability & Operations
- [ ] Thread safety hardening (TSan enabled in CI)
- [ ] Export/import snapshots preserving tags and edges
- [ ] Space reclamation and integrity verification CLI
- [ ] Prometheus metrics endpoint
- [ ] Structured logging with trace correlation

### Documentation
- [ ] Managed hosting early access guide
- [ ] P2P sync protocol specification

### Platform Expansion
- [ ] Cross-repository search federation

---

## Release History

| Version | Date | Highlights | Changelog |
|---------|------|------------|-----------|
| **v0.12** | 2026-04-02 | TurboQuant vector compression, download controls (stop/start/resume), ONNX ABI hardening, Catch2 migration | (see `CHANGELOG.md`) |
| **v0.10** | 2026-02-28 | Sandbox detection, MCP refactoring, daemon communication optimizations, package signing, graph improvements | [v0.10](changelogs/v0.10.md) |
| **v0.9** | 2026-02-24 | Gradient2 concurrency limiters, MCP code mode, IOCoordinator, multi-client stability (16 clients), embedding chunking | [v0.9](changelogs/v0.9.md) |
| **v0.8** | 2026-01-24 | HNSW rewrite, KG-boosted search, ONNX dynamic padding (70-85x), session reuse, download streaming, Zig support | [v0.8](changelogs/v0.8.md) |
| **v0.7** | | WorkCoordinator, hierarchical embeddings, tree-sitter symbols, grep SIMD, parallel post-processing | [v0.7](changelogs/v0.7.md) |
| **v0.6** | | Protobuf IPC, connection FSM, tuning profiles, RRF fusion, post-ingest pipeline, plugin system v0.1 | [v0.6](changelogs/v0.6.md) |
| **v0.5** | | Service architecture, HNSW index, daemon pooling, repair coordinator, PDF extraction | [v0.5](changelogs/v0.5.md) |
| **v0.4** | | Daemon architecture, universal content handlers, snapshot versioning, tree diff | [v0.4](changelogs/v0.4.md) |
| **v0.3** | | Hybrid search, vector database (sqlite-vec), MCP server, Apple Silicon optimizations | [v0.3](changelogs/v0.3.md) |
| **v0.2** | | FTS5 search, content chunking, basic CLI | [v0.2](changelogs/v0.2.md) |
| **v0.1** | | Initial storage engine, add/get/delete | [v0.1](changelogs/v0.1.md) |

---

## Contributing

1. **Open an issue** with a short proposal: problem, approach, scope, risks
2. **For larger items**: include evaluation plan and acceptance criteria
3. **Keep changes incremental** and testable

See the repo `CONTRIBUTING.md` for development setup.
