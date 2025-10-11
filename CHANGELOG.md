# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

## [v0.7.5] - Unreleased
### Added
- Grep: enabling `[search.path_tree]` now lets explicit path filters reuse the metadata-backed
  path-tree engine, and tag-only invocations default the pattern to `.*`, removing the need for
  placeholder expressions. citesrc/app/services/grep_service.cpp:240src/cli/commands/grep_command.cpp:305
- **Tree-based List with Filters**: Extended tree-based path queries to support tag, MIME type, and extension filtering. The `list` command now uses the tree index even when filters are applied, improving performance for pattern+filter queries (e.g., `yams list --name "docs/**" --tags "test"`).
- **Benchmark: Tree List Filters**: New benchmark suite (`tree_list_filter_bench`) measures query performance with various filter combinations. Results show 100-160μs query times with up to 10k queries/sec throughput. Filter queries often outperform path-only queries due to reduced result set sizes.
- **Grep SQL-Level Pattern Filtering**: Added `queryDocumentsByGlobPatterns()` function that converts glob patterns (e.g., `tests/**/*.cpp`) to SQL LIKE patterns and queries the database directly, eliminating the need to load all documents into memory before filtering. Grep performance with `--include` patterns improved dramatically on large repositories.
- **Search Multi-Pattern Support**: Added `pathPatterns` vector field to `SearchRequest` IPC protocol (field 34) enabling server-side filtering of multiple include patterns. Search command now sends all patterns to daemon instead of filtering results client-side, eliminating timeouts and OOM errors in sandboxed environments.
- **MCP Search Multi-Pattern Support**: Added `include_patterns` array parameter to MCP search tool, enabling clients to specify multiple path patterns with OR logic. The MCP server now populates `pathPatterns` in daemon requests, matching CLI behavior. (`include/yams/mcp/tool_registry.h`, `src/mcp/mcp_server.cpp`)

### Changed
- MCP stdio transport: stdout buffering now adapts to interactive vs non-interactive streams,
  stderr is forced unbuffered, and JSON-RPC batch arrays over stdio are parsed in-line to match
  the Model Context Protocol 2025-03-26 transport requirements. Additional unit coverage exercises
  batch handling and error budgets for framed headers.
- **MCP Server**: `cat` and `get` tools now resolve relative paths using `weakly_canonical`, improving document lookup for non-absolute paths.
- **Path Canonicalization**: Document paths are now canonicalized using `weakly_canonical()` at ingestion time to ensure consistent path matching across symlinked directories (e.g., `/var` → `/private/var` on macOS). This fixes pattern-based queries that previously failed due to path mismatch between indexed and query paths. (`src/metadata/path_utils.cpp`)
- **Integration Test Stability**: Improved `TreeBasedListE2E` test reliability by replacing fixed sleep with polling-based wait for document indexing completion. Test pass rate improved from ~60% to 100%.
- **Grep Performance**: Grep service now uses SQL-level pattern filtering when `--include` patterns are provided, fetching only matching documents from the database instead of loading all documents and filtering in memory. Converts glob patterns to SQL LIKE patterns (e.g., `*.cpp` → `%.cpp`, `tests/**/*.h` → `tests/%.h`). This eliminates hangs on large repositories (10K+ documents).
- **Search Service**: Updated to handle multiple path patterns via `pathPatterns` vector field, iterating through all patterns with OR logic for server-side filtering. Removed client-side filtering that previously caused timeouts with multiple `--include` patterns.

### Fixed
- **Document Service**: Improved `resolveNameToHash` to correctly handle filename-only lookups by searching for paths ending with the given name, ensuring that commands like `cat` with a simple filename succeed.
- **Tree Query Pattern Matching**: Fixed wildcard pattern parsing to correctly handle recursive patterns (`/**`) by stripping all trailing wildcards iteratively instead of checking for a single wildcard character. (`src/app/services/document_service.cpp`)
- **Grep Hang**: Fixed grep command hanging indefinitely when using `--include` patterns on large repositories. The service was fetching all documents before filtering; now uses SQL-level pattern matching to fetch only relevant documents.
- **Search Timeout**: Fixed search command timeouts/OOM when using multiple `--include` patterns. Previously only the first pattern was sent to daemon with remaining patterns filtered client-side after retrieving ALL results. Now all patterns are sent to daemon for server-side filtering.
- **Search Pattern Matching**: Fixed glob pattern normalization to correctly match filename patterns like `*.md` and `*.cpp` anywhere in the path tree. Patterns starting with a single `*` (e.g., `*.ext`) are now automatically prefixed with `**/` to match paths at any depth. This ensures patterns like `--include="*.md,*.cpp"` correctly return all matching files regardless of their directory location.
- **Search Async Path**: Fixed `SearchCommand::executeAsync()` not populating `pathPatterns` field in daemon request, causing server-side multi-pattern filtering to fail. The async code path (default execution) now correctly sends all include patterns to the daemon, matching the behavior of the sync path. (`src/cli/commands/search_command.cpp:1360-1365`)
- **Database Schema Compatibility**: Fixed "constraint failed" errors during document insertion on databases with migration v12 (pre-path-indexing schema). The `insertDocument()` function now conditionally builds INSERT statements based on the `hasPathIndexing_` flag, supporting both legacy (13-column) and modern (17-column with path indexing) schemas. This allows YAMS to work correctly regardless of whether migration v13 has been applied. (`src/metadata/metadata_repository.cpp:318-380`)
- **MCP Protocol Version Negotiation**: Fixed "Unsupported protocol version requested by client" error (code -32901) by making protocol version negotiation permissive by default (`strictProtocol_ = false`). The server now gracefully accepts any protocol version requested by clients, falling back to the latest supported version (`2025-03-26`) if the requested version is not in the supported list. Also added intermediate MCP protocol versions (`2024-12-05`, `2025-01-15`) to the supported list. This ensures maximum compatibility with MCP clients regardless of which spec version they implement. (`src/mcp/mcp_server.cpp:560,1254-1260`)


## [v0.7.4] - 2025-10-010

### Changed
- **MetadataRepository**: Added atomic counters (`cachedDocumentCount_`, `cachedIndexedCount_`, `cachedExtractedCount_`) updated on every insert/delete/update operation. Eliminated 3 `COUNT(*)` queries from hot path (220-400ms → <1μs)
- **VectorDatabase**: Added `cachedVectorCount_` atomic counter updated on insert/delete operations. Eliminated `COUNT(*)` query from `getVectorCount()` 
- **ServiceManager Concurrency**: Converted `searchEngineMutex_` from `std::mutex` to `std::shared_mutex` enabling N concurrent readers with single exclusive writer. Allows parallel status requests without serialization bottleneck
- **Status Request Optimization**: Removed blocking VectorDatabase initialization from hot path. Status handler now reports readiness accurately without attempting to "fix" uninitialized state, eliminating 1-5s blocking operations
- **Performance**: Sequential request throughput improved to ~1960 req/s with sub-millisecond latency (avg: 0.02ms, max: 1ms). First connection latency: 2ms. Daemon readiness validation added to prevent test methodology races with initialization
- **Document Retrieval Optimization**: Replaced O(n) full table scans with O(log n) indexed lookups in `cat`/`get` operations. Changed from `queryDocumentsByPattern('%')` → `getDocumentByHash(hash)` eliminating 120K+ document scans per retrieval (lines 850, 934 in document_service.cpp)
- **Name Resolution Fix**: Fixed pattern generation for basename-only queries. Now generates `'%/basename'` pattern FIRST to use `containsFragment` query instead of failing `exactPath` (path_hash) match. `yams get --name` and `yams cat <name>` now work correctly
- **Grep FTS-First**: Optimized grep to START with FTS5 index search for literal patterns before falling back to full document scan. Regex patterns still use full scan. Significantly improves grep performance on large repositories
- **ONNX Plugin**: Upgraded the ONNX plugin to conform to the modern `model_provider_v1` (v1.2) interface specification.
- Enhanced `ui_helpers.hpp` with 30+ new utilities: value formatters (`format_bytes`, `format_number`, `format_duration`, `format_percentage`), status indicators (`status_ok`, `status_warning`, `status_error`), table rendering (`Table`, `render_table`), progress bars, text utilities (word wrap, centering, indentation)
- Improved `yams status` with color-coded severity indicators, human-readable formatting, and sectioned layout
- Enhanced `yams daemon status` with humanized counter names (CAS, IPC, EMA, DB acronyms preserved), smart byte/number formatting
- Added `yams daemon status -d` detailed view with storage overhead breakdown showing disk usage by component (CAS blocks, ref counter DB, metadata DB, vector DB, vector index) with overhead percentage relative to content

### Deprecated
- **MCP `get_by_name` tool**: Use `get` tool with `name` parameter instead. The `get` tool now smartly handles both hash and name lookups with optimized pattern matching

### Fixed
- **Streaming Protocol Bug**: Fixed critical bug where `GetResponse`/`CatResponse` sent header-only frame (empty content) followed by data frame, causing CLI to process first frame and fail. Added `force_unary_response` check in request_handler.cpp to disable streaming for these response types, forcing single complete frame transmission
- **Protobuf Schema**: Added missing `bool has_content = 6` field to `GetResponse` message in ipc_envelope.proto. Updated serialization to explicitly set/read flag instead of recalculating, preventing desync between daemon and CLI
- **Daemon**: Fixed a regression in the plugin loader that prevented legacy model provider plugins (like the ONNX provider) from being correctly detected and adopted. The loader now includes a fallback to detect and register providers using the legacy `getProviderName`/`createProvider` symbols, restoring embedding generation functionality.
- **Grep Service**: Fixed critical bug where `--paths-only` mode returned all candidate documents without checking pattern matches, causing incorrect "(no results)" responses. Removed premature fast-exit optimization; grep now properly runs pattern matching and returns only files that match. (Issue: 135K docs indexed but grep returned empty, audit revealed fast-exit bypassed validation)
- **Grep CLI**: Fixed session pattern handling bug where session include patterns were incorrectly used as document selectors instead of result filters. Session patterns now properly merged into `includePatterns` for filtering, not `paths` for selection. This prevented grep from finding any results when a session was active.

## [v0.7.3] - 2025-10-08

### Added
- Bench: minimal daemon warm-start latency check moved to an opt-in bench target and suite.
  - New standalone binary `tests/yams_bench_daemon_warm` executes a bounded start/sleep/stop
    cycle with vectors disabled and tight init timeouts; asserts <5s end-to-end.
  - Meson test registered as `bench_daemon_warm_latency` in the `yams:bench` suite.
  - Disabled by default in CI; enable by setting `RUN_DAEMON_WARM_BENCH=true` (workflow env)
    and `YAMS_ENABLE_DAEMON_BENCH=1` (step env) to run only this bench.
- Tree-Diff Metadata & Retrieval Modernization🎉
  - **Tree-based snapshot comparison**: Implemented Merkle tree-based diff algorithm for efficient snapshot comparison with O(log n) subtree hash optimization for unchanged directories.
  - **Rename detection**: Hash-based rename/move detection with ≥99% accuracy, enabled by default in `yams diff` command.
  - **Knowledge Graph integration**: Path and blob nodes with version edges and rename tracking via `fetchPathHistory()` API.
  - **Enhanced graph command**: `yams graph` now queries KG store for same-content relationships and rename chains.
  - **Tree diff as default**: `yams diff` uses tree-based comparison by default; `--flat-diff` flag available for legacy behavior.
  - **RPC/IPC exposure**: Added `ListTreeDiff` method to daemon protocol (protobuf + binary serialization).

### Changed
- **Compression-first retrieval** DocumentService, CLI, and daemon IPC now default to
  returning compressed payloads with full metadata (algorithm, CRC32s, sizes)
- **Path query pipeline**: Replaced the legacy `findDocumentsByPath` helper with the normalized `queryDocuments` API and the shared `queryDocumentsByPattern` utility. All services (daemon, CLI, MCP, mobile bindings, repair tooling, vector ingestion) now issue structured queries that leverage the `path_prefix`, `reverse_path`, and `path_hash` indexes plus FTS5 for suffix matches, eliminating full-table LIKE scans.
- **Schema migration**: Migration v13 (`Add path indexing schema`) continues to govern the derived columns/indices; applying this release replays the up hook in place (normalizing existing rows and rebuilding the FTS table), so existing deployments automatically benefit from the optimized lookups after the usual migration step.
- **CLI Retrieval (get/cat)**: partial-hash resolution now routes through `RetrievalService`
  using the daemon’s streaming search and the metadata-layer hash-prefix index.
  - `yams get` and `yams cat` accept 6–64 hex prefixes; ambiguity can be resolved via
    `--latest/--oldest`. No more local metadata table scans; latency improves especially on
    large catalogs.
  - Internals: `RetrievalService::resolveHashPrefix` consumes `SearchService` hash results and applies newest/oldest selection hints; `GetCommand` validates and normalizes hash input before issuing a daemon `Get`.

### Fixed
- **Daemon IPC:** Fixed a regression in the `grep` IPC protocol where `GrepRequest` and `GrepResponse` messages were not fully serialized, causing data loss. The protocol definitions and serializers have been updated to correctly handle all fields, including `show_diff` in requests and detailed statistics in responses.
- **Indexing**: Fixed an issue where updated files were not being re-indexed. The change detection logic now correctly considers file modification time and size, in addition to content hash, to reliably identify changes.
- **Indexing**: Corrected the document update process to prevent duplicate records for the same file path when a file is updated. The indexer now properly distinguishes between new documents and updates to existing ones.
- **Daemon IPC**: Fixed an issue where `search` and `grep` commands could time out without producing output by improving the efficiency of the daemon's streaming response mechanism.
- **Daemon IPC**: Optimized non-multiplexed communication paths to prevent performance issues and potential timeouts with large responses from commands like `get` and `cat`.
