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
## [v0.7.7] - November 1, 2025

### Added
- **ServiceManager & Daemon Lifecycle Improvements**
  - **Structured Concurrency**: Replaced manual backpressure logic with `std::counting_semaphore` for natural bounded concurrency
  - **SocketServer Improvements**: 
    - Converted async_accept to `as_tuple` pattern, eliminating exception overhead during shutdown
    - Connection future tracking for graceful shutdown with 2s timeout verification
  - **Modern Error Handling**: Consistent use of `boost::asio::as_tuple(use_awaitable)` for error codes instead of exceptions
  - **Future Tracking**: Replaced detached spawns with `use_future` for verifiable connection lifecycle management
- **Doctor Prune Command**: Intelligent cleanup of build artifacts, logs, cache, and temporary files
  - Support for 9 build systems (CMake, Ninja, Meson, Make, Gradle, Maven, NPM/Yarn, Cargo, Go)
  - Detection across 10+ programming languages (C/C++, Java, Python, JavaScript, Rust, Go, OCaml, Haskell, Erlang, etc.)
  - Hierarchical category system: build-artifacts, build-system, logs, cache, temp, coverage, IDE
  - Dry-run by default with `--apply` flag for execution
  - Usage: `yams doctor prune --category build-artifacts --older-than 30d --apply`
- Started C++23 Compatibility support expansion
- Migrated vectordb to [https://github.com/trvon/sqlite-vec-cpp](https://github.com/trvon/sqlite-vec-cpp)
- **Tree-sitter Symbol Extraction Plugin** (PBI-073): Enhanced multi-language symbol extraction with Solidity support
  - **Solidity Support**: Added complete Solidity language support with 4 query patterns (functions, constructors, modifiers, fallback/receive)
  - **Enhanced C++ Patterns**: 16 function patterns + 6 class patterns including templates, constructors, destructors, operator overloads, method declarations inside class bodies
  - **Multi-Language Improvements**: Enhanced patterns for Python (decorated functions), Rust (impl/trait methods), JavaScript/TypeScript (arrow functions, generators, async), Kotlin (property declarations) across all 15 supported languages
  - **Critical Bug Fix**: Fixed query execution early-return bug that caused pattern short-circuiting - now executes all patterns resulting in **2.2x recall improvement** (20.6% → 45.1%)
  - **Benchmark Infrastructure**: Catch2-based benchmark suite with quality metrics (Recall/Precision/F1), performance metrics (Throughput/Latency), and JSON output for CI integration
  - **GTest Suite**: 7 Solidity tests covering ERC20 tokens, inheritance, interfaces, events, and modifiers (372 lines, all passing)
  - Plugin auto-downloads tree-sitter grammars on first use (configurable via `plugins.symbol_extraction.auto_download_grammars`)
  - CLI commands: `yams config grammar list/download/path/auto-enable/auto-disable`
  - Supports tree-sitter v13-15 grammar versions
- **Entity Graph Service**: Background service for extracting and materializing code symbols into Knowledge Graph
  - Wired into IndexingPipeline and RepairCoordinator for automatic symbol extraction
  - Supports plugin-based language-specific symbol extraction
  - Foundation for symbol-aware search and code intelligence features
- **Database Schema v16**: Added `symbol_metadata` table for rich symbol information storage
  - Stores symbol definitions, references, and metadata from code analysis plugins
  - Indexed by document hash and symbol name for efficient lookups
  - Integrated with Knowledge Graph for entity relationship tracking
  - Migration includes tests for both schema changes and symbol metadata storage
- **Symbol-Aware Search Infrastructure**: Enhanced search with symbol/entity detection and enrichment
  - `SymbolEnricher` class extracts rich metadata from Knowledge Graph (definitions, references, call graphs)
  - Symbol context includes type, scope, caller/callee counts, and related symbols
  - **Hybrid Search Symbol Integration**: Symbol metadata now actively boosts search ranking
    - Added `symbol_weight` configuration field (default: 0.15 = 15% multiplicative boost)
    - `HybridSearchEngine::setSymbolEnricher()` method wires SymbolEnricher into search pipeline
    - Symbol matches receive score boost when `isSymbolQuery && symbolScore > 0.3`

### Fixed
- **Embedding System Architecture Simplification**: Simplified FSM readiness logic to check provider availability directly instead of waiting for model load events
  - IModelProvider checks `isAvailable()` immediately after plugin adoption
  - Eliminates unnecessary ModelLoading state transition
  - Fixes "Embedding Ready: Waiting" status showing incorrectly when embeddings were actually available
  - Model dimension retrieved via `getEmbeddingDim()` at adoption time
- **Database Schema Recovery**: Manual creation of missing `kg_doc_entities` table from migration 7
  - Table includes 8 columns with foreign keys to documents and kg_nodes
  - Created indexes: `idx_kg_doc_entities_document`, `idx_kg_doc_entities_node`
  - Fixes search query errors: "no such table: kg_doc_entities"
- **Worker Thread Premature Exit**: Fixed io_context workers exiting immediately on startup by adding `executor_work_guard` to keep the context alive until explicit shutdown.
- **SocketServer Backpressure**: Manual backpressure polling with `std::counting_semaphore`, eliminating 5-20ms delay loops and providing natural bounded concurrency.
- **Embedding Consumer Deadlock**: Fixed race condition causing embedding job consumer to stall
  - Added defensive retry mechanism with exponential backoff for queue state recovery
  - Impact: Embedding background processing now reliable under high load
- **FSM Cleanup & Degradation Tracking**: Standardized FSM usage across ServiceManager
  - Added `DaemonLifecycleFsm` reference to ServiceManager for centralized subsystem degradation tracking
- **ServiceManager FSM Architecture**: Centralized state management and eliminated duplication
  - Added `DaemonLifecycleFsm& lifecycleFsm_` reference to ServiceManager for daemon-level degradation tracking
  - Removed scattered manual FSM state checks in favor of FSM query methods (`isReady()`, `isLoadingOrReady()`)
- **Text Extraction for Source Code**: Fixed critical issue where JavaScript/TypeScript/Solidity/config files failed FTS5 extraction
    - `src/extraction/extraction_util.cpp`: Replaced hardcoded `is_text_like()` with `FileTypeDetector::isTextMimeType()` which uses comprehensive `magic_numbers.hpp` database; added extension normalization (handles both `.js` and `js` formats)
    - `src/extraction/plain_text_extractor.cpp`: 
      - Removed hardcoded 50+ extension list, delegating to `FileTypeDetector` for dynamic detection
      - Enhanced `isBinaryFile()` with UTF-8 BOM support and reduced false positives
      - Added `isParseableText()` with proper UTF-8 validation (validates multi-byte sequences, continuation bytes)
      - Baseline registration now includes common config/markup extensions (`.toml`, `.ini`, `.yml`, `.md`, `.rst`)
    - `src/app/services/search_service.cpp`: Updated lightweight indexing to use `FileTypeDetector::isTextMimeType()`

### Changed
- **Embedding Provider Lifecycle**: Transitioned from event-driven model loading to direct availability checking
  - Provider adoption now immediately dispatches `ModelLoadedEvent` if `isAvailable()` returns true
  - Simplified from 4-state FSM (Unavailable → ProviderAdopted → ModelLoading → ModelReady) to immediate ready transition
  - Aligns FSM with IModelProvider on-demand model loading architecture

### Removed
- **Comprehensive Dead Code Cleanup** (90+ lines removed via clang-tidy analysis):
  - **ServiceManager.cpp** (85 lines): Removed `launchModelEventConsumer()` function and its background event polling loop (obsolete with synchronous provider availability checking)
  - **PostIngestQueue.cpp**: Removed unused `haveTask` boolean assignment
  - **plugin_command.cpp**: Removed unused `have_typed` and `have_json` boolean calculations
  - **status_command.cpp**: Removed unused `embeddingsWarn` variable
  - **mcp_server.cpp**: Removed unused `verbose` environment variable check in `handleDownload()`
  - All dead stores identified through static analysis with `clang-tidy --checks=clang-analyzer-deadcode.DeadStores`
- **Fuzzy Index Memory Optimization**: Enhanced BK-tree index building with intelligent document prioritization
  - Uses metadata and Knowledge Graph to rank documents by relevance (tagged > KG-connected > recent > code files)
  - Limits index to 50,000 documents by default (configurable via `YAMS_FUZZY_INDEX_LIMIT` environment variable)
  - Graceful degradation with `std::bad_alloc` handling prevents daemon crashes on large repositories
  - **Known Limitation**: Fuzzy search on very large repositories (>100k documents) may experience memory pressure. Consider using metadata/KG filters or grep with exact patterns for better performance
- **ONNX Plugin Model Path Resolution**: Enhanced model path search to support XDG Base Directory specification
- **Platform-Aware Plugin Installation**: Build system now auto-detects Homebrew prefix on macOS
  - `/opt/homebrew` on Apple Silicon, `/usr/local` on Intel Macs and Linux
  - System plugin directory automatically trusted by daemon at runtime
  - Override via `YAMS_INSTALL_PREFIX` environment variable
- Model loading timeouts hardened: adapter and ONNX plugin now use std::async with bounded wait; removed detached threads causing UAF/segfaults (AsioConnectionPool guarded)
- Vector DB dim resolution no longer hardcodes 384; resolves from DB/config/env/provider preferred model, else warns and defers embeddings
- ONNX plugin: removed implicit 384 defaults, derives embeddingDim dynamically from model/config; added env override YAMS_ONNX_PRECREATE_RESOURCES
- Improved load diagnostics: detailed logs for ABI table pointers, phases, and timeout causes
- **Search Service Path Heuristic**: Tightened path-first detection to only trigger for single-token or quoted path-like queries (slashes, wildcards, or extensions). Multi-word queries now proceed to hybrid/metadata search, restoring results for phrases such as `"docs/delivery backlog prd tasks PBI"` while preserving fast path lookups for actual paths.

- Daemon stop reliability: `yams daemon stop` now only reports success after the process actually exits and will fall back to PID-based termination (and orphan cleanup) when the socket path is unresponsive.
- Prompt termination on signals: the daemon now handles SIGTERM/SIGINT to exit promptly when graceful shutdown isn't possible, addressing lingering yams-daemon processes after stop.
- **Hybrid Search Simplification**: Removed complexity and environment variable overrides
  - Removed 6 environment variables: `YAMS_DISABLE_KEYWORD`, `YAMS_DISABLE_ONNX`, `YAMS_DISABLE_KG`, `YAMS_ADAPTIVE_TUNING`, `YAMS_FUSION_WEIGHTS`, `YAMS_OVERRELIANCE_PENALTY`
  - Kept `YAMS_DISABLE_VECTOR` for CI compatibility
  - Removed adaptive weight tuning logic (~30 LOC)
  - Removed over-reliance penalty mechanism
  - Keyword search now always executes (controlled by `config.keyword_weight`)
  - Fixed fusion weights for `LEARNED_FUSION` strategy: `{-2.0f, 3.0f, 2.0f, 1.5f, 1.0f}`

### Removed
- Removed WASM, and legacy plugin system from codebase and ServiceManager

## [v0.7.6] - 10-13-2025
### Added
- **CLI Pattern Ergonomics**: Added `--pattern/-p` flag to `list` command as an alias for `--name`, improving consistency with other commands. The flag supports glob wildcards (`*`, `?`, `**`) and auto-normalizes relative paths to absolute when no wildcards are present. (`src/cli/commands/list_command.cpp`)
- **Grep Literal Text Hints**: Added smart error detection and helpful hints when grep patterns contain regex special characters. When a pattern fails regex compilation or returns no results, grep now suggests using the `-F` flag with the exact command to run. Added `-Q` as a short alias for `-F/--fixed-strings/--literal-text` to match git grep convention. (`src/cli/commands/grep_command.cpp`)
- **Search Literal Text Aliases**: Added `-F/-Q/--fixed-strings` aliases to `search` command for consistency with `grep`. These short flags make it easier to search for literal text containing special characters like `()[]{}.*+?`. Updated help text with concrete examples. (`src/cli/commands/search_command.cpp`)
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
- **Build System**: Fixed VS Code task definitions with correct Conan 2.x output paths. Meson native file paths updated from `builddir/conan_meson_native.ini` to `builddir/build-debug/conan/conan_meson_native.ini` and `build/release/build-release/conan/conan_meson_native.ini` to match actual Conan 2 directory structure. (`.vscode/tasks.json`)

### Fixed
- **Document Service**: Improved `resolveNameToHash` to correctly handle filename-only lookups by searching for paths ending with the given name, ensuring that commands like `cat` with a simple filename succeed.
- **Tree Query Pattern Matching**: Fixed wildcard pattern parsing to correctly handle recursive patterns (`/**`) by stripping all trailing wildcards iteratively instead of checking for a single wildcard character. (`src/app/services/document_service.cpp`)
- **Grep Hang**: Fixed grep command hanging indefinitely when using `--include` patterns on large repositories. The service was fetching all documents before filtering; now uses SQL-level pattern matching to fetch only relevant documents.
- **Search Timeout**: Fixed search command timeouts/OOM when using multiple `--include` patterns. Previously only the first pattern was sent to daemon with remaining patterns filtered client-side after retrieving ALL results. Now all patterns are sent to daemon for server-side filtering.
- **Search Pattern Matching**: Fixed glob pattern normalization to correctly match filename patterns like `*.md` and `*.cpp` anywhere in the path tree. Patterns starting with a single `*` (e.g., `*.ext`) are now automatically prefixed with `**/` to match paths at any depth. This ensures patterns like `--include="*.md,*.cpp"` correctly return all matching files regardless of their directory location.
- **Search Async Path**: Fixed `SearchCommand::executeAsync()` not populating `pathPatterns` field in daemon request, causing server-side multi-pattern filtering to fail. The async code path (default execution) now correctly sends all include patterns to the daemon, matching the behavior of the sync path. (`src/cli/commands/search_command.cpp:1360-1365`)
- **Database Schema Compatibility**: Fixed "constraint failed" errors during document insertion on databases with migration v12 (pre-path-indexing schema). The `insertDocument()` function now conditionally builds INSERT statements based on the `hasPathIndexing_` flag, supporting both legacy (13-column) and modern (17-column with path indexing) schemas. This allows YAMS to work correctly regardless of whether migration v13 has been applied. (`src/metadata/metadata_repository.cpp:318-380`)
- **MCP Protocol Version Negotiation**: Fixed "Unsupported protocol version requested by client" error (code -32901) by making protocol version negotiation permissive by default (`strictProtocol_ = false`). The server now gracefully accepts any protocol version requested by clients, falling back to the latest supported version (`2025-03-26`) if the requested version is not in the supported list. Also added intermediate MCP protocol versions (`2024-12-05`, `2025-01-15`) to the supported list. This ensures maximum compatibility with MCP clients regardless of which spec version they implement. (`src/mcp/mcp_server.cpp:560,1254-1260`)

