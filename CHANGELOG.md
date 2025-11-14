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

## [v0.7.8] - 2025-11-14

### Added
- **Thread Pool Consolidation**
  - **WorkCoordinator Component**: New centralized thread pool manager with Boost.Asio io_context
    - Replaces 3 separate thread pools (IngestService, PostIngestQueue, EmbeddingService)
    - Provides strand allocation for per-service ordering guarantees
    - Hardware-aware thread count (8-32 threads based on CPU cores)
  - **Search Service Parallel Post-Processing** 
    - New `ParallelPostProcessor` class for concurrent search result processing
    - Parallelizes filtering, facet generation, and highlighting when result count ≥ 100
    - Uses `std::async` to run independent operations concurrently
    - Threshold-based activation (PARALLEL_THRESHOLD = 100) avoids overhead on small result sets
    - **Performance Measured** (100 iterations):
      - 100 results: 0.06ms (~1.66M ops/sec) - sequential path
      - 500 results: 0.23ms (~2.21M ops/sec) - parallel path
      - 1000 results: 0.43ms (~2.32M ops/sec) - parallel path
      - **Speedup**: ~3.4x faster at 1000 results vs linear scaling
    - Location: `include/yams/search/parallel_post_processor.hpp`, `src/search/parallel_post_processor.cpp`
    - Integration: `search_executor.cpp` now uses ParallelPostProcessor instead of sequential processing
    - Benchmarks: `tests/benchmarks/search_benchmarks.cpp`
### Changed
- **Service Architecture Refactor**
  - **IngestService**: Converted from manual thread pool to strand-based channel polling
    - Removed `kSyncThreshold` heuristics and `compat::jthread` pool
    - New `channelPoller()` awaitable for document processing
  - **PostIngestQueue**: Converted from worker threads to strand-based pipeline
    - Removed Worker struct, thread pool, and token bucket scheduler (~200 lines)
    - Implemented awaitable pipeline: `processMetadataStage → (processKnowledgeGraphStage || processEmbeddingStage)`
    - Parallel KG and Embedding stages using `make_parallel_group`
  - **EmbeddingService**: Converted from worker threads to strand-based channel polling
    - Removed worker thread pool (~70 lines)
    - New `channelPoller()` awaitable with async timer
  - **TuningManager**: Converted from manual thread to strand-based periodic execution
    - Removed `compat::jthread` with stop_token
    - New `tuningLoop()` awaitable with `boost::asio::steady_timer`
    - Uses WorkCoordinator strand for pool size adjustments
    - Maintains `TuneAdvisor::statusTickMs()` polling interval
  - **DaemonMetrics**: Converted from manual thread to strand-based polling loop
    - Removed `std::thread` for CPU/memory metrics collection
    - New `pollingLoop()` awaitable with 250ms timer interval
    - Uses WorkCoordinator strand for metric updates
    - Thread-safe snapshot access via `shared_mutex`
  - **BackgroundTaskManager**: Migrated from GlobalIOContext to WorkCoordinator
    - Removed fallback to GlobalIOContext (proper architectural separation)
    - Now uses WorkCoordinator executor for all background tasks
    - Integrated with unified work-stealing thread pool
    - Fts5Job consumer polling delay reduced: 200ms → 10ms (20x throughput improvement)
    - Fixed orphan scan queue overflow (was causing hundreds of dropped batches)
  - **ServiceManager**: Refactored async operations
    - Eliminated all 5 uses of `std::future`/`std::async`
    - Converted database operations to use `make_parallel_group` with timeouts
- **SearchPool Removal**
  - Deleted the unused `SearchPool` component and associated meson/build wiring
  - `ServiceManager` no longer constructs dead search infrastructure; `HybridSearchEngine`
    remains the sole search path
  - `TuneAdvisor`/`TuningManager` now derive concurrency targets directly from
    `SearchExecutor` load metrics instead of phantom pool sizes
- **Ingestion Pipeline Cleanup (PBI-004 Phase 1)**
  - **Removed `deferExtraction` Technical Debt**: Eliminated bypass mechanism that skipped full production pipeline
    - Removed `deferExtraction` field from `StoreDocumentRequest` and `AddDirectoryRequest` structs
    - Removed conditional logic in DocumentService that skipped FTS5 extraction
    - All document ingestion now uses full pipeline: metadata storage → FTS5 extraction → PostIngestQueue → (KG extraction || Embedding generation)
    - Updated IngestService to always enqueue to PostIngestQueue (removed lines setting `deferExtraction=true`)
    - Updated CLI add_command fallback paths (3 locations) to use full pipeline
    - Updated mobile bindings to remove `sync_now`-based deferral
    - Removed `--defer-extraction` and `--no-defer-extraction` flags from ingestion_throughput_bench
    - Updated test helpers (tests/common/capability.h, integration test) to use full pipeline
  - **Impact**: Benchmarks and production code now exercise identical code paths, improving test coverage and eliminating hidden complexity
- **Grep Output Update**
  - New default output format
  - Example output:
    ```
    === Results for "TaskManager" in 3 files (5 regex, 2 semantic) ===

    File: src/core/TaskManager.cpp (cpp)
       Matches: 3 (3 regex)

       Line   45: [Regex] class TaskManager {
       Line  102: [Regex] TaskManager::TaskManager() : initialized_(false) {
       Line  237: [Regex] void TaskManager::shutdown() {

    [Total: 7 matches across 3 files]
    ```
  - Location: `src/cli/commands/grep_command.cpp:531-645`
- **Grep Service Optimizations**
  - **Literal Extraction from Regex Patterns**
    - New `LiteralExtractor` utility extracts literal substrings from regex patterns
    - Enables two-phase matching: fast literal pre-filter → full regex only on candidates
    - Based on ripgrep's literal extraction strategy
  - **Boyer-Moore-Horspool (BMH) String Search**
    - Replaces `std::string::find()` with BMH algorithm for patterns ≥ 3 characters
  - **SIMD Vectorized Newline Scanning**
    - Platform-specific implementations: AVX2 (32 bytes), SSE2 (16 bytes), NEON (16 bytes)
    - Scalar fallback using optimized memchr for portability
    - Replaces byte-by-byte scanning in line boundary detection
    - Performance: 4-8x speedup on large files
  - **Parallel Candidate Filtering**
    - Pre-filters unsuitable files before worker distribution using `std::async`
    - Integrates `magic_numbers.hpp` for accurate binary detection (86 compile-time patterns)
    - Filters build artifacts (.o, .class, .pyc), libraries (.a, .so, .dll), executables, packages
    - Chunk-based parallel processing for large candidate sets (>100 files)
    - Performance: 2-4x speedup on large corpora

### Fixed
- **Cold Start Vector Index Loading**: Fixed issue where search and grep commands returned no results after daemon cold start despite having indexed documents. 
- **Search Async Path**: Fixed `SearchCommand::executeAsync()` not populating `pathPatterns` field in daemon request, causing server-side multi-pattern filtering to fail. The async code path (default execution) now correctly sends all include patterns to the daemon, matching the behavior of the sync path. (`src/cli/commands/search_command.cpp:1360-1365`)
- **Database Schema Compatibility**: Fixed "constraint failed" errors during document insertion on databases with migration v12 (pre-path-indexing schema). The `insertDocument()` function now conditionally builds INSERT statements based on the `hasPathIndexing_` flag, supporting both legacy (13-column) and modern (17-column with path indexing) schemas. This allows YAMS to work correctly regardless of whether migration v13 has been applied. (`src/metadata/metadata_repository.cpp:318-380`)
- **MCP Protocol Version Negotiation**: Fixed "Unsupported protocol version requested by client" error (code -32901) by making protocol version negotiation permissive by default (`strictProtocol_ = false`). The server now gracefully accepts any protocol version requested by clients, falling back to the latest supported version (`2025-03-26`) if the requested version is not in the supported list. Also added intermediate MCP protocol versions (`2024-12-05`, `2025-01-15`) to the supported list. This ensures maximum compatibility with MCP clients regardless of which spec version they implement. (`src/mcp/mcp_server.cpp:560,1254-1260`)
- **MCP Large Response Buffering**: Fixed "Error: MPC -32602: Error: End of file" errors when MCP server sends large responses (list, search, grep with many results). Implemented chunked buffered output in `StdioTransport::sendFramedSerialized()` that breaks payloads >512KB into 64KB chunks with explicit flushes between chunks. This prevents stdout buffer overflow and ensures reliable delivery of large JSON-RPC responses over stdio transport. Also added threshold-based routing in `MCPServer::sendResponse()` to use buffered sending for payloads >256KB. (`src/mcp/mcp_server.cpp:69-95,169-203`)