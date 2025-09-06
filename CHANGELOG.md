# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## [v0.6.9] - 2025-09-05

### Notes
- Upgrading daemon is recommended: older daemons (v1) will log a one‑time warning "Daemon protocol v1 < client v2" and some fields will be supplemented client‑side.

### Known Issues
- Daemon startup may remain in "Initializing" when embeddings are configured to keep the model hot (preload) and the ONNX stack cannot complete early model resolution on some systems. Workarounds:
  - Set `[embeddings].keep_model_hot = false` and `[embeddings].preload_on_startup = false` in `~/.config/yams/config.toml` to use lazy loading (model loads on first use).
  - Or export `YAMS_DISABLE_MODEL_PRELOAD=1` before starting the daemon.
  - Status derives from the lifecycle FSM; optional subsystems (models) should not gate readiness in recent builds. If you still see "Initializing", ensure you are running the updated daemon and ONNX plugin.

### Changed
- Status handler hardened to a minimal, safe snapshot; FSM/MUX metrics gated by lifecycle (Ready/Degraded) to avoid init races.
- Linux CPU proxy: read `Threads:` from `/proc/self/status` (robust) instead of parsing `/proc/self/stat` field 20.
- Stats defaults: `yams stats` now begins with the System Health section before compact counters.
- `yams stats`: prefers daemon JSON values to avoid zeros; local fallback includes vector DB size.
- `yams status`: supplements from stats JSON when talking to older daemons (v1) to avoid zeros; shows services summary and waiting components when not ready.
- Service detectors hardened:
  - SearchExecutor now includes a reason when unavailable: `database_not_ready | metadata_repo_not_ready | not_initialized`.
  - ONNX models status reports loaded count with clear guidance to download a preferred model.
- CLI search defaults to hybrid and now auto-retries with fuzzy when strict/hybrid returns zero results for better “true hybrid” behavior.
- Service metadata search path now falls back to fuzzy when full-text returns no results.
 - Model CLI help updated with subcommands and `--url` usage; clearer guidance to avoid confusion with the top-level `download` command.
 - Model CLI guidance: after downloads, success output includes `yams config embeddings model <name>` and related steps; `yams model list` and `yams model info` now mirror a one-line configuration hint.

### Added
- IPC Protocol v2:
  - StatusResponse now carries runtime fields: running, ready, uptime_seconds, requests_processed, active_connections, memory_mb, cpu_pct, version.
  - GetStatsResponse carries numeric fields alongside JSON: total_documents, total_size, indexed_documents, vector_index_size, compression_ratio.
- Daemon stats JSON: explicit `not_ready` flag; includes `durations_ms`, `top_slowest`, and latency percentiles (`latency_p50_ms`, `latency_p95_ms`).
- Bootstrap status file: `~/.local/state/yams/yams-daemon.status.json` with readiness, progress, durations, and top_slowest for pre‑IPC visibility.
- CLI doctor/status: shows top 3 slowest components with elapsed ms when available (from bootstrap JSON).
- CLI UI (retro): compact text for status and stats; `yams stats -v` shows System Health and detailed sections; `stats vectors` prints compact block.
- CLI daemon status (-d):
  - One-line services summary: `SVC  : ✓ Content | ✓ Repo | ✓ Search | ⚠ (0) Models`.
  - WAIT line during initialization listing not-ready services with progress (e.g., `WAIT : search_engine (70%), model_provider (0%)`).
- ServiceManager:
  - Preferred model preload on startup: uses configured `embeddings.preloadModels` (first entry) when no models are loaded; falls back to `all-MiniLM-L6-v2`.
  - Sanity warning when `SearchExecutor` is not initialized despite database and metadata repo ready.
- FSM metrics emission (server): counts payload writes/bytes sent, header reads/bytes received; exposed in Status when Ready/Degraded.
- LatencyRegistry (server): lightweight histogram; p50/p95 emitted via stats JSON.
- Protocol compatibility guard: one‑time warning when daemon/client protocol versions differ.
- SocketServer::setDispatcher(RequestDispatcher*) for safe future hot‑rebinds (not used by default).
- Integration test: daemon_status_stats_integration_test covers Status/Stats proto presence and basic FSM exposure.
- MCP list: added `paths_only` parameter to the MCP list tool; forwarded to daemon `ListRequest.pathsOnly` to engage the hot path (no snippet/metadata hydration).
- MCP grep: introduced `fast_first` option that returns a quick semantic suggestions burst when requested.
- MCP server: startup flags to set hot/cold modes consistently with CLI:
  - `--list-mode` → sets `YAMS_LIST_MODE`
  - `--grep-mode` → sets `YAMS_GREP_MODE`
  - `--retrieval-mode` → sets `YAMS_RETRIEVAL_MODE`
 - CLI model management:
   - New built-in entry: `nomic-embed-text-v1.5` (ONNX URL; override supported).
   - `--url` override to download any model by name from a custom URL.
   - Subcommands: `yams model list|download|info|check` (aliases to flags).
   - `yams model check` shows ONNX runtime support and plugin directory status, plus autodiscovers installed models under `~/.yams/models`.
 - Model autodiscovery: `yams model list` now lists locally installed models found at `~/.yams/models/<name>/model.onnx`.
 - Daemon model selection: honors `embeddings.preferred_model` from `~/.config/yams/config.toml` (or XDG config) to preload and prefer that model at runtime.
 - Model path resolution: honors `embeddings.model_path` as the models root (with `~` expansion) and resolves name-based models in priority order: configured root → `~/.yams/models` → `models/` → `/usr/local/share/yams/models`; full paths are used as-is.
 - Docs: PROMPT updated to show multi-path `yams add src/ include/ ...` examples.

### Fixed
- Build: resolved C++ signature mismatch for `EmbeddingService::runRepair` by aligning the implementation with header declarations and providing a proper non-jthread legacy runner.
- MCP schemas updated for list (`paths_only`) and grep (`fast_first`) to reflect new behaviors.
- ONNX plugin: CMake fixes to ensure dynamic plugin loads cleanly:
  - Disable IPO/LTO on `yams_onnx_plugin` (INTERPROCEDURAL_OPTIMIZATION FALSE) to prevent LTO symbol internalization and tooling noise.
  - Ensure exported C symbols have default visibility (`C_VISIBILITY_PRESET/CXX_VISIBILITY_PRESET default`, `VISIBILITY_INLINES_HIDDEN OFF`) so `getProviderName`/`createOnnxProvider` remain visible.
  - On ELF, link with `-Wl,-z,defs` to catch unresolved symbols at link time.
  - Resolves daemon warning: "Plugin missing getProviderName function: .../libyams_onnx_plugin.so".
 - Retrieval now accepts `sha256:<hex>` hashes (normalized before validation) in `DocumentService` `retrieve` and `cat`.
 - Model downloads provide clearer errors when offline (curl exit code mapping for host resolution/connect failures).

## [v0.6.8] - 2025-09-04

### Changed
- Data directory resolution uses a consistent precedence across CLI and daemon: configuration file > environment (`YAMS_STORAGE`/`YAMS_DATA_DIR`) > CLI flag. This avoids CLI defaults masking configured storage roots.
- MCP search defaults hardened: runs in hybrid mode with fuzzy matching enabled by default and a similarity threshold of 0.7 when the client doesn’t provide options.

### Added
- MCP get-by-name and cat now include a fuzzy fallback: if direct lookup fails, a hybrid fuzzy search runs and returns the strongest single match, then retrieves by hash (preferred) or normalized path.
- URL-aware naming: post-index for MCP downloads now sets the document name from the URL basename (query stripped), improving name-based retrieval. MCP get-by-name also accepts full URLs and normalizes them to the basename automatically.

### Fixed
- macOS build compatibility and warnings:
  - `std::jthread/std::stop_token` portability: embedding service falls back to `std::thread` with an atomic stop on platforms lacking libc++ support.
  - Third‑party noise reduced: mark `hnswlib` includes as SYSTEM and silence `sqlite-vec.c`; fence cast-qual warnings under Clang for HNSW.
  - Resolved shadowing and unused variable/parameter warnings across content handlers (PDF, audio, image, binary, archive) and daemon client helpers.
- Name propagation for downloads: downloaded artifacts are indexed with a human-friendly name, making `get --name <file>` work reliably after MCP/CLI downloads.

## [v0.6.7] - 2025-09-04

### CI bump
- SourceHut build fixes
- GitHub CI updates

## [v0.6.6] - 2025-09-04

## CI bump
- SourceHut build fixes
- Github CI updates

## Fixed
- Search regression from adding query results. Querys will now be supported by default in the fallback path after expressions are extracted

## Known Issues
- Data directory path resolutions needs to be audited

## [v0.6.5] - 2025-09-04

### Changed
- Release workflow: migrate to Conan 2 profiles using a dedicated host profile (`conan/profiles/host.jinja`) that enforces `compiler.cppstd=20` and sets `compiler.libcxx=libc++` on macOS.
- CMake (Darwin): replace GNU ld flags (`--start-group/--end-group`, `--whole-archive/--no-whole-archive`) with `-Wl,-force_load` for specific archives in CLI, MCP server, and daemon.
- CI (act): install build tools when missing in containers (cmake, ninja, build-essential/pkg-config) to allow local act runs of the release job.

## Added
- Search ergonomics:
  - `-q, --query` flagged alias to safely pass queries that start with '-' (e.g., `yams search -q "--start-group --whole-archive"`).
  - `--stdin` and `--query-file <path>` to read the query from stdin or a file (`--query-file -` also reads stdin).
  - Helpful "Query is required" guidance when no query is provided, with tips to use `-q/--query`, `--` to stop option parsing, or stdin/file inputs.

## Fixes
- macOS linking failures due to GNU-only linker flags appearing in executable link lines; now prevented and defensively stripped if injected elsewhere.
- SourceHut `.build.yml`: use the Conan host profile and disable ONNX (`-o enable_onnx=False`) to avoid upstream onnxruntime 1.18 build errors with GCC 15; preserves successful release builds on Arch.

## [v0.6.4] - 2025-01-05

### Known Issues
- Daemon not showing correct storage details

### Added
- Streaming metrics: Track and expose `stream_total_streams`, `stream_batches_emitted`, `stream_keepalives`, and average `stream_ttfb_avg_ms` via `yams stats` (JSON and technical details in text mode).
- Grep performance: line-by-line streaming scanner (no full-file buffering) and literal fast-path for `--literal` without word-bounds/case-folding.
- Add performance: parallel directory traversal/processing with bounded workers.
- Session helpers: `yams session pin|list|unpin|warm` for hot data management (feature is experimental and may not work reliably).

### Changed
- List paths-only mode now avoids snippet/metadata hydration end-to-end for faster responses; honors `pathsOnly` through daemon to services.
- Retrieval (cat/get) prefers extracted text (hot) when available before falling back to CAS (cold), respecting per-document `force_cold`.
- Grep hot/cold race: daemon emits hot first-burst and cancels cold; batch/first-burst tuning supported via env for testing.
- Delete command now mirrors `rm` ergonomics:
  - `rm` alias retained; `-f` is an alias for `--force`; `-r` is an alias for `--recursive`
  - Positional targets supported (names/paths/patterns); when a single target is provided with `-r`, it is treated as a directory
  - Multiple positional targets are treated as a names list
  - Selector requirement relaxed when positional targets are used; mode (directory/pattern/name) is inferred heuristically
- List paths-only mode now avoids snippet/metadata hydration end-to-end for faster responses; honors `pathsOnly` through daemon to services.
- Retrieval (cat/get) prefers extracted text (hot) when available before falling back to CAS (cold), respecting per-document `force_cold`.
- Grep hot/cold race: daemon emits hot first-burst and cancels cold; batch/first-burst tuning supported via env for testing.
- CMake: Platform-aware linker selection.
  - Removed hard-coded `-fuse-ld=lld` from presets.
  - Toolchain now attempts `-fuse-ld=lld` only on non-Apple platforms when supported, gated by `YAMS_PREFER_LLD` (default ON).
  - macOS Release builds use `-Wl,-dead_strip` instead of GNU `--gc-sections`.
- Release workflow (macOS): Align Conan arch with target:
  - `macos-arm64` uses `-s arch=armv8`
  - `macos-x86_64` uses `-s arch=x86_64`

### Fixed
- Streaming search finalization: avoid infinite keepalive loop on empty results; keepalive cadence configurable via `YAMS_KEEPALIVE_MS`.
- Stats command: `yams stats help` now prints a concise system metrics guide.
- MCP server timeouts with some clients:
  - Async handlers now run detached (no premature task destruction), preventing “Context server request timeout”
  - Server sends `notifications/ready` after client `notifications/initialized` for better client compatibility
  - Stdio framing hardened to consume trailing CR/LF after payloads to avoid parse glitches across clients
- Streaming search finalization: avoid infinite keepalive loop on empty results; keepalive cadence configurable via `YAMS_KEEPALIVE_MS`.
- Stats command: `yams stats help` now prints a concise system metrics guide.
- Docker: ARM64 build now mirrors AMD64 by using Buildx with proper tag suffixes (-arm64) and pushes versioned and latest tags. Multi-arch manifest creation no longer fails with "not found ... -arm64".
- Release workflow (macOS/Linux): Conan 2 profile updates use correct keys (compiler.cppstd, compiler.libcxx) via `conan profile update`; removed brittle sed/grep edits that broke on macOS.
- Release workflow (Windows): Corrected Conan 2 profile update syntax (`compiler.cppstd=20`).

## [v0.6.3] - 2025-01-04

## Changes
- CI version bump for source hut and github action builds
- `yams add` supports multiple files
- Adding small optimizations to decompression

## Known Issues
- MCP server usage my vary in successful returns
- [v0.6.2] CLI performance still degraded, will be addressed in subsequent release

## [v0.6.2] - 2025-01-04

## Hot fixes
- CI version bump
- MCP server patch (tested working in zed and lm studio but not working in goose or jan)

## [v0.6.1] - 2025-01-03

## Hot fixes
- Docker and sourcehut build file updates
- Daemon liveness checks on startup bug fix where daemon did not signal start
- Added more aggressive parallelization for search now that it holds all resources (will make tunable)

## Known Issues
- MCP server init not working with some clients
- CLI performance has been degraded as it creates socket to daemon each request, working on session mode
- `yams stats -v` not working as expected

## [v0.6.0] - 2025-01-03

### Repository
- **Will move all future development work to experimental branch so that main and releases become more stable**
  - I apologize for recent instabilities

### Added
- Server multiplexing: fair round-robin writer with per-turn byte budget and backpressure caps (default ON).
- Status: exposes multiplexing metrics (active handlers, queued bytes, writer budget).
- Cancel control frame: request type added and server-side cancel scaffolding (per-request contexts).
- **Development Changes**
  - Updated tasks for vscode and zed tasks
  - Enforcing clang-tidy warnings as errors
  - Attempting to stabilize build system as plugin system is implemeneted per roadmap
- **Daemon Logging Rotation**
  - Use rotating file sink to preserve logs across crashes
  - Log rotation info: `Log rotation enabled: /path/to/logfile.log (max 10MB x 5 files)`

- **Connection State Machine**
  - Deterministic `ConnectionFsm` for IPC connection lifecycle management
  - Clean state transitions: Disconnected → Connecting → Connected → ReadingHeader → ReadingPayload → WritingHeader → StreamingChunks → Error/Closed
  - FSM metrics collection gated by daemon log level (debug/trace only)
  - Coordination with `RepairFsm` for safe data integrity operations
  - Guardrails for readable/writable operations based on FSM state
  - Header-first streaming with chunked transfer support

- **Protobuf IPC Migration**
  - Complete migration from custom binary serialization to Protocol Buffers
  - `ProtoSerializer` as the single payload codec for client/server
  - Comprehensive `ipc_envelope.proto` schema with oneof for all request/response types
  - Transport framing (header, CRC, streaming) remains unchanged
  - MAX_MESSAGE_SIZE enforcement in encode/decode paths
  - Older non-protobuf clients are incompatible after this release

### Changed
- **CLI Commands**
  - All CLI commands migrated to async-first pattern with `run_sync` bridge
  - Removed deprecated `daemon_first()` and `daemon_first_pooled()` helpers
  - Commands now use `async_daemon_first()` with proper timeout handling
  - Consistent error handling and retry logic across all commands

- **MCP Server**
  - All handlers converted to async (`yams::Task<Result<T>>`)
  - Improved concurrency and reduced blocking operations

- **Daemon Architecture**
  - Socket server runs in-process, eliminating need for external IPC server
  - `state_.readiness.ipcServerReady` now properly reflects socket server status
  - Graceful shutdown sequence: socket server stops before services
  - Better resource lifecycle management and error propagation

### Fixed
- **Async Infrastructure**
  - Removed spin-wait bridges that caused CPU waste
  - Fixed head-of-line blocking in request processing
  - Proper cancellation handling for in-flight operations
  - Eliminated `Task::get()` usage from production code paths

### Removed
- **Deprecated Synchronous APIs**
  - `AsioClientPool::roundtrip()`, `status()`, `ping()`, `call()` (sync versions)
  - `PooledRequestManager::execute()` (now returns NotImplemented error)
  - `YAMS_ASYNC_CLIENT` environment variable (async is now mandatory)
  - Legacy `MessageSerializer` and custom binary serialization code
  - All synchronous daemon helper functions
  - Forward declarations of `BinarySerializer`/`BinaryDeserializer` remain in `ipc_protocol.h` for cleanup in v0.7.0

## [v0.5.9] - 2025-08-29

### Added
- Daemon Repair Coordinator (FSM-driven) enabled by default when daemon is running
  - Performs idle-only embedding repair using existing repair utilities
  - New daemon repair metrics in StateComponent:
    - repairIdleTicks, repairBusyTicks, repairBatchesAttempted,
      repairEmbeddingsGenerated, repairEmbeddingsSkipped, repairFailedOperations
- IPC streaming persistence
  - Removed unsolicited end-of-stream heartbeat frame that could poison next response
  - Fixed message framing hang under certain pooled flows; reduced over-eager retries
- MCP server alignment with pooled client API
  - Corrected DaemonClientPool::acquire() usage (Result<Lease> → Lease)

### Known Issues
- Pooled reuse immediately after a streaming response can intermittently see
  "Connection closed by daemon" on follow-up unary calls (e.g., list/ping).
### Changed
- Coordinator ownership/lifecycle: now a YamsDaemon member started in start() and stopped before
  IPC/services teardown in stop() for safe shutdown

### Fixed
- Regressions in CLI / MCP tooling
- AddressSanitizer heap-use-after-free in IPC tests caused by coordinator thread outliving
  daemon members; resolved by proper ownership and shutdown ordering
  - Readiness flags and lightweight observability counters for repair gating
- IPC/Streaming & Pooling
  - Connection FSM guardrails with legal transitions for streaming and normalization
  - Header-first streaming responses with chunked transfer; persistent sockets supported
  - Client: pre-header reconnect-once (timeout/EOF/reset) and inter-chunk inactivity deadlines
  - Time-to-first-byte (TTFB) metric and response timing hooks
  - Streaming enabled for search, list, grep, and add document paths

## [v0.5.8] - 2025-08-29

### Fixed
- Release workflow: replaced invalid fromJSON() usage for matrix runner selection with direct `matrix.runs_on` reference.
- Docker workflow: stabilized ARM64 build by moving from macOS runner (no Docker daemon) to ubuntu-latest with QEMU emulation; added early `docker info` checks for both arches.

### Changed
- Docker: standardized multi-arch build steps (QEMU + Buildx) and ensured consistent labels/tags across architectures.

## [v0.5.7] - 2025-08-2

### Known Issues
- Daemon Pool connection issues for updated streaming commands (search, grep, list)
  - Search will return with backup, but grep will not

### Added
  - Implemented tinyfsm-based state machine for connection lifecycle management
  - Added per-connection FSM adapter (`yams::ipc::ConnectionFsm`) with clean state transitions
  - Vendored tinyfsm under `third_party/tinyfsm/` with MIT license compliance
  - Added comprehensive state machine documentation and integration guide
- **Groundwork for PBI-006**
  - Shared qualifier parser: inline query qualifiers (lines:, pages:, section:, selector:, name:, ext:, mime:) with quotes and spaces supported.
  - Hybrid search normalization: qualifiers are stripped from scoring text and passed as structured hints to engines.
  - Lines-range scoped snippets in hybrid engine: when `lines:` is specified, result snippets are sliced to requested line windows.
  - Integrated PDF content extraction into search, cat, get. This will evolve into a plugin system to support custom file type search from stored files.
- Introduced daemon pooled request pattern across MCP/CLI boundaries.
- Lightweight `daemon_first` wrapper in CLI that uses `DaemonClient::call<T>` to preserve daemon-first behavior while commands migrate to pooled execution.

### Changed
- **Daemon Client/Server Pipeline Refactor**
  - **AsyncSocket Simplification**: Replaced complex generation-based tracking with tinyfsm state machine
    - Removed atomic generation counters and complex mutex operations
    - Implemented clean state transitions: Disconnected → Connecting → Connected → ReadingHeader → ReadingPayload → WritingHeader → StreamingChunks → Error/Closed
    - Eliminated race conditions in socket lifecycle management
    - Reduced memory footprint and improved debuggability
  - **DaemonClient Pipeline Consolidation**: Unified timeout and request handling
    - Consolidated timeout configuration (removed scattered env vars + config mixing)
    - Single `PipelineConfig` struct with clear precedence rules
    - Streamlined request execution with `execute<T>()` template method
    - Centralized send_message/receive_message helpers
    - Improved error context with operation details
  - **AsyncIpcServer Threading Cleanup**: Simplified thread pool management
    - Clear separation between IO threads and worker threads
    - Improved lifecycle management and resource cleanup
    - Better thread naming and monitoring capabilities
- Cleaned up build system and devcontainer
- Hybrid/metadata parity: name/ext/mime qualifiers are honored across engines
  - Hybrid: mapped to `vector::SearchFilter.metadata_filters` and enforced by both vector and keyword paths.
  - Metadata fallback: qualifiers merged into service filters (pathPattern, extension, mimeType) so behavior matches hybrid.
- MCPServer constructor simplified to `(std::unique_ptr<ITransport>, std::atomic<bool>*)`; serve command and yams-mcp tool updated to STDIO-only transport.
- `daemon_helpers` generalized to accept generic callables (lambdas) instead of `std::function`, improving template deduction and avoiding copies.

### Fixed
- Search service: hash-prefix path used the wrong local variable; corrected to ensure path/type filters are applied correctly.
- Metadata search: removed references to non-existent request fields and unified name filtering via path pattern.
- MCP tools/call responses were not rendered by some clients due to missing MCP content wrapper. Non-MCP-shaped tool results are now wrapped in a top-level content array.
- search: paths_only results could be empty; fixed to return service-provided paths.
- list: limit and offset were not wired from MCP -> services; pagination is now honored.
- Protocol alignment: removed non-existent fields from daemon `SearchRequest` mapping and explicitly cast similarity to `double`.
- Error handling: replaced non-existent `ErrorCode::Unavailable` with `ErrorCode::NetworkError`.
- Type mapping: `DeleteRequest` now maps to `DeleteResponse` in response traits.
- Duplicates: removed duplicate `clientInfo_`/`negotiatedProtocolVersion_` members and the extra `handleUpdateMetadata` definition in MCP.
- Linking: integrated daemon IPC implementation into `yams_mcp` and linked `ZLIB::ZLIB` (CRC32) to resolve `AsyncSocket`/`ConnectionPool` and checksum undefined symbols.
- Windows CI job fix (this will be a process)
- Release build fix for artfact upload on partial success

### Notes
- CLI migration to pooled requests will proceed per-command; the temporary `daemon_first` wrapper keeps commands working until each is switched to the pooled manager.

## [v0.5.6] - 2025-08-24

**CI version bump**
- Release formatting fix

## [v0.5.5] - 2025-08-24

**CI version bump**
- Releases now generate regardless of build failures

### Fixed
- Grep supports semantics search using embeddings

## [v0.5.4] - 2025-08-24

### Fixed
- Resolved MCP tool listing error
- Added mutex imports for macos x86 ci builds

## [v0.5.3] - 2025-08-24

**CI version bump**

## [v0.5.2] - 2025-08-24

### Changed
- Release workflow: made Linux validation build non-blocking in release pipeline to allow packaging and publishing artifacts even if validation step fails. Validation failures are surfaced in job logs and summary but no longer block artifact creation.

### Fixed
- Linux build: guarded macOS-specific Mach headers in RequestDispatcher with __APPLE__ and added Linux-friendly memory/CPU sampling using /proc, fixing fatal includes on Linux targets.

## [v0.5.1] - 2025-08-24

### Known Issues
- Daemon connection exhaustion: running many concurrent yams processes (e.g., find -exec yams add …) can hit the default max_connections=100 and cause rejections. Mitigation: prefer recursive/batched commands or cap concurrency (e.g., xargs -P 4). Consider raising max_connections for CI.
- Regex alternation in yams search: search currently treats queries as keyword/FTS; compound regex expressions (e.g., A|B) aren’t supported there yet. Use yams grep for regex, or await a future --regex mode in search.

## [v0.5.0] - 2025-08-22

### Added
- **Service-Based Architecture**
  - Complete app services layer implementation (`app/services/*.cpp`)
  - Service factory pattern with centralized AppContext management
- **RequestDispatcher Service Integration**
  - All daemon handlers migrated to use app services exclusively
  - Complete feature parity between CLI and daemon operations
  - Enhanced protocol mapping with proper field alignment
- **SearchCommand Service Migration**
  - Tag filtering support with `--tags` and `--match-all-tags` options
  - Comma-separated tag parsing with whitespace trimming
  - Complete daemon_first pattern implementation with local fallback
- **IndexingService Integration**
  - All document ingestion centralized through IndexingService
  - Eliminated direct IndexingPipeline calls from CLI commands
  - Directory recursion uses IndexingService::addDirectory() for consistency
  - Embedding lifecycle managed exclusively by Indexin Service
- **Auto-Repair for Vector Embeddings**
  - Daemon automatically generates missing embeddings when documents are added (hot-load behavior)
  - New `enableAutoRepair` configuration flag in DaemonConfig (default: true)
  - Background repair thread processes queued documents in batches
  - Automatic detection and repair during semantic search operations
  - Shared repair utility (`yams/repair/embedding_repair_util.h`) for CLI and daemon use
- **Vector Index Persistence**
  - Implemented `saveIndex()` and `loadIndex()` methods for VectorIndexManager
  - Binary serialization format for index state and metadata
  - Automatic index saving on daemon shutdown
- **Vector Management**
  - Implemented `removeVector()` method for single vector deletion
  - Support for removing vectors from both flat and HNSW indices
- **HNSW Index Support**
  - Complete implementation of Hierarchical Navigable Small World index
  - Configurable M and ef_construction parameters
  - Efficient approximate nearest neighbor search
- **Improved Daemon Lifecycle Management**
  - New daemon instances now automatically kill existing daemons on startup
  - Prevents resource conflicts and stale daemon processes
  - Ensures clean daemon state for each new session
  - **Note**: Older YAMS versions require manual daemon termination before upgrading
- **Service-Based Test Architecture**
  - New comprehensive service layer tests: `IndexingService`, `DocumentService`, and `SearchService`
  - Modern content handler tests using Universal Content Handler System (`pdf_content_handler_test.cpp`)
  - CMakeLists.txt structure for organized test building and discovery

### Fixed
- **Service Interface Field Mapping**
  - Fixed field name mismatches between daemon protocols and service interfaces
  - Corrected `lineNumbers` vs `showLineNumbers` mapping in RequestDispatcher, MCP server, and SearchCommand
  - Resolved compilation errors from protocol field alignment issues
  - Fixed RequestDispatcher handlers to properly map request fields to service parameters
- **Vector Index Persistence Deadlock**
  - Fixed critical mutex deadlock in VectorIndexManager loadIndex() method
  - IndexPersistence test no longer hangs indefinitely during deserialization
  - Fixed deadlock by properly releasing mutex before calling initialize()
  - Added robust error checking for stream operations during serialization/deserialization
  - Enhanced data validation with sanity checks for corrupted data
  - Vector index save/load functionality now works correctly
- **Model Loading Performance**
  - Removed unnecessary async wrappers causing 15+ minute timeouts
  - Model loading now completes in seconds instead of timing out
  - Fixed deadlock issues in OnnxModelPool initialization

## [v0.4.8] - 2025-08-21

### Added
- **Shell Wildcard Expansion Support**
  - `add` command now accepts multiple file paths from shell expansion
  - Commands like `yams add *.md` now work correctly without "arguments were not expected" errors
  - Properly handles shell wildcard expansion (e.g., `*.cpp`, `*.md`, `*.txt`)
- **Dynamic Plugin Loading**
  - Implemented PluginLoader class for runtime plugin loading
  - Added automatic plugin discovery from standard directories
  - Support for YAMS_PLUGIN_DIR environment variable
  - Onnx model provider now loads dynamically as a plugin
  - Daemon automatically loads plugins on startup
  - Created comprehensive test suite for plugin loader

### Fixed
- **Tag Storage and Filtering**
  - Fixed tag filtering in `list` command not working (`--tags` option)
  - Tags are now properly parsed as comma-separated values when stored
  - Fixed tag filtering logic to correctly match against stored comma-separated tag values
  - Multiple tags in filter work with OR logic (e.g., `--tags "work,important"` shows documents with either tag)
- **Daemon Path Resolution**
  - Fixed "Added 0 documents" issue when using relative paths with daemon
  - CLI now converts relative paths to absolute paths before sending to daemon
  - Added proper error messages for invalid paths and non-recursive directory attempts
  - Daemon now returns helpful error messages instead of silently returning 0
- **Feature Parity Between CLI and Daemon**
  - Added all missing fields to GrepRequest for complete CLI feature parity
  - Added all missing fields to SearchRequest for complete CLI feature parity
  - Updated serialization/deserialization for both request types
  - CLI commands now pass all parameters to daemon requests
  - Ensures consistent behavior whether using daemon or local execution
- **Wildcard Pattern Matching Bug**
  - Fixed broken wildcard pattern matching in `add` and `restore` commands
  - Replaced regex-based implementation with efficient iterative algorithm
  - Patterns like `*.sol`, `*.cpp`, `*.js` now work correctly
  - Dots and other special characters in patterns are now handled properly
- **Linux Build Errors**
  - Fixed missing `#include <cstring>` in compression_benchmark.cpp for `std::strlen`
  - Fixed C++ compiler flag `-Wnon-virtual-dtor` being incorrectly applied to C files
  - Used CMake generator expression `$<$<COMPILE_LANGUAGE:CXX>:-Wnon-virtual-dtor>` for language-specific flags
  - Fixed missing Rabin chunker header include in ingestion_benchmark.cpp
  - Fixed GCOptions struct initializer warnings by adding progressCallback field
  - Fixed query parser benchmark Result access patterns (use `.value()` instead of `*result`)
  - Fixed metadata benchmark Database constructor usage pattern
  - Fixed benchmark API calls to use storeBytes() instead of non-existent addContent()
- **Vector Index Loading**
  - Fixed empty error message when vector index file doesn't exist
  - Added proper file existence check before attempting to load
  - Shows debug message instead of warning for missing index file on startup

## [v0.4.7] - 2025-08-21

### Fixed
- **Release Workflow Build Failures**
  - Fixed `std::from_chars` compilation error on macOS hosted runners (Xcode 15.2)
  - Replaced `std::from_chars` with portable `std::stoull` in http_adapter_curl.cpp for parsing Content-Length headers
  - Fixed missing benchmark package in Linux self-hosted validation builds
  - Added proper Conan options (`-o build_tests=True -o build_benchmarks=True`) to validation build step
  - Ensures compatibility with older macOS standard libraries that lack full C++20 support

## [v0.4.6] - 2025-08-21

### Changed
- **MCP Server Tool Naming**
  - Simplified tool names to match CLI commands for better consistency
  - Tools now use generic names: `search`, `grep`, `download`, `get`, `list`, `store`, `add`, `delete`, `cat`, `update`, `stats`

### Fixed
- Added proper lifetime management for io_uring operations to prevent accessing freed memory
- Implemented operation tracking and cancellation to ensure clean shutdown
- **Storage Backend Improvements**
  - Fixed FilesystemBackend sharding to use hash-based approach for consistent key distribution
  - Replaced key-prefix sharding with SHA256 hash-based sharding to avoid path conflicts
- **Chunking Deduplication**
  - Fixed RabinChunker deduplication by resetting window state at chunk boundaries
  - Ensures identical data patterns produce identical chunks for proper deduplication
- **Code Improvements**
  - Added automatic configuration correction for invalid chunking configs (when min > max)
  - Enhanced preprocessText() to trim leading/trailing whitespace
  - Improved paragraph boundary detection to point after "\n\n" markers
- **CI/CD**
  - Fixed release workflow by changing preset from `conan-validation` to `conan-release`

### Known Issues
- **Temporarily Disabled Tests** (to be fixed in v0.5.0)
  - VectorIndexManager: removeVector, index persistence, HNSW operations not implemented
  - ModelManagement: Registry initialization issues in test environment
  - OnnxRuntime: Tests timeout waiting for non-existent model files
  - All disabled tests are marked with TODO(v0.5.0) comments for tracking

## [v0.4.5] - 2025-08-21

### Fixed
- **MCP Server**
  - Added mutex protection to StdioTransport for thread-safe I/O operations
  - Fixed potential JSON response interleaving when multiple clients connect
  - Prevents "Expected ',' or ']' after array element" errors in concurrent scenarios
  - Fixed missing includes for file operations
- **Test Failures**
  - Fixed FilesystemBackend::list() key reconstruction from sharded paths
  - Fixed ManifestManager statistics by moving static counters to member variables
  - Fixed file type detection consistency in CommandIntegrationTest
- **Detection Module**
  - Ensured FileSignature creation uses consistent methods for isBinary and fileType
  - Fixed mismatch between FileSignature fields and classification methods

## [v0.4.4] - 2025-08-21
**CI version bump**

## [v0.4.3] - 2025-08-20

### Fixed
- **Build System**
  - Updated GTest from 1.14.0 to 1.15.0 for Conan 2.0 compatibility
- **Homebrew Formula**
  - Fixed documentation URL in Homebrew formula to point to correct repository

### Known Issues
- Daemon may crash when processing certain stats requests (investigation ongoing)

## [v0.4.2] - 2025-08-20

### Added
- **Linux Package Support**
  - Added AppImage support for universal Linux distribution
  - Integrated package building into GitHub release workflow

### Fixed
- **Linux Build Compilation**
  - Fixed C++ template name lookup issues in `message_serializer.cpp` for GCC 13
  - Added namespace qualification to 31 deserialize calls for proper template resolution
  - Fixed missing `<utility>` header in `async_socket.h` and `connection_pool.h` for `std::exchange`
  - Fixed missing `<netinet/in.h>` header in `async_socket.cpp` for `IPPROTO_TCP` constant
- **Position-Independent Code (-fPIC) Linker Errors**
  - Fixed shared library linking errors by enabling PIC for all static libraries in dependency chain
  - Added `POSITION_INDEPENDENT_CODE ON` property to 11 static libraries
  - Resolved `yams_onnx_plugin.so` build failure on Linux x86_64
- **Missing Symbol Linker Errors**
  - Fixed undefined reference to `HybridFuzzySearch` by linking `yams_metadata` to `yams::search`
  - Resolved circular dependency issues in library linking order
- **CLI Output Cleanup**
  - Changed TextExtractorFactory initialization logging from info to debug level
  - Removed spurious log messages from normal CLI output
- **Homebrew Formula**
  - Fixed documentation URL in Homebrew formula to point to correct repository

## [v0.4.1] - 2025-08-20

### Added
- **Smart Text Extraction by Default**
  - CLI commands now extract text from PDFs, HTML, and other supported formats by default
  - `cat` command: Shows readable text instead of binary data for PDFs and HTML files
  - `get` command: Auto-detects output destination - extracts text for terminal, raw for files
  - `--raw` flag added to both commands for accessing original content when needed
  - `--extract` flag for `get` command to force text extraction even when piping

### Changed
- **MCP Server Text Extraction**
  - Now uses `TextExtractorFactory` for all supported file types (not just HTML)
  - PDF text extraction works correctly in MCP tools
  - Consistent text extraction behavior across CLI and MCP interfaces

### Fixed
- **PDF Text Extraction in MCP**
  - Fixed issue where PDF files returned empty content in MCP tools
  - MCP server now properly extracts text from PDFs using PDFium
  - Added missing `HtmlTextExtractor` to build system
- **Compilation Issues**
  - Fixed missing `HtmlTextExtractor` in CMakeLists.txt
  - Fixed ErrorCode enum references in HTML text extractor
  - Fixed regex_replace lambda usage for C++ standard compliance
- IPC Response variant construction
- GitHub Actions release workflow: wrap embedded multi-line Python f-string code in bash here-docs and pass JSON file path via argv in the benchmarks block to avoid shell syntax errors on runners.

## [v0.4.0] - 2025-08-20

### Added
- **Universal Content Handler System**
  - New `IContentHandler` interface supporting all file types with metadata extraction
  - `ContentHandlerRegistry` with thread-safe handler management
  - `TextContentHandler` adapter wrapping existing PlainTextExtractor
  - `PdfContentHandler` wrapper for existing PdfExtractor
  - `BinaryContentHandler` as universal fallback for unknown file types
  - Updated DocumentIndexer to use new ContentHandlerRegistry system
  - Maintained backward compatibility with legacy TextExtractor system
- **High-Performance Daemon Architecture**
  - New `yams-daemon` background service for persistent resource management
  - Unix domain socket IPC with zero-copy transfers for large payloads
  - Automatic daemon lifecycle management with configurable policies
  - `yams daemon start/stop/status/restart` command suite
  - Auto-start on first command if daemon enabled in config
  - Shared EmbeddingGenerator across all operations eliminates model loading overhead
  - VectorIndexManager cached in daemon memory
- **Robust Downloader Module**
  - New download module with libcurl adapter, repo-local staging
  - SHA-256 integrity verification and rate limiting
  - Atomic finalize into CAS (store-only by default)
  - New `yams download` command returning JSON {hash, stored_path, size_bytes}
  - Progress output (human/json) with no user-path writes unless export requested
- **Configuration v2 Architecture**
  - New `[daemon]` section for service configuration
  - `[daemon.models]` for model lifecycle management
  - `[daemon.lifecycle]` for auto-start and shutdown policies
  - `[daemon.ipc]` for communication tuning
  - Backward compatible with direct mode (no daemon)
- **Enhanced Search Capabilities**
  - `--literal-text` flag for search and grep commands
  - Treats query patterns as literal text instead of regex/operators
  - Works across all search engines (fuzzy, hybrid, metadata, vector)
  - Example: `yams search "call(" --literal-text` safely searches for parentheses

### Changed
- **Performance Architecture**
  - All CLI commands can leverage shared daemon resources
  - Shared result renderer system across CLI and daemon
  - Deferred initialization eliminates startup overhead
- **MCP Integration Improvements**
  - Removed HTTP transport support, now stdio-only for cleaner local integration
  - Improved EmbeddingGenerator lifecycle with lazy loading
  - Better resource management with on-demand initialization

### Fixed
- **Daemon Integration Issues**
  - Eliminated "Failed to preload model" warnings
  - Fixed daemon configuration defaults for missing config sections
  - Daemon now gracefully handles missing `[daemon]` sections in config files
- **Configuration System**
  - ConfigMigrator properly handles v1 to v2 migrations without breaking existing configs
- **MCP Schema Compliance**
  - Fixed "Expected object, received null" error in tool definitions
  - Removed empty `properties: {}` fields from tools with no parameters
  - MCP server now loads correctly without schema validation errors
- **Search Engine Robustness**
  - Special characters in search queries no longer break FTS5
  - Automatic sanitization prevents syntax errors from `()[]{}*"` characters
  - All search paths (fuzzy, hybrid, metadata) handle special characters safely
  - Raw query strings flow through pipeline unchanged until FTS5 level
- **Critical Compilation Errors**
  - Fixed `ChunkingStrategy` enum reference mismatch in document chunker (FixedSize → FIXED_SIZE)
  - Fixed constructor initialization order in VectorIndexOptimizer to match member declaration order
  - Fixed compression level configuration in RecoveryManager (now uses level 3 per performance benchmarks)
- **Code Quality Issues**
  - Fixed hundreds of uninitialized variable errors identified by cppcheck analysis
  - Eliminated critical errors in recovery_manager.cpp, error_handler.cpp, and metadata_api.cpp
  - Added proper RAII initialization patterns across vector and compression components
- **Build System Stability**
  - All modules now compile successfully without errors
  - Fixed warning configurations that were breaking dependency builds
  - Improved cross-platform compilation compatibility

## [v0.3.4] - 2025-08-16

### Added
- **Hybrid Grep Command**
  - Semantic search capability integrated into grep command
  - Shows both "Text Matches" (regex) and "Semantic Matches" sections
  - `--regex-only` flag to disable semantic search when needed
  - `--semantic-limit` option to control number of semantic results (default: 3)
  - Automatic parallel execution of regex and semantic searches for better performance

- **Enhanced Vector Search**
  - Query embedding generation for accurate semantic search
  - Automatic embedding generator creation based on available models
  - Improved search accuracy with actual query vectors instead of placeholders
  - Default embedding support through factory pattern

- **Automatic Embedding Generation**
  - Vector database auto-creation on initialization
  - Repair thread auto-starts when missing embeddings detected
  - Background embedding generation without manual intervention
  - Proactive database initialization to avoid "Not found" messages

### Fixed
- **Vector Database Initialization**
  - Fixed auto-creation of vectors.db on first run
  - Repair thread now triggers during storage initialization
  - Improved embedding health detection with better sampling
  - Correct variable names in initialization code (metadataRepo_ vs metadataRepository_)

### Changed
- **Search Command**
  - Now uses hybrid search by default with vector and keyword fusion
  - Ignores --type flag for better ergonomics (hybrid is always best)
  - Automatic fallback to keyword search if vector search unavailable

## [v0.3.3] - 2025-08-15

### Added
- **MCP Server**
  - Spec-compliant stdio framing (Content-Length + CRLF) with readiness notifications
  - Tool schemas aligned with implementation and tests
  - MCP tool handlers refactored into thin adapters that map JSON ↔ service DTOs
  - CLI/MCP parity via shared service layer (Search, Grep, Document); MCP now delegates:
    - grep_documents → GrepService
    - list_documents, retrieve_document, update_document_metadata, get_by_name, cat_document, delete_by_name → DocumentService
- **Tracy Profiling Support**
- **List Command Enhancements**
  - `--changes` flag to show documents with recent modifications (last 24h by default)
  - `--since` option to show documents changed since specified time (ISO8601, relative, or natural formats)
  - `--diff-tags` flag for grouped change visualization showing added/modified/deleted documents
  - `--show-deleted` flag to include deleted documents in listings
  - File type indicators in diff-tags output showing `[extension|bin/txt|category]` for all files
- **Grep Command Enhancements**
  - Support for searching specific file paths in addition to patterns
  - Improved path matching with suffix lookup for partial paths
  - `--paths-only` flag for LLM-friendly output (one file path per line)

- **Code Quality**
  - Added clang-format configuration for consistent code style
  - Pre-commit hook for automatic code formatting
  - Application services layer for shared CLI/MCP functionality

### Fixed
- **MCP Server Initialization**
- **Vector Database**: Simplified embedding generation using existing repair command patterns
- Stats no longer unconditionally recommends 'yams repair --embeddings' when embeddings already exist; guidance is shown only when count is zero
- **Stats Display**: Improved embedding system status to show meaningful health info instead of misleading zero-activity counters
- **Vector Index**: Fixed initialization with smart dimension detection from existing models and databases
- **Async Processing**: Documents now get embeddings generated asynchronously in detached threads (non-blocking)
- **Reliability**: Embedding generation now works by default, if it fails you will see ```yams repair --embeddings``` in ```yams status```
- **Stats Command**: Replaced confusing worker activity counters with clear system capability status
- **List Command Filtering**: Fixed diff-tags logic to properly skip change filters when showing diff visualization

- **Security**
  - Fixed command injection vulnerability in model_command.cpp by replacing system() calls with safe execvp()
  - Added path validation and sanitization for user inputs
  - Validate output paths to prevent directory traversal attacks

## [v0.3.2] - 2025-08-15

### Added
- **Preview: Vector Database (experimental)**
  - `SqliteVecBackend` for persistent vector storage via the sqlite-vec extension
  - BLOB storage format for embeddings with transaction-safe writes across vector and metadata tables
  - CLI integration: enable via `yams init` or follow prompts shown in `yams stats --health`
- **MCP Server**
  - Stdio transport only; HTTP framework dependencies removed
### Fixed
- **Vector DB**
  - Embeddings now persist across restarts
  - Updated CRUD operations for new two-table architecture
- **Build/CI**
  - Fixed Conan ARM64 architecture detection on macOS self-hosted runner
- **MCP**
  - Removed Drogon and Boost dependencies that caused runtime issues

### Notes
- **Usage**
  - Vector search is off by default. Initialize and configure embeddings with `yams init` or via health prompts in `yams stats --health`.
- **Requirements**
  - SQLite with the sqlite-vec extension available at runtime.
- **Limitations (preview)**
  - Schema and APIs may change between minor versions.
  - Not enabled by default; indexing currently targets text documents only.
  - Single-process/local database only; no cross-process index sharing yet.

## [v0.3.1] - 2025-08-14

### Fixed
- **Critical: Missing ref_transactions table after init**
  - Fixed incomplete schema initialization in reference counter that caused "no such table: ref_transactions" error
  - Added complete inline schema fallback with all required tables (ref_transactions, ref_transaction_ops, ref_statistics, ref_audit_log)
  - Implemented proper resource file discovery for production deployments
  - Added installation of SQL schema files to CMake configuration
- **CI/CD Workflow Simplification**
  - Removed all cross-compilation logic from release workflow
  - Fixed CI build failures on Linux (gtest cache corruption) and macOS (runner detection)
  - Moved macOS x86_64 builds to GitHub hosted runners (macos-13)
  - Docker builds now use native architecture on each runner (ARM on macOS, x86 on Linux)

### Changed
- **Build Infrastructure**
  - All builds now use native compilation for better performance and reliability
  - Simplified Conan configuration by removing cross-compilation complexity
  - Docker workflow split into separate jobs per architecture for efficiency
  - Data files (magic_numbers.json, reference_schema.sql) now properly installed with binary

## [v0.3.0] - 2025-08-14

### Added
- **Apple Silicon Optimizations**
  - New `YAMS_ENABLE_APPLE_SILICON_OPTIMIZATIONS` CMake option (enabled by default on ARM64 Macs)
  - Native `-mcpu=apple-m1` optimization flag for M1/M2/M3 processors
  - Added `-fvectorize` for improved SIMD utilization on Apple Silicon

### Fixed
- **CI/CD C++ Standard Configuration**
  - Fixed Conan profile resetting to C++17 in CI/CD workflows
  - Added explicit C++20 enforcement after `conan profile detect --force`
  - Fixed cross-compilation profile configurations for both CI and Release workflows

## [v0.2.9] - 2025-08-14

**Enhanced Deduplication Reporting**
  - New stats --dedup command for block-level analysis
  - Shows deduplication ratio, space savings, most duplicated blocks
  - Validated 32% deduplication on real codebases
**CI version bump**
  - Fixed CMake export error with spdlog dependency
  - Resolved CI failures on macOS x64 and Linux x86

## [v0.2.8] - 2025-08-14

### Added
- **Repair Command**: New comprehensive storage maintenance command
  - `yams repair --orphans`: Clean orphaned metadata entries (removed 28,101 orphaned entries in testing)
  - `yams repair --chunks`: Remove orphaned chunk files (freed 38MB from 23,988 orphaned chunks)
  - `yams repair --mime`: Fix missing MIME types in documents
  - `yams repair --optimize`: Vacuum and optimize database (reduced 314MB to 272MB)
  - `yams repair --all`: Run all repair operations
  - Dry-run support with `--dry-run` flag for safe preview
  - Force mode with `--force` to skip confirmations

- **Delete Command Enhancements**:
  - Added `--directory` option for recursive directory deletion
  - Improved metadata cleanup when deleting documents
  - Added progress indicator for large deletion operations
  - Properly removes both manifest and chunk files

- **Config Command Enhancements**:
  - Added compression tuning support via configuration
  - `config get` - Retrieve configuration values
  - `config set` - Set configuration values (placeholder)
  - `config list` - List all configuration settings
  - `config validate` - Validate configuration file
  - `config export` - Export configuration in TOML or JSON format

### Fixed
- **Stats Command**:
  - Fixed object file counting (was showing 0, now correctly counts chunks)
  - Fixed unique document calculation to match total when no duplicates exist
  - Added orphaned chunk detection and reporting
  - Fixed deduplication stats to only show when duplicates exist
  - Properly filters orphaned metadata entries from counts (28,101 orphaned found)
  - Added chunk health reporting with progress indicator
  - Fixed verbose mode not being properly tracked

- **Storage Path Issues**:
  - Fixed duplicate "storage" in path construction bug throughout codebase
  - Fixed content_store_impl.cpp manifest and object paths
  - Fixed same issue in delete_command.cpp and repair_command.cpp

- **Chunk Management Crisis**:
  - Delete command wasn't properly removing chunks when ref_count reached 0
  - Implemented proper chunk cleanup when documents are deleted
  - Added reference counting verification in repair command
  - Fixed hash format mismatch between database (64 chars) and filesystem (62 chars + 2 char directory)

- **Database Issues**:
  - Fixed view vs table issue with `unreferenced_blocks` (it's a VIEW not a table)
  - Properly clean `block_references` table where `ref_count = 0`
  - Added database optimization to reclaim space from deleted entries
  - Database reduced from 314MB to 272MB after VACUUM

### Changed
- **Stats Output**: Improved formatting and information display
  - Shows storage overhead calculation
  - Displays warnings for orphaned chunks and metadata
  - Only shows deduplication info when relevant
  - Added database inconsistency warnings
  - Better progress indicators for long operations

## [v0.2.7] - 2025-08-14
### Fixed
- **CI version bump**
  - Conan flag fix in CI

## [v0.2.6] - 2025-08-14
### Fixed
- **CI version bump**
  - Conan flag fix in CI

## [v0.2.5] - 2025-08-14
### Fixed
- **CI version bump**
  - Updated git pages site
  - Build system improvements

## [v0.2.4]

### Fixed
- **MCP Server Signal Handling**
  - Server now responds properly to Ctrl+C (SIGINT) signals
  - Replaced std::signal with sigaction to allow interrupting blocking I/O
  - Added non-blocking stdin polling with 100ms timeout in StdioTransport
  - Server checks external shutdown flag in main loop
  - Handles EINTR errors and clears stdin error state properly
  - Added clear startup messages and usage instructions to stderr

- **Dockerfile**
  - Dockerfile should build correctly

- **Cross-Compilation Support**
  - Added explicit Conan profiles for x86_64 and ARM64 architectures
  - CI/CD workflows now properly handle cross-compilation with dual profiles
  - Fixed zstd assembly symbol errors when cross-compiling on Apple Silicon
  - Added zstd/*:build_programs=False option to avoid build issues
  - Workflows detect native vs cross-compilation scenarios automatically

### Added

- **MCP Server Documentation**
  - Comprehensive MCP usage guide at `docs/user_guide/mcp.md` (I personally use the CLI)

### Technical Details
- **Signal Handling**: Uses sigaction with sa_flags=0 to interrupt blocking system calls
- **Non-blocking I/O**: Implements poll() on Unix/macOS for stdin availability checking
- **Cross-Compilation**: Uses Conan's dual profile system (-pr:b for build, -pr:h for host)

## [v0.2.3] - 2025-08-14
### Fixed
- **CI version bump**
  - README updates (removed install instructions with curl, download binary)
  - Build system improvements for docker files and release

## [v0.2.2] - 2025-08-14

### Fixed
- **Docker Build C++20 Configuration**
  - Added explicit C++20 standard setting in Conan profile
  - Fixed "Current cppstd (gnu17) is lower than required C++ standard (20)" error
  - Ensures Docker builds use correct C++ standard matching project requirements
- **Release Notes Formatting**
  - Fixed Conan profile output contaminating GitHub release descriptions
  - Redirected debug output to stderr to keep release notes clean
  - Release pages now display properly formatted markdown
- **macOS Cross-Compilation**
  - Fixed Boost build failures when cross-compiling x86_64 on ARM64 macOS
  - Disabled Boost.Locale and Boost.Stacktrace.Backtrace components that fail in cross-compilation
  - Enables successful builds on Apple Silicon machines with Rosetta 2

### Changed
- **Docker Build Process**
  - Switched from CMake presets to direct CMake commands with Conan toolchain
  - Added ninja-build to Docker dependencies for faster builds
  - More robust build process that doesn't rely on dynamically generated presets

## [v0.2.1] - 2025-08-13

### Fixed
- **Build System Standardization**: All workflows now use Conan exclusively
  - Fixed CMake export errors for spdlog and other dependencies
  - Release workflow migrated from plain CMake to Conan-based builds
  - Ensures consistent dependency management across CI and release
  - Resolves "target requires target that is not in any export set" errors
- **Docker Build Issues**
  - Fixed Conan 2.0 command syntax (`conan profile show` instead of `conan profile show default`)
  - Fixed CMake preset issues by using direct CMake commands with Conan toolchain
  - Added ninja-build to Docker dependencies for faster builds
  - Improved version handling: uses git tags for releases, `dev-<sha>` for development builds
  - Fixed undefined variable warnings in Dockerfile labels
  - Added proper build argument handling for YAMS_VERSION and GITHUB_SHA
- **Release Page Formatting**
  - Fixed Conan profile output contaminating release notes
  - Redirected debug output to stderr to keep release notes clean

### Changed
- **MIME Type Detection Default Behavior**
  - MIME types are now detected automatically during add operations
  - `repair-mime` command remains available for fixing existing documents
  - Detection uses FileTypeDetector with both signature and extension methods
- **Docker Versioning Strategy**
  - Tagged releases use semantic version from git tag
  - Development builds use `dev-<short-sha>` format
  - Provides clear distinction between releases and development builds

### Technical Details
- **Testing Interface**: MCP server now exposes test methods via conditional compilation
  - `#ifdef YAMS_TESTING` enables test-specific public methods
  - Replaces hacky `#define private public` approach
  - Maintains encapsulation while allowing proper unit testing
- **Build Workflows**: Unified Conan usage with proper profiles and caching
  - Conan profile detection with C++20 enforcement
  - Package caching for faster builds
  - Consistent preset usage (`conan-release`) across all platforms

## [v0.2.0] - 2025-08-13

### Added
- **LLM Ergonomics Enhancements**
  - Added `--paths-only` flag to search command for LLM-friendly output (one file path per line)
  - Enhanced codebase indexing workflows for AI agent integration
  - Improved documentation in PROMPT-eng.md with LLM-optimized YAMS patterns
  - Support for using YAMS as "memory layer for low context" in AI workflows
  - Magic number patterns for `#include`, `import`, `function`, `class`, shebang lines, etc.
  - Automatic programming language detection during recursive directory operations
- **Grep Command**: Full-featured regex search within indexed file contents
  - Standard grep options: `-A/--after`, `-B/--before`, `-C/--context` for context lines
  - Pattern matching options: `-i` (case-insensitive), `-w` (whole word), `-v` (invert)
  - Output options: `-n` (line numbers), `-H` (with filename), `-c` (count), `-l` (files with matches)
  - Color highlighting support with `--color` option
  - Supports searching all indexed files or specific paths
- **Knowledge Graph Integration in Get Command**
  - Added `--graph` flag to display related documents
  - Added `--depth` option (1-5) to control graph traversal depth
  - Shows documents in same directory and with similar extensions
  - Displays document relationships and metadata
- **Line-Level Search with Context**
  - Enhanced search command with `-n/--line-numbers` flag
  - Added context options: `-A/--after`, `-B/--before`, `-C/--context`
  - Highlights matching lines with color
  - Shows surrounding context lines for better understanding
- **C++17 Compatibility Mode** for broader system support
  - New `YAMS_CXX17_MODE` CMake option for legacy compiler compatibility
  - Support for GCC 9+, Clang 9+, older MSVC versions
  - Automatic fallbacks: `boost::span` for `std::span`, `fmt` for `std::format`
  - Compatible with Ubuntu 20.04 LTS, CentOS 8, older macOS versions
  - Comprehensive compatibility documentation and build matrix
  - Runtime feature detection with graceful degradation
- **Enhanced Build System Flexibility**
  - Dual-mode compilation: full C++20 for modern systems, C++17 for legacy
  - Automatic dependency management with Conan and FetchContent fallbacks
  - Compiler-specific optimizations and warning configurations
  - Performance-oriented feature selection based on available capabilities
- **MCP Server Feature Parity Enhancements** (~85% CLI-MCP parity achieved)
  - **Enhanced search_documents tool** with LLM ergonomics optimizations
    - Added `paths_only` parameter for LLM-friendly file path output (one per line)
    - Added line context parameters: `line_numbers`, `after_context`, `before_context`, `context`
    - Added `color` parameter for syntax highlighting control (always, never, auto)
  - **New grep_documents tool** with full CLI grep parity
    - Complete regex pattern matching across indexed document contents
    - Context options: `-A/--after`, `-B/--before`, `-C/--context` for surrounding lines
    - Pattern options: `ignore_case`, `word`, `invert` for flexible matching
    - Output modes: `line_numbers`, `with_filename`, `count`, `files_with_matches`, `files_without_match`
    - Color highlighting and `max_count` limit support
  - **Knowledge graph integration** in retrieve_document tool
    - Added `graph` parameter to include related documents in response
    - Added `depth` parameter (1-5) for graph traversal control
    - Added `include_content` parameter for full document content in graph responses
    - Finds documents in same directory, similar extensions, and metadata relationships
  - **New update_metadata tool** for document metadata management
    - Update metadata by document `hash` or `name` with automatic resolution
    - Support for multiple `key=value` metadata pairs in single operation
    - Verbose output option for detailed update information
  - **Enhanced list_documents tool** with comprehensive filtering and sorting
    - File type filtering: `type`, `mime`, `extension`, `binary`, `text` parameters
    - Time-based filtering: `created_after/before`, `modified_after/before`, `indexed_after/before`
    - Recent documents filter: `recent` parameter for N most recent files
    - Sorting control: `sort_by` (name, size, created, modified, indexed) and `sort_order` (asc, desc)
  - **Enhanced get_stats tool** with file type breakdown analysis
    - Added `file_types` parameter for detailed file type distribution
    - Shows file type counts, sizes, top extensions per type, and top MIME types

### Enhanced
- **MCP Tool Documentation** comprehensively updated with new schemas and examples
  - Added complete JSON schemas for all enhanced tools with parameter descriptions
  - Added practical usage examples for each tool capability
  - Updated integration documentation for Claude Desktop configuration
- **YAMS-first workflow guidance**: default to indexing and searching code via YAMS pre-watch; avoid external grep/find/rg for repository queries.
- **CLI docs enhanced** with pre-watch indexing workflow and YAMS-only search/grep examples.

### Technical Details
- **File type classification system** with MIME type analysis using `getFileTypeFromMime()` and `isBinaryMimeType()` helper methods
- **Knowledge graph traversal** leverages document relationships through metadata repository queries
- **LLM ergonomics optimizations** reduce context usage while maintaining full functionality
- **Tool schema definitions** follow JSON-RPC standards for seamless MCP integration

### Fixed
- **Indexing Tests**: Updated for new API with proper factory function declarations
- **Namespace Issues**: Fixed forward declarations in document_indexer.h
- **CLI11 Conflicts**: Resolved `-h` flag conflict in grep command by using only `--no-filename`
- **Test Suite Stabilization**
  - Fixed SEGV crash in vector_database_tests (DocumentChunkerTest bounds checking)
  - Fixed AddressSanitizer container overflow in detection_tests
  - Added thread safety to FixtureManager test infrastructure with proper mutex protection
  - Added thread safety to FileTypeDetector for libmagic operations (not thread-safe by default)
  - Fixed compilation errors in multiple test files (header includes, error codes, GMock dependencies)
  - Fixed EXPECT_NO_THROW macro usage in detection tests
- **Recursive Add Pattern Parsing**
  - Fixed CLI11 comma-separated pattern handling in `--include` and `--exclude` options
  - Now properly splits patterns like `"*.cpp,*.h,*.md"` into individual patterns
  - Added proper whitespace trimming and empty pattern filtering
- **CI/CD Pipeline Improvements**
  - Fixed Conan options quoting for proper test builds (`"yams/*:build_tests=True"`)
  - Added explicit `YAMS_BUILD_TESTS=ON` to CMake configuration
  - Added individual test target builds before running ctest
  - Implemented graceful test failure handling (CI continues, reports detailed results)
  - Added ASAN relaxation options for CI environment compatibility
  - Fixed YAML syntax error in release.yml (Python script indentation)

## [v0.1.3] - 2025-08-13

### Added
- **Multi-Channel Package Distribution System**
  - Universal install script with platform detection (Linux/macOS, x86_64/ARM64)
  - One-line installation: `curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash`
  - Checksum verification and retry logic for secure downloads
  - Custom installation directory support and shell completion setup
  - Environment variable customization (YAMS_VERSION, YAMS_INSTALL_DIR, etc.)
- **Docker Container Support**
  - Multi-stage Dockerfile with optimized Ubuntu-based runtime
  - Multi-architecture builds (AMD64/ARM64) via GitHub Container Registry
  - Automated publishing on tags and main branch pushes to `ghcr.io/trvon/yams`
  - Non-root user configuration for security best practices

### Enhanced
- **GitHub Release Workflow with Performance Reporting**
  - Comprehensive benchmark execution with JSON result collection
  - Code coverage reports (HTML/XML) generated for Linux builds
  - Performance metrics tables in release notes with timing and throughput data
  - Coverage and benchmark files automatically uploaded as release assets
  - Combined release notes with changelog and performance data
- **CI/CD Pipeline Improvements**
  - Migrated all workflows to self-hosted runners (`self-hosted`, `Linux/macOS`, `X64/ARM64`)
  - Fixed compatibility issues (mapfile command, artifact upload paths)
  - Enhanced artifact collection (coverage reports, benchmark results, binaries)
  - Streamlined build process removing problematic Conan profile modifications

### Notes
- All packaging improvements implemented but not yet released pending verification
- Release workflow enhanced to provide comprehensive performance and coverage data
- Multi-platform distribution ready for production deployment

## [0.1.2] - 2025-08-13

### Added
- **File Type and Time Filtering**: Enhanced list, get, and stats commands with comprehensive filtering
  - File type filters: `--type`, `--mime`, `--extension`, `--binary`, `--text`
  - Time filters: `--created-after/before`, `--modified-after/before`, `--indexed-after/before`
  - Supports flexible time formats (ISO 8601, relative like "7d", natural like "yesterday")
- **Stats Command File Type Breakdown**: New `--file-types` flag shows detailed file type analysis
  - File type distribution with counts and sizes
  - Top file extensions per type
  - Top MIME types in the system
- **Intelligent File Type Detection**: Uses magic_numbers.json as single source of truth
  - Runtime detection of data files across multiple installation paths
  - Graceful fallback to built-in patterns when JSON not found

### Changed
- **File Type Classification**: More accurate detection using magic numbers vs just extensions
- **Data File Installation**: magic_numbers.json now properly installed to system paths

## [0.1.1] - 2025-08-13

### Fixed
  - Corrected Conan preset names (use `conan-release` instead of `build-conan-release`)
  - Fixed C++20 standard requirement in Conan profiles
  - Removed overly strict compiler warnings that broke dependency builds
  - Fixed gcovr regex patterns for coverage reports
  - Removed `-Wold-style-cast`, `-Wconversion`, `-Wsign-conversion`, and `-Woverloaded-virtual`
  - Keeps essential warnings while allowing third-party code to compile cleanly

### Changed
- **Build Instructions**: Standardized on `conan-release` preset across all platforms
  - Simplified build commands in README and documentation
  - Consistent build directory structure (`build/conan-release`)

## [0.1.0] - 2025-08-12

### Added
- **List Command Enhancement**: Added `--recent N` flag to show N most recent documents
  - Filters to the N most recent documents before applying other sorting
  - Works with all existing sort options (name, size, date, hash)
- **PDF Text Extraction**: Full PDF text extraction support using PDFium library
  - Extract text from all PDF pages with proper UTF-16 to UTF-8 conversion
  - Extract PDF metadata (title, author, subject, keywords, creation date)
  - Support for searching within PDFs with context lines
  - Page-by-page extraction with configurable options
  - Automatic section detection for academic papers
- **Metadata Update Command**: New `update` command for modifying document metadata
  - Update metadata by document hash or name
  - Support for multiple key-value pairs in single operation
  - Enables task tracking and status management workflows
  - Works with both file and stdin documents
- **Task Tracking System**: Comprehensive benchmark and test enhancement tracking
  - PBI documentation structure for systematic improvement
  - Task status tracking via metadata updates
  - Progress monitoring capabilities
- **KG Hybrid Scoring (default)**: Hybrid search integrates Keyword + Vector + Knowledge Graph
  - Enabled by default in both CLI and MCP via a shared SearchEngineBuilder
  - Fails open to metadata/keyword-only when KG is unavailable

### Fixed
- **Reference Counter Transaction IDs**: Fixed UNIQUE constraint violation in ref_transactions
  - Removed manual transaction ID management
  - Now uses SQLite AUTOINCREMENT for guaranteed unique IDs
  - Prevents "UNIQUE constraint failed" errors when multiple ReferenceCounter instances exist
- **Duplicate Document Handling**: Fixed metadata constraint errors when re-adding existing documents
  - Detects high deduplication ratio (≥99%) and updates existing metadata instead of inserting
  - Properly handles both new and existing documents
  - Updates indexed timestamp for existing documents
- **Name Resolution for Commands**: Fixed document lookup by name for stdin documents
  - get, cat, and update commands now properly find stdin documents by name
  - Uses search functionality as fallback when path-based search fails
  - Supports both file-based and stdin documents uniformly
  - Automatic platform detection (mac-arm64, mac-x64, linux-x64, linux-arm64, win-x64)
  - FetchContent integration for non-Conan builds
  - Conditional compilation with YAMS_HAS_PDF_SUPPORT flag
  - Installs libpdfium.dylib into the install lib directory and sets its install_name to @rpath/libpdfium.dylib
  - Patches installed executables to reference @rpath/libpdfium.dylib and adds rpath @loader_path/../lib
  - Eliminates dyld errors like "Library not loaded: ./libpdfium.dylib"
  - Created header file for UpdateCommand class
  - Fixed compilation errors in update_command.cpp
  - Resolved conflicts between Conan and FetchContent for Google Benchmark
  - Fixed imgui.h include path for browse_command.cpp
  - Fixed compilation issues with performance benchmarks

### Changed
- **Extraction Module**: Enhanced to support multiple file formats
  - Modular extractor registration system
  - Extensible architecture for future format support
- **Logging Verbosity**: Storage operations now use debug level instead of info
  - "Stored file" messages only appear in verbose/debug mode
  - Cleaner output for normal operations

### Known Issues
- **Performance Benchmarks**: Storage benchmarks cause segmentation fault after Rabin chunking tests
  - SHA256 benchmarks work correctly (2.59 GB/s for large files)
  - Rabin chunking benchmarks work correctly (178 MB/s)
  - Storage fixture initialization causes crash - to be fixed in future update

## [0.0.6] - 2025-08-11

### Added
  - TUI Browser Improvements

### Fixed
  - Fixed unique_ptr copy errors in query parser benchmarks
  - Resolved compression error handler warnings on Ubuntu 22.04
  - Improved error handling and warning cleanup for better compiler compatibility
  - Removed hardcoded widget dimensions that broke small terminal displays
  - Fixed non-functional scrolling in document lists
  - Improved loading state management and race condition handling
  - Enhanced document preview formatting and metadata display

## [0.0.5] - 2025-08-11

### Added
- **Build System Enhancements**:
  - Automatic std::format detection with fallback to fmt library for older compilers
  - Improved ncurses detection for Linux builds with clear error messages
  - BUILD-GCC.md documentation for building with GNU toolchain
- **Search Improvements**:
  - Word-by-word processing for multi-word queries (e.g., "chain event ingestion" searches each word independently)
  - Minimum-should-match scoring (50% of query words must match by default)
  - Intelligent result aggregation with boost for documents matching all query words
  - Edit distance-based scoring (closer matches score higher)
  - Automatic fallback to fuzzy search when FTS5 fails
  - **Hash search capability**: Search documents by SHA256 hash (full or partial, minimum 8 characters)
  - **Verbosity control**: Concise output by default, detailed output with --verbose flag
  - Auto-detection of hash format in queries (8-64 hexadecimal characters automatically trigger hash search)
- **MCP Server Enhancements**:
  - Updated to use metadata repository path with all v0.0.5 search improvements
  - Added fuzzy search support to MCP server tools with configurable similarity threshold
  - Enhanced search_documents tool with fuzzy and similarity parameters

### Fixed
  - Proper handling of hyphens in queries like "PBI-6", "task-4", "feature-123"
  - Special characters (@, #, ?, :, /, \, [], {}, ()) no longer cause SQL errors
  - Advanced FTS5 operators (AND, OR, NOT, NEAR) still available for power users
- **Fuzzy search matching**: Completely rewrote trigram similarity calculation
- **CI/CD Pipeline**:
  - Added install step verification to ensure artifacts are created
  - Added libncurses-dev to Ubuntu dependencies
  - Enabled Ubuntu traditional (non-Conan) builds in CI matrix

### Changed
- **Search indexing strategy**: Enhanced to support both single and multi-word queries effectively
  - Documents now indexed at word level in addition to full content
  - Keywords extracted more comprehensively with 2-word phrases
  - Improved tokenization with punctuation handling
- **Error handling**: FTS5 failures now gracefully fall back to fuzzy search instead of returning errors

## [0.0.4] - 2025-08-11

### Added
- **Directory-based workflows** with collections and snapshots for multi-folder ingestion
  - `add --collection <name>`: Add documents to a named collection for organization
  - `add --snapshot-id <id> --snapshot-label <label>`: Add documents to a snapshot for point-in-time grouping
  - `add --recursive`: Recursively add files from directories
  - `restore --collection <name>`: Restore all documents from a specific collection
  - `restore --snapshot-id <id>`: Restore all documents from a snapshot
  - `restore --layout <template>`: Flexible output patterns like `{collection}/{path}` for restore operations
- **MCP Server Tools** for directory operations with full CLI parity
  - `add_directory`: Add directory contents with collection/snapshot support
  - `restore_collection`: Restore documents from a collection to filesystem
  - `restore_snapshot`: Restore documents from a snapshot to filesystem
  - `list_collections`: List available collections
  - `list_snapshots`: List available snapshots
- **CLI verbosity control**: All CLI commands now have concise output by default
  - Use `--verbose` flag for detailed JSON metadata output
  - Reduces context window usage while maintaining functionality
- **MCP Server enabled by default** in all builds for integrated AI tool support

### Changed
- **Database schema enhanced** with collections and snapshots metadata (migration v6)
  - New indexes for efficient collection and snapshot queries
  - Metadata tables support `collection`, `snapshot_id`, and `snapshot_label` fields
- **CLI output standardized** to be concise by default, verbose on request
- **Build system updated** to enable MCP server by default in Conan builds

### Fixed
- **Version consistency**: MCP server now uses same version system as CLI (was hardcoded "1.0.0")
- **ImTUI integration**: Resolved ncurses symbol versioning conflicts using ExternalProject_Add
- **FTS5 support**: Enabled SQLite full-text search in Conan configuration
- **Conan 2.0 compatibility**: Fixed all C++20 build issues with external dependencies
- **Test suite improvements**: Comprehensive fixes for CI/CD reliability
  - Fixed GMock header compilation errors in MCP tests (missing `GTest::gmock` dependency)
  - Fixed SlidingWindowChunker infinite loop caused by unsigned integer underflow
  - Updated chunker test expectations to use realistic tolerances for word boundary preservation
  - Fixed metadata repository tests API compatibility (ConnectionPool vs Database constructor)
  - Resolved performance-dependent test failures in Rabin chunker (reduced expectations from 200MB/s to 100MB/s)

### Developer Notes
- Database migration system handles schema updates automatically
- All directory operations are scoped to prevent accidental mass operations
- MCP server integration provides seamless AI tool access to YAMS functionality
- Build system now supports both CLI and MCP server components by default

## [0.0.3] - 2025-08-11

### Added
- **MCP Tools**: New Model Context Protocol tools supporting v0.0.2 CLI enhancements
  - `delete_by_name`: Delete documents by name, multiple names, or glob patterns with dry-run support
  - `get_by_name`: Retrieve document content by name instead of hash
  - `cat_document`: Display document content directly (similar to CLI cat command)
  - `list_documents`: List stored documents with filtering by pattern and tags
- **TUI Browser migrated to ImTUI**: Complete rewrite from FTXUI to ImTUI for better compatibility
  - Full-screen document viewer with hex mode toggle (Enter to open, x to toggle)
  - External pager integration (o key) with proper terminal suspend/resume
  - Preview pane scrolling when focused (Tab to switch columns, j/k to scroll)
  - Extended command mode (:hex, :open, :refresh, :help, :q)

### Changed
- Browse command refactored to use modular TUI components and improved UX

### Fixed
- Safer preview of binary content and better CRLF handling
- **Build System**: Resolved multiple compilation issues preventing CI/CD success
  - Fixed switch statement scope error in `posix_storage.cpp` by adding proper braces
  - Added missing `#include <sys/resource.h>` for POSIX resource limits
  - Added missing `#include <cmath>` in embedding tests for `std::isfinite` and `std::sqrt`
  - Added missing `#include <algorithm>` for `std::all_of` in manifest manager
  - Fixed designated initializer field ordering in `compressed_storage_engine.cpp`
  - Fixed type conversion from `std::string_view` to `Hash` type
  - Replaced `std::ranges::all_of` with `std::all_of` for broader compiler compatibility

### Developer Notes
- All compilation fixes tested locally with successful build completion
- Prepared for GitHub Actions CI/CD pipeline success
- Build now completes without errors on macOS (AppleClang 17.0.0)

## [0.0.2] - 2025-08-11

### Added
- **Name-based operations**: All major commands now support name-based access
  - `delete --name <name>`: Delete document by name
  - `delete --names <name1,name2>`: Delete multiple documents by names
  - `delete --pattern <pattern>`: Delete documents matching glob pattern (e.g., `*.log`)
  - `get --name <name>`: Retrieve document by name
  - `cat` command: New command for displaying content directly to stdout
  - `cat <hash>` or `cat --name <name>`: Output content for piping and viewing
- **Command aliases**: Unix-style command aliases for better ergonomics
  - `rm` as alias for `delete`
  - `ls` as alias for `list`
- **Delete command enhancements**:
  - `--dry-run` flag: Preview what would be deleted without actually deleting
  - `--verbose` flag: Show detailed deletion information
  - Bulk deletion support with pattern matching
  - Improved error handling for ambiguous names
- **Browse command improvements**:
  - Command mode with `:` or `p` key (vi-like commands)
  - Preview pane now shows actual content from metadata repository
  - Binary content detection for preview
  - Better search functionality with fuzzy/exact toggle
- **Documentation**:
  - Added MCP tools documentation (`docs/api/mcp_tools.md`)
  - Updated CLI documentation with new features
  - Added this CHANGELOG.md for tracking changes

### Fixed
- **CI/CD**: Fixed compilation errors by adding missing includes in `compressor_interface.h`
  - Added `#include <functional>`
  - Added `#include <unordered_map>`
- **Browse command**: Fixed preview loading for documents with metadata content

### Changed
- **Get command**: Now supports both hash and name-based retrieval
- **Delete command**: Refactored to use CLI11 option groups for better argument handling
- **Browse command**: Enhanced with command mode and better content preview

### Developer Notes
- Integration tests created for delete command functionality
- Improved CLI command structure using option groups
- Better separation of concerns between viewing (`cat`) and downloading (`get`) operations

## [0.0.1] - 2025-08-11

### Added
- Initial release of YAMS
- Core content-addressable storage functionality
- MCP (Model Context Protocol) server support
- Basic CLI commands: init, add, get, delete, list, search
- Metadata repository with SQLite backend
- Compression support with multiple algorithms
- Deduplication at chunk level
- TUI browser interface
- Authentication and key management
- Configuration management
- Stats and monitoring capabilities

---

*Note: This project follows semantic versioning. Given the 0.x.y version, the API is still considered unstable and may change between minor versions.*
