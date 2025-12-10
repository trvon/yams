# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- [SourceHut](https://sr.ht/~trvon/yams/): https://sr.ht/~trvon/yams/

## Archived Changelogs
- v0.7.x archive: docs/changelogs/v0.7.md
- v0.6.x archive: docs/changelogs/v0.6.md
- v0.5.x archive: docs/changelogs/v0.5.md
- v0.4.x archive: docs/changelogs/v0.4.md
- v0.3.x archive: docs/changelogs/v0.3.md
- v0.2.x archive: docs/changelogs/v0.2.md
- v0.1.x archive: docs/changelogs/v0.1.md

## [v0.8.0] - Unreleased

### Added
- **ExternalPluginHost**: New plugin host for Python/process-based plugins (RFC-EPH-001)
  - Implements `IPluginHost` interface for external plugins running as separate processes
  - JSON-RPC 2.0 communication over stdio using existing `PluginProcess` and `JsonRpcClient`
  - Supported plugin types: Python (`.py`), Node.js (`.js`), any executable with JSON-RPC support
  - Process lifecycle management: spawn, monitor, health checks, graceful shutdown
  - Automatic crash recovery with configurable restart policy (max retries, backoff)
  - Trust-based security model with persistent trust file
  - RPC gateway for calling arbitrary plugin methods (`callRpc`)
  - Plugin statistics tracking (uptime, restart count, health status)
  - State change callbacks for monitoring plugin lifecycle events
  - Location: `include/yams/daemon/resource/external_plugin_host.h`, `src/daemon/resource/external_plugin_host.cpp`
- **Auto-init mode**: New `yams init --auto` flag for containerized/headless environments
  - Enables vector database with default model (`all-MiniLM-L6-v2`)
  - Enables plugins directory setup
  - Generates authentication keys
  - Skips S3 configuration (uses local storage)
  - Non-interactive: no prompts, uses sensible defaults
- **New embedding model option**: Added `multi-qa-MiniLM-L6-cos-v1` as second model choice
  - Trained on 215M question-answer pairs for semantic search optimization
  - Same dimensions (384) as default model for compatibility
  - Replaces `all-mpnet-base-v2` (768 dim) in model selection
- **Git-based version detection**: Build system now auto-detects version from git tags
  - Uses most recent semver tag (`v*`) as effective version
  - Falls back to project version only if no tags exist
  - Command-line override (`-Dyams-version=X.Y.Z`) takes highest priority
- **Commit hash in version output**: `yams --version` now shows short commit hash
  - Format: `0.7.9 (commit: c16939f) built:2025-11-29T17:30:15Z`
  - Helps identify exact build for bug reports and debugging
- **Init command tests**: New test suite for init command model download functionality
  - Tests for valid HuggingFace URLs, model dimensions, naming conventions
  - CLI flag acceptance tests (`--auto`, `--non-interactive`, `--force`)
- **Content-type-aware search profiles**: New `CorpusProfile` enum and auto-detection
  - `CODE`: Boosts symbol/path search for source code repositories (60%+ code files)
  - `PROSE`: Boosts FTS5/vector search for text-heavy corpora (60%+ docs)
  - `DOCS`: Balanced weights for mixed code/documentation
  - `MIXED`: Default balanced weights for heterogeneous corpora
  - `SearchEngineConfig::detectProfile()`: Auto-detects from file extension distribution
  - `SearchEngineConfig::forProfile()`: Returns preset weights for a profile
- **Session-isolated memory**: Documents can now be isolated to working sessions
  - New CLI commands: `yams session create`, `open`, `close`, `status`, `merge`, `discard`
  - Documents added during an active session are tagged with `session_id` metadata
  - Session documents are invisible to global searches (use `--global` to bypass)
  - `merge`: Removes session tag to promote documents to global index
  - `discard`: Permanently deletes all session documents
  - Supports multiple concurrent sessions with automatic isolation
  - Database migration adds session tracking to metadata repository
- **Windows Job Object for plugin processes**: External plugin child process cleanup
  - Plugin processes are now assigned to Windows Job Objects
  - All child processes are automatically terminated when plugin unloads
  - Prevents orphaned processes from holding file locks (e.g., PID files)
  - Uses `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` for reliable cleanup
  - Location: `src/extraction/plugin_process.cpp`

### Changed
- **Embedding model list**: Both recommended models now have 384 dimensions
  - `all-MiniLM-L6-v2`: Lightweight general-purpose semantic search (default)
  - `multi-qa-MiniLM-L6-cos-v1`: Optimized for question-answer semantic search
- **ServiceManager Decomposition**: Extracted focused components from monolithic ServiceManager
  - New `ConfigResolver`: Static config/env resolution utilities (248 lines)
  - New `VectorSystemManager`: Vector DB and index lifecycle (397 lines)
  - New `DatabaseManager`: Metadata DB, connection pool, KG store lifecycle (254 lines)
  - New `PluginManager`: Plugin host, loader, and interface adoption (515 lines)
  - ServiceManager accessors now delegate to extracted managers
- **Configurable Vector DB Capacity**: Vector index `max_elements` now configurable
  - Environment variable: `YAMS_VECTOR_MAX_ELEMENTS`
  - Config file: `[vector_database] max_elements`
  - Default: 100,000 (range: 1,000 - 10,000,000)
- **FTS5 index hygiene (migration v18)**: Removed unused `content_type` column from FTS5 index
  - `content_type` was indexed but never queried via FTS MATCH
  - Content type filtering uses JOIN on `documents.mime_type` instead
  - Reduces FTS5 index size and improves indexing performance
  - Automatic migration rebuilds index on first database open

### Fixed
- **Model download mapping**: Added `multi-qa-MiniLM-L6-cos-v1` to HuggingFace repo mapping
  - Ensures model download works for new model option
- **Version display**: Fixed `yams --version` showing fallback values instead of actual version
  - Added generated include directory to CLI build to resolve `version_generated.h`
  - Version now correctly shows git tag and commit hash
- **Socket crash on shutdown**: Fixed `EXC_BAD_ACCESS` in `kqueue_reactor::deregister_descriptor` during program exit
  - Added `ConnectionRegistry` to track all daemon client connections globally
  - Sockets are now released before `io_context` shutdown to prevent reactor access after destruction
  - Fixes race condition where coroutine frames holding socket references were destroyed during scheduler shutdown
  - Related: [boost/asio#1347](https://github.com/chriskohlhoff/asio/issues/1347)

### CLI Improvements
- **PowerShell completion**: Added `yams completion powershell` for PowerShell auto-complete
  - Uses `Register-ArgumentCompleter` with dynamic subcommand and option completion
  - Supports bash, zsh, fish, and PowerShell shells
- **Consistent `--json` output**: Extended JSON output support across commands
  - `yams doctor --json`: Machine-readable health check results
  - `yams delete --json`: Deletion results as JSON array
  - `yams grep --json`: Match results with file, line, matchType, confidence
- **Actionable error hints**: Centralized error hint system (`error_hints.h`)
  - Pattern-based hints for FTS5, embedding, daemon, database errors
  - Error code fallback hints for generic error types
  - Format: `💡 Hint:` with suggested `📋 Try:` command
- **Daemon error messages**: Enhanced daemon start/stop failure messages
  - Clear hints for common issues (daemon already running, permission denied)
  - Suggested recovery commands (`yams daemon stop --force`, `pkill yams-daemon`)

### Fixed
- **Plugin interface parsing**: Fixed `parseInterfacesFromManifest` to handle object-format interfaces
  - Plugins using `[{"id": "model_provider_v1", "version": 2}]` format now parse correctly
  - Previously only simple string arrays `["interface_name"]` were supported
  - Affects ONNX plugin and other plugins with versioned interface declarations
  - Location: `src/daemon/resource/abi_plugin_loader.cpp`

- **Plugin host sharing**: Fixed model provider adoption failure 
  - `ServiceManager::autoloadPluginsNow()` loaded plugins into `abiHost_`
  - `PluginManager::adoptModelProvider()` was querying its own empty `pluginHost_`
  - Added `sharedPluginHost` option to `PluginManager::Dependencies` for host sharing
  - `PluginManager` now uses shared host from ServiceManager when provided
  - Location: `include/yams/daemon/components/PluginManager.h`, `src/daemon/components/PluginManager.cpp`

- **VectorIndexManager initialization**: Fixed search engine build failure "VectorIndexManager not provided"
  - `VectorSystemManager::initializeOnce()` only initialized vector database, not index manager
  - Added call to `initializeIndexManager()` after successful database init
  - Added call to `loadPersistedIndex()` to restore saved index on startup
  - Location: `src/daemon/components/ServiceManager.cpp`
