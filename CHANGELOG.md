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

## [v0.7.9] - Unreleased

### Removed
- **TUI/Browse Interface**: Removed FTXUI-based terminal UI components in preparation for Flutter mobile application
  - Removed `src/cli/tui/` directory and all TUI source files
  - Removed `include/yams/cli/tui/` directory and TUI headers
  - Removed browse command (`src/cli/commands/browse_command.cpp`)
  - Removed FTXUI dependencies from build system
  - Updated command registry and CLI help to remove browse references

### Fixed
- **Thread Safety**: Fixed critical race conditions detected by ThreadSanitizer (TSan)
  - Fixed AsioConnection destructor race by properly canceling and closing sockets before destruction
  - Fixed RepairCoordinator access race by adding mutex protection around all accesses from TuningManager callbacks
  - Added mutex synchronization for RepairCoordinator lifecycle (creation, access, destruction)
  - Enabled ThreadSanitizer by default for Debug builds to catch race conditions early
- **Streaming Response Hang**: Fixed coroutine race condition and idle timeout bypass causing streaming requests to hang
  - Streaming requests would timeout after connection went idle due to writer_drain not completing properly
  - Root cause 1: `enqueue_frame` coroutine suspension between flag check and flag set allowed multiple writers to start
  - Root cause 2: Connections with in-flight requests bypassed idle timeout, staying open indefinitely (105+ minutes observed)
  - Solution 1: Created synchronous `enqueue_frame_sync()` that sets `writer_running_` flag atomically before suspension
  - Solution 2: Close idle connections even with in-flight requests - if client stopped reading, requests are stuck anyway
  - Added comprehensive test suite (`writer_drain_test.cpp`) with 5 integration tests validating the fix
  - Files: `src/daemon/ipc/request_handler.cpp`, `include/yams/daemon/ipc/request_handler.h`
- **Document Retrieval**: Fixed "Document not found" error when multiple instances of a document exist
  - `yams get --name` now returns the most recently indexed document by default when multiple matches exist
  - Added `--oldest` flag support to retrieve the oldest indexed version instead
  - Updated `resolveNameToHash` to accept disambiguation strategy parameter
  - Non-blocking for LLM usage - always returns a result without requiring manual selection
- **Snapshot Creation**: Fixed "File found in index but not in any snapshot" error
  - Simplified snapshot logic to always create snapshots on every ingestion
  - Removed conditional 24-hour check that prevented snapshot creation for existing documents
  - All documents now properly tracked in version history via `yams list <filename>`
- **Connection Pool Reliability**: Simplified connection lifecycle management following daemon-managed best practices
  - Removed all client-side staleness prediction logic (`is_stale()`, age-based checks, peek logic)
  - Daemon now fully controls connection lifecycle via idle timeout (6 seconds)
  - Client pool simplified to only remove expired weak_ptrs, no health prediction
  - Read loop starts immediately on connection creation for proper request/response flow
  - Client reacts to natural I/O errors with automatic retry instead of trying to predict failures
  - Removed synchronization complexity (`read_loop_ready` flag and polling)
  - Unix domain socket connections are lightweight - daemon manages when to close idle connections
- **Response Latency**: Optimized future polling to reduce post-response delay
  - Changed polling from 10ms blocking wait to instant check with 1ms sleep
  - Eliminates up to 10ms unnecessary delay after responses arrive
  - Average response latency improved by ~5ms (50% reduction in polling overhead)

### Changed
- **Build System**: Removed `enable-tui` build option from meson_options.txt
- **Developer Experience**: ThreadSanitizer now enabled by default in Debug builds via setup.sh