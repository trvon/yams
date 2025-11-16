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
- **Document Retrieval**: Fixed "Document not found" error when multiple instances of a document exist
  - `yams get --name` now returns the most recently indexed document by default when multiple matches exist
  - Added `--oldest` flag support to retrieve the oldest indexed version instead
  - Updated `resolveNameToHash` to accept disambiguation strategy parameter
  - Non-blocking for LLM usage - always returns a result without requiring manual selection
- **Snapshot Creation**: Fixed "File found in index but not in any snapshot" error
  - Simplified snapshot logic to always create snapshots on every ingestion
  - Removed conditional 24-hour check that prevented snapshot creation for existing documents
  - All documents now properly tracked in version history via `yams list <filename>`
- **Connection Pool Reliability**: Simplified stale connection detection following industry best practices
  - Removed complex non-blocking peek logic that added unreliable staleness detection
  - Rely on age-based eviction and natural I/O error detection during actual operations
  - Added SO_LINGER with timeout=0 for instant EOF detection on Unix domain sockets
  - Retry on connection acquisition failure (not just post-send EOF)

### Changed
- **Build System**: Removed `enable-tui` build option from meson_options.txt
- **Developer Experience**: ThreadSanitizer now enabled by default in Debug builds via setup.sh