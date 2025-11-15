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

### Changed
- **Build System**: Removed `enable-tui` build option from meson_options.txt
- **Developer Experience**: ThreadSanitizer now enabled by default in Debug builds via setup.sh