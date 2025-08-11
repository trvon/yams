# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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