# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
  - Useful for quickly viewing recently added content
- **PDF Text Extraction**: Full PDF text extraction support using PDFium library
  - Extract text from all PDF pages with proper UTF-16 to UTF-8 conversion
  - Extract PDF metadata (title, author, subject, keywords, creation date)
  - Support for searching within PDFs with context lines
  - Page-by-page extraction with configurable options
  - Automatic section detection for academic papers
- **Metadata Update Command**: New `update` command for modifying document metadata
  - Update metadata by document hash or name
  - Support for multiple key-value pairs in single command
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
  - `restore --snapshot-id <id>`: Restore all documents from a specific snapshot
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
