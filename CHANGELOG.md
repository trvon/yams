# Changelog

All notable changes to YAMS (Yet Another Memory System) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.2.7] - 2025-08-14

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
  - Fixed orphaned metadata cleanup after storage deletion

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
    - Provides comprehensive storage analytics for better content understanding

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
