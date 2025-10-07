# YAMS CLI Reference

A practical, task-focused reference for the `yams` command-line interface.

This document covers global flags, environment variables, and each subcommand with usage notes and examples. It is structured so tools can extract command-specific sections.

If your build supports verbose help, you can run:
- yams --help-all          to print the full CLI reference
- yams <command> --help --verbose  to print command-specific verbose help

Note: If verbose help isn’t available in your build, use yams --help and yams <command> --help for concise usage.

## Synopsis

- yams [--storage <path>] [--json] [--verbose] <command> [options]
- yams <command> --help [--verbose]

## Environment

Precedence: configuration file > environment (YAMS_STORAGE, XDG_*) > CLI flag (--storage/--data-dir). This prevents CLI defaults from masking configured storage roots defined in your config file.

- YAMS_STORAGE
  - Storage root directory. When set, overrides the CLI flag unless a configuration file explicitly sets a storage root (configuration takes precedence).
- XDG_DATA_HOME
  - Used to derive the default storage directory when YAMS_STORAGE is not set (defaults to ~/.local/share if unset).
- XDG_CONFIG_HOME
  - Used to derive the default config location (defaults to ~/.config if unset).
- HOME
  - Used as a fallback when XDG paths are not set.

## Global Options

- --storage, --data-dir <path>
  - Override the storage root directory (default: $XDG_DATA_HOME/yams or ~/.local/share/yams).
- --json
  - Request JSON output where supported.
- -v, --verbose
  - Enable verbose logging.
- --version
  - Print the CLI version.

## Commands

- init
- add
- get
- graph
- cat
- delete
- list
- search
- grep
- completion
- config (subcommands: get, set, list, validate, export)
- auth
- stats
- doctor
- uninstall
- migrate
- update
- browse
- serve (only when built with YAMS_BUILD_MCP_SERVER=ON)

---

## init {#cmd-init}

Initialize YAMS storage and configuration (interactive or non-interactive).

Synopsis:
- yams init [--non-interactive] [--force] [--no-keygen] [--print]

Options:
- --non-interactive
  - Run without prompts, using defaults and passed flags.
- --force
  - Overwrite existing config/keys if already initialized.
- --no-keygen
  - Skip authentication key generation.
- --print
  - Print resulting configuration to stdout (secrets masked).

Notes:
- The storage directory can be set globally via --storage/--data-dir or YAMS_STORAGE.
- On first run, initialization will create the storage directory, database, and configuration.

Examples:
```
yams init --non-interactive
yams --storage "$HOME/.local/share/yams" init --force
yams init --print
```

---

## add {#cmd-add}

Pre-watch code indexing (recommended)
Use YAMS to index code updates until folder track/watch is available.

Examples:
```bash
# Index entire source tree (initial import)
yams add src/ --recursive --include="*.cpp,*.hpp,*.h" --tags "code,source"
yams add include/ --recursive --include="*.hpp,*.h" --tags "code,headers"

# Re-index after local edits (quick pass)
yams add . --recursive --include="*.cpp,*.hpp,*.h,*.md" --tags "code,working"

# Add a single updated file
yams add path/to/file.cpp --tags "code,source"

# Update metadata for an existing document by name
yams update --name path/to/file.cpp --metadata "updated=$(date -Iseconds)"
```

Tips:
- Prefer comma-separated patterns with --include for multiple file types.
- Tag code consistently (e.g., code,source; code,headers) to improve later queries.

Add a document to the store from a file or stdin.

Synopsis:
- yams add <path> [options]
- yams add - [options]    # read from stdin

Options:
- -n, --name <name>
  - Set the document name (especially useful for stdin input)
- -t, --tags <tags>
  - Comma-separated tags for the document
- -m, --metadata <key=value>
  - Custom metadata key-value pairs (can be used multiple times)
- --mime-type <type>
  - Override MIME type detection
- --label <label>
  - Human-readable label for the automatic snapshot (optional)
- --collection <name>
  - Group snapshots into a named collection

Description:
- Ingests the specified file or standard input and stores it in the content-addressed store.
- Rich metadata support for tagging, naming, and custom properties.
- Content is automatically indexed for full-text and fuzzy search.
- **Automatic snapshots (PBI-043):** Every `yams add` operation automatically creates a snapshot with a timestamp-based ID (e.g., `2025-10-01T14:30:00.123Z`). A Merkle tree is built and stored for efficient diff operations.
- Trees enable fast `yams diff` operations using O(log n) subtree matching.
- **Git integration:** When run in a git repository, snapshot metadata includes the current commit hash and branch name.

Examples:
```bash
# Basic document addition (automatic snapshot created)
yams add ./README.md
# → Snapshot: 2025-10-01T14:30:00.123Z

# Add with human-friendly label
yams add . --recursive --label "Release 1.0"
# → Snapshot: 2025-10-01T14:30:00.456Z (label: "Release 1.0")

# Organize by collection
yams add docs/ --recursive --collection=documentation
# → Snapshot: 2025-10-01T14:30:01.789Z (collection: "documentation")

# Standard operations (all create automatic snapshots)
cat notes.txt | yams add - --name "meeting-notes" --tags "work,meeting"
yams add document.pdf --tags "research,papers" --metadata "author=John Doe"
yams add src/ --recursive --include="*.cpp,*.h" --tags "code,source"
```

**Automatic Snapshot Workflows:**

1. **Initial import (automatic snapshot):**
   ```bash
   yams add . --recursive --include="*.cpp,*.h,*.md" --label "Initial import"
   # → Snapshot: 2025-10-01T09:00:00.000Z
   ```

2. **After making changes (automatic snapshot):**
   ```bash
   yams add . --recursive --include="*.cpp,*.h,*.md" --label "Added new feature"
   # → Snapshot: 2025-10-01T14:30:00.000Z
   ```

3. **Compare any two snapshots:**
   ```bash
   # List all snapshots to find IDs
   yams list --snapshots
   
   # Compare using timestamp IDs
   yams diff 2025-10-01T09:00:00.000Z 2025-10-01T14:30:00.000Z
   ```

4. **View snapshot timeline:**
   ```bash
   yams list --snapshots
   # Shows chronological list of all automatic snapshots
   ```

**Performance Notes:**
- Tree building adds ~5-10% overhead to `yams add` but enables O(log n) diff operations.
- Trees are stored once in CAS and deduplicated across snapshots.
- For large repositories (>10K files), tree-based diff is 10-100x faster than flat comparison.
- Automatic snapshots have zero mental overhead - just use `yams add` normally.

---

## get {#cmd-get}

Retrieve a document by hash or name for downloading.

Synopsis:
- yams get <hash> [options]
- yams get --name <name> [options]

Options:
- --name <name>
  - Retrieve document by name instead of hash
- -o, --output <path>
  - Write output to specified file instead of stdout
- -v, --verbose
  - Enable verbose output
- --graph
  - Show related documents from knowledge graph
- --depth <N>
  - Depth of graph traversal, 1-5 (default: 1)

Description:
- Downloads content by hash or name.
- For viewing content directly, use the `cat` command instead.
- Supports both stdout redirection and explicit output file specification.

Examples:
```
yams get abcd1234... -o output.txt
yams get --name "document.pdf" -o restored.pdf
yams get --name "config.json" > config.json
yams get abcd1234... --verbose
```

---

## graph {#cmd-graph}

Show related documents via the knowledge graph for a given document (read-only).

Synopsis:
- yams graph <hash> [--depth N]
- yams graph --name <path|name> [--depth N]

Options:
- --name <path|name>
  - Resolve the target by path/name instead of hash
- --depth <N>
  - Depth of graph traversal, 1-5 (default: 1)
- -v, --verbose
  - Verbose header details

Description:
- Mirrors the graph view available in `get --graph` without fetching content.
- Uses the same daemon request path; shows related documents and their relationship/distance.

Examples:
```
yams graph 0123abcd... --depth 2
yams graph --name "docs/readme.md" --depth 3
```

---

## cat {#cmd-cat}

Display document content to stdout.

Synopsis:
- yams cat <hash>
- yams cat --name <name>

Options:
- --name <name>
  - Display document by name instead of hash

Description:
- Outputs content directly to stdout for viewing or piping.
- Silent operation - no status messages (ideal for piping).
- Use `get` command for downloading files.

Examples:
```
yams cat abcd1234...
yams cat --name "notes.txt"
yams cat --name "config.json" | jq .
yams cat --name "script.sh" | bash
```

---

## delete {#cmd-delete}
## rm {#cmd-rm}

Delete documents by hash, name, or pattern. (Alias: `rm`)

Synopsis:
- yams delete <hash> [options]
- yams delete --name <name> [options]
- yams delete --names <name1,name2,...> [options]
- yams delete --pattern <pattern> [options]

Options:
- --name <name>
  - Delete a document by its name
- --names <names>
  - Delete multiple documents by names (comma-separated)
- --pattern <pattern>
  - Delete documents matching a glob pattern (e.g., *.log, temp_*.txt)
- --force, --no-confirm
  - Skip confirmation prompt
- --dry-run
  - Preview what would be deleted without actually deleting
- --keep-refs
  - Keep reference counts (don't decrement)
- -v, --verbose
  - Show detailed deletion progress

Description:
- Supports deletion by hash (original behavior), name, multiple names, or pattern matching.
- When deleting by name and multiple documents have the same name, you'll be prompted to confirm unless --force is used.
- Pattern matching supports standard glob patterns (* for any characters, ? for single character).
- Dry-run mode shows what would be deleted without making changes.
- Bulk deletions show progress and report both successes and failures.

Examples:
```
# Delete by hash (original)
yams delete abcd1234...
yams rm abcd1234...  # Using alias

# Delete by name
yams delete --name "meeting-notes.txt"
yams rm --name "old-file.txt" --force  # Using alias

# Delete multiple files
yams delete --names "old-report.pdf,draft.txt,temp.log"

# Delete by pattern
yams delete --pattern "*.tmp"
yams delete --pattern "backup_*.zip" --dry-run

# Force deletion without confirmation
yams delete --pattern "temp_*" --force

# Verbose output
yams delete --names "file1.txt,file2.txt" --verbose
```

---

## list {#cmd-list}
## ls {#cmd-ls}

List stored documents with rich metadata display. (Alias: `ls`)

Synopsis:
- yams list [options]

Options:
- --format <format>
  - Output format: table | json | csv | minimal (default: table)
- -l, --limit <number>
  - Maximum number of documents to list (default: 100)
- --offset <number>
  - Offset for pagination (default: 0)
- --recent <N>
  - Show N most recent documents
- --sort <field>
  - Sort by: name | size | date | hash (default: date)
- -r, --reverse
  - Reverse sort order
- -v, --verbose
  - Show detailed information
- --show-snippets
  - Show content previews (default: true)
- --show-metadata
  - Show all metadata for each document
- --show-tags
  - Show document tags (default: true)
- --snippet-length <N>
  - Length of content snippets (default: 50)
- --no-snippets
  - Disable content previews

Description:
- Displays documents with comprehensive metadata including names, types, sizes, content snippets, tags, and timestamps.
- Rich table format shows NAME, TYPE, SIZE, SNIPPET, TAGS, and WHEN columns.
- The --recent flag filters to the N most recent documents before applying other sorting options.
- Content snippets provide quick previews of document contents.
- Multiple output formats support different use cases from human-readable to machine processing.

Examples:
```
yams list
yams ls  # Using alias
yams list --recent 10  # Show 10 most recent documents
yams list --sort size --reverse  # Sort by size, largest first
yams list --format json
yams ls --format csv --limit 50  # Using alias
yams list --format minimal --offset 20
yams list --no-snippets --show-metadata  # No previews, full metadata
```

### Snapshot Operations (Enhanced in PBI-043)

The `list` command has been enhanced with smart snapshot and file history capabilities:

**List all snapshots:**
```bash
yams list --snapshots
yams list --snapshots --format json
yams list --snapshots --collection myproject
```

**Show file history across snapshots:**
```bash
# Shows timeline of changes (Added, Modified, Renamed) across all snapshots
yams list src/main.cpp

# Output example:
# File: src/main.cpp
# 
# Snapshot History:
# v1.2  2025-10-01 14:30  Modified  hash: abc123...  2.5 KB  (current)
# v1.1  2025-09-22 10:15  Modified  hash: def456...  2.3 KB  
# v1.0  2025-09-15 09:00  Added     hash: ghi789...  2.1 KB
```

**Show file at specific snapshot:**
```bash
yams list src/main.cpp --snapshot-id=v1.0

# Output shows metadata and suggests next actions:
# File: src/main.cpp (v1.0)
# Size: 2.1 KB
# Hash: ghi789...
# 
# Use 'yams cat src/main.cpp --snapshot-id=v1.0' to view content
# Use 'yams list src/main.cpp' to see history across all snapshots
```

**Compare file between snapshots (inline diff):**
```bash
yams list src/main.cpp --snapshot-id=v1.0 --compare-to=v1.1

# Shows inline diff with syntax highlighting:
# File: src/main.cpp
# Comparing: v1.0 → v1.1
# 
# @@ -10,6 +10,8 @@
#  int main(int argc, char** argv) {
# +    // Initialize logging
# +    initLogger();
#      processArgs(argc, argv);
#      return runApp();
#  }
```

**Smart behavior:**
- `yams list` → List all documents
- `yams list --snapshots` → List all available snapshots
- `yams list <filepath>` → Show file history across snapshots
- `yams list <filepath> --snapshot-id=X` → Show file at snapshot X
- `yams list <filepath> --snapshot-id=X --compare-to=Y` → Inline diff

The command automatically detects file paths (contain `/` or `.`) and adapts its behavior accordingly.

---

## diff {#cmd-diff}

Compare two snapshots and show file changes with tree diff.

**New in PBI-043:** Efficient snapshot comparison using Merkle tree diffs with rename detection.

Synopsis:
- yams diff <snapshotA> <snapshotB> [options]

Options:
- --format <format>
  - Output format: tree (default) | flat | json
  - tree: Structured diff with rename detection (recommended)
  - flat: Legacy flat diff without rename tracking
  - json: Machine-readable output for tools
- --include <patterns>
  - Include only files matching patterns (comma-separated globs)
  - Example: --include="*.cpp,*.h"
- --exclude <patterns>
  - Exclude files matching patterns (comma-separated globs)
  - Example: --exclude="*.log,*.tmp,build/**"
- --type <change-type>
  - Filter by change type: added | deleted | modified | renamed
  - Example: --type=modified (show only modified files)
- --stats
  - Show only summary statistics (no file list)
- --no-renames
  - Disable rename detection (faster, but less accurate)
- -v, --verbose
  - Show detailed information including file hashes

Description:
- Compares two snapshots and displays changes using efficient tree diff algorithm.
- **Rename detection:** Automatically detects moved/renamed files by matching content hashes.
- **O(log n) subtree matching:** Unchanged subtrees are skipped for performance.
- Changes are grouped by type: Added, Deleted, Modified, Renamed, Moved.
- Tree format is the default and recommended for most use cases.

Examples:
```bash
# Basic diff
yams diff v1.0 v1.1

# Output example:
# Comparing v1.0 → v1.1
# 
# Added (6 files):
#   + src/new-feature.cpp
#   + src/new-feature.h
#   + tests/test_new_feature.cpp
#   + docs/new-feature.md
#   + lib/dependency.so
#   + config/feature-flags.json
# 
# Deleted (2 files):
#   - src/deprecated.cpp
#   - src/deprecated.h
# 
# Modified (3 files):
#   ~ src/main.cpp                 (2.3 KB → 2.5 KB)
#   ~ README.md                     (1.1 KB → 1.3 KB)
#   ~ Makefile                      (845 B → 920 B)
# 
# Renamed (1 file):
#   → src/utils.cpp → src/common/utils.cpp
# 
# Summary:
#   +6 files added
#   -2 files deleted
#   ~3 files modified
#   →1 file renamed
#   Total: 12 changes

# Filter by file type
yams diff v1.0 v1.1 --include="*.cpp,*.h"

# Show only modified files
yams diff v1.0 v1.1 --type=modified

# Show summary statistics only
yams diff v1.0 v1.1 --stats

# Output example (stats):
# Snapshot Comparison: v1.0 → v1.1
#   Files added:    6
#   Files deleted:  2
#   Files modified: 3
#   Files renamed:  1
#   Total changes:  12

# JSON output for tools
yams diff v1.0 v1.1 --format=json

# Legacy flat diff (convenience flag)
yams diff v1.0 v1.1 --flat-diff
# Or equivalently:
yams diff v1.0 v1.1 --format=flat

# Compare specific directory
yams diff v1.0 v1.1 --include="src/**"

# Exclude build artifacts
yams diff v1.0 v1.1 --exclude="build/**,*.o,*.so"

# Verbose output with hashes
yams diff v1.0 v1.1 -v
```

**Performance Notes:**
- Tree diff is computed in O(log n) time when trees are pre-built during `yams add`.
- If trees don't exist, they will be computed on-demand (may be slower for large snapshots).
- Use `--no-renames` to skip rename detection for faster results on very large diffs.

**See Also:**
- `yams list --snapshots` - List available snapshots
- `yams list <file>` - Show file history across snapshots
- `yams add --snapshot-id` - Create snapshots

---

## search {#cmd-search}
Note: Default search type is hybrid. When strict/hybrid returns zero results, the CLI auto-retries with fuzzy (similarity 0.7).

YAMS-first code search
Always use YAMS to search the indexed codebase (no external grep/find/rg).

Examples:
```bash
# List only file paths for efficient context
yams search "IndexingPipeline" --paths-only

# Fuzzy search for broader discovery
yams search "vector database" --fuzzy --similarity 0.7 --paths-only

# Restrict by extension via keyword filters
yams search "class DocumentIndexer ext:.cpp" --paths-only

# Show JSON for scripting
yams search "SyncManager" --json

# Queries that start with '-' (option-looking) — pass safely
yams search -q "--start-group --whole-archive --end-group -force_load Darwin" --paths-only
yams search -- "--start-group --whole-archive --end-group -force_load Darwin" --paths-only
printf '%s\n' "--start-group --whole-archive --end-group -force_load Darwin" | yams search --stdin --paths-only
yams search --query-file /tmp/query.txt --paths-only
yams search --query-file - --paths-only < /tmp/query.txt
```

Hints:
- Prefer hybrid or fuzzy search for exploratory queries; narrow with exact keywords as you iterate.
- Combine with --paths-only to feed subsequent yams get calls.
- Session helpers (experimental): Use `yams session pin|list|unpin|warm` to manage hot data. See PROMPT docs for examples: ../PROMPT-eng.md and ../PROMPT.md

Search for documents with advanced query capabilities.

Synopsis:
- yams search "<query>" [options]
- yams search -q "<query>" [options]
- yams search --stdin [options]
- yams search --query-file <path> [options]

Options:
- -q, --query <text>
  - Provide the query as an option (useful when it starts with '-')
- --stdin
  - Read query from standard input when no query is provided
- --query-file <path>
  - Read query from a file path (use '-' to read from stdin)
- -l, --limit <number>
  - Maximum number of results to return (default: 20)
- -t, --type <type>
  - Search type: keyword | semantic | hybrid (default: hybrid)
- -f, --fuzzy
  - Enable fuzzy search for approximate matching
- --similarity <value>
  - Minimum similarity for fuzzy search, 0.0-1.0 (default: 0.7)
- --hash <hash>
  - Search by file hash (full or partial, minimum 8 characters)
- -n, --line-numbers
  - Show line numbers with matches
- -A, --after <N>
  - Show N lines after match (default: 0)
- -B, --before <N>
  - Show N lines before match (default: 0)
- -C, --context <N>
  - Show N lines before and after match (default: 0)

Description:
- Supports both exact keyword searches and fuzzy approximate matching.
- Fuzzy search uses BK-tree indexing for efficient similarity matching with configurable thresholds.
- Full-text search with FTS5 indexing provides fast document content queries with robust special character handling.
- Hash search allows finding documents by their SHA256 hash (full 64-character hash or partial prefix).
- Automatic fallback: if FTS5 query fails (due to special characters), automatically falls back to fuzzy search.
- Robust query sanitization handles special characters like hyphens, quotes, and operators (e.g., "PBI-6", "task 4-").
- JSON output includes relevance scores, execution times, and content snippets.
- Searches document names, content, tags, and metadata fields.
- Auto-detects hash format: if query looks like a hash (8-64 hex chars), automatically searches by hash.
- Verbosity control: concise output by default, detailed output with --verbose flag.

Examples:
```
# Text search
yams search "database performance"
yams search "config file" --fuzzy --similarity 0.6
yams search "meeting notes" --limit 10 --type keyword
yams search "project roadmap" --json

# Hash search
yams search --hash abcd1234ef567890  # Partial hash (minimum 8 chars)
yams search --hash abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890  # Full hash
yams search abcd1234ef567890  # Auto-detected hash search

# Verbose output for detailed information
yams search "query" --verbose
yams search --hash abcd1234 --verbose
```

---

## grep {#cmd-grep}

Regex across indexed code (YAMS only, hybrid by default)
Use YAMS grep for project-wide regex; avoid system utilities for repository queries.

Examples:
```bash
# Find class definitions
yams grep "class\\s+IndexingPipeline" --include="**/*.hpp,**/*.cpp"

# Locate TODOs in headers and sources
yams grep "TODO\\b" --include="**/*.hpp,**/*.h,**/*.cpp"

# Search for CLI subcommand declarations
yams grep "##\\s+(watch|git|sync)\\b" --include="**/*.md"
```

Notes:
- Default mode is hybrid: regex plus semantic suggestions; use --regex-only to disable. Tune with --semantic-limit (default: 10).
- Semantic suggestions are also shown in -l/--files-with-matches, -L/--files-without-match, --paths-only, and -c/--count modes. Regex counts only reflect text matches.
- Hot/Cold modes: control behavior via environment variables. For grep, set YAMS_GREP_MODE=hot_only|cold_only|auto (hot uses extracted text cache; cold scans CAS bytes). For list, use YAMS_LIST_MODE, and for retrieval (cat/get), use YAMS_RETRIEVAL_MODE.
- --include accepts comma-separated globs or repeated usage; prefer quoting patterns.
- Pair with yams search --paths-only to scope subsequent grep runs.

Search for regex patterns within file contents.

Synopsis:
- yams grep <pattern> [paths...] [options]

Options:
- -A, --after <N>
  - Show N lines after match (default: 0)
- -B, --before <N>
  - Show N lines before match (default: 0)
- -C, --context <N>
  - Show N lines before and after match (default: 0)
- -i, --ignore-case
  - Case-insensitive search
- -w, --word
  - Match whole words only
- -v, --invert
  - Invert match (show non-matching lines)
- -n, --line-numbers
  - Show line numbers
- -H, --with-filename
  - Show filename with matches
- --no-filename
  - Never show filename
- -c, --count
  - Show only count of matching lines
- -l, --files-with-matches
  - Show only filenames with matches
- -L, --files-without-match
  - Show only filenames without matches
- --color <mode>
  - Color mode: always, never, auto (default: auto)
- --max-count <N>
  - Stop after N matches per file
- --limit <N>
  - Alias for --max-count

Description:
- Searches through the content of all indexed files using regular expressions.
- Supports standard grep-like options for context, case sensitivity, and output control.
- Automatically highlights matches when outputting to a terminal.
- Can search all indexed files or specific paths.
- Uses ECMAScript regex syntax for pattern matching.

Examples:
```
# Basic pattern search
yams grep "TODO"
yams grep "error.*failed" -i

# With context
yams grep "function" -C 2
yams grep "class.*Repository" -A 5

# File listing
yams grep "deprecated" -l
yams grep "test" src/ -c

# Complex patterns
yams grep "^import.*from" -n
yams grep "\bclass\s+\w+Command\b" --color=always
```

---

## config {#cmd-config}

Manage configuration.

Subcommands:
- get <key>
  - Print a configuration value.
- set <key> <value>
  - Update a configuration value.
- list
  - Print all keys/values.
- validate [--config-path <file>]
  - Validate configuration.
- export [--format toml|json]
  - Output current configuration.

Examples:
```
yams config list
yams config get core.data_dir
yams config set core.storage_engine local
yams config validate
yams config export --format json
```

---

## auth {#cmd-auth}

Manage authentication material (keys, tokens, API keys). Availability and exact options depend on your build.

Examples:
```
yams auth --help
```

---

## stats {#cmd-stats}

Show storage statistics and health.

Synopsis:
- yams stats [options]

Description:
- Adaptive default view: when relevant, includes Recommendations and Service Status along with the compact footer; use -v for full System Health and detailed sections.
- Telemetry includes auto-repair counters (repair_queue_depth, repair_batches_attempted, repair_embeddings_generated, repair_failed_operations), latency percentiles (p50/p95), top_slowest components, and not_ready flags.
- Use --json for machine-readable output.

Examples:
```
yams stats
yams stats -v
yams stats --json
```

---

## uninstall {#cmd-uninstall}

Remove YAMS data/config from the system.

Synopsis:
- yams uninstall [--force]

Warning:
- This is destructive. Make sure you have backups.

Examples:
```
yams uninstall
yams uninstall --force
```

---

## migrate {#cmd-migrate}

Run metadata migrations if any are pending.

Synopsis:
- yams migrate

Examples:
```
yams migrate
```

---

## update {#cmd-update}

Update metadata for existing documents.

Synopsis:
- yams update <hash> --metadata <key=value>...
- yams update --name <name> --metadata <key=value>...

Options:
- hash
  - Document hash (full or partial) to update
- --name <name>
  - Document name to update (useful for stdin documents)
- -m, --metadata <key=value>
  - Metadata key-value pairs to set (can be specified multiple times)
- -v, --verbose
  - Show detailed update information

Notes:
- Either hash or --name must be specified, but not both
- Multiple metadata pairs can be updated in a single command
- Existing metadata values will be overwritten
- Works with both file-based and stdin documents
- If multiple documents have the same name, you'll be prompted to use the hash

Examples:
```
# Update by hash
yams update fc8fc5fa --metadata "status=completed" --metadata "reviewed=true"

# Update by name  
yams update --name "project-notes.md" --metadata "priority=high"

# Update stdin document
yams update --name "arxiv-2402.05391" --metadata "read=true" --metadata "rating=5"

# Task tracking example
yams update --name "task-001.md" --metadata "status=in_progress" --metadata "assignee=alice"
```

---

## browse {#cmd-browse}

Interactive terminal UI to browse, search, preview, and manage documents.

Synopsis:
- yams browse

### Modes

- Normal mode (default)
  - Navigate collections, documents, and the preview pane.
  - Enter opens the full-screen viewer; : enters command mode; / enters search mode.
  - Tab cycles focus across columns (Collections → Documents → Preview).
- Search mode (/)
  - Incremental filter; Ctrl-f toggles fuzzy/exact; Enter applies; Esc cancels.
- Command mode (: or p)
  - Execute commands like :q, :open, :help, :hex, :text, :refresh; Enter runs; Esc cancels.
- Viewer mode (full-screen)
  - View the selected document’s content with robust scrolling; q/Esc closes.

### Keybindings (Normal mode)

- Navigation
  - j / k or ArrowDown / ArrowUp: Move selection up/down in the active column
  - g / G: Jump to top/bottom
  - PageDown / PageUp: Page movement (documents or preview, depending on focus)
  - Ctrl-d / Ctrl-u: Half-page down/up
  - h / l or ArrowLeft / ArrowRight: Move column focus left/right
  - Tab: Cycle focus (Collections → Documents → Preview → Collections)
- Actions
  - Enter: Open full-screen viewer for the selected document
  - o: Open selected document in external pager ($PAGER or less -R)
  - x: Toggle preview mode between hex and text (Auto mode available via :auto)
  - r: Refresh the lists
  - d then D: Delete selected (confirmation)
  - ?: Toggle help
  - q / Esc: Quit TUI (or close viewer if open)

### Search (incremental)

- /: Enter search mode
- Typing filters results immediately
- Ctrl-f: Toggle fuzzy vs exact search
- Enter: Apply filter and exit search mode
- Esc: Cancel search mode and clear query

### Command prompt (vi-like)

Enter command mode with : or p. Supported commands:
- :q, :quit, :wq
  - Exit the TUI
- :help
  - Show help overlay
- :open or :pager
  - Open the selected document in external pager ($PAGER or less -R)
- :hex
  - Switch preview mode to hex
- :text
  - Switch preview mode to text
- :auto
  - Switch preview mode to auto (try text, fallback to “binary” notice)
- :refresh
  - Refresh documents/collections

Notes:
- Unknown commands show a status message with the command name.
- Commands can be chained across sessions; state persists while the TUI is running.

### Viewer mode (full-screen)

- j / k or ArrowDown / ArrowUp: Line scroll
- PageDown / PageUp: Page scroll
- Ctrl-d / Ctrl-u: Half-page scroll
- g / G: Jump to top/bottom
- x: Toggle hex/text (viewer content rebuilt accordingly)
- q / Esc: Close viewer and return to browse

### Preview modes

- Auto (default): Prefer text from metadata; fallback to bytes (if it looks like text). If content looks binary, shows a “Binary content. Preview unavailable.” notice.
- Text: Force text-only; binary-looking content shows a notice.
- Hex: Hex dump with ASCII gutter for safe inspection of binary data.

Switching modes
- Global toggle:
  - x switches between hex and text
- Explicit via command mode:
  - :hex, :text, :auto

### External pager integration

- o or :open uses $PAGER if set; otherwise falls back to less -R
- The TUI temporarily exits, runs the pager, then resumes with state intact and a status message
- Pager receives either extracted text (preferred) or raw bytes (cap applied)

### Tips

- Keep your hands on the home row with j/k/g/G for navigation and : for commands
- Use fuzzy search (Ctrl-f) to quickly locate documents by approximate name
- Use hex mode (x) when previewing binary files

Features:
- Real-time document browsing with metadata integration
- Fuzzy search mode with BK-tree approximate matching
- Live filtering and search result highlighting
- Document preview and metadata display
- Interactive navigation and selection

Common keybindings:
- j/k or ArrowUp/ArrowDown: navigate
- h/l or ArrowLeft/ArrowRight: switch columns
- /: search
- Enter: open viewer
- o: open in external pager ($PAGER, fallback to less -R)
- r: refresh
- d: delete (confirmation)
- q or Esc: quit

Examples:
```
yams browse
```

---

## model {#cmd-model}

Manage embedding models: list installed, download new, inspect, and verify runtime.

Synopsis:
- yams model list
- yams model download <name> [--url <onnx_url>]
- yams model info <name>
- yams model check

Description:
- list: Shows locally available models (autodiscovers ~/.yams/models/<name>/model.onnx and other configured roots)
- download: Fetches a model by name; use --url to override with a custom ONNX URL
- info: Prints details for a model (dimensionality, path, notes when available)
- check: Verifies ONNX runtime support, plugin directory status, and autodiscovery paths

Configuring the preferred model:
- yams config embeddings model <name>  # sets embeddings.preferred_model
- embeddings.model_path can point to the models root; name-based resolution order:
  configured root → ~/.yams/models → models/ → /usr/local/share/yams/models

Examples:
```
yams model list
yams model download all-MiniLM-L6-v2
yams model download my-custom --url https://example.com/models/custom.onnx
yams model info all-MiniLM-L6-v2
yams model check
yams config embeddings model all-MiniLM-L6-v2
```

---

## plugin {#cmd-plugin}

Manage plugins: scanning, trust policy, load/unload, and info.

Synopsis:
- yams plugin list
- yams plugin scan [--dir DIR] [TARGET]
- yams plugin info <name>
- yams plugin load <path|name> [--config FILE] [--dry-run]
- yams plugin unload <name>
- yams plugin trust add <path> | list | remove <path>

Description:
- list: Show loaded plugins and basic details (name/path).
- scan: Inspect default directories (or a given dir/target file) for plugins without initializing them; prints name/version/ABI and interfaces.
- info: Show manifest/health JSON for a loaded plugin.
- load: Load a plugin by absolute path or name (resolved in discovery paths); respects trust policy; `--dry-run` scans only.
- unload: Unload a plugin by name.
- trust: Manage the trust policy file `~/.config/yams/plugins_trust.txt`.

Notes:
- Default discovery order: `YAMS_PLUGIN_DIR` (exclusive if set), `$HOME/.local/lib/yams/plugins`, `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`, and `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`.
- The daemon prefers host‑backed `model_provider_v1` when an ONNX plugin is trusted/loaded; otherwise it falls back to the legacy registry or mock/null provider (see README for env toggles).
- Disable plugin subsystem: start the daemon with `--no-plugins`.

Examples:
```
# discover and trust a system plugins directory
yams plugin scan
yams plugin trust add /usr/local/lib/yams/plugins
yams plugin trust list

# load ONNX plugin by path (respects trust policy)
yams plugin load /usr/local/lib/yams/plugins/libyams_onnx_plugin.so

# inspect and unload
yams plugin info onnx
yams plugin unload onnx
```

---

## doctor {#cmd-doctor}

Diagnose daemon connectivity, model/provider readiness, vector DB dimensions, and plugins. Includes repair helpers.

Synopsis:
- yams doctor [--fix] [--fix-config-dims] [--recreate-vectors [--dim N]] [--stop-daemon]
- yams doctor daemon
- yams doctor plugin [<path|name>] [--iface <id>] [--iface-version <N>] [--no-daemon]
- yams doctor plugins
- yams doctor repair [--embeddings] [--fts5] [--graph] [--all]

Highlights:
- Summary shows daemon status, vector DB dimension vs model target, and loaded plugins.
- Knowledge Graph section appears when available; if empty, recommends:
  `yams doctor repair --graph` to build from tags/metadata.

Repair options:
- --embeddings: Generate missing document embeddings (daemon streaming when available; local fallback).
- --fts5: Rebuild the text index best-effort.
- --graph: Construct/repair the knowledge graph from tags/metadata.
- --all: Run all repair operations.

Examples:
```
yams doctor
yams doctor plugin onnx
yams doctor repair --graph
yams doctor --recreate-vectors --dim 768 --stop-daemon
```

---

## repair {#cmd-repair}

Run storage/database maintenance and (optionally) embedding generation.

Synopsis:
- yams repair [--orphans] [--chunks] [--mime] [--optimize] [--all] [--dry-run] [--force]
- yams repair --embeddings [--include-mime <mime>] [--limit <N>]

Description:
- --orphans: Clean orphaned metadata entries
- --chunks: Remove orphaned chunk files and reclaim space
- --mime: Fix missing MIME types in documents
- --optimize: Vacuum/optimize the database
- --all: Run all non-embedding repair operations
- --dry-run: Preview operations without making changes
- --force: Skip confirmations
- --embeddings: Generate missing embeddings for eligible documents
- --include-mime <mime>: Opt-in additional MIME types (e.g., application/pdf) for embeddings

Notes:
- By default, embedding repair targets text-like MIME types only; binaries (PDFs/images) are skipped unless explicitly included with --include-mime.
- PDF text extraction and embedding depend on build configuration (PDF support must be available).

Examples:
```
yams repair --all --dry-run
yams repair --orphans --chunks --optimize --force
yams repair --embeddings
yams repair --embeddings --include-mime application/pdf
```

---

## status {#cmd-status}

Show service readiness, subsystem health, and runtime stats (daemon-aware).

Synopsis:
- yams status

Description:
- Prints a concise services summary (e.g., ✓ Content | ✓ Repo | ✓ Search | ⚠ (N) Models)
- During initialization, shows a WAIT line with not-ready components and progress
- Exposes fields such as running/ready state, uptime, request counts, active connections, memory, CPU, and version when available
- Highlights top slowest components and actionable recommendations when relevant

Tips:
- Use yams stats -v for detailed system health, recommendations, and service status sections.
- Bootstrap status file: ~/.local/state/yams/yams-daemon.status.json

Examples:
```
yams status
```

---

## daemon {#cmd-daemon}

Control the background daemon (if included in your build).

Synopsis:
- yams daemon start
- yams daemon stop
- yams daemon status
- yams daemon restart

Description:
- start/stop/restart the daemon process
- status shows readiness and subsystem overview similar to yams status

Notes:
- When client/daemon protocol versions differ, a one-time warning may be shown; upgrade the daemon if features appear limited.

Examples:
```
yams daemon status
yams daemon restart
```

---

## serve (conditional) {#cmd-serve}

Start the MCP (Model Context Protocol) server over stdio (only when built with YAMS_BUILD_MCP_SERVER=ON).

Synopsis:
- yams serve

Options:
- None. Stdio transport only.

Description:
- Exposes YAMS functionality through the Model Context Protocol for AI tool integration.
- Uses stdio transport for direct AI integration.
- Provides search, retrieval, and document management capabilities to AI systems.
- Graceful shutdown on SIGINT/SIGTERM signals.

Examples:
```
yams serve
```

---

## completion {#cmd-completion}

Generate shell completion scripts for popular shells.

Synopsis:
- yams completion bash
- yams completion zsh
- yams completion fish

Notes:
- Bash: if bash-completion isn’t installed, a minimal fallback is baked into the script.
- Zsh: the generated script auto-runs compinit if needed to prevent “_arguments: command not found”.
- Fish: installs via standard fish completions.

Examples:
```
# Quick use without installing
source <(yams completion bash)

# Install for current user
yams completion bash > ~/.local/share/bash-completion/completions/yams
yams completion zsh  > ~/.local/share/zsh/site-functions/_yams
yams completion fish > ~/.config/fish/completions/yams.fish
```

## Exit Codes

- 0  Success.
- 1  General error (unexpected exception or failure).
- Non-zero values may indicate specific errors depending on the subcommand.

---

## Snapshot Workflows

**New in PBI-043:** Efficient snapshot management with Merkle tree diffs and rename detection.

### Overview

Snapshots provide point-in-time captures of your repository content. YAMS automatically builds Merkle trees during `yams add --snapshot-id` operations, enabling fast O(log n) diff comparisons between snapshots.

### Basic Workflow

**1. Initial import (automatic snapshot):**
```bash
# Capture current state of source code
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.md" \
  --label "Initial release" \
  --tags "code,release"

# Snapshot created automatically: 2025-10-01T09:00:00.000Z
```

**2. Make changes to your codebase:**
```bash
# Edit files, add features, fix bugs...
vim src/main.cpp
git commit -am "Added new feature"
```

**3. Capture updated state (automatic snapshot):**
```bash
# Just add again - snapshot created automatically
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.md" \
  --label "Feature release" \
  --tags "code,release"

# New snapshot: 2025-10-01T14:30:00.000Z
```

**4. Compare snapshots:**
```bash
# List snapshots to find timestamp IDs
yams list --snapshots

# Compare using timestamp IDs (fast O(log n) tree diff)
yams diff 2025-10-01T09:00:00.000Z 2025-10-01T14:30:00.000Z

# Output example:
# Comparing v1.0 → v1.1
# 
# Added (3 files):
#   + src/new-feature.cpp
#   + src/new-feature.h
#   + tests/test_new_feature.cpp
# 
# Modified (2 files):
#   ~ src/main.cpp                 (2.3 KB → 2.5 KB)
#   ~ README.md                     (1.1 KB → 1.3 KB)
# 
# Renamed (1 file):
#   → src/utils.cpp → src/common/utils.cpp
# 
# Summary: 6 changes (+3, ~2, →1)
```

### Snapshot Discovery

**List all snapshots:**
```bash
yams list --snapshots

# Output example (chronological, newest first):
# Snapshot ID                  Label              Collection      Files  Date
# 2025-10-01T14:30:00.000Z    Feature release    -               125    2025-10-01 14:30
# 2025-09-20T10:15:00.000Z    Updated docs       documentation    45    2025-09-20 10:15
# 2025-09-15T09:00:00.000Z    Initial release    -               122    2025-09-15 09:00
```

**Filter by collection:**
```bash
yams list --snapshots --collection=releases
```

**JSON output for tools:**
```bash
yams list --snapshots --format=json | jq '.[] | select(.label | contains("release"))'
```

### File History Tracking

**View file history across snapshots:**
```bash
yams list src/main.cpp

# Output example:
# File: src/main.cpp
# 
# Snapshot History:
# v1.1  2025-10-01 14:30  Modified  hash: abc123...  2.5 KB  (current)
# v1.0  2025-09-15 09:00  Added     hash: def456...  2.3 KB
```

**View file at specific snapshot:**
```bash
# See metadata
yams list src/main.cpp --snapshot-id=v1.0

# View content
yams cat src/main.cpp --snapshot-id=v1.0

# Restore to disk
yams get --name src/main.cpp --snapshot-id=v1.0 -o main-v1.0.cpp
```

**Inline diff between snapshots:**
```bash
yams list src/main.cpp --snapshot-id=v1.0 --compare-to=v1.1

# Shows inline diff with syntax highlighting
```

### Advanced Diff Operations

**Filter by file type:**
```bash
# Show only C++ changes
yams diff v1.0 v1.1 --include="*.cpp,*.hpp"

# Exclude build artifacts
yams diff v1.0 v1.1 --exclude="build/**,*.o"
```

**Filter by change type:**
```bash
# Show only added files
yams diff v1.0 v1.1 --type=added

# Show only renames (useful for tracking file moves)
yams diff v1.0 v1.1 --type=renamed
```

**Summary statistics:**
```bash
yams diff v1.0 v1.1 --stats

# Output:
# Snapshot Comparison: v1.0 → v1.1
#   Files added:    3
#   Files deleted:  0
#   Files modified: 2
#   Files renamed:  1
#   Total changes:  6
```

**JSON output for tools:**
```bash
yams diff v1.0 v1.1 --format=json > diff.json

# Analyze with jq:
jq '.changes[] | select(.type == "modified")' diff.json
```

**Verbose output with hashes:**
```bash
yams diff v1.0 v1.1 -v

# Shows full file hashes for each change
```

### Collections and Organization

**Organize snapshots by collection:**
```bash
# Development snapshots
yams add src/ --recursive --collection=dev --snapshot-id=dev-2025-10-01

# Documentation snapshots
yams add docs/ --recursive --collection=documentation --snapshot-id=docs-v3

# Release snapshots
yams add . --recursive --collection=releases --snapshot-id=v2.0 --snapshot-label="Major release"
```

**Compare within collections:**
```bash
yams list --snapshots --collection=releases
yams diff v1.0 v2.0  # Compare releases
```

### Restoration Workflows

**Restore entire snapshot:**
```bash
# Restore to specific directory
yams restore --snapshot-id=v1.0 --output-directory=./restore-v1.0

# Preview without writing files
yams restore --snapshot-id=v1.0 --output-directory=./test --dry-run

# Overwrite existing files
yams restore --snapshot-id=v1.0 --output-directory=. --overwrite
```

**Selective restoration:**
```bash
# Restore only specific file types
yams restore --snapshot-id=v1.0 \
  --output-directory=./restore \
  --include-patterns="*.cpp,*.h"

# Exclude directories
yams restore --snapshot-id=v1.0 \
  --output-directory=./restore \
  --exclude-patterns="build/**,third_party/**"
```

**Restore by collection:**
```bash
yams restore-collection --collection=releases --output-directory=./releases
```

### Performance Characteristics

**Tree-based diff performance:**
- **Small changes (< 1%)**: O(log n) subtree matching, ~100x faster than flat diff
- **Large changes (> 50%)**: Approaches O(n) but still faster due to hash-based comparison
- **Rename detection**: Hash-based matching is O(n) but only runs on Added/Deleted pairs

**Storage efficiency:**
- Merkle trees are stored once in CAS and deduplicated across snapshots
- Unchanged subtrees share the same tree hash (deduplication)
- Tree overhead: ~5-10% additional storage for large repositories
- Diff computation: ~5-10% slower during `yams add --snapshot-id` (tree building)

**Optimization tips:**
- Use `--exclude` patterns to skip build artifacts, caches, and temporary files
- Use `--no-renames` for faster diff when rename tracking isn't needed
- Pre-build trees during snapshot creation for instant diff operations

### Integration with Git

**Automatic git integration:**
When `yams add` runs in a git repository, snapshot metadata automatically includes:
- `git_commit`: Current commit hash (e.g., `abc123def456`)
- `git_branch`: Current branch name (e.g., `main`, `develop`)
- `git_remote`: Remote repository URL (if configured)

This creates a natural bidirectional link between YAMS snapshots and git history.

**Snapshot on git commit (hook):**
```bash
#!/bin/bash
# Hook: .git/hooks/post-commit
# Automatically snapshot on every commit
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.md" \
  --label "$(git log -1 --pretty=%B | head -1)" \
  --tags "code,git,auto"
```

**Snapshot on git tag:**
```bash
#!/bin/bash
# Hook: .git/hooks/post-tag
TAG=$(git describe --tags --abbrev=0)
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.md" \
  --label="Git tag: $TAG" \
  --tags "code,git,release"
```

**Daily snapshots:**
```bash
#!/bin/bash
# Cron: daily snapshot at midnight
yams add . --recursive \
  --include="*.cpp,*.hpp,*.h,*.md" \
  --label="Daily backup" \
  --tags "code,daily"
# Snapshot ID is auto-generated with current timestamp
```

### Troubleshooting

**Missing trees:**
```bash
# If diff is slow, trees may not exist
# Rebuild trees for existing snapshots:
yams list --snapshots  # Find snapshot IDs
yams doctor repair --snapshots  # Rebuild missing trees (future feature)
```

**Large diff output:**
```bash
# Use filters to reduce output
yams diff v1.0 v2.0 --type=modified --include="src/**"
yams diff v1.0 v2.0 --stats  # Summary only
```

**Restore conflicts:**
```bash
# Preview first
yams restore --snapshot-id=v1.0 --output-directory=./test --dry-run

# Use overwrite cautiously
yams restore --snapshot-id=v1.0 --output-directory=. --overwrite
```

### Best Practices

1. **Use labels liberally:** Labels make snapshot timelines human-readable (`--label "Before refactor"`)
2. **Leverage automatic IDs:** No need to manually manage snapshot IDs - timestamps provide natural ordering
3. **Exclude build artifacts:** Use `--exclude="build/**,*.o,*.so"` to reduce noise and improve performance
4. **Tag meaningful snapshots:** Use `--tags` for filtering (e.g., `release`, `milestone`, `backup`)
5. **Collections for organization:** Group related content (`--collection=releases`, `--collection=docs`)
6. **Automate with git hooks:** Snapshot on every commit or tag for complete history tracking
7. **Test restores:** Verify snapshots with `--dry-run` before overwriting files
8. **Trust the timestamps:** Snapshot IDs are ISO 8601 timestamps - sort naturally and are globally unique

## Tips

YAMS-first workflow (until watch/track is available)
- Always search the codebase with YAMS (search/grep). Do not use system grep/find/rg for repository queries.
- After editing code, re-index affected files or directories via yams add (pre-watch workflow).
- Use --paths-only for path lists you can pipe into further commands or editors.
- Prefer --include with comma-separated patterns (e.g., "*.cpp,*.hpp,*.h") to bound searches and indexing.
- Retrieve exact files for review with yams get --name <path> -o <dest>.
- Keep tags consistent (e.g., code,source; code,headers; code,working) and update metadata with yams update as needed.

- Use --json where supported to integrate with scripts and tools.
- Specify a storage directory explicitly with --storage or via YAMS_STORAGE to keep data separate for testing vs production.
- When available, use --help --verbose for detailed per-command help, or yams --help-all for the full reference in the terminal.
