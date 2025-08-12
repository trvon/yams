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

- YAMS_STORAGE
  - Storage root directory. Overrides defaults and the --storage/--data-dir flag when set.
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
- cat
- delete
- list
- search
- config (subcommands: get, set, list, validate, export)
- auth
- stats
- uninstall
- migrate
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

Description:
- Ingests the specified file or standard input and stores it in the content-addressed store.
- Rich metadata support for tagging, naming, and custom properties.
- Content is automatically indexed for full-text and fuzzy search.

Examples:
```
yams add ./README.md
cat notes.txt | yams add - --name "meeting-notes" --tags "work,meeting"
yams add document.pdf --tags "research,papers" --metadata "author=John Doe"
echo "Quick note" | yams add - --name "reminder.txt" --mime-type "text/plain"
```

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

Description:
- Displays documents with comprehensive metadata including names, types, sizes, content snippets, tags, and timestamps.
- Rich table format shows NAME, TYPE, SIZE, SNIPPET, TAGS, and WHEN columns.
- Content snippets provide quick previews of document contents.
- Multiple output formats support different use cases from human-readable to machine processing.

Examples:
```
yams list
yams ls  # Using alias
yams list --format json
yams ls --format csv --limit 50  # Using alias
yams list --format minimal --offset 20
```

---

## search {#cmd-search}

Search for documents with advanced query capabilities.

Synopsis:
- yams search "<query>" [options]

Options:
- -l, --limit <number>
  - Maximum number of results to return (default: 20)
- -t, --type <type>
  - Search type: keyword | semantic | hybrid (default: keyword)
- -f, --fuzzy
  - Enable fuzzy search for approximate matching
- --similarity <value>
  - Minimum similarity for fuzzy search, 0.0-1.0 (default: 0.7)
- --hash <hash>
  - Search by file hash (full or partial, minimum 8 characters)

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
- Prints health or usage information about your store. Use --json where supported for machine-readable output.

Examples:
```
yams stats
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

## serve (conditional) {#cmd-serve}

Start the MCP (Model Context Protocol) server with multiple transport options (only when built with YAMS_BUILD_MCP_SERVER=ON).

Synopsis:
- yams serve [options]

Options:
- -t, --transport <type>
  - Transport type: stdio | websocket (default: stdio)
- -p, --port <number>
  - WebSocket port when using websocket transport (default: 8080)
- --host <address>
  - WebSocket host address (default: 127.0.0.1)
- --path <path>
  - WebSocket path (default: /mcp)
- --ssl
  - Use TLS for WebSocket connections (wss://)

Description:
- Exposes YAMS functionality through the Model Context Protocol for AI tool integration.
- Supports both stdio transport (for direct AI integration) and WebSocket transport (for network access).
- Provides search, retrieval, and document management capabilities to AI systems.
- Graceful shutdown on SIGINT/SIGTERM signals.

Examples:
```
yams serve                                    # stdio transport
yams serve --transport websocket              # WebSocket on default port
yams serve -t websocket -p 9000 --ssl        # Secure WebSocket on port 9000
yams serve --transport websocket --host 0.0.0.0 --port 8080  # Network accessible
```

---

## Exit Codes

- 0  Success.
- 1  General error (unexpected exception or failure).
- Non-zero values may indicate specific errors depending on the subcommand.

## Tips

- Use --json where supported to integrate with scripts and tools.
- Specify a storage directory explicitly with --storage or via YAMS_STORAGE to keep data separate for testing vs production.
- When available, use --help --verbose for detailed per-command help, or yams --help-all for the full reference in the terminal.
