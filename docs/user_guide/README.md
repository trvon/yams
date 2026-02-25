# YAMS User Guide

Command-line interface for content-addressed storage with semantic search.

## Quick Start

```bash
# Initialize storage (interactive - prompts for grammar downloads)
yams init

# Or auto mode for containers/headless
yams init --auto

# Or specify custom location
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init --non-interactive

# Init also bootstraps a per-project session (scoping + watch) unless
# YAMS_DISABLE_PROJECT_SESSION=1.

# Enable auto-ingest for an existing project
yams watch

# Add content
yams add ./README.md --tags docs
yams add src/ --recursive --include="*.cpp,*.h" --tags code
echo "note" | yams add - --name "quick-note.txt"

# Search and retrieve
yams search "config file" --limit 5
yams grep "TODO" --include="*.cpp"
yams list --limit 20
yams get <hash> -o ./output.bin

# System status
yams status
yams doctor
```

## Documentation

| Topic | File | Description |
|-------|------|-------------|
| Installation | [installation.md](./installation.md) | Install via Homebrew, Docker, or source |
| CLI Reference | [cli.md](./cli.md) | All commands and options |
| MCP Server | [mcp.md](./mcp.md) | Model Context Protocol integration |
| Embeddings | [embeddings.md](./embeddings.md) | Vector search and model setup |
| Tutorials | [tutorials](./tutorials/README.md) | Task-oriented guides |
| Operations | [../admin/operations.md](../admin/operations.md) | Tuning, maintenance, configuration |

## Key Commands

| Command | Description |
|---------|-------------|
| `yams init` | Initialize storage (interactive or `--auto`) |
| `yams add` | Add files/directories (`--recursive`, `--include`, `--tags`) |
| `yams search` | Hybrid search (keyword + semantic) |
| `yams grep` | Regex search with semantic suggestions |
| `yams list` | List documents (`--snapshots`, `--format json`) |
| `yams get` | Retrieve by hash or name |
| `yams diff` | Compare snapshots (Merkle tree diff) |
| `yams watch` | Enable auto-ingest for a project session |
| `yams serve` | Start MCP server (stdio) |
| `yams doctor` | Diagnose issues |
| `yams repair` | Fix storage/embeddings |

## Build Options (Meson)

| Option | Default | Description |
|--------|---------|-------------|
| `build-cli` | `true` | Build CLI binary |
| `build-mcp-server` | `true` | Build MCP server |
| `build-tests` | `false` | Build test suite |
| `plugin-pdf` | `false` | PDF text extraction |
| `plugin-onnx` | `true` | ONNX embedding models |
| `plugin-symbols` | `true` | Tree-sitter symbol extraction |
| `plugin-s3` | `true` | S3 object storage |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `YAMS_STORAGE` | Storage root directory (overrides default) |
| `XDG_DATA_HOME` | Default storage location (`~/.local/share/yams`) |
| `XDG_CONFIG_HOME` | Config location (`~/.config/yams`) |
| `YAMS_PLUGIN_DIR` | Plugin search directory |
| `YAMS_DAEMON_SOCKET` | Daemon socket path override |
| `YAMS_EMBEDDED` | Daemon transport mode: `1/true` in-process, `0/false` socket, `auto` probe |

## Common Patterns

```bash
# JSON output for scripting
yams list --format json | jq '.'
yams search "query" --limit 10 --json

# Per-project storage
YAMS_STORAGE="$PWD/.yams" yams init --non-interactive
YAMS_STORAGE="$PWD/.yams" yams add ./src/

# Auto-ingest for the current project
yams watch --interval 2000

# Batch indexing with tags
yams add src/ --recursive --include="*.cpp,*.hpp,*.h" --tags "code,source"
yams add docs/ --recursive --include="*.md" --tags "docs"

# Snapshot workflow
yams add . --recursive --include="*.cpp,*.h" --snapshot-label "v1.0"
yams list --snapshots
yams diff v1.0 v1.1 --stats

# MCP server for AI assistants
yams serve
```

## Embedding Models

| Model | Dimensions | Notes |
|-------|------------|-------|
| `all-MiniLM-L6-v2` | 384 | Recommended: fast, lightweight |
| `all-mpnet-base-v2` | 768 | Higher quality, slower |

```bash
# Download and configure a model
yams model download all-MiniLM-L6-v2
yams model check

# Set preferred model
yams config set embeddings.preferred_model all-MiniLM-L6-v2
```

## Symbol Extraction

Extract functions, classes, and methods from source code:

```bash
# Download grammars
yams config grammar list
yams config grammar download cpp python rust

# Symbols are extracted automatically on add
yams add src/ --recursive --include="*.cpp,*.py,*.rs"
```

**Supported:** C, C++, Python, JavaScript, TypeScript, Rust, Go, Java, C#, PHP, Kotlin, Dart, SQL, Solidity

## Plugins

```bash
yams plugin list                              # List loaded
yams plugin trust add ~/.local/lib/yams/plugins
yams plugin health                            # Check status
yams doctor plugin onnx                       # Diagnose
```

## Troubleshooting

```bash
yams doctor              # Full diagnostics
yams doctor --json       # JSON output
yams stats --verbose     # Storage statistics
yams repair --all        # Fix common issues
yams repair --embeddings # Generate missing embeddings
```

## Links

- **Docs:** [yamsmemory.ai](https://yamsmemory.ai)
- **GitHub:** [github.com/trvon/yams](https://github.com/trvon/yams)
- **Discord:** [discord.gg/rTBmRHdTEc](https://discord.gg/rTBmRHdTEc)
