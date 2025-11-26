# YAMS User Guide

Command-line interface for content-addressed storage with semantic search.

## Quick Start

```bash
# Initialize storage
yams init --non-interactive

# Add content
yams add ./README.md
echo "note" | yams add - --name "quick-note.txt"

# Search and retrieve
yams search "config file" --limit 5
yams list --limit 20
yams get <hash> -o ./output.bin

# System status
yams status
yams doctor
```

## Documentation

| Topic | File | Description |
|-------|------|-------------|
| Installation | [installation.md](./installation.md) | Install from Homebrew or source |
| CLI Reference | [cli.md](./cli.md) | All commands and options |
| MCP Server | [mcp.md](./mcp.md) | Model Context Protocol integration |
| Embeddings | [embeddings.md](./embeddings.md) | Vector search and model setup |
| Operations | [../admin/operations.md](../admin/operations.md) | Tuning, maintenance, configuration |

## Build Options (Meson)

| Option | Default | Description |
|--------|---------|-------------|
| `build-cli` | `true` | Build CLI binary |
| `build-mcp-server` | `true` | Build MCP server |
| `build-tests` | `false` | Build test suite |
| `enable-pdf` | `enabled` | PDF text extraction |
| `enable-onnx` | `enabled` | ONNX embedding models |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `YAMS_STORAGE` | Storage root directory (overrides default) |
| `XDG_DATA_HOME` | Default storage location (`~/.local/share/yams`) |
| `XDG_CONFIG_HOME` | Config location (`~/.config/yams`) |
| `YAMS_PLUGIN_DIR` | Plugin search directory |

## Common Patterns

```bash
# JSON output for scripting
yams list --format json | jq '.'
yams search "query" --limit 10 --json

# Per-project storage
YAMS_STORAGE="$PWD/.yams" yams init --non-interactive
YAMS_STORAGE="$PWD/.yams" yams add ./src/

# Batch indexing
yams add src/ --recursive --include="*.cpp,*.hpp,*.h" --tags "code"
```

## Embedding Models

| Model | Dimensions | Notes |
|-------|------------|-------|
| `all-MiniLM-L6-v2` | 384 | Recommended: fast, lightweight |
| `all-mpnet-base-v2` | 768 | Higher quality, slower |

```bash
# Download and configure a model
yams model download all-MiniLM-L6-v2 --apply-config
```
