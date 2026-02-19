# YAMS Documentation Index

Content-addressed storage with deduplication and semantic search.

## Quick Links

- [Installation](user_guide/installation.md)
- [CLI Reference](user_guide/cli.md)
- [MCP Server](user_guide/mcp.md)
- [Admin: Operations](admin/operations.md)

## Documentation Structure

- **Get Started:** Installation, CLI, MCP integration
- **User Guides:** Tutorials, embeddings, search
- **Admin:** Operations, configuration, tuning
- **Developer:** Contributing, build system, architecture
- **API:** MCP tools, plugin specs
- **Hosting:** Managed hosting, self-hosting

## Plugin System

YAMS supports C-ABI plugins loaded from trusted directories.

**Trust file:** `~/.config/yams/plugins_trust.txt`

**Discovery paths:**
- `YAMS_PLUGIN_DIR` (exclusive if set)
- `$HOME/.local/lib/yams/plugins`
- `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`

**CLI:**
```bash
yams plugin scan
yams plugin trust add /usr/local/lib/yams/plugins
yams plugin load /path/to/plugin.so
```

See [PLUGINS.md](PLUGINS.md) for details.

## Build Options (Meson)

| Option | Default | Description |
|--------|---------|-------------|
| `build-cli` | `true` | Build CLI binary |
| `build-mcp-server` | `true` | Build MCP server |
| `build-tests` | `false` | Build test suite |
| `enable-pdf` | `enabled` | PDF text extraction (qpdf) |
| `enable-onnx` | `enabled` | ONNX embedding models |

See [BUILD.md](BUILD.md) for full build instructions.

## Links

- [SourceHut](https://sr.ht/~trvon/yams/)
- [GitHub](https://github.com/trvon/yams)
- [Releases](https://github.com/trvon/yams/releases)
- [Issues](https://github.com/trvon/yams/issues)
