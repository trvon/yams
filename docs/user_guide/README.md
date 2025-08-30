# YAMS User Guide

An index for using YAMS from the command line. Start here, jump to what you need, and get things done.

## TL;DR (after install)
```bash
# Initialize (uses XDG paths or YAMS_STORAGE)
yams init --non-interactive

# Add content (file or stdin)
yams add ./README.md
echo "note" | yams add - --name "quick-note.txt" --mime-type text/plain

# Explore
yams list --format table --limit 20
yams search "config file" --limit 5

# Retrieve and delete
yams get <hash> -o ./file.bin
yams delete <hash> --force
```

Tips:
- Global verbose help: `yams --help-all`
- Per-command verbose help: `yams <command> --help --verbose`
- Set a custom data root: `export YAMS_STORAGE="$HOME/.local/share/yams"`

## Topics

- Install and build
  - Installation Guide: [installation.md](./installation.md)
  - Build Options Matrix: see below
- Command‑line usage
  - CLI Reference: [cli.md](./cli.md)
- Searching your documents
  - Keyword & FTS: [search_guide.md](./search_guide.md)
  - Semantic & hybrid search: [vector_search_guide.md](./vector_search_guide.md)
- Configuration (advanced)
  - Admin/Config Overview: [../admin/configuration.md](../admin/configuration.md)
- Troubleshooting
  - Search issues: [../troubleshooting/search_issues.md](../troubleshooting/search_issues.md)

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `YAMS_USE_CONAN` | OFF | Use Conan package manager |
| `YAMS_BUILD_CLI` | ON | CLI with optional TUI browser |
| `YAMS_BUILD_MCP_SERVER` | ON | MCP server |
| `YAMS_BUILD_TESTS` | OFF | Unit and integration tests |
| `YAMS_BUILD_BENCHMARKS` | OFF | Performance benchmarks |
| `YAMS_ENABLE_PDF` | ON | PDF text extraction support (may download PDFium via FetchContent) |
| `YAMS_ENABLE_TUI` | OFF | Enables TUI browser (adds ncurses via Conan; ImTUI via FetchContent) |
| `YAMS_ENABLE_ONNX` | ON | Enables ONNX Runtime features (pulls onnxruntime; may pull Boost transitively) |
| `CMAKE_BUILD_TYPE` | Release | Debug/Release/RelWithDebInfo |

## Common patterns

- Use JSON for scripting:
  - `yams list --format json | jq '.'`
  - `yams search "error logs" --limit 10 | jq '.results[] | {title, score}'`
- Keep data separate per project:
  - `YAMS_STORAGE="$PWD/.yams" yams init --non-interactive`
  - `YAMS_STORAGE="$PWD/.yams" yams add ./doc.md`

## See also

- Documentation Hub: [../README.md](../README.md)
- Manpages (if built): `man yams`

---
Last updated: This guide is kept minimal and links to authoritative pages above. For exhaustive flags/options, see the CLI Reference or use the verbose help.
