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
yams status
yams stats -v

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
  - Embedding models: see notes below
- Configuration (advanced)
  - Admin/Config Overview: [../admin/configuration.md](../admin/configuration.md)
  - Performance Tuning: [../admin/performance_tuning.md](../admin/performance_tuning.md)
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

See also:
- CLI Status: ./cli.md#cmd-status
- CLI Stats: ./cli.md#cmd-stats

## Common patterns

- Use JSON for scripting:
  - `yams list --format json | jq '.'`
  - `yams search "error logs" --limit 10 | jq '.results[] | {title, score}'`
- Keep data separate per project:
  - `YAMS_STORAGE="$PWD/.yams" yams init --non-interactive`
  - `YAMS_STORAGE="$PWD/.yams" yams add ./doc.md`

## Performance & Tuning (quick)

- Status and doctor show live resource use and readiness:
  - `yams status` — daemon readiness and resource snapshot
  - `yams doctor` — includes RAM (PSS/RSS) and CPU%, plus vector scoring state
- Auto‑embeddings on add are load‑aware:
  - By default, embeddings generate on add only when the daemon is idle.
  - When busy, embeddings are deferred to the background repair coordinator.
- For deeper guidance (embedding batch safety/doc‑cap/pause, worker scaling), see the Admin guide: [Performance Tuning](../admin/performance_tuning.md).

## See also

- Documentation Hub: [../README.md](../README.md)
- Manpages (if built): `man yams`

---
Last updated: This guide is kept minimal and links to authoritative pages above. For exhaustive flags/options, see the CLI Reference or use the verbose help.

## Embedding Models — Important Notes

- Recommended models for most users:
  - `all-MiniLM-L6-v2` (384‑dim): fast, lightweight
  - `all-mpnet-base-v2` (768‑dim): higher quality, slower
- Known issue: `nomic-embed-text-v1.5` (ONNX)
  - Current ONNX provider lacks batch embedding and preload support; throughput and UX may be degraded.
  - Use at your own risk. If you choose to use it, set dimensions to 768 and consider running repairs overnight.
  - We’ve removed it from the recommended dialogs for now. You can still download it explicitly via `yams model download --hf nomic-ai/nomic-embed-text-v1.5`.
  - After downloading, use `--apply-config` (or follow the printed `yams config set …` commands) to align `embeddings.embedding_dim`, `vector_database.embedding_dim`, and `vector_index.dimension`.
