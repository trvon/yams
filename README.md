<p align="center">
<h1 align="center">YAMS — Yet Another Memory System</h1>
<h6 align="center">Persistent memory for LLMs and apps. Content‑addressed storage with dedupe, compression, full‑text and vector search.</h6>
</p>
<p align="center">
<img alt="license" src="https://img.shields.io/github/license/trvon/yams?style=flat-square">
<img alt="language" src="https://img.shields.io/github/languages/top/trvon/yams?style=flat-square">
<img alt="github builds" src="https://img.shields.io/github/actions/workflow/status/trvon/yams/release.yml">
<img alt="last commit" src="https://img.shields.io/github/last-commit/trvon/yams?style=flat-square">
</p>

> [!WARNING]
> **Experimental Software - Not Production Ready**
> 
> YAMS is under active development and should be considered **experimental**. The software may contain bugs, incomplete features, and breaking changes between versions. Use at your own risk.

> For production workloads, please wait for a stable 1.0 release.


## Features
- SHA‑256 content‑addressed storage
- Block‑level dedupe (Rabin)
- Full‑text search (SQLite FTS5) + semantic search (embeddings)
- WAL‑backed durability, high‑throughput I/O, thread‑safe
- Portable CLI and MCP server
- Extensible with Plugin Support

## Links
- SourceHut: https://sr.ht/~trvon/yams/
- GitHub mirror: https://github.com/trvon/yams
- Docs: https://yamsmemory.ai
- Discord: https://discord.gg/rTBmRHdTEc
- License: GPL-3.0-or-later

## Install
Supported platforms: Linux x86_64/ARM64, macOS x86_64/ARM64

### Build with Meson (Recommended)

```bash
# Configure dependencies + Meson in one step
./setup.sh    # or ./setup.sh Debug

# Build / test
meson compile -C builddir          # Debug output
meson test -C builddir --print-errorlogs
meson compile -C build/release     # Release output

# (Optional) Install
meson install -C build/release
```

The setup script prefers Clang when available, falling back to GCC. Debug artifacts live under `builddir/`, release
under `build/release/`.


Dependencies quick ref:

- Linux: libssl-dev sqlite3 libsqlite3-dev protobuf-compiler ninja-build cmake (TUI enabled by default)
- macOS (Homebrew): openssl@3 protobuf sqlite3 ninja cmake (TUI enabled by default)
  - Export `OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)` if CMake cannot locate OpenSSL
- ONNX runtime (plugins): requires oneTBB at runtime. Provide via system packages (e.g., libtbb12/libtbb-dev) or Conan (`onetbb`).

Further build documentation:
- GCC specifics / quick reference: `docs/BUILD-GCC.md`
- Developer build system & internal ONNX Runtime path: `docs/developer/build_system.md`

## Quick Start
```bash
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init --non-interactive

# add
echo hello | yams add - --tags demo

# search
yams search hello --limit 5

# get
yams list --format minimal --limit 1 
```

### Path-Tree Search (Experimental)

The CLI and daemon can bias path-aware traversals when `[search.path_tree]` is enabled in
`config.toml`. This instructs the metadata layer to reuse the hierarchical index that powers the
PBI-051 benchmarks, yielding significantly faster prefix and diff scans. Toggle it by adding:

```toml
[search.path_tree]
enable = true
mode = "preferred" # or "fallback" to keep legacy scans as backup
```

When enabled, `yams grep` accepts tag or path filters (it defaults the pattern to `.*` for pure
filters) and, when you specify explicit path prefixes, the service seeds candidates through
`MetadataRepository::listPathTreeChildren`—the same engine exercised by
`tests/benchmarks/search_tree_bench.cpp`. citetests/benchmarks/search_tree_bench.cpp:188src/app/services/grep_service.cpp:247

## MCP
```bash
yams serve  # stdio transport
```

MCP config (example):
```json
{
  "mcpServers": { "yams": { "command": "/usr/local/bin/yams", "args": ["serve"] } }
}
```

## Troubleshooting
Conan: create default profile
```bash
conan profile detect --force
```

PDF support issues: build with `-DYAMS_ENABLE_PDF=OFF`.

Plugins not listed by `yams plugin list`:
- Ensure the ONNX plugin exists in a scanned directory (install prefix or `~/.local/lib/yams/plugins`).
- Ensure the directory is trusted (`yams plugin trust add <dir>` or via `yams init`).
- Ensure onnxruntime shared libs are resolvable by the loader (e.g., `ldd libyams_onnx_plugin.so`).
- Check the daemon startup log for: `Plugin scan directories: dir1;dir2;...` to confirm discovery paths.

Monitor with `yams stats --verbose` and `yams doctor`.

### Cite
```aiignore
@misc{yams,
author = {Trevon Williams},
title = {yams: Content addressable storage with excellent search },
year = {2025},
publisher = {GitHub},
url = {https://github.com/trvon/yams}
}
```