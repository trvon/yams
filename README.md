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
Supported platforms: Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64

### macOS (Homebrew)
```bash
# Stable release (recommended)
brew install trvon/yams/yams

# Or get nightly builds for latest features
brew install trvon/yams/yams@nightly

# If linking fails due to conflicts, force link
brew link --overwrite yams

# Verify installation
yams --version

# Run as a service (optional)
brew services start yams
```

### Build from Source

**Linux/macOS:**
```bash
# Quick build (auto-detects Clang/GCC, configures Conan + Meson)
./setup.sh Release

# Build
meson compile -C build/release

# Optional: Install system-wide
meson install -C build/release
```

**Windows:**
```pwsh
# Quick build (MSVC + Conan + Meson)
./setup.ps1 Release

# Build
meson compile -C build/release
```

**Prerequisites:**
- Compiler: GCC 13+, Clang 16+, or MSVC 2022+ (C++20 minimum)
- Build tools: meson, ninja-build, cmake, pkg-config, conan
- System libs: libssl-dev, libsqlite3-dev, protobuf-compiler (Linux/macOS)

See [docs/BUILD.md](docs/BUILD.md) for detailed build instructions, compiler configuration, Conan profiles, and troubleshooting.

## Quick Start
```bash
# Initialize YAMS storage in current directory
yams init .

# Or specify custom location
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init

# Add content
echo hello | yams add - --tags demo

# Search
yams search hello --limit 5

# List
yams list --format minimal --limit 1 
```

### Path-Tree Search (Experimental)

Enable hierarchical index for faster prefix/path scans in `config.toml`:

```toml
[search.path_tree]
enable = true
mode = "preferred" # or "fallback" to keep legacy scans as backup
```


Optimizes `yams grep` with explicit path prefixes via `listPathTreeChildren` index.

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

**Build issues:** See [docs/BUILD.md](docs/BUILD.md) for compiler setup, Conan profiles, and dependency resolution.

**Plugin discovery:** Verify with `yams plugin list`. If empty:
- Check trusted directories: `yams plugin trust list`
- Add plugin path: `yams plugin trust add ~/.local/lib/yams/plugins`
- Verify shared libs: `ldd libyams_onnx_plugin.so`

**Monitor:** `yams stats --verbose` and `yams doctor` for diagnostics.

### Cite
```aiignore
@misc{yams,
  author = {Trevon Williams},
  title = {yams: Content addressable storage with   excellent search },
  year = {2025},
  publisher = {GitHub},
  url = {https://github.com/trvon/yams}
}
```