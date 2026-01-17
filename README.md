<p align="center">
<h1 align="center">YAMS — Yet Another Memory System</h1>
<h6 align="center">Persistent memory for LLMs and apps. Content-addressed storage with dedupe, compression, full-text and vector search.</h6>
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
- SHA-256 content-addressed storage with block-level dedupe (Rabin chunking)
- Full-text search (SQLite FTS5) + semantic vector search (embeddings)
- Symbol extraction from source code (tree-sitter: C, C++, Python, JS, TS, Rust, Go, Java, C#, PHP, Kotlin, Dart, SQL, Solidity)
- Snapshot management with Merkle tree diffs and rename detection
- WAL-backed durability, high-throughput I/O, thread-safe
- Portable CLI, MCP server, and plugin architecture (ONNX, S3, PDF extraction)

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

# Verify installation
yams --version
```

### Docker
```bash
# Pull and run
docker pull ghcr.io/trvon/yams:latest
docker run --rm -v yams-data:/home/yams/.local/share/yams ghcr.io/trvon/yams:latest --version

# MCP server
docker run -i --rm -v yams-data:/home/yams/.local/share/yams ghcr.io/trvon/yams:latest serve
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
./setup.ps1 Release
meson compile -C build/release
```

**Prerequisites:** GCC 13+, Clang 16+, or MSVC 2022+ (C++20); meson, ninja-build, cmake, pkg-config, conan

See [docs/BUILD.md](docs/BUILD.md) for detailed instructions.

## Quick Start
```bash
# Initialize (interactive - prompts for grammar downloads)
yams init .

# Auto mode for containers/headless
yams init --auto

# Add content
yams add ./README.md --tags docs
yams add src/ --recursive --include="*.cpp,*.h" --tags code

# Search
yams search "config file" --limit 5
yams grep "TODO" --include="*.cpp"

# List and retrieve
yams list --limit 20
yams get <hash> -o ./output.bin
```

### Symbol Extraction

YAMS extracts symbols (functions, classes, methods) from source code using tree-sitter:

```bash
# Interactive grammar setup
yams init

# Or auto-download recommended grammars
yams init --auto

# Manage grammars separately
yams config grammar list
yams config grammar download cpp python rust
```

**Supported:** C, C++, Python, JavaScript, TypeScript, Rust, Go, Java, C#, PHP, Kotlin, Dart, SQL, Solidity

### Snapshots & Diff

```bash
# Snapshots are created automatically on add
yams add . --recursive --include="*.cpp,*.h" --snapshot-label "v1.0"

# List snapshots
yams list --snapshots

# Compare snapshots (Merkle tree diff with rename detection)
yams diff v1.0 v1.1
yams diff v1.0 v1.1 --include="*.cpp" --stats
```

## MCP Server
```bash
yams serve  # stdio transport (JSON-RPC)
```

**Claude Desktop config** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"]
    }
  }
}
```

## Plugins

YAMS supports plugins for embeddings (ONNX), storage (S3), and content extraction (PDF):

```bash
yams plugin list                    # List loaded plugins
yams plugin trust add ~/.local/lib/yams/plugins
yams plugin health                  # Check plugin status
yams doctor plugin onnx             # Diagnose specific plugin
```

**Recommended model:** `all-MiniLM-L6-v2` (384-dim) — best speed/quality tradeoff. On Windows, avoid 768-dim+ models due to single-threaded ONNX execution.

## Troubleshooting

```bash
yams doctor              # Full diagnostics
yams stats --verbose     # Storage statistics
yams repair --all        # Repair common issues
```

**Build issues:** See [docs/BUILD.md](docs/BUILD.md)

**Plugin discovery:** `yams plugin list` empty? Add trust path: `yams plugin trust add ~/.local/lib/yams/plugins`

### Cite
```bibtex
@misc{yams,
  author = {Trevon Williams},
  title = {YAMS: Content-addressable storage with semantic search},
  year = {2025},
  publisher = {GitHub},
  url = {https://github.com/trvon/yams}
}
```