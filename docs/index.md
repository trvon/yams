# YAMS — Yet Another Memory System

[![Latest tag](https://img.shields.io/github/v/tag/trvon/yams?sort=semver&label=latest%20tag)](https://github.com/trvon/yams/tags)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

!!! warning "Experimental Software"
    YAMS is under active development. Expect bugs and breaking changes. Wait for 1.0 for production use.

Content-addressed storage with deduplication, compression, full-text and vector search.

## Features

| Feature | Description |
|---------|-------------|
| Content-addressed | SHA-256 hashing for immutable references |
| Deduplication | Rabin fingerprinting block-level dedupe |
| Search | FTS5 full-text + ONNX embedding vector search |
| Durability | WAL-backed, thread-safe, high-throughput |
| Interface | CLI and MCP server |
| Extensible | Plugin architecture |

## Install

**Platforms**: Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64

### Homebrew (macOS)

```bash
brew install trvon/yams/yams
yams --version
```

### Build from Source

```bash
# Linux/macOS
./setup.sh Release
meson compile -C build/release

# Windows (PowerShell)
./setup.ps1 Release
meson compile -C build/release
```

**Requirements**: GCC 13+, Clang 16+, or MSVC 2022+; meson, conan, ninja-build

See [BUILD.md](BUILD.md) for details.

## Quick Start

```bash
yams init
echo "hello world" | yams add - --tags demo
yams search hello --limit 5
yams list --limit 10
```

## MCP Server

```bash
yams serve  # stdio transport
```

Config example:
```json
{
  "mcpServers": {
    "yams": { "command": "yams", "args": ["serve"] }
  }
}
```

## Documentation

| Section | Content |
|---------|---------|
| [User Guide](user_guide/) | CLI reference, search, configuration |
| [Architecture](architecture/) | System design, daemon, search internals |
| [API](api/) | MCP tools, REST endpoints |
| [Developer](developer/) | Building, testing, contributing |

## Links

| Resource | URL |
|----------|-----|
| SourceHut | https://sr.ht/~trvon/yams/ |
| GitHub | https://github.com/trvon/yams |
| Discord | https://discord.gg/rTBmRHdTEc |
| License | GPL-3.0-or-later |