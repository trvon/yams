# YAMS — Yet Another Memory System

Persistent, searchable memory for your code, documents, and AI workflows.
Store once, find anything, never lose context.

[![Latest tag](https://img.shields.io/github/v/tag/trvon/yams?sort=semver&label=latest%20tag)](https://github.com/trvon/yams/tags)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

!!! warning "Experimental Software"
    YAMS is under active development. Expect bugs and breaking changes. Wait for 1.0 for production use.

---

## Why YAMS

- **Store anything, find it later.** Content-addressed storage with automatic deduplication and compression.
- **Search by meaning, not just keywords.** Hybrid full-text + vector embeddings.
- **Works with your tools.** CLI, MCP server for AI assistants, C-ABI plugin architecture.

## Quick Start

```bash
brew install trvon/yams/yams       # or see Install below
yams init
echo "hello world" | yams add - --tags demo
yams search hello
```

## Use with AI Assistants

YAMS runs as an MCP server over stdio (JSON-RPC).

```bash
yams serve
```

```json
{
  "mcpServers": {
    "yams": { "command": "yams", "args": ["serve"] }
  }
}
```

Full MCP setup: [user_guide/mcp.md](user_guide/mcp.md).

## Install

| Platform            | Command                                                              |
|---------------------|----------------------------------------------------------------------|
| macOS (Homebrew)    | `brew install trvon/yams/yams`                                       |
| Debian / Ubuntu     | APT repo — see [installation.md#apt](user_guide/installation.md#apt) |
| Fedora / RHEL       | DNF repo — see [installation.md#dnf](user_guide/installation.md#dnf) |
| Docker              | `docker pull ghcr.io/trvon/yams:latest`                              |
| From source         | See [BUILD.md](BUILD.md)                                             |

Supported: Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64.

## Links

| Resource   | URL                                               |
|------------|---------------------------------------------------|
| Full docs  | [README.md](README.md)                            |
| Benchmarks | [benchmarks/README.md](benchmarks/README.md)      |
| Roadmap    | [roadmap.md](roadmap.md)                          |
| SourceHut  | https://sr.ht/~trvon/yams/                        |
| GitHub     | https://github.com/trvon/yams                     |
| Discord    | https://discord.gg/rTBmRHdTEc                     |
| License    | GPL-3.0-or-later                                  |
