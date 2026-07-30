# YAMS — Yet Another Memory System

Persistent, searchable memory for code, documents, and local applications.
YAMS stores content once, deduplicates it, and retrieves it through text, vector,
graph, and grep surfaces.

> **Experimental:** YAMS is pre-1.0. Expect bugs and breaking changes.

## What it provides

- SHA-256 content-addressed storage with chunk-level deduplication and compression
- SQLite FTS5, Simeon embeddings, hybrid search, and knowledge-graph retrieval
- Tree-sitter symbol extraction and source-aware graph traversal
- Snapshots, Merkle tree diffs, corruption detection, and repair tooling
- A CLI, an MCP server over stdio, and a C ABI for plugins and mobile hosts
- Local-first operation with no account or hosted service requirement

## Install

```bash
# macOS
brew install trvon/yams/yams

# Container
docker pull ghcr.io/trvon/yams:latest
```

Linux packages and Windows artifacts are published with releases. To build from
source, see [docs/BUILD.md](docs/BUILD.md).

## Start

```bash
yams init
yams add ./README.md --tags docs
yams add src/ --recursive --include="*.cpp,*.h" --tags code

yams search "daemon lifecycle" --limit 5
yams grep "TODO" --include="*.cpp"
yams graph --explore "LifecycleComponent"
```

Run `yams --help` or `yams <command> --help` for the current command reference.

## MCP

YAMS can expose a local corpus over MCP for compatible developer tools:

```bash
yams serve
```

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

## Project pages

- [Build from source](docs/BUILD.md)
- [Benchmarks](docs/benchmarks/)
- [Roadmap](docs/roadmap.md)
- [Newsletter](docs/newsletter.md)
- [Contributing](CONTRIBUTING.md)
- [Project site](https://yamsmemory.ai)

## Project

- SourceHut: <https://sr.ht/~trvon/yams/>
- GitHub mirror: <https://github.com/trvon/yams>
- Discord: <https://discord.gg/rTBmRHdTEc>
- License: GPL-3.0-or-later

See `CONTRIBUTING.md` for contribution workflow and `SECURITY.md` for responsible
vulnerability reporting.
