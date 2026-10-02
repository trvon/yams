# YAMS — Yet Another Memory System

Persistent, searchable memory for code, documents, and local applications.
YAMS stores content once, deduplicates it, and retrieves it through text, vector,
graph, and grep surfaces.

> **Experimental:** YAMS is pre-1.0. Expect bugs and breaking changes.

## Why YAMS

YAMS is local memory for people who work with LLMs. It runs on your machine and
is harness-agnostic.

We believe LLM memory should be memory, and yours. It lives in a data
directory you choose, in formats you can inspect and it can be exported 
or deleted whenever you decide. No account, no hosted service, and no model 
provider sits between you and what you have stored.

## What it provides

- SHA-256 content-addressed storage with chunk-level deduplication and compression
- SQLite FTS5, Simeon embeddings, hybrid search, and knowledge-graph retrieval
- Document graph traversal over paths, versions, entities, and semantic neighbors
- Snapshots, Merkle tree diffs, corruption detection, and repair tooling
- A CLI, an MCP server over stdio, and a C ABI for plugins and mobile hosts
- Local-first operation with no account or hosted service requirement

## Install

```bash
# macOS
brew install trvon/yams/yams

# Debian / Ubuntu
curl -fsSL https://repo.yamsmemory.ai/gpg.key \
  | sudo gpg --dearmor -o /usr/share/keyrings/yams-stable.gpg
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/yams-stable.gpg] https://repo.yamsmemory.ai/aptrepo stable main" \
  | sudo tee /etc/apt/sources.list.d/yams-stable.list
sudo apt-get update && sudo apt-get install yams

# Container
docker pull ghcr.io/trvon/yams:latest
```

Fedora (DNF), Arch (pacman), Windows (MSI) and release archives are covered in
the [install guide](docs/install.md), along with where YAMS keeps its config,
data and logs, how to run the daemon as a service, and the settings most people
change. To build from source, see [docs/BUILD.md](docs/BUILD.md).

By default YAMS keeps its config in `~/.config/yams/config.toml`, your corpus in
`~/.local/share/yams`, and daemon logs in `~/.local/state/yams`
(`%APPDATA%` / `%LOCALAPPDATA%` on Windows).

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

- [Install and setup](docs/install.md)
- [Build from source](docs/BUILD.md)
- [Benchmarks](docs/benchmarks/)
- [Roadmap](docs/roadmap.md)
- [Newsletter](docs/newsletter.md)
- [Contributing](CONTRIBUTING.md)
- [Project site](https://yamsmemory.ai)

## Project

- GitHub: <https://github.com/trvon/yams>
- Self-hosted mirror: <https://git.trevon.dev/trevon/yams>
- Discord: <https://discord.gg/rTBmRHdTEc>
- License: GPL-3.0-or-later

See `CONTRIBUTING.md` for contribution workflow and `SECURITY.md` for responsible
vulnerability reporting.
