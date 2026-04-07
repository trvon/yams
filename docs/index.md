# YAMS — Yet Another Memory System

Persistent, searchable memory for your code, documents, and AI workflows.
Store once, find anything, never lose context.

[![Latest tag](https://img.shields.io/github/v/tag/trvon/yams?sort=semver&label=latest%20tag)](https://github.com/trvon/yams/tags)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

!!! warning "Experimental Software"
    YAMS is under active development. Expect bugs and breaking changes. Wait for 1.0 for production use.

---

## Why YAMS

- **Store anything, find it later.** Content-addressed storage with automatic deduplication and compression. Files, notes, code, PDFs — YAMS keeps one copy and retrieves it instantly.

- **Search by meaning, not just keywords.** Hybrid search combines full-text indexing with vector embeddings so you can search by concept, not just exact text.

- **Works with your tools.** CLI for scripts and automation. MCP server for AI assistants like Claude Desktop. Plugin architecture for custom integrations.

---

## Quick Start

```bash
brew install trvon/yams/yams   # or see Install below for apt/dnf/docker
yams init
echo "hello world" | yams add - --tags demo
yams search hello
```

## Use with AI Assistants

YAMS runs as an MCP server, giving AI assistants persistent memory with 3 composite tools (query, execute, session).

```bash
yams serve  # stdio transport
```

Add to your Claude Desktop config:

```json
{
  "mcpServers": {
    "yams": { "command": "yams", "args": ["serve"] }
  }
}
```

See the [MCP guide](user_guide/mcp.md) for full setup and usage.

## Install

### Debian / Ubuntu (APT)

```bash
echo "deb [arch=amd64,arm64 trusted=yes] https://repo.yamsmemory.ai/aptrepo stable main" \
  | sudo tee /etc/apt/sources.list.d/yams.list
sudo apt-get update && sudo apt-get install yams
```

### Fedora / RHEL (YUM / DNF)

!!! note "Untested"
    YUM/DNF packages are published but not yet validated in CI.

```bash
sudo tee /etc/yum.repos.d/yams.repo <<'REPO'
[yams]
name=YAMS Repository
baseurl=https://repo.yamsmemory.ai/yumrepo/
enabled=1
gpgcheck=0
REPO
sudo dnf makecache && sudo dnf install yams
```

### macOS (Homebrew)

```bash
brew install trvon/yams/yams
```

### Other methods

| Method | Details |
|--------|---------|
| Docker | `docker run --rm -it ghcr.io/trvon/yams:latest --version` |
| Direct download | `curl -fsSL https://repo.yamsmemory.ai/latest.json | jq .` |
| Build from source | See [Installation](user_guide/installation.md) |

**Platforms**: Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64

## Links

| Resource | URL |
|----------|-----|
| Roadmap | [What's planned](roadmap.md) |
| SourceHut | https://sr.ht/~trvon/yams/ |
| GitHub | https://github.com/trvon/yams |
| Discord | https://discord.gg/rTBmRHdTEc |
| License | GPL-3.0-or-later |
