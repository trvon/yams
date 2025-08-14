# YAMS — Yet Another Memory System

<div class="hero" style="border:1px solid #00ff7f;padding:1rem;background:#0b0f10;margin:1rem 0">
<pre style="margin:0;color:#9bffb0">
$ yams --version
YAMS: persistent memory for LLMs and applications
SHA-256 CAS • Rabin dedupe • zstd/LZMA • FTS5 + vectors • WAL
</pre>
</div>

Persistent memory for LLMs and applications. Content‑addressed storage with deduplication, compression, semantic search, and full‑text indexing.

## What it does

- Content‑addressed storage (SHA‑256)
- Block‑level deduplication (Rabin fingerprinting)
- Compression: zstd and LZMA
- Search: full‑text (SQLite FTS5) + semantic (vector)
- Crash safety: WAL
- Fast and portable CLI + MCP server

## Install

Quick install (prebuilt binaries):

```bash
curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash
```

Docker:

```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```

From source (Conan): see Get Started → Installation.

## Quick start

```bash
# init storage (non-interactive)
yams init --non-interactive

# store from stdin
echo "hello world" | yams add - --tags example

# search
yams search "hello" --json

# retrieve
yams list --format minimal --limit 1 | xargs yams get
```

## Docs

- Get Started: Installation, CLI, and prompts
- Usage: Search guide, vector search, tutorials
- API: REST/OpenAPI, MCP tools
- Architecture: search and vector systems
- Developer/Operations/Admin: build, deploy, configure, tune

Use the left navigation to browse all docs.

## Links

- GitHub: https://github.com/trvon/yams
- License: Apache-2.0
