# YAMS — Yet Another Memory System

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/trvon/yams)  [![Latest tag](https://img.shields.io/github/v/tag/trvon/yams?sort=semver&label=latest%20tag)](https://github.com/trvon/yams/tags)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

!!! warning "Experimental Software - Not Production Ready"
    YAMS is under active development and should be considered **experimental**. The software may contain bugs, incomplete features, and breaking changes between versions. Use at your own risk.
    
    For production workloads, please wait for a stable 1.0 release.

Persistent memory for LLMs and apps. Content‑addressed storage with dedupe, compression, full‑text and vector search.

## Features

- SHA‑256 content‑addressed storage
- Block‑level dedupe (Rabin)
- Full‑text search (SQLite FTS5) + semantic search (embeddings)
- WAL‑backed durability, high‑throughput I/O, thread‑safe
- Portable CLI and MCP server
- Extensible with Plugin Support

<div class="hero-cta">
  <h2>Managed hosting coming soon</h2>
  <p>Get hosting updates. Help shape the roadmap.</p>
  <button class="waitlist-toggle-btn" type="button">Sign up</button>
  <form action="https://formspree.io/f/xgvzbbzy" method="POST" class="waitlist-form" style="display:none">
    <input type="email" name="email" placeholder="email@domain.com" required />
    <input type="text" name="name" placeholder="Full name (optional)" />
    <input type="text" name="use_case" placeholder="Primary use case (optional)" />
    <!-- Honeypot field -->
    <input type="text" name="_gotcha" style="display:none" />
    <!-- Redirect to thanks page -->
    <input type="hidden" name="_redirect" value="/thanks/" />
    <!-- Tag the submission -->
    <input type="hidden" name="list" value="hosting-early-access" />
    <button type="submit">Join waitlist</button>
  </form>
  <p class="privacy-note">We only email about hosting. Unsubscribe anytime.</p>
</div>
<script>
document.addEventListener('DOMContentLoaded', function () {
  var cta = document.querySelector('.hero-cta');
  if (!cta) return;
  var btn = cta.querySelector('.waitlist-toggle-btn');
  var form = cta.querySelector('form.waitlist-form');
  var heading = cta.querySelector('h1, h2, h3, h4, h5, h6');
  if (!btn || !form) return;
  btn.addEventListener('click', function () {
    var isHidden = form.style.display === 'none' || form.hidden;
    form.style.display = isHidden ? '' : 'none';
    form.hidden = !isHidden;
    if (isHidden && heading && (heading.hidden || heading.getAttribute('aria-hidden') === 'true')) {
      heading.hidden = false;
      heading.removeAttribute('aria-hidden');
    }
  });
});
</script>


## Links

- SourceHut: https://sr.ht/~trvon/yams/
- GitHub mirror: https://github.com/trvon/yams
- Docs: https://yamsmemory.ai
- Discord: https://discord.gg/rTBmRHdTEc
- License: GPL-3.0-or-later

## Install

Supported platforms: Linux x86_64/ARM64, macOS x86_64/ARM64

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

```bash
# Quick build (auto-detects Clang/GCC, configures Conan + Meson)
./setup.sh Release

# Build
meson compile -C build/release

# Optional: Install system-wide
meson install -C build/release
```

**Prerequisites:**

- Compiler: GCC 13+ or Clang 16+ (C++20 minimum)
- Build tools: meson, ninja-build, cmake, pkg-config, conan
- System libs: libssl-dev, libsqlite3-dev, protobuf-compiler

See [BUILD-GCC.md](BUILD-GCC.md) for detailed build instructions, compiler configuration, Conan profiles, and troubleshooting.

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

## Versioning

YAMS provides comprehensive versioning through content-addressed storage. Every stored document gets a unique SHA-256 hash that serves as an immutable version identifier. You can track changes using metadata updates (`yams update`), organize versions with collections (`--collection release-v1.0`), and capture point-in-time states with snapshots (`--snapshot-id 2024Q4`).

## Troubleshooting

**Build issues:** See [BUILD-GCC.md](BUILD-GCC.md) for compiler setup, Conan profiles, and dependency resolution.

**Plugin discovery:** Verify with `yams plugin list`. If empty:

- Check trusted directories: `yams plugin trust list`
- Add plugin path: `yams plugin trust add ~/.local/lib/yams/plugins`
- Verify shared libs: `ldd libyams_onnx_plugin.so`

**Monitor:** `yams stats --verbose` and `yams doctor` for diagnostics.

## Docs

- Get Started: Installation, CLI, and prompts
- Usage: Search guide, vector search, tutorials
- API: REST/OpenAPI, MCP tools
- Architecture: search and vector systems
- Developer/Operations/Admin: build, deploy, configure, tune

Use the left navigation to browse all docs.

## Links

- SourceHut: https://sr.ht/~trvon/yams/
- GitHub (mirror): https://github.com/trvon/yams
- License: GPL-3.0-or-later
