# YAMS — Yet Another Memory System

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/trvon/yams)  [![Latest tag](https://img.shields.io/github/v/tag/trvon/yams?sort=semver&label=latest%20tag)](https://github.com/trvon/yams/tags)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

**Note**: Pre‑1.0 releases (v0.x) are not considered stable. Expect breaking changes until v1.0.

Persistent memory for LLMs and applications. Content‑addressed storage with deduplication, compression, semantic search, and full‑text indexing.

## What it does

- Content‑addressed storage (SHA‑256)
- Block‑level deduplication (Rabin fingerprinting)
- Compression: zstd and LZMA
- Search: full‑text (SQLite FTS5) + semantic (vector)
- Crash safety: WAL
- Fast and portable CLI + MCP server

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


## Install

### Docker (simplest)

```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```

### Build from Source

```bash
# Install Conan
pip install conan

# One-time: create default Conan profile
conan profile detect --force

# Build with Conan (recommended - this is what creates the release binaries)
conan install . --output-folder=build/yams-release -s build_type=Release --build=missing
cmake --preset yams-release
cmake --build --preset build-yams-release
sudo cmake --install build/yams-release
```

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

## Versioning

YAMS provides comprehensive versioning through content-addressed storage. Every stored document gets a unique SHA-256 hash that serves as an immutable version identifier. You can track changes using metadata updates (`yams update`), organize versions with collections (`--collection release-v1.0`), and capture point-in-time states with snapshots (`--snapshot-id 2024Q4`).

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
- License: Apache-2.0
