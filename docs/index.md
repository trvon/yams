# YAMS — Yet Another Memory System

[![Latest tag](https://img.shields.io/github/v/tag/trvon/yams?sort=semver&label=latest%20tag)](https://github.com/trvon/yams/tags)
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

## Versioning

YAMS provides comprehensive versioning through content-addressed storage. Every stored document gets a unique SHA-256 hash that serves as an immutable version identifier. You can track changes using metadata updates (`yams update`), organize versions with collections (`--collection release-v1.0`), and capture point-in-time states with snapshots (`--snapshot-id 2024Q4`).

## Install

### Docker (simplest)

```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```

### Linux Packages

**DEB (Debian/Ubuntu):**
```bash
# Download and install the .deb package
curl -L https://github.com/trvon/yams/releases/latest/download/yams-latest-amd64.deb -o yams.deb
sudo dpkg -i yams.deb
# Fix any dependency issues
sudo apt-get install -f
```

**RPM (Fedora/RedHat/CentOS):**
```bash
# Download and install the .rpm package
curl -L https://github.com/trvon/yams/releases/latest/download/yams-latest-x86_64.rpm -o yams.rpm
sudo rpm -i yams.rpm
# Or with yum/dnf
sudo dnf install ./yams.rpm
```

**AppImage (Universal Linux):**
```bash
# Download AppImage (works on most Linux distributions)
curl -L https://github.com/trvon/yams/releases/latest/download/yams-latest-x86_64.AppImage -o yams
chmod +x yams
./yams --version
# Optionally move to PATH
sudo mv yams /usr/local/bin/
```

### Native Binary

```bash
# macOS ARM64
curl -L https://github.com/trvon/yams/releases/latest/download/yams-macos-arm64.zip -o yams.zip
unzip yams.zip && sudo mv yams /usr/local/bin/

# macOS x86_64  
curl -L https://github.com/trvon/yams/releases/latest/download/yams-macos-x86_64.zip -o yams.zip
unzip yams.zip && sudo mv yams /usr/local/bin/

# Linux x86_64
curl -L https://github.com/trvon/yams/releases/latest/download/yams-linux-x86_64.tar.gz | tar xz
sudo mv yams /usr/local/bin/
```

### Build from Source

```bash
# Install Conan
pip install conan

# One-time: create default Conan profile
conan profile detect --force

# Build with Conan (recommended - this is what creates the release binaries)
conan install . --output-folder=build/conan-release -s build_type=Release --build=missing
cmake --preset conan-release
cmake --build --preset conan-release
sudo cmake --install build/conan-release/build/Release
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
