[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/trvon/yams)  [![Release](https://github.com/trvon/yams/actions/workflows/release.yml/badge.svg)](https://github.com/trvon/yams/actions/workflows/release.yml)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

# YAMS — Yet Another Memory System
Persistent memory for LLMs and apps. Content‑addressed storage with dedupe, compression, full‑text and vector search.

- SourceHut: https://sr.ht/~trvon/yams/
- GitHub mirror: https://github.com/trvon/yams
- Docs: https://trvon.github.io/yams

## Features
- SHA‑256 content‑addressed storage
- Block‑level dedupe (Rabin)
- Full‑text search (SQLite FTS5) + semantic search (embeddings)
- WAL‑backed durability, high‑throughput I/O, thread‑safe
- Portable CLI and MCP server
- Extensible with Plugin Support

## Install
Supported platforms: Linux x86_64/ARM64, macOS x86_64/ARM64

Build with Conan (recommended):

```bash
pip install conan
conan profile detect --force
# Enable ONNX by default (optional but recommended)
conan install . -of build/yams-release -s build_type=Release -b missing -o yams/*:enable_onnx=True
cmake --preset yams-release
cmake --build --preset yams-release -j
sudo cmake --install build/yams-release && sudo ldconfig || true
```

Dependencies quick ref:

- Linux: libssl-dev sqlite3 libsqlite3-dev protobuf-compiler libncurses-dev ninja-build cmake
- macOS (Homebrew): openssl@3 protobuf sqlite3 ncurses ninja cmake
  - Export `OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)` if CMake cannot locate OpenSSL

Common build options: `YAMS_BUILD_TESTS=ON|OFF`, `YAMS_BUILD_BENCHMARKS=ON|OFF`, `YAMS_ENABLE_PDF=ON|OFF`, `YAMS_ENABLE_TUI=ON|OFF`, `YAMS_ENABLE_ONNX=ON|OFF`.

ONNX embeddings (experimental):

- The ONNX plugin is experimental and may not work as intended.
- Conan: default profiles enable `yams/*:enable_onnx=True`. With custom profiles, pass `-o yams/*:enable_onnx=True` to `conan install`.
- Plain CMake: configure with `-DYAMS_ENABLE_ONNX=ON` and ensure `onnxruntime` is discoverable (e.g., via `CMAKE_PREFIX_PATH`).
- Verify configure logs include: `ONNX Runtime found - enabling local embedding generation` (and not disabled).

Note: Plain CMake without Conan may miss dependencies; prefer Conan builds.

## Quick Start
```bash
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init --non-interactive

# add
echo hello | yams add - --tags demo

# search
yams search hello --limit 5

# get
yams list --format minimal --limit 1 | xargs yams get
```

## CLI Cheat Sheet
```bash
# set storage per-run
yams --data-dir /tmp/yams add -

# list (minimal for pipes)
yams list --format minimal | head -3

# fuzzy search
yams search database --fuzzy --similarity 0.8

# delete preview
yams delete --pattern "*.log" --dry-run
```

## LLM‑Friendly Patterns
```bash
# cache web content
curl -s https://example.com | yams add - --tags web,cache --name example.html

# stash code diffs
git diff | yams add - --tags git,diff,$(date +%Y%m%d)

# chain search -> get
hash=$(yams search "topic" --format minimal | head -1); yams get "$hash"
```

## Plugins (ONNX Provider)
YAMS loads optional plugins via a stable C‑ABI host with a simple trust policy.

- Trust file: `~/.config/yams/plugins_trust.txt` (one absolute path per line; default deny)
- Discovery order:
  - `YAMS_PLUGIN_DIR` (exclusive override)
  - `$HOME/.local/lib/yams/plugins`
  - `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`
  - `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`
- Disable plugin subsystem: start daemon with `--no-plugins`.

ONNX plugin build/install/runtime:

- Prerequisite: onnxruntime (headers + shared libraries) must be available at build and runtime.
- Build: `yams_onnx_plugin` is built when `onnxruntime` is found and ONNX is enabled.
- Install: plugin installs under `${CMAKE_INSTALL_LIBDIR}/yams/plugins` (e.g., `/usr/local/lib/yams/plugins`).
- Packaging: set `-DYAMS_PACKAGE_PLUGINS=ON` (default) and run `cpack` to include the plugin in binary packages.
- Discovery: daemon logs a line on startup with plugin scan directories (useful for troubleshooting).

Usage (CLI):
```bash
# scan, trust, load
yams plugin scan
yams plugin trust add /usr/local/lib/yams/plugins
yams plugin load /usr/local/lib/yams/plugins/libyams_onnx_plugin.so
```

First‑time setup with `yams init`:

- The init dialog asks whether to enable plugins; if yes, it creates and trusts `~/.local/lib/yams/plugins`.
- Non‑interactive: pass `--enable-plugins`.

Dev overrides:

- Set `YAMS_PLUGIN_DIR` to your build output (e.g., `.../build/.../plugins/onnx`) to have the daemon scan it.

Behavior:

- If a trusted plugin advertises `model_provider_v1`, the daemon prefers it for embeddings. Otherwise it falls back to the legacy registry or mock/null providers (env: `YAMS_USE_MOCK_PROVIDER`, `YAMS_DISABLE_ONNX`).

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

## Troubleshooting
Conan: create default profile
```bash
conan profile detect --force
```

PDF support issues: build with `-DYAMS_ENABLE_PDF=OFF`.

Plugins not listed by `yams plugin list`:
- Ensure the ONNX plugin exists in a scanned directory (install prefix or `~/.local/lib/yams/plugins`).
- Ensure the directory is trusted (`yams plugin trust add <dir>` or via `yams init`).
- Ensure onnxruntime shared libs are resolvable by the loader (e.g., `ldd libyams_onnx_plugin.so`).
- Check the daemon startup log for: `Plugin scan directories: dir1;dir2;...` to confirm discovery paths.

Monitor with `yams stats --verbose` and `yams doctor`.

## License
Apache-2.0
