<p align="center">
<h1 align="center">YAMS — Yet Another Memory System</h1>
<h6 align="center">Persistent memory for LLMs and apps. Content‑addressed storage with dedupe, compression, full‑text and vector search.</h6>
</p>
<p align="center">
<img alt="license" src="https://img.shields.io/github/license/trvon/yams?style=flat-square">
<img alt="language" src="https://img.shields.io/github/languages/top/trvon/yams?style=flat-square">
<img alt="github builds" src="https://img.shields.io/github/actions/workflow/status/trvon/yams/release.yml">
<img alt="last commit" src="https://img.shields.io/github/last-commit/trvon/yams?style=flat-square">
</p>


## Features
- SHA‑256 content‑addressed storage
- Block‑level dedupe (Rabin)
- Full‑text search (SQLite FTS5) + semantic search (embeddings)
- WAL‑backed durability, high‑throughput I/O, thread‑safe
- Portable CLI and MCP server
- Extensible with Plugin Support
- Experimental path-tree traversal for prefix/semantic/diff workloads (see `[search.path_tree]`
  in the config template) enabling ~62–66% faster scans vs. baseline grep queries when coupled
  with the new hierarchical benchmarks. citedocs/delivery/051/benchmark_plan.md:160

## Links
- SourceHut: https://sr.ht/~trvon/yams/
- GitHub mirror: https://github.com/trvon/yams
- Docs: https://yamsmemory.ai
- Discord: https://discord.gg/rTBmRHdTEc
- License: GPL-3.0-or-later

## Install
Supported platforms: Linux x86_64/ARM64, macOS x86_64/ARM64

### Build with Meson (Recommended)

```bash
# 1. Resolve dependencies
conan install . -of build/release -s build_type=Release -b missing

# 2. Configure
meson setup build/release \
  --native-file build/release/build-release/conan/conan_meson_native.ini \
  --buildtype=release

# 3. Build
meson compile -C build/release

# 4. (Optional) Install
meson install -C build/release
```


Dependencies quick ref:

- Linux: libssl-dev sqlite3 libsqlite3-dev protobuf-compiler ninja-build cmake (TUI enabled by default)
- macOS (Homebrew): openssl@3 protobuf sqlite3 ninja cmake (TUI enabled by default)
  - Export `OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)` if CMake cannot locate OpenSSL
- ONNX runtime (plugins): requires oneTBB at runtime. Provide via system packages (e.g., libtbb12/libtbb-dev) or Conan (`onetbb`).

Common build options (Meson): `-Dbuild-tests=true|false`, `-Denable-tui=true|false` (defaults to true), `-Denable-onnx=enabled|disabled|auto`, `-Dplugin-onnx=true|false`, `-Dyams-version=...`.
Fast iteration: set `FAST_MODE=1` when running `meson setup --reconfigure` to disable ONNX & tests in CI (SourceHut) or locally.
Media metadata: install `mediainfo` + dev package (e.g. `libmediainfo-dev`) or FFmpeg (`ffprobe`) to enable richer video parsing.

Further build documentation:
- GCC specifics / quick reference: `docs/BUILD-GCC.md`
- Developer build system & internal ONNX Runtime path: `docs/developer/build_system.md`

ONNX embeddings (experimental):

- The ONNX plugin is experimental and may not work as intended.
- Conan: default profiles enable `yams/*:enable_onnx=True`. With custom profiles, pass `-o yams/*:enable_onnx=True` to `conan install`.
- Plain CMake: configure with `-DYAMS_ENABLE_ONNX=ON` and ensure `onnxruntime` is discoverable (e.g., via `CMAKE_PREFIX_PATH`).
- Verify configure logs include: `ONNX Runtime found - enabling local embedding generation` (and not disabled).
- Internal newer ORT (GenAI headers) path: run Conan with `-o yams/*:use_conan_onnx=False` and configure with `-DYAMS_BUILD_INTERNAL_ONNXRUNTIME=ON` (see developer build doc for details).

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
yams list --format minimal --limit 1 
```

### Path-Tree Search (Experimental)

The CLI and daemon can bias path-aware traversals when `[search.path_tree]` is enabled in
`config.toml`. This instructs the metadata layer to reuse the hierarchical index that powers the
PBI-051 benchmarks, yielding significantly faster prefix and diff scans. Toggle it by adding:

```toml
[search.path_tree]
enable = true
mode = "preferred" # or "fallback" to keep legacy scans as backup
```

When enabled, `yams grep` accepts tag or path filters (it defaults the pattern to `.*` for pure
filters) and, when you specify explicit path prefixes, the service seeds candidates through
`MetadataRepository::listPathTreeChildren`—the same engine exercised by
`tests/benchmarks/search_tree_bench.cpp`. citetests/benchmarks/search_tree_bench.cpp:188src/app/services/grep_service.cpp:247

## CLI Cheat Sheet
```bash
# set storage per-run
yams --data-dir /tmp/yams add -

# list (minimal for pipes)
yams list --format minimal

# fuzzy search
yams search database --fuzzy --similarity 0.8

# delete preview
yams delete --pattern "*.log" --dry-run
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

### Cite
```aiignore
@misc{yams,
author = {Trevon Williams},
title = {yams: Content addressable storage with excellent search },
year = {2025},
publisher = {GitHub},
url = {https://github.com/trvon/yams}
}
```
