[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/trvon/yams)  [![Release](https://github.com/trvon/yams/actions/workflows/release.yml/badge.svg)](https://github.com/trvon/yams/actions/workflows/release.yml)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

# YAMS — Yet Another Memory System
Persistent memory for LLMs and apps. Content-addressed storage with dedupe, compression, full-text and vector search.

- SourceHut: https://sr.ht/~trvon/yams/
- GitHub mirror: https://github.com/trvon/yams
- Docs: https://trvon.github.io/yams

## What it is

- SHA-256 content-addressed store with block-level dedupe (Rabin)
- Full-text search (SQLite FTS5) and semantic search (embeddings)
- WAL-backed durability, high-throughput I/O, thread-safe

## Install

Supported: Linux x86_64/ARM64, macOS x86_64/ARM64

Build with Conan (recommended):

```bash
pip install conan
conan profile detect --force
conan install . -of build/yams-release -s build_type=Release -b missing -o yams/*:enable_onnx=True
cmake --preset yams-release
cmake --build --preset yams-release -j
sudo cmake --install build/yams-release && sudo ldconfig
```

Deps quick refs:

- Linux: libssl-dev sqlite3 libsqlite3-dev protobuf-compiler libncurses-dev ninja-build cmake
- macOS: openssl@3 protobuf sqlite3 ncurses (export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)) ninja-build cmake

Build options (common): `YAMS_BUILD_TESTS=ON|OFF`, `YAMS_BUILD_BENCHMARKS=ON|OFF`, `YAMS_ENABLE_PDF=ON|OFF`, `YAMS_ENABLE_TUI=ON|OFF`, `YAMS_ENABLE_ONNX=ON|OFF`.

ONNX embeddings (recommended):
**Plugin is experiemental and make not work as intended**

- Conan default: this repo’s Conan profiles set `yams/*:enable_onnx=True` so ONNX Runtime is pulled and linked by default. If you use a custom profile, add `-o yams/*:enable_onnx=True` to `conan install`.
- Plain CMake: configure with `-DYAMS_ENABLE_ONNX=ON` and make sure CMake can find `onnxruntime` (e.g., set `CMAKE_PREFIX_PATH` to its install prefix).
- Verify configure logs contain: `ONNX Runtime found - enabling local embedding generation` and not `ONNX Runtime disabled`.

Note: Plain CMake without Conan may miss deps. Prefer Conan builds.

## Plugin System & ONNX Provider

YAMS loads optional plugins via a stable C‑ABI host with a simple trust policy.

- Trust file: `~/.config/yams/plugins_trust.txt` (one absolute path per line; default deny)
- Discovery order:
  - `YAMS_PLUGIN_DIR` (exclusive override)
  - `$HOME/.local/lib/yams/plugins`
  - `/usr/local/lib/yams/plugins`, `/usr/lib/yams/plugins`
  - `${CMAKE_INSTALL_PREFIX}/lib/yams/plugins`
- Disable plugin subsystem: start daemon with `--no-plugins`.

ONNX plugin build install & runtime:

- Prerequisite: onnxruntime (headers + shared libraries) must be available at build and runtime.
  - Conan: add onnxruntime to your profile/lock; plain CMake users should install system packages.
- Build: the `yams_onnx_plugin` shared library is built when `onnxruntime` is found and ONNX is not disabled.
- Install: the plugin is installed under `${CMAKE_INSTALL_LIBDIR}/yams/plugins` (e.g. `/usr/local/lib/yams/plugins`).
- Packaging: set `-DYAMS_PACKAGE_PLUGINS=ON` (default) and run `cpack` to include the plugin in binary packages.
- Discovery: the daemon logs a single line on startup with the plugin scan directories for quick troubleshooting.

Usage (CLI):

```
# scan, trust, load
yams plugin scan
yams plugin trust add /usr/local/lib/yams/plugins
yams plugin load /usr/local/lib/yams/plugins/libyams_onnx_plugin.so
```

First‑time setup with `yams init`:

- The init dialog asks whether to enable plugins. If yes, it creates and trusts `~/.local/lib/yams/plugins`.
- Non‑interactive: pass `--enable-plugins`.

Dev overrides:

- Set `YAMS_PLUGIN_DIR` to point at your build output (e.g., `.../build/.../plugins/onnx`). The daemon will scan it.

Behavior:

- If a trusted plugin advertises `model_provider_v1`, the daemon prefers it for embeddings. Otherwise, it falls back to the legacy registry or mock/null providers (env: `YAMS_USE_MOCK_PROVIDER`, `YAMS_DISABLE_ONNX`).

## Quick start

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

## CLI cheat sheet

```bash
# set storage per-run
yams --data-dir /tmp/yams add -

# list (minimal for pipes)
yams list --format minimal | head -3

# fuzzy search
yams search databse --fuzzy --similarity 0.8

# delete preview
yams delete --pattern "*.log" --dry-run
```

## LLM-friendly patterns

```bash
# cache web content
curl -s https://example.com | yams add - --tags web,cache --name example.html

# stash code diffs
git diff | yams add - --tags git,diff,$(date +%Y%m%d)

# chain search -> get
hash=$(yams search "topic" --format minimal | head -1); yams get "$hash"
```

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

## API (C++)

```cpp
#include <yams/api/content_store.h>
auto store = yams::api::createContentStore(getenv("YAMS_STORAGE"));
yams::api::ContentMetadata meta{.tags={"code","v1.0"}};
auto r = store->store("file.txt", meta);
auto q = store->search("query", 10);
store->retrieve(hash, "out.txt");
```

## Troubleshooting

Conan: create default profile

```bash
conan profile detect --force
```

PDF support issues: build with `-DYAMS_ENABLE_PDF=OFF`.

Plugins not listed by `yams plugin list`:

- Ensure the ONNX plugin exists in a scanned directory (install prefix or `~/.local/lib/yams/plugins`).
- Ensure the directory is trusted (init enables user dir; or `yams plugin trust add <dir>`).
- Ensure onnxruntime shared libs are resolvable by the loader (e.g., `ldd libyams_onnx_plugin.so`).
- Check the daemon startup log for: `Plugin scan directories: dir1;dir2;...` to confirm discovery paths.

## Performance and Tuning

The daemon exposes centralized tuning knobs (via TuneAdvisor) to improve throughput, especially for add/index operations:

- `YAMS_WORKER_THREADS` — CPU worker threads for embeddings/model work (default ~half cores).
- `YAMS_WRITER_BUDGET_BYTES` — Base writer budget per turn (bytes) used by the mux (default ~3 MiB).
- `YAMS_SERVER_WRITER_BUDGET_BYTES` — Server override for per‑turn writer budget (bytes).
- `YAMS_MAX_MUX_BYTES` — Backpressure threshold for total queued bytes (default 256 MiB).
- `YAMS_SERVER_QUEUE_BYTES_CAP` — Per‑connection queued bytes cap (default 256 MiB).
- `YAMS_SERVER_QUEUE_FRAMES_CAP` — Per‑request queued frames cap (default 1024).
- `YAMS_SERVER_MAX_INFLIGHT` — Max inflight requests per connection (default 2048).

Example settings for faster local adds:

```bash
export YAMS_WORKER_THREADS=$(nproc)
export YAMS_WRITER_BUDGET_BYTES=$((4*1024*1024))
export YAMS_SERVER_WRITER_BUDGET_BYTES=$((4*1024*1024))
export YAMS_MAX_MUX_BYTES=$((512*1024*1024))
```

Monitor with `yams stats --verbose` and `yams doctor`.

## License

Apache-2.0
