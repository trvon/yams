[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/trvon/yams)  [![Release](https://github.com/trvon/yams/actions/workflows/release.yml/badge.svg)](https://github.com/trvon/yams/actions/workflows/release.yml)  [![builds.sr.ht status](https://builds.sr.ht/~trvon.svg)](https://builds.sr.ht/~trvon?)

# YAMS — Yet Another Memory System
**Version v0.6.x will focus on improving stability, performance and the CI. All new features will move to the experimental or v0.7.0 branch**

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
conan install . -of build/yams-release -s build_type=Release -b missing
cmake --preset yams-release
cmake --build --preset yams-release -j
sudo cmake --install build/yams-release && sudo ldconfig
```

Deps quick refs:

- Linux: libssl-dev sqlite3 libsqlite3-dev protobuf-compiler libncurses-dev ninja-build cmake
- macOS: openssl@3 protobuf sqlite3 ncurses (export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)) ninja-build cmake

Build options (common): `YAMS_BUILD_TESTS=ON|OFF`, `YAMS_BUILD_BENCHMARKS=ON|OFF`, `YAMS_ENABLE_PDF=ON|OFF`, `YAMS_ENABLE_TUI=ON|OFF`, `YAMS_ENABLE_ONNX=ON|OFF`.

Note: Plain CMake without Conan may miss deps. Prefer Conan builds.

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

## License

Apache-2.0
