<p align="center">
<h1 align="center">YAMS — Yet Another Memory System</h1>
<h6 align="center">Persistent memory for LLMs and apps. Content-addressed storage with dedupe, compression, full-text and vector search.</h6>
</p>
<p align="center">
<img alt="license" src="https://img.shields.io/github/license/trvon/yams?style=flat-square">
<img alt="language" src="https://img.shields.io/github/languages/top/trvon/yams?style=flat-square">
<img alt="github builds" src="https://img.shields.io/github/actions/workflow/status/trvon/yams/release.yml">
<img alt="last commit" src="https://img.shields.io/github/last-commit/trvon/yams?style=flat-square">
</p>

> [!WARNING]
> **Experimental — not production ready.** Expect bugs and breaking changes until 1.0.

## Features

- SHA-256 content-addressed storage with block-level dedupe (Rabin chunking)
- Full-text search (SQLite FTS5) + semantic vector search (embeddings)
- Tree-sitter symbol extraction for 18 languages ([list](docs/user_guide/cli.md#symbol-extraction))
- Snapshot management with Merkle tree diffs and rename detection
- WAL-backed durability, high-throughput I/O, thread-safe
- CLI, MCP server, and C-ABI plugins (ONNX/GLiNER/ColBERT, S3 storage, PDF via ZYP)
- Interactive relevance tuning through CLI tuning and doctor workflows

## Documentation

| Topic                | Link                                                                |
|----------------------|---------------------------------------------------------------------|
| Install              | [docs/user_guide/installation.md](docs/user_guide/installation.md)  |
| CLI reference        | [docs/user_guide/cli.md](docs/user_guide/cli.md)                    |
| MCP server           | [docs/user_guide/mcp.md](docs/user_guide/mcp.md)                    |
| Embeddings           | [docs/user_guide/embeddings.md](docs/user_guide/embeddings.md)      |
| Plugins              | [docs/PLUGINS.md](docs/PLUGINS.md)                                  |
| Build from source    | [docs/BUILD.md](docs/BUILD.md)                                      |
| Architecture         | [docs/architecture/](docs/architecture/)                            |
| Benchmarks           | [docs/benchmarks/README.md](docs/benchmarks/README.md)              |
| Changelog            | [docs/changelogs/](docs/changelogs/)                                |
| Roadmap              | [docs/roadmap.md](docs/roadmap.md)                                  |

## Links

- SourceHut: https://sr.ht/~trvon/yams/
- GitHub mirror: https://github.com/trvon/yams
- Docs site: https://yamsmemory.ai
- Discord: https://discord.gg/rTBmRHdTEc
- License: GPL-3.0-or-later

## Install

Supported: Linux x86_64/ARM64, macOS x86_64/ARM64, Windows x86_64.

```bash
# macOS
brew install trvon/yams/yams

# Docker
docker pull ghcr.io/trvon/yams:latest

# Debian/Ubuntu, Fedora/RHEL, Windows: see installation guide
```

Full install matrix and package repos: [docs/user_guide/installation.md](docs/user_guide/installation.md).

### Build from source

```bash
./setup.sh Release          # Linux/macOS (auto-detects toolchain, runs Conan + Meson)
meson compile -C build/release
```

```pwsh
./setup.ps1 Release         # Windows
meson compile -C build/release
```

Requires a C++20 toolchain (GCC 13+, Clang 16+, or MSVC 2022+ recommended), Meson, Ninja, CMake, pkg-config, and Conan. See [docs/BUILD.md](docs/BUILD.md).

## Quick Start

```bash
yams init                                # interactive; use --auto for headless
yams add ./README.md --tags docs
yams add src/ --recursive --include="*.cpp,*.h" --tags code

yams search "config file" --limit 5
yams grep "TODO" --include="*.cpp"

yams list --limit 20
yams get <hash> -o ./output.bin
```

Shell completions: `yams completion bash|zsh|fish|powershell`. Install instructions: [docs/user_guide/cli.md#cmd-completion](docs/user_guide/cli.md#cmd-completion).

## MCP Server

YAMS ships an MCP server over stdio (JSON-RPC) for AI assistants.

```bash
yams serve
```

```json
{
  "mcpServers": {
    "yams": { "command": "yams", "args": ["serve"] }
  }
}
```

Tool reference and MCP client setup: [docs/user_guide/mcp.md](docs/user_guide/mcp.md).

## Plugins

```bash
yams plugin list                                  # loaded plugins
yams plugin trust add ~/.local/lib/yams/plugins   # trust a directory
yams plugin health                                # status
yams doctor plugin onnx                           # diagnose
```

Plugin architecture, trust model, and bundled plugins (ONNX, S3, ZYP, GLiNER, symbol extractor): [docs/PLUGINS.md](docs/PLUGINS.md).

### GPU acceleration (ONNX)

| Platform | Provider   | Hardware                                    |
|----------|------------|---------------------------------------------|
| macOS    | CoreML     | Apple Silicon Neural Engine + GPU           |
| Linux    | CUDA       | NVIDIA GPUs                                 |
| Linux    | MIGraphX   | AMD GPUs (ROCm)                             |
| Windows  | DirectML   | Any DirectX 12 GPU (NVIDIA, AMD, Intel)     |

Auto-detected at build. Override with `YAMS_ONNX_GPU=auto|cuda|coreml|directml|migraphx|none`. Details: [plugins/onnx/README.md](plugins/onnx/README.md).

Default retrieval backend: `simeon` (training-free, model-free, 1024-d on new installs).

### Simeon embedding backend

YAMS now uses the training-free `simeon` backend by default for retrieval embeddings. No embedding model download is required for the normal semantic-search path.

Enable it via config:

```toml
[embeddings]
backend = "simeon"
preferred_model = "simeon-default"
embedding_dim = 1024  # existing vector DBs keep their stored dim
```

Simeon's output dimension is tunable — training-free, so higher dims have no training cost, only storage and compute cost:

| `embedding_dim` | Use when                                              |
|-----------------|-------------------------------------------------------|
| 384             | Tight-budget fallback; smallest index, fastest.       |
| 768             | More separability, fewer hash collisions; 2× storage. |
| 1024            | Default for new installs; balanced quality / size.    |
| 1536+           | Experimental headroom; diminishing returns past ~2k.  |

Set via config (`embedding_dim`). Internal ngram/sketch/projection knobs: [third_party/simeon/README.md](third_party/simeon/README.md).

Requirements: no model file, no ONNX embedding runtime, no external runtime deps. When building from source, fetch the submodule: `git submodule update --init --recursive`.

ONNX remains relevant for optional plugin paths such as GLiNER and ColBERT, not for the default retrieval embedding flow.

Benchmark summaries are maintained under [docs/benchmarks/](docs/benchmarks/).

## Troubleshooting

```bash
yams doctor              # full diagnostics
yams stats --verbose     # storage statistics
yams repair --all        # repair common issues
```

Build issues: [docs/BUILD.md](docs/BUILD.md). Empty `yams plugin list`? Add a trust path: `yams plugin trust add ~/.local/lib/yams/plugins`.

## Cite

```bibtex
@misc{yams,
  author = {Trevon Williams},
  title = {YAMS: Content-addressable storage with semantic search},
  year = {2025},
  publisher = {GitHub},
  url = {https://github.com/trvon/yams}
}
```
