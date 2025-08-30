# YAMS Documentation

Content-addressed storage with deduplication and semantic search.

## Quick Start

- [Installation](user_guide/installation.md) - Setup YAMS
- [CLI Guide](user_guide/cli.md) - Command reference  
- [API Docs](api/README.md) - Integration reference
- [Examples](user_guide/tutorials/README.md) - Usage examples

## User Docs

- [Installation](user_guide/installation.md) - Setup instructions
- [User Guide](user_guide/README.md) - Usage and configuration
- [CLI Reference](user_guide/cli.md) - Command documentation
- [Tutorials](user_guide/tutorials/README.md) - Step-by-step guides

## Developer Docs

- [Contributing](developer/contributing.md) - Development workflow
- [Architecture](developer/architecture/README.md) - System design
- [API Reference](api/README.md) - Integration guide
- [Testing](developer/testing/README.md) - Test coverage and strategy

## Operations

- [Deployment](operations/deployment.md) - Production setup
- [Monitoring](operations/monitoring.md) - Performance tracking
- [Troubleshooting](troubleshooting/search_issues.md) - Common issues
- [Performance Tuning](admin/performance_tuning.md) - Optimization

## Features

- [Content Storage](developer/architecture/README.md) - SHA-256 deduplication
- [Search](user_guide/search_guide.md) - Full-text and semantic
- [MCP Server](mcp_websocket_transport.md) - AI agent integration
- [REST API](api/README.md) - HTTP interface

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `YAMS_USE_CONAN` | OFF | Use Conan package manager |
| `YAMS_BUILD_CLI` | ON | CLI with optional TUI browser |
| `YAMS_BUILD_MCP_SERVER` | ON | MCP server |
| `YAMS_BUILD_TESTS` | OFF | Unit and integration tests |
| `YAMS_BUILD_BENCHMARKS` | OFF | Performance benchmarks |
| `YAMS_ENABLE_PDF` | ON | PDF text extraction support (may download PDFium via FetchContent) |
| `YAMS_ENABLE_TUI` | OFF | Enables TUI browser (adds ncurses via Conan; ImTUI via FetchContent) |
| `YAMS_ENABLE_ONNX` | ON | Enables ONNX Runtime features (pulls onnxruntime; may pull Boost transitively) |
| `CMAKE_BUILD_TYPE` | Release | Debug/Release/RelWithDebInfo |

## Resources

- [GitHub](https://github.com/trvon/yams) - Source code
- [Releases](https://github.com/trvon/yams/releases) - Downloads
- [Issues](https://github.com/trvon/yams/issues) - Bug reports
- [Discussions](https://github.com/trvon/yams/discussions) - Community

## Academic Work

- [Academic Paper](operations/academic_paper.md) - USENIX conference paper development
- [Paper PBI](delivery/paper-pbi.md) - Academic publication project tracking
- [Paper Source](/paper/) - LaTeX source and compilation