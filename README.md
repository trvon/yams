[![Release](https://github.com/trvon/yams/actions/workflows/release.yml/badge.svg)](https://github.com/trvon/yams/actions/workflows/release.yml) [![CI](https://github.com/trvon/yams/actions/workflows/ci.yml/badge.svg)](https://github.com/trvon/yams/actions/workflows/ci.yml)
# YAMS - Yet Another Memory System

Persistent memory for LLMs and applications. Content-addressed storage with deduplication, semantic search, and full-text indexing.

## Features

- **Content-Addressed Storage** - SHA-256 based, ensures data integrity
- **Deduplication** - Block-level with Rabin fingerprinting
- **Compression** - Zstandard and LZMA with intelligent policies
- **Search** - Full-text (SQLite FTS5) and semantic (vector embeddings)
- **Crash Recovery** - Write-ahead logging for durability
- **High Performance** - 100MB/s+ throughput, thread-safe

My prompt for CLI usage is [PROMPT.md](docs/PROMPT.md) and [PROMPT-eng.md](docs/PROMPT-eng.md) for programming.

## Installation

### Quick Install (Recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash
```

This script downloads **pre-built binaries** and will:
- Auto-detect your platform (Linux/macOS, x86_64/ARM64)
- Download the appropriate binary from GitHub Releases
- Install to `~/.local/bin` by default
- Set up shell completions if available
- Verify the installation

> **Note:** This is the recommended installation method for all users. No build tools or dependencies required.

**Custom installation options:**
```bash
# Install specific version
curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash -s -- --version 0.1.2

# Install to custom directory
curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash -s -- --install-dir /usr/local/bin

# Environment variables
export YAMS_VERSION="0.1.2"
export YAMS_INSTALL_DIR="/usr/local/bin"
curl -fsSL https://raw.githubusercontent.com/trvon/yams/main/install.sh | bash
```

**Supported platforms:**
- Linux x86_64, ARM64
- macOS x86_64 (Intel), ARM64 (Apple Silicon)

### Package Managers

**Docker:**
```bash
docker run --rm -it ghcr.io/trvon/yams:latest --version
```

**Homebrew (coming soon):**
```bash
brew tap trvon/yams && brew install yams
```

### Build from Source (For Developers)

> **⚠️ Note:** Building from source is only needed for development. Most users should use the install script above.

**Working Build Method (Conan):**
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

**Requirements:**
- C++20 compiler (GCC 11+, Clang 14+, AppleClang 14+)
- CMake 3.20+
- Python 3.8+ (for Conan)

> **Known Issue:** Traditional CMake builds (without Conan) currently have dependency resolution issues. Use Conan builds for reliable compilation.

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `YAMS_USE_CONAN` | OFF | Use Conan package manager |
| `YAMS_BUILD_CLI` | ON | CLI with TUI browser |
| `YAMS_BUILD_MCP_SERVER` | ON | MCP server (requires Boost) |
| `YAMS_BUILD_TESTS` | OFF | Unit and integration tests |
| `YAMS_BUILD_BENCHMARKS` | OFF | Performance benchmarks |
| `YAMS_ENABLE_PDF` | ON | PDF text extraction support |
| `CMAKE_BUILD_TYPE` | Release | Debug/Release/RelWithDebInfo |

### Dependencies

```bash
# macOS
brew install openssl@3 protobuf sqlite3 ncurses
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)

# Linux
apt install libssl-dev libsqlite3-dev protobuf-compiler libncurses-dev
```

## Setup

```bash
# Initialize with XDG defaults (non-interactive)
yams init --non-interactive

# Optional: custom storage root
export YAMS_STORAGE="$HOME/.local/share/yams"
yams init --non-interactive

# Print resulting config (secrets masked)
yams init --non-interactive --print
```

### Troubleshooting
If you see:
```text
ERROR: The default build profile '/home/trevon/.conan2/profiles/default' doesn't exist.
You need to create a default profile (type 'conan profile detect' command)
or specify your own profile with '--profile:build=<myprofile>'
```

Fix:
```bash
# Create default profile
conan profile detect --force

# Optional: ensure C++20 in the default profile
# Linux/macOS (GNU sed):
sed -i 's/compiler.cppstd=.*/compiler.cppstd=20/' ~/.conan2/profiles/default || true
# macOS (BSD sed):
# sed -i '' 's/compiler.cppstd=.*/compiler.cppstd=20/' ~/.conan2/profiles/default || true
```

Then re-run:
```bash
conan install . \
  --output-folder=build/conan-ninja-release \
  -s build_type=Release \
  --build=missing

cmake --preset conan-ninja-release
cmake --build build/conan-ninja-release -j
```

#### PDF Support Issues

If PDF extraction fails or PDFium download fails:

```bash
# Disable PDF support temporarily
cmake -B build -DYAMS_ENABLE_PDF=OFF

# Or explicitly specify a different PDFium version if needed
# (check https://github.com/bblanchon/pdfium-binaries/releases for available versions)
```

If you see network errors during PDFium download:
- Check internet connectivity
- Corporate firewalls may block GitHub releases
- Consider using a VPN or different network
- PDFium binaries are ~20MB per platform

### LLM Integration Guide

YAMS is designed to work seamlessly with Large Language Models through simple, pipeline-friendly commands:

```bash
# Store conversation context with descriptive name
echo "User asked about X, I explained Y" | yams add - --name "context-$(date +%Y%m%d).txt"

# Store code snippets with tags
echo "def function(): return 42" | yams add - --name "helper.py" --tags "python,utils"

# Delete temporary files by pattern
yams delete --pattern "temp_*.txt" --force

# Delete multiple specific files
yams delete --names "draft1.md,draft2.md,notes.txt"

# Retrieve documents by name (coming soon)
# yams get --name "meeting-notes.txt"

# Search with fuzzy matching
yams search "databse" --fuzzy --similarity 0.8

# List with rich metadata
yams list --format table --limit 20

# Chain commands for batch operations
yams list --format minimal | tail -5 | while read hash; do
  yams get $hash
done

# Preview deletions before executing
yams delete --pattern "*.log" --dry-run
```

#### Best Practices for LLMs

1. **Use stdin for content storage**: Avoids file creation
   ```bash
   echo "content to store" | yams add -
   ```

2. **Use minimal format for piping**: Clean output for processing
   ```bash
   yams list --format minimal | head -5
   ```

3. **Explicit data directory**: Always specify storage location
   ```bash
   yams --data-dir /tmp/project-memory add -
   ```

4. **JSON for structured data**: Parse responses easily
   ```bash
   yams stats --format json | jq '.totalObjects'
   ```

5. **Direct stdout retrieval**: No intermediate files
   ```bash
   yams get <hash> | process_somehow
   ```

## Usage

### MCP Server

Start the MCP server over WebSocket:
```bash
yams serve --transport websocket --host 127.0.0.1 --port 8080 --path /mcp
```

Use TLS (wss):
```bash
yams serve --transport websocket --host your.domain --port 443 --path /mcp --ssl
```

StdIO transport (recommended for local integration and Claude Desktop):
```bash
yams serve --transport stdio
```

### Claude Desktop (MCP) Integration

Use stdio (recommended). Add this to your Claude Desktop config (e.g., ~/Library/Application Support/Claude/claude_desktop_config.json):
```json
{
  "mcpServers": {
    "yams": {
      "command": "/usr/local/bin/yams",
      "args": ["serve", "--transport", "stdio"],
      "env": {
        "YAMS_STORAGE": "$HOME/.local/share/yams"
      }
    }
  }
}
```

Alternative (WebSocket) for clients that support ws:
```json
{
  "mcpServers": {
    "yams-ws": {
      "command": "/usr/local/bin/yams",
      "args": ["serve", "--transport", "websocket", "--host", "127.0.0.1", "--port", "8080", "--path", "/mcp"],
      "env": {
        "YAMS_STORAGE": "$HOME/.local/share/yams"
      }
    }
  }
}
```

### CLI Usage

#### Initialize Storage
```bash
# One-time setup with defaults
yams init --non-interactive --no-keygen

# Custom storage location
yams --data-dir /path/to/storage init --non-interactive
```

#### Store Documents
```bash
# Add a file
yams add file.txt

# Add from stdin
echo "content" | yams add -
cat file.txt | yams add -

# Add multiple files
find . -name "*.txt" -exec yams add {} \;
```

#### List Documents
```bash
# Table format (default)
yams list

# JSON output for programmatic use
yams list --format json

# Just hashes for piping
yams list --format minimal

# Sort and filter
yams list --sort size --reverse --limit 10
yams list --sort date
```

#### Retrieve Documents
```bash
# Output to stdout
yams get <hash>

# Save to file
yams get <hash> -o output.txt

# Pipe to other commands
yams get <hash> | grep pattern
yams get <hash> | wc -l

# Get first document from list
yams list --format minimal --limit 1 | xargs yams get
```

#### Browse Documents (TUI)
```bash
# Launch ranger-style browser
yams browse

# Navigation:
#   j/k or ↑/↓     - Move up/down
#   h/l or ←/→     - Switch columns
#   g/G           - Jump to top/bottom
#   d then D      - Delete document
#   r             - Refresh
#   ?             - Help
#   q or Esc      - Quit
```

#### Search
yams search "query" --limit 10
yams search "error" --type "log"

# Retrieve
yams get <hash>
yams get <hash> --output file.txt

# List & Stats
yams list --recent 20
yams stats --json  # JSON output for scripts

# Browse (interactive TUI)
yams browse  # Interactive document browser
```

### API
```cpp
#include <yams/api/content_store.h>

auto store = yams::api::createContentStore(getenv("YAMS_STORAGE"));

// Store
yams::api::ContentMetadata meta{.tags = {"code", "v1.0"}};
auto result = store->store("file.txt", meta);

// Search
auto results = store->search("query", 10);

// Retrieve
store->retrieve(hash, "output.txt");
```

### Python
```python
import subprocess, json

def yams_store(content, tags=[], type="text"):
    cmd = ["yams", "store", content]
    if tags: cmd.extend(["--tags", ",".join(tags)])
    if type: cmd.extend(["--type", type])
    return subprocess.run(cmd, capture_output=True, text=True)

def yams_search(query, limit=10):
    cmd = ["yams", "search", query, "--limit", str(limit)]
    return subprocess.run(cmd, capture_output=True, text=True)
```

## LLM Usage Guide

### Quick Start for LLMs

```bash
# Always specify data directory explicitly
export YAMS_STORAGE="/tmp/yams-data"

# Initialize once (quiet mode)
yams --data-dir "$YAMS_STORAGE" init --non-interactive --force

# Store content from stdin (most common for LLMs)
echo "Important information to remember" | yams --data-dir "$YAMS_STORAGE" add --tags "memory"

# Search for content
yams --data-dir "$YAMS_STORAGE" search "important" --json

# Retrieve specific document
yams --data-dir "$YAMS_STORAGE" get <hash> --json

# Get storage statistics
yams --data-dir "$YAMS_STORAGE" stats --json
```

### Common Patterns

```bash
# Store code changes
git diff | yams --data-dir "$YAMS_STORAGE" add --tags "git-diff,$(date +%Y%m%d)"

# Store conversation context
echo "User asked about: $TOPIC" | yams --data-dir "$YAMS_STORAGE" add --tags "context,$TOPIC"

# Store external documentation
curl -s "$API_DOCS_URL" | yams --data-dir "$YAMS_STORAGE" add --tags "api-docs,external"

# Search and retrieve in one line
hash=$(yams --data-dir "$YAMS_STORAGE" search "$QUERY" --json | jq -r '.results[0].hash')
yams --data-dir "$YAMS_STORAGE" get "$hash"
```

## LLM Integration

### When to Use YAMS

YAMS is ideal for LLMs to maintain persistent memory across sessions:

- **Code Development**: Track changes, store working versions, remember context
- **Research**: Cache web content, API responses, documentation
- **Conversation Context**: Store important discussions, decisions, requirements
- **Knowledge Base**: Build searchable repository of project knowledge

### CLI Usage Examples

#### Development Workflow
```bash
# Store current code state before making changes
git diff | yams store - --tags "pre-refactor,auth-module,$(date +%Y%m%d)"

# Track implementation decisions
yams store "Decided to use JWT tokens with 24h expiry for auth" \
  --tags "decision,auth,architecture"

# Store error context for debugging
yams store "$(tail -100 app.log)" --tags "error,production,$(date +%Y%m%d-%H%M)"

# Save working implementation
yams store-file auth_handler.py --tags "working,auth,v2.1"
```

#### Research & Documentation
```bash
# Store web research
curl -s https://api.example.com/docs | yams store - \
  --tags "api-docs,external,example-api" \
  --source "https://api.example.com/docs"

# Cache fetched content
yams store "$WEB_CONTENT" --tags "research,oauth,implementation-guide"

# Store meeting notes
yams store-file meeting-notes-2024-01-15.md --tags "meeting,requirements,client"
```

#### Search Patterns
```bash
# Find related code changes
yams search "authentication" --type "code" --limit 10

# Retrieve specific version
yams search "working auth" --tags "v2.1"

# Get recent errors
yams list --recent 20 --tags "error"

# Semantic search for concepts
yams search "token expiry handling"
```

### MCP (Model Context Protocol) Integration

MCP provides direct integration with Claude Desktop and other MCP-compatible clients.

### Performance Tuning

**Optimize for large files**:
```bash
# Adjust chunk size for better deduplication
export YAMS_CHUNK_SIZE=64KB  # Default: 16KB

# Increase cache size
export YAMS_CACHE_SIZE=1GB   # Default: 256MB
```

**Reduce memory usage**:
```bash
# Use streaming mode for large files
yams store-file --stream large-file.bin

# Enable compression
export YAMS_COMPRESSION=zstd  # Options: none, zstd, lzma
```

## License

Apache-2.0
