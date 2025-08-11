# YAMS - Yet Another Memory System

Persistent memory for LLMs and applications. Content-addressed storage with deduplication, semantic search, and full-text indexing.

## Features

- **Content-Addressed Storage** - SHA-256 based, ensures data integrity
- **Deduplication** - Block-level with Rabin fingerprinting
- **Compression** - Zstandard and LZMA with intelligent policies
- **Search** - Full-text (SQLite FTS5) and semantic (vector embeddings)
- **Crash Recovery** - Write-ahead logging for durability
- **High Performance** - 100MB/s+ throughput, thread-safe

## Build

### Requirements
- C++20 compiler (GCC 11+, Clang 14+)
- CMake 3.20+
- OpenSSL 3.0+
- SQLite3
- Protocol Buffers 3.0+

### Dependencies

#### macOS
```bash
brew install cmake openssl@3 protobuf sqlite3
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)
```

#### Linux
```bash
# Ubuntu/Debian
apt install build-essential cmake libssl-dev libsqlite3-dev protobuf-compiler

# Fedora/RHEL
dnf install cmake openssl-devel sqlite-devel protobuf-compiler
```

### Build Profiles

#### Quick Start - Release Build
For end users who just want the CLI and MCP server:
```bash
cmake -B build -DYAMS_BUILD_PROFILE=release
make -j$(nproc)
sudo make install
# Installs: yams, yams-mcp-server
```

**Note**: If you encounter WebSocket compilation errors, build without MCP server:
```bash
cmake -DYAMS_BUILD_PROFILE=custom -DYAMS_BUILD_MCP_SERVER=OFF ..
```

#### Development Build
For developers who need everything including tests:
```bash
mkdir build && cd build
cmake -DYAMS_BUILD_PROFILE=dev ..
make -j$(nproc)
ctest --output-on-failure
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `YAMS_BUILD_PROFILE` | custom | Preset: `release`, `dev`, or `custom` |
| `YAMS_BUILD_CLI` | ON | Build command-line interface |
| `YAMS_BUILD_MCP_SERVER` | ON | Build MCP server  |
| `YAMS_BUILD_MAINTENANCE_TOOLS` | OFF | Build gc and stats tools |
| `YAMS_BUILD_TESTS` | OFF | Build unit tests |
| `YAMS_BUILD_BENCHMARKS` | OFF | Build performance benchmarks |
| `CMAKE_BUILD_TYPE` | Release | Build type: `Debug`, `Release`, `RelWithDebInfo` |

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

### LLM Integration Guide

YAMS is designed to work seamlessly with Large Language Models through simple, pipeline-friendly commands:

```bash
# Store conversation context
echo "User asked about X, I explained Y" | yams add -

# Store code snippets
echo "def function(): return 42" | yams add -

# Retrieve previous context
yams list --format minimal | tail -5 | while read hash; do
  yams get $hash
done

# Search for relevant content
yams search "error handling" --format json

# Chain commands
yams list --format minimal | \
  grep -v manifest | \
  head -10 | \
  xargs -I {} yams get {} > combined.txt
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

### Best Practices for LLMs

1. **Always use explicit paths**: Use `--data-dir` to avoid ambiguity
2. **Use JSON output**: Add `--json` flag for structured data
3. **Prefer stdin input**: Pipe content directly instead of creating temp files
4. **Use simple commands**: `add`, `search`, `get`, `stats` instead of complex TUI commands
5. **Tag consistently**: Use descriptive tags like "code", "config", "memory", "context"

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

#### When to Use MCP vs CLI

**Use MCP when:**
- Working in Claude Desktop or MCP-enabled environment
- Need real-time memory access during conversations
- Want automatic context persistence
- Building knowledge bases incrementally

**Use CLI when:**
- Scripting or automation needed
- Working with git hooks or CI/CD
- Batch processing files
- Integration with existing tools

#### MCP Server Setup

1. **Install MCP Server**:
```bash
# Clone and build MCP server
cd tools/mcp-server
npm install
npm run build
```

2. **Configure Claude Desktop**:
Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "yams": {
      "command": "/path/to/yams/mcp-server",
      "args": ["--storage", "$HOME/yams"],
      "env": {
        "YAMS_STORAGE": "$HOME/yams"
      }
    }
  }
}
```

3. **MCP Usage Examples**:
```typescript
// Store via MCP
await mcp.store({
  content: "Implementation details...",
  tags: ["feature", "auth"],
  metadata: { version: "2.1" }
});

// Search via MCP
const results = await mcp.search({
  query: "authentication flow",
  limit: 5
});
```

## Architecture

```
yams/
├── include/yams/    # Headers
├── src/             # Implementation
├── tests/           # Unit tests
├── benchmarks/      # Performance
└── tools/           # CLI
```

### Components
- **Core** - Types, error handling
- **Storage** - Content-addressed store
- **Chunking** - Content-defined chunking
- **Compression** - Multi-algorithm
- **Metadata** - SQLite with FTS5
- **Search** - Full-text and semantic
- **WAL** - Write-ahead logging

## Development

```bash
# Debug build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make -j

# Test
ctest --output-on-failure

# Coverage
cmake -DYAMS_ENABLE_COVERAGE=ON ..
make coverage
```

## Troubleshooting

### Build Issues

**OpenSSL not found on macOS**:
```bash
export OPENSSL_ROOT_DIR=$(brew --prefix openssl@3)
export PKG_CONFIG_PATH="$OPENSSL_ROOT_DIR/lib/pkgconfig"
```

**Protobuf compilation errors**:
```bash
# Ensure protoc is in PATH
which protoc
# Reinstall if needed
brew reinstall protobuf
```

### Runtime Issues

**Storage initialization fails**:
```bash
# Check permissions
ls -la ~/yams
# Fix permissions
chmod -R 755 ~/yams
```

**Search returns no results**:
```bash
# Rebuild search index
yams reindex --full
# Check metadata database
yams stats --verbose
```

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
