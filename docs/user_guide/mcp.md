# YAMS MCP Server Guide

A comprehensive guide for using YAMS as a Model Context Protocol (MCP) server with AI assistants.

## Overview

The YAMS MCP server exposes content-addressable storage and search capabilities through the Model Context Protocol, enabling AI assistants to:
- Search and retrieve documents from your knowledge base
- Store and manage content with rich metadata
- Perform regex searches across indexed content
- Access file type analytics and statistics

## Quick Start

### Running the MCP Server

```bash
# Stdio transport (for Claude Desktop and similar)
yams serve

# WebSocket transport (for network access)
yams serve --transport websocket --port 8080

# Docker (stdio transport)
docker run -i ghcr.io/trvon/yams:latest serve

# Docker (websocket transport)
docker run -p 8080:8080 ghcr.io/trvon/yams:latest serve --transport websocket --host 0.0.0.0
```

## Transport Options

### Stdio Transport (Default)

The stdio transport uses standard input/output for JSON-RPC communication. This is the standard for local MCP integrations.

**When to use:**
- Claude Desktop integration
- Local AI assistants
- Direct process communication

**Characteristics:**
- No network exposure
- Secure by default
- Simple configuration

### WebSocket Transport

WebSocket transport enables network-based communication, useful for remote or containerized deployments.

**When to use:**
- Remote AI assistants
- Container deployments
- Multi-client scenarios

**Options:**
- `--host <address>`: Bind address (default: 127.0.0.1)
- `--port <number>`: Port number (default: 8080)
- `--path <path>`: WebSocket path (default: /mcp)
- `--ssl`: Enable TLS/WSS

## Claude Desktop Integration

### Configuration

Add YAMS to your Claude Desktop configuration file:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"],
      "env": {
        "YAMS_STORAGE": "/path/to/your/yams/storage"
      }
    }
  }
}
```

### Docker Configuration for Claude Desktop

```json
{
  "mcpServers": {
    "yams": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "-v", "/path/to/yams/storage:/data",
        "-e", "YAMS_STORAGE=/data",
        "ghcr.io/trvon/yams:latest",
        "serve"
      ]
    }
  }
}
```

After updating the configuration, restart Claude Desktop to load the MCP server.

## Docker Usage

### Basic Docker Commands

```bash
# Run with local storage mounted
docker run -i --rm \
  -v ~/.local/share/yams:/data \
  -e YAMS_STORAGE=/data \
  ghcr.io/trvon/yams:latest serve

# WebSocket server accessible from network
docker run -d --name yams-mcp \
  -p 8080:8080 \
  -v ~/.local/share/yams:/data \
  -e YAMS_STORAGE=/data \
  ghcr.io/trvon/yams:latest \
  serve --transport websocket --host 0.0.0.0

# With custom configuration
docker run -i --rm \
  -v ~/.config/yams:/config \
  -v ~/.local/share/yams:/data \
  -e YAMS_CONFIG=/config/config.toml \
  -e YAMS_STORAGE=/data \
  ghcr.io/trvon/yams:latest serve
```

## Available Tools

The MCP server provides the following tools to AI assistants:

### search_documents

Search for documents using various strategies.

**Parameters:**
- `query` (required): Search query string
- `limit`: Maximum results (default: 10)
- `fuzzy`: Enable fuzzy matching (default: false)
- `similarity`: Fuzzy match threshold 0.0-1.0 (default: 0.7)
- `type`: Search type - keyword|semantic|hybrid (default: hybrid)
- `paths_only`: Return only file paths (default: false)

**Example:**
```json
{
  "tool": "search_documents",
  "arguments": {
    "query": "configuration management",
    "limit": 5,
    "type": "hybrid"
  }
}
```

### grep_documents

Search using regular expressions across indexed content.

**Parameters:**
- `pattern` (required): Regex pattern
- `paths`: Files/directories to search (optional)
- `ignore_case`: Case-insensitive search
- `line_numbers`: Include line numbers
- `context`: Lines of context around matches

**Example:**
```json
{
  "tool": "grep_documents", 
  "arguments": {
    "pattern": "class\\s+\\w+Handler",
    "ignore_case": false,
    "line_numbers": true,
    "context": 2
  }
}
```

### store_document

Store a document with metadata.

**Parameters:**
- `path` (required): File path to store
- `tags`: Array of tags
- `metadata`: Key-value metadata object

### retrieve_document

Retrieve document by hash.

**Parameters:**
- `hash` (required): Document hash
- `outputPath`: Where to save (optional)
- `graph`: Include related documents
- `depth`: Graph traversal depth (1-5)

### get_stats

Get storage statistics and analytics.

**Parameters:**
- `detailed`: Include detailed breakdown
- `file_types`: Include file type analysis

### list_documents

List stored documents with filtering.

**Parameters:**
- `limit`: Maximum documents to return
- `offset`: Pagination offset
- `sort_by`: Sort field (name|size|created|modified)
- `sort_order`: asc|desc
- `recent`: Show N most recent documents

## Testing the MCP Server

### Manual Testing with JSON-RPC

Test the MCP server directly with JSON-RPC messages:

```bash
# Initialize the connection
echo '{"jsonrpc":"2.0","method":"initialize","params":{"clientInfo":{"name":"test","version":"1.0"}},"id":1}' | yams serve

# List available tools
echo '{"jsonrpc":"2.0","method":"tools/list","id":2}' | yams serve

# Search for documents
echo '{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search_documents","arguments":{"query":"test"}},"id":3}' | yams serve
```

### Using curl with WebSocket

```bash
# Start WebSocket server
yams serve --transport websocket &

# Send requests via curl (requires wscat or similar)
wscat -c ws://localhost:8080/mcp
> {"jsonrpc":"2.0","method":"initialize","params":{},"id":1}
```

### Test Script

Create a test script `test-mcp.sh`:

```bash
#!/bin/bash

# Start server in background
yams serve --transport websocket --port 9999 &
SERVER_PID=$!

# Wait for startup
sleep 2

# Test with wscat or curl
# ... test commands ...

# Cleanup
kill $SERVER_PID
```

## Integration Examples

### Python Client

```python
import json
import subprocess

class YamsMCP:
    def __init__(self):
        self.proc = subprocess.Popen(
            ['yams', 'serve'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        self.request_id = 0
    
    def call_tool(self, tool_name, arguments):
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments
            },
            "id": self.request_id
        }
        self.proc.stdin.write(json.dumps(request) + '\n')
        self.proc.stdin.flush()
        response = self.proc.stdout.readline()
        return json.loads(response)

# Usage
mcp = YamsMCP()
result = mcp.call_tool("search_documents", {"query": "test", "limit": 5})
```

### Node.js Client

```javascript
const { spawn } = require('child_process');

class YamsMCP {
  constructor() {
    this.proc = spawn('yams', ['serve']);
    this.requestId = 0;
  }
  
  async callTool(toolName, args) {
    this.requestId++;
    const request = {
      jsonrpc: "2.0",
      method: "tools/call",
      params: { name: toolName, arguments: args },
      id: this.requestId
    };
    
    this.proc.stdin.write(JSON.stringify(request) + '\n');
    
    return new Promise((resolve) => {
      this.proc.stdout.once('data', (data) => {
        resolve(JSON.parse(data.toString()));
      });
    });
  }
}
```

## Troubleshooting

### Server doesn't respond to Ctrl+C

The server should respond immediately to Ctrl+C. If it doesn't:
1. Check you're running the latest version
2. Try `kill -TERM <pid>` instead
3. Report an issue if the problem persists

### "No startup message" when running serve

You should see startup messages on stderr:
```
=== YAMS MCP Server ===
Transport: stdio (JSON-RPC over stdin/stdout)
Status: Waiting for client connection...
Press Ctrl+C to stop the server
```

If not visible:
1. Check stderr isn't being redirected
2. Ensure you're not in a non-interactive environment
3. Try `--transport websocket` for testing

### Claude Desktop doesn't show YAMS tools

1. Verify configuration file location and JSON syntax
2. Check YAMS is in PATH or use absolute path
3. Restart Claude Desktop after configuration changes
4. Check Claude Desktop logs for errors

### Docker container exits immediately

For stdio transport, the container needs `-i` (interactive) flag:
```bash
docker run -i ghcr.io/trvon/yams:latest serve  # Correct
docker run ghcr.io/trvon/yams:latest serve     # Wrong - will exit
```

### WebSocket connection refused

1. Check the port isn't already in use
2. Use `--host 0.0.0.0` for external access (not just 127.0.0.1)
3. Verify firewall/security group rules
4. Check Docker port mapping with `-p`

## Security Considerations

### Stdio Transport
- Runs with the permissions of the calling process
- No network exposure
- Ideal for local AI assistants

### WebSocket Transport
- Consider using `--ssl` for TLS encryption
- Bind to 127.0.0.1 unless external access is needed
- Use firewall rules to restrict access
- Consider authentication mechanisms for production

### Docker Deployment
- Use read-only mounts where possible
- Run as non-root user
- Limit container resources
- Use specific version tags, not `latest`

## Performance Tips

1. **Index your content first**: Use `yams add` to pre-index files
2. **Use paths_only**: Reduces response size for large result sets
3. **Limit results**: Use reasonable limits to avoid overwhelming the AI
4. **Cache common queries**: YAMS caches search results automatically
5. **Use appropriate search type**: 
   - `keyword` for exact matches
   - `fuzzy` for approximate matches
   - `hybrid` for best overall results

## Best Practices

1. **Initialize storage before starting MCP**: Run `yams init` first
2. **Set YAMS_STORAGE environment variable**: Ensures consistent storage location
3. **Use descriptive metadata**: Tag and name documents for better search
4. **Regular maintenance**: Run `yams stats` to monitor storage health
5. **Test configuration**: Verify MCP tools are accessible before production use

---

For more information:
- [CLI Reference](cli.md) - Complete command-line documentation
- [API Documentation](../api/mcp_tools.md) - Detailed tool schemas
- [GitHub Issues](https://github.com/trvon/yams/issues) - Report problems or request features