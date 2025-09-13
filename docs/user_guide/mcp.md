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

# Docker (stdio transport)
docker run -i ghcr.io/trvon/yams:latest serve
```

## Transport Options

### Stdio Transport

The stdio transport uses standard input/output for JSON-RPC communication. This is the standard for local MCP integrations.

Framing compatibility:
- The server auto-detects the client's framing on first request and mirrors it in responses:
  - If the client sends raw JSON (ndjson), responses are newline-delimited JSON.
  - If the client sends LSP-style headers (Content-Length), responses use headers + payload (+ CRLF).
This keeps YAMS compatible with multiple MCP clients (Inspector, Goose/Jan, LM Studio).

**When to use:**
- Claude Desktop integration
- Local AI assistants
- Direct process communication

**Characteristics:**
- No network exposure
- Secure by default
- Simple configuration

### HTTP + SSE Transport (Default)

The HTTP + SSE transport provides a network-accessible interface for the MCP server, enabling web-based or remote clients.

**When to use:**
- Web-based AI applications
- Remote AI assistants
- Cross-language integrations

**Characteristics:**
- Network accessible
- Supports Server-Sent Events for notifications
- Configurable bind address and port

**Configuration:**
- Default bind address: `127.0.0.1:8757`
- JSON-RPC endpoint: `POST /mcp/jsonrpc`
- SSE endpoint: `GET /mcp/events?session=<sessionId>`
- To change the bind address, set the environment variable: `YAMS_MCP_HTTP_BIND=your_address:your_port`

**Session Management:**
- Initialize via `POST /mcp/jsonrpc`; the result of the `initialize` method will include a `sessionId`.
- Open an SSE connection at `GET /mcp/events?session=<sessionId>` using the obtained `sessionId` to receive `notifications/ready`, logs, and progress updates.
- Send the `initialized` notification via `POST /mcp/jsonrpc` using the same session to trigger the server's readiness.



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

## Prompt Templates

YAMS exposes basic prompt templates via `prompts/list` and `prompts/get`. In addition to built-ins (e.g., `search_codebase`, `summarize_document`, `rag/*`), you can add file-backed templates:

- Create a directory for prompts (Markdown files):
  - Default search path order:
    1) `YAMS_MCP_PROMPTS_DIR` (env override)
    2) `[mcp_server].prompts_dir` in `config.toml`
    3) `$XDG_DATA_HOME/yams/prompts` or `~/.local/share/yams/prompts`
    4) `./docs/prompts` (when running from repo)
- File naming: `PROMPT-*.md` → name exposed as `*` with dashes converted to underscores.
  - Example: `PROMPT-research-mcp.md` → name `research_mcp`
- prompts/list merges built-ins with any file-backed templates.
- prompts/get returns the template content as a single assistant text message.

Tips:
- To seed prompts, copy examples from the repo's `docs/prompts` into your prompts dir or provide a gist/raw URL for users to download.^
- Future: `yams init prompts` can be wired to fetch/populate a starter set.

## Readiness and Initialization

During startup, the daemon may report an initializing state. The CLI (yams status) and the MCP server will indicate not-ready services and progress. If models are configured to preload and you see prolonged initialization, consider lazy loading and verify model/provider readiness. Refer to yams stats -v for recommendations and detailed service status.

## Docker Usage

### Basic Docker Commands

```bash
# Run with local storage mounted
docker run -i --rm \
  -v ~/.local/share/yams:/data \
  -e YAMS_STORAGE=/data \
  ghcr.io/trvon/yams:latest serve



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
- `fuzzy`: Enable fuzzy matching (default: true)
- `similarity`: Fuzzy match threshold 0.0-1.0 (default: 0.7)
- `type`: Search type - keyword|semantic|hybrid (default: hybrid)
- `paths_only`: Return only file paths (default: false)

Defaults: When the client omits search options, the server runs hybrid search with fuzzy enabled (similarity 0.7).

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

## Hot/Cold Modes

You can control hot/cold behavior via startup flags (passed to the MCP server):
- --list-mode hot_only|cold_only|auto
- --grep-mode hot_only|cold_only|auto
- --retrieval-mode hot_only|cold_only|auto

Environment alternatives:
- YAMS_LIST_MODE, YAMS_GREP_MODE, YAMS_RETRIEVAL_MODE

Note: paths_only typically engages hot paths where supported, and reduces response size for large result sets.

## Additional Tools

The MCP server also exposes additional tools for batch and collection workflows. See the API reference for schemas and examples: ../api/mcp_tools.md

- add_directory: Index directory contents recursively (recursive defaults to true).
- downloader.download: Fetch artifacts by URL into CAS with checksum support.
- restore_collection, restore_snapshot: Restore documents to the filesystem.
- list_collections, list_snapshots: Enumerate collections and snapshots.

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

## Integration Examples

### Python Client

This example shows how to communicate with the `yams serve` process over stdio using the required `Content-Length` message framing.

```python
import json
import subprocess
import os
import re

class YamsMCP:
    def __init__(self):
        # Use text=False (binary mode) for correct byte handling
        self.proc = subprocess.Popen(
            ['yams', 'serve'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        self.request_id = 0

    def send_request(self, request_data):
        """Sends a JSON-RPC request with Content-Length framing."""
        message = json.dumps(request_data)
        body = message.encode('utf-8')
        header = f"Content-Length: {len(body)}\r\n\r\n".encode('utf-8')
        
        self.proc.stdin.write(header + body)
        self.proc.stdin.flush()

    def read_response(self):
        """Reads a JSON-RPC response with Content-Length framing."""
        line = self.proc.stdout.readline().decode('utf-8')
        if not line:
            stderr = self.proc.stderr.read().decode('utf-8')
            raise ConnectionError(f"Failed to read header. Server stderr:\n{stderr}")

        match = re.match(r"Content-Length: (\d+)", line)
        if not match:
            raise ConnectionError(f"Invalid header received: {line}")

        content_length = int(match.group(1))
        # Read the blank line separator
        self.proc.stdout.read(2) # Reads \r\n
        
        body_bytes = self.proc.stdout.read(content_length)
        return json.loads(body_bytes.decode('utf-8'))

    def call(self, method, params):
        """Makes a generic JSON-RPC call."""
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self.request_id
        }
        self.send_request(request)
        return self.read_response()

# --- Usage Example ---
if __name__ == "__main__":
    mcp = YamsMCP()
    
    print("--> Initializing connection...")
    init_response = mcp.call("initialize", {
        "clientInfo": {
            "name": "python-example-client",
            "version": "1.0"
        }
    })
    print("<-- Server initialized:", json.dumps(init_response, indent=2))

    print("\n--> Listing tools...")
    tools = mcp.call("tools/list", {})
    print("<-- Tools available:", json.dumps(tools, indent=2))

    print("\n--> Calling 'search' tool...")
    search_result = mcp.call("tools/call", {
        "name": "search",
        "arguments": {"query": "test", "limit": 5}
    })
    print("<-- Search result:", json.dumps(search_result, indent=2))
    
    # Cleanly shutdown the process
    mcp.proc.terminate()
```

### Node.js Client

This example shows how to communicate with the `yams serve` process over stdio using the required `Content-Length` message framing. It includes a simple parser to handle the response stream.

```javascript
const { spawn } = require('child_process');

class YamsMCP {
  constructor() {
    this.proc = spawn('yams', ['serve']);
    this.requestId = 0;
    this.responseCallbacks = new Map();

    let buffer = Buffer.alloc(0);
    this.proc.stdout.on('data', (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      while (true) {
        const bufferStr = buffer.toString('utf-8');
        const match = bufferStr.match(/Content-Length: (\d+)\r\n\r\n/);
        if (!match) {
          break;
        }

        const contentLength = parseInt(match[1], 10);
        const messageStart = match.index + match[0].length;

        if (buffer.length < messageStart + contentLength) {
          break;
        }

        const messageBytes = buffer.slice(messageStart, messageStart + contentLength);
        buffer = buffer.slice(messageStart + contentLength);

        const response = JSON.parse(messageBytes.toString('utf-8'));
        if (this.responseCallbacks.has(response.id)) {
          this.responseCallbacks.get(response.id)(response);
          this.responseCallbacks.delete(response.id);
        }
      }
    });
    
    this.proc.stderr.on('data', (data) => {
        console.error(`Server STDERR: ${data}`);
    });
  }

  _send(request) {
    const message = JSON.stringify(request);
    const body = Buffer.from(message, 'utf-8');
    const header = `Content-Length: ${body.length}\r\n\r\n`;
    this.proc.stdin.write(header);
    this.proc.stdin.write(body);
  }

  call(method, params) {
    this.requestId++;
    const id = this.requestId;
    const request = {
      jsonrpc: "2.0",
      method: method,
      params: params,
      id: id
    };

    return new Promise((resolve, reject) => {
      this.responseCallbacks.set(id, resolve);
      this._send(request);
      // Optional: Add a timeout
      setTimeout(() => {
        if (this.responseCallbacks.has(id)) {
          this.responseCallbacks.delete(id);
          reject(new Error(`Request ${id} timed out`));
        }
      }, 5000); // 5 second timeout
    });
  }
  
  shutdown() {
      this.proc.kill();
  }
}

// --- Usage Example ---
async function main() {
    const mcp = new YamsMCP();

    console.log("--> Initializing connection...");
    const initResponse = await mcp.call("initialize", {
        "clientInfo": {"name": "js-example-client", "version": "1.0"}
    });
    console.log("<-- Server initialized:", JSON.stringify(initResponse, null, 2));

    console.log("\n--> Listing tools...");
    const tools = await mcp.call("tools/list", {});
    console.log("<-- Tools available:", JSON.stringify(tools, null, 2));

    console.log("\n--> Calling 'search' tool...");
    const searchResult = await mcp.call("tools/call", {
        "name": "search",
        "arguments": {"query": "test", "limit": 5}
    });
    console.log("<-- Search result:", JSON.stringify(searchResult, null, 2));

    mcp.shutdown();
}

main().catch(console.error);
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



## Security Considerations

### Stdio Transport
- Runs with the permissions of the calling process
- No network exposure
- Ideal for local AI assistants



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


