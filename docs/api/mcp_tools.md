# MCP Server Tools Reference

The YAMS MCP (Model Context Protocol) server provides tools for LLMs to interact with the content store programmatically.

## Overview

The MCP server can be started with:
```bash
# StdIO transport (recommended for Claude Desktop)
yams serve

# WebSocket transport
yams serve --transport websocket --host 127.0.0.1 --port 8080
```

## Available Tools

### search_documents

Search for documents using keyword search, fuzzy matching, or hash lookup with full CLI parity.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "query": {
      "type": "string",
      "description": "Search query"
    },
    "limit": {
      "type": "integer",
      "description": "Maximum results",
      "default": 10
    },
    "fuzzy": {
      "type": "boolean",
      "description": "Enable fuzzy search for approximate matching",
      "default": false
    },
    "similarity": {
      "type": "number",
      "description": "Minimum similarity for fuzzy search (0.0-1.0)",
      "default": 0.7
    },
    "hash": {
      "type": "string",
      "description": "Search by file hash (full or partial, minimum 8 characters)"
    },
    "type": {
      "type": "string",
      "description": "Search type: keyword, semantic, hybrid",
      "default": "keyword"
    }
  },
  "required": ["query"]
}
```

**Response:**
```json
{
  "total": 42,
  "type": "full-text",
  "execution_time_ms": 25,
  "results": [
    {
      "id": 123,
      "hash": "abcd1234ef567890abcd1234ef567890abcd1234ef567890abcd1234ef567890",
      "title": "Document Title",
      "path": "/path/to/document",
      "score": 0.95,
      "snippet": "...relevant content preview..."
    }
  ]
}
```

**Examples:**

Text search:
```json
{
  "tool": "search_documents",
  "arguments": {
    "query": "database optimization",
    "limit": 5
  }
}
```

Fuzzy search:
```json
{
  "tool": "search_documents",
  "arguments": {
    "query": "databse optimizaton",
    "fuzzy": true,
    "similarity": 0.6
  }
}
```

Hash search:
```json
{
  "tool": "search_documents",
  "arguments": {
    "query": "placeholder",
    "hash": "abcd1234ef567890"
  }
}
```

Auto-detected hash search:
```json
{
  "tool": "search_documents",
  "arguments": {
    "query": "abcd1234ef567890"
  }
}
```

### store_document

Store a document with optional metadata.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "path": {
      "type": "string",
      "description": "File path"
    },
    "tags": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Document tags"
    },
    "metadata": {
      "type": "object",
      "description": "Additional metadata"
    }
  },
  "required": ["path"]
}
```

**Response:**
```json
{
  "hash": "sha256_hash_of_content",
  "bytes_stored": 1024,
  "bytes_deduped": 512
}
```

**Example:**
```json
{
  "tool": "store_document",
  "arguments": {
    "path": "/tmp/meeting-notes.txt",
    "tags": ["meeting", "project-x", "2024-01"],
    "metadata": {
      "author": "John Doe",
      "department": "Engineering"
    }
  }
}
```

### retrieve_document

Retrieve a document by its content hash.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "hash": {
      "type": "string",
      "description": "Document hash"
    },
    "outputPath": {
      "type": "string",
      "description": "Output file path (optional)"
    }
  },
  "required": ["hash"]
}
```

**Response:**
```json
{
  "found": true,
  "size": 2048,
  "path": "/tmp/retrieved-document.txt"
}
```

**Example:**
```json
{
  "tool": "retrieve_document",
  "arguments": {
    "hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "outputPath": "/tmp/output.txt"
  }
}
```

### get_stats

Get storage statistics.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "detailed": {
      "type": "boolean",
      "default": false
    }
  }
}
```

**Response:**
```json
{
  "total_objects": 1000,
  "total_bytes": 10485760,
  "unique_blocks": 5000,
  "deduplicated_bytes": 2097152,
  "dedup_ratio": 0.2
}
```

**Example:**
```json
{
  "tool": "get_stats",
  "arguments": {
    "detailed": true
  }
}
```

## Planned Tools (Coming Soon)

### list_documents
List all documents with filtering and pagination.

### delete_document
Delete documents by hash or name.

### get_document_by_name
Retrieve documents by name instead of hash.

### store_text
Store text content directly without creating files.

### search_by_tag
Search specifically within tagged documents.

### bulk_operations
Perform multiple operations in a single request.

## Error Handling

All tools return errors in a consistent format:

```json
{
  "error": "Description of what went wrong"
}
```

Common error codes:
- Document not found
- Invalid arguments
- Storage not initialized
- Permission denied
- Internal server error

## Best Practices

1. **Use search before retrieve**: Search to find relevant documents, then retrieve specific ones
2. **Tag consistently**: Use a standard tagging scheme for easier retrieval
3. **Batch operations**: Group related operations when possible
4. **Handle errors gracefully**: Always check for error responses
5. **Use appropriate limits**: Don't request more results than needed

## Integration with Claude Desktop

Add to your Claude Desktop configuration:

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"],
      "env": {
        "YAMS_STORAGE": "/path/to/your/storage"
      }
    }
  }
}
```

This allows Claude to directly interact with your YAMS storage for enhanced memory and context management.