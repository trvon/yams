# MCP Server Tools Reference

The YAMS MCP (Model Context Protocol) server provides tools for LLMs to interact with the content store programmatically.

## Overview

The MCP server can be started with:
```bash
# StdIO transport (recommended for Claude Desktop)
yams serve

```

## Available Tools

### search_documents

Search for documents using keyword search, fuzzy matching, or hash lookup with enhanced LLM ergonomics.

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
      "default": "hybrid"
    },
    "paths_only": {
      "type": "boolean",
      "description": "Output only file paths (LLM-friendly format)",
      "default": false
    },
    "line_numbers": {
      "type": "boolean",
      "description": "Show line numbers with matches",
      "default": false
    },
    "after_context": {
      "type": "integer",
      "description": "Show N lines after match",
      "default": 0
    },
    "before_context": {
      "type": "integer",
      "description": "Show N lines before match",
      "default": 0
    },
    "context": {
      "type": "integer",
      "description": "Show N lines before and after match",
      "default": 0
    },
    "color": {
      "type": "string",
      "description": "Color highlighting: always, never, auto",
      "default": "never"
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

### grep_documents

Search for regex patterns within document contents (similar to grep command).

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "pattern": {
      "type": "string",
      "description": "Regular expression pattern to search for"
    },
    "paths": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Files or directories to search (default: all indexed files)"
    },
    "after_context": {
      "type": "integer",
      "description": "Show N lines after match",
      "default": 0
    },
    "before_context": {
      "type": "integer",
      "description": "Show N lines before match",
      "default": 0
    },
    "context": {
      "type": "integer",
      "description": "Show N lines before and after match",
      "default": 0
    },
    "ignore_case": {
      "type": "boolean",
      "description": "Case-insensitive search",
      "default": false
    },
    "word": {
      "type": "boolean",
      "description": "Match whole words only",
      "default": false
    },
    "invert": {
      "type": "boolean",
      "description": "Invert match (show non-matching lines)",
      "default": false
    },
    "line_numbers": {
      "type": "boolean",
      "description": "Show line numbers",
      "default": false
    },
    "with_filename": {
      "type": "boolean",
      "description": "Show filename with matches",
      "default": false
    },
    "count": {
      "type": "boolean",
      "description": "Show only count of matching lines",
      "default": false
    },
    "files_with_matches": {
      "type": "boolean",
      "description": "Show only filenames with matches",
      "default": false
    },
    "files_without_match": {
      "type": "boolean",
      "description": "Show only filenames without matches",
      "default": false
    },
    "color": {
      "type": "string",
      "description": "Color mode: always, never, auto",
      "default": "never"
    },
    "max_count": {
      "type": "integer",
      "description": "Stop after N matches per file",
      "default": 0
    }
  },
  "required": ["pattern"]
}
```

**Example:**
```json
{
  "tool": "grep_documents",
  "arguments": {
    "pattern": "function.*async",
    "paths": ["src/"],
    "line_numbers": true,
    "context": 2
  }
}
```

### retrieve_document

Retrieve a document by its content hash with optional knowledge graph relationships.

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
    },
    "graph": {
      "type": "boolean",
      "description": "Include related documents in response",
      "default": false
    },
    "depth": {
      "type": "integer",
      "description": "Graph traversal depth (1-5)",
      "default": 2,
      "minimum": 1,
      "maximum": 5
    },
    "include_content": {
      "type": "boolean",
      "description": "Include full document content in response",
      "default": false
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

### update_metadata

Update metadata for an existing document.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "hash": {
      "type": "string",
      "description": "Hash of the document to update"
    },
    "name": {
      "type": "string",
      "description": "Name of the document to update"
    },
    "metadata": {
      "type": "array",
      "items": {"type": "string"},
      "description": "Metadata key=value pairs to update"
    },
    "verbose": {
      "type": "boolean",
      "description": "Enable verbose output",
      "default": false
    }
  },
  "oneOf": [
    {"required": ["hash", "metadata"]},
    {"required": ["name", "metadata"]}
  ]
}
```

**Example:**
```json
{
  "tool": "update_metadata",
  "arguments": {
    "hash": "abc123...",
    "metadata": ["status=completed", "priority=high", "author=John Doe"],
    "verbose": true
  }
}
```

### get_stats

Get storage statistics with optional file type breakdown.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "detailed": {
      "type": "boolean",
      "default": false
    },
    "file_types": {
      "type": "boolean",
      "description": "Include detailed file type analysis",
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

## Enhanced Tools

### list_documents
List all documents with comprehensive filtering and sorting capabilities.

**Enhanced Features:**
- File type filtering (`type`, `mime`, `extension`, `binary`, `text`)
- Time-based filtering (`created_after/before`, `modified_after/before`, `indexed_after/before`)
- Recent documents (`recent` parameter)
- Sorting options (`sort_by`: name, size, created, modified, indexed)
- Sort order control (`sort_order`: asc, desc)
- Full metadata and tag inclusion

**Example:**
```json
{
  "tool": "list_documents",
  "arguments": {
    "type": "text",
    "extension": ".py",
    "recent": 10,
    "sort_by": "modified",
    "sort_order": "desc"
  }
}
```

## Legacy Tools (Available)

### delete_by_name
Delete documents by name, multiple names, or glob patterns.

### get_by_name
Retrieve document content by name instead of hash.

### cat_document
Display document content directly (similar to CLI cat command).

## Planned Tools (Coming Soon)

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
      "env": {}
    }
  }
}
```

This allows Claude to directly interact with your YAMS storage for enhanced memory and context management.
