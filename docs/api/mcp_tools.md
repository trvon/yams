# MCP Server Tools Reference

The YAMS MCP (Model Context Protocol) server provides tools for LLMs to interact with the content store programmatically.

## Overview

The MCP server can be started with:
```bash
# StdIO transport (recommended for Claude Desktop)
yams serve

```

## Available Tools

### prompts/list

List available prompt templates (built-ins and file-backed). File-backed templates are Markdown files named `PROMPT-*.md` in the configured prompts directory (see user guide). Names are derived by stripping `PROMPT-` and extension and converting dashes to underscores.

**Response:**
```json
{
  "prompts": [
    {"name": "search_codebase", "description": "Search for code patterns in the codebase", "arguments": [...]},
    {"name": "research_mcp", "description": "Research workflow for MCP (from PROMPT-research-mcp.md)"}
  ]
}
```

### prompts/get

Get a prompt template by name.

When the prompt is file-backed, the entire Markdown content is returned as a single assistant message content.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "prompts/get",
  "params": {"name": "research_mcp"}
}
```

**Response (example):**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "messages": [
      {"role": "assistant", "content": {"type": "text", "text": "...markdown content..."}}
    ]
  }
}
```

### search

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
      "default": true
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

Defaults: When options are omitted, the server runs hybrid search with fuzzy enabled (similarity 0.7).

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
  "tool": "search",
  "arguments": {
    "query": "database optimization",
    "limit": 5
  }
}
```

Fuzzy search:
```json
{
  "tool": "search",
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
  "tool": "search",
  "arguments": {
    "query": "placeholder",
    "hash": "abcd1234ef567890"
  }
}
```

Auto-detected hash search:
```json
{
  "tool": "search",
  "arguments": {
    "query": "abcd1234ef567890"
  }
}
```

### add

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
  "tool": "add",
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

### grep

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
    },
    "fast_first": {
      "type": "boolean",
      "description": "Return a quick semantic suggestions burst first",
      "default": false
    }
  },
  "required": ["pattern"]
}
```

**Example:**
```json
{
  "tool": "grep",
  "arguments": {
    "pattern": "function.*async",
    "paths": ["src/"],
    "line_numbers": true,
    "context": 2
  }
}
```

### get

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
  "tool": "get",
  "arguments": {
    "hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "outputPath": "/tmp/output.txt"
  }
}
```

### graph

Query the knowledge graph (matches `yams graph`).

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "hash": { "type": "string", "description": "Document hash" },
    "name": { "type": "string", "description": "Document name/path" },
    "node_key": { "type": "string", "description": "Direct KG node key" },
    "node_id": { "type": "integer", "description": "Direct KG node id" },
    "list_types": { "type": "boolean", "description": "List available node types" },
    "list_type": { "type": "string", "description": "List nodes by type" },
    "isolated": { "type": "boolean", "description": "List isolated nodes" },
    "relation": { "type": "string", "description": "Single relation filter" },
    "relation_filters": {
      "type": "array",
      "items": { "type": "string" },
      "description": "Relation filters"
    },
    "depth": { "type": "integer", "description": "Traversal depth (1-5)" },
    "limit": { "type": "integer", "description": "Maximum results", "default": 100 },
    "offset": { "type": "integer", "description": "Pagination offset", "default": 0 },
    "reverse": { "type": "boolean", "description": "Traverse incoming edges" },
    "include_node_properties": {
      "type": "boolean",
      "description": "Include node properties JSON"
    },
    "include_edge_properties": {
      "type": "boolean",
      "description": "Include edge properties JSON"
    },
    "hydrate_fully": {
      "type": "boolean",
      "description": "Hydrate metadata for nodes"
    },
    "scope_snapshot": {
      "type": "string",
      "description": "Restrict traversal to a snapshot id"
    }
  }
}
```

**Example:**
```json
{
  "tool": "graph",
  "arguments": {
    "name": "src/cli/commands/graph_command.cpp",
    "depth": 2,
    "relation": "CALLS",
    "limit": 50
  }
}
```

### update

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
  "tool": "update",
  "arguments": {
    "hash": "abc123...",
    "metadata": ["status=completed", "priority=high", "author=John Doe"],
    "verbose": true
  }
}
```

### status

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
  "tool": "status",
  "arguments": {
    "detailed": true
  }
}
```

## Enhanced Tools

### list
List all documents with comprehensive filtering and sorting capabilities.

**Enhanced Features:**
- File type filtering (`type`, `mime`, `extension`, `binary`, `text`)
- Time-based filtering (`created_after/before`, `modified_after/before`, `indexed_after/before`)
- Recent documents (`recent` parameter)
- Sorting options (`sort_by`: name, size, created, modified, indexed)
- Sort order control (`sort_order`: asc, desc)
- Full metadata and tag inclusion
- Paths-only output (`paths_only`) to return only file paths

**Example:**
```json
{
  "tool": "list",
  "arguments": {
    "type": "text",
    "extension": ".py",
    "recent": 10,
    "sort_by": "modified",
    "sort_order": "desc"
  }
}
```

## Additional Tools

### download
Download and store an artifact by URL with checksum support. Name derives from URL basename; returns hash and stored path.

### restore
Restore documents from a collection or snapshot.

**Parameters:**
- `collection`: Collection name (optional)
- `snapshot_id`: Snapshot ID (optional)
- `snapshot_label`: Snapshot label (optional)
- `output_directory`: Output directory (required)
- `layout_template`: Layout template (default: `{path}`)
- `include_patterns`: Only restore matching patterns
- `exclude_patterns`: Exclude matching patterns
- `overwrite`: Overwrite existing files (default: false)
- `create_dirs`: Create parent directories (default: true)
- `dry_run`: Preview without writing (default: false)

**Example:**
```json
{
  "tool": "restore",
  "arguments": {
    "collection": "docs",
    "output_directory": "/tmp/restore",
    "dry_run": true
  }
}
```

### list_collections
List available collections.

### list_snapshots
List available snapshots.

## Other Tools

### delete_by_name
Delete documents by name, multiple names, or glob patterns.
