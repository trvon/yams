# MCP Server Tools Reference

The YAMS MCP server exposes **3 composite tools** — `query`, `execute`, and `session` — that consolidate all operations into a compressed tool surface (~800 tokens vs ~4,000 for individual tools). Agents discover operation schemas on demand via the built-in `describe` operation.

For design rationale and architecture, see [mcp-code-mode.md](../design/mcp-code-mode.md).

## Overview

```bash
# Start the MCP server (stdio transport)
yams serve
```

### Tool Surface

| Tool | Purpose | Operations |
|------|---------|------------|
| `query` | Read-only pipeline | `search`, `grep`, `list`, `list_collections`, `list_snapshots`, `graph`, `get`, `status`, `describe` |
| `execute` | Write batch | `add`, `update`, `delete`, `restore`, `download` |
| `session` | Session lifecycle | `start`, `stop`, `pin`, `unpin`, `watch` |

### Progressive Discovery

Use the `describe` operation to discover the full parameter schema for any op at runtime — no need to memorize schemas:

```json
{"steps": [{"op": "describe", "params": {"target": "search"}}]}
```

Omit `target` to list all available operations:

```json
{"steps": [{"op": "describe"}]}
```

## Prompt Templates

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

Get a prompt template by name. File-backed prompts return the entire Markdown content as a single assistant message.

**Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "prompts/get",
  "params": {"name": "research_mcp"}
}
```

**Response:**
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

---

## Tool 1: `query` — Read Pipeline

Read-only operations with multi-step pipeline support. Each step's result is available as `$prev` in subsequent steps.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "steps": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "op": {
            "type": "string",
            "enum": ["search", "grep", "list", "list_collections", "list_snapshots", "graph", "get", "status", "describe"]
          },
          "params": { "type": "object" }
        },
        "required": ["op"]
      }
    }
  },
  "required": ["steps"]
}
```

**Annotations:** `readOnlyHint: true`, `destructiveHint: false`, `idempotentHint: true`

**Single-step shortcut:** When `steps` has exactly one element, the response is the direct result (no wrapper array).

### `$prev` Reference Syntax

String values in `params` can reference the previous step's result:

| Pattern | Resolves To |
|---------|-------------|
| `$prev` | Entire previous result object |
| `$prev.field` | Top-level field |
| `$prev.field[N]` | Array element at index N |
| `$prev.field[N].subfield` | Nested field within array element |

If resolution fails (missing field, out-of-bounds index), the param receives `null`. `$prev` is `{}` for the first step.

### Query Operations Reference

#### search

Search for documents using keyword, semantic, or hybrid search with optional fuzzy matching.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | *(required)* | Search query text |
| `limit` | integer | 10 | Maximum results |
| `type` | string | `"hybrid"` | `keyword`, `semantic`, or `hybrid` |
| `fuzzy` | boolean | true | Enable fuzzy matching |
| `similarity` | number | 0.7 | Fuzzy match threshold (0.0–1.0) |
| `hash` | string | — | Search by content hash (full or partial, min 8 chars) |
| `paths_only` | boolean | false | Return only file paths |
| `line_numbers` | boolean | false | Show line numbers with matches |
| `context` | integer | 0 | Lines of context around matches |

**Example:**
```json
{
  "steps": [{"op": "search", "params": {"query": "database optimization", "limit": 5}}]
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
      "hash": "abcd1234...",
      "title": "Document Title",
      "path": "/path/to/document",
      "score": 0.95,
      "snippet": "...relevant content preview..."
    }
  ]
}
```

#### grep

Search for regex patterns within document contents.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `pattern` | string | *(required)* | Regular expression pattern |
| `paths` | array | all indexed | Files or directories to search |
| `ignore_case` | boolean | false | Case-insensitive search |
| `line_numbers` | boolean | false | Show line numbers |
| `context` | integer | 0 | Lines of context around matches |
| `word` | boolean | false | Match whole words only |
| `count` | boolean | false | Show only count of matching lines |
| `files_with_matches` | boolean | false | Show only filenames with matches |
| `max_count` | integer | 0 | Stop after N matches per file |

**Example:**
```json
{
  "steps": [{"op": "grep", "params": {"pattern": "class\\s+\\w+Handler", "line_numbers": true, "context": 2}}]
}
```

#### list

List documents with filtering and sorting.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | — | Maximum documents |
| `offset` | integer | 0 | Pagination offset |
| `sort_by` | string | — | `name`, `size`, `created`, `modified`, `indexed` |
| `sort_order` | string | — | `asc` or `desc` |
| `recent` | integer | — | Show N most recent documents |
| `type` | string | — | Filter: `text` or `binary` |
| `extension` | string | — | Filter by file extension |
| `paths_only` | boolean | false | Return only file paths |

**Example:**
```json
{
  "steps": [{"op": "list", "params": {"extension": ".py", "recent": 10, "sort_by": "modified", "sort_order": "desc"}}]
}
```

#### get

Retrieve a document by content hash with optional knowledge graph relationships.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hash` | string | *(required)* | Document content hash |
| `outputPath` | string | — | Write content to this path |
| `graph` | boolean | false | Include related documents |
| `depth` | integer | 2 | Graph traversal depth (1–5) |
| `include_content` | boolean | false | Include full content in response |

**Example:**
```json
{
  "steps": [{"op": "get", "params": {"hash": "e3b0c442...", "include_content": true}}]
}
```

#### graph

Query the knowledge graph.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hash` | string | — | Document hash |
| `name` | string | — | Document name/path |
| `node_key` | string | — | Direct KG node key |
| `list_types` | boolean | — | List available node types with counts |
| `list_type` | string | — | List nodes of a specific type |
| `relation` | string | — | Filter by relation type |
| `depth` | integer | — | Traversal depth (1–5) |
| `limit` | integer | 100 | Maximum results |
| `reverse` | boolean | — | Traverse incoming edges |

**Example:**
```json
{
  "steps": [{"op": "graph", "params": {"name": "src/main.cpp", "depth": 2, "relation": "CALLS", "limit": 50}}]
}
```

#### list_collections

List available collections. No required parameters.

```json
{"steps": [{"op": "list_collections"}]}
```

#### list_snapshots

List available snapshots. No required parameters.

```json
{"steps": [{"op": "list_snapshots"}]}
```

#### status

Get storage statistics and analytics.

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `detailed` | boolean | false | Include detailed breakdown |
| `file_types` | boolean | false | Include file type analysis |

**Example:**
```json
{"steps": [{"op": "status", "params": {"detailed": true}}]}
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

#### describe

Return the full JSON Schema for any operation's parameters. See [Progressive Discovery](#progressive-discovery).

### Pipeline Examples

**Search → Get (retrieve top result's full content):**
```json
{
  "steps": [
    {"op": "search", "params": {"query": "authentication middleware", "limit": 1}},
    {"op": "get", "params": {"hash": "$prev.results[0].hash", "include_content": true}}
  ]
}
```

**List → Grep (find pattern in recent documents):**
```json
{
  "steps": [
    {"op": "list", "params": {"limit": 50, "sort_by": "modified", "sort_order": "desc"}},
    {"op": "grep", "params": {"pattern": "TODO|FIXME"}}
  ]
}
```

**Graph traversal (find dependencies of a file):**
```json
{
  "steps": [
    {"op": "graph", "params": {"name": "src/main.cpp", "relation": "IMPORTS", "depth": 2}},
    {"op": "get", "params": {"hash": "$prev.nodes[0].hash"}}
  ]
}
```

---

## Tool 2: `execute` — Write Batch

Write operations executed sequentially. Stops on first error unless `continueOnError` is set.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "operations": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "op": {
            "type": "string",
            "enum": ["add", "update", "delete", "restore", "download"]
          },
          "params": { "type": "object" }
        },
        "required": ["op", "params"]
      }
    },
    "continueOnError": {
      "type": "boolean",
      "default": false
    }
  },
  "required": ["operations"]
}
```

**Annotations:** `readOnlyHint: false`, `destructiveHint: true`, `idempotentHint: false`

**Response:**
```json
{
  "results": [
    {"op": "add", "success": true, "hash": "sha256-abc123..."},
    {"op": "update", "success": true, "matched": 1}
  ],
  "totalOps": 2,
  "succeeded": 2,
  "failed": 0
}
```

### Execute Operations Reference

#### add

Store a document with optional metadata and tags.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | string | *(required)* | File path to store |
| `tags` | array | — | Document tags |
| `metadata` | object | — | Key-value metadata |

**Example:**
```json
{
  "operations": [
    {"op": "add", "params": {"path": "/tmp/notes.md", "tags": ["meeting", "2024-01"], "metadata": {"author": "Jane"}}}
  ]
}
```

#### update

Update metadata for an existing document (identified by `hash` or `name`).

**Key Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `hash` | string | Document hash |
| `name` | string | Document name/path |
| `metadata` | array | `key=value` pairs |

**Example:**
```json
{
  "operations": [
    {"op": "update", "params": {"name": "notes.md", "metadata": ["status=reviewed", "priority=high"]}}
  ]
}
```

#### delete

Delete documents by name or glob pattern.

**Key Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `name` | string | Document name or glob pattern |
| `names` | array | Multiple names/patterns |

**Example:**
```json
{
  "operations": [
    {"op": "delete", "params": {"name": "*.tmp"}}
  ]
}
```

#### restore

Restore documents from a collection or snapshot to the filesystem.

**Key Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `collection` | string | — | Collection name |
| `snapshot_id` | string | — | Snapshot ID |
| `output_directory` | string | *(required)* | Output directory |
| `dry_run` | boolean | false | Preview without writing |
| `overwrite` | boolean | false | Overwrite existing files |
| `include_patterns` | array | — | Only restore matching patterns |
| `exclude_patterns` | array | — | Exclude matching patterns |

**Example:**
```json
{
  "operations": [
    {"op": "restore", "params": {"collection": "docs", "output_directory": "/tmp/restore", "dry_run": true}}
  ]
}
```

#### download

Download and store an artifact by URL with optional checksum verification.

**Example:**
```json
{
  "operations": [
    {"op": "download", "params": {"url": "https://example.com/data.csv"}}
  ]
}
```

### Batch Example

**Add a file, then tag it:**
```json
{
  "operations": [
    {"op": "add", "params": {"path": "/tmp/design.md"}},
    {"op": "update", "params": {"name": "design.md", "metadata": ["status=draft", "team=platform"]}}
  ]
}
```

---

## Tool 3: `session` — Session Lifecycle

Manage sessions for scoped work.

**Input Schema:**
```json
{
  "type": "object",
  "properties": {
    "action": {
      "type": "string",
      "enum": ["start", "stop", "pin", "unpin", "watch"]
    },
    "params": { "type": "object" }
  },
  "required": ["action"]
}
```

**Annotations:** `readOnlyHint: false`, `destructiveHint: false`, `idempotentHint: false`

### Session Actions

| Action | Description |
|--------|-------------|
| `start` | Begin a new session |
| `stop` | End the current session |
| `pin` | Pin artifacts to the current session |
| `unpin` | Unpin artifacts from the current session |
| `watch` | Watch paths for changes during the session |

**Example — start a session:**
```json
{"action": "start", "params": {"label": "feature-review"}}
```

**Example — pin an artifact:**
```json
{"action": "pin", "params": {"hash": "abc123..."}}
```

---

## Utility Tool: `mcp.echo`

Always registered for protocol testing. Echoes back the input message.

```json
{"tool": "mcp.echo", "arguments": {"message": "hello"}}
```
