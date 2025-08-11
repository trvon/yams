# YAMS Feature Parity Matrix

This document shows the feature coverage across different interfaces: HTTP API, CLI, and MCP Server.

## Core Document Operations

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Store single document | ✅ POST /api/v1/content | ✅ yams add | ✅ store_document | |
| Retrieve document | ✅ GET /api/v1/content/{hash} | ✅ yams get | ✅ retrieve_document | |
| Delete document | ✅ DELETE /api/v1/content/{hash} | ✅ yams delete | ✅ delete_document | |
| Check existence | ✅ HEAD /api/v1/content/{hash} | ❌ | ❌ | Available internally; no dedicated CLI command |
| List documents | ❌ | ✅ yams list | ❌ | HTTP API needs listing endpoint |
| Batch store | ✅ POST /api/v1/content/batch | ❌ | ❌ | MCP needs batch support |
| Batch delete | ✅ DELETE /api/v1/content/batch | ❌ | ❌ | MCP needs batch support |

## Metadata Operations

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Get metadata | ✅ GET /api/v1/content/{hash}/metadata | ❌ | ❌ | No dedicated CLI command |
| Update metadata | ✅ PUT /api/v1/content/{hash}/metadata | ❌ | ✅ update_metadata | |
| Add tags | ❌ | ❌ | ❌ | HTTP endpoint needed; no CLI commands |
| Remove tags | ❌ | ❌ | ❌ | HTTP endpoint needed; no CLI commands |

## Search Operations

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Text search | ✅ GET /api/v1/content/search | ✅ yams search | ✅ search_documents | |
| Similar documents | ❌ | ❌ | ❌ | HTTP/MCP endpoints and CLI command needed |

## Memory System (Mem0-inspired)

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| List memories | ❌ | ❌ | ❌ | Not exposed via HTTP; no CLI command |
| Search memories | ❌ | ❌ | ✅ retrieve_memories | Not exposed via HTTP |
| Record memory | ❌ | ❌ | ✅ record_memory | CLI command needed |
| Consolidate memories | ❌ | ❌ | ❌ | MCP tool needed |

## System Operations

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Get statistics | ✅ GET /api/v1/content/stats | ✅ yams stats | ✅ get_stats | |
| Health check | ✅ GET /api/v1/health | ❌ | ❌ | CLI command could be useful |
| Garbage collection | ❌ | ❌ | ❌ | Admin operation (future) |
| Verify integrity | ❌ | ❌ | ❌ | Admin operation (future) |
| Export data | ❌ | ❌ | ❌ | Admin operation (future) |
| Import data | ❌ | ❌ | ❌ | Admin operation (future) |

## Configuration & Setup

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Initialize config | ❌ | ✅ yams init | ❌ | Setup operation |
| Get config | ❌ | ✅ yams config get | ❌ | Admin operation |
| Set config | ❌ | ✅ yams config set | ❌ | Admin operation |
| Validate config | ❌ | ✅ yams config validate | ❌ | Admin operation |
| Export config | ❌ | ✅ yams config export | ❌ | Admin operation |

## Authentication & Security

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| JWT authentication | ✅ Via headers | ❌ | ❌ | HTTP uses JWT tokens |
| API key auth | ✅ Via headers | ❌ | ❌ | HTTP uses API keys |
| Generate keys | ❌ | ❌ | ❌ | Admin operation (future) |
| List keys | ❌ | ❌ | ❌ | Admin operation (future) |
| Revoke keys | ❌ | ❌ | ❌ | Admin operation (future) |

## Service Management

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Install service | ❌ | ❌ | ❌ | OS-level operation (future) |
| Start service | ❌ | ❌ | ❌ | OS-level operation (future) |
| Stop service | ❌ | ❌ | ❌ | OS-level operation (future) |
| Service status | ❌ | ❌ | ❌ | OS-level operation (future) |
| View logs | ❌ | ❌ | ❌ | OS-level operation (future) |

## IPFS Integration

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Add to IPFS | ❌ | ❌ | ❌ | Could add HTTP endpoint (future) |
| Get from IPFS | ❌ | ❌ | ❌ | Could add HTTP endpoint (future) |
| Pin content | ❌ | ❌ | ❌ | Could add HTTP endpoint (future) |
| Unpin content | ❌ | ❌ | ❌ | Could add HTTP endpoint (future) |
| IPFS GC | ❌ | ❌ | ❌ | Admin operation (future) |

## Server Operations

| Feature | HTTP API | CLI | MCP Tool | Notes |
|---------|----------|-----|----------|-------|
| Start HTTP server | ✅ Built-in | ❌ | ❌ | HTTP server is the interface |
| Start MCP server | ❌ | ✅ yams serve | ✅ Built-in | MCP server is the interface |

## Planned/WIP

The following items are not part of the current CLI surface and/or HTTP API, and are planned or under consideration for future releases:
- CLI commands:
  - exists
  - batch-add, batch-delete
  - metadata-get, metadata-update
  - tag, untag
  - similar
  - memory list/search/record/consolidate
  - gc, verify, export, import
  - service install/start/stop/status/logs
  - ipfs add/get/pin/unpin/gc
- HTTP API coverage:
  - Listing endpoint for documents
  - Similar documents endpoint
  - Tag operations endpoints
  - IPFS endpoints

## Summary

### Coverage by Interface:
- **HTTP API**: 14/45 features (31.1%) - Core content APIs and health
- **CLI**: 12/45 features (26.7%) - Primary developer interface; many advanced ops are Planned/WIP
- **MCP Tools**: 9/45 features (20.0%) - Focused on AI/agent operations

### Recommendations for Feature Parity:

1. **High Priority**:
   - Add document listing to HTTP API
   - Add tag operations to HTTP API
   - Add batch operations to MCP tools
   - Add CLI command for recording memories

2. **Medium Priority**:
   - Add similar document search to HTTP API and MCP
   - Add metadata retrieval to MCP tools
   - Add health check to CLI
   - Expose memory operations via HTTP API (with appropriate auth)

3. **Low Priority**:
   - Add IPFS operations to HTTP API
   - Add more MCP tools for admin operations
   - Add consolidate memories to MCP tools

### Design Rationale:
- **CLI** has the most features as it's the primary interface for developers and power users
- **HTTP API** focuses on core operations suitable for web integration
- **MCP Tools** focus on operations relevant to AI agents, avoiding admin/system operations
- Some operations (like service management) are intentionally CLI-only for security reasons