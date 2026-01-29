# MCP Apps Extension Support - Implementation Plan

## Overview

MCP Apps (SEP-1865) is a new official MCP extension that enables servers to deliver interactive UI components. This document outlines what YAMS needs to implement to support this extension.

**Status**: Stable (2026-01-26)  
**Extension ID**: `io.modelcontextprotocol/ui`  
**MIME Type**: `text/html;profile=mcp-app`

## Current State

YAMS currently supports:
- ✅ Standard MCP protocol versions (2024-10-07 through 2025-11-25)
- ✅ Tools, Resources, Prompts
- ✅ Basic capability negotiation

YAMS does NOT currently support:
- ❌ MCP Apps extension
- ❌ UI resources (`ui://` scheme)
- ❌ Tool-UI metadata linkage
- ❌ Extension capability negotiation

## Required Changes

### 1. Extension Capability Negotiation

**File**: `src/mcp/mcp_server.cpp` (initialize method)

Add support for detecting and advertising the MCP Apps extension:

```cpp
// Check if client supports MCP Apps extension
if (params.contains("capabilities") && 
    params["capabilities"].contains("extensions") &&
    params["capabilities"]["extensions"].contains("io.modelcontextprotocol/ui")) {
    
    auto uiCap = params["capabilities"]["extensions"]["io.modelcontextprotocol/ui"];
    if (uiCap.contains("mimeTypes")) {
        // Check if client supports text/html;profile=mcp-app
        for (const auto& mimeType : uiCap["mimeTypes"]) {
            if (mimeType == "text/html;profile=mcp-app") {
                clientSupportsMcpApps_ = true;
                break;
            }
        }
    }
}
```

### 2. UI Resource Support

**File**: `src/mcp/mcp_server.cpp` (new methods needed)

Add support for `ui://` resources:

```cpp
// New method to list UI resources
json listUIResources();

// New method to read UI resources  
json readUIResource(const std::string& uri);

// UI resource storage
std::unordered_map<std::string, UIResource> uiResources_;
```

### 3. Tool-UI Linkage

**File**: `src/mcp/mcp_server.cpp` (initializeToolRegistry)

Modify tool registration to include UI metadata:

```cpp
// When registering tools, add _meta.ui field if UI is available
if (clientSupportsMcpApps_) {
    tool["_meta"] = {
        {"ui", {
            {"resourceUri", "ui://yams/dashboard"},
            {"visibility", json::array({"model", "app"})}
        }}
    };
}
```

### 4. UI Resource Data Structure

**File**: `include/yams/mcp/mcp_server.h`

Add UI resource structure:

```cpp
struct UIResource {
    std::string uri;           // ui://...
    std::string name;
    std::string description;
    std::string mimeType;      // text/html;profile=mcp-app
    std::string htmlContent;   // HTML content
    
    struct {
        struct {
            std::vector<std::string> connectDomains;
            std::vector<std::string> resourceDomains;
            std::vector<std::string> frameDomains;
        } csp;
        struct {
            bool camera = false;
            bool microphone = false;
            bool geolocation = false;
            bool clipboardWrite = false;
        } permissions;
        std::optional<std::string> domain;
        std::optional<bool> prefersBorder;
    } meta;
};
```

### 5. New Request Handlers

**File**: `src/mcp/mcp_server.cpp` (handleRequest)

Add handlers for:
- `resources/read` for `ui://` URIs
- UI-specific lifecycle management

### 6. Example UI Resources

YAMS could provide these UI resources:

1. **Search Results Dashboard** (`ui://yams/search`)
   - Interactive search results with filtering
   - Document preview cards
   - Graph visualization

2. **Document Viewer** (`ui://yams/document`)
   - Syntax-highlighted code viewer
   - Metadata sidebar
   - Related documents

3. **Knowledge Graph Explorer** (`ui://yams/graph`)
   - Interactive graph visualization
   - Node/edge inspection
   - Path finding

4. **Stats Dashboard** (`ui://yams/stats`)
   - Storage usage charts
   - Index statistics
   - Recent activity

## Implementation Priority

### Phase 1: Basic Support (MVP)
1. Extension capability negotiation
2. Basic UI resource listing
3. Simple HTML resource serving
4. Tool-UI linkage for 1-2 tools

### Phase 2: Rich UI
1. Multiple UI resources
2. Interactive components
3. Real-time updates
4. Theming support

### Phase 3: Advanced Features
1. Bidirectional communication
2. Custom tool calls from UI
3. State persistence
4. Complex visualizations

## Testing Requirements

Add tests to verify:
1. Extension capability negotiation
2. UI resource discovery
3. HTML content serving
4. Tool-UI metadata linkage
5. CSP and security headers
6. Fallback behavior for non-supporting clients

## Security Considerations

1. **CSP Enforcement**: Hosts will enforce CSP based on declared domains
2. **Sandboxing**: All UI runs in sandboxed iframes
3. **Audit Trail**: All UI-to-host communication is loggable
4. **User Consent**: Hosts may require approval for UI-initiated actions

## Backward Compatibility

MCP Apps is designed to be backward compatible:
- Clients that don't support the extension get standard text responses
- Tools still work without UI (graceful degradation)
- Extension is opt-in via capability negotiation

## Example Usage

Once implemented, users could configure Claude Desktop:

```json
{
  "mcpServers": {
    "yams": {
      "command": "yams",
      "args": ["serve"],
      "env": {
        "YAMS_ENABLE_UI": "1"
      }
    }
  }
}
```

Then when using YAMS tools in Claude, users would see interactive dashboards instead of just text output.

## References

- [MCP Apps Blog Post](https://blog.modelcontextprotocol.io/posts/2026-01-26-mcp-apps/)
- [ext-apps Repository](https://github.com/modelcontextprotocol/ext-apps)
- [Specification](https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx)
- [SEP-1865](https://github.com/modelcontextprotocol/specification/discussions/1865)
