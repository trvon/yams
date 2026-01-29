# MCP Apps Extension Test Specification
## PBI-083: MCP Apps Support - Phase 1

**Status**: In Progress  
**Owner**: codex  
**Created**: 2026-01-29  
**Spec Version**: SEP-1865 (Stable 2026-01-26)

---

## Overview

This document provides comprehensive test specifications for implementing MCP Apps extension support in YAMS. The tests are organized to match the specification sections and provide full coverage of the 2026-01-26 stable spec.

**Goal**: Validate that YAMS correctly implements all MCP Apps behaviors as both an MCP server (tool + resource provider).

---

## Test Categories

### 1. Capability Negotiation Tests

#### 1.1 Extension Discovery
**Test ID**: `mcp-apps-cap-01`
**Description**: Verify client can discover server's MCP Apps support
**Preconditions**: Server initialized with MCP Apps enabled
**Steps**:
1. Client sends `initialize` with `extensions["io.modelcontextprotocol/ui"]` containing `mimeTypes: ["text/html;profile=mcp-app"]`
2. Server responds with capabilities
**Expected Result**:
- Server includes `extensions["io.modelcontextprotocol/ui"]` in response
- Mime type `text/html;profile=mcp-app` is present

#### 1.2 Extension Not Advertised When Disabled
**Test ID**: `mcp-apps-cap-02`
**Description**: Verify server doesn't advertise extension when client doesn't support it
**Preconditions**: Server initialized, client doesn't send UI extension capability
**Steps**:
1. Client sends `initialize` without UI extension
2. Server responds with capabilities
**Expected Result**:
- Server response does NOT include `extensions["io.modelcontextprotocol/ui"]`

#### 1.3 Mime Type Validation - Supported
**Test ID**: `mcp-apps-cap-03`
**Description**: Server accepts supported mime type
**Preconditions**: Server supports `text/html;profile=mcp-app`
**Steps**:
1. Client sends supported mime type
2. Server processes initialization
**Expected Result**:
- Server accepts and stores capability
- UI resources become available

#### 1.4 Mime Type Validation - Unsupported
**Test ID**: `mcp-apps-cap-04`
**Description**: Server handles unsupported mime types gracefully
**Preconditions**: Server only supports `text/html;profile=mcp-app`
**Steps**:
1. Client sends unsupported mime type (e.g., `application/vnd.custom`)
2. Server processes initialization
**Expected Result**:
- Server does NOT enable MCP Apps
- Server falls back to standard MCP behavior
- No error response (graceful degradation)

#### 1.5 Graceful Fallback
**Test ID**: `mcp-apps-cap-05`
**Description**: Server provides text-only fallback when UI not supported
**Preconditions**: Tool has UI metadata, client doesn't support UI
**Steps**:
1. Initialize without UI support
2. Call tool that has UI resource
**Expected Result**:
- Tool executes successfully
- Returns standard text content (no UI metadata)
- No UI resource references in response

---

### 2. UI Resource Handling Tests

#### 2.1 Resource Declaration - With UI Support
**Test ID**: `mcp-apps-res-01`
**Description**: UI resources appear in resources/list when supported
**Preconditions**: Client supports MCP Apps, server has UI resources registered
**Steps**:
1. Initialize with UI support
2. Call `resources/list`
**Expected Result**:
- Response includes `ui://` resources
- Each resource has: `uri`, `name`, `description`, `mimeType`
- MimeType is `text/html;profile=mcp-app`

#### 2.2 Resource Declaration - Without UI Support
**Test ID**: `mcp-apps-res-02`
**Description**: UI resources hidden when client doesn't support them
**Preconditions**: Client doesn't support MCP Apps
**Steps**:
1. Initialize without UI support
2. Call `resources/list`
**Expected Result**:
- Response does NOT include `ui://` resources
- Only standard resources listed

#### 2.3 Resource Content Retrieval - Text
**Test ID**: `mcp-apps-res-03`
**Description**: Server returns HTML content as text
**Preconditions**: UI resource registered with text content
**Steps**:
1. Initialize with UI support
2. Call `resources/read` with `uri: "ui://example/resource"`
**Expected Result**:
- Response contains `contents` array
- Content has `mimeType: "text/html;profile=mcp-app"`
- Content has `text` field with HTML string
- Content has matching `uri`

#### 2.4 Resource Content Retrieval - Blob
**Test ID**: `mcp-apps-res-04`
**Description**: Server returns HTML content as base64 blob
**Preconditions**: UI resource registered with blob content
**Steps**:
1. Initialize with UI support
2. Call `resources/read` with blob-based resource
**Expected Result**:
- Response contains `contents` array
- Content has `mimeType: "text/html;profile=mcp-app"`
- Content has `blob` field with base64-encoded HTML

#### 2.5 Resource Metadata - CSP
**Test ID**: `mcp-apps-res-05`
**Description**: Server includes CSP metadata in resource
**Preconditions**: UI resource has CSP configuration
**Steps**:
1. Initialize with UI support
2. Call `resources/read`
**Expected Result**:
- Response includes `_meta.ui.csp`
- CSP contains: `connectDomains`, `resourceDomains`, `frameDomains`, `baseUriDomains`
- All fields are string arrays

#### 2.6 Resource Metadata - Permissions
**Test ID**: `mcp-apps-res-06`
**Description**: Server includes permission metadata
**Preconditions**: UI resource requests permissions
**Steps**:
1. Initialize with UI support
2. Call `resources/read`
**Expected Result**:
- Response includes `_meta.ui.permissions`
- Permissions object contains: `camera`, `microphone`, `geolocation`, `clipboardWrite`
- Each permission is an object (empty or with config)

#### 2.7 Resource Metadata - Domain
**Test ID**: `mcp-apps-res-07`
**Description**: Server includes dedicated domain if specified
**Preconditions**: UI resource has domain configuration
**Steps**:
1. Initialize with UI support
2. Call `resources/read`
**Expected Result**:
- Response includes `_meta.ui.domain` as string
- Domain follows host-specific format

#### 2.8 Resource Metadata - Border Preference
**Test ID**: `mcp-apps-res-08`
**Description**: Server includes border preference
**Preconditions**: UI resource has border preference
**Steps**:
1. Initialize with UI support
2. Call `resources/read`
**Expected Result**:
- Response includes `_meta.ui.prefersBorder` as boolean

#### 2.9 Resource Not Found
**Test ID**: `mcp-apps-res-09`
**Description**: Server returns error for non-existent UI resource
**Preconditions**: Resource URI doesn't exist
**Steps**:
1. Initialize with UI support
2. Call `resources/read` with invalid URI
**Expected Result**:
- Response contains error
- Error code: -32002 (ResourceNotFound) or similar
- Error message indicates resource not found

---

### 3. Tool Metadata & Visibility Tests

#### 3.1 Tool-UI Linkage - With Support
**Test ID**: `mcp-apps-tool-01`
**Description**: Tool includes UI metadata when client supports it
**Preconditions**: Tool has UI resource, client supports MCP Apps
**Steps**:
1. Initialize with UI support
2. Call `tools/list`
**Expected Result**:
- Tool includes `_meta.ui.resourceUri`
- Resource URI starts with `ui://`
- Optional: `_meta.ui.visibility` present

#### 3.2 Tool-UI Linkage - Without Support
**Test ID**: `mcp-apps-tool-02`
**Description**: Tool omits UI metadata when client doesn't support it
**Preconditions**: Tool has UI resource, client doesn't support MCP Apps
**Steps**:
1. Initialize without UI support
2. Call `tools/list`
**Expected Result**:
- Tool does NOT include `_meta.ui` field
- Tool appears as standard text-only tool

#### 3.3 Visibility - Model and App
**Test ID**: `mcp-apps-tool-03`
**Description**: Tool visible to both model and app
**Preconditions**: Tool has `visibility: ["model", "app"]`
**Steps**:
1. Initialize with UI support
2. Call `tools/list`
3. Attempt to call tool from app context
**Expected Result**:
- Tool appears in `tools/list`
- Tool can be called from app context

#### 3.4 Visibility - App Only
**Test ID**: `mcp-apps-tool-04`
**Description**: App-only tool hidden from model
**Preconditions**: Tool has `visibility: ["app"]`
**Steps**:
1. Initialize with UI support
2. Call `tools/list`
**Expected Result**:
- Tool does NOT appear in `tools/list` (model's view)
- Tool can still be called from app context

#### 3.5 Visibility - Model Only
**Test ID**: `mcp-apps-tool-05`
**Description**: Model-only tool not callable from app
**Preconditions**: Tool has `visibility: ["model"]`
**Steps**:
1. Initialize with UI support
2. Call `tools/list`
3. Attempt to call tool from app context
**Expected Result**:
- Tool appears in `tools/list`
- App call is rejected with appropriate error

#### 3.6 Visibility - Default
**Test ID**: `mcp-apps-tool-06`
**Description**: Default visibility is model and app
**Preconditions**: Tool has UI but no explicit visibility
**Steps**:
1. Initialize with UI support
2. Check tool metadata
**Expected Result**:
- Tool behaves as if `visibility: ["model", "app"]`

#### 3.7 Invalid Visibility
**Test ID**: `mcp-apps-tool-07`
**Description**: Server handles invalid visibility values
**Preconditions**: Tool has invalid visibility value
**Steps**:
1. Initialize with UI support
2. Call `tools/list`
**Expected Result**:
- Server filters out invalid values
- Or defaults to safe visibility
- No crash or undefined behavior

---

### 4. Lifecycle Notification Tests

#### 4.1 Tool Input Notification
**Test ID**: `mcp-apps-life-01`
**Description**: Server sends tool-input notification
**Preconditions**: Tool with UI called, UI initialized
**Steps**:
1. Initialize with UI support
2. Call tool with UI
3. UI sends `ui/initialize`
4. Server responds with `McpUiInitializeResult`
5. UI sends `ui/notifications/initialized`
**Expected Result**:
- Server sends `ui/notifications/tool-input` with complete arguments
- Arguments match original tool call params

#### 4.2 Tool Input Partial Notification
**Test ID**: `mcp-apps-life-02`
**Description**: Server may send partial tool input during streaming
**Preconditions**: Tool arguments being streamed
**Steps**:
1. Start tool call with streaming
2. Observe notifications before completion
**Expected Result**:
- Server MAY send `ui/notifications/tool-input-partial`
- Partial arguments have unclosed structures auto-closed
- Final `ui/notifications/tool-input` sent with complete args

#### 4.3 Tool Result Notification
**Test ID**: `mcp-apps-life-03`
**Description**: Server sends tool-result notification
**Preconditions**: Tool execution complete
**Steps**:
1. Call tool with UI
2. Wait for tool execution
**Expected Result**:
- Server sends `ui/notifications/tool-result`
- Result follows `CallToolResult` schema
- Includes `content` array
- May include `structuredContent`

#### 4.4 Tool Cancelled Notification
**Test ID**: `mcp-apps-life-04`
**Description**: Server sends cancellation notification
**Preconditions**: Tool execution in progress
**Steps**:
1. Call tool with UI
2. Send `notifications/cancelled` before completion
**Expected Result**:
- Server sends `ui/notifications/tool-cancelled`
- Includes optional `reason` field
- Tool execution stops

#### 4.5 Resource Teardown Request
**Test ID**: `mcp-apps-life-05`
**Description**: Server handles teardown request
**Preconditions**: UI resource active
**Steps**:
1. Host sends `ui/resource-teardown`
2. Server processes request
**Expected Result**:
- Server responds with empty result on success
- Or error if teardown fails
- Server waits for response before cleanup

#### 4.6 Size Changed Notification
**Test ID**: `mcp-apps-life-06`
**Description**: Server handles size-changed notification
**Preconditions**: UI using flexible dimensions
**Steps**:
1. UI sends `ui/notifications/size-changed`
2. Server records new dimensions
**Expected Result**:
- Server accepts notification
- Dimensions updated in host context

#### 4.7 Host Context Changed Notification
**Test ID**: `mcp-apps-life-07`
**Description**: Server sends host-context-changed
**Preconditions**: Host context changes (theme, display mode, etc.)
**Steps**:
1. Host context changes
2. Server notifies UI
**Expected Result**:
- Server sends `ui/notifications/host-context-changed`
- Contains partial `HostContext` update
- UI merges with existing context

---

### 5. Host Context & Theming Tests

#### 5.1 Theme Variables in Initialize
**Test ID**: `mcp-apps-ctx-01`
**Description**: Server provides theme variables in initialize response
**Preconditions**: UI initializing
**Steps**:
1. UI sends `ui/initialize`
2. Server responds with `McpUiInitializeResult`
**Expected Result**:
- Response includes `hostContext.styles.variables`
- Variables follow standardized naming (e.g., `--color-background-primary`)

#### 5.2 Display Mode Support
**Test ID**: `mcp-apps-ctx-02`
**Description**: Server respects display mode capabilities
**Preconditions**: UI declares supported display modes
**Steps**:
1. UI sends `ui/initialize` with `appCapabilities.availableDisplayModes`
2. Server responds with `hostContext.availableDisplayModes`
**Expected Result**:
- Server only offers modes supported by both parties
- Intersection of capabilities provided

#### 5.3 Display Mode Request
**Test ID**: `mcp-apps-ctx-03`
**Description**: Server handles display mode change request
**Preconditions**: UI initialized
**Steps**:
1. UI sends `ui/request-display-mode`
2. Server processes request
**Expected Result**:
- Server responds with actual mode set
- May differ from requested mode
- Returns current mode if change denied

#### 5.4 Container Dimensions - Fixed
**Test ID**: `mcp-apps-ctx-04`
**Description**: Server handles fixed container dimensions
**Preconditions**: Host specifies fixed dimensions
**Steps**:
1. Server provides `hostContext.containerDimensions` with `width` and `height`
**Expected Result**:
- UI should fill container (100%)
- No auto-resize notifications needed

#### 5.5 Container Dimensions - Flexible
**Test ID**: `mcp-apps-ctx-05`
**Description**: Server handles flexible container dimensions
**Preconditions**: Host specifies max dimensions
**Steps**:
1. Server provides `hostContext.containerDimensions` with `maxWidth`/`maxHeight`
2. UI resizes based on content
3. UI sends `ui/notifications/size-changed`
**Expected Result**:
- Server updates iframe dimensions to match
- Dimensions don't exceed max values

#### 5.6 Fonts Loading
**Test ID**: `mcp-apps-ctx-06`
**Description**: Server provides font CSS
**Preconditions**: Host has custom fonts
**Steps**:
1. Server includes `hostContext.styles.css.fonts`
**Expected Result**:
- CSS contains valid `@font-face` or `@import` rules
- Fonts reference valid URLs

---

### 6. UI Requests to Host Tests

#### 6.1 Open Link Request
**Test ID**: `mcp-apps-req-01`
**Description**: Server handles open-link request
**Preconditions**: UI wants to open external URL
**Steps**:
1. UI sends `ui/open-link` with URL
2. Server processes request
**Expected Result**:
- On success: empty result
- On denial: error with code -32000 and message

#### 6.2 Message Request
**Test ID**: `mcp-apps-req-02`
**Description**: Server handles message request
**Preconditions**: UI wants to send message to conversation
**Steps**:
1. UI sends `ui/message` with role and content
2. Server processes request
**Expected Result**:
- Message injected into conversation
- Response indicates success or denial

#### 6.3 Update Model Context Request
**Test ID**: `mcp-apps-req-03`
**Description**: Server handles context update
**Preconditions**: UI wants to update model context
**Steps**:
1. UI sends `ui/update-model-context`
2. Server stores context
3. Next turn uses updated context
**Expected Result**:
- Context stored for future turns
- Overwrites previous context from same UI
- May be deduplicated

#### 6.4 Tool Call from UI
**Test ID**: `mcp-apps-req-04`
**Description**: Server proxies tool call from UI
**Preconditions**: UI wants to call server tool
**Steps**:
1. UI sends `tools/call`
2. Server proxies to actual tool
3. Result returned to UI
**Expected Result**:
- Tool executes
- Result returned via standard JSON-RPC
- Works for tools with `visibility: ["app"]`

---

### 7. Sandbox Proxy Tests

#### 7.1 Sandbox Proxy Ready
**Test ID**: `mcp-apps-sbox-01`
**Description**: Server handles sandbox-proxy-ready
**Preconditions**: Web-based host with sandbox
**Steps**:
1. Sandbox sends `ui/notifications/sandbox-proxy-ready`
2. Server responds with `ui/notifications/sandbox-resource-ready`
**Expected Result**:
- Server sends HTML content
- Includes CSP and permissions configuration

#### 7.2 CSP Enforcement
**Test ID**: `mcp-apps-sbox-02`
**Description**: Server enforces declared CSP domains
**Preconditions**: UI resource has CSP metadata
**Steps**:
1. Server provides CSP configuration
2. Sandbox enforces restrictions
**Expected Result**:
- Only declared domains allowed
- Default restrictive policy if no CSP provided

#### 7.3 Permission Policy
**Test ID**: `mcp-apps-sbox-03`
**Description**: Server respects permission requests
**Preconditions**: UI requests permissions
**Steps**:
1. UI declares permissions in metadata
2. Sandbox sets iframe `allow` attribute
**Expected Result**:
- Camera, microphone, geolocation, clipboard permissions set
- Host may deny permissions not granted

---

### 8. Regression & Compatibility Tests

#### 8.1 Non-Supporting Client Fallback
**Test ID**: `mcp-apps-reg-01`
**Description**: Server works with non-UI clients
**Preconditions**: Standard MCP client without Apps support
**Steps**:
1. Initialize without UI extension
2. Call tools/list
3. Call tool
**Expected Result**:
- Tools work normally
- Text responses only
- No UI metadata exposed

#### 8.2 Backward Compatibility - All Protocol Versions
**Test ID**: `mcp-apps-reg-02`
**Description**: MCP Apps doesn't break existing protocol versions
**Preconditions**: Test all supported versions
**Steps**:
1. Initialize with each protocol version (2024-10-07 through 2025-11-25)
2. Verify basic MCP functionality
**Expected Result**:
- All versions work as before
- MCP Apps is additive only

#### 8.3 Mixed Tool Set
**Test ID**: `mcp-apps-reg-03`
**Description**: UI and non-UI tools coexist
**Preconditions**: Server has both types of tools
**Steps**:
1. Initialize with UI support
2. Call tools/list
3. Call both UI and non-UI tools
**Expected Result**:
- Both tool types work
- UI tools return UI metadata
- Non-UI tools work normally

#### 8.4 Resource Read Standard Resources
**Test ID**: `mcp-apps-reg-04`
**Description**: Standard resources still work
**Preconditions**: Server has standard and UI resources
**Steps**:
1. Initialize with UI support
2. Read standard resource (non-ui://)
**Expected Result**:
- Standard resources return normally
- No UI-specific processing applied

---

## Test Implementation Notes

### Test File Organization

```
tests/unit/mcp/mcp_apps/
├── capability_negotiation_test.cpp      # Tests 1.1 - 1.5
├── ui_resource_handling_test.cpp        # Tests 2.1 - 2.9
├── tool_metadata_test.cpp               # Tests 3.1 - 3.7
├── lifecycle_notifications_test.cpp     # Tests 4.1 - 4.7
├── host_context_test.cpp                # Tests 5.1 - 5.6
├── ui_requests_test.cpp                 # Tests 6.1 - 6.4
├── sandbox_proxy_test.cpp               # Tests 7.1 - 7.3
└── regression_compatibility_test.cpp    # Tests 8.1 - 8.4
```

### Test Utilities Needed

1. **Mock UI Resource Factory**: Create test UI resources with various configurations
2. **Capability Helper**: Set up client capabilities with/without MCP Apps
3. **Lifecycle Tracker**: Track notification sequences
4. **Host Context Builder**: Construct various host context scenarios
5. **Sandbox Simulator**: Simulate sandbox proxy behavior

### Dependencies

- Catch2 testing framework (already in use)
- JSON library (nlohmann/json, already in use)
- MCP protocol types (already defined)
- UI resource types (to be defined)

### Phase Implementation

**Phase 1**: Tests 1.1-1.5, 2.1-2.4, 3.1-3.3, 8.1-8.2 (Basic support)
**Phase 2**: Tests 2.5-2.9, 3.4-3.7, 4.1-4.3 (Rich metadata & lifecycle)
**Phase 3**: Tests 4.4-4.7, 5.1-5.6, 6.1-6.4 (Advanced features)
**Phase 4**: Tests 7.1-7.3, 8.3-8.4 (Sandbox & comprehensive regression)

---

## Success Criteria

- All 45+ test cases pass
- 100% spec coverage for stable features
- Graceful degradation verified
- No regressions in existing MCP functionality
- Documentation updated with implementation details

---

## References

- [SEP-1865 Specification](https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx)
- [MCP Apps Blog Post](https://blog.modelcontextprotocol.io/posts/2026-01-26-mcp-apps/)
- [ext-apps Repository](https://github.com/modelcontextprotocol/ext-apps)
- [MCP Specification](https://modelcontextprotocol.io/specification/2025-11-25)
