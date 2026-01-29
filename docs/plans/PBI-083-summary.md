# PBI-083: MCP Apps Extension Support - Implementation Summary

**Status**: Phase 1 Complete (Test Specification and Phase 1 Tests)  
**PBI**: PBI-083  
**Owner**: codex  
**Created**: 2026-01-29

## Overview

Comprehensive test specification and initial test suite for MCP Apps extension support in YAMS, based on SEP-1865 (Stable 2026-01-26).

## Deliverables

### 1. Test Specification Document
**File**: `docs/plans/PBI-083-mcp-apps-test-spec.md`

Complete test specification covering:
- 45+ test cases across 9 categories
- Full spec coverage for stable features
- Phase-based implementation plan
- Success criteria and references

**Categories**:
1. Capability Negotiation (5 tests)
2. UI Resource Handling (9 tests)
3. Tool Metadata and Visibility (7 tests)
4. Lifecycle Notifications (7 tests)
5. Host Context and Theming (6 tests)
6. UI Requests to Host (4 tests)
7. Sandbox Proxy (3 tests)
8. Regression and Compatibility (4 tests)

### 2. Phase 1 Test Implementation
**Files**:
- `tests/unit/mcp/mcp_apps/capability_negotiation_test.cpp` (5 tests)
- `tests/unit/mcp/mcp_apps/ui_resource_handling_test.cpp` (2 tests)
- `tests/unit/mcp/mcp_apps/tool_metadata_test.cpp` (3 tests)

**Total**: 10 test cases implemented, all currently SKIPPED pending implementation

## Test Organization

```
tests/unit/mcp/mcp_apps/
├── capability_negotiation_test.cpp      # Phase 1 - Complete
├── ui_resource_handling_test.cpp        # Phase 1 - Partial
├── tool_metadata_test.cpp               # Phase 1 - Partial
├── lifecycle_notifications_test.cpp     # Phase 2 - TODO
├── host_context_test.cpp                # Phase 2 - TODO
├── ui_requests_test.cpp                 # Phase 3 - TODO
├── sandbox_proxy_test.cpp               # Phase 4 - TODO
└── regression_compatibility_test.cpp    # Phase 4 - TODO
```

## Key Features to Implement

### Phase 1 (Basic Support)
- Extension capability negotiation
- UI resource listing
- HTML content serving (text)
- Tool-UI metadata linkage
- Basic visibility support

### Phase 2 (Rich Metadata and Lifecycle)
- CSP and permissions metadata
- Resource blob encoding
- Tool input/result notifications
- Cancellation support
- Host context management

### Phase 3 (Advanced Features)
- Display modes
- Theme variables
- UI requests (open-link, message, context)
- Bidirectional communication

### Phase 4 (Sandbox and Production)
- Sandbox proxy support
- Comprehensive regression tests
- Security validation
- Production readiness

## Implementation Notes

### Required Code Changes

1. **Extension Detection** (`src/mcp/mcp_server.cpp`)
   - Parse `extensions["io.modelcontextprotocol/ui"]`
   - Validate mime types
   - Store capability state

2. **UI Resource Types** (`include/yams/mcp/mcp_server.h`)
   - Add `UIResource` struct
   - Add `uiResources_` storage
   - CSP and permissions structures

3. **Resource Handlers** (`src/mcp/mcp_server.cpp`)
   - `resources/list` - filter by capability
   - `resources/read` - serve HTML content
   - Support both text and blob encoding

4. **Tool Metadata** (`src/mcp/mcp_server.cpp`)
   - Add `_meta.ui` to tool definitions
   - Handle visibility filtering
   - Conditional metadata based on capability

## Next Steps

1. **Implement capability negotiation** (Reference: capability_negotiation_test.cpp)
2. **Add UI resource infrastructure** (Reference: ui_resource_handling_test.cpp)
3. **Implement tool metadata** (Reference: tool_metadata_test.cpp)
4. **Enable tests incrementally** as features are implemented
5. **Continue with Phase 2-4** tests and implementation

## References

- [SEP-1865 Specification](https://github.com/modelcontextprotocol/ext-apps/blob/main/specification/2026-01-26/apps.mdx)
- [MCP Apps Blog](https://blog.modelcontextprotocol.io/posts/2026-01-26-mcp-apps/)
- [ext-apps Repository](https://github.com/modelcontextprotocol/ext-apps)
