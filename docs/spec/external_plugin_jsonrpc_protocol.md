# YAMS External Plugin JSON-RPC Protocol Specification

**Version:** 1.0  
**Date:** 2025-11-01  
**Status:** Draft  
**Related:** PBI-075, docs/PLUGINS.md, external/yams_sdk/

---

## Table of Contents

1. [Overview](#overview)
2. [Transport](#transport)
3. [Message Format](#message-format)
4. [Lifecycle Methods](#lifecycle-methods)
5. [Content Extraction Interface](#content-extraction-interface)
6. [Error Handling](#error-handling)
7. [Examples](#examples)
8. [Implementation Notes](#implementation-notes)

---

## Overview

External YAMS plugins communicate with the host (YAMS daemon or ExternalPluginExtractor) via **JSON-RPC 2.0** over **stdio** (newline-delimited JSON). This protocol enables:

- **Language-agnostic plugins**: Python, JavaScript, Ruby, etc.
- **Process isolation**: Plugins run in separate processes
- **Simple debugging**: Human-readable JSON messages
- **Existing tooling**: Standard JSON-RPC libraries

### Design Principles

1. **JSON-RPC 2.0 compliant**: Standard request/response/notification format
2. **Newline-delimited**: One message per line for easy parsing
3. **Synchronous**: Request-response pairs (async via batching if needed)
4. **Stateful**: Plugin maintains state across multiple requests
5. **Graceful degradation**: Missing optional methods return NotImplemented error

---

## Transport

### Stdio Communication

- **Protocol**: JSON-RPC 2.0 over stdin/stdout
- **Framing**: Newline-delimited JSON (NDJSON)
- **Encoding**: UTF-8
- **Direction**: Bidirectional (host sends requests, plugin sends responses)

### Message Flow

```
Host (C++)                          Plugin (Python/etc.)
───────────                         ────────────────────
  stdin  ──────────────────────────>  receives line
         {"jsonrpc":"2.0",...}         parses JSON
         
  stdout <──────────────────────────  sends line
         {"jsonrpc":"2.0",...}         response JSON
```

### Process Management

- **Spawn**: Host spawns plugin process (e.g., `python plugin.py`)
- **Handshake**: First message MUST be `handshake.manifest`
- **Init**: Second message SHOULD be `plugin.init` with config
- **Operation**: Host sends content extraction or custom RPC requests
- **Shutdown**: Host sends `plugin.shutdown`, then terminates process
- **Timeout**: Configurable per-request timeout (default: 30s for content extraction)
- **Crash handling**: Host detects broken pipe, logs error, marks plugin unhealthy

---

## Message Format

### JSON-RPC 2.0 Structure

All messages follow [JSON-RPC 2.0](https://www.jsonrpc.org/specification) specification.

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "method_name",
  "params": {
    "param1": "value1",
    "param2": "value2"
  }
}
```

- `jsonrpc`: MUST be `"2.0"`
- `id`: Unique request identifier (number or string). Omit for notifications.
- `method`: Method name (string, case-sensitive)
- `params`: Parameters object (optional, defaults to `{}`)

#### Success Response

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "key": "value"
  }
}
```

#### Error Response

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32601,
    "message": "Method not found",
    "data": {
      "method": "unknown_method"
    }
  }
}
```

### Standard Error Codes

Per JSON-RPC 2.0 + YAMS extensions:

| Code | Name | Meaning |
|------|------|---------|
| -32700 | Parse error | Invalid JSON |
| -32600 | Invalid Request | Missing required fields |
| -32601 | Method not found | Method does not exist |
| -32602 | Invalid params | Invalid method parameters |
| -32603 | Internal error | Plugin internal error |
| -32000 | Extraction failed | Content extraction failed (YAMS) |
| -32001 | Unsupported format | Plugin cannot handle this file type (YAMS) |
| -32002 | Timeout | Operation timed out (YAMS) |
| -32003 | Not initialized | Plugin not initialized (YAMS) |

---

## Lifecycle Methods

### 1. handshake.manifest

**Purpose**: Discover plugin capabilities  
**Called**: First message after plugin spawn  
**Required**: Yes

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "handshake.manifest"
}
```

#### Response

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "name": "yams_ghidra",
    "version": "0.0.1",
    "description": "Ghidra binary analysis plugin",
    "author": "YAMS Contributors",
    "interfaces": ["ghidra_analysis_v1", "content_extractor_v1"],
    "capabilities": {
      "content_extraction": {
        "formats": ["application/x-executable", "application/x-sharedlib"],
        "extensions": [".exe", ".dll", ".so", ".dylib", ".elf"]
      }
    }
  }
}
```

**Fields:**
- `name` (string, required): Plugin identifier (alphanumeric + underscore)
- `version` (string, required): Semantic version (e.g., "1.0.0")
- `description` (string, optional): Human-readable description
- `author` (string, optional): Plugin author
- `interfaces` (array of strings, required): Supported interface IDs
- `capabilities` (object, optional): Detailed capability metadata

---

### 2. plugin.init

**Purpose**: Initialize plugin with configuration  
**Called**: After handshake, before any content extraction  
**Required**: Yes

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "plugin.init",
  "params": {
    "config": {
      "ghidra_install": "/opt/ghidra",
      "project_dir": "/tmp/yams-ghidra",
      "max_functions": 100,
      "timeout_sec": 30
    }
  }
}
```

**Params:**
- `config` (object, required): Plugin-specific configuration

#### Response

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": {
    "status": "initialized",
    "config_applied": {
      "ghidra_install": "/opt/ghidra",
      "project_dir": "/tmp/yams-ghidra-12345"
    }
  }
}
```

**Result:**
- `status` (string, required): "initialized" or "error"
- `config_applied` (object, optional): Actually applied configuration

---

### 3. plugin.health

**Purpose**: Check plugin health and readiness  
**Called**: Periodically or before heavy operations  
**Required**: Yes

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "plugin.health"
}
```

#### Response

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "status": "ok",
    "started": true,
    "project_dir": "/tmp/yams-ghidra-12345",
    "memory_mb": 512,
    "uptime_sec": 120
  }
}
```

**Result:**
- `status` (string, required): "ok" | "degraded" | "error"
- Additional fields are plugin-specific

---

### 4. plugin.shutdown

**Purpose**: Graceful plugin shutdown  
**Called**: Before process termination  
**Required**: Yes

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "method": "plugin.shutdown"
}
```

#### Response

```json
{
  "jsonrpc": "2.0",
  "id": 4,
  "result": null
}
```

**Notes:**
- Plugin should cleanup resources (temp files, JVM, etc.)
- Host will terminate process after timeout (default: 5s)

---

## Content Extraction Interface

### content_extractor_v1

For plugins implementing `content_extractor_v1` interface.

### 5. extractor.supports

**Purpose**: Check if plugin can extract content from file type  
**Called**: Before extraction to validate support  
**Required**: Yes for `content_extractor_v1`

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "method": "extractor.supports",
  "params": {
    "mime_type": "application/x-executable",
    "extension": ".elf"
  }
}
```

**Params:**
- `mime_type` (string, optional): MIME type (e.g., "application/x-executable")
- `extension` (string, optional): File extension (e.g., ".elf")
- At least one MUST be provided

#### Response

```json
{
  "jsonrpc": "2.0",
  "id": 5,
  "result": {
    "supported": true,
    "confidence": 1.0,
    "method": "ghidra_decompile"
  }
}
```

**Result:**
- `supported` (bool, required): True if plugin can handle this file type
- `confidence` (float, optional): Confidence level (0.0-1.0, default: 1.0)
- `method` (string, optional): Extraction method name

---

### 6. extractor.extract

**Purpose**: Extract searchable content from binary  
**Called**: After validation via `supports()`  
**Required**: Yes for `content_extractor_v1`

#### Request

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "method": "extractor.extract",
  "params": {
    "source": {
      "type": "path",
      "path": "/tmp/yams-binary-abc123"
    },
    "options": {
      "max_functions": 100,
      "decompile": true,
      "include_metadata": true,
      "timeout_sec": 30
    }
  }
}
```

**Params:**
- `source` (object, required): Content source
  - `type` (string, required): "path" or "bytes"
  - `path` (string, required if type=path): Temporary file path
  - `data` (string, required if type=bytes): Base64-encoded binary data
- `options` (object, optional): Extraction options
  - `max_functions` (int, optional): Maximum functions to extract (default: 100)
  - `decompile` (bool, optional): Include decompiled code (default: true)
  - `include_metadata` (bool, optional): Include metadata (default: true)
  - `timeout_sec` (int, optional): Per-function timeout (default: 10)

#### Success Response

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "result": {
    "success": true,
    "content": "Binary: /tmp/yams-binary-abc123\nType: ELF 64-bit LSB executable\nArchitecture: x86:LE:64:default\nEntry Point: 0x401000\n\nFunctions:\n---\nFunction: main\nAddress: 0x401000\nSize: 256 bytes\nDecompiled:\nint main(int argc, char** argv) {\n    if (argc < 2) {\n        printf(\"Usage: %s <input>\\n\", argv[0]);\n        return 1;\n    }\n    return process_input(argv[1]);\n}\n\n---\nFunction: process_input\nAddress: 0x401100\nSize: 128 bytes\nDecompiled:\nint process_input(char* input) {\n    size_t len = strlen(input);\n    if (len > MAX_LEN) {\n        return -1;\n    }\n    return validate(input, len);\n}\n",
    "metadata": {
      "architecture": "x86:LE:64:default",
      "type": "ELF",
      "functions_count": "42",
      "entry_point": "0x401000",
      "extraction_method": "ghidra_decompile",
      "extraction_time_ms": "1234"
    },
    "stats": {
      "functions_extracted": 42,
      "functions_decompiled": 38,
      "extraction_time_ms": 1234,
      "decompilation_failures": 4
    }
  }
}
```

**Result:**
- `success` (bool, required): True if extraction succeeded
- `content` (string, required if success=true): Extracted searchable text
- `metadata` (object, optional): Key-value metadata
- `stats` (object, optional): Extraction statistics

#### Error Response

```json
{
  "jsonrpc": "2.0",
  "id": 6,
  "error": {
    "code": -32000,
    "message": "Extraction failed: Ghidra could not analyze binary",
    "data": {
      "reason": "UnsupportedArchitecture",
      "architecture": "ARM:BE:32:Cortex",
      "partial_functions": 5
    }
  }
}
```

---

## Error Handling

### Plugin Error Response Format

Plugins SHOULD return structured errors:

```json
{
  "jsonrpc": "2.0",
  "id": 123,
  "error": {
    "code": -32000,
    "message": "Human-readable error message",
    "data": {
      "reason": "ErrorCategory",
      "details": "Additional context",
      "recoverable": false
    }
  }
}
```

### Common Error Scenarios

#### 1. Plugin Not Initialized

```json
{
  "code": -32003,
  "message": "Plugin not initialized. Call plugin.init first.",
  "data": {
    "reason": "NotInitialized",
    "required_method": "plugin.init"
  }
}
```

#### 2. Unsupported File Type

```json
{
  "code": -32001,
  "message": "Unsupported file format: application/octet-stream",
  "data": {
    "reason": "UnsupportedFormat",
    "mime_type": "application/octet-stream",
    "extension": ".bin",
    "supported_formats": ["application/x-executable"]
  }
}
```

#### 3. Extraction Timeout

```json
{
  "code": -32002,
  "message": "Extraction timed out after 30 seconds",
  "data": {
    "reason": "Timeout",
    "timeout_sec": 30,
    "partial_extraction": true,
    "functions_extracted": 15
  }
}
```

#### 4. External Dependency Missing

```json
{
  "code": -32603,
  "message": "Ghidra not found. Set GHIDRA_INSTALL_DIR or provide ghidra_install in config.",
  "data": {
    "reason": "DependencyMissing",
    "dependency": "ghidra",
    "env_var": "GHIDRA_INSTALL_DIR",
    "config_key": "ghidra_install"
  }
}
```

---

## Examples

### Example 1: Complete Binary Extraction Flow

```json
# 1. Host spawns plugin: python plugins/yams-ghidra-plugin/plugin.py

# 2. Handshake
→ {"jsonrpc":"2.0","id":1,"method":"handshake.manifest"}
← {"jsonrpc":"2.0","id":1,"result":{"name":"yams_ghidra","version":"0.0.1","interfaces":["content_extractor_v1"]}}

# 3. Initialize
→ {"jsonrpc":"2.0","id":2,"method":"plugin.init","params":{"config":{"ghidra_install":"/opt/ghidra"}}}
← {"jsonrpc":"2.0","id":2,"result":{"status":"initialized"}}

# 4. Check health
→ {"jsonrpc":"2.0","id":3,"method":"plugin.health"}
← {"jsonrpc":"2.0","id":3,"result":{"status":"ok","started":true}}

# 5. Check support
→ {"jsonrpc":"2.0","id":4,"method":"extractor.supports","params":{"mime_type":"application/x-executable","extension":".elf"}}
← {"jsonrpc":"2.0","id":4,"result":{"supported":true}}

# 6. Extract content
→ {"jsonrpc":"2.0","id":5,"method":"extractor.extract","params":{"source":{"type":"path","path":"/tmp/test.elf"},"options":{"max_functions":50}}}
← {"jsonrpc":"2.0","id":5,"result":{"success":true,"content":"Binary: /tmp/test.elf\n...","metadata":{"functions_count":"42"}}}

# 7. Shutdown
→ {"jsonrpc":"2.0","id":6,"method":"plugin.shutdown"}
← {"jsonrpc":"2.0","id":6,"result":null}

# 8. Host terminates process
```

### Example 2: Error Handling

```json
# Extract from unsupported file
→ {"jsonrpc":"2.0","id":10,"method":"extractor.extract","params":{"source":{"type":"path","path":"/tmp/test.txt"}}}
← {"jsonrpc":"2.0","id":10,"error":{"code":-32001,"message":"Unsupported format","data":{"extension":".txt"}}}
```

---

## Implementation Notes

### For Plugin Developers (Python SDK)

The YAMS SDK (`yams_sdk`) provides `BasePlugin` class that handles protocol automatically:

```python
from yams_sdk import BasePlugin, rpc

class MyExtractor(BasePlugin):
    def manifest(self):
        return {
            "name": "my_extractor",
            "version": "1.0.0",
            "interfaces": ["content_extractor_v1"]
        }
    
    @rpc("extractor.supports")
    def supports(self, mime_type=None, extension=None):
        return {"supported": extension == ".myformat"}
    
    @rpc("extractor.extract")
    def extract(self, source, options=None):
        path = source["path"]
        # ... extraction logic ...
        return {
            "success": True,
            "content": extracted_text,
            "metadata": {}
        }

if __name__ == "__main__":
    MyExtractor().run()
```

### For Host Developers (C++ ExternalPluginExtractor)

Key requirements:
1. **Process spawning**: Use `fork()`/`exec()` or `posix_spawn()`
2. **Stdio piping**: Capture stdout, send to stdin
3. **Line buffering**: Read/write complete lines (NDJSON)
4. **Timeout handling**: Per-request timeouts with `SIGTERM` fallback
5. **Error recovery**: Detect crashes, log, mark plugin unhealthy
6. **JSON parsing**: Use nlohmann/json library
7. **Thread safety**: Synchronize plugin access if multi-threaded

Example skeleton:

```cpp
class ExternalPluginExtractor {
    Result<json> call(const std::string& method, const json& params) {
        // Generate request ID
        int id = nextRequestId_++;
        
        // Build JSON-RPC request
        json request = {
            {"jsonrpc", "2.0"},
            {"id", id},
            {"method", method},
            {"params", params}
        };
        
        // Send to plugin stdin
        pluginStdin_ << request.dump() << "\n" << std::flush;
        
        // Read response from plugin stdout (with timeout)
        std::string line;
        if (!readLineWithTimeout(pluginStdout_, line, timeout_)) {
            return Error{ErrorCode::Timeout, "Plugin response timeout"};
        }
        
        // Parse JSON response
        json response = json::parse(line);
        
        // Check for error
        if (response.contains("error")) {
            return Error{
                ErrorCode::ExternalError,
                response["error"]["message"]
            };
        }
        
        return response["result"];
    }
};
```

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-11-01 | Initial specification for PBI-075 |

---

## References

- [JSON-RPC 2.0 Specification](https://www.jsonrpc.org/specification)
- YAMS SDK: `external/yams_sdk/`
- Ghidra Plugin: `plugins/yams-ghidra-plugin/`
- Plugin Spec: `docs/spec/plugin_spec.md`
- Content Extractor C ABI: `include/yams/plugins/content_extractor_v1.h`
