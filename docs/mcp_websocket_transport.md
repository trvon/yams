# MCP WebSocket Transport

This document describes the WebSocket transport implementation for the YAMS MCP (Model Context Protocol) server.

## Overview

The WebSocket transport allows the MCP server to communicate over WebSocket connections instead of the default standard I/O transport. This enables:

- Remote MCP server access over network connections
- Integration with web applications and browser-based clients
- Secure communication using WSS (WebSocket Secure)
- Connection pooling and load balancing capabilities

## Implementation

### WebSocketTransport Class

The `WebSocketTransport` class implements the `ITransport` interface and provides:

```cpp
class WebSocketTransport : public ITransport {
public:
    struct Config {
        std::string host = "localhost";
        uint16_t port = 8080;
        std::string path = "/mcp";
        bool useSSL = false;
        std::chrono::milliseconds connectTimeout{5000};
        std::chrono::milliseconds receiveTimeout{30000};
        size_t maxMessageSize = 1024 * 1024; // 1MB
        bool enablePingPong = true;
        std::chrono::seconds pingInterval{30};
    };
    
    explicit WebSocketTransport(const Config& config);
    
    // ITransport interface
    void send(const json& message) override;
    json receive() override;
    bool isConnected() const override;
    void close() override;
    
    // WebSocket-specific methods
    bool connect();
    void reconnect();
    bool waitForConnection(std::chrono::milliseconds timeout);
};
```

### Key Features

1. **Protocol Support**: Both WebSocket (ws://) and WebSocket Secure (wss://) protocols
2. **Connection Management**: Automatic connection establishment with timeout handling
3. **Message Queuing**: Thread-safe message queue with configurable timeouts
4. **SSL/TLS Support**: Full SSL support for secure connections using OpenSSL
5. **Error Handling**: Comprehensive error handling and connection recovery
6. **Threading**: Multi-threaded implementation with proper synchronization

### Dependencies

- **websocketpp**: Header-only WebSocket library
- **OpenSSL**: SSL/TLS support for secure connections
- **Threads**: POSIX threads for concurrent operation
- **nlohmann/json**: JSON serialization/deserialization

## Usage Examples

### Basic WebSocket Connection

```cpp
#include <yams/mcp/mcp_server.h>

// Configure WebSocket transport
yams::mcp::WebSocketTransport::Config config;
config.host = "localhost";
config.port = 8080;
config.path = "/mcp";

// Create transport and connect
auto transport = std::make_unique<yams::mcp::WebSocketTransport>(config);
if (!transport->connect()) {
    // Handle connection failure
}

// Create MCP server with WebSocket transport
yams::mcp::MCPServer server(contentStore, searchExecutor, std::move(transport));
server.start();
```

### Secure WebSocket Connection (WSS)

```cpp
// Configure for secure WebSocket
yams::mcp::WebSocketTransport::Config config;
config.host = "secure-server.example.com";
config.port = 443;
config.path = "/api/mcp";
config.useSSL = true;

auto transport = std::make_unique<yams::mcp::WebSocketTransport>(config);
```

### Custom Configuration

```cpp
// Custom timeouts and settings
yams::mcp::WebSocketTransport::Config config;
config.host = "mcp-server.internal";
config.port = 9090;
config.connectTimeout = std::chrono::milliseconds{10000};  // 10 seconds
config.receiveTimeout = std::chrono::milliseconds{60000};  // 60 seconds
config.maxMessageSize = 2 * 1024 * 1024;                  // 2MB
config.enablePingPong = true;
config.pingInterval = std::chrono::seconds{15};           // 15 seconds

auto transport = std::make_unique<yams::mcp::WebSocketTransport>(config);
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `host` | string | "localhost" | WebSocket server hostname |
| `port` | uint16_t | 8080 | WebSocket server port |
| `path` | string | "/mcp" | WebSocket endpoint path |
| `useSSL` | bool | false | Enable WSS (secure WebSocket) |
| `connectTimeout` | milliseconds | 5000 | Connection timeout |
| `receiveTimeout` | milliseconds | 30000 | Message receive timeout |
| `maxMessageSize` | size_t | 1MB | Maximum message size |
| `enablePingPong` | bool | true | Enable WebSocket ping/pong |
| `pingInterval` | seconds | 30 | Ping interval for keep-alive |

## Thread Safety

The WebSocket transport implementation is fully thread-safe:

- Connection state is managed with atomic variables
- Message queue access is protected with mutexes
- Condition variables are used for efficient waiting
- Proper cleanup ensures no resource leaks

## Error Handling

The transport handles various error conditions:

- **Connection Failures**: Automatic retry with exponential backoff
- **Network Timeouts**: Configurable timeouts for all operations
- **Message Parsing**: Robust JSON parsing with error recovery
- **SSL Errors**: Detailed SSL error reporting and handling
- **Protocol Violations**: WebSocket protocol compliance checking

## Integration with MCP Server

The WebSocket transport integrates seamlessly with the existing MCP server:

```cpp
// Create any transport (stdio or WebSocket)
std::unique_ptr<ITransport> transport;

if (useWebSocket) {
    WebSocketTransport::Config config;
    // ... configure as needed
    transport = std::make_unique<WebSocketTransport>(config);
    static_cast<WebSocketTransport*>(transport.get())->connect();
} else {
    transport = std::make_unique<StdioTransport>();
}

// MCP server works identically with any transport
MCPServer server(contentStore, searchExecutor, std::move(transport));
server.start();
```

## Building

The WebSocket transport is automatically included when building with tools enabled:

```bash
mkdir build && cd build
cmake -DYAMS_BUILD_TOOLS=ON ..
make
```

Required system dependencies:
- OpenSSL development headers
- pthread library
- C++20 compatible compiler

## Testing

Example test client for WebSocket transport:

```cpp
// Test WebSocket transport connectivity
WebSocketTransport::Config config;
WebSocketTransport transport(config);

if (transport.connect()) {
    // Send test message
    nlohmann::json testMessage = {
        {"jsonrpc", "2.0"},
        {"method", "initialize"},
        {"id", 1},
        {"params", {{"clientInfo", {{"name", "test-client"}}}}}
    };
    
    transport.send(testMessage);
    
    // Receive response
    auto response = transport.receive();
    // Verify response...
}
```

This implementation provides a robust, production-ready WebSocket transport for the YAMS MCP server, enabling flexible deployment scenarios and integration with web-based applications.