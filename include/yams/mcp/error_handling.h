#pragma once

#include <nlohmann/json.hpp>
#include <yams/core/types.h>

#include <concepts>
#include <string>
#include <string_view>
#include <system_error>

namespace yams::mcp {

using json = nlohmann::json;

// Error categories for MCP operations
enum class MCPErrorCode {
    InvalidJson = 1,
    TransportClosed,
    ProtocolViolation,
    TimeoutExpired,
    NetworkError,
    InternalError
};

// Custom error category for MCP errors
class MCPErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "mcp_error"; }

    std::string message(int ev) const override {
        switch (static_cast<MCPErrorCode>(ev)) {
            case MCPErrorCode::InvalidJson:
                return "Invalid JSON message";
            case MCPErrorCode::TransportClosed:
                return "Transport connection closed";
            case MCPErrorCode::ProtocolViolation:
                return "MCP protocol violation";
            case MCPErrorCode::TimeoutExpired:
                return "Operation timeout expired";
            case MCPErrorCode::NetworkError:
                return "Network communication error";
            case MCPErrorCode::InternalError:
                return "Internal MCP error";
            default:
                return "Unknown MCP error";
        }
    }
};

// Global error category instance
inline const MCPErrorCategory& mcp_error_category() {
    static MCPErrorCategory instance;
    return instance;
}

// Error code factory functions
inline std::error_code make_error_code(MCPErrorCode e) {
    return {static_cast<int>(e), mcp_error_category()};
}

// Detailed error information
struct MCPError {
    MCPErrorCode code;
    std::string message;
    std::string context;

    MCPError(MCPErrorCode c, std::string msg, std::string ctx = "")
        : code(c), message(std::move(msg)), context(std::move(ctx)) {}

    std::error_code error_code() const { return make_error_code(code); }

    std::string full_message() const {
        if (context.empty()) {
            return message;
        }
        return message + " (context: " + context + ")";
    }
};

// Result types using yams::Result
template <typename T> using MCPResult = Result<T>;

using JsonParseResult = MCPResult<json>;
using MessageResult = MCPResult<json>;

// Transport state management with atomic operations
enum class TransportState : int {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
    Error = 3,
    Closing = 4
};

// C++20 concepts for MCP validation

// Concept for JSON-RPC message validation
template <typename T>
concept JsonRPCMessage = requires(T msg) {
    { msg.is_object() } -> std::same_as<bool>;
    { msg.contains("jsonrpc") } -> std::same_as<bool>;
    { msg["jsonrpc"].template get<std::string>() } -> std::same_as<std::string>;
};

// Concept for MCP transport implementations
template <typename T>
concept MCPTransport = requires(T transport, const json& msg) {
    { transport.send(msg) } -> std::same_as<void>;
    { transport.receive() } -> std::convertible_to<MessageResult>;
    { transport.isConnected() } -> std::same_as<bool>;
    { transport.close() } -> std::same_as<void>;
};

// Concept for serializable message types
template <typename T>
concept JsonSerializable = requires(T obj) {
    { obj.to_json() } -> std::convertible_to<json>;
    { T::from_json(json{}) } -> std::convertible_to<MCPResult<T>>;
};

// Protocol constants (constexpr for compile-time validation)
namespace protocol {
constexpr std::string_view JSONRPC_VERSION = "2.0";
constexpr std::string_view METHOD_INITIALIZE = "initialize";
constexpr std::string_view METHOD_INITIALIZED = "initialized";
constexpr std::string_view METHOD_TOOLS_LIST = "tools/list";
constexpr std::string_view METHOD_TOOLS_CALL = "tools/call";
constexpr std::string_view METHOD_RESOURCES_LIST = "resources/list";
constexpr std::string_view METHOD_RESOURCES_READ = "resources/read";

// Error codes from JSON-RPC 2.0 specification
constexpr int PARSE_ERROR = -32700;
constexpr int INVALID_REQUEST = -32600;
constexpr int METHOD_NOT_FOUND = -32601;
constexpr int INVALID_PARAMS = -32602;
constexpr int INTERNAL_ERROR = -32603;
} // namespace protocol

// Utility functions for monadic error handling

// Chain operations with automatic error propagation
template <typename T, typename F>
auto and_then(const MCPResult<T>& result, F&& func) -> decltype(func(result.value())) {
    if (result) {
        return func(result.value());
    } else {
        using ReturnType = decltype(func(result.value()));
        return ReturnType{result.error()};
    }
}

// Provide fallback value on error
template <typename T, typename F>
auto or_else(const MCPResult<T>& result, F&& func) -> MCPResult<T> {
    if (result) {
        return result;
    } else {
        return func(result.error());
    }
}

// Transform error types
template <typename T, typename F>
auto transform_error(const MCPResult<T>& result, F&& func) -> MCPResult<T> {
    if (result) {
        return result;
    } else {
        auto newError = func(result.error());
        return MCPResult<T>{newError};
    }
}

// JSON parsing utilities with error handling
namespace json_utils {
// Safe JSON parsing without exceptions
inline JsonParseResult parse_json(std::string_view input) noexcept {
    if (input.empty()) {
        return Error{ErrorCode::InvalidData, "Empty input string for JSON parsing"};
    }

    try {
        auto result = json::parse(input);
        return result;
    } catch (const json::parse_error& e) {
        return Error{ErrorCode::InvalidData, std::string("JSON parse error: ") + e.what() +
                                                 " at position " + std::to_string(e.byte)};
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData, std::string("JSON parsing failed: ") + e.what()};
    }
}

// Validate JSON-RPC message structure
inline MCPResult<json> validate_jsonrpc_message(const json& msg) noexcept {
    if (!msg.is_object()) {
        return Error{ErrorCode::InvalidData, "Message must be a JSON object"};
    }

    if (!msg.contains("jsonrpc")) {
        return Error{ErrorCode::InvalidData, "Missing 'jsonrpc' field"};
    }

    const auto& version = msg["jsonrpc"];
    if (!version.is_string() || version.get<std::string>() != protocol::JSONRPC_VERSION) {
        return Error{ErrorCode::InvalidData, "Invalid or missing jsonrpc version"};
    }

    return msg;
}

// Safe JSON field access
template <typename T>
MCPResult<T> get_field(const json& obj, std::string_view field_name) noexcept {
    try {
        if (!obj.contains(field_name)) {
            return Error{ErrorCode::InvalidData,
                         std::string("Missing required field: ") + std::string(field_name)};
        }
        return obj[field_name].get<T>();
    } catch (const std::exception& e) {
        return Error{ErrorCode::InvalidData,
                     std::string("Invalid field '") + std::string(field_name) + "': " + e.what()};
    }
}
} // namespace json_utils

} // namespace yams::mcp