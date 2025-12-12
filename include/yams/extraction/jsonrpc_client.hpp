/**
 * @file jsonrpc_client.hpp
 * @brief Type-safe JSON-RPC 2.0 client for plugin communication
 *
 * Provides a high-level interface for JSON-RPC communication over PluginProcess.
 * Handles NDJSON framing, request/response matching, and timeout management.
 */

#pragma once

#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <yams/extraction/plugin_process.hpp>

namespace yams::extraction {

using json = nlohmann::json;

/**
 * @brief JSON-RPC 2.0 client for typed communication with external plugins
 *
 * Features:
 * - Automatic request ID management
 * - NDJSON framing (newline-delimited JSON)
 * - Response timeout handling
 * - Thread-safe request/response tracking
 * - Type-safe result extraction
 *
 * Example:
 * @code
 * PluginProcessConfig config{.executable = "python3", .args = {"plugin.py"}};
 * PluginProcess process{std::move(config)};
 * JsonRpcClient client{process};
 *
 * auto result = client.call("handshake.manifest");
 * if (result) {
 *     std::cout << result->dump() << "\n";
 * }
 * @endcode
 */
class JsonRpcClient {
public:
    /**
     * @brief Construct JSON-RPC client for an existing process
     * @param process PluginProcess to communicate with
     */
    explicit JsonRpcClient(PluginProcess& process);

    /**
     * @brief Call a JSON-RPC method with optional parameters
     * @param method Method name (e.g., "handshake.manifest")
     * @param params Optional parameters (nullptr for no params)
     * @param timeout Maximum time to wait for response
     * @return JSON response result, or std::nullopt on timeout/error
     */
    [[nodiscard]] std::optional<json> call(std::string_view method, json params = nullptr,
                                           std::chrono::milliseconds timeout = std::chrono::seconds{
                                               3});

    /**
     * @brief Call a JSON-RPC method expecting a specific result type
     * @tparam T Expected result type
     * @param method Method name
     * @param params Optional parameters
     * @param timeout Maximum time to wait for response
     * @return Typed result, or std::nullopt on timeout/error/type mismatch
     */
    template <typename T>
    [[nodiscard]] std::optional<T>
    call(std::string_view method, json params = nullptr,
         std::chrono::milliseconds timeout = std::chrono::seconds{3});

    /**
     * @brief Send a JSON-RPC notification (no response expected)
     * @param method Method name
     * @param params Optional parameters
     */
    void notify(std::string_view method, json params = nullptr);

    /**
     * @brief Get the next request ID
     * @return Unique request ID
     */
    [[nodiscard]] int next_id() noexcept {
        return next_id_.fetch_add(1, std::memory_order_relaxed);
    }

    /**
     * @brief Reset the stdout buffer read position
     *
     * Useful when recovering from errors or starting fresh communication.
     */
    void reset_buffer_position() noexcept {
        std::lock_guard lock{mutex_};
        read_pos_ = 0;
    }

private:
    /**
     * @brief Read the next complete JSON response from stdout buffer
     *
     * Non-blocking. Consumes the response from the buffer if found.
     * @return Parsed JSON response, or std::nullopt if no complete response available
     */
    [[nodiscard]] std::optional<json> read_next_response();

    /**
     * @brief Read a JSON-RPC response from stdout (legacy, with timeout)
     * @param timeout Maximum time to wait
     * @return Parsed JSON response, or std::nullopt on timeout
     */
    [[nodiscard]] std::optional<json> read_response(std::chrono::milliseconds timeout);

    /**
     * @brief Build a JSON-RPC 2.0 request
     * @param id Request ID
     * @param method Method name
     * @param params Optional parameters
     * @return Formatted JSON request
     */
    [[nodiscard]] static json build_request(int id, std::string_view method, json params);

    /**
     * @brief Build a JSON-RPC 2.0 notification
     * @param method Method name
     * @param params Optional parameters
     * @return Formatted JSON notification (no ID)
     */
    [[nodiscard]] static json build_notification(std::string_view method, json params);

    PluginProcess& process_;      ///< Reference to managed process
    std::atomic<int> next_id_{1}; ///< Next request ID (thread-safe)
    size_t read_pos_{0};          ///< Current position in stdout buffer
    std::mutex mutex_;            ///< Protects read_pos_
};

// Template implementation
template <typename T>
std::optional<T> JsonRpcClient::call(std::string_view method, json params,
                                     std::chrono::milliseconds timeout) {
    auto response = call(method, params, timeout);
    if (!response) {
        return std::nullopt;
    }

    try {
        return response->get<T>();
    } catch (const json::exception&) {
        return std::nullopt;
    }
}

} // namespace yams::extraction
