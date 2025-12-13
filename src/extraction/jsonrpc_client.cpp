/**
 * @file jsonrpc_client.cpp
 * @brief Implementation of JSON-RPC 2.0 client
 */

#include <spdlog/spdlog.h>
#include <thread>
#include <yams/extraction/jsonrpc_client.hpp>

namespace yams::extraction {

JsonRpcClient::JsonRpcClient(PluginProcess& process)
    : process_(process), next_id_(1), read_pos_(0) {}

std::optional<json> JsonRpcClient::call(std::string_view method, json params,
                                        std::chrono::milliseconds timeout) {
    std::lock_guard lock{mutex_};

    if (!process_.is_alive()) {
        spdlog::warn("JsonRpcClient: Process not alive, cannot call method '{}'", method);
        return std::nullopt;
    }

    int id = next_id();
    json request = build_request(id, method, params);
    std::string request_str = request.dump() + "\n";

    // Send request
    spdlog::debug("JsonRpcClient: Sending request id={} method='{}'", id, method);
    std::span<const std::byte> request_bytes{reinterpret_cast<const std::byte*>(request_str.data()),
                                             request_str.size()};

    size_t written = process_.write_stdin(request_bytes);
    if (written != request_str.size()) {
        spdlog::error("JsonRpcClient: Failed to write full request ({}/{} bytes)", written,
                      request_str.size());
        return std::nullopt;
    }

    // Wait for and read responses until we find the one with our ID
    // This handles the case where responses arrive out of order
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto response = read_next_response();

        if (!response) {
            // No complete response yet, wait a bit
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
            continue;
        }

        // Validate JSON-RPC version
        if (!response->contains("jsonrpc") || (*response)["jsonrpc"] != "2.0") {
            spdlog::error("JsonRpcClient: Invalid JSON-RPC version in response");
            return std::nullopt;
        }

        // Check if this is our response
        int responseId = response->value("id", -1);
        if (responseId == id) {
            spdlog::debug("JsonRpcClient: Got response for id={} method='{}'", id, method);

            // Check for error response
            if (response->contains("error")) {
                auto& error = (*response)["error"];
                spdlog::error("JsonRpcClient: RPC error for '{}': code={}, message={}", method,
                              error.value("code", -1), error.value("message", "unknown"));
                return std::nullopt;
            }

            // Extract result
            if (!response->contains("result")) {
                spdlog::error("JsonRpcClient: Response missing 'result' field");
                return std::nullopt;
            }

            return (*response)["result"];
        }

        // This response is for a different request ID - log and continue waiting
        // This can happen if a previous request timed out but the response arrived late
        spdlog::debug("JsonRpcClient: Discarding response for old id={} (expected {})", responseId,
                      id);
    }

    // Timeout
    spdlog::warn("JsonRpcClient: Timeout after {}ms waiting for response to '{}' (id={})",
                 timeout.count(), method, id);
    return std::nullopt;
}

void JsonRpcClient::notify(std::string_view method, json params) {
    if (!process_.is_alive()) {
        spdlog::warn("JsonRpcClient: Process not alive, cannot send notification '{}'", method);
        return;
    }

    json notification = build_notification(method, params);
    std::string notification_str = notification.dump() + "\n";

    spdlog::debug("JsonRpcClient: Sending notification: {}", notification_str);
    std::span<const std::byte> notification_bytes{
        reinterpret_cast<const std::byte*>(notification_str.data()), notification_str.size()};

    process_.write_stdin(notification_bytes);
}

std::optional<json> JsonRpcClient::read_next_response() {
    auto data = process_.read_stdout();

    if (data.empty()) {
        return std::nullopt;
    }

    // Look for a complete JSON line (terminated by newline)
    std::string_view buffer{reinterpret_cast<const char*>(data.data()), data.size()};
    size_t newline_pos = buffer.find('\n');

    if (newline_pos == std::string::npos) {
        // No complete line yet
        return std::nullopt;
    }

    // Extract the line and consume it from the buffer
    std::string line{buffer.substr(0, newline_pos)};
    process_.consume_stdout(newline_pos + 1); // +1 to include the newline

    if (line.empty()) {
        return std::nullopt;
    }

    try {
        return json::parse(line);
    } catch (const json::exception& e) {
        spdlog::error("JsonRpcClient: JSON parse error: {}", e.what());
        return std::nullopt;
    }
}

std::optional<json> JsonRpcClient::read_response(std::chrono::milliseconds timeout) {
    // Legacy method - now just wraps read_next_response with timeout
    auto start = std::chrono::steady_clock::now();

    while (std::chrono::steady_clock::now() - start < timeout) {
        auto response = read_next_response();
        if (response) {
            return response;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    return std::nullopt;
}

json JsonRpcClient::build_request(int id, std::string_view method, json params) {
    json request = {{"jsonrpc", "2.0"}, {"id", id}, {"method", method}};

    if (!params.is_null()) {
        request["params"] = params;
    }

    return request;
}

json JsonRpcClient::build_notification(std::string_view method, json params) {
    json notification = {{"jsonrpc", "2.0"}, {"method", method}};

    if (!params.is_null()) {
        notification["params"] = params;
    }

    return notification;
}

} // namespace yams::extraction
