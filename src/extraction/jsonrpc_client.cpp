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
    if (!process_.is_alive()) {
        spdlog::warn("JsonRpcClient: Process not alive, cannot call method '{}'", method);
        return std::nullopt;
    }

    int id = next_id();
    json request = build_request(id, method, params);
    std::string request_str = request.dump() + "\n";

    // Send request
    spdlog::info("JsonRpcClient: Sending request to method '{}': {}", method, request_str);
    std::span<const std::byte> request_bytes{reinterpret_cast<const std::byte*>(request_str.data()),
                                             request_str.size()};

    size_t written = process_.write_stdin(request_bytes);
    spdlog::info("JsonRpcClient: Wrote {} of {} bytes to stdin", written, request_str.size());
    if (written != request_str.size()) {
        spdlog::error("JsonRpcClient: Failed to write full request ({}/{} bytes)", written,
                      request_str.size());
        return std::nullopt;
    }

    // Give plugin a moment to process
    spdlog::info("JsonRpcClient: Waiting 100ms for plugin to process request");
    std::this_thread::sleep_for(std::chrono::milliseconds{100});

    // Read response
    spdlog::info("JsonRpcClient: Reading response with timeout {}ms", timeout.count());
    auto response = read_response(timeout);
    if (!response) {
        spdlog::warn("JsonRpcClient: Timeout waiting for response to '{}'", method);
        return std::nullopt;
    }

    spdlog::info("JsonRpcClient: Received response: {}", response->dump());

    // Validate response
    if (!response->contains("jsonrpc") || (*response)["jsonrpc"] != "2.0") {
        spdlog::error("JsonRpcClient: Invalid JSON-RPC version in response");
        return std::nullopt;
    }

    if (!response->contains("id") || (*response)["id"] != id) {
        spdlog::error("JsonRpcClient: Response ID mismatch (expected {}, got {})", id,
                      response->value("id", -1));
        return std::nullopt;
    }

    // Check for error response
    if (response->contains("error")) {
        auto& error = (*response)["error"];
        spdlog::error("JsonRpcClient: RPC error: code={}, message={}", error.value("code", -1),
                      error.value("message", "unknown"));
        return std::nullopt;
    }

    // Extract result
    if (!response->contains("result")) {
        spdlog::error("JsonRpcClient: Response missing 'result' field");
        return std::nullopt;
    }

    return (*response)["result"];
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

std::optional<json> JsonRpcClient::read_response(std::chrono::milliseconds timeout) {
    auto start = std::chrono::steady_clock::now();
    int poll_count = 0;

    while (std::chrono::steady_clock::now() - start < timeout) {
        std::lock_guard lock{mutex_};
        auto data = process_.read_stdout();

        if (poll_count % 100 == 0) { // Log every ~1 second
            spdlog::info("JsonRpcClient: poll #{}, buffer_size={}, read_pos={}", poll_count,
                         data.size(), read_pos_);
        }
        poll_count++;

        if (data.size() > read_pos_) {
            // We have new data
            std::string_view remaining{reinterpret_cast<const char*>(data.data() + read_pos_),
                                       data.size() - read_pos_};
            spdlog::info("JsonRpcClient: Got {} new bytes", remaining.size());

            // Look for newline (NDJSON framing)
            size_t newline_pos = remaining.find('\n');
            if (newline_pos != std::string::npos) {
                std::string line{remaining.substr(0, newline_pos)};
                read_pos_ += newline_pos + 1;

                if (!line.empty()) {
                    try {
                        return json::parse(line);
                    } catch (const json::exception& e) {
                        spdlog::error("JsonRpcClient: JSON parse error: {}", e.what());
                        return std::nullopt;
                    }
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }

    // Timeout
    std::lock_guard lock{mutex_};
    auto data = process_.read_stdout();
    spdlog::warn("JsonRpcClient: read_response timeout after {}ms. Buffer size: {}, read_pos: {}",
                 timeout.count(), data.size(), read_pos_);

    // Dump whatever we have in the buffer for debugging
    if (data.size() > 0) {
        std::string_view content{reinterpret_cast<const char*>(data.data()),
                                 std::min<size_t>(data.size(), 500)};
        spdlog::warn("JsonRpcClient: Buffer content (first 500 bytes): '{}'", content);
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
