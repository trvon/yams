#include <yams/mcp/mcp_server.h>

#include <spdlog/spdlog.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace yams::mcp {

thread_local std::string MCPServer::tlsSessionId_;
thread_local nlohmann::json MCPServer::tlsProgressToken_ = nullptr;

void MCPServer::registerCancelable(const nlohmann::json& id) {
    if (id.is_null())
        return;
    std::lock_guard<std::mutex> lk(cancelMutex_);
    std::string key = id.is_string() ? id.get<std::string>() : id.dump();
    if (cancelTokens_.find(key) == cancelTokens_.end()) {
        cancelTokens_[key] = std::make_shared<std::atomic<bool>>(false);
    }
}

void MCPServer::cancelRequest(const nlohmann::json& id) {
    if (id.is_null())
        return;
    std::lock_guard<std::mutex> lk(cancelMutex_);
    std::string key = id.is_string() ? id.get<std::string>() : id.dump();
    auto it = cancelTokens_.find(key);
    if (it != cancelTokens_.end()) {
        it->second->store(true);
    }
}

bool MCPServer::isCanceled(const nlohmann::json& id) const {
    if (id.is_null())
        return false;
    std::lock_guard<std::mutex> lk(cancelMutex_);
    std::string key = id.is_string() ? id.get<std::string>() : id.dump();
    auto it = cancelTokens_.find(key);
    if (it == cancelTokens_.end())
        return false;
    return it->second->load();
}

void MCPServer::handleCancelRequest(const nlohmann::json& params,
                                    [[maybe_unused]] const nlohmann::json& id) {
    // Expect params: { "id": <original request id> } but also accept { "requestId": ... }
    nlohmann::json target;
    if (params.contains("id")) {
        target = params["id"];
    } else if (params.contains("requestId")) {
        target = params["requestId"];
    } else {
        spdlog::warn("cancel: missing id/requestId field");
        return;
    }
    cancelRequest(target);
    spdlog::info("Cancel requested for original id '{}'",
                 target.is_string() ? target.get<std::string>() : target.dump());
    // Optionally emit a progress notification indicating cancellation acknowledged
    sendProgress("cancel", 100.0, "Cancellation acknowledged");
}

void MCPServer::sendProgress(const std::string& /*phase*/, double percent,
                             const std::string& message,
                             std::optional<nlohmann::json> progressToken) {
    // Per MCP spec, notifications/progress MUST include a progressToken from the request's
    // params._meta.progressToken. If absent, do not emit a progress notification.
    nlohmann::json token = nullptr;
    if (progressToken)
        token = *progressToken;
    else if (!MCPServer::tlsProgressToken_.is_null())
        token = MCPServer::tlsProgressToken_;

    if (token.is_null()) {
        return; // No valid token available; skip
    }

    double clamped = percent;
    if (clamped < 0.0)
        clamped = 0.0;
    if (clamped > 100.0)
        clamped = 100.0;

    json p = {{"progressToken", token}, {"progress", clamped}, {"total", 100.0}};
    if (!message.empty())
        p["message"] = message;
    sendResponse({{"jsonrpc", "2.0"}, {"method", "notifications/progress"}, {"params", p}});
}

void MCPServer::beginSessionContext(
    std::string sessionId,
    std::function<void(const std::string&, const nlohmann::json&)> publisher) {
    tlsSessionId_ = std::move(sessionId);
    httpPublisher_ = std::move(publisher);
}

void MCPServer::endSessionContext() {
    tlsSessionId_.clear();
    httpPublisher_ = nullptr;
}

void MCPServer::notifyToolsListChanged() {
    // Send notifications/tools/list_changed per MCP spec
    // This is a server-initiated notification (no params needed)
    json notification = {{"jsonrpc", "2.0"},
                         {"method", "notifications/tools/list_changed"},
                         {"params", json::object()}};
    sendResponse(notification);
    spdlog::debug("Sent notifications/tools/list_changed");
}

} // namespace yams::mcp
