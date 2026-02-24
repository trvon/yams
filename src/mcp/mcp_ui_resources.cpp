#include <yams/mcp/mcp_server.h>

#include <nlohmann/json.hpp>

#include <mutex>
#include <string>
#include <unordered_map>

namespace yams::mcp {

bool MCPServer::isUiResourceUri(const std::string& uri) const {
    return uri.rfind("ui://", 0) == 0;
}

void MCPServer::ensureUiResourcesInitialized() {
    std::call_once(uiResourcesInitOnce_, [this]() {
        uiResources_.clear();

        auto add = [&](std::string uri, std::string name, std::string desc, std::string html) {
            UIResource r;
            r.uri = std::move(uri);
            r.name = std::move(name);
            r.description = std::move(desc);
            r.mimeType = mcpAppsMimeType_.empty() ? "text/html;profile=mcp-app" : mcpAppsMimeType_;
            r.htmlContent = std::move(html);
            r.meta = json::object();
            uiResources_.emplace(r.uri, std::move(r));
        };

        // Minimal placeholder UI templates (hosts render in sandboxed iframe).
        // These are intentionally small; Phase 2+ will provide richer apps.
        add("ui://yams/dashboard", "YAMS Dashboard", "YAMS overview dashboard",
            "<!doctype html><html><head><meta charset=\"utf-8\" /><title>YAMS Dashboard</title>"
            "</head><body><h1>YAMS</h1><p>UI is enabled. This is a placeholder dashboard.</p>"
            "</body></html>");

        add("ui://yams/live-graph", "Live Graph Watcher",
            "Real-time view of the CWD knowledge graph building",
            "<!doctype html><html><head><meta charset=\"utf-8\" />"
            "<title>Live Graph Watcher</title></head><body>"
            "<h1>Live Graph Watcher</h1>"
            "<p>Placeholder UI. Intended to visualize nodes/edges as they appear while "
            "indexing.</p>"
            "</body></html>");

        add("ui://yams/blackboard", "Blackboard Task Viewer",
            "View tasks/findings from opencode-blackboard and vscode-blackboard",
            "<!doctype html><html><head><meta charset=\"utf-8\" />"
            "<title>Blackboard</title></head><body>"
            "<h1>Blackboard</h1>"
            "<p>Placeholder UI. Intended to show tasks/findings and dependency graphs.</p>"
            "</body></html>");
    });
}

json MCPServer::uiResourcesAsMcpResources() {
    ensureUiResourcesInitialized();
    json arr = json::array();
    for (const auto& [uri, r] : uiResources_) {
        json item;
        item["uri"] = r.uri;
        item["name"] = r.name;
        if (!r.description.empty()) {
            item["description"] = r.description;
        }
        item["mimeType"] = r.mimeType;
        if (r.meta.is_object() && !r.meta.empty()) {
            item["_meta"] = r.meta;
        }
        arr.push_back(std::move(item));
    }
    return arr;
}

json MCPServer::readUiResource(const std::string& uri) {
    ensureUiResourcesInitialized();
    auto it = uiResources_.find(uri);
    if (it == uiResources_.end()) {
        throw std::runtime_error("Unknown UI resource URI: " + uri);
    }

    const auto& r = it->second;
    json content;
    content["uri"] = r.uri;
    content["mimeType"] = r.mimeType;
    content["text"] = r.htmlContent;
    if (r.meta.is_object() && !r.meta.empty()) {
        content["_meta"] = r.meta;
    }
    return json{{"contents", json::array({std::move(content)})}};
}

} // namespace yams::mcp
