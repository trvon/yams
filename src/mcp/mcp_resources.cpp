#include <yams/mcp/mcp_server.h>

#if !defined(YAMS_WASI)
#include <yams/metadata/query_helpers.h>
#endif

#include <chrono>
#include <stdexcept>
#include <string>

namespace yams::mcp {

json MCPServer::listResources() {
#if defined(YAMS_WASI)
    // WASI profile: no file-backed resources.
    return json{{"resources", json::array()}};
#else
    json resources = json::array();

    // MCP Apps: include UI resources only when negotiated.
    if (mcpAppsSupported_.load()) {
        json ui = uiResourcesAsMcpResources();
        if (ui.is_array()) {
            for (auto& item : ui) {
                resources.push_back(std::move(item));
            }
        }
    }

    // Add a resource for the YAMS storage statistics
    resources.push_back({{"uri", "yams://stats"},
                         {"name", "Storage Statistics"},
                         {"description", "Current YAMS storage statistics and health status"},
                         {"mimeType", "application/json"}});

    // Daemon status (symmetry with stats)
    resources.push_back({{"uri", "yams://status"},
                         {"name", "Daemon Status"},
                         {"description", "YAMS daemon status and readiness metrics"},
                         {"mimeType", "application/json"}});

    // Add a resource for recent documents
    resources.push_back({{"uri", "yams://recent"},
                         {"name", "Recent Documents"},
                         {"description", "Recently added documents in YAMS storage"},
                         {"mimeType", "application/json"}});

    return {{"resources", resources}};
#endif
}

json MCPServer::readResource(const std::string& uri) {
#if defined(YAMS_WASI)
    (void)uri;
    throw std::runtime_error("Resources not supported in WASI build");
#else
    if (mcpAppsSupported_.load() && isUiResourceUri(uri)) {
        return readUiResource(uri);
    }

    if (uri == "yams://stats") {
        // Get storage statistics
        if (!store_) {
            return {{"contents",
                     {{{{"uri", uri},
                        {"mimeType", "application/json"},
                        {"text", json({{"error", "Storage not initialized"}}).dump()}}}}}};
        }
        auto stats = store_->getStats();
        auto health = store_->checkHealth();

        return {{"contents",
                 {{{{"uri", uri},
                    {"mimeType", "application/json"},
                    {"text", json({{"storage",
                                    {{"totalObjects", stats.totalObjects},
                                     {"totalBytes", stats.totalBytes},
                                     {"uniqueBlocks", stats.uniqueBlocks},
                                     {"deduplicatedBytes", stats.deduplicatedBytes}}},
                                   {"health",
                                    {{"isHealthy", health.isHealthy},
                                     {"status", health.status},
                                     {"warnings", health.warnings},
                                     {"errors", health.errors}}}})
                                 .dump()}}}}}};
    } else if (uri == "yams://status") {
        try {
            auto ensure = ensureDaemonClient();
            if (!ensure) {
                return {{"contents",
                         {{{{"uri", uri},
                            {"mimeType", "application/json"},
                            {"text", json({{"error", std::string("status error: ") +
                                                        ensure.error().message}})
                                         .dump()}}}}}};
            }
            auto st = yams::cli::run_result(daemon_client_->status(), std::chrono::seconds(3));
            if (!st) {
                return {{"contents",
                         {{{{"uri", uri},
                            {"mimeType", "application/json"},
                            {"text", json({{"error", std::string("status error: ") +
                                                        st.error().message}})
                                         .dump()}}}}}};
            }
            const auto& s = st.value();
            json j;
            j["running"] = s.running;
            j["ready"] = s.ready;
            j["uptimeSeconds"] = s.uptimeSeconds;
            j["requestsProcessed"] = s.requestsProcessed;
            j["activeConnections"] = s.activeConnections;
            j["memoryUsageMb"] = s.memoryUsageMb;
            j["cpuUsagePercent"] = s.cpuUsagePercent;
            j["version"] = s.version;
            j["overallStatus"] = s.overallStatus;
            j["lifecycleState"] = s.lifecycleState;
            j["lastError"] = s.lastError;
            j["readinessStates"] = s.readinessStates;
            j["initProgress"] = s.initProgress;
            j["counters"] = s.requestCounts;
            // MCP worker counters - thread pool removed, always 0
            j["counters"]["mcp_worker_threads"] = 0;
            j["counters"]["mcp_worker_active"] = false;
            j["counters"]["mcp_worker_queued"] = 0;
            j["counters"]["mcp_worker_processed"] = 0;
            j["counters"]["mcp_worker_failed"] = 0;
            return {{"contents", {{{{"uri", uri}, {"mimeType", "application/json"},
                                   {"text", j.dump()}}}}}};
        } catch (...) {
            return {{"contents",
                     {{{{"uri", uri},
                        {"mimeType", "application/json"},
                        {"text", json({{"error", "status exception"}}).dump()}}}}}};
        }
    } else if (uri == "yams://recent") {
        // Get recent documents
        if (!metadataRepo_) {
            return {{"contents",
                     {{{{"uri", uri},
                        {"mimeType", "application/json"},
                        {"text", json({{"error", "Metadata repository not initialized"}})
                                     .dump()}}}}}};
        }
        auto docsResult = metadata::queryDocumentsByPattern(*metadataRepo_, "%");
        if (!docsResult) {
            return {{"contents", {{"text", "Failed to list documents"}}}};
        }
        auto docs = docsResult.value();
        // Limit to 20 most recent
        if (docs.size() > 20) {
            docs.resize(20);
        }

        json docList = json::array();
        for (const auto& doc : docs) {
            docList.push_back(
                {{"hash", doc.sha256Hash}, {"name", doc.fileName}, {"size", doc.fileSize},
                 {"mimeType", doc.mimeType}});
        }

        return {{"contents",
                 {{{{"uri", uri},
                    {"mimeType", "application/json"},
                    {"text", json({{"documents", docList}}).dump()}}}}}};
    } else {
        throw std::runtime_error("Unknown resource URI: " + uri);
    }
#endif
}

} // namespace yams::mcp
