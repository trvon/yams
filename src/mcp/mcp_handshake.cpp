#include <yams/mcp/mcp_server.h>

#include <spdlog/spdlog.h>

#include <algorithm>
#include <string>
#include <vector>

namespace yams::mcp {

json MCPServer::initialize(const json& params) {
    static const std::vector<std::string> kSupported = {"2025-11-25", "2025-06-18", "2025-03-26",
                                                        "2024-11-05", "2024-10-07"};
    const std::string latest = "2025-11-25";

    // Extract requested version (optional)
    std::string requested = latest;
    if (params.contains("protocolVersion") && params["protocolVersion"].is_string()) {
        requested = params["protocolVersion"].get<std::string>();
    }
    spdlog::debug("MCP client requested protocol version: {}", requested);

    // Negotiate (fallback to latest if unsupported)
    std::string negotiated = latest;
    bool matched = std::find(kSupported.begin(), kSupported.end(), requested) != kSupported.end();
    if (matched) {
        negotiated = requested;
    } else if (strictProtocol_) {
        json error_data = {{"supportedVersions", kSupported}};
        return {{"_initialize_error", true},
                {"code", kErrUnsupportedProtocolVersion},
                {"message", "Unsupported protocol version requested by client"},
                {"data", error_data}};
    }

    // Capture client info if present (tolerant)
    if (params.contains("clientInfo") && params["clientInfo"].is_object()) {
        clientInfo_.name = params["clientInfo"].value("name", "unknown");
        clientInfo_.version = params["clientInfo"].value("version", "unknown");
    } else {
        clientInfo_.name = "unknown";
        clientInfo_.version = "unknown";
    }

    negotiatedProtocolVersion_ = negotiated;

    // --- MCP Apps Extension Capability Detection ---
    // Check if client supports MCP Apps (io.modelcontextprotocol/ui extension)
    mcpAppsSupported_.store(false);
    mcpAppsMimeType_.clear();
    if (params.contains("capabilities") && params["capabilities"].is_object()) {
        const auto& caps = params["capabilities"];
        if (caps.contains("extensions") && caps["extensions"].is_object()) {
            const auto& extensions = caps["extensions"];
            if (extensions.contains("io.modelcontextprotocol/ui") &&
                extensions["io.modelcontextprotocol/ui"].is_object()) {
                const auto& uiExt = extensions["io.modelcontextprotocol/ui"];
                if (uiExt.contains("mimeTypes") && uiExt["mimeTypes"].is_array()) {
                    // Check if client supports our mime type
                    for (const auto& mimeType : uiExt["mimeTypes"]) {
                        if (mimeType.is_string() &&
                            mimeType.get<std::string>() == "text/html;profile=mcp-app") {
                            mcpAppsSupported_.store(true);
                            mcpAppsMimeType_ = "text/html;profile=mcp-app";
                            spdlog::info("MCP Apps extension supported by client (mimeType: {})",
                                         mcpAppsMimeType_);
                            break;
                        }
                    }
                }
            }
        }
    }

    // Always build server capabilities (do NOT rely on client-supplied capabilities)
    json caps = buildServerCapabilities();

    json result = {{"protocolVersion", negotiated},
                   {"serverInfo", {{"name", serverInfo_.name}, {"version", serverInfo_.version}}},
                   {"capabilities", caps},
                   {"_meta", {{"instanceId", instanceId_}}}};

    // Debug logging to diagnose empty result issues
    spdlog::debug("MCP initialize() result built:");
    spdlog::debug("  - protocolVersion: {}", negotiated);
    spdlog::debug("  - serverInfo.name: {}", serverInfo_.name);
    spdlog::debug("  - serverInfo.version: {}", serverInfo_.version);
    spdlog::debug("  - capabilities size: {}", caps.size());
    spdlog::debug("  - result is_null: {}, is_object: {}, empty: {}, size: {}", result.is_null(),
                  result.is_object(), result.empty(), result.size());

    return result;
}

} // namespace yams::mcp
