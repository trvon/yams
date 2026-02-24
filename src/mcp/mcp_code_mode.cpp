#include <yams/mcp/mcp_server.h>

#include <boost/asio/awaitable.hpp>

#include <nlohmann/json.hpp>

#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace yams::mcp {

// ── Code Mode helpers ──

// Resolve $prev references in a params object using the previous step's result.
// Supports: $prev, $prev.field, $prev.field[N], $prev.field[N].subfield
json MCPServer::resolvePrevRefs(const json& params, const json& prev) {
    if (!params.is_object()) {
        return params;
    }

    json resolved = params;
    for (auto& [key, val] : resolved.items()) {
        if (!val.is_string())
            continue;
        auto s = val.get<std::string>();
        if (!s.starts_with("$prev"))
            continue;

        // Parse the path after "$prev"
        std::string_view path(s);
        path.remove_prefix(5); // strip "$prev"

        const json* current = &prev;

        while (!path.empty() && current != nullptr) {
            if (path.starts_with('.')) {
                path.remove_prefix(1);
                // Read field name until next . or [
                auto end = path.find_first_of(".[");
                auto field = std::string(path.substr(0, end));
                if (current->is_object() && current->contains(field)) {
                    current = &(*current)[field];
                } else {
                    current = nullptr;
                }
                path.remove_prefix(end == std::string_view::npos ? path.size() : end);
            } else if (path.starts_with('[')) {
                path.remove_prefix(1);
                auto close = path.find(']');
                if (close == std::string_view::npos) {
                    current = nullptr;
                    break;
                }
                auto idxStr = std::string(path.substr(0, close));
                path.remove_prefix(close + 1);
                try {
                    auto idx = std::stoull(idxStr);
                    if (current->is_array() && idx < current->size()) {
                        current = &(*current)[idx];
                    } else {
                        current = nullptr;
                    }
                } catch (...) {
                    current = nullptr;
                }
            } else {
                current = nullptr;
            }
        }

        if (current != nullptr) {
            resolved[key] = *current;
        } else {
            resolved[key] = nullptr;
        }
    }

    return resolved;
}

// Describe a single operation or all operations
json MCPServer::describeOp(const std::string& target) const {
    // Map op names to their schemas and descriptions
    struct OpInfo {
        std::string description;
        json paramsSchema;
        std::string category; // "query", "execute", "session"
        bool readOnly;
    };

    static const auto buildOpMap = []() {
        std::unordered_map<std::string, OpInfo> ops;

        ops["search"] = {
            "Search documents using hybrid search (vector + full-text + knowledge graph)",
            json{
                {"type", "object"},
                {"properties",
                 {{"query", {{"type", "string"}, {"description", "Search query"}}},
                  {"limit",
                   {{"type", "integer"}, {"description", "Maximum results"}, {"default", 10}}},
                  {"fuzzy",
                   {{"type", "boolean"},
                    {"description", "Enable fuzzy search"},
                    {"default", false}}},
                  {"similarity",
                   {{"type", "number"}, {"description", "Similarity threshold"}, {"default", 0.7}}},
                  {"type",
                   {{"type", "string"}, {"description", "Search type"}, {"default", "hybrid"}}},
                  {"paths_only",
                   {{"type", "boolean"}, {"description", "Return only paths"}, {"default", false}}},
                  {"path_pattern",
                   {{"type", "string"}, {"description", "Glob pattern to filter results"}}},
                  {"include_patterns",
                   {{"type", "array"},
                    {"items", {{"type", "string"}}},
                    {"description", "Multiple path patterns (OR logic)"}}},
                  {"tags",
                   {{"type", "array"},
                    {"items", {{"type", "string"}}},
                    {"description", "Filter by tags"}}},
                  {"match_all_tags",
                   {{"type", "boolean"},
                    {"description", "Require all tags to match (AND logic)"},
                    {"default", false}}},
                  {"cwd",
                   {{"type", "string"},
                    {"description", "Scope search to files under this directory"}}}}},
                {"required", json::array({"query"})}},
            "query", true};

        ops["grep"] = {
            "Search documents using regular expressions with grep-like functionality",
            json{
                {"type", "object"},
                {"properties",
                 {{"pattern", {{"type", "string"}, {"description", "Regex pattern"}}},
                  {"name", {{"type", "string"}, {"description", "File name or subpath to scope"}}},
                  {"paths",
                   {{"type", "array"},
                    {"items", {{"type", "string"}}},
                    {"description", "Paths to search"}}},
                  {"include_patterns",
                   {{"type", "array"},
                    {"items", {{"type", "string"}}},
                    {"description", "File include globs"}}},
                  {"ignore_case",
                   {{"type", "boolean"}, {"description", "Case insensitive"}, {"default", false}}},
                  {"line_numbers",
                   {{"type", "boolean"}, {"description", "Show line numbers"}, {"default", false}}},
                  {"context",
                   {{"type", "integer"}, {"description", "Context lines"}, {"default", 0}}},
                  {"cwd", {{"type", "string"}, {"description", "Scope grep to directory"}}}}},
                {"required", json::array({"pattern"})}},
            "query", true};

        ops["list"] = {
            "List documents with filtering by pattern, tags, type, or recency",
            json{{"type", "object"},
                 {"properties",
                  {{"pattern", {{"type", "string"}, {"description", "Name pattern filter"}}},
                   {"name", {{"type", "string"}, {"description", "Exact name filter"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Filter by tags"}}},
                   {"recent",
                    {{"type", "integer"}, {"description", "Show N most recent documents"}}},
                   {"paths_only",
                    {{"type", "boolean"},
                     {"description", "Output only file paths"},
                     {"default", false}}},
                   {"limit",
                    {{"type", "integer"}, {"description", "Maximum results"}, {"default", 100}}},
                   {"offset",
                    {{"type", "integer"}, {"description", "Pagination offset"}, {"default", 0}}}}}},
            "query", true};

        ops["get"] = {"Retrieve documents from storage by hash or name",
                      json{{"type", "object"},
                           {"properties",
                            {{"hash", {{"type", "string"}, {"description", "Document hash"}}},
                             {"name", {{"type", "string"}, {"description", "Document name"}}},
                             {"include_content",
                              {{"type", "boolean"},
                               {"description", "Include content in response"},
                               {"default", true}}}}}},
                      "query", true};

        ops["status"] = {"Get daemon status, readiness, and metrics",
                         json{{"type", "object"},
                              {"properties",
                               {{"detailed",
                                 {{"type", "boolean"},
                                  {"description", "Include verbose metrics"},
                                  {"default", false}}}}}},
                         "query", true};

        ops["list_collections"] = {"List available collections", json{{"type", "object"}}, "query",
                                   true};

        ops["list_snapshots"] = {
            "List available snapshots",
            json{{"type", "object"},
                 {"properties",
                  {{"collection", {{"type", "string"}, {"description", "Filter by collection"}}},
                   {"with_labels",
                    {{"type", "boolean"},
                     {"description", "Include snapshot labels"},
                     {"default", true}}}}}},
            "query", true};

        ops["graph"] = {
            "Query the knowledge graph for relationships and entities",
            json{{"type", "object"},
                 {"properties",
                  {{"action",
                    {{"type", "string"},
                     {"enum", json::array({"query", "ingest"})},
                     {"default", "query"}}},
                   {"hash", {{"type", "string"}, {"description", "Document hash"}}},
                   {"name", {{"type", "string"}, {"description", "Document name"}}},
                   {"node_key", {{"type", "string"}, {"description", "Direct node key lookup"}}},
                   {"list_types",
                    {{"type", "boolean"},
                     {"description", "List available node types"},
                     {"default", false}}},
                   {"relation", {{"type", "string"}, {"description", "Filter by relation type"}}},
                   {"depth",
                    {{"type", "integer"},
                     {"description", "BFS traversal depth (1-5)"},
                     {"default", 1}}},
                   {"limit",
                    {{"type", "integer"}, {"description", "Maximum results"}, {"default", 100}}}}}},
            "query", true};

        ops["add"] = {
            "Store documents with deduplication",
            json{{"type", "object"},
                 {"properties",
                  {{"path", {{"type", "string"}, {"description", "File or directory path"}}},
                   {"content", {{"type", "string"}, {"description", "Inline content"}}},
                   {"name", {{"type", "string"}, {"description", "Document name"}}},
                   {"recursive",
                    {{"type", "boolean"},
                     {"description", "Recursively add from directories"},
                     {"default", false}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Document tags"}}},
                   {"metadata",
                    {{"type", "object"}, {"description", "Metadata key/value pairs"}}}}}},
            "execute", false};

        ops["update"] = {
            "Update metadata/tags by hash, name, path, or pattern",
            json{{"type", "object"},
                 {"properties",
                  {{"hash", {{"type", "string"}, {"description", "Document hash"}}},
                   {"name", {{"type", "string"}, {"description", "Document name"}}},
                   {"pattern", {{"type", "string"}, {"description", "Glob-like pattern"}}},
                   {"metadata", {{"type", "object"}, {"description", "Metadata to update"}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Tags to add"}}},
                   {"remove_tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Tags to remove"}}},
                   {"dry_run",
                    {{"type", "boolean"},
                     {"description", "Preview changes only"},
                     {"default", false}}}}}},
            "execute", false};

        ops["delete"] = {
            "Delete documents by name, names array, or pattern",
            json{{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Single name to delete"}}},
                   {"names",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Multiple names to delete"}}},
                   {"pattern",
                    {{"type", "string"}, {"description", "Glob pattern for matching names"}}},
                   {"dry_run",
                    {{"type", "boolean"},
                     {"description", "Preview what would be deleted"},
                     {"default", false}}}}}},
            "execute", false};

        ops["restore"] = {
            "Restore documents from a collection or snapshot",
            json{{"type", "object"},
                 {"properties",
                  {{"collection", {{"type", "string"}, {"description", "Collection name"}}},
                   {"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID"}}},
                   {"output_directory", {{"type", "string"}, {"description", "Output directory"}}},
                   {"overwrite",
                    {{"type", "boolean"},
                     {"description", "Overwrite existing files"},
                     {"default", false}}},
                   {"dry_run",
                    {{"type", "boolean"},
                     {"description", "Preview without writing"},
                     {"default", false}}}}},
                 {"required", json::array({"output_directory"})}},
            "execute", false};

        ops["download"] = {
            "Download files from URLs and store in YAMS",
            json{{"type", "object"},
                 {"properties",
                  {{"url", {{"type", "string"}, {"description", "URL to download"}}},
                   {"post_index",
                    {{"type", "boolean"},
                     {"description", "Index after storing"},
                     {"default", true}}},
                   {"tags",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Tags for indexing"}}},
                   {"metadata", {{"type", "object"}, {"description", "Metadata for indexing"}}}}},
                 {"required", json::array({"url"})}},
            "execute", false};

        ops["session_start"] = {
            "Start (and optionally warm) a session",
            json{{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Session name"}}},
                   {"description", {{"type", "string"}, {"description", "Session description"}}},
                   {"warm", {{"type", "boolean"}, {"default", true}}}}}},
            "session", false};

        ops["session_stop"] = {
            "Stop session (clear materialized cache)",
            json{{"type", "object"},
                 {"properties",
                  {{"name", {{"type", "string"}, {"description", "Session name"}}},
                   {"clear", {{"type", "boolean"}, {"default", true}}}}}},
            "session", false};

        ops["session_pin"] = {
            "Pin documents by path pattern",
            json{{"type", "object"},
                 {"properties",
                  {{"path", {{"type", "string"}, {"description", "Path glob pattern to pin"}}}}},
                 {"required", json::array({"path"})}},
            "session", false};

        ops["session_unpin"] = {
            "Unpin documents by path pattern",
            json{{"type", "object"},
                 {"properties",
                  {{"path", {{"type", "string"}, {"description", "Path glob pattern to unpin"}}}}},
                 {"required", json::array({"path"})}},
            "session", false};

        ops["session_watch"] = {
            "Enable or disable auto-ingest for a project session",
            json{{"type", "object"},
                 {"properties",
                  {{"session", {{"type", "string"}, {"description", "Session name"}}},
                   {"root", {{"type", "string"}, {"description", "Project root to watch"}}},
                   {"enable",
                    {{"type", "boolean"},
                     {"description", "Enable watch (false disables)"},
                     {"default", true}}}}}},
            "session", false};

        return ops;
    };

    static const auto opMap = buildOpMap();

    if (auto it = opMap.find(target); it != opMap.end()) {
        return json{{"op", target},
                    {"description", it->second.description},
                    {"paramsSchema", it->second.paramsSchema},
                    {"category", it->second.category},
                    {"readOnly", it->second.readOnly}};
    }

    return json{{"error", "Unknown operation: " + target}, {"available", describeAllOps()}};
}

json MCPServer::describeAllOps() const {
    return json{
        {"query_ops", json::array({"search", "grep", "list", "list_collections", "list_snapshots",
                                   "graph", "get", "status", "describe"})},
        {"execute_ops", json::array({"add", "update", "delete", "restore", "download"})},
        {"session_ops", json::array({"start", "stop", "pin", "unpin", "watch"})}};
}

boost::asio::awaitable<json> MCPServer::handlePipelineQuery(const json& args) {
    try {
        if (!args.contains("steps") || !args["steps"].is_array()) {
            co_return wrapToolResultStructured(
                json::array({content::text("Error: 'steps' array is required")}), std::nullopt,
                true);
        }

        const auto& steps = args["steps"];
        if (steps.empty()) {
            co_return wrapToolResultStructured(
                json::array({content::text("Error: 'steps' array must not be empty")}),
                std::nullopt, true);
        }

        // Allowed read-only ops for the query tool
        static const std::unordered_set<std::string> allowedOps = {
            "search", "grep", "list",   "list_collections", "list_snapshots",
            "graph",  "get",  "status", "describe"};

        json prevResult = json::object();
        json allResults = json::array();
        bool hasError = false;

        for (size_t i = 0; i < steps.size(); ++i) {
            const auto& step = steps[i];
            if (!step.is_object() || !step.contains("op")) {
                co_return wrapToolResultStructured(
                    json::array({content::text("Error: step " + std::to_string(i) +
                                               " missing 'op' field")}),
                    std::nullopt, true);
            }

            auto op = step["op"].get<std::string>();

            // Handle describe specially (no dispatch needed)
            if (op == "describe") {
                auto params = step.value("params", json::object());
                auto target = params.value("target", std::string{});
                if (target.empty()) {
                    prevResult = describeAllOps();
                } else {
                    prevResult = describeOp(target);
                }
                allResults.push_back(json{{"stepIndex", i}, {"op", op}, {"result", prevResult}});
                continue;
            }

            if (allowedOps.find(op) == allowedOps.end()) {
                co_return wrapToolResultStructured(
                    json::array({content::text("Error: '" + op +
                                               "' is not a read operation. Use the 'execute' tool "
                                               "for write operations "
                                               "(add, update, delete, restore, download).")}),
                    std::nullopt, true);
            }

            // Resolve $prev references in params
            auto params = step.value("params", json::object());
            auto resolvedParams = resolvePrevRefs(params, prevResult);

            // Dispatch via the internal registry (all individual tools)
            auto toolResult = co_await internalRegistry_->callTool(op, resolvedParams);

            // Extract the structured data from the tool result for $prev chaining.
            // Tool results come back as { "content": [...], "structuredContent": {...} }
            // We want the data for chaining, not the MCP wrapper.
            if (toolResult.contains("structuredContent") &&
                toolResult["structuredContent"].contains("data")) {
                prevResult = toolResult["structuredContent"]["data"];
            } else if (toolResult.contains("content") && toolResult["content"].is_array() &&
                       !toolResult["content"].empty()) {
                // Try to parse the text content as JSON for chaining
                auto& firstContent = toolResult["content"][0];
                if (firstContent.contains("text")) {
                    try {
                        prevResult = json::parse(firstContent["text"].get<std::string>());
                    } catch (...) {
                        prevResult = json{{"text", firstContent["text"]}};
                    }
                }
            }

            bool isError = toolResult.value("isError", false);
            if (isError)
                hasError = true;

            allResults.push_back(
                json{{"stepIndex", i}, {"op", op}, {"result", prevResult}, {"isError", isError}});

            if (isError)
                break; // Stop pipeline on error
        }

        // For single-step pipelines, return the direct result
        json finalResult;
        if (steps.size() == 1) {
            finalResult = allResults[0]["result"];
        } else {
            finalResult = json{{"steps", allResults},
                               {"totalSteps", steps.size()},
                               {"completedSteps", allResults.size()}};
        }

        // Build summary text
        std::ostringstream summary;
        summary << "Pipeline: " << allResults.size() << "/" << steps.size() << " steps";
        if (hasError)
            summary << " (stopped on error)";

        // Prefer an actionable first-line error for clients that only surface error.message.
        std::optional<std::string> primaryError;
        if (hasError) {
            for (const auto& step : allResults) {
                if (!step.is_object() || !step.value("isError", false))
                    continue;
                if (!step.contains("result"))
                    continue;
                const auto& r = step["result"];
                if (r.is_string()) {
                    primaryError = r.get<std::string>();
                    break;
                }
                if (r.is_object()) {
                    if (r.contains("error") && r["error"].is_string()) {
                        primaryError = r["error"].get<std::string>();
                        break;
                    }
                    if (r.contains("text") && r["text"].is_string()) {
                        primaryError = r["text"].get<std::string>();
                        break;
                    }
                }
            }
        }

        // Some clients/tools ignore structuredContent. Provide a JSON text payload too.
        // Keep this bounded via limitToolResultDup_ to avoid duplicating large results.
        std::string firstLine = summary.str();
        if (primaryError.has_value() && !primaryError->empty()) {
            firstLine = "Error: " + *primaryError + "\n" + firstLine;
        }
        json contentItems = json::array({content::text(firstLine)});
        if (!limitToolResultDup_) {
            contentItems.push_back(content::json(finalResult, 2));
        }

        std::optional<json> structured;
        if (negotiatedProtocolVersion_ >= "2024-11-05") {
            structured = json{{"type", "tool_result"}, {"data", finalResult}};
        }

        co_return wrapToolResultStructured(contentItems, structured, hasError);
    } catch (const json::exception& e) {
        co_return wrapToolResultStructured(
            json::array({content::text(std::string("JSON error: ") + e.what())}), std::nullopt,
            true);
    } catch (const std::exception& e) {
        co_return wrapToolResultStructured(
            json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt, true);
    }
}

boost::asio::awaitable<json> MCPServer::handleBatchExecute(const json& args) {
    try {
        if (!args.contains("operations") || !args["operations"].is_array()) {
            co_return wrapToolResultStructured(
                json::array({content::text("Error: 'operations' array is required")}), std::nullopt,
                true);
        }

        const auto& operations = args["operations"];
        if (operations.empty()) {
            co_return wrapToolResultStructured(
                json::array({content::text("Error: 'operations' array must not be empty")}),
                std::nullopt, true);
        }

        bool continueOnError = args.value("continueOnError", false);

        // Allowed write ops
        // "delete" maps to the "delete_by_name" tool internally
        static const std::unordered_map<std::string, std::string> opToTool = {
            {"add", "add"},         {"update", "update"},     {"delete", "delete_by_name"},
            {"restore", "restore"}, {"download", "download"},
        };

        json results = json::array();
        size_t succeeded = 0;
        size_t failed = 0;

        for (size_t i = 0; i < operations.size(); ++i) {
            const auto& operation = operations[i];
            if (!operation.is_object() || !operation.contains("op")) {
                auto errResult = json{{"stepIndex", i},
                                      {"op", "unknown"},
                                      {"success", false},
                                      {"error", "Missing 'op' field"}};
                results.push_back(errResult);
                ++failed;
                if (!continueOnError)
                    break;
                continue;
            }

            auto op = operation["op"].get<std::string>();
            auto toolIt = opToTool.find(op);
            if (toolIt == opToTool.end()) {
                auto errResult =
                    json{{"stepIndex", i},
                         {"op", op},
                         {"success", false},
                         {"error", "'" + op +
                                       "' is not a write operation. Use the 'query' tool for "
                                       "read operations (search, grep, list, etc)."}};
                results.push_back(errResult);
                ++failed;
                if (!continueOnError)
                    break;
                continue;
            }

            auto params = operation.value("params", json::object());

            // Dispatch via the internal registry
            auto toolResult = co_await internalRegistry_->callTool(toolIt->second, params);

            bool isError = toolResult.value("isError", false);
            json stepResult = json{{"stepIndex", i}, {"op", op}, {"success", !isError}};

            // Extract data from tool result
            if (toolResult.contains("structuredContent") &&
                toolResult["structuredContent"].contains("data")) {
                stepResult["data"] = toolResult["structuredContent"]["data"];
            } else if (toolResult.contains("content") && toolResult["content"].is_array() &&
                       !toolResult["content"].empty()) {
                auto& firstContent = toolResult["content"][0];
                if (firstContent.contains("text")) {
                    try {
                        stepResult["data"] = json::parse(firstContent["text"].get<std::string>());
                    } catch (...) {
                        stepResult["data"] = json{{"text", firstContent["text"]}};
                    }
                }
            }

            if (isError) {
                ++failed;
                // Extract error message
                if (toolResult.contains("content") && toolResult["content"].is_array() &&
                    !toolResult["content"].empty()) {
                    auto& fc = toolResult["content"][0];
                    if (fc.contains("text")) {
                        stepResult["error"] = fc["text"];
                    }
                }
            } else {
                ++succeeded;
            }

            results.push_back(stepResult);

            if (isError && !continueOnError)
                break;
        }

        json finalResult = json{{"results", results},
                                {"totalOps", operations.size()},
                                {"succeeded", succeeded},
                                {"failed", failed}};

        std::ostringstream summary;
        summary << "Execute: " << succeeded << " succeeded, " << failed << " failed out of "
                << operations.size() << " operations";

        // Prefer an actionable first-line error for clients that only surface error.message.
        std::optional<std::string> primaryError;
        if (failed > 0) {
            for (const auto& step : results) {
                if (!step.is_object())
                    continue;
                if (step.value("success", true))
                    continue;
                if (step.contains("error") && step["error"].is_string()) {
                    primaryError = step["error"].get<std::string>();
                    break;
                }
            }
        }

        // Some clients/tools ignore structuredContent. Provide a JSON text payload too.
        std::string firstLine = summary.str();
        if (primaryError.has_value() && !primaryError->empty()) {
            firstLine = "Error: " + *primaryError + "\n" + firstLine;
        }
        json contentItems = json::array({content::text(firstLine)});
        if (!limitToolResultDup_) {
            contentItems.push_back(content::json(finalResult, 2));
        }

        // Heuristic hints: common confusion between add/download schemas.
        if (failed > 0) {
            bool sawAddWithUrl = false;
            bool sawAddWithTitleOrDesc = false;
            for (const auto& op : operations) {
                if (!op.is_object())
                    continue;
                const auto opName = op.value("op", std::string{});
                if (opName != "add")
                    continue;
                const auto params = op.value("params", json::object());
                if (!params.is_object())
                    continue;
                if (params.contains("url"))
                    sawAddWithUrl = true;
                if (params.contains("title") || params.contains("description"))
                    sawAddWithTitleOrDesc = true;
            }
            if (sawAddWithUrl || sawAddWithTitleOrDesc) {
                contentItems.push_back(content::text(
                    "Hint: execute op 'add' expects {path} or {content}+{name} (optional: "
                    "mime_type, tags, metadata, collection, snapshot_id). If you're trying to "
                    "fetch a URL, use op 'download' with {url} and optional {post_index:true}."));
            }
        }

        std::optional<json> structured;
        if (negotiatedProtocolVersion_ >= "2024-11-05") {
            structured = json{{"type", "tool_result"}, {"data", finalResult}};
        }

        co_return wrapToolResultStructured(contentItems, structured, failed > 0);
    } catch (const json::exception& e) {
        co_return wrapToolResultStructured(
            json::array({content::text(std::string("JSON error: ") + e.what())}), std::nullopt,
            true);
    } catch (const std::exception& e) {
        co_return wrapToolResultStructured(
            json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt, true);
    }
}

boost::asio::awaitable<json> MCPServer::handleSessionAction(const json& args) {
    try {
        if (!args.contains("action") || !args["action"].is_string()) {
            co_return wrapToolResultStructured(
                json::array({content::text("Error: 'action' string is required")}), std::nullopt,
                true);
        }

        auto action = args["action"].get<std::string>();
        auto params = args.value("params", json::object());

        // Map action names to tool names
        static const std::unordered_map<std::string, std::string> actionToTool = {
            {"start", "session_start"}, {"stop", "session_stop"}, {"pin", "session_pin"},
            {"unpin", "session_unpin"}, {"watch", "watch"},
        };

        auto toolIt = actionToTool.find(action);
        if (toolIt == actionToTool.end()) {
            co_return wrapToolResultStructured(
                json::array({content::text("Error: Unknown session action '" + action +
                                           "'. Valid actions: start, stop, pin, unpin, watch")}),
                std::nullopt, true);
        }

        // Dispatch via the internal registry
        co_return co_await internalRegistry_->callTool(toolIt->second, params);
    } catch (const json::exception& e) {
        co_return wrapToolResultStructured(
            json::array({content::text(std::string("JSON error: ") + e.what())}), std::nullopt,
            true);
    } catch (const std::exception& e) {
        co_return wrapToolResultStructured(
            json::array({content::text(std::string("Error: ") + e.what())}), std::nullopt, true);
    }
}

} // namespace yams::mcp
