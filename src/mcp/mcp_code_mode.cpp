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

namespace {

json extractCompositeStepParams(const json& step) {
    json params = step.value("params", json::object());
    if (!params.is_object()) {
        params = json::object();
    }

    if (!step.is_object()) {
        return params;
    }

    for (const auto& [key, value] : step.items()) {
        if (key == "op" || key == "params") {
            continue;
        }
        if (!params.contains(key)) {
            params[key] = value;
        }
    }

    return params;
}

json extractToolResultData(json&& toolResult) {
    if (toolResult.contains("structuredContent") &&
        toolResult["structuredContent"].contains("data")) {
        return std::move(toolResult["structuredContent"]["data"]);
    }
    if (toolResult.contains("content") && toolResult["content"].is_array() &&
        !toolResult["content"].empty()) {
        auto& firstContent = toolResult["content"][0];
        if (firstContent.contains("text")) {
            try {
                if (firstContent["text"].is_string()) {
                    return json::parse(firstContent["text"].get_ref<const std::string&>());
                }
            } catch (...) {
                return json{{"text", firstContent["text"]}};
            }
            return json{{"text", firstContent["text"]}};
        }
    }
    return json::object();
}

std::optional<std::string> extractToolPrimaryError(const json& toolResult) {
    if (!toolResult.value("isError", false)) {
        return std::nullopt;
    }
    if (toolResult.contains("content") && toolResult["content"].is_array() &&
        !toolResult["content"].empty()) {
        const auto& first = toolResult["content"][0];
        if (first.contains("text") && first["text"].is_string()) {
            return first["text"].get<std::string>();
        }
    }
    return std::nullopt;
}

bool hasNonEmptyStringField(const json& object, std::string_view key) {
    auto it = object.find(std::string(key));
    return it != object.end() && it->is_string() && !it->get_ref<const std::string&>().empty();
}

bool hasGraphNodeIdField(const json& object) {
    auto it = object.find("node_id");
    if (it == object.end() || it->is_null()) {
        return false;
    }
    if (it->is_number_integer() || it->is_number_unsigned()) {
        return true;
    }
    return it->is_string() && !it->get_ref<const std::string&>().empty();
}

std::optional<std::string> normalizeQueryParamsForOp(const std::string& op, json& params) {
    if (!params.is_object()) {
        params = json::object();
    }

    if (op == "search") {
        if (!hasNonEmptyStringField(params, "query") && hasNonEmptyStringField(params, "q")) {
            params["query"] = params["q"];
        }
        if (!params.contains("paths_only") && params.contains("pathsOnly")) {
            params["paths_only"] = params["pathsOnly"];
        }
        if (!hasNonEmptyStringField(params, "path_pattern") &&
            hasNonEmptyStringField(params, "pathPattern")) {
            params["path_pattern"] = params["pathPattern"];
        }

        if (!hasNonEmptyStringField(params, "query") && !hasNonEmptyStringField(params, "hash")) {
            return "Provide 'query' for search text, or 'hash' for a direct hash lookup. If "
                   "you used 'q', rename it to 'query' or place it under step.params.";
        }
    }

    if (op == "get") {
        if (!hasNonEmptyStringField(params, "name") && hasNonEmptyStringField(params, "path")) {
            params["name"] = params["path"];
        }
        if (!params.contains("include_content") && params.contains("includeContent")) {
            params["include_content"] = params["includeContent"];
        }

        if (!hasNonEmptyStringField(params, "hash") && !hasNonEmptyStringField(params, "name")) {
            if (hasNonEmptyStringField(params, "query")) {
                return "Provide 'hash' or 'name' for get. If you want text lookup, use op "
                       "'search' with 'query'.";
            }
            if (hasNonEmptyStringField(params, "id")) {
                return "Provide 'hash' or 'name' for get. If this identifier is a document "
                       "hash, pass it as 'hash'.";
            }
            return "Provide 'hash' or 'name' for get. Use 'name' for a stored path/name "
                   "lookup.";
        }
    }

    if (op == "graph") {
        if (!hasNonEmptyStringField(params, "node_key") &&
            hasNonEmptyStringField(params, "nodeKey")) {
            params["node_key"] = params["nodeKey"];
        }
        if (!params.contains("node_id") && params.contains("nodeId")) {
            params["node_id"] = params["nodeId"];
        }
        if (!params.contains("list_types") && params.contains("listTypes")) {
            params["list_types"] = params["listTypes"];
        }

        const std::string action = params.value("action", std::string{"query"});
        if (action != "ingest") {
            const bool hasTarget =
                hasNonEmptyStringField(params, "hash") || hasNonEmptyStringField(params, "name") ||
                hasNonEmptyStringField(params, "node_key") || hasGraphNodeIdField(params);
            const bool listTypes = params.value("list_types", false);
            const bool listByType = hasNonEmptyStringField(params, "list_type");
            if (!hasTarget && !listTypes && !listByType) {
                return "Provide one of 'hash', 'name', 'node_key', or 'node_id' for graph "
                       "queries, or set 'list_types': true.";
            }
        }
    }

    return std::nullopt;
}

std::string normalizeCompositePrimaryError(std::string error) {
    constexpr std::string_view kPrefix = "Error: ";
    while (error.starts_with(kPrefix)) {
        error.erase(0, kPrefix.size());
    }
    return error;
}

std::string buildCompositeFirstLine(std::string summary,
                                    const std::optional<std::string>& primaryError) {
    if (!primaryError.has_value() || primaryError->empty()) {
        return summary;
    }

    std::string normalized = normalizeCompositePrimaryError(*primaryError);
    if (normalized.empty()) {
        return summary;
    }
    return normalized + "\n" + summary;
}

} // namespace

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
            "Search documents using hybrid search (vector + full-text + knowledge graph). "
            "Provide 'query' for text lookup or 'hash' for a direct hash lookup.",
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

        ops["get"] = {"Retrieve documents from storage by 'hash' or 'name'. Use 'search' with "
                      "'query' for text lookup.",
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

        ops["suggest_context"] = {
            "Suggest relevant memory snapshots for a draft topic or query",
            json{{"type", "object"},
                 {"properties",
                  {{"query", {{"type", "string"}, {"description", "Draft topic or query"}}},
                   {"limit",
                    {{"type", "integer"},
                     {"description", "Maximum snapshot suggestions"},
                     {"default", 5}}},
                   {"use_session",
                    {{"type", "boolean"},
                     {"description", "Scope to the current or provided session"},
                     {"default", false}}},
                   {"session",
                    {{"type", "string"},
                     {"description", "Specific session name when use_session is enabled"}}},
                   {"global_search",
                    {{"type", "boolean"},
                     {"description", "Bypass session isolation when session scoping is used"},
                     {"default", false}}}}},
                 {"required", json::array({"query"})}},
            "query", true};

        ops["semantic_dedupe"] = {
            "Inspect persisted semantic duplicate groups and members",
            json{{"type", "object"},
                 {"properties",
                  {{"group_key",
                    {{"type", "string"}, {"description", "Specific semantic duplicate group"}}},
                   {"document_ids",
                    {{"type", "array"},
                     {"items", {{"type", "integer"}}},
                     {"description", "Filter groups by member document ids"}}},
                   {"limit",
                    {{"type", "integer"},
                     {"description", "Maximum groups to return"},
                     {"default", 25}}}}}},
            "query", true};

        ops["graph"] = {
            "Query the knowledge graph for relationships and entities. Provide one of 'hash', "
            "'name', 'node_key', or 'node_id', or set 'list_types' to true.",
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
            "Store one file or one directory per operation, or inline content with a name. "
            "Use 'path' for files or directories, set 'recursive' when ingesting a directory, "
            "and use multiple add operations instead of a 'paths' array.",
            json{{"type", "object"},
                 {"properties",
                  {{"path",
                    {{"type", "string"},
                     {"description", "Single file or directory path for this add operation"}}},
                   {"content", {{"type", "string"}, {"description", "Inline document content"}}},
                   {"name",
                    {{"type", "string"},
                     {"description", "Document name when sending inline content"}}},
                   {"mime_type", {{"type", "string"}, {"description", "MIME type override"}}},
                   {"disable_auto_mime",
                    {{"type", "boolean"}, {"description", "Disable automatic MIME detection"}}},
                   {"no_embeddings",
                    {{"type", "boolean"},
                     {"description", "Disable automatic embedding generation"},
                     {"default", false}}},
                   {"collection", {{"type", "string"}, {"description", "Collection name"}}},
                   {"snapshot_id", {{"type", "string"}, {"description", "Snapshot ID"}}},
                   {"snapshot_label", {{"type", "string"}, {"description", "Snapshot label"}}},
                   {"recursive",
                    {{"type", "boolean"},
                     {"description", "Recursively add files from a directory path"},
                     {"default", false}}},
                   {"include",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Include patterns for recursive directory adds"}}},
                   {"exclude",
                    {{"type", "array"},
                     {"items", {{"type", "string"}}},
                     {"description", "Exclude patterns for recursive directory adds"}}},
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

        ops["download_status"] = {
            "Get daemon-managed download job status",
            json{{"type", "object"},
                 {"properties",
                  {{"job_id", {{"type", "string"}, {"description", "Download job id"}}}}},
                 {"required", json::array({"job_id"})}},
            "query", true};

        ops["download_list_jobs"] = {"List daemon-managed download jobs", json{{"type", "object"}},
                                     "query", true};

        ops["download_cancel"] = {
            "Cancel daemon-managed download job",
            json{{"type", "object"},
                 {"properties",
                  {{"job_id", {{"type", "string"}, {"description", "Download job id"}}}}},
                 {"required", json::array({"job_id"})}},
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
    return json{{"query_ops", json::array({"search", "grep", "list", "list_collections",
                                           "list_snapshots", "suggest_context", "semantic_dedupe",
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
            "search",           "grep",           "list",
            "list_collections", "list_snapshots", "suggest_context",
            "semantic_dedupe",  "graph",          "get",
            "status",           "describe"};

        struct QueryDispatchResult {
            json result = json::object();
            bool isError = false;
            std::optional<std::string> primaryError;
        };

        auto dispatchQueryOp = [this](const std::string& op,
                                      json params) -> boost::asio::awaitable<QueryDispatchResult> {
            if (op == "describe") {
                const auto target = params.value("target", std::string{});
                co_return QueryDispatchResult{.result = target.empty() ? describeAllOps()
                                                                       : describeOp(target)};
            }

            if (auto routingError = normalizeQueryParamsForOp(op, params)) {
                co_return QueryDispatchResult{.result = json{{"error", *routingError}},
                                              .isError = true,
                                              .primaryError = *routingError};
            }

            auto fail = [](std::string message) {
                return QueryDispatchResult{.result = json{{"error", message}},
                                           .isError = true,
                                           .primaryError = std::move(message)};
            };

            if (op == "search") {
                auto req = MCPSearchRequest::fromJson(params);
                auto result = co_await handleSearchDocuments(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "grep") {
                auto req = MCPGrepRequest::fromJson(params);
                auto result = co_await handleGrepDocuments(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "list") {
                auto req = MCPListDocumentsRequest::fromJson(params);
                auto result = co_await handleListDocuments(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "list_collections") {
                auto req = MCPListCollectionsRequest::fromJson(params);
                auto result = co_await handleListCollections(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "list_snapshots") {
                auto req = MCPListSnapshotsRequest::fromJson(params);
                auto result = co_await handleListSnapshots(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "suggest_context") {
                auto req = MCPSuggestContextRequest::fromJson(params);
                auto result = co_await handleSuggestContext(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "semantic_dedupe") {
                auto req = MCPSemanticDedupeRequest::fromJson(params);
                auto result = co_await handleSemanticDedupe(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "graph") {
                auto req = MCPGraphRequest::fromJson(params);
                auto result = co_await handleGraphQuery(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "get") {
                auto req = MCPRetrieveDocumentRequest::fromJson(params);
                auto result = co_await handleRetrieveDocument(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }
            if (op == "status") {
                auto req = MCPStatusRequest::fromJson(params);
                auto result = co_await handleGetStatus(req);
                if (!result) {
                    co_return fail(result.error().message);
                }
                co_return QueryDispatchResult{.result = result.value().toJson()};
            }

            auto toolResult = co_await internalRegistry_->callTool(op, params);
            const bool isError = toolResult.value("isError", false);
            auto primaryError = extractToolPrimaryError(toolResult);
            auto resultData = extractToolResultData(std::move(toolResult));
            co_return QueryDispatchResult{.result = std::move(resultData),
                                          .isError = isError,
                                          .primaryError = std::move(primaryError)};
        };

        if (steps.size() == 1) {
            const auto& step = steps[0];
            if (!step.is_object() || !step.contains("op")) {
                co_return wrapToolResultStructured(
                    json::array({content::text("Error: step 0 missing 'op' field")}), std::nullopt,
                    true);
            }

            const auto op = step["op"].get<std::string>();
            if (allowedOps.find(op) == allowedOps.end()) {
                co_return wrapToolResultStructured(
                    json::array({content::text("Error: '" + op +
                                               "' is not a read operation. Use the 'execute' "
                                               "tool for write operations "
                                               "(add, update, delete, restore, download).")}),
                    std::nullopt, true);
            }

            auto params = resolvePrevRefs(extractCompositeStepParams(step), json::object());
            auto dispatch = co_await dispatchQueryOp(op, std::move(params));
            std::string firstLine =
                buildCompositeFirstLine("Pipeline: 1/1 steps", dispatch.primaryError);

            json contentItems = json::array({content::text(firstLine)});
            if (!limitToolResultDup_) {
                contentItems.push_back(content::json(dispatch.result, 2));
            }

            std::optional<json> structured;
            if (negotiatedProtocolVersion_ >= "2024-11-05") {
                structured = json{{"type", "tool_result"}, {"data", dispatch.result}};
            }

            co_return wrapToolResultStructured(contentItems, structured, dispatch.isError);
        }

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

            if (allowedOps.find(op) == allowedOps.end()) {
                co_return wrapToolResultStructured(
                    json::array({content::text("Error: '" + op +
                                               "' is not a read operation. Use the 'execute' tool "
                                               "for write operations "
                                               "(add, update, delete, restore, download).")}),
                    std::nullopt, true);
            }

            auto resolvedParams = resolvePrevRefs(extractCompositeStepParams(step), prevResult);
            auto dispatch = co_await dispatchQueryOp(op, std::move(resolvedParams));
            const bool isError = dispatch.isError;
            prevResult = dispatch.result;

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
        std::string firstLine = buildCompositeFirstLine(summary.str(), primaryError);
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
            {"add", "add"},
            {"update", "update"},
            {"delete", "delete_by_name"},
            {"restore", "restore"},
            {"download", "download"},
            {"download_cancel", "download_cancel"},
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

            auto params = extractCompositeStepParams(operation);
            if (op == "add") {
                auto normalized = detail::normalizeAddParams(params);
                params = std::move(normalized.params);
                if (normalized.error) {
                    auto errResult = json{{"stepIndex", i},
                                          {"op", op},
                                          {"success", false},
                                          {"error", *normalized.error}};
                    results.push_back(std::move(errResult));
                    ++failed;
                    if (!continueOnError) {
                        break;
                    }
                    continue;
                }
            }

            // Dispatch via the internal registry
            auto toolResult = co_await internalRegistry_->callTool(toolIt->second, params);

            bool isError = toolResult.value("isError", false);
            json stepResult = json{{"stepIndex", i}, {"op", op}, {"success", !isError}};

            // Extract data from tool result
            auto primaryToolError = extractToolPrimaryError(toolResult);
            stepResult["data"] = extractToolResultData(std::move(toolResult));

            if (isError) {
                ++failed;
                if (primaryToolError.has_value()) {
                    stepResult["error"] = *primaryToolError;
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
        std::string firstLine = buildCompositeFirstLine(summary.str(), primaryError);
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
                const auto params = extractCompositeStepParams(op);
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
