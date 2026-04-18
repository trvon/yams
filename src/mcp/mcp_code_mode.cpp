#include <yams/mcp/mcp_server.h>
#include <yams/mcp/mode_router.h>

#include <boost/asio/awaitable.hpp>

#include <nlohmann/json.hpp>

#include <memory>
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

namespace {
const std::unordered_map<std::string, std::string>& queryOpTable() {
    static const std::unordered_map<std::string, std::string> table = {
        {"search", "search"},
        {"grep", "grep"},
        {"list", "list"},
        {"list_collections", "list_collections"},
        {"list_snapshots", "list_snapshots"},
        {"suggest_context", "suggest_context"},
        {"semantic_dedupe", "semantic_dedupe"},
        {"graph", "graph"},
        {"get", "get"},
        {"status", "status"},
        {"describe", "describe"},
    };
    return table;
}
} // namespace

boost::asio::awaitable<ModeRouterStepResult>
MCPServer::dispatchPipelineStep(std::string op, json params, std::size_t stepIndex,
                                std::shared_ptr<json> prevResultPtr) {
    ModeRouterStepResult step;
    step.stepIndex = stepIndex;
    step.op = op;

    auto fail = [&step](std::string message) {
        step.isError = true;
        step.primaryError = message;
        step.data = json{{"error", std::move(message)}};
    };

    if (op == "describe") {
        const auto target = params.value("target", std::string{});
        step.data = target.empty() ? describeAllOps() : describeOp(target);
    } else if (op == "search") {
        auto req = MCPSearchRequest::fromJson(params);
        auto result = co_await handleSearchDocuments(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "grep") {
        auto req = MCPGrepRequest::fromJson(params);
        auto result = co_await handleGrepDocuments(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "list") {
        auto req = MCPListDocumentsRequest::fromJson(params);
        auto result = co_await handleListDocuments(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "list_collections") {
        auto req = MCPListCollectionsRequest::fromJson(params);
        auto result = co_await handleListCollections(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "list_snapshots") {
        auto req = MCPListSnapshotsRequest::fromJson(params);
        auto result = co_await handleListSnapshots(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "suggest_context") {
        auto req = MCPSuggestContextRequest::fromJson(params);
        auto result = co_await handleSuggestContext(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "semantic_dedupe") {
        auto req = MCPSemanticDedupeRequest::fromJson(params);
        auto result = co_await handleSemanticDedupe(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "graph") {
        auto req = MCPGraphRequest::fromJson(params);
        auto result = co_await handleGraphQuery(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "get") {
        auto req = MCPRetrieveDocumentRequest::fromJson(params);
        auto result = co_await handleRetrieveDocument(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else if (op == "status") {
        auto req = MCPStatusRequest::fromJson(params);
        auto result = co_await handleGetStatus(req);
        if (!result) {
            fail(result.error().message);
        } else {
            step.data = result.value().toJson();
        }
    } else {
        auto toolResult = co_await internalRegistry_->callTool(op, params);
        step.isError = toolResult.value("isError", false);
        if (step.isError) {
            step.primaryError = extractToolPrimaryError(toolResult);
        }
        step.data = extractToolResultData(std::move(toolResult));
    }

    *prevResultPtr = step.data;
    co_return step;
}

boost::asio::awaitable<json> MCPServer::handlePipelineQuery(const json& args) {
    auto prevResultPtr = std::make_shared<json>(json::object());
    const auto& opTable = queryOpTable();

    if (args.contains("steps") && args["steps"].is_array()) {
        for (const auto& step : args["steps"]) {
            if (!step.is_object() || !step.contains("op")) {
                continue;
            }
            const auto op = step["op"].get<std::string>();
            if (!opTable.contains(op)) {
                co_return wrapToolResultStructured(
                    json::array({content::text(
                        "Error: '" + op +
                        "' is not a read operation. Use the 'execute' tool for write operations "
                        "(add, update, delete, restore, download).")}),
                    std::nullopt, true);
            }
        }
    }

    ModeRouterConfig cfg;
    cfg.kind = "read";
    cfg.stepsKey = "steps";
    cfg.summaryPrefix = "Pipeline";
    cfg.invalidOpHint =
        "Use the 'execute' tool for write operations (add, update, delete, restore, download).";
    cfg.opTable = opTable;
    cfg.stepProjector = projections::queryStepProjection;
    cfg.finalResultBuilder = projections::queryFinalResult;
    cfg.singleStepUnwrap = true;

    cfg.normalize = [prevResultPtr](const std::string& op,
                                    const json& params) -> ModeRouterNormalizeResult {
        json resolved = resolvePrevRefs(params, *prevResultPtr);
        if (op == "describe") {
            return ModeRouterNormalizeResult{std::move(resolved), std::nullopt};
        }
        auto err = normalizeQueryParamsForOp(op, resolved);
        return ModeRouterNormalizeResult{std::move(resolved), std::move(err)};
    };

    cfg.dispatch = [this, prevResultPtr](
                       std::string op, std::string /*target*/, json params,
                       std::size_t stepIndex) -> boost::asio::awaitable<ModeRouterStepResult> {
        co_return co_await dispatchPipelineStep(std::move(op), std::move(params), stepIndex,
                                                prevResultPtr);
    };

    ModeRouter router(std::move(cfg));
    co_return co_await router.handle(args, negotiatedProtocolVersion_, limitToolResultDup_);
}

boost::asio::awaitable<json> MCPServer::handleBatchExecute(const json& args) {
    ModeRouterConfig cfg;
    cfg.kind = "write";
    cfg.stepsKey = "operations";
    cfg.summaryPrefix = "Execute";
    cfg.invalidOpHint = "Use the 'query' tool for read operations (search, grep, list, etc).";
    cfg.opTable = {
        {"add", "add"},         {"update", "update"},     {"delete", "delete_by_name"},
        {"restore", "restore"}, {"download", "download"}, {"download_cancel", "download_cancel"},
    };
    cfg.stepProjector = projections::executeStepProjection;
    cfg.finalResultBuilder = projections::executeFinalResult;

    cfg.normalize = [](const std::string& op, json params) -> ModeRouterNormalizeResult {
        if (op == "add") {
            auto normalized = detail::normalizeAddParams(params);
            return ModeRouterNormalizeResult{std::move(normalized.params),
                                             std::move(normalized.error)};
        }
        return ModeRouterNormalizeResult{std::move(params), std::nullopt};
    };

    cfg.dispatch = [this](std::string op, std::string target, json params,
                          std::size_t stepIndex) -> boost::asio::awaitable<ModeRouterStepResult> {
        auto toolResult = co_await internalRegistry_->callTool(target, params);
        ModeRouterStepResult step;
        step.stepIndex = stepIndex;
        step.op = std::move(op);
        step.isError = toolResult.value("isError", false);
        if (step.isError) {
            step.primaryError = extractToolPrimaryError(toolResult);
        }
        step.data = extractToolResultData(std::move(toolResult));
        co_return step;
    };

    ModeRouter router(std::move(cfg));
    json result = co_await router.handle(args, negotiatedProtocolVersion_, limitToolResultDup_);
    appendExecuteAddHint(result, args);
    co_return result;
}

void MCPServer::appendExecuteAddHint(json& result, const json& args) {
    if (!result.value("isError", false) || !args.contains("operations") ||
        !args["operations"].is_array()) {
        return;
    }
    bool sawAddWithUrl = false;
    bool sawAddWithTitleOrDesc = false;
    for (const auto& op : args["operations"]) {
        if (!op.is_object()) {
            continue;
        }
        if (op.value("op", std::string{}) != "add") {
            continue;
        }
        const auto params = extractCompositeStepParams(op);
        if (!params.is_object()) {
            continue;
        }
        if (params.contains("url")) {
            sawAddWithUrl = true;
        }
        if (params.contains("title") || params.contains("description")) {
            sawAddWithTitleOrDesc = true;
        }
    }
    if ((sawAddWithUrl || sawAddWithTitleOrDesc) && result.contains("content") &&
        result["content"].is_array()) {
        result["content"].push_back(content::text(
            "Hint: execute op 'add' expects {path} or {content}+{name} (optional: "
            "mime_type, tags, metadata, collection, snapshot_id). If you're trying to "
            "fetch a URL, use op 'download' with {url} and optional {post_index:true}."));
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
