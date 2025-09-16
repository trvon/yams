#pragma once

#include <nlohmann/json.hpp>
#include <concepts>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <yams/core/types.h>

namespace yams::mcp {

using json = nlohmann::json;

[[maybe_unused]] static nlohmann::json wrapToolResult(const nlohmann::json& structured,
                                                      bool isError = false) {
    nlohmann::json result;
    // Format according to MCP spec - content array with text items
    if (isError) {
        // For errors, return the error object directly without wrapping in content
        return structured;
    }

    // For successful results, wrap in content array as per MCP spec
    result["content"] =
        nlohmann::json::array({nlohmann::json{{"type", "text"}, {"text", structured.dump()}}});

    return result;
}

// C++20 concepts for tool system
template <typename T>
concept ToolRequest = requires {
    typename T::RequestType;
    requires std::same_as<T, typename T::RequestType>;
};

template <typename T>
concept ToolResponse = requires {
    typename T::ResponseType;
    requires std::same_as<T, typename T::ResponseType>;
};

template <typename T>
concept ToolSerializable = requires(const T& t, const json& j) {
    { T::fromJson(j) } -> std::same_as<T>;
    { t.toJson() } -> std::same_as<json>;
};

// Tool request/response DTOs
struct MCPSearchRequest {
    using RequestType = MCPSearchRequest;

    std::string query;
    size_t limit = 10;
    bool fuzzy = false;
    float similarity = 0.7f;
    std::string hash;
    std::string type = "hybrid";
    bool verbose = false;
    bool pathsOnly = false;
    bool lineNumbers = false;
    int beforeContext = 0;
    int afterContext = 0;
    int context = 0;
    std::string colorMode = "never";
    std::string pathPattern;
    std::vector<std::string> tags;
    bool matchAllTags = false;
    // Session scoping
    bool useSession = true;  // default: scope to current session when available
    std::string sessionName; // optional: target a specific session

    static MCPSearchRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPSearchResponse {
    using ResponseType = MCPSearchResponse;

    size_t total = 0;
    std::string type;
    uint64_t executionTimeMs = 0;
    std::vector<std::string> paths;

    struct Result {
        std::string id;
        std::string hash;
        std::string title;
        std::string path;
        float score = 0.0f;
        std::string snippet;
        std::optional<float> vectorScore;
        std::optional<float> keywordScore;
        std::optional<float> kgEntityScore;
        std::optional<float> structuralScore;
    };
    std::vector<Result> results;

    static MCPSearchResponse fromJson(const json& j);
    json toJson() const;
};

struct MCPGrepRequest {
    using RequestType = MCPGrepRequest;

    std::string pattern;
    std::vector<std::string> paths;
    bool ignoreCase = false;
    bool word = false;
    bool invert = false;
    bool lineNumbers = false;
    bool withFilename = true;
    bool count = false;
    bool filesWithMatches = false;
    bool filesWithoutMatch = false;
    int afterContext = 0;
    int beforeContext = 0;
    int context = 0;
    std::optional<int> maxCount;
    std::string color = "auto";
    bool fastFirst = false;
    // Session scoping
    bool useSession = true;
    std::string sessionName;

    static MCPGrepRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPGrepResponse {
    using ResponseType = MCPGrepResponse;

    std::string output;
    size_t matchCount = 0;
    size_t fileCount = 0;

    static MCPGrepResponse fromJson(const json& j);
    json toJson() const;
};

// Download tool DTOs
struct MCPDownloadRequest {
    using RequestType = MCPDownloadRequest;

    // Download parameters
    std::string url;
    std::vector<std::string> headers;
    std::string checksum;
    int concurrency = 4;
    size_t chunkSizeBytes = 8'388'608;
    int timeoutMs = 60'000;
    bool resume = true;
    std::string proxy;
    bool followRedirects = true;
    bool storeOnly = true;
    std::string exportPath;
    std::string overwrite = "never";

    // Optional post-indexing parameters (when true, index downloaded artifact)
    bool postIndex = true;
    std::vector<std::string> tags;
    std::map<std::string, std::string> metadata;
    std::string collection;
    std::string snapshotId;
    std::string snapshotLabel;

    static MCPDownloadRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPDownloadResponse {
    using ResponseType = MCPDownloadResponse;

    std::string url;
    std::string hash;
    std::string storedPath;
    uint64_t sizeBytes = 0;
    bool success = false;
    bool indexed = false;
    std::optional<int> httpStatus;
    std::optional<std::string> etag;
    std::optional<std::string> lastModified;
    std::optional<bool> checksumOk;
    // Exposed HTTP-derived metadata for clients
    std::optional<std::string> contentType;   // e.g., "application/pdf"
    std::optional<std::string> suggestedName; // from Content-Disposition filename

    static MCPDownloadResponse fromJson(const json& j);
    json toJson() const;
};

// Store document DTOs
struct MCPStoreDocumentRequest {
    using RequestType = MCPStoreDocumentRequest;

    std::string path;
    std::string content;
    std::string name;
    std::string mimeType;
    // Collection/snapshot and directory options to mirror CLI 'add'
    std::string collection;
    std::string snapshotId;
    std::string snapshotLabel;
    bool recursive = false;
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool disableAutoMime = false;
    bool noEmbeddings = false;
    std::vector<std::string> tags;
    json metadata;

    static MCPStoreDocumentRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPStoreDocumentResponse {
    using ResponseType = MCPStoreDocumentResponse;

    std::string hash;
    uint64_t bytesStored = 0;
    uint64_t bytesDeduped = 0;

    static MCPStoreDocumentResponse fromJson(const json& j);
    json toJson() const;
};

// Retrieve document DTOs
struct MCPRetrieveDocumentRequest {
    using RequestType = MCPRetrieveDocumentRequest;

    std::string hash;
    std::string name; // optional: retrieve by name
    std::string outputPath;
    bool graph = false;
    int depth = 1;
    bool includeContent = false;
    // Session scoping for name resolution
    bool useSession = true;
    std::string sessionName;

    static MCPRetrieveDocumentRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPRetrieveDocumentResponse {
    using ResponseType = MCPRetrieveDocumentResponse;

    std::string hash;
    std::string path;
    std::string name;
    uint64_t size = 0;
    std::string mimeType;
    std::optional<std::string> content;
    bool graphEnabled = false;
    std::vector<json> related;

    static MCPRetrieveDocumentResponse fromJson(const json& j);
    json toJson() const;
};

// List documents DTOs
struct MCPListDocumentsRequest {
    using RequestType = MCPListDocumentsRequest;

    std::string pattern;
    std::string name;
    std::vector<std::string> tags;
    std::string type;
    std::string mime;
    std::string extension;
    bool binary = false;
    bool text = false;
    int recent = 0;
    int limit = 100;
    int offset = 0;
    std::string sortBy = "modified";
    std::string sortOrder = "desc";
    bool pathsOnly = false;
    // Session scoping
    bool useSession = true;
    std::string sessionName;

    static MCPListDocumentsRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPListDocumentsResponse {
    using ResponseType = MCPListDocumentsResponse;

    std::vector<json> documents;
    size_t total = 0;

    static MCPListDocumentsResponse fromJson(const json& j);
    json toJson() const;
};

// Stats DTOs
struct MCPStatsRequest {
    using RequestType = MCPStatsRequest;

    bool fileTypes = false;
    bool verbose = false;

    static MCPStatsRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPStatsResponse {
    using ResponseType = MCPStatsResponse;

    uint64_t totalObjects = 0;
    uint64_t totalBytes = 0;
    uint64_t uniqueHashes = 0;
    uint64_t deduplicationSavings = 0;
    std::vector<json> fileTypes;
    json additionalStats;

    static MCPStatsResponse fromJson(const json& j);
    json toJson() const;
};

// Status DTOs
struct MCPStatusRequest {
    using RequestType = MCPStatusRequest;
    bool detailed = false;
    static MCPStatusRequest fromJson(const json& j) {
        MCPStatusRequest r;
        if (j.contains("detailed"))
            r.detailed = j.at("detailed").get<bool>();
        return r;
    }
    json toJson() const {
        json j;
        j["detailed"] = detailed;
        return j;
    }
};

struct MCPStatusResponse {
    using ResponseType = MCPStatusResponse;
    bool running = false;
    bool ready = false;
    std::string overallStatus;
    std::string lifecycleState;
    std::string lastError;
    std::string version;
    uint64_t uptimeSeconds = 0;
    uint64_t requestsProcessed = 0;
    uint64_t activeConnections = 0;
    double memoryUsageMb = 0.0;
    double cpuUsagePercent = 0.0;
    json counters;        // flattened requestCounts
    json readinessStates; // subsystem readiness map
    json initProgress;    // subsystem init progress map

    static MCPStatusResponse fromJson(const json& j) {
        MCPStatusResponse r;
        r.running = j.value("running", false);
        r.ready = j.value("ready", false);
        r.overallStatus = j.value("overallStatus", "");
        r.lifecycleState = j.value("lifecycleState", "");
        r.lastError = j.value("lastError", "");
        r.version = j.value("version", "");
        r.uptimeSeconds = j.value("uptimeSeconds", 0ull);
        r.requestsProcessed = j.value("requestsProcessed", 0ull);
        r.activeConnections = j.value("activeConnections", 0ull);
        r.memoryUsageMb = j.value("memoryUsageMb", 0.0);
        r.cpuUsagePercent = j.value("cpuUsagePercent", 0.0);
        if (j.contains("counters"))
            r.counters = j.at("counters");
        if (j.contains("readinessStates"))
            r.readinessStates = j.at("readinessStates");
        if (j.contains("initProgress"))
            r.initProgress = j.at("initProgress");
        return r;
    }
    json toJson() const {
        json j;
        j["running"] = running;
        j["ready"] = ready;
        j["overallStatus"] = overallStatus;
        j["lifecycleState"] = lifecycleState;
        j["lastError"] = lastError;
        j["version"] = version;
        j["uptimeSeconds"] = uptimeSeconds;
        j["requestsProcessed"] = requestsProcessed;
        j["activeConnections"] = activeConnections;
        j["memoryUsageMb"] = memoryUsageMb;
        j["cpuUsagePercent"] = cpuUsagePercent;
        j["counters"] = counters;
        j["readinessStates"] = readinessStates;
        j["initProgress"] = initProgress;
        return j;
    }
};

// Doctor tool
struct MCPDoctorRequest {
    using RequestType = MCPDoctorRequest;
    bool verbose = true;
    static MCPDoctorRequest fromJson(const json& j) {
        MCPDoctorRequest r;
        if (j.contains("verbose"))
            r.verbose = j.at("verbose").get<bool>();
        return r;
    }
    json toJson() const {
        json j;
        j["verbose"] = verbose;
        return j;
    }
};

struct MCPDoctorResponse {
    using ResponseType = MCPDoctorResponse;
    std::string summary;
    std::vector<std::string> issues;
    json details;
    static MCPDoctorResponse fromJson(const json& j) {
        MCPDoctorResponse r;
        r.summary = j.value("summary", "");
        if (j.contains("issues"))
            r.issues = j.at("issues").get<std::vector<std::string>>();
        if (j.contains("details"))
            r.details = j.at("details");
        return r;
    }
    json toJson() const {
        json j;
        j["summary"] = summary;
        j["issues"] = issues;
        j["details"] = details;
        return j;
    }
};

// Add directory DTOs
struct MCPAddDirectoryRequest {
    using RequestType = MCPAddDirectoryRequest;

    std::string directoryPath;
    std::string collection;
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    json metadata;
    bool recursive = true;
    bool followSymlinks = false;
    std::string snapshotId;
    std::string snapshotLabel;
    std::vector<std::string> tags;

    static MCPAddDirectoryRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPAddDirectoryResponse {
    using ResponseType = MCPAddDirectoryResponse;

    std::string directoryPath;
    std::string collection;
    size_t filesProcessed = 0;
    size_t filesIndexed = 0;
    size_t filesSkipped = 0;
    size_t filesFailed = 0;
    std::vector<json> results;

    static MCPAddDirectoryResponse fromJson(const json& j);
    json toJson() const;
};

// Tool descriptor for registry
struct ToolDescriptor {
    std::string_view name;
    std::function<json(const json&)> handler;
    json schema;
    std::string_view description;

    ToolDescriptor(std::string_view n, std::function<json(const json&)> h, json s = {},
                   std::string_view d = {})
        : name(n), handler(std::move(h)), schema(std::move(s)), description(d) {}
};

// Get by name DTOs
struct MCPGetByNameRequest {
    using RequestType = MCPGetByNameRequest;

    std::string name;
    // New: explicit path-based resolution (supports subpath suffix)
    std::string path;        // if set, prefer exact path match; fallback to suffix
    bool subpath = true;     // allow suffix match when exact not found
    bool rawContent = false; // Return raw content without processing
    bool extractText = true; // Apply text extraction for HTML/supported formats
    bool latest = false;     // When ambiguous, select newest
    bool oldest = false;     // When ambiguous, select oldest

    static MCPGetByNameRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPGetByNameResponse {
    using ResponseType = MCPGetByNameResponse;

    std::string hash;
    std::string name;
    std::string path;
    uint64_t size = 0;
    std::string mimeType;
    std::string content;

    static MCPGetByNameResponse fromJson(const json& j);
    json toJson() const;
};

// Delete by name DTOs
struct MCPDeleteByNameRequest {
    using RequestType = MCPDeleteByNameRequest;

    std::string name;
    std::vector<std::string> names;
    std::string pattern;
    bool dryRun = false;

    static MCPDeleteByNameRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPDeleteByNameResponse {
    using ResponseType = MCPDeleteByNameResponse;

    std::vector<std::string> deleted;
    size_t count = 0;
    bool dryRun = false;

    static MCPDeleteByNameResponse fromJson(const json& j);
    json toJson() const;
};

// Cat document DTOs
struct MCPCatDocumentRequest {
    using RequestType = MCPCatDocumentRequest;

    std::string hash;
    std::string name;
    bool rawContent = false; // Return raw content without processing
    bool extractText = true; // Apply text extraction for HTML/supported formats
    bool latest = false;     // When ambiguous, select newest
    bool oldest = false;     // When ambiguous, select oldest

    static MCPCatDocumentRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPCatDocumentResponse {
    using ResponseType = MCPCatDocumentResponse;

    std::string content;
    std::string hash;
    std::string name;
    uint64_t size = 0;

    static MCPCatDocumentResponse fromJson(const json& j);
    json toJson() const;
};

// Update metadata DTOs
struct MCPUpdateMetadataRequest {
    using RequestType = MCPUpdateMetadataRequest;

    std::string hash;
    std::string name;
    // Extended selectors and options (parity with get_by_name)
    std::string path;               // explicit absolute/relative path
    std::vector<std::string> names; // batch by names
    std::string pattern;            // glob-like pattern for batch
    bool latest = false;            // disambiguation when multiple match
    bool oldest = false;            // disambiguation when multiple match
    json metadata;
    std::vector<std::string> tags;       // tags to add
    std::vector<std::string> removeTags; // tags to remove
    bool dryRun = false;
    // Session scoping
    bool useSession = true;
    std::string sessionName;

    static MCPUpdateMetadataRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPUpdateMetadataResponse {
    using ResponseType = MCPUpdateMetadataResponse;

    bool success = false;
    std::string message;
    // Aggregated details for batch updates
    std::size_t matched = 0;
    std::size_t updated = 0;
    std::vector<std::string> updatedHashes;

    static MCPUpdateMetadataResponse fromJson(const json& j);
    json toJson() const;
};

// Collection/Snapshot DTOs
struct MCPRestoreCollectionRequest {
    using RequestType = MCPRestoreCollectionRequest;

    std::string collection;
    std::string outputDirectory;
    std::string layoutTemplate = "{path}";
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool overwrite = false;
    bool createDirs = true;
    bool dryRun = false;

    static MCPRestoreCollectionRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPRestoreCollectionResponse {
    using ResponseType = MCPRestoreCollectionResponse;

    size_t filesRestored = 0;
    std::vector<std::string> restoredPaths;
    bool dryRun = false;

    static MCPRestoreCollectionResponse fromJson(const json& j);
    json toJson() const;
};

struct MCPRestoreSnapshotRequest {
    using RequestType = MCPRestoreSnapshotRequest;

    std::string snapshotId;
    std::string snapshotLabel;
    std::string outputDirectory;
    std::string layoutTemplate = "{path}";
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool overwrite = false;
    bool createDirs = true;
    bool dryRun = false;

    static MCPRestoreSnapshotRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPRestoreSnapshotResponse {
    using ResponseType = MCPRestoreSnapshotResponse;

    size_t filesRestored = 0;
    std::vector<std::string> restoredPaths;
    bool dryRun = false;

    static MCPRestoreSnapshotResponse fromJson(const json& j);
    json toJson() const;
};

struct MCPListCollectionsRequest {
    using RequestType = MCPListCollectionsRequest;

    static MCPListCollectionsRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPListCollectionsResponse {
    using ResponseType = MCPListCollectionsResponse;

    std::vector<std::string> collections;

    static MCPListCollectionsResponse fromJson(const json& j);
    json toJson() const;
};

struct MCPListSnapshotsRequest {
    using RequestType = MCPListSnapshotsRequest;

    std::string collection;
    bool withLabels = true;

    static MCPListSnapshotsRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPListSnapshotsResponse {
    using ResponseType = MCPListSnapshotsResponse;

    std::vector<json> snapshots;

    static MCPListSnapshotsResponse fromJson(const json& j);
    json toJson() const;
};

// Session start/stop (simplified)
struct MCPSessionStartRequest {
    using RequestType = MCPSessionStartRequest;
    std::string name;        // optional; empty means use existing
    std::string description; // optional
    bool warm = true;        // warm after starting
    int limit = 200;         // docs per selector
    int snippetLen = 160;    // snippet length
    int cores = -1;
    int memoryGb = -1;
    long timeMs = -1;
    bool aggressive = false;

    static MCPSessionStartRequest fromJson(const json& j) {
        MCPSessionStartRequest r;
        if (j.contains("name"))
            r.name = j.value("name", "");
        if (j.contains("description"))
            r.description = j.value("description", "");
        r.warm = j.value("warm", true);
        r.limit = j.value("limit", 200);
        r.snippetLen = j.value("snippet_len", 160);
        r.cores = j.value("cores", -1);
        r.memoryGb = j.value("memory_gb", -1);
        r.timeMs = j.value<long>("time_ms", -1);
        r.aggressive = j.value("aggressive", false);
        return r;
    }
    json toJson() const {
        return json{
            {"name", name},          {"description", description}, {"warm", warm},
            {"limit", limit},        {"snippet_len", snippetLen},  {"cores", cores},
            {"memory_gb", memoryGb}, {"time_ms", timeMs},          {"aggressive", aggressive}};
    }
};

struct MCPSessionStartResponse {
    using ResponseType = MCPSessionStartResponse;
    std::string name;
    uint64_t warmedCount = 0;
    static MCPSessionStartResponse fromJson(const json& j) {
        MCPSessionStartResponse r;
        r.name = j.value("name", "");
        r.warmedCount = j.value<uint64_t>("warmed_count", 0);
        return r;
    }
    json toJson() const { return json{{"name", name}, {"warmed_count", warmedCount}}; }
};

struct MCPSessionStopRequest {
    using RequestType = MCPSessionStopRequest;
    std::string name;  // optional; empty = current
    bool clear = true; // clear materialized cache
    static MCPSessionStopRequest fromJson(const json& j) {
        MCPSessionStopRequest r;
        if (j.contains("name"))
            r.name = j.value("name", "");
        r.clear = j.value("clear", true);
        return r;
    }
    json toJson() const { return json{{"name", name}, {"clear", clear}}; }
};

struct MCPSessionStopResponse {
    using ResponseType = MCPSessionStopResponse;
    std::string name;
    bool cleared = false;
    static MCPSessionStopResponse fromJson(const json& j) {
        MCPSessionStopResponse r;
        r.name = j.value("name", "");
        r.cleared = j.value("cleared", false);
        return r;
    }
    json toJson() const { return json{{"name", name}, {"cleared", cleared}}; }
};

// Session pin/unpin DTOs
struct MCPSessionPinRequest {
    using RequestType = MCPSessionPinRequest;

    std::string path;
    std::vector<std::string> tags;
    json metadata;

    static MCPSessionPinRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPSessionPinResponse {
    using ResponseType = MCPSessionPinResponse;

    size_t updated = 0;

    static MCPSessionPinResponse fromJson(const json& j);
    json toJson() const;
};

struct MCPSessionUnpinRequest {
    using RequestType = MCPSessionUnpinRequest;

    std::string path;

    static MCPSessionUnpinRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPSessionUnpinResponse {
    using ResponseType = MCPSessionUnpinResponse;

    size_t updated = 0;

    static MCPSessionUnpinResponse fromJson(const json& j);
    json toJson() const;
};

template <ToolRequest RequestType, ToolResponse ResponseType>
requires ToolSerializable<RequestType> && ToolSerializable<ResponseType>
class ToolWrapper {
public:
    using HandlerFn = std::function<Result<ResponseType>(const RequestType&)>;

    explicit ToolWrapper(HandlerFn handler) : handler_(std::move(handler)) {}

    json operator()(const json& args) {
        try {
            auto req = RequestType::fromJson(args);
            auto result = handler_(req);

            if (!result) {
                // Return error wrapped in content array as per MCP spec
                json errorContent = {
                    {"content", json::array({json{{"type", "text"},
                                                  {"text", "Error: " + result.error().message}}})},
                    {"isError", true}};
                return errorContent;
            }

            auto responseJson = result.value().toJson();
            return wrapToolResult(responseJson, false);

        } catch (const json::exception& e) {
            // Return error wrapped in content array as per MCP spec
            json errorContent = {
                {"content", json::array({json{{"type", "text"},
                                              {"text", "JSON error: " + std::string(e.what())}}})},
                {"isError", true}};
            return errorContent;
        } catch (const std::exception& e) {
            // Return error wrapped in content array as per MCP spec
            json errorContent = {
                {"content", json::array({json{{"type", "text"},
                                              {"text", "Error: " + std::string(e.what())}}})},
                {"isError", true}};
            return errorContent;
        }
    }

private:
    HandlerFn handler_;
};
// Async tool wrapper template for coroutine-based handlers
template <ToolRequest RequestType, ToolResponse ResponseType>
requires ToolSerializable<RequestType> && ToolSerializable<ResponseType>
class AsyncToolWrapper {
public:
    using AsyncHandlerFn =
        std::function<boost::asio::awaitable<Result<ResponseType>>(const RequestType&)>;

    explicit AsyncToolWrapper(AsyncHandlerFn handler) : handler_(std::move(handler)) {}

    boost::asio::awaitable<json> operator()(const json& args) {
        try {
            auto req = RequestType::fromJson(args);
            auto result = co_await handler_(req);

            if (!result) {
                // Return error wrapped in content array as per MCP spec
                json errorContent = {
                    {"content", json::array({json{{"type", "text"},
                                                  {"text", "Error: " + result.error().message}}})},
                    {"isError", true}};
                co_return errorContent;
            }

            auto responseJson = result.value().toJson();
            co_return wrapToolResult(responseJson, false);

        } catch (const json::exception& e) {
            // Return error wrapped in content array as per MCP spec
            json errorContent = {
                {"content", json::array({json{{"type", "text"},
                                              {"text", "JSON error: " + std::string(e.what())}}})},
                {"isError", true}};
            co_return errorContent;
        } catch (const std::exception& e) {
            // Return error wrapped in content array as per MCP spec
            json errorContent = {
                {"content", json::array({json{{"type", "text"},
                                              {"text", "Error: " + std::string(e.what())}}})},
                {"isError", true}};
            co_return errorContent;
        }
    }

private:
    AsyncHandlerFn handler_;
};

// Async tool registry for coroutine-based handlers
class ToolRegistry {
public:
    using AsyncHandlerMap =
        std::unordered_map<std::string, std::function<boost::asio::awaitable<json>(const json&)>>;

    ToolRegistry() {
        handlers_.reserve(32); // Pre-allocate for performance
    }

    template <ToolRequest RequestType, ToolResponse ResponseType>
    requires ToolSerializable<RequestType> && ToolSerializable<ResponseType>
    void registerTool(
        std::string_view name,
        std::function<boost::asio::awaitable<Result<ResponseType>>(const RequestType&)> handler,
        json schema = {}, std::string_view description = {}) {
        auto wrapper = AsyncToolWrapper<RequestType, ResponseType>(std::move(handler));
        auto handlerFn = [wrapper = std::move(wrapper)](
                             const json& args) mutable -> boost::asio::awaitable<json> {
            return wrapper(args);
        };

        // Use iterator from emplace to avoid double lookup
        auto [it, inserted] = handlers_.emplace(std::string(name), std::move(handlerFn));
        if (inserted) {
            descriptors_.emplace_back(it->first, it->second, std::move(schema), description);
        }
    }

    boost::asio::awaitable<json> callTool(std::string_view name, const json& arguments) {
        if (auto it = handlers_.find(std::string(name)); it != handlers_.end()) {
            co_return co_await it->second(arguments);
        }
        // Return error wrapped in content array as per MCP spec
        json errorContent = {
            {"content",
             json::array({json{{"type", "text"}, {"text", "Unknown tool: " + std::string(name)}}})},
            {"isError", true}};
        co_return errorContent;
    }

    json listTools() const {
        json tools = json::array();
        for (const auto& desc : descriptors_) {
            json tool;
            tool["name"] = desc.name;
            tool["description"] = desc.description.empty() ? "" : std::string(desc.description);
            if (!desc.schema.empty()) {
                tool["inputSchema"] = desc.schema;
            }
            tools.push_back(std::move(tool));
        }
        return json{{"tools", std::move(tools)}};
    }

private:
    struct AsyncToolDescriptor {
        std::string_view name;
        const std::function<boost::asio::awaitable<json>(const json&)>& handler;
        json schema;
        std::string_view description;

        AsyncToolDescriptor(std::string_view n,
                            const std::function<boost::asio::awaitable<json>(const json&)>& h,
                            json s, std::string_view d)
            : name(n), handler(h), schema(std::move(s)), description(d) {}
    };

    AsyncHandlerMap handlers_;
    std::vector<AsyncToolDescriptor> descriptors_;
};

} // namespace yams::mcp
