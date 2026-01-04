#pragma once

#include <nlohmann/json.hpp>
#include <concepts>
#include <functional>
#include <iterator>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <yams/core/types.h>

namespace yams::mcp {

using json = nlohmann::json;

namespace detail {

inline json makeTextContent(std::string text) {
    return json::array({json{{"type", "text"}, {"text", std::move(text)}}});
}

[[nodiscard]] inline json makeErrorResponse(std::string message) {
    return json{{"content", makeTextContent(std::move(message))}, {"isError", true}};
}

[[nodiscard]] inline json makeErrorResponse(std::string_view message) {
    return makeErrorResponse(std::string(message));
}

template <typename T>
[[nodiscard]] auto jsonValueOr(const json& j, std::string_view key, T&& defaultValue)
    -> std::decay_t<T> {
    if (const auto it = j.find(key); it != j.end() && !it->is_null()) {
        return it->get<std::decay_t<T>>();
    }
    return std::forward<T>(defaultValue);
}

template <typename Container>
concept StringPushBackContainer = requires(Container& c, std::string value) {
    { c.push_back(std::move(value)) } -> std::same_as<void>;
};

template <StringPushBackContainer Container>
void readStringArray(const json& j, std::string_view key, Container& out) {
    if (const auto it = j.find(key); it != j.end() && !it->is_null()) {
        if (it->is_array()) {
            if constexpr (requires(Container& cont, std::size_t size) { cont.reserve(size); }) {
                out.reserve(out.size() + it->size());
            }
            for (const auto& element : *it) {
                if (element.is_string()) {
                    out.push_back(element.template get<std::string>());
                }
            }
        } else if (it->is_string()) {
            out.push_back(it->template get<std::string>());
        }
    }
}

template <typename Handler, typename Request, typename Response>
concept SyncToolHandler =
    std::invocable<Handler&, const Request&> &&
    std::same_as<std::invoke_result_t<Handler&, const Request&>, Result<Response>>;

template <typename Handler, typename Request, typename Response>
concept AsyncToolHandler = std::invocable<Handler&, const Request&> &&
                           std::same_as<std::invoke_result_t<Handler&, const Request&>,
                                        boost::asio::awaitable<Result<Response>>>;

} // namespace detail

// MCP 1.x requires textual responses wrapped in a content array. Retain this format until the
// protocol relaxes the constraint; callers rely on the text payload today.
[[maybe_unused]] inline nlohmann::json wrapToolResult(const nlohmann::json& structured,
                                                      bool /*isError*/ = false) {
    nlohmann::json result;
    result["content"] = detail::makeTextContent(structured.dump());
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
    std::vector<std::string> includePatterns; // Multiple include patterns (OR logic)
    std::vector<std::string> tags;
    bool matchAllTags = false;
    bool includeDiff = false; // include structured diff when pathPattern refers to a local file
    // Session scoping
    bool useSession = true;  // default: scope to current session when available
    std::string sessionName; // optional: target a specific session

    // Symbol ranking
    bool symbolRank = true; // Enable automatic symbol ranking boost for code-like queries

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
        std::optional<json> diff;                  // structured diff (added/removed/truncated)
        std::optional<std::string> localInputFile; // absolute local path (when diff produced)
    };
    std::vector<Result> results;

    static MCPSearchResponse fromJson(const json& j);
    json toJson() const;
};

struct MCPGrepRequest {
    using RequestType = MCPGrepRequest;

    std::string pattern;
    std::vector<std::string> paths;
    std::vector<std::string> includePatterns; // file globs
    std::string name;                         // optional path-like name to scope grep
    bool subpath = true;                      // allow suffix-style subpath expansion
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
    bool compressed = false;
    std::optional<uint8_t> compressionAlgorithm;
    std::optional<uint8_t> compressionLevel;
    std::optional<uint64_t> uncompressedSize;
    std::optional<uint32_t> compressedCrc32;
    std::optional<uint32_t> uncompressedCrc32;
    std::optional<std::string> compressionHeader;
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
    bool includeDiff = false; // include structured diff when name is a local file
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
        r.detailed = detail::jsonValueOr(j, "detailed", r.detailed);
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
        r.running = detail::jsonValueOr(j, "running", r.running);
        r.ready = detail::jsonValueOr(j, "ready", r.ready);
        r.overallStatus = detail::jsonValueOr(j, "overallStatus", r.overallStatus);
        r.lifecycleState = detail::jsonValueOr(j, "lifecycleState", r.lifecycleState);
        r.lastError = detail::jsonValueOr(j, "lastError", r.lastError);
        r.version = detail::jsonValueOr(j, "version", r.version);
        r.uptimeSeconds = detail::jsonValueOr(j, "uptimeSeconds", r.uptimeSeconds);
        r.requestsProcessed = detail::jsonValueOr(j, "requestsProcessed", r.requestsProcessed);
        r.activeConnections = detail::jsonValueOr(j, "activeConnections", r.activeConnections);
        r.memoryUsageMb = detail::jsonValueOr(j, "memoryUsageMb", r.memoryUsageMb);
        r.cpuUsagePercent = detail::jsonValueOr(j, "cpuUsagePercent", r.cpuUsagePercent);
        r.counters = detail::jsonValueOr(j, "counters", r.counters);
        r.readinessStates = detail::jsonValueOr(j, "readinessStates", r.readinessStates);
        r.initProgress = detail::jsonValueOr(j, "initProgress", r.initProgress);
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
        r.verbose = detail::jsonValueOr(j, "verbose", r.verbose);
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
        r.summary = detail::jsonValueOr(j, "summary", r.summary);
        r.issues = detail::jsonValueOr(j, "issues", r.issues);
        r.details = detail::jsonValueOr(j, "details", r.details);
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
    std::string name;
    std::function<json(const json&)> handler;
    json schema;
    std::string description;

    ToolDescriptor(std::string n, std::function<json(const json&)> h, json s = {},
                   std::string d = {})
        : name(std::move(n)), handler(std::move(h)), schema(std::move(s)),
          description(std::move(d)) {}
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
    // Extended selectors and options (parity with get-by-name use)
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

struct MCPRestoreRequest {
    using RequestType = MCPRestoreRequest;

    std::string collection;
    std::string snapshotId;
    std::string snapshotLabel;
    std::string outputDirectory;
    std::string layoutTemplate = "{path}";
    std::vector<std::string> includePatterns;
    std::vector<std::string> excludePatterns;
    bool overwrite = false;
    bool createDirs = true;
    bool dryRun = false;

    static MCPRestoreRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPRestoreResponse {
    using ResponseType = MCPRestoreResponse;
    size_t filesRestored = 0;
    std::vector<std::string> restoredPaths;
    bool dryRun = false;

    static MCPRestoreResponse fromJson(const json& j);
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

struct MCPGraphRequest {
    using RequestType = MCPGraphRequest;

    // Target selection (match CLI graph)
    std::string hash;
    std::string name;
    std::string nodeKey;
    int64_t nodeId{-1};

    // Modes
    bool listTypes{false};
    std::string listType;
    bool isolated{false};

    // Traversal options
    std::vector<std::string> relationFilters;
    std::string relation;
    int depth{1};
    size_t limit{100};
    size_t offset{0};
    bool reverse{false};

    // Output control
    bool includeNodeProperties{false};
    bool includeEdgeProperties{false};
    bool hydrateFully{true};

    // Snapshot scoping
    std::string scopeSnapshot;

    static MCPGraphRequest fromJson(const json& j);
    json toJson() const;
};

struct MCPGraphResponse {
    using ResponseType = MCPGraphResponse;

    json origin;
    json connectedNodes;
    json nodeTypeCounts;

    uint64_t totalNodesFound{0};
    uint64_t totalEdgesTraversed{0};
    bool truncated{false};
    int maxDepthReached{0};
    int64_t queryTimeMs{0};
    bool kgAvailable{true};
    std::string warning;

    static MCPGraphResponse fromJson(const json& j);
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
        r.name = detail::jsonValueOr(j, "name", r.name);
        r.description = detail::jsonValueOr(j, "description", r.description);
        r.warm = detail::jsonValueOr(j, "warm", r.warm);
        r.limit = detail::jsonValueOr(j, "limit", r.limit);
        r.snippetLen = detail::jsonValueOr(j, "snippet_len", r.snippetLen);
        r.cores = detail::jsonValueOr(j, "cores", r.cores);
        r.memoryGb = detail::jsonValueOr(j, "memory_gb", r.memoryGb);
        r.timeMs = detail::jsonValueOr(j, "time_ms", r.timeMs);
        r.aggressive = detail::jsonValueOr(j, "aggressive", r.aggressive);
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
        r.name = detail::jsonValueOr(j, "name", r.name);
        r.warmedCount = detail::jsonValueOr(j, "warmed_count", r.warmedCount);
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
        r.name = detail::jsonValueOr(j, "name", r.name);
        r.clear = detail::jsonValueOr(j, "clear", r.clear);
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
        r.name = detail::jsonValueOr(j, "name", r.name);
        r.cleared = detail::jsonValueOr(j, "cleared", r.cleared);
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

// Session watch DTOs
struct MCPSessionWatchRequest {
    using RequestType = MCPSessionWatchRequest;

    std::string session;
    std::string root;
    uint32_t intervalMs = 0;
    bool enable = true;
    bool addSelector = true;
    bool setCurrent = true;
    bool allowCreate = true;

    static MCPSessionWatchRequest fromJson(const json& j) {
        MCPSessionWatchRequest r;
        r.session = detail::jsonValueOr(j, "session", r.session);
        r.root = detail::jsonValueOr(j, "root", r.root);
        r.intervalMs = detail::jsonValueOr(j, "interval_ms", r.intervalMs);
        if (r.intervalMs == 0 && j.contains("interval")) {
            r.intervalMs = detail::jsonValueOr(j, "interval", r.intervalMs);
        }
        r.enable = detail::jsonValueOr(j, "enable", r.enable);
        r.enable = detail::jsonValueOr(j, "enabled", r.enable);
        if (j.contains("stop") && detail::jsonValueOr(j, "stop", false))
            r.enable = false;
        if (j.contains("disable") && detail::jsonValueOr(j, "disable", false))
            r.enable = false;
        r.addSelector = detail::jsonValueOr(j, "add_selector", r.addSelector);
        if (j.contains("no_selector") && detail::jsonValueOr(j, "no_selector", false))
            r.addSelector = false;
        r.setCurrent = detail::jsonValueOr(j, "set_current", r.setCurrent);
        if (j.contains("no_use") && detail::jsonValueOr(j, "no_use", false))
            r.setCurrent = false;
        r.allowCreate = detail::jsonValueOr(j, "allow_create", r.allowCreate);
        return r;
    }
    json toJson() const {
        return json{{"session", session},          {"root", root},
                    {"interval_ms", intervalMs},   {"enable", enable},
                    {"add_selector", addSelector}, {"set_current", setCurrent},
                    {"allow_create", allowCreate}};
    }
};

struct MCPSessionWatchResponse {
    using ResponseType = MCPSessionWatchResponse;

    std::string session;
    std::string root;
    bool enabled = false;
    uint32_t intervalMs = 0;
    bool created = false;
    bool selectorAdded = false;

    static MCPSessionWatchResponse fromJson(const json& j) {
        MCPSessionWatchResponse r;
        r.session = detail::jsonValueOr(j, "session", r.session);
        r.root = detail::jsonValueOr(j, "root", r.root);
        r.enabled = detail::jsonValueOr(j, "enabled", r.enabled);
        r.intervalMs = detail::jsonValueOr(j, "interval_ms", r.intervalMs);
        r.created = detail::jsonValueOr(j, "created", r.created);
        r.selectorAdded = detail::jsonValueOr(j, "selector_added", r.selectorAdded);
        return r;
    }
    json toJson() const {
        return json{{"session", session}, {"root", root},
                    {"enabled", enabled}, {"interval_ms", intervalMs},
                    {"created", created}, {"selector_added", selectorAdded}};
    }
};

template <ToolRequest RequestType, ToolResponse ResponseType>
requires ToolSerializable<RequestType> && ToolSerializable<ResponseType>
class ToolWrapper {
public:
    using HandlerFn = std::function<Result<ResponseType>(const RequestType&)>;

    template <typename Handler>
    requires detail::SyncToolHandler<std::decay_t<Handler>, RequestType, ResponseType>
    explicit ToolWrapper(Handler&& handler) : handler_(HandlerFn(std::forward<Handler>(handler))) {}

    json operator()(const json& args) {
        try {
            auto req = RequestType::fromJson(args);
            auto result = handler_(req);

            if (!result) {
                return detail::makeErrorResponse(std::string("Error: ") + result.error().message);
            }

            return wrapToolResult(result.value().toJson());

        } catch (const json::exception& e) {
            return detail::makeErrorResponse(std::string("JSON error: ") + e.what());
        } catch (const std::exception& e) {
            return detail::makeErrorResponse(std::string("Error: ") + e.what());
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

    template <typename Handler>
    requires detail::AsyncToolHandler<std::decay_t<Handler>, RequestType, ResponseType>
    explicit AsyncToolWrapper(Handler&& handler)
        : handler_(AsyncHandlerFn(std::forward<Handler>(handler))) {}

    boost::asio::awaitable<json> operator()(const json& args) {
        try {
            auto req = RequestType::fromJson(args);
            auto result = co_await handler_(req);

            if (!result) {
                co_return detail::makeErrorResponse(std::string("Error: ") +
                                                    result.error().message);
            }

            co_return wrapToolResult(result.value().toJson());

        } catch (const json::exception& e) {
            co_return detail::makeErrorResponse(std::string("JSON error: ") + e.what());
        } catch (const std::exception& e) {
            co_return detail::makeErrorResponse(std::string("Error: ") + e.what());
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
        descriptors_.reserve(32);
    }

    template <ToolRequest RequestType, ToolResponse ResponseType, typename Handler>
    requires ToolSerializable<RequestType> && ToolSerializable<ResponseType> &&
             detail::AsyncToolHandler<std::decay_t<Handler>, RequestType, ResponseType>
    void registerTool(std::string_view name, Handler&& handler, json schema = {},
                      std::string description = {}) {
        auto wrapper = AsyncToolWrapper<RequestType, ResponseType>(std::forward<Handler>(handler));
        auto handlerFn = [wrapper](const json& args) mutable -> boost::asio::awaitable<json> {
            return wrapper(args);
        };

        auto [it, inserted] = handlers_.emplace(std::string(name), std::move(handlerFn));
        if (inserted) {
            descriptors_.push_back({it->first, std::move(schema), std::move(description)});
        }
    }

    boost::asio::awaitable<json> callTool(std::string_view name, const json& arguments) {
        if (auto it = handlers_.find(std::string(name)); it != handlers_.end()) {
            co_return co_await it->second(arguments);
        }
        co_return detail::makeErrorResponse(std::string("Unknown tool: ") + std::string(name));
    }

    json listTools() const {
        json tools = json::array();
        std::ranges::transform(descriptors_, std::back_inserter(tools), [](const auto& desc) {
            json tool;
            tool["name"] = desc.name;
            tool["description"] = desc.description;
            if (!desc.schema.empty()) {
                tool["inputSchema"] = desc.schema;
            }
            return tool;
        });
        return json{{"tools", std::move(tools)}};
    }

private:
    struct AsyncToolDescriptor {
        std::string name;
        json schema;
        std::string description;
    };

    AsyncHandlerMap handlers_;
    std::vector<AsyncToolDescriptor> descriptors_;
};

} // namespace yams::mcp
