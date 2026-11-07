#pragma once

#include <yams/daemon/ipc/ipc_protocol_requests.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

namespace yams::daemon {

// ============================================================================
// Message Envelope
// ============================================================================

struct Message {
    uint32_t version = 1;
    uint64_t requestId;
    std::chrono::steady_clock::time_point timestamp;

    // Payload
    std::variant<Request, Response> payload;

    // Optional fields
    std::optional<std::string> sessionId = std::nullopt;
    std::optional<std::string> instanceId = std::nullopt;
    std::optional<std::string> clientVersion = std::nullopt;

    // Streaming preference - client indicates if it expects chunked/streaming response
    bool expectsStreamingResponse = false;
};

// ============================================================================
// Protocol Constants
// ============================================================================

constexpr uint32_t PROTOCOL_VERSION = 2;
constexpr size_t MAX_MESSAGE_SIZE =
    static_cast<size_t>(16) * static_cast<size_t>(1024) * static_cast<size_t>(1024); // 16MB
constexpr size_t HEADER_SIZE = 16; // version(4) + size(4) + requestId(8)

// Message type tags for serialization
enum class MessageType : uint8_t {
    // Retired values, never reuse: 26, 28, 29, 30, 44, 72, 153, 154, 165, 170, 179, 180, 181,
    // 182 (removed ListSessions, path-selector, tree-diff, graph impact/tests/path-history and
    // batch messages).
    // Requests
    SearchRequest = 1,
    AddRequest = 2,
    GetRequest = 3,
    GetInitRequest = 14,
    GetChunkRequest = 15,
    GetEndRequest = 16,
    DeleteRequest = 4,
    ListRequest = 5,
    ShutdownRequest = 6,
    StatusRequest = 7,
    PingRequest = 8,
    GenerateEmbeddingRequest = 9,
    BatchEmbeddingRequest = 10,
    LoadModelRequest = 11,
    UnloadModelRequest = 12,
    ModelStatusRequest = 13,
    DownloadRequest = 17,
    DownloadStatusRequest = 74,
    CancelDownloadJobRequest = 75,
    ListDownloadJobsRequest = 76,
    AddDocumentRequest = 18,
    GrepRequest = 19,
    UpdateDocumentRequest = 20,
    GetStatsRequest = 21,
    CancelRequest = 22,
    PrepareSessionRequest = 23,
    EmbedDocumentsRequest = 24,
    // Session and utility requests
    CatRequest = 25,

    UseSessionRequest = 27,

    // Tree diff requests (PBI-043)

    // File history request (PBI-043 enhancement)
    FileHistoryRequest = 31,
    // Prune request (PBI-062)
    PruneRequest = 32,
    // Snapshot requests (PBI-066) - collections use generic metadata query
    ListSnapshotsRequest = 34,
    RestoreCollectionRequest = 35,
    RestoreSnapshotRequest = 36,
    // Plugin requests
    PluginScanRequest = 37,
    PluginLoadRequest = 38,
    PluginUnloadRequest = 39,
    PluginTrustListRequest = 40,
    PluginTrustAddRequest = 41,
    PluginTrustRemoveRequest = 42,
    // Graph query requests (PBI-009)
    GraphQueryRequest = 43,
    GraphExploreRequest = 77,
    GraphSymbolLookupRequest = 175,
    GraphTraceRequest = 177,

    // Graph maintenance requests (PBI-009 Phase 4.3)
    GraphRepairRequest = 45,
    GraphValidateRequest = 46,
    // Lifecycle-owned memory sync operations.
    MemorySyncRequest = 78,
    UnlinkPathRequest = 79,

    // Responses
    SearchResponse = 128,
    AddResponse = 129,
    GetResponse = 130,
    GetInitResponse = 139,
    GetChunkResponse = 140,
    StatusResponse = 131,
    SuccessResponse = 132,
    ErrorResponse = 133,
    PongResponse = 134,
    EmbeddingResponse = 135,
    BatchEmbeddingResponse = 136,
    ModelLoadResponse = 137,
    ModelStatusResponse = 138,
    ListResponse = 141,
    DownloadResponse = 142,
    ListDownloadJobsResponse = 173,
    AddDocumentResponse = 143,
    GrepResponse = 144,
    UpdateDocumentResponse = 145,
    GetStatsResponse = 146,
    DeleteResponse = 147,
    PrepareSessionResponse = 148,
    CatResponse = 152,

    // Tree diff responses (PBI-043)

    // File history response (PBI-043 enhancement)
    FileHistoryResponse = 155,
    // Prune response (PBI-062)
    PruneResponse = 156,
    // Snapshot responses (PBI-066) - collections use generic metadata query
    ListSnapshotsResponse = 158,
    RestoreCollectionResponse = 159,
    RestoreSnapshotResponse = 160,
    // Plugin responses
    PluginScanResponse = 161,
    PluginLoadResponse = 162,
    PluginTrustListResponse = 163,
    // Graph query responses (PBI-009)
    GraphQueryResponse = 164,
    GraphExploreResponse = 174,
    GraphSymbolLookupResponse = 176,
    GraphTraceResponse = 178,

    // Graph maintenance responses (PBI-009 Phase 4.3)
    GraphRepairResponse = 166,
    GraphValidateResponse = 167,
    MemorySyncResponse = 183,
    // KG ingest (PBI-093 Phase 2)
    KgIngestRequest = 70,
    KgIngestResponse = 168,
    // Generic metadata value counts query (MCP client mode)
    MetadataValueCountsRequest = 71,
    // Batch request (Track B: Communication Overhead Reduction)

    MetadataValueCountsResponse = 169,
    // Batch response (Track B: Communication Overhead Reduction)

    // Repair service
    RepairRequest_MsgType = 73,
    RepairResponse_MsgType = 171,
    RepairEvent_MsgType = 172,
    // Events
    EmbeddingEvent = 149,
    ModelLoadEvent = 150,
    EmbedDocumentsResponse = 151
};

// ============================================================================
// Helper Functions
// ============================================================================

// Get message type from variant
MessageType getMessageType(const Request& req);
MessageType getMessageType(const Response& res);

// Request and response names for logging and protocol diagnostics
std::string getRequestName(const Request& req);
std::string getResponseName(const Response& res);

} // namespace yams::daemon
