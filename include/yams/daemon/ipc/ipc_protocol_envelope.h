#pragma once

#include <yams/daemon/ipc/ipc_protocol_requests.h>
#include <yams/daemon/ipc/ipc_protocol_responses.h>

namespace yams::daemon {

// ============================================================================
// Batch Request/Response Types (Track B: Communication Overhead Reduction)
// ============================================================================

struct BatchItem;

struct BatchItemResponse {
    uint32_t sequenceId = 0;
    bool success = false;
    std::variant<SearchResponse, GetResponse, AddDocumentResponse, GrepResponse,
                 UpdateDocumentResponse, DeleteResponse, ListResponse, GraphQueryResponse,
                 ErrorResponse>
        response;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << sequenceId << success;
        std::visit(
            [&ser](const auto& r) {
                using T = std::decay_t<decltype(r)>;
                if constexpr (std::is_same_v<T, SearchResponse>)
                    ser << uint8_t(0);
                else if constexpr (std::is_same_v<T, GetResponse>)
                    ser << uint8_t(1);
                else if constexpr (std::is_same_v<T, AddDocumentResponse>)
                    ser << uint8_t(2);
                else if constexpr (std::is_same_v<T, GrepResponse>)
                    ser << uint8_t(3);
                else if constexpr (std::is_same_v<T, UpdateDocumentResponse>)
                    ser << uint8_t(4);
                else if constexpr (std::is_same_v<T, DeleteResponse>)
                    ser << uint8_t(5);
                else if constexpr (std::is_same_v<T, ListResponse>)
                    ser << uint8_t(6);
                else if constexpr (std::is_same_v<T, GraphQueryResponse>)
                    ser << uint8_t(7);
                else if constexpr (std::is_same_v<T, ErrorResponse>)
                    ser << uint8_t(8);
                ser << r;
            },
            response);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<BatchItemResponse> deserialize(Deserializer& deser) {
        BatchItemResponse resp;
        auto seq = deser.template read<uint32_t>();
        if (!seq)
            return seq.error();
        resp.sequenceId = seq.value();
        auto succ = deser.template read<bool>();
        if (!succ)
            return succ.error();
        resp.success = succ.value();
        auto type = deser.template read<uint8_t>();
        if (!type)
            return type.error();
        switch (type.value()) {
            case 0: {
                auto r = SearchResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 1: {
                auto r = GetResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 2: {
                auto r = AddDocumentResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 3: {
                auto r = GrepResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 4: {
                auto r = UpdateDocumentResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 5: {
                auto r = DeleteResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 6: {
                auto r = ListResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 7: {
                auto r = GraphQueryResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            case 8: {
                auto r = ErrorResponse::deserialize(deser);
                if (!r)
                    return r.error();
                resp.response = r.value();
                break;
            }
            default:
                return Error{ErrorCode::InvalidData, "Unknown BatchItemResponse type"};
        }
        return resp;
    }
};

struct BatchResponse {
    std::vector<BatchItemResponse> items;
    uint32_t totalCount = 0;
    uint32_t successCount = 0;
    uint32_t errorCount = 0;
    std::chrono::milliseconds elapsed{0};

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << items << totalCount << successCount << errorCount << elapsed;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<BatchResponse> deserialize(Deserializer& deser) {
        BatchResponse resp;
        auto items = deser.template readVector<BatchItemResponse>();
        if (!items)
            return items.error();
        resp.items = items.value();
        auto total = deser.template read<uint32_t>();
        if (!total)
            return total.error();
        resp.totalCount = total.value();
        auto success = deser.template read<uint32_t>();
        if (!success)
            return success.error();
        resp.successCount = success.value();
        auto error = deser.template read<uint32_t>();
        if (!error)
            return error.error();
        resp.errorCount = error.value();
        auto elapsed = deser.template readDuration<std::chrono::milliseconds>();
        if (elapsed)
            resp.elapsed = elapsed.value();
        return resp;
    }
};

struct BatchItem {
    uint32_t sequenceId = 0;
    std::variant<SearchRequest, GetRequest, AddDocumentRequest, GrepRequest, UpdateDocumentRequest,
                 DeleteRequest, ListRequest, GraphQueryRequest>
        request;

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << sequenceId;
        std::visit(
            [&ser](const auto& r) {
                using T = std::decay_t<decltype(r)>;
                if constexpr (std::is_same_v<T, SearchRequest>)
                    ser << uint8_t(0);
                else if constexpr (std::is_same_v<T, GetRequest>)
                    ser << uint8_t(1);
                else if constexpr (std::is_same_v<T, AddDocumentRequest>)
                    ser << uint8_t(2);
                else if constexpr (std::is_same_v<T, GrepRequest>)
                    ser << uint8_t(3);
                else if constexpr (std::is_same_v<T, UpdateDocumentRequest>)
                    ser << uint8_t(4);
                else if constexpr (std::is_same_v<T, DeleteRequest>)
                    ser << uint8_t(5);
                else if constexpr (std::is_same_v<T, ListRequest>)
                    ser << uint8_t(6);
                else if constexpr (std::is_same_v<T, GraphQueryRequest>)
                    ser << uint8_t(7);
                ser << r;
            },
            request);
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<BatchItem> deserialize(Deserializer& deser) {
        BatchItem item;
        auto seq = deser.template read<uint32_t>();
        if (!seq)
            return seq.error();
        item.sequenceId = seq.value();
        auto type = deser.template read<uint8_t>();
        if (!type)
            return type.error();
        switch (type.value()) {
            case 0: {
                auto r = SearchRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 1: {
                auto r = GetRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 2: {
                auto r = AddDocumentRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 3: {
                auto r = GrepRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 4: {
                auto r = UpdateDocumentRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 5: {
                auto r = DeleteRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 6: {
                auto r = ListRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            case 7: {
                auto r = GraphQueryRequest::deserialize(deser);
                if (!r)
                    return r.error();
                item.request = r.value();
                break;
            }
            default:
                return Error{ErrorCode::InvalidData, "Unknown BatchItem request type"};
        }
        return item;
    }
};

struct BatchRequest {
    std::vector<BatchItem> items;
    bool continueOnError = false;
    std::string sessionId;
    std::string instanceId; // Instance-level isolation

    template <typename Serializer>
    requires IsSerializer<Serializer>
    void serialize(Serializer& ser) const {
        ser << items << continueOnError << sessionId << instanceId;
    }

    template <typename Deserializer>
    requires IsDeserializer<Deserializer>
    static Result<BatchRequest> deserialize(Deserializer& deser) {
        BatchRequest req;
        auto items = deser.template readVector<BatchItem>();
        if (!items)
            return items.error();
        req.items = items.value();
        auto cont = deser.template read<bool>();
        if (!cont)
            return cont.error();
        req.continueOnError = cont.value();
        auto session = deser.readString();
        if (session)
            req.sessionId = session.value();
        if (auto ii = deser.readString(); ii)
            req.instanceId = std::move(ii.value());
        return req;
    }
};

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
    ListSessionsRequest = 26,
    UseSessionRequest = 27,
    AddPathSelectorRequest = 28,
    RemovePathSelectorRequest = 29,
    // Tree diff requests (PBI-043)
    ListTreeDiffRequest = 30,
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
    GraphImpactRequest = 179,
    GraphAffectedTestsRequest = 181,
    GraphPathHistoryRequest = 44,
    // Graph maintenance requests (PBI-009 Phase 4.3)
    GraphRepairRequest = 45,
    GraphValidateRequest = 46,

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
    ListSessionsResponse = 153,
    // Tree diff responses (PBI-043)
    ListTreeDiffResponse = 154,
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
    GraphImpactResponse = 180,
    GraphAffectedTestsResponse = 182,
    GraphPathHistoryResponse = 165,
    // Graph maintenance responses (PBI-009 Phase 4.3)
    GraphRepairResponse = 166,
    GraphValidateResponse = 167,
    // KG ingest (PBI-093 Phase 2)
    KgIngestRequest = 70,
    KgIngestResponse = 168,
    // Generic metadata value counts query (MCP client mode)
    MetadataValueCountsRequest = 71,
    // Batch request (Track B: Communication Overhead Reduction)
    BatchRequest = 72,
    MetadataValueCountsResponse = 169,
    // Batch response (Track B: Communication Overhead Reduction)
    BatchResponse = 170,
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

// Request name for logging
std::string getRequestName(const Request& req);

} // namespace yams::daemon
