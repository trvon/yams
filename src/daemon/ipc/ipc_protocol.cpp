#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

// Trait to map request/response types to MessageType
template <typename T> struct MessageTypeTraits {};

// Request type mappings
template <> struct MessageTypeTraits<SearchRequest> {
    static constexpr MessageType value = MessageType::SearchRequest;
    static constexpr const char* name = "Search";
};
template <> struct MessageTypeTraits<GetRequest> {
    static constexpr MessageType value = MessageType::GetRequest;
    static constexpr const char* name = "Get";
};
template <> struct MessageTypeTraits<GetInitRequest> {
    static constexpr MessageType value = MessageType::GetInitRequest;
    static constexpr const char* name = "GetInit";
};
template <> struct MessageTypeTraits<GetChunkRequest> {
    static constexpr MessageType value = MessageType::GetChunkRequest;
    static constexpr const char* name = "GetChunk";
};
template <> struct MessageTypeTraits<GetEndRequest> {
    static constexpr MessageType value = MessageType::GetEndRequest;
    static constexpr const char* name = "GetEnd";
};
template <> struct MessageTypeTraits<DeleteRequest> {
    static constexpr MessageType value = MessageType::DeleteRequest;
    static constexpr const char* name = "Delete";
};
template <> struct MessageTypeTraits<ListRequest> {
    static constexpr MessageType value = MessageType::ListRequest;
    static constexpr const char* name = "List";
};
template <> struct MessageTypeTraits<ShutdownRequest> {
    static constexpr MessageType value = MessageType::ShutdownRequest;
    static constexpr const char* name = "Shutdown";
};
template <> struct MessageTypeTraits<StatusRequest> {
    static constexpr MessageType value = MessageType::StatusRequest;
    static constexpr const char* name = "Status";
};
template <> struct MessageTypeTraits<PingRequest> {
    static constexpr MessageType value = MessageType::PingRequest;
    static constexpr const char* name = "Ping";
};
template <> struct MessageTypeTraits<GenerateEmbeddingRequest> {
    static constexpr MessageType value = MessageType::GenerateEmbeddingRequest;
    static constexpr const char* name = "GenerateEmbedding";
};
template <> struct MessageTypeTraits<BatchEmbeddingRequest> {
    static constexpr MessageType value = MessageType::BatchEmbeddingRequest;
    static constexpr const char* name = "BatchEmbedding";
};
template <> struct MessageTypeTraits<LoadModelRequest> {
    static constexpr MessageType value = MessageType::LoadModelRequest;
    static constexpr const char* name = "LoadModel";
};
template <> struct MessageTypeTraits<UnloadModelRequest> {
    static constexpr MessageType value = MessageType::UnloadModelRequest;
    static constexpr const char* name = "UnloadModel";
};
template <> struct MessageTypeTraits<ModelStatusRequest> {
    static constexpr MessageType value = MessageType::ModelStatusRequest;
    static constexpr const char* name = "ModelStatus";
};
template <> struct MessageTypeTraits<AddDocumentRequest> {
    static constexpr MessageType value = MessageType::AddDocumentRequest;
    static constexpr const char* name = "AddDocument";
};
template <> struct MessageTypeTraits<GrepRequest> {
    static constexpr MessageType value = MessageType::GrepRequest;
    static constexpr const char* name = "Grep";
};
template <> struct MessageTypeTraits<UpdateDocumentRequest> {
    static constexpr MessageType value = MessageType::UpdateDocumentRequest;
    static constexpr const char* name = "UpdateDocument";
};
template <> struct MessageTypeTraits<DownloadRequest> {
    static constexpr MessageType value = MessageType::DownloadRequest;
    static constexpr const char* name = "Download";
};
template <> struct MessageTypeTraits<GetStatsRequest> {
    static constexpr MessageType value = MessageType::GetStatsRequest;
    static constexpr const char* name = "GetStats";
};
template <> struct MessageTypeTraits<CancelRequest> {
    static constexpr MessageType value = MessageType::CancelRequest;
    static constexpr const char* name = "Cancel";
};
template <> struct MessageTypeTraits<EmbedDocumentsRequest> {
    static constexpr MessageType value = MessageType::EmbedDocumentsRequest;
    static constexpr const char* name = "EmbedDocuments";
};
template <> struct MessageTypeTraits<CatRequest> {
    static constexpr MessageType value = MessageType::CatRequest;
    static constexpr const char* name = "Cat";
};
template <> struct MessageTypeTraits<ListSessionsRequest> {
    static constexpr MessageType value = MessageType::ListSessionsRequest;
    static constexpr const char* name = "ListSessions";
};
template <> struct MessageTypeTraits<UseSessionRequest> {
    static constexpr MessageType value = MessageType::UseSessionRequest;
    static constexpr const char* name = "UseSession";
};
template <> struct MessageTypeTraits<AddPathSelectorRequest> {
    static constexpr MessageType value = MessageType::AddPathSelectorRequest;
    static constexpr const char* name = "AddPathSelector";
};
template <> struct MessageTypeTraits<RemovePathSelectorRequest> {
    static constexpr MessageType value = MessageType::RemovePathSelectorRequest;
    static constexpr const char* name = "RemovePathSelector";
};
template <> struct MessageTypeTraits<FileHistoryRequest> {
    static constexpr MessageType value = MessageType::FileHistoryRequest;
    static constexpr const char* name = "FileHistory";
};
template <> struct MessageTypeTraits<PruneRequest> {
    static constexpr MessageType value = MessageType::PruneRequest;
    static constexpr const char* name = "Prune";
};
template <> struct MessageTypeTraits<PrepareSessionRequest> {
    static constexpr MessageType value = MessageType::PrepareSessionRequest;
    static constexpr const char* name = "PrepareSession";
};
// ListCollectionsRequest removed - use getMetadataValueCounts(["collection"], {})
template <> struct MessageTypeTraits<ListSnapshotsRequest> {
    static constexpr MessageType value = MessageType::ListSnapshotsRequest;
    static constexpr const char* name = "ListSnapshots";
};
template <> struct MessageTypeTraits<RestoreCollectionRequest> {
    static constexpr MessageType value = MessageType::RestoreCollectionRequest;
    static constexpr const char* name = "RestoreCollection";
};
template <> struct MessageTypeTraits<RestoreSnapshotRequest> {
    static constexpr MessageType value = MessageType::RestoreSnapshotRequest;
    static constexpr const char* name = "RestoreSnapshot";
};
template <> struct MessageTypeTraits<PluginScanRequest> {
    static constexpr MessageType value = MessageType::PluginScanRequest;
    static constexpr const char* name = "PluginScan";
};
template <> struct MessageTypeTraits<PluginLoadRequest> {
    static constexpr MessageType value = MessageType::PluginLoadRequest;
    static constexpr const char* name = "PluginLoad";
};
template <> struct MessageTypeTraits<PluginUnloadRequest> {
    static constexpr MessageType value = MessageType::PluginUnloadRequest;
    static constexpr const char* name = "PluginUnload";
};
template <> struct MessageTypeTraits<PluginTrustListRequest> {
    static constexpr MessageType value = MessageType::PluginTrustListRequest;
    static constexpr const char* name = "PluginTrustList";
};
template <> struct MessageTypeTraits<PluginTrustAddRequest> {
    static constexpr MessageType value = MessageType::PluginTrustAddRequest;
    static constexpr const char* name = "PluginTrustAdd";
};
template <> struct MessageTypeTraits<PluginTrustRemoveRequest> {
    static constexpr MessageType value = MessageType::PluginTrustRemoveRequest;
    static constexpr const char* name = "PluginTrustRemove";
};
template <> struct MessageTypeTraits<ListTreeDiffRequest> {
    static constexpr MessageType value = MessageType::ListTreeDiffRequest;
    static constexpr const char* name = "ListTreeDiff";
};
template <> struct MessageTypeTraits<GraphQueryRequest> {
    static constexpr MessageType value = MessageType::GraphQueryRequest;
    static constexpr const char* name = "GraphQuery";
};
template <> struct MessageTypeTraits<GraphPathHistoryRequest> {
    static constexpr MessageType value = MessageType::GraphPathHistoryRequest;
    static constexpr const char* name = "GraphPathHistory";
};
template <> struct MessageTypeTraits<GraphRepairRequest> {
    static constexpr MessageType value = MessageType::GraphRepairRequest;
    static constexpr const char* name = "GraphRepair";
};
template <> struct MessageTypeTraits<GraphValidateRequest> {
    static constexpr MessageType value = MessageType::GraphValidateRequest;
    static constexpr const char* name = "GraphValidate";
};
template <> struct MessageTypeTraits<KgIngestRequest> {
    static constexpr MessageType value = MessageType::KgIngestRequest;
    static constexpr const char* name = "KgIngest";
};
template <> struct MessageTypeTraits<MetadataValueCountsRequest> {
    static constexpr MessageType value = MessageType::MetadataValueCountsRequest;
    static constexpr const char* name = "MetadataValueCounts";
};
template <> struct MessageTypeTraits<BatchRequest> {
    static constexpr MessageType value = MessageType::BatchRequest;
    static constexpr const char* name = "Batch";
};

// Response type mappings
template <> struct MessageTypeTraits<SearchResponse> {
    static constexpr MessageType value = MessageType::SearchResponse;
};
template <> struct MessageTypeTraits<AddResponse> {
    static constexpr MessageType value = MessageType::AddResponse;
};
template <> struct MessageTypeTraits<GetResponse> {
    static constexpr MessageType value = MessageType::GetResponse;
};
template <> struct MessageTypeTraits<GetInitResponse> {
    static constexpr MessageType value = MessageType::GetInitResponse;
};
template <> struct MessageTypeTraits<GetChunkResponse> {
    static constexpr MessageType value = MessageType::GetChunkResponse;
};
template <> struct MessageTypeTraits<StatusResponse> {
    static constexpr MessageType value = MessageType::StatusResponse;
};
template <> struct MessageTypeTraits<SuccessResponse> {
    static constexpr MessageType value = MessageType::SuccessResponse;
};
template <> struct MessageTypeTraits<ErrorResponse> {
    static constexpr MessageType value = MessageType::ErrorResponse;
};
template <> struct MessageTypeTraits<PongResponse> {
    static constexpr MessageType value = MessageType::PongResponse;
};
template <> struct MessageTypeTraits<EmbeddingResponse> {
    static constexpr MessageType value = MessageType::EmbeddingResponse;
};
template <> struct MessageTypeTraits<BatchEmbeddingResponse> {
    static constexpr MessageType value = MessageType::BatchEmbeddingResponse;
};
template <> struct MessageTypeTraits<ModelLoadResponse> {
    static constexpr MessageType value = MessageType::ModelLoadResponse;
};
template <> struct MessageTypeTraits<ModelStatusResponse> {
    static constexpr MessageType value = MessageType::ModelStatusResponse;
};
template <> struct MessageTypeTraits<ListResponse> {
    static constexpr MessageType value = MessageType::ListResponse;
};
template <> struct MessageTypeTraits<AddDocumentResponse> {
    static constexpr MessageType value = MessageType::AddDocumentResponse;
};
template <> struct MessageTypeTraits<GrepResponse> {
    static constexpr MessageType value = MessageType::GrepResponse;
};
template <> struct MessageTypeTraits<UpdateDocumentResponse> {
    static constexpr MessageType value = MessageType::UpdateDocumentResponse;
};
template <> struct MessageTypeTraits<GetStatsResponse> {
    static constexpr MessageType value = MessageType::GetStatsResponse;
};
template <> struct MessageTypeTraits<DownloadResponse> {
    static constexpr MessageType value = MessageType::DownloadResponse;
};
template <> struct MessageTypeTraits<DeleteResponse> {
    static constexpr MessageType value = MessageType::DeleteResponse;
};
template <> struct MessageTypeTraits<PrepareSessionResponse> {
    static constexpr MessageType value = MessageType::PrepareSessionResponse;
};
template <> struct MessageTypeTraits<EmbedDocumentsResponse> {
    static constexpr MessageType value = MessageType::EmbedDocumentsResponse;
};
template <> struct MessageTypeTraits<CatResponse> {
    static constexpr MessageType value = MessageType::CatResponse;
};
template <> struct MessageTypeTraits<ListSessionsResponse> {
    static constexpr MessageType value = MessageType::ListSessionsResponse;
};
template <> struct MessageTypeTraits<FileHistoryResponse> {
    static constexpr MessageType value = MessageType::FileHistoryResponse;
};
template <> struct MessageTypeTraits<PruneResponse> {
    static constexpr MessageType value = MessageType::PruneResponse;
};
// ListCollectionsResponse removed - use getMetadataValueCounts(["collection"], {})
template <> struct MessageTypeTraits<ListSnapshotsResponse> {
    static constexpr MessageType value = MessageType::ListSnapshotsResponse;
};
template <> struct MessageTypeTraits<RestoreCollectionResponse> {
    static constexpr MessageType value = MessageType::RestoreCollectionResponse;
};
template <> struct MessageTypeTraits<RestoreSnapshotResponse> {
    static constexpr MessageType value = MessageType::RestoreSnapshotResponse;
};
template <> struct MessageTypeTraits<EmbeddingEvent> {
    static constexpr MessageType value = MessageType::EmbeddingEvent;
};
template <> struct MessageTypeTraits<ModelLoadEvent> {
    static constexpr MessageType value = MessageType::ModelLoadEvent;
};
template <> struct MessageTypeTraits<ListTreeDiffResponse> {
    static constexpr MessageType value = MessageType::ListTreeDiffResponse;
};
template <> struct MessageTypeTraits<PluginScanResponse> {
    static constexpr MessageType value = MessageType::PluginScanResponse;
};
template <> struct MessageTypeTraits<PluginLoadResponse> {
    static constexpr MessageType value = MessageType::PluginLoadResponse;
};
template <> struct MessageTypeTraits<PluginTrustListResponse> {
    static constexpr MessageType value = MessageType::PluginTrustListResponse;
};
template <> struct MessageTypeTraits<GraphQueryResponse> {
    static constexpr MessageType value = MessageType::GraphQueryResponse;
};
template <> struct MessageTypeTraits<GraphPathHistoryResponse> {
    static constexpr MessageType value = MessageType::GraphPathHistoryResponse;
};
template <> struct MessageTypeTraits<GraphRepairResponse> {
    static constexpr MessageType value = MessageType::GraphRepairResponse;
};
template <> struct MessageTypeTraits<GraphValidateResponse> {
    static constexpr MessageType value = MessageType::GraphValidateResponse;
};
template <> struct MessageTypeTraits<KgIngestResponse> {
    static constexpr MessageType value = MessageType::KgIngestResponse;
};
template <> struct MessageTypeTraits<MetadataValueCountsResponse> {
    static constexpr MessageType value = MessageType::MetadataValueCountsResponse;
};
template <> struct MessageTypeTraits<BatchResponse> {
    static constexpr MessageType value = MessageType::BatchResponse;
};

MessageType getMessageType(const Request& req) {
    return std::visit(
        [](auto&& r) -> MessageType {
            using T = std::decay_t<decltype(r)>;
            return MessageTypeTraits<T>::value;
        },
        req);
}

MessageType getMessageType(const Response& res) {
    return std::visit(
        [](auto&& r) -> MessageType {
            using T = std::decay_t<decltype(r)>;
            return MessageTypeTraits<T>::value;
        },
        res);
}

std::string getRequestName(const Request& req) {
    return std::visit(
        [](auto&& r) -> std::string {
            using T = std::decay_t<decltype(r)>;
            return MessageTypeTraits<T>::name;
        },
        req);
}

} // namespace yams::daemon
