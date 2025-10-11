// Lightweight mapping from request types to their response types.
#pragma once

#include <type_traits>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

template <typename Req>
struct ResponseOf; // primary template left undefined to force specializations

template <> struct ResponseOf<SearchRequest> {
    using type = SearchResponse;
};
template <> struct ResponseOf<GetRequest> {
    using type = GetResponse;
};
template <> struct ResponseOf<GetInitRequest> {
    using type = GetInitResponse;
};
template <> struct ResponseOf<GetChunkRequest> {
    using type = GetChunkResponse;
};
template <> struct ResponseOf<GetEndRequest> {
    using type = SuccessResponse;
};
template <> struct ResponseOf<ListRequest> {
    using type = ListResponse;
};
template <> struct ResponseOf<DeleteRequest> {
    using type = DeleteResponse;
};
template <> struct ResponseOf<StatusRequest> {
    using type = StatusResponse;
};
template <> struct ResponseOf<PingRequest> {
    using type = PongResponse;
};

template <> struct ResponseOf<GenerateEmbeddingRequest> {
    using type = EmbeddingResponse;
};
template <> struct ResponseOf<BatchEmbeddingRequest> {
    using type = BatchEmbeddingResponse;
};
template <> struct ResponseOf<LoadModelRequest> {
    using type = ModelLoadResponse;
};
template <> struct ResponseOf<UnloadModelRequest> {
    using type = SuccessResponse;
};
template <> struct ResponseOf<ModelStatusRequest> {
    using type = ModelStatusResponse;
};

template <> struct ResponseOf<AddDocumentRequest> {
    using type = AddDocumentResponse;
};
template <> struct ResponseOf<GrepRequest> {
    using type = GrepResponse;
};
template <> struct ResponseOf<FileHistoryRequest> {
    using type = FileHistoryResponse;
};
template <> struct ResponseOf<UpdateDocumentRequest> {
    using type = UpdateDocumentResponse;
};
template <> struct ResponseOf<GetStatsRequest> {
    using type = GetStatsResponse;
};
template <> struct ResponseOf<PrepareSessionRequest> {
    using type = PrepareSessionResponse;
};
template <> struct ResponseOf<DownloadRequest> {
    using type = DownloadResponse;
};
// Embedding documents (persist)
template <> struct ResponseOf<EmbedDocumentsRequest> {
    using type = EmbedDocumentsResponse;
};

// Plugin management requests
template <> struct ResponseOf<PluginScanRequest> {
    using type = PluginScanResponse;
};
template <> struct ResponseOf<PluginLoadRequest> {
    using type = PluginLoadResponse;
};
template <> struct ResponseOf<PluginUnloadRequest> {
    using type = SuccessResponse;
};
template <> struct ResponseOf<PluginTrustListRequest> {
    using type = PluginTrustListResponse;
};
template <> struct ResponseOf<PluginTrustAddRequest> {
    using type = SuccessResponse;
};
template <> struct ResponseOf<PluginTrustRemoveRequest> {
    using type = SuccessResponse;
};

// Helper alias
template <typename Req> using ResponseOfT = typename ResponseOf<Req>::type;

} // namespace yams::daemon
