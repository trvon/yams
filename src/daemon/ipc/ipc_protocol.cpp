#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

MessageType getMessageType(const Request& req) {
    return std::visit(
        [](auto&& r) -> MessageType {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, SearchRequest>) {
                return MessageType::SearchRequest;
            } else if constexpr (std::is_same_v<T, AddRequest>) {
                return MessageType::AddRequest;
            } else if constexpr (std::is_same_v<T, GetRequest>) {
                return MessageType::GetRequest;
            } else if constexpr (std::is_same_v<T, GetInitRequest>) {
                return MessageType::GetInitRequest;
            } else if constexpr (std::is_same_v<T, GetChunkRequest>) {
                return MessageType::GetChunkRequest;
            } else if constexpr (std::is_same_v<T, GetEndRequest>) {
                return MessageType::GetEndRequest;
            } else if constexpr (std::is_same_v<T, DeleteRequest>) {
                return MessageType::DeleteRequest;
            } else if constexpr (std::is_same_v<T, ListRequest>) {
                return MessageType::ListRequest;
            } else if constexpr (std::is_same_v<T, ShutdownRequest>) {
                return MessageType::ShutdownRequest;
            } else if constexpr (std::is_same_v<T, StatusRequest>) {
                return MessageType::StatusRequest;
            } else if constexpr (std::is_same_v<T, PingRequest>) {
                return MessageType::PingRequest;
            } else if constexpr (std::is_same_v<T, GenerateEmbeddingRequest>) {
                return MessageType::GenerateEmbeddingRequest;
            } else if constexpr (std::is_same_v<T, BatchEmbeddingRequest>) {
                return MessageType::BatchEmbeddingRequest;
            } else if constexpr (std::is_same_v<T, LoadModelRequest>) {
                return MessageType::LoadModelRequest;
            } else if constexpr (std::is_same_v<T, UnloadModelRequest>) {
                return MessageType::UnloadModelRequest;
            } else if constexpr (std::is_same_v<T, ModelStatusRequest>) {
                return MessageType::ModelStatusRequest;
            } else if constexpr (std::is_same_v<T, AddDocumentRequest>) {
                return MessageType::AddDocumentRequest;
            } else if constexpr (std::is_same_v<T, GrepRequest>) {
                return MessageType::GrepRequest;
            } else if constexpr (std::is_same_v<T, UpdateDocumentRequest>) {
                return MessageType::UpdateDocumentRequest;
            } else if constexpr (std::is_same_v<T, DownloadRequest>) {
                return MessageType::DownloadRequest;
            } else if constexpr (std::is_same_v<T, GetStatsRequest>) {
                return MessageType::GetStatsRequest;
            } else if constexpr (std::is_same_v<T, CancelRequest>) {
                return MessageType::CancelRequest;
            }
            return MessageType::StatusRequest; // Default
        },
        req);
}

MessageType getMessageType(const Response& res) {
    return std::visit(
        [](auto&& r) -> MessageType {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, SearchResponse>) {
                return MessageType::SearchResponse;
            } else if constexpr (std::is_same_v<T, AddResponse>) {
                return MessageType::AddResponse;
            } else if constexpr (std::is_same_v<T, GetResponse>) {
                return MessageType::GetResponse;
            } else if constexpr (std::is_same_v<T, GetInitResponse>) {
                return MessageType::GetInitResponse;
            } else if constexpr (std::is_same_v<T, GetChunkResponse>) {
                return MessageType::GetChunkResponse;
            } else if constexpr (std::is_same_v<T, StatusResponse>) {
                return MessageType::StatusResponse;
            } else if constexpr (std::is_same_v<T, SuccessResponse>) {
                return MessageType::SuccessResponse;
            } else if constexpr (std::is_same_v<T, ErrorResponse>) {
                return MessageType::ErrorResponse;
            } else if constexpr (std::is_same_v<T, PongResponse>) {
                return MessageType::PongResponse;
            } else if constexpr (std::is_same_v<T, EmbeddingResponse>) {
                return MessageType::EmbeddingResponse;
            } else if constexpr (std::is_same_v<T, BatchEmbeddingResponse>) {
                return MessageType::BatchEmbeddingResponse;
            } else if constexpr (std::is_same_v<T, ModelLoadResponse>) {
                return MessageType::ModelLoadResponse;
            } else if constexpr (std::is_same_v<T, ModelStatusResponse>) {
                return MessageType::ModelStatusResponse;
            } else if constexpr (std::is_same_v<T, ListResponse>) {
                return MessageType::ListResponse;
            } else if constexpr (std::is_same_v<T, AddDocumentResponse>) {
                return MessageType::AddDocumentResponse;
            } else if constexpr (std::is_same_v<T, GrepResponse>) {
                return MessageType::GrepResponse;
            } else if constexpr (std::is_same_v<T, UpdateDocumentResponse>) {
                return MessageType::UpdateDocumentResponse;
            } else if constexpr (std::is_same_v<T, DownloadResponse>) {
                return MessageType::DownloadResponse;
            } else if constexpr (std::is_same_v<T, GetStatsResponse>) {
                return MessageType::GetStatsResponse;
            } else if constexpr (std::is_same_v<T, DeleteResponse>) {
                return MessageType::DeleteResponse;
            }
            return MessageType::ErrorResponse; // Default
        },
        res);
}

std::string getRequestName(const Request& req) {
    return std::visit(
        [](auto&& r) -> std::string {
            using T = std::decay_t<decltype(r)>;

            if constexpr (std::is_same_v<T, SearchRequest>) {
                return "Search";
            } else if constexpr (std::is_same_v<T, AddRequest>) {
                return "Add";
            } else if constexpr (std::is_same_v<T, GetRequest>) {
                return "Get";
            } else if constexpr (std::is_same_v<T, GetInitRequest>) {
                return "GetInit";
            } else if constexpr (std::is_same_v<T, GetChunkRequest>) {
                return "GetChunk";
            } else if constexpr (std::is_same_v<T, GetEndRequest>) {
                return "GetEnd";
            } else if constexpr (std::is_same_v<T, DeleteRequest>) {
                return "Delete";
            } else if constexpr (std::is_same_v<T, ListRequest>) {
                return "List";
            } else if constexpr (std::is_same_v<T, ShutdownRequest>) {
                return "Shutdown";
            } else if constexpr (std::is_same_v<T, StatusRequest>) {
                return "Status";
            } else if constexpr (std::is_same_v<T, PingRequest>) {
                return "Ping";
            } else if constexpr (std::is_same_v<T, GenerateEmbeddingRequest>) {
                return "GenerateEmbedding";
            } else if constexpr (std::is_same_v<T, BatchEmbeddingRequest>) {
                return "BatchEmbedding";
            } else if constexpr (std::is_same_v<T, LoadModelRequest>) {
                return "LoadModel";
            } else if constexpr (std::is_same_v<T, UnloadModelRequest>) {
                return "UnloadModel";
            } else if constexpr (std::is_same_v<T, ModelStatusRequest>) {
                return "ModelStatus";
            }
            return "Unknown";
        },
        req);
}

} // namespace yams::daemon
