#pragma once

#include <memory>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <yams/daemon/ipc/ipc_protocol.h>

namespace yams::daemon {

class YamsDaemon;
class ServiceManager;
struct StateComponent;

} // namespace yams::daemon

namespace yams::metadata {
class MetadataRepository;
class KnowledgeGraphStore;
} // namespace yams::metadata

namespace yams::api {
class IContentStore;
}

namespace yams::daemon {

// Forward declare trait template for friend declaration
template <typename T> struct RequestHandlerTraits;

class RequestDispatcher {
public:
    RequestDispatcher(YamsDaemon* daemon, ServiceManager* serviceManager, StateComponent* state);
    // Overload with metrics component for centralized status rendering
    RequestDispatcher(YamsDaemon* daemon, ServiceManager* serviceManager, StateComponent* state,
                      class DaemonMetrics* metrics);
    ~RequestDispatcher();

    boost::asio::awaitable<Response> dispatch(const Request& req);
    // Expose worker executor so SocketServer can pass it into RequestHandler config
    boost::asio::any_io_executor getWorkerExecutor() const;
    boost::asio::any_io_executor getCliExecutor() const;
    std::function<void(bool)> getWorkerJobSignal() const;
    // Optional accessor, used internally or by tests
    ServiceManager* getServiceManager() const;

    // PBI-008-11: Session prepare (warming) options and entrypoint
    struct PrepareSessionOptions {
        std::string sessionName;
        int maxCores{-1};
        int maxMemoryGb{-1};
        long maxTimeMs{-1};
        bool aggressive{false};
        std::size_t limit{200};
        std::size_t snippetLen{160};
    };

    // Best-effort synchronous prepare that delegates to ISessionService
    // Returns number of warmed documents or negative on error
    int prepareSession(const PrepareSessionOptions& opts);

private:
    template <typename T> friend struct RequestHandlerTraits;

    // One handler for each request type
    boost::asio::awaitable<Response> handleStatusRequest(const StatusRequest& req);
    boost::asio::awaitable<Response> handleShutdownRequest(const ShutdownRequest& req);
    boost::asio::awaitable<Response> handleCancelRequest(const CancelRequest& req);
    boost::asio::awaitable<Response> handleSearchRequest(const SearchRequest& req);
    boost::asio::awaitable<Response> handleGetRequest(const GetRequest& req);
    boost::asio::awaitable<Response> handleGetInitRequest(const GetInitRequest& req);
    boost::asio::awaitable<Response> handleGetChunkRequest(const GetChunkRequest& req);
    boost::asio::awaitable<Response> handleGetEndRequest(const GetEndRequest& req);
    boost::asio::awaitable<Response> handleCatRequest(const CatRequest& req);
    boost::asio::awaitable<Response> handleAddDocumentRequest(const AddDocumentRequest& req);
    boost::asio::awaitable<Response> handleListRequest(const ListRequest& req);
    boost::asio::awaitable<Response> handleDeleteRequest(const DeleteRequest& req);
    boost::asio::awaitable<Response> handleGetStatsRequest(const GetStatsRequest& req);
    boost::asio::awaitable<Response> handlePingRequest(const PingRequest& req);
    boost::asio::awaitable<Response> handleUpdateDocumentRequest(const UpdateDocumentRequest& req);
    boost::asio::awaitable<Response> handleGrepRequest(const GrepRequest& req);
    boost::asio::awaitable<Response> handleDownloadRequest(const DownloadRequest& req);
    boost::asio::awaitable<Response> handlePrepareSessionRequest(const PrepareSessionRequest& req);

    // Session management handlers
    boost::asio::awaitable<Response> handleListSessionsRequest(const ListSessionsRequest& req);
    boost::asio::awaitable<Response> handleUseSessionRequest(const UseSessionRequest& req);
    boost::asio::awaitable<Response>
    handleAddPathSelectorRequest(const AddPathSelectorRequest& req);
    boost::asio::awaitable<Response>
    handleRemovePathSelectorRequest(const RemovePathSelectorRequest& req);

    // Tree diff handlers (PBI-043)
    boost::asio::awaitable<Response> handleListTreeDiffRequest(const ListTreeDiffRequest& req);

    // File history handler (PBI-043)
    boost::asio::awaitable<Response> handleFileHistoryRequest(const FileHistoryRequest& req);

    // Prune handler (PBI-062)
    boost::asio::awaitable<Response> handlePruneRequest(const PruneRequest& req);

    // Collection and snapshot handlers (PBI-066)
    boost::asio::awaitable<Response>
    handleListCollectionsRequest(const ListCollectionsRequest& req);
    boost::asio::awaitable<Response> handleListSnapshotsRequest(const ListSnapshotsRequest& req);
    boost::asio::awaitable<Response>
    handleRestoreCollectionRequest(const RestoreCollectionRequest& req);
    boost::asio::awaitable<Response>
    handleRestoreSnapshotRequest(const RestoreSnapshotRequest& req);

    // Plugin management handlers
    boost::asio::awaitable<Response> handlePluginScanRequest(const PluginScanRequest& req);
    boost::asio::awaitable<Response> handlePluginLoadRequest(const PluginLoadRequest& req);
    boost::asio::awaitable<Response>
    handlePluginUnloadRequest(const PluginUnloadRequest& req) const;
    boost::asio::awaitable<Response>
    handlePluginTrustListRequest(const PluginTrustListRequest& req) const;
    boost::asio::awaitable<Response>
    handlePluginTrustAddRequest(const PluginTrustAddRequest& req) const;
    boost::asio::awaitable<Response>
    handlePluginTrustRemoveRequest(const PluginTrustRemoveRequest& req) const;

    // Embedding/model provider handlers
    boost::asio::awaitable<Response>
    handleGenerateEmbeddingRequest(const GenerateEmbeddingRequest& req);
    boost::asio::awaitable<Response> handleBatchEmbeddingRequest(const BatchEmbeddingRequest& req);
    boost::asio::awaitable<Response> handleEmbedDocumentsRequest(const EmbedDocumentsRequest& req);
    boost::asio::awaitable<Response> handleLoadModelRequest(const LoadModelRequest& req);
    boost::asio::awaitable<Response> handleUnloadModelRequest(const UnloadModelRequest& req);
    boost::asio::awaitable<Response> handleModelStatusRequest(const ModelStatusRequest& req);

    // Graph query handlers (PBI-009)
    boost::asio::awaitable<Response> handleGraphQueryRequest(const GraphQueryRequest& req);
    boost::asio::awaitable<Response>
    handleGraphPathHistoryRequest(const GraphPathHistoryRequest& req);
    // PBI-093: Helper for listByType mode
    boost::asio::awaitable<Response>
    handleGraphQueryListByType(const GraphQueryRequest& req,
                               yams::metadata::KnowledgeGraphStore* kgStore);
    boost::asio::awaitable<Response>
    handleGraphQueryIsolatedMode(const GraphQueryRequest& req,
                                 yams::metadata::KnowledgeGraphStore* kgStore);
    // yams-66h: Helper for listTypes mode
    boost::asio::awaitable<Response>
    handleGraphQueryListTypes(const GraphQueryRequest& req,
                              yams::metadata::KnowledgeGraphStore* kgStore);
    // yams-kt5t: Helper for listRelations mode (relation type statistics)
    boost::asio::awaitable<Response>
    handleGraphQueryListRelations(const GraphQueryRequest& req,
                                  yams::metadata::KnowledgeGraphStore* kgStore);
    // yams-kt5t: Helper for search mode (search nodes by label pattern)
    boost::asio::awaitable<Response>
    handleGraphQuerySearchMode(const GraphQueryRequest& req,
                               yams::metadata::KnowledgeGraphStore* kgStore);

    // Graph maintenance handlers (PBI-009 Phase 4.3)
    boost::asio::awaitable<Response> handleGraphRepairRequest(const GraphRepairRequest& req);
    boost::asio::awaitable<Response> handleGraphValidateRequest(const GraphValidateRequest& req);

    // KG ingest handler (PBI-093 Phase 2)
    boost::asio::awaitable<Response> handleKgIngestRequest(const KgIngestRequest& req);

    // Legacy helper declarations removed after dispatcher split

private:
    YamsDaemon* daemon_;
    ServiceManager* serviceManager_;
    StateComponent* state_;
    class DaemonMetrics* metrics_{nullptr};
};

} // namespace yams::daemon
