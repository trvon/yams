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
}

namespace yams::api {
class IContentStore;
}

namespace yams::daemon {

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

    // Plugin management handlers
    boost::asio::awaitable<Response> handlePluginScanRequest(const PluginScanRequest& req);
    boost::asio::awaitable<Response> handlePluginLoadRequest(const PluginLoadRequest& req);
    boost::asio::awaitable<Response> handlePluginUnloadRequest(const PluginUnloadRequest& req);
    boost::asio::awaitable<Response>
    handlePluginTrustListRequest(const PluginTrustListRequest& req);
    boost::asio::awaitable<Response> handlePluginTrustAddRequest(const PluginTrustAddRequest& req);
    boost::asio::awaitable<Response>
    handlePluginTrustRemoveRequest(const PluginTrustRemoveRequest& req);

    // Embedding/model provider handlers
    boost::asio::awaitable<Response>
    handleGenerateEmbeddingRequest(const GenerateEmbeddingRequest& req);
    boost::asio::awaitable<Response> handleBatchEmbeddingRequest(const BatchEmbeddingRequest& req);
    boost::asio::awaitable<Response> handleEmbedDocumentsRequest(const EmbedDocumentsRequest& req);
    boost::asio::awaitable<Response> handleLoadModelRequest(const LoadModelRequest& req);
    boost::asio::awaitable<Response> handleUnloadModelRequest(const UnloadModelRequest& req);
    boost::asio::awaitable<Response> handleModelStatusRequest(const ModelStatusRequest& req);

    // Legacy helper declarations removed after dispatcher split

private:
    YamsDaemon* daemon_;
    ServiceManager* serviceManager_;
    StateComponent* state_;
    class DaemonMetrics* metrics_{nullptr};
};

} // namespace yams::daemon
