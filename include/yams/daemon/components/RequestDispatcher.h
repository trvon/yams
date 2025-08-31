#pragma once

#include <memory>
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

class DaemonFSM;

class RequestDispatcher {
public:
    RequestDispatcher(YamsDaemon* daemon, ServiceManager* serviceManager, StateComponent* state);
    ~RequestDispatcher();
    void setFsm(DaemonFSM* fsm) { fsm_ = fsm; }

    Response dispatch(const Request& req);

private:
    // One handler for each request type
    Response handleStatusRequest(const StatusRequest& req);
    Response handleShutdownRequest(const ShutdownRequest& req);
    Response handleSearchRequest(const SearchRequest& req);
    Response handleGetRequest(const GetRequest& req);
    Response handleGetInitRequest(const GetInitRequest& req);
    Response handleGetChunkRequest(const GetChunkRequest& req);
    Response handleGetEndRequest(const GetEndRequest& req);
    Response handleAddDocumentRequest(const AddDocumentRequest& req);
    Response handleListRequest(const ListRequest& req);
    Response handleDeleteRequest(const DeleteRequest& req);
    Response handleGetStatsRequest(const GetStatsRequest& req);
    Response handlePingRequest(const PingRequest& req);
    Response handleUpdateDocumentRequest(const UpdateDocumentRequest& req);
    Response handleGrepRequest(const GrepRequest& req);
    Response handleDownloadRequest(const DownloadRequest& req);

    // Helper functions for status reporting
    double getMemoryUsage();
    double getCpuUsage();

    // Search helper methods
    Response handleHashSearch(const SearchRequest& req,
                              std::shared_ptr<metadata::MetadataRepository> metadataRepo);
    Response handleFuzzySearch(const SearchRequest& req,
                               std::shared_ptr<metadata::MetadataRepository> metadataRepo);
    Response handleHybridSearch(const SearchRequest& req,
                                std::shared_ptr<metadata::MetadataRepository> metadataRepo);
    Response handleMetadataSearch(const SearchRequest& req,
                                  std::shared_ptr<metadata::MetadataRepository> metadataRepo);
    bool isValidHash(const std::string& hash);
    std::string truncateSnippet(const std::string& snippet, size_t maxLength);

    // Grep helper methods
    bool matchesAnyPattern(const std::string& filename, const std::vector<std::string>& patterns);
    std::string escapeRegex(const std::string& text);

    // AddDocument helper methods
    Result<std::pair<std::string, std::string>>
    handleSingleFileAdd(const std::filesystem::path& filePath, const AddDocumentRequest& req,
                        std::shared_ptr<yams::api::IContentStore> contentStore,
                        std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo);
    Result<std::pair<std::string, std::string>>
    handleContentAdd(const AddDocumentRequest& req,
                     std::shared_ptr<yams::api::IContentStore> contentStore,
                     std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo);
    Result<size_t>
    handleDirectoryAdd(const std::filesystem::path& dirPath, const AddDocumentRequest& req,
                       std::shared_ptr<yams::api::IContentStore> contentStore,
                       std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo);
    void addTagsAndMetadata(int64_t docId, const std::vector<std::string>& tags,
                            const std::map<std::string, std::string>& metadata,
                            std::shared_ptr<yams::metadata::MetadataRepository> metadataRepo);

private:
    YamsDaemon* daemon_;
    ServiceManager* serviceManager_;
    StateComponent* state_;
    DaemonFSM* fsm_;
};

} // namespace yams::daemon
