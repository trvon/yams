#pragma once

#include <yams/api/content_store.h>
#include <yams/app/services/factory.hpp>
#include <yams/app/services/services.hpp>
#include <yams/core/types.h>
#include <yams/mcp/error_handling.h>
#include <yams/mcp/tool_registry.h>
#include <yams/metadata/metadata_repository.h>
#include <yams/search/hybrid_search_engine.h>
#include <yams/search/search_executor.h>
#include <yams/version.hpp>

#include <nlohmann/json.hpp>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <regex>
#include <string>
#include <thread>

namespace yams::mcp {

using json = nlohmann::json;

/**
 * Transport interface for MCP communication with modern error handling
 */
class ITransport {
public:
    virtual ~ITransport() = default;
    virtual void send(const json& message) = 0;
    virtual MessageResult receive() = 0;
    virtual bool isConnected() const = 0;
    virtual void close() = 0;
    virtual TransportState getState() const = 0;
};

/**
 * Standard I/O transport (default for MCP) with atomic state management
 */
class StdioTransport : public ITransport {
public:
    StdioTransport();
    void send(const json& message) override;
    MessageResult receive() override;
    bool isConnected() const override { return state_.load() == TransportState::Connected; }
    void close() override { state_.store(TransportState::Closing); }
    TransportState getState() const override { return state_.load(); }

    // Set external shutdown flag for non-blocking checks
    void setShutdownFlag(std::atomic<bool>* shutdown) { externalShutdown_ = shutdown; }

private:
    std::atomic<TransportState> state_{TransportState::Connected};
    std::atomic<bool>* externalShutdown_{nullptr};
    std::atomic<size_t> errorCount_{0};

    // Mutex for thread-safe I/O operations
    static std::mutex io_mutex_;

    // Helper for non-blocking stdin check
    bool isInputAvailable(int timeoutMs = 100) const;

    // Error recovery and circuit breaker
    bool shouldRetryAfterError() const noexcept;
    void recordError() noexcept;
    void resetErrorCount() noexcept;
};

// WebSocket transport removed - not needed for current implementation
// TODO: Add back if WebSocket support is required in the future

/**
 * MCP Server implementation
 */
class MCPServer {
public:
    MCPServer(std::shared_ptr<api::IContentStore> store,
              std::shared_ptr<search::SearchExecutor> searchExecutor,
              std::shared_ptr<metadata::MetadataRepository> metadataRepo,
              std::shared_ptr<search::HybridSearchEngine> hybridEngine = nullptr,
              std::unique_ptr<ITransport> transport = std::make_unique<StdioTransport>(),
              std::atomic<bool>* externalShutdown = nullptr);
    ~MCPServer();

    /**
     * Start the server and begin processing requests
     */
    void start();

    /**
     * Stop the server
     */
    void stop();

    /**
     * Check if server is running
     */
    bool isRunning() const { return running_; }

#ifdef YAMS_TESTING
    // Public testing interface - only available when building tests
    json testListTools() { return listTools(); }

    // Expose modern handle* methods for testing
    // Request types are already in the yams::mcp namespace

    Result<MCPSearchResponse> testHandleSearchDocuments(const MCPSearchRequest& req) {
        return handleSearchDocuments(req);
    }
    Result<MCPGrepResponse> testHandleGrepDocuments(const MCPGrepRequest& req) {
        return handleGrepDocuments(req);
    }
    Result<MCPRetrieveDocumentResponse>
    testHandleRetrieveDocument(const MCPRetrieveDocumentRequest& req) {
        return handleRetrieveDocument(req);
    }
    Result<MCPListDocumentsResponse> testHandleListDocuments(const MCPListDocumentsRequest& req) {
        return handleListDocuments(req);
    }
    Result<MCPStatsResponse> testHandleGetStats(const MCPStatsRequest& req) {
        return handleGetStats(req);
    }
#endif

private:
    // MCP protocol methods with error handling
    MessageResult handleRequest(const json& request);
    json initialize(const json& params);
    json listResources();
    json listTools();
    json listPrompts();
    json callTool(const std::string& name, const json& arguments);
    json readResource(const std::string& uri);

    // Modern C++20 tool handlers (type-safe, clean)
    Result<MCPSearchResponse> handleSearchDocuments(const MCPSearchRequest& req);
    Result<MCPGrepResponse> handleGrepDocuments(const MCPGrepRequest& req);
    Result<MCPDownloadResponse> handleDownload(const MCPDownloadRequest& req);
    Result<MCPStoreDocumentResponse> handleStoreDocument(const MCPStoreDocumentRequest& req);
    Result<MCPRetrieveDocumentResponse>
    handleRetrieveDocument(const MCPRetrieveDocumentRequest& req);
    Result<MCPListDocumentsResponse> handleListDocuments(const MCPListDocumentsRequest& req);
    Result<MCPStatsResponse> handleGetStats(const MCPStatsRequest& req);
    Result<MCPAddDirectoryResponse> handleAddDirectory(const MCPAddDirectoryRequest& req);

    Result<MCPGetByNameResponse> handleGetByName(const MCPGetByNameRequest& req);
    Result<MCPDeleteByNameResponse> handleDeleteByName(const MCPDeleteByNameRequest& req);
    Result<MCPCatDocumentResponse> handleCatDocument(const MCPCatDocumentRequest& req);
    Result<MCPUpdateMetadataResponse> handleUpdateMetadata(const MCPUpdateMetadataRequest& req);
    Result<MCPRestoreCollectionResponse>
    handleRestoreCollection(const MCPRestoreCollectionRequest& req);
    Result<MCPRestoreSnapshotResponse> handleRestoreSnapshot(const MCPRestoreSnapshotRequest& req);
    Result<MCPListCollectionsResponse> handleListCollections(const MCPListCollectionsRequest& req);
    Result<MCPListSnapshotsResponse> handleListSnapshots(const MCPListSnapshotsRequest& req);

    // Legacy JSON-based tool implementations (for gradual migration)
    json storeDocument(const json& args);
    json deleteDocument(const json& args);
    json updateMetadata(const json& args);

    // New v0.0.2 CLI integration tools
    json deleteByName(const json& args);
    json getByName(const json& args);
    json catDocument(const json& args);
    json listDocuments(const json& args);

    // New v0.0.4 directory and collection tools
    json addDirectory(const json& args);
    json restoreCollection(const json& args);
    json restoreSnapshot(const json& args);
    json listCollections(const json& args);
    json listSnapshots(const json& args);

    // Helper methods
    json createResponse(const json& id, const json& result);
    json createError(const json& id, int code, const std::string& message);

    // Name resolution helpers (similar to CLI commands)
    Result<std::string> resolveNameToHash(const std::string& name);
    Result<std::vector<std::pair<std::string, std::string>>>
    resolveNameToHashes(const std::string& name);
    Result<std::vector<std::pair<std::string, std::string>>>
    resolveNamesToHashes(const std::vector<std::string>& names);
    Result<std::vector<std::pair<std::string, std::string>>>
    resolvePatternToHashes(const std::string& pattern);

    // Collection and snapshot helpers
    json performRestore(const std::vector<metadata::DocumentInfo>& documents,
                        const std::string& outputDir, const std::string& layoutTemplate,
                        bool overwrite, bool createDirs, bool dryRun, const std::string& scope);
    std::string
    expandLayoutTemplate(const std::string& layoutTemplate, const metadata::DocumentInfo& doc,
                         const std::unordered_map<std::string, metadata::MetadataValue>& metadata);

    // Helper methods for hash search
    bool isValidHash(const std::string& str);
    json searchByHash(const std::string& hash, size_t limit);

    // Helper method for formatting search results with context
    std::string formatSnippetWithContext(const std::string& content, const std::string& query,
                                         int beforeContext, int afterContext, bool showLineNumbers,
                                         const std::string& colorMode);

    // Helper structures and methods for grep functionality
    struct GrepMatch {
        size_t lineNumber;
        size_t columnStart;
        size_t columnEnd;
        std::string line;
    };

    std::vector<GrepMatch> processGrepFile(const std::string& filename, const std::string& content,
                                           const std::regex& pattern, bool invertMatch,
                                           int maxCount);
    std::string formatGrepContext(const std::string& content, size_t lineNumber, int beforeContext,
                                  int afterContext);

    // Helper structures and methods for knowledge graph functionality
    struct RelatedDocument {
        std::string hash;
        std::string path;
        std::string relationship;
        int distance;
        json metadata;
    };

    std::vector<RelatedDocument> findRelatedDocuments(const metadata::DocumentInfo& baseDoc,
                                                      int depth, int maxResults = 20);
    json buildKnowledgeGraphResponse(const metadata::DocumentInfo& baseDoc,
                                     const std::vector<RelatedDocument>& related,
                                     bool includeContent, const std::string& outputPath = "");

    // Helper methods for file type classification
    std::string getFileTypeFromMime(const std::string& mimeType);
    bool isBinaryMimeType(const std::string& mimeType);

private:
    // Core services
    std::shared_ptr<api::IContentStore> store_;
    std::shared_ptr<search::SearchExecutor> searchExecutor_;
    std::shared_ptr<metadata::MetadataRepository> metadataRepo_;
    std::shared_ptr<search::HybridSearchEngine> hybridEngine_;
    std::unique_ptr<ITransport> transport_;

    // App context and services for business logic
    app::services::AppContext appContext_;
    std::shared_ptr<app::services::ISearchService> searchService_;
    std::shared_ptr<app::services::IGrepService> grepService_;
    std::shared_ptr<app::services::IDocumentService> documentService_;
    std::shared_ptr<app::services::IDownloadService> downloadService_;
    std::shared_ptr<app::services::IIndexingService> indexingService_;
    std::shared_ptr<app::services::IStatsService> statsService_;

    // Modern tool registry
    std::unique_ptr<ToolRegistry> toolRegistry_;

    // Tool registry initialization
    void initializeToolRegistry();

    std::atomic<bool> running_{false};
    std::atomic<bool> initialized_{false};
    std::atomic<bool> readyPending_{false};
    std::atomic<bool>* externalShutdown_{nullptr};

    // Server info
    struct {
        std::string name = "yams-mcp";
        std::string version = YAMS_VERSION_STRING;
    } serverInfo_;

    // Client info (set during initialize)
    struct {
        std::string name;
        std::string version;
    } clientInfo_;

    // Negotiated protocol version (set during initialize)
    std::string negotiatedProtocolVersion_{"2024-11-05"};

public:
    // Process a single JSON-RPC message. For requests (with id), returns a JSON-RPC response.
    // For notifications (no id), returns an error for no response expected.
    MessageResult processMessage(const json& message) {
        auto result = json_utils::validate_jsonrpc_message(message);
        if (!result) {
            return result.error();
        }
        return handleRequest(result.value());
    }
};

} // namespace yams::mcp
